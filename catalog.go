package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	maxCatalogBytes   = 8 << 20 // 读目录响应的上限，超出按回源失败处理
	codexCatalogBytes = 1 << 20 // codex 只收这么大以内的目录，更大的不缓存
)

var errCatalogTooLarge = errors.New("目录响应超过 8 MiB")

// usableCatalog：codex 收得下的目录（合法 JSON、不超过 1 MiB）才写缓存、才拿缓存返回。
func usableCatalog(body []byte) bool { return len(body) <= codexCatalogBytes && json.Valid(body) }

// serveCatalog 处理模型目录请求：先回源，在 catalogTimeout 内拿到 codex 收得下的 200 目录就返回它；
// 回源失败（连接、读响应出错）、网关回 5xx / 429、200 的正文 codex 用不了、或到点还没回时，返回缓存
// （取法见 catalogCache.load）。其他状态（401、404 等）原样转给客户端，不用缓存掩盖。网关已知不可用时
// 直接返回缓存，不再花回源时间——codex 拉目录总共只等 5 秒，失败一次就会清空它手上的目录。
//
// 回源不跟着请求走（startFetch）：同一（凭据, 路径, 查询串）同时只有一个回源，后到的请求等同一个结果；
// 请求到点就不再等，回源在后台做到 catalogFetchTimeout，拿到目录照样写缓存。网关比 catalogTimeout 慢时，
// 缓存靠它刷新，新 key 的第一份缓存也靠它攒下。
//
// 客户端自己没带 If-None-Match 时，用缓存的 ETag 向网关做条件请求，网关回 304 就返回缓存，
// 目录没变时不必再传一整份（约 550 KB）。codex 不发 If-None-Match，也不认 304，对它总是回 200。
// 带了条件头或 Range 的请求（ownFetch）单独回源、结果原样交给它，不与别的请求合并。
func (p *proxy) serveCatalog(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	auth := authHash(r.Header)
	if p.gate.isDown() && p.serveCachedCatalog(w, r, auth, "网关不可用", start) {
		return
	}
	var res catalogResult
	pending := false
	if ownFetch(r.Header) {
		ctx, cancel := context.WithTimeout(r.Context(), p.cfg.CatalogTimeout.D())
		res = p.fetchCatalog(ctx, auth, newCatalogReq(r), "")
		cancel()
		if r.Context().Err() != nil {
			return
		}
	} else {
		f := p.startFetch(auth, r)
		timer := time.NewTimer(time.Until(start.Add(p.cfg.CatalogTimeout.D())))
		defer timer.Stop()
		select {
		case <-f.done:
			res = f.res
		case <-timer.C:
			pending = true
		case <-r.Context().Done():
			return
		}
	}
	var reason string
	switch {
	case pending:
		reason = fmt.Sprintf("回源 %s 内未返回（后台继续）", p.cfg.CatalogTimeout.D())
	case res.err != nil:
		reason = fmt.Sprintf("回源失败: %v", res.err)
	case res.status == http.StatusNotModified && res.inm != "":
		writeCatalog(w, http.StatusOK, res.cached.Header, res.cached.Body, "revalidated")
		log.Printf("GET %s 200 %.1fs 目录未变（304），返回缓存", r.URL.Path, time.Since(start).Seconds())
		return
	case res.status == http.StatusOK && usableCatalog(res.body):
		writeCatalog(w, http.StatusOK, res.header, res.body, "upstream")
		log.Printf("GET %s 200 %.1fs 目录回源 %d 字节", r.URL.Path, time.Since(start).Seconds(), len(res.body))
		return
	case res.status == http.StatusOK:
		reason = fmt.Sprintf("网关返回的目录 codex 用不了（%d 字节，不是 JSON 或超过 1 MiB）", len(res.body))
	case res.status >= 500 || res.status == http.StatusTooManyRequests:
		reason = "网关返回 " + strconv.Itoa(res.status)
	default:
		writeCatalog(w, res.status, res.header, res.body, "upstream")
		log.Printf("GET %s %d %.1fs 目录回源（原样转发）", r.URL.Path, res.status, time.Since(start).Seconds())
		return
	}
	if p.serveCachedCatalog(w, r, auth, reason, start) {
		return
	}
	log.Printf("GET %s %s，没有缓存", r.URL.Path, reason)
	if !pending && res.err == nil {
		writeCatalog(w, res.status, res.header, res.body, "upstream")
		return
	}
	writeError(w, http.StatusBadGateway, "local proxy: "+reason)
}

// serveCachedCatalog 返回缓存（取法见 catalogCache.load）；没有可用的缓存时返回 false、不写响应。
// 借用其他凭据的缓存时响应头标 cache-shared，日志写明借自哪个凭据摘要。
func (p *proxy) serveCachedCatalog(w http.ResponseWriter, r *http.Request, auth, reason string, start time.Time) bool {
	e, from := p.cache.load(auth, r.URL.Path, r.URL.RawQuery, p.cfg.CatalogShareAcrossKeys)
	if e == nil {
		return false
	}
	source, note := "cache", ""
	if from != auth {
		source, note = "cache-shared", "，借自凭据 "+from
	}
	writeCatalog(w, http.StatusOK, e.Header, e.Body, source)
	log.Printf("GET %s 200 %.1fs %s，返回 %s 的目录缓存（query %q%s）",
		r.URL.Path, time.Since(start).Seconds(), reason, e.FetchedAt.Format(time.DateTime), e.Query, note)
	return true
}

// ownFetchHeaders 是让响应只属于发出它的那个请求的头（条件请求、范围请求）。
var ownFetchHeaders = []string{"If-None-Match", "If-Modified-Since", "If-Match", "If-Unmodified-Since", "Range", "If-Range"}

// ownFetch 判断请求是否带了 ownFetchHeaders 之一：带了就单独回源，不加入、也不发起共享的回源。
func ownFetch(h http.Header) bool {
	for _, k := range ownFetchHeaders {
		if h.Get(k) != "" {
			return true
		}
	}
	return false
}

// catalogReq 是回源要用的请求内容，在请求进来时取出：回源可能比请求活得久。
type catalogReq struct {
	path, rawPath, query string
	header               http.Header
}

func newCatalogReq(r *http.Request) catalogReq {
	return catalogReq{path: r.URL.Path, rawPath: r.URL.RawPath, query: r.URL.RawQuery, header: endToEnd(r.Header)}
}

// catalogResult 是一次回源的结果；可能交给多个等待的请求，只读。inm 是代理自己带的 If-None-Match，
// cached 是它所依据的缓存（网关回 304 时返回它）。
type catalogResult struct {
	status int
	header http.Header
	body   []byte
	err    error
	inm    string
	cached *catalogEntry
}

func (res catalogResult) summary() string {
	switch {
	case res.err != nil:
		return fmt.Sprintf("失败: %v", res.err)
	case res.status == http.StatusOK && usableCatalog(res.body):
		return fmt.Sprintf("200，已写缓存 %d 字节", len(res.body))
	case res.status == http.StatusNotModified && res.inm != "":
		return "目录未变（304）"
	default:
		return "网关返回 " + strconv.Itoa(res.status)
	}
}

// catalogFetches 登记进行中的目录回源，同一个键同时只有一个。
type catalogFetches struct {
	mu sync.Mutex
	m  map[string]*catalogFetch
}

type catalogFetch struct {
	done chan struct{} // 回源结束时关闭，之后 res 只读
	res  catalogResult
}

// start 返回键上进行中的回源；没有就在新协程里用 run 发起一个。
func (fs *catalogFetches) start(key string, run func() catalogResult) *catalogFetch {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if f := fs.m[key]; f != nil {
		return f
	}
	f := &catalogFetch{done: make(chan struct{})}
	fs.m[key] = f
	go func() {
		f.res = run()
		fs.mu.Lock()
		delete(fs.m, key)
		fs.mu.Unlock()
		close(f.done)
	}()
	return f
}

// startFetch 取这个请求的（凭据, 路径, 查询串）上进行中的回源，没有就发起一个。回源用自己的上下文、
// 上限 catalogFetchTimeout，不随发起它的请求结束而取消。比 catalogTimeout 慢的回源结束时记一行日志：
// 那时可能已经没有请求在等它。
func (p *proxy) startFetch(auth string, r *http.Request) *catalogFetch {
	q := newCatalogReq(r)
	return p.fetches.start(auth+"\x00"+q.path+"?"+q.query, func() catalogResult {
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), p.cfg.CatalogFetchTimeout.D())
		defer cancel()
		cached := p.cache.exact(auth, q.path, q.query)
		inm := ""
		if cached != nil {
			inm = cached.Header.Get("Etag")
		}
		res := p.fetchCatalog(ctx, auth, q, inm)
		res.cached = cached
		if el := time.Since(start); el > p.cfg.CatalogTimeout.D() {
			log.Printf("GET %s 后台回源 %.1fs：%s", q.path, el.Seconds(), res.summary())
		}
		return res
	})
}

// fetchCatalog 向网关取一次目录，ctx 管到响应体读完；inm 非空时带上 If-None-Match。拿到 codex 收得下的
// 200 目录就写缓存；网关地址已在配置里换掉（闸门停用）时不写，结果仍交给等它的请求。连接与读响应出错时
// 把网关标为不可用；ctx 到点或被取消引起的不算——目录接口慢不说明网关连不上，标成不可用只会让其他请求
// 白等一次探活。
func (p *proxy) fetchCatalog(ctx context.Context, auth string, q catalogReq, inm string) catalogResult {
	res := catalogResult{inm: inm}
	res.status, res.header, res.body, res.err = p.getCatalog(ctx, q, inm)
	if res.err != nil && !errors.Is(res.err, errCatalogTooLarge) && ctx.Err() == nil {
		p.gate.reportFailure(fmt.Errorf("目录回源: %w", res.err))
	}
	if res.err == nil && res.status == http.StatusOK && usableCatalog(res.body) {
		e := &catalogEntry{FetchedAt: time.Now(), Path: q.path, Query: q.query, Header: res.header, Body: res.body}
		var err error
		if !p.gate.ifActive(func() { err = p.cache.store(auth, e) }) {
			log.Printf("GET %s 网关地址已换，原地址取得的目录不写缓存", q.path)
		} else if err != nil {
			log.Printf("写目录缓存失败: %v", err)
		}
	}
	return res
}

func (p *proxy) getCatalog(ctx context.Context, q catalogReq, inm string) (int, http.Header, []byte, error) {
	u := *p.cfg.upstreamURL
	u.Path, u.RawPath, u.RawQuery = q.path, q.rawPath, q.query
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return 0, nil, nil, err
	}
	req.Header = q.header.Clone()
	req.Header.Del("Accept-Encoding") // 要明文，缓存与返回都不涉及编码
	if inm != "" {
		req.Header.Set("If-None-Match", inm)
	}
	resp, err := p.catalog.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxCatalogBytes+1))
	if err != nil {
		return 0, nil, nil, err
	}
	if len(body) > maxCatalogBytes {
		return 0, nil, nil, errCatalogTooLarge
	}
	h := endToEnd(resp.Header)
	h.Del("Content-Length")
	h.Del("Date")
	return resp.StatusCode, h, body, nil
}

func writeCatalog(w http.ResponseWriter, status int, header http.Header, body []byte, source string) {
	maps.Copy(w.Header(), header)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.Header().Set("X-Local-Proxy-Catalog", source)
	w.WriteHeader(status)
	w.Write(body)
}

// hopHeaders 是逐跳头，不随请求或缓存转给另一端。
var hopHeaders = []string{
	"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
	"Te", "Trailer", "Transfer-Encoding", "Upgrade",
}

func endToEnd(h http.Header) http.Header {
	out := h.Clone()
	for _, k := range hopHeaders {
		out.Del(k)
	}
	return out
}

// authHash 是凭据的摘要：不同 key 可能属于不同分组、拿到不同目录，缓存按它分开，文件里不存凭据原文。
func authHash(h http.Header) string {
	sum := sha256.Sum256([]byte(h.Get("Authorization") + "\x00" + h.Get("X-Api-Key")))
	return hex.EncodeToString(sum[:8])
}

type catalogEntry struct {
	FetchedAt time.Time   `json:"fetched_at"`
	Path      string      `json:"path"`
	Query     string      `json:"query"`
	Header    http.Header `json:"header"`
	Body      []byte      `json:"body"`
}

// catalogCache 把每个（凭据, 路径, 查询串）的最近一次成功目录存成一个文件，进程重启后仍可用。
type catalogCache struct {
	dir string
	mu  sync.Mutex // Windows 上文件被读时不能被 rename 覆盖，读写串行
}

func (c *catalogCache) file(auth, path, query string) string {
	sum := sha256.Sum256([]byte(path + "?" + query))
	return filepath.Join(c.dir, "catalog-"+auth+"-"+hex.EncodeToString(sum[:8])+".json")
}

func (c *catalogCache) store(auth string, e *catalogEntry) error {
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	tmp, err := os.CreateTemp(c.dir, "catalog-*.tmp")
	if err != nil {
		return err
	}
	_, err = tmp.Write(data)
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp.Name(), c.file(auth, e.Path, e.Query))
	}
	if err != nil {
		os.Remove(tmp.Name())
	}
	return err
}

// exact 取查询串完全相同的缓存。
func (c *catalogCache) exact(auth, path, query string) *catalogEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	return readEntry(c.file(auth, path, query))
}

// load 取可用的缓存，依次：同一凭据、查询串完全相同；同一凭据、查询串只差 client_version 的最新一份
// （codex 升级后 client_version 变了，首次回源就失败时仍有目录可用）。share 时再依次取其他凭据查询串
// 完全相同、只差 client_version 的最新一份（新 key 还没攒下自己的缓存时）。返回缓存与它所属的凭据摘要。
func (c *catalogCache) load(auth, path, query string, share bool) (*catalogEntry, string) {
	if e := c.exact(auth, path, query); e != nil {
		return e, auth
	}
	pattern := "catalog-" + auth + "-*.json"
	if share {
		pattern = "catalog-*.json"
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	names, _ := filepath.Glob(filepath.Join(c.dir, pattern))
	var best *catalogEntry
	bestFrom, bestRank := "", 0
	for _, name := range names {
		e := readEntry(name)
		if e == nil || e.Path != path || withoutVersion(e.Query) != withoutVersion(query) {
			continue
		}
		from := entryAuth(name)
		rank := 1 // 小的优先：1 同一凭据跨版本，2 其他凭据同查询串，3 其他凭据跨版本
		if from != auth {
			rank = 3
			if e.Query == query {
				rank = 2
			}
		}
		if best == nil || rank < bestRank || rank == bestRank && e.FetchedAt.After(best.FetchedAt) {
			best, bestFrom, bestRank = e, from, rank
		}
	}
	return best, bestFrom
}

// entryAuth 从缓存文件名 catalog-<凭据摘要>-<路径与查询串摘要>.json 取凭据摘要。
func entryAuth(name string) string {
	auth, _, _ := strings.Cut(strings.TrimPrefix(filepath.Base(name), "catalog-"), "-")
	return auth
}

func readEntry(name string) *catalogEntry {
	data, err := os.ReadFile(name)
	if err != nil {
		return nil
	}
	var e catalogEntry
	if json.Unmarshal(data, &e) != nil || !usableCatalog(e.Body) {
		return nil
	}
	return &e
}

// withoutVersion 去掉查询串里的 client_version，跨 codex 版本回退时用它比较其余参数。
func withoutVersion(query string) string {
	v, err := url.ParseQuery(query)
	if err != nil {
		return query
	}
	v.Del("client_version")
	return v.Encode()
}
