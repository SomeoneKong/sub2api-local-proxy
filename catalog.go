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

// serveCatalog 处理模型目录请求：先回源，拿到 codex 收得下的 200 目录就更新缓存并返回；
// 回源失败（连接、超时、读响应出错）、网关回 5xx / 429、或 200 的正文 codex 用不了时，返回同一把 key
// 的最新缓存。其他状态（401、404 等）原样转给客户端，不用缓存掩盖。网关已知不可用时直接返回缓存，
// 不再花回源超时——codex 拉目录总共只等 5 秒，失败一次就会清空它手上的目录。
//
// 客户端自己没带 If-None-Match 时，用缓存的 ETag 向网关做条件请求，网关回 304 就返回缓存，
// 目录没变时不必再传一整份（约 550 KB）。codex 不发 If-None-Match，也不认 304，对它总是回 200。
func (p *proxy) serveCatalog(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	auth := authHash(r.Header)
	if p.gate.isDown() && p.serveCachedCatalog(w, r, auth, "网关不可用", start) {
		return
	}
	var cached *catalogEntry
	inm := ""
	if r.Header.Get("If-None-Match") == "" {
		if cached = p.cache.exact(auth, r.URL.Path, r.URL.RawQuery); cached != nil {
			inm = cached.Header.Get("Etag")
		}
	}
	status, header, body, err := p.fetchCatalog(r, inm)
	if err != nil && !errors.Is(err, errCatalogTooLarge) && r.Context().Err() == nil {
		p.gate.reportFailure(fmt.Errorf("目录回源: %w", err))
	}
	var reason string
	switch {
	case err != nil:
		reason = fmt.Sprintf("回源失败: %v", err)
	case status == http.StatusNotModified && inm != "":
		writeCatalog(w, http.StatusOK, cached.Header, cached.Body, "revalidated")
		log.Printf("GET %s 200 %.1fs 目录未变（304），返回缓存", r.URL.Path, time.Since(start).Seconds())
		return
	case status == http.StatusOK && usableCatalog(body):
		e := &catalogEntry{FetchedAt: time.Now(), Path: r.URL.Path, Query: r.URL.RawQuery, Header: header, Body: body}
		if err := p.cache.store(auth, e); err != nil {
			log.Printf("写目录缓存失败: %v", err)
		}
		writeCatalog(w, http.StatusOK, header, body, "upstream")
		log.Printf("GET %s 200 %.1fs 目录回源 %d 字节", r.URL.Path, time.Since(start).Seconds(), len(body))
		return
	case status == http.StatusOK:
		reason = fmt.Sprintf("网关返回的目录 codex 用不了（%d 字节，不是 JSON 或超过 1 MiB）", len(body))
	case status >= 500 || status == http.StatusTooManyRequests:
		reason = "网关返回 " + strconv.Itoa(status)
	default:
		writeCatalog(w, status, header, body, "upstream")
		log.Printf("GET %s %d %.1fs 目录回源（原样转发）", r.URL.Path, status, time.Since(start).Seconds())
		return
	}
	if p.serveCachedCatalog(w, r, auth, reason, start) {
		return
	}
	log.Printf("GET %s %s，没有缓存", r.URL.Path, reason)
	if err == nil {
		writeCatalog(w, status, header, body, "upstream")
		return
	}
	writeError(w, http.StatusBadGateway, "local proxy: "+reason)
}

// serveCachedCatalog 返回同一把 key 的最新缓存；没有缓存时返回 false、不写响应。
func (p *proxy) serveCachedCatalog(w http.ResponseWriter, r *http.Request, auth, reason string, start time.Time) bool {
	e := p.cache.load(auth, r.URL.Path, r.URL.RawQuery)
	if e == nil {
		return false
	}
	writeCatalog(w, http.StatusOK, e.Header, e.Body, "cache")
	log.Printf("GET %s 200 %.1fs %s，返回 %s 的目录缓存（query %q）",
		r.URL.Path, time.Since(start).Seconds(), reason, e.FetchedAt.Format(time.DateTime), e.Query)
	return true
}

// fetchCatalog 在 catalogTimeout 内向网关取目录；超时覆盖整个响应体的读取。inm 非空时带上 If-None-Match。
func (p *proxy) fetchCatalog(r *http.Request, inm string) (int, http.Header, []byte, error) {
	ctx, cancel := context.WithTimeout(r.Context(), p.cfg.CatalogTimeout.D())
	defer cancel()
	u := *p.cfg.upstreamURL
	u.Path, u.RawPath, u.RawQuery = r.URL.Path, r.URL.RawPath, r.URL.RawQuery
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return 0, nil, nil, err
	}
	req.Header = endToEnd(r.Header)
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

// load 先取查询串完全相同的缓存；没有时取同一凭据、同一路径、查询串只差 client_version 的最新一份
// （codex 升级后 client_version 变了，首次回源就失败时仍有目录可用）。
func (c *catalogCache) load(auth, path, query string) *catalogEntry {
	if e := c.exact(auth, path, query); e != nil {
		return e
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	names, _ := filepath.Glob(filepath.Join(c.dir, "catalog-"+auth+"-*.json"))
	var best *catalogEntry
	for _, name := range names {
		if e := readEntry(name); e != nil && e.Path == path && withoutVersion(e.Query) == withoutVersion(query) &&
			(best == nil || e.FetchedAt.After(best.FetchedAt)) {
			best = e
		}
	}
	return best
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
