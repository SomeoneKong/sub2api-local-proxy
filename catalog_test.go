package main

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// catalogUpstream 模拟网关的目录接口；mode 控制当前的表现。
type catalogUpstream struct {
	*httptest.Server
	mode atomic.Value // "ok" | "503" | "401" | "slow" | "bad-json" | "big" | "down"
	body atomic.Value // 当前目录
	hits atomic.Int32 // 打到网关的目录请求数（含不可达时被关掉的），不含探活
}

func newCatalogUpstream(t *testing.T) *catalogUpstream {
	u := &catalogUpstream{}
	u.mode.Store("ok")
	u.body.Store(`{"models":[{"slug":"gpt-6.1-sol"}]}`)
	u.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if u.mode.Load() == "down" { // 经 TUN 时网关不可达的表现：连上即被关
			if r.URL.Path != "/health" {
				u.hits.Add(1)
			}
			if conn, _, err := http.NewResponseController(w).Hijack(); err == nil {
				conn.Close()
			}
			return
		}
		if r.URL.Path == "/health" {
			io.WriteString(w, "ok")
			return
		}
		u.hits.Add(1)
		switch u.mode.Load() {
		case "503":
			http.Error(w, "upstream down", http.StatusServiceUnavailable)
			return
		case "401":
			http.Error(w, `{"error":"invalid key"}`, http.StatusUnauthorized)
			return
		case "slow":
			time.Sleep(2 * time.Second)
		case "bad-json":
			io.WriteString(w, "<html>oops</html>")
			return
		case "big": // 合法 JSON，但超过 codex 收得下的 1 MiB
			io.WriteString(w, `{"models":[],"pad":"`+strings.Repeat("x", codexCatalogBytes)+`"}`)
			return
		}
		etag := fmt.Sprintf(`W/"%x"`, sha256.Sum256([]byte(u.body.Load().(string))))
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("ETag", etag)
		io.WriteString(w, u.body.Load().(string)+"\n")
	}))
	t.Cleanup(u.Close)
	return u
}

func getCatalog(t *testing.T, base, key, version string) (int, string, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, base+"/v1/models?client_version="+url.QueryEscape(version), nil)
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), resp.Header.Get("X-Local-Proxy-Catalog")
}

const catalogV1 = `{"models":[{"slug":"gpt-6.1-sol"}]}` + "\n"

// waitGateUp 等探活把网关标回可用（回源连接失败会触发一次探活）。
func waitGateUp(t *testing.T, p *proxy) {
	t.Helper()
	for i := 0; i < 100 && p.gate.isDown(); i++ {
		time.Sleep(20 * time.Millisecond)
	}
	if p.gate.isDown() {
		t.Fatal("网关一直被标为不可用")
	}
}

func TestCatalogFallsBackToCache(t *testing.T) {
	up := newCatalogUpstream(t)
	px, p := newTestProxy(t, up.URL)
	if code, body, src := getCatalog(t, px.URL, "k1", "0.160.1"); code != 200 || body != catalogV1 || src != "upstream" {
		t.Fatalf("首次回源: %d %q %q", code, body, src)
	}
	for _, mode := range []string{"bad-json", "big", "503", "slow"} {
		up.mode.Store(mode)
		start := time.Now()
		code, body, src := getCatalog(t, px.URL, "k1", "0.160.1")
		if code != 200 || body != catalogV1 || src != "cache" {
			t.Fatalf("%s: %d %q %q，应返回缓存", mode, code, body, src)
		}
		if el := time.Since(start); el > 1500*time.Millisecond {
			t.Fatalf("%s: 用了 %v，应在回源超时后立即返回缓存", mode, el)
		}
		waitGateUp(t, p)
		waitFetchesIdle(t, p) // 慢网关的回源在后台继续，等它结束再换下一种表现
	}
	// 网关恢复且目录更新后，返回新目录并更新缓存。
	up.mode.Store("ok")
	up.body.Store(`{"models":[{"slug":"gpt-6.1-sol"},{"slug":"gpt-6.2-sol"}]}`)
	if _, body, src := getCatalog(t, px.URL, "k1", "0.160.1"); src != "upstream" || body == catalogV1 {
		t.Fatalf("恢复后应回源拿到新目录，得到 %q %q", body, src)
	}
	up.mode.Store("503")
	if _, body, src := getCatalog(t, px.URL, "k1", "0.160.1"); src != "cache" || body == catalogV1 {
		t.Fatalf("缓存应是最新一次成功的目录，得到 %q %q", body, src)
	}
}

func TestCatalogCacheSurvivesRestart(t *testing.T) {
	up := newCatalogUpstream(t)
	dir := t.TempDir()
	px1, _ := newTestProxy(t, up.URL, func(c *config) { c.cacheDir = dir })
	getCatalog(t, px1.URL, "k1", "0.160.1")
	px1.Close()
	up.Close() // 网关彻底连不上
	px2, _ := newTestProxy(t, up.URL, func(c *config) { c.cacheDir = dir })
	if code, body, src := getCatalog(t, px2.URL, "k1", "0.160.1"); code != 200 || body != catalogV1 || src != "cache" {
		t.Fatalf("重启后: %d %q %q", code, body, src)
	}
}

func TestCatalogNotMaskingClientErrors(t *testing.T) {
	up := newCatalogUpstream(t)
	px, _ := newTestProxy(t, up.URL)
	getCatalog(t, px.URL, "k1", "0.160.1")
	up.mode.Store("401")
	if code, _, src := getCatalog(t, px.URL, "k1", "0.160.1"); code != 401 || src != "upstream" {
		t.Fatalf("401 应原样转发，得到 %d %q", code, src)
	}
}

func TestCatalogCacheIsPerKey(t *testing.T) {
	up := newCatalogUpstream(t)
	px, _ := newTestProxy(t, up.URL)
	getCatalog(t, px.URL, "k1", "0.160.1")
	up.mode.Store("503")
	if code, _, src := getCatalog(t, px.URL, "k2", "0.160.1"); code != 503 || src != "upstream" {
		t.Fatalf("另一把 key 不应拿到 k1 的缓存，得到 %d %q", code, src)
	}
}

func TestCatalogFallbackAcrossClientVersion(t *testing.T) {
	up := newCatalogUpstream(t)
	px, _ := newTestProxy(t, up.URL)
	getCatalog(t, px.URL, "k1", "0.160.1")
	up.mode.Store("503")
	if code, body, src := getCatalog(t, px.URL, "k1", "0.161.0"); code != 200 || body != catalogV1 || src != "cache" {
		t.Fatalf("codex 升级后首次回源失败，应退到同一 key 的最新缓存，得到 %d %q %q", code, body, src)
	}
}

func TestCatalogNoCacheUnreachable(t *testing.T) {
	up := newCatalogUpstream(t)
	up.Close()
	px, _ := newTestProxy(t, up.URL)
	if code, _, _ := getCatalog(t, px.URL, "k1", "0.160.1"); code != http.StatusBadGateway {
		t.Fatalf("没有缓存且连不上，应为 502，得到 %d", code)
	}
}

// 目录没变时网关回 304，代理返回缓存；客户端自己带 If-None-Match 时 304 原样转给它。
func TestCatalogRevalidate(t *testing.T) {
	up := newCatalogUpstream(t)
	px, _ := newTestProxy(t, up.URL)
	getCatalog(t, px.URL, "k1", "0.160.1")
	if code, body, src := getCatalog(t, px.URL, "k1", "0.160.1"); code != 200 || body != catalogV1 || src != "revalidated" {
		t.Fatalf("目录未变: %d %q %q", code, body, src)
	}
	up.body.Store(`{"models":[]}`)
	if _, body, src := getCatalog(t, px.URL, "k1", "0.160.1"); src != "upstream" || body != `{"models":[]}`+"\n" {
		t.Fatalf("目录变了应拿到新目录: %q %q", body, src)
	}
	req, _ := http.NewRequest(http.MethodGet, px.URL+"/v1/models?client_version=0.160.1", nil)
	req.Header.Set("Authorization", "Bearer k1")
	req.Header.Set("If-None-Match", fmt.Sprintf(`W/"%x"`, sha256.Sum256([]byte(`{"models":[]}`))))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotModified {
		t.Fatalf("客户端自带 If-None-Match，应原样拿到 304，得到 %d", resp.StatusCode)
	}
}

// 已知网关不可用时直接返回缓存，不花回源超时，也不去撞网关。
func TestCatalogServesCacheImmediatelyWhenDown(t *testing.T) {
	up := newCatalogUpstream(t)
	px, p := newTestProxy(t, up.URL, func(c *config) { c.CatalogTimeout = duration(3 * time.Second) })
	getCatalog(t, px.URL, "k1", "0.160.1")
	up.mode.Store("down")
	p.gate.reportFailure(errors.New("测试"))
	hits := up.hits.Load()
	start := time.Now()
	if code, body, src := getCatalog(t, px.URL, "k1", "0.160.1"); code != 200 || body != catalogV1 || src != "cache" {
		t.Fatalf("%d %q %q", code, body, src)
	}
	if el := time.Since(start); el > 300*time.Millisecond || up.hits.Load() != hits {
		t.Fatalf("用了 %v、回源 %d 次，应直接返回缓存", el, up.hits.Load()-hits)
	}
}

// 回源时连上即被关（TUN 下网关不可达）：返回缓存，并把网关标为不可用。
func TestCatalogConnectionDropMarksDown(t *testing.T) {
	up := newCatalogUpstream(t)
	px, p := newTestProxy(t, up.URL)
	getCatalog(t, px.URL, "k1", "0.160.1")
	up.mode.Store("down")
	if code, _, src := getCatalog(t, px.URL, "k1", "0.160.1"); code != 200 || src != "cache" {
		t.Fatalf("%d %q", code, src)
	}
	if !p.gate.isDown() {
		t.Fatal("连接被关应把网关标为不可用")
	}
	up.mode.Store("ok")
	waitGateUp(t, p)
}

func TestCatalogCacheLoadOrder(t *testing.T) {
	c := &catalogCache{dir: t.TempDir()}
	base := time.Now()
	put := func(path, query, body string, age time.Duration) {
		e := &catalogEntry{FetchedAt: base.Add(-age), Path: path, Query: query, Body: []byte(body)}
		if err := c.store("a", e); err != nil {
			t.Fatal(err)
		}
	}
	put("/v1/models", "client_version=0.160.1", `"exact"`, time.Hour)
	put("/v1/models", "client_version=0.159.0", `"newer"`, time.Minute)
	put("/models", "client_version=0.161.0", `"other-path"`, 0)
	big := `"` + strings.Repeat("x", codexCatalogBytes) + `"`
	put("/v1/models", "client_version=0.158.0", big, 0) // 更新但超过 1 MiB 的旧缓存要跳过
	if e, _ := c.load("a", "/v1/models", "client_version=0.160.1", false); e == nil || string(e.Body) != `"exact"` {
		t.Fatalf("应先取查询串完全相同的缓存，得到 %+v", e)
	}
	if e, _ := c.load("a", "/v1/models", "client_version=0.162.0", false); e == nil || string(e.Body) != `"newer"` {
		t.Fatalf("回退应取同一路径下最新的一份，得到 %+v", e)
	}
	if e, _ := c.load("b", "/v1/models", "client_version=0.160.1", false); e != nil {
		t.Fatalf("不共享时另一把 key 不应取到缓存，得到 %+v", e)
	}
	// 共享时：其他凭据查询串完全相同的优先于更新的跨版本缓存；本凭据的跨版本缓存又优先于其他凭据的。
	if e, from := c.load("b", "/v1/models", "client_version=0.160.1", true); e == nil || string(e.Body) != `"exact"` || from != "a" {
		t.Fatalf("共享时应取 a 的同查询串缓存，得到 %+v（来自 %q）", e, from)
	}
	if e, from := c.load("b", "/v1/models", "client_version=0.163.0", true); e == nil || string(e.Body) != `"newer"` || from != "a" {
		t.Fatalf("共享时没有同查询串的应取 a 最新的一份，得到 %+v（来自 %q）", e, from)
	}
	put2 := func(auth, query, body string) {
		if err := c.store(auth, &catalogEntry{FetchedAt: base.Add(-2 * time.Hour), Path: "/v1/models", Query: query, Body: []byte(body)}); err != nil {
			t.Fatal(err)
		}
	}
	put2("b", "client_version=0.150.0", `"own-old"`)
	if e, from := c.load("b", "/v1/models", "client_version=0.160.1", true); e == nil || string(e.Body) != `"own-old"` || from != "b" {
		t.Fatalf("本凭据有跨版本缓存时应先用自己的，得到 %+v（来自 %q）", e, from)
	}
}

// waitFetchesIdle 等后台回源全部结束（测试结束前调用，免得回源写缓存时临时目录已被删）。
func waitFetchesIdle(t *testing.T, p *proxy) {
	t.Helper()
	for i := 0; i < 250; i++ {
		p.fetches.mu.Lock()
		n := len(p.fetches.m)
		p.fetches.mu.Unlock()
		if n == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("后台回源一直没有结束")
}

// 网关比 catalogTimeout 慢：请求到点拿缓存，回源在后台做完并刷新缓存；慢不把网关标为不可用。
func TestCatalogSlowUpstreamRefreshesInBackground(t *testing.T) {
	up := newCatalogUpstream(t)
	px, p := newTestProxy(t, up.URL)
	getCatalog(t, px.URL, "k1", "0.160.1")
	const v2 = `{"models":[{"slug":"gpt-6.1-sol"},{"slug":"gpt-6.2-sol"}]}`
	up.body.Store(v2)
	up.mode.Store("slow")
	start := time.Now()
	if code, body, src := getCatalog(t, px.URL, "k1", "0.160.1"); code != 200 || body != catalogV1 || src != "cache" {
		t.Fatalf("慢网关: %d %q %q，应到点返回旧缓存", code, body, src)
	}
	if el := time.Since(start); el > 1500*time.Millisecond {
		t.Fatalf("用了 %v，应在 catalog_timeout 到点返回", el)
	}
	if p.gate.isDown() {
		t.Fatal("目录接口慢不应把网关标为不可用")
	}
	waitFetchesIdle(t, p)
	up.mode.Store("503")
	if _, body, src := getCatalog(t, px.URL, "k1", "0.160.1"); src != "cache" || body != v2+"\n" {
		t.Fatalf("后台回源应已把新目录写进缓存，得到 %q %q", body, src)
	}
}

// 同一凭据并发的目录请求只回源一次。
func TestCatalogConcurrentRequestsShareFetch(t *testing.T) {
	up := newCatalogUpstream(t)
	px, p := newTestProxy(t, up.URL)
	getCatalog(t, px.URL, "k1", "0.160.1")
	up.mode.Store("slow")
	hits := up.hits.Load()
	done := make(chan string, 5)
	for i := 0; i < 5; i++ {
		go func() {
			req, _ := http.NewRequest(http.MethodGet, px.URL+"/v1/models?client_version=0.160.1", nil)
			req.Header.Set("Authorization", "Bearer k1")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				done <- err.Error()
				return
			}
			resp.Body.Close()
			done <- resp.Header.Get("X-Local-Proxy-Catalog")
		}()
	}
	for i := 0; i < 5; i++ {
		if src := <-done; src != "cache" {
			t.Fatalf("第 %d 个请求来源 %q，应为 cache", i, src)
		}
	}
	waitFetchesIdle(t, p)
	if n := up.hits.Load() - hits; n != 1 {
		t.Fatalf("并发 5 个请求回源 %d 次，应只 1 次", n)
	}
}

// 带条件头或 Range 的请求单独回源：不加入普通请求在途的回源，普通请求也不加入它的；两个普通请求仍只回源一次。
func TestCatalogOwnFetchNotShared(t *testing.T) {
	up := newCatalogUpstream(t)
	px, p := newTestProxy(t, up.URL, func(c *config) { c.CatalogTimeout = duration(3 * time.Second) })
	up.mode.Store("slow")
	send := func(h http.Header) <-chan int {
		ch := make(chan int, 1)
		go func() {
			req, _ := http.NewRequest(http.MethodGet, px.URL+"/v1/models?client_version=0.160.1", nil)
			req.Header = h
			req.Header.Set("Authorization", "Bearer k1")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				ch <- 0
				return
			}
			resp.Body.Close()
			ch <- resp.StatusCode
		}()
		return ch
	}
	for _, k := range []string{"If-Modified-Since", "Range", "If-Match"} {
		for _, ownFirst := range []bool{false, true} {
			hits := up.hits.Load()
			own := http.Header{k: {"x"}}
			var a, b, c <-chan int
			if ownFirst {
				a = send(own)
				time.Sleep(100 * time.Millisecond)
				b, c = send(http.Header{}), send(http.Header{})
			} else {
				b, c = send(http.Header{}), send(http.Header{})
				time.Sleep(100 * time.Millisecond)
				a = send(own)
			}
			<-a
			<-b
			<-c
			waitFetchesIdle(t, p)
			if n := up.hits.Load() - hits; n != 2 {
				t.Fatalf("%s（先发 %v）: 回源 %d 次，应为 2（带头的单独一次、两个普通请求合并一次）", k, ownFirst, n)
			}
		}
	}
}

// 新 key 还没有缓存、客户端先走了：回源照样做完，留下这把 key 的第一份缓存。
func TestCatalogBackgroundFetchOutlivesClient(t *testing.T) {
	up := newCatalogUpstream(t)
	px, p := newTestProxy(t, up.URL, func(c *config) { c.CatalogTimeout = duration(3 * time.Second) })
	up.mode.Store("slow")
	req, _ := http.NewRequest(http.MethodGet, px.URL+"/v1/models?client_version=0.160.1", nil)
	req.Header.Set("Authorization", "Bearer k1")
	if _, err := (&http.Client{Timeout: 300 * time.Millisecond}).Do(req); err == nil {
		t.Fatal("客户端应先超时")
	}
	waitFetchesIdle(t, p)
	up.mode.Store("503")
	if code, body, src := getCatalog(t, px.URL, "k1", "0.160.1"); code != 200 || body != catalogV1 || src != "cache" {
		t.Fatalf("客户端离开后回源应仍写下缓存，得到 %d %q %q", code, body, src)
	}
}

// 回源超过 catalog_fetch_timeout：放弃，不写缓存，也不把网关标为不可用。
func TestCatalogFetchTimeout(t *testing.T) {
	up := newCatalogUpstream(t)
	px, p := newTestProxy(t, up.URL, func(c *config) { c.CatalogFetchTimeout = duration(time.Second) })
	up.mode.Store("slow")
	if code, _, _ := getCatalog(t, px.URL, "k1", "0.160.1"); code != http.StatusBadGateway {
		t.Fatalf("没有缓存、到点未回，应为 502，得到 %d", code)
	}
	waitFetchesIdle(t, p)
	if p.gate.isDown() {
		t.Fatal("回源超时不应把网关标为不可用")
	}
	if e, _ := p.cache.load(authHash(http.Header{"Authorization": {"Bearer k1"}}), "/v1/models", "client_version=0.160.1", false); e != nil {
		t.Fatal("超时放弃的回源不应写缓存")
	}
}

// 打开共享时，没有自己缓存的 key 借用其他 key 的缓存，响应头标 cache-shared。
func TestCatalogSharedAcrossKeys(t *testing.T) {
	up := newCatalogUpstream(t)
	px, _ := newTestProxy(t, up.URL, func(c *config) { c.CatalogShareAcrossKeys = true })
	getCatalog(t, px.URL, "k1", "0.160.1")
	up.mode.Store("503")
	if code, body, src := getCatalog(t, px.URL, "k2", "0.160.1"); code != 200 || body != catalogV1 || src != "cache-shared" {
		t.Fatalf("k2 应借用 k1 的缓存，得到 %d %q %q", code, body, src)
	}
	if code, _, src := getCatalog(t, px.URL, "k1", "0.160.1"); code != 200 || src != "cache" {
		t.Fatalf("k1 用自己的缓存，得到 %d %q", code, src)
	}
}

// 200 但 codex 用不了（不是 JSON、超过 1 MiB）且没有缓存：原样转发，不写缓存。
func TestCatalogUnusableWithoutCachePassesThrough(t *testing.T) {
	up := newCatalogUpstream(t)
	px, p := newTestProxy(t, up.URL)
	for _, mode := range []string{"bad-json", "big"} {
		up.mode.Store(mode)
		code, body, src := getCatalog(t, px.URL, "k1", "0.160.1")
		if code != 200 || src != "upstream" || body == catalogV1 {
			t.Fatalf("%s: %d %q", mode, code, src)
		}
	}
	if e, _ := p.cache.load(authHash(http.Header{"Authorization": {"Bearer k1"}}), "/v1/models", "client_version=0.160.1", false); e != nil {
		t.Fatal("codex 用不了的目录不应写进缓存")
	}
	if p.gate.isDown() {
		t.Fatal("目录本身有问题不说明网关不可达")
	}
}

// 不带 client_version 的 /v1/models 是普通模型列表：原样转发，不进缓存逻辑。
func TestCatalogPlainModelListNotCached(t *testing.T) {
	up := newCatalogUpstream(t)
	px, _ := newTestProxy(t, up.URL)
	req, _ := http.NewRequest(http.MethodGet, px.URL+"/v1/models", nil)
	req.Header.Set("Authorization", "Bearer k1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 || resp.Header.Get("X-Local-Proxy-Catalog") != "" {
		t.Fatalf("status %d，来源 %q，应原样转发", resp.StatusCode, resp.Header.Get("X-Local-Proxy-Catalog"))
	}
	up.mode.Store("503")
	if code, _, src := getCatalog(t, px.URL, "k1", "0.160.1"); code != 503 || src != "upstream" {
		t.Fatalf("普通模型列表不应成为 codex 目录的缓存，得到 %d %q", code, src)
	}
}

// 跨版本回退只放宽 client_version，其他查询参数不同的缓存不混用。
func TestCatalogFallbackKeepsOtherParams(t *testing.T) {
	up := newCatalogUpstream(t)
	px, _ := newTestProxy(t, up.URL)
	get := func(query string) (int, string) {
		req, _ := http.NewRequest(http.MethodGet, px.URL+"/v1/models?"+query, nil)
		req.Header.Set("Authorization", "Bearer k1")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode, resp.Header.Get("X-Local-Proxy-Catalog")
	}
	get("client_version=0.160.1&tier=a")
	up.mode.Store("503")
	if code, src := get("client_version=0.161.0&tier=b"); code != 503 || src != "upstream" {
		t.Fatalf("其他参数不同不应回退，得到 %d %q", code, src)
	}
	if code, src := get("tier=a&client_version=0.161.0"); code != 200 || src != "cache" {
		t.Fatalf("只差 client_version 应回退，得到 %d %q", code, src)
	}
}
