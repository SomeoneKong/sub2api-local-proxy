package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

func newTestProxy(t *testing.T, upstream string, mutate ...func(*config)) (*httptest.Server, *proxy) {
	t.Helper()
	cfg := defaultConfig()
	cfg.Upstream = upstream
	cfg.CatalogTimeout = duration(500 * time.Millisecond)
	cfg.CompressPaths = []string{"/v1/responses", "/v1/responses/compact"}
	cfg.UpstreamWait = duration(5 * time.Second)
	cfg.cacheDir = t.TempDir()
	for _, m := range mutate {
		m(&cfg)
	}
	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}
	p, err := newProxy(&cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	s := httptest.NewServer(p)
	t.Cleanup(func() { s.Close(); p.close(); p.gate.stop() })
	return s, p
}

type seen struct {
	header http.Header
	body   []byte // 解压后的请求体
	raw    int    // 网关实际收到的字节数
}

// recordingUpstream 模拟网关：按 Content-Encoding 解开 zstd 后记下请求体。
func recordingUpstream(t *testing.T) (*httptest.Server, chan seen) {
	ch := make(chan seen, 4)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		body := raw
		if r.Header.Get("Content-Encoding") == "zstd" {
			dec, _ := zstd.NewReader(nil)
			var err error
			if body, err = dec.DecodeAll(raw, nil); err != nil {
				t.Errorf("网关解压失败: %v", err)
			}
		}
		if r.ContentLength != int64(len(raw)) {
			t.Errorf("Content-Length %d 与实际 %d 字节不符", r.ContentLength, len(raw))
		}
		ch <- seen{header: r.Header.Clone(), body: body, raw: len(raw)}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(s.Close)
	return s, ch
}

func post(t *testing.T, url string, body []byte, header map[string]string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	resp.Body = io.NopCloser(bytes.NewReader(b))
	return resp
}

func TestCompressResponsesBody(t *testing.T) {
	up, ch := recordingUpstream(t)
	px, _ := newTestProxy(t, up.URL)
	body := []byte(`{"model":"gpt-6.1-sol","input":"` + strings.Repeat("hello codex ", 2000) + `"}`)
	post(t, px.URL+"/v1/responses", body, map[string]string{
		"Authorization": "Bearer k1", "User-Agent": "codex_exec/0.160.1", "Originator": "codex_exec",
		"X-Codex-Turn-State": "ts1",
	})
	got := <-ch
	if got.header.Get("Content-Encoding") != "zstd" {
		t.Fatalf("Content-Encoding = %q，应为 zstd", got.header.Get("Content-Encoding"))
	}
	if !bytes.Equal(got.body, body) {
		t.Fatal("网关解压后的请求体与原文不一致")
	}
	if got.raw >= len(body)/2 {
		t.Fatalf("压缩后 %d 字节，原文 %d，没有压下来", got.raw, len(body))
	}
	for k, v := range map[string]string{"Authorization": "Bearer k1", "User-Agent": "codex_exec/0.160.1",
		"Originator": "codex_exec", "X-Codex-Turn-State": "ts1"} {
		if got.header.Get(k) != v {
			t.Errorf("%s = %q，应原样转发 %q", k, got.header.Get(k), v)
		}
	}
	for _, k := range []string{"X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto", "Forwarded", "X-Real-Ip"} {
		if got.header.Get(k) != "" {
			t.Errorf("不应添加 %s", k)
		}
	}
}

func TestCompressSkipped(t *testing.T) {
	up, ch := recordingUpstream(t)
	px, _ := newTestProxy(t, up.URL)
	big := []byte(strings.Repeat("x", 4096))
	huge := bytes.Repeat([]byte("y"), maxZstdBody+1)
	cases := []struct {
		name, path string
		body       []byte
		header     map[string]string
		wantEnc    string
	}{
		{"短请求体", "/v1/responses", []byte(`{"a":1}`), nil, ""},
		{"不在名单的路径", "/v1/chat/completions", big, nil, ""},
		{"已带编码", "/v1/responses", big, map[string]string{"Content-Encoding": "gzip"}, "gzip"},
		{"超过网关解压上限", "/v1/responses", huge, nil, ""},
	}
	for _, c := range cases {
		post(t, px.URL+c.path, c.body, c.header)
		got := <-ch
		if enc := got.header.Get("Content-Encoding"); enc != c.wantEnc {
			t.Errorf("%s: Content-Encoding = %q，应为 %q", c.name, enc, c.wantEnc)
		}
		if got.raw != len(c.body) || !bytes.Equal(got.body, c.body) {
			t.Errorf("%s: 请求体被改动", c.name)
		}
	}
}

func TestCompactCompressed(t *testing.T) {
	up, ch := recordingUpstream(t)
	px, _ := newTestProxy(t, up.URL)
	body := []byte(strings.Repeat(`{"type":"message"},`, 500))
	post(t, px.URL+"/v1/responses/compact", body, nil)
	if got := <-ch; got.header.Get("Content-Encoding") != "zstd" || !bytes.Equal(got.body, body) {
		t.Fatal("compact 请求应压缩且内容不变")
	}
}

// SSE 必须逐块转发：网关发出第一个事件后停住，客户端要能先读到它。
func TestSSEStreamsIncrementally(t *testing.T) {
	release := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: a\ndata: 1\n\n")
		w.(http.Flusher).Flush()
		<-release
		fmt.Fprint(w, "event: b\ndata: 2\n\n")
	}))
	defer up.Close()
	defer close(release)
	px, _ := newTestProxy(t, up.URL)
	resp, err := http.Post(px.URL+"/v1/responses", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	line := make(chan string, 1)
	go func() {
		l, _ := bufio.NewReader(resp.Body).ReadString('\n')
		line <- l
	}()
	select {
	case l := <-line:
		if l != "event: a\n" {
			t.Fatalf("首行 %q", l)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("第一个事件被缓冲，没有及时转发")
	}
}

// flakyUpstream 模拟经透明代理 TUN 访问的网关：down 时连接建立后立刻被关、没有任何响应
// （远端不可达或网关未服务时 TUN 的表现），/health 也一样；up 时正常服务。
type flakyUpstream struct {
	*httptest.Server
	down     atomic.Bool
	requests atomic.Int32 // 网关真正处理的业务请求数
	dropped  atomic.Int32 // down 期间被关掉的业务请求数（不含探活）
	dropBiz  atomic.Bool  // 只关业务请求、探活照常 200（网关活着但这条连接断了）
	handler  http.HandlerFunc
}

func newFlakyUpstream(t *testing.T, h http.HandlerFunc) *flakyUpstream {
	u := &flakyUpstream{handler: h}
	u.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if u.down.Load() || (u.dropBiz.Load() && r.URL.Path != "/health") {
			if r.URL.Path != "/health" {
				u.dropped.Add(1)
			}
			conn, _, err := http.NewResponseController(w).Hijack()
			if err == nil {
				conn.Close()
			}
			return
		}
		if r.URL.Path == "/health" {
			io.WriteString(w, "ok")
			return
		}
		u.requests.Add(1)
		u.handler(w, r)
	}))
	t.Cleanup(u.Close)
	return u
}

func echoBody(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	if r.Header.Get("Content-Encoding") == "zstd" {
		dec, _ := zstd.NewReader(nil)
		b, _ = dec.DecodeAll(b, nil)
	}
	w.Write(b)
}

// 网关不可达期间到达的请求：等网关恢复后重发，请求体与原文一致。
func TestWaitsForUpstreamRecovery(t *testing.T) {
	up := newFlakyUpstream(t, echoBody)
	up.down.Store(true)
	px, p := newTestProxy(t, up.URL)
	go func() {
		time.Sleep(1500 * time.Millisecond)
		up.down.Store(false)
	}()
	body := []byte(strings.Repeat("turn ", 1000))
	start := time.Now()
	resp := post(t, px.URL+"/v1/responses", body, nil)
	got, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || !bytes.Equal(got, body) {
		t.Fatalf("status %d，回显 %d 字节（原文 %d）", resp.StatusCode, len(got), len(body))
	}
	if el := time.Since(start); el < time.Second {
		t.Fatalf("用了 %v，应等到网关恢复", el)
	}
	if n := up.requests.Load(); n != 1 {
		t.Fatalf("网关处理了 %d 次，应只有恢复后的 1 次", n)
	}
	if p.gate.isDown() {
		t.Fatal("恢复后应标为可用")
	}
}

// 已知网关不可用时，新请求先在闸门上等，不去撞网关。
func TestNewRequestsWaitWhileDown(t *testing.T) {
	up := newFlakyUpstream(t, echoBody)
	px, p := newTestProxy(t, up.URL)
	up.down.Store(true)
	p.gate.reportFailure(fmt.Errorf("测试"))
	go func() {
		time.Sleep(1200 * time.Millisecond)
		up.down.Store(false)
	}()
	resp := post(t, px.URL+"/v1/responses", []byte("{}"), nil)
	if resp.StatusCode != 200 || up.requests.Load() != 1 {
		t.Fatalf("status %d，网关处理 %d 次", resp.StatusCode, up.requests.Load())
	}
	if n := up.dropped.Load(); n != 0 {
		t.Fatalf("不可用期间有 %d 个请求被发出去，应在闸门上等", n)
	}
}

// 等待超过 upstream_wait 仍不可用：回 502 与 OpenAI 格式的错误，交给 codex 自己重试。
func TestWaitTimeoutReturns502(t *testing.T) {
	up := newFlakyUpstream(t, echoBody)
	up.down.Store(true)
	px, _ := newTestProxy(t, up.URL, func(c *config) { c.UpstreamWait = duration(1500 * time.Millisecond) })
	start := time.Now()
	resp := post(t, px.URL+"/v1/responses", []byte("{}"), nil)
	el := time.Since(start)
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status %d，应为 502", resp.StatusCode)
	}
	var e struct {
		Error struct{ Message, Type string }
	}
	if err := json.NewDecoder(resp.Body).Decode(&e); err != nil || e.Error.Type != "local_proxy_error" {
		t.Fatalf("错误体不对: %+v %v", e, err)
	}
	if el < 1200*time.Millisecond || el > 4*time.Second {
		t.Fatalf("用了 %v，应在等待时长前后返回", el)
	}
}

// 网关自己回的 HTTP 错误原样交给客户端，不重发、不改成别的状态。
func TestUpstreamHTTPErrorPassthrough(t *testing.T) {
	up := newFlakyUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":{"message":"No available accounts"}}`, http.StatusServiceUnavailable)
	})
	px, p := newTestProxy(t, up.URL)
	resp := post(t, px.URL+"/v1/responses", []byte("{}"), nil)
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 503 || !strings.Contains(string(b), "No available accounts") {
		t.Fatalf("status %d body %q", resp.StatusCode, b)
	}
	if up.requests.Load() != 1 || p.gate.isDown() {
		t.Fatalf("网关处理 %d 次、闸门 down=%v；HTTP 错误不应重发，也不说明网关不可达", up.requests.Load(), p.gate.isDown())
	}
}

// 响应开始后网关中断：客户端必须看到流中断（而不是正常结束），网关被标为不可用。
func TestMidStreamBreakPropagates(t *testing.T) {
	var up *flakyUpstream
	up = newFlakyUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: a\ndata: 1\n\n")
		w.(http.Flusher).Flush()
		up.down.Store(true) // 隧道断了：流被掐断，之后探活也不通
		conn, _, err := http.NewResponseController(w).Hijack()
		if err == nil {
			conn.Close()
		}
	})
	px, p := newTestProxy(t, up.URL)
	resp, err := http.Post(px.URL+"/v1/responses", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err == nil {
		t.Fatalf("客户端读到正常结束（%q），应是流中断", b)
	}
	if !strings.HasPrefix(string(b), "event: a") {
		t.Fatalf("中断前的事件应已转发，得到 %q", b)
	}
	if up.requests.Load() != 1 {
		t.Fatalf("响应开始后不能重发，网关处理了 %d 次", up.requests.Load())
	}
	if !p.gate.isDown() {
		t.Fatal("流中断应把网关标为不可用，让后续请求先等它恢复")
	}
}

// 网关拒绝连接（端口没人监听，如非 TUN 环境下网关重启）时同样等它起来。
func TestWaitsForRefusedUpstream(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	px, _ := newTestProxy(t, "http://"+addr)
	go func() {
		time.Sleep(1200 * time.Millisecond)
		l, err := net.Listen("tcp", addr)
		if err != nil {
			t.Errorf("重新监听 %s: %v", addr, err)
			return
		}
		up := &httptest.Server{Listener: l, Config: &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, "ok")
		})}}
		up.Start()
		t.Cleanup(up.Close)
	}()
	resp := post(t, px.URL+"/v1/responses", []byte("{}"), nil)
	if b, _ := io.ReadAll(resp.Body); resp.StatusCode != 200 || string(b) != "ok" {
		t.Fatalf("status %d body %q", resp.StatusCode, b)
	}
}

// wait_for_upstream 关闭：不等待、不重发，失败直接 502。
func TestNoWaitWhenDisabled(t *testing.T) {
	up := newFlakyUpstream(t, echoBody)
	up.down.Store(true)
	px, _ := newTestProxy(t, up.URL, func(c *config) { c.WaitForUpstream = false })
	start := time.Now()
	resp := post(t, px.URL+"/v1/responses", []byte("{}"), nil)
	if resp.StatusCode != http.StatusBadGateway || time.Since(start) > time.Second {
		t.Fatalf("status %d，用了 %v", resp.StatusCode, time.Since(start))
	}
}

// WebSocket 升级要能透传：网关回 101 后双向转发原始字节。
func TestUpgradePassthrough(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Upgrade") != "websocket" {
			http.Error(w, "need upgrade", 400)
			return
		}
		conn, brw, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		brw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
		brw.Flush()
		buf := make([]byte, 5)
		io.ReadFull(brw, buf)
		conn.Write(append([]byte("echo:"), buf...))
	}))
	defer up.Close()
	px, _ := newTestProxy(t, up.URL)
	conn, err := net.Dial("tcp", strings.TrimPrefix(px.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(3 * time.Second))
	fmt.Fprint(conn, "GET /v1/responses HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n")
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status %d，应为 101", resp.StatusCode)
	}
	conn.Write([]byte("hello"))
	got := make([]byte, 10)
	if _, err := io.ReadFull(br, got); err != nil || string(got) != "echo:hello" {
		t.Fatalf("回显 %q err %v", got, err)
	}
}

// 发出请求后迟迟没有响应头（TUN 下远端不可达时会一直挂住）：到 response_header_timeout 回 504，不重发。
func TestResponseHeaderTimeout(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	up := newFlakyUpstream(t, func(w http.ResponseWriter, r *http.Request) { <-release })
	px, _ := newTestProxy(t, up.URL, func(c *config) { c.ResponseHeaderTimeout = duration(300 * time.Millisecond) })
	start := time.Now()
	resp := post(t, px.URL+"/v1/responses", []byte("{}"), nil)
	if resp.StatusCode != http.StatusGatewayTimeout || time.Since(start) > 2*time.Second {
		t.Fatalf("status %d，用了 %v，应在超时后回 504", resp.StatusCode, time.Since(start))
	}
	time.Sleep(1500 * time.Millisecond) // 超过重试间隔，确认没有重发
	if n := up.requests.Load(); n != 1 {
		t.Fatalf("网关收到 %d 次，等响应头超时不应重发", n)
	}
}

// 响应头在时限内到达后，之后的 SSE 不受响应头超时限制。
func TestHeaderTimeoutStopsAfterHeaders(t *testing.T) {
	up := newFlakyUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		time.Sleep(700 * time.Millisecond)
		fmt.Fprint(w, "event: done\ndata: 1\n\n")
	})
	px, _ := newTestProxy(t, up.URL, func(c *config) { c.ResponseHeaderTimeout = duration(300 * time.Millisecond) })
	resp := post(t, px.URL+"/v1/responses", []byte("{}"), nil)
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || !strings.Contains(string(b), "event: done") {
		t.Fatalf("status %d body %q，响应头之后的数据不应被超时掐断", resp.StatusCode, b)
	}
}

// 超过 upstream_wait 后不再重发，即使此时探活显示网关可用。
func TestNoResendAfterDeadline(t *testing.T) {
	up := newFlakyUpstream(t, echoBody)
	up.dropBiz.Store(true)
	px, _ := newTestProxy(t, up.URL, func(c *config) { c.UpstreamWait = duration(100 * time.Millisecond) })
	resp := post(t, px.URL+"/v1/responses", []byte("{}"), nil)
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status %d，应为 502", resp.StatusCode)
	}
	if n := up.dropped.Load(); n != 1 {
		t.Fatalf("发了 %d 次，过了等待期限不应再发", n)
	}
}
