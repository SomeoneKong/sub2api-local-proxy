package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"time"

	"github.com/klauspost/compress/zstd"
)

const (
	// maxBody 与网关的请求体上限（max_body_size 默认 256 MiB）一致；请求体要整份缓存才能重发。
	maxBody = 256 << 20
	// maxZstdBody：网关解压 zstd 后只读前 64 MiB，超出部分被静默截断，更长的请求体发明文。
	maxZstdBody = 64 << 20
)

type proxy struct {
	cfg      *config
	base     *http.Transport
	rp       *httputil.ReverseProxy
	catalog  *http.Client // 目录回源不经等待与重发，失败就用缓存
	gate     *upstreamGate
	enc      *zstd.Encoder
	compress map[string]bool
	cache    *catalogCache
	fetches  *catalogFetches
}

// newProxy 按一份已校验的配置组装转发链路。配置热加载时整体换新的 proxy，旧的在途请求走完后由 close 收尾。
// gate 是同一探活地址上沿用的闸门（网关地址没变时，等待中的请求不受重新加载影响）；传 nil 则新建。
func newProxy(cfg *config, gate *upstreamGate) (*proxy, error) {
	if err := os.MkdirAll(cfg.cacheDir, 0o700); err != nil {
		return nil, err
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(cfg.ZstdLevel)))
	if err != nil {
		return nil, err
	}
	base := &http.Transport{
		Proxy:               nil, // 直连网关，不受 HTTP_PROXY 等环境变量影响
		DialContext:         (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        64,
		MaxIdleConnsPerHost: 32,
		IdleConnTimeout:     90 * time.Second, // 短于网关的 120 秒，避免复用网关已关掉的连接
		TLSHandshakeTimeout: 10 * time.Second,
		// 不替客户端加 Accept-Encoding、不自动解压，响应字节原样交回客户端。
		DisableCompression: true,
	}
	if gate == nil {
		gate = newUpstreamGate(cfg.healthURL(), base)
	}
	p := &proxy{
		cfg:      cfg,
		base:     base,
		catalog:  &http.Client{Transport: base, CheckRedirect: noRedirect},
		gate:     gate,
		enc:      enc,
		compress: map[string]bool{},
		cache:    &catalogCache{dir: cfg.cacheDir},
		fetches:  &catalogFetches{m: map[string]*catalogFetch{}},
	}
	for _, path := range cfg.CompressPaths {
		p.compress[path] = true
	}
	wait := time.Duration(0)
	if cfg.WaitForUpstream {
		wait = cfg.UpstreamWait.D()
	}
	var r429 *retry429
	if cfg.Retry429 {
		r429 = &retry429{codes: map[string]bool{}, maxWait: cfg.Retry429MaxWait.D(),
			base: cfg.Retry429BaseDelay.D(), maxDelay: cfg.Retry429MaxDelay.D()}
		for _, c := range cfg.Retry429Codes {
			r429.codes[c] = true
		}
	}
	p.rp = &httputil.ReverseProxy{
		// Rewrite 模式不加 X-Forwarded-*，入站的同名头也会被去掉；Host 换成网关的。
		// 网关默认信任这些头来认客户端 IP，加上会影响按 IP 限制的 key。
		Rewrite: func(pr *httputil.ProxyRequest) { pr.SetURL(cfg.upstreamURL) },
		Transport: &resilientTransport{base: base, gate: gate, wait: wait,
			headerTimeout: cfg.ResponseHeaderTimeout.D(), r429: r429},
		FlushInterval: -1, // SSE 逐块转发
		ErrorHandler:  proxyError,
	}
	return p, nil
}

// close 在 proxy 被新配置换下后调用：关掉空闲连接，在途请求不受影响。闸门由 app 决定是否沿用。
func (p *proxy) close() { p.base.CloseIdleConnections() }

func noRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

func (p *proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if p.cfg.CatalogCache && r.Method == http.MethodGet && r.URL.Path == p.cfg.CatalogPath &&
		r.URL.Query().Get("client_version") != "" {
		p.serveCatalog(w, r)
		return
	}
	start := time.Now()
	raw, sent, err := p.bufferBody(r)
	if err != nil {
		code := http.StatusBadRequest
		if errors.Is(err, errBodyTooLarge) {
			code = http.StatusRequestEntityTooLarge
		}
		log.Printf("%s %s 读请求体失败: %v", r.Method, r.URL.Path, err)
		writeError(w, code, err.Error())
		return
	}
	note := ""
	if r.Header.Get("Content-Encoding") == "zstd" && sent != raw {
		note = fmt.Sprintf(" zstd %d→%d", raw, sent)
	}
	rn := &retryNote{lid: fmt.Sprintf("%012x", rand.Uint64()>>16)}
	r = r.WithContext(context.WithValue(r.Context(), retryNoteKey{}, rn))
	rec := &statusRecorder{ResponseWriter: w}
	defer func() {
		// 响应开始后转发不下去时，ReverseProxy 以 ErrAbortHandler 掐断客户端连接：上游中断时客户端看到的是
		// 流中断而不是正常结束，由它自己重试。codex 收到 response.completed 后会自己先关连接，也走到这里。
		// 这里只补一行日志再继续向上抛。
		if v := recover(); v != nil {
			why := "上游响应中途中断"
			if r.Context().Err() != nil {
				why = "客户端先断开"
			}
			log.Printf("%s %s %d %.1fs%s%s %s", r.Method, r.URL.Path, rec.status, time.Since(start).Seconds(), note, rn.fields(rec.status, r), why)
			panic(v)
		}
	}()
	p.rp.ServeHTTP(rec, r)
	log.Printf("%s %s %d %.1fs%s%s", r.Method, r.URL.Path, rec.status, time.Since(start).Seconds(), note, rn.fields(rec.status, r))
}

// fields 是日志行末尾的 429 字段：最终回了 429 或中途重发过时才有。记本地请求 ID（与重发行对上）、
// 最后一次 429 的 code 与等待对象、重发次数、退避等待与失败发送的总时长、结局（见 retryNote.end）、
// X-Request-ID，以及会话头 session-id 的前 8 位（同一会话的几次 429 可以对上）。请求总耗时在行首的秒数里，
// 含成功那次发送的排队与之后的整段响应体，所以不能拿它减去 wait、fail 当作等待。
func (n *retryNote) fields(status int, r *http.Request) string {
	if status != http.StatusTooManyRequests && n.retries == 0 && n.end == "" {
		return ""
	}
	sess := r.Header.Get("session-id")
	if len(sess) > 8 {
		sess = sess[:8]
	}
	return fmt.Sprintf(" lid=%s code=%s scope=%s retries=%d wait=%.1fs fail=%.1fs end=%s req_id=%s sess=%s",
		n.lid, n.code, n.scope, n.retries, n.waited.Seconds(), n.failed.Seconds(), n.end, n.reqID, sess)
}

var errBodyTooLarge = errors.New("请求体超过 256 MiB")

// bufferBody 把请求体整份读进内存并设好 GetBody，使请求在拿到响应头之前失败时可以重发；
// 发往压缩名单路径、长度在 [compressMin, 64 MiB] 内的请求体改用 zstd 压缩后的版本。
// 返回原长与实际发送的长度。
func (p *proxy) bufferBody(r *http.Request) (int, int, error) {
	if r.Body == nil || r.Body == http.NoBody {
		return 0, 0, nil
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxBody+1))
	r.Body.Close()
	if err != nil {
		return 0, 0, err
	}
	if len(raw) > maxBody {
		return 0, 0, errBodyTooLarge
	}
	body := raw
	if p.cfg.Compress && r.Method == http.MethodPost && p.compress[r.URL.Path] && r.Header.Get("Content-Encoding") == "" &&
		len(raw) >= p.cfg.CompressMinBytes && len(raw) <= maxZstdBody {
		body = p.enc.EncodeAll(raw, make([]byte, 0, len(raw)/2))
		r.Header.Set("Content-Encoding", "zstd")
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
	r.ContentLength = int64(len(body))
	r.TransferEncoding = nil
	r.Header.Del("Content-Length")
	return len(raw), len(body), nil
}

// proxyError 处理拿到响应头之前就失败的请求（等待网关恢复超时、重发用尽、请求体不能重发、等响应头超时）。
// 回 502（等响应头超时回 504），codex 会按自己的重试策略再来；客户端已离开时不写响应。
func proxyError(w http.ResponseWriter, r *http.Request, err error) {
	if r.Context().Err() != nil {
		return
	}
	log.Printf("%s %s 转发失败: %v", r.Method, r.URL.Path, err)
	if n := noteFrom(r.Context()); n.retries > 0 {
		markRetried(w.Header(), n)
	}
	code := http.StatusBadGateway
	if errors.Is(err, errHeaderTimeout) {
		code = http.StatusGatewayTimeout
	}
	writeError(w, code, "local proxy: "+err.Error())
}

// writeError 用 OpenAI 的错误格式回本地产生的错误，codex 能把消息显示出来。
func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"message": msg, "type": "local_proxy_error"}})
}

// statusRecorder 记下响应状态码；Unwrap 让 ReverseProxy 经 ResponseController 拿到 Flush 与 Hijack。
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	if s.status == 0 {
		s.status = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	return s.ResponseWriter.Write(b)
}

func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }
