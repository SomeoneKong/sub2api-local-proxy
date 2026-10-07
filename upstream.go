package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

// 客户端经透明代理的 TUN 模式（如 mihomo / Clash）访问网关时，TCP 握手在本机就完成了：网关不可达
// （隧道断、网关未服务）时拨号照样成功，表现为写出请求后连接被关（EOF / reset）或一直没有响应。所以"网关能不能用"
// 不看拨号，而是由请求在拿到响应头之前的失败触发，再用 /health 探活确认恢复。

const (
	probeInterval = time.Second
	probeTimeout  = 3 * time.Second
	// maxAttempts 是网关探活正常、同一请求仍在拿到响应头前失败时的总尝试次数。
	maxAttempts  = 5
	retryBackoff = time.Second
)

var (
	errUpstreamDown  = errors.New("网关不可用，等待超时")
	errGateStopped   = errors.New("网关地址已在配置里改掉，放弃在原地址上等待")
	errHeaderTimeout = errors.New("等待网关响应头超时")
)

// upstreamGate 记录网关当前是否可用。不可用期间只有一个探活协程在跑，请求在 waitUp 上等它恢复。
type upstreamGate struct {
	healthURL string
	client    *http.Client

	mu      sync.Mutex
	down    bool
	since   time.Time
	upCh    chan struct{} // 可用时是已关闭的通道；转为不可用时换成新通道，恢复时关闭
	stopped chan struct{} // 配置换了网关地址时关闭：探活退出，等待中的请求放弃
}

func newUpstreamGate(healthURL string, transport *http.Transport) *upstreamGate {
	t := transport.Clone()
	t.DisableKeepAlives = true // 每次探活都走新连接，不被池里的旧连接误导
	up := make(chan struct{})
	close(up)
	return &upstreamGate{
		healthURL: healthURL,
		client:    &http.Client{Transport: t, Timeout: probeTimeout, CheckRedirect: noRedirect},
		upCh:      up,
		stopped:   make(chan struct{}),
	}
}

func (g *upstreamGate) stop() { close(g.stopped) }

func (g *upstreamGate) isDown() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.down
}

// reportFailure 把网关标为不可用并开始探活；已在不可用状态时什么也不做。
func (g *upstreamGate) reportFailure(err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.down {
		return
	}
	g.down, g.since, g.upCh = true, time.Now(), make(chan struct{})
	log.Printf("网关不可用（%v），开始探活", err)
	go g.probe()
}

func (g *upstreamGate) probe() {
	for {
		select {
		case <-g.stopped:
			return
		default:
		}
		if err := g.check(); err == nil {
			g.mu.Lock()
			g.down = false
			close(g.upCh)
			log.Printf("网关恢复，不可用 %.1f 秒", time.Since(g.since).Seconds())
			g.mu.Unlock()
			return
		}
		time.Sleep(probeInterval)
	}
}

func (g *upstreamGate) check() error {
	resp, err := g.client.Get(g.healthURL)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health %d", resp.StatusCode)
	}
	return nil
}

// waitUp 在网关可用时立即返回；不可用时等到恢复、deadline、客户端离开或闸门停用。
func (g *upstreamGate) waitUp(ctx context.Context, deadline time.Time) error {
	g.mu.Lock()
	ch := g.upCh
	g.mu.Unlock()
	select {
	case <-ch:
		return nil
	default:
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-g.stopped:
		return errGateStopped
	case <-timer.C:
		return errUpstreamDown
	}
}

// resilientTransport 只在拿到响应头之前兜底：网关不可用时等它恢复，请求在响应头之前失败时
// 等网关恢复后重发（请求体已整份缓存在 GetBody 里）。拿到响应头之后的中断无法对客户端隐藏，
// 只记一次失败，让后续请求先等网关恢复。网关自己回的 HTTP 错误（4xx / 5xx）一律原样交给客户端。
//
// 重发可能让网关收到同一请求两次（它已收下请求、却在回响应头前断开时），这与 codex 自己遇到
// 连接错误时的重试是同一种后果。等响应头超时（headerTimeout）不重发：网关可能仍在处理这个请求。
type resilientTransport struct {
	base          *http.Transport
	gate          *upstreamGate
	wait          time.Duration
	headerTimeout time.Duration
}

func (t *resilientTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	deadline := time.Now().Add(t.wait)
	var lastErr error
	for attempt := 1; ; attempt++ {
		if attempt > 1 && !time.Now().Before(deadline) {
			return nil, fmt.Errorf("%w（最近一次错误: %v）", errUpstreamDown, lastErr)
		}
		if t.wait > 0 {
			if err := t.gate.waitUp(ctx, deadline); err != nil {
				if lastErr != nil && errors.Is(err, errUpstreamDown) {
					err = fmt.Errorf("%w（最近一次错误: %v）", err, lastErr)
				}
				return nil, err
			}
		}
		out := req
		if attempt > 1 {
			out = req.Clone(ctx)
			if req.GetBody != nil {
				body, err := req.GetBody()
				if err != nil {
					return nil, err
				}
				out.Body = body
			}
		}
		resp, err := t.send(out)
		if err == nil {
			if resp.StatusCode != http.StatusSwitchingProtocols {
				resp.Body = &watchedBody{ReadCloser: resp.Body, ctx: ctx, gate: t.gate}
			}
			return resp, nil
		}
		if ctx.Err() != nil {
			return nil, err
		}
		lastErr = err
		t.gate.reportFailure(err)
		replayable := req.Body == nil || req.Body == http.NoBody || req.GetBody != nil
		if errors.Is(err, errHeaderTimeout) || !replayable || attempt >= maxAttempts || t.wait <= 0 {
			return nil, err
		}
		log.Printf("%s %s 第 %d 次发送失败（%v），等网关可用后重发", req.Method, req.URL.Path, attempt, err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(retryBackoff):
		}
	}
}

// send 发一次请求，headerTimeout 内没拿到响应头就放弃。拿到响应头后计时停止，之后的响应体
// （SSE、WebSocket）不受限制；派生的 ctx 随原请求结束而释放。
func (t *resilientTransport) send(req *http.Request) (*http.Response, error) {
	if t.headerTimeout <= 0 {
		return t.base.RoundTrip(req)
	}
	ctx, cancel := context.WithCancelCause(req.Context())
	timer := time.AfterFunc(t.headerTimeout, func() { cancel(errHeaderTimeout) })
	resp, err := t.base.RoundTrip(req.WithContext(ctx))
	if !timer.Stop() { // 计时已到：即使响应头刚好到了，ctx 也已取消，响应体读不出来
		if err == nil {
			resp.Body.Close()
		}
		return nil, errHeaderTimeout
	}
	return resp, err
}

// watchedBody 在响应体读出错（流中断）时把网关标为不可用；客户端主动离开引起的错误不算。
type watchedBody struct {
	io.ReadCloser
	ctx  context.Context
	gate *upstreamGate
}

func (b *watchedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil && err != io.EOF && b.ctx.Err() == nil {
		b.gate.reportFailure(fmt.Errorf("响应中断: %w", err))
	}
	return n, err
}
