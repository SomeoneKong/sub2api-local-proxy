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
	"net/http"
	"regexp"
	"strconv"
	"strings"
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

func (g *upstreamGate) isStopped() bool {
	select {
	case <-g.stopped:
		return true
	default:
		return false
	}
}

// waitUp 在网关可用时立即返回；不可用时等到恢复、deadline、客户端离开或闸门停用。
// 闸门已停用（配置换了网关地址）时一律返回 errGateStopped，不再向原地址发送。
func (g *upstreamGate) waitUp(ctx context.Context, deadline time.Time) error {
	if g.isStopped() {
		return errGateStopped
	}
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
		if g.isStopped() {
			return errGateStopped
		}
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
// 只记一次失败，让后续请求先等网关恢复。网关自己回的 HTTP 错误原样交给客户端，唯一的例外是
// 排队类 429（见 retry429）：退避后重发同一请求。
//
// 重发可能让网关收到同一请求两次（它已收下请求、却在回响应头前断开时），这与 codex 自己遇到
// 连接错误时的重试是同一种后果。等响应头超时（headerTimeout）不重发：网关可能仍在处理这个请求。
type resilientTransport struct {
	base          *http.Transport
	gate          *upstreamGate
	wait          time.Duration
	headerTimeout time.Duration
	r429          *retry429 // nil 表示 429 不重发
}

// retry429 是排队类 429 的重发规则。网关的会话粘性路由把一个会话固定在一个账号上，这个账号并发满时
// 请求在它的队列里等，队满或等太久就回 429，而别的账号可能还有空位：这种 429 说明的是这一个账号
// 一时忙，不是额度用完。codex 对 HTTP 429 一次也不重试、整轮失败，所以由代理在这里等一等再发。
// 重发的是同一请求（头与请求体不变），粘性键不变，仍回到同一个账号。
type retry429 struct {
	codes    map[string]bool
	maxWait  time.Duration
	base     time.Duration
	maxDelay time.Duration
}

// maxErrBody 是为判断 429 能否重发而读入的响应体上限；网关的错误体只有几百字节。
const maxErrBody = 8 << 10

// probeReadTimeout 是读 429 响应体的时限（同时不超过重发预算的截止时间）：网关的错误体随响应头一起写出，
// 这么久还读不完说明连接出了问题，此时不再重发。
var probeReadTimeout = 10 * time.Second

var errRetryBudget = errors.New("429 重发预算或读体时限已到")

// retryNote 由 ServeHTTP 放进请求的 context，RoundTrip 把 429 与重发的情况记在里面，供日志使用。
type retryNote struct {
	lid     string        // 本地请求 ID：同一请求的各条重发行与结束行用它对上（各次发送的上游 X-Request-ID 互不相同）
	code    string        // 最后一次 429 的 error.code
	scope   string        // gateway_concurrency_limit 等的是哪一级的空位（account／user），取自 error.message
	reqID   string        // 最后一次 429 的 X-Request-ID，可到网关日志里对
	retries int           // 重发次数
	waited  time.Duration // 退避等待的总时长，不含各次发送在网关里排队的时间
	failed  time.Duration // 失败发送的总时长：各次收到 429 的发送累计到响应头（包括连接恢复后收到 429 的那次），
	// 重发到预算仍没收到响应头的发送累计到取消；不计等网关恢复、连接失败的发送及其重试间隔
	// end 是 429 之后的结局：ok 重发后拿到非 429 响应；pass 不在重发范围；budget 下一次间隔会超出预算；
	// deadline 重发在预算内没拿到响应头（交回上一次的 429）；error 重发失败或网关地址已换（本地回 502／504）；
	// cancel 退避中客户端离开。
	end string
}

type retryNoteKey struct{}

func noteFrom(ctx context.Context) *retryNote {
	if n, ok := ctx.Value(retryNoteKey{}).(*retryNote); ok {
		return n
	}
	return &retryNote{}
}

// RoundTrip 在 roundTrip（网关不可达时的等待与重发）之外再处理排队类 429。预算从请求进来时起算，
// 第一次发送在网关里排队的时间也算在内：等完下一个间隔会超过 maxWait 就不再重发，把这次 429 原样交回；
// 重发出去的请求到 maxWait 还没拿到响应头就取消，交回上一次的 429。所以一个请求拿到响应头的时间
// 最多是「第一次发送的时长」与 maxWait 中较大的一个；第一次发送仍只受 headerTimeout 限制，
// 没遇到 429 的请求不受影响。拿到响应头之后的响应体（SSE）不受预算限制。
func (t *resilientTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	start := time.Now()
	note := noteFrom(req.Context())
	var last *http.Response // 上一次可重发的 429，响应体已整份读入
	for n := 0; ; n++ {
		out := req
		if n > 0 {
			var err error
			if out, err = resend(req); err != nil {
				return nil, err
			}
		}
		// 每次发送挂一个可取消的 context：预算到点、读 429 体超时都靠取消它中止这次发送。
		// 取消与正在进行的读可以安全并发，roundTrip 与 watchedBody 见到 context 已取消，不会把网关标为不可用。
		ctx, cancel := context.WithCancelCause(out.Context())
		abort := func() { cancel(errRetryBudget) }
		out = out.WithContext(ctx)
		budget := func() bool { return true }
		if n > 0 { // 重发到 maxWait 还没拿到响应头就取消；拿到响应头后解除，之后的响应体不受影响
			budget = time.AfterFunc(time.Until(start.Add(t.r429.maxWait)), abort).Stop
		}
		var took time.Duration
		resp, err := t.roundTrip(out, &took)
		if !budget() {
			note.failed += took
			if err == nil {
				resp.Body.Close()
			}
			if req.Context().Err() != nil {
				return nil, req.Context().Err()
			}
			note.end = "deadline"
			markRetried(last.Header, note)
			return last, nil
		}
		if err == nil && resp.StatusCode == http.StatusTooManyRequests {
			note.failed += took
		}
		if err != nil || resp.StatusCode != http.StatusTooManyRequests || t.r429 == nil {
			if n > 0 {
				note.end = "ok"
				if err != nil {
					note.end = "error" // 本地回的 502／504 由 proxyError 补上重发标记头
				} else {
					markRetried(resp.Header, note)
				}
			}
			return resp, err
		}
		deadline := start.Add(t.r429.maxWait)
		if probeEnd := time.Now().Add(probeReadTimeout); probeEnd.Before(deadline) {
			deadline = probeEnd
		}
		body, ok, late := t.r429.classify(resp, note, deadline, abort)
		if late && last != nil { // 重发的 429 体在时限内没读完：交回上一次完整的 429
			resp.Body.Close()
			note.end = "deadline"
			markRetried(last.Header, note)
			return last, nil
		}
		delay := t.r429.delay(n, resp.Header.Get("Retry-After"))
		if !ok || !replayable(req) || time.Since(start)+delay > t.r429.maxWait {
			note.end = "budget"
			if !late && (!ok || !replayable(req)) {
				note.end = "pass"
			}
			if n > 0 {
				markRetried(resp.Header, note)
			}
			return resp, nil
		}
		resp.Body.Close()
		resp.Body = io.NopCloser(bytes.NewReader(body))
		last = resp
		log.Printf("%s %s 429 %s%s lid=%s req_id=%s，这次发送用了 %.1f 秒，%.1f 秒后第 %d 次重发", req.Method, req.URL.Path,
			note.code, scopeSuffix(note.scope), note.lid, note.reqID, took.Seconds(), delay.Seconds(), n+1)
		if err := t.backoff(req.Context(), delay, note); err != nil {
			return nil, err
		}
		note.retries = n + 1
	}
}

// backoff 等 delay 再重发，等待时长按实际经过累计。客户端离开、或配置换了网关地址（旧闸门停用）时
// 立即返回，不再向旧地址重发。
func (t *resilientTransport) backoff(ctx context.Context, delay time.Duration, note *retryNote) error {
	began := time.Now()
	defer func() { note.waited += time.Since(began) }()
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		note.end = "cancel"
		return ctx.Err()
	case <-t.gate.stopped:
		note.end = "error"
		return errGateStopped
	case <-timer.C:
	}
	select { // 计时与停用同时就绪时不再发
	case <-t.gate.stopped:
		note.end = "error"
		return errGateStopped
	default:
		return nil
	}
}

func scopeSuffix(scope string) string {
	if scope == "" {
		return ""
	}
	return "（" + scope + "）"
}

// markRetried 在发生过 429 重发的响应上加两个头，便于从客户端一侧核对；codex 不读它们。
func markRetried(h http.Header, note *retryNote) {
	h.Set("X-Local-Proxy-Retries", strconv.Itoa(note.retries))
	h.Set("X-Local-Proxy-Wait-Ms", strconv.FormatInt(note.waited.Milliseconds(), 10))
}

func replayable(req *http.Request) bool {
	return req.Body == nil || req.Body == http.NoBody || req.GetBody != nil
}

// resend 复制一份请求用于重发，请求体从 GetBody 重新取。
func resend(req *http.Request) (*http.Request, error) {
	out := req.Clone(req.Context())
	if req.GetBody != nil {
		body, err := req.GetBody()
		if err != nil {
			return nil, err
		}
		out.Body = body
	}
	return out, nil
}

var scopePattern = regexp.MustCompile(`exceeded for (\w+)`)

// classify 在 deadline 之前读出 429 的响应体（不超过 maxErrBody）判断能否重发，把这次的 code 等记进 note，
// 再把读出的字节接回响应体：不重发时客户端拿到的仍是原样的响应。能重发时返回的 body 就是完整的响应体。
// 读出错、超出上限、带压缩、不是 JSON 的不重发。deadline 已过时不读，原样交回；读到 deadline 还没读完时
// 经 abort 中止这次发送（late），客户端拿到读出的部分与随后的读错误——网关的错误体随响应头一起写出，
// 读不完说明连接出了问题。
func (r *retry429) classify(resp *http.Response, note *retryNote, deadline time.Time, abort func()) (body []byte, ok, late bool) {
	note.code, note.scope, note.reqID = "", "", resp.Header.Get("X-Request-ID")
	if enc := resp.Header.Get("Content-Encoding"); enc != "" && enc != "identity" {
		return nil, false, false
	}
	if !time.Now().Before(deadline) {
		return nil, false, true
	}
	guard := time.AfterFunc(time.Until(deadline), abort)
	buf, err := io.ReadAll(io.LimitReader(resp.Body, maxErrBody+1))
	late = !guard.Stop()
	resp.Body = struct {
		io.Reader
		io.Closer
	}{io.MultiReader(bytes.NewReader(buf), resp.Body), resp.Body}
	if late || err != nil || len(buf) > maxErrBody {
		return nil, false, late
	}
	var e struct {
		Error struct {
			Type    string `json:"type"`
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(buf, &e) != nil {
		return nil, false, false
	}
	note.code = e.Error.Code
	if m := scopePattern.FindStringSubmatch(e.Error.Message); m != nil {
		note.scope = m[1]
	}
	return buf, e.Error.Type == "rate_limit_error" && r.codes[e.Error.Code], false
}

// delay 给出第 n 次（从 0 起）重发前的等待：从 base 起翻倍、不超过 maxDelay，再乘以 0.7–1.3 的随机系数，
// 免得同时被拒的请求又同时重发。响应带 Retry-After 时它是下限，在它之上再加 0–20% 的随机余量。
func (r *retry429) delay(n int, retryAfter string) time.Duration {
	d := r.maxDelay
	if n < 16 {
		d = min(r.base<<n, r.maxDelay)
	}
	d = time.Duration(float64(d) * (0.7 + 0.6*rand.Float64()))
	if hint := parseRetryAfter(retryAfter); hint > 0 {
		d = max(d, time.Duration(float64(hint)*(1+0.2*rand.Float64())))
	}
	return d
}

// parseRetryAfter 解析秒数或 HTTP 日期两种写法；没有、写错或已过期都返回 0。
func parseRetryAfter(v string) time.Duration {
	v = strings.TrimSpace(v)
	if s, err := strconv.Atoi(v); err == nil && s > 0 {
		return time.Duration(s) * time.Second
	}
	if at, err := http.ParseTime(v); err == nil {
		return max(time.Until(at), 0)
	}
	return 0
}

// roundTrip 是拿到响应头之前的兜底：网关不可用时等它恢复，发送失败时等网关恢复后重发。
// took 记最后一次发送从发出到响应头的时长；在发送中被取消的也记到取消为止；在等待恢复或连接失败后的间隔里
// 退出的记 0（这段时间不是在网关里排队）。
func (t *resilientTransport) roundTrip(req *http.Request, took *time.Duration) (*http.Response, error) {
	ctx := req.Context()
	deadline := time.Now().Add(t.wait)
	var lastErr error
	for attempt := 1; ; attempt++ {
		*took = 0
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
			var err error
			if out, err = resend(req); err != nil {
				return nil, err
			}
		}
		sent := time.Now()
		resp, err := t.send(out)
		if err == nil {
			*took = time.Since(sent)
			if resp.StatusCode != http.StatusSwitchingProtocols {
				resp.Body = &watchedBody{ReadCloser: resp.Body, ctx: ctx, gate: t.gate}
			}
			return resp, nil
		}
		if ctx.Err() != nil {
			*took = time.Since(sent)
			return nil, err
		}
		lastErr = err
		t.gate.reportFailure(err)
		if errors.Is(err, errHeaderTimeout) || !replayable(req) || attempt >= maxAttempts || t.wait <= 0 {
			return nil, err
		}
		log.Printf("%s %s 第 %d 次发送失败（%v），等网关可用后重发", req.Method, req.URL.Path, attempt, err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-t.gate.stopped:
			return nil, errGateStopped
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
