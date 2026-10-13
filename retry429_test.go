package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// gatewayBusy 模拟网关的排队类 429 响应体。
func gatewayBusy(w http.ResponseWriter, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusTooManyRequests)
	fmt.Fprintf(w, `{"error":{"type":"rate_limit_error","code":%q,"message":"Concurrency limit exceeded for account, please retry later"}}`, code)
}

func fastRetry(c *config) {
	c.Retry429BaseDelay = duration(50 * time.Millisecond)
	c.Retry429MaxDelay = duration(100 * time.Millisecond)
}

// 排队类 429 由代理退避后重发同一请求（头与请求体不变），客户端只看到最终的 200。
func TestRetry429ThenSuccess(t *testing.T) {
	var mu sync.Mutex
	var bodies, sessions []string
	up := newFlakyUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies, sessions = append(bodies, string(b)), append(sessions, r.Header.Get("session-id"))
		n := len(bodies)
		mu.Unlock()
		if n <= 2 {
			gatewayBusy(w, "gateway_queue_full")
			return
		}
		w.Write(b)
	})
	px, _ := newTestProxy(t, up.URL, fastRetry, func(c *config) { c.Compress = false })
	resp := post(t, px.URL+"/v1/responses", []byte(`{"input":"x"}`), map[string]string{"session-id": "sess-0123456789"})
	got, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || string(got) != `{"input":"x"}` {
		t.Fatalf("status %d body %q", resp.StatusCode, got)
	}
	if resp.Header.Get("X-Local-Proxy-Retries") != "2" || resp.Header.Get("X-Local-Proxy-Wait-Ms") == "" {
		t.Fatalf("重发标记头不对: %v", resp.Header)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 3 {
		t.Fatalf("网关收到 %d 次，应为 3 次", len(bodies))
	}
	for i := range bodies {
		if bodies[i] != bodies[0] || sessions[i] != "sess-0123456789" {
			t.Fatalf("第 %d 次请求体 %q、session-id %q，重发应与原请求一致", i+1, bodies[i], sessions[i])
		}
	}
}

// 一直 429：从请求发出起算，下一次重发会超过 retry_429_max_wait 就停，把最后一次 429 原样交回。
func TestRetry429GivesUpAtMaxWait(t *testing.T) {
	up := newFlakyUpstream(t, func(w http.ResponseWriter, r *http.Request) { gatewayBusy(w, "gateway_concurrency_limit") })
	px, _ := newTestProxy(t, up.URL, fastRetry, func(c *config) { c.Retry429MaxWait = duration(600 * time.Millisecond) })
	start := time.Now()
	resp := post(t, px.URL+"/v1/responses", []byte("{}"), nil)
	el := time.Since(start)
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 429 || !strings.Contains(string(b), `"code":"gateway_concurrency_limit"`) {
		t.Fatalf("status %d body %q，应原样交回最后一次 429", resp.StatusCode, b)
	}
	if el > 1500*time.Millisecond {
		t.Fatalf("用了 %v，不应明显超过 retry_429_max_wait", el)
	}
	if n := up.requests.Load(); n < 3 || resp.Header.Get("X-Local-Proxy-Retries") != strconv.Itoa(int(n)-1) {
		t.Fatalf("网关收到 %d 次，重发标记 %q", n, resp.Header.Get("X-Local-Proxy-Retries"))
	}
}

// 额度、RPM 一类的 429，以及不是 JSON 的 429，都原样交回、不重发。
func TestRetry429OtherCodesPassThrough(t *testing.T) {
	for name, body := range map[string]string{
		"额度用完":   `{"error":{"type":"usage_limit_reached","code":"usage_limit_reached","message":"quota"}}`,
		"key 限速": `{"error":{"type":"rate_limit_error","code":"rate_limit_exceeded","message":"rpm"}}`,
		"非 JSON": `Too Many Requests`,
	} {
		up := newFlakyUpstream(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusTooManyRequests)
			io.WriteString(w, body)
		})
		px, _ := newTestProxy(t, up.URL, fastRetry)
		resp := post(t, px.URL+"/v1/responses", []byte("{}"), nil)
		b, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 429 || string(b) != body || up.requests.Load() != 1 {
			t.Errorf("%s: status %d body %q，网关收到 %d 次", name, resp.StatusCode, b, up.requests.Load())
		}
	}
}

// 无 code 的 rate_limit_error（上游账号都在限速）也按排队类重发；Retry-After 优先于退避间隔，
// Retry-After 超出剩余预算时直接交回。
func TestRetry429RetryAfter(t *testing.T) {
	var n atomic.Int32
	up := newFlakyUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			io.WriteString(w, `{"error":{"type":"rate_limit_error","message":"All available accounts are currently rate-limited. Please retry later."}}`)
			return
		}
		io.WriteString(w, "ok")
	})
	px, _ := newTestProxy(t, up.URL, fastRetry)
	start := time.Now()
	resp := post(t, px.URL+"/v1/responses", []byte("{}"), nil)
	if resp.StatusCode != 200 || time.Since(start) < time.Second {
		t.Fatalf("status %d，用了 %v，应按 Retry-After 等 1 秒后重发成功", resp.StatusCode, time.Since(start))
	}

	far := newFlakyUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "30")
		gatewayBusy(w, "gateway_queue_full")
	})
	px2, _ := newTestProxy(t, far.URL, fastRetry, func(c *config) { c.Retry429MaxWait = duration(2 * time.Second) })
	start = time.Now()
	resp = post(t, px2.URL+"/v1/responses", []byte("{}"), nil)
	if resp.StatusCode != 429 || far.requests.Load() != 1 || time.Since(start) > time.Second {
		t.Fatalf("status %d，网关收到 %d 次，用了 %v；Retry-After 超出预算应立即交回", resp.StatusCode, far.requests.Load(), time.Since(start))
	}
}

// 客户端离开后不再重发。
func TestRetry429StopsWhenClientLeaves(t *testing.T) {
	up := newFlakyUpstream(t, func(w http.ResponseWriter, r *http.Request) { gatewayBusy(w, "gateway_queue_full") })
	px, _ := newTestProxy(t, up.URL, func(c *config) {
		c.Retry429BaseDelay = duration(200 * time.Millisecond)
		c.Retry429MaxDelay = duration(200 * time.Millisecond)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, px.URL+"/v1/responses", strings.NewReader("{}"))
	if resp, err := http.DefaultClient.Do(req); err == nil {
		resp.Body.Close()
		t.Fatalf("客户端超时前收到了 %d", resp.StatusCode)
	}
	time.Sleep(100 * time.Millisecond)
	sent := up.requests.Load()
	time.Sleep(800 * time.Millisecond)
	if n := up.requests.Load(); n != sent || n > 4 {
		t.Fatalf("客户端离开时网关收到 %d 次，之后变成 %d 次，应停止重发", sent, n)
	}
}

// retry_429 关掉时 429 原样交回。
func TestRetry429Disabled(t *testing.T) {
	up := newFlakyUpstream(t, func(w http.ResponseWriter, r *http.Request) { gatewayBusy(w, "gateway_queue_full") })
	px, _ := newTestProxy(t, up.URL, func(c *config) { c.Retry429 = false })
	resp := post(t, px.URL+"/v1/responses", []byte("{}"), nil)
	if resp.StatusCode != 429 || up.requests.Load() != 1 || resp.Header.Get("X-Local-Proxy-Retries") != "" {
		t.Fatalf("status %d，网关收到 %d 次", resp.StatusCode, up.requests.Load())
	}
}

// 退避间隔：从 base 起翻倍、不超过 maxDelay，抖动在 ±30% 内；Retry-After（秒数或 HTTP 日期）是下限，
// 在它之上只加不减；写错、为 0 或已过期的 Retry-After 不起作用。
func TestRetry429Delay(t *testing.T) {
	r := &retry429{base: 2 * time.Second, maxDelay: 30 * time.Second}
	for n, want := range []time.Duration{2, 4, 8, 16, 30, 30} {
		want *= time.Second
		for range 20 {
			if d := r.delay(n, ""); d < want*7/10 || d > want*13/10 {
				t.Fatalf("第 %d 次间隔 %v，应在 %v ±30%% 内", n, d, want)
			}
		}
	}
	if d := r.delay(100, ""); d > 39*time.Second {
		t.Fatalf("次数很大时间隔 %v，应封顶在 maxDelay", d)
	}
	for range 20 {
		if d := r.delay(0, "7"); d < 7*time.Second || d > 8400*time.Millisecond {
			t.Fatalf("Retry-After 秒数：%v，应在 7 秒到 8.4 秒之间", d)
		}
	}
	at := time.Now().Add(5 * time.Second).UTC().Format(http.TimeFormat)
	if d := r.delay(0, at); d < 3*time.Second || d > 7*time.Second {
		t.Fatalf("Retry-After 日期：%v", d)
	}
	past := time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat)
	for _, v := range []string{"0", "-3", "abc", past} {
		if d := r.delay(0, v); d < 1400*time.Millisecond || d > 2600*time.Millisecond {
			t.Fatalf("Retry-After %q 应不起作用，间隔 %v", v, d)
		}
	}
}

// 重发出去的请求到预算还没拿到响应头就取消，交回上一次的 429（原样的体），网关不被标为不可用。
func TestRetry429ResendCutAtBudget(t *testing.T) {
	var n atomic.Int32
	release := make(chan struct{})
	defer close(release)
	up := newFlakyUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) == 1 {
			gatewayBusy(w, "gateway_concurrency_limit")
			return
		}
		select { // 第二次发送在网关里一直排着
		case <-release:
		case <-r.Context().Done():
		}
	})
	px, p := newTestProxy(t, up.URL, fastRetry, func(c *config) { c.Retry429MaxWait = duration(800 * time.Millisecond) })
	start := time.Now()
	resp := post(t, px.URL+"/v1/responses", []byte("{}"), nil)
	el := time.Since(start)
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 429 || !strings.Contains(string(b), "Concurrency limit exceeded for account") {
		t.Fatalf("status %d body %q，应交回上一次的 429", resp.StatusCode, b)
	}
	if el < 700*time.Millisecond || el > 2*time.Second {
		t.Fatalf("用了 %v，应在预算到时交回", el)
	}
	if resp.Header.Get("X-Local-Proxy-Retries") != "1" || p.gate.isDown() {
		t.Fatalf("重发标记 %q、闸门 down=%v；预算到时取消不说明网关不可达", resp.Header.Get("X-Local-Proxy-Retries"), p.gate.isDown())
	}
}

// 重发拿到响应头后，响应体（SSE）持续多久都不受预算限制。
func TestRetry429BudgetNotAppliedToBody(t *testing.T) {
	var n atomic.Int32
	up := newFlakyUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) == 1 {
			gatewayBusy(w, "gateway_queue_full")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		time.Sleep(time.Second)
		io.WriteString(w, "event: done\ndata: 1\n\n")
	})
	px, _ := newTestProxy(t, up.URL, fastRetry, func(c *config) { c.Retry429MaxWait = duration(500 * time.Millisecond) })
	resp := post(t, px.URL+"/v1/responses", []byte("{}"), nil)
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || !strings.Contains(string(b), "event: done") {
		t.Fatalf("status %d body %q，响应头之后的数据不应被预算掐断", resp.StatusCode, b)
	}
}

// 429 重发后连接失败：本地回的 502 也带重发标记头；第一次就失败的请求不带。
func TestRetry429ThenTransportErrorMarked(t *testing.T) {
	var up *flakyUpstream
	up = newFlakyUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		up.dropBiz.Store(true) // 之后的业务请求连接一建立就被关
		gatewayBusy(w, "gateway_queue_full")
	})
	px, _ := newTestProxy(t, up.URL, fastRetry, func(c *config) { c.WaitForUpstream = false })
	resp := post(t, px.URL+"/v1/responses", []byte("{}"), nil)
	if resp.StatusCode != http.StatusBadGateway || resp.Header.Get("X-Local-Proxy-Retries") != "1" {
		t.Fatalf("status %d，重发标记 %q", resp.StatusCode, resp.Header.Get("X-Local-Proxy-Retries"))
	}
	resp = post(t, px.URL+"/v1/responses", []byte("{}"), nil)
	if resp.StatusCode != http.StatusBadGateway || resp.Header.Get("X-Local-Proxy-Retries") != "" {
		t.Fatalf("status %d，第一次就失败不应带重发标记 %q", resp.StatusCode, resp.Header.Get("X-Local-Proxy-Retries"))
	}
}

// 直接调 RoundTrip，带上记录 429 情况的 note。
func roundTripWithNote(t *testing.T, p *proxy, ctx context.Context, url string) (*http.Response, error, *retryNote) {
	t.Helper()
	note := &retryNote{}
	req, _ := http.NewRequestWithContext(context.WithValue(ctx, retryNoteKey{}, note), http.MethodPost, url, nil)
	resp, err := p.rp.Transport.RoundTrip(req)
	return resp, err, note
}

// 退避中客户端离开：重发次数不计这次没发出去的，等待时长按实际经过算。
func TestRetry429CancelDuringBackoffAccounting(t *testing.T) {
	up := newFlakyUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "1")
		gatewayBusy(w, "gateway_queue_full")
	})
	_, p := newTestProxy(t, up.URL, fastRetry)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err, note := roundTripWithNote(t, p, ctx, up.URL+"/v1/responses")
	if err == nil || note.retries != 0 || note.waited > 600*time.Millisecond || note.end != "cancel" {
		t.Fatalf("err %v，retries %d，waited %v，end %q", err, note.retries, note.waited, note.end)
	}
	if n := up.requests.Load(); n != 1 {
		t.Fatalf("网关收到 %d 次，应只有第一次", n)
	}
}

// 配置换了网关地址（旧闸门停用）时，退避中的请求立即结束，不再向旧地址重发。
func TestRetry429GateStoppedDuringBackoff(t *testing.T) {
	up := newFlakyUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "3")
		gatewayBusy(w, "gateway_queue_full")
	})
	_, p := newTestProxy(t, up.URL, fastRetry, func(c *config) { c.Retry429MaxWait = duration(10 * time.Second) })
	gate := newUpstreamGate(p.cfg.healthURL(), p.base) // 换成本测试自己停用的闸门，清理时停的是原闸门
	p.rp.Transport.(*resilientTransport).gate = gate
	go func() {
		time.Sleep(300 * time.Millisecond)
		gate.stop()
	}()
	start := time.Now()
	_, err, _ := roundTripWithNote(t, p, context.Background(), up.URL+"/v1/responses")
	if !errors.Is(err, errGateStopped) || time.Since(start) > 2*time.Second {
		t.Fatalf("err %v，用了 %v，应在闸门停用时立即结束", err, time.Since(start))
	}
	time.Sleep(200 * time.Millisecond)
	if n := up.requests.Load(); n != 1 {
		t.Fatalf("旧网关收到 %d 次，停用后不应再重发", n)
	}
}

// 429 响应头到了、响应体迟迟不来：读体有时限，不让客户端一直等不到响应头。
func TestRetry429StalledBody(t *testing.T) {
	old := probeReadTimeout
	probeReadTimeout = 200 * time.Millisecond
	defer func() { probeReadTimeout = old }()
	release := make(chan struct{})
	defer close(release)
	up := newFlakyUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		io.WriteString(w, `{"error":{"type":"rate_limit_error",`)
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	px, _ := newTestProxy(t, up.URL, fastRetry)
	start := time.Now()
	req, _ := http.NewRequest(http.MethodPost, px.URL+"/v1/responses", strings.NewReader("{}"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 429 || time.Since(start) > 2*time.Second || up.requests.Load() != 1 {
		t.Fatalf("status %d，用了 %v，网关收到 %d 次", resp.StatusCode, time.Since(start), up.requests.Load())
	}
	b, err := io.ReadAll(resp.Body)
	if err == nil || string(b) != `{"error":{"type":"rate_limit_error",` || time.Since(start) > 2*time.Second {
		t.Fatalf("body %q err %v，用了 %v；应拿到已读的部分后出错，不挂住", b, err, time.Since(start))
	}
}

// 读 429 体到时限与上游写 trailer 重叠时，读与计时回调、ReverseProxy 对响应的访问不并发（配合 -race 跑），
// 也不会卡住。时限极短，多数响应读不完、被截断交回，客户端见到的是连接中断，这里只要求请求都及时结束。
func TestRetry429ProbeTimeoutWithTrailers(t *testing.T) {
	old := probeReadTimeout
	probeReadTimeout = 200 * time.Microsecond
	defer func() { probeReadTimeout = old }()
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for i := range 50 {
			w.Header().Add("Trailer", fmt.Sprintf("X-Trace-%d", i))
		}
		w.WriteHeader(http.StatusTooManyRequests)
		w.(http.Flusher).Flush()
		time.Sleep(200 * time.Microsecond)
		io.WriteString(w, "unclassified")
		for i := range 20 {
			w.Header().Set(fmt.Sprintf("X-Trace-%d", i), "complete")
		}
	}))
	t.Cleanup(up.Close)
	px, _ := newTestProxy(t, up.URL, func(c *config) { c.WaitForUpstream = false })
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				resp, err := http.Post(px.URL+"/v1/responses", "application/json", strings.NewReader("{}"))
				if err != nil {
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("请求没有及时结束")
	}
}

// 429 重发遇到连接失败、转入连接重试的退避时闸门停用：立即结束，不再向旧网关发送。
func TestRetry429GateStoppedDuringConnectBackoff(t *testing.T) {
	var up *flakyUpstream
	up = newFlakyUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		up.dropBiz.Store(true)
		gatewayBusy(w, "gateway_queue_full")
	})
	_, p := newTestProxy(t, up.URL, fastRetry, func(c *config) { c.Retry429MaxWait = duration(10 * time.Second) })
	gate := newUpstreamGate(p.cfg.healthURL(), p.base)
	p.rp.Transport.(*resilientTransport).gate = gate
	go func() {
		for up.dropped.Load() == 0 {
			time.Sleep(10 * time.Millisecond)
		}
		time.Sleep(200 * time.Millisecond) // 落在连接重试的 1 秒退避里
		gate.stop()
	}()
	start := time.Now()
	_, err, _ := roundTripWithNote(t, p, context.Background(), up.URL+"/v1/responses")
	if !errors.Is(err, errGateStopped) || time.Since(start) > 900*time.Millisecond {
		t.Fatalf("err %v，用了 %v，应在闸门停用时立即结束", err, time.Since(start))
	}
	time.Sleep(1200 * time.Millisecond)
	if n, d := up.requests.Load(), up.dropped.Load(); n != 1 || d != 1 {
		t.Fatalf("旧网关处理 %d 次、断开 %d 次，停用后不应再发", n, d)
	}
}

// slow429 先回 429 响应头，响应体过 bodyDelay 才写出。
func slow429(w http.ResponseWriter, bodyDelay time.Duration) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusTooManyRequests)
	w.(http.Flusher).Flush()
	time.Sleep(bodyDelay)
	io.WriteString(w, `{"error":{"type":"rate_limit_error","code":"gateway_queue_full","message":"late"}}`)
}

// 读 429 响应体也受预算截止约束：重发的 429 体到点没读完，交回上一次完整的 429；第一次发送拿到响应头时
// 预算已用完，不读、原样交回，体随后照常到达。
func TestRetry429ProbeBoundedByBudget(t *testing.T) {
	var n atomic.Int32
	up := newFlakyUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) == 1 {
			gatewayBusy(w, "gateway_queue_full")
			return
		}
		slow429(w, 1200*time.Millisecond)
	})
	px, _ := newTestProxy(t, up.URL, fastRetry, func(c *config) { c.Retry429MaxWait = duration(400 * time.Millisecond) })
	start := time.Now()
	resp, err := http.Post(px.URL+"/v1/responses", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	headers := time.Since(start)
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 429 || headers > 900*time.Millisecond || !strings.Contains(string(b), "exceeded for account") ||
		resp.Header.Get("X-Local-Proxy-Retries") != "1" {
		t.Fatalf("status %d，响应头用了 %v，body %q，重发标记 %q", resp.StatusCode, headers, b, resp.Header.Get("X-Local-Proxy-Retries"))
	}

	first := newFlakyUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(350 * time.Millisecond) // 第一次发送拿到响应头时预算已用完
		slow429(w, time.Second)
	})
	px2, _ := newTestProxy(t, first.URL, fastRetry, func(c *config) { c.Retry429MaxWait = duration(200 * time.Millisecond) })
	start = time.Now()
	resp, err = http.Post(px2.URL+"/v1/responses", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	headers = time.Since(start)
	b, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 429 || headers > 800*time.Millisecond || !strings.Contains(string(b), `"message":"late"`) || first.requests.Load() != 1 {
		t.Fatalf("status %d，响应头用了 %v，body %q，网关收到 %d 次", resp.StatusCode, headers, b, first.requests.Load())
	}
}

// 最后一次 429 无法分类时，记录里不沿用上一次的 code 与 scope，请求号取最后一次的。
func TestRetry429NoteResetPerResponse(t *testing.T) {
	var n atomic.Int32
	up := newFlakyUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) == 1 {
			w.Header().Set("X-Request-ID", "r1")
			gatewayBusy(w, "gateway_concurrency_limit")
			return
		}
		w.Header().Set("X-Request-ID", "r2")
		w.WriteHeader(http.StatusTooManyRequests)
		io.WriteString(w, "Too Many Requests")
	})
	_, p := newTestProxy(t, up.URL, fastRetry)
	resp, err, note := roundTripWithNote(t, p, context.Background(), up.URL+"/v1/responses")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(b) != "Too Many Requests" || note.code != "" || note.scope != "" || note.reqID != "r2" || note.retries != 1 || note.end != "pass" {
		t.Fatalf("body %q，note %+v", b, *note)
	}
}
