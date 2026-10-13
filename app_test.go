package main

import (
	"bytes"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeConfig(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestConfigDefaultsAndOverlay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	writeConfig(t, path, `{"upstream": "http://10.0.0.1:8080/", "upstream_wait": "90s", "compress": false}`)
	c, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.UpstreamWait.D() != 90*time.Second || c.Compress || !c.WaitForUpstream || !c.CatalogCache {
		t.Fatalf("写了的字段应覆盖默认值，没写的取默认值: %+v", c)
	}
	if c.Listen != "127.0.0.1:28770" || c.ZstdLevel != 3 || len(c.CompressPaths) != 3 {
		t.Fatalf("默认值不对: %+v", c)
	}
	if !c.Retry429 || c.Retry429MaxWait.D() != 2*time.Minute || len(c.Retry429Codes) != 3 {
		t.Fatalf("429 重试默认值不对: %+v", c)
	}
	if c.upstreamURL.String() != "http://10.0.0.1:8080" || c.cacheDir != filepath.Join(dir, "cache") {
		t.Fatalf("upstream %s，cacheDir %s", c.upstreamURL, c.cacheDir)
	}
}

func TestConfigRejects(t *testing.T) {
	cases := map[string]string{
		"缺 upstream":        `{}`,
		"upstream 带路径":      `{"upstream": "http://h:1/v1"}`,
		"upstream 没有协议":     `{"upstream": "h:1"}`,
		"拼错字段":              `{"upstream": "http://h:1", "upstrem_wait": "1s"}`,
		"时长写成数字":            `{"upstream": "http://h:1", "upstream_wait": 60}`,
		"等待打开但时长为 0":        `{"upstream": "http://h:1", "upstream_wait": "0s"}`,
		"zstd 级别越界":         `{"upstream": "http://h:1", "zstd_level": 0}`,
		"路径不以 / 开头":         `{"upstream": "http://h:1", "health_path": "health"}`,
		"listen 不是地址":       `{"upstream": "http://h:1", "listen": "28770"}`,
		"压缩路径不以 / 开头":       `{"upstream": "http://h:1", "compress_paths": ["v1/responses"]}`,
		"JSON 坏了":           `{"upstream": "http://h:1",`,
		"catalog_timeout 0": `{"upstream": "http://h:1", "catalog_timeout": "0s"}`,
		"upstream 没有主机名":    `{"upstream": "http://:8080"}`,
		"尾随垃圾":              `{"upstream": "http://h:1"} trailing`,
		"第二个对象":             `{"upstream": "http://h:1"} {"compress": false}`,
		"响应头超时为负":           `{"upstream": "http://h:1", "response_header_timeout": "-1s"}`,
		"429 重试预算为 0":       `{"upstream": "http://h:1", "retry_429_max_wait": "0s"}`,
		"429 最大间隔小于起始间隔":    `{"upstream": "http://h:1", "retry_429_base_delay": "10s", "retry_429_max_delay": "5s"}`,
	}
	for name, body := range cases {
		path := filepath.Join(t.TempDir(), "config.json")
		writeConfig(t, path, body)
		if _, err := loadConfig(path); err == nil {
			t.Errorf("%s: 应报错", name)
		}
	}
	for name, body := range map[string]string{
		"等待关闭时 upstream_wait 不受限制": `{"upstream": "http://h:1", "wait_for_upstream": false, "upstream_wait": "0s"}`,
		"末尾空白":            "{\"upstream\": \"http://h:1\"}\n\n  ",
		"IPv6":            `{"upstream": "http://[::1]:8080"}`,
		"响应头超时 0 表示不限":    `{"upstream": "http://h:1", "response_header_timeout": "0s"}`,
		"429 重试关闭时预算不受限制": `{"upstream": "http://h:1", "retry_429": false, "retry_429_max_wait": "0s"}`,
	} {
		path := filepath.Join(t.TempDir(), "config.json")
		writeConfig(t, path, body)
		if _, err := loadConfig(path); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestConfigTemplateCreated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "config.json")
	if _, err := loadConfig(path); !errors.Is(err, errConfigCreated) {
		t.Fatalf("文件不存在时应生成模板，得到 %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(path); err == nil || !strings.Contains(err.Error(), "upstream") {
		t.Fatalf("模板的 upstream 是空的，应提示填写，得到 %v", err)
	}
}

func TestConfigLogFileTilde(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	writeConfig(t, path, `{"upstream": "http://h:1", "log_file": "~/.sub2api-local-proxy/proxy.log"}`)
	c, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	home, _ := os.UserHomeDir()
	if c.LogFile != filepath.Join(home, ".sub2api-local-proxy", "proxy.log") {
		t.Fatalf("log_file = %q", c.LogFile)
	}
}

// startApp 用临时目录里的配置起一个 app，监听随机端口。
func startApp(t *testing.T, body string) (*app, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	writeConfig(t, path, body)
	a, err := newApp(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		a.mu.Lock()
		a.srv.Close()
		a.mu.Unlock()
		a.cur.Load().close()
		a.cur.Load().gate.stop()
	})
	return a, path
}

func freeAddr(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func appConfig(listen, upstream, extra string) string {
	return `{"listen": "` + listen + `", "upstream": "` + upstream + `"` + extra + `}`
}

func TestReloadSwitchesUpstreamAndFeatures(t *testing.T) {
	upA, chA := recordingUpstream(t)
	upB, chB := recordingUpstream(t)
	a, path := startApp(t, appConfig("127.0.0.1:0", upA.URL, ""))
	body := []byte(strings.Repeat("hello ", 1000))
	post(t, "http://"+a.addr()+"/v1/responses", body, nil)
	if got := <-chA; got.header.Get("Content-Encoding") != "zstd" {
		t.Fatal("默认应压缩")
	}

	writeConfig(t, path, appConfig("127.0.0.1:0", upB.URL, `, "compress": false`))
	a.reload()
	post(t, "http://"+a.addr()+"/v1/responses", body, nil)
	select {
	case got := <-chB:
		if got.header.Get("Content-Encoding") != "" || !bytes.Equal(got.body, body) {
			t.Fatal("关闭压缩后应发明文")
		}
	case <-chA:
		t.Fatal("新配置的请求仍发到了旧网关")
	}

	// 配置写坏了：继续用原配置。
	writeConfig(t, path, `{"upstream": `)
	a.reload()
	post(t, "http://"+a.addr()+"/v1/responses", body, nil)
	select {
	case <-chB:
	case <-chA:
		t.Fatal("坏配置不应生效")
	}
}

func TestReloadCatalogCacheSwitch(t *testing.T) {
	up := newCatalogUpstream(t)
	a, path := startApp(t, appConfig("127.0.0.1:0", up.URL, ""))
	if _, _, src := getCatalog(t, "http://"+a.addr(), "k1", "0.160.1"); src != "upstream" {
		t.Fatalf("开着目录缓存时应经缓存逻辑，来源 %q", src)
	}
	writeConfig(t, path, appConfig("127.0.0.1:0", up.URL, `, "catalog_cache": false`))
	a.reload()
	if code, body, src := getCatalog(t, "http://"+a.addr(), "k1", "0.160.1"); code != 200 || src != "" || body != catalogV1 {
		t.Fatalf("关掉后应原样转发: %d %q %q", code, body, src)
	}
}

func TestReloadListen(t *testing.T) {
	up, ch := recordingUpstream(t)
	a, path := startApp(t, appConfig("127.0.0.1:0", up.URL, ""))
	oldAddr := a.addr()
	newAddr := freeAddr(t)
	writeConfig(t, path, appConfig(newAddr, up.URL, ""))
	a.reload()
	if a.addr() != newAddr {
		t.Fatalf("监听 %s，应换到 %s", a.addr(), newAddr)
	}
	post(t, "http://"+newAddr+"/v1/responses", []byte("{}"), nil)
	<-ch
	time.Sleep(100 * time.Millisecond)
	if c, err := net.DialTimeout("tcp", oldAddr, time.Second); err == nil {
		c.Close()
		t.Fatalf("旧地址 %s 应已停止监听", oldAddr)
	}

	// 新地址被占用：保留原监听，其余改动照常生效。
	busy, _ := net.Listen("tcp", "127.0.0.1:0")
	defer busy.Close()
	writeConfig(t, path, appConfig(busy.Addr().String(), up.URL, `, "compress": false`))
	a.reload()
	if a.addr() != newAddr {
		t.Fatalf("新地址不可用时应保留 %s，现在 %s", newAddr, a.addr())
	}
	post(t, "http://"+newAddr+"/v1/responses", []byte(strings.Repeat("x", 4096)), nil)
	if got := <-ch; got.header.Get("Content-Encoding") != "" {
		t.Fatal("监听没换成，其余改动也应生效")
	}
}

// checkConfig 按修改时间与大小发现配置文件变了才重新加载。
func TestCheckConfigDetectsChange(t *testing.T) {
	upA, chA := recordingUpstream(t)
	upB, chB := recordingUpstream(t)
	a, path := startApp(t, appConfig("127.0.0.1:0", upA.URL, ""))
	a.checkConfig() // 没变：什么也不做
	post(t, "http://"+a.addr()+"/v1/responses", []byte("{}"), nil)
	<-chA
	writeConfig(t, path, appConfig("127.0.0.1:0", upB.URL, ""))
	later := time.Now().Add(time.Minute)
	os.Chtimes(path, later, later)
	a.checkConfig()
	post(t, "http://"+a.addr()+"/v1/responses", []byte("{}"), nil)
	select {
	case <-chB:
	case <-chA:
		t.Fatal("配置文件变了却没有重新加载")
	}
}

func TestLogRotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "proxy.log")
	l := &logSink{}
	if err := l.set(path); err != nil {
		t.Fatal(err)
	}
	defer l.set("")
	line := bytes.Repeat([]byte("x"), 1<<20)
	for i := 0; i < 11; i++ {
		if _, err := l.Write(line); err != nil {
			t.Fatal(err)
		}
	}
	st1, err1 := os.Stat(path + ".1")
	st, err := os.Stat(path)
	if err1 != nil || err != nil || st1.Size() > logRotateBytes || st.Size() >= st1.Size() {
		t.Fatalf("应在 10 MiB 处轮转: %v %v", err1, err)
	}
}

// 网关不可用时改了与网关无关的配置：等待中的请求继续等，网关恢复后照常完成。
func TestReloadKeepsWaitersWhenUpstreamUnchanged(t *testing.T) {
	up := newFlakyUpstream(t, echoBody)
	up.down.Store(true)
	a, path := startApp(t, appConfig("127.0.0.1:0", up.URL, ""))
	done := make(chan int, 1)
	go func() {
		resp := post(t, "http://"+a.addr()+"/v1/responses", []byte("{}"), nil)
		done <- resp.StatusCode
	}()
	time.Sleep(500 * time.Millisecond)
	writeConfig(t, path, appConfig(a.addr(), up.URL, `, "compress": false`))
	a.reload()
	time.Sleep(300 * time.Millisecond)
	up.down.Store(false)
	select {
	case code := <-done:
		if code != 200 {
			t.Fatalf("status %d，应等到网关恢复后完成", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("请求没有完成")
	}
}

// 网关地址改了：在原地址上等待的请求立即放弃（502），不必等到超时。
func TestReloadReleasesWaitersWhenUpstreamChanged(t *testing.T) {
	dead := newFlakyUpstream(t, echoBody)
	dead.down.Store(true)
	good, _ := recordingUpstream(t)
	a, path := startApp(t, appConfig("127.0.0.1:0", dead.URL, ""))
	done := make(chan int, 1)
	go func() {
		resp := post(t, "http://"+a.addr()+"/v1/responses", []byte("{}"), nil)
		done <- resp.StatusCode
	}()
	time.Sleep(500 * time.Millisecond)
	writeConfig(t, path, appConfig(a.addr(), good.URL, ""))
	start := time.Now()
	a.reload()
	select {
	case code := <-done:
		if code != http.StatusBadGateway || time.Since(start) > 2*time.Second {
			t.Fatalf("status %d，用了 %v", code, time.Since(start))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("换了网关地址，原地址上的等待应立即结束")
	}
}

// 新配置建 proxy 失败（缓存目录建不出来）：监听、日志、网关都不变；问题排除后同一份配置可以生效。
func TestReloadFailureChangesNothing(t *testing.T) {
	upA, chA := recordingUpstream(t)
	upB, _ := recordingUpstream(t)
	a, path := startApp(t, appConfig("127.0.0.1:0", upA.URL, ""))
	oldAddr := a.addr()
	cacheDir := filepath.Join(filepath.Dir(path), "cache")
	os.RemoveAll(cacheDir)
	writeConfig(t, cacheDir, "挡住缓存目录")
	logPath := filepath.Join(t.TempDir(), "proxy.log")
	newAddr := freeAddr(t)
	writeConfig(t, path, appConfig(newAddr, upB.URL, `, "log_file": "`+filepath.ToSlash(logPath)+`"`))
	a.reload()
	if a.addr() != oldAddr {
		t.Fatalf("建 proxy 失败时不应换监听，现在 %s", a.addr())
	}
	if _, err := os.Stat(logPath); err == nil {
		t.Fatal("建 proxy 失败时不应换日志文件")
	}
	post(t, "http://"+oldAddr+"/v1/responses", []byte("{}"), nil)
	<-chA
	os.Remove(cacheDir)
	a.reload()
	if a.addr() != newAddr {
		t.Fatalf("问题排除后应生效，监听 %s", a.addr())
	}
	a.logs.set("") // 让临时目录能删掉
}

// 日志文件被别的程序开着（Windows 上改名会失败）时，大小仍受轮转上限约束。
func TestLogRotationWhenFileHeld(t *testing.T) {
	path := filepath.Join(t.TempDir(), "proxy.log")
	l := &logSink{}
	if err := l.set(path); err != nil {
		t.Fatal(err)
	}
	defer l.set("")
	holder, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	line := bytes.Repeat([]byte("x"), 1<<20)
	for i := 0; i < 25; i++ {
		if _, err := l.Write(line); err != nil {
			t.Fatal(err)
		}
	}
	if st, err := os.Stat(path); err != nil || st.Size() > logRotateBytes {
		t.Fatalf("日志 %d 字节，超过上限: %v", st.Size(), err)
	}
}
