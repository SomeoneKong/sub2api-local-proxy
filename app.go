package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	configPollInterval = 2 * time.Second
	logRotateBytes     = 10 << 20
)

// app 持有监听与当前生效的 proxy。配置文件变了就整体换一个 proxy：新请求走新配置，
// 在途请求在旧 proxy 上走完；监听地址变了就换监听，旧监听上的在途连接照常收尾。
type app struct {
	path  string
	cur   atomic.Pointer[proxy]
	logs  *logSink
	mu    sync.Mutex // 保护下面几项，只有 reload 会改
	cfg   *config
	srv   *http.Server
	ln    net.Listener
	stamp fileStamp
}

type fileStamp struct {
	mod  time.Time
	size int64
}

func statConfig(path string) (fileStamp, error) {
	st, err := os.Stat(path)
	if err != nil {
		return fileStamp{}, err
	}
	return fileStamp{st.ModTime(), st.Size()}, nil
}

// newApp 读配置、开始监听；监听或配置有问题直接返回错误。
func newApp(path string) (*app, error) {
	stamp, _ := statConfig(path)
	cfg, err := loadConfig(path)
	if err != nil {
		return nil, err
	}
	a := &app{path: path, logs: &logSink{}, stamp: stamp}
	if err := a.logs.set(cfg.LogFile); err != nil {
		return nil, err
	}
	log.SetOutput(a.logs)
	p, err := newProxy(cfg, nil)
	if err != nil {
		return nil, err
	}
	a.cur.Store(p)
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return nil, err
	}
	a.cfg = cfg
	a.serve(ln)
	log.Printf("监听 %s → %s，配置 %s，目录缓存 %s", ln.Addr(), cfg.upstreamURL, path, cfg.cacheDir)
	return a, nil
}

func (a *app) ServeHTTP(w http.ResponseWriter, r *http.Request) { a.cur.Load().ServeHTTP(w, r) }

func (a *app) serve(ln net.Listener) {
	srv := &http.Server{Handler: a, ReadHeaderTimeout: 30 * time.Second}
	a.srv, a.ln = srv, ln
	go func() {
		if err := srv.Serve(ln); err != http.ErrServerClosed {
			log.Fatalf("监听 %s 出错: %v", ln.Addr(), err)
		}
	}()
}

func (a *app) addr() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.ln.Addr().String()
}

// watch 每隔 configPollInterval 看一次配置文件的修改时间与大小，变了就重新加载。
// 用轮询而不是文件系统通知：三个平台行为一致，也不怕编辑器用"写临时文件再改名"的方式保存。
func (a *app) watch() {
	for range time.Tick(configPollInterval) {
		a.checkConfig()
	}
}

func (a *app) checkConfig() {
	stamp, err := statConfig(a.path)
	if err != nil {
		return // 编辑器保存的瞬间文件可能不存在，下一轮再看
	}
	a.mu.Lock()
	changed := stamp != a.stamp
	a.stamp = stamp
	a.mu.Unlock()
	if changed {
		a.reload()
	}
}

// reload 重新加载配置；新配置有误时记日志并继续用原配置。
func (a *app) reload() {
	cfg, err := loadConfig(a.path)
	if err != nil {
		log.Printf("配置未生效，继续用原配置: %v", err)
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	old := a.cfg
	if reflect.DeepEqual(cfg, old) {
		return
	}
	// 先做完会失败的准备（建 proxy、绑新监听、开新日志），再统一切换；建 proxy 失败时什么都不变。
	// 网关地址没变就沿用闸门：不可用状态与等待中的请求都延续下去。
	prev := a.cur.Load()
	var gate *upstreamGate
	if cfg.healthURL() == old.healthURL() {
		gate = prev.gate
	}
	p, err := newProxy(cfg, gate)
	if err != nil {
		log.Printf("配置未生效，继续用原配置: %v", err)
		return
	}
	var ln net.Listener
	if cfg.Listen != old.Listen {
		if ln, err = net.Listen("tcp", cfg.Listen); err != nil {
			log.Printf("新监听地址 %s 不可用，继续监听 %s: %v", cfg.Listen, a.ln.Addr(), err)
			cfg.Listen = old.Listen
		}
	}
	if cfg.LogFile != old.LogFile {
		if err := a.logs.set(cfg.LogFile); err != nil {
			log.Printf("打开日志文件 %q 失败，日志仍写原处: %v", cfg.LogFile, err)
			cfg.LogFile = old.LogFile
		}
	}
	a.cur.Store(p)
	prev.close()
	if gate == nil {
		prev.gate.stop()
	}
	if ln != nil {
		oldSrv := a.srv
		a.serve(ln)
		go oldSrv.Shutdown(context.Background()) // 关掉旧监听，在途请求照常收尾
	}
	a.cfg = cfg
	log.Printf("已加载新配置，变化: %s", diffConfig(old, cfg))
}

// diffConfig 列出配置文件里取值变了的字段。
func diffConfig(a, b *config) string {
	toMap := func(c *config) map[string]any {
		var m map[string]any
		data, _ := json.Marshal(c)
		json.Unmarshal(data, &m)
		return m
	}
	ma, mb := toMap(a), toMap(b)
	var out []string
	for k, v := range mb {
		if !reflect.DeepEqual(ma[k], v) {
			out = append(out, fmt.Sprintf("%s=%v", k, v))
		}
	}
	sort.Strings(out)
	if len(out) == 0 {
		return "无"
	}
	return fmt.Sprint(out)
}

// logSink 是日志的去处：没配 log_file 写 stderr；配了就追加写文件，超过 10 MiB 时轮转（rotate）。
type logSink struct {
	mu   sync.Mutex
	path string
	f    *os.File
	size int64
}

func (l *logSink) set(path string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var f *os.File
	var size int64
	if path != "" {
		var err error
		if f, size, err = openLog(path); err != nil {
			return err
		}
	}
	if l.f != nil {
		l.f.Close()
	}
	l.path, l.f, l.size = path, f, size
	return nil
}

func openLog(path string) (*os.File, int64, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, 0, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	return f, st.Size(), nil
}

func (l *logSink) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return os.Stderr.Write(p)
	}
	if l.size+int64(len(p)) > logRotateBytes {
		if err := l.rotate(); err != nil {
			return os.Stderr.Write(p)
		}
	}
	n, err := l.f.Write(p)
	l.size += int64(n)
	return n, err
}

// rotate 把当前文件改名为 .1（覆盖上一份）再开新文件。改名失败时（Windows 上文件被别的程序以
// 不允许改名的方式打开，比如正在 tail）退回到清空原文件；连清空也失败就继续追加，但再写满 10 MiB
// 才重试，免得每写一行都重试一次。失败原因写进新文件开头。
func (l *logSink) rotate() error {
	l.f.Close()
	note := ""
	if err := os.Rename(l.path, l.path+".1"); err != nil {
		if terr := os.Truncate(l.path, 0); terr != nil {
			note = fmt.Sprintf("日志轮转失败，继续追加: 改名 %v；清空 %v", err, terr)
		} else {
			note = fmt.Sprintf("日志改名为 .1 失败（%v），已清空原文件", err)
		}
	}
	f, size, err := openLog(l.path)
	if err != nil {
		l.f = nil
		return err
	}
	l.f, l.size = f, size
	if note != "" {
		n, _ := fmt.Fprintf(f, "%s %s\n", time.Now().Format("2006/01/02 15:04:05"), note)
		l.size = int64(n)
	}
	return nil
}
