package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// config 是配置文件 config.json 的内容；文件里没写的字段取 defaultConfig 的值。
type config struct {
	Listen   string `json:"listen"`
	Upstream string `json:"upstream"` // 网关地址，如 http://sub2api.example.com:8080，不带路径

	// 网关暂时不可用（隧道断、网关重启）时，请求是否等它恢复、并在恢复后重发拿到响应头前失败的请求。
	// 关掉时失败立即回 502，由 codex 自己重试。
	WaitForUpstream bool     `json:"wait_for_upstream"`
	UpstreamWait    duration `json:"upstream_wait"` // 每个请求最多等多久
	HealthPath      string   `json:"health_path"`   // 探活路径，返回 200 即视为可用

	// 发出请求后等响应头的上限，0 不限。网关在等账号空位、等上游首包时会合法地迟迟不回响应头
	// （粘性会话等位最长 120 秒，上游 OpenAI 的响应头不设超时），所以要留足余量。超时回 504、不重发。
	ResponseHeaderTimeout duration `json:"response_header_timeout"`

	// 网关排队类 429（error.type 为 rate_limit_error、error.code 在 retry_429_codes 里，空串表示无 code）
	// 由代理退避后重发同一请求，客户端看不到这次 429。预算从请求进来时起算：下一次间隔会超过
	// retry_429_max_wait 就不再重发，重发出去的到 retry_429_max_wait 还没拿到响应头就取消，都交回 429。
	// 间隔从 base_delay 起翻倍、不超过 max_delay，再加 ±30% 抖动，Retry-After 是下限。
	// 其它 429（额度、RPM 等）一律原样交回。
	Retry429          bool     `json:"retry_429"`
	Retry429Codes     []string `json:"retry_429_codes"`
	Retry429MaxWait   duration `json:"retry_429_max_wait"`
	Retry429BaseDelay duration `json:"retry_429_base_delay"`
	Retry429MaxDelay  duration `json:"retry_429_max_delay"`

	// 模型目录缓存（只管带 client_version 的 codex 格式目录）：回源失败时返回最近一次成功的目录。
	// 关掉时目录请求与其他请求一样原样转发。
	CatalogCache   bool     `json:"catalog_cache"`
	CatalogPath    string   `json:"catalog_path"`
	CatalogTimeout duration `json:"catalog_timeout"` // 请求等回源的上限，到点即用缓存；要小于 codex 的 5 秒
	// 回源本身的上限：请求不再等之后回源在后台继续，拿到目录照样写缓存，网关慢时缓存也能刷新。
	CatalogFetchTimeout duration `json:"catalog_fetch_timeout"`
	// 本凭据没有缓存时借用其他凭据的缓存。只在全部 key 属于同一分组（目录相同）时打开。
	CatalogShareAcrossKeys bool `json:"catalog_share_across_keys"`

	// 上行 zstd 压缩：只能填网关读请求体时会解压的路径。
	Compress         bool     `json:"compress"`
	CompressPaths    []string `json:"compress_paths"`
	CompressMinBytes int      `json:"compress_min_bytes"`
	ZstdLevel        int      `json:"zstd_level"` // 1–22，按 zstd 命令行的级别映射到编码器档位

	LogFile string `json:"log_file"` // 空则写 stderr；可用 ~/ 开头

	upstreamURL *url.URL
	cacheDir    string
}

func defaultConfig() config {
	return config{
		Listen:                "127.0.0.1:28770",
		WaitForUpstream:       true,
		UpstreamWait:          duration(60 * time.Second),
		HealthPath:            "/health",
		ResponseHeaderTimeout: duration(5 * time.Minute),
		Retry429:              true,
		Retry429Codes:         []string{"gateway_queue_full", "gateway_concurrency_limit", ""},
		Retry429MaxWait:       duration(2 * time.Minute),
		Retry429BaseDelay:     duration(2 * time.Second),
		Retry429MaxDelay:      duration(30 * time.Second),
		CatalogCache:          true,
		CatalogPath:           "/v1/models",
		CatalogTimeout:        duration(3 * time.Second),
		CatalogFetchTimeout:   duration(60 * time.Second),
		Compress:              true,
		CompressPaths:         []string{"/v1/responses", "/v1/responses/compact", "/v1/alpha/search"},
		CompressMinBytes:      1024,
		ZstdLevel:             3,
	}
}

// homeDir 是默认的数据目录：配置、目录缓存都放在这里。
func homeDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		home = "."
	}
	return filepath.Join(home, ".sub2api-local-proxy")
}

var errConfigCreated = errors.New("已生成配置模板")

// writeTemplate 在配置文件不存在时写一份含全部字段默认值的模板；已存在则不动，返回 false。
func writeTemplate(path, upstream, logFile string) (bool, error) {
	if _, err := os.Stat(path); err == nil {
		return false, nil
	}
	c := defaultConfig()
	c.Upstream, c.LogFile = upstream, logFile
	if upstream != "" {
		check := c // validate 会展开 log_file 里的 ~，文件里保留原样
		if err := check.validate(); err != nil {
			return false, err
		}
	}
	tpl, _ := json.MarshalIndent(c, "", "  ")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return false, err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return false, err
	}
	_, err = f.Write(append(tpl, '\n'))
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err == nil, err
}

// loadConfig 读配置并校验。文件不存在时写一份模板（upstream 留空）并返回 errConfigCreated。
// 数据目录取配置文件所在目录，目录缓存在其下的 cache/。
func loadConfig(path string) (*config, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		if _, err := writeTemplate(path, "", ""); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("%w %s，填好 upstream 后重新启动", errConfigCreated, path)
	}
	if err != nil {
		return nil, err
	}
	c := defaultConfig()
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("解析 %s: %w", path, err)
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("解析 %s: 第一个 JSON 对象之后还有内容", path)
	}
	if err := c.validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	c.cacheDir = filepath.Join(filepath.Dir(path), "cache")
	return &c, nil
}

func (c *config) validate() error {
	if _, _, err := net.SplitHostPort(c.Listen); err != nil {
		return fmt.Errorf("listen %q 不是 host:port: %w", c.Listen, err)
	}
	u, err := url.Parse(c.Upstream)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
		return fmt.Errorf("upstream 必须是 http(s)://host:port 形式，当前值 %q", c.Upstream)
	}
	if u.Path != "" && u.Path != "/" || u.RawQuery != "" {
		return fmt.Errorf("upstream 不能带路径或查询串（%q），请求路径原样拼在网关地址后", c.Upstream)
	}
	u.Path = ""
	c.upstreamURL = u
	if c.WaitForUpstream && c.UpstreamWait <= 0 {
		return errors.New("wait_for_upstream 打开时 upstream_wait 必须大于 0")
	}
	for name, p := range map[string]string{"health_path": c.HealthPath, "catalog_path": c.CatalogPath} {
		if !strings.HasPrefix(p, "/") {
			return fmt.Errorf("%s 必须以 / 开头，当前值 %q", name, p)
		}
	}
	for _, p := range c.CompressPaths {
		if !strings.HasPrefix(p, "/") {
			return fmt.Errorf("compress_paths 里的 %q 必须以 / 开头", p)
		}
	}
	if c.ResponseHeaderTimeout < 0 {
		return errors.New("response_header_timeout 不能为负")
	}
	if c.Retry429 && (c.Retry429MaxWait <= 0 || c.Retry429BaseDelay <= 0 || c.Retry429MaxDelay < c.Retry429BaseDelay) {
		return errors.New("retry_429 打开时 retry_429_max_wait、retry_429_base_delay 必须大于 0，retry_429_max_delay 不能小于 retry_429_base_delay")
	}
	if c.CatalogTimeout <= 0 {
		return errors.New("catalog_timeout 必须大于 0")
	}
	if c.CatalogFetchTimeout < c.CatalogTimeout {
		return errors.New("catalog_fetch_timeout 不能小于 catalog_timeout")
	}
	if c.CompressMinBytes < 0 {
		return errors.New("compress_min_bytes 不能为负")
	}
	if c.ZstdLevel < 1 || c.ZstdLevel > 22 {
		return fmt.Errorf("zstd_level 必须在 1–22 之间，当前值 %d", c.ZstdLevel)
	}
	if rest, ok := strings.CutPrefix(c.LogFile, "~/"); ok {
		home, err := os.UserHomeDir()
		if err != nil {
			return err
		}
		c.LogFile = filepath.Join(home, rest)
	}
	return nil
}

func (c *config) healthURL() string {
	u := *c.upstreamURL
	u.Path = c.HealthPath
	return u.String()
}

// duration 在 JSON 里写成 "60s"、"1m30s" 这样的字符串。
type duration time.Duration

func (d duration) D() time.Duration { return time.Duration(d) }

func (d duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

func (d *duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("时长要写成字符串，如 \"60s\": %w", err)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = duration(v)
	return nil
}
