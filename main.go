// sub2api-local-proxy：codex 客户端与 sub2api 网关之间的本地反向代理。
//
// 其余请求原样转发，只多做三件事：
//   - 模型目录：每次先回源，回源失败、到点未回或网关 5xx 时返回磁盘上的最新缓存，慢的回源在后台做完（catalog.go）；
//   - 上行压缩：发往指定路径的 POST 请求体用 zstd 压缩后再发给网关；
//   - 网关暂时不可用（隧道断、网关重启）时，请求等它恢复，拿到响应头之前失败的请求在恢复后重发（upstream.go）。
//
// 配置与数据放在 ~/.sub2api-local-proxy/（config.json、cache/），配置文件改了自动重新加载（app.go）。
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

func main() {
	path := flag.String("config", filepath.Join(homeDir(), "config.json"), "配置文件；目录缓存放在它所在目录的 cache/ 下")
	initOnly := flag.Bool("init", false, "只生成配置文件（已存在则不动）后退出，供安装脚本使用")
	upstream := flag.String("upstream", "", "与 -init 一起用：写进新配置的网关地址")
	logFile := flag.String("log-file", "", "与 -init 一起用：写进新配置的日志文件")
	flag.Parse()
	if *initOnly {
		if err := initConfig(*path, *upstream, *logFile); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	a, err := newApp(*path)
	if err != nil {
		log.Print(err)
		if errors.Is(err, errConfigCreated) {
			os.Exit(2)
		}
		os.Exit(1)
	}
	a.watch()
}

func initConfig(path, upstream, logFile string) error {
	created, err := writeTemplate(path, upstream, logFile)
	if err != nil {
		return err
	}
	if !created {
		fmt.Printf("配置已存在，未改动: %s\n", path)
	} else {
		fmt.Printf("已生成配置: %s\n", path)
	}
	_, err = loadConfig(path)
	return err
}
