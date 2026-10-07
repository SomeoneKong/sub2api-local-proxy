#!/usr/bin/env bash
# 交叉编译到 dist/，连同安装脚本一起，整个目录拷到目标机器上运行对应的 install-*.sh / .ps1。
# Windows 版编成无控制台窗口的程序（计划任务启动时不弹窗），日志写 log_file。
set -euo pipefail
cd "$(dirname "$0")"

name=sub2api-local-proxy
rm -rf dist
mkdir -p dist
build() { # GOOS GOARCH 后缀 额外的链接参数
  CGO_ENABLED=0 GOOS=$1 GOARCH=$2 go build -trimpath -ldflags "-s -w $4" -o "dist/$name-$1-$2$3" .
}
build linux amd64 "" ""
build linux arm64 "" ""
build darwin amd64 "" ""
build darwin arm64 "" ""
build windows amd64 .exe "-H=windowsgui"
cp install-linux.sh install-macos.sh install-windows.ps1 README.md LICENSE dist/
(cd dist && sha256sum $name-* >SHA256SUMS)
ls -l dist
