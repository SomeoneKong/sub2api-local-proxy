#!/usr/bin/env bash
# 在 macOS 上安装为当前用户的 launchd 代理（登录后自动启动、退出后自动拉起）；
# 程序、配置、目录缓存与日志都在 ~/.sub2api-local-proxy/。重复执行即升级（配置不动）。
# 新程序先暂存并用它校验配置、plist 先过 plutil 检查，都通过了才停旧代理、替换、启动。
#
# 用法：
#   ./install-macos.sh --upstream http://sub2api.example.com:8080 [--binary 程序路径]
#   ./install-macos.sh --uninstall      # 停止并删除 launchd 代理，保留 ~/.sub2api-local-proxy/
set -euo pipefail

name=sub2api-local-proxy
label=local.$name
data="$HOME/.sub2api-local-proxy"
plist="$HOME/Library/LaunchAgents/$label.plist"
domain="gui/$(id -u)"
upstream=""
binary=""
uninstall=0

while [ $# -gt 0 ]; do
  case "$1" in
    --upstream) upstream="$2"; shift 2 ;;
    --binary) binary="$2"; shift 2 ;;
    --uninstall) uninstall=1; shift ;;
    *) echo "未知参数: $1" >&2; exit 1 ;;
  esac
done

if [ "$uninstall" = 1 ]; then
  launchctl bootout "$domain/$label" 2>/dev/null || true
  rm -f "$plist"
  echo "已删除 launchd 代理；配置、缓存与日志保留在 $data"
  exit 0
fi

if [ -z "$binary" ]; then
  case "$(uname -m)" in
    x86_64) arch=amd64 ;;
    arm64) arch=arm64 ;;
    *) echo "不支持的架构 $(uname -m)，用 --binary 指定程序" >&2; exit 1 ;;
  esac
  binary="$(cd "$(dirname "$0")" && pwd)/$name-darwin-$arch"
fi
[ -f "$binary" ] || { echo "找不到程序 $binary" >&2; exit 1; }
if [ ! -f "$data/config.json" ] && [ -z "$upstream" ]; then
  echo "首次安装要用 --upstream 指定网关地址" >&2
  exit 1
fi

mkdir -p "$data/bin" "$HOME/Library/LaunchAgents"
staged="$data/bin/$name.new"
install -m 755 "$binary" "$staged"
xattr -d com.apple.quarantine "$staged" 2>/dev/null || true
if ! "$staged" -init -config "$data/config.json" -upstream "$upstream" \
  -log-file "~/.sub2api-local-proxy/proxy.log"; then
  rm -f "$staged"
  echo "新程序不能运行或配置有误，未改动正在运行的代理" >&2
  exit 1
fi

xml() { printf '%s' "$1" | sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g'; }
new_plist="$plist.new"
cat >"$new_plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$label</string>
  <key>ProgramArguments</key>
  <array>
    <string>$(xml "$data/bin/$name")</string>
    <string>-config</string>
    <string>$(xml "$data/config.json")</string>
  </array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardErrorPath</key><string>$(xml "$data/launchd.err")</string>
</dict>
</plist>
EOF
if ! plutil -lint "$new_plist" >/dev/null; then
  rm -f "$staged" "$new_plist"
  echo "生成的 plist 不合法，未改动正在运行的代理" >&2
  exit 1
fi

launchctl bootout "$domain/$label" 2>/dev/null || true
mv -f "$staged" "$data/bin/$name"
mv -f "$new_plist" "$plist"
launchctl bootstrap "$domain" "$plist"

# 程序启动后若立刻退出（比如端口被占），launchd 不会显示 running。
sleep 2
if ! launchctl print "$domain/$label" | grep -q "state = running"; then
  tail -n 10 "$data/proxy.log" "$data/launchd.err" 2>/dev/null >&2 || true
  echo "代理没有起来，见上面的日志" >&2
  exit 1
fi
echo "已启动。日志: $data/proxy.log；改 $data/config.json 后自动生效"
