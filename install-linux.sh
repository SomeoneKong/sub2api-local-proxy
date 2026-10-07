#!/usr/bin/env bash
# 在 Linux（systemd）上安装为当前用户的 systemd 用户服务，并开启 linger，使它开机即启动、不必登录；
# 崩溃后自动拉起。程序、配置、目录缓存都在 ~/.sub2api-local-proxy/。
# 重复执行即升级（换程序、重启服务，配置不动）。只有首次开启 linger 可能要 sudo，之后都不需要 root。
# 新程序先暂存并用它校验配置，通过了才替换、重启；校验不过时正在运行的服务不受影响。
#
# 用法（以要运行服务的用户执行，不要加 sudo）：
#   ./install-linux.sh --upstream http://sub2api.example.com:8080 [--binary 程序路径]
#   ./install-linux.sh --uninstall      # 停止并删除服务，保留 ~/.sub2api-local-proxy/ 与 linger 设置
set -euo pipefail

name=sub2api-local-proxy
data="$HOME/.sub2api-local-proxy"
unit_dir="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
unit="$unit_dir/$name.service"
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

if [ "$(id -u)" = 0 ]; then
  echo "请以要运行服务的普通用户执行（不要用 root 或 sudo）" >&2
  exit 1
fi

if [ "$uninstall" = 1 ]; then
  systemctl --user disable --now "$name" 2>/dev/null || true
  rm -f "$unit"
  systemctl --user daemon-reload
  echo "已删除服务；配置与缓存保留在 $data，linger 设置未改动"
  exit 0
fi

if [ -z "$binary" ]; then
  case "$(uname -m)" in
    x86_64) arch=amd64 ;;
    aarch64|arm64) arch=arm64 ;;
    *) echo "不支持的架构 $(uname -m)，用 --binary 指定程序" >&2; exit 1 ;;
  esac
  binary="$(cd "$(dirname "$0")" && pwd)/$name-linux-$arch"
fi
[ -f "$binary" ] || { echo "找不到程序 $binary" >&2; exit 1; }
if [ ! -f "$data/config.json" ] && [ -z "$upstream" ]; then
  echo "首次安装要用 --upstream 指定网关地址" >&2
  exit 1
fi

mkdir -p "$data/bin" "$unit_dir"
staged="$data/bin/$name.new"
install -m 755 "$binary" "$staged"
if ! "$staged" -init -config "$data/config.json" -upstream "$upstream"; then
  rm -f "$staged"
  echo "新程序不能运行或配置有误，未改动正在运行的服务" >&2
  exit 1
fi

# linger：用户的 systemd 实例开机即启动，用户服务不必等登录。
if [ "$(loginctl show-user "$USER" -p Linger --value 2>/dev/null)" != yes ]; then
  loginctl enable-linger "$USER" 2>/dev/null || sudo loginctl enable-linger "$USER"
fi

mv -f "$staged" "$data/bin/$name" # 改名是原子的，正在运行的旧进程不受影响

cat >"$unit" <<'EOF'
[Unit]
Description=sub2api local proxy (codex -> sub2api)

[Service]
ExecStart="%h/.sub2api-local-proxy/bin/sub2api-local-proxy" -config "%h/.sub2api-local-proxy/config.json"
Restart=always
RestartSec=3

[Install]
WantedBy=default.target
EOF
systemctl --user daemon-reload
systemctl --user enable "$name"
systemctl --user restart "$name"

# 程序启动后若立刻退出（比如端口被占），systemd 会进入等待重启的状态，2 秒后看状态即可发现。
sleep 2
if [ "$(systemctl --user is-active "$name")" != active ]; then
  journalctl --user -u "$name" -n 20 --no-pager >&2 || true
  echo "服务没有起来，见上面的日志" >&2
  exit 1
fi
systemctl --user --no-pager --lines=3 status "$name" || true
echo "日志: journalctl --user -u $name -f；改 $data/config.json 后自动生效，不用重启服务"
