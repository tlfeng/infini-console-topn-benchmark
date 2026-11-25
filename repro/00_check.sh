#!/bin/bash
# =============================================================================
# 00_check.sh —— 环境体检：在解包/启动前先跑，确认依赖与端口
# =============================================================================
set -uo pipefail
BASE=${BASE:-}
if [ -z "$BASE" ]; then
  for d in "$HOME/test" /home/user/test; do [ -d "$d" ] && { BASE=$d; break; }; done
fi
BASE=${BASE:-$HOME/test}
ok() { printf '\033[32m[ OK ]\033[0m %s\n' "$*"; }
no() { printf '\033[31m[FAIL]\033[0m %s\n' "$*"; }
info() { printf '\033[36m[info]\033[0m %s\n' "$*"; }

info "systemd: $(systemctl is-system-running 2>&1)"
info "cpu: $(nproc) 核   内存: $(free -g | awk '/Mem:/{print $2" G"}')   磁盘可用: $(df -h /home | awk 'NR==2{print $4}')"

jh=""
for h in "${JAVA_HOME:-}" "$HOME"/jdks/jdk-21.0.13+9 /usr/lib/jvm/java-21-openjdk-amd64; do
  [ -n "$h" ] && [ -x "$h/bin/java" ] || continue
  v=$("$h/bin/java" -version 2>&1 | head -1)
  case "$v" in *\"21*|*\"2[2-9]*|*\"3[0-9]*) jh="$h"; break ;; esac
done
[ -n "$jh" ] && ok "JDK 21+: $jh" || no "未找到 JDK 21+（Easysearch 3.0 必需）"

for f in "$BASE/easysearch-3.0.0-3073-linux-amd64.tar.gz" "$BASE/console-1.30.3-2407-linux-amd64.tar.gz"; do
  [ -f "$f" ] && ok "安装包: $f" || no "缺少安装包: $f"
done

for p in python3 curl ss systemctl; do
  command -v "$p" >/dev/null && ok "$p: $(command -v $p)" || no "缺少命令: $p"
done

info "端口占用（9000 / 9200 / 9201 / 9211-9219 / 9300-9301 / 9311-9319）"
busy=$(ss -lnt 2>/dev/null | grep -E ':(9000|9200|9201|921[0-9]|930[01]|931[0-9])\b' || true)
[ -z "$busy" ] && ok "端口空闲" || { echo "$busy"; info "如为本环境旧进程，先 bash 03_stop.sh"; }
