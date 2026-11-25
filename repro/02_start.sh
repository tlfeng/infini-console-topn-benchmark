#!/bin/bash
# =============================================================================
# 02_start.sh —— 按依赖顺序拉起 store(9200) → console(9000)
#
#   被监控的业务集群不再真实搭建：指标由 09_metrics_feeder.py 直接注入
#   .infini_metrics（离线填数 / 在线数据流），Console 只消费 store 里的数据。
#
# 用法:
#   bash 02_start.sh              # 全起（默认：store + console）
#   bash 02_start.sh store        # 只起 store
#   bash 02_start.sh console      # 只起 console（需 store 已在跑）
#
# 说明：WSL 的 wsl.exe 会话退出会回收该会话下的进程，因此统一用 systemd
#       单元托管（01_setup.sh 安装，需 root）。
# =============================================================================
set -uo pipefail

BASE=${BASE:-}
if [ -z "$BASE" ]; then
  for d in "$HOME/test" /home/user/test; do [ -d "$d" ] && { BASE=$d; break; }; done
fi
BASE=${BASE:-$HOME/test}
T=${TOPTOOLS:-$BASE/topn}
JAVA_HOME=$(cat "$T/java_home" 2>/dev/null || echo "")
[ -x "${JAVA_HOME:-}/bin/java" ] || JAVA_HOME=$(ls -d "$HOME"/jdks/jdk-21* /home/user/jdks/jdk-21* /usr/lib/jvm/java-21* 2>/dev/null | head -1)
export JAVA_HOME ES_JAVA_HOME="$JAVA_HOME"
S=http://127.0.0.1:9200

log() { printf '\033[36m[start]\033[0m %s\n' "$*"; }
warn() { printf '\033[33m[start][WARN]\033[0m %s\n' "$*"; }
err() { printf '\033[31m[start][ERROR]\033[0m %s\n' "$*" >&2; }

wait_http() {  # $1=url $2=秒 $3=名字
  local url=$1 secs=${2:-120} name=$3 i
  for ((i = 1; i <= secs; i++)); do
    if curl -s -m 2 -o /dev/null "$url"; then log "$name 就绪 (${i}s)"; return 0; fi
    sleep 1
  done
  err "$name 在 ${secs}s 内未就绪: $url"
  return 1
}

use_systemd() { systemctl cat topn-es-store.service >/dev/null 2>&1; }

# WSL 的 polkit 要求交互认证，普通用户 systemctl start 会得到
# "Access denied as the requested operation requires interactive authentication"，
# 且要等满超时才报错。这里提前拦下。
if use_systemd && [ "$(id -u)" != "0" ]; then
  err "需要 root：当前用户 $(whoami) 无权操作 systemd 单元（WSL polkit 要求交互认证）"
  err "  sudo bash 02_start.sh $*   或   wsl -u root bash 02_start.sh $*"
  exit 1
fi

# ---------- 参数归一：固定 store → console ----------
want_store=0; want_console=0
if [ $# -eq 0 ]; then
  want_store=1; want_console=1
else
  for a in "$@"; do
    case "$a" in
      all) want_store=1; want_console=1 ;;
      store) want_store=1 ;;
      console) want_console=1 ;;
      *) err "未知目标: $a（可选 store|console|all）"; exit 1 ;;
    esac
  done
fi
[ "$want_console" = 1 ] && want_store=1

start_store() {
  if curl -s -m 2 -o /dev/null "$S/"; then
    log "es-store 已在运行 (:9200)"; return 0
  fi
  log "启动 es-store (:9200)"
  if use_systemd; then
    systemctl start topn-es-store.service
  else
    ( cd "$T/es-store" && setsid ./bin/easysearch -d -p "$T/es-store/es.pid" \
        </dev/null >"$T/logs/store-start.out" 2>&1 )
  fi
  wait_http "$S/" 150 "es-store" || {
    err "启动失败，最近日志："
    systemctl status topn-es-store.service --no-pager -l 2>/dev/null | tail -20
    tail -30 "$T"/logs/store/*.log 2>/dev/null
    return 1
  }
  # 端口刚通时分片还在恢复，此刻 health 常是 red；等它脱离 red 再打印，避免误判
  local st i
  for ((i = 1; i <= 60; i++)); do
    st=$(curl -s -m 2 "$S/_cluster/health?filter_path=status" 2>/dev/null \
         | python3 -c 'import json,sys;print(json.load(sys.stdin).get("status",""))' 2>/dev/null)
    [ "$st" != "red" ] && [ -n "$st" ] && break
    sleep 1
  done
  curl -s "$S/_cluster/health" ; echo
}

start_console() {
  if curl -s -m 2 -o /dev/null http://127.0.0.1:9000/; then
    log "console 已在运行 (:9000)"; return 0
  fi
  log "启动 console (:9000)"
  if use_systemd; then
    systemctl start topn-console.service
  else
    ( cd "$T/console" \
      && INFINI_CONSOLE_ENDPOINT="http://127.0.0.1:9000" \
         INGEST_CLUSTER_ENDPOINT="http://127.0.0.1:9200" \
         setsid nohup ./console-linux-amd64 </dev/null \
           > "$T/logs/console/console.out" 2>&1 &
      echo $! > "$T/console/console.pid" )
  fi
  wait_http "http://127.0.0.1:9000/" 120 "console" || {
    err "console 启动失败："
    tail -40 "$T/logs/console/console.out"
    return 1
  }
}

rc=0
if [ "$want_store" = 1 ]; then
  start_store || rc=1
fi
if [ "$want_console" = 1 ] && [ "$rc" = 0 ]; then
  start_console || rc=1
fi

CONF_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
if [ "$want_console" = 1 ] && curl -s -m 2 -o /dev/null http://127.0.0.1:9000/; then
  bash "$CONF_DIR/08_prepare_system_indices.sh" || warn "系统索引映射修复失败，集群管理页可能 500"
fi

echo
log "监听端口:"
ss -lnt 2>/dev/null | grep -E ':(9000|9200|93[0-9][0-9])\b' | awk '{print "  " $4}' || true
echo
log "自检：bash 04_status.sh"
echo "填数：python3 09_metrics_feeder.py fill --side both   /   stream --side both --interval 60"
exit $rc
