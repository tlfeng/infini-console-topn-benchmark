#!/bin/bash
# =============================================================================
# 02_start.sh —— 按依赖顺序拉起各实例
#
# 顺序恒定：store(9200) → biz5(9211..) → biz(9201) → console(9000)
#   Console 必须最后起：它启动时探测不到某个集群，就不会为该集群注册采集任务，
#   之后集群恢复也不会补注册，指标永久不入库（图恒空）。
#
# 用法:
#   bash 02_start.sh              # 最小可用：store + console
#   bash 02_start.sh biz          # 追加：单节点业务集群 9201（索引页签）
#   bash 02_start.sh biz5         # 追加：N 节点业务集群 9211..（节点页签）
#   bash 02_start.sh all          # 全起：store → biz5 → biz → console
#   bash 02_start.sh store console biz5   # 参数顺序无所谓，内部按依赖排序
#
# 说明：WSL 的 wsl.exe 会话退出会回收该会话下的进程，因此统一用 systemd
#       单元托管（01_setup.sh / 06_setup_biz5.sh 安装，需 root）。
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

STARTED_ES=0   # 本次是否真的拉起过某个 ES 集群（决定要不要重启 console 补注册）

# ---------- 参数归一：内部固定按 store → biz5 → biz → console 执行 ----------
want_store=0; want_biz5=0; want_biz=0; want_console=0
if [ $# -eq 0 ]; then
  want_store=1; want_console=1          # 默认最小可用
else
  for a in "$@"; do
    case "$a" in
      all) want_store=1; want_biz5=1; want_biz=1; want_console=1 ;;
      store) want_store=1 ;;
      biz5) want_biz5=1 ;;
      biz) want_biz=1 ;;
      console) want_console=1 ;;
      *) err "未知目标: $a（可选 store|biz5|biz|console|all）"; exit 1 ;;
    esac
  done
fi
# 起了任何 ES 就必须起 store 兜底（Console 与指标库都依赖它）
[ $((want_biz + want_biz5 + want_console)) -gt 0 ] && want_store=1

start_es() {
  local role=$1 port=9200 unit="topn-es-$1"
  [ "$role" = biz ] && port=9201
  if curl -s -m 2 -o /dev/null "http://127.0.0.1:$port/"; then
    log "es-$role 已在运行 (:$port)"; return 0
  fi
  log "启动 es-$role (:$port)"
  STARTED_ES=1
  if use_systemd; then
    systemctl start "$unit.service"
  else
    ( cd "$T/es-$role" && setsid ./bin/easysearch -d -p "$T/es-$role/es.pid" \
        </dev/null >"$T/logs/$role-start.out" 2>&1 )
  fi
  wait_http "http://127.0.0.1:$port/" 150 "es-$role" || {
    err "启动失败，最近日志："
    systemctl status "$unit.service" --no-pager -l 2>/dev/null | tail -20
    tail -30 "$T"/logs/$role/*.log 2>/dev/null
    return 1
  }
  # 端口刚通时分片还在恢复，此刻 health 常是 red；等它脱离 red 再打印，避免误判
  local st i
  for ((i = 1; i <= 60; i++)); do
    st=$(curl -s -m 2 "http://127.0.0.1:$port/_cluster/health?filter_path=status" 2>/dev/null \
         | python3 -c 'import json,sys;print(json.load(sys.stdin).get("status",""))' 2>/dev/null)
    [ "$st" != "red" ] && [ -n "$st" ] && break
    sleep 1
  done
  curl -s "http://127.0.0.1:$port/_cluster/health" ; echo
}

start_biz5() {
  local units n=0 up=0
  mapfile -t units < <(ls -1 /etc/systemd/system/topn-biz5-node*.service 2>/dev/null)
  if [ ${#units[@]} -eq 0 ]; then
    err "未找到 biz5 的 systemd 单元。先以 root 执行: bash 06_setup_biz5.sh 8"
    return 1
  fi
  n=${#units[@]}

  up=$(curl -s -m 2 "http://127.0.0.1:9211/_cluster/health?filter_path=number_of_nodes" 2>/dev/null \
       | python3 -c 'import json,sys;print(json.load(sys.stdin).get("number_of_nodes",0))' 2>/dev/null || echo 0)
  if [ "$up" = "$n" ]; then
    log "biz5 已在运行（$n 节点）"; return 0
  fi

  log "启动 biz5：$n 个节点（9211-$((9210 + n))）"
  STARTED_ES=1
  local u
  for u in "${units[@]}"; do systemctl start "$(basename "$u")" 2>/dev/null; done

  log "等待集群成形（最多 180s）"
  local i
  for ((i = 1; i <= 180; i++)); do
    up=$(curl -s -m 2 "http://127.0.0.1:9211/_cluster/health?filter_path=number_of_nodes" 2>/dev/null \
         | python3 -c 'import json,sys;print(json.load(sys.stdin).get("number_of_nodes",0))' 2>/dev/null || echo 0)
    [ "$up" = "$n" ] && { log "biz5 已成形：$up 节点"; break; }
    sleep 1
  done
  if [ "$up" != "$n" ]; then
    err "biz5 只起来 $up/$n 个节点，检查: journalctl -u topn-biz5-node1 -n 50"
    return 1
  fi
  curl -s "http://127.0.0.1:9211/_cat/nodes?h=name,http_address,heap.percent,cpu&s=name"
}

start_console() {
  if curl -s -m 2 -o /dev/null http://127.0.0.1:9000/; then
    log "console 已在运行 (:9000)"; return 0
  fi
  # 额外提示：已纳管但当前不可达的集群，Console 起后不会采集它
  local cid ep
  curl -s -m 3 "$S/.infini_cluster/_search?size=20&filter_path=hits.hits._source.id,hits.hits._source.endpoint,hits.hits._source.monitored" 2>/dev/null \
  | python3 -c '
import json,sys,urllib.request
try: d=json.load(sys.stdin)
except Exception: sys.exit()
for h in d.get("hits",{}).get("hits",[]):
    s=h.get("_source",{})
    if not s.get("monitored"): continue
    ep=s.get("endpoint","")
    try:
        urllib.request.urlopen(ep, timeout=2).read(1)
    except Exception:
        print("  [WARN] 集群 %s (%s) 当前不可达 —— 起 Console 后不会采集它的指标" % (s.get("id"), ep))
' 2>/dev/null || true

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
[ "$want_store" = 1 ]   && { start_es store || rc=1; }
[ "$want_biz5" = 1 ]    && { start_biz5   || rc=1; }
[ "$want_biz" = 1 ]     && { start_es biz || rc=1; }
[ "$want_console" = 1 ] && { start_console || rc=1; }

# console 已经在跑、这次又新起了集群：它启动时没看到新集群，不会注册采集任务。
# 不重启就会「端口都在、图却一直空」，所以这里自动补一次重启。
if [ "$want_console" = 0 ] && [ "$STARTED_ES" = 1 ] && curl -s -m 2 -o /dev/null http://127.0.0.1:9000/; then
  warn "console 已在运行，但本次新起了集群 —— 重启一次以确保注册采集"
  if use_systemd; then
    systemctl restart topn-console.service
    wait_http "http://127.0.0.1:9000/" 120 "console" || rc=1
  else
    warn "非 systemd 方式：请手动重启 console，否则新集群指标不入库"
  fi
fi

CONF_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
if [ "$want_console" = 1 ] && curl -s -m 2 -o /dev/null http://127.0.0.1:9000/; then
  bash "$CONF_DIR/08_prepare_system_indices.sh" || warn "系统索引映射修复失败，集群管理页可能 500"
fi

echo
log "监听端口:"
ss -lnt 2>/dev/null | grep -E ':(9000|9200|9201|921[0-9]|93[0-9][0-9])\b' | awk '{print "  " $4}' || true
echo
log "自检：bash 04_status.sh"
exit $rc
