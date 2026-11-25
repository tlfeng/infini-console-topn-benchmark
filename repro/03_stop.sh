#!/bin/bash
# =============================================================================
# 03_stop.sh —— 停止实例（逆依赖序：console → store）
#
# 用法:
#   bash 03_stop.sh                # 全停：console + store
#   bash 03_stop.sh console        # 只停某个（console|store）
#   bash 03_stop.sh --clean        # 全停 + 清空数据（保留程序与脚本）
#   bash 03_stop.sh --keep-data    # 全停，不删任何数据（默认行为）
#
# 注意：--clean 会删掉 data/store 与 console-kv（含 .infini_metrics 里注入的
#       全部数据），下次要重新 09 fill/stream 才能复跑。
# =============================================================================
set -uo pipefail
BASE=${BASE:-}
if [ -z "$BASE" ]; then
  for d in "$HOME/test" /home/user/test; do [ -d "$d" ] && { BASE=$d; break; }; done
fi
BASE=${BASE:-$HOME/test}
T=${TOPTOOLS:-$BASE/topn}
CLEAN=0
targets=()
for a in "$@"; do
  case "$a" in
    --clean) CLEAN=1 ;;
    --keep-data) CLEAN=0 ;;
    -*) echo "未知参数: $a"; exit 1 ;;
    *) targets+=("$a") ;;
  esac
done
for target in "${targets[@]}"; do
  case "$target" in console|store) ;; *) echo "未知目标: $target" >&2; exit 1 ;; esac
done
if [ "$CLEAN" = 1 ] && [ ${#targets[@]} -ne 0 ]; then
  echo "--clean 必须停止全部服务，不可与指定目标一起使用" >&2
  exit 1
fi
want_all=0
[ ${#targets[@]} -eq 0 ] && want_all=1

log() { printf '\033[36m[stop]\033[0m %s\n' "$*"; }
err() { printf '\033[31m[stop][ERROR]\033[0m %s\n' "$*" >&2; }
use_systemd() { systemctl cat topn-es-store.service >/dev/null 2>&1; }

# 同 02_start.sh：WSL 下普通用户 systemctl stop 会被 polkit 拒绝，提前拦下
if use_systemd && [ "$(id -u)" != "0" ]; then
  err "需要 root：当前用户 $(whoami) 无权操作 systemd 单元（WSL polkit 要求交互认证）"
  err "  sudo bash 03_stop.sh $*   或   wsl -u root bash 03_stop.sh $*"
  exit 1
fi

stop_unit() {  # $1=unit(不含 .service)
  if use_systemd && systemctl cat "$1.service" >/dev/null 2>&1; then
    if systemctl is-active --quiet "$1.service"; then
      log "systemctl stop $1"
      systemctl stop "$1.service"
    else
      log "$1 未在运行"
    fi
  else
    log "$1 未安装"
  fi
}

stop_store() {
  local pidf="$T/es-store/es.pid"
  stop_unit topn-es-store
  if [ -f "$pidf" ] && kill -0 "$(cat "$pidf")" 2>/dev/null; then
    log "清理残留 es-store (pid $(cat "$pidf"))"
    kill "$(cat "$pidf")" 2>/dev/null; sleep 3; kill -9 "$(cat "$pidf")" 2>/dev/null
    rm -f "$pidf"
  fi
}

stop_console() {
  if use_systemd && systemctl cat topn-console.service >/dev/null 2>&1; then
    stop_unit topn-console
  else
    pkill -f console-linux-amd64 2>/dev/null && log "已按进程名停止 console" || log "console 未在运行"
  fi
}

stop_one() {
  case "$1" in
    console) stop_console ;;
    store)   stop_store ;;
    *) err "未知目标: $1（可选 console|store）"; return 1 ;;
  esac
}

rc=0
if [ "$want_all" = 1 ]; then
  for t in console store; do stop_one "$t" || rc=1; done
else
  for order in console store; do
    for t in "${targets[@]}"; do [ "$t" = "$order" ] && { stop_one "$t" || rc=1; }; done
  done
fi

if [ "$CLEAN" = "1" ]; then
  log "清空数据目录（保留程序与脚本）"
  rm -rf "${T:?}"/data/store/* "${T:?}"/data/store/.[!.]* 2>/dev/null
  rm -rf "${T:?}"/data/console-kv/* 2>/dev/null
  rm -rf "${T:?}"/work/raw/* "${T:?}"/work/report/* 2>/dev/null
  rm -rf "${T:?}/console/data" "${T:?}/console/logs" 2>/dev/null
  log "已清空：data/store、console-kv、work/{raw,report}"
fi

echo
log "剩余监听端口:"
ss -lnt 2>/dev/null | grep -E ':(9000|9200)\b' | awk '{print "  " $4}' || echo "  无"
log "完成"
exit $rc
