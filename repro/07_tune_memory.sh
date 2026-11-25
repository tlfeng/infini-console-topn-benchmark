#!/bin/bash
# =============================================================================
# 07_tune_memory.sh —— 压缩 Easysearch 实例的内存占用（不改数据，只调 JVM/线程规模）
#
# 单机跑 10 个 ES 实例时，RSS 大头不是堆，而是：
#   · 按 CPU 核数放大的线程池 + 线程栈（16 核 → 上百线程 × 1M）
#   · 元空间 / CodeCache / 堆外直接内存（网络缓冲）
# 所以这里做四件事：压堆、限直接内存、限元空间、把 ES 认为的核数降到 2。
#
# 用法：
#   bash 07_tune_memory.sh --heap 256m            # 只改配置
#   bash 07_tune_memory.sh --heap 256m --apply    # 改配置并重启相关服务
#   bash 07_tune_memory.sh --heap 512m --only store
# =============================================================================
set -uo pipefail
T=${TOPTOOLS:-}
if [ -z "$T" ]; then
  for d in "$HOME/test/topn" /home/user/test/topn; do [ -d "$d" ] && { T=$d; break; }; done
fi
T=${T:-$HOME/test/topn}
HEAP=256m
APPLY=0
ONLY=all
while [ $# -gt 0 ]; do
  case "$1" in
    --heap) HEAP=$2; shift 2 ;;
    --apply) APPLY=1; shift ;;
    --only) ONLY=$2; shift 2 ;;
    *) echo "未知参数 $1"; exit 1 ;;
  esac
done

log() { printf '\033[36m[tune]\033[0m %s\n' "$*"; }

tune_file() {  # $1=jvm.options
  local f=$1
  [ -f "$f" ] || { log "跳过（不存在）: $f"; return; }
  python3 - "$f" "$HEAP" <<'PY'
import re, sys
path, heap = sys.argv[1], sys.argv[2]
drop = re.compile(r'^\s*(-Xms|-Xmx|-Xss|-XX:MaxDirectMemorySize|-XX:MaxMetaspaceSize|'
                  r'-XX:ActiveProcessorCount|-XX:ReservedCodeCacheSize)')
extra = [
    f"-Xms{heap}", f"-Xmx{heap}",
    "-Xss512k",
    "-XX:MaxDirectMemorySize=128m",
    "-XX:MaxMetaspaceSize=160m",
    "-XX:ReservedCodeCacheSize=64m",
    "-XX:ActiveProcessorCount=2",
]
lines = [l for l in open(path, encoding="utf-8").read().splitlines() if not drop.match(l)]
# 去掉上一次由本脚本写入的标记块
try:
    i = lines.index("# TOPN-REPRO-MEM")
    lines = lines[:i]
except ValueError:
    pass
lines += ["", "# TOPN-REPRO-MEM"] + extra
open(path, "w", encoding="utf-8").write("\n".join(lines) + "\n")
PY
  log "已调优 $f (heap=$HEAP)"
}

units=()
case "$ONLY" in
  store|all)
    tune_file "$T/es-store/config/jvm.options"; units+=(topn-es-store) ;;
esac
case "$ONLY" in
  biz|all)
    tune_file "$T/es-biz/config/jvm.options"; units+=(topn-es-biz) ;;
esac
case "$ONLY" in
  biz5|all)
    for d in "$T"/conf/biz5/node-*/; do
      n=$(basename "$d"); n=${n#node-}
      tune_file "$d/jvm.options"; units+=("topn-biz5-node$n")
    done ;;
esac

if [ "$APPLY" = "1" ]; then
  log "重启服务：${units[*]}"
  if [ "$(id -u)" != "0" ]; then
    log "需要 root 才能 restart，跳过重启"
    exit 0
  fi
  # 顺序很重要：store → 业务集群 → 最后 console。
  # 若 console 早于业务集群就绪启动，它探测不到集群，不会注册采集任务，
  # 之后即便集群 available 也不会补注册（指标就一直不进库）。
  systemctl restart topn-es-store.service 2>/dev/null
  for _ in $(seq 1 90); do curl -s -m 2 -o /dev/null http://127.0.0.1:9200/ && break; sleep 1; done

  for u in "${units[@]}"; do
    [ "$u" = "topn-es-store" ] && continue
    systemctl restart "$u.service" 2>/dev/null
  done

  # 只等本次真正重启过的集群，别为没参与的集群干等
  wait_cluster() {  # $1=endpoint $2=期望节点数 $3=名字
    local ep=$1 want=$2 nm=$3 n="" i
    for i in $(seq 1 180); do
      n=$(curl -s -m 2 "$ep/_cluster/health?filter_path=number_of_nodes" 2>/dev/null \
          | python3 -c 'import json,sys;print(json.load(sys.stdin).get("number_of_nodes",0))' 2>/dev/null || echo 0)
      [ "$n" = "$want" ] && { log "$nm 已就绪（$n 节点）"; return 0; }
      sleep 1
    done
    log "[warn] $nm 未在 180s 内达到 $want 节点（当前 ${n:-0}）"
  }

  has_biz=0; biz5_n=0
  for u in "${units[@]}"; do
    [ "$u" = "topn-es-biz" ] && has_biz=1
    case "$u" in topn-biz5-node*) biz5_n=$((biz5_n + 1)) ;; esac
  done
  [ "$has_biz" = 1 ] && wait_cluster "http://127.0.0.1:9201" 1 "topn-biz"
  [ "$biz5_n" -gt 0 ] && wait_cluster "http://127.0.0.1:9211" "$biz5_n" "topn-biz5"
  sleep 10

  log "重启 console（此时各集群已就绪，才能注册采集任务）"
  systemctl restart topn-console.service 2>/dev/null
  for _ in $(seq 1 90); do curl -s -m 2 -o /dev/null http://127.0.0.1:9000/health && break; sleep 1; done
fi

log "当前内存"
free -m | head -2
