#!/bin/bash
# =============================================================================
# 40_run_all.sh —— 索引页签一轮评测：造数 → 压测+采样 → 比对
#
#   bash 40_run_all.sh                  # 默认 300s 压测
#   bash 40_run_all.sh --duration 600   # 压 10 分钟
#   bash 40_run_all.sh --skip-seed      # 复用已有数据
#   bash 40_run_all.sh --fresh          # 清库重来（会删 biz 数据与已采指标）
#   bash 40_run_all.sh --freeze         # 按画像冻结只读（写画像仍可写）
#
# 前置：02_start.sh biz 已起（store+biz+console），且指标在入库（本脚本会自检）
# 产物：work/raw/stats_*.jsonl、work/report/topn_report_*.{md,json}
# =============================================================================
set -uo pipefail
D=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$D" || exit 1
WORK="$D/work"
STORE=http://127.0.0.1:9200
BIZ=http://127.0.0.1:9201

DURATION=300
SCALE=1.0
SKIP_SEED=0
FRESH=0
WARMUP=60
FREEZE=0
while [ $# -gt 0 ]; do
  case "$1" in
    --duration) DURATION=$2; shift 2 ;;
    --scale) SCALE=$2; shift 2 ;;
    --skip-seed) SKIP_SEED=1; shift ;;
    --fresh) FRESH=1; shift ;;
    --warmup) WARMUP=$2; shift 2 ;;
    --freeze) FREEZE=1; shift ;;
    *) echo "未知参数: $1"; exit 1 ;;
  esac
done

log() { printf '\n\033[36m[run]\033[0m %s\n' "$*"; }
die() { printf '\033[31m[run][ERROR]\033[0m %s\n' "$*" >&2; exit 1; }

mkdir -p "$WORK/raw" "$WORK/report"

# 中途失败 / Ctrl-C 也要把索引恢复可写，否则下次造数会写不进去
FROZEN=0
restore_rw() {
  [ "$FROZEN" = "1" ] || return 0
  log "恢复可写（blocks.write=false, refresh_interval=1s）"
  python3 "$D/15_set_readonly.py" --endpoint "$BIZ" --off >/dev/null 2>&1
  FROZEN=0
}
trap 'restore_rw; exit 130' INT TERM
trap 'restore_rw' EXIT

if [ "$FRESH" = "1" ]; then
  log "清空旧数据"
  curl -s -XDELETE "$BIZ/prod_*,.kibana_task_manager_1?ignore_unavailable=true" >/dev/null
  curl -s -XDELETE "$STORE/.infini_metrics?ignore_unavailable=true" >/dev/null
  # 只清本脚本产的采样，保留 stats_view_*.jsonl（32/33 边界验证的输入）
  find "$WORK/raw" -maxdepth 1 -name 'stats_*.jsonl' ! -name 'stats_view_*' -delete 2>/dev/null || true
  bash "$D/05_prepare_store.sh" || die "指标库准备失败"
fi

log "1/5 检查服务"
for u in "$STORE" "$BIZ" http://127.0.0.1:9000/health; do
  curl -s -m 3 -o /dev/null "$u" || die "$u 不可达。先执行: bash 02_start.sh biz"
done
echo "服务正常"

# Console 早于集群启动就不会注册采集任务：用"指标是否还在更新"间接判断
age=$(curl -s -m 5 -H 'Content-Type: application/json' "$STORE/.infini_metrics/_search" \
  -d '{"size":0,"query":{"bool":{"filter":[{"term":{"metadata.name":"index_stats"}},{"term":{"metadata.labels.cluster_id":"topn-biz"}}]}},"aggs":{"mx":{"max":{"field":"timestamp"}}}}' \
  2>/dev/null | python3 -c 'import json,sys,time
try:
    d=json.load(sys.stdin); v=(d.get("aggregations",{}).get("mx",{}) or {}).get("value") or 0
    print(int(time.time()*1000)-int(v) if v else -1)
except Exception: print(-1)' 2>/dev/null)
age=${age:--1}
if [ "$age" -lt 0 ]; then
  die "metrics 里没有 topn-biz 的 index_stats，采集未注册。按顺序重启: bash 03_stop.sh && bash 02_start.sh store biz console"
elif [ "$age" -gt 120000 ]; then
  die "topn-biz 的指标已 $((age/1000))s 没更新，采集任务可能没注册：按顺序重启 store → biz → console"
fi
echo "采集正常（最新 index_stats 距今 $((age/1000))s）"

log "2/5 校验指标库映射"
bash "$D/05_prepare_store.sh"

log "3/5 Console 初始化与纳管"
python3 "$D/20_console_init.py" || die "Console 初始化失败"
bash "$D/08_prepare_system_indices.sh" || die "系统索引映射补齐失败（集群列表会 500）"

if [ "$SKIP_SEED" = "0" ]; then
  log "4/5 造数（scale=$SCALE）"
  python3 "$D/10_seed.py" --scale "$SCALE" || die "造数失败"
else
  log "4/5 跳过造数"
fi

log "5/5 压测 ${DURATION}s + 采样，然后比对"
if [ "$FREEZE" = "1" ]; then
  log "按需只读：写画像索引保持可写，其余冻结（blocks.write + refresh_interval=-1）"
  python3 "$D/15_set_readonly.py" --endpoint "$BIZ" --on | tail -18 || die "冻结失败"
  FROZEN=1
fi
python3 "$D/12_sample.py" --duration $((DURATION + 90)) \
  --out "$WORK/raw/stats_$(date +%Y%m%d_%H%M%S).jsonl" > "$WORK/raw/sampler.log" 2>&1 &
SPID=$!
sleep 3
python3 "$D/11_load.py" --duration "$DURATION" 2>&1 | tail -3
log "等待指标落库（30s）"
sleep 30
kill $SPID 2>/dev/null
wait $SPID 2>/dev/null

restore_rw
python3 "$D/30_verify.py" --warmup "$WARMUP" | tail -40

LATEST=$(ls -t "$WORK"/report/topn_report_*.md 2>/dev/null | head -1)
log "完成。报告: ${LATEST:-未生成}"
