#!/bin/bash
# =============================================================================
# 41_run_node.sh —— 节点页签一轮评测：采样 + 压测 + 比对
#
#   bash 41_run_node.sh --duration 200
#   bash 41_run_node.sh --duration 200 --freeze       # 按画像冻结（写画像仍可写）
#   bash 41_run_node.sh --duration 200 --all-readonly # 全冻结且不跑写流量（最省）
#
# 前置：02_start.sh biz5 已起（store+biz5+console），biz5 已被 Console 纳管并采集
# 产物：work/raw/nodes_*.jsonl、work/report/node_topn_*.{md,json}
# =============================================================================
set -uo pipefail
D=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$D" || exit 1
WORK="$D/work"
STORE=http://127.0.0.1:9200

DUR=200
CL=topn-biz5
EP=http://127.0.0.1:9211
WARM=60
FREEZE=0
ALL_RO=0
while [ $# -gt 0 ]; do
  case "$1" in
    --duration) DUR=$2; shift 2 ;;
    --cluster) CL=$2; shift 2 ;;
    --endpoint) EP=$2; shift 2 ;;
    --warmup) WARM=$2; shift 2 ;;
    --freeze) FREEZE=1; shift ;;
    --all-readonly) ALL_RO=1; FREEZE=1; shift ;;
    *) echo "未知参数 $1"; exit 1 ;;
  esac
done

mkdir -p "$WORK/raw" "$WORK/report"
# 只清本脚本产的节点采样，保留 nodes_view_*.jsonl（32/33 边界验证的输入）
find "$WORK/raw" -maxdepth 1 -name 'nodes_*.jsonl' ! -name 'nodes_view_*' -delete 2>/dev/null || true

# 中途失败 / Ctrl-C 也要恢复可写
FROZEN=0
restore_rw() {
  [ "$FROZEN" = "1" ] || return 0
  echo "[node] 恢复可写"
  python3 "$D/15_set_readonly.py" --endpoint "$EP" --off >/dev/null 2>&1
  FROZEN=0
}
trap 'restore_rw; exit 130' INT TERM
trap 'restore_rw' EXIT

# ---------- 前置检查：集群在跑、节点数够、Console 在采集 ----------
if ! curl -s -m 3 -o /dev/null "$EP/"; then
  echo "[node] $EP 不可达。先执行: bash 02_start.sh biz5" >&2
  echo "[node] （首次搭建需 root: bash 06_setup_biz5.sh 8，再 bash 02_start.sh biz5）" >&2
  exit 1
fi
want_nodes=$(ls -1 /etc/systemd/system/topn-biz5-node*.service 2>/dev/null | wc -l)
have_nodes=$(curl -s -m 3 "$EP/_cluster/health?filter_path=number_of_nodes" 2>/dev/null \
  | python3 -c 'import json,sys;print(json.load(sys.stdin).get("number_of_nodes",0))' 2>/dev/null || echo 0)
if [ "${want_nodes:-0}" -gt 0 ] && [ "$have_nodes" != "$want_nodes" ]; then
  echo "[node] 集群只有 $have_nodes/$want_nodes 个节点，TopN 结论会不准。先 bash 02_start.sh biz5" >&2
  exit 1
fi
age=$(curl -s -m 5 -H 'Content-Type: application/json' "$STORE/.infini_metrics/_search" \
  -d "{\"size\":0,\"query\":{\"bool\":{\"filter\":[{\"term\":{\"metadata.name\":\"node_stats\"}},{\"term\":{\"metadata.labels.cluster_id\":\"$CL\"}}]}},\"aggs\":{\"mx\":{\"max\":{\"field\":\"timestamp\"}}}}" \
  2>/dev/null | python3 -c 'import json,sys,time
try:
    d=json.load(sys.stdin); v=(d.get("aggregations",{}).get("mx",{}) or {}).get("value") or 0
    print(int(time.time()*1000)-int(v) if v else -1)
except Exception: print(-1)' 2>/dev/null)
age=${age:--1}
if [ "$age" -lt 0 ]; then
  echo "[node] metrics 里没有 $CL 的 node_stats：Console 未采集该集群。" >&2
  echo "[node] 按顺序重启 store → biz5 → console: bash 03_stop.sh && bash 02_start.sh store biz5 console" >&2
  exit 1
elif [ "$age" -gt 180000 ]; then
  echo "[node] $CL 的 node_stats 已 $((age/1000))s 未更新，采集可能中断（Console 早于集群启动）。" >&2
  echo "[node] 处理: bash 03_stop.sh && bash 02_start.sh store biz5 console" >&2
  exit 1
fi

bash "$D/08_prepare_system_indices.sh" || { echo "[node] 系统索引映射补齐失败（集群列表会 500）" >&2; exit 1; }

LOAD_ARGS=(--endpoint "$EP" --duration "$DUR")
if [ "$ALL_RO" = "1" ]; then
  echo "[node] 全只读：所有索引冻结，且不跑写流量（最省内存，写 QPS 不参与候选）"
  python3 "$D/15_set_readonly.py" --endpoint "$EP" --on --writable "" | tail -20
  LOAD_ARGS+=(--no-write)
elif [ "$FREEZE" = "1" ]; then
  echo "[node] 按需只读：写画像保持可写，其余索引冻结"
  python3 "$D/15_set_readonly.py" --endpoint "$EP" --on | tail -20
fi
[ "$FREEZE" = "1" ] && FROZEN=1

STAMP=$(date +%Y%m%d_%H%M%S)
echo "[node] 采样 + 压测 ${DUR}s（集群 $CL / $EP）"
python3 "$D/14_sample_nodes.py" --endpoint "$EP" --duration $((DUR + 60)) \
  --out "$WORK/raw/nodes_$STAMP.jsonl" > "$WORK/raw/sampler_nodes.log" 2>&1 &
SPID=$!
sleep 3
python3 "$D/11_load.py" "${LOAD_ARGS[@]}" 2>&1 | tail -3
echo "[node] 等指标落库 30s"
sleep 30
kill $SPID 2>/dev/null
wait $SPID 2>/dev/null

restore_rw
python3 "$D/31_verify_node.py" --cluster "$CL" --warmup "$WARM"

LATEST=$(ls -t "$WORK"/report/node_topn_*.md 2>/dev/null | head -1)
echo "[node] 完成。报告: ${LATEST:-未生成}"
