#!/bin/bash
# =============================================================================
# 04_status.sh —— 环境自检：端口、集群、Console 状态、指标入库、最近评测
# =============================================================================
set -uo pipefail
T=${TOPTOOLS:-}
if [ -z "$T" ]; then
  for d in "$HOME/test/topn" /home/user/test/topn; do [ -d "$d" ] && { T=$d; break; }; done
fi
T=${T:-$HOME/test/topn}
R=${REPRO_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}
S=http://127.0.0.1:9200
C=http://127.0.0.1:9000

hr() { printf '\033[36m%s\033[0m\n' "──── $* ────"; }

hr "进程 / 端口"
ss -lnt 2>/dev/null | grep -E ':(9000|9200)\b' | awk '{print "  " $4}' || echo "无监听"
for u in topn-es-store topn-console; do
  printf '%-20s %s\n' "$u" "$(systemctl is-active $u.service 2>/dev/null || echo n/a)"
done

hr "存储集群 9200（Console 指标库）"
curl -s -m 3 "$S/_cluster/health?pretty" 2>/dev/null | head -8 || echo "不可达"

hr "内存"
free -m | head -2
ps -eo rss,comm 2>/dev/null | grep java | sort -nr | head -3 | awk '{printf "  最大实例 RSS: %d MB\n", $1/1024}'

hr "Console"
curl -s -m 3 "$C/health" || echo "不可达"
echo
echo "已纳管集群:"
curl -s -m 3 "$S/.infini_cluster/_search?filter_path=hits.hits._source.name,hits.hits._source.id,hits.hits._source.monitored" 2>/dev/null | head -c 600
echo

hr "集群管理页接口"
CU=${CONSOLE_USER:-admin}
CP=${CONSOLE_PASS:-Admin@12345}
tok=$(curl -s -m 5 -X POST "$C/account/login" -H 'Content-Type: application/json' \
  -d "{\"username\":\"$CU\",\"password\":\"$CP\"}" 2>/dev/null \
  | python3 -c 'import json,sys;print(json.load(sys.stdin).get("access_token",""))' 2>/dev/null)
if [ -n "$tok" ]; then
  code=$(curl -s -m 5 -o /tmp/console_clusters.json -w '%{http_code}' \
    -H "Authorization: Bearer $tok" "$C/elasticsearch/_search")
  echo "  /elasticsearch/_search HTTP $code"
  [ "$code" = "200" ] && python3 -c 'import json;d=json.load(open("/tmp/console_clusters.json"));print("  返回集群数:", (d.get("hits",{}).get("total",{}) or {}).get("value"))' 2>/dev/null
else
  echo "  登录失败（尚未初始化）"
fi
echo "  .infini_cluster.labels.health_status: $(curl -s -m 3 "$S/.infini_cluster/_mapping" \
  | python3 -c 'import json,sys;d=json.load(sys.stdin);m=next(iter(d.values()))["mappings"];p=m.get("properties",{}).get("labels",{}).get("properties",{}).get("health_status",{});print(p.get("type","?")+(" fielddata" if p.get("fielddata") else ""))' 2>/dev/null)"

hr "指标入库（注入数据分布）"
curl -s -m 5 -H 'Content-Type: application/json' "$S/.infini_metrics/_search" \
  -d '{"size":0,"query":{"bool":{"should":[{"term":{"metadata.name":"index_stats"}},{"term":{"metadata.name":"node_stats"}}]}},"aggs":{"c":{"terms":{"field":"metadata.labels.cluster_id","size":5},"aggs":{"n":{"terms":{"field":"metadata.name","size":3}}}}}}' 2>/dev/null | head -c 600
echo
echo "metrics 索引:"
curl -s -m 3 "$S/_cat/indices/.infini_metrics*?h=index,docs.count,store.size" 2>/dev/null

hr "最近一次评测报告"
ls -t "$R"/work/report/topn_view_*.md 2>/dev/null | head -1 | while read -r f; do
  echo "$f"; sed -n '1,12p' "$f"
done
