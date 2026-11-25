#!/bin/bash
# =============================================================================
# 05_prepare_store.sh —— 预建 Console 指标库的索引模板与 .infini_metrics
#
# 为什么必须预建：
#   Console 写入的指标文档里 metadata.name / metadata.labels.index_name 等字段，
#   在 TopN 聚合里是直接按原始路径做 terms 聚合的（不带 .keyword）。
#   如果让 ES 动态映射，这些字段会变成 text（只有 .keyword 子字段），
#   聚合直接报 "Text fields are not optimised for ... aggregations"，
#   所有 TopN 图恒为空——评测就失去意义。
#
# 用法: bash 05_prepare_store.sh [--rebuild]
# =============================================================================
set -uo pipefail
STORE=${STORE:-http://127.0.0.1:9200}
REBUILD=0
[ "${1:-}" = "--rebuild" ] && REBUILD=1

log() { printf '\033[36m[store]\033[0m %s\n' "$*"; }
die() { printf '\033[31m[store][ERROR]\033[0m %s\n' "$*" >&2; exit 1; }

curl -s -m 3 -o /dev/null "$STORE/" || die "存储集群不可达: $STORE"

log "安装模板 .infini_metrics"
read -r -d '' TPL <<'JSON'
{
  "order": 100,
  "index_patterns": [".infini_metrics*"],
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "refresh_interval": "5s",
    "index.mapping.total_fields.limit": 20000
  },
  "mappings": {
    "dynamic": true,
    "dynamic_templates": [
      {
        "strings_as_keyword": {
          "match_mapping_type": "string",
          "mapping": {"type": "keyword", "ignore_above": 512}
        }
      }
    ],
    "properties": {
      "timestamp": {"type": "date"},
      "metadata": {
        "properties": {
          "name": {"type": "keyword"},
          "category": {"type": "keyword"},
          "datatype": {"type": "keyword"},
          "labels": {
            "dynamic": true,
            "properties": {
              "cluster_id": {"type": "keyword"},
              "cluster_uuid": {"type": "keyword"},
              "index_name": {"type": "keyword"},
              "index_uuid": {"type": "keyword"},
              "index_id": {"type": "keyword"},
              "node_id": {"type": "keyword"},
              "shard_id": {"type": "keyword"}
            }
          }
        }
      },
      "payload": {"type": "object", "dynamic": true}
    }
  }
}
JSON

code=$(curl -s -o /tmp/tpl.out -w '%{http_code}' -XPUT "$STORE/_template/.infini_metrics" \
  -H 'Content-Type: application/json' -d "$TPL")
[ "$code" = "200" ] && log "模板已安装" || { cat /tmp/tpl.out; die "模板安装失败($code)"; }

state=$(curl -s "$STORE/.infini_metrics" | python3 -c '
import json,sys
try:
    m=json.load(sys.stdin)
    if "error" in m: print("missing"); raise SystemExit
    p=list(m.values())[0]["mappings"]["properties"]
    t=p.get("metadata",{}).get("properties",{}).get("labels",{}).get("properties",{}).get("index_name",{})
    print(t.get("type","unknown"))
except Exception:
    print("missing")
' 2>/dev/null)

log "当前 .infini_metrics 映射状态: $state"
if [ "$state" = "missing" ]; then
  log "创建 .infini_metrics（显式 keyword 映射）"
  read -r -d '' BODY <<'JSON'
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "refresh_interval": "5s",
    "index.mapping.total_fields.limit": 20000
  },
  "mappings": {
    "dynamic": true,
    "dynamic_templates": [
      {"strings_as_keyword": {"match_mapping_type": "string",
        "mapping": {"type": "keyword", "ignore_above": 512}}}
    ],
    "properties": {
      "timestamp": {"type": "date"},
      "metadata": {"properties": {
        "name": {"type": "keyword"},
        "category": {"type": "keyword"},
        "datatype": {"type": "keyword"},
        "labels": {"dynamic": true, "properties": {
          "cluster_id": {"type": "keyword"},
          "cluster_uuid": {"type": "keyword"},
          "index_name": {"type": "keyword"},
          "index_uuid": {"type": "keyword"},
          "index_id": {"type": "keyword"},
          "node_id": {"type": "keyword"},
          "shard_id": {"type": "keyword"}
        }}
      }},
      "payload": {"type": "object", "dynamic": true}
    }
  }
}
JSON
  code=$(curl -s -o /tmp/idx.out -w '%{http_code}' -XPUT "$STORE/.infini_metrics" \
    -H 'Content-Type: application/json' -d "$BODY")
  [ "$code" = "200" ] && log ".infini_metrics 已创建" || { cat /tmp/idx.out; die "创建失败($code)"; }
elif [ "$state" != "keyword" ]; then
  if [ "$REBUILD" = "1" ]; then
    log "映射类型为 $state（非 keyword），按 --rebuild 删除重建"
    curl -s -XDELETE "$STORE/.infini_metrics" >/dev/null
    exec bash "$0"
  else
    log "警告：index_name 映射为 $state，TopN 聚合会失败。请加 --rebuild 重建（会清空已采集指标）"
  fi
else
  log "映射正确（keyword），跳过"
fi

# 字段上限兜底（历史上被动态映射成 text 的索引可现场修，不用重建）
curl -s -XPUT "$STORE/.infini_metrics/_settings" -H 'Content-Type: application/json' \
  -d '{"index.mapping.total_fields.limit":20000}' >/dev/null

log "完成"
