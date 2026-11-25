#!/bin/bash
# =============================================================================
# 08_prepare_system_indices.sh —— 补齐 Console 系统索引的 fielddata 聚合映射
#
# 背景：
#   旧环境下 .infini_cluster / .infini_activities 可能早于 .infini 模板创建，
#   其中的 labels.health_status、metadata.labels.cluster_name 等被动态映射成
#   text。Console 集群管理页对这些字段做 terms 聚合，会直接 400/500，页面
#   显示不出已纳管集群。
#
# 处理：
#   - 确保 .infini 模板存在（字符串 -> keyword，供新建索引使用）
#   - 自动扫描 .infini_cluster / .infini_activities 的全部 text 字段，逐个补
#     fielddata=true（不删数据、不重建索引）。 historically 按已知字段逐个补
#     （health_status → distribution → version → tags ...），页面每换一个聚合
#     维度就冒一个新字段，所以改为全量扫描一次性补齐；索引文档极少，开销可忽略。
#   - 如果 Console 可登录，再调用 /elasticsearch/_search 验证集群列表
#
# 用法：
#   bash 08_prepare_system_indices.sh
#   STORE=http://127.0.0.1:9200 CONSOLE=http://127.0.0.1:9000 bash 08_prepare_system_indices.sh
# =============================================================================
set -uo pipefail
STORE=${STORE:-http://127.0.0.1:9200}
CONSOLE=${CONSOLE:-http://127.0.0.1:9000}
CONSOLE_USER=${CONSOLE_USER:-admin}
CONSOLE_PASS=${CONSOLE_PASS:-Admin@12345}

log() { printf '\033[36m[system-idx]\033[0m %s\n' "$*"; }
warn() { printf '\033[33m[system-idx][WARN]\033[0m %s\n' "$*"; }
err() { printf '\033[31m[system-idx][ERROR]\033[0m %s\n' "$*" >&2; }

curl -s -m 3 -o /dev/null "$STORE/" || { err "存储集群不可达: $STORE"; exit 1; }

STORE="$STORE" CONSOLE="$CONSOLE" CONSOLE_USER="$CONSOLE_USER" CONSOLE_PASS="$CONSOLE_PASS" python3 - <<'PY'
import json
import os
import sys
import urllib.error
import urllib.request

STORE = os.environ["STORE"].rstrip("/")
CONSOLE = os.environ["CONSOLE"].rstrip("/")
USER = os.environ["CONSOLE_USER"]
PASSWORD = os.environ["CONSOLE_PASS"]

TEMPLATE = {
    "order": 0,
    "index_patterns": [".infini_*"],
    "settings": {"index": {"max_result_window": "10000000",
                           "mapping": {"total_fields": {"limit": "20000"}}}},
    "mappings": {"dynamic_templates": [
        {"strings": {"match_mapping_type": "string",
                     "mapping": {"type": "keyword", "ignore_above": 256}}}]},
}

# 要处理的系统索引。历史上这里按"已知聚合字段"逐个列（health_status → distribution
# → version → tags ...），但页面每换一个聚合维度就冒一个新字段，永远补不完。
# 现在改为：自动扫描索引里所有 text 字段，全部补 fielddata=true（索引文档数极少，开销可忽略）。
SYSTEM_INDICES = [".infini_cluster", ".infini_activities"]


def http(method, url, body=None, token=None, timeout=30):
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        raw = e.read().decode(errors="replace")
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, raw[:300]


def ensure_template():
    st, tpl = http("GET", STORE + "/_template/.infini")
    ok = False
    if st == 200:
        for t in (tpl.get(".infini", {}).get("mappings", {}) or {}).get("dynamic_templates", []):
            if isinstance(t, dict) and "strings" in t:
                ok = t["strings"].get("mapping", {}).get("type") == "keyword"
                break
    if ok:
        print("模板 .infini: 已是 keyword 映射")
        return
    st, out = http("PUT", STORE + "/_template/.infini", TEMPLATE)
    if st not in (200, 201):
        print("模板 .infini 写入失败: %s %s" % (st, out))
        sys.exit(1)
    print("模板 .infini: 已安装（字符串 -> keyword）")


def collect_text_fields(props, prefix=""):
    """递归收集所有 text 字段的点分路径（跳过已开 fielddata 的）。"""
    found = []
    for key, val in props.items():
        path = prefix + key
        if "properties" in val:
            found.extend(collect_text_fields(val["properties"], path + "."))
        elif val.get("type") == "text" and val.get("fielddata") is not True:
            found.append(path)
    return found


def patch_index(index):
    st, body = http("GET", "%s/%s/_mapping" % (STORE, index))
    if st == 404:
        print("%-20s 不存在，跳过（新建时会走模板）" % index)
        return
    if st != 200:
        print("%-20s 读取映射失败: %s" % (index, st))
        return
    mapping = next(iter(body.values())).get("mappings", {})
    fields = collect_text_fields(mapping.get("properties", {}))
    if not fields:
        print("%-20s 无需处理（所有 text 字段已开 fielddata）" % index)
        return
    print("%-20s 发现 %d 个未开 fielddata 的 text 字段，逐个补：" % (index, len(fields)))
    fixed = 0
    for path in fields:
        parts = path.split(".")
        patch = {"properties": {}}
        cur = patch
        for part in parts[:-1]:
            cur["properties"].setdefault(part, {"properties": {}})
            cur = cur["properties"][part]
        cur["properties"][parts[-1]] = {"type": "text", "fielddata": True}
        st, out = http("PUT", "%s/%s/_mapping" % (STORE, index), patch)
        if st == 200:
            fixed += 1
            print("  %-45s OK" % path)
        else:
            print("  %-45s 失败 %s %s" % (path, st, str(out)[:100]))
    print("%-20s 已补 %d/%d" % (index, fixed, len(fields)))


def check_console():
    st, out = http("POST", CONSOLE + "/account/login",
                   {"username": USER, "password": PASSWORD})
    if st != 200 or not isinstance(out, dict) or not out.get("access_token"):
        print("Console 校验: 登录失败（可能尚未初始化），跳过")
        return 0
    st, body = http("GET", CONSOLE + "/elasticsearch/_search", token=out["access_token"])
    if st != 200:
        print("Console 集群列表接口失败: HTTP %s %s" % (st, str(body)[:200]))
        return 2
    total = ((body.get("hits") or {}).get("total") or {}).get("value")
    print("Console 集群列表接口: HTTP 200, 已纳管 %s 个集群" % total)
    return 0


ensure_template()
for index in SYSTEM_INDICES:
    patch_index(index)
sys.exit(check_console())
PY
rc=$?
[ "$rc" = "0" ] || err "集群列表校验失败，请检查 .infini_cluster / .infini_activities 映射"
log "完成"
exit $rc