#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
10_seed.py —— 造数：按"索引画像"创建一批索引，使各指标的 Top5 与
              "读写峰值 QPS" 的 Top5 显著不同（保证准确度评测的区分度）

画像设计（每个索引只把 1~2 个维度做突出，其余压平）：

  prod_hot_search   搜索 QPS 极高、数据量很小          → 读写 QPS 榜单常客
  prod_bulk_store   数据量极大（大字段），几乎无读写    → index_storage 真第 1，但 QPS 榜外
  prod_write_burst  写入速率高但总量不大               → indexing_rate 榜前列
  prod_slow_query   查询次数少、单次极慢               → query_latency 真第 1，QPS 榜外
  prod_scroll_ctx   大量 scroll 上下文                 → scroll_times 真第 1
  prod_cache_churn  随机查询导致 query cache 抖动      → query_cache_* 真第 1
  prod_delete_heavy 文档多、删除多                     → docs_deleted 真第 1
  prod_get_heavy    GET by id 为主                     → get 系列（A 类指标）
  .kibana_task_mgr  系统点号索引，极低 QPS             → 覆盖系统索引整体缺席的形态
  prod_medium_*     中等规模填充，制造"第 2~5 名争夺"

用法：
  python3 10_seed.py                 # 默认写入 http://127.0.0.1:9201
  python3 10_seed.py --endpoint http://127.0.0.1:9201 --scale 1.0
"""
import argparse
import json
import os
import random
import sys
import time
import urllib.error
import urllib.request

# ---------------------------------------------------------------- HTTP 工具


def http(method, url, body=None, headers=None, timeout=120, ndjson=False):
    data = None
    h = dict(headers or {})
    if body is not None:
        if ndjson:
            data = body.encode()
            h["Content-Type"] = "application/x-ndjson"
        else:
            data = json.dumps(body).encode()
            h["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode(errors="replace")
            return r.status, (json.loads(raw) if raw.strip().startswith(("{", "[")) else raw)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="replace")[:500]
    except Exception as e:            # 连接被拒 / 超时：不要抛栈，交给调用方判定
        return -1, str(e)


WORDS = ("policy premium insure claim vehicle risk ncd motor tariff quote renewal "
         "agent broker channel product coverage deductible liability collision").split()


def rand_text(nbytes, rnd):
    parts = []
    size = 0
    while size < nbytes:
        w = rnd.choice(WORDS)
        parts.append(w)
        size += len(w) + 1
    return " ".join(parts)


# ---------------------------------------------------------------- 画像定义
# docs: 文档数；text: 正文字节数；shards: 主分片；replicas: 副本
PROFILES = [
    dict(name="prod_hot_search", docs=15000, text=80, shards=1, replicas=0,
         desc="高搜索 QPS / 小数据量"),
    dict(name="prod_bulk_store", docs=90000, text=1400, shards=2, replicas=0,
         desc="超大存储 / 近乎零读写"),
    dict(name="prod_write_burst", docs=12000, text=120, shards=1, replicas=0,
         desc="写入突发"),
    dict(name="prod_slow_query", docs=40000, text=200, shards=2, replicas=0,
         desc="低 QPS / 高时延"),
    dict(name="prod_scroll_ctx", docs=30000, text=200, shards=1, replicas=0,
         desc="scroll 上下文多"),
    dict(name="prod_cache_churn", docs=20000, text=120, shards=1, replicas=0,
         desc="查询缓存抖动"),
    dict(name="prod_delete_heavy", docs=45000, text=100, shards=1, replicas=0,
         desc="删除量大"),
    dict(name="prod_get_heavy", docs=25000, text=100, shards=1, replicas=0,
         desc="GET by id 为主"),
    dict(name=".kibana_task_manager_1", docs=3000, text=400, shards=1, replicas=0,
         desc="系统点号索引"),
    dict(name="prod_medium_a", docs=18000, text=300, shards=1, replicas=0, desc="中等规模"),
    dict(name="prod_medium_b", docs=12000, text=600, shards=1, replicas=0, desc="中等规模"),
    dict(name="prod_medium_c", docs=6000, text=900, shards=1, replicas=0, desc="中等规模"),
]


def mapping():
    return {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "refresh_interval": "1s",
        },
        "mappings": {
            "properties": {
                "policy_no": {"type": "keyword"},
                "vin": {"type": "keyword"},
                "region": {"type": "keyword"},
                "product": {"type": "keyword"},
                "amount": {"type": "double"},
                "claim_count": {"type": "integer"},
                "created_at": {"type": "date"},
                "body": {"type": "text"},
            }
        },
    }


def create_index(ep, prof, rnd, node=""):
    name = prof["name"]
    st, body = http("HEAD", f"{ep}/{name}")
    if st == 200:
        print(f"  [skip] {name} 已存在")
        return False
    m = mapping()
    m["settings"]["number_of_shards"] = prof["shards"]
    m["settings"]["number_of_replicas"] = prof["replicas"]
    if node:
        # 把整个索引钉在指定节点上，用于制造"节点画像"
        m["settings"]["index.routing.allocation.require._name"] = node
    st, body = http("PUT", f"{ep}/{name}", m)
    if st not in (200, 201):
        raise SystemExit(f"创建索引 {name} 失败: {st} {body}")
    print(f"  [create] {name:22s} {prof['desc']}" + (f"  -> {node}" if node else ""))
    return True


def wait_shards(ep, name, node, secs=90):
    """等索引分片全部 STARTED 且落在期望节点"""
    for _ in range(secs):
        st, rows = http("GET", f"{ep}/_cat/shards/{name}?format=json")
        if st == 200 and isinstance(rows, list) and rows:
            if all(r.get("state") == "STARTED" and (not node or r.get("node") == node) for r in rows):
                return True
        time.sleep(1)
    return False


def gen_doc(i, prof, rnd):
    return {
        "policy_no": f"P{prof['name'][:4].upper()}{i:08d}",
        "vin": f"LSV{rnd.randrange(10**11):011d}",
        "region": rnd.choice(["east", "south", "north", "west", "central"]),
        "product": rnd.choice(["motor", "property", "liability", "accident"]),
        "amount": round(rnd.uniform(100, 100000), 2),
        "claim_count": rnd.randrange(0, 5),
        "created_at": "2026-11-%02dT%02d:%02d:%02dZ" % (
            rnd.randrange(1, 20), rnd.randrange(24), rnd.randrange(60), rnd.randrange(60)),
        "body": rand_text(prof["text"], rnd),
    }


def bulk_fill(ep, prof, rnd, batch=1000):
    name = prof["name"]
    total = prof["docs"]
    done = 0
    t0 = time.time()
    while done < total:
        n = min(batch, total - done)
        lines = []
        for i in range(done, done + n):
            lines.append(json.dumps({"index": {"_index": name, "_id": f"{i}"}}))
            lines.append(json.dumps(gen_doc(i, prof, rnd), ensure_ascii=False))
        st, body = http("POST", f"{ep}/_bulk?refresh=false", "\n".join(lines) + "\n", ndjson=True)
        if st not in (200, 201) or (isinstance(body, dict) and body.get("errors")):
            raise SystemExit(f"bulk 失败 {name}: {st} {str(body)[:300]}")
        done += n
        if done % (batch * 10) == 0 or done >= total:
            rate = done / max(time.time() - t0, 0.001)
            print(f"    {name}: {done}/{total} ({rate:.0f} docs/s)", flush=True)
    http("POST", f"{ep}/{name}/_refresh")
    st, stats = http("GET", f"{ep}/{name}/_stats/store,docs")
    size = stats["_all"]["primaries"]["store"]["size_in_bytes"] if isinstance(stats, dict) else -1
    print(f"    -> {name} 完成，primaries.store={size/1048576:.1f} MB")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", default="http://127.0.0.1:9201")
    ap.add_argument("--scale", type=float, default=1.0, help="文档量缩放系数")
    ap.add_argument("--seed", type=int, default=20261120)
    ap.add_argument("--node-map", default="",
                    help='索引→节点 的 JSON（文件路径或内联），如 {"prod_hot_search":"node-1"}')
    ap.add_argument("--recreate", action="store_true", help="已存在则删除重建（多节点分配需要）")
    args = ap.parse_args()

    ep = args.endpoint.rstrip("/")
    st, info = http("GET", ep)
    if st != 200:
        raise SystemExit(f"业务集群不可达: {ep} ({st})")
    print(f"目标集群: {info.get('cluster_name')} {info.get('version', {}).get('number')}")
    rnd = random.Random(args.seed)

    node_map = {}
    if args.node_map:
        if os.path.isfile(args.node_map):
            with open(args.node_map, encoding="utf-8") as f:
                node_map = json.load(f)
        else:
            node_map = json.loads(args.node_map)
        print(f"节点分配: {node_map}")

    t_all = time.time()
    for prof in PROFILES:
        p = dict(prof)
        p["docs"] = max(100, int(p["docs"] * args.scale))
        node = node_map.get(p["name"], "")
        if args.recreate:
            http("DELETE", f"{ep}/{p['name']}")
        create_index(ep, p, rnd, node)
        if node:
            ok = wait_shards(ep, p["name"], node)
            print(f"    分片分配 -> {node}: {'OK' if ok else '超时，继续'}")
    print()
    for prof in PROFILES:
        p = dict(prof)
        p["docs"] = max(100, int(p["docs"] * args.scale))
        print(f"填充 {p['name']} ({p['docs']} docs, text={p['text']}B)")
        bulk_fill(ep, p, rnd)
    print(f"\n造数完成，用时 {time.time() - t_all:.1f}s")
    st, cat = http("GET", f"{ep}/_cat/indices?v&h=index,docs.count,store.size&s=store.size:desc")
    print(cat if isinstance(cat, str) else json.dumps(cat)[:2000])


if __name__ == "__main__":
    sys.exit(main())
