#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
11_load.py —— 持续压测，制造"各指标真 Top5 互不相同"的流量形态

每个 worker 只打一类流量，速率由 --rate-scale 统一缩放：

  hot_search   高 QPS match 查询          → query_times / fetch / search QPS 榜
  slow_query   terms 聚合 + 脚本排序      → query_latency 真第 1（QPS 很低）
  scroll       持续开 scroll 并翻页       → scroll_times / open_contexts
  cache_churn  随机 term/range 组合查询   → query_cache_* 抖动
  get_by_id    GET /_doc/{id} 为主        → get 系列（页面无图，A 类）
  write_burst  小批量 bulk 写入           → indexing_rate / indexing_bytes
  delete       周期性 delete_by_query     → docs_deleted
  medium       中低速率查询               → 第 2~5 名的争夺
  bulk_store   几乎不打流量（60s 一次）   → 保证存储榜第 1 掉出 QPS 榜

用法：
  python3 11_load.py --duration 300            # 压 5 分钟
  python3 11_load.py --duration 600 --rate-scale 1.5
"""
import argparse
import json
import random
import signal
import sys
import threading
import time
import urllib.error
import urllib.request

STOP = threading.Event()
COUNT = {}
LOCK = threading.Lock()


def bump(k, n=1):
    with LOCK:
        COUNT[k] = COUNT.get(k, 0) + n


def http(method, url, body=None, timeout=60, ndjson=False, raw=False):
    data = None
    h = {}
    if body is not None:
        if ndjson:
            data = body.encode()
            h["Content-Type"] = "application/x-ndjson"
        elif raw:
            data = body.encode()
            h["Content-Type"] = "application/json"
        else:
            data = json.dumps(body).encode()
            h["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            txt = r.read().decode(errors="replace")
            return r.status, (json.loads(txt) if txt[:1] in "[{" else txt)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="replace")[:300]
    except Exception as e:
        return -1, str(e)


def search(ep, idx, body, tag):
    st, _ = http("POST", f"{ep}/{idx}/_search", body)
    if st == 200:
        bump(tag)


def dsl_hot(rnd):
    return {"size": 10, "query": {"match": {"body": rnd.choice(["policy", "premium", "claim", "vehicle"])}},
            "track_total_hits": True}


def dsl_slow(rnd):
    return {
        "size": 5,
        "query": {"bool": {"filter": [{"term": {"region": rnd.choice(["east", "south", "north", "west", "central"])}},
                                      {"range": {"amount": {"gte": 1000}}}]}},
        "aggs": {
            "by_product": {
                "terms": {"field": "product", "size": 20},
                "aggs": {
                    "by_region": {
                        "terms": {"field": "region", "size": 20},
                        "aggs": {"avg_amt": {"avg": {"field": "amount"}},
                                 "scripted": {"sum": {"script": {
                                     "source": "doc['amount'].value > 5000 ? 1 : 0"}}}},
                    }
                },
            }
        },
        "sort": [{"amount": "desc"}],
    }


def dsl_cache(rnd):
    # 每轮换字段组合，制造大量不同的 query，触发缓存抖动/eviction
    q = {"bool": {"must": [{"match": {"body": rnd.choice(["policy", "premium", "tariff", "renewal"])}}],
                  "filter": [{"term": {"product": rnd.choice(["motor", "property", "liability", "accident"])}},
                             {"range": {"claim_count": {"gte": rnd.randrange(0, 4)}}}]}}
    if rnd.random() < 0.5:
        q["bool"]["filter"].append({"term": {"region": "east"}})
    return {"size": 3, "query": q}


def worker_hot(ep, rate):
    rnd = random.Random(1)
    while not STOP.is_set():
        t0 = time.time()
        search(ep, "prod_hot_search", dsl_hot(rnd), "hot_search")
        dt = 1.0 / rate - (time.time() - t0)
        if dt > 0:
            time.sleep(dt)


def worker_slow(ep, rate):
    rnd = random.Random(2)
    while not STOP.is_set():
        t0 = time.time()
        search(ep, "prod_slow_query", dsl_slow(rnd), "slow_query")
        dt = 1.0 / rate - (time.time() - t0)
        if dt > 0:
            time.sleep(dt)


def worker_cache(ep, rate):
    rnd = random.Random(3)
    while not STOP.is_set():
        t0 = time.time()
        search(ep, "prod_cache_churn", dsl_cache(rnd), "cache_churn")
        dt = 1.0 / rate - (time.time() - t0)
        if dt > 0:
            time.sleep(dt)


def worker_scroll(ep, keep=4):
    rnd = random.Random(4)
    while not STOP.is_set():
        st, body = http("POST", f"{ep}/prod_scroll_ctx/_search?scroll=2m",
                        {"size": 200, "query": {"match_all": {}}})
        if st != 200:
            time.sleep(1)
            continue
        sid = body.get("_scroll_id")
        bump("scroll_open")
        for _ in range(5):
            if STOP.is_set() or not sid:
                break
            st, body = http("POST", f"{ep}/_search/scroll", {"scroll": "2m", "scroll_id": sid})
            if st != 200 or not body.get("hits", {}).get("hits"):
                break
            sid = body.get("_scroll_id", sid)
            bump("scroll_page")
            time.sleep(0.05)
        if sid:
            http("DELETE", f"{ep}/_search/scroll", {"scroll_id": [sid]})
        # 保持若干上下文长期打开，避免 open_contexts 立刻归零
        time.sleep(0.2)


def worker_get(ep, ids, rate):
    rnd = random.Random(5)
    while not STOP.is_set():
        t0 = time.time()
        i = rnd.randrange(ids)
        st, _ = http("GET", f"{ep}/prod_get_heavy/_doc/{i}")
        if st == 200:
            bump("get_by_id")
        dt = 1.0 / rate - (time.time() - t0)
        if dt > 0:
            time.sleep(dt)


def worker_write(ep, rate, rnd_seed=6):
    rnd = random.Random(rnd_seed)
    seq = 1_000_000 + rnd_seed * 100_000
    while not STOP.is_set():
        lines = []
        n = max(1, int(rate / 5))
        for _ in range(n):
            seq += 1
            doc = {"policy_no": f"PBURST{seq:08d}", "vin": f"LSV{seq:011d}",
                   "region": rnd.choice(["east", "south"]), "product": "motor",
                   "amount": round(rnd.uniform(100, 9000), 2), "claim_count": rnd.randrange(5),
                   "created_at": "2026-11-20T13:00:00Z",
                   "body": "policy premium burst write tariff renewal"}
            lines.append(json.dumps({"index": {"_index": "prod_write_burst", "_id": str(seq)}}))
            lines.append(json.dumps(doc))
        st, body = http("POST", f"{ep}/_bulk", "\n".join(lines) + "\n", ndjson=True)
        if st == 200:
            bump("write_burst", n)
        if STOP.wait(0.2):
            break


def worker_delete(ep, period=40):
    rnd = random.Random(7)
    while not STOP.wait(period):
        st, _ = http("POST", f"{ep}/prod_delete_heavy/_delete_by_query?conflicts=proceed",
                     {"query": {"range": {"claim_count": {"lte": 0}}}}, timeout=120)
        if st == 200:
            bump("delete_by_query")
        # 删除后补写，保持数据量与写入量
        nd = 2000
        lines = []
        for i in range(nd):
            k = rnd.randrange(10**6)
            lines.append(json.dumps({"index": {"_index": "prod_delete_heavy", "_id": f"r{k}"}}))
            lines.append(json.dumps({"policy_no": f"PDEL{k}", "vin": f"LSV{k:011d}",
                                     "region": "east", "product": "motor", "amount": 100.0,
                                     "claim_count": 1, "created_at": "2026-11-20T13:00:00Z",
                                     "body": "policy claim renew"}))
        http("POST", f"{ep}/_bulk", "\n".join(lines) + "\n", ndjson=True)


def worker_medium(ep, idx, rate, seed):
    rnd = random.Random(seed)
    while not STOP.is_set():
        t0 = time.time()
        search(ep, idx, {"size": 5, "query": {"term": {"region": rnd.choice(["east", "south", "north"])}}},
               f"medium:{idx}")
        dt = 1.0 / rate - (time.time() - t0)
        if dt > 0:
            time.sleep(dt)


def worker_idle_bbig(ep, period=60):
    rnd = random.Random(9)
    while not STOP.wait(period):
        search(ep, "prod_bulk_store", {"size": 1, "query": {"match": {"body": rnd.choice(["policy", "claim"])}}},
               "idle_bulk_store")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", default="http://127.0.0.1:9201")
    ap.add_argument("--duration", type=int, default=300, help="压测秒数")
    ap.add_argument("--rate-scale", type=float, default=1.0)
    ap.add_argument("--no-write", action="store_true",
                    help="只读模式：不跑写入/删除 worker（省内存，候选集只由读 QPS 决定）")
    args = ap.parse_args()
    ep = args.endpoint.rstrip("/")
    rs = args.rate_scale

    st, info = http("GET", ep)
    if st != 200:
        raise SystemExit(f"业务集群不可达: {ep}")
    print(f"压测目标: {info.get('cluster_name')}  时长 {args.duration}s  rate-scale={rs}")

    st, _ = http("GET", f"{ep}/prod_hot_search/_count")
    if st != 200:
        raise SystemExit("索引不存在，请先执行 10_seed.py")

    threads = [
        *[threading.Thread(target=worker_hot, args=(ep, 40 * rs), daemon=True) for _ in range(5)],
        threading.Thread(target=worker_slow, args=(ep, 2 * rs), daemon=True),
        threading.Thread(target=worker_cache, args=(ep, 25 * rs), daemon=True),
        threading.Thread(target=worker_scroll, args=(ep,), daemon=True),
        threading.Thread(target=worker_get, args=(ep, 25000, 60 * rs), daemon=True),
        threading.Thread(target=worker_medium, args=(ep, "prod_medium_a", 15 * rs, 11), daemon=True),
        threading.Thread(target=worker_medium, args=(ep, "prod_medium_b", 8 * rs, 12), daemon=True),
        threading.Thread(target=worker_medium, args=(ep, "prod_medium_c", 3 * rs, 13), daemon=True),
        threading.Thread(target=worker_idle_bbig, args=(ep, 60), daemon=True),
    ]
    if args.no_write:
        print("只读模式：跳过 write_burst / delete 两个 worker"
              "（prod_bulk_store 的空闲流量已由上面的常驻 worker 提供）")
    else:
        threads += [
            threading.Thread(target=worker_write, args=(ep, 800 * rs, 6), daemon=True),
            threading.Thread(target=worker_delete, args=(ep, 40), daemon=True),
        ]
    for t in threads:
        t.start()

    def stop(sig, frm):
        STOP.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    t0 = time.time()
    while time.time() - t0 < args.duration and not STOP.is_set():
        time.sleep(10)
        elapsed = int(time.time() - t0)
        with LOCK:
            snap = dict(COUNT)
        print(f"[{elapsed:4d}s] " + "  ".join(f"{k}={v}" for k, v in sorted(snap.items())), flush=True)
    STOP.set()
    time.sleep(2)
    print("\n压测结束，累计:", json.dumps(COUNT, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    sys.exit(main())
