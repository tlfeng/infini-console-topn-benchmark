#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
12_sample.py —— 采样业务集群的真实 _stats，作为 TopN 真值来源

每 --interval 秒抓一次 GET /_stats（indices 级），逐行写 JSONL：
  {"ts": <epoch_ms>, "indices": {"<index>": {"primaries": {...}, "total": {...}}}}

配合 30_verify.py 计算"按指标自身口径"的 Top5，与 Console API 返回的榜单做差集。
"""
import argparse
import json
import os
import sys
import time
import urllib.request

KEYS = ("docs,store,indexing,search,get,merge,refresh,flush,segments,query_cache,"
        "request_cache,fielddata,translog,completion,warmer")


def fetch(ep):
    url = f"{ep}/_stats/{KEYS}?level=indices&filter_path=indices"
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.loads(r.read().decode())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", default="http://127.0.0.1:9201")
    ap.add_argument("--interval", type=float, default=10.0)
    ap.add_argument("--duration", type=int, default=0, help="0=一直采到 SIGTERM")
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    ep = args.endpoint.rstrip("/")
    out = args.out or os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                   "work", "raw", "stats_%s.jsonl" % time.strftime("%Y%m%d_%H%M%S"))
    os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)

    t0 = time.time()
    n = 0
    with open(out, "w", encoding="utf-8") as f:
        while True:
            try:
                ts = int(time.time() * 1000)
                data = fetch(ep)
                f.write(json.dumps({"ts": ts, "indices": data.get("indices", {})},
                                   ensure_ascii=False) + "\n")
                f.flush()
                n += 1
                print(f"[sample {n:03d}] ts={ts} indices={len(data.get('indices', {}))}", flush=True)
            except Exception as e:
                print(f"[sample] 抓取失败: {e}", file=sys.stderr, flush=True)
            if args.duration and time.time() - t0 >= args.duration:
                break
            time.sleep(args.interval)
    print(f"\n采样结束 -> {os.path.abspath(out)} ({n} 个点)")


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n采样中断")
