#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
14_sample_nodes.py —— 采样节点级真实 /_nodes/stats，作为节点 TopN 的真值来源

每 --interval 秒抓一次，逐行写 JSONL：
  {"ts": <epoch_ms>, "nodes": {"node-1": {"transport_address": "127.0.0.1:9311",
        "os": {...}, "jvm": {...}, "indices": {...}, "fs": {...}}}}

用法：
  python3 14_sample_nodes.py --endpoint http://127.0.0.1:9211 --duration 300
"""
import argparse
import json
import os
import sys
import time
import urllib.request

FILTER = ("nodes.*.name,nodes.*.transport_address,nodes.*.os.cpu.percent,"
          "nodes.*.os.mem.used_percent,nodes.*.os.mem.used_in_bytes,"
          "nodes.*.jvm.mem.heap_used_percent,nodes.*.jvm.mem.heap_used_in_bytes,"
          "nodes.*.indices.store.size_in_bytes,nodes.*.indices.segments.count,"
          "nodes.*.indices.docs.count,nodes.*.indices.indexing.index_total,"
          "nodes.*.indices.indexing.index_time_in_millis,"
          "nodes.*.indices.search.query_total,nodes.*.indices.search.query_time_in_millis,"
          "nodes.*.indices.search.scroll_total,"
          "nodes.*.fs.total.available_in_bytes,nodes.*.fs.total.total_in_bytes,"
          "nodes.*.thread_pool.write.rejected,nodes.*.thread_pool.search.rejected")


def fetch(ep):
    url = f"{ep}/_nodes/stats?filter_path={FILTER}"
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.loads(r.read().decode())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", default="http://127.0.0.1:9211")
    ap.add_argument("--interval", type=float, default=10.0)
    ap.add_argument("--duration", type=int, default=0)
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    ep = args.endpoint.rstrip("/")
    out = args.out or os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                   "work", "raw", "nodes_%s.jsonl" % time.strftime("%Y%m%d_%H%M%S"))
    os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)

    t0 = time.time()
    n = 0
    with open(out, "w", encoding="utf-8") as f:
        while True:
            try:
                ts = int(time.time() * 1000)
                data = fetch(ep).get("nodes", {})
                snap = {}
                for nid, nd in data.items():
                    snap[nd.get("name", nid)] = {
                        "transport_address": nd.get("transport_address", ""),
                        "os": nd.get("os", {}),
                        "jvm": nd.get("jvm", {}),
                        "indices": nd.get("indices", {}),
                        "fs": nd.get("fs", {}),
                        "thread_pool": nd.get("thread_pool", {}),
                    }
                f.write(json.dumps({"ts": ts, "nodes": snap}, ensure_ascii=False) + "\n")
                f.flush()
                n += 1
                print(f"[sample {n:03d}] {len(snap)} nodes", flush=True)
            except Exception as e:
                print(f"[sample] 失败: {e}", file=sys.stderr, flush=True)
            if args.duration and time.time() - t0 >= args.duration:
                break
            time.sleep(args.interval)
    print(f"\n节点采样结束 -> {os.path.abspath(out)} ({n} 点)")


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n中断")
