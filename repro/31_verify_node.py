#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
31_verify_node.py —— 节点页签 TopN 评测：Console /node_metrics 给出的 TopN vs 按指标自身口径的真 TopN

真值来自 14_sample_nodes.py 采到的 /_nodes/stats 序列。
图上节点用 metadata.labels.transport_address 标识（如 127.0.0.1:9311），真值侧做同样映射。

用法：
  python3 31_verify_node.py --cluster topn-biz5 --warmup 60
  python3 31_verify_node.py --samples work/raw/nodes_xxx.jsonl --top 5
"""
import argparse
import glob
import json
import os
import sys
import time
import urllib.parse
import urllib.request

CONSOLE = os.environ.get("CONSOLE", "http://127.0.0.1:9000")

# key, mode, 字段路径（相对单个节点的 stats）
METRICS = [
    ("os_cpu",              "instant", "os.cpu.percent"),
    ("os_used_mem",         "instant", "os.mem.used_percent"),
    ("jvm_heap_used_percent", "instant", "jvm.mem.heap_used_percent"),
    ("index_storage",       "instant", "indices.store.size_in_bytes"),
    ("segment_count",       "instant", "indices.segments.count"),
    ("docs_count",          "instant", "indices.docs.count"),
    ("indexing_rate",       "rate",    "indices.indexing.index_total"),
    ("query_rate",          "rate",    "indices.search.query_total"),
    ("scroll_rate",         "rate",    "indices.search.scroll_total"),
    ("indexing_latency",    "latency", "indices.indexing.index_time_in_millis|indices.indexing.index_total"),
    ("query_latency",       "latency", "indices.search.query_time_in_millis|indices.search.query_total"),
]


def dig(d, path):
    cur = d
    for p in path.split("."):
        if not isinstance(cur, dict) or p not in cur:
            return None
        cur = cur[p]
    return cur


def login():
    body = json.dumps({"username": os.environ.get("CONSOLE_USER", "admin"),
                       "password": os.environ.get("CONSOLE_PASS", "Admin@12345")}).encode()
    req = urllib.request.Request(CONSOLE + "/account/login", data=body,
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())["access_token"]


def api_nodes(tok, cluster, key, top, mn, mx):
    q = urllib.parse.urlencode({"key": key, "top": top, "min": mn, "max": mx})
    req = urllib.request.Request(f"{CONSOLE}/elasticsearch/{cluster}/node_metrics?{q}",
                                 headers={"Authorization": "Bearer " + tok})
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read())


def truth(samples, warmup_s, top):
    rows = [r for r in samples if r["ts"] >= samples[0]["ts"] + warmup_s * 1000]
    if len(rows) < 2:
        rows = samples[-3:]
    out = {}
    for key, mode, field in METRICS:
        score = {}
        if mode == "instant":
            for r in rows:
                for name, nd in r["nodes"].items():
                    v = dig(nd, field)
                    if v is None:
                        continue
                    if v > score.get(name, float("-inf")):
                        score[name] = v
        elif mode == "rate":
            for a, b in zip(rows, rows[1:]):
                dt = (b["ts"] - a["ts"]) / 1000.0
                if dt <= 0:
                    continue
                for name in b["nodes"]:
                    v1 = dig(a["nodes"].get(name, {}), field)
                    v2 = dig(b["nodes"].get(name, {}), field)
                    if v1 is None or v2 is None or v2 < v1:
                        continue
                    q = (v2 - v1) / dt
                    if q > score.get(name, float("-inf")):
                        score[name] = q
        else:
            fn, fd = field.split("|")
            for a, b in zip(rows, rows[1:]):
                for name in b["nodes"]:
                    n1, n2 = dig(a["nodes"].get(name, {}), fn), dig(b["nodes"].get(name, {}), fn)
                    d1, d2 = dig(a["nodes"].get(name, {}), fd), dig(b["nodes"].get(name, {}), fd)
                    if None in (n1, n2, d1, d2) or d2 <= d1 or n2 < n1:
                        continue
                    lat = (n2 - n1) / (d2 - d1)
                    if lat > score.get(name, float("-inf")):
                        score[name] = lat
        out[key] = sorted(score.items(), key=lambda kv: kv[1], reverse=True)[:top]
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cluster", default="topn-biz5")
    ap.add_argument("--samples", default="")
    ap.add_argument("--top", type=int, default=5)
    ap.add_argument("--warmup", type=int, default=60)
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    base = os.path.dirname(os.path.abspath(__file__))
    sp = args.samples or max(glob.glob(os.path.join(base, "work/raw/nodes_*.jsonl")), key=os.path.getmtime)
    samples = [json.loads(l) for l in open(sp, encoding="utf-8") if l.strip()]
    samples.sort(key=lambda r: r["ts"])
    if len(samples) < 2:
        raise SystemExit("采样点不足")
    mn = samples[0]["ts"] + args.warmup * 1000
    mx = samples[-1]["ts"]
    print(f"采样: {sp}  ({len(samples)} 点)")

    # 节点名 -> transport_address（合并全部采样点，避免个别点缺节点导致漏映射）
    name2addr = {}
    for snap in samples:
        for nname, nd in snap["nodes"].items():
            if nd.get("transport_address"):
                name2addr[nname] = nd["transport_address"]
    addr2name = {v: k for k, v in name2addr.items() if v}

    tok = login()
    tru = truth(samples, args.warmup, args.top)

    rows = []
    for key, mode, field in METRICS:
        try:
            resp = api_nodes(tok, args.cluster, key, args.top, mn, mx)
        except Exception as e:
            rows.append(dict(key=key, shown_names=[], shown=[], truth=[], miss=0, miss_leader=False, err=str(e)))
            continue
        m = resp.get("metrics", {}).get(key, {})
        shown_addr = [ln.get("metric", {}).get("label") for ln in m.get("lines", [])]
        shown_addr = [s for s in shown_addr if s]
        shown_names = [addr2name.get(a, a) for a in shown_addr]
        t = [k for k, _ in tru.get(key, [])]
        miss = [x for x in t if x not in shown_names]
        rows.append(dict(key=key, shown=shown_addr, shown_names=shown_names, truth=t,
                         miss=len(miss), miss_names=miss,
                         miss_leader=bool(t and t[0] not in shown_names) if t else False))

    ok = [r for r in rows if r["shown"]]
    bad = [r for r in ok if r["miss"] > 0]
    leader = [r for r in ok if r["miss_leader"]]

    print(f"\n节点数: {len(name2addr)}   有数据的图: {len(ok)}   "
          f"Top{args.top} 未完全上榜: {len(bad)}   榜首不一致: {len(leader)}\n")
    w = max(len(r["key"]) for r in rows)
    for r in rows:
        flag = "LEADER" if r.get("miss_leader") else ("MISS" if r.get("miss") else "OK")
        print(f"{r['key']:<{w}}  [{flag:6s}] 图上={r['shown_names']}")
        print(f"{'':<{w}}            真值={r['truth']}")
        if r.get("miss_names"):
            print(f"{'':<{w}}            未上榜={r['miss_names']}")

    out = args.out or os.path.join(base, "work/report", f"node_topn_{time.strftime('%Y%m%d_%H%M%S')}.md")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        f.write("# 节点页签 TopN 评测报告\n\n")
        f.write(f"- 集群: `{args.cluster}` / Console `{CONSOLE}`\n")
        f.write(f"- 真值窗口: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(mn/1000))} ~ "
                f"{time.strftime('%H:%M:%S', time.localtime(mx/1000))}（预热 {args.warmup}s）\n")
        f.write(f"- 采样: `{sp}`\n\n")
        f.write(f"**有数据的图 {len(ok)} 张，{len(bad)} 张 Top{args.top} 未完全上榜，{len(leader)} 张榜首不一致**\n\n")
        f.write("| 指标 | 图上节点 | 真 Top%d | 未上榜成员 |\n|---|---|---|---|\n" % args.top)
        for r in rows:
            mark = "🔴" if r.get("miss_leader") else ("🟡" if r.get("miss") else "🟢")
            f.write(f"| {mark} `{r['key']}` | {', '.join(r['shown_names']) or '—'} | "
                    f"{', '.join(r['truth']) or '—'} | {', '.join(r.get('miss_names') or []) or '—'} |\n")
    print(f"\n报告: {out}")
    jout = out.replace(".md", ".json")
    json.dump(dict(cluster=args.cluster, window=[mn, mx], rows=rows), open(jout, "w", encoding="utf-8"),
              ensure_ascii=False, indent=2)


if __name__ == "__main__":
    sys.exit(main())
