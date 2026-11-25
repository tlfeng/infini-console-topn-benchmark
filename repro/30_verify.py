#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
30_verify.py —— TopN 评测：把 Console 图表实际给出的 Top5 与"按指标自身口径"的真 Top5 做差集

输入：
  work/raw/stats_*.jsonl   12_sample.py 采到的真实 _stats 序列（真值来源）
  Console /elasticsearch/:id/index_metrics?key=&top=   （被测对象）

判据：
  miss_leader  真 Top5 第 1 名与图上榜首不一致
  miss_n       真 Top5 中未上榜的成员数

另外记录每张图后端返回的 terms.order 字段，用于对照各图的排序口径。

用法：
  python3 30_verify.py                      # 用最新采样文件，跳过前 60s 预热
  python3 30_verify.py --samples work/raw/stats_xxx.jsonl --warmup 90 --top 5
  python3 30_verify.py --out work/report/topn_report.md
"""
import argparse
import glob
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

CONSOLE = os.environ.get("CONSOLE", "http://127.0.0.1:9000")
CLUSTER_ID = os.environ.get("CLUSTER_ID", "topn-biz")

# 指标口径：mode=instant|rate|latency；scope=primaries|total；field=ES stats 路径
METRICS = [
    ("index_storage",      "instant", "total",     "store.size_in_bytes",                     "bytes"),
    ("doc_count",          "instant", "total",     "docs.count",                              "num"),
    ("docs_deleted",       "instant", "total",     "docs.deleted",                            "num"),
    ("segment_count",      "instant", "total",     "segments.count",                          "num"),
    ("indexing_rate",      "rate",    "primaries", "indexing.index_total",                    "num"),
    # 注意：ES 索引级 _stats 没有"写入字节数"，Console 的 Indexing Bytes 图实际也取 index_total，
    # 这里沿用同一口径（与图对齐），因此它和 indexing_rate 的排名必然一致，不作为独立结论。
    ("indexing_bytes",     "rate",    "primaries", "indexing.index_total",                    "bytes"),
    ("query_times",        "rate",    "total",     "search.query_total",                      "num"),
    ("fetch_times",        "rate",    "total",     "search.fetch_total",                      "num"),
    ("scroll_times",       "rate",    "total",     "search.scroll_total",                     "num"),
    ("merge_times",        "rate",    "total",     "merges.total",                            "num"),
    ("refresh_times",      "rate",    "total",     "refresh.total",                            "num"),
    ("flush_times",        "rate",    "total",     "flush.total",                              "num"),
    ("indexing_latency",   "latency", "primaries", "indexing.index_time_in_millis|indexing.index_total", "ms"),
    ("query_latency",      "latency", "total",     "search.query_time_in_millis|search.query_total",     "ms"),
    ("fetch_latency",      "latency", "total",     "search.fetch_time_in_millis|search.fetch_total",     "ms"),
    ("scroll_latency",     "latency", "total",     "search.scroll_time_in_millis|search.scroll_total",   "ms"),
    ("segment_terms_memory", "instant", "total",   "segments.terms_memory_in_bytes",          "bytes"),
    ("segment_points_memory", "instant", "total",  "segments.points_memory_in_bytes",         "bytes"),
    ("query_cache",        "instant", "total",     "query_cache.memory_size_in_bytes",        "bytes"),
    ("fielddata_cache",    "instant", "total",     "fielddata.memory_size_in_bytes",          "bytes"),
    ("query_cache_count",  "rate",    "total",     "query_cache.total_count",                 "num"),
    ("query_cache_hit",    "rate",    "total",     "query_cache.hit_count",                   "num"),
    ("query_cache_miss",   "rate",    "total",     "query_cache.miss_count",                  "num"),
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


def api_metrics(tok, cluster, key, top, mn, mx):
    q = urllib.parse.urlencode({"key": key, "top": top, "min": mn, "max": mx})
    req = urllib.request.Request(f"{CONSOLE}/elasticsearch/{cluster}/index_metrics?{q}",
                                 headers={"Authorization": "Bearer " + tok})
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read())


def load_samples(path):
    rows = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    rows.sort(key=lambda x: x["ts"])
    return rows


def truth(samples, warmup_s, top):
    """按每个指标自身口径算 TopN。samples 已按时间排序。"""
    if not samples:
        return {}
    t_end = samples[-1]["ts"]
    t_start = t_end - (samples[-1]["ts"] - samples[0]["ts"]) + warmup_s * 1000
    rows = [r for r in samples if r["ts"] >= t_start]
    if len(rows) < 2:
        rows = samples[-3:]

    out = {}
    for key, mode, scope, field, _unit in METRICS:
        score = {}
        if mode == "instant":
            for r in rows:
                for idx, st in r["indices"].items():
                    v = dig(st.get(scope, {}), field)
                    if v is None:
                        continue
                    if v > score.get(idx, float("-inf")):
                        score[idx] = v
        elif mode == "rate":
            for a, b in zip(rows, rows[1:]):
                dt = (b["ts"] - a["ts"]) / 1000.0
                if dt <= 0:
                    continue
                for idx in b["indices"]:
                    v1 = dig(a["indices"].get(idx, {}).get(scope, {}), field)
                    v2 = dig(b["indices"].get(idx, {}).get(scope, {}), field)
                    if v1 is None or v2 is None or v2 < v1:
                        continue
                    rate = (v2 - v1) / dt
                    if rate > score.get(idx, float("-inf")):
                        score[idx] = rate
        else:  # latency
            f_num, f_den = field.split("|")
            for a, b in zip(rows, rows[1:]):
                for idx in b["indices"]:
                    n1 = dig(a["indices"].get(idx, {}).get(scope, {}), f_num)
                    n2 = dig(b["indices"].get(idx, {}).get(scope, {}), f_num)
                    d1 = dig(a["indices"].get(idx, {}).get(scope, {}), f_den)
                    d2 = dig(b["indices"].get(idx, {}).get(scope, {}), f_den)
                    if None in (n1, n2, d1, d2):
                        continue
                    dc = d2 - d1
                    if dc <= 0 or n2 < n1:
                        continue
                    lat = (n2 - n1) / dc
                    if lat > score.get(idx, float("-inf")):
                        score[idx] = lat
        ranked = sorted(score.items(), key=lambda kv: kv[1], reverse=True)
        out[key] = ranked[:top]
    return out


def dsl_order(raw):
    """从返回 DSL 里取出候选/展示排序用的字段，作为"口径"证据"""
    try:
        dsl = json.loads(raw["metrics"][list(raw["metrics"])[0]]["request"])
        terms = dsl["aggs"]["group_by_level"]["terms"]
        return terms.get("order", {})
    except Exception:
        return {}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--samples", default="")
    ap.add_argument("--cluster", default=CLUSTER_ID, help="Console 里的集群 ID")
    ap.add_argument("--top", type=int, default=5)
    ap.add_argument("--warmup", type=int, default=60, help="跳过采样头部多少秒（预热）")
    ap.add_argument("--out", default="")
    args = ap.parse_args()
    cluster = args.cluster

    base = os.path.dirname(os.path.abspath(__file__))
    sp = args.samples or max(glob.glob(os.path.join(base, "work/raw/stats_*.jsonl")), key=os.path.getmtime)
    samples = load_samples(sp)
    if len(samples) < 2:
        raise SystemExit(f"采样点不足: {sp}")
    mn = samples[0]["ts"] + args.warmup * 1000
    mx = samples[-1]["ts"]
    print(f"采样文件: {sp}\n采样点: {len(samples)}  真值窗口: {time.strftime('%H:%M:%S', time.localtime(mn/1000))}"
          f" ~ {time.strftime('%H:%M:%S', time.localtime(mx/1000))}")

    tok = login()
    tru = truth(samples, args.warmup, args.top)

    rows = []
    for key, mode, scope, field, unit in METRICS:
        try:
            resp = api_metrics(tok, cluster, key, args.top, mn, mx)
        except Exception as e:
            rows.append(dict(key=key, status=f"API 失败: {e}", shown=[], truth=[], miss=0, miss_leader=False))
            continue
        m = resp.get("metrics", {}).get(key, {})
        shown = [ln.get("metric", {}).get("label") for ln in m.get("lines", [])]
        shown = [s for s in shown if s]
        t = [k for k, _ in tru.get(key, [])]
        miss = [x for x in t if x not in shown]
        rows.append(dict(key=key, mode=mode, unit=unit,
                         status="ok" if m else "无返回",
                         shown=shown, truth=t,
                         miss=len(miss), miss_names=miss,
                         miss_leader=bool(t and shown and t[0] not in shown) if t else False,
                         order=dsl_order(resp)))

    ok = [r for r in rows if r["status"] == "ok" and r["shown"]]
    bad = [r for r in ok if r["miss"] > 0]
    leader = [r for r in ok if r["miss_leader"]]

    orders = {}
    for r in ok:
        orders[json.dumps(r.get("order"), ensure_ascii=False)] = orders.get(
            json.dumps(r.get("order"), ensure_ascii=False), 0) + 1

    print(f"\n有数据的图: {len(ok)}   Top{args.top} 未完全上榜: {len(bad)}   榜首不一致: {len(leader)}")
    print(f"排序口径分布: {orders}\n")

    width = max(len(r["key"]) for r in rows)
    for r in rows:
        flag = "LEADER" if r.get("miss_leader") else ("MISS" if r.get("miss") else "OK")
        print(f"{r['key']:<{width}}  [{flag:6s}] 图上={r.get('shown')}  真值={r.get('truth')}")
        if r.get("miss_names"):
            print(f"{'':<{width}}           未上榜={r['miss_names']}")

    out = args.out or os.path.join(base, "work/report", f"topn_report_{time.strftime('%Y%m%d_%H%M%S')}.md")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        f.write("# TopN 评测报告\n\n")
        f.write(f"- 集群: `{args.cluster}` / Console `{CONSOLE}`\n")
        f.write(f"- 真值窗口: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(mn/1000))} ~ "
                f"{time.strftime('%H:%M:%S', time.localtime(mx/1000))}（跳过预热 {args.warmup}s）\n")
        f.write(f"- 采样文件: `{sp}`\n\n")
        f.write(f"**有数据的图 {len(ok)} 张，其中 {len(bad)} 张 Top{args.top} 未完全上榜，"
                f"{len(leader)} 张榜首不一致。**\n\n")
        f.write("| 指标 | 图上 Top%d | 真 Top%d | 未上榜成员 |\n|---|---|---|---|\n" % (args.top, args.top))
        for r in rows:
            mark = "🔴" if r.get("miss_leader") else ("🟡" if r.get("miss") else "🟢")
            f.write(f"| {mark} `{r['key']}` | {', '.join(r.get('shown') or []) or '—'} | "
                    f"{', '.join(r.get('truth') or []) or '—'} | {', '.join(r.get('miss_names') or []) or '—'} |\n")
        f.write("\n## 候选口径证据\n\n")
        f.write("每张图后端返回的 terms 聚合排序字段（38 张图共用同一套候选时，此处会完全一致）：\n\n")
        for k, v in orders.items():
            f.write(f"- `{k}` × {v}\n")
    print(f"\n报告: {out}")

    jout = out.replace(".md", ".json")
    with open(jout, "w", encoding="utf-8") as f:
        json.dump(dict(cluster=cluster, window=[mn, mx], top=args.top, rows=rows),
                  f, ensure_ascii=False, indent=2)
    print(f"明细: {jout}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
