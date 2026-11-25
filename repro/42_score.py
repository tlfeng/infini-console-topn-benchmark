#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
42_score.py —— TopN 评测评分卡：汇总报告 JSON 的 Top5 命中率与榜首一致率

解析 work/report/ 下的评测报告（30/31/32 产出的 JSON，格式统一含
truth / shown / miss_leader 字段），对每张图计算：

  Top5 命中率   |图上 ∩ 真值| / |真值|
  榜首一致      图上第 1 名 == 真值第 1 名

并给出整轮汇总，用于与 reports/BASELINE.md 及历史轮次对照。

用法：
  python3 42_score.py                                # 自动取各家族最新一份报告
  python3 42_score.py work/report/node_topn_*.json   # 指定报告（可多份）
"""
import argparse
import glob
import json
import os

BASE = os.path.dirname(os.path.abspath(__file__))
FAMILIES = ["topn_report_*.json", "node_topn_*.json", "topn_view_*.json"]


def load_entries(path):
    """报告 JSON 统一取 rows（30/31）或 tests（32）。"""
    with open(path, encoding="utf-8") as f:
        d = json.load(f)
    return d.get("rows") or d.get("tests") or []


def score(entries):
    """返回逐图评分；跳过无真值或采集中断（status != ok）的条目。"""
    out = []
    for e in entries:
        truth = e.get("truth") or []
        shown = e.get("shown_names") or e.get("shown") or []
        if not truth or e.get("status", "ok") != "ok":
            continue
        hit_n = len(set(shown) & set(truth))
        out.append({
            "key": e.get("key", "?"),
            "hit_n": hit_n,
            "hit_total": len(truth),
            "leader_ok": not bool(e.get("miss_leader")),
            "missing": sorted(set(truth) - set(shown)),
        })
    return out


def print_card(path, entries):
    rows = score(entries)
    if not rows:
        print(f"== {path}\n   （无可评分条目）\n")
        return 0, 0, 0, 0.0
    width = max(len(r["key"]) for r in rows)
    print(f"== {path}")
    for r in rows:
        leader = "榜首一致" if r["leader_ok"] else "榜首不一致"
        miss = f"未上榜: {r['missing']}" if r["missing"] else ""
        print(f"  {r['key']:<{width}}  命中 {r['hit_n']}/{r['hit_total']}  {leader}  {miss}")
    full = sum(1 for r in rows if r["hit_n"] == r["hit_total"])
    leader_ok = sum(1 for r in rows if r["leader_ok"])
    avg_hit = sum(r["hit_n"] / r["hit_total"] for r in rows) / len(rows)
    n = len(rows)
    print(f"  -- 汇总: 图 {n} · Top5 完全一致 {full}/{n} ({full * 100 // n}%)"
          f" · 榜首一致 {leader_ok}/{n} ({leader_ok * 100 // n}%)"
          f" · 平均命中率 {avg_hit * 100:.0f}%\n")
    return n, full, leader_ok, avg_hit


def main():
    ap = argparse.ArgumentParser(description="TopN 评测评分卡（命中率 / 榜首一致率），对照 reports/BASELINE.md")
    ap.add_argument("files", nargs="*", help="报告 JSON 路径；缺省取各家族最新一份")
    args = ap.parse_args()

    files = args.files
    if not files:
        for pat in FAMILIES:
            hits = sorted(glob.glob(os.path.join(BASE, "work/report", pat)))
            if hits:
                files.append(hits[-1])
    if not files:
        raise SystemExit("未找到报告：先跑 40/41/32 生成 work/report/*.json，或用参数指定文件")

    tot_n = tot_full = tot_leader = 0
    for path in files:
        n, full, leader, _ = print_card(path, load_entries(path))
        tot_n += n
        tot_full += full
        tot_leader += leader

    if len(files) > 1 and tot_n:
        print(f"== 合计: 图 {tot_n} · Top5 完全一致 {tot_full}/{tot_n}"
              f" · 榜首一致 {tot_leader}/{tot_n}"
              f"（对照 reports/BASELINE.md 基线：索引页签 1/23、13/23；节点页签 2/11、6/11）")


if __name__ == "__main__":
    main()
