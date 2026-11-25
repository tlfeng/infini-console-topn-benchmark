#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
15_set_readonly.py —— 按画像给索引设"只读冻结"：只有需要写入的索引保持可写，其余只读

只读索引会被设成：
  index.blocks.write = true      # 禁止写入
  index.refresh_interval = -1    # 关掉周期 refresh（不再反复生成新段、不再 merge）
可写索引恢复为 refresh_interval=1s。

这样既保留"写 QPS"这一路候选（节点/索引页签都需要），又省掉只读索引的写入缓冲、
refresh 与 merge 开销 —— 更接近生产里"历史索引只读、热点索引在写"的形态。

用法：
  python3 15_set_readonly.py --on                       # 冻结只读索引
  python3 15_set_readonly.py --off                      # 恢复全部可写
  python3 15_set_readonly.py --on --writable prod_write_burst,prod_delete_heavy
  python3 15_set_readonly.py --on --dry-run             # 只打印将要处理的索引
"""
import argparse
import json
import os
import sys
import urllib.error
import urllib.request

# 需要持续写入的索引（写画像）
DEFAULT_WRITABLE = ["prod_write_burst", "prod_delete_heavy"]
# 系统索引里保留可写的（Easysearch 自己要写）
SYSTEM_WRITABLE_PREFIX = (".infini", ".ilm", ".job-scheduler", ".security", ".tasks", ".geoip")


def http(method, url, body=None, timeout=60):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode(errors="replace")
            return r.status, (json.loads(raw) if raw[:1] in "[{" else raw)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="replace")[:300]


def list_indices(ep):
    st, out = http("GET", f"{ep}/_cat/indices?h=index&format=json")
    if st != 200:
        raise SystemExit(f"取索引列表失败: {st} {out}")
    return sorted(r["index"] for r in out)


def apply_settings(ep, indices, settings):
    if not indices:
        return
    # 索引名可能含点号/通配需求，逐个设置更稳（索引数量只有十几个）
    for name in indices:
        st, out = http("PUT", f"{ep}/{name}/_settings", {"index": settings})
        if st != 200:
            print(f"  [warn] {name}: {st} {str(out)[:120]}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", default=os.environ.get("BIZ_ENDPOINT", "http://127.0.0.1:9211"))
    ap.add_argument("--on", action="store_true", help="冻结只读索引")
    ap.add_argument("--off", action="store_true", help="恢复全部可写")
    ap.add_argument("--writable", default=",".join(DEFAULT_WRITABLE),
                    help="保持可写的索引，逗号分隔")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    ep = args.endpoint.rstrip("/")
    all_idx = list_indices(ep)
    writable = {x.strip() for x in args.writable.split(",") if x.strip()}

    readonly = []
    for name in all_idx:
        if name in writable:
            continue
        if name.startswith(".") and name.startswith(SYSTEM_WRITABLE_PREFIX):
            continue          # 系统索引不动
        readonly.append(name)

    if args.off:
        # 系统索引不碰：它们的 refresh/blocks 由 Easysearch 自己管理
        target = [n for n in all_idx
                  if not (n.startswith(".") and n.startswith(SYSTEM_WRITABLE_PREFIX))]
        print(f"恢复可写：{len(target)} 个索引（refresh_interval=1s），跳过系统索引 {len(all_idx) - len(target)} 个")
        if not args.dry_run:
            apply_settings(ep, target, {"blocks": {"write": False}, "refresh_interval": "1s"})
    elif args.on:
        print(f"保持可写（写画像）：{sorted(writable)}")
        print(f"设为只读：{readonly}")
        if not args.dry_run:
            apply_settings(ep, readonly, {"blocks": {"write": True}, "refresh_interval": "-1"})
    else:
        print("需要 --on 或 --off")
        return 1

    if not args.dry_run:
        st, rows = http("GET", f"{ep}/_cat/indices?h=index,status,docs.count&format=json&s=index")
        print("\n索引状态：")
        for r in rows:
            print(f"  {r['index']:26} {r.get('status',''):6} docs={r.get('docs.count','-')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
