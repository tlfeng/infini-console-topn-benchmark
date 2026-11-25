#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
36_rollup_ab_bench.py —— rollup A/B 基准：raw / 原生改写 / 直查产物 三路径对照

目的（与 35_rollup_seed.py 配套：35 造数+起作业，36 出结论）：
  量化「TopN 长窗改用 Console 自带 rollup」的两个维度：
  - 正确性：三条路径的 TopN 排序、逐桶峰值是否一致（含精确 ACL 与窗口边界行为）
  - 性能：同一窗口下三路径耗时（p50/p95，交错采样）

  三条路径：
    raw     关闭改写查源索引（等价 Console 现有 raw 路径查询形状，带逐样本 gte:0 过滤）
    native  同一 DSL + enable_rollup=true（Easysearch 自动改写，Console 改动最小）
    direct  手写缩写字段 DSL 直查 rollup 产物（最快，但 Console 要自维护翻译层）

硬性前提（2026-11-25 实测确认，违反即失真）：
  1. 窗口必须「整分钟闭合」：min 对齐分钟起点、max 对齐分钟结尾（本脚本自动对齐）。
     否则 rollup 边界分钟会把窗外样本计入，实测数值偏高 3e-5~6e-5 且可能换榜
     （--skip-edge 可关掉复现该现象的对照实验）。
  2. 原生改写命中条件：查询带作业过滤项（metadata.name）、size:0、单个顶层聚合、
     字段被作业收录、精确 terms ACL。通配符 ACL、指标数值过滤（gte:0）会让改写
     静默回退 raw——本脚本逐轮校验 origin，回退即告警。
  3. rollup 存 float32（~1e-7 相对噪声，容差内视为一致）；逐样本 gte:0 无法在
     rollup 侧表达，若指标可能有非法负值，rollup 榜单会包含 raw 会剔除的样本。

2026-11-25 实测基线（Easysearch 3.0.0 单节点，64 实体 × 10s × 30d = 16,588,800 条 →
rollup 2,764,800 条；8h 桶 ×90，Top20 + Top5 曲线，28 轮交错，p50/p95 毫秒）：
  raw     668/704（引擎 656/693）
  native  25/47（引擎 4/11）——比 raw 快约 27 倍
  direct  129/140（引擎 118/128）
  正确性：整分钟闭合下三路径排序与全部曲线点一致（1e-6 容差）

用法：
  # 冒烟（先跑 35 --seed --rollup --tag smoke）
  python3 36_rollup_ab_bench.py --tag smoke --store http://127.0.0.1:19222

  # 30 天完整基准（35 已跑过 --tag 30d 后）
  python3 36_rollup_ab_bench.py --tag 30d

  # 只验正确性，不跑耗时
  python3 36_rollup_ab_bench.py --tag 30d --rounds 0
"""
import argparse
import json
import os
import statistics
import time

STORE = os.environ.get('STORE', 'http://127.0.0.1:9200')
FIELD = 'payload.elasticsearch.index_stats.total.store.size_in_bytes'
ENT = 'metadata.labels.index_name'


def http(method, url, body=None, timeout=90):
    import urllib.request
    h = {}
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        h['Content-Type'] = 'application/json'
    req = urllib.request.Request(url, data=data, method=method, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        text = r.read().decode()
        return json.loads(text) if text.strip().startswith(('{', '[')) else text


def bucket_ms(text):
    unit = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}
    assert text[-1] in unit, f'不支持的桶宽: {text}'
    ms = int(text[:-1]) * unit[text[-1]] * 1000
    assert ms >= 60_000 and ms % 60_000 == 0, '桶宽必须是 ≥1m 的整数分钟倍'
    return ms


def load_cfg(args):
    mpath = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         'work', 'rollup', f'rollup_{args.tag}.json')
    with open(mpath, encoding='utf-8') as f:
        m = json.load(f)
    store = args.store or m.get('store') or STORE
    tag = args.tag
    cfg = {
        'src': args.src or m.get('src') or f'topn_rollup_src_{tag}',
        'dst': args.dst or m.get('dst') or f'rollup_topn_{tag}',
        'job': args.job or m.get('job') or f'rollup_topn_{tag}',
        'cluster': args.cluster or m.get('cluster') or f'rollup_{tag}',
        'entity_prefix': args.entity_prefix or m.get('entity_prefix') or f'rollup_{tag}-e',
        'entities': args.entities or m['entities'],
        'field': args.field,
        'bucket': args.bucket,
        'top': args.top,
        'curves': args.curves,
    }
    start = args.start or m['start']
    end = args.end or m['end']
    assert start % 60_000 == 0 and end % 60_000 == 0, 'manifest 的 start/end 必须分钟对齐（35 已保证）'
    # 整分钟闭合窗口：end 是最后一分钟「起点」，+59999 才是它的结尾。
    cfg.update(start=start, end_full=end + 59_999, end_partial=end, store=store)
    # rollup 指标缩写：从产物 mapping 的 _meta 反查（作业创建时按字段展开顺序编号）。
    mapping = http('GET', f'{store}/{cfg["dst"]}/_mapping')
    rollups = next(iter(mapping.values()))['mappings']['_meta']['rollups']
    meta = rollups[cfg['job']]
    cfg['abbr'] = next(x['target_field'] for x in meta['metrics'] if x['source_field'] == cfg['field'])
    return cfg


def entity_name(cfg, i):
    return f'{cfg["entity_prefix"]}{i:03d}'


def search(cfg, store, route, kind, start, end, names=None, acl_names=None):
    direct = route == 'direct'
    sfx = '.terms' if direct else ''
    ent = ENT + sfx
    ts = 'timestamp.date_histogram' if direct else 'timestamp'
    metric = f'{cfg["abbr"]}.max' if direct else cfg['field']
    filters = [
        {'term': {f'metadata.labels.cluster_id{sfx}': cfg['cluster']}},
        {'term': {f'metadata.category{sfx}': 'elasticsearch'}},
        {'range': {ts: {'gte': start, 'lte': end}}},
    ]
    if direct:
        filters.append({'term': {'rollup._id': cfg['job']}})
    if acl_names:
        filters.append({'terms': {ent: acl_names}})
    if names:
        filters.append({'terms': {ent: names}})
    if route == 'raw':
        filters.append({'range': {cfg['field']: {'gte': 0}}})
    aggs = {'peak': {'max': {'field': metric}}}
    if kind == 'rank':
        terms = {'field': ent, 'size': cfg['top'], 'shard_size': cfg['top'],
                 'order': [{'peak': 'desc'}, {'_key': 'asc'}]}
    else:
        terms = {'field': ent, 'size': max(len(names) if names else cfg['entities'], 1),
                 'order': {'_key': 'asc'}}
        aggs = {'curve': {'date_histogram': {'field': ts, 'fixed_interval': cfg['bucket'],
                                             'min_doc_count': 1}, 'aggs': aggs}}
    must = [] if direct else [{'term': {'metadata.name': 'index_stats'}}]
    body = {'size': 0, 'track_total_hits': False,
            'query': {'bool': {'must': must, 'filter': filters,
                               'must_not': [{'term': {ent: '_all'}}]}},
            'aggs': {'entities': {'terms': terms, 'aggs': aggs}}}
    idx = cfg['dst'] if direct else cfg['src']
    url = f'{store}/{idx}/_search?enable_rollup={"true" if route == "native" else "false"}&request_cache=false'
    if route == 'native':
        url += '&debug=true'
    r = http('POST', url, body)
    assert not r['timed_out'] and r['_shards']['failed'] == 0, str(r)[:300]
    rows = r['aggregations']['entities']['buckets']
    if kind == 'rank':
        data = [(x['key'], x['peak']['value']) for x in rows]
    else:
        data = {x['key']: [(p['key'], p['peak']['value']) for p in x['curve']['buckets']] for x in rows}
    return data, r.get('took', 0), r.get('origin')


def peak(cfg, store, route, start, end, acl_names=None):
    rank, t1, o1 = search(cfg, store, route, 'rank', start, end, acl_names=acl_names)
    curves, t2, o2 = search(cfg, store, route, 'curve', start, end,
                            names=[k for k, _ in rank[:cfg['curves']]], acl_names=acl_names)
    return (rank, curves), t1 + t2, (o1, o2)


def close(a, v, tol):
    return abs(a - v) <= max(tol * max(abs(a), abs(v)), tol)


def compare(cfg, store, tag_label, start, end, acl_names, tol):
    base, _, _ = peak(cfg, store, 'raw', start, end, acl_names)
    summary = []
    for route in ('native', 'direct'):
        other, took, origins = peak(cfg, store, route, start, end, acl_names)
        bad_rank = sum(not close(v1, v2, tol)
                       for (_, v1), (_, v2) in zip(base[0], other[0]))
        same_order = [k for k, _ in base[0]] == [k for k, _ in other[0]]
        bad_pts = 0
        total_pts = 0
        for name, rows in base[1].items():
            orows = other[1].get(name, [])
            assert [k for k, _ in rows] == [k for k, _ in orows], (route, name, len(rows), len(orows))
            total_pts += len(rows)
            bad_pts += sum(not close(v1, v2, tol) for (_, v1), (_, v2) in zip(rows, orows))
        hit = all(o and str(o).startswith('rollup') for o in origins)
        summary.append((route, same_order, bad_rank, bad_pts, total_pts, took, hit))
        note = 'origin=rollup ✓' if route == 'native' and hit else (
            '⚠ 原生改写未命中（回退 raw）' if route == 'native' else 'direct 直查产物，无 origin')
        print(f'  [{tag_label}] {route:6s} 排序一致={same_order} 数值超差: 榜单 {bad_rank}/{len(base[0])}, '
              f'曲线点 {bad_pts}/{total_pts}（容差 {tol:g}），引擎 took {took}ms，{note}')
        if bad_rank or bad_pts:
            for (_, v1), (_, v2) in list(zip(base[0], other[0]))[:3]:
                print(f'    示例: raw={v1!r} {route}={v2!r}')
    return base, summary


def edge_demo(cfg, store):
    """复现边界陷阱：窗口末端只对齐到分钟起点（不闭合）时，直查会算入整分钟数据。"""
    print(f'边界对照（故意违反整分钟闭合，end={cfg["end_partial"]}）：')
    raw, _, _ = peak(cfg, store, 'raw', cfg['start'], cfg['end_partial'])
    direct, _, _ = peak(cfg, store, 'direct', cfg['start'], cfg['end_partial'])
    rel = []
    for (_, v1), (_, v2) in zip(raw[0], direct[0]):
        if v1 != v2:
            rel.append(abs(v2 - v1) / max(abs(v1), 1e-9))
    flip = [k for k, _ in raw[0][:cfg['curves']]] != [k for k, _ in direct[0][:cfg['curves']]]
    worst = max(rel) if rel else 0
    print(f'  实体数值漂移 {len(rel)}/{len(raw[0])}，最大相对偏差 {worst:.2e}，'
          f'Top{cfg["curves"]} 榜单{"发生变化 ⚠" if flip else "未变"}')


def bench(cfg, store, acl_names, warmup, rounds, deadline):
    routes = ('raw', 'native', 'direct')
    wall = {r: [] for r in routes}
    took = {r: [] for r in routes}
    fallback = 0
    t0 = time.time()
    for i in range(warmup + rounds):
        for r in (routes if i % 2 == 0 else routes[::-1]):
            s = time.perf_counter()
            _, tk, origins = peak(cfg, store, r, cfg['start'], cfg['end_full'], acl_names)
            elapsed = (time.perf_counter() - s) * 1000
            if i >= warmup:
                took[r].append(tk)
                wall[r].append(elapsed)
                if r == 'native' and not all(o and str(o).startswith('rollup') for o in origins):
                    fallback += 1
        if i >= warmup + 1 and time.time() > t0 + deadline:
            print(f'  达到 {deadline}s 上限，提前结束（每路径已记录 {len(took["raw"])} 轮）')
            break

    def fmt(xs):
        xs = sorted(xs)
        if not xs:
            return 'n/a'
        return f'{statistics.median(xs):.0f}/{xs[max(0, int(len(xs) * 0.95) - 1)]:.0f}'

    print(f'耗时（交错 {len(took["raw"])} 轮，p50/p95 毫秒；rank Top{cfg["top"]} + Top{cfg["curves"]} 曲线）：')
    for r in routes:
        print(f'  {r:6s} 墙钟 {fmt(wall[r])}，引擎 took {fmt(took[r])}')
    if fallback:
        print(f'  ⚠ native 有 {fallback} 轮未命中改写。常见原因：窗口整体晚于 rollup.hours_before'
              f'（冒烟窗口即如此，属预期，无历史可改写）、通配符 ACL、指标数值过滤、DSL 形状不匹配。')


def main():
    ap = argparse.ArgumentParser(description='rollup A/B 基准：raw / 原生改写 / 直查产物 三路径对照')
    ap.add_argument('--tag', default='cont', help='读 35 的 manifest（work/rollup/rollup_<tag>.json）')
    ap.add_argument('--store', default=None, help='监控存储地址（默认取 manifest）')
    ap.add_argument('--src', default=None, help='源索引覆盖（默认 manifest 或 35 命名）')
    ap.add_argument('--dst', default=None, help='rollup 目标索引覆盖')
    ap.add_argument('--job', default=None, help='rollup 作业 ID 覆盖')
    ap.add_argument('--entity-prefix', default=None, help='实体名前缀覆盖')
    ap.add_argument('--cluster', default=None, help='cluster_id 覆盖')
    ap.add_argument('--entities', type=int, default=0, help='实体数覆盖（默认取 manifest）')
    ap.add_argument('--start', type=int, default=0, help='窗口起点毫秒覆盖（默认取 manifest）')
    ap.add_argument('--end', type=int, default=0, help='窗口分钟点覆盖（默认取 manifest）')
    ap.add_argument('--field', default=FIELD, help='峰值指标字段')
    ap.add_argument('--bucket', default='8h', help='图表桶宽（≥1m 的整数分钟倍）')
    ap.add_argument('--top', type=int, default=20, help='榜单长度')
    ap.add_argument('--curves', type=int, default=5, help='取前 N 名画曲线')
    ap.add_argument('--acl-n', type=int, default=10, help='精确 ACL 实体数（前 N 个，0=不做 ACL 对照）')
    ap.add_argument('--tol', type=float, default=1e-6, help='数值相对容差')
    ap.add_argument('--warmup', type=int, default=2, help='预热轮次')
    ap.add_argument('--rounds', type=int, default=20, help='计时轮次（0=只验正确性）')
    ap.add_argument('--deadline', type=int, default=300, help='计时阶段秒数上限')
    ap.add_argument('--skip-edge', action='store_true', help='跳过边界陷阱对照')
    args = ap.parse_args()
    bucket_ms(args.bucket)
    cfg = load_cfg(args)
    print(f'源 {cfg["src"]}（{cfg["entities"]} 实体）→ 目标 {cfg["dst"]}；'
          f'窗口 {cfg["start"]} ~ {cfg["end_full"]}（整分钟闭合），桶宽 {cfg["bucket"]}')

    if not args.skip_edge:
        edge_demo(cfg, cfg['store'])

    print('正确性（整分钟闭合窗口）：')
    acl_names = [entity_name(cfg, i) for i in range(min(args.acl_n, cfg['entities']))] if args.acl_n else None
    compare(cfg, cfg['store'], '全量', cfg['start'], cfg['end_full'], None, args.tol)
    if acl_names:
        compare(cfg, cfg['store'], f'ACL({len(acl_names)})', cfg['start'], cfg['end_full'], acl_names, args.tol)

    if args.rounds > 0:
        bench(cfg, cfg['store'], acl_names, args.warmup, args.rounds, args.deadline)


if __name__ == '__main__':
    main()
