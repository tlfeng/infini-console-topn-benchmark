#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
34_raw_long_window.py —— raw 长窗合成数据测试（确认桶宽 / 断档行为）

目的（对应调查结论第一期）：
  raw 路径的 rate/latency 依赖「严格相邻且等宽的时间桶」配对（counterPoint：
  current.Time - previous.Time == width）。本环境真实数据只有两段、中间断档，
  导致 30 天窗口下大面积出空，且桶宽变化会让结果在 0/有数据之间跳变。

  本脚本用「连续 30 天合成数据」把变量隔离出来：
    - 连续数据 + 任意桶宽：应当稳定出数（证明桶宽本身不是问题）
    - 人为断档 + 同一桶宽：应当出空/掉点（量化断档对出数的影响）
  从而在调整算法前，先有一份可重复、可对照的出数基线。

数据流（不经过 Console 采集，直接灌监控存储 .infini_metrics，避免时序依赖）：
  造数 -> store(.infini_metrics) -> Console GET index_metrics?advanced_topn=true -> 解析 lines

用法：
  # 1) 冒烟自检（造最近 6h、15min 间隔，只测 900s 桶）；确认写入/查询/配对链路通
  python3 34_raw_long_window.py --seed --days 0.25 --interval 900 --buckets 900s

  # 2) 完整连续 30 天，测多桶宽
  python3 34_raw_long_window.py --seed --days 30 --interval 900 \
      --buckets 900s,1h,3h,8h,1d

  # 3) 断档对照：连续 30 天但中间挖掉 10 天（第 10~20 天无数据）
  python3 34_raw_long_window.py --seed --days 30 --interval 900 \
      --gap-start 10 --gap-days 10 --buckets 900s,3h,8h,1d --tag gap10

  # 4) 清理本脚本造的数据（按 index_name 前缀 + cluster_id 精确删）
  python3 34_raw_long_window.py --cleanup

说明：
  - 用已纳管集群 topn-biz5（admin 全局权限可查），合成索引用前缀 synth30d- 隔离，
    查询后按 label 前缀过滤，只统计本脚本造的曲线，不污染真实数据结论。
  - index_total / query_total / query_time_in_millis 单调递增，保证任意相邻观测
    都能算出正 rate 和正 latency；每 15min 固定增长，便于核对数值。

2026-11-24 实测基线（store 9200 + console 9000，30 天窗口，15min 采样，5 实体）：
  A. 连续数据（无断档）
     900s / 1h  -> topn_status=budget_exceeded，0 曲线（细桶在 30 天窗口撞预算上限）
     3h  -> 5 曲线，单线 240 点（理论 239）  满点OK
     8h  -> 5 曲线，单线  90 点（理论  89）  满点OK
     1d  -> 5 曲线，单线  30 点（理论  29）  满点OK
     => 连续数据下粗桶算法完全正常，点数与理论吻合。

  B. 断档 10 天（窗口中间 11-04~11-14 无数据）
     900s / 1h  -> 仍然 budget_exceeded 空（与断档无关，是桶数预算）
     3h  -> 单线 161 点（连续时 240，少 ~79 ≈ 10天/3h）
     8h  -> 单线  61 点（连续时  90，少 ~29 ≈ 10天/8h）
     1d  -> 单线  21 点（连续时  30，少  ~9 ≈ 10天/1d）
     => 断档只造成掉点，粗桶不会因断档整体归零。

  结论：30 天 raw 出空有两条独立原因——
    (1) 细桶(<=1h)的 budget_exceeded 预算上限（与数据是否连续无关）；
    (2) 真实环境历史 sealed 桶内缺少计数器变化，latency 无法配对（需真实数据侧验证）。
  连续合成数据证明 counterPoint 的相邻等宽桶算法本身在粗桶下工作正常；
  真正要修的是「预算导致的空」和「无配对时的明确 topn_status」，而非相邻配对逻辑。
"""
import argparse
import datetime as dt
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

# ----------------------------------------------------------------- 默认配置
STORE = os.environ.get('STORE', 'http://127.0.0.1:9200')
CONSOLE = os.environ.get('CONSOLE', 'http://127.0.0.1:9000')
CLUSTER = os.environ.get('CLUSTER', 'topn-biz5')
RAW_INDEX = '.infini_metrics'
BASE_PREFIX = 'synth30d-'     # 合成索引名基础前缀
# 每个 tag 用独立前缀 synth30d-<tag>-，使 cleanup 能按 tag 精确删，互不干扰。
def prefix_for(tag):
    return f'{BASE_PREFIX}{tag}-'
METRIC = 'index_stats'
ENTITY_FIELD = 'metadata.labels.index_name'

# 合成实体：名字 + 每 15min 的写入增量（doc/s 各不同，便于看 TopN 排序）
# 实际 index_name = prefix_for(tag) + 这里的基础名，如 synth30d-test30d-hot
ENTITIES = [
    ('hot',   120000),
    ('mid',    60000),
    ('low',    24000),
    ('slow',    6000),
    ('tiny',    1200),
]
# latency: 每 15min query_time_in_millis 增量 = query_total 增量 * 该值(ms)
LATENCY_MS = 12.5


def http(method, url, body=None, headers=None, timeout=60, raw=False):
    h = dict(headers or {})
    data = None
    if body is not None:
        if isinstance(body, (dict, list)):
            data = json.dumps(body).encode()
            h['Content-Type'] = 'application/json'
        else:
            data = body.encode()
    req = urllib.request.Request(url, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            out = r.read().decode(errors='replace')
            return r.status, (out if raw else _maybe_json(out))
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors='replace')[:1000]
    except Exception as e:
        return -1, str(e)


def _maybe_json(s):
    try:
        return json.loads(s)
    except Exception:
        return s


def login():
    st, out = http('POST', CONSOLE + '/account/login',
                   {'username': os.environ.get('CONSOLE_USER', 'admin'),
                    'password': os.environ.get('CONSOLE_PASS', 'Admin@12345')})
    if st != 200 or not isinstance(out, dict) or 'access_token' not in out:
        raise SystemExit(f'登录 Console 失败: {st} {out}')
    return out['access_token']


def align_ms(ts_ms, interval_s):
    step = interval_s * 1000
    return ts_ms // step * step


def doc_for(name, ts_ms, idx, idx_total, query_total, qtime_ms, store_bytes, docs,
            scroll_total=0, scroll_time_ms=0, merge_total=0, merge_time_ms=0):
    """构造一条 index_stats 指标文档，字段路径对齐真实采集文档。

    scroll / merges 计数器默认 0（与真实环境一致）；只有显式传入递增值时，
    scroll_latency / merge_latency 才能配对出点——这正是验证"这两个指标出数与否
    取决于数据侧计数器是否增长"的开关。
    """
    return {
        'timestamp': ts_ms,
        'metadata': {
            'name': METRIC,
            'category': 'elasticsearch',
            'datatype': 'snapshot',
            'labels': {
                'cluster_id': CLUSTER,
                'cluster_uuid': '',
                'index_id': f'{CLUSTER}:{name}',
                'index_name': name,
                'index_uuid': 'synth-uuid-' + name,
            },
        },
        'payload': {
            'elasticsearch': {
                'index_stats': {
                    'index_info': {'index': name, 'status': 'open', 'health': 'green',
                                   'shards': 1, 'replicas': 0},
                    'total': {
                        'docs': {'count': docs, 'deleted': 0},
                        'store': {'size_in_bytes': store_bytes},
                        'search': {
                            'query_total': query_total,
                            'query_time_in_millis': qtime_ms,
                            'fetch_total': 0, 'fetch_time_in_millis': 0,
                            'scroll_total': scroll_total,
                            'scroll_time_in_millis': scroll_time_ms,
                        },
                        'merges': {
                            'total': merge_total,
                            'total_time_in_millis': merge_time_ms,
                            'current': 0, 'total_docs': 0,
                        },
                    },
                    'primaries': {
                        'docs': {'count': docs, 'deleted': 0},
                        'store': {'size_in_bytes': store_bytes},
                        'indexing': {
                            'index_total': idx_total,
                            'index_time_in_millis': idx_total * LATENCY_MS,
                            'delete_total': 0, 'index_failed': 0,
                        },
                        'search': {
                            'query_total': query_total,
                            'query_time_in_millis': qtime_ms,
                        },
                    },
                }
            }
        },
    }


def seed(args):
    interval = args.interval
    end = align_ms(int(time.time() * 1000), interval) - interval   # 留一个间隔余量
    start = end - int(args.days * 86400 * 1000)
    start = align_ms(start, interval)
    n_steps = int((end - start) / (interval * 1000)) + 1
    gap_lo = gap_hi = None
    if args.gap_days > 0:
        gd = args.gap_start
        gap_lo = start + int(gd * 86400 * 1000)
        gap_hi = gap_lo + int(args.gap_days * 86400 * 1000)

    prefix = prefix_for(args.tag)
    print(f'[seed] cluster={CLUSTER} days={args.days} interval={interval}s '
          f'steps={n_steps} entities={len(ENTITIES)} prefix={prefix}')
    print(f'[seed] window start={dt.datetime.fromtimestamp(start/1000):%Y-%m-%d %H:%M} '
          f'end={dt.datetime.fromtimestamp(end/1000):%Y-%m-%d %H:%M}')
    if gap_lo:
        print(f'[seed] GAP {dt.datetime.fromtimestamp(gap_lo/1000):%Y-%m-%d %H:%M} -> '
              f'{dt.datetime.fromtimestamp(gap_hi/1000):%Y-%m-%d %H:%M}')

    lines = []
    ts_list = []
    for i in range(n_steps):
        ts = start + i * interval * 1000
        if gap_lo is not None and gap_lo <= ts < gap_hi:
            continue
        ts_list.append(ts)
        for base_name, per_step in ENTITIES:
            name = prefix + base_name          # synth30d-<tag>-hot 等，按 tag 隔离
            idx_total = per_step * (i + 1)
            q_total = int(per_step * 0.6) * (i + 1)
            qtime = int(q_total * LATENCY_MS)
            store = 10 * 1024 * 1024 + idx_total * 512
            docs = 1000 + idx_total
            # scroll/merge 计数器：默认 0（对齐真实环境的"暂无数据"）；
            # --scroll-merge 时造单调递增，用于验证这两指标能正常出数。
            sw = sm = mw = mm = 0
            if args.scroll_merge:
                s_total = int(per_step * 0.05) * (i + 1)        # scroll 次数
                sw = s_total
                sm = int(s_total * 8.0)                          # scroll 时延 ~8ms
                m_total = int(per_step * 0.02) * (i + 1)        # merge 次数
                mw = m_total
                mm = int(m_total * 25.0)                         # merge 时延 ~25ms
            doc = doc_for(name, ts, i, idx_total, q_total, qtime, store, docs,
                          scroll_total=sw, scroll_time_ms=sm,
                          merge_total=mw, merge_time_ms=mm)
            lines.append(json.dumps({'index': {'_index': RAW_INDEX,
                                               '_id': f'synth30d|{name}|{ts}'}},
                                    ensure_ascii=False))
            lines.append(json.dumps(doc, ensure_ascii=False))

    print(f'[seed] docs to write: {len(ts_list)} steps x {len(ENTITIES)} = {len(lines)//2}')
    batch = 2000  # 每批 1000 文档（2 行/文档）
    ok = 0
    for s in range(0, len(lines), batch):
        chunk = '\n'.join(lines[s:s + batch]) + '\n'
        st, resp = http('POST', f'{STORE}/{RAW_INDEX}/_bulk?refresh=false',
                        chunk, headers={'Content-Type': 'application/x-ndjson'},
                        timeout=120, raw=True)
        if st != 200 or (isinstance(resp, str) and '"errors":true' in resp.replace(' ', '')):
            raise SystemExit(f'[seed] bulk 失败: {st} {str(resp)[:500]}')
        ok += len(chunk.splitlines()) // 2
    http('POST', f'{STORE}/{RAW_INDEX}/_refresh')
    print(f'[seed] 写入完成 {ok} docs')

    # 落一个 manifest，probe 用同一窗口
    manifest = {'start': start, 'end': end, 'interval': interval,
                'gap_lo': gap_lo, 'gap_hi': gap_hi, 'prefix': prefix,
                'entities': [prefix + e[0] for e in ENTITIES]}
    mf = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                      f'work/raw/long_window_{args.tag}.json')
    os.makedirs(os.path.dirname(mf), exist_ok=True)
    with open(mf, 'w', encoding='utf-8') as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    print(f'[seed] manifest -> {mf}')
    return 0


BUCKET_SEC = {'900s': 900, '1h': 3600, '3h': 10800, '8h': 28800, '1d': 86400,
              '15m': 900, '30m': 1800, '6h': 21600, '12h': 43200}


def bucket_to_sec(b):
    return BUCKET_SEC.get(b)


def probe(args):
    mf = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                      f'work/raw/long_window_{args.tag}.json')
    if not os.path.isfile(mf):
        raise SystemExit(f'[probe] 找不到 manifest: {mf}，先跑 --seed')
    with open(mf, encoding='utf-8') as f:
        m = json.load(f)
    tok = login()

    keys = [k.strip() for k in args.keys.split(',') if k.strip()]
    buckets = [b.strip() for b in args.buckets.split(',') if b.strip()]
    print(f'[probe] cluster={CLUSTER} keys={keys} buckets={buckets} top={args.top}')
    print(f'[probe] window {dt.datetime.fromtimestamp(m["start"]/1000):%m-%d %H:%M} -> '
          f'{dt.datetime.fromtimestamp(m["end"]/1000):%m-%d %H:%M}\n')

    results = []
    for b in buckets:
        sec = bucket_to_sec(b)
        if sec is None:
            print(f'  [skip] 未知桶宽 {b}')
            continue
        for key in keys:
            qs = urllib.parse.urlencode({
                'key': key, 'advanced_topn': 'true',
                'min': m['start'], 'max': m['end'],
                'bucket_size': f'{sec}s', 'top': args.top, 'timeout': '120s',
            })
            url = f'{CONSOLE}/elasticsearch/{CLUSTER}/index_metrics?{qs}'
            t0 = time.time()
            st, resp = http('GET', url, headers={'Authorization': 'Bearer ' + tok}, timeout=180)
            dt_ms = int((time.time() - t0) * 1000)
            lines = []
            status = None
            if isinstance(resp, dict):
                status = resp.get('topn_status')
                metric = (resp.get('metrics') or {}).get(key) or {}
                lines = metric.get('lines') or []
            # 只统计本 tag 造的合成曲线（前缀从 manifest 读，兼容旧产物）
            prefix = m.get('prefix', BASE_PREFIX)
            synth = [ln for ln in lines
                     if str(((ln.get('metric') or {}).get('label') or '')).startswith(prefix)]
            pts = [len(ln.get('data') or []) for ln in synth]
            results.append({
                'bucket': b, 'key': key, 'http': st, 'topn_status': status,
                'all_lines': len(lines), 'synth_lines': len(synth),
                'synth_points': sum(pts), 'per_entity': pts, 'ms': dt_ms,
            })
            print(f'  bucket={b:>5} key={key:<14} HTTP={st:<3} status={str(status):<20} '
                  f'synth曲线={len(synth):<2} 点数={sum(pts):<6} ({dt_ms}ms)')

    # 期望点数（连续数据理论值），用于判定
    span_s = (m['end'] - m['start']) // 1000
    gap_s = ((m['gap_hi'] - m['gap_lo']) // 1000) if m.get('gap_lo') else 0
    print('\n' + '=' * 90)
    print('桶宽/断档行为对比（synth30d-* 合成曲线，连续数据理论满点数 = 窗口/桶宽 - 1）')
    print('=' * 90)
    hdr = f'{"桶宽":>6} {"指标":<14} {"HTTP":>4} {"topn_status":<17} {"曲线":>4} {"单线点数":>8} {"理论满点":>9} {"判定":<9}'
    print(hdr)
    print('-' * 96)
    for r in results:
        sec = bucket_to_sec(r['bucket'])
        full_pts = max(0, span_s // sec - 1)          # 单条曲线理论点数
        per = r['per_entity'] or []
        per_line = min(per) if per else 0            # 单条曲线实际点数（取最少的）
        if gap_s:
            expect_lo = max(0, full_pts - gap_s // sec - 2)
            mark = f'~{expect_lo}-{full_pts}'
        else:
            mark = str(full_pts)
        if r['http'] != 200:
            verdict = 'HTTP错误'
        elif r['synth_lines'] == 0:
            verdict = ('预算空' if str(r['topn_status']) == 'budget_exceeded' else '空(0)')
        elif per_line >= full_pts * 0.8:
            verdict = '满点OK'
        elif per_line >= full_pts * 0.3:
            verdict = '点偏少'
        else:
            verdict = '稀疏'
        print(f'{r["bucket"]:>6} {r["key"]:<14} {r["http"]:>4} '
              f'{str(r["topn_status"]):<17} {r["synth_lines"]:>4} {per_line:>8} '
              f'{mark:>9} {verdict:<9}')

    out = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                       f'work/report/raw_long_window_{args.tag}.json')
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, 'w', encoding='utf-8') as f:
        json.dump({'manifest': m, 'results': results}, f, ensure_ascii=False, indent=2)
    print(f'\n明细 -> {out}')
    return 0


def cleanup(args):
    prefix = prefix_for(args.tag)
    q = {'query': {'bool': {'filter': [
        {'prefix': {ENTITY_FIELD: prefix}},
        {'term': {'metadata.labels.cluster_id': CLUSTER}},
    ]}}}
    print(f'[cleanup] 删除 {ENTITY_FIELD} 前缀={prefix} cluster={CLUSTER} 的指标文档')
    st, resp = http('POST', f'{STORE}/{RAW_INDEX}/_delete_by_query?refresh=true&conflicts=proceed',
                    q, timeout=120)
    print(f'[cleanup] HTTP={st} deleted={resp.get("deleted") if isinstance(resp, dict) else resp}')
    # 清 work 产物
    base = os.path.dirname(os.path.abspath(__file__))
    for p in [f'{base}/work/raw/long_window_{args.tag}.json',
              f'{base}/work/report/raw_long_window_{args.tag}.json']:
        if os.path.isfile(p):
            os.remove(p)
            print(f'[cleanup] removed {p}')
    return 0


def main():
    ap = argparse.ArgumentParser(description='raw 长窗合成数据测试（桶宽/断档行为）')
    ap.add_argument('--seed', action='store_true', help='灌入合成数据')
    ap.add_argument('--probe', action='store_true', help='跑多桶宽查询并输出对比')
    ap.add_argument('--cleanup', action='store_true', help='清理合成数据')
    ap.add_argument('--days', type=float, default=30, help='窗口天数（0.25=6h 冒烟）')
    ap.add_argument('--interval', type=int, default=900, help='采样间隔秒（默认 900=15min）')
    ap.add_argument('--gap-start', type=float, default=0, help='断档起始天（相对窗口起点）')
    ap.add_argument('--gap-days', type=float, default=0, help='断档持续天数')
    ap.add_argument('--buckets', default='900s,1h,3h,8h,1d', help='逗号分隔的桶宽')
    ap.add_argument('--keys', default='indexing_rate,query_latency', help='逗号分隔的指标 key')
    ap.add_argument('--scroll-merge', action='store_true',
                    help='造数时给 scroll/merge 计数器造递增值（默认 0，对齐真实环境的暂无数据）')
    ap.add_argument('--top', type=int, default=20, help='top N')
    ap.add_argument('--tag', default='cont', help='产物标签（区分连续/断档）')
    args = ap.parse_args()

    if not (args.seed or args.probe or args.cleanup):
        ap.print_help()
        return 1
    rc = 0
    if args.cleanup:
        rc |= cleanup(args)
    if args.seed:
        rc |= seed(args)
    if args.probe:
        rc |= probe(args)
    return rc


if __name__ == '__main__':
    sys.exit(main())
