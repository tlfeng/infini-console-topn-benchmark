#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
32_verify_topn.py —— 视图 TopN（/visualization/data）准确性验证

被测对象：WebUI 集群监控 -> TopN 页签 -> 节点 / 索引 / 分片
请求完全照抄前端 web/src/pages/Platform/Overview/components/TopN/index.jsx 的构造方式。

真值：12_sample.py（索引 _stats）/ 14_sample_nodes.py（节点 _nodes/stats）采样，
      按各指标自身口径（瞬时量取 max，速率取相邻采样差分峰值，时延取 Δtime/Δcount 峰值）。

用法：
  python3 32_verify_topn.py --cluster topn-biz5 --type node --top 5
  python3 32_verify_topn.py --cluster topn-biz  --type index --top 5
"""
import argparse
import glob
import json
import os
import sys
import time
import urllib.error
import urllib.request

CONSOLE = os.environ.get('CONSOLE', 'http://127.0.0.1:9000')
SYS = os.environ.get('SYS_CLUSTER', 'infini_default_system_cluster')
BASE = os.path.dirname(os.path.abspath(__file__))


def call(method, url, body=None, tok=None, timeout=240):
    data = json.dumps(body).encode() if body is not None else None
    h = {'Content-Type': 'application/json'}
    if tok:
        h['Authorization'] = 'Bearer ' + tok
    req = urllib.request.Request(url, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b'{}')
    except urllib.error.HTTPError as e:
        raw = e.read().decode(errors='replace')
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, raw[:1000]


def login():
    st, out = call('POST', CONSOLE + '/account/login',
                   {'username': os.environ.get('CONSOLE_USER', 'admin'),
                    'password': os.environ.get('CONSOLE_PASS', 'Admin@12345')})
    if st != 200:
        raise SystemExit('登录失败: %s' % out)
    return out['access_token']


# ---------------------------------------------------------------- 请求构造（照抄前端）

NAME_MAP = {'node': 'node_stats', 'index': 'index_stats', 'shard': 'shard_stats'}
GROUP_FIELD = {
    'node': 'metadata.labels.node_name',
    'index': 'metadata.labels.index_name',
    'shard': 'metadata.labels.shard_id',
}


def build_body(typ, cluster, items, formulas, top, mn, mx, bucket_size=None):
    must = [
        {'term': {'metadata.name': {'value': NAME_MAP[typ]}}},
        {'term': {'metadata.category': {'value': 'elasticsearch'}}},
        {'range': {'timestamp': {'gte': mn, 'lte': mx}}},
    ]
    must_not = []
    if typ == 'index':
        must_not.append({'term': {'metadata.labels.index_name': {'value': '_all'}}})
    body = {
        'index_pattern': '.infini_metrics*',
        'filter': {'bool': {'must': must, 'must_not': must_not,
                            'filter': [{'term': {'metadata.labels.cluster_id': cluster}}]}},
        'formulas': formulas,
        'items': items,
        'groups': [{'field': GROUP_FIELD[typ], 'limit': top}],
        'sort': [{'direction': 'desc', 'key': items[0]['name']}],
    }
    if bucket_size:
        body['time_field'] = 'timestamp'
        body['bucket_size'] = bucket_size
    return body


def item_instant(name, field):
    return {'name': name, 'function': {'max': {'field': field}}}


def item_rate(name, field):
    return {'name': name, 'function': {'rate': {'field': field, 'scale': 1}}}


def item_latency(name, dividend, divisor):
    return {'name': name, 'function': {'latency': {'dividend': dividend, 'divisor': divisor}}}


def run(tok, typ, cluster, items, formulas, top, mn, mx, bucket_size=None, sort_key=None):
    body = build_body(typ, cluster, items, formulas, top, mn, mx, bucket_size)
    if sort_key:
        body['sort'] = [{'direction': 'desc', 'key': sort_key}]
    st, resp = call('POST', '%s/elasticsearch/%s/visualization/data' % (CONSOLE, SYS), body, tok)
    if st != 200:
        return {'ok': False, 'status': st, 'resp': resp, 'body': body}
    formula = formulas[0]
    per_group = {}
    for it in resp.get('data') or []:
        g = (it.get('groups') or [None])[0]
        if not g:
            continue
        v = it.get('value')
        val = v.get(formula) if isinstance(v, dict) else v
        if isinstance(val, (int, float)):
            per_group.setdefault(g, []).append(float(val))
    names = sorted(per_group, key=lambda k: max(per_group[k]), reverse=True)
    return {'ok': True, 'status': 200, 'names': names,
            'values': {k: max(v) for k, v in per_group.items()},
            'points_per_group': {k: len(v) for k, v in per_group.items()},
            'dsl': resp.get('request'), 'body': body}


# ---------------------------------------------------------------- 真值

def dig(d, path):
    cur = d
    for p in path.split('.'):
        if not isinstance(cur, dict) or p not in cur:
            return None
        cur = cur[p]
    return cur


def load_samples(path):
    rows = [json.loads(l) for l in open(path, encoding='utf-8') if l.strip()]
    rows.sort(key=lambda r: r['ts'])
    return rows


def truth(rows, entries, top):
    out = {}
    scores_all = {}
    for key, mode, scope, field in entries:
        score = {}
        if mode == 'instant':
            for r in rows:
                for name, st in r['indices'].items():
                    v = dig(st.get(scope, {}), field)
                    if v is not None and v > score.get(name, float('-inf')):
                        score[name] = v
        elif mode == 'rate':
            for a, b in zip(rows, rows[1:]):
                dt = (b['ts'] - a['ts']) / 1000.0
                if dt <= 0:
                    continue
                for name in b['indices']:
                    v1 = dig(a['indices'].get(name, {}).get(scope, {}), field)
                    v2 = dig(b['indices'].get(name, {}).get(scope, {}), field)
                    if v1 is None or v2 is None or v2 < v1:
                        continue
                    rate = (v2 - v1) / dt
                    if rate > score.get(name, float('-inf')):
                        score[name] = rate
        else:  # latency
            f_num, f_den = field if isinstance(field, tuple) else field.split('|')
            for a, b in zip(rows, rows[1:]):
                for name in b['indices']:
                    n1 = dig(a['indices'].get(name, {}).get(scope, {}), f_num)
                    n2 = dig(b['indices'].get(name, {}).get(scope, {}), f_num)
                    d1 = dig(a['indices'].get(name, {}).get(scope, {}), f_den)
                    d2 = dig(b['indices'].get(name, {}).get(scope, {}), f_den)
                    if None in (n1, n2, d1, d2):
                        continue
                    dc = d2 - d1
                    if dc <= 0 or n2 < n1:
                        continue
                    lat = (n2 - n1) / dc
                    if lat > score.get(name, float('-inf')):
                        score[name] = lat
        out[key] = [k for k, _ in sorted(score.items(), key=lambda kv: kv[1], reverse=True)[:top]]
        scores_all[key] = {k: float(v) for k, v in score.items()}
    return out, scores_all


def truth_nodes(rows, entries, top):
    out = {}
    scores_all = {}
    for key, mode, field in entries:
        score = {}
        if mode == 'instant':
            for r in rows:
                for name, nd in r['nodes'].items():
                    v = dig(nd, field)
                    if v is not None and v > score.get(name, float('-inf')):
                        score[name] = v
        elif mode == 'rate':
            for a, b in zip(rows, rows[1:]):
                dt = (b['ts'] - a['ts']) / 1000.0
                if dt <= 0:
                    continue
                for name in b['nodes']:
                    v1, v2 = dig(a['nodes'][name], field), dig(b['nodes'][name], field)
                    if v1 is None or v2 is None or v2 < v1:
                        continue
                    rate = (v2 - v1) / dt
                    if rate > score.get(name, float('-inf')):
                        score[name] = rate
        else:
            f_num, f_den = field if isinstance(field, tuple) else field.split('|')
            for a, b in zip(rows, rows[1:]):
                for name in b['nodes']:
                    n1, n2 = dig(a['nodes'][name], f_num), dig(b['nodes'][name], f_num)
                    d1, d2 = dig(a['nodes'][name], f_den), dig(b['nodes'][name], f_den)
                    if None in (n1, n2, d1, d2):
                        continue
                    dc = d2 - d1
                    if dc <= 0 or n2 < n1:
                        continue
                    lat = (n2 - n1) / dc
                    if lat > score.get(name, float('-inf')):
                        score[name] = lat
        out[key] = [k for k, _ in sorted(score.items(), key=lambda kv: kv[1], reverse=True)[:top]]
        scores_all[key] = {k: float(v) for k, v in score.items()}
    return out, scores_all


# ---------------------------------------------------------------- 指标表

P = 'payload.elasticsearch.'
IDX = [
    ('index_storage', 'instant', 'total', 'store.size_in_bytes',
     P + 'index_stats.total.store.size_in_bytes'),
    ('doc_count', 'instant', 'total', 'docs.count',
     P + 'index_stats.total.docs.count'),
    ('segment_count', 'instant', 'total', 'segments.count',
     P + 'index_stats.total.segments.count'),
    ('docs_deleted', 'instant', 'total', 'docs.deleted',
     P + 'index_stats.total.docs.deleted'),
    ('indexing_rate', 'rate', 'primaries', 'indexing.index_total',
     P + 'index_stats.primaries.indexing.index_total'),
    ('query_times', 'rate', 'total', 'search.query_total',
     P + 'index_stats.total.search.query_total'),
    ('query_latency', 'latency', 'total', 'search.query_time_in_millis|search.query_total',
     (P + 'index_stats.total.search.query_time_in_millis',
      P + 'index_stats.total.search.query_total')),
]
NODE = [
    ('index_storage', 'instant', 'indices.store.size_in_bytes',
     P + 'node_stats.indices.store.size_in_bytes'),
    ('segment_count', 'instant', 'indices.segments.count',
     P + 'node_stats.indices.segments.count'),
    ('docs_count', 'instant', 'indices.docs.count',
     P + 'node_stats.indices.docs.count'),
    ('os_cpu', 'instant', 'os.cpu.percent',
     P + 'node_stats.os.cpu.percent'),
    ('os_used_mem', 'instant', 'os.mem.used_percent',
     P + 'node_stats.os.mem.used_percent'),
    ('jvm_heap', 'instant', 'jvm.mem.heap_used_percent',
     P + 'node_stats.jvm.mem.heap_used_percent'),
    ('indexing_rate', 'rate', 'indices.indexing.index_total',
     P + 'node_stats.indices.indexing.index_total'),
    ('query_rate', 'rate', 'indices.search.query_total',
     P + 'node_stats.indices.search.query_total'),
    ('query_latency', 'latency', 'indices.search.query_time_in_millis|indices.search.query_total',
     (P + 'node_stats.indices.search.query_time_in_millis',
      P + 'node_stats.indices.search.query_total')),
]


def diff(shown, true, top):
    miss = [x for x in true if x not in shown]
    return {'miss': len(miss), 'miss_names': miss,
            'miss_leader': bool(true and (not shown or true[0] not in shown))}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--cluster', required=True)
    ap.add_argument('--type', choices=['node', 'index', 'shard'], required=True)
    ap.add_argument('--samples', default='')
    ap.add_argument('--top', type=int, default=5)
    ap.add_argument('--warmup', type=int, default=30)
    ap.add_argument('--out', default='')
    args = ap.parse_args()
    top = args.top

    pat = 'work/raw/nodes_*.jsonl' if args.type == 'node' else 'work/raw/stats_*.jsonl'
    sp = args.samples or max(glob.glob(os.path.join(BASE, pat)), key=os.path.getmtime)
    rows = load_samples(sp)
    if len(rows) < 3:
        raise SystemExit('采样点不足: %s' % sp)
    mn = rows[0]['ts'] + args.warmup * 1000
    mx = rows[-1]['ts']
    rows = [r for r in rows if mn <= r['ts'] <= mx]
    window_s = max(1, round((mx - mn) / 1000))
    print('采样: %s  点=%d  窗口=%s ~ %s (%ds)' % (
        sp, len(rows), time.strftime('%H:%M:%S', time.localtime(mn / 1000)),
        time.strftime('%H:%M:%S', time.localtime(mx / 1000)), window_s))

    tok = login()
    report = {'cluster': args.cluster, 'type': args.type, 'top': top,
              'window': [mn, mx], 'samples': sp, 'tests': []}

    if args.type == 'node':
        tru, tvals = truth_nodes(rows, [(k, m, s) for k, m, s, _f in NODE], top)
        table = NODE
    else:
        tru, tvals = truth(rows, [(k, m, s, f) for k, m, s, f, *_ in IDX], top)
        table = IDX

    for row in table:
        key, mode = row[0], row[1]
        if mode == 'instant':
            items = [item_instant('m1', row[-1])]
            formulas = ['m1']
            bs = None
        elif mode == 'rate':
            items = [item_rate('m1', row[-1])]
            formulas = ['m1/{{.bucket_size_in_second}}']
            bs = '%ds' % window_s
        else:
            items = [item_latency('m1', row[-1][0], row[-1][1])]
            formulas = ['m1']
            bs = '%ds' % window_s
        res = run(tok, args.type, args.cluster, items, formulas, top, mn, mx, bs)
        if not res['ok']:
            report['tests'].append({'key': key, 'status': 'API失败:%s' % res['status'],
                                    'resp': str(res['resp'])[:300]})
            print('%-16s API 失败 %s' % (key, res['status']))
            continue
        t = {'key': key, 'mode': mode, 'truth': tru.get(key, []),
             'shown': res['names'][:top], 'points_per_group': res['points_per_group'],
             'truth_values': {k: tvals.get(key, {}).get(k) for k in tru.get(key, [])},
             'shown_values': {k: res['values'].get(k) for k in res['names'][:top]}}
        t.update(diff(res['names'][:top], tru.get(key, []), top))
        report['tests'].append(t)
        flag = 'LEADER' if t['miss_leader'] else ('MISS' if t['miss'] else 'OK')
        pts = sorted(set(res['points_per_group'].values()) or {0})
        print('%-16s [%-6s] 点数/组=%s 图上=%s 真值=%s%s' % (
            key, flag, pts, t['shown'], t['truth'],
            (' 未上榜=' + str(t['miss_names']) if t['miss_names'] else '')))
        if t['miss']:
            for n in res['names'][:top]:
                print('    图上 %-26s %s' % (n, res['values'].get(n)))
            for n in tru.get(key, []):
                print('    真值 %-26s %s' % (n, tvals.get(key, {}).get(n)))

        # 对照组：速率/时延改用细桶，检验"整窗单桶"假设
        if mode in ('rate', 'latency'):
            res2 = run(tok, args.type, args.cluster, items, formulas, top, mn, mx, '30s')
            if res2['ok']:
                c = {'key': key + '@30s', 'shown': res2['names'][:top],
                     'points_per_group': res2['points_per_group'],
                     'truth': tru.get(key, [])}
                c.update(diff(res2['names'][:top], tru.get(key, []), top))
                report['tests'].append(c)
                flag2 = 'LEADER' if c['miss_leader'] else ('MISS' if c['miss'] else 'OK')
                print('  %-14s [%-6s] 点数/组=%s 图上=%s%s' % (
                    '(bucket=30s)', flag2,
                    sorted(set(res2['points_per_group'].values()) or {0}),
                    c['shown'],
                    (' 未上榜=' + str(c['miss_names']) if c['miss_names'] else '')))

    # 对照组：不被 limit 截断的完整排序，检验 terms 截断是否改变成员
    full_key = table[0][0]
    if table[0][1] == 'instant':
        items = [item_instant('m1', table[0][-1])]
        formulas = ['m1']
        bs = None
    elif table[0][1] == 'latency':
        # latency 的 field 是 (dividend, divisor) 元组，必须走 item_latency；
        # 否则把 tuple 传给 item_rate 会生成非法 DSL（此前 table[0] 恰好是 instant 才没触发）。
        div, den = table[0][-1]
        items = [item_latency('m1', div, den)]
        formulas = ['m1']
        bs = '%ds' % window_s
    else:
        items = [item_rate('m1', table[0][-1])]
        formulas = ['m1/{{.bucket_size_in_second}}']
        bs = '%ds' % window_s
    full = run(tok, args.type, args.cluster, items, formulas, 10000, mn, mx, bs)
    if full['ok']:
        c = {'key': full_key + '@limit=10000', 'shown': full['names'][:top],
             'total_groups': len(full['names']), 'truth': tru.get(full_key, [])}
        c.update(diff(full['names'][:top], tru.get(full_key, []), top))
        report['tests'].append(c)
        print('%-16s [完整排序] 组数=%d 前%d=%s' % (
            full_key, len(full['names']), top, c['shown']))

    out = args.out or os.path.join(BASE, 'work/report/topn_view_%s_%s_%s.json' % (
        args.type, args.cluster, time.strftime('%Y%m%d_%H%M%S')))
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print('\n明细: %s' % out)
    return 0


if __name__ == '__main__':
    sys.exit(main())