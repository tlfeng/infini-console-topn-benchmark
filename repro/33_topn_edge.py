#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
33_topn_edge.py —— 视图 TopN 边界验证

1) 面积 / 颜色双指标：后端按面积截断、前端按颜色展示，验证成员与展示排序是否自洽
2) 分片页签：agentless 环境是否有 shard_stats 数据源
3) Top 值超过实体数时的行为（不做截断）

用法：
  python3 33_topn_edge.py --cluster topn-biz --type index
  python3 33_topn_edge.py --cluster topn-biz5 --type node
  python3 33_topn_edge.py --cluster topn-biz5 --type shard
"""
import argparse
import glob
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import importlib.util

# 复用 32 的 login/call/build_body/item_*/load_samples（文件名以数字开头，不能直接 import）
_spec = importlib.util.spec_from_file_location(
    'verify32', os.path.join(os.path.dirname(os.path.abspath(__file__)), '32_verify_topn.py'))
v = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(v)

BASE = os.path.dirname(os.path.abspath(__file__))
P = 'payload.elasticsearch.'

FIELDS = {
    'index': {
        'area': ('index_storage', P + 'index_stats.total.store.size_in_bytes'),
        'color': ('doc_count', P + 'index_stats.total.docs.count'),
        'samples': 'work/raw/stats_*.jsonl',
    },
    'node': {
        'area': ('index_storage', P + 'node_stats.indices.store.size_in_bytes'),
        'color': ('index_storage_proxy_os_cpu', P + 'node_stats.os.cpu.percent'),
        'samples': 'work/raw/nodes_*.jsonl',
    },
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--cluster', required=True)
    ap.add_argument('--type', choices=['index', 'node', 'shard'], required=True)
    ap.add_argument('--top', type=int, default=5)
    ap.add_argument('--warmup', type=int, default=30)
    ap.add_argument('--out', default='')
    args = ap.parse_args()
    top = args.top

    tok = v.login()
    report = {'cluster': args.cluster, 'type': args.type, 'top': top, 'checks': []}

    if args.type == 'shard':
        now = int(time.time() * 1000)
        st, resp = v.call('POST', '%s/elasticsearch/%s/visualization/data' % (v.CONSOLE, v.SYS),
                          v.build_body('shard', args.cluster,
                                       [v.item_rate('m1', P + 'shard_stats.indexing.index_total')],
                                       ['m1/{{.bucket_size_in_second}}'], top,
                                       now - 900000, now, '60s'), tok)
        docs = v.call('POST', os.environ.get('STORE', 'http://127.0.0.1:9200') + '/.infini_metrics/_count',
                      {'query': {'bool': {'filter': [
                          {'term': {'metadata.name': 'shard_stats'}},
                          {'term': {'metadata.labels.cluster_id': args.cluster}}]}}})
        n = (resp or {}).get('data') or []
        r = {'check': 'shard', 'api_status': st, 'items': len(n),
             'shard_stats_docs': docs[1].get('count') if isinstance(docs[1], dict) else None,
             'request': (resp or {}).get('request')}
        report['checks'].append(r)
        print('分片页签: API=%s 返回条目=%d shard_stats 文档数=%s' % (
            st, len(n), r['shard_stats_docs']))
        print('  DSL 片段:', json.dumps(json.loads(r['request']).get('aggs'), ensure_ascii=False)[:400]
              if r['request'] else '—')

    elif args.type in FIELDS:
        f = FIELDS[args.type]
        sp = max(glob.glob(os.path.join(BASE, f['samples'])), key=os.path.getmtime)
        rows = v.load_samples(sp)
        mn = rows[0]['ts'] + args.warmup * 1000
        mx = rows[-1]['ts']
        window_s = max(1, round((mx - mn) / 1000))
        print('采样: %s 窗口 %ds' % (sp, window_s))

        items = [v.item_instant('m_area', f['area'][1]), v.item_instant('m_color', f['color'][1])]
        formulas = ['m_area', 'm_color']

        # 请求 1：按面积截断（前端实际行为：sort key = areaID）
        st1, r1 = v.call('POST', '%s/elasticsearch/%s/visualization/data' % (v.CONSOLE, v.SYS),
                         v.build_body(args.type, args.cluster, items, formulas, top, mn, mx),
                         tok)
        # 请求 2：按颜色截断
        b2 = v.build_body(args.type, args.cluster, items, formulas, top, mn, mx)
        b2['sort'] = [{'direction': 'desc', 'key': 'm_color'}]
        st2, r2 = v.call('POST', '%s/elasticsearch/%s/visualization/data' % (v.CONSOLE, v.SYS), b2, tok)

        def collect(resp):
            out = {}
            for it in (resp or {}).get('data') or []:
                g = (it.get('groups') or [None])[0]
                val = it.get('value') or {}
                if isinstance(val, dict):
                    out[g] = (val.get('m_area'), val.get('m_color'))
            return out

        a = collect(r1)
        c = collect(r2)
        area_rank = sorted(a, key=lambda k: (a[k][0] is not None, a[k][0]), reverse=True)
        color_rank = sorted(c, key=lambda k: (c[k][1] is not None, c[k][1]), reverse=True)
        # 前端展示：成员=a 的集合，顺序按颜色
        displayed = sorted(a, key=lambda k: (a[k][1] is not None, a[k][1]), reverse=True)
        color_leader = color_rank[0] if color_rank else None
        r = {'check': 'area_vs_color',
             'area_key': f['area'][0], 'color_key': f['color'][0],
             'membership_by_area': area_rank,
             'membership_by_color': color_rank,
             'display_order_by_color': displayed,
             'color_leader_in_area_set': color_leader in a,
             'color_leader': color_leader,
             'rows': {k: {'area': a[k][0], 'color': a[k][1]} for k in a}}
        report['checks'].append(r)
        print('面积=%s 颜色=%s' % (f['area'][0], f['color'][0]))
        print('  按面积 Top%d（成员）: %s' % (top, area_rank))
        print('  按颜色 Top%d（真榜）: %s' % (top, color_rank))
        print('  颜色第 1 名是否在成员集内: %s（%s）' % (r['color_leader_in_area_set'], color_leader))
        for k in area_rank:
            print('    %-26s area=%-14s color=%s' % (k, a[k][0], a[k][1]))

        # Top 超过实体数
        st3, r3 = v.call('POST', '%s/elasticsearch/%s/visualization/data' % (v.CONSOLE, v.SYS),
                         v.build_body(args.type, args.cluster, items, formulas, 1000, mn, mx), tok)
        n3 = len(collect(r3))
        report['checks'].append({'check': 'top_gt_cardinality', 'returned': n3})
        print('  Top=1000 时返回实体数: %d' % n3)

    out = args.out or os.path.join(BASE, 'work/report/topn_view_edge_%s_%s_%s.json' % (
        args.type, args.cluster, time.strftime('%Y%m%d_%H%M%S')))
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, 'w', encoding='utf-8') as fh:
        json.dump(report, fh, ensure_ascii=False, indent=2)
    print('\n明细: %s' % out)
    return 0


if __name__ == '__main__':
    sys.exit(main())