#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
35_rollup_seed.py —— 生产密度合成数据 + Console 自带 rollup 作业（rollup vs raw A/B 基座）

目的（补 34_raw_long_window.py 的盲区）：
  34 用 15min 采样验证 raw 桶宽/断档行为；但真实 .infini_metrics 默认 10s 采样，
  rollup 的压缩比和 raw 扫描代价只有在生产密度下才成立（10s × N 实体 → 1m rollup
  ≈ 6:1，900s 采样则约 1:1 毫无收益）。本脚本按生产密度造数，并一键创建/启动
  Console setup 自带的 index_stats rollup 作业（镜像 template_rollup.tpl 的
  rollup_index_stats），为「TopN 长窗改用 rollup 快路径」提供可重复的 A/B 基座。

  数据形状与 2026-11-25 的 30 天基准一致（64 实体 × 10s × 30d ≈ 1660 万条）：
    - inc = (1+i%5)*(step+1)，index_total/docs 单调递增，任意相邻观测可算正 rate
    - store = 1e7 + i*5e5 + inc*512 + 噪声，制造不同实体在不同时刻登顶
    - query_total = inc*0.6，query_time = query_total*12.5ms（名义延迟 12.5ms）

用法：
  # 冒烟：4 实体 × 10s × 30min，含 rollup 作业跑完
  python3 35_rollup_seed.py --seed --rollup --days 0.02 --interval 10 --entities 4 \
      --tag smoke --store http://127.0.0.1:19222

  # 30 天完整基座（约 1660 万条，灌数约 7 分钟 + rollup 约 10 分钟）
  python3 35_rollup_seed.py --seed --rollup --days 30 --interval 10 --entities 64 \
      --tag 30d --timeout 1800 --store http://127.0.0.1:19222

  # 只看作业进度 / 清理本脚本产物（按索引名精确删，不碰 .infini_metrics）
  python3 35_rollup_seed.py --status --tag 30d --store http://127.0.0.1:19222
  python3 35_rollup_seed.py --cleanup --tag 30d --store http://127.0.0.1:19222

踩坑说明：
  - 字符串字段必须显式 keyword（dynamic_templates）。动态默认 text 会让 rollup
    分组聚合直接报 fielddata 错误——真实 .infini_metrics 的映射本来就是 keyword。
  - 窗口整体早于 now-24h：rollup 引擎要求「窗口结束+1h」才处理，24h 同时是
    Easysearch rollup 查询切换阈值（rollup.hours_before），保证作业一次跑完、
    查询能命中 rollup 改写。
  - 独立具体索引，不入 .infini_metrics，避免污染 Console 真实结论。
"""
import argparse
import json
import os
import time
import urllib.error
import urllib.request

STORE = os.environ.get('STORE', 'http://127.0.0.1:9200')

# 镜像 Console config/setup/easysearch/template_rollup.tpl 的 rollup_index_stats。
# Console setup 渲染该模板创建同名作业；这里独立索引自建，配置保持一致以便对照。
JOB_TPL = {
    'timestamp': 'timestamp',
    'continuous': False,
    'cron': '* * * * *',
    'timezone': 'UTC',
    'interval': '1m',
    'delay': 0,
    'stats': [{'max': {}}, {'min': {}}, {'value_count': {}}],
    'identity': [
        'metadata.labels.cluster_id', 'metadata.labels.cluster_uuid',
        'metadata.labels.index_id', 'metadata.labels.index_name',
        'metadata.category', 'payload.elasticsearch.index_stats.index_info.health',
    ],
    'attributes': ['agent.*', 'metadata.*'],
    'metrics': ['payload.elasticsearch.index_stats.*'],
    'filter': {'metadata.name': 'index_stats'},
    'field_abbr': True,
}

DOC_TPL = ('{"timestamp":%d,"metadata":{"name":"index_stats","category":"elasticsearch",'
           '"labels":{"cluster_id":"%s","cluster_uuid":"%s-uuid","index_id":"id-%03d","index_name":"%s"}},'
           '"payload":{"elasticsearch":{"index_stats":{"index_info":{"health":"green"},'
           '"primaries":{"store":{"size_in_bytes":%d},"docs":{"count":%d},"indexing":{"index_total":%d},'
           '"search":{"query_total":%d,"query_time_in_millis":%d}},'
           '"total":{"store":{"size_in_bytes":%d},"docs":{"count":%d}}}}}}')
ACT = b'{"index":{}}\n'


def http(method, url, body=None, timeout=120, ndjson=False):
    """统一 HTTP，返回 (status, 解析后的 JSON 或原文本)。"""
    h = {}
    data = None
    if body is not None:
        if isinstance(body, (dict, list)):
            data = json.dumps(body).encode()
            h['Content-Type'] = 'application/json'
        else:
            data = body if isinstance(body, bytes) else body.encode()
            h['Content-Type'] = 'application/x-ndjson' if ndjson else 'application/json'
    req = urllib.request.Request(url, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            text = r.read().decode()
            return r.status, json.loads(text) if text.strip().startswith(('{', '[')) else text
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:500]


def paths(tag):
    src = f'topn_rollup_src_{tag}'
    return {'src': src, 'dst': f'rollup_topn_{tag}', 'job': f'rollup_topn_{tag}',
            'manifest': os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                     'work', 'rollup', f'rollup_{tag}.json')}


def seed(args, p):
    days = args.days
    step_ms = args.interval * 1000
    steps = int(days * 86400 * 1000) // step_ms
    # 整分钟对齐，且整体早于 now-24h：rollup 引擎要窗口结束+1h，24h 也是查询切换阈值。
    now_min = int(time.time() * 1000) // 60000 * 60000
    start = now_min - (int(days * 86400) + 7200) * 1000
    span = steps * step_ms

    st, resp = http('PUT', f'{args.store}/{p["src"]}', {
        'settings': {'number_of_shards': 1, 'number_of_replicas': 0, 'refresh_interval': '-1'},
        'mappings': {'properties': {'timestamp': {'type': 'date'}},
                     'dynamic_templates': [{'strings': {'match_mapping_type': 'string',
                                                        'mapping': {'type': 'keyword'}}}]}})
    assert st in (200, 201), (st, resp)

    cluster = f'rollup_{args.tag}'
    lines = []
    t0 = time.time()
    total = 0
    for s in range(steps):
        ts = start + s * step_ms + 1
        for i in range(args.entities):
            base = 1 + i % 5
            inc = base * (s + 1)
            store = 10_000_000 + i * 500_000 + inc * 512 + (i * 7919 + s * 997) % 100_001
            docs = 1000 + inc
            qt = inc * 3 // 5
            name = f'{cluster}-e{i:03}'
            doc = DOC_TPL % (ts, cluster, cluster, i, name, store, docs, inc, qt, qt * 25 // 2, store, docs)
            lines.append(f'{{"index":{{"_index":"{p["src"]}","_id":"{name}|{ts}"}}}}\n{doc}\n')
            total += 1
        if len(lines) >= 16000:
            flush(args, p, lines)
    flush(args, p, lines)
    st, resp = http('POST', f'{args.store}/{p["src"]}/_refresh')
    assert st == 200, (st, resp)
    count = http('GET', f'{args.store}/{p["src"]}/_count')[1]['count']
    manifest = {'store': args.store, 'start': start, 'end': start + span - 60_000,
                'days': days, 'interval_sec': args.interval, 'entities': args.entities,
                'docs': total, 'count': count, 'seed_seconds': round(time.time() - t0, 1),
                'cluster': cluster, 'entity_prefix': f'{cluster}-e'}
    os.makedirs(os.path.dirname(p['manifest']), exist_ok=True)
    with open(p['manifest'], 'w', encoding='utf-8') as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    print(f'[seed] 写入 {count} docs，用时 {manifest["seed_seconds"]}s，manifest -> {p["manifest"]}')


def flush(args, p, lines):
    if not lines:
        return
    st, resp = http('POST', f'{args.store}/{p["src"]}/_bulk?refresh=false&filter_path=errors',
                    ''.join(lines), ndjson=True, timeout=300)
    assert st == 200 and not resp.get('errors'), (st, str(resp)[:300])
    lines.clear()


def rollup_setup(args, p):
    src = http('GET', f'{args.store}/{p["src"]}/_count')
    assert src[0] == 200, f'源索引不存在，先 --seed：{src}'
    roll = dict(JOB_TPL)
    roll.update(source_index=p['src'], target_index=p['dst'], page_size=args.page_size)
    http('DELETE', f'{args.store}/_rollup/jobs/{p["job"]}')
    st, resp = http('PUT', f'{args.store}/_rollup/jobs/{p["job"]}', {'rollup': roll})
    assert st in (200, 201), (st, resp)
    st, resp = http('POST', f'{args.store}/_rollup/jobs/{p["job"]}/_start')
    assert st == 200, (st, resp)
    print(f'[rollup] 作业 {p["job"]} 已创建并启动（page_size={args.page_size}）')
    wait_rollup(args, p)


def wait_rollup(args, p):
    deadline = time.time() + args.timeout
    last = ''
    while time.time() < deadline:
        st, resp = http('GET', f'{args.store}/_rollup/jobs/{p["job"]}/_explain')
        meta = (resp.get(p['job']) or {}).get('rollup_metadata') or {}
        s = meta.get('stats') or {}
        cur = f'{meta.get("status")} pages={s.get("pages_processed")} docs={s.get("documents_processed")}'
        if cur != last:
            print(f'[rollup] {cur}')
            last = cur
        if meta.get('status') in ('finished', 'failed'):
            assert meta.get('status') == 'finished', meta.get('failure_reason')
            http('POST', f'{args.store}/{p["dst"]}/_refresh')
            dst = http('GET', f'{args.store}/{p["dst"]}/_count')[1]['count']
            srcn = http('GET', f'{args.store}/{p["src"]}/_count')[1]['count']
            print(f'[rollup] 完成：raw {srcn} -> rollup {dst}（压缩 {srcn / max(dst, 1):.1f}:1），'
                  f'作业耗时 {(s.get("index_time_in_millis", 0) + s.get("search_time_in_millis", 0)) / 1000:.1f}s')
            return meta
        time.sleep(10)
    raise SystemExit(f'[rollup] 超时（{args.timeout}s），可用 --status 继续观察')


def show_status(args, p):
    st, resp = http('GET', f'{args.store}/_rollup/jobs/{p["job"]}/_explain')
    print(json.dumps(resp, ensure_ascii=False, indent=2)[:1200])


def cleanup(args, p):
    for name in (f'_rollup/jobs/{p["job"]}', f'/{p["src"]}', f'/{p["dst"]}'):
        st, resp = http('DELETE', f'{args.store}/{name}' if name.startswith('_') else f'{args.store}{name}')
        print(f'[cleanup] {name}: {st}')
    if os.path.exists(p['manifest']):
        os.remove(p['manifest'])
        print(f'[cleanup] manifest 已删')


def main():
    ap = argparse.ArgumentParser(description='生产密度合成数据 + Console 自带 rollup 作业（rollup vs raw A/B 基座）')
    ap.add_argument('--seed', action='store_true', help='灌入合成数据')
    ap.add_argument('--rollup', action='store_true', help='创建/启动 rollup 作业并等待完成')
    ap.add_argument('--status', action='store_true', help='只看作业进度')
    ap.add_argument('--cleanup', action='store_true', help='清理本脚本的作业/索引/manifest')
    ap.add_argument('--days', type=float, default=30, help='窗口天数（0.02≈30min 冒烟）')
    ap.add_argument('--interval', type=int, default=10, help='采样间隔秒（默认 10=生产密度）')
    ap.add_argument('--entities', type=int, default=64, help='合成索引实体数')
    ap.add_argument('--page-size', type=int, default=3000, help='rollup 作业 page_size')
    ap.add_argument('--timeout', type=int, default=1800, help='等待 rollup 完成的秒数')
    ap.add_argument('--store', default=STORE, help='监控存储地址')
    ap.add_argument('--tag', default='cont', help='产物标签（区分多套基座）')
    args = ap.parse_args()
    p = paths(args.tag)

    if args.status:
        show_status(args, p)
    if args.cleanup:
        cleanup(args, p)
    if args.seed:
        seed(args, p)
    if args.rollup:
        rollup_setup(args, p)
    if not (args.seed or args.rollup or args.status or args.cleanup):
        ap.print_help()


if __name__ == '__main__':
    main()
