#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
benchmark_topn.py —— TopN 查询层性能基准：三种选榜/出图算法的耗时对比

在隔离的 Easysearch 实例上，用稠密合成监控样本对比三种选榜/出图方式：

  old_store / old_rate   候选并集路径：读写活动候选
                         （固定 60s 桶 + derivative 峰值 + terms partition 0，
                         候选失败则无名称过滤按存储出图）+ 按存储排序的展示查询
  new_store              直查快路径：max(field) DESC 两阶段（先精确选名，再只取
                         榜上实体的时间桶），对应单字段瞬时指标
  new_rate               复合分页路径：composite 按实体分页拉全量时间桶，
                         客户端差分算分取 TopN（页大小 --page，默认 128）

  注：old_* / new_* 为历史输出键名，与留档结果兼容，含义如上。

边界：
  - 只测查询层：不做 PIT、并发、授权、真实 Console 端到端；
  - 造数为稠密、不可变合成样本（无需 PIT），不代表生产数据形态；
  - 墙钟含本地 HTTP、JSON 解析与客户端选榜，不是生产延迟；
  - 快路径榜单与独立基准断言一致（names == truth），候选并集路径只记录命中数。

安全护栏：只允许写入 cluster_name 为 console-topn-local-bench 的集群；
测完默认删除本次造出的 topn-bench-* 索引（--keep-data 保留）。

用法：
  # 先启动隔离实例（端口、集群名必须一致）：
  #   easysearch -Ehttp.port=19219 -Ecluster.name=console-topn-local-bench \
  #     -Ediscovery.type=single-node -Enetwork.host=127.0.0.1
  python3 benchmark_topn.py                                  # 全部规模
  python3 benchmark_topn.py --counts 24 1000 --windows 900   # 小矩阵快速验证

输出：JSONL，每行一个组合（实体数、窗口、桶宽、top、路径、p50/p95 墙钟、
搜索 took 中位数、搜索次数、响应字节中位数、榜单命中数、旧版降级次数）。
"""

import argparse
import http.client
import json
import math
import random
import statistics
import time
import uuid

KEY, TS = 'index_name', 'timestamp'
STORE, READ, WRITE, PRIMARY = 'store', 'read_total', 'write_total', 'primary_total'
START = 1_800_000_000_000  # 对齐 20s 与 10m 的固定窗口起点；造数不依赖当前时间
GUARD_CLUSTER = 'console-topn-local-bench'


class TooManyBuckets(RuntimeError):
    """监控引擎返回 search.max_buckets 超限（旧候选路径在长窗口下的已知行为）。"""


# ---------- DSL 构造 ----------

def mx(field):
    return {'max': {'field': field}}


def hist(width, aggs):
    return {'date_histogram': {'field': TS, 'fixed_interval': f'{width}s'}, 'aggs': aggs}


def bounds(duration):
    return {'range': {TS: {'gte': START, 'lte': START + duration * 1000}}}


# ---------- HTTP 客户端 ----------

class Client:
    def __init__(self, host, port):
        self.conn = http.client.HTTPConnection(host, port, timeout=120)
        self.measures = []   # 每次搜索的 (took_ms, 响应字节)
        self.fallbacks = 0   # 旧候选因桶超限降级的次数

    def req(self, method, path, body=None):
        wire = json.dumps(body).encode() if isinstance(body, dict) else body
        self.conn.request(method, path, wire, {'Content-Type': 'application/x-ndjson' if path.startswith('/_bulk') else 'application/json'})
        res = self.conn.getresponse()
        raw = res.read()
        data = json.loads(raw)
        if data.get('error', {}).get('caused_by', {}).get('type') == 'too_many_buckets_exception':
            raise TooManyBuckets(data['error']['caused_by']['reason'])
        if res.status >= 300 or data.get('errors') or data.get('timed_out') or data.get('_shards', {}).get('failed'):
            raise RuntimeError(f'{method} {path}: {raw[:1000]!r}')
        if path.endswith('/_search') or '/_search?' in path:
            self.measures.append((data.get('took', 0), len(raw)))
        return data


# ---------- 造数 ----------

def seed(client, index, count, duration, step):
    """稠密采样：存储峰值在高位实体、写入速率峰值在低位实体，两类榜首不同。"""
    mapping = {field: {'type': 'long'} for field in (STORE, READ, WRITE, PRIMARY)}
    mapping.update({KEY: {'type': 'keyword'}, TS: {'type': 'date'}})
    client.req('PUT', '/' + index, {'settings': {'number_of_shards': 3, 'number_of_replicas': 0, 'refresh_interval': '-1', 'index.requests.cache.enable': False}, 'mappings': {'properties': mapping}})
    header = json.dumps({'index': {'_index': index}}).encode() + b'\n'
    block = []
    for tick in range(duration // step + 1):
        for i in range(count):
            w = (count - i) * tick
            rec = {KEY: f'idx-{i:05d}', TS: START + tick * step * 1000, STORE: i * 10000 + tick, READ: (1 + i % 23) * tick, WRITE: (1 + i % 43) * tick, PRIMARY: w}
            block.append(header + json.dumps(rec, separators=(',', ':')).encode() + b'\n')
            if len(block) == 6000:
                client.req('POST', '/_bulk?filter_path=errors,items.*.error', b''.join(block))
                block.clear()
    if block:
        client.req('POST', '/_bulk?filter_path=errors,items.*.error', b''.join(block))
    client.req('POST', f'/{index}/_refresh')
    print(f'{index}: {count * (duration // step + 1)} docs ready', flush=True)


# ---------- 三条对比路径 ----------

def search(client, index, duration, agg, names=None, required=None):
    filters = [bounds(duration)]
    if names is not None:
        filters.append({'terms': {KEY: names}})
    if required:
        filters.append({'exists': {'field': required}})
    return client.req('POST', f'/{index}/_search?allow_partial_search_results=false&request_cache=false', {'size': 0, 'query': {'bool': {'filter': filters}}, 'aggs': agg})['aggregations']


def chart(client, index, duration, width, top, field, names, legacy=False):
    """展示查询：榜上实体 × 时间桶；legacy=True 时按存储峰值排序（旧降级行为）。"""
    subs = {'value': mx(field)}
    if legacy and field == PRIMARY:
        subs['diff'] = {'derivative': {'buckets_path': 'value'}}
    agg = {'terms': {'field': KEY, 'size': top}, 'aggs': {'dates': hist(width, subs)}}
    if legacy:
        agg['terms']['order'] = {'store_peak': 'desc'}
        agg['aggs']['store_peak'] = mx(STORE)
    return search(client, index, duration, {'entities': agg}, names)['entities']['buckets']


def legacy_topn(client, index, count, duration, width, top, field):
    """旧路径：两路读写活动候选（60s 桶差分峰值，partition 0）合成榜后按榜取曲线。"""
    terms = {'field': KEY, 'size': 10000, 'include': {'partition': 0, 'num_partitions': max(1, count // 20)}}
    candidate = {f'cand{k}': {'terms': terms, 'aggs': {
        'dates': hist(60, {'value': mx(counter), 'diff': {'derivative': {'buckets_path': 'value'}}}),
        'peak': {'max_bucket': {'buckets_path': 'dates>diff'}},
        'limit': {'bucket_sort': {'sort': [{'peak': {'order': 'desc'}}], 'size': top}},
    }} for k, counter in enumerate((READ, WRITE))}
    try:
        res = search(client, index, duration, candidate)
    except TooManyBuckets:
        # Console 记日志后不带名称过滤直接出图（按存储排序）——旧版的降级行为。
        client.fallbacks += 1
        res = {}
    scores = {}
    for group in res.values():
        for b in group['buckets']:
            scores[b['key']] = scores.get(b['key'], 0) + (b['peak']['value'] or 0)
    names = sorted(scores, key=lambda k: (-scores[k], k))[:top]
    return [b['key'] for b in chart(client, index, duration, width, top, field, names or None, True)]


def fast_topn(client, index, duration, width, top):
    """新快路径：max DESC 两阶段。第一次选名不生成全实体时间桶，第二次只取榜上曲线。"""
    rank = {'terms': {'field': KEY, 'size': top, 'shard_size': top,
                      'order': [{'peak': 'desc'}, {'_key': 'asc'}]}, 'aggs': {'peak': mx(STORE)}}
    names = [b['key'] for b in search(client, index, duration, {'rank': rank}, required=STORE)['rank']['buckets']]
    curves = {b['key']: b for b in chart(client, index, duration, width, top, STORE, names)}
    assert set(curves) == set(names)
    return names


def paged_topn(client, index, duration, width, top, page):
    """新复合分页：composite 枚举全部实体，客户端差分算分，流式保留全局 TopN。"""
    comp = {'size': page, 'sources': [{'entity': {'terms': {'field': KEY}}}]}
    agg = {'entities': {'composite': comp, 'aggs': {'dates': hist(width, {'value': mx(PRIMARY)})}}}
    winners = []
    while True:
        data = search(client, index, duration, agg)['entities']
        for b in data['buckets']:
            points = b['dates']['buckets']
            score = max(((q['value']['value'] - p['value']['value']) / width
                         for p, q in zip(points, points[1:])
                         if q['key'] - p['key'] == width * 1000 and p['value']['value'] is not None
                         and q['value']['value'] is not None and q['value']['value'] >= p['value']['value']), default=None)
            if score is not None:
                winners.append((-score, b['key']['entity'], points))
                winners.sort(key=lambda item: item[:2])
                del winners[top:]
        after = data.get('after_key')
        if not data['buckets'] or after is None:
            break
        assert comp.get('after') != after
        comp['after'] = after
    return [k for _, k, _ in winners]


# ---------- 基准循环 ----------

def p95(samples):
    return sorted(samples)[math.ceil(.95 * len(samples)) - 1]


def run(args):
    client = Client(args.host, args.port)
    info = client.req('GET', '/')
    if info['cluster_name'] != GUARD_CLUSTER:
        raise RuntimeError(f'拒绝写入非基准集群 {info["cluster_name"]!r}（要求 {GUARD_CLUSTER!r}）')
    print(info['version'], flush=True)
    created = []
    try:
        for count in args.counts:
            for duration in args.windows:
                width, step = (20, 20) if duration <= 3600 else (600, 60)
                index = 'topn-bench-' + uuid.uuid4().hex
                created.append(index)
                seed(client, index, count, duration, step)
                for top in args.top:
                    ops = {
                        'old_store': lambda: legacy_topn(client, index, count, duration, width, top, STORE),
                        'new_store': lambda: fast_topn(client, index, duration, width, top),
                        'old_rate': lambda: legacy_topn(client, index, count, duration, width, top, PRIMARY),
                        'new_rate': lambda: paged_topn(client, index, duration, width, top, args.page),
                    }
                    expected = {'store': [f'idx-{i:05d}' for i in range(count - 1, count - top - 1, -1)],
                                'rate': [f'idx-{i:05d}' for i in range(top)]}
                    samples = {path: [] for path in ops}
                    rand = random.Random(3)  # 固定洗牌顺序，保证各路径交错受热
                    for trial in range(args.runs + args.warmup):
                        order = list(ops)
                        rand.shuffle(order)
                        for path in order:
                            client.measures.clear()
                            client.fallbacks = 0
                            t0 = time.perf_counter()
                            names = ops[path]()
                            ms = (time.perf_counter() - t0) * 1000
                            truth = expected[path.split('_')[1]]
                            if path.startswith('new'):
                                assert names == truth, (path, names, truth)
                            if trial >= args.warmup:
                                samples[path].append((ms, sum(took for took, _ in client.measures), len(client.measures),
                                                      sum(n for _, n in client.measures), len(set(names) & set(truth)), client.fallbacks))
                    for path, rows in samples.items():
                        wall = [r[0] for r in rows]
                        print(json.dumps(dict(entities=count, seconds=duration, width=width, top=top, path=path,
                                              p50_ms=round(statistics.median(wall), 1), p95_ms=round(p95(wall), 1),
                                              took_p50=statistics.median(r[1] for r in rows), requests=rows[0][2],
                                              bytes_p50=statistics.median(r[3] for r in rows), matches=rows[0][4],
                                              fallbacks=sum(r[5] for r in rows))), flush=True)
    finally:
        if args.keep_data:
            print(f'keep-data: 保留 {len(created)} 个索引', flush=True)
        else:
            for index in created:
                client.req('DELETE', '/' + index)
            print(f'cleanup: 删除 {len(created)} 个基准索引', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='TopN 查询层性能基准（三种选榜算法对比），详见文件头注释')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', default=19219, type=int)
    parser.add_argument('--counts', default=[24, 100, 1000, 5000], nargs='+', type=int,
                        help='实体数矩阵；100 近似节点页规模，默认四档')
    parser.add_argument('--windows', default=[900, 86400], nargs='+', type=int,
                        help='窗口秒数；≤3600 用 20s 桶，否则 600s 桶')
    parser.add_argument('--top', default=[5, 20], nargs='+', type=int)
    parser.add_argument('--runs', default=20, type=int, help='每组合正式测量次数')
    parser.add_argument('--warmup', default=5, type=int)
    parser.add_argument('--page', default=128, type=int, help='composite 页大小')
    parser.add_argument('--keep-data', action='store_true', help='保留造出的基准索引（默认测完删除）')
    try:
        run(parser.parse_args())
    except ConnectionError as e:
        parser.exit(1, f'无法连接基准实例：{e};请先启动 cluster.name={GUARD_CLUSTER} 的隔离 Easysearch（见 repro/README.md「性能基准」）\n')
