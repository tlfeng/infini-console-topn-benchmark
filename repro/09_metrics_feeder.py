#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
09_metrics_feeder.py —— TopN 监控指标数据维护（离线填数 / 在线数据流 / 真值导出 / 清理）

为什么需要它：
  Console 的 TopN（advanced_topn raw 路径）读的是监控存储 .infini_metrics 里的
  index_stats / node_stats 文档，rate/latency 依赖「同一实体相邻等宽桶内计数器单调
  增长」。真实环境数据断档、计数器不增长，导致长窗/短窗各种空图。本脚本按 Console
  采集文档的同款形状直接注入合成指标，把数据侧变量完全掌控，便于评测 TopN 算法
  与前端展示。业务集群不再真实搭建——Console 只消费 store 里被注入的数据。

  数据写在 .infini_metrics，用独立前缀 topn90d-<tag>- 隔离，cluster_id 标签默认
  topn-biz（CLUSTER env 可覆盖，与 32_verify_topn.py 的 --cluster 对齐）；
  不碰真实采集数据、不与 34 脚本的 synth30d- 前缀冲突。

两个填数方式：
  fill    离线填数：一次性预填充历史数据（默认最近 90 天，以当前时间为终点往回）
  stream  在线数据流：前台持续写入，Ctrl-C 停止；计数器自动续接；--auto-prune 可开关

辅助子命令：
  resample  把注入数据按 32/33 消费的 JSONL 形状导出真值（stats_*.jsonl/nodes_*.jsonl）
            ——替代已删除的 12/14 采样脚本（它们依赖真实业务集群）
  expected  导出解析式期望榜（画像增量/时延常数即真值声明），供冒烟断言
  prune     删除保留窗口之外的合成指标（默认 2160h=90天，支持 --dry-run）
  status    查看当前数据量、时间范围、各实体计数器

维度选择（--side）：
  index  只注入/导出索引维度（index_stats）
  node   只注入/导出节点维度（node_stats）
  both   两个维度同时（离线/在线/导出均适用）

计数器模型（保证所有 TopN 指标都能出数）：
  每个采样步 step 从 1 递增；各计数器 = 每步增量 × step（单调递增）。
  时延计数器 = 操作计数器 × 时延常数 × 实体系数（各实体系数不同 → latency 榜有区分度），
  相邻桶总能配对出正 rate / 正 latency。

真值口径与边界（详见 README「覆盖边界」）：
  - resample 导出的真值来自注入文档本身（真值同源）；expected 是独立于 ES 的解析式
    期望，两者交叉可发现注入器自身的错误。
  - 计数器严格单调无重置；乱序观测/同毫秒冲突等真实扰动不在覆盖范围。

用法：
  # 离线填数：最近 90 天，索引+节点两个维度（15min 间隔）
  python3 09_metrics_feeder.py fill --side both
  python3 09_metrics_feeder.py fill --days 30 --interval 300 --tag trial --side both

  # 在线数据流（计数器从 fill 的末尾自动续接）+ 自动清理 90 天外数据
  python3 09_metrics_feeder.py stream --side both --interval 60 --auto-prune
  python3 09_metrics_feeder.py stream --side both --interval 60          # 不清理

  # 真值导出（给 32/33 用）+ 期望榜（冒烟断言）
  python3 09_metrics_feeder.py resample --side both --hours 6
  python3 09_metrics_feeder.py expected --side both

  # 手动清理保留窗口之外（先看会删多少，再真删）；边界对齐整点
  python3 09_metrics_feeder.py prune --dry-run
  python3 09_metrics_feeder.py prune --hours 48             # 只保留最近 48 小时

  # 状态
  python3 09_metrics_feeder.py status --side both

建议桶宽（受 raw 路径预算 searchBucketLimit=6000 约束）：
  - 90 天窗口：用 8h / 1d 桶（900s/1h 会 budget_exceeded）
  - 30 天窗口：用 3h / 8h / 1d 桶
  - 短窗口（≤6h）：可用 60s / 900s 桶
  采样间隔要 ≤ 桶宽，否则桶内无相邻观测配不出 rate/latency。
"""
import argparse
import datetime as dt
import json
import hashlib
import math
import re
import os
import signal
import sys
import time
import urllib.error
import urllib.request

# ----------------------------------------------------------------- 配置
STORE = os.environ.get('STORE', 'http://127.0.0.1:9200')
RAW_INDEX = '.infini_metrics'
CLUSTER = os.environ.get('CLUSTER', 'topn-biz')
BASE_PREFIX = 'topn90d'
SIDES = ('index', 'node')

# 索引维度画像：(基础名, 每步 index_total 增量, 时延系数)。系数不同 → latency 榜与 rate 榜
# 排序不同（latency 榜按系数降序：mid > hot > scroll > low > tiny > slow）。
ENTITIES = [
    ('hot',   12000, 1.0),
    ('mid',    6000, 1.8),
    ('low',    2400, 0.8),
    ('slow',    600, 0.5),
    ('tiny',    120, 0.6),
    ('scroll',  3000, 1.2),
]

# 节点维度画像：(基础名, 每步 index_total 增量, 时延系数, transport_address)。
# rate 榜 = hot > mid > low > tiny；latency 榜 = mid > hot > low > tiny。
NODE_ENTITIES = [
    ('hot',  8000, 1.0, '127.0.0.1:9401'),
    ('mid',  4000, 1.6, '127.0.0.1:9402'),
    ('low',  1500, 0.8, '127.0.0.1:9403'),
    ('tiny',  300, 0.5, '127.0.0.1:9404'),
]

# 各操作计数器相对 index_total 的比例；时延(ms)常数
RATIO = {
    'index':   1.00,
    'query':   0.60,
    'fetch':   0.30,
    'scroll':  0.05,
    'merge':   0.02,
    'refresh': 0.40,
    'flush':   0.10,
    'delete':  0.01,
}
LATENCY_MS = {
    'index':   5.0,
    'query':  12.0,
    'fetch':   8.0,
    'scroll': 30.0,
    'merge':  50.0,
    'refresh': 3.0,
    'flush':  20.0,
}

STEP_FIELD = {
    'index': 'payload.elasticsearch.index_stats._synth_step',
    'node':  'payload.elasticsearch.node_stats._synth_step',
}
ENTITY_FIELD = {
    'index': 'metadata.labels.index_name',
    'node':  'metadata.labels.node_name',
}
METRIC_NAME = {'index': 'index_stats', 'node': 'node_stats'}


# ----------------------------------------------------------------- HTTP
def http(method, url, body=None, headers=None, timeout=120, raw=False):
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
        return e.code, e.read().decode(errors='replace')[:800]
    except Exception as e:
        return -1, str(e)


def _maybe_json(s):
    try:
        return json.loads(s)
    except Exception:
        return s


def prefix_for(tag):
    if not isinstance(tag, str) or not re.fullmatch(r'[A-Za-z0-9_]+', tag):
        raise ValueError('tag must contain only ASCII letters, digits or underscore')
    return f'{BASE_PREFIX}-{tag}-'


def retention_hours(args):
    """保留窗口：优先用 --hours（小时），否则用 --days×24；都没给则默认 2160h(90天)。"""
    hours = getattr(args, 'hours', None)
    if hours is not None:
        return float(hours)
    days = getattr(args, 'days', None)
    if days is not None:
        return float(days) * 24
    return 2160.0


def align_ms(ts_ms, interval_s):
    step = interval_s * 1000
    return ts_ms // step * step


def sides_of(args):
    """把 --side 参数展开成维度元组。"""
    if args.side == 'index':
        return ('index',)
    if args.side == 'node':
        return ('node',)
    return SIDES


def entities_of(side):
    if side == 'index':
        return [(prefix_free, ps, factor, '') for prefix_free, ps, factor in ENTITIES]
    return [(n, ps, factor, addr) for n, ps, factor, addr in NODE_ENTITIES]


def prefix_query(sides, prefix):
    """Bind each metric name to its own entity prefix, within one cluster."""
    if not CLUSTER or not CLUSTER.strip() or not sides or any(s not in SIDES for s in sides):
        raise ValueError('invalid cluster or sides')
    if not re.fullmatch(r'topn90d-[A-Za-z0-9_]+-', prefix):
        raise ValueError('invalid synthetic prefix')
    branches = [{'bool': {'filter': [
        {'term': {'metadata.name': METRIC_NAME[side]}},
        {'prefix': {ENTITY_FIELD[side]: prefix}},
    ]}} for side in sides]
    return {'bool': {'filter': [
        {'term': {'metadata.labels.cluster_id': CLUSTER}},
        {'term': {'metadata.category': 'elasticsearch'}},
        {'bool': {'should': branches, 'minimum_should_match': 1}},
    ]}}


def checked_response(st, resp, operation):
    if st != 200 or not isinstance(resp, dict) or resp.get('timed_out') is True:
        raise SystemExit(f'{operation} failed: {st} {str(resp)[:400]}')
    if resp.get('_shards', {}).get('failed', 0) or resp.get('failures') or resp.get('version_conflicts', 0):
        raise SystemExit(f'{operation} incomplete: {str(resp)[:400]}')
    return resp


# ----------------------------------------------------------------- 造文档
def make_doc(side, name, ts_ms, step, per_step, factor, addr=''):
    """构造一条与 Console 采集同形状的指标文档，覆盖 TopN 常用的全部字段。"""

    def ops(key):
        return int(per_step * RATIO[key]) * step

    def latency(key):
        return int(ops(key) * LATENCY_MS[key] * factor)

    if side == 'index':
        idx_total = ops('index')
        q_total, f_total, sc_total = ops('query'), ops('fetch'), ops('scroll')
        mg_total, rf_total, fl_total = ops('merge'), ops('refresh'), ops('flush')
        docs = 1000 + idx_total
        deleted = ops('delete')
        store = 10 * 1024 * 1024 + idx_total * 512
        is_stats = {
            '_synth_step': step,
            'index_info': {'index': name, 'status': 'open', 'health': 'green',
                           'shards': 1, 'replicas': 0},
            'total': {
                'docs': {'count': docs, 'deleted': deleted},
                'store': {'size_in_bytes': store},
                'segments': {'count': 1 + step // 100},
                'search': {'query_total': q_total, 'query_time_in_millis': latency('query'),
                           'fetch_total': f_total, 'fetch_time_in_millis': latency('fetch'),
                           'scroll_total': sc_total, 'scroll_time_in_millis': latency('scroll'),
                           'suggest_total': 0, 'suggest_time_in_millis': 0},
                'indexing': {'index_total': idx_total, 'index_time_in_millis': latency('index'),
                             'delete_total': deleted, 'delete_time_in_millis': deleted * 3,
                             'index_failed': 0},
                'merges': {'total': mg_total, 'total_time_in_millis': latency('merge'),
                           'current': 0, 'total_docs': 0},
                'refresh': {'total': rf_total, 'total_time_in_millis': latency('refresh')},
                'flush': {'total': fl_total, 'total_time_in_millis': latency('flush')},
            },
            'primaries': {
                'docs': {'count': docs, 'deleted': deleted},
                'store': {'size_in_bytes': store},
                'search': {'query_total': q_total, 'query_time_in_millis': latency('query'),
                           'fetch_total': f_total, 'fetch_time_in_millis': latency('fetch'),
                           'scroll_total': sc_total, 'scroll_time_in_millis': latency('scroll')},
                'indexing': {'index_total': idx_total, 'index_time_in_millis': latency('index'),
                             'delete_total': deleted, 'delete_time_in_millis': deleted * 3},
            },
        }
        return {
            'timestamp': ts_ms,
            'metadata': {
                'name': 'index_stats', 'category': 'elasticsearch', 'datatype': 'snapshot',
                'labels': {
                    'cluster_id': CLUSTER, 'cluster_uuid': '',
                    'index_id': f'{CLUSTER}:{name}', 'index_name': name,
                    'index_uuid': BASE_PREFIX + '-uuid-' + name,
                },
            },
            'payload': {'elasticsearch': {'index_stats': is_stats}},
        }

    # ---- node 维度：字段布局与真实 ES _nodes/stats 对齐（37 号扫描验证过的形状） ----
    idx_total = ops('index')
    q_total, f_total, sc_total = ops('query'), ops('fetch'), ops('scroll')
    mg_total, rf_total, fl_total = ops('merge'), ops('refresh'), ops('flush')
    gidx = i = None  # 占位，i 由调用序号决定；波形用 step 与 name 哈希保持确定性
    salt = sum(ord(c) for c in name) % 97
    segs = 3 + (step // 50 + salt) % 12
    seg_mem = segs * 2_000_000 + (step * 7919 + salt) % 900_001
    ns = {
        '_synth_step': step,
        'process': {'cpu': {'percent': 10 + (step * 7 + salt) % 60},
                    'open_file_descriptors': 500 + (step * 23 + salt) % 300,
                    'max_file_descriptors': 65536},
        'os': {'cpu': {'percent': 10 + (step * 5 + salt) % 55,
                       'load_average': {'1m': (5 + (step + salt) % 40) / 10}},
               'mem': {'used_percent': 30 + (step * 3 + salt) % 40},
               'swap': {'used_in_bytes': 100_000_000 + (step * 997 + salt) % 300_000_001,
                        'total_in_bytes': 1_000_000_000}},
        'indices': {
            'query_cache': {'memory_size_in_bytes': 30_000_000 + (step * 613 + salt) % 10_000_001,
                            'cache_count': q_total // 3, 'hit_count': q_total // 4,
                            'miss_count': q_total // 6},
            'request_cache': {'memory_size_in_bytes': 15_000_000 + (step * 769 + salt) % 5_000_001,
                              'hit_count': q_total // 2, 'miss_count': q_total // 5},
            'fielddata': {'memory_size_in_bytes': 8_000_000 + (step * 577 + salt) % 4_000_001},
            'segments': {'count': segs, 'memory_in_bytes': seg_mem,
                         'stored_fields_memory_in_bytes': seg_mem * 2 // 10,
                         'terms_memory_in_bytes': seg_mem * 15 // 100,
                         'doc_values_memory_in_bytes': seg_mem * 4 // 10,
                         'index_writer_memory_in_bytes': seg_mem // 10,
                         'term_vectors_memory_in_bytes': seg_mem * 3 // 100},
            'docs': {'count': 5000 + idx_total // 10, 'deleted': idx_total // 20},
            'store': {'size_in_bytes': 20_000_000 + salt * 1_000_000 + idx_total * 64},
            'search': {'query_total': q_total, 'query_time_in_millis': latency('query'),
                       'fetch_total': f_total, 'fetch_time_in_millis': latency('fetch'),
                       'scroll_total': sc_total, 'scroll_time_in_millis': latency('scroll'),
                       'open_contexts': (step // 10 + salt) % 6},
            'indexing': {'index_total': idx_total, 'index_time_in_millis': latency('index')},
            'refresh': {'total': rf_total, 'total_time_in_millis': latency('refresh')},
            'flush': {'total': fl_total, 'total_time_in_millis': latency('flush')},
            'merges': {'total': mg_total, 'total_time_in_millis': latency('merge')},
        },
        'http': {'current_open': 10 + (step + salt) % 25, 'total_opened': step * 2},
        'jvm': {'mem': {'heap_used_percent': 30 + (step * 11 + salt) % 45,
                        'pools': {'young': {'used_in_bytes': 100_000_000 + (step % 90) * 3_000_000 + salt * 1_000_000,
                                            'peak_used_in_bytes': 400_000_000 + salt * 1_000_000},
                                  'old': {'used_in_bytes': 200_000_000 + (step * 97) % 50_000_001,
                                          'peak_used_in_bytes': 350_000_000 + salt * 500_000}},
                        'heap_used_in_bytes': 100_000_000 + (step % 90) * 1_500_000
                                              + 200_000_000 + (step * 97) % 50_000_001},
                'gc': {'collectors': {
                    'young': {'collection_count': step // 50 + 1,
                              'collection_time_in_millis': (step // 50 + 1) * 200 + salt * 50},
                    'old': {'collection_count': step // 500 + 1,
                            'collection_time_in_millis': (step // 500 + 1) * 1000}}}},
        'transport': {'total_outbound_connections': 2 + salt % 7,
                      'tx_count': step, 'rx_count': step * 2,
                      'tx_size_in_bytes': step * 1200, 'rx_size_in_bytes': step * 800},
        'fs': {'io_stats': {'total': {'operations': step * 3, 'read_operations': step,
                                      'write_operations': step * 2}},
               'total': {'total_in_bytes': 400_000_000 + salt * 20_000_000,
                         'available_in_bytes': 200_000_000 - (step * 64) % 5_000_001}},
        'breakers': {'parent': {'tripped': step // 1000},
                     'accounting': {'tripped': step // 2000},
                     'fielddata': {'tripped': step // 3000},
                     'request': {'tripped': step // 2500},
                     'in_flight_requests': {'tripped': step // 5000},
                     'model_inference': {'tripped': step // 6000},
                     'eql_sequence': {'tripped': step // 7000},
                     'inflight_requests': {'tripped': step // 8000}},
    }
    return {
        'timestamp': ts_ms,
        'metadata': {
            'name': 'node_stats', 'category': 'elasticsearch', 'datatype': 'snapshot',
            'labels': {
                'cluster_id': CLUSTER, 'cluster_uuid': '',
                'node_id': f'{CLUSTER}:{name}', 'node_name': name,
                'transport_address': addr,
            },
        },
        'payload': {'elasticsearch': {'node_stats': ns}},
    }


def bulk_write(docs, id_prefix=BASE_PREFIX):
    """docs: [(index_name, ts_ms, doc), ...]，按 _id 幂等写入。"""
    lines = []
    for name, ts_ms, doc in docs:
        identity = [id_prefix, doc['metadata']['labels']['cluster_id'],
                    doc['metadata']['name'], name, ts_ms]
        doc_id = hashlib.sha256(json.dumps(identity, ensure_ascii=False,
                                           separators=(',', ':')).encode()).hexdigest()
        lines.append(json.dumps({'index': {'_index': RAW_INDEX, '_id': doc_id}}))
        lines.append(json.dumps(doc, ensure_ascii=False))
    batch = 2000  # 1000 文档/批
    for s in range(0, len(lines), batch):
        chunk = '\n'.join(lines[s:s + batch]) + '\n'
        st, resp = http('POST', f'{STORE}/{RAW_INDEX}/_bulk?refresh=false', chunk,
                        headers={'Content-Type': 'application/x-ndjson'}, timeout=180, raw=True)
        resp = _maybe_json(resp) if isinstance(resp, str) else resp
        if st != 200 or not isinstance(resp, dict) or resp.get('errors') is not False:
            raise SystemExit(f'bulk failed: {st} {str(resp)[:400]}')
        items = resp.get('items')
        if not isinstance(items, list) or len(items) != min(batch, len(lines) - s) // 2:
            raise SystemExit('bulk incomplete: missing item results')
        if any(not isinstance(item.get('index'), dict) or
               not 200 <= item['index'].get('status', 0) < 300 for item in items):
            raise SystemExit('bulk incomplete: item failure')
    if docs:
        st, resp = http('POST', f'{STORE}/{RAW_INDEX}/_refresh')
        checked_response(st, resp, 'refresh')
    return len(docs)


def build_step_docs(sides, ts_ms, step_map, prefix):
    """一个采样步的全部文档：[(index_name, ts_ms, doc), ...]。"""
    docs = []
    for side in sides:
        for name, per_step, factor, addr in entities_of(side):
            full = prefix + name
            step = step_map[(side, full)]
            docs.append((full, ts_ms, make_doc(side, full, ts_ms, step, per_step, factor, addr)))
    return docs


# ----------------------------------------------------------------- fill
def cmd_fill(args):
    interval = args.interval
    end = align_ms(int(time.time() * 1000), interval)
    start = align_ms(end - int(args.days * 86400 * 1000), interval)
    n = int((end - start) / (interval * 1000)) + 1
    prefix = prefix_for(args.tag)
    sides = sides_of(args)
    ents = {s: len(entities_of(s)) for s in sides}
    print(f'[fill] cluster={CLUSTER} 前缀={prefix} 天数={args.days} 间隔={interval}s 维度={"/".join(sides)}')
    print(f'[fill] 窗口 {fmt(start)} -> {fmt(end)}  步数={n}  实体={ents}')
    print(f'[fill] 预计文档 {n * sum(ents.values()):,} 条，开始写入...')
    t0 = time.time()
    written = 0
    step_batch = 200
    for base in range(0, n, step_batch):
        docs = []
        for i in range(base, min(base + step_batch, n)):
            ts = start + i * interval * 1000
            step = i + 1
            for side in sides:
                for name, per_step, factor, addr in entities_of(side):
                    full = prefix + name
                    docs.append((full, ts, make_doc(side, full, ts, step, per_step, factor, addr)))
        written += bulk_write(docs)
        print(f'  {written:,}/{n * sum(ents.values()):,} ({time.time()-t0:.0f}s)', flush=True)
    print(f'[fill] 完成 {written:,} 条，用时 {time.time()-t0:.1f}s')
    print('[fill] 提示：长窗口请用大桶宽（8h/1d），否则 raw 路径会 budget_exceeded')
    return 0


# ----------------------------------------------------------------- stream 续接状态
def read_state(tag, sides):
    """Read the step and timestamp from the same latest document per entity."""
    prefix = prefix_for(tag)
    state = {}
    for side in sides:
        for name, _, _, _ in entities_of(side):
            full = prefix + name
            body = {'size': 2, 'track_total_hits': True,
                    'query': {'bool': {'filter': [prefix_query((side,), prefix),
                        {'term': {ENTITY_FIELD[side]: full}}]}},
                    'sort': [{'timestamp': {'order': 'desc'}}]}
            st, resp = http('POST', f'{STORE}/{RAW_INDEX}/_search', body)
            checked_response(st, resp, 'state search')
            try:
                hits = resp['hits']['hits']
                total = resp['hits']['total']
                total = total['value'] if isinstance(total, dict) and total.get('relation') == 'eq' else total
                if not isinstance(total, int) or total < 0 or len(hits) != min(total, 2):
                    raise ValueError('incomplete hits')
                if hits:
                    src = hits[0]['_source']
                    if len(hits) > 1 and hits[1]['_source']['timestamp'] == src['timestamp']:
                        raise ValueError('ambiguous latest timestamp')
                    ts = src['timestamp']
                    step = src['payload']['elasticsearch'][METRIC_NAME[side]]['_synth_step']
                    if (isinstance(ts, bool) or not isinstance(ts, int) or ts < 0 or
                            isinstance(step, bool) or not isinstance(step, int) or step < 0):
                        raise ValueError('invalid timestamp or step')
                    state[(side, full)] = {'step': step, 'ts': ts}
            except (KeyError, TypeError, ValueError, IndexError) as exc:
                raise SystemExit(f'state search incomplete for {full}: {exc}') from exc
    return state


def cmd_stream(args):
    interval = args.interval
    prefix = prefix_for(args.tag)
    sides = sides_of(args)
    state = read_state(args.tag, sides)
    steps = {}
    for side in sides:
        for name, per_step, factor, addr in entities_of(side):
            full = prefix + name
            steps[(side, full)] = state.get((side, full), {}).get('step', 0) + 1
    last_ts = max((v['ts'] for v in state.values()), default=0)
    hours = retention_hours(args)
    prune_ms = args.prune_every_hours * 3600 * 1000
    last_prune = 0
    print(f'[stream] cluster={CLUSTER} 前缀={prefix} 间隔={interval}s 维度={"/".join(sides)}')
    if args.auto_prune:
        print(f'[stream] 自动清理=开：保留 {hours}h，每 {args.prune_every_hours}h 检查一次'
              f'（边界对齐整点，删除严格早于整点的数据）')
    else:
        print('[stream] 自动清理=关（加 --auto-prune 开启）')
    print(f'[stream] 从已有数据续接：起始 step={ {k[1].split(prefix)[-1]: s for k, s in steps.items()} }，'
          f'已有最新时间={fmt(last_ts) if last_ts else "无"}')
    print('[stream] 前台运行，Ctrl-C 停止\n')

    tick = 0
    stopped = {'flag': False}

    def on_sigint(signum, frame):
        stopped['flag'] = True

    signal.signal(signal.SIGINT, on_sigint)

    try:
        while not stopped['flag']:
            now_ms = int(time.time() * 1000)
            if args.auto_prune and now_ms - last_prune >= prune_ms:
                do_prune(hours, prefix, dry_run=False, sides=sides)
                last_prune = now_ms
            ts = align_ms(now_ms, interval)
            if ts > last_ts:
                docs = build_step_docs(sides, ts, steps, prefix)
                n = bulk_write(docs)
                last_ts = ts
                for k in steps:
                    steps[k] += 1
                tick += 1
                print(f'  [{dt.datetime.now():%H:%M:%S}] 写入 {n} 条 @ {fmt(ts)}  '
                      f'(step={min(steps.values())-1})', flush=True)
            now_ms = int(time.time() * 1000)
            wait = max(0.5, (align_ms(now_ms, interval) + interval * 1000 - now_ms) / 1000)
            deadline = time.time() + wait
            while time.time() < deadline and not stopped['flag']:
                time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    print(f'\n[stream] 已停止，本次写入 {tick} 轮')
    return 0


# ----------------------------------------------------------------- prune
def retention_cutoff(hours):
    return align_ms(int(time.time() * 1000) - int(hours * 3600 * 1000), 3600)


def do_prune(hours, prefix, dry_run=False, sides=None):
    if not math.isfinite(hours) or hours <= 0:
        raise ValueError('retention must be finite and positive')
    sides = SIDES if sides is None else sides
    cutoff = retention_cutoff(hours)
    body = {
        'query': {'bool': {'filter': [
            prefix_query(sides, prefix),
            {'range': {'timestamp': {'lt': cutoff, 'format': 'epoch_millis'}}},
        ]}}
    }
    scope = f'前缀={prefix} 保留{hours}h 早于 {fmt(cutoff)}'
    if dry_run:
        st, resp = http('POST', f'{STORE}/{RAW_INDEX}/_count', {'query': body['query']})
        checked_response(st, resp, 'prune count')
        if not isinstance(resp.get('count'), int):
            raise SystemExit('prune count incomplete')
        cnt = resp['count']
        print(f'[prune] DRY-RUN {scope} 的文档 {cnt} 条（未删除）')
        return cnt
    st, resp = http('POST', f'{STORE}/{RAW_INDEX}/_delete_by_query?refresh=true&conflicts=proceed',
                    body, timeout=180)
    checked_response(st, resp, 'prune delete')
    if not isinstance(resp.get('deleted'), int):
        raise SystemExit('prune delete incomplete')
    deleted = resp['deleted']
    print(f'[prune] {scope} 已删除 {deleted} 条')
    return deleted


def cmd_prune(args):
    hours = retention_hours(args)
    do_prune(hours, prefix_for(args.tag), dry_run=args.dry_run, sides=sides_of(args))
    return 0


# ----------------------------------------------------------------- status
def cmd_status(args):
    prefix = prefix_for(args.tag)
    sides = sides_of(args)
    pq = prefix_query(sides, prefix)
    body = {
        'size': 0,
        'track_total_hits': True,
        'query': {'bool': {'filter': [pq]}},
        'aggs': {
            'mn': {'min': {'field': 'timestamp'}},
            'mx': {'max': {'field': 'timestamp'}},
            'kinds': {'terms': {'field': 'metadata.name', 'size': 5}},
        },
    }
    st, resp = http('POST', f'{STORE}/{RAW_INDEX}/_search', body)
    if st != 200 or not isinstance(resp, dict):
        print(f'[status] 查询失败: {st} {resp}')
        return 1
    checked_response(st, resp, 'status search')
    total = resp['hits']['total']['value']
    agg = resp['aggregations']
    print(f'[status] 前缀={prefix} cluster={CLUSTER} 维度={"/".join(sides)}')
    print(f'[status] 文档总数={total:,}  时间范围={fmt(agg["mn"]["value"])} -> {fmt(agg["mx"]["value"])}')
    for kb in agg['kinds']['buckets']:
        side = 'index' if kb['key'] == 'index_stats' else 'node'
        ef, sf = ENTITY_FIELD[side], STEP_FIELD[side]
        print(f'[status] {kb["key"]}: {kb["doc_count"]:,} 条；各实体最新 step:')
        sbody = {'size': 0, 'query': {'bool': {'filter': [
            prefix_query((side,), prefix)]}},
            'aggs': {'g': {'terms': {'field': ef, 'size': 200},
                           'aggs': {'step': {'max': {'field': sf}}}}}}
        sst, sresp = http('POST', f'{STORE}/{RAW_INDEX}/_search', sbody)
        checked_response(sst, sresp, 'status entity search')
        for b in (sresp.get('aggregations', {}).get('g', {}).get('buckets') or []):
            print(f'  {b["key"]:<32} step={int(b["step"]["value"] or 0):,}  docs={b["doc_count"]:,}')
    return 0


# ----------------------------------------------------------------- resample（真值导出）
def resample_side(side, prefix, start_ms, end_ms, step_s, out_f):
    """Export every source document at its actual timestamp, without thinning."""
    if step_s <= 0 or start_ms > end_ms:
        raise ValueError('invalid export window or step')
    metric = METRIC_NAME[side]
    field = 'indices' if side == 'index' else 'nodes'
    body = {'size': 1000, 'track_total_hits': True,
            'query': {'bool': {'filter': [prefix_query((side,), prefix),
                {'range': {'timestamp': {'gte': start_ms, 'lte': end_ms,
                                         'format': 'epoch_millis'}}}]}},
            'sort': [{'timestamp': {'order': 'asc'}}, {'_doc': {'order': 'asc'}}]}
    scroll_id = None
    count, n, total = 0, 0, None
    names, rows, current_ts = set(), [], None
    try:
        st, resp = http('POST', f'{STORE}/{RAW_INDEX}/_search?scroll=2m', body, timeout=300)
        while True:
            checked_response(st, resp, 'resample scroll')
            scroll_id = resp.get('_scroll_id') or scroll_id
            try:
                page = resp['hits']['hits']
                if not isinstance(page, list):
                    raise ValueError('invalid page')
                if total is None:
                    total_obj = resp['hits']['total']
                    if isinstance(total_obj, dict):
                        if total_obj.get('relation') != 'eq':
                            raise ValueError('inexact total')
                        total_obj = total_obj['value']
                    if isinstance(total_obj, bool) or not isinstance(total_obj, int) or total_obj < 0:
                        raise ValueError('invalid total')
                    total = total_obj
                if not page:
                    if count != total:
                        raise ValueError(f'expected {total} documents, received {count}')
                    break
                if not scroll_id:
                    raise ValueError('missing scroll id')
                for hit in page:
                    src = hit['_source']
                    ts = src['timestamp']
                    labels = src['metadata']['labels']
                    name = labels['index_name' if side == 'index' else 'node_name']
                    value = src['payload']['elasticsearch'][metric]
                    if (isinstance(ts, bool) or not isinstance(ts, int) or
                            not start_ms <= ts <= end_ms or not isinstance(name, str) or
                            not name.startswith(prefix) or not isinstance(value, dict) or
                            (current_ts is not None and ts < current_ts)):
                        raise ValueError('invalid or unsorted source')
                    if ts != current_ts:
                        for row in rows:
                            out_f.write(json.dumps({'ts': current_ts, field: row}, ensure_ascii=False) + '\n')
                            n += 1
                        rows = []
                        current_ts = ts
                    if side == 'node':
                        value = dict(value)
                        value['transport_address'] = labels.get('transport_address', value.get('transport_address', ''))
                    # Preserve duplicates in separate rows; JSON object keys are unique.
                    row = next((row for row in rows if name not in row), None)
                    if row is None:
                        row = {}
                        rows.append(row)
                    row[name] = value
                    names.add(name)
                    count += 1
                if count > total:
                    raise ValueError('received more documents than total')
            except (KeyError, TypeError, ValueError) as exc:
                raise SystemExit(f'resample incomplete: {exc}') from exc
            st, resp = http('POST', f'{STORE}/_search/scroll',
                            {'scroll': '2m', 'scroll_id': scroll_id}, timeout=300)
        for row in rows:
            out_f.write(json.dumps({'ts': current_ts, field: row}, ensure_ascii=False) + '\n')
            n += 1
        return n, sorted(names)
    finally:
        if scroll_id:
            http('DELETE', f'{STORE}/_search/scroll', {'scroll_id': [scroll_id]})


def entities_map_node(prefix):
    return {prefix + n: (n, addr) for n, _, _, addr in NODE_ENTITIES}


def cmd_resample(args):
    prefix = prefix_for(args.tag)
    sides = sides_of(args)
    end = int(time.time() * 1000)
    start = end - int(args.hours * 3600 * 1000)
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'work', 'raw')
    os.makedirs(out_dir, exist_ok=True)
    stamp = dt.datetime.now().strftime('%Y%m%d_%H%M%S')
    print(f'[resample] cluster={CLUSTER} 前缀={prefix} 维度={"/".join(sides)} '
          f'窗口 {fmt(start)} -> {fmt(end)}  兼容参数 step={args.step}s')
    print('[resample] --step is compatibility-only; exporting every raw document without downsampling')
    print('[resample] 32 compares adjacent rows: sparse or duplicate timestamps may not yield reliable rates')
    for side in sides:
        path = os.path.join(out_dir, f"{'stats' if side == 'index' else 'nodes'}_{args.tag}_{stamp}.jsonl")
        partial = path + '.partial'
        with open(partial, 'x', encoding='utf-8') as out_f:
            n, names = resample_side(side, prefix, start, end, args.step, out_f)
        # Never expose a partial export as a completed JSONL file.
        os.link(partial, path)
        os.unlink(partial)
        print(f'[resample:{side}] {n} 个采样点 × {len(names)} 实体 -> {path}')
        if names:
            print(f'[resample:{side}] 实体: {names}')
    print('[resample] 提示：32_verify_topn.py --cluster ' + CLUSTER + ' 即可用这份真值评测')
    return 0


# ----------------------------------------------------------------- expected（解析式期望榜）
def cmd_expected(args):
    prefix = prefix_for(args.tag)
    sides = sides_of(args)
    out = {'_reliability': 'model-only estimates; not measured truth; assumes equal steps and unmodified synthetic data'}
    for side in sides:
        ents = entities_of(side)
        by_rate = [prefix + n for n, ps, _, _ in sorted(
            [(e[0], e[1], e[2], e[3]) for e in ents], key=lambda x: -x[1])]
        by_latency = [prefix + n for n, _, f, _ in sorted(ents, key=lambda x: -x[2])]
        kinds = {'rate_order': by_rate, 'latency_order': by_latency}
        if side == 'index':
            kinds['docs_count_order'] = by_rate      # docs = 1000 + idx_total → 同 rate 序
            kinds['indexing_bytes_order'] = by_rate  # store 随 idx_total 增长
        else:
            kinds['docs_count_order'] = [prefix + n for n, ps, _, _ in
                                         sorted(ents, key=lambda x: -x[1])]
        out[side] = kinds
    text = json.dumps(out, ensure_ascii=False, indent=2)
    print(text)
    if args.out:
        out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'work', 'rollup')
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, args.out)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(text + '\n')
        print(f'[expected] 已写入 {path}')
    print('[expected] MODEL-ONLY / UNVERIFIED: not measured ground truth')
    print('[expected] 说明：rate 榜按每步增量降序，latency 榜按实体时延系数降序；'
          'peak 类（缓存/段内存等波动型）不在期望榜内，用 32 的细桶对照覆盖。')
    return 0


def fmt(ms):
    if not ms:
        return '—'
    return dt.datetime.fromtimestamp(ms / 1000).strftime('%Y-%m-%d %H:%M')


def main():
    ap = argparse.ArgumentParser(description='TopN 监控指标数据维护（离线填数/在线数据流/真值导出/清理）')
    sub = ap.add_subparsers(dest='cmd', required=True)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument('--tag', default='main', help='数据隔离标签，仅允许字母、数字、下划线（默认 main）')
    common.add_argument('--interval', type=int, default=900, help='采样间隔秒（默认 900=15min）')
    common.add_argument('--side', choices=['index', 'node', 'both'], default='index',
                        help='注入/导出维度（默认 index；both=索引+节点）')

    p_fill = sub.add_parser('fill', parents=[common], help='离线填数：一次性预填充历史数据')
    p_fill.add_argument('--days', type=float, default=90, help='往回填充天数（默认 90）')

    p_stream = sub.add_parser('stream', parents=[common], help='在线数据流：前台持续写入，Ctrl-C 停止')
    p_stream.add_argument('--auto-prune', action='store_true',
                          help='开启自动清理（默认关）；删除保留窗口之外的数据')
    p_stream.add_argument('--hours', type=float, default=None,
                          help='保留窗口小时数（默认 2160=90天；边界对齐整点）')
    p_stream.add_argument('--days', type=float, default=None,
                          help='保留窗口天数，等价 hours=days×24（与 --hours 二选一）')
    p_stream.add_argument('--prune-every-hours', type=float, default=1,
                          help='每隔多少小时检查一次清理（默认 1）')

    p_resample = sub.add_parser('resample', parents=[common],
                                help='把注入数据导出为 32/33 消费的真值 JSONL')
    p_resample.add_argument('--hours', type=float, default=6,
                            help='导出最近多少小时（默认 6）')
    p_resample.add_argument('--step', type=int, default=10,
                            help='兼容旧参数，不降采样；导出窗口内所有原始文档（默认 10）')

    sub.add_parser('expected', parents=[common], help='导出解析式期望榜（rate/latency 排序声明）')
    ap_expected = sub.choices['expected']
    ap_expected.add_argument('--out', default=None, help='写入 work/rollup/<file>（可选）')

    p_prune = sub.add_parser('prune', parents=[common], help='删除保留窗口之外的合成指标')
    p_prune.add_argument('--hours', type=float, default=None,
                         help='保留窗口小时数（默认 2160=90天；边界对齐整点）')
    p_prune.add_argument('--days', type=float, default=None,
                         help='保留窗口天数，等价 hours=days×24（兼容旧参数）')
    p_prune.add_argument('--dry-run', action='store_true', help='只统计不删除')

    sub.add_parser('status', parents=[common], help='查看数据量/时间范围/各实体 step')

    args = ap.parse_args()
    try:
        prefix_for(args.tag)
        if not CLUSTER or not CLUSTER.strip():
            raise ValueError('CLUSTER must not be empty')
        for key in ('interval', 'days', 'hours', 'step', 'prune_every_hours'):
            value = getattr(args, key, None)
            if value is not None and (not math.isfinite(value) or value <= 0):
                raise ValueError(f'--{key.replace("_", "-")} must be finite and positive')
        if args.cmd in ('stream', 'prune') and args.hours is not None and args.days is not None:
            raise ValueError('--hours and --days are mutually exclusive')
        if args.cmd == 'expected' and args.out and (os.path.basename(args.out) != args.out or args.out in ('.', '..')):
            raise ValueError('--out must be a filename')
    except ValueError as exc:
        ap.error(str(exc))
    return {'fill': cmd_fill, 'stream': cmd_stream, 'resample': cmd_resample,
            'expected': cmd_expected, 'prune': cmd_prune, 'status': cmd_status}[args.cmd](args)


if __name__ == '__main__':
    sys.exit(main())
