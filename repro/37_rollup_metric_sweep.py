#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
37_rollup_metric_sweep.py —— 全量 TopN 指标的 rollup 可行性扫描（造数 + 作业 + 逐指标对照）

目的：
  35/36 验证了单字段峰值（index_storage）的 rollup 等价性与长窗提速。本脚本把
  Advanced Agentless 全部 TopN 指标（index 37 = 17 peak + 13 rate + 7 latency；
  node 70 = 27 peak + 33 rate + 7 latency + 3 ratio；逐字取自 console-topn-raw 的
  node_metrics.go / index_metrics.go switch 与 coverage 测试）逐个对照
  「rollup 表达」vs「raw 精确口径」，量化可行性与误差。

rollup 表达（按 topn.NewMetric 的 mode 规则）：
  peak    max(field) 的 max-of-max —— 精确（float32 相对噪声 ~1e-7）
  rate    Δmax(field)/桶宽秒，负差分跳过 —— 与 raw 的桶 max 差分同规则，数值精确；
          丢失身份连续性（index_uuid 跨桶）证据 → 同名索引重建会被错误计入
  latency Δmax(field)/Δmax(field2)：单调计数器下两个 max 同源于桶内最后观测，
          即 raw 的配对结果 —— 数值精确；平手观测（field2 相同）无法表达 field desc 次序
  ratio   max(a)/max(b) 近似 max(a/b)（disk 反向 available/total）—— 真近似：
          分母稳定时精确，分母波动（磁盘缩容）会低估尖峰

真值口径（monotone-equivalent truth）：
  合成计数器严格单调，raw 代表性观测（field2 desc / ts desc / field desc）= 桶内
  ops 最大观测，用 top_hits 按同序排序取一条；rate 真值 = raw 桶 max 差分（分支同规则）；
  index 侧真值叠加 uuid 身份校验（相邻桶 uuid 变化即断线）；ratio 真值 = 分支同款
  painless 逐观测 a/b*100 取 max。对抗实体：index 的 synth-sw-062（计数器重置）与
  synth-sw-063（同名换 uuid）；node 的 node-sw-30（breaker 跳变）与 node-sw-31
  （fs.total 缩容 10 倍制造 ratio 尖峰）。

用法：
  python3 37_rollup_metric_sweep.py --seed index --store http://127.0.0.1:19222
  python3 37_rollup_metric_sweep.py --seed node  --store http://127.0.0.1:19222
  python3 37_rollup_metric_sweep.py --rollup     --store http://127.0.0.1:19222
  python3 37_rollup_metric_sweep.py --sweep index --store http://127.0.0.1:19222
  python3 37_rollup_metric_sweep.py --sweep node  --store http://127.0.0.1:19222
  python3 37_rollup_metric_sweep.py --cleanup    --store http://127.0.0.1:19222
"""
import argparse
import json
import os
import statistics
import time
import urllib.request

STORE = os.environ.get('STORE', 'http://127.0.0.1:9200')
TPL = '/home/user/github/console-topn-raw/config/setup/easysearch/template_rollup.tpl'
IP = 'payload.elasticsearch.index_stats.'
NP = 'payload.elasticsearch.node_stats.'
ENT_FIELD = {'index': 'metadata.labels.index_name', 'node': 'metadata.labels.transport_address'}
SRC_NAME = {'index': 'index_stats', 'node': 'node_stats'}

# ---------------------------------------------------------------- 指标清单（key, field, field2, derivative）
INDEX_METRICS = [
    ('index_storage', IP + 'total.store.size_in_bytes', '', False),
    ('segment_count', IP + 'total.segments.count', '', False),
    ('doc_count', IP + 'total.docs.count', '', False),
    ('docs_deleted', IP + 'total.docs.deleted', '', False),
    ('query_cache', IP + 'total.query_cache.memory_size_in_bytes', '', False),
    ('request_cache', IP + 'total.request_cache.memory_size_in_bytes', '', False),
    ('fielddata_cache', IP + 'total.fielddata.memory_size_in_bytes', '', False),
    ('segment_memory', IP + 'total.segments.memory_in_bytes', '', False),
    ('segment_doc_values_memory', IP + 'total.segments.doc_values_memory_in_bytes', '', False),
    ('segment_terms_memory', IP + 'total.segments.terms_memory_in_bytes', '', False),
    ('segment_fields_memory', IP + 'total.segments.stored_fields_memory_in_bytes', '', False),
    ('segment_index_writer_memory', IP + 'total.segments.index_writer_memory_in_bytes', '', False),
    ('segment_term_vectors_memory', IP + 'total.segments.term_vectors_memory_in_bytes', '', False),
    ('segment_norms_memory', IP + 'total.segments.norms_memory_in_bytes', '', False),
    ('segment_points_memory', IP + 'total.segments.points_memory_in_bytes', '', False),
    ('segment_version_map', IP + 'total.segments.version_map_memory_in_bytes', '', False),
    ('segment_fixed_bit_set', IP + 'total.segments.fixed_bit_set_memory_in_bytes', '', False),
    ('query_times', IP + 'total.search.query_total', '', True),
    ('fetch_times', IP + 'total.search.fetch_total', '', True),
    ('scroll_times', IP + 'total.search.scroll_total', '', True),
    ('merge_times', IP + 'total.merges.total', '', True),
    ('refresh_times', IP + 'total.refresh.total', '', True),
    ('flush_times', IP + 'total.flush.total', '', True),
    ('indexing_rate', IP + 'primaries.indexing.index_total', '', True),
    ('indexing_bytes', IP + 'primaries.store.size_in_bytes', '', True),
    ('request_cache_hit', IP + 'total.request_cache.hit_count', '', True),
    ('request_cache_miss', IP + 'total.request_cache.miss_count', '', True),
    ('query_cache_count', IP + 'total.query_cache.cache_count', '', True),
    ('query_cache_hit', IP + 'total.query_cache.hit_count', '', True),
    ('query_cache_miss', IP + 'total.query_cache.miss_count', '', True),
    ('indexing_latency', IP + 'primaries.indexing.index_time_in_millis', IP + 'primaries.indexing.index_total', True),
    ('query_latency', IP + 'total.search.query_time_in_millis', IP + 'total.search.query_total', True),
    ('fetch_latency', IP + 'total.search.fetch_time_in_millis', IP + 'total.search.fetch_total', True),
    ('merge_latency', IP + 'total.merges.total_time_in_millis', IP + 'total.merges.total', True),
    ('refresh_latency', IP + 'total.refresh.total_time_in_millis', IP + 'total.refresh.total', True),
    ('scroll_latency', IP + 'total.search.scroll_time_in_millis', IP + 'total.search.scroll_total', True),
    ('flush_latency', IP + 'total.flush.total_time_in_millis', IP + 'total.flush.total', True),
]
NODE_METRICS = [
    ('cpu', NP + 'process.cpu.percent', '', False),
    ('os_cpu', NP + 'os.cpu.percent', '', False),
    ('os_used_mem', NP + 'os.mem.used_percent', '', False),
    ('os_load_average_1m', NP + 'os.cpu.load_average.1m', '', False),
    ('open_file', NP + 'process.open_file_descriptors', '', False),
    ('query_cache', NP + 'indices.query_cache.memory_size_in_bytes', '', False),
    ('request_cache', NP + 'indices.request_cache.memory_size_in_bytes', '', False),
    ('fielddata_cache', NP + 'indices.fielddata.memory_size_in_bytes', '', False),
    ('http_connect_num', NP + 'http.current_open', '', False),
    ('segment_count', NP + 'indices.segments.count', '', False),
    ('segment_memory', NP + 'indices.segments.memory_in_bytes', '', False),
    ('segment_stored_fields_memory', NP + 'indices.segments.stored_fields_memory_in_bytes', '', False),
    ('segment_terms_memory', NP + 'indices.segments.terms_memory_in_bytes', '', False),
    ('segment_doc_values_memory', NP + 'indices.segments.doc_values_memory_in_bytes', '', False),
    ('segment_index_writer_memory', NP + 'indices.segments.index_writer_memory_in_bytes', '', False),
    ('segment_term_vectors_memory', NP + 'indices.segments.term_vectors_memory_in_bytes', '', False),
    ('docs_count', NP + 'indices.docs.count', '', False),
    ('docs_deleted', NP + 'indices.docs.deleted', '', False),
    ('index_storage', NP + 'indices.store.size_in_bytes', '', False),
    ('jvm_heap_used_percent', NP + 'jvm.mem.heap_used_percent', '', False),
    ('jvm_mem_young_used', NP + 'jvm.mem.pools.young.used_in_bytes', '', False),
    ('jvm_mem_young_peak_used', NP + 'jvm.mem.pools.young.peak_used_in_bytes', '', False),
    ('jvm_mem_old_used', NP + 'jvm.mem.pools.old.used_in_bytes', '', False),
    ('jvm_mem_old_peak_used', NP + 'jvm.mem.pools.old.peak_used_in_bytes', '', False),
    ('jvm_used_heap', NP + 'jvm.mem.heap_used_in_bytes', '', False),
    ('transport_outbound_connections', NP + 'transport.total_outbound_connections', '', False),
    ('scroll_open_contexts', NP + 'indices.search.open_contexts', '', False),
    ('indexing_rate', NP + 'indices.indexing.index_total', '', True),
    ('indexing_bytes', NP + 'indices.store.size_in_bytes', '', True),
    ('query_rate', NP + 'indices.search.query_total', '', True),
    ('fetch_rate', NP + 'indices.search.fetch_total', '', True),
    ('scroll_rate', NP + 'indices.search.scroll_total', '', True),
    ('refresh_rate', NP + 'indices.refresh.total', '', True),
    ('flush_rate', NP + 'indices.flush.total', '', True),
    ('merges_rate', NP + 'indices.merges.total', '', True),
    ('request_cache_hit', NP + 'indices.request_cache.hit_count', '', True),
    ('request_cache_miss', NP + 'indices.request_cache.miss_count', '', True),
    ('query_cache_count', NP + 'indices.query_cache.cache_count', '', True),
    ('query_cache_hit', NP + 'indices.query_cache.hit_count', '', True),
    ('query_cache_miss', NP + 'indices.query_cache.miss_count', '', True),
    ('http_rate', NP + 'http.total_opened', '', True),
    ('jvm_young_gc_rate', NP + 'jvm.gc.collectors.young.collection_count', '', True),
    ('jvm_young_gc_latency', NP + 'jvm.gc.collectors.young.collection_time_in_millis', '', True),
    ('jvm_old_gc_rate', NP + 'jvm.gc.collectors.old.collection_count', '', True),
    ('jvm_old_gc_latency', NP + 'jvm.gc.collectors.old.collection_time_in_millis', '', True),
    ('transport_tx_rate', NP + 'transport.tx_count', '', True),
    ('transport_rx_rate', NP + 'transport.rx_count', '', True),
    ('transport_tx_bytes', NP + 'transport.tx_size_in_bytes', '', True),
    ('transport_rx_bytes', NP + 'transport.rx_size_in_bytes', '', True),
    ('total_io_operations', NP + 'fs.io_stats.total.operations', '', True),
    ('total_read_io_operations', NP + 'fs.io_stats.total.read_operations', '', True),
    ('total_write_io_operations', NP + 'fs.io_stats.total.write_operations', '', True),
    ('parent_breaker', NP + 'breakers.parent.tripped', '', True),
    ('accounting_breaker', NP + 'breakers.accounting.tripped', '', True),
    ('fielddata_breaker', NP + 'breakers.fielddata.tripped', '', True),
    ('request_breaker', NP + 'breakers.request.tripped', '', True),
    ('in_flight_requests_breaker', NP + 'breakers.in_flight_requests.tripped', '', True),
    ('model_inference_breaker', NP + 'breakers.model_inference.tripped', '', True),
    ('eql_sequence_breaker', NP + 'breakers.eql_sequence.tripped', '', True),
    ('inflight_requests_breaker', NP + 'breakers.inflight_requests.tripped', '', True),
    ('indexing_latency', NP + 'indices.indexing.index_time_in_millis', NP + 'indices.indexing.index_total', True),
    ('query_latency', NP + 'indices.search.query_time_in_millis', NP + 'indices.search.query_total', True),
    ('fetch_latency', NP + 'indices.search.fetch_time_in_millis', NP + 'indices.search.fetch_total', True),
    ('scroll_latency', NP + 'indices.search.scroll_time_in_millis', NP + 'indices.search.scroll_total', True),
    ('merge_latency', NP + 'indices.merges.total_time_in_millis', NP + 'indices.merges.total', True),
    ('refresh_latency', NP + 'indices.refresh.total_time_in_millis', NP + 'indices.refresh.total', True),
    ('flush_latency', NP + 'indices.flush.total_time_in_millis', NP + 'indices.flush.total', True),
    ('os_used_swap', NP + 'os.swap.used_in_bytes', NP + 'os.swap.total_in_bytes', False),
    ('open_file_percent', NP + 'process.open_file_descriptors', NP + 'process.max_file_descriptors', False),
    ('disk', NP + 'fs.total.total_in_bytes', NP + 'fs.total.available_in_bytes', False),
]
RATIO_REVERSE = {'disk'}  # 分支 reverseRatio：disk = available/total

SIDES = {
    'index': {'src': 'sweep_index_src', 'dst': 'rollup_sweep_index', 'job': 'rollup_sweep_index',
              'tpl_job': 'rollup_index_stats', 'prefix': 'synth-sw-', 'cluster': 'swab', 'entities': 64,
              'metrics': INDEX_METRICS, 'manifest': 'sweep_index.json'},
    'node': {'src': 'sweep_node_src', 'dst': 'rollup_sweep_node', 'job': 'rollup_sweep_node',
             'tpl_job': 'rollup_node_stats', 'prefix': 'node-sw-', 'cluster': 'swbn', 'entities': 32,
             'metrics': NODE_METRICS, 'manifest': 'sweep_node.json'},
}


def http(method, url, body=None, timeout=600):
    import urllib.error
    h = {}
    data = None
    if body is not None:
        if isinstance(body, (dict, list)):
            data = json.dumps(body).encode()
            h['Content-Type'] = 'application/json'
        else:
            data = body.encode()
            h['Content-Type'] = 'application/x-ndjson'
    req = urllib.request.Request(url, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            text = r.read().decode()
            return json.loads(text) if text.strip().startswith(('{', '[')) else text
    except urllib.error.HTTPError as e:
        raise RuntimeError(f'{method} {url} -> {e.code}: {e.read().decode()[:300]}') from None


def manifest_path(side):
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), 'work', 'rollup', SIDES[side]['manifest'])


def fields_of(side):
    seen, out = set(), []
    for _, f, f2, _ in SIDES[side]['metrics']:
        for x in (f, f2):
            if x and x not in seen:
                seen.add(x)
                out.append(x)
    return out


# ---------------------------------------------------------------- 造数
def event_step(steps):
    return 6 * (steps // 12)  # 中段、分钟对齐（10s 采样每分钟 6 步）


def idx_obs(i, s, inc, noise, uuid):
    segs = 8 + (i * 3 + s // 1800) % 40
    seg_mem = segs * 2_000_000 + noise % 500_001
    q = inc * 3 // 5
    return {
        'total': {
            'store': {'size_in_bytes': 10_000_000 + i * 500_000 + inc * 512 + noise % 100_001},
            'segments': {'count': segs, 'memory_in_bytes': seg_mem,
                         'doc_values_memory_in_bytes': seg_mem * 4 // 10,
                         'terms_memory_in_bytes': seg_mem * 15 // 100,
                         'stored_fields_memory_in_bytes': seg_mem * 2 // 10,
                         'index_writer_memory_in_bytes': seg_mem // 10,
                         'term_vectors_memory_in_bytes': seg_mem * 3 // 100,
                         'norms_memory_in_bytes': seg_mem // 50,
                         'points_memory_in_bytes': seg_mem // 25,
                         'version_map_memory_in_bytes': seg_mem // 100,
                         'fixed_bit_set_memory_in_bytes': seg_mem // 200},
            'docs': {'count': 1000 + inc, 'deleted': inc // 7},
            'query_cache': {'memory_size_in_bytes': 50_000_000 + (i * 104729 + (s // 3600) * 83861) % 20_000_001,
                            'cache_count': q // 3, 'hit_count': q // 4, 'miss_count': q // 6},
            'request_cache': {'memory_size_in_bytes': 20_000_000 + (i * 31337 + (s // 1800) * 60789) % 8_000_001,
                              'hit_count': q // 2, 'miss_count': q // 5},
            'fielddata': {'memory_size_in_bytes': 5_000_000 + (i * 7907 + (s // 7200) * 26539) % 3_000_001},
            'search': {'query_total': q, 'query_time_in_millis': q * 25 // 2,
                       'fetch_total': q // 10, 'fetch_time_in_millis': q // 10 * 7,
                       'scroll_total': q // 20, 'scroll_time_in_millis': q // 20 * 8},
            'merges': {'total': inc // 100 + 1, 'total_time_in_millis': (inc // 100 + 1) * 25},
            'refresh': {'total': inc // 30 + 1, 'total_time_in_millis': (inc // 30 + 1) * 3},
            'flush': {'total': inc // 150 + 1, 'total_time_in_millis': (inc // 150 + 1) * 180},
        },
        'primaries': {
            'store': {'size_in_bytes': (10_000_000 + i * 500_000 + inc * 512 + noise % 100_001) * 95 // 100},
            'indexing': {'index_total': inc, 'index_time_in_millis': inc * 2},
        },
    }


def node_obs(i, s, inc, noise):
    segs = 15 + (i * 2 + s // 900) % 35
    seg_mem = segs * 3_000_000 + noise % 900_001
    q = inc * 3 // 5
    young = 100_000_000 + (s % 540) * 400_000 + i * 1_000_000
    old = 200_000_000 + (inc * 32) % 350_000_000
    gyc = inc // 2000 + 1
    storm = 1_000 if i == 30 and (s // 7200) % 5 == 0 else 0
    fs_total = 50_000_000 if i == 31 and s >= node_obs.disk_collapse else 500_000_000 + i * 1_000_000
    return {
        'process': {'cpu': {'percent': 20 + (i * 11 + s) % 50},
                    'open_file_descriptors': 500 + (i * 37 + (s // 300) * 23) % 400,
                    'max_file_descriptors': 65536},
        'os': {'cpu': {'percent': 15 + (i * 5 + s // 2) % 60,
                       'load_average': {'1m': (100 + (i + s // 60) % 800) / 10.0}},
               'mem': {'used_percent': 40 + (i * 3 + s // 600) % 30},
               'swap': {'used_in_bytes': 100_000_000 + (i * 9973 + (s // 60) * 811) % 400_000_001,
                        'total_in_bytes': 1_000_000_000}},
        'indices': {
            'query_cache': {'memory_size_in_bytes': 30_000_000 + noise % 10_000_001,
                            'cache_count': q // 3, 'hit_count': q // 4, 'miss_count': q // 6},
            'request_cache': {'memory_size_in_bytes': 15_000_000 + (i * 7699 + s) % 5_000_001,
                              'hit_count': q // 2, 'miss_count': q // 5},
            'fielddata': {'memory_size_in_bytes': 8_000_000 + (i * 57731 + s // 60) % 4_000_001},
            'segments': {'count': segs, 'memory_in_bytes': seg_mem,
                         'stored_fields_memory_in_bytes': seg_mem * 2 // 10,
                         'terms_memory_in_bytes': seg_mem * 15 // 100,
                         'doc_values_memory_in_bytes': seg_mem * 4 // 10,
                         'index_writer_memory_in_bytes': seg_mem // 10,
                         'term_vectors_memory_in_bytes': seg_mem * 3 // 100},
            'docs': {'count': 1000 + inc // 10, 'deleted': inc // 50},
            'store': {'size_in_bytes': 20_000_000 + i * 300_000 + inc * 16 + noise % 50_001},
            'search': {'query_total': q, 'query_time_in_millis': q * 25 // 2,
                       'fetch_total': q // 10, 'fetch_time_in_millis': q // 10 * 7,
                       'scroll_total': q // 20, 'scroll_time_in_millis': q // 20 * 8,
                       'open_contexts': (s // 600 + i) % 6},
            'indexing': {'index_total': inc, 'index_time_in_millis': inc * 2},
            'refresh': {'total': inc // 30 + 1, 'total_time_in_millis': (inc // 30 + 1) * 3},
            'flush': {'total': inc // 150 + 1, 'total_time_in_millis': (inc // 150 + 1) * 180},
            'merges': {'total': inc // 100 + 1, 'total_time_in_millis': (inc // 100 + 1) * 25},
        },
        'jvm': {'mem': {'heap_used_percent': 30 + (i * 13 + s // 120) % 50,
                        'pools': {'young': {'used_in_bytes': young,
                                            'peak_used_in_bytes': 400_000_000 + i * 1_000_000},
                                  'old': {'used_in_bytes': old,
                                          'peak_used_in_bytes': 350_000_000 + i * 500_000}},
                        'heap_used_in_bytes': young // 2 + old},
                'gc': {'collectors': {
                    'young': {'collection_count': gyc,
                              'collection_time_in_millis': gyc * 300 + (50_000 if (s // 600) % 7 == i % 7 else 0)},
                    'old': {'collection_count': inc // 20000 + 1,
                            'collection_time_in_millis': (inc // 20000 + 1) * 2000}}}},
        'transport': {'total_outbound_connections': 3 + i % 7,
                      'tx_count': inc // 3, 'rx_count': inc // 2,
                      'tx_size_in_bytes': inc // 3 * 1400, 'rx_size_in_bytes': inc // 2 * 900},
        'fs': {'io_stats': {'total': {'operations': inc, 'read_operations': inc // 3,
                                      'write_operations': inc // 2}},
               'total': {'total_in_bytes': fs_total,
                         'available_in_bytes': 250_000_000 - (inc * 16) % 50_000_001}},
        'breakers': {'parent': {'tripped': inc // 10000 + storm},
                     'accounting': {'tripped': inc // 20000},
                     'fielddata': {'tripped': inc // 30000},
                     'request': {'tripped': inc // 25000},
                     'in_flight_requests': {'tripped': inc // 50000},
                     'model_inference': {'tripped': inc // 60000},
                     'eql_sequence': {'tripped': inc // 70000},
                     'inflight_requests': {'tripped': inc // 80000}},
    }


def seed(args, side):
    cfg = SIDES[side]
    state_p = f'/tmp/sweep_seed_{side}.json'
    step_ms = 10_000
    steps = int(args.days * 86400 * 1000) // step_ms
    now_min = int(time.time() * 1000) // 60000 * 60000
    start = now_min - ((int(args.days * 86400) + 7200) // 60 * 60) * 1000
    covered = span // 60_000 if (span := steps * step_ms) else 0
    assert covered >= 2
    ev = event_step(steps)
    node_obs.disk_collapse = ev
    ents = cfg['entities']
    done = 0
    if getattr(args, 'resume', False) and os.path.exists(f'/tmp/sweep_seed_{side}.json'):
        with open(f'/tmp/sweep_seed_{side}.json', encoding='utf-8') as fh:
            st_state = json.load(fh)
        start, done = st_state['start'], st_state['done']
        print(f'[seed:{side}] resume from step {done}/{steps}', flush=True)
    else:
        try:
            http('DELETE', f'{args.store}/{cfg["src"]}')
        except Exception:
            pass
        resp = http('PUT', f'{args.store}/{cfg["src"]}', {
        'settings': {'number_of_shards': 1, 'number_of_replicas': 0, 'refresh_interval': '-1',
                     'index.translog.durability': 'async', 'index.translog.flush_threshold_size': '1gb'},
        'mappings': {'properties': {'timestamp': {'type': 'date'}},
                     'dynamic_templates': [{'strings': {'match_mapping_type': 'string',
                                                        'mapping': {'type': 'keyword'}}}]}})
        assert isinstance(resp, dict) and resp.get('acknowledged'), resp

    def mkdoc(i, s, ts):
        base = 1 + i % (5 if side == 'index' else 4)
        inc = base * (s + 1)
        if i == 62 and side == 'index' and s >= ev:
            inc = base * (s - ev + 1)  # adv-reset：计数器重置
        noise = (i * 7919 + s * 997) if side == 'index' else (i * 104729 + s * 7919)
        if side == 'index':
            uuid = 'u-a' if i != 63 or s < ev else 'u-b'  # adv-uuid：同名换 uuid
            return {'timestamp': ts,
                    'metadata': {'name': 'index_stats', 'category': 'elasticsearch',
                                 'labels': {'cluster_id': cfg['cluster'], 'cluster_uuid': 'swab-uuid',
                                            'index_id': f'id-{i:03d}', 'index_name': f'{cfg["prefix"]}{i:03d}',
                                            'index_uuid': uuid}},
                    'payload': {'elasticsearch': {'index_stats': idx_obs(i, s, inc, noise, uuid)}}}
        return {'timestamp': ts,
                'metadata': {'name': 'node_stats', 'category': 'elasticsearch',
                             'labels': {'cluster_id': cfg['cluster'], 'cluster_uuid': 'swbn-uuid',
                                        'node_id': f'nd-{i:02d}', 'node_name': f'{cfg["prefix"]}{i:02d}',
                                        'transport_address': f'127.0.0.1:93{10 + i:02d}'}},
                'payload': {'elasticsearch': {'node_stats': dict(
                    node_obs(i, s, inc, noise),
                    http={'current_open': 20 + (i + s // 120) % 60,
                          'total_opened': inc // 2})}}}

    # 渲染自检：两个实体各一条，必须能被 json 解析且字段齐全
    for probe in (0, ents - 1):
        json.loads(json.dumps(mkdoc(probe, ev, start + 1000)))
    json.loads(json.dumps(mkdoc(ents - 1, ev + 1, start + 2000)))

    lines, total, t0 = [], 0, time.time()
    act = json.JSONEncoder(separators=(',', ':')).encode
    for stp in range(done, steps):
        ts = start + stp * step_ms + 1
        for i in range(ents):
            lines.append('{"index":{}}\n' + act(mkdoc(i, stp, ts)) + '\n')
            total += 1
        if len(lines) >= 8000:
            flush_bulk(args, cfg, lines)
        if stp % 20000 == 0:
            print(f'[seed:{side}] step {stp}/{steps} docs {total} elapsed {time.time()-t0:.0f}s', flush=True)
        if args.max_seconds and time.time() > t0 + args.max_seconds and (stp + 1) % 6 == 0:
            flush_bulk(args, cfg, lines)
            with open(f'/tmp/sweep_seed_{side}.json', 'w', encoding='utf-8') as fh:
                json.dump({'start': start, 'done': stp + 1}, fh)
            print(f'[seed:{side}] slice done at step {stp + 1}/{steps}, rerun with --resume', flush=True)
            return
    flush_bulk(args, cfg, lines)
    http('POST', f'{args.store}/{cfg["src"]}/_refresh')
    count = http('GET', f'{args.store}/{cfg["src"]}/_count')['count']
    m = {'store': args.store, 'start': start, 'end': start + (covered - 1) * 60_000,
         'days': args.days, 'interval_sec': 10, 'entities': ents, 'docs': total, 'count': count,
         'seconds': round(time.time() - t0, 1), 'bucket': args.bucket, 'event_step': ev}
    os.makedirs(os.path.dirname(manifest_path(side)), exist_ok=True)
    with open(manifest_path(side), 'w', encoding='utf-8') as f:
        json.dump(m, f, ensure_ascii=False, indent=1)
    print(f'[seed:{side}] 完成 {count} docs（{m["seconds"]}s），manifest -> {manifest_path(side)}')


def flush_bulk(args, cfg, lines):
    if not lines:
        return
    resp = http('POST', f'{args.store}/{cfg["src"]}/_bulk?refresh=false'
                         '&filter_path=errors,items.*.error.type,items.*.error.reason,items.*.error.caused_by.reason',
                ''.join(lines), timeout=600)
    if resp.get('errors'):
        first = next(it for it in resp.get('items', [])
                     if isinstance(it, dict) and 'error' in it.get('index', {}))
        raise RuntimeError(f'bulk item error: {json.dumps(first.get("index", {}).get("error"))[:400]}')
    lines.clear()


# ---------------------------------------------------------------- rollup 作业
def rollup_jobs(args):
    with open(TPL, encoding='utf-8') as f:
        text = f.read()
    for side, cfg in SIDES.items():
        marker = f'PUT /_rollup/jobs/{cfg["tpl_job"]}'
        seg = text[text.index(marker):]
        body = json.JSONDecoder().raw_decode(seg[seg.index('{'):])[0]['rollup']
        body.update(source_index=cfg['src'], target_index=cfg['dst'], continuous=False,
                    delay=0, cron='* * * * *', page_size=args.page_size)
        try:
            http('DELETE', f'{args.store}/_rollup/jobs/{cfg["job"]}')
        except Exception:
            pass
        resp = http('PUT', f'{args.store}/_rollup/jobs/{cfg["job"]}', {'rollup': body})
        assert isinstance(resp, dict), (side, resp)
        http('POST', f'{args.store}/_rollup/jobs/{cfg["job"]}/_start')
        print(f'[rollup:{side}] {cfg["job"]} 已启动', flush=True)
    pending, deadline = dict(SIDES), time.time() + args.timeout
    while pending and time.time() < deadline:
        for side, cfg in list(pending.items()):
            meta = (http('GET', f'{args.store}/_rollup/jobs/{cfg["job"]}/_explain')
                    .get(cfg['job'], {}).get('rollup_metadata')) or {}
            s = meta.get('stats') or {}
            status = meta.get('status')
            print(f'[rollup:{side}] {status} pages={s.get("pages_processed")} docs={s.get("documents_processed")}',
                  flush=True)
            if status == 'finished':
                http('POST', f'{args.store}/{cfg["dst"]}/_refresh')
                srcn = http('GET', f'{args.store}/{cfg["src"]}/_count')['count']
                dstn = http('GET', f'{args.store}/{cfg["dst"]}/_count')['count']
                print(f'[rollup:{side}] 完成 raw {srcn} -> rollup {dstn}（{srcn / max(dstn, 1):.1f}:1）')
                del pending[side]
            elif status == 'failed':
                raise SystemExit(f'{side} 作业失败: {meta.get("failure_reason")}')
        if pending:
            time.sleep(60)
    if pending:
        raise SystemExit(f'超时未完成: {list(pending)}')


# ---------------------------------------------------------------- 对照扫描
def bucket_ms(text):
    unit = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}
    ms = int(text[:-1]) * unit[text[-1]] * 1000
    assert ms >= 60_000 and ms % 60_000 == 0
    return ms


def load_cfg(args, side):
    cfg = dict(SIDES[side])
    with open(manifest_path(side), encoding='utf-8') as f:
        m = json.load(f)
    cfg.update(start=m['start'], end_full=m['end'] + 59_999, bucket=args.bucket,
               store=args.store or m['store'], top=args.top)
    mapping = http('GET', f'{cfg["store"]}/{cfg["dst"]}/_mapping')
    meta = next(iter(mapping.values()))['mappings']['_meta']['rollups'][cfg['job']]
    cfg['abbr'] = {x['source_field']: x['target_field'] for x in meta['metrics']}
    return cfg


def per_bucket_max(cfg, route, start, end, flds):
    """terms(实体) × date_histogram(bucket) × 逐字段 max。route: raw | rollup"""
    direct = route == 'rollup'
    sfx = '.terms' if direct else ''
    entity = ENT_FIELD['index' if 'index' in cfg['src'] else 'node'] + sfx
    ts = 'timestamp.date_histogram' if direct else 'timestamp'
    aggs, order = {}, []
    for f in flds:
        name = 'f%d' % len(order)
        order.append(name)
        aggs[name] = {'max': {'field': (cfg['abbr'][f] + '.max') if direct else f}}
    filters = [{'term': {f'metadata.labels.cluster_id{sfx}': cfg['cluster']}},
               {'term': {f'metadata.category{sfx}': 'elasticsearch'}},
               {'range': {ts: {'gte': start, 'lte': end}}}]
    if direct:
        filters.append({'term': {'rollup._id': cfg['job']}})
    must = [] if direct else [{'term': {'metadata.name': SRC_NAME['index' if 'index' in cfg['src'] else 'node']}}]
    body = {'size': 0, 'track_total_hits': False,
            'query': {'bool': {'must': must, 'filter': filters,
                               'must_not': [{'term': {entity: '_all'}}]}},
            'aggs': {'entities': {'terms': {'field': entity, 'size': cfg['entities'], 'order': {'_key': 'asc'}},
                                  'aggs': {'curve': {'date_histogram': {'field': ts, 'fixed_interval': cfg['bucket'],
                                                                        'min_doc_count': 1}, 'aggs': aggs}}}}}
    idx = cfg['dst'] if direct else cfg['src']
    t0 = time.time()
    r = http('POST', f'{cfg["store"]}/{idx}/_search?enable_rollup=false&request_cache=false', body)
    took = (time.time() - t0) * 1000
    assert not r['timed_out'] and r['_shards']['failed'] == 0, str(r)[:300]
    out = {}
    for row in r['aggregations']['entities']['buckets']:
        data = {}
        for b in row['curve']['buckets']:
            vals = {}
            for name, f in zip(order, flds):
                v = b[name].get('value')
                if v is not None:
                    vals[f] = v
            if vals:
                data[b['key']] = vals
        out[row['key']] = data
    return out, took


def raw_top_hits(cfg, start, end, sort_field, source_fields):
    """每桶按 sort_field desc（缺算 0）+ ts desc 取一条观测。"""
    side = 'index' if 'index' in cfg['src'] else 'node'
    ent = ENT_FIELD[side]
    sorts = [{sort_field: {'order': 'desc', 'missing': 0}}, {'timestamp': {'order': 'desc'}}]
    body = {'size': 0, 'track_total_hits': False,
            'query': {'bool': {'filter': [
                {'term': {'metadata.labels.cluster_id': cfg['cluster']}},
                {'term': {'metadata.category': 'elasticsearch'}},
                {'term': {'metadata.name': SRC_NAME[side]}},
                {'range': {'timestamp': {'gte': start, 'lte': end}}}],
                'must_not': [{'term': {ent: '_all'}}]}},
            'aggs': {'entities': {'terms': {'field': ent, 'size': cfg['entities'], 'order': {'_key': 'asc'}},
                                  'aggs': {'curve': {'date_histogram': {'field': 'timestamp',
                                                                        'fixed_interval': cfg['bucket'],
                                                                        'min_doc_count': 1},
                                                     'aggs': {'top': {'top_hits': {'size': 1, 'sort': sorts,
                                                                                   '_source': source_fields}}}}}}}}
    t0 = time.time()
    r = http('POST', f'{cfg["store"]}/{cfg["src"]}/_search?enable_rollup=false&request_cache=false', body)
    took = (time.time() - t0) * 1000
    assert not r['timed_out'] and r['_shards']['failed'] == 0, str(r)[:300]
    out = {}
    for row in r['aggregations']['entities']['buckets']:
        data = {}
        for b in row['curve']['buckets']:
            hits = b['top']['hits']['hits']
            if hits:
                data[b['key']] = hits[0]['_source']
        out[row['key']] = data
    return out, took


def raw_ratio_truth(cfg, start, end, fa, fb):
    """分支同款 painless：逐观测 a/b*100 取桶内 max（fa/fb 已按 reverse 换好）。"""
    side = 'index' if 'index' in cfg['src'] else 'node'
    ent = ENT_FIELD[side]
    script = {'source': f"doc['{fb}'].size()>0 && doc['{fb}'].value>0 ? doc['{fa}'].value*100.0/doc['{fb}'].value : null"}
    body = {'size': 0, 'track_total_hits': False,
            'query': {'bool': {'filter': [
                {'term': {'metadata.labels.cluster_id': cfg['cluster']}},
                {'term': {'metadata.category': 'elasticsearch'}},
                {'term': {'metadata.name': SRC_NAME[side]}},
                {'range': {'timestamp': {'gte': start, 'lte': end}}}],
                'must_not': [{'term': {ent: '_all'}}]}},
            'aggs': {'entities': {'terms': {'field': ent, 'size': cfg['entities'], 'order': {'_key': 'asc'}},
                                  'aggs': {'curve': {'date_histogram': {'field': 'timestamp',
                                                                        'fixed_interval': cfg['bucket'],
                                                                        'min_doc_count': 1},
                                                     'aggs': {'r': {'max': {'script': script}}}}}}}}
    t0 = time.time()
    r = http('POST', f'{cfg["store"]}/{cfg["src"]}/_search?enable_rollup=false&request_cache=false', body)
    took = (time.time() - t0) * 1000
    assert not r['timed_out'] and r['_shards']['failed'] == 0, str(r)[:400]
    out = {}
    for row in r['aggregations']['entities']['buckets']:
        data = {}
        for b in row['curve']['buckets']:
            v = b['r'].get('value')
            if v is not None:
                data[b['key']] = v
        out[row['key']] = data
    return out, took


def get_path(doc, path):
    cur = doc
    for part in path.split('.'):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def diff_series(pairs, width_ms, identity=None):
    """pairs: {bucket: (v, v2 或 None)}；相邻桶差分，identity 变化/缺失断线。"""
    w = width_ms / 1000.0
    out, prev = {}, None
    for k in sorted(pairs):
        if prev is not None and int(k) - int(prev) == width_ms:
            if identity is not None and (not identity.get(k) or identity.get(k) != identity.get(prev)):
                prev = k
                continue
            va, v2a = pairs[k]
            pa, p2a = pairs[prev]
            if v2a is None:
                d = va - pa
                if d >= 0:
                    out[k] = d / w
            else:
                dt, dn = va - pa, v2a - p2a
                if dn > 0 and dt >= 0:
                    out[k] = dt / dn
        prev = k
    return out


def sweep(args, side):
    cfg = load_cfg(args, side)
    metrics = cfg['metrics']
    idx_side = 'index' in cfg['src']
    peak_fields = list(dict.fromkeys(x for _, f, f2, _ in metrics for x in ((f, f2) if f2 else (f,))))
    w = bucket_ms(cfg['bucket'])
    print(f'== {side}: {len(metrics)} 指标，{cfg["entities"]} 实体，桶宽 {cfg["bucket"]}，'
          f'字段 {len(peak_fields)}', flush=True)

    t0 = time.time()
    raw_max, t_rm = per_bucket_max(cfg, 'raw', cfg['start'], cfg['end_full'], peak_fields)
    print(f'raw max 聚合 {t_rm:.0f}ms', flush=True)
    src_fields = sorted(set(peak_fields) | ({'metadata.labels.index_uuid'} if idx_side else set()))
    ts_hits, t_th = raw_top_hits(cfg, cfg['start'], cfg['end_full'], 'timestamp', src_fields)
    uuid = {e: {k: get_path(v, 'metadata.labels.index_uuid')
                for k, v in d.items()} for e, d in ts_hits.items()} if idx_side else None
    print(f'ts-desc top_hits {t_th:.0f}ms', flush=True)
    ru_fields = [x for x in peak_fields if x in cfg['abbr']]
    ru_max, t_ru = per_bucket_max(cfg, 'rollup', cfg['start'], cfg['end_full'], ru_fields)
    print(f'rollup max 聚合 {t_ru:.0f}ms（累计 {time.time()-t0:.0f}s）', flush=True)

    ratio_took, hit_took = [], []
    results = []
    skipped = []
    for key, f, f2, deriv in metrics:
        if f not in cfg['abbr'] or (f2 and f2 not in cfg['abbr']):
            skipped.append(key)
            results.append((key, 'no-field', {'rank_equal': False, 'nt': 0, 'bad': 0,
                                              'missing': 0, 'extra': 0, 'maxrel': 1.0}))
            continue
        if not deriv and not f2:  # peak
            truth = {e: {k: v[f] for k, v in d.items() if f in v} for e, d in raw_max.items()}
            approx = {e: {k: v[f] for k, v in d.items() if f in v} for e, d in ru_max.items()}
            kind, rt = 'peak', compare(truth, approx)
        elif deriv and not f2:  # rate：raw 桶 max 差分（分支同规则）+ uuid 身份
            truth = {}
            for e, d in raw_max.items():
                pairs = {k: (v[f], None) for k, v in d.items() if f in v}
                truth[e] = diff_series(pairs, w, uuid.get(e) if uuid else None)
            approx = {}
            for e, d in ru_max.items():
                pairs = {k: (v[f], None) for k, v in d.items() if f in v}
                approx[e] = diff_series(pairs, w)
            kind, rt = 'rate', compare(truth, approx)
        elif deriv:  # latency：top_hits 按 field2 desc 配对
            pairs_raw, tk = raw_top_hits(cfg, cfg['start'], cfg['end_full'], f2,
                                         sorted({f, f2} | ({'metadata.labels.index_uuid'} if idx_side else set())))
            hit_took.append(tk)
            truth = {}
            for e, d in pairs_raw.items():
                pairs = {}
                for k, v in d.items():
                    fv, f2v = get_path(v, f), get_path(v, f2)
                    if isinstance(fv, (int, float)) and isinstance(f2v, (int, float)):
                        pairs[k] = (fv, f2v)
                truth[e] = diff_series(pairs, w, uuid.get(e) if uuid else None)
            approx = {}
            for e, d in ru_max.items():
                pairs = {}
                for k, v in d.items():
                    if f in v and f2 in v:
                        pairs[k] = (v[f], v[f2])
                approx[e] = diff_series(pairs, w)
            kind, rt = 'latency', compare(truth, approx)
        else:  # ratio：真值 = painless 逐观测，近似 = max/max
            reverse = key in RATIO_REVERSE
            fa, fb = (f2, f) if reverse else (f, f2)
            truth, tk = raw_ratio_truth(cfg, cfg['start'], cfg['end_full'], fa, fb)
            ratio_took.append(tk)
            approx = {}
            for e, d in ru_max.items():
                am = {k: v[fa] for k, v in d.items() if fa in v}
                bm = {k: v[fb] for k, v in d.items() if fb in v}
                approx[e] = {k: am[k] / bm[k] * 100.0 for k in am if k in bm and bm[k]}
            kind, rt = 'ratio~', compare(truth, approx)
        results.append((key, kind, rt))

    print(f'\n== {side} 逐指标对照（truth=raw 精确口径 / approx=rollup 表达）==')
    print(f'{"metric":28s} {"kind":8s} {"榜":4s} {"点差":6s} {"缺点":6s} {"最大相对差":11s} 结论')
    bad = []
    for key, kind, rt in results:
        clean = not rt['missing'] and not rt['extra']
        if not rt['rank_equal'] and clean and rt['maxrel'] <= 1e-6:
            verdict = 'TIE-FLIP'   # 数值全部一致，仅近似平手的排序被 float32 噪声翻转
        elif rt['rank_equal'] and clean and rt['maxrel'] <= 1e-6:
            verdict = 'EXACT'
        elif rt['rank_equal'] and rt['maxrel'] <= 0.02 and not rt['missing'] and not rt['extra']:
            verdict = 'NEAR'
        elif not rt['rank_equal'] and rt['maxrel'] <= 0.02:
            verdict = 'NEAR+RANK~'
        elif rt['maxrel'] <= 0.5 and not rt['missing'] and not rt['extra']:
            verdict = 'APPROX'
        else:
            verdict = 'DIVERGE'
        if verdict in ('DIVERGE',) or rt['missing'] + rt['extra'] > rt['nt'] // 100:
            bad.append((key, verdict))
        print(f'{key:28s} {kind:8s} {"✓" if rt["rank_equal"] else "✗":4s} '
              f'{rt["bad"]:6d} {rt["missing"] + rt["extra"]:6d} {rt["maxrel"]:11.2e} {verdict}')
    if skipped:
        print(f'rollup 作业未收录、只能走 raw 的指标: {skipped}')
    print(f'\n汇总: {len(results) - len(bad)}/{len(results)} 无异常；'
          f'异常指标: {bad if bad else "无"}')
    print(f'查询耗时: raw max {t_rm:.0f}ms / ts-top_hits {t_th:.0f}ms / rollup max {t_ru:.0f}ms; '
          f'latency 真值 top_hits 中位 {statistics.median(hit_took):.0f}ms; '
          f'ratio painless 中位 {statistics.median(ratio_took):.0f}ms' if ratio_took else
          f'查询耗时: raw max {t_rm:.0f}ms / ts-top_hits {t_th:.0f}ms / rollup max {t_ru:.0f}ms; '
          f'latency 真值 top_hits 中位 {statistics.median(hit_took):.0f}ms')


def compare(truth, approx):
    ents = sorted(truth)
    def top20(series_map):
        rows = []
        for e in ents:
            vals = [v for v in series_map.get(e, {}).values() if v is not None]
            if vals:
                rows.append((e, max(vals)))
        return [k for k, _ in sorted(rows, key=lambda kv: (-kv[1], kv[0]))][:20]
    rank_equal = top20(truth) == top20(approx)
    nt = bad = missing = extra = 0
    maxrel = 0.0
    for e in ents:
        tser, aser = truth.get(e, {}), approx.get(e, {})
        for k in set(tser) | set(aser):
            tv, av = tser.get(k), aser.get(k)
            if tv is None:
                extra += 1
                continue
            if av is None:
                missing += 1
                continue
            nt += 1
            rel = abs(tv - av) / max(abs(tv), 1e-12)
            maxrel = max(maxrel, rel)
            if rel > 1e-6:
                bad += 1
    return {'rank_equal': rank_equal, 'nt': nt, 'bad': bad, 'missing': missing,
            'extra': extra, 'maxrel': maxrel}


def cleanup(args):
    for side, cfg in SIDES.items():
        for name in (f'_rollup/jobs/{cfg["job"]}', f'/{cfg["src"]}', f'/{cfg["dst"]}*?expand_wildcards=all'):
            resp = http('DELETE', f'{args.store}/{name}' if name.startswith('_') else f'{args.store}{name}')
            print(f'[cleanup:{side}] {name}: ok')
        p = manifest_path(side)
        if os.path.exists(p):
            os.remove(p)


def main():
    ap = argparse.ArgumentParser(description='全量 TopN 指标的 rollup 可行性扫描')
    ap.add_argument('--seed', choices=['index', 'node'])
    ap.add_argument('--rollup', action='store_true', help='index+node 两个作业一起启动并等待完成')
    ap.add_argument('--sweep', choices=['index', 'node'])
    ap.add_argument('--cleanup', action='store_true')
    ap.add_argument('--days', type=float, default=30)
    ap.add_argument('--resume', action='store_true')
    ap.add_argument('--max-seconds', type=int, default=0)
    ap.add_argument('--bucket', default='8h')
    ap.add_argument('--top', type=int, default=20)
    ap.add_argument('--page-size', type=int, default=3000)
    ap.add_argument('--timeout', type=int, default=7200)
    ap.add_argument('--store', default=STORE)
    args = ap.parse_args()
    bucket_ms(args.bucket)
    if args.cleanup:
        cleanup(args)
    if args.seed:
        seed(args, args.seed)
    if args.rollup:
        rollup_jobs(args)
    if args.sweep:
        sweep(args, args.sweep)
    if not (args.seed or args.rollup or args.sweep or args.cleanup):
        ap.print_help()


if __name__ == '__main__':
    main()
