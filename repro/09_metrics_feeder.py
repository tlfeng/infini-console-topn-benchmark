#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
09_metrics_feeder.py —— TopN 监控指标数据维护（预填充 / 持续写入 / 清理）

为什么需要它：
  Console 的 TopN（advanced_topn raw 路径）读的是监控存储 .infini_metrics 里的
  index_stats 文档，rate/latency 依赖「同一实体相邻等宽桶内计数器单调增长」。
  真实环境数据断档、计数器不增长，导致长窗/短窗各种空图。本脚本直接灌合成指标，
  把数据侧变量完全掌控，便于评测 TopN 算法与前端展示。

  数据写在 .infini_metrics，用独立前缀 topn90d-<tag>- 隔离，挂到已纳管的
  topn-biz5 集群，Console 页面可直接查；不碰真实采集数据、不与 34 脚本的
  synth30d- 前缀冲突。

四个子命令：
  fill    一次性预填充历史数据（默认最近 90 天，以当前时间为终点往回）
  stream  前台持续写入，Ctrl-C 停止；计数器自动续接；--auto-prune 可开关自动清理
  prune   删除保留窗口之外的合成指标（默认 2160h=90天，支持 --dry-run）
  status  查看当前数据量、时间范围、各实体计数器

自动清理（stream --auto-prune / prune 子命令）：
  - 保留窗口按小时指定：--hours N（默认 2160=90天；旧参数 --days 仍兼容，等价 ×24）
  - 删除边界精确到小时：cutoff = (当前时间 - N 小时) 向下对齐到整点，删除早于该整点的数据。
    边界稳定在整点、不随执行时刻毫秒漂移。
  - stream 里清理按周期触发：--prune-every-hours（默认 1 小时），与写入轮数无关；
    启动时先清一次。不指定 --auto-prune 则完全不清理（默认关）。

计数器模型（保证所有 TopN 指标都能出数）：
  每个采样步 step 从 1 递增；各计数器 = 每步增量 × step（单调递增）。
  时延计数器 = 操作计数器 × 固定毫秒常数，故 latency=(Δtime)/(Δops)=常数，
  相邻桶总能配对出正 rate / 正 latency。

用法：
  # 预填充最近 90 天（15min 间隔，6 个画像实体）
  python3 09_metrics_feeder.py fill
  python3 09_metrics_feeder.py fill --days 30 --interval 300 --tag trial

  # 持续写入（计数器从 fill 的末尾自动续接）+ 自动清理 90 天外数据（每小时检查）
  python3 09_metrics_feeder.py stream --auto-prune
  python3 09_metrics_feeder.py stream --auto-prune --hours 48 --prune-every-hours 2  # 保留48h/每2h查
  python3 09_metrics_feeder.py stream --interval 60          # 1min 粒度，测短窗口；不清理

  # 手动清理保留窗口之外（先看会删多少，再真删）；边界对齐整点
  python3 09_metrics_feeder.py prune --dry-run
  python3 09_metrics_feeder.py prune --hours 48             # 只保留最近 48 小时
  python3 09_metrics_feeder.py prune --days 90              # 兼容旧写法

  # 状态
  python3 09_metrics_feeder.py status

建议桶宽（受 raw 路径预算 searchBucketLimit=6000 约束）：
  - 90 天窗口：用 8h / 1d 桶（900s/1h 会 budget_exceeded）
  - 30 天窗口：用 3h / 8h / 1d 桶
  - 短窗口（≤6h）：可用 60s / 900s 桶
  采样间隔要 ≤ 桶宽，否则桶内无相邻观测配不出 rate/latency。
"""
import argparse
import datetime as dt
import json
import os
import signal
import sys
import time
import urllib.error
import urllib.request

# ----------------------------------------------------------------- 配置
STORE = os.environ.get('STORE', 'http://127.0.0.1:9200')
RAW_INDEX = '.infini_metrics'
CLUSTER = os.environ.get('CLUSTER', 'topn-biz5')
BASE_PREFIX = 'topn90d'
METRIC = 'index_stats'
ENTITY_FIELD = 'metadata.labels.index_name'
STEP_FIELD = 'payload.elasticsearch.index_stats._synth_step'   # 步骤号，供 stream 续接

# 画像实体：(基础名, 每步 index_total 增量)。各时延常数见下，全部相差明显便于看 TopN 排序。
ENTITIES = [
    ('hot',   12000),
    ('mid',    6000),
    ('low',    2400),
    ('slow',    600),
    ('tiny',    120),
    ('scroll',  3000),
]

# 各操作计数器相对 index_total 的比例；时延(ms)常数
RATIO = {
    'index':   1.00,   # primaries.indexing.index_total
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


def entity_names(tag):
    return [prefix_for(tag) + name for name, _ in ENTITIES]


# ----------------------------------------------------------------- 造文档
def make_doc(name, ts_ms, step, per_step):
    """构造一条 index_stats 指标文档，覆盖 TopN 常用的全部计数器字段。"""
    def ops(key):
        return int(per_step * RATIO[key]) * step

    idx_total = ops('index')
    idx_time = int(idx_total * LATENCY_MS['index'])
    q_total = ops('query')
    q_time = int(q_total * LATENCY_MS['query'])
    f_total = ops('fetch')
    f_time = int(f_total * LATENCY_MS['fetch'])
    sc_total = ops('scroll')
    sc_time = int(sc_total * LATENCY_MS['scroll'])
    mg_total = ops('merge')
    mg_time = int(mg_total * LATENCY_MS['merge'])
    rf_total = ops('refresh')
    rf_time = int(rf_total * LATENCY_MS['refresh'])
    fl_total = ops('flush')
    fl_time = int(fl_total * LATENCY_MS['flush'])
    docs = 1000 + idx_total
    deleted = ops('delete')
    store = 10 * 1024 * 1024 + idx_total * 512

    search = {
        'query_total': q_total, 'query_time_in_millis': q_time,
        'fetch_total': f_total, 'fetch_time_in_millis': f_time,
        'scroll_total': sc_total, 'scroll_time_in_millis': sc_time,
        'suggest_total': 0, 'suggest_time_in_millis': 0,
    }
    indexing = {
        'index_total': idx_total, 'index_time_in_millis': idx_time,
        'delete_total': deleted, 'delete_time_in_millis': deleted * 3,
        'index_failed': 0,
    }
    is_stats = {
        '_synth_step': step,
        'index_info': {'index': name, 'status': 'open', 'health': 'green',
                       'shards': 1, 'replicas': 0},
        'total': {
            'docs': {'count': docs, 'deleted': deleted},
            'store': {'size_in_bytes': store},
            'segments': {'count': 1 + step // 100},
            'search': dict(search),
            'indexing': dict(indexing),
            'merges': {'total': mg_total, 'total_time_in_millis': mg_time,
                       'current': 0, 'total_docs': 0},
            'refresh': {'total': rf_total, 'total_time_in_millis': rf_time},
            'flush': {'total': fl_total, 'total_time_in_millis': fl_time},
        },
        'primaries': {
            'docs': {'count': docs, 'deleted': deleted},
            'store': {'size_in_bytes': store},
            'search': dict(search),
            'indexing': dict(indexing),
        },
    }
    return {
        'timestamp': ts_ms,
        'metadata': {
            'name': METRIC, 'category': 'elasticsearch', 'datatype': 'snapshot',
            'labels': {
                'cluster_id': CLUSTER,
                'cluster_uuid': '',
                'index_id': f'{CLUSTER}:{name}',
                'index_name': name,
                'index_uuid': 'topn90d-uuid-' + name,
            },
        },
        'payload': {'elasticsearch': {'index_stats': is_stats}},
    }


def bulk_write(docs, id_prefix='topn90d'):
    """docs: [(index_name, ts_ms, doc), ...]，按 _id 幂等写入。"""
    lines = []
    for name, ts_ms, doc in docs:
        lines.append(json.dumps({'index': {'_index': RAW_INDEX,
                                           '_id': f'{id_prefix}|{name}|{ts_ms}'}}))
        lines.append(json.dumps(doc, ensure_ascii=False))
    batch = 2000  # 1000 文档/批
    for s in range(0, len(lines), batch):
        chunk = '\n'.join(lines[s:s + batch]) + '\n'
        st, resp = http('POST', f'{STORE}/{RAW_INDEX}/_bulk?refresh=false', chunk,
                        headers={'Content-Type': 'application/x-ndjson'}, timeout=180, raw=True)
        if st != 200 or (isinstance(resp, str) and '"errors":true' in resp.replace(' ', '')):
            raise SystemExit(f'bulk 失败: {st} {str(resp)[:400]}')
    if docs:
        http('POST', f'{STORE}/{RAW_INDEX}/_refresh')
    return len(docs)


# ----------------------------------------------------------------- fill
def cmd_fill(args):
    interval = args.interval
    end = align_ms(int(time.time() * 1000), interval)
    start = align_ms(end - int(args.days * 86400 * 1000), interval)
    n = int((end - start) / (interval * 1000)) + 1
    prefix = prefix_for(args.tag)
    print(f'[fill] cluster={CLUSTER} 前缀={prefix} 天数={args.days} 间隔={interval}s')
    print(f'[fill] 窗口 {fmt(start)} -> {fmt(end)}  步数={n}  实体={len(ENTITIES)}')
    print(f'[fill] 预计文档 {n * len(ENTITIES):,} 条，开始写入...')
    t0 = time.time()
    written = 0
    # 分批按步推进，避免一次性构造几十万条占内存
    step_batch = 200
    for base in range(0, n, step_batch):
        docs = []
        for i in range(base, min(base + step_batch, n)):
            ts = start + i * interval * 1000
            step = i + 1
            for name, per_step in ENTITIES:
                docs.append((prefix + name, ts, make_doc(prefix + name, ts, step, per_step)))
        written += bulk_write(docs)
        print(f'  {written:,}/{n * len(ENTITIES):,} ({time.time()-t0:.0f}s)', flush=True)
    print(f'[fill] 完成 {written:,} 条，用时 {time.time()-t0:.1f}s')
    print('[fill] 提示：长窗口请用大桶宽（8h/1d），否则 raw 路径会 budget_exceeded')
    return 0


# ----------------------------------------------------------------- stream 续接状态
def read_state(tag):
    """从 ES 读取每个实体当前最大 step 与最大 timestamp，供 stream 续接。"""
    prefix = prefix_for(tag)
    body = {
        'size': 0,
        'query': {'bool': {'filter': [{'prefix': {ENTITY_FIELD: prefix}}]}},
        'aggs': {'g': {'terms': {'field': ENTITY_FIELD, 'size': 100},
                       'aggs': {'step': {'max': {'field': STEP_FIELD}},
                                'ts': {'max': {'field': 'timestamp'}}}}},
    }
    st, resp = http('POST', f'{STORE}/{RAW_INDEX}/_search', body)
    state = {}
    if st == 200 and isinstance(resp, dict):
        for b in (resp.get('aggregations', {}).get('g', {}).get('buckets') or []):
            state[b['key']] = {'step': int(b['step']['value'] or 0),
                               'ts': int(b['ts']['value'] or 0)}
    return state


def cmd_stream(args):
    interval = args.interval
    prefix = prefix_for(args.tag)
    state = read_state(args.tag)
    # 每个实体的起始 step = 已有最大值 + 1；若某实体没有则从 1 开始
    steps = {}
    per_step_map = {prefix + name: ps for name, ps in ENTITIES}
    for name in per_step_map:
        steps[name] = state.get(name, {}).get('step', 0) + 1
    last_ts = max((v['ts'] for v in state.values()), default=0)
    hours = retention_hours(args)
    # 自动清理：按小时周期检查，边界对齐整点。启动时先清一次，之后每 prune_every_hours 小时一次。
    prune_ms = args.prune_every_hours * 3600 * 1000
    last_prune = 0  # 0 = 首轮立即检查
    print(f'[stream] cluster={CLUSTER} 前缀={prefix} 间隔={interval}s')
    if args.auto_prune:
        print(f'[stream] 自动清理=开：保留 {hours}h，每 {args.prune_every_hours}h 检查一次'
              f'（边界对齐整点，删除严格早于整点的数据）')
    else:
        print('[stream] 自动清理=关（加 --auto-prune 开启）')
    print(f'[stream] 从已有数据续接：起始 step={ {n.split(prefix)[-1]:s for n,s in steps.items()} }，'
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
            # 清理按小时周期触发，与是否写入无关
            if args.auto_prune and now_ms - last_prune >= prune_ms:
                do_prune(hours, prefix, dry_run=False)
                last_prune = now_ms
            ts = align_ms(now_ms, interval)
            if ts > last_ts:
                docs = []
                for name, step in steps.items():
                    docs.append((name, ts, make_doc(name, ts, step, per_step_map[name])))
                n = bulk_write(docs)
                last_ts = ts
                for name in steps:
                    steps[name] += 1
                tick += 1
                print(f'  [{dt.datetime.now():%H:%M:%S}] 写入 {n} 条 @ {fmt(ts)}  '
                      f'(step={min(steps.values())-1})', flush=True)
            # 睡到下一个间隔边界，响应 Ctrl-C
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
    """保留窗口的删除边界，精确到小时（对齐到整点）。

    cutoff = 当前时间往前 `hours` 小时，再向下对齐到小时边界。
    这样边界稳定在整点、可预测，不随每次执行的毫秒漂移；删除严格早于该整点的数据。
    """
    return align_ms(int(time.time() * 1000) - int(hours * 3600 * 1000), 3600)


def do_prune(hours, prefix, dry_run=False):
    cutoff = retention_cutoff(hours)
    body = {
        'query': {'bool': {'filter': [
            {'prefix': {ENTITY_FIELD: prefix}},
            {'range': {'timestamp': {'lt': cutoff, 'format': 'epoch_millis'}}},
        ]}}
    }
    scope = f'前缀={prefix} 保留{hours}h 早于 {fmt(cutoff)}'
    if dry_run:
        st, resp = http('POST', f'{STORE}/{RAW_INDEX}/_count', {'query': body['query']})
        cnt = resp.get('count') if isinstance(resp, dict) else resp
        print(f'[prune] DRY-RUN {scope} 的文档 {cnt} 条（未删除）')
        return cnt
    st, resp = http('POST', f'{STORE}/{RAW_INDEX}/_delete_by_query?refresh=true&conflicts=proceed',
                    body, timeout=180)
    deleted = resp.get('deleted') if isinstance(resp, dict) else resp
    print(f'[prune] {scope} 已删除 {deleted} 条')
    return deleted


def cmd_prune(args):
    hours = retention_hours(args)
    do_prune(hours, prefix_for(args.tag), dry_run=args.dry_run)
    return 0


# ----------------------------------------------------------------- status
def cmd_status(args):
    prefix = prefix_for(args.tag)
    body = {
        'size': 0,
        'query': {'bool': {'filter': [{'prefix': {ENTITY_FIELD: prefix}}]}},
        'aggs': {
            'mn': {'min': {'field': 'timestamp'}},
            'mx': {'max': {'field': 'timestamp'}},
            'g': {'terms': {'field': ENTITY_FIELD, 'size': 100},
                  'aggs': {'step': {'max': {'field': STEP_FIELD}}}},
        },
    }
    st, resp = http('POST', f'{STORE}/{RAW_INDEX}/_search', body)
    if st != 200 or not isinstance(resp, dict):
        print(f'[status] 查询失败: {st} {resp}')
        return 1
    total = resp['hits']['total']['value']
    agg = resp['aggregations']
    print(f'[status] 前缀={prefix} cluster={CLUSTER}')
    print(f'[status] 文档总数={total:,}  时间范围={fmt(agg["mn"]["value"])} -> {fmt(agg["mx"]["value"])}')
    print('[status] 各实体最新 step:')
    for b in agg['g']['buckets']:
        print(f'  {b["key"]:<28} step={int(b["step"]["value"] or 0):,}  docs={b["doc_count"]:,}')
    return 0


def fmt(ms):
    if not ms:
        return '—'
    return dt.datetime.fromtimestamp(ms / 1000).strftime('%Y-%m-%d %H:%M')


def main():
    ap = argparse.ArgumentParser(description='TopN 监控指标数据维护（预填充/持续写入/清理）')
    sub = ap.add_subparsers(dest='cmd', required=True)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument('--tag', default='main', help='数据隔离标签，前缀 topn90d-<tag>-（默认 main）')
    common.add_argument('--interval', type=int, default=900, help='采样间隔秒（默认 900=15min）')

    p_fill = sub.add_parser('fill', parents=[common], help='一次性预填充历史数据')
    p_fill.add_argument('--days', type=float, default=90, help='往回填充天数（默认 90）')

    p_stream = sub.add_parser('stream', parents=[common], help='前台持续写入，Ctrl-C 停止')
    p_stream.add_argument('--auto-prune', action='store_true',
                          help='开启自动清理（默认关）；删除保留窗口之外的数据')
    p_stream.add_argument('--hours', type=float, default=None,
                          help='保留窗口小时数（默认 2160=90天；边界对齐整点）')
    p_stream.add_argument('--days', type=float, default=None,
                          help='保留窗口天数，等价 hours=days×24（与 --hours 二选一）')
    p_stream.add_argument('--prune-every-hours', type=float, default=1,
                          help='每隔多少小时检查一次清理（默认 1）')

    p_prune = sub.add_parser('prune', parents=[common], help='删除保留窗口之外的合成指标')
    p_prune.add_argument('--hours', type=float, default=None,
                         help='保留窗口小时数（默认 2160=90天；边界对齐整点）')
    p_prune.add_argument('--days', type=float, default=None,
                         help='保留窗口天数，等价 hours=days×24（兼容旧参数）')
    p_prune.add_argument('--dry-run', action='store_true', help='只统计不删除')

    sub.add_parser('status', parents=[common], help='查看数据量/时间范围/各实体 step')

    args = ap.parse_args()
    return {'fill': cmd_fill, 'stream': cmd_stream,
            'prune': cmd_prune, 'status': cmd_status}[args.cmd](args)


if __name__ == '__main__':
    sys.exit(main())
