#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
common.py —— 评测脚本公共 helper（HTTP / 登录 / 真值计算 / 采样读取 / 造数工具）

背景：
  原 00–41 系列脚本各自复制了一份 http()/login()/dig()/truth()/load_samples()，
  三套验证脚本（30/31/32）的真值算法几乎逐行相同，两个造数脚本（09/34）的
  prefix/align/bulk/doc 也重复。本模块把这些收为一处，消除漂移。

使用方式（兼容优先）：
  各脚本仍可保留自己的同名函数，改为薄别名即可，例如：
      from common import http, login, dig, load_samples, truth, truth_nodes
  或直接调用本模块函数。benchmark_topn.py 刻意独立（http.client + 集群名护栏），
  不要接入本模块。

真值算法说明（truth / truth_nodes）：
  输入是 12_sample.py / 14_sample_nodes.py 产出的 JSONL 行（按 ts 排序）。
  - instant：取窗口内最大值
  - rate：相邻采样差分 / Δt，取峰值；计数器回退(v2<v1)跳过
  - latency：相邻采样 (Δnumerator)/(Δdenominator)，取峰值；分母不增跳过
  entries 形状：索引 = [(key, mode, scope, field), ...]；
                节点 = [(key, mode, field), ...]。
  返回 (榜单 dict, 全量分数 dict)。
"""
import json
import os
import urllib.error
import urllib.request

# ----------------------------------------------------------------- 环境常量
STORE = os.environ.get('STORE', 'http://127.0.0.1:9200')
CONSOLE = os.environ.get('CONSOLE', 'http://127.0.0.1:9000')
CONSOLE_USER = os.environ.get('CONSOLE_USER', 'admin')
CONSOLE_PASS = os.environ.get('CONSOLE_PASS', 'Admin@12345')
SYS_CLUSTER = os.environ.get('SYS_CLUSTER', 'infini_default_system_cluster')
CLUSTER_ID = os.environ.get('CLUSTER_ID', 'topn-biz')


# ----------------------------------------------------------------- 基础工具
def maybe_json(s):
    """把字符串尽量解析成 JSON，失败原样返回。"""
    try:
        return json.loads(s)
    except Exception:
        return s


def http(method, url, body=None, headers=None, timeout=120, raw=False):
    """统一 HTTP：body 支持 dict/list(自动 json) 或 str(原样，用于 ndjson)。"""
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
            return r.status, (out if raw else maybe_json(out))
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors='replace')[:800]
    except Exception as e:
        return -1, str(e)


def login(console=None, user=None, password=None):
    """登录 Console，返回 access_token；失败直接 SystemExit（与各脚本原行为一致）。"""
    console = console or CONSOLE
    st, out = http('POST', console + '/account/login',
                   {'username': user or CONSOLE_USER, 'password': password or CONSOLE_PASS})
    if st != 200 or not isinstance(out, dict) or 'access_token' not in out:
        raise SystemExit(f'登录失败: {st} {out}')
    return out['access_token']


def dig(d, path):
    """按点分路径安全取值，任一层缺失返回 None。"""
    cur = d
    for p in path.split('.'):
        if not isinstance(cur, dict) or p not in cur:
            return None
        cur = cur[p]
    return cur


def align_ms(ts_ms, interval_s):
    """把毫秒时间戳对齐到 interval 边界。"""
    step = interval_s * 1000
    return ts_ms // step * step


def prefix_for(tag, base='topn90d'):
    """合成数据的索引名前缀：base-tag-。"""
    return f'{base}-{tag}-'


# ----------------------------------------------------------------- 采样读取
def load_samples(path):
    """读 JSONL 真值采样并按 ts 排序。"""
    with open(path, encoding='utf-8') as f:
        rows = [json.loads(l) for l in f if l.strip()]
    rows.sort(key=lambda r: r['ts'])
    return rows


# ----------------------------------------------------------------- 真值计算
def _score_rank(score, top):
    ranked = [k for k, _ in sorted(score.items(), key=lambda kv: kv[1], reverse=True)[:top]]
    return ranked, {k: float(v) for k, v in score.items()}


def truth(rows, entries, top):
    """索引真值。entries=[(key, mode, scope, field), ...]；返回 (榜单, 全量分数)。"""
    out, scores_all = {}, {}
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
        out[key], scores_all[key] = _score_rank(score, top)
    return out, scores_all


def truth_nodes(rows, entries, top):
    """节点真值。entries=[(key, mode, field), ...]；返回 (榜单, 全量分数)。

    兼容性：原 32.truth_nodes 用 a['nodes'][name] 直接索引，采样点缺节点时 KeyError；
    这里统一用 .get(name, {}) 兜底（与 30/31 一致，行为更稳）。
    """
    out, scores_all = {}, {}
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
                    v1 = dig(a['nodes'].get(name, {}), field)
                    v2 = dig(b['nodes'].get(name, {}), field)
                    if v1 is None or v2 is None or v2 < v1:
                        continue
                    rate = (v2 - v1) / dt
                    if rate > score.get(name, float('-inf')):
                        score[name] = rate
        else:
            f_num, f_den = field if isinstance(field, tuple) else field.split('|')
            for a, b in zip(rows, rows[1:]):
                for name in b['nodes']:
                    n1 = dig(a['nodes'].get(name, {}), f_num)
                    n2 = dig(b['nodes'].get(name, {}), f_num)
                    d1 = dig(a['nodes'].get(name, {}), f_den)
                    d2 = dig(b['nodes'].get(name, {}), f_den)
                    if None in (n1, n2, d1, d2):
                        continue
                    dc = d2 - d1
                    if dc <= 0 or n2 < n1:
                        continue
                    lat = (n2 - n1) / dc
                    if lat > score.get(name, float('-inf')):
                        score[name] = lat
        out[key], scores_all[key] = _score_rank(score, top)
    return out, scores_all


# ----------------------------------------------------------------- 造数工具
def bulk_write(store, index, docs, id_fn, batch=2000):
    """docs=[(name, ts_ms, doc), ...]；按 id_fn(name, ts) 生成 _id，ndjson 批量写。"""
    lines = []
    for name, ts_ms, doc in docs:
        lines.append(json.dumps({'index': {'_index': index, '_id': id_fn(name, ts_ms)}}))
        lines.append(json.dumps(doc, ensure_ascii=False))
    for s in range(0, len(lines), batch):
        chunk = '\n'.join(lines[s:s + batch]) + '\n'
        st, resp = http('POST', f'{store}/{index}/_bulk?refresh=false', chunk,
                        headers={'Content-Type': 'application/x-ndjson'}, timeout=180, raw=True)
        if st != 200 or (isinstance(resp, str) and '"errors":true' in resp.replace(' ', '')):
            raise SystemExit(f'bulk 失败: {st} {str(resp)[:400]}')
    if docs:
        http('POST', f'{store}/{index}/_refresh')
    return len(docs)
