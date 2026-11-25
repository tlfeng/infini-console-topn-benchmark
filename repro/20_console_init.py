#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
20_console_init.py —— Console 初始化 + 纳管业务集群（幂等，可重复执行）

做三件事：
  1) 首次初始化：POST /setup/_validate → /setup/_initialize（创建 admin、指定系统集群 9200）
  2) 登录拿 access_token
  3) 把业务集群（默认 9201）注册进 Console

关于第 3 步：Console 1.30.3 的"注册集群"前端接口没有公开文档，探测到的
/elasticsearch/_register 会落到 ES 代理返回 405。因此这里直接往系统集群的
.infini_cluster 索引写一条集群配置文档（Console 启动时 "loading [N] remote configs"
就是从该索引读的），写完重启 Console 使其生效。字段完全对齐系统集群文档。

用法：
  python3 20_console_init.py
  CLUSTER_ID=topn-biz BIZ_ENDPOINT=http://127.0.0.1:9201 python3 20_console_init.py
"""
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

CONSOLE = os.environ.get("CONSOLE", "http://127.0.0.1:9000").rstrip("/")
STORE = os.environ.get("STORE", "http://127.0.0.1:9200").rstrip("/")
BIZ = os.environ.get("BIZ_ENDPOINT", "http://127.0.0.1:9201").rstrip("/")
CLUSTER_ID = os.environ.get("CLUSTER_ID", "topn-biz")
CLUSTER_NAME = os.environ.get("CLUSTER_NAME", "topn-biz")
ADMIN_USER = os.environ.get("CONSOLE_USER", "admin")
ADMIN_PASS = os.environ.get("CONSOLE_PASS", "Admin@12345")
SECRET = os.environ.get("CREDENTIAL_SECRET", "topnrepro-secret-2026")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def prepare_system_indices():
    script = os.path.join(BASE_DIR, "08_prepare_system_indices.sh")
    if not os.path.isfile(script):
        print("[init][WARN] 缺少 08_prepare_system_indices.sh，跳过系统索引映射补齐")
        return True
    r = subprocess.run(["bash", script], capture_output=True, text=True)
    if r.stdout:
        print(r.stdout.rstrip())
    if r.returncode != 0:
        if r.stderr:
            print(r.stderr.rstrip())
        return False
    return True


def call(method, url, body=None, token=None, timeout=60):
    data = json.dumps(body).encode() if body is not None else None
    h = {"Content-Type": "application/json"}
    if token:
        h["Authorization"] = "Bearer " + token
    req = urllib.request.Request(url, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode(errors="replace")
            return r.status, (json.loads(raw) if raw[:1] in "[{" else raw)
    except urllib.error.HTTPError as e:
        raw = e.read().decode(errors="replace")
        return e.code, (json.loads(raw) if raw[:1] in "[{" else raw[:300])


def wait_http(url, secs=120, name=""):
    for i in range(secs):
        try:
            with urllib.request.urlopen(url, timeout=3) as r:
                r.read(1)
                return True
        except Exception:
            time.sleep(1)
    print(f"[warn] {name or url} 在 {secs}s 内未就绪")
    return False


def setup_console():
    st, health = call("GET", CONSOLE + "/health")
    if st != 200:
        raise SystemExit(f"Console 不可达: {CONSOLE}")
    if not health.get("setup_required"):
        print("[init] Console 已初始化过，跳过 setup")
        return False

    body = {
        "cluster": {"endpoint": STORE, "host": STORE.replace("http://", ""),
                    "schema": "http", "username": "", "password": "",
                    "hosts": [STORE.replace("http://", "")]},
        "skip": False,
        "bootstrap_username": ADMIN_USER,
        "bootstrap_password": ADMIN_PASS,
        "credential_secret": SECRET,
    }
    st, out = call("POST", CONSOLE + "/setup/_validate", body, timeout=120)
    print("[init] validate ->", st, out)
    if st != 200 or not out.get("success"):
        raise SystemExit("validate 失败，检查存储集群版本/健康状态")
    st, out = call("POST", CONSOLE + "/setup/_initialize", body, timeout=180)
    print("[init] initialize ->", st, out)
    if st != 200 or not out.get("success"):
        raise SystemExit("initialize 失败")
    return True


def login():
    for i in range(30):
        st, out = call("POST", CONSOLE + "/account/login",
                       {"username": ADMIN_USER, "password": ADMIN_PASS})
        if st == 200 and isinstance(out, dict) and out.get("access_token"):
            return out["access_token"]
        time.sleep(1)
    raise SystemExit("登录失败，检查账号密码")


def register_cluster():
    st, doc = call("GET", f"{STORE}/.infini_cluster/_doc/{CLUSTER_ID}")
    if st == 200 and doc.get("_source"):
        print(f"[init] 集群 {CLUSTER_ID} 已在 Console 中注册")
        return False

    st, sysres = call("GET", f"{STORE}/.infini_cluster/_search")
    hits = sysres.get("hits", {}).get("hits", [])
    if not hits:
        raise SystemExit("找不到系统集群配置文档，setup 可能未完成")
    cfg = next((h["_source"] for h in hits if h["_source"].get("reserved")), hits[0]["_source"])
    cfg = json.loads(json.dumps(cfg))
    hh = BIZ.replace("http://", "").replace("https://", "")
    cfg.update({
        "id": CLUSTER_ID, "name": CLUSTER_NAME, "raw_name": CLUSTER_NAME,
        "endpoint": BIZ, "host": hh, "schema": "http",
        "reserved": False, "monitored": True, "enabled": True,
        "source": "elastic", "distribution": "easysearch", "version": "3.0.0",
        "basic_auth": {}, "credential_id": "", "cluster_uuid": "",
        "tags": ["topn-repro"],
    })
    for k in ("created", "updated", "labels", "cluster_uuid"):
        cfg.pop(k, None)
    st, out = call("PUT", f"{STORE}/.infini_cluster/_doc/{CLUSTER_ID}?refresh=true", cfg)
    print(f"[init] 写入集群配置 {CLUSTER_ID} ->", st, str(out)[:120])
    return True


def restart_console():
    r = subprocess.run(["systemctl", "restart", "topn-console.service"],
                       capture_output=True, text=True)
    if r.returncode != 0:
        print("[init] 无法通过 systemd 重启 Console（通常是没有 root 权限）。")
        print("[init] 请手动执行后再继续: sudo systemctl restart topn-console")
        print("[init] 未重启则新注册的集群不会被采集，指标永久不入库。")
        print("[init] 详情:", r.stderr.strip()[:200])
        return False
    print("[init] Console 重启中…")
    wait_http(CONSOLE + "/health", 120, "console")
    return True


def main():
    wait_http(CONSOLE + "/health", 120, "console")
    setup_console()
    if not prepare_system_indices():
        raise SystemExit("系统索引映射补齐失败：先手动执行 bash 08_prepare_system_indices.sh")
    tok = login()
    print("[init] 登录成功")
    changed = register_cluster()
    if changed:
        if not restart_console():
            raise SystemExit("Console 重启失败，新注册的集群不会被采集")
        tok = login()
    # 等待 Console 发现该集群，并确认列表接口没有因 text 映射报 500
    total = -1
    for i in range(60):
        st, out = call("GET", CONSOLE + f"/elasticsearch/_search", token=tok)
        if st != 200:
            raise SystemExit("集群列表接口 HTTP %s：%s\n先执行 bash 08_prepare_system_indices.sh"
                             % (st, str(out)[:300]))
        total = out.get("hits", {}).get("total", {}).get("value", 0) if isinstance(out, dict) else 0
        if total >= 2:
            break
        time.sleep(2)
    if total < 2:
        raise SystemExit("等待超时，集群列表仍只有 %s 条，检查 Console 是否发现新集群" % total)
    print(f"[init] Console 已纳管 {total} 个集群")
    print("[init] 完成。下一步: 造数 10_seed.py / 压测 11_load.py")


if __name__ == "__main__":
    sys.exit(main())
