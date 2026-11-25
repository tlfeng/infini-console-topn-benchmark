#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
20_console_init.py —— Console 初始化（幂等，可重复执行）

业务集群不再由本仓库搭建或纳管。09_metrics_feeder.py 直接把 index_stats/node_stats
写入 Console 的系统存储 .infini_metrics；本脚本只负责：
  1) 首次初始化 Console（系统集群指向 STORE）
  2) 补齐系统索引 fielddata 映射
  3) 验证 Console 健康与集群列表接口

用法：
  python3 20_console_init.py
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
ADMIN_USER = os.environ.get("CONSOLE_USER", "admin")
ADMIN_PASS = os.environ.get("CONSOLE_PASS", "Admin@12345")
SECRET = os.environ.get("CREDENTIAL_SECRET", "topnrepro-secret-2026")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def call(method, url, body=None, token=None, timeout=60):
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode(errors="replace")
            return r.status, (json.loads(raw) if raw[:1] in "[{" else raw)
    except urllib.error.HTTPError as e:
        raw = e.read().decode(errors="replace")
        return e.code, (json.loads(raw) if raw[:1] in "[{" else raw[:300])


def wait_http(url, secs=120, name=""):
    for _ in range(secs):
        try:
            with urllib.request.urlopen(url, timeout=3) as r:
                r.read(1)
                return True
        except Exception:
            time.sleep(1)
    print(f"[warn] {name or url} 在 {secs}s 内未就绪")
    return False


def prepare_system_indices():
    script = os.path.join(BASE_DIR, "08_prepare_system_indices.sh")
    if not os.path.isfile(script):
        print("[init][WARN] 缺少 08_prepare_system_indices.sh，跳过系统索引映射补齐")
        return True
    result = subprocess.run(["bash", script], capture_output=True, text=True)
    if result.stdout:
        print(result.stdout.rstrip())
    if result.returncode != 0:
        if result.stderr:
            print(result.stderr.rstrip())
        return False
    return True


def setup_console():
    status, health = call("GET", CONSOLE + "/health")
    if status != 200:
        raise SystemExit(f"Console 不可达: {CONSOLE}")
    if not health.get("setup_required"):
        print("[init] Console 已初始化过，跳过 setup")
        return

    body = {
        "cluster": {"endpoint": STORE, "host": STORE.replace("http://", ""),
                    "schema": "http", "username": "", "password": "",
                    "hosts": [STORE.replace("http://", "")]},
        "skip": False,
        "bootstrap_username": ADMIN_USER,
        "bootstrap_password": ADMIN_PASS,
        "credential_secret": SECRET,
    }
    status, output = call("POST", CONSOLE + "/setup/_validate", body, timeout=120)
    print("[init] validate ->", status, output)
    if status != 200 or not output.get("success"):
        raise SystemExit("validate 失败，检查存储集群版本/健康状态")
    status, output = call("POST", CONSOLE + "/setup/_initialize", body, timeout=180)
    print("[init] initialize ->", status, output)
    if status != 200 or not output.get("success"):
        raise SystemExit("initialize 失败")


def login():
    for _ in range(30):
        status, output = call("POST", CONSOLE + "/account/login",
                              {"username": ADMIN_USER, "password": ADMIN_PASS})
        if status == 200 and isinstance(output, dict) and output.get("access_token"):
            return output["access_token"]
        time.sleep(1)
    raise SystemExit("登录失败，检查账号密码")


def main():
    if not wait_http(CONSOLE + "/health", 120, "console"):
        raise SystemExit("Console 未就绪")
    setup_console()
    if not prepare_system_indices():
        raise SystemExit("系统索引映射补齐失败：先手动执行 bash 08_prepare_system_indices.sh")
    token = login()
    status, output = call("GET", CONSOLE + "/elasticsearch/_search", token=token)
    if status != 200:
        raise SystemExit("集群列表接口 HTTP %s：%s" % (status, str(output)[:300]))
    total = output.get("hits", {}).get("total", {}).get("value", 0) if isinstance(output, dict) else 0
    print(f"[init] Console 已初始化，系统集群列表返回 {total} 条")
    print("[init] 完成。下一步：09_metrics_feeder.py fill/stream --side both")


if __name__ == "__main__":
    sys.exit(main())
