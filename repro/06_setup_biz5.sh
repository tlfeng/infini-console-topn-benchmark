#!/bin/bash
# =============================================================================
# 06_setup_biz5.sh —— 搭一个 N 节点业务集群（默认 8），用于「节点页签 TopN」评测
#
#   为什么需要多节点：节点 TopN 是"从所有节点里挑 top 个画图"，
#   节点数 <= top 时无论怎么挑都是全集，评测没有区分度。生产一般 6~20 个节点。
#
#   共用一份 easysearch 安装目录（$T/es-biz5），每个节点一个独立 config 目录，
#   通过 ES_PATH_CONF 区分；systemd 单元 topn-biz5-node<N>.service。
#
# 端口：http 9211..921N  transport 9311..931N
# 用法：bash 06_setup_biz5.sh [节点数，默认 8]
# =============================================================================
set -euo pipefail
T=${TOPTOOLS:-}
if [ -z "$T" ]; then
  for d in "$HOME/test/topn" /home/user/test/topn; do [ -d "$d" ] && { T=$d; break; }; done
fi
T=${T:-$HOME/test/topn}
N=${1:-8}
JAVA=$(cat "$T/java_home" 2>/dev/null || echo "")
[ -x "${JAVA:-}/bin/java" ] || JAVA=$(ls -d "$HOME"/jdks/jdk-21* /home/user/jdks/jdk-21* /usr/lib/jvm/java-21* 2>/dev/null | head -1)
RUN_USER=${RUN_USER:-${SUDO_USER:-user}}

log() { printf '\033[36m[biz5]\033[0m %s\n' "$*"; }
die() { printf '\033[31m[biz5][ERROR]\033[0m %s\n' "$*" >&2; exit 1; }

# cluster.initial_master_nodes 固定给 3 个候选，节点数少于 3 时集群无法成形
[ "$N" -ge 3 ] 2>/dev/null || die "节点数至少 3（当前: $N）"

[ -x "$T/es-biz/bin/easysearch" ] || die "找不到 $T/es-biz，请先跑 01_setup.sh"
[ -x "$T/es-biz5/bin/easysearch" ] || cp -r "$T/es-biz" "$T/es-biz5"

# ---- 节点配置 ----
seeds=""
for i in $(seq 1 "$N"); do
  seeds="$seeds\"127.0.0.1:$((9310 + i))\","
done
seeds="${seeds%,}"

for i in $(seq 1 "$N"); do
  d="$T/conf/biz5/node-$i"
  mkdir -p "$d" "$T/data/biz5/node-$i" "$T/logs/biz5/node-$i"
  cp -f "$T/es-biz/config/jvm.options" "$T/es-biz/config/log4j2.properties" "$d/" 2>/dev/null
  cp -rf "$T/es-biz/config/jvm.options.d" "$d/" 2>/dev/null
  sed -i 's/^-Xms.*/-Xms512m/; s/^-Xmx.*/-Xmx512m/' "$d/jvm.options"
  cat > "$d/easysearch.yml" <<EOF
# TopN 评测：节点页签专用集群（node-$i）
cluster.name: topn-biz5
node.name: node-$i
node.roles: [ master, data, ingest, search, remote_cluster_client ]

path.data: $T/data/biz5/node-$i
path.logs: $T/logs/biz5/node-$i
network.host: 127.0.0.1
http.port: $((9210 + i))
transport.port: $((9310 + i))

discovery.seed_hosts: [$seeds]
cluster.initial_master_nodes: ["node-1", "node-2", "node-3"]

security.enabled: false
security.audit.type: noop
bootstrap.memory_lock: false
bootstrap.system_call_filter: false
cluster.routing.allocation.disk.threshold_enabled: false
action.destructive_requires_name: false
EOF
  chmod 600 "$d/easysearch.yml"
done
log "已生成 $N 个节点的配置（端口 9211-$((9210+N))）"

# ---- systemd 单元 ----
if [ "$(id -u)" = "0" ]; then
  chown -R "$RUN_USER":"$RUN_USER" "$T/conf/biz5" "$T/data/biz5" "$T/logs/biz5" 2>/dev/null || true
  for i in $(seq 1 "$N"); do
    cat > "/etc/systemd/system/topn-biz5-node$i.service" <<EOF
[Unit]
Description=TopN repro - easysearch multi-node cluster (node-$i)
After=network-online.target

[Service]
Type=simple
User=$RUN_USER
WorkingDirectory=$T/es-biz5
Environment=JAVA_HOME=$JAVA
Environment=ES_JAVA_HOME=$JAVA
Environment=ES_PATH_CONF=$T/conf/biz5/node-$i
ExecStart=$T/es-biz5/bin/easysearch
Restart=no
LimitNOFILE=65535
LimitMEMLOCK=infinity
TimeoutStopSec=90
SuccessExitStatus=143

[Install]
WantedBy=multi-user.target
EOF
  done
  systemctl daemon-reload
  log "systemd 单元已安装：topn-biz5-node1..$N"
else
  log "非 root：跳过 systemd 安装（用 wsl -u root 重跑以启用 systemd）"
fi

# ---- 启动 ----
log "启动节点"
if [ "$(id -u)" = "0" ]; then
  for i in $(seq 1 "$N"); do systemctl start "topn-biz5-node$i.service"; done
else
  for i in $(seq 1 "$N"); do
    ( cd "$T/es-biz5" && ES_PATH_CONF="$T/conf/biz5/node-$i" \
        JAVA_HOME="$JAVA" ES_JAVA_HOME="$JAVA" \
        setsid ./bin/easysearch -d -p "$T/conf/biz5/node-$i/es.pid" </dev/null \
        >"$T/logs/biz5/node-$i/start.out" 2>&1 )
  done
fi

log "等待集群成形（最多 180s）"
for i in $(seq 1 180); do
  n=$(curl -s -m 2 "http://127.0.0.1:9211/_cluster/health?filter_path=number_of_nodes" 2>/dev/null \
      | python3 -c 'import json,sys;print(json.load(sys.stdin).get("number_of_nodes",0))' 2>/dev/null || echo 0)
  if [ "$n" = "$N" ]; then log "集群已成形：$n 个节点"; break; fi
  sleep 1
done
curl -s "http://127.0.0.1:9211/_cluster/health?pretty" | head -12
curl -s "http://127.0.0.1:9211/_cat/nodes?v&h=name,http_address,node.role,heap.percent,cpu,disk.avail" || true
