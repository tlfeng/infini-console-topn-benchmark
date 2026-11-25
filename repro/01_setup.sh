#!/bin/bash
# =============================================================================
# 01_setup.sh —— 解包并生成 TopN 基准环境（两套 Easysearch + 一套 Console）
#
#   9200  存储/采集集群 es-store  （Console 自己的指标库，不参与被监控）
#   9201  业务集群     es-biz    （造数目标，TopN 榜单就是在这个集群上评测）
#   9000  Console 1.30.3
#
# 幂等：重复执行只补缺失部分，不覆盖已有 data 目录。
# =============================================================================
set -euo pipefail

BASE=${BASE:-}
if [ -z "$BASE" ]; then
  for d in "$HOME/test" /home/user/test; do [ -d "$d" ] && { BASE=$d; break; }; done
fi
BASE=${BASE:-$HOME/test}
TOPTOOLS=${TOPTOOLS:-$BASE/topn}
PKG_ES=${PKG_ES:-$BASE/easysearch-3.0.0-3073-linux-amd64.tar.gz}
PKG_CONSOLE=${PKG_CONSOLE:-$BASE/console-1.30.3-2407-linux-amd64.tar.gz}

JAVA_HOME_CANDIDATES=(
  "${JAVA_HOME:-}"
  "$HOME"/jdks/jdk-21.0.13+9
  /home/user/jdks/jdk-21.0.13+9
  /usr/lib/jvm/java-21-openjdk-amd64
)

log() { printf '\033[36m[setup]\033[0m %s\n' "$*"; }
die() { printf '\033[31m[setup][ERROR]\033[0m %s\n' "$*" >&2; exit 1; }

mkdir -p "$TOPTOOLS"

# ---------- 0. 依赖检查 ----------
log "检查 JDK（Easysearch 3.0 基于 Lucene 10.x，需要 JDK 21+）"
JAVA_OK=""
for h in "${JAVA_HOME_CANDIDATES[@]}"; do
  [ -n "$h" ] && [ -x "$h/bin/java" ] || continue
  v=$("$h/bin/java" -version 2>&1 | head -1)
  case "$v" in
    *\"21*|*\"2[2-9]*|*\"3[0-9]*) JAVA_OK="$h"; break ;;
  esac
done
[ -n "$JAVA_OK" ] || die "未找到 JDK 21+，请先安装或设置 JAVA_HOME"
log "使用 JDK: $JAVA_OK ($("$JAVA_OK/bin/java" -version 2>&1 | head -1))"
echo "$JAVA_OK" > "$TOPTOOLS/java_home"

# ---------- 1. 目录 ----------
[ -f "$PKG_ES" ]      || die "找不到 $PKG_ES"
[ -f "$PKG_CONSOLE" ] || die "找不到 $PKG_CONSOLE"
mkdir -p "$TOPTOOLS"/{data/store,data/biz,logs/store,logs/biz,logs/console,work/raw,work/report,console}
log "工作目录: $TOPTOOLS"

# ---------- 2. 解包 ----------
unpack_es() {  # $1=目标目录
  local dst=$1
  if [ ! -x "$dst/bin/easysearch" ]; then
    log "解包 Easysearch -> $dst"
    mkdir -p "$dst"
    tar xzf "$PKG_ES" -C "$dst" --strip-components=1
  else
    log "已存在 $dst，跳过解包"
  fi
}
unpack_es "$TOPTOOLS/es-store"
unpack_es "$TOPTOOLS/es-biz"

if [ ! -x "$TOPTOOLS/console/console-linux-amd64" ]; then
  log "解包 Console -> $TOPTOOLS/console"
  tar xzf "$PKG_CONSOLE" -C "$TOPTOOLS/console"
else
  log "已存在 console，跳过解包"
fi
chmod +x "$TOPTOOLS/console/console-linux-amd64" || true

# ---------- 3. 生成 Easysearch 配置 ----------
# 关闭 security（基准环境，不需要认证噪音），单节点，本地回环
write_es_conf() {  # $1=角色(store|biz) $2=http端口 $3=transport端口
  local role=$1 http=$2 tp=$3 dir="$TOPTOOLS/es-$1"
  local cfg="$dir/config/easysearch.yml"
  local cname="topn-$role" nname="node-$role"
  local data="$TOPTOOLS/data/$role" logs="$TOPTOOLS/logs/$role"
  mkdir -p "$data" "$logs"
  cp -n "$cfg" "$cfg.orig" 2>/dev/null || true
  cat > "$cfg" <<EOF
# ---- TopN 基准环境生成配置（勿手改，改 01_setup.sh）----
cluster.name: $cname
node.name: $nname
node.roles: [ master, data, ingest, search, remote_cluster_client ]

path.data: $data
path.logs: $logs
network.host: 127.0.0.1
http.port: $http
transport.port: $tp
discovery.type: single-node

# 基准环境：关闭安全认证，便于 curl / console 直接纳管
security.enabled: false
security.audit.type: noop
security.ssl.http.enabled: false
security.ssl.transport.enabled: false

bootstrap.memory_lock: false
bootstrap.system_call_filter: false
cluster.routing.allocation.disk.threshold_enabled: false
action.destructive_requires_name: false

# Console 前端跨域 + 造数脚本连接
http.cors.enabled: true
http.cors.allow-origin: "*"
http.cors.allow-headers: X-Requested-With,Content-Type,Content-Length,Authorization
http.compression: true

# 画像需要快速产生 segments / 缓存抖动，同时保持查询开销可控
indices.memory.index_buffer_size: 20%
thread_pool.write.queue_size: 1000
EOF
  chmod 600 "$cfg"
  # JVM 堆
  if ! grep -qE '^# TOPN-(REPRO|BENCH)' "$dir/config/jvm.options"; then
    {
      echo ""
      echo "# TOPN-BENCH"
      echo "-Xms${ES_HEAP:-1g}"
      echo "-Xmx${ES_HEAP:-1g}"
    } >> "$dir/config/jvm.options"
  fi
  log "配置 $cfg (http:$http, data:$data)"
}

write_es_conf store 9200 9300
write_es_conf biz   9201 9301

# ---------- 4. 生成 Console 配置 ----------
CONF="$TOPTOOLS/console/console.yml"
if [ ! -f "$CONF.orig" ]; then
  cp "$TOPTOOLS/console/console.yml" "$CONF.orig"
fi
cat > "$CONF" <<'EOF'
# ---- TopN 基准环境生成配置（勿手改，改 01_setup.sh）----
path.configs: "config"
configs:
  managed: false
  auto_reload: true
  manager:
    local_configs_repo_path: ./config_repo/
  tls:
    enabled: false
    ca_file: config/certs/ca.crt
    cert_file: config/certs/ca.crt
    key_file: config/certs/ca.key
    skip_insecure_verify: true

web:
  enabled: true
  embedding_api: true
  security:
    enabled: true          # 首次访问走 /setup 初始化管理员账号
  ui:
    enabled: true
    path: .public
    vfs: true
    local: true
  network:
    binding: 0.0.0.0:9000
    skip_occupied_port: false
  gzip:
    enabled: true

elastic:
  enabled: true
  remote_configs: true
  health_check:
    enabled: true
    interval: 30s
  availability_check:
    enabled: true
    interval: 60s
  metadata_refresh:
    enabled: true
    interval: 30s
  cluster_settings_check:
    enabled: true
    interval: 20s
  store:
    enabled: true          # 指标落 9200（es-store）
  orm:
    enabled: true
    init_template: true
    template_name: ".infini"
    index_prefix: ".infini_"
    build_template_for_object: false

metrics:
  enabled: true
  queue: metrics
  elasticsearch:
    enabled: true
    cluster_stats: true
    node_stats: true
    index_stats: true

badger:
  enabled: true
  single_bucket_mode: true
  path: ''
  memory_mode: false
  sync_writes: false
  mem_table_size: 10485760
  num_mem_tables: 1
  value_log_max_entries: 1000000
  value_log_file_size: 536870912
  value_threshold: 1048576
  num_level0_tables: 1
  num_level0_tables_stall: 2

security:
  enabled: true

http_client:
  default:
    tls:
      skip_insecure_verify: true
      skip_domain_verify: true
    proxy:
      enabled: false
EOF
# Console 自身的 badger 库放在工作目录
mkdir -p "$TOPTOOLS/data/console-kv"

log "Console 配置: $CONF"

# ---------- 5. Python 依赖 ----------
PY=$(command -v python3 || true)
[ -n "$PY" ] || die "未找到 python3"
log "python3: $PY ($($PY -V 2>&1))"
$PY - <<'PYEOF' || true
mods = []
for m in ("requests", "yaml"):
    try:
        __import__(m)
    except Exception:
        mods.append(m)
print("缺少 python 模块: %s （造数脚本仅用标准库，可不装）" % (mods or "无"))
PYEOF

# ---------- 6. systemd 服务（WSL 里 nohup 的进程会随会话退出被回收，必须交给 systemd） ----------
if [ "$(id -u)" = "0" ]; then
  # ES 拒绝以 root 运行：优先用 sudo 的调用者，其次找常见普通用户
  if [ -z "${RUN_USER:-}" ]; then
    for cand in "${SUDO_USER:-}" user; do
      if [ -n "$cand" ] && [ "$cand" != "root" ] && id -u "$cand" >/dev/null 2>&1; then
        RUN_USER=$cand; break
      fi
    done
  fi
  [ -n "${RUN_USER:-}" ] || die "找不到可用的非 root 运行用户，请用 RUN_USER=<用户名> 指定（Easysearch 不能以 root 运行）"
  log "服务运行用户: $RUN_USER"
  chown -R "$RUN_USER":"$RUN_USER" "$TOPTOOLS" 2>/dev/null || true

  write_unit() {  # $1=name $2=description $3=workdir $4=exec $5=extra-env
    cat > "/etc/systemd/system/$1.service" <<EOF
[Unit]
Description=$2
After=network-online.target

[Service]
Type=simple
User=$RUN_USER
WorkingDirectory=$3
Environment=JAVA_HOME=$JAVA_OK
Environment=ES_JAVA_HOME=$JAVA_OK
$5
ExecStart=$4
Restart=no
LimitNOFILE=65535
LimitMEMLOCK=infinity
TimeoutStopSec=90
SuccessExitStatus=143

[Install]
WantedBy=multi-user.target
EOF
    log "写入 systemd 单元: /etc/systemd/system/$1.service"
  }

  write_unit topn-es-store "TopN repro - easysearch store (9200)" \
    "$TOPTOOLS/es-store" "$TOPTOOLS/es-store/bin/easysearch" ""
  write_unit topn-es-biz "TopN repro - easysearch biz (9201)" \
    "$TOPTOOLS/es-biz" "$TOPTOOLS/es-biz/bin/easysearch" ""
  write_unit topn-console "TopN repro - console (9000)" \
    "$TOPTOOLS/console" "$TOPTOOLS/console/console-linux-amd64" \
    $'Environment=INFINI_CONSOLE_ENDPOINT=http://127.0.0.1:9000\nEnvironment=INGEST_CLUSTER_ENDPOINT=http://127.0.0.1:9200'

  systemctl daemon-reload
  log "systemd 单元已安装（systemctl start topn-es-store / topn-es-biz / topn-console）"
else
  log "非 root 运行：跳过 systemd 单元安装。请用 'wsl -d Ubuntu -u root' 重跑本脚本。"
fi

echo
log "完成。下一步: bash 02_start.sh"
