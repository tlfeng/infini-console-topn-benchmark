# TopN 评测脚本包（Easysearch 3.0.0 + Console 1.30.3）

一整套可独立运行的评测脚本：自造数据 → 压测采样 → 与 Console 图表比对。
本目录**自包含**：脚本、节点分配表都在这里，运行产物写到本目录下的 `work/`。

定位、判据与基线快照见上一级 `../README.md`。

## 文件

| 文件 | 作用 | 是否需要 root |
|---|---|---|
| `00_check.sh` | 体检：JDK 21、安装包、命令、端口 | 否 |
| `01_setup.sh` | 解包 + 双实例配置 + 装 systemd 单元（只装不起） | **是** |
| `02_start.sh` | 启动：固定 store → biz5 → biz → console | **是** |
| `03_stop.sh` | 停止：逆序 console → biz5 → biz → store | **是** |
| `04_status.sh` | 状态总览（端口/集群/纳管/指标入库/最近一轮评测） | 否 |
| `05_prepare_store.sh` | 预建 `.infini_metrics` 映射（**必须先做**） | 否 |
| `06_setup_biz5.sh` | 生成 N 节点集群（默认 8）配置与单元并启动 | **是** |
| `07_tune_memory.sh` | 压内存（堆/直接内存/元空间/线程规模） | **是**（`--apply`） |
| `08_prepare_system_indices.sh` | 补齐 `.infini_cluster` / `.infini_activities` 的 fielddata 映射（集群管理页 400） | 否 |
| `09_metrics_feeder.py` | **指标数据维护**：fill 预填充 90 天 / stream 持续写入 / prune 清理旧数据 / status | 否 |
| `10_seed.py` | 造数：12 个画像索引，可 `--node-map` 钉到指定节点 | 否 |
| `11_load.py` | 压测：9 类流量；`--no-write` 只读 | 否 |
| `12_sample.py` | 采样索引 `_stats` → 真值 JSONL | 否 |
| `14_sample_nodes.py` | 采样 `_nodes/stats` → 节点真值 | 否 |
| `15_set_readonly.py` | 按画像冻结：写索引可写、其余只读 | 否 |
| `20_console_init.py` | Console 初始化 + 纳管集群（幂等） | 首次需（要重启 console） |
| `30_verify.py` | 索引页签老接口（`/index_metrics`）：API 榜单 vs 真 TopN | 否 |
| `31_verify_node.py` | 节点页签老接口（`/node_metrics`）：API 榜单 vs 真 TopN | 否 |
| `32_verify_topn.py` | **前端实际接口**（`/visualization/data`）评测，node/index/shard，含细桶与全量排序对照 | 否 |
| `33_topn_edge.py` | 视图 TopN 边界评测（面积/颜色、shard 数据源、Top>实体数） | 否 |
| `34_raw_long_window.py` | raw 长窗受控实验：连续/断档合成的桶宽矩阵（独立于 00–41 主流程） | 否 |
| `common.py` | 公共 helper：HTTP / 登录 / 真值算法 / 采样读取 / 造数工具（各脚本复用） | — |
| `40_run_all.sh` | 一键：索引页签一轮评测 | 否 |
| `41_run_node.sh` | 一键：节点页签一轮评测 | 否 |
| `42_score.py` | 评分卡：汇总本轮报告的 Top5 命中率 / 榜首一致率，对照 `../reports/BASELINE.md` | 否 |
| `benchmark_topn.py` | 性能基准：三种选榜算法查询层耗时对比（独立于上面的评测流程，见下文「性能基准」） | 否 |
| `node_map.json` | 索引 → 节点 的画像分配表 | — |

## 前置

- WSL（systemd 可用）、JDK 21+、`python3`、`curl`
- 两个安装包：`easysearch-3.0.0-3073-linux-amd64.tar.gz`、`console-1.30.3-2407-linux-amd64.tar.gz`
  默认在 `$HOME/test`（不存在则回退 `/home/user/test`）下查找，可用 `BASE=/path/to/pkgs` 覆盖；
  也可单独指定 `PKG_ES` / `PKG_CONSOLE`
- 运行目录默认 `$HOME/test/topn`，可用 `TOPTOOLS=/path` 覆盖

## 一次性搭建

```bash
cd ~/github/infini-console-topn-benchmark/repro

bash 00_check.sh                 # 体检
sudo bash 01_setup.sh            # 解包 + 装单元（不启动）
sudo bash 02_start.sh store      # 先起 9200，下一步依赖它
bash 05_prepare_store.sh         # ★ 预建指标库映射
sudo bash 06_setup_biz5.sh 8     # 8 节点集群（生成配置并拉起）
```

## 每次启动 / 停止

```bash
sudo bash 02_start.sh            # 最小可用：store + console
sudo bash 02_start.sh biz        # 索引页签：store → biz → console
sudo bash 02_start.sh biz5       # 节点页签：store → biz5 → console
sudo bash 02_start.sh all        # 全起
bash 04_status.sh                # 自检

sudo bash 03_stop.sh             # 全停（逆序）
sudo bash 03_stop.sh biz5        # 只停 8 节点，省内存
sudo bash 03_stop.sh --clean     # 全停 + 清数据
```

> **顺序不能乱**：Console 最后起。它启动时探测不到某个集群就不会给该集群注册采集任务，
> 之后集群恢复也不会补注册，指标永久不入库（图恒空）。`02_start.sh` 已内置该顺序，
> 且在 Console 已运行、又新起集群时会自动重启一次 Console 补注册。

## 评测（复跑一轮）

### 索引页签（单节点 9201）

```bash
sudo bash 02_start.sh biz
sudo python3 20_console_init.py          # 首次纳管
bash 40_run_all.sh --duration 300        # 造数 + 压测 + 采样 + 比对
bash 40_run_all.sh --skip-seed --duration 120 --freeze   # 复用数据 + 按需只读
```

### 节点页签（8 节点 9211-9218）

```bash
sudo bash 02_start.sh biz5
BIZ_ENDPOINT=http://127.0.0.1:9211 CLUSTER_ID=topn-biz5 sudo -E python3 20_console_init.py
python3 10_seed.py --endpoint http://127.0.0.1:9211 --node-map node_map.json --recreate

bash 41_run_node.sh --duration 200                 # 可写模式，画像最完整
bash 41_run_node.sh --duration 200 --freeze        # 按需只读：写索引仍可写
bash 41_run_node.sh --duration 200 --all-readonly  # 全冻结 + 不跑写流量，最省
```

`40` 自检端口、`.infini_metrics` 映射、指标新鲜度；`41` 额外检查节点数（非 systemd 环境下该检查会跳过）。
任一项不满足会直接报错并给出处理命令。`--freeze` 模式即使中途 Ctrl-C 也会把索引恢复可写（trap 兜底）。

### 评分卡

```bash
python3 42_score.py                        # 最近一轮报告的命中率 / 榜首一致率
python3 42_score.py --json work/report/topn_view_index_topn-biz_*.json   # 指定报告
```

输出每张图的 Top5 命中率与榜首一致情况，并给出整轮汇总；对照 `../reports/BASELINE.md`。

## 指标数据维护（TopN 长窗 / 持续写入）

`09_metrics_feeder.py` 直接往监控存储 `.infini_metrics` 灌合成 `index_stats`，供 TopN 页签查询，
不依赖 Console 采集时序。数据用 `topn90d-<tag>-` 前缀隔离（默认 tag=`main`），挂已纳管的
`topn-biz5`；计数器按步单调递增，rate/latency（含 merge/scroll）都能配出点。

```bash
# 1) 预填充最近 90 天（15min 间隔，6 个画像实体，约 5.2 万条）
python3 09_metrics_feeder.py fill                       # --days 30 --interval 300 可改

# 2) 持续写入：前台运行，Ctrl-C 停止；计数器从 fill 末尾自动续接
python3 09_metrics_feeder.py stream                     # 默认不清理
python3 09_metrics_feeder.py stream --interval 60        # 1min 粒度测短窗口
# 自动清理开关：删保留窗口之外的数据，边界精确到整点，每小时检查一次
python3 09_metrics_feeder.py stream --auto-prune                              # 默认保留 2160h(90天)
python3 09_metrics_feeder.py stream --auto-prune --hours 48 --prune-every-hours 2  # 保留48h/每2h查

# 3) 手动清理（先 dry-run 看数量再真删；边界对齐整点）
python3 09_metrics_feeder.py prune --dry-run
python3 09_metrics_feeder.py prune --hours 48       # 只保留最近 48 小时
python3 09_metrics_feeder.py prune --days 90        # 兼容旧写法；--days 0 = 清空该 tag

# 4) 查看数据量 / 时间范围 / 各实体 step
python3 09_metrics_feeder.py status
```

**自动清理细节**：`stream` 默认**不清理**；加 `--auto-prune` 后启动时先清一次，之后每
`--prune-every-hours`（默认 1）小时检查一次。保留窗口用 `--hours N`（默认 2160=90天，
`--days` 兼容等价 ×24）。删除边界精确到小时：`cutoff = (当前 - N小时) 向下对齐整点`，
删除严格早于该整点的数据——边界稳定、不随执行时刻的毫秒漂移。清理只作用于本 tag 前缀，
不碰真实采集数据。

**桶宽选择**（raw 路径有 `searchBucketLimit=6000` 的结构性上限，rate/latency 桶开销 = 3×(桶数+1)）：

| 窗口 | 可用桶宽 | 说明 |
|---|---|---|
| 90 天 | 8h / 1d | 900s/1h 会 `budget_exceeded` |
| 30 天 | 3h / 8h / 1d | 900s/1h 同样超限 |
| ≤6 小时 | 60s / 900s | 短窗口细桶可用 |

采样间隔要 ≤ 桶宽，否则桶内无相邻观测、配不出 rate/latency。想让「最近 15 分钟」这类短窗口也出数，
用 `stream --interval 60` 保证写入密度。

实测（2026-11-24）：fill 2 天/5min=3462 条约 0.6s；stream 从 step 577 续接到 581、整点写入、
SIGINT 优雅停止；prune dry-run/真删数量一致；四类 TopN（indexing_rate/query_latency/
merge_latency/scroll_latency）均出 6 条曲线。

## 产物

```
work/raw/stats_*.jsonl        索引真值采样（40/30）
work/raw/nodes_*.jsonl        节点真值采样（41/31）
work/raw/stats_view_*.jsonl   视图评测采样（32/33，40/41 的清理不会删 view 文件）
work/raw/nodes_view_*.jsonl
work/report/topn_report_*.md / .json    老接口索引评测（30）
work/report/node_topn_*.md  / .json     老接口节点评测（31）
work/report/topn_view_*.md  / .json     视图接口评测（32）
work/report/topn_view_edge_*.json       边界评测（33）
work/raw/long_window_*.json             长窗实验 manifest（34）
work/report/raw_long_window_*.json      长窗实验结果（34）
```

## 性能基准（与上面评测流程无关，可单独运行）

`benchmark_topn.py` 在**隔离的 Easysearch 单节点**上用稠密合成样本，对比三种选榜/出图算法
的查询层耗时：

- `old_*`（候选并集路径）：读写活动候选（固定 60s 桶 + derivative 峰值 + terms partition 0，
  候选失败则无名称过滤按存储出图）+ 按存储排序的展示查询
- `new_store`（直查快路径）：单字段 `max` DESC 两阶段——先精确选名，再只取榜上实体的时间桶
- `new_rate`（复合分页路径）：`composite` 按实体分页拉全量时间桶，客户端差分算分取 TopN
  （页大小 `--page`，默认 128）

> `old_*` / `new_*` 为历史输出键名，与留档结果兼容，含义如上。

与 00–41 号脚本无关，不需要 Console。

```bash
# 1. 起隔离实例（集群名是安全护栏，必须一致；端口可换 --port）
/path/to/easysearch-2.1.2/bin/easysearch \
  -Ehttp.port=19219 -Ecluster.name=console-topn-local-bench \
  -Ediscovery.type=single-node -Enetwork.host=127.0.0.1

# 2. 跑基准（默认 24/100/1000/5000 实体 × 15min/24h，约 10 分钟；
#    快速验证用小矩阵）
python3 benchmark_topn.py
python3 benchmark_topn.py --counts 24 --windows 900 --runs 2 --warmup 1
```

- 断言：快路径榜单与造数基准逐名一致（`matches == top`）；候选并集路径只记录命中数与降级次数。
- 输出 JSONL 字段：`p50_ms/p95_ms` 客户端墙钟（含本地 HTTP 与 JSON 解析）、
  `took_p50` 搜索端耗时、`requests` 搜索次数、`bytes_p50` 响应字节、
  `matches` 榜单命中、`fallbacks` 候选桶超限降级次数。
- 留档结果见 `../reports/perf_topn_benchmark.{md,json}`（Easysearch 2.1.2、
  Apple M4、单并发，非生产端到端）。
- 脚本测完默认删除本次造出的 `topn-bench-*` 索引；`--keep-data` 保留。

## 常见问题

| 现象 | 原因 | 处理 |
|---|---|---|
| `Access denied ... interactive authentication` | 非 root 调 systemctl | 加 `sudo`；`02/03` 会提前拦下 |
| 图恒空、`05` 警告映射非 keyword | 指标库被动态映射成 text | `bash 05_prepare_store.sh --rebuild` |
| 端口都通但指标不更新 | Console 早于集群启动 | `03_stop.sh` 后 `02_start.sh all` |
| `02_start.sh biz5` 报缺单元 | 没装 biz5 | `sudo bash 06_setup_biz5.sh 8` |
| 集群管理页 400 / 平台概览 500（报 `Text fields are not optimised for ... set fielddata=true on [xxx]`） | `.infini_cluster` / `.infini_activities` 的 text 字段无 fielddata；页面换聚合维度就冒新字段（health_status → distribution → version → tags → ...） | `bash 08_prepare_system_indices.sh`（自动扫描全部 text 字段补 fielddata，幂等，无需逐个列字段） |
| 9200 显示 yellow | `.infini_*` 默认 1 副本 | 正常，忽略 |
