# TopN 准确度与性能基准（Easysearch 3.0.0 + Console 1.30.3）

一套可重复、可自造数据的本地基准环境，长期量化 INFINI Console「索引页签 / 节点页签 TopN 图」的两个维度：

- **准确度**：Console 榜单与「按指标自身口径独立计算的真 TopN」的一致程度（Top5 命中率、榜首一致率）
- **性能**：TopN 查询层在不同实体规模 / 时间窗口下的耗时（p50 / p95）

数据画像让每个指标的真 TopN 形态互不相同，并覆盖「低流量但单指标突出」的实体——这类实体是榜单一致性最敏感的样本。Console 升级或配置调整后复跑一轮，与 [`reports/BASELINE.md`](reports/BASELINE.md) 对照，即可量化变化、跟踪长期改善趋势。

- **实测版本**：`easysearch-3.0.0-3073`、`console-1.30.3-2407`
- **集群布局**
  - `9200` `topn-store`：Console 自己的指标库（不参与被监控）
  - `9211-9218` `topn-biz5`：**8 节点**业务集群（节点页签评测用）
  - `9201` `topn-biz`：单节点业务集群（索引页签评测用，默认不启动，省内存）
  - `9000` Console
- **基线快照**（详见 [`reports/BASELINE.md`](reports/BASELINE.md)）
  - 索引页签：23 张有数据的图，Top5 完全一致 **1/23**，榜首一致 **13/23**
  - 节点页签（8 节点可写）：11 张图，Top5 完全一致 **2/11**，榜首一致 **6/11**
  - 查询层：长窗口下直查快路径较候选并集路径快约 **22 倍**（100 实体 / 24 小时，存储榜 p50 63.2ms → 2.8ms）

> 仓库只含脚本与基线报告，**不含** Easysearch / Console 安装包（体积大且受各自许可约束）。
> 需要自备：`easysearch-3.0.0-3073-linux-amd64.tar.gz`、`console-1.30.3-2407-linux-amd64.tar.gz`。
> 开箱操作见 [`repro/README.md`](repro/README.md)。

---

## 1. 评测对象与方法

### 被测接口（黑盒）

| 页签 | 接口 | 说明 |
|---|---|---|
| 索引页签 | `GET /elasticsearch/:id/index_metrics?key=<metric>&top=5&min=<ms>&max=<ms>` | 老接口，`30_verify.py` |
| 节点页签 | `GET /elasticsearch/:id/node_metrics?key=<metric>&top=5&min&max` | 老接口，`31_verify_node.py` |
| 图表实际数据 | `POST /visualization/data` | 前端真实调用路径，`32_verify_topn.py`（当前主力） |

评测只依赖以上公开接口与集群自身的 `_stats` / `_nodes/stats`，不涉及 Console 内部实现。

### 真值算法（独立于被测方）

`30_verify.py` / `31_verify_node.py` 用采到的真实 `_stats` / `_nodes/stats`，按每个指标**自身口径**算真 TopN：

- 瞬时量（存储、文档数、段数…）取窗口内 max
- 速率（QPS、indexing rate…）取分桶导数峰值
- 时延（query / indexing / scroll latency…）取桶级 `Δtime / Δcount`

### 判据字段

| 字段 | 含义 |
|---|---|
| `miss` (`miss_n`) | 真 TopN 中未上榜的成员数（0 = Top5 完全一致） |
| `miss_leader` | 真 TopN 第 1 名是否不在图上（`false` = 榜首一致） |

### 性能基准（独立流程）

`repro/benchmark_topn.py` 在**隔离的 Easysearch 单节点**上，用稠密合成样本对比三种选榜/出图算法的查询层耗时（候选并集路径 / 直查快路径 / 复合分页路径），与 00–41 号评测流程无关、不需要 Console，详见 [`repro/README.md`](repro/README.md)「性能基准」。

## 2. 目录

```
infini-console-topn-benchmark/
├─ README.md                定位、方法与基线快照（本文）
├─ reports/                 基线报告与示例输出（实跑留档）
│  ├─ BASELINE.md           ★ 长期对照锚点：准确度 + 性能基线汇总
│  ├─ topn_report_example.*     索引页签准确度评测（老接口）
│  ├─ node_topn_example.*       节点页签准确度评测（老接口，可写模式）
│  ├─ node_topn_freeze_example.* 节点页签准确度评测（按需只读模式）
│  └─ perf_topn_benchmark.*     查询层性能基准（三算法对比）
└─ repro/                   ★ 评测脚本包（自包含，产物写到 repro/work/）
   ├─ README.md             脚本清单与操作卡
   ├─ 00_check.sh           环境体检（JDK / 安装包 / 命令 / 端口）
   ├─ 01_setup.sh           解包 + 双实例配置 + systemd 单元（只装不起）
   ├─ 02_start.sh           启动（固定 store → biz5 → biz → console；支持 all）
   ├─ 03_stop.sh            停止（逆序 console → biz5 → biz → store；--clean 清数据）
   ├─ 04_status.sh          状态总览
   ├─ 05_prepare_store.sh   预建 .infini_metrics 模板与索引（**关键**，见 §7.1）
   ├─ 06_setup_biz5.sh      搭 N 节点集群（默认 8）并启动
   ├─ 07_tune_memory.sh     压内存（堆/直接内存/元空间/线程规模）+ 按正确顺序重启
   ├─ 08_prepare_system_indices.sh  补齐 .infini_cluster 的 fielddata 聚合映射（集群管理页 400）
   ├─ 09_metrics_feeder.py  指标数据维护：fill 90 天 / stream 持续写 / prune 清理 / status
   ├─ common.py             公共 helper（HTTP / 登录 / 真值算法 / 采样 / 造数）
   ├─ 10_seed.py            造数：12 个画像索引（--node-map 钉到指定节点）
   ├─ 11_load.py            压测：9 类流量（--no-write 只读）
   ├─ 12_sample.py          采样索引 _stats → 索引真值
   ├─ 14_sample_nodes.py    采样 _nodes/stats → 节点真值
   ├─ 15_set_readonly.py    按画像冻结：写索引可写、其余只读
   ├─ 20_console_init.py    Console 初始化 + 纳管集群（幂等）
   ├─ 30_verify.py          索引页签老接口：API 榜单 vs 真 TopN
   ├─ 31_verify_node.py     节点页签老接口：API 榜单 vs 真 TopN
   ├─ 32_verify_topn.py     前端实际接口 /visualization/data 评测（当前主力）
   ├─ 33_topn_edge.py       视图 TopN 边界评测（面积/颜色、shard、Top>基数）
   ├─ 34_raw_long_window.py raw 长窗受控实验（连续/断档 × 桶宽矩阵）
   ├─ 40_run_all.sh         一键：索引页签一轮评测
   ├─ 41_run_node.sh        一键：节点页签一轮评测（--freeze / --all-readonly）
   ├─ 42_score.py           评分卡：汇总本轮报告的命中率/榜首一致率，对照 BASELINE.md
   ├─ benchmark_topn.py     性能基准：三种选榜算法查询层耗时对比（独立运行）
   └─ node_map.json         索引→节点 的画像分配表
```

运行期数据在 WSL：`/home/user/test/topn/{es-store,es-biz,es-biz5,conf,data,logs}`；
采样与报告在 `repro/work/{raw,report}`。

## 3. 启动步骤（顺序不能乱）

### 3.0 谁依赖谁

```
① store 9200     Console 的指标库，也是采集数据的落点          必须最先起
② biz5 9211..   被测集群（节点页签用，8 节点）  ┐ 两者并行，
   biz  9201     被测集群（索引页签用，单节点）  ┘ 都必须在 ③ 之前
③ console 9000   最后起。启动时探测不到哪个集群，就不给它注册采集任务；
                 之后集群恢复也不会补注册 → 指标不入库、图恒空
```

`02_start.sh` 内部**固定按 store → biz5 → biz → console 执行**，参数写什么顺序都不影响；只要起了 biz/biz5/console 就会自动补上 store。
如果 Console 已经在跑、这次又新起了集群，脚本会**自动重启一次 Console** 来补注册采集（否则新集群指标不入库）。

> **所有启停都要 root。** WSL 的 polkit 要求交互认证，普通用户 `systemctl start` 会得到
> `Access denied as the requested operation requires interactive authentication`。
> `02_start.sh` / `03_stop.sh` 已内置检查，非 root 会立刻报错而不是干等超时。
> 统一写成 `sudo bash repro/...`；不想输密码就用 `wsl -u root bash repro/...`。

### 3.1 一次性准备（只做一遍）

```bash
cd ~/github/infini-console-topn-benchmark

bash repro/00_check.sh                 # 体检：JDK 21 / 端口 / 内存（不需要 root）
sudo bash repro/01_setup.sh            # 解包 + 双实例配置 + 装 systemd 单元（只装不起）
sudo bash repro/02_start.sh store      # 先起 9200：下一步依赖它
bash repro/05_prepare_store.sh         # ★ 预建 .infini_metrics 映射（见 §7.1）
sudo bash repro/06_setup_biz5.sh 8     # 生成 8 节点配置 + 单元，装完会直接把节点拉起来
```

### 3.2 每次开机后的启动

```bash
sudo bash repro/02_start.sh            # 最小可用：store + console
bash repro/04_status.sh                # 自检：端口 / 集群 / 纳管 / 指标入库
# 要跑评测再按 3.3 / 3.4 追加对应集群，不要先起 console 再起集群
```

### 3.3 A. 索引页签评测（单节点 9201）

```bash
sudo bash repro/02_start.sh biz        # store → biz → console
sudo python3 repro/20_console_init.py  # 首次纳管（幂等，已注册则跳过）；要重启 Console 故需 root
bash repro/40_run_all.sh --duration 300
```

### 3.4 B. 节点页签评测（8 节点 9211-9218）

```bash
sudo bash repro/02_start.sh biz5       # store → biz5（等 8 节点成形）→ console

BIZ_ENDPOINT=http://127.0.0.1:9211 CLUSTER_ID=topn-biz5 \
  sudo -E python3 repro/20_console_init.py   # 首次纳管；会重启 Console，之后采集才开始

python3 repro/10_seed.py --endpoint http://127.0.0.1:9211 \
  --node-map repro/node_map.json --recreate

bash repro/41_run_node.sh --duration 200                # 可写模式，画像最完整
bash repro/41_run_node.sh --duration 200 --freeze       # 按需只读：写索引可写、其余冻结
bash repro/41_run_node.sh --duration 200 --all-readonly  # 全只读 + 不跑写流量，最省
```

> 两个 `run` 脚本开头会**前置自检**：集群端口不通、节点数不足、或 metrics 里该集群的指标已超过 120~180s 没更新（=采集没注册），都会直接报错并给出处理命令，不会闷头跑出一份假报告。

### 3.5 停止

```bash
sudo bash repro/03_stop.sh                 # 全停：console → biz5 → biz → store（逆依赖序）
sudo bash repro/03_stop.sh console biz5    # 只停指定目标
sudo bash repro/03_stop.sh --clean         # 全停 + 清空 data/{store,biz,biz5} 与 console-kv
```

停完 `free -m` 应回到 ~1.1G 已用、0 个 java 进程、无监听端口（跑满 10 个 ES 实例时是 ~5.8G）。

### 3.6 启动异常对照

| 现象 | 原因 | 处理 |
|---|---|---|
| `Access denied ... requires interactive authentication` | 非 root 调 systemctl | `sudo bash repro/02_start.sh ...` |
| `02_start.sh biz5` 报「未找到 systemd 单元」 | 还没装 biz5 | `sudo bash repro/06_setup_biz5.sh 8` |
| 有端口、但图上一直空 | Console 早于集群启动，采集未注册 | `sudo bash repro/03_stop.sh && sudo bash repro/02_start.sh all` |
| `04_status.sh` 里 `topn-biz5-node*` 只有 3/8 active | 节点 OOM 或 conf 缺失 | `journalctl -u topn-biz5-node<N> -n 50` |
| 存储集群 yellow | `.infini_*` 默认 1 副本，单节点分不出来 | 正常现象，不用处理 |

Console 账号：`admin / Admin@12345`（可用 `CONSOLE_USER/CONSOLE_PASS` 覆盖）。

## 4. 数据画像设计

评测的区分度来自画像：让**每个指标的真 TopN 互不相同**，且榜首与「流量榜首」错开，覆盖各种排名形态。

**索引维度**（`10_seed.py`）：每个索引只把 1~2 个维度做突出，其余压平。

| 索引 | 画像 | 真值榜首目标 |
|---|---|---|
| `prod_hot_search` | 搜索 QPS ~200/s、数据仅 2.4MB | 读 QPS 榜成员 |
| `prod_bulk_store` | 73MB 大字段、几乎零读写 | `index_storage` 真第 1 |
| `prod_write_burst` | 持续 bulk 写入 ~800 docs/s | `indexing_rate` 第 1 |
| `prod_slow_query` | 2 qps 但每查都是多级聚合+脚本 | `query_latency` 真第 1 |
| `prod_scroll_ctx` | 常驻 scroll + 连续翻页 | `scroll_times` 真第 1 |
| `prod_cache_churn` | 随机字段组合查询，缓存反复失效 | `query_cache_*` 真第 1 |
| `prod_delete_heavy` | 周期 delete_by_query + 回补 | `docs_deleted` 第 1 |
| `prod_get_heavy` | GET by id ~60/s | get 系列（页面无图） |

**节点维度**（`node_map.json`）：把整个索引钉在指定节点（`index.routing.allocation.require._name`），制造节点画像。

| 节点 | 承载 | 期望的真榜位 |
|---|---|---|
| node-1 | prod_hot_search | 读 QPS 第 1 |
| node-2 | prod_write_burst + medium_a | 写 QPS 第 1 |
| node-3 | prod_slow_query | query_latency 第 1（QPS 很低） |
| node-4 | prod_bulk_store（73MB） | index_storage / segment_count 第 1（QPS≈0） |
| node-5 | delete + cache_churn | 删除/缓存 |
| node-6 | prod_scroll_ctx | scroll_rate 第 1 |
| node-7 | get_heavy + medium_b | GET 流量 |
| node-8 | 系统索引 + medium_c | 几乎空闲，覆盖「低流量节点」样本 |

## 5. 评测判据与基线

判据见 §1「判据字段」：`miss`（真 TopN 中未上榜成员数）与 `miss_leader`（榜首是否一致）。
每轮评测后跑 `python3 repro/42_score.py` 得到评分卡，与本节数字及 `reports/BASELINE.md` 对照。

### 索引页签基线（`reports/topn_report_example.md`）

23 张有数据的图 / Top5 完全一致 **1/23** / 榜首一致 **13/23**。

| 指标 | 图上 Top5 | 真 Top5 |
|---|---|---|
| index_storage | write_burst, delete_heavy, medium_a, cache_churn, hot_search | **bulk_store**, write_burst, slow_query, delete_heavy, scroll_ctx |
| query_latency | 同上 | **slow_query**, bulk_store, cache_churn, scroll_ctx, hot_search |
| scroll_times | 同上 | **scroll_ctx**, delete_heavy, get_heavy, medium_c, medium_b |

### 节点页签基线（`reports/node_topn_example.md`）

8 节点、可写压测：11 张有数据的图 / Top5 完全一致 **2/11** / 榜首一致 **6/11**；
11 张图的榜单成员完全相同（同一组 5 个节点），而各指标的真值榜首分布差异很大。

| 指标 | 图上节点 | 真 Top5 |
|---|---|---|
| index_storage | 1,2,5,6,7 | **4**,2,5,7,3 |
| segment_count | 1,2,5,6,7 | **4**,2,5,3,7 |
| query_latency | 1,2,5,6,7 | **4**,3,5,8,7 |
| os_cpu / os_used_mem | 1,2,5,6,7 | 4,6,3,8,2 |

按需只读模式（`--freeze`，写索引仍在写）一致率相同：Top5 完全一致 2/11、榜首一致 8/11，见 `reports/node_topn_freeze_example.md`。

### 长期跟踪

- **改进目标**：Top5 命中率与榜首一致率逐步提升至 100%（所有图 `miss=0` 且 `miss_leader=false`）
- 每次升级 Console / 调整配置后复跑 §3.3 或 §3.4，用 `42_score.py` 生成评分卡，与 `reports/BASELINE.md` 与历史轮次对比
- 性能维度用 `benchmark_topn.py` 在隔离实例上复跑，对照 `reports/perf_topn_benchmark.md`

## 6. 内存占用（单机 10 个 ES 实例）

调优前后（同一台 16 核 / 15GB WSL）：

| 项 | 调优前 | 调优后 |
|---|---|---|
| 系统已用内存 | 13.3 GB | **5.7 GB** |
| 可用内存 | 2.0 GB | **9.5 GB** |
| 单节点 RSS（8 节点集群） | ~1.1 GB | **~550 MB** |

调优手段（`07_tune_memory.sh`，写进各实例 `jvm.options`）：

```
-Xms256m -Xmx256m                 # 数据量小，堆不用大（store 用 512m）
-Xss512k                          # 线程栈
-XX:MaxDirectMemorySize=128m      # 网络/堆外缓冲
-XX:MaxMetaspaceSize=160m
-XX:ReservedCodeCacheSize=64m
-XX:ActiveProcessorCount=2        # ★ 关键：16 核会让 ES 线程池开上百线程
```

`-XX:ActiveProcessorCount=2` 是收益最大的一项：ES 的 search/write/refresh 线程池都按核数缩放，16 核时线程栈与缓冲池占用远超堆本身。

再往下压的几档（按需取舍）：

```bash
sudo bash repro/07_tune_memory.sh --heap 192m --only biz5 --apply   # 再省 ~1.5G
sudo bash repro/06_setup_biz5.sh 6                                   # 6 节点仍有区分度
sudo bash repro/03_stop.sh biz5                                      # 平时不跑节点评测就关掉（省 ~4.4G）
sudo bash repro/03_stop.sh biz                                       # 索引页签集群同样按需启动
```

**索引冻结：写画像可写，其余只读**（`--freeze`，`15_set_readonly.py`）

不是一刀切全只读。按画像分两类处理，贴近生产里「历史索引只读、热点索引在写」的形态：

| 类型 | 索引 | 设置 |
|---|---|---|
| 写画像（保持可写） | `prod_write_burst`、`prod_delete_heavy` | `blocks.write=false`、`refresh_interval=1s` |
| 只读画像（冻结） | 其余 10 个（hot_search / bulk_store / slow_query / scroll_ctx / cache_churn / get_heavy / medium_* / .kibana_task_manager_1） | `blocks.write=true`、`refresh_interval=-1` |

```bash
bash repro/41_run_node.sh --duration 200 --freeze        # 节点页签
bash repro/40_run_all.sh --freeze                        # 索引页签（9201）
python3 repro/15_set_readonly.py --on|--off              # 单独开关
```

收益：只读索引不再产生新段、不再 merge，写入缓冲只由两个写索引占用；写速率画像仍然保留，
`indexing_rate` / `docs_deleted` 的评测不受影响。实测该模式下一致率与可写模式相同（Top5 完全一致 2/11、榜首一致 8/11），内存 5.8G。

需要更省资源时用 `--all-readonly`（全部冻结且不跑写 worker）；该模式下榜单成员分布会变化，与可写模式基线不可直接对比。

## 7. 注意事项

1. **`.infini_metrics` 映射必须预建为 keyword**
   Console 的聚合直接打 `metadata.labels.index_name` / `metadata.name` 原始路径，
   若让 ES 动态映射成 `text`（只有 `.keyword` 子字段），聚合会报
   `Text fields are not optimised for ... aggregations`，**所有图恒为空**，前端不报错。
   `05_prepare_store.sh` 会装模板并建好带 keyword 映射的索引；已建错的用 `--rebuild`。

   同类事项：`metadata.labels.cluster_id` / `index_name` 本身就是 `keyword` 类型，**没有 `.keyword` 子字段**。
   查询时写 `cluster_id.keyword` 不报错、但**恒查空**——脚本里的采集自检对此有防护，避免误判成「没数据」。

2. **WSL 里不能用 nohup 长期托管**
   `wsl.exe` 会话退出会回收该会话下的进程（ES 启动 0.3s 后就被 SIGTERM）。
   统一用 systemd 单元（`topn-es-store` / `topn-es-biz` / `topn-biz5-node1..N` / `topn-console`）。

3. **接口时间是 `min`/`max`（毫秒），不是 `from`/`to`**
   传错不报错，后端退化成 `now-1s ~ now` 的空窗口，返回 `lines: []`。

4. **重启顺序：store → 业务集群 → console**
   Console 启动时若探测不到业务集群，就**不会注册采集任务**；之后集群恢复 available 也不会补注册，
   指标从此不入库（图恒空，且只有 `journalctl` 里能看出）。`07_tune_memory.sh --apply` 已按此顺序执行。

5. **节点数必须大于 TopN 才有区分度**
   单节点集群下节点 TopN 恒等于全集，评测没有区分度；节点页签评测至少要 6~8 个节点。

6. **启停必须 root，否则 polkit 拒绝后还要干等超时**
   普通用户 `systemctl start/stop topn-*` 报 `Access denied as the requested operation requires interactive authentication`，
   而 `02_start.sh` 会继续等 120s 才判失败，很容易误判成「启动很慢」。现在两个脚本开头就会检查 `id -u`，非 root 立刻退出并给出 `sudo` 写法。

另外：Console 1.30.3 没有可用的公开「注册集群」REST（`/elasticsearch/_register` 会落到 ES 代理返回 405），
`20_console_init.py` 采用「直接写 `.infini_cluster` 一条配置文档 + 重启 Console」的方式纳管。

## 8. 环境信息

| 项 | 值 |
|---|---|
| JDK | `/home/user/jdks/jdk-21.0.13+9`（Easysearch 3.0 / Lucene 10.5 需 21+） |
| 堆 | store 512m；biz 1g（未调）；biz5 各 256m |
| 安全 | 业务集群关闭；Console 保留登录 |
| 数据 | `/home/user/test/topn/data/{store,biz,biz5}` |
| 日志 | `/home/user/test/topn/logs/*` + `journalctl -u topn-console` |
| 采样/报告 | `/home/user/test/topn-repro/repro/work/{raw,report}` |

> 存储集群 9200 显示 `yellow` 是正常的：Console 初始化模板给 `.infini_*` 默认 1 副本，单节点下副本无法分配。

## 9. 常用操作

```bash
bash repro/04_status.sh                        # 环境状态总览（只读，不需 root）
systemctl status 'topn-*' --no-pager             # 所有实例
sudo bash repro/02_start.sh all                # 全起（store→biz5→biz→console）
sudo bash repro/03_stop.sh                     # 全停
sudo bash repro/03_stop.sh biz5                # 只停 8 节点集群，省内存
journalctl -u topn-console -f                    # Console 日志
sudo bash repro/03_stop.sh --clean             # 停服 + 清数据
python3 repro/31_verify_node.py --warmup 60    # 只重跑节点比对（复用最近采样）
python3 repro/42_score.py                      # 最近一轮评测的命中率/一致率评分卡
```
