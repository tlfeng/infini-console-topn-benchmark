# 后台扫描长窗 TopN 基准（1,396 实体 / 30d / 8h 默认粒度）

- **日期**：2026-11-25
- **被测对象**：Console 高级监控（无 Agent）TopN 自动榜的**后台扫描路径** —— 长窗（≥24h）自动图不再走单次交互式复合分页，而是经内存注册表（single-flight + 结果缓存，不落盘）由后台任务逐页扫描 raw store。分支 [`tlfeng/console@work/async-raw-longwindow`](https://github.com/tlfeng/console/tree/work/async-raw-longwindow)（`b82b1de8`，基于 `fix/agentless-metric-topn-raw` 的 `32d02b9c`，配套 Framework `60ace893`）。
- **性质**：查询层实测（直接驱动 Console 的 TopN 计算内核），不是浏览器整页或生产共载测量。

## 结论速览

| 指标（30d/8h，Top5） | 交互路径（对照） | 后台首算 | 缓存命中 | 4 并发冷启动 |
|---|---|---|---|---|
| 写入速率 indexing_rate | ❌ budget_exceeded（48 页 / 25.4 MB / 3.8s） | **5.45s**（68 搜索 / 35.2 MB） | 284µs / 0 搜索 | 5.62s / **合计 68 搜索** |
| 查询速率 query_times | ❌ 同上（4.1s） | 5.78s | 367µs | 5.81s / 68 搜索 |
| 写入时延 indexing_latency | ❌ budget_exceeded（52.2 MB / 5.9s） | 8.17s | 300µs | 8.24s / 68 搜索 |
| 查询时延 query_latency | ❌ 同上（6.0s） | 8.25s | 281µs | 8.27s / 68 搜索 |

- **正确性**：四个指标全部通过逐点真值校验 —— Top5 榜序 + 每条曲线 91 个桶的每个点值（误差 1e-9），与夹具声明的实体画像一致。
- **正确性语义**：后台扫描与交互路径逐页完全相同（每页 21 实体 × 91 桶，同一桶成本公式、同一 identity/邻接/单调性守卫）；只放宽了两项 —— 页数上限（48 → 512）与任务期限（5 分钟）。四个请求方共享一次扫描（68 而非 272 次搜索）。
- 首算 5.4-8.3s 全部落在 30s 请求内等待预算内：**首个请求直接拿到图**，busy 兜底未触发。

## 环境

| 项 | 值 |
|---|---|
| 引擎 | Easysearch 2.1.2（integ-test-zip + 手工补装 `lang-painless-2.1.2` 模块），单节点，JVM 堆 1 GiB，Apple M4 (darwin arm64) |
| 引擎设置 | `script.allowed_types: [inline]`（时延图的配对观测查询需要 painless） |
| 夹具 | `topn-fixture-v2-index-2592000`：1,396 实体 × 5 分钟采样 × 30 天 = **12,196,852 条**（1.3 GB，3 shard，完全连续） |
| 查询 | 30 天窗 / 8h 桶（Console 默认粒度）/ Top5，`advanced_topn=true` 自动榜 |
| 每次扫描 | 1 次 PIT 打开 + 67 页（页大小 21 = min(64, 6000/(3×91+1))） |

## 边界（不能外推）

- 查询层隔离实测：**不含**浏览器渲染、多人持续刷新、采集写入共载、网关链路。
- 只测了速率/时延四图（复合分页路径）；峰值模式图（存储、内存类）走单 terms 聚合，不经分页，未在本轮范围。
- 实体规模上限：150,000 实体×桶守卫在 90 桶下约容 1,648 实体，超出仍诚实拒绝（不出半榜）。
- 夹具数据完全连续；生产环境 sealed 历史桶缺计数器变化、断档导致的空图/掉点是数据侧语义，后台扫描不改变它。
- **不可与既有报告混比**：与 [perf_topn_benchmark.md](perf_topn_benchmark.md)（不同 API/实体/窗口）、[36 号 rollup A/B](../repro/36_rollup_ab_bench.py)（Easysearch 3.0.0、64 实体、rollup 改写路径）的条件均不可比。

## 复现操作卡

1. **夹具引擎**：`127.0.0.1:19219`，集群名 `console-topn-local-bench`（easysearch 2.1.2，数据目录 `~/github/console-topn-fixture/data`）。注意两点，缺一即失败：
   - 精简 integ-test-zip 构建不带 painless，需把源码树 `modules/lang-painless/build/distributions/lang-painless-2.1.2.zip` 解压进安装目录 `modules/lang-painless/`；
   - `easysearch.yml` 加 `script.allowed_types: [inline]`（不要设 `script.allowed_contexts`，easysearch 的 context 名与 ES 不同，`sort`/`aggregation` 都是非法值）。
2. **源码**：`git clone`（或 worktree）`tlfeng/console` 分支 `work/async-raw-longwindow` 到目录 `console`；Framework 取 [`tlfeng/framework@fix/topn-jvm-start`](https://github.com/tlfeng/framework/tree/fix/topn-jvm-start) 的 `60ace893` 放到兄弟目录 `framework`（go.mod 用相对 replace `../framework`）。若 framework 缺编译常量报 `LastFrameworkCommitLog undefined`，在 `framework/config/` 下补一个文件：
   ```go
   package config

   const LastFrameworkCommitLog = "a74a6a79"
   const LastFrameworkVendorCommitLog = "local test"
   ```
3. **确认夹具**：`curl 127.0.0.1:19219/topn-fixture-v2-index-2592000/_count` → `12196852`。
4. **跑基准**：
   ```sh
   cd console && CONSOLE_TOPN_INTEGRATION=1 \
     go test ./modules/elastic/api/internal/topn -run TestLongWindowAsync1396 -count=1 -v -timeout 600s
   ```
   输出四个指标各四行：交互对照（应报 budget_exceeded）、后台首算（耗时/搜索数/字节数）、缓存命中（≈0.3ms/0 搜索）、4 并发冷启动（共享一次扫描）。

原始数字见 [async_longwindow_benchmark.json](async_longwindow_benchmark.json)。
