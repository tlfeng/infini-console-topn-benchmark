# Console TopN 合成指标评测

本仓库保留 Console 和单节点指标存储 store，不再搭建业务集群。`repro/09_metrics_feeder.py` 将合成的 `index_stats` / `node_stats` 直接写入 store 的 `.infini_metrics`，用于离线历史填充和在线流式注入。它不模拟真实业务集群的所有行为，也不保证 Console UI、集群选择器或全部图表可用。

脚本操作与前提见 [repro/README.md](repro/README.md)；已有真实集群结果及性能报告见 [reports/BASELINE.md](reports/BASELINE.md)。仓库不含 Easysearch / Console 安装包，需自行准备。历史准确度测试使用 Easysearch `3.0.0-3073` 和 Console `1.30.3-2407`；这不是本次修改的新实测结果。

## 测试对象与边界

| 脚本 | 对象 | 边界 |
|---|---|---|
| `09_metrics_feeder.py` | 向 `.infini_metrics` 注入索引和节点合成指标 | 默认 cluster 标签 `topn-biz`、tag `main`；仅隔离测试 store 使用 |
| `32_verify_topn.py`、`33_topn_edge.py` | Console `/elasticsearch/:system_cluster/visualization/data` 视图查询及边界 | **不是 advanced TopN API 验证**；脚本请求成功不等于页面及选择器可用；09 不产生 shard 数据 |
| `30_verify.py`、`31_verify_node.py` | 保留的 `/elasticsearch/:id/index_metrics`、`/elasticsearch/:id/node_metrics` advanced TopN API 验证 | 需要匹配的真实或测试 cluster metadata、相应权限与 agentless 等前置条件；仅有注入的标签不够，需另行准备兼容采样 |
| `34_raw_long_window.py` | 合成长窗实验 | 其旧 API probe 仍需 cluster metadata 和权限，不能仅凭 09 注入直接假设可运行 |
| `35_rollup_seed.py`–`37_rollup_metric_sweep.py` | 保留的 rollup 实验 | 安全边界与结果可靠性待复核，不作为当前可直接运行的流程或结论 |

`09 resample` 从被测 store 中的注入文档导出采样，是**同源对照而非独立真值**；`09 expected` 输出基于注入画像的解析式期望，可用于有限的交叉检查，但与注入器共享设计假设，不能证明端到端正确。单调计数器合成模型不覆盖重置、乱序等真实扰动。要做独立准确度验证，需另行准备独立来源及口径匹配的样本。

原真实集群报告中的准确率和独立性能实验均保留为历史记录；旧环境、数据和 API 路径不同，**不能与新合成视图流程直接对比**。当前未建立新流程基线，也没有因本次文档修改声称通过任何运行验证。

## 文件索引

| 路径 | 作用 |
|---|---|
| `repro/00_check.sh`–`05_prepare_store.sh` | 环境检查、安装、启停、状态与指标存储准备 |
| `repro/08_prepare_system_indices.sh` | 系统索引映射准备 |
| `repro/09_metrics_feeder.py` | fill、stream、resample、expected、status 等指标维护 |
| `repro/20_console_init.py` | Console 初始化，不注册业务集群 |
| `repro/30_verify.py`、`31_verify_node.py` | 保留的 advanced API 验证 |
| `repro/32_verify_topn.py`、`33_topn_edge.py` | visualization 视图查询及边界验证 |
| `repro/34_raw_long_window.py` | 历史长窗实验 |
| `repro/35_rollup_seed.py`–`37_rollup_metric_sweep.py` | 待复核的 rollup 实验 |
| `repro/42_score.py` | 已有报告评分工具，须确认输入格式和适用范围 |
| `repro/benchmark_topn.py` | 独立查询层性能实验 |
| `reports/BASELINE.md` | 历史结果与适用边界 |
| `reports/topn_report_example.*`、`node_topn_example.*`、`node_topn_freeze_example.*` | 历史真实集群准确度报告 |
| `reports/perf_topn_benchmark.*` | 历史独立性能报告 |
