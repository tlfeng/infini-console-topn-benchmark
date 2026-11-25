# TopN 合成指标操作卡

当前流程只保留 Console 与 store，不部署或纳管业务集群。请只在可丢弃的隔离测试环境运行：安装、初始化、映射准备和 `09` 都会修改目标环境；确认端口、路径及目标 store 后再执行。不要连接生产数据，也不要把这些命令用于共享存储。

## 前置与启动

- WSL（systemd 可用）、JDK 21、`python3`、`curl`；自行准备 `easysearch-3.0.0-3073-linux-amd64.tar.gz` 和 `console-1.30.3-2407-linux-amd64.tar.gz`。
- 脚本默认从 `$HOME/test`（不存在时尝试 `/home/user/test`）找安装包，运行目录默认为其下的 `topn`；可通过 `BASE`、`TOPTOOLS`、`PKG_ES`、`PKG_CONSOLE` 配置。使用 sudo 时应确认相关环境变量和路径保持一致。
- 默认 store 为 `http://127.0.0.1:9200`，Console 为 `http://127.0.0.1:9000`。确认端口没有指向其他服务；systemd 操作需要权限。

在仓库 `repro` 目录执行：

```bash
bash 00_check.sh
sudo bash 01_setup.sh           # 安装配置与单元，不启动
sudo bash 02_start.sh store     # 先启动 store
bash 05_prepare_store.sh        # 在隔离 store 中准备指标映射
sudo bash 02_start.sh           # 启动 store + Console
python3 20_console_init.py      # 初始化 Console 与系统索引映射，不注册业务集群
bash 04_status.sh
```

初始化使用脚本的 `CONSOLE`、`STORE`、`CONSOLE_USER`、`CONSOLE_PASS` 等环境变量；已有环境应核对实际配置。`05` 会安装模板，已有不兼容映射不会因重跑自动修复。不要在含有价值数据的存储上尝试重建。停止服务可用 `sudo bash 03_stop.sh`；不要使用清数据选项作为常规操作。

## 09 离线填充、在线注入和导出

所有选项放在子命令后。默认 `STORE=http://127.0.0.1:9200`、`CLUSTER=topn-biz`、`--tag main`、`--side index`、`--interval 900`；以下显式选择 index 和 node 两侧。`CLUSTER` 是注入的标签，不会创建 Console cluster metadata。

```bash
# 隔离测试 store：填充最近 1 天，5 分钟间隔
python3 09_metrics_feeder.py fill --side both --days 1 --interval 300 --tag trial
python3 09_metrics_feeder.py status --side both --tag trial

# 前台持续注入，Ctrl-C 停止；默认不自动删除
python3 09_metrics_feeder.py stream --side both --interval 60 --tag trial

# 在另一个终端按需导出最近 6 小时的同源采样及解析式期望
python3 09_metrics_feeder.py resample --side both --hours 6 --tag trial
python3 09_metrics_feeder.py expected --side both --tag trial
```

`fill` 默认 90 天；`stream` 尝试从已有数据续接计数器。`resample` 写入 `repro/work/raw/stats_<tag>_*.jsonl` 和 `nodes_<tag>_*.jsonl`，`--step` 控制导出分桶秒数，默认 10；`expected --out <文件名>` 可写入 `repro/work/rollup/`。按实际采样密度和查询窗口解释结果：填数间隔过稀可能无法形成所需的相邻样本或短窗口速率。

`resample` 从注入文档读回数据，**不是独立真值**。`expected` 为基于注入画像的解析式期望，仅覆盖部分排序且与注入器共享假设；不能单凭这两者证明真实集群准确度。合成数据以单调计数器为主，不覆盖重置、乱序等扰动。

`prune` 和 `stream --auto-prune` 会按标签前缀及维度删除旧数据；前缀并非可靠的生产隔离边界，不能假定按 cluster 完整隔离。默认 stream 不启用自动删除。需要评估时仅在隔离测试 store 先使用 `python3 09_metrics_feeder.py prune --side both --tag trial --dry-run`，核对目标与范围；不提供实际删除作为流程步骤。

## 验证路径与限制

`32` 和 `33` 构造的是 `/elasticsearch/:system_cluster/visualization/data` 视图请求，不是 advanced TopN API。完成注入和 resample 后，可尝试以下请求；`32` 使用最近匹配的采样文件，也可通过 `--samples` 指定准确路径，避免混用轮次：

```bash
python3 32_verify_topn.py --cluster topn-biz --type index --top 5
python3 32_verify_topn.py --cluster topn-biz --type node --top 5
python3 33_topn_edge.py --cluster topn-biz --type index --top 5
```

`--cluster` 要与注入时的 `CLUSTER` 一致，并依赖 Console 登录、系统集群和相应权限；这些命令仅是待运行步骤，不保证成功或有数据，也不验证 UI 和集群选择器。`09` 不注入 `shard_stats`，因此不能由此声称 shard 覆盖。请检查采样时间范围、命名及报告内容，不将同源一致性描述为独立准确度。

`30_verify.py` / `31_verify_node.py` 保留用于 advanced TopN 的旧 index/node metrics API。它们需要对应真实或测试 cluster metadata、适当权限和 agentless 等运行前提；注入标签本身不创建 metadata。历史真实集群采样脚本已删除，若要执行需自行准备符合脚本格式与时间范围的独立样本，并核对指标口径；`09 resample` 即使格式可用也不是独立样本。不要把 32/33 的结果当作 30/31 的替代。

`34_raw_long_window.py` 的旧 API probe 同样需要 cluster metadata、权限和相应运行条件，不能把纯注入当成已满足。`35`–`37` 的写入、清理等安全边界及实验结果可靠性尚待复核，暂不推荐执行，也不能作为已验证的优化结论。

历史 rollup 实验曾记录 64 实体、30 天下 raw/native/direct 的预热 p50/p95 分别为 155/178、20/25、44/51 ms，以及全指标扫描中的精度、边界分钟与字段映射观察；这些仅是待复核的历史记录，不应据此推断生产效果或安全性。

## 现有文件

| 文件 | 作用 |
|---|---|
| `00_check.sh` | 环境检查 |
| `01_setup.sh` | 安装 store、Console 及 systemd 单元 |
| `02_start.sh`、`03_stop.sh`、`04_status.sh` | 启停与状态 |
| `05_prepare_store.sh`、`08_prepare_system_indices.sh` | 指标及系统索引映射准备 |
| `09_metrics_feeder.py` | fill、stream、resample、expected、status、prune |
| `20_console_init.py` | 初始化 Console，不纳管业务集群 |
| `30_verify.py`、`31_verify_node.py` | 保留的 advanced API 验证 |
| `32_verify_topn.py`、`33_topn_edge.py` | visualization 视图及边界验证 |
| `34_raw_long_window.py` | 长窗实验，旧 API probe 有 metadata 前提 |
| `35_rollup_seed.py`、`36_rollup_ab_bench.py`、`37_rollup_metric_sweep.py` | 安全和结果待复核的 rollup 实验 |
| `42_score.py` | 已有报告评分；确认报告格式适配后使用 |
| `benchmark_topn.py` | 独立查询层性能实验，历史结果见 `../reports/perf_topn_benchmark.md` |

历史真实集群报告和对比限制见 `../reports/BASELINE.md`；本次文档更新没有运行验证，也没有建立新的合成数据基线。
