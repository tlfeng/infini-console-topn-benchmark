# TopN 准确度评测报告（节点页签 · 按需只读模式 · 基线）

- 集群: `topn-biz5` / Console `http://127.0.0.1:9000`
- 真值窗口: 2026-11-20 14:45:56 ~ 14:47:26（预热 60s）
- 采样: `/home/user/test/topn-repro/work/raw/nodes_20261120_144456.jsonl`
- 图例: 🔴 榜首不一致 / 🟡 部分成员不一致 / 🟢 Top5 完全一致

**有数据的图 11 张，Top5 完全一致 2/11，榜首一致 8/11。**
按需只读模式（写画像索引仍可写）下一致率与可写模式相同。

| 指标 | 图上节点 | 真 Top5 | 未上榜成员 |
|---|---|---|---|
| 🟡 `os_cpu` | node-1, node-2, node-5, node-6, node-7 | node-6, node-8, node-4, node-7, node-2 | node-8, node-4 |
| 🟡 `os_used_mem` | node-1, node-2, node-5, node-6, node-7 | node-6, node-8, node-4, node-7, node-2 | node-8, node-4 |
| 🟡 `jvm_heap_used_percent` | node-1, node-2, node-5, node-6, node-7 | node-2, node-1, node-8, node-7, node-3 | node-8, node-3 |
| 🔴 `index_storage` | node-1, node-2, node-5, node-6, node-7 | node-4, node-2, node-5, node-7, node-3 | node-4, node-3 |
| 🔴 `segment_count` | node-1, node-2, node-5, node-6, node-7 | node-4, node-2, node-5, node-3, node-7 | node-4, node-3 |
| 🟡 `docs_count` | node-1, node-2, node-5, node-6, node-7 | node-2, node-4, node-5, node-3, node-7 | node-4, node-3 |
| 🟡 `indexing_rate` | node-1, node-2, node-5, node-6, node-7 | node-2, node-5, node-6, node-8, node-4 | node-8, node-4 |
| 🟢 `query_rate` | node-1, node-2, node-5, node-6, node-7 | node-1, node-5, node-2, node-6, node-7 | — |
| 🟡 `scroll_rate` | node-1, node-2, node-5, node-6, node-7 | node-6, node-5, node-8, node-4, node-7 | node-8, node-4 |
| 🟢 `indexing_latency` | node-1, node-2, node-5, node-6, node-7 | node-5, node-2 | — |
| 🔴 `query_latency` | node-1, node-2, node-5, node-6, node-7 | node-4, node-3, node-5, node-8, node-7 | node-4, node-3, node-8 |
