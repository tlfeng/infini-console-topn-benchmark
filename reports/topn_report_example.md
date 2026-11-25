# TopN 准确度评测报告（索引页签 · 基线）

- 集群: `topn-biz` / Console `http://127.0.0.1:9000`
- 真值窗口: 2026-11-20 14:06:20 ~ 14:08:20（跳过预热 60s）
- 采样文件: `/home/user/test/topn-repro/work/raw/stats_20261120_140520.jsonl`
- 图例: 🔴 榜首不一致 / 🟡 部分成员不一致 / 🟢 Top5 完全一致

**有数据的图 23 张，Top5 完全一致 1/23，榜首一致 13/23。**

| 指标 | 图上 Top5 | 真 Top5 | 未上榜成员 |
|---|---|---|---|
| 🔴 `index_storage` | prod_write_burst, prod_delete_heavy, prod_medium_a, prod_cache_churn, prod_hot_search | prod_bulk_store, prod_write_burst, prod_slow_query, prod_delete_heavy, prod_scroll_ctx | prod_bulk_store, prod_slow_query, prod_scroll_ctx |
| 🟡 `doc_count` | prod_write_burst, prod_delete_heavy, prod_medium_a, prod_cache_churn, prod_hot_search | prod_write_burst, prod_bulk_store, prod_delete_heavy, prod_slow_query, prod_scroll_ctx | prod_bulk_store, prod_slow_query, prod_scroll_ctx |
| 🟡 `docs_deleted` | prod_write_burst, prod_delete_heavy, prod_medium_a, prod_cache_churn, prod_hot_search | prod_write_burst, prod_delete_heavy, prod_get_heavy, prod_medium_c, prod_medium_b | prod_get_heavy, prod_medium_c, prod_medium_b |
| 🔴 `segment_count` | prod_write_burst, prod_delete_heavy, prod_medium_a, prod_cache_churn, prod_hot_search | prod_bulk_store, prod_write_burst, prod_delete_heavy, prod_slow_query, prod_get_heavy | prod_bulk_store, prod_slow_query, prod_get_heavy |
| 🟡 `indexing_rate` | prod_write_burst, prod_delete_heavy, prod_medium_a, prod_cache_churn, prod_hot_search | prod_write_burst, prod_delete_heavy, prod_get_heavy, prod_medium_c, prod_medium_b | prod_get_heavy, prod_medium_c, prod_medium_b |
| 🟡 `indexing_bytes` | prod_write_burst, prod_delete_heavy, prod_medium_a, prod_cache_churn, prod_hot_search | prod_write_burst, prod_delete_heavy, prod_get_heavy, prod_medium_c, prod_medium_b | prod_get_heavy, prod_medium_c, prod_medium_b |
| 🟡 `query_times` | prod_write_burst, prod_delete_heavy, prod_medium_a, prod_cache_churn, prod_hot_search | prod_hot_search, prod_cache_churn, prod_medium_a, prod_scroll_ctx, prod_medium_b | prod_scroll_ctx, prod_medium_b |
| 🟡 `fetch_times` | prod_write_burst, prod_delete_heavy, prod_medium_a, prod_cache_churn, prod_hot_search | prod_hot_search, prod_cache_churn, prod_medium_a, prod_scroll_ctx, prod_medium_b | prod_scroll_ctx, prod_medium_b |
| 🔴 `scroll_times` | prod_write_burst, prod_delete_heavy, prod_medium_a, prod_cache_churn, prod_hot_search | prod_scroll_ctx, prod_delete_heavy, prod_get_heavy, prod_medium_c, prod_medium_b | prod_scroll_ctx, prod_get_heavy, prod_medium_c, prod_medium_b |
| 🟡 `merge_times` | prod_write_burst, prod_delete_heavy, prod_medium_a, prod_cache_churn, prod_hot_search | prod_write_burst, prod_get_heavy, prod_medium_c, prod_medium_b, prod_bulk_store | prod_get_heavy, prod_medium_c, prod_medium_b, prod_bulk_store |
| 🟡 `refresh_times` | prod_write_burst, prod_delete_heavy, prod_medium_a, prod_cache_churn, prod_hot_search | prod_write_burst, prod_delete_heavy, prod_get_heavy, prod_medium_c, prod_medium_b | prod_get_heavy, prod_medium_c, prod_medium_b |
| 🟡 `flush_times` | prod_write_burst, prod_delete_heavy, prod_medium_a, prod_cache_churn, prod_hot_search | prod_write_burst, prod_delete_heavy, prod_get_heavy, prod_medium_c, prod_medium_b | prod_get_heavy, prod_medium_c, prod_medium_b |
| 🟢 `indexing_latency` | prod_write_burst, prod_delete_heavy, prod_medium_a, prod_cache_churn, prod_hot_search | prod_write_burst, prod_delete_heavy | — |
| 🔴 `query_latency` | prod_write_burst, prod_delete_heavy, prod_medium_a, prod_cache_churn, prod_hot_search | prod_slow_query, prod_bulk_store, prod_cache_churn, prod_scroll_ctx, prod_hot_search | prod_slow_query, prod_bulk_store, prod_scroll_ctx |
| 🔴 `fetch_latency` | prod_write_burst, prod_delete_heavy, prod_medium_a, prod_cache_churn, prod_hot_search | prod_scroll_ctx, prod_hot_search, prod_medium_b, prod_medium_c, prod_medium_a | prod_scroll_ctx, prod_medium_b, prod_medium_c |
| 🔴 `scroll_latency` | prod_write_burst, prod_delete_heavy, prod_medium_a, prod_cache_churn, prod_hot_search | prod_scroll_ctx, prod_delete_heavy | prod_scroll_ctx |
| 🔴 `segment_terms_memory` | prod_write_burst, prod_delete_heavy, prod_medium_a, prod_cache_churn, prod_hot_search | prod_get_heavy, prod_medium_c, prod_medium_b, prod_bulk_store, prod_hot_search | prod_get_heavy, prod_medium_c, prod_medium_b, prod_bulk_store |
| 🔴 `segment_points_memory` | prod_write_burst, prod_delete_heavy, prod_medium_a, prod_cache_churn, prod_hot_search | prod_get_heavy, prod_medium_c, prod_medium_b, prod_bulk_store, prod_hot_search | prod_get_heavy, prod_medium_c, prod_medium_b, prod_bulk_store |
| 🔴 `query_cache` | prod_write_burst, prod_delete_heavy, prod_medium_a, prod_cache_churn, prod_hot_search | prod_slow_query, prod_cache_churn, prod_delete_heavy, prod_get_heavy, prod_medium_c | prod_slow_query, prod_get_heavy, prod_medium_c |
| 🔴 `fielddata_cache` | prod_write_burst, prod_delete_heavy, prod_medium_a, prod_cache_churn, prod_hot_search | prod_slow_query, prod_get_heavy, prod_medium_c, prod_medium_b, prod_bulk_store | prod_slow_query, prod_get_heavy, prod_medium_c, prod_medium_b, prod_bulk_store |
| 🟡 `query_cache_count` | prod_write_burst, prod_delete_heavy, prod_medium_a, prod_cache_churn, prod_hot_search | prod_cache_churn, prod_slow_query, prod_delete_heavy, prod_get_heavy, prod_medium_c | prod_slow_query, prod_get_heavy, prod_medium_c |
| 🟡 `query_cache_hit` | prod_write_burst, prod_delete_heavy, prod_medium_a, prod_cache_churn, prod_hot_search | prod_cache_churn, prod_slow_query, prod_delete_heavy, prod_get_heavy, prod_medium_c | prod_slow_query, prod_get_heavy, prod_medium_c |
| 🟡 `query_cache_miss` | prod_write_burst, prod_delete_heavy, prod_medium_a, prod_cache_churn, prod_hot_search | prod_cache_churn, prod_get_heavy, prod_medium_c, prod_medium_b, prod_bulk_store | prod_get_heavy, prod_medium_c, prod_medium_b, prod_bulk_store |
