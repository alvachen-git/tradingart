# 索引候选清单（Phase 1，按分区小步上线）

目标：只覆盖最近优化的 4 个页面（ETF期权、商品期权、商品持仓、相关分析）对应的高频查询。  
原则：先 `EXPLAIN`，再加索引；每次只上 1-2 张表，观察 `PERF_PAGE db_ms` 再继续。

## 分批建议

1. 批次 A（优先，收益最高）
- `futures_price`: `idx_fp_ts_code_trade_date (ts_code, trade_date)`
- `commodity_iv_history`: `idx_civ_ts_code_trade_date (ts_code, trade_date)`
- `futures_holding`: `idx_fh_ts_code_trade_date_broker (ts_code, trade_date, broker)`

2. 批次 B（ETF/宏观查询路径）
- `etf_iv_history`: `idx_eiv_etf_code_trade_date (etf_code, trade_date)`
- `stock_price`: `idx_sp_ts_code_trade_date (ts_code, trade_date)`
- `index_price`: `idx_ip_ts_code_trade_date (ts_code, trade_date)`

3. 批次 C（持仓看板卡片）
- `market_sentiment`: `idx_ms_trade_date_ts_code (trade_date, ts_code)`
- `market_rank_daily`: `idx_mrd_trade_date_rank_type_score (trade_date, rank_type, score)`
- `foreign_capital_analysis`: `idx_fca_trade_date_symbol (trade_date, symbol)`
- `market_conflict_daily`: `idx_mcd_trade_date_symbol (trade_date, symbol)`

4. 批次 D（ETF期权 join 路径）
- `option_basic`: `idx_ob_underlying_ts_code (underlying, ts_code)`
- `option_daily`: `idx_od_ts_code_trade_date (ts_code, trade_date)`

## 为什么是这些索引

1. 大部分慢查询是 `WHERE ts_code=? AND trade_date>=? ORDER BY trade_date`  
现有主键如果是 `(trade_date, ts_code)`，对这类查询不友好；补 `(ts_code, trade_date)` 通常能明显降扫描行数。

2. 商品期权和 ETF 页都依赖按代码取时间序列  
`commodity_iv_history / futures_price / etf_iv_history / stock_price / index_price` 都是典型时间序列读多场景。

3. 商品持仓页大量按 `trade_date` 取当天卡片数据  
`foreign_capital_analysis / market_conflict_daily / market_rank_daily` 用 `(trade_date, ...)` 组合最稳。

## 风险与取舍

1. 优点
- 查询加速明显，页面切换更流畅。
- 在读多写少场景收益通常大于成本。

2. 代价
- 占用额外磁盘与 buffer pool。
- 写入会略慢（维护索引开销）。

3. 控制方式
- 严格分批上线。
- 每批上线后对比 `PERF_PAGE`（同口径 p50/p95）。
- 命中不明显就停止继续加索引，避免“索引过量”。

## 上线前必做检查

1. `SHOW INDEX FROM <table>` 确认不是重复索引。  
2. `EXPLAIN` 对比加索引前后 `type/key/rows/Extra`。  
3. 避免把函数包在索引列上（如 `REPLACE(trade_date, '-', '')`），这会让索引失效。

