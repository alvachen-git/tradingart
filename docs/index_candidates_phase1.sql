-- 索引候选执行模板（Phase 1）
-- 用法：按“批次”逐步执行，每批执行后观察 24h PERF_PAGE 指标再继续。
-- 注意：先执行检查 SQL，再决定是否执行 ALTER TABLE。

-- =====================================================
-- 0) 现状检查：先看已有索引，避免重复
-- =====================================================
SELECT
    TABLE_NAME,
    INDEX_NAME,
    NON_UNIQUE,
    GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS idx_cols
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME IN (
    'futures_price',
    'commodity_iv_history',
    'futures_holding',
    'etf_iv_history',
    'stock_price',
    'index_price',
    'market_sentiment',
    'market_rank_daily',
    'foreign_capital_analysis',
    'market_conflict_daily',
    'option_basic',
    'option_daily'
  )
GROUP BY TABLE_NAME, INDEX_NAME, NON_UNIQUE
ORDER BY TABLE_NAME, INDEX_NAME;

-- =====================================================
-- 1) 批次 A（优先）
-- =====================================================
-- 1.1 futures_price：支撑 WHERE ts_code + 时间范围 + ORDER BY trade_date
ALTER TABLE futures_price
  ADD INDEX idx_fp_ts_code_trade_date (ts_code, trade_date);

-- 1.2 commodity_iv_history：支撑按合约代码取历史 IV
ALTER TABLE commodity_iv_history
  ADD INDEX idx_civ_ts_code_trade_date (ts_code, trade_date);

-- 1.3 futures_holding：支撑按品种聚合经纪商持仓
ALTER TABLE futures_holding
  ADD INDEX idx_fh_ts_code_trade_date_broker (ts_code, trade_date, broker);

-- =====================================================
-- 2) 批次 B（ETF/相关分析）
-- =====================================================
ALTER TABLE etf_iv_history
  ADD INDEX idx_eiv_etf_code_trade_date (etf_code, trade_date);

ALTER TABLE stock_price
  ADD INDEX idx_sp_ts_code_trade_date (ts_code, trade_date);

ALTER TABLE index_price
  ADD INDEX idx_ip_ts_code_trade_date (ts_code, trade_date);

-- =====================================================
-- 3) 批次 C（商品持仓卡片与排行榜）
-- =====================================================
ALTER TABLE market_sentiment
  ADD INDEX idx_ms_trade_date_ts_code (trade_date, ts_code);

ALTER TABLE market_rank_daily
  ADD INDEX idx_mrd_trade_date_rank_type_score (trade_date, rank_type, score);

ALTER TABLE foreign_capital_analysis
  ADD INDEX idx_fca_trade_date_symbol (trade_date, symbol);

ALTER TABLE market_conflict_daily
  ADD INDEX idx_mcd_trade_date_symbol (trade_date, symbol);

-- =====================================================
-- 4) 批次 D（ETF期权 join 路径）
-- =====================================================
ALTER TABLE option_basic
  ADD INDEX idx_ob_underlying_ts_code (underlying, ts_code);

ALTER TABLE option_daily
  ADD INDEX idx_od_ts_code_trade_date (ts_code, trade_date);

-- =====================================================
-- 5) EXPLAIN 验证模板（执行前后各跑一次）
-- =====================================================
-- 商品期权页
EXPLAIN SELECT trade_date, iv, hv, used_contract
FROM commodity_iv_history
WHERE ts_code = 'AU2506'
ORDER BY trade_date;

EXPLAIN SELECT trade_date, open_price, high_price, low_price, close_price
FROM futures_price
WHERE ts_code = 'AU2506'
ORDER BY trade_date;

-- 商品持仓页
EXPLAIN SELECT trade_date, close_price, oi, pct_chg
FROM futures_price
WHERE ts_code = 'au' AND trade_date >= '20250101'
ORDER BY trade_date;

EXPLAIN SELECT trade_date, broker, net_vol
FROM futures_holding
WHERE ts_code = 'au';

-- ETF页
EXPLAIN SELECT trade_date, iv
FROM etf_iv_history
WHERE etf_code = '510050.SH'
ORDER BY trade_date DESC
LIMIT 252;

-- 相关分析页
EXPLAIN SELECT trade_date, close_price
FROM stock_price
WHERE ts_code = '600519.SH' AND trade_date >= '20250101'
ORDER BY trade_date;

EXPLAIN SELECT trade_date, close_price
FROM index_price
WHERE ts_code = '000300.SH' AND trade_date >= '20250101'
ORDER BY trade_date;

-- =====================================================
-- 6) 回退模板（仅在确认索引导致副作用时使用）
-- =====================================================
-- ALTER TABLE futures_price DROP INDEX idx_fp_ts_code_trade_date;
-- ALTER TABLE commodity_iv_history DROP INDEX idx_civ_ts_code_trade_date;
-- ALTER TABLE futures_holding DROP INDEX idx_fh_ts_code_trade_date_broker;
-- ALTER TABLE etf_iv_history DROP INDEX idx_eiv_etf_code_trade_date;
-- ALTER TABLE stock_price DROP INDEX idx_sp_ts_code_trade_date;
-- ALTER TABLE index_price DROP INDEX idx_ip_ts_code_trade_date;
-- ALTER TABLE market_sentiment DROP INDEX idx_ms_trade_date_ts_code;
-- ALTER TABLE market_rank_daily DROP INDEX idx_mrd_trade_date_rank_type_score;
-- ALTER TABLE foreign_capital_analysis DROP INDEX idx_fca_trade_date_symbol;
-- ALTER TABLE market_conflict_daily DROP INDEX idx_mcd_trade_date_symbol;
-- ALTER TABLE option_basic DROP INDEX idx_ob_underlying_ts_code;
-- ALTER TABLE option_daily DROP INDEX idx_od_ts_code_trade_date;

