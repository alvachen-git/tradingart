from unified_stock_view import UNIFIED_STOCK_VIEW_NAME, build_unified_stock_view_sql


def test_unified_stock_view_sql_contains_required_fields():
    sql = build_unified_stock_view_sql().upper()
    assert f"CREATE OR REPLACE VIEW {UNIFIED_STOCK_VIEW_NAME.upper()} AS" in sql
    for field in [
        "TS_CODE",
        "NAME",
        "TRADE_DATE",
        "OPEN_PRICE",
        "HIGH_PRICE",
        "LOW_PRICE",
        "CLOSE_PRICE",
        "VOLUME",
        "PCT_CHG",
        "MARKET",
    ]:
        assert field in sql


def test_unified_stock_view_sql_includes_us_pct_chg_window_logic():
    sql = build_unified_stock_view_sql().upper()
    assert "LAG(CLOSE) OVER (PARTITION BY UPPER(SYMBOL) ORDER BY DATE)" in sql
    assert "ROUND((US.CLOSE_PRICE - US.PREV_CLOSE) / US.PREV_CLOSE * 100, 4)" in sql

