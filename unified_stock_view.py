from __future__ import annotations

from sqlalchemy import text

UNIFIED_STOCK_VIEW_NAME = "v_stock_price_unified"


def _pick_column(columns: set[str], candidates: list[str]) -> str | None:
    for c in candidates:
        if c in columns:
            return c
    return None


def _normalized_trade_date_expr(expr: str) -> str:
    return f"LEFT(REPLACE(REPLACE(CAST({expr} AS CHAR), '-', ''), '/', ''), 8)"


def _build_unified_stock_view_sql(stock_price_cols: set[str], stock_prices_cols: set[str]) -> str:
    branches: list[str] = []

    # CN/HK branch (stock_price)
    if stock_price_cols:
        ts_code_col = _pick_column(stock_price_cols, ["ts_code", "symbol", "code"])
        name_col = _pick_column(stock_price_cols, ["name", "stock_name", "symbol"])
        trade_date_col = _pick_column(stock_price_cols, ["trade_date", "date"])
        open_col = _pick_column(stock_price_cols, ["open_price", "open"])
        high_col = _pick_column(stock_price_cols, ["high_price", "high"])
        low_col = _pick_column(stock_price_cols, ["low_price", "low"])
        close_col = _pick_column(stock_price_cols, ["close_price", "close"])
        volume_col = _pick_column(stock_price_cols, ["volume", "vol"])
        pct_chg_col = _pick_column(stock_price_cols, ["pct_chg", "pct_change", "change_pct", "p_change"])

        if ts_code_col and trade_date_col and close_col:
            ts_expr = f"sp.{ts_code_col}"
            close_expr = f"sp.{close_col}"
            open_expr = f"sp.{open_col}" if open_col else close_expr
            high_expr = f"sp.{high_col}" if high_col else close_expr
            low_expr = f"sp.{low_col}" if low_col else close_expr
            volume_expr = f"sp.{volume_col}" if volume_col else "NULL"
            pct_expr = f"sp.{pct_chg_col}" if pct_chg_col else "NULL"
            name_expr = f"sp.{name_col}" if name_col else f"UPPER({ts_expr})"

            branches.append(
                f"""
SELECT
    UPPER({ts_expr}) AS ts_code,
    COALESCE({name_expr}, UPPER({ts_expr})) AS name,
    {_normalized_trade_date_expr(f"sp.{trade_date_col}")} AS trade_date,
    {open_expr} AS open_price,
    {high_expr} AS high_price,
    {low_expr} AS low_price,
    {close_expr} AS close_price,
    {volume_expr} AS volume,
    {pct_expr} AS pct_chg,
    CASE
        WHEN UPPER({ts_expr}) LIKE '%.HK' THEN 'HK'
        WHEN UPPER({ts_expr}) LIKE '%.US' THEN 'US'
        ELSE 'CN'
    END AS market
FROM stock_price sp
""".strip()
            )

    # US branch (stock_prices)
    if stock_prices_cols:
        symbol_col = _pick_column(stock_prices_cols, ["symbol", "ts_code", "code"])
        trade_date_col = _pick_column(stock_prices_cols, ["date", "trade_date"])
        open_col = _pick_column(stock_prices_cols, ["open", "open_price"])
        high_col = _pick_column(stock_prices_cols, ["high", "high_price"])
        low_col = _pick_column(stock_prices_cols, ["low", "low_price"])
        close_col = _pick_column(stock_prices_cols, ["close", "close_price"])
        volume_col = _pick_column(stock_prices_cols, ["volume", "vol"])

        if symbol_col and trade_date_col and close_col:
            close_expr = close_col
            open_expr = open_col or close_col
            high_expr = high_col or close_col
            low_expr = low_col or close_col
            volume_expr = volume_col if volume_col else "NULL"

            branches.append(
                f"""
SELECT
    CONCAT(us.symbol, '.US') AS ts_code,
    us.symbol AS name,
    DATE_FORMAT(us.trade_date, '%Y%m%d') AS trade_date,
    us.open_price AS open_price,
    us.high_price AS high_price,
    us.low_price AS low_price,
    us.close_price AS close_price,
    us.volume AS volume,
    CASE
        WHEN us.prev_close IS NULL OR us.prev_close = 0 THEN NULL
        ELSE ROUND((us.close_price - us.prev_close) / us.prev_close * 100, 4)
    END AS pct_chg,
    'US' AS market
FROM (
    SELECT
        UPPER({symbol_col}) AS symbol,
        {trade_date_col} AS trade_date,
        {open_expr} AS open_price,
        {high_expr} AS high_price,
        {low_expr} AS low_price,
        {close_expr} AS close_price,
        {volume_expr} AS volume,
        LAG({close_expr}) OVER (PARTITION BY UPPER({symbol_col}) ORDER BY {trade_date_col}) AS prev_close
    FROM stock_prices
) us
""".strip()
            )

    if not branches:
        raise ValueError("No compatible source columns found for unified stock view.")

    return f"CREATE OR REPLACE VIEW {UNIFIED_STOCK_VIEW_NAME} AS\n" + "\nUNION ALL\n".join(branches)


def build_unified_stock_view_sql() -> str:
    """Build canonical SQL for the unified stock daily view."""
    canonical_stock_price_cols = {
        "ts_code",
        "name",
        "trade_date",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "pct_chg",
    }
    canonical_stock_prices_cols = {"symbol", "date", "open", "high", "low", "close", "volume"}
    return _build_unified_stock_view_sql(canonical_stock_price_cols, canonical_stock_prices_cols)


def _table_exists(conn, table_name: str) -> bool:
    sql = text(
        """
        SELECT COUNT(*) AS cnt
        FROM information_schema.tables
        WHERE table_schema = DATABASE() AND table_name = :table_name
        """
    )
    row = conn.execute(sql, {"table_name": table_name}).fetchone()
    return bool(row and int(row[0]) > 0)


def _view_exists(conn, view_name: str) -> bool:
    sql = text(
        """
        SELECT COUNT(*) AS cnt
        FROM information_schema.views
        WHERE table_schema = DATABASE() AND table_name = :view_name
        """
    )
    row = conn.execute(sql, {"view_name": view_name}).fetchone()
    return bool(row and int(row[0]) > 0)


def _table_columns(conn, table_name: str) -> set[str]:
    sql = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = :table_name
        """
    )
    rows = conn.execute(sql, {"table_name": table_name}).fetchall()
    return {str(r[0]).lower() for r in rows}


def ensure_unified_stock_view(engine) -> bool:
    """
    Create/refresh the unified view when both source tables are available.
    Returns True when the view exists at the end of the call.
    """
    if engine is None:
        return False

    try:
        with engine.begin() as conn:
            has_stock_price = _table_exists(conn, "stock_price")
            has_stock_prices = _table_exists(conn, "stock_prices")

            # If source tables are not ready, keep current state.
            if not has_stock_price and not has_stock_prices:
                return _view_exists(conn, UNIFIED_STOCK_VIEW_NAME)

            stock_price_cols = _table_columns(conn, "stock_price") if has_stock_price else set()
            stock_prices_cols = _table_columns(conn, "stock_prices") if has_stock_prices else set()
            sql = _build_unified_stock_view_sql(stock_price_cols, stock_prices_cols)
            conn.execute(text(sql))
            return True
    except Exception:
        return False


def get_stock_price_source(engine) -> str:
    """
    Return the best available stock-daily read source.
    Prefer unified view, then fallback to stock_price.
    """
    if engine is None:
        return "stock_price"

    try:
        with engine.connect() as conn:
            if _view_exists(conn, UNIFIED_STOCK_VIEW_NAME):
                return UNIFIED_STOCK_VIEW_NAME
    except Exception:
        return "stock_price"

    if ensure_unified_stock_view(engine):
        return UNIFIED_STOCK_VIEW_NAME
    return "stock_price"
