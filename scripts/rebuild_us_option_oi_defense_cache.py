#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from us_market_dashboard_data import (  # noqa: E402
    DEFAULT_DASHBOARD_UNDERLYINGS,
    OI_DEFENSE_CACHE_TABLE,
    OI_DEFENSE_COLUMNS,
    load_latest_option_trade_date,
    load_oi_defense_history,
)
from us_options_polygon import get_db_engine  # noqa: E402


CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {OI_DEFENSE_CACHE_TABLE} (
    trade_date VARCHAR(8) NOT NULL,
    date DATE NULL,
    underlying VARCHAR(32) NOT NULL,
    underlying_close DOUBLE NULL,
    call_strike DOUBLE NULL,
    call_oi DOUBLE NULL,
    call_distance_pct DOUBLE NULL,
    call_expiration DATE NULL,
    put_strike DOUBLE NULL,
    put_oi DOUBLE NULL,
    put_distance_pct DOUBLE NULL,
    put_expiration DATE NULL,
    total_call_oi DOUBLE NULL,
    total_put_oi DOUBLE NULL,
    put_call_oi DOUBLE NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date, underlying),
    KEY idx_underlying_date (underlying, trade_date)
)
"""


def _load_env() -> None:
    current = Path(__file__).resolve()
    for parent in [current.parent, *current.parents]:
        env_path = parent / ".env"
        if env_path.exists():
            load_dotenv(env_path, override=True)
            return
    load_dotenv(override=True)


def _parse_underlyings(value: str | None) -> list[str]:
    if not value:
        return list(DEFAULT_DASHBOARD_UNDERLYINGS)
    return [item.strip().upper() for item in value.split(",") if item.strip()]


def _clean_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if hasattr(value, "isoformat") and not isinstance(value, str):
        return value.isoformat()
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _rows_from_frame(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in df.to_dict(orient="records"):
        rows.append({col: _clean_value(row.get(col)) for col in OI_DEFENSE_COLUMNS})
    return rows


def _upsert_rows(conn, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    columns = OI_DEFENSE_COLUMNS
    column_sql = ", ".join(f"`{col}`" for col in columns)
    value_sql = ", ".join(f":{col}" for col in columns)
    update_sql = ", ".join(
        f"`{col}` = VALUES(`{col}`)" for col in columns if col not in {"trade_date", "underlying"}
    )
    conn.execute(
        text(
            f"""
            INSERT INTO {OI_DEFENSE_CACHE_TABLE} ({column_sql})
            VALUES ({value_sql})
            ON DUPLICATE KEY UPDATE {update_sql}
            """
        ),
        rows,
    )


def rebuild_cache(*, underlyings: list[str], window: int, apply: bool) -> list[dict[str, Any]]:
    _load_env()
    engine = get_db_engine()
    if engine is None:
        raise RuntimeError("数据库环境变量未配置，请检查 DB_USER、DB_PASSWORD、DB_HOST、DB_PORT、DB_NAME")

    summaries: list[dict[str, Any]] = []
    if apply:
        with engine.begin() as conn:
            conn.execute(text(CREATE_TABLE_SQL))

    for underlying in underlyings:
        latest = load_latest_option_trade_date(underlying, use_test_tables=False, engine=engine)
        if not latest:
            summaries.append({"underlying": underlying, "latest": None, "rows": 0, "status": "missing_date"})
            continue
        if not apply:
            summaries.append({"underlying": underlying, "latest": latest, "rows": 0, "status": "dry_run"})
            continue
        df = load_oi_defense_history(
            underlying,
            latest,
            window=window,
            use_test_tables=False,
            prefer_cache=False,
            engine=engine,
        )
        rows = _rows_from_frame(df)
        with engine.begin() as conn:
            _upsert_rows(conn, rows)
        summaries.append({"underlying": underlying, "latest": latest, "rows": len(rows), "status": "updated"})
    return summaries


def main() -> int:
    parser = argparse.ArgumentParser(description="Rebuild preaggregated OI defense rows for the US options dashboard.")
    parser.add_argument("--underlyings", help="Comma-separated underlyings. Default: dashboard underlyings.")
    parser.add_argument("--window", type=int, default=20, help="Trading days to cache per underlying.")
    parser.add_argument("--apply", action="store_true", help="Actually create/update cache rows. Default is dry-run.")
    args = parser.parse_args()

    summaries = rebuild_cache(
        underlyings=_parse_underlyings(args.underlyings),
        window=max(1, min(int(args.window or 20), 260)),
        apply=bool(args.apply),
    )
    print(json.dumps(summaries, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
