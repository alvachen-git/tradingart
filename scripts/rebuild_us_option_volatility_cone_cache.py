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
    VOLATILITY_CONE_DAILY_CACHE_COLUMNS,
    VOLATILITY_CONE_DAILY_CACHE_TABLE,
    load_available_option_trade_dates,
    load_volatility_cone_line_snapshot,
)
from us_options_polygon import get_db_engine  # noqa: E402


CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {VOLATILITY_CONE_DAILY_CACHE_TABLE} (
    trade_date VARCHAR(8) NOT NULL,
    underlying VARCHAR(32) NOT NULL,
    dte_target INT NOT NULL,
    dte DOUBLE NULL,
    expiration_date DATE NULL,
    iv_pct DOUBLE NULL,
    sample_count INT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (underlying, trade_date, dte_target),
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


def _rows_from_frame(underlying: str, trade_date: str, df: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if df is None or df.empty:
        return rows
    for row in df.to_dict(orient="records"):
        item = {col: _clean_value(row.get(col)) for col in VOLATILITY_CONE_DAILY_CACHE_COLUMNS}
        item["trade_date"] = str(trade_date)
        item["underlying"] = str(underlying).upper()
        rows.append(item)
    return rows


def _upsert_rows(conn, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    columns = VOLATILITY_CONE_DAILY_CACHE_COLUMNS
    column_sql = ", ".join(f"`{col}`" for col in columns)
    value_sql = ", ".join(f":{col}" for col in columns)
    update_sql = ", ".join(
        f"`{col}` = VALUES(`{col}`)" for col in columns if col not in {"trade_date", "underlying", "dte_target"}
    )
    conn.execute(
        text(
            f"""
            INSERT INTO {VOLATILITY_CONE_DAILY_CACHE_TABLE} ({column_sql})
            VALUES ({value_sql})
            ON DUPLICATE KEY UPDATE {update_sql}
            """
        ),
        rows,
    )


def _select_dates(dates: list[str], *, start: str | None, end: str | None, window: int) -> list[str]:
    selected = [date for date in dates if date]
    if end:
        selected = [date for date in selected if date <= end]
    if start:
        selected = [date for date in selected if date >= start]
    selected = sorted(set(selected))
    if not start and window > 0:
        selected = selected[-window:]
    return selected


def rebuild_cache(
    *,
    underlyings: list[str],
    window: int,
    start: str | None,
    end: str | None,
    apply: bool,
) -> list[dict[str, Any]]:
    _load_env()
    engine = get_db_engine()
    if engine is None:
        raise RuntimeError("数据库环境变量未配置，请检查 DB_USER、DB_PASSWORD、DB_HOST、DB_PORT、DB_NAME")

    summaries: list[dict[str, Any]] = []
    if apply:
        with engine.begin() as conn:
            conn.execute(text(CREATE_TABLE_SQL))

    date_limit = 5000 if start else max(window * 3, window, 30)
    for underlying in underlyings:
        all_dates = load_available_option_trade_dates(
            underlying,
            use_test_tables=False,
            limit=min(max(date_limit, 1), 5000),
            engine=engine,
        )
        selected_dates = _select_dates(all_dates, start=start, end=end, window=window)
        if not apply:
            summaries.append(
                {
                    "underlying": underlying,
                    "dates": len(selected_dates),
                    "start": selected_dates[0] if selected_dates else None,
                    "end": selected_dates[-1] if selected_dates else None,
                    "rows": 0,
                    "status": "dry_run",
                }
            )
            continue

        total_rows = 0
        for trade_date in selected_dates:
            frame = load_volatility_cone_line_snapshot(
                underlying,
                trade_date,
                use_test_tables=False,
                engine=engine,
            )
            rows = _rows_from_frame(underlying, trade_date, frame)
            total_rows += len(rows)
            with engine.begin() as conn:
                _upsert_rows(conn, rows)
        summaries.append(
            {
                "underlying": underlying,
                "dates": len(selected_dates),
                "start": selected_dates[0] if selected_dates else None,
                "end": selected_dates[-1] if selected_dates else None,
                "rows": total_rows,
                "status": "updated",
            }
        )
    return summaries


def main() -> int:
    parser = argparse.ArgumentParser(description="Rebuild volatility cone daily cache for the US options dashboard.")
    parser.add_argument("--underlyings", help="Comma-separated underlyings. Default: dashboard underlyings.")
    parser.add_argument("--window", type=int, default=252, help="Trading days to cache when --start is omitted.")
    parser.add_argument("--start", help="Start trade_date YYYYMMDD. When set, ignores the rolling-window lower bound.")
    parser.add_argument("--end", help="End trade_date YYYYMMDD.")
    parser.add_argument("--apply", action="store_true", help="Actually create/update cache rows. Default is dry-run.")
    args = parser.parse_args()

    summaries = rebuild_cache(
        underlyings=_parse_underlyings(args.underlyings),
        window=max(1, min(int(args.window or 252), 1000)),
        start=args.start,
        end=args.end,
        apply=bool(args.apply),
    )
    print(json.dumps(summaries, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
