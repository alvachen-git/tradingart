from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any, Sequence

from sqlalchemy import text

from us_options_polygon import (
    MassiveOptionsClient,
    compact_date,
    default_trade_date,
    get_db_engine,
    live_update,
    parse_underlyings,
    table_names,
)


DEFAULT_UNDERLYINGS = ("SPY", "QQQ", "IWM")
REQUIRED_METRIC_FIELDS = ("put_call_oi", "total_open_interest", "atm_iv_pct")


def _int_value(value: Any) -> int:
    if value is None:
        return 0
    try:
        return int(value)
    except Exception:
        return 0


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _fetch_one(engine, sql: str, params: dict[str, Any]) -> dict[str, Any]:
    with engine.connect() as conn:
        row = conn.execute(text(sql), params).mappings().first()
    return dict(row or {})


def _db_health_for_underlying(
    engine,
    underlying: str,
    trade_date: str,
    use_test_tables: bool = False,
) -> dict[str, Any]:
    names = table_names(use_test_tables)
    daily = names["daily"]
    iv = names["iv"]
    metrics = names["metrics"]
    params = {"underlying": underlying.upper(), "trade_date": compact_date(trade_date)}

    daily_row = _fetch_one(
        engine,
        f"""
        SELECT COUNT(*) AS daily_rows,
               SUM(CASE WHEN open_interest IS NOT NULL THEN 1 ELSE 0 END) AS open_interest_rows
        FROM {daily}
        WHERE underlying = :underlying AND trade_date = :trade_date
        """,
        params,
    )
    iv_row = _fetch_one(
        engine,
        f"""
        SELECT COUNT(*) AS iv_rows,
               SUM(CASE WHEN provider_iv IS NOT NULL THEN 1 ELSE 0 END) AS provider_iv_rows,
               SUM(CASE WHEN open_interest IS NOT NULL THEN 1 ELSE 0 END) AS iv_open_interest_rows
        FROM {iv}
        WHERE underlying = :underlying AND trade_date = :trade_date
        """,
        params,
    )
    metrics_row = _fetch_one(
        engine,
        f"""
        SELECT COUNT(*) AS metrics_rows,
               MAX(put_call_oi) AS put_call_oi,
               MAX(total_open_interest) AS total_open_interest,
               MAX(atm_iv_pct) AS atm_iv_pct,
               MAX(provider_iv_rows) AS metrics_provider_iv_rows,
               MAX(open_interest_rows) AS metrics_open_interest_rows
        FROM {metrics}
        WHERE underlying = :underlying AND trade_date = :trade_date
        """,
        params,
    )
    return {
        "underlying": underlying.upper(),
        "trade_date": compact_date(trade_date),
        "daily_rows": _int_value(daily_row.get("daily_rows")),
        "open_interest_rows": _int_value(daily_row.get("open_interest_rows")),
        "iv_rows": _int_value(iv_row.get("iv_rows")),
        "provider_iv_rows": _int_value(iv_row.get("provider_iv_rows")),
        "iv_open_interest_rows": _int_value(iv_row.get("iv_open_interest_rows")),
        "metrics_rows": _int_value(metrics_row.get("metrics_rows")),
        "put_call_oi": _float_or_none(metrics_row.get("put_call_oi")),
        "total_open_interest": _float_or_none(metrics_row.get("total_open_interest")),
        "atm_iv_pct": _float_or_none(metrics_row.get("atm_iv_pct")),
        "metrics_provider_iv_rows": _int_value(metrics_row.get("metrics_provider_iv_rows")),
        "metrics_open_interest_rows": _int_value(metrics_row.get("metrics_open_interest_rows")),
    }


def _dry_run_health_for_underlying(result: dict[str, Any], underlying: str, trade_date: str) -> dict[str, Any]:
    item = (result.get("per_underlying") or {}).get(underlying.upper()) or {}
    return {
        "underlying": underlying.upper(),
        "trade_date": compact_date(trade_date),
        "daily_rows": _int_value(item.get("daily")),
        "open_interest_rows": _int_value(item.get("open_interest_rows")),
        "iv_rows": _int_value(item.get("iv")),
        "provider_iv_rows": _int_value(item.get("provider_iv_rows")),
        "iv_open_interest_rows": _int_value(item.get("open_interest_rows")),
        "metrics_rows": 0,
        "put_call_oi": None,
        "total_open_interest": None,
        "atm_iv_pct": None,
        "metrics_provider_iv_rows": 0,
        "metrics_open_interest_rows": 0,
    }


def _issues_for_item(item: dict[str, Any], dry_run: bool = False) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    underlying = str(item.get("underlying") or "")
    if _int_value(item.get("daily_rows")) <= 0:
        issues.append({"underlying": underlying, "severity": "warning", "code": "no_daily_rows"})
        return issues
    if _int_value(item.get("open_interest_rows")) <= 0:
        issues.append({"underlying": underlying, "severity": "error", "code": "missing_open_interest"})
    if _int_value(item.get("provider_iv_rows")) <= 0:
        issues.append({"underlying": underlying, "severity": "error", "code": "missing_provider_iv"})
    if dry_run:
        return issues
    if _int_value(item.get("metrics_rows")) <= 0:
        issues.append({"underlying": underlying, "severity": "error", "code": "missing_metrics"})
        return issues
    for field in REQUIRED_METRIC_FIELDS:
        if item.get(field) is None:
            issues.append({"underlying": underlying, "severity": "warning", "code": f"missing_metric_{field}"})
    return issues


def build_health_report(
    *,
    engine,
    update_result: dict[str, Any],
    underlyings: Sequence[str],
    trade_date: str,
    use_test_tables: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    checks = []
    issues: list[dict[str, str]] = []
    for underlying in underlyings:
        item = (
            _dry_run_health_for_underlying(update_result, underlying, trade_date)
            if dry_run
            else _db_health_for_underlying(engine, underlying, trade_date, use_test_tables=use_test_tables)
        )
        item_issues = _issues_for_item(item, dry_run=dry_run)
        item["issues"] = item_issues
        checks.append(item)
        issues.extend(item_issues)

    if checks and all(_int_value(item.get("daily_rows")) <= 0 for item in checks):
        status = "no_data_or_market_holiday"
    elif any(issue.get("severity") == "error" for issue in issues):
        status = "health_failed"
    elif issues:
        status = "warning"
    else:
        status = "ok"
    return {
        "status": status,
        "trade_date": compact_date(trade_date),
        "use_test_tables": bool(use_test_tables),
        "dry_run": bool(dry_run),
        "checks": checks,
        "issues": issues,
    }


def run_options_daily(
    *,
    date: str | None = None,
    underlyings: Sequence[str] = DEFAULT_UNDERLYINGS,
    short_strike_band_pct: float = 5.0,
    use_test_tables: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    start = time.time()
    trade_date = compact_date(date) or default_trade_date()
    target_underlyings = [str(item).strip().upper() for item in underlyings if str(item).strip()]
    if not target_underlyings:
        raise RuntimeError("No underlyings configured for US options daily update.")

    engine = None if dry_run else get_db_engine()
    if engine is None and not dry_run:
        raise RuntimeError("Database env is incomplete. Set DB_USER/DB_PASSWORD/DB_HOST/DB_NAME.")

    client = MassiveOptionsClient()
    update_result = live_update(
        engine,
        client,
        target_underlyings,
        trade_date,
        short_strike_band_pct=short_strike_band_pct,
        use_test_tables=use_test_tables,
        dry_run=dry_run,
    )
    health = build_health_report(
        engine=engine,
        update_result=update_result,
        underlyings=target_underlyings,
        trade_date=trade_date,
        use_test_tables=use_test_tables,
        dry_run=dry_run,
    )
    elapsed_seconds = round(time.time() - start, 3)
    return {
        "status": health["status"],
        "trade_date": trade_date,
        "underlyings": target_underlyings,
        "source": "massive_snapshot",
        "elapsed_seconds": elapsed_seconds,
        "update": update_result,
        "health": health,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the daily US options snapshot update and health check.")
    parser.add_argument("--date", help="Trade date to update, YYYYMMDD. Defaults to previous/settled US trading day.")
    parser.add_argument("--underlyings", default=",".join(DEFAULT_UNDERLYINGS))
    parser.add_argument("--short-strike-band-pct", type=float, default=5.0)
    parser.add_argument("--use-test-tables", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="Request Massive snapshot data without writing database rows.")
    parser.add_argument("--ignore-health-failures", action="store_true", help="Always exit 0 after printing JSON.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    try:
        result = run_options_daily(
            date=args.date,
            underlyings=parse_underlyings(args.underlyings),
            short_strike_band_pct=args.short_strike_band_pct,
            use_test_tables=args.use_test_tables,
            dry_run=args.dry_run,
        )
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "error",
                    "error_type": exc.__class__.__name__,
                    "error": str(exc),
                },
                ensure_ascii=False,
                indent=2,
                default=str,
            )
        )
        return 1

    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    if args.ignore_health_failures:
        return 0
    return 2 if result.get("status") == "health_failed" else 0


if __name__ == "__main__":
    sys.exit(main())
