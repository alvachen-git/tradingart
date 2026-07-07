#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from us_market_dashboard_data import (  # noqa: E402
    DEFAULT_DASHBOARD_UNDERLYINGS,
    rebuild_underlying_profile_cache,
)
from us_options_polygon import get_db_engine  # noqa: E402


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


def main() -> int:
    parser = argparse.ArgumentParser(description="Rebuild US option underlying profile daily cache.")
    parser.add_argument("--date", help="Cache as-of date YYYYMMDD. Defaults to today.")
    parser.add_argument("--underlyings", help="Comma-separated underlyings. Default: dashboard underlyings.")
    parser.add_argument("--lookback-days", type=int, default=30)
    parser.add_argument("--use-test-tables", action="store_true")
    parser.add_argument("--apply", action="store_true", help="Actually write cache rows. Default is dry-run.")
    args = parser.parse_args()

    _load_env()
    engine = get_db_engine()
    if engine is None:
        raise RuntimeError("Database env is incomplete. Set DB_USER/DB_PASSWORD/DB_HOST/DB_NAME.")

    underlyings = _parse_underlyings(args.underlyings)
    result = rebuild_underlying_profile_cache(
        underlyings=underlyings,
        as_of_date=args.date,
        lookback_days=max(int(args.lookback_days or 30), 1),
        apply=bool(args.apply),
        use_test_tables=bool(args.use_test_tables),
        engine=engine,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
