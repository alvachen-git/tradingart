import argparse
import json
from pathlib import Path
import sys
from typing import List


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ai_simulation_service import (  # noqa: E402
    OFFICIAL_PORTFOLIO_2_ID,
    OFFICIAL_PORTFOLIO_ID,
    run_daily_simulation,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Rerun official AI simulation portfolios.")
    parser.add_argument("--trade-date", required=True, help="Trade date, for example 20260529.")
    parser.add_argument(
        "--portfolio-id",
        action="append",
        default=None,
        help="Portfolio id to rerun. Can be passed more than once. Defaults to official v1 and v2.",
    )
    parser.add_argument("--no-force", action="store_true", help="Do not overwrite an existing settled day.")
    args = parser.parse_args()

    portfolio_ids: List[str] = args.portfolio_id or [OFFICIAL_PORTFOLIO_ID, OFFICIAL_PORTFOLIO_2_ID]
    results = []
    for portfolio_id in portfolio_ids:
        try:
            result = run_daily_simulation(
                trade_date=args.trade_date,
                portfolio_id=portfolio_id,
                force=not args.no_force,
            )
        except Exception as exc:
            result = {"status": "error", "portfolio_id": portfolio_id, "error": str(exc)}
        results.append({"portfolio_id": portfolio_id, "result": result})

    print(json.dumps({"trade_date": args.trade_date, "results": results}, ensure_ascii=False, indent=2))
    return 0 if all(item["result"].get("status") in {"success", "skipped"} for item in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
