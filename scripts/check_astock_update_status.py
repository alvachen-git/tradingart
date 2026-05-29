import argparse
import ast
import json
import os
from datetime import datetime
from pathlib import Path
import sys
from typing import Any, Dict, Iterable, List, Sequence

import pytz
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


ROOT = Path(__file__).resolve().parents[1]


ETF_PREFIXES = ("159", "510", "511", "512", "513", "515", "516", "518", "588")
PORTFOLIO_IDS = ("official_cn_a_etf_v1", "official_cn_a_etf_v2")


def compact_date(value: Any) -> str:
    text_value = "".join(ch for ch in str(value or "") if ch.isdigit())[:8]
    return text_value if len(text_value) == 8 else ""


def normalize_symbol(symbol: str) -> str:
    raw = str(symbol or "").strip().upper().replace(" ", "")
    if not raw:
        return ""
    if "." in raw:
        return raw.replace(".XSHG", ".SH").replace(".XSHE", ".SZ")
    if raw.startswith(("6", "5", "9")):
        return f"{raw}.SH"
    if raw.startswith(("0", "1", "2", "3")):
        return f"{raw}.SZ"
    return raw


def is_etf_symbol(symbol: str) -> bool:
    code = normalize_symbol(symbol).split(".")[0]
    return code.startswith(ETF_PREFIXES)


def load_db_engine():
    load_dotenv(ROOT / ".env", override=False)
    load_dotenv(ROOT.parent / ".env", override=False)
    load_dotenv(override=False)
    required = ["DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_NAME"]
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError(f"Missing DB env: {', '.join(missing)}")
    url = (
        f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    return create_engine(url, pool_pre_ping=True)


def fetch_mappings(conn, sql: str, params: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
    return [dict(row) for row in conn.execute(text(sql), params or {}).mappings().fetchall()]


def parse_literal_targets(script_path: Path, names: Sequence[str]) -> Dict[str, List[str]]:
    try:
        tree = ast.parse(script_path.read_text(encoding="utf-8"))
    except Exception:
        return {name: [] for name in names}

    targets: Dict[str, List[str]] = {name: [] for name in names}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        assigned = [target.id for target in node.targets if isinstance(target, ast.Name)]
        for name in names:
            if name not in assigned:
                continue
            try:
                value = ast.literal_eval(node.value)
            except Exception:
                continue
            if isinstance(value, list):
                targets[name] = [normalize_symbol(str(item)) for item in value if str(item).strip()]
    return targets


def symbol_price_status(conn, symbols: Iterable[str], trade_date: str) -> List[Dict[str, Any]]:
    clean = sorted({normalize_symbol(symbol) for symbol in symbols if normalize_symbol(symbol)})
    if not clean:
        return []
    params: Dict[str, Any] = {"td": trade_date}
    placeholders = []
    for idx, symbol in enumerate(clean):
        key = f"s{idx}"
        params[key] = symbol
        placeholders.append(f":{key}")

    rows = fetch_mappings(
        conn,
        f"""
        SELECT ts_code, trade_date, close_price
        FROM stock_price
        WHERE ts_code IN ({",".join(placeholders)})
          AND trade_date = :td
        """,
        params,
    )
    today_map = {normalize_symbol(row.get("ts_code")): row for row in rows}

    latest_rows = fetch_mappings(
        conn,
        f"""
        SELECT s.ts_code, s.trade_date, s.close_price
        FROM stock_price s
        INNER JOIN (
            SELECT ts_code, MAX(trade_date) AS max_td
            FROM stock_price
            WHERE ts_code IN ({",".join(placeholders)})
              AND trade_date <= :td
            GROUP BY ts_code
        ) t ON t.ts_code = s.ts_code AND t.max_td = s.trade_date
        """,
        params,
    )
    latest_map = {normalize_symbol(row.get("ts_code")): row for row in latest_rows}

    out = []
    for symbol in clean:
        today_row = today_map.get(symbol)
        latest_row = latest_map.get(symbol)
        out.append(
            {
                "symbol": symbol,
                "kind": "ETF" if is_etf_symbol(symbol) else "STOCK",
                "has_target_date": bool(today_row and float(today_row.get("close_price") or 0) > 0),
                "target_close": float(today_row.get("close_price") or 0) if today_row else 0.0,
                "latest_price_date": str(latest_row.get("trade_date") or "") if latest_row else "",
                "latest_close": float(latest_row.get("close_price") or 0) if latest_row else 0.0,
            }
        )
    return out


def portfolio_position_symbols(conn, portfolio_id: str, trade_date: str) -> Dict[str, Any]:
    prev_td = conn.execute(
        text(
            """
            SELECT MAX(trade_date)
            FROM ai_sim_positions
            WHERE portfolio_id = :pid AND trade_date < :td
            """
        ),
        {"pid": portfolio_id, "td": trade_date},
    ).scalar()
    exact_td = conn.execute(
        text(
            """
            SELECT MAX(trade_date)
            FROM ai_sim_positions
            WHERE portfolio_id = :pid AND trade_date = :td
            """
        ),
        {"pid": portfolio_id, "td": trade_date},
    ).scalar()
    source_td = str(prev_td or exact_td or "")
    rows = []
    if source_td:
        rows = fetch_mappings(
            conn,
            """
            SELECT symbol, name, quantity, close_price, market_value
            FROM ai_sim_positions
            WHERE portfolio_id = :pid AND trade_date = :td
            ORDER BY market_value DESC
            """,
            {"pid": portfolio_id, "td": source_td},
        )
    return {
        "portfolio_id": portfolio_id,
        "position_source_date": source_td,
        "symbols": [normalize_symbol(row.get("symbol")) for row in rows],
        "positions": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Check A-share and ETF stock_price update coverage.")
    parser.add_argument(
        "--trade-date",
        default=datetime.now(pytz.timezone("Asia/Shanghai")).strftime("%Y%m%d"),
        help="Target trade date, for example 20260529.",
    )
    parser.add_argument("--json", action="store_true", help="Print raw JSON only.")
    args = parser.parse_args()
    trade_date = compact_date(args.trade_date)
    if not trade_date:
        raise SystemExit("Invalid --trade-date")

    engine = load_db_engine()
    targets = parse_literal_targets(ROOT / "update_astock_daily.py", ["ETF_TARGETS", "STOCK_TARGETS"])
    raw_update_targets = set(targets.get("ETF_TARGETS", []) + targets.get("STOCK_TARGETS", []))

    with engine.connect() as conn:
        latest_groups = fetch_mappings(
            conn,
            """
            SELECT trade_date, COUNT(*) AS rows_count
            FROM stock_price
            GROUP BY trade_date
            ORDER BY trade_date DESC
            LIMIT 8
            """,
        )
        target_rows = fetch_mappings(
            conn,
            """
            SELECT ts_code, close_price
            FROM stock_price
            WHERE trade_date = :td
              AND close_price IS NOT NULL
              AND close_price > 0
            """,
            {"td": trade_date},
        )
        symbols_today = [normalize_symbol(row["ts_code"]) for row in target_rows]
        today_stock_count = sum(1 for symbol in symbols_today if not is_etf_symbol(symbol))
        today_etf_count = sum(1 for symbol in symbols_today if is_etf_symbol(symbol))

        portfolio_payloads = []
        all_position_symbols: List[str] = []
        for portfolio_id in PORTFOLIO_IDS:
            payload = portfolio_position_symbols(conn, portfolio_id, trade_date)
            prices = symbol_price_status(conn, payload["symbols"], trade_date)
            missing = [row for row in prices if not row["has_target_date"]]
            payload["price_status"] = prices
            payload["missing_or_stale"] = missing
            portfolio_payloads.append(payload)
            all_position_symbols.extend(payload["symbols"])

        all_position_status = symbol_price_status(conn, all_position_symbols, trade_date)

    output = {
        "trade_date": trade_date,
        "stock_price_latest_dates": latest_groups,
        "target_date_rows": {
            "total": len(symbols_today),
            "stock": today_stock_count,
            "etf": today_etf_count,
        },
        "update_astock_daily_literal_targets": {
            "etf_count": len(targets.get("ETF_TARGETS", [])),
            "stock_count": len(targets.get("STOCK_TARGETS", [])),
        },
        "portfolios": portfolio_payloads,
        "all_position_price_status": all_position_status,
    }

    if args.json:
        print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
        return 0

    print(f"Trade date: {trade_date}")
    print("Recent stock_price dates:")
    for row in latest_groups:
        print(f"  {row.get('trade_date')}: {row.get('rows_count')} rows")
    print(
        "Target date rows: "
        f"total={len(symbols_today)}, stock={today_stock_count}, etf={today_etf_count}"
    )
    print(
        "update_astock_daily literal targets: "
        f"ETF={len(targets.get('ETF_TARGETS', []))}, STOCK={len(targets.get('STOCK_TARGETS', []))}"
    )
    print("")

    for payload in portfolio_payloads:
        print(f"{payload['portfolio_id']} positions from {payload['position_source_date'] or 'N/A'}")
        missing = payload["missing_or_stale"]
        if not missing:
            print("  OK: all position symbols have target-date prices")
            continue
        print(f"  Missing/stale: {len(missing)}")
        for row in missing:
            symbol = row["symbol"]
            in_targets = symbol in raw_update_targets
            print(
                "  - "
                f"{symbol} {row['kind']} latest={row['latest_price_date'] or 'NONE'} "
                f"close={row['latest_close']} in_update_astock_daily={'YES' if in_targets else 'NO'}"
            )

    etf_missing = [row for row in all_position_status if row["kind"] == "ETF" and not row["has_target_date"]]
    stock_missing = [row for row in all_position_status if row["kind"] == "STOCK" and not row["has_target_date"]]
    print("")
    if etf_missing or stock_missing:
        print(f"Summary: stale/missing position prices: ETF={len(etf_missing)}, STOCK={len(stock_missing)}")
        print("Fix: update missing symbols in stock_price first, then rerun AI simulation.")
    else:
        print("Summary: AI position price coverage is complete for the target date.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
