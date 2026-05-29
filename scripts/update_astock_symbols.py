import argparse
from datetime import datetime
from pathlib import Path
import sys
import time
from typing import List

import pytz


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from update_astock_daily import fetch_and_save_data  # noqa: E402


ETF_PREFIXES = ("159", "510", "511", "512", "513", "515", "516", "518", "588")


def compact_date(value: str) -> str:
    out = "".join(ch for ch in str(value or "") if ch.isdigit())[:8]
    if len(out) != 8:
        raise argparse.ArgumentTypeError("date must look like 20260529")
    return out


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


def asset_type_for(symbol: str) -> str:
    code = normalize_symbol(symbol).split(".")[0]
    return "E" if code.startswith(ETF_PREFIXES) else "S"


def main() -> int:
    today = datetime.now(pytz.timezone("Asia/Shanghai")).strftime("%Y%m%d")
    parser = argparse.ArgumentParser(description="Update selected A-share or ETF symbols into stock_price.")
    parser.add_argument("symbols", nargs="+", help="Symbols such as 510300.SH 002371.SZ")
    parser.add_argument("--start-date", type=compact_date, default=today)
    parser.add_argument("--end-date", type=compact_date, default=today)
    parser.add_argument("--sleep", type=float, default=0.3)
    args = parser.parse_args()

    symbols: List[str] = []
    for symbol in args.symbols:
        normalized = normalize_symbol(symbol)
        if normalized and normalized not in symbols:
            symbols.append(normalized)

    print(f"Updating {len(symbols)} symbols from {args.start_date} to {args.end_date}")
    for symbol in symbols:
        kind = asset_type_for(symbol)
        print(f"==> {symbol} asset_type={kind}")
        fetch_and_save_data(symbol, args.start_date, args.end_date, asset_type=kind)
        time.sleep(max(0.0, args.sleep))
    print("Done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
