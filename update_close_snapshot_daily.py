import argparse
import os
import re
import sys
import time
from datetime import datetime

import pandas as pd
import requests
import tushare as ts
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(override=True)

DEFAULT_ETF_SYMBOLS = [
    "510050.SH",
    "510300.SH",
    "510500.SH",
    "588000.SH",
    "159915.SZ",
]

ETF_NAME_MAP = {
    "510050.SH": "上证50ETF",
    "510300.SH": "沪深300ETF",
    "510500.SH": "中证500ETF",
    "588000.SH": "科创50ETF",
    "159915.SZ": "创业板ETF",
}

EXCHANGE_LIST = ["SHFE", "DCE", "CZCE", "CFFEX", "INE", "GFEX"]
TS_CODE_RE = re.compile(r"^\d{6}\.(SH|SZ)$", re.IGNORECASE)
DATE_RE = re.compile(r"^\d{8}$")


def parse_args():
    parser = argparse.ArgumentParser(description="15:05 close snapshot updater.")
    parser.add_argument(
        "--date",
        default=datetime.now().strftime("%Y%m%d"),
        help="Target trade date in YYYYMMDD. Defaults to today.",
    )
    parser.add_argument(
        "--etf-symbols",
        default=",".join(DEFAULT_ETF_SYMBOLS),
        help="ETF TS codes, comma-separated.",
    )
    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="Prepare and print snapshot rows without writing DB.",
    )
    return parser.parse_args()


def build_engine():
    required = ["DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_NAME"]
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError(f".env missing database config: {missing}")

    db_url = (
        f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    return create_engine(db_url, pool_recycle=3600)


def build_tushare_client():
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        raise RuntimeError(".env missing TUSHARE_TOKEN")
    ts.set_token(token)
    return ts.pro_api(timeout=120)


def build_sina_session():
    session = requests.Session()
    session.trust_env = False
    session.headers.update(
        {
            "Referer": "https://finance.sina.com.cn",
            "User-Agent": "Mozilla/5.0",
        }
    )
    return session


def normalize_trade_date(value):
    if pd.isna(value):
        return ""
    text_value = str(value).strip()
    if text_value.endswith(".0"):
        text_value = text_value[:-2]
    return text_value.replace("-", "")


def normalize_etf_symbols(raw_symbols):
    items = [item.strip().upper() for item in str(raw_symbols).split(",") if item.strip()]
    if not items:
        raise ValueError("ETF symbols is empty")

    invalid = [symbol for symbol in items if not TS_CODE_RE.match(symbol)]
    if invalid:
        raise ValueError(f"invalid ETF symbols: {invalid}")

    unique_symbols = []
    seen = set()
    for symbol in items:
        if symbol not in seen:
            unique_symbols.append(symbol)
            seen.add(symbol)
    return unique_symbols


def is_trading_day(pro, trade_date):
    try:
        cal = pro.trade_cal(
            exchange="SSE",
            start_date=trade_date,
            end_date=trade_date,
            fields="cal_date,is_open",
        )
        if cal is None or cal.empty:
            print(f"[-] trade_cal empty for {trade_date}, skip for safety.")
            return False
        return int(cal.iloc[0].get("is_open", 0)) == 1
    except Exception as exc:
        print(f"[-] trade calendar check failed: {exc}. skip for safety.")
        return False


def etf_to_sina_code(ts_code):
    code, market = ts_code.split(".")
    market = market.upper()
    if market == "SH":
        return f"sh{code}"
    if market == "SZ":
        return f"sz{code}"
    return ""


def parse_sina_etf_line(line):
    match = re.match(r'var hq_str_(\w+)="(.*)";', line.strip())
    if not match:
        return None, None

    sina_code = match.group(1)
    values = match.group(2).split(",")
    if len(values) < 10:
        return sina_code, None

    try:
        name = (values[0] or "").strip()
        open_price = float(values[1] or 0)
        pre_close = float(values[2] or 0)
        close_price = float(values[3] or 0)
        high_price = float(values[4] or 0)
        low_price = float(values[5] or 0)
        vol = float(values[8] or 0)
        amount = float(values[9] or 0)
    except Exception:
        return sina_code, None

    if close_price <= 0 and pre_close > 0:
        close_price = pre_close
    if open_price <= 0 and close_price > 0:
        open_price = close_price
    if high_price <= 0 and close_price > 0:
        high_price = close_price
    if low_price <= 0 and close_price > 0:
        low_price = close_price

    quote_date = ""
    if len(values) > 30:
        quote_date = normalize_trade_date(values[30])

    pct_chg = 0.0
    if pre_close > 0:
        pct_chg = (close_price - pre_close) / pre_close * 100

    return sina_code, {
        "name": name,
        "open_price": open_price,
        "high_price": high_price,
        "low_price": low_price,
        "close_price": close_price,
        "vol": vol,
        "amount": amount,
        "pct_chg": pct_chg,
        "quote_date": quote_date,
    }


def fetch_sina_etf_quotes(session, sina_codes, retries=3, timeout=(2, 3)):
    if not sina_codes:
        return {}

    url = "https://hq.sinajs.cn/list=" + ",".join(sina_codes)
    last_exc = None
    for attempt in range(retries):
        try:
            response = session.get(url, timeout=timeout)
            raw_text = response.content.decode("gbk", errors="replace")
            parsed = {}
            for line in raw_text.splitlines():
                sina_code, data = parse_sina_etf_line(line)
                if sina_code and data:
                    parsed[sina_code] = data
            return parsed
        except Exception as exc:
            last_exc = exc
            print(f"[warn] ETF sina request failed, retry {attempt + 1}/{retries}: {exc}")
            time.sleep(0.5)

    raise RuntimeError(f"ETF sina request failed: {last_exc}")


def build_etf_rows(symbols, trade_date, quote_map):
    rows = []
    for ts_code in symbols:
        sina_code = etf_to_sina_code(ts_code)
        quote = quote_map.get(sina_code)
        if not quote:
            print(f"[warn] missing ETF quote: {ts_code} ({sina_code})")
            continue

        quote_date = normalize_trade_date(quote.get("quote_date", ""))
        if quote_date and quote_date != trade_date:
            print(
                f"[warn] ETF quote date mismatch: {ts_code}, "
                f"expected={trade_date}, actual={quote_date}. skip."
            )
            continue

        rows.append(
            {
                "trade_date": trade_date,
                "ts_code": ts_code,
                "name": quote.get("name") or ETF_NAME_MAP.get(ts_code, ts_code),
                "open_price": float(quote.get("open_price", 0.0) or 0.0),
                "high_price": float(quote.get("high_price", 0.0) or 0.0),
                "low_price": float(quote.get("low_price", 0.0) or 0.0),
                "close_price": float(quote.get("close_price", 0.0) or 0.0),
                "vol": float(quote.get("vol", 0.0) or 0.0),
                "amount": float(quote.get("amount", 0.0) or 0.0),
                "pct_chg": float(quote.get("pct_chg", 0.0) or 0.0),
            }
        )
    return rows


def futures_to_sina_code(ts_code):
    symbol = str(ts_code).split(".")[0]
    return f"nf_{symbol}"


def parse_sina_futures_text(raw_text):
    data_list = []
    for line in raw_text.split("\n"):
        if not line.strip():
            continue
        try:
            eq_idx = line.find("=")
            if eq_idx == -1:
                continue

            code_part = line[:eq_idx]
            if code_part.startswith("var hq_str_"):
                sina_code = code_part.replace("var hq_str_", "")
            else:
                parts = code_part.split("_")
                sina_code = parts[-2] + "_" + parts[-1]

            val_part = line[eq_idx + 1 :].strip().strip('";')
            if not val_part:
                continue
            vals = val_part.split(",")

            is_cffex = any(
                key in sina_code.upper()
                for key in ["NF_IF", "NF_IC", "NF_IH", "NF_IM", "NF_TF", "NF_T", "NF_TS"]
            )

            if is_cffex:
                if len(vals) < 7:
                    continue
                open_p = float(vals[0])
                high_p = float(vals[1])
                low_p = float(vals[2])
                current_p = float(vals[3])
                vol = float(vals[4])
                oi = float(vals[6])
                pre_close = open_p
            else:
                if len(vals) < 15:
                    continue
                open_p = float(vals[2])
                high_p = float(vals[3])
                low_p = float(vals[4])
                current_p = float(vals[8])
                pre_close = float(vals[5])
                oi = float(vals[13])
                vol = float(vals[14])

            if open_p == 0 and current_p == 0:
                continue
            if low_p > high_p:
                low_p = current_p
                high_p = current_p

            data_list.append(
                {
                    "sina_code": sina_code,
                    "price": current_p,
                    "open": open_p,
                    "high": high_p,
                    "low": low_p,
                    "pre_close": pre_close,
                    "volume": vol,
                    "amount": 0,
                    "position": oi,
                }
            )
        except Exception:
            continue

    return pd.DataFrame(data_list)


def fetch_sina_futures_quotes(session, sina_codes):
    if not sina_codes:
        return pd.DataFrame()

    url = f"http://hq.sinajs.cn/list={','.join(sina_codes)}"
    try:
        response = session.get(url, headers={"Referer": "http://finance.sina.com.cn/"}, timeout=5)
        return parse_sina_futures_text(response.text)
    except Exception as exc:
        print(f"      [!] futures realtime request failed: {exc}")
        return pd.DataFrame()


def fetch_futures_exchange_snapshot(pro, session, exchange, trade_date):
    print(f"   [*] fetch active contracts for {exchange}")
    try:
        df_list = pro.fut_basic(
            exchange=exchange, fut_type="1", status="L", fields="ts_code,symbol"
        )
    except Exception as exc:
        print(f"      [skip] fut_basic failed for {exchange}: {exc}")
        return pd.DataFrame()

    if df_list is None or df_list.empty:
        print(f"      [skip] no active contracts for {exchange}")
        return pd.DataFrame()

    df_list = df_list.copy()
    df_list["sina_code"] = df_list["ts_code"].apply(futures_to_sina_code)
    sina_codes = df_list["sina_code"].tolist()
    if not sina_codes:
        return pd.DataFrame()

    print(f"      -> {exchange} requesting {len(sina_codes)} realtime quotes")
    all_realtime_data = []
    for i in range(0, len(sina_codes), 50):
        batch = sina_codes[i : i + 50]
        df_rt = fetch_sina_futures_quotes(session, batch)
        if not df_rt.empty:
            all_realtime_data.append(df_rt)
        time.sleep(0.05)

    if not all_realtime_data:
        print(f"      [skip] no realtime rows for {exchange}")
        return pd.DataFrame()

    df_snapshot = pd.concat(all_realtime_data, ignore_index=True)
    df_merged = pd.merge(df_snapshot, df_list, on="sina_code", how="inner")
    if df_merged.empty:
        print(f"      [skip] merged realtime rows empty for {exchange}")
        return pd.DataFrame()

    output = pd.DataFrame()
    output["ts_code"] = (
        df_merged["ts_code"].astype(str).str.split(".").str[0].str.upper()
    )
    output["trade_date"] = trade_date
    output["open_price"] = pd.to_numeric(df_merged["open"], errors="coerce").fillna(0.0)
    output["high_price"] = pd.to_numeric(df_merged["high"], errors="coerce").fillna(0.0)
    output["low_price"] = pd.to_numeric(df_merged["low"], errors="coerce").fillna(0.0)
    output["close_price"] = pd.to_numeric(df_merged["price"], errors="coerce").fillna(0.0)
    output["vol"] = pd.to_numeric(df_merged["volume"], errors="coerce").fillna(0.0)
    output["oi"] = pd.to_numeric(df_merged["position"], errors="coerce").fillna(0.0)
    output["settle_price"] = output["close_price"]

    pre_close = pd.to_numeric(df_merged["pre_close"], errors="coerce").fillna(0.0)
    output["pct_chg"] = 0.0
    mask = pre_close > 0
    output.loc[mask, "pct_chg"] = (
        (output.loc[mask, "close_price"] - pre_close[mask]) / pre_close[mask] * 100
    )

    output["symbol"] = output["ts_code"].str.extract(r"^([a-zA-Z]+)")
    idx_max = output.dropna(subset=["symbol"]).groupby("symbol")["oi"].idxmax()
    df_dom = output.loc[idx_max].copy()
    df_dom["ts_code"] = df_dom["symbol"]

    output_final = pd.concat([output, df_dom], ignore_index=True)
    output_final = output_final.drop(columns=["symbol"], errors="ignore")
    return output_final


def dedupe_futures_rows(df):
    if df.empty:
        return df
    return (
        df.sort_values(
            by=["trade_date", "ts_code", "oi", "vol"],
            ascending=[True, True, False, False],
        )
        .drop_duplicates(subset=["trade_date", "ts_code"], keep="first")
        .reset_index(drop=True)
    )


def prepare_futures_snapshot(pro, session, trade_date):
    all_batches = []
    for exchange in EXCHANGE_LIST:
        try:
            df = fetch_futures_exchange_snapshot(pro, session, exchange, trade_date)
            if df.empty:
                continue

            actual_dates = {normalize_trade_date(value) for value in df["trade_date"]}
            if actual_dates != {trade_date}:
                print(
                    f"   [warn] {exchange} trade_date mismatch, expected={trade_date}, "
                    f"actual={sorted(actual_dates)}. skip this exchange."
                )
                continue

            all_batches.append(df)
            print(f"   [ok] {exchange} prepared {len(df)} rows.")
        except Exception as exc:
            print(f"   [!] {exchange} failed: {exc}")

    if not all_batches:
        return pd.DataFrame()

    df_save = pd.concat(all_batches, ignore_index=True)
    df_save = dedupe_futures_rows(df_save)
    actual_dates = {normalize_trade_date(value) for value in df_save["trade_date"]}
    if actual_dates != {trade_date}:
        print(
            f"[warn] final futures trade_date mismatch, expected={trade_date}, "
            f"actual={sorted(actual_dates)}. skip futures write."
        )
        return pd.DataFrame()

    return df_save.sort_values(["ts_code"]).reset_index(drop=True)


def prepare_etf_snapshot(session, symbols, trade_date):
    sina_codes = [etf_to_sina_code(symbol) for symbol in symbols]
    quote_map = fetch_sina_etf_quotes(session, sina_codes, retries=3, timeout=(2, 3))
    return build_etf_rows(symbols, trade_date, quote_map)


def save_close_snapshot(engine, futures_df, etf_rows, trade_date):
    etf_symbols = [row["ts_code"] for row in etf_rows]
    placeholders = ", ".join([f":s{i}" for i in range(len(etf_symbols))])
    params = {"trade_date": trade_date}
    params.update({f"s{i}": symbol for i, symbol in enumerate(etf_symbols)})

    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM futures_price WHERE trade_date = :trade_date"),
            {"trade_date": trade_date},
        )
        futures_df.to_sql("futures_price", conn, if_exists="append", index=False, chunksize=2000)

        conn.execute(
            text(
                f"""
                DELETE FROM stock_price
                WHERE trade_date = :trade_date
                  AND ts_code IN ({placeholders})
                """
            ),
            params,
        )
        pd.DataFrame(etf_rows).to_sql(
            "stock_price", conn, if_exists="append", index=False, chunksize=2000
        )


def run_close_snapshot(
    trade_date,
    etf_symbols=None,
    test_mode=False,
    pro=None,
    engine=None,
    session=None,
):
    trade_date = str(trade_date).strip()
    if not DATE_RE.match(trade_date):
        raise ValueError(f"invalid trade_date: {trade_date}")

    symbols = etf_symbols or DEFAULT_ETF_SYMBOLS
    print(
        f"[Close Snapshot] start date={trade_date}, "
        f"etf_symbols={symbols}, test_mode={test_mode}"
    )

    own_session = session is None
    sina_session = session or build_sina_session()
    try:
        ts_client = pro or build_tushare_client()

        if not is_trading_day(ts_client, trade_date):
            print(f"[-] non-trading day ({trade_date}), skip.")
            return 0

        futures_df = prepare_futures_snapshot(ts_client, sina_session, trade_date)
        etf_rows = prepare_etf_snapshot(sina_session, symbols, trade_date)

        if futures_df.empty:
            raise RuntimeError("no valid futures snapshot rows; skip write")
        if not etf_rows:
            raise RuntimeError("no valid ETF snapshot rows; skip write")

        print(f"[*] futures prepared rows={len(futures_df)}")
        print(futures_df.head(10).to_markdown(index=False))
        print(f"[*] ETF prepared rows={len(etf_rows)}")
        print(pd.DataFrame(etf_rows).to_markdown(index=False))

        if test_mode:
            print("[TEST] test-mode enabled, skip DB write.")
            return 0

        db_engine = engine or build_engine()
        save_close_snapshot(db_engine, futures_df, etf_rows, trade_date)
        print(
            f"[ok] close snapshot write done. "
            f"futures_rows={len(futures_df)}, etf_rows={len(etf_rows)}"
        )
        return 0
    finally:
        if own_session:
            sina_session.close()


def main():
    args = parse_args()
    symbols = normalize_etf_symbols(args.etf_symbols)
    return run_close_snapshot(
        trade_date=args.date,
        etf_symbols=symbols,
        test_mode=bool(args.test_mode),
    )


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"[fatal] close snapshot failed: {exc}")
        sys.exit(1)
