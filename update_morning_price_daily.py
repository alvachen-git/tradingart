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

from mobile_trading_day import _NIGHT_SESSION_PRODUCTS
from symbol_match import sql_prefix_condition

load_dotenv(override=True)

if not os.getenv("DB_USER"):
    print("[Error] .env not loaded, please check env path.")
    sys.exit(1)

db_url = (
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
engine = create_engine(db_url, pool_recycle=3600)

ts.set_token(os.getenv("TUSHARE_TOKEN"))
pro = ts.pro_api(timeout=120)

EXCHANGE_LIST = ["SHFE", "DCE", "CZCE", "CFFEX", "INE", "GFEX"]
NIGHT_PRODUCTS = {str(x).lower() for x in _NIGHT_SESSION_PRODUCTS}
DATE_RE = re.compile(r"^\d{8}$")
_http_session = requests.Session()
_http_session.trust_env = False


def is_trading_day(date_str):
    """
    Fail-closed:
    - trade_cal request error / empty result => treat as non-trading day.
    """
    try:
        df = pro.trade_cal(exchange="SHFE", start_date=date_str, end_date=date_str)
        if df is None or df.empty:
            print(f"[-] trade_cal empty for {date_str}, skip for safety.")
            return False
        is_open = int(df.iloc[0].get("is_open", 0))
        return is_open == 1
    except Exception as exc:
        print(f"[-] trade calendar check failed: {exc}. skip for safety.")
        return False


def check_futures_price_unique_index():
    sql = """
    SELECT INDEX_NAME, GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS cols
    FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'futures_price'
      AND NON_UNIQUE = 0
    GROUP BY INDEX_NAME
    """
    try:
        idx_df = pd.read_sql(sql, engine)
        if idx_df.empty:
            print(
                "[warn] futures_price has no unique index. "
                "recommend unique key (trade_date, ts_code)."
            )
            return

        normalized = {
            str(cols).replace(" ", "").lower() for cols in idx_df["cols"].tolist()
        }
        if "trade_date,ts_code" not in normalized:
            print(
                "[warn] futures_price unique key does not contain (trade_date, ts_code). "
                "recommend adding it for idempotency."
            )
    except Exception as exc:
        print(f"[warn] unique index check failed: {exc}")


def map_code_to_sina(ts_code):
    symbol = str(ts_code).split(".")[0]
    return f"nf_{symbol}"


def _extract_product_prefix(code):
    match = re.match(r"^([A-Za-z]+)", str(code or ""))
    return match.group(1).lower() if match else ""


def _is_night_row_code(code):
    if not code:
        return False
    code_upper = str(code).upper()
    if "TAS" in code_upper:
        return False
    return _extract_product_prefix(code_upper) in NIGHT_PRODUCTS


def get_sina_futures_custom(sina_codes):
    if not sina_codes:
        return pd.DataFrame()

    url = f"http://hq.sinajs.cn/list={','.join(sina_codes)}"
    headers = {"Referer": "http://finance.sina.com.cn/"}

    try:
        response = _http_session.get(url, headers=headers, timeout=5)
        raw_text = response.text
    except Exception as exc:
        print(f"      [!] realtime request failed: {exc}")
        return pd.DataFrame()

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
                x in sina_code.upper()
                for x in ["NF_IF", "NF_IC", "NF_IH", "NF_IM", "NF_TF", "NF_T", "NF_TS"]
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


def fetch_realtime_snapshot(exchange, trade_date):
    print(f"   [*] fetch active contracts for {exchange}")

    try:
        df_list = pro.fut_basic(
            exchange=exchange, fut_type="1", status="L", fields="ts_code,symbol"
        )
    except Exception:
        df_list = pd.DataFrame()

    if df_list.empty:
        print(f"      [skip] no active contracts for {exchange}")
        return pd.DataFrame()

    df_list = df_list.copy()
    df_list["sina_code"] = df_list["ts_code"].apply(map_code_to_sina)
    sina_codes = df_list["sina_code"].tolist()
    if not sina_codes:
        return pd.DataFrame()

    print(f"      -> {exchange} requesting {len(sina_codes)} realtime quotes")
    all_realtime_data = []
    chunk_size = 50

    for i in range(0, len(sina_codes), chunk_size):
        batch = sina_codes[i : i + chunk_size]
        df_rt = get_sina_futures_custom(batch)
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
    pre_close = pd.to_numeric(df_merged["pre_close"], errors="coerce").fillna(0.0)
    output["vol"] = pd.to_numeric(df_merged["volume"], errors="coerce").fillna(0.0)
    output["oi"] = pd.to_numeric(df_merged["position"], errors="coerce").fillna(0.0)
    output["settle_price"] = output["close_price"]
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
    output_final = output_final.drop_duplicates(subset=["ts_code"], keep="last")
    output_final = output_final.drop(columns=["symbol"], errors="ignore")
    return output_final


def _normalize_trade_date(value):
    if pd.isna(value):
        return ""
    s = str(value).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s


def _dedupe_rows(df):
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


def _delete_trade_date_night_rows(trade_date):
    clauses = [f"({sql_prefix_condition(p, 'ts_code')})" for p in sorted(NIGHT_PRODUCTS)]
    delete_sql = f"""
    DELETE FROM futures_price
    WHERE trade_date = :trade_date
      AND UPPER(ts_code) NOT LIKE '%%TAS%%'
      AND ({' OR '.join(clauses)})
    """
    with engine.connect() as conn:
        conn.execute(text(delete_sql), {"trade_date": trade_date})
        conn.commit()


def run_morning_update(trade_date, test_mode=False):
    print(f"[Morning Update] start. TEST_MODE={test_mode}, date={trade_date}")

    if not DATE_RE.match(str(trade_date)):
        raise ValueError(f"invalid trade_date: {trade_date}")

    if not is_trading_day(trade_date):
        print(f"[-] non-trading day ({trade_date}), skip.")
        return

    check_futures_price_unique_index()

    all_batches = []
    for exchange in EXCHANGE_LIST:
        try:
            df = fetch_realtime_snapshot(exchange, trade_date)
            if df.empty:
                continue

            actual_dates = {_normalize_trade_date(v) for v in df["trade_date"]}
            if actual_dates != {trade_date}:
                print(
                    f"   [warn] {exchange} trade_date mismatch, expected={trade_date}, "
                    f"actual={sorted(actual_dates)}. skip this exchange."
                )
                continue

            df = df[df["ts_code"].apply(_is_night_row_code)].copy()
            if df.empty:
                print(f"   [skip] {exchange} has no night-session rows.")
                continue

            all_batches.append(df)
            print(f"   [ok] {exchange} prepared night rows={len(df)}.")
        except Exception as exc:
            print(f"   [!] {exchange} failed: {exc}")

    if not all_batches:
        print("[-] no valid night-session futures rows, skip delete/insert.")
        return

    df_save = pd.concat(all_batches, ignore_index=True)
    df_save = _dedupe_rows(df_save)
    actual_dates = {_normalize_trade_date(v) for v in df_save["trade_date"]}
    if actual_dates != {trade_date}:
        print(
            f"[warn] final trade_date mismatch, expected={trade_date}, "
            f"actual={sorted(actual_dates)}. skip write."
        )
        return

    df_save = df_save.sort_values(["ts_code"]).reset_index(drop=True)
    if test_mode:
        products = sorted(
            {
                _extract_product_prefix(ts_code).upper()
                for ts_code in df_save["ts_code"].tolist()
                if _extract_product_prefix(ts_code)
            }
        )
        print(
            f"[TEST] prepared rows={len(df_save)}, "
            f"products={len(products)}, sample_products={products[:12]}"
        )
        print(df_save.head(10).to_markdown(index=False))
        return

    _delete_trade_date_night_rows(trade_date)
    print("[*] old night-session rows deleted for target date.")

    df_save.to_sql("futures_price", engine, if_exists="append", index=False, chunksize=2000)
    print(f"[ok] morning futures update done. rows={len(df_save)}")


def parse_args():
    parser = argparse.ArgumentParser(description="Morning futures updater (night-session only).")
    parser.add_argument(
        "--date",
        default=datetime.now().strftime("%Y%m%d"),
        help="Target trade date in YYYYMMDD. Defaults to today.",
    )
    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="Prepare and print sample rows without writing DB.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    try:
        run_morning_update(trade_date=str(args.date), test_mode=bool(args.test_mode))
    except Exception as exc:
        print(f"[fatal] morning update failed: {exc}")
        sys.exit(1)
