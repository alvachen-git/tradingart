import os
import sys
import time
from datetime import datetime

import numpy as np
import pandas as pd
import tushare as ts
from dotenv import load_dotenv
from requests.exceptions import ConnectionError, ReadTimeout
from sqlalchemy import create_engine, text

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

EXCHANGE_CONFIG = {
    "SHFE": ".SHF",
    "DCE": ".DCE",
    "CZCE": ".ZCE",
    "CFFEX": ".CFX",
    "INE": ".INE",
    "GFEX": ".GFE",
}


def fetch_tushare_safe(api_func, max_retries=5, sleep_time=3, **kwargs):
    for i in range(max_retries):
        try:
            return api_func(**kwargs)
        except (ReadTimeout, ConnectionError):
            print(f"   [retry] network error, retry {i + 1}/{max_retries}")
            time.sleep(sleep_time)
        except Exception as exc:
            print(f"   [retry] api error: {exc}, retry {i + 1}/{max_retries}")
            time.sleep(sleep_time)

    print(f"   [skip] retried {max_retries} times but still failed.")
    return pd.DataFrame()


def is_trading_day(date_str):
    """
    Fail-closed:
    - trade_cal fetch failed / empty -> treat as non-trading day
    """
    try:
        df = fetch_tushare_safe(
            pro.trade_cal, exchange="SHFE", start_date=date_str, end_date=date_str
        )
        if df.empty:
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


def _normalize_trade_date(value):
    if pd.isna(value):
        return ""
    s = str(value).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s


def dedupe_by_primary_key(df, key_cols):
    """
    入库前按主键去重，优先保留 oi/vol 更大的记录，避免 to_sql 触发主键冲突。
    """
    if df.empty:
        return df

    dup_mask = df.duplicated(subset=key_cols, keep=False)
    if not dup_mask.any():
        return df

    dup_keys = df.loc[dup_mask, key_cols].drop_duplicates()
    sample_keys = (
        dup_keys.astype(str).agg("-".join, axis=1).head(10).tolist()
    )
    print(
        f"[warn] detected duplicated primary keys before insert: {len(dup_keys)} keys. "
        f"sample={sample_keys}"
    )

    deduped = (
        df.sort_values(
            by=key_cols + ["oi", "vol"],
            ascending=[True] * len(key_cols) + [False, False],
        )
        .drop_duplicates(subset=key_cols, keep="first")
        .reset_index(drop=True)
    )
    print(f"[*] dedupe done: {len(df)} -> {len(deduped)} rows.")
    return deduped


def prepare_exchange_data(trade_date, exchange, suffix):
    df = fetch_tushare_safe(
        pro.fut_daily, max_retries=5, trade_date=trade_date, exchange=exchange
    )
    if df.empty:
        print(f"   [skip] {exchange} empty response.")
        return pd.DataFrame()

    if "ts_code" not in df.columns:
        print(f"   [skip] {exchange} missing ts_code.")
        return pd.DataFrame()

    df = df[df["ts_code"].astype(str).str.endswith(suffix)].copy()
    if df.empty:
        print(f"   [skip] {exchange} filtered empty by suffix {suffix}.")
        return pd.DataFrame()

    if "trade_date" not in df.columns:
        print(f"   [skip] {exchange} missing trade_date.")
        return pd.DataFrame()

    actual_dates = {_normalize_trade_date(v) for v in df["trade_date"]}
    if actual_dates != {trade_date}:
        print(
            f"   [warn] {exchange} trade_date mismatch, expected={trade_date}, "
            f"actual={sorted(actual_dates)}. skip this exchange."
        )
        return pd.DataFrame()

    df["ts_code"] = df["ts_code"].astype(str).str.split(".").str[0].str.upper()
    df["trade_date"] = trade_date

    df = df.rename(
        columns={
            "open": "open_price",
            "high": "high_price",
            "low": "low_price",
            "close": "close_price",
            "settle": "settle_price",
        }
    )

    numeric_cols = [
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "settle_price",
        "vol",
        "oi",
    ]
    for col in numeric_cols:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    mask_fix = (df["close_price"] <= 0.001) & (df["settle_price"] > 0)
    df.loc[mask_fix, "close_price"] = df.loc[mask_fix, "settle_price"]
    for col in ["open_price", "high_price", "low_price"]:
        df.loc[df[col] <= 0.001, col] = df["close_price"]

    if "pct_chg" in df.columns:
        df["pct_chg"] = pd.to_numeric(df["pct_chg"], errors="coerce").fillna(0.0)
    elif "pre_close" in df.columns:
        pre_close = pd.to_numeric(df["pre_close"], errors="coerce").fillna(0.0)
        df["pct_chg"] = np.where(
            pre_close > 0,
            (df["close_price"] - pre_close) / pre_close * 100,
            0.0,
        )
    else:
        df["pct_chg"] = 0.0

    df["symbol"] = df["ts_code"].str.extract(r"^([a-zA-Z]+)")
    idx_max = df.dropna(subset=["symbol"]).groupby("symbol")["oi"].idxmax()
    df_dom = df.loc[idx_max].copy()
    df_dom["ts_code"] = df_dom["symbol"]

    df_final = pd.concat([df, df_dom], ignore_index=True)
    df_final = df_final.drop_duplicates(subset=["ts_code"], keep="last")

    final_cols = [
        "trade_date",
        "ts_code",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "settle_price",
        "vol",
        "oi",
        "pct_chg",
    ]
    return df_final[final_cols].copy()


def update_daily_data(trade_date):
    print(f"[*] start daily futures update: {trade_date}")
    start_t = time.time()

    all_batches = []
    total_records = 0

    for exchange, suffix in EXCHANGE_CONFIG.items():
        try:
            df_ready = prepare_exchange_data(trade_date, exchange, suffix)
            if df_ready.empty:
                continue
            all_batches.append(df_ready)
            total_records += len(df_ready)
            print(f"   [ok] {exchange} prepared {len(df_ready)} rows.")
        except Exception as exc:
            print(f"   [!] {exchange} prepare failed: {exc}")

    if not all_batches:
        print("[-] no valid futures rows, skip delete/insert.")
        return

    df_save = pd.concat(all_batches, ignore_index=True)
    df_save = dedupe_by_primary_key(df_save, ["trade_date", "ts_code"])
    total_records = len(df_save)
    actual_dates = {_normalize_trade_date(v) for v in df_save["trade_date"]}
    if actual_dates != {trade_date}:
        print(
            f"[warn] final trade_date mismatch, expected={trade_date}, "
            f"actual={sorted(actual_dates)}. skip write."
        )
        return

    with engine.connect() as conn:
        conn.execute(
            text("DELETE FROM futures_price WHERE trade_date = :trade_date"),
            {"trade_date": trade_date},
        )
        conn.commit()

    print("[*] old rows deleted for target date.")
    df_save.to_sql("futures_price", engine, if_exists="append", index=False, chunksize=2000)

    duration = time.time() - start_t
    print(
        f"[ok] futures update done. date={trade_date}, rows={total_records}, "
        f"cost={duration:.2f}s"
    )


if __name__ == "__main__":
    now = datetime.now()
    today_str = now.strftime("%Y%m%d")

    if now.weekday() >= 5:
        print(f"[-] weekend ({today_str}), skip.")
        sys.exit(0)

    if not is_trading_day(today_str):
        print(f"[-] non-trading day ({today_str}), skip.")
        sys.exit(0)

    check_futures_price_unique_index()

    try:
        update_daily_data(today_str)
    except Exception as exc:
        print(f"[fatal] update failed: {exc}")
        sys.exit(1)
