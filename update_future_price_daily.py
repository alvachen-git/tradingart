import os
import sys
import time
import argparse
import html
import socket
from datetime import datetime

import numpy as np
import pandas as pd
import tushare as ts
from dotenv import load_dotenv
from requests.exceptions import ConnectionError, ReadTimeout
from sqlalchemy import create_engine, text

try:
    from email_utils2 import send_email as send_alert_email
except Exception as exc:
    send_alert_email = None
    print(f"[warn] email alert sender unavailable: {exc}")

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

MIN_TOTAL_ROWS = int(os.getenv("FUTURES_PRICE_MIN_TOTAL_ROWS", "800"))
MIN_EXCHANGE_ROWS = int(os.getenv("FUTURES_PRICE_MIN_EXCHANGE_ROWS", "10"))
ALERT_EMAIL_TO = (
    os.getenv("FUTURES_PRICE_ALERT_EMAIL_TO", "").strip()
    or os.getenv("ALERT_EMAIL_TO", "").strip()
    or "alvachenart@163.com"
)
ALERT_EMAIL_ENABLED = os.getenv("FUTURES_PRICE_ALERT_EMAIL_ENABLED", "1").strip() != "0"


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
    - trade_cal fetch failed / empty -> unknown, caller should alert and skip
    """
    try:
        df = fetch_tushare_safe(
            pro.trade_cal, exchange="SHFE", start_date=date_str, end_date=date_str
        )
        if df.empty:
            print(f"[-] trade_cal empty for {date_str}, skip for safety.")
            return None

        is_open = int(df.iloc[0].get("is_open", 0))
        return is_open == 1
    except Exception as exc:
        print(f"[-] trade calendar check failed: {exc}. skip for safety.")
        return None


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


def parse_alert_recipients(raw_value):
    return [
        item.strip()
        for item in str(raw_value or "").replace(";", ",").split(",")
        if item.strip()
    ]


def send_update_alert(trade_date, reason, details=None):
    if not ALERT_EMAIL_ENABLED:
        print("[alert] email alert disabled by FUTURES_PRICE_ALERT_EMAIL_ENABLED=0")
        return False

    recipients = parse_alert_recipients(ALERT_EMAIL_TO)
    if not recipients:
        print("[alert] no alert email recipients configured.")
        return False

    if send_alert_email is None:
        print("[alert] email sender unavailable, cannot send alert.")
        return False

    detail_lines = details or []
    escaped_details = "".join(
        f"<li>{html.escape(str(line))}</li>" for line in detail_lines
    )
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    host = socket.gethostname()
    subject = f"[期货行情告警] {trade_date} 日线数据未完整更新"
    content = f"""
    <div style="font-family:Arial,'Microsoft YaHei',sans-serif;line-height:1.7;color:#222;">
      <h2>期货行情日线更新告警</h2>
      <p><b>交易日：</b>{html.escape(str(trade_date))}</p>
      <p><b>服务器：</b>{html.escape(host)}</p>
      <p><b>时间：</b>{html.escape(now_text)}</p>
      <p><b>原因：</b>{html.escape(str(reason))}</p>
      <p>系统已跳过本次覆盖写入，避免把 futures_price 写成半包数据。后续任务会继续执行。</p>
      <ul>{escaped_details}</ul>
    </div>
    """

    ok_count = 0
    for addr in recipients:
        try:
            if bool(send_alert_email(addr, subject, content)):
                ok_count += 1
        except Exception as exc:
            print(f"[alert] send email failed for {addr}: {exc}")

    print(f"[alert] email sent {ok_count}/{len(recipients)} recipients.")
    return ok_count > 0


def validate_exchange_batches(exchange_stats, total_records):
    errors = []

    missing_exchanges = [
        exchange for exchange in EXCHANGE_CONFIG if exchange not in exchange_stats
    ]
    if missing_exchanges:
        errors.append(f"missing exchanges: {missing_exchanges}")

    too_small = {
        exchange: rows
        for exchange, rows in exchange_stats.items()
        if rows < MIN_EXCHANGE_ROWS
    }
    if too_small:
        errors.append(
            f"exchange rows below threshold {MIN_EXCHANGE_ROWS}: {too_small}"
        )

    if total_records < MIN_TOTAL_ROWS:
        errors.append(
            f"total rows below threshold {MIN_TOTAL_ROWS}: {total_records}"
        )

    return errors


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
    exchange_stats = {}
    failed_exchanges = []
    total_records = 0

    for exchange, suffix in EXCHANGE_CONFIG.items():
        try:
            df_ready = prepare_exchange_data(trade_date, exchange, suffix)
            if df_ready.empty:
                failed_exchanges.append(exchange)
                continue
            all_batches.append(df_ready)
            row_count = len(df_ready)
            exchange_stats[exchange] = row_count
            total_records += row_count
            print(f"   [ok] {exchange} prepared {row_count} rows.")
        except Exception as exc:
            failed_exchanges.append(exchange)
            print(f"   [!] {exchange} prepare failed: {exc}")

    if not all_batches:
        raise RuntimeError("no valid futures rows, skip delete/insert")

    validation_errors = validate_exchange_batches(exchange_stats, total_records)
    if failed_exchanges:
        validation_errors.append(f"failed or empty exchanges: {failed_exchanges}")
    if validation_errors:
        raise RuntimeError(
            "incomplete futures data, skip delete/insert. "
            + "; ".join(validation_errors)
        )

    df_save = pd.concat(all_batches, ignore_index=True)
    df_save = dedupe_by_primary_key(df_save, ["trade_date", "ts_code"])
    total_records = len(df_save)
    actual_dates = {_normalize_trade_date(v) for v in df_save["trade_date"]}
    if actual_dates != {trade_date}:
        raise RuntimeError(
            f"final trade_date mismatch, expected={trade_date}, "
            f"actual={sorted(actual_dates)}. skip write."
        )

    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM futures_price WHERE trade_date = :trade_date"),
            {"trade_date": trade_date},
        )
        print("[*] old rows deleted for target date.")
        df_save.to_sql(
            "futures_price",
            conn,
            if_exists="append",
            index=False,
            chunksize=2000,
        )

    print("[*] new rows inserted for target date.")

    duration = time.time() - start_t
    print(
        f"[ok] futures update done. date={trade_date}, rows={total_records}, "
        f"cost={duration:.2f}s"
    )


def normalize_target_date(value):
    if not value:
        return ""
    s = str(value).strip().replace("-", "")
    if len(s) != 8 or not s.isdigit():
        raise ValueError("date must be YYYYMMDD or YYYY-MM-DD")
    try:
        datetime.strptime(s, "%Y%m%d")
    except ValueError as exc:
        raise ValueError("date must be a valid calendar date") from exc
    return s


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Fetch daily futures prices from Tushare and overwrite futures_price rows for the target date."
    )
    parser.add_argument(
        "--date",
        dest="target_date",
        help="Target trade date to refetch, e.g. 20260522. Default: today.",
    )
    parser.add_argument(
        "--skip-calendar-check",
        action="store_true",
        help="Skip Tushare trade calendar check. Use only when the calendar API is unavailable or a make-up trading day is known.",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args()
    now = datetime.now()
    today_str = now.strftime("%Y%m%d")
    manual_date = bool(args.target_date)

    try:
        target_date = normalize_target_date(args.target_date) if manual_date else today_str
    except ValueError as exc:
        print(f"[fatal] invalid --date: {exc}")
        sys.exit(2)

    if not manual_date and now.weekday() >= 5:
        print(f"[-] weekend ({today_str}), skip.")
        sys.exit(0)

    if args.skip_calendar_check:
        print(f"[*] skip calendar check for {target_date}.")
    else:
        calendar_open = is_trading_day(target_date)
        if calendar_open is None:
            reason = f"trade calendar unavailable for {target_date}"
            print(f"[alert] {reason}, skip update.")
            send_update_alert(target_date, reason)
            sys.exit(0)
        if not calendar_open:
            print(f"[-] non-trading day ({target_date}), skip.")
            sys.exit(0)

    check_futures_price_unique_index()

    try:
        update_daily_data(target_date)
    except Exception as exc:
        print(f"[alert] futures update skipped: {exc}")
        send_update_alert(target_date, exc)
        sys.exit(0)
