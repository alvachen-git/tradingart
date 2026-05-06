import argparse
import os
import time

import pandas as pd
import tushare as ts
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, types


load_dotenv(override=True)

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

ts_token = os.getenv("TUSHARE_TOKEN")
pro = ts.pro_api(ts_token)

db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)

DAILY_FIELDS = ["ts_code", "trade_date", "close", "oi", "vol"]


def _sql_in(values):
    return ", ".join(f"'{str(v).replace("'", "''")}'" for v in values)


def _get_exchange_and_keyword(etf_code: str):
    exchange = "SSE"
    keyword = ""
    if "510050" in etf_code:
        keyword = "50ETF"
    elif "510300" in etf_code:
        keyword = "300ETF"
    elif "510500" in etf_code:
        keyword = "500ETF"
    elif "588000" in etf_code:
        keyword = "科创"
    elif "159915" in etf_code:
        keyword = "创业板"
        exchange = "SZSE"
    else:
        raise ValueError(f"Unknown ETF: {etf_code}")
    return exchange, keyword


def _upsert_option_basic(df_basic: pd.DataFrame, dry_run: bool = False):
    if df_basic.empty:
        return 0
    if dry_run:
        print(f"  [DRY-RUN] option_basic would refresh {len(df_basic)} contracts")
        return len(df_basic)

    codes_str = _sql_in(df_basic["ts_code"].tolist())
    with engine.connect() as conn:
        conn.execute(text(f"DELETE FROM option_basic WHERE ts_code IN ({codes_str})"))
        conn.commit()

    df_basic.to_sql(
        "option_basic",
        engine,
        if_exists="append",
        index=False,
        dtype={"exercise_price": types.Float()},
    )
    return len(df_basic)


def _replace_option_daily(df_daily: pd.DataFrame, trade_date: str, dry_run: bool = False):
    if df_daily.empty:
        return 0
    if dry_run:
        print(f"  [DRY-RUN] {trade_date}: option_daily would replace {len(df_daily)} rows")
        return len(df_daily)

    codes_str = _sql_in(df_daily["ts_code"].unique().tolist())
    with engine.begin() as conn:
        conn.execute(
            text(
                f"DELETE FROM option_daily "
                f"WHERE trade_date = :trade_date AND ts_code IN ({codes_str})"
            ),
            {"trade_date": trade_date},
        )
        df_daily.to_sql(
            "option_daily",
            conn,
            if_exists="append",
            index=False,
            dtype={"close": types.Float(), "oi": types.Float(), "vol": types.Float()},
        )
    return len(df_daily)


def _load_basic_from_db(underlying: str):
    sql = text(
        """
        SELECT ts_code, name, call_put, exercise_price, list_date, delist_date, underlying
        FROM option_basic
        WHERE underlying = :u
        """
    )
    return pd.read_sql(sql, engine, params={"u": underlying})


def _trade_days(exchange: str, start_date: str, end_date: str):
    cal = pro.trade_cal(exchange=exchange, start_date=start_date, end_date=end_date, is_open="1")
    if cal.empty:
        return []
    return cal["cal_date"].astype(str).tolist()


def _find_missing_days(underlying: str, trade_days: list[str]):
    if not trade_days:
        return []
    sql = text(
        """
        SELECT trade_date, COUNT(*) AS cnt
        FROM option_daily d
        JOIN option_basic b ON d.ts_code = b.ts_code
        WHERE b.underlying = :u AND d.trade_date BETWEEN :s AND :e
        GROUP BY trade_date
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(
            sql,
            conn,
            params={"u": underlying, "s": trade_days[0], "e": trade_days[-1]},
        )
    if df.empty:
        return trade_days

    day_cnt = {str(r["trade_date"]): int(r["cnt"]) for _, r in df.iterrows()}
    cnt_values = list(day_cnt.values())
    if not cnt_values:
        return trade_days

    median_cnt = int(pd.Series(cnt_values).median())
    missing = []
    for d in trade_days:
        cnt = day_cnt.get(d, 0)
        if cnt == 0 or cnt < int(median_cnt * 0.8):
            missing.append(d)
    return missing


def _filter_active_contracts(df_basic: pd.DataFrame, trade_date: str):
    return df_basic[
        (df_basic["list_date"].astype(str) <= trade_date)
        & (df_basic["delist_date"].astype(str) >= trade_date)
    ]


def _load_existing_daily(trade_date: str, target_codes: list[str]):
    if not target_codes:
        return pd.DataFrame(columns=DAILY_FIELDS)

    codes_str = _sql_in(target_codes)
    sql = text(
        f"""
        SELECT ts_code, trade_date, close, oi, vol
        FROM option_daily
        WHERE trade_date = :d AND ts_code IN ({codes_str})
        """
    )
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"d": trade_date})


def _normalize_daily(df: pd.DataFrame):
    df = df.copy()
    if "ts_code" in df.columns:
        df["ts_code"] = df["ts_code"].astype(str)
    if "trade_date" in df.columns:
        df["trade_date"] = df["trade_date"].astype(str)
    for col in ("close", "oi", "vol"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _build_rows_to_save(
    df_api: pd.DataFrame,
    df_existing: pd.DataFrame,
    target_codes: list[str],
    *,
    only_missing: bool,
    reconcile_existing: bool,
    oi_abs_warn: float,
    oi_pct_warn: float,
):
    api = _normalize_daily(df_api[df_api["ts_code"].isin(target_codes)])
    existing = _normalize_daily(df_existing)
    existing_by_code = existing.set_index("ts_code").to_dict("index") if not existing.empty else {}

    if not only_missing and not reconcile_existing:
        return api[DAILY_FIELDS], [], []

    rows_to_save = []
    missing = []
    diffs = []

    for _, api_row in api.iterrows():
        code = str(api_row["ts_code"])
        existing_row = existing_by_code.get(code)
        if existing_row is None:
            missing.append(code)
            rows_to_save.append(api_row)
            continue
        if not reconcile_existing:
            continue

        field_diffs = []
        for field in ("close", "oi", "vol"):
            old = existing_row.get(field)
            new = api_row.get(field)
            if pd.isna(old) and pd.isna(new):
                continue
            if pd.isna(old) or pd.isna(new) or abs(float(old) - float(new)) > 1e-9:
                field_diffs.append((field, old, new))

        if not field_diffs:
            continue

        warn = False
        for field, old, new in field_diffs:
            if field != "oi" or pd.isna(old) or pd.isna(new):
                continue
            old_f = float(old)
            new_f = float(new)
            abs_diff = abs(new_f - old_f)
            pct_diff = abs_diff / abs(old_f) * 100 if old_f else 100.0
            if abs_diff >= oi_abs_warn or pct_diff >= oi_pct_warn:
                warn = True

        diffs.append({"ts_code": code, "fields": field_diffs, "warn": warn})
        rows_to_save.append(api_row)

    if not rows_to_save:
        return pd.DataFrame(columns=DAILY_FIELDS), missing, diffs
    return pd.DataFrame(rows_to_save)[DAILY_FIELDS], missing, diffs


def _print_diff_report(trade_date: str, missing: list[str], diffs: list[dict], limit: int = 20):
    if missing:
        sample = ", ".join(missing[:limit])
        suffix = "" if len(missing) <= limit else f" ... +{len(missing) - limit}"
        print(f"  {trade_date}: missing {len(missing)} rows: {sample}{suffix}")

    for item in diffs[:limit]:
        prefix = "WARNING " if item["warn"] else ""
        parts = [f"{field} {old} -> {new}" for field, old, new in item["fields"]]
        print(f"  {prefix}{trade_date} {item['ts_code']}: " + "; ".join(parts))
    if len(diffs) > limit:
        print(f"  {trade_date}: ... {len(diffs) - limit} more changed rows")


def fill_missing(
    etf_code: str,
    start_date: str,
    end_date: str,
    only_missing: bool = True,
    sleep_s: float = 0.3,
    only_missing_days: bool = False,
    reconcile_existing: bool = False,
    dry_run: bool = False,
    oi_abs_warn: float = 3000,
    oi_pct_warn: float = 5.0,
):
    exchange, keyword = _get_exchange_and_keyword(etf_code)
    print(f"=== ETF option repair: {etf_code} {start_date} ~ {end_date} exchange={exchange} ===")
    if reconcile_existing:
        print("=== Reconcile existing rows: ON ===")
    if dry_run:
        print("=== DRY-RUN: no database writes ===")

    print("[1/3] Refreshing option_basic contracts (L + D)...")
    df_basic = pro.opt_basic(
        exchange=exchange,
        list_status="L,D",
        fields="ts_code,name,call_put,exercise_price,list_date,delist_date",
    )
    df_basic = df_basic[df_basic["name"].astype(str).str.contains(keyword, na=False)].copy()
    df_basic["underlying"] = etf_code
    inserted = _upsert_option_basic(df_basic, dry_run=dry_run)
    print(f"  contracts refreshed: {inserted}")

    df_basic_db = df_basic if dry_run else _load_basic_from_db(etf_code)
    if df_basic_db.empty:
        print("  [!] option_basic is empty; stop.")
        return

    trade_days = _trade_days(exchange, start_date, end_date)
    if only_missing_days:
        trade_days = _find_missing_days(etf_code, trade_days)
        print(f"[2/3] missing trade days: {len(trade_days)}")
    else:
        print(f"[2/3] trade days: {len(trade_days)}")

    total_replaced = 0
    total_diffs = 0
    total_missing = 0

    for d in trade_days:
        active = _filter_active_contracts(df_basic_db, d)
        if active.empty:
            continue

        target_codes = active["ts_code"].astype(str).tolist()
        existing = _load_existing_daily(d, target_codes)

        if only_missing and not reconcile_existing and len(existing) >= len(target_codes):
            continue

        df_daily = pro.opt_daily(
            trade_date=d,
            exchange=exchange,
            fields="ts_code,trade_date,close,oi,vol",
        )
        if df_daily.empty:
            continue

        df_save, missing, diffs = _build_rows_to_save(
            df_daily,
            existing,
            target_codes,
            only_missing=only_missing,
            reconcile_existing=reconcile_existing,
            oi_abs_warn=oi_abs_warn,
            oi_pct_warn=oi_pct_warn,
        )
        if df_save.empty:
            continue

        _print_diff_report(d, missing, diffs)
        replaced = _replace_option_daily(df_save, d, dry_run=dry_run)
        action = "would update" if dry_run else "updated"
        print(f"  {d}: {action} {replaced} rows")
        total_replaced += replaced
        total_missing += len(missing)
        total_diffs += len(diffs)
        time.sleep(sleep_s)

    print(
        f"[3/3] done. rows={total_replaced}, missing={total_missing}, "
        f"reconciled_diffs={total_diffs}"
    )


def parse_args():
    parser = argparse.ArgumentParser(description="Backfill and reconcile ETF option daily data")
    parser.add_argument("--etf", required=True, help="Example: 510050.SH")
    parser.add_argument("--start", required=True, help="YYYYMMDD")
    parser.add_argument("--end", required=True, help="YYYYMMDD")
    parser.add_argument("--all", action="store_true", help="Refresh all active rows instead of only missing rows")
    parser.add_argument("--only-missing-days", action="store_true", help="Only scan days with too few rows")
    parser.add_argument(
        "--reconcile-existing",
        action="store_true",
        help="Compare existing rows with current Tushare values and repair mismatches",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print planned repairs without database writes")
    parser.add_argument("--oi-abs-warn", type=float, default=3000, help="Warn when OI absolute diff is this large")
    parser.add_argument("--oi-pct-warn", type=float, default=5.0, help="Warn when OI percentage diff is this large")
    parser.add_argument("--sleep", type=float, default=0.3, help="Tushare request interval in seconds")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    fill_missing(
        etf_code=args.etf,
        start_date=args.start,
        end_date=args.end,
        only_missing=not args.all,
        sleep_s=args.sleep,
        only_missing_days=args.only_missing_days,
        reconcile_existing=args.reconcile_existing,
        dry_run=args.dry_run,
        oi_abs_warn=args.oi_abs_warn,
        oi_pct_warn=args.oi_pct_warn,
    )
