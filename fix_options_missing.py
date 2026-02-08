import argparse
from datetime import datetime, timedelta
import time
import os

import pandas as pd
import tushare as ts
from sqlalchemy import create_engine, text, types
from dotenv import load_dotenv


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


def _upsert_option_basic(df_basic: pd.DataFrame):
    if df_basic.empty:
        return 0
    codes = df_basic["ts_code"].tolist()
    codes_str = "', '".join(codes)
    with engine.connect() as conn:
        del_sql = f"DELETE FROM option_basic WHERE ts_code IN ('{codes_str}')"
        conn.execute(text(del_sql))
        conn.commit()
    df_basic.to_sql(
        "option_basic",
        engine,
        if_exists="append",
        index=False,
        dtype={"exercise_price": types.Float()},
    )
    return len(df_basic)


def _upsert_option_daily(df_daily: pd.DataFrame, trade_date: str):
    if df_daily.empty:
        return 0
    codes = df_daily["ts_code"].unique().tolist()
    codes_str = "', '".join(codes)
    with engine.connect() as conn:
        del_sql = f"DELETE FROM option_daily WHERE trade_date='{trade_date}' AND ts_code IN ('{codes_str}')"
        conn.execute(text(del_sql))
        conn.commit()
    df_daily.to_sql(
        "option_daily",
        engine,
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
    return cal["cal_date"].tolist()


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
    start_date = trade_days[0]
    end_date = trade_days[-1]
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"u": underlying, "s": start_date, "e": end_date})
    if df.empty:
        return trade_days
    day_cnt = {str(r["trade_date"]): int(r["cnt"]) for _, r in df.iterrows()}
    cnt_values = list(day_cnt.values())
    if not cnt_values:
        return trade_days
    # 使用中位数作为“正常交易日合约数量”的基准
    median_cnt = int(pd.Series(cnt_values).median())
    missing = []
    for d in trade_days:
        cnt = day_cnt.get(d, 0)
        if cnt == 0 or cnt < int(median_cnt * 0.8):
            missing.append(d)
    return missing


def _filter_active_contracts(df_basic: pd.DataFrame, trade_date: str):
    return df_basic[(df_basic["list_date"] <= trade_date) & (df_basic["delist_date"] >= trade_date)]


def fill_missing(
    etf_code: str,
    start_date: str,
    end_date: str,
    only_missing: bool = True,
    sleep_s: float = 0.3,
    only_missing_days: bool = False,
):
    exchange, keyword = _get_exchange_and_keyword(etf_code)
    print(f"=== 补缺: {etf_code} {start_date} ~ {end_date} 交易所={exchange} ===")

    # 1) 先从 Tushare 拉完整合约列表 (L + D)，写入 option_basic
    print("[1/3] 拉取并更新合约列表 (L + D)...")
    df_basic = pro.opt_basic(
        exchange=exchange,
        list_status="L,D",
        fields="ts_code,name,call_put,exercise_price,list_date,delist_date",
    )
    df_basic = df_basic[df_basic["name"].str.contains(keyword)]
    df_basic["underlying"] = etf_code
    inserted = _upsert_option_basic(df_basic)
    print(f"  合约更新: {inserted} 条")

    # 2) 读取 DB 里的合约列表
    df_basic_db = _load_basic_from_db(etf_code)
    if df_basic_db.empty:
        print("  [!] option_basic 为空，停止")
        return

    # 3) 按交易日补行情
    trade_days = _trade_days(exchange, start_date, end_date)
    if only_missing_days:
        trade_days = _find_missing_days(etf_code, trade_days)
        print(f"[2/3] 缺口交易日数量: {len(trade_days)}")
    else:
        print(f"[2/3] 交易日数量: {len(trade_days)}")

    with engine.connect() as conn:
        for d in trade_days:
            active = _filter_active_contracts(df_basic_db, d)
            if active.empty:
                continue

            target_codes = active["ts_code"].tolist()
            if only_missing:
                sql = text(
                    """
                    SELECT ts_code FROM option_daily
                    WHERE trade_date = :d AND ts_code IN :codes
                    """
                )
                existing = conn.execute(sql, {"d": d, "codes": tuple(target_codes)}).fetchall()
                existing_codes = {r[0] for r in existing}
                need_codes = [c for c in target_codes if c not in existing_codes]
            else:
                need_codes = target_codes

            if not need_codes:
                continue

            df_daily = pro.opt_daily(
                trade_date=d,
                exchange=exchange,
                fields="ts_code,trade_date,close,oi,vol",
            )
            if df_daily.empty:
                continue

            df_save = df_daily[df_daily["ts_code"].isin(need_codes)]
            if df_save.empty:
                continue

            added = _upsert_option_daily(df_save, d)
            print(f"  {d}: +{added} 条")
            time.sleep(sleep_s)

    print("[3/3] 补缺完成")


def parse_args():
    parser = argparse.ArgumentParser(description="补齐 ETF 期权缺失行情")
    parser.add_argument("--etf", required=True, help="例如 510050.SH")
    parser.add_argument("--start", required=True, help="YYYYMMDD")
    parser.add_argument("--end", required=True, help="YYYYMMDD")
    parser.add_argument("--all", action="store_true", help="重抓全部（默认只补缺）")
    parser.add_argument("--only-missing-days", action="store_true", help="仅补缺口交易日")
    parser.add_argument("--sleep", type=float, default=0.3, help="请求间隔秒数")
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
    )
