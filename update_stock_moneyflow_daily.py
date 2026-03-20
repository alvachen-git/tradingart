from __future__ import annotations

import argparse
import time
from datetime import datetime
from typing import Optional

import pandas as pd
import tushare as ts
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(override=True)


def get_db_engine():
    import os

    user = os.getenv("DB_USER")
    pwd = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "3306")
    name = os.getenv("DB_NAME")
    if not all([user, pwd, host, name]):
        return None
    url = f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{name}"
    return create_engine(url, pool_pre_ping=True, pool_recycle=3600)


def get_tushare_pro():
    import os

    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        return None
    ts.set_token(token)
    return ts.pro_api()


def validate_trade_date(trade_date: Optional[str]) -> str:
    if not trade_date:
        return datetime.now().strftime("%Y%m%d")
    cleaned = str(trade_date).strip().replace("-", "")
    if len(cleaned) != 8 or not cleaned.isdigit():
        raise ValueError(f"非法交易日格式: {trade_date}, 期望 YYYYMMDD")
    return cleaned


def ensure_stock_moneyflow_daily_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS stock_moneyflow_daily (
                  trade_date VARCHAR(8) NOT NULL,
                  ts_code VARCHAR(16) NOT NULL,
                  net_mf_amount DOUBLE DEFAULT 0,
                  main_net_amount DOUBLE DEFAULT 0,
                  small_mid_net_amount DOUBLE DEFAULT 0,
                  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  PRIMARY KEY (trade_date, ts_code),
                  KEY idx_ts_code_date (ts_code, trade_date)
                ) DEFAULT CHARSET=utf8mb4
                """
            )
        )


def fetch_moneyflow_by_trade_date(pro, trade_date: str, retries: int = 3) -> pd.DataFrame:
    last_error = None
    for i in range(retries + 1):
        try:
            df = pro.moneyflow(trade_date=trade_date)
            return df if df is not None else pd.DataFrame()
        except Exception as e:
            last_error = e
            time.sleep(1.0 * (i + 1))
    raise RuntimeError(f"拉取 moneyflow 失败: {last_error}")


def transform_moneyflow_df(raw_df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    if raw_df is None or raw_df.empty:
        return pd.DataFrame(
            columns=[
                "trade_date",
                "ts_code",
                "net_mf_amount",
                "main_net_amount",
                "small_mid_net_amount",
            ]
        )

    df = raw_df.copy()
    for c in [
        "net_mf_amount",
        "buy_lg_amount",
        "buy_elg_amount",
        "sell_lg_amount",
        "sell_elg_amount",
        "buy_sm_amount",
        "buy_md_amount",
        "sell_sm_amount",
        "sell_md_amount",
    ]:
        if c not in df.columns:
            df[c] = 0.0

    df["ts_code"] = df["ts_code"].astype(str).str.strip().str.upper()
    df = df[df["ts_code"].str.endswith((".SH", ".SZ"))].copy()

    for c in [
        "net_mf_amount",
        "buy_lg_amount",
        "buy_elg_amount",
        "sell_lg_amount",
        "sell_elg_amount",
        "buy_sm_amount",
        "buy_md_amount",
        "sell_sm_amount",
        "sell_md_amount",
    ]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    df["main_net_amount"] = (
        df["buy_lg_amount"] + df["buy_elg_amount"] - df["sell_lg_amount"] - df["sell_elg_amount"]
    )
    df["small_mid_net_amount"] = (
        df["buy_sm_amount"] + df["buy_md_amount"] - df["sell_sm_amount"] - df["sell_md_amount"]
    )
    df["trade_date"] = trade_date

    out = df[
        ["trade_date", "ts_code", "net_mf_amount", "main_net_amount", "small_mid_net_amount"]
    ].copy()
    out = out.drop_duplicates(subset=["trade_date", "ts_code"], keep="last")
    out = out.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)
    return out


def run_update(trade_date: str, dry_run: bool = False) -> int:
    engine = get_db_engine()
    if engine is None:
        raise RuntimeError("数据库配置缺失，无法建立连接")

    pro = get_tushare_pro()
    if pro is None:
        raise RuntimeError("TUSHARE_TOKEN 缺失，无法拉取个股资金流")

    ensure_stock_moneyflow_daily_table(engine)

    print(f"🚀 开始更新 stock_moneyflow_daily | trade_date={trade_date} | dry_run={dry_run}")
    raw_df = fetch_moneyflow_by_trade_date(pro=pro, trade_date=trade_date, retries=3)
    if raw_df.empty:
        raise RuntimeError(f"{trade_date} 未拉到 moneyflow 数据")

    save_df = transform_moneyflow_df(raw_df, trade_date=trade_date)
    if save_df.empty:
        raise RuntimeError("转换后无可入库记录")

    print(f"📊 原始行数={len(raw_df)} | A股有效行数={len(save_df)}")
    print("🔎 预览前5行:")
    print(save_df.head(5).to_string(index=False))

    if dry_run:
        print("✅ dry-run 完成，未写入数据库")
        return len(save_df)

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM stock_moneyflow_daily WHERE trade_date=:d"), {"d": trade_date})
        save_df.to_sql(
            "stock_moneyflow_daily",
            conn,
            if_exists="append",
            index=False,
            chunksize=1000,
            method="multi",
        )

    print(f"✅ 写入完成: {trade_date} 共 {len(save_df)} 条")
    return len(save_df)


def main():
    parser = argparse.ArgumentParser(description="日更 A 股个股资金流到 stock_moneyflow_daily")
    parser.add_argument("--trade-date", type=str, default=None, help="交易日 YYYYMMDD，默认今天")
    parser.add_argument("--dry-run", action="store_true", help="只拉取与转换，不写库")
    args = parser.parse_args()

    d = validate_trade_date(args.trade_date)
    run_update(trade_date=d, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
