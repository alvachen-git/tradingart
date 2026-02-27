import tushare as ts
import pandas as pd
import os
import argparse
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import time
from datetime import datetime, timedelta

# 1. 初始化
load_dotenv(override=True)
TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN")
pro = ts.pro_api(TUSHARE_TOKEN) if TUSHARE_TOKEN else None

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)


def _normalize_trade_date(value):
    """把数据库里的日期值规范成 YYYYMMDD 字符串。"""
    if value is None:
        return None
    s = str(value).strip().replace("-", "")
    if len(s) == 8 and s.isdigit():
        return s
    return None


def get_latest_saved_trade_date():
    """读取 sector_moneyflow 里已保存的最新交易日。"""
    if engine is None:
        return None
    with engine.connect() as conn:
        latest = conn.execute(text("SELECT MAX(trade_date) FROM sector_moneyflow")).scalar()
    return _normalize_trade_date(latest)


def build_target_trade_dates(recent_days=None, bootstrap_days=5, overlap_days=2):
    """
    默认增量更新：
    - 若表里已有数据：从最新日期往回 overlap_days 天开始补到今天
    - 若表为空：回补 bootstrap_days 天
    也支持 --recent-days 强制只更新最近 N 天。
    """
    today = datetime.now().date()

    if recent_days is not None:
        recent_days = max(1, int(recent_days))
        start_date = today - timedelta(days=recent_days - 1)
        print(f"🗓️ 更新模式: recent_days={recent_days}, 区间 {start_date} ~ {today}")
    else:
        latest_str = get_latest_saved_trade_date()
        if latest_str:
            latest_dt = datetime.strptime(latest_str, "%Y%m%d").date()
            start_date = latest_dt - timedelta(days=max(0, int(overlap_days)))
            print(
                f"🗓️ 更新模式: incremental(latest={latest_str}, overlap_days={overlap_days}), 区间 {start_date} ~ {today}"
            )
        else:
            bootstrap_days = max(1, int(bootstrap_days))
            start_date = today - timedelta(days=bootstrap_days - 1)
            print(f"🗓️ 更新模式: bootstrap_days={bootstrap_days}, 区间 {start_date} ~ {today}")

    if start_date > today:
        start_date = today

    dates = []
    current = start_date
    while current <= today:
        if current.weekday() < 5:
            dates.append(current.strftime('%Y%m%d'))
        current += timedelta(days=1)
    return dates


def ensure_sector_moneyflow_schema():
    """
    兼容旧表结构：
    旧主键是 (trade_date, industry)，会和新版的“行业/概念同名”数据冲突。
    新主键升级为 (trade_date, industry, sector_type)。
    """
    if engine is None:
        raise ValueError("数据库引擎未初始化")

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS sector_moneyflow (
                trade_date VARCHAR(8) NOT NULL,
                industry VARCHAR(128) NOT NULL,
                sector_type VARCHAR(20) NOT NULL DEFAULT '行业',
                main_net_inflow DOUBLE,
                medium_net_inflow DOUBLE,
                small_net_inflow DOUBLE,
                total_turnover DOUBLE,
                pct_change DOUBLE,
                net_rate DOUBLE,
                PRIMARY KEY (trade_date, industry, sector_type)
            ) DEFAULT CHARSET=utf8mb4
        """))

        col_rows = conn.execute(text("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA=:db_name AND TABLE_NAME='sector_moneyflow'
        """), {"db_name": DB_NAME}).fetchall()
        col_set = {r[0] for r in col_rows}

        if "sector_type" not in col_set:
            conn.execute(text("""
                ALTER TABLE sector_moneyflow
                ADD COLUMN sector_type VARCHAR(20) NOT NULL DEFAULT '行业' AFTER industry
            """))

        # 兜底，避免 NULL/空字符串导致主键构建失败
        conn.execute(text("""
            UPDATE sector_moneyflow
            SET sector_type='行业'
            WHERE sector_type IS NULL OR sector_type=''
        """))

        pk_rows = conn.execute(text("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA=:db_name
              AND TABLE_NAME='sector_moneyflow'
              AND CONSTRAINT_NAME='PRIMARY'
            ORDER BY ORDINAL_POSITION
        """), {"db_name": DB_NAME}).fetchall()
        pk_cols = [r[0] for r in pk_rows]

        target_pk = ['trade_date', 'industry', 'sector_type']
        if pk_cols != target_pk:
            if pk_cols:
                conn.execute(text("ALTER TABLE sector_moneyflow DROP PRIMARY KEY"))
            conn.execute(text("""
                ALTER TABLE sector_moneyflow
                ADD PRIMARY KEY (trade_date, industry, sector_type)
            """))
            print(f"   🔧 已完成 sector_moneyflow 主键迁移: {pk_cols} -> {target_pk}")


def fetch_sector_moneyflow(trade_date):
    print(f"🚀 正在抓取 {trade_date} 的资金流 (行业+概念)...")

    try:
        if pro is None:
            raise ValueError("TUSHARE_TOKEN 缺失，无法调用 tushare 接口")

        # 接口: moneyflow_ind_dc (东财源)
        df = pro.moneyflow_ind_dc(trade_date=trade_date)

        if df.empty:
            print(f"   ⚠️ {trade_date} 无数据")
            return

        # --- 1. 数据分类 (关键修改) ---
        # 我们不再过滤 content_type，而是把它保留下来
        # content_type 只有两个值：'行业' 或 '概念'
        if 'content_type' not in df.columns:
            df['content_type'] = '未知'  # 防止接口变动

        # 补全字段
        expected_cols = ['net_amount', 'buy_md_amount', 'buy_sm_amount', 'net_amount_rate']
        for col in expected_cols:
            if col not in df.columns: df[col] = 0.0

        # --- 2. 核心计算 (单位：万元) ---
        df['main_net_inflow'] = df['net_amount'] / 10000.0
        df['medium_net_inflow'] = df['buy_md_amount'] / 10000.0
        df['small_net_inflow'] = df['buy_sm_amount'] / 10000.0

        # 反推成交额
        def calc_turnover(row):
            try:
                rate = row['net_amount_rate']
                net = row['net_amount']
                if rate != 0: return (net / (rate / 100.0)) / 10000.0
                return 0
            except:
                return 0

        df['total_turnover'] = df.apply(calc_turnover, axis=1)

        if 'net_amount_rate' in df.columns:
            df['net_rate'] = df['net_amount_rate']
        else:
            df['net_rate'] = 0

        if 'name' in df.columns: df.rename(columns={'name': 'industry'}, inplace=True)
        if 'pct_change' not in df.columns: df['pct_change'] = 0

        # --- 3. 构造入库数据 ---
        # 新增 sector_type 字段，对应接口里的 content_type
        df.rename(columns={'content_type': 'sector_type'}, inplace=True)

        data_to_save = df[[
            'trade_date', 'industry', 'sector_type',  # <--- 新增 sector_type
            'main_net_inflow', 'medium_net_inflow', 'small_net_inflow',
            'total_turnover', 'pct_change', 'net_rate'
        ]].copy()

        data_to_save.fillna(0, inplace=True)
        # 去重：同一天、同一个名字、同一个类型
        data_to_save = data_to_save.drop_duplicates(subset=['trade_date', 'industry', 'sector_type'])

        # --- 入库 ---
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM sector_moneyflow WHERE trade_date=:d"), {"d": trade_date})
            data_to_save.to_sql('sector_moneyflow', conn, if_exists='append', index=False)

        print(
            f"   ✅ 入库完成: 行业 {len(data_to_save[data_to_save['sector_type'] == '行业'])} 条 | 概念 {len(data_to_save[data_to_save['sector_type'] == '概念'])} 条")

    except Exception as e:
        print(f"   ❌ 抓取失败: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="更新行业/概念资金流数据")
    parser.add_argument(
        "--recent-days",
        type=int,
        default=None,
        help="强制只更新最近 N 个自然日（包含今天）。例如 --recent-days 7",
    )
    parser.add_argument(
        "--bootstrap-days",
        type=int,
        default=5,
        help="当表为空时，首次回补天数（默认 5）",
    )
    parser.add_argument(
        "--overlap-days",
        type=int,
        default=2,
        help="增量更新时，从最新日期往回重叠更新天数（默认 2）",
    )
    args = parser.parse_args()

    ensure_sector_moneyflow_schema()
    target_dates = build_target_trade_dates(
        recent_days=args.recent_days,
        bootstrap_days=args.bootstrap_days,
        overlap_days=args.overlap_days,
    )
    if not target_dates:
        print("⚠️ 当前区间无交易日，无需更新。")
    else:
        for d_str in target_dates:
            fetch_sector_moneyflow(d_str)
            time.sleep(1)
