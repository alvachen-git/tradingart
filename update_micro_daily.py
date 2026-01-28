import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import yfinance as yf
import time
import os
import traceback
from dotenv import load_dotenv

# ==========================================
# 1. 配置区
# ==========================================
load_dotenv(override=True)

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME")

if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
    raise ValueError("❌ 数据库配置缺失！请检查 .env 文件")

DB_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
# 连接池配置：服务器长久运行时很重要
engine = create_engine(DB_URL, pool_recycle=3600, pool_pre_ping=True)

# 🔔 核心配置：每次只回溯更新过去 10 天的数据 (覆盖周末/节假日)
LOOKBACK_DAYS = 10


# ==========================================
# 2. 通用工具函数
# ==========================================
def get_date_range():
    """计算查询的开始和结束日期"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


def save_to_db(result_dict):
    """批量保存到数据库 (REPLACE INTO 模式)"""
    if not result_dict:
        return 0

    saved_count = 0
    with engine.begin() as conn:  # 使用事务
        for code, data in result_dict.items():
            df = data['df']
            name = data['name']
            category = data['category']

            if df.empty:
                continue

            # 写入前最后一次过滤，确保只写入最近的数据
            # 防止某些接口无视参数返回了全量历史
            start_str, _ = get_date_range()
            df = df[df['trade_date'] >= pd.to_datetime(start_str)]

            for _, row in df.iterrows():
                # 计算涨跌 (如果数据源没提供，简单计算)
                # 注意：增量更新时，很难计算涨跌幅(需要昨天的数据)，这里简化处理或依赖源数据
                change_val = row.get('change', 0.0)
                pct_chg = row.get('pct_chg', 0.0)

                sql = text("""
                           REPLACE
                           INTO macro_daily 
                    (trade_date, indicator_code, indicator_name, category, close_value, change_value, change_pct)
                    VALUES (:date, :code, :name, :cat, :val, :chg, :pct)
                           """)

                conn.execute(sql, {
                    "date": row['trade_date'],
                    "code": code,
                    "name": name,
                    "cat": category,
                    "val": row['close_value'],
                    "chg": change_val,
                    "pct": pct_chg
                })
                saved_count += 1

    return saved_count


# ==========================================
# 3. 各板块抓取函数 (高性能版)
# ==========================================

def fetch_us_bond_yields():
    """[增量] 美国国债收益率 (Investing.com)"""
    results = {}
    print(f"  🇺🇸 更新美债数据 (近{LOOKBACK_DAYS}天)...")

    start_date, end_date = get_date_range()

    # 映射关系: 期限 -> Investing的名称代码
    # 注意：Investing接口参数名可能变动，这里使用常见参数
    bonds = {
        "US2Y": "美国2年期国债收益率",
        "US10Y": "美国10年期国债收益率",
        "US30Y": "美国30年期国债收益率"
    }

    for code, name in bonds.items():
        try:
            time.sleep(1)  # 礼貌延时
            df = ak.index_investing_global(
                country="美国",
                index_name=name,
                period="每日",
                start_date=start_date,
                end_date=end_date
            )

            if not df.empty:
                df = df.rename(columns={'日期': 'trade_date', '收盘': 'close_value', '涨跌幅': 'pct_chg'})
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                df['close_value'] = pd.to_numeric(df['close_value'])
                # Investing 的涨跌幅是百分比字符串 '1.2%' -> 1.2
                # df['pct_chg'] = df['pct_chg'].str.rstrip('%').astype(float)

                results[code] = {'df': df, 'name': name, 'category': 'bond'}
                print(f"    ✓ {code}: 获取 {len(df)} 条")
        except Exception as e:
            print(f"    ❌ {code} 失败: {e}")

    return results


def fetch_china_bond_yields():
    """[增量] 中国国债收益率 (AkShare)"""
    results = {}
    print(f"  🇨🇳 更新中债数据 (近{LOOKBACK_DAYS}天)...")

    # AkShare 的中债接口通常返回近一年的，我们在内存截取
    try:
        # 这里的接口比较多变，推荐使用 bond_china_yield
        df = ak.bond_china_yield(start_date=get_date_range()[0].replace('-', ''),
                                 end_date=get_date_range()[1].replace('-', ''))

        # 假设返回: 日期, 2年, 10年, 30年
        if not df.empty:
            df['日期'] = pd.to_datetime(df['日期'])

            # 2年期
            df_2y = df[['日期', '2年']].rename(columns={'日期': 'trade_date', '2年': 'close_value'})
            results['CN2Y'] = {'df': df_2y, 'name': '中债2年收益率', 'category': 'bond'}

            # 10年期
            df_10y = df[['日期', '10年']].rename(columns={'日期': 'trade_date', '10年': 'close_value'})
            results['CN10Y'] = {'df': df_10y, 'name': '中债10年收益率', 'category': 'bond'}

            print(f"    ✓ 中债数据: 获取 {len(df)} 条")

    except Exception as e:
        print(f"    ❌ 中债失败: {e}")

    return results


def fetch_dxy_investing():
    """[增量] 美元指数 (Investing)"""
    results = {}
    print(f"  💵 更新美元指数 (Investing)...")
    start_date, end_date = get_date_range()

    try:
        df = ak.index_investing_global(
            country="美国",
            index_name="美元指数",
            period="每日",
            start_date=start_date,
            end_date=end_date
        )
        if not df.empty:
            df = df.rename(columns={'日期': 'trade_date', '收盘': 'close_value'})
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            df['close_value'] = pd.to_numeric(df['close_value'])
            results['DXY'] = {'df': df, 'name': '美元指数', 'category': 'fx'}
            print(f"    ✓ DXY: 获取 {len(df)} 条")
    except Exception as e:
        print(f"    ❌ DXY 失败: {e}")
    return results


def fetch_offshore_cny_yahoo():
    """[增量] 离岸人民币 (Yahoo Finance)"""
    results = {}
    print(f"  💱 更新离岸人民币 (Yahoo)...")

    try:
        # CNH=F 是 Yahoo 的离岸人民币代码
        # period='1mo' 非常快，且足够覆盖lookback
        ticker = yf.Ticker("CNH=F")
        df = ticker.history(period="1mo")

        if not df.empty:
            df = df.reset_index()
            # 统一时区
            df['Date'] = df['Date'].dt.tz_localize(None)

            # 截取最近 N 天
            start_dt = pd.to_datetime(get_date_range()[0])
            df = df[df['Date'] >= start_dt]

            temp_df = df[['Date', 'Close']].rename(columns={'Date': 'trade_date', 'Close': 'close_value'})
            results['USDCNH'] = {'df': temp_df, 'name': '离岸人民币', 'category': 'fx'}
            print(f"    ✓ USDCNH: 获取 {len(temp_df)} 条")
    except Exception as e:
        print(f"    ❌ USDCNH 失败: {e}")
    return results


def fetch_bdi_index():
    """[增量] BDI 指数 (AkShare)"""
    results = {}
    print(f"  🚢 更新波罗的海指数 (BDI)...")

    try:
        # 这个接口通常返回全量，必须做切片
        df = ak.index_bdi()
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])

            # 🔥 内存切片，只留最近10天
            start_dt = pd.to_datetime(get_date_range()[0])
            df = df[df['date'] >= start_dt]

            df = df.rename(columns={'date': 'trade_date', 'close': 'close_value'})
            results['BDI'] = {'df': df, 'name': '波罗的海干散货指数', 'category': 'shipping'}
            print(f"    ✓ BDI: 获取 {len(df)} 条")
    except Exception as e:
        print(f"    ❌ BDI 失败: {e}")
    return results


# ==========================================
# 4. 主执行函数
# ==========================================
def run_daily_update():
    print(f"\n{'=' * 40}")
    print(f"🚀 宏观数据每日更新任务 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    print(f"📅 更新窗口: {get_date_range()[0]} 至 {get_date_range()[1]}")
    print(f"{'=' * 40}\n")

    total_saved = 0

    # 1. 债券
    res = fetch_us_bond_yields()
    total_saved += save_to_db(res)

    res = fetch_china_bond_yields()
    total_saved += save_to_db(res)

    # 2. 汇率
    res = fetch_dxy_investing()
    total_saved += save_to_db(res)

    res = fetch_offshore_cny_yahoo()
    total_saved += save_to_db(res)

    # 3. 航运
    res = fetch_bdi_index()
    total_saved += save_to_db(res)

    print(f"\n✅ 任务完成! 本次共更新/插入 {total_saved} 条数据。")


if __name__ == "__main__":
    try:
        run_daily_update()
    except Exception as e:
        print(f"\n💥 致命错误: {e}")
        traceback.print_exc()