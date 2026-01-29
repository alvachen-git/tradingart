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

# 🔔 核心配置：每次只回溯更新过去 N 天的数据 (覆盖周末/节假日)
LOOKBACK_DAYS = 5


# ==========================================
# 2. 通用工具函数
# ==========================================
def get_date_range():
    """计算查询的开始和结束日期"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


def save_to_db(result_dict):
    """批量保存到数据库"""
    if not result_dict:
        return 0
    saved_count = 0
    with engine.begin() as conn:
        for code, data in result_dict.items():
            df = data['df']
            if df.empty:
                continue

            # 双重保险：过滤旧数据
            start_str, _ = get_date_range()
            df = df[df['trade_date'] >= pd.to_datetime(start_str)]

            for _, row in df.iterrows():
                # 安全获取字段
                chg = row.get('change', 0.0)
                pct = row.get('pct_chg', 0.0)
                if pd.isna(chg):
                    chg = 0.0
                if pd.isna(pct):
                    pct = 0.0

                sql = text("""
                           REPLACE
                           INTO macro_daily 
                    (trade_date, indicator_code, indicator_name, category, close_value, change_value, change_pct)
                    VALUES (:date, :code, :name, :cat, :val, :chg, :pct)
                           """)
                conn.execute(sql, {
                    "date": row['trade_date'],
                    "code": code,
                    "name": data['name'],
                    "cat": data['category'],
                    "val": row['close_value'],
                    "chg": chg,
                    "pct": pct
                })
                saved_count += 1
    return saved_count


# ==========================================
# 3. 各板块抓取函数 (修复版)
# ==========================================

def fetch_bond_yields():
    """[修复] 中美国债收益率 (东方财富-AkShare)

    使用接口: ak.bond_zh_us_rate()
    返回列: 日期, 中国国债收益率2年, 中国国债收益率5年, 中国国债收益率10年, 中国国债收益率30年,
            美国国债收益率2年, 美国国债收益率5年, 美国国债收益率10年, 美国国债收益率30年,
            美国国债收益率10年-2年, 美国GDP年增率
    """
    results = {}
    print(f"  🇺🇸🇨🇳 更新中美国债数据 (近{LOOKBACK_DAYS}天)...")
    start_date, end_date = get_date_range()

    try:
        # 使用正确的接口
        df = ak.bond_zh_us_rate(start_date=start_date.replace("-", ""))

        if df.empty:
            print("    ⚠️ 中美国债数据为空")
            return results

        # 统一日期格式
        df['日期'] = pd.to_datetime(df['日期'])

        # 过滤最近 N 天
        start_dt = pd.to_datetime(start_date)
        df = df[df['日期'] >= start_dt]

        # --- 美国10年期国债 ---
        if '美国国债收益率10年' in df.columns:
            temp_df = df[['日期', '美国国债收益率10年']].copy()
            temp_df = temp_df.rename(columns={'日期': 'trade_date', '美国国债收益率10年': 'close_value'})
            temp_df['close_value'] = pd.to_numeric(temp_df['close_value'], errors='coerce')
            temp_df = temp_df.dropna(subset=['close_value'])
            results['US10Y'] = {'df': temp_df, 'name': '美国10年期国债收益率', 'category': 'bond'}
            print(f"    ✓ US10Y: 获取 {len(temp_df)} 条")

        # --- 美国2年期国债 ---
        if '美国国债收益率2年' in df.columns:
            temp_df = df[['日期', '美国国债收益率2年']].copy()
            temp_df = temp_df.rename(columns={'日期': 'trade_date', '美国国债收益率2年': 'close_value'})
            temp_df['close_value'] = pd.to_numeric(temp_df['close_value'], errors='coerce')
            temp_df = temp_df.dropna(subset=['close_value'])
            results['US2Y'] = {'df': temp_df, 'name': '美国2年期国债收益率', 'category': 'bond'}
            print(f"    ✓ US2Y: 获取 {len(temp_df)} 条")

        # --- 中国10年期国债 ---
        if '中国国债收益率10年' in df.columns:
            temp_df = df[['日期', '中国国债收益率10年']].copy()
            temp_df = temp_df.rename(columns={'日期': 'trade_date', '中国国债收益率10年': 'close_value'})
            temp_df['close_value'] = pd.to_numeric(temp_df['close_value'], errors='coerce')
            temp_df = temp_df.dropna(subset=['close_value'])
            results['CN10Y'] = {'df': temp_df, 'name': '中国10年期国债收益率', 'category': 'bond'}
            print(f"    ✓ CN10Y: 获取 {len(temp_df)} 条")

        # --- 中国2年期国债 ---
        if '中国国债收益率2年' in df.columns:
            temp_df = df[['日期', '中国国债收益率2年']].copy()
            temp_df = temp_df.rename(columns={'日期': 'trade_date', '中国国债收益率2年': 'close_value'})
            temp_df['close_value'] = pd.to_numeric(temp_df['close_value'], errors='coerce')
            temp_df = temp_df.dropna(subset=['close_value'])
            results['CN2Y'] = {'df': temp_df, 'name': '中国2年期国债收益率', 'category': 'bond'}
            print(f"    ✓ CN2Y: 获取 {len(temp_df)} 条")

        # --- 美国10Y-2Y利差 ---
        if '美国国债收益率10年-2年' in df.columns:
            temp_df = df[['日期', '美国国债收益率10年-2年']].copy()
            temp_df = temp_df.rename(columns={'日期': 'trade_date', '美国国债收益率10年-2年': 'close_value'})
            temp_df['close_value'] = pd.to_numeric(temp_df['close_value'], errors='coerce')
            temp_df = temp_df.dropna(subset=['close_value'])
            results['US10Y2Y'] = {'df': temp_df, 'name': '美国10Y-2Y利差', 'category': 'bond'}
            print(f"    ✓ US10Y2Y: 获取 {len(temp_df)} 条")

    except Exception as e:
        print(f"    ❌ 中美国债数据获取失败: {e}")
        traceback.print_exc()

    return results


def fetch_dxy_index():
    """[修复] 美元指数 (东方财富-AkShare)

    使用接口: ak.index_global_hist_em(symbol="美元指数")
    返回列: 日期, 代码, 名称, 今开, 最新价, 最高, 最低, 振幅
    """
    results = {}
    print(f"  💵 更新美元指数 (近{LOOKBACK_DAYS}天)...")
    start_date, _ = get_date_range()

    try:
        # 使用正确的接口
        df = ak.index_global_hist_em(symbol="美元指数")

        if df.empty:
            print("    ⚠️ 美元指数数据为空")
            return results

        # 统一日期格式
        df['日期'] = pd.to_datetime(df['日期'])

        # 过滤最近 N 天
        start_dt = pd.to_datetime(start_date)
        df = df[df['日期'] >= start_dt]

        # 提取需要的列
        temp_df = df[['日期', '最新价']].copy()
        temp_df = temp_df.rename(columns={'日期': 'trade_date', '最新价': 'close_value'})
        temp_df['close_value'] = pd.to_numeric(temp_df['close_value'], errors='coerce')
        temp_df = temp_df.dropna(subset=['close_value'])

        results['DXY'] = {'df': temp_df, 'name': '美元指数', 'category': 'fx'}
        print(f"    ✓ DXY: 获取 {len(temp_df)} 条")

    except Exception as e:
        print(f"    ❌ DXY 失败: {e}")
        traceback.print_exc()

    return results


def fetch_offshore_cny_yahoo():
    """[保持不变] 离岸人民币 (Yahoo Finance)"""
    results = {}
    print(f"  💱 更新离岸人民币 (Yahoo)...")

    try:
        # CNH=F 是 Yahoo 的离岸人民币代码
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
        traceback.print_exc()

    return results


def fetch_bdi_index():
    """[修复] BDI 波罗的海干散货指数 (东方财富-AkShare)

    使用接口: ak.macro_shipping_bdi()
    返回列: 日期, 最新值, 涨跌幅, 近1月涨跌幅, 近3月涨跌幅, 近6月涨跌幅, 近1年涨跌幅, 近2年涨跌幅, 近3年涨跌幅
    """
    results = {}
    print(f"  🚢 更新波罗的海指数 (BDI)...")

    try:
        # 使用正确的接口
        df = ak.macro_shipping_bdi()

        if df.empty:
            print("    ⚠️ BDI数据为空")
            return results

        # 统一日期格式
        df['日期'] = pd.to_datetime(df['日期'])

        # 过滤最近 N 天
        start_dt = pd.to_datetime(get_date_range()[0])
        df = df[df['日期'] >= start_dt]

        # 提取需要的列
        temp_df = df[['日期', '最新值']].copy()
        temp_df = temp_df.rename(columns={'日期': 'trade_date', '最新值': 'close_value'})
        temp_df['close_value'] = pd.to_numeric(temp_df['close_value'], errors='coerce')

        # 添加涨跌幅（如果有的话）
        if '涨跌幅' in df.columns:
            temp_df['pct_chg'] = pd.to_numeric(df['涨跌幅'], errors='coerce')

        temp_df = temp_df.dropna(subset=['close_value'])

        results['BDI'] = {'df': temp_df, 'name': '波罗的海干散货指数', 'category': 'shipping'}
        print(f"    ✓ BDI: 获取 {len(temp_df)} 条")

    except Exception as e:
        print(f"    ❌ BDI 失败: {e}")
        traceback.print_exc()

    return results


# ==========================================
# 4. 主执行函数
# ==========================================
def run_daily_update():
    print(f"\n{'=' * 50}")
    print(f"🚀 宏观数据每日更新任务 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    print(f"📅 更新窗口: {get_date_range()[0]} ~ {get_date_range()[1]}")
    print(f"{'=' * 50}\n")

    total = 0

    # 中美国债 (一个接口获取所有)
    total += save_to_db(fetch_bond_yields())

    # 美元指数
    total += save_to_db(fetch_dxy_index())

    # 离岸人民币
    total += save_to_db(fetch_offshore_cny_yahoo())

    # BDI指数
    total += save_to_db(fetch_bdi_index())

    print(f"\n✅ 任务完成! 共更新 {total} 条数据。")


if __name__ == "__main__":
    try:
        run_daily_update()
    except Exception as e:
        print(f"\n💥 致命错误: {e}")
        traceback.print_exc()