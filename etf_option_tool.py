import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import streamlit as st
from datetime import datetime, timedelta
import tushare as ts

# 1. 初始化配置
load_dotenv(override=True)

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Tushare 初始化
ts_token = os.getenv("TUSHARE_TOKEN")
if ts_token:
    ts.set_token(ts_token)
    pro = ts.pro_api()


# 数据库引擎 (使用 cache_resource 避免重复连接)
@st.cache_resource
def get_db_engine():
    if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]): return None
    db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(db_url)


engine = get_db_engine()


# ==========================================
#   功能 1: ETF 期权主力持仓防线分析
# ==========================================
@st.cache_data(ttl=600)
def get_etf_option_analysis(etf_code="510050", days=100):
    if engine is None: return None

    # 智能后缀补全
    if "." not in etf_code:
        if etf_code.startswith("15") or etf_code.startswith("16"):
            etf_code += ".SZ"
        else:
            etf_code += ".SH"

    print(f"[*] 正在从数据库分析 {etf_code} (独立模块)...")

    try:
        # 1. 确定日期范围
        date_limit_sql = f"SELECT DISTINCT trade_date FROM option_daily ORDER BY trade_date DESC LIMIT {days}"
        dates_df = pd.read_sql(date_limit_sql, engine)
        if dates_df.empty: return None
        min_date = dates_df['trade_date'].min()

        # 2. 执行 SQL 查询 (期权数据)
        sql = f"""
            SELECT 
                d.trade_date as date,
                b.call_put,
                b.exercise_price as strike,
                d.oi,
                d.close as price,
                d.ts_code as code
            FROM option_daily d
            JOIN option_basic b ON d.ts_code = b.ts_code
            WHERE b.underlying = '{etf_code}'
              AND d.trade_date >= '{min_date}'
              AND d.oi > 0
        """

        df_raw = pd.read_sql(sql, engine)
        if df_raw.empty: return None

        df_raw['type'] = df_raw['call_put'].map({'C': '认购', 'P': '认沽'})

        # 2.5 获取标的价格 (新增步骤：用于判断行权价偏离度)
        kline_sql = f"""
            SELECT trade_date as date, close_price as underlying_price
            FROM stock_price 
            WHERE ts_code='{etf_code}' 
              AND trade_date >= '{min_date}'
        """
        df_stock = pd.read_sql(kline_sql, engine)

        # 将标的价格合并到期权数据中
        if not df_stock.empty:
            df_raw = pd.merge(df_raw, df_stock, on='date', how='left')
        else:
            df_raw['underlying_price'] = np.nan

        # 3. 每日候选池构建
        daily_candidates_map = {}
        grouped = df_raw.groupby(['date', 'type'])

        for (date, otype), group in grouped:
            if group.empty: continue

            # 按持仓量降序排列
            group = group.sort_values('oi', ascending=False)

            # --- 新增逻辑：10% 偏离度过滤 ---
            # 获取当日标的价格
            u_price = group['underlying_price'].iloc[0] if 'underlying_price' in group.columns else np.nan

            candidates_list = []

            # 如果有标的价格，优先寻找偏离度 <= 10% 的合约
            if pd.notna(u_price) and u_price > 0:
                filtered_group = group[abs(group['strike'] - u_price) / u_price <= 0.1]

                # 如果过滤后有合约，取前3名
                if not filtered_group.empty:
                    top3_df = filtered_group.head(3)
                else:
                    # 如果所有合约都偏离很大（极端情况），回退到原始 Top3
                    top3_df = group.head(3)
            else:
                # 如果没有标的价格数据，回退到原始 Top3
                top3_df = group.head(3)

            for _, row in top3_df.iterrows():
                candidates_list.append({
                    'strike': row['strike'], 'oi': row['oi'],
                    'price': row['price'], 'code': row['code']
                })

            if date not in daily_candidates_map: daily_candidates_map[date] = {}
            daily_candidates_map[date][otype] = candidates_list

        # 4. 智能平滑算法 (保持不变，但输入数据已优化)
        final_results = []
        sorted_dates = sorted(daily_candidates_map.keys())

        for otype_raw in ['认购', '认沽']:
            type_label = f"{otype_raw} ({'压力' if otype_raw == '认购' else '支撑'})"
            last_strike = None

            for date in sorted_dates:
                day_data = daily_candidates_map[date]
                if otype_raw not in day_data: continue

                candidates = day_data[otype_raw]
                if not candidates: continue

                selected = candidates[0]

                if last_strike is not None:
                    diff1 = abs(selected['strike'] - last_strike) / last_strike
                    if diff1 > 0.05:
                        if len(candidates) > 1:
                            cand2 = candidates[1]
                            diff2 = abs(cand2['strike'] - last_strike) / last_strike
                            if diff2 <= 0.05:
                                selected = cand2
                            elif len(candidates) > 2:
                                cand3 = candidates[2]
                                diff3 = abs(cand3['strike'] - last_strike) / last_strike
                                if diff3 <= 0.05: selected = cand3

                last_strike = selected['strike']

                final_results.append({
                    'date': date, 'type': type_label,
                    'strike': selected['strike'], 'oi': selected['oi'],
                    'price': selected['price'], 'code': selected['code']
                })

        return pd.DataFrame(final_results)

    except Exception as e:
        print(f" [!] 分析出错: {e}")
        return None


# ==========================================
#   功能 2: IV Rank 计算
# ==========================================
@st.cache_data(ttl=600)
def get_iv_rank_data(etf_code, window=252):
    if engine is None: return None
    if "." not in etf_code:
        etf_code += ".SZ" if etf_code.startswith("15") else ".SH"

    try:
        sql = f"""
            SELECT trade_date, iv FROM etf_iv_history 
            WHERE etf_code='{etf_code}' ORDER BY trade_date DESC LIMIT {window}
        """
        df = pd.read_sql(sql, engine)
        if df.empty or len(df) < 10: return None

        current_iv = df.iloc[0]['iv']
        max_iv = df['iv'].max()
        min_iv = df['iv'].min()

        iv_rank = 0 if max_iv == min_iv else (current_iv - min_iv) / (max_iv - min_iv) * 100
        count_below = len(df[df['iv'] < current_iv])
        iv_percentile = (count_below / len(df)) * 100

        return {
            "current_iv": current_iv, "iv_rank": iv_rank,
            "iv_percentile": iv_percentile, "max_iv": max_iv, "min_iv": min_iv,
            "date": df.iloc[0]['trade_date']
        }
    except:
        return None


# ==========================================
#   功能 3: 获取绘图数据 (K线 + IV) - 新增
# ==========================================
@st.cache_data(ttl=600)
def get_kline_and_iv_data(etf_code, limit=100):
    """
    一次性获取清洗好的 K 线和 IV 数据，供前端直接绘图
    """
    if engine is None: return pd.DataFrame(), pd.DataFrame()

    # 补全后缀
    if "." not in etf_code:
        etf_code += ".SZ" if etf_code.startswith("15") else ".SH"

    try:
        # 1. 获取 IV 数据
        iv_sql = f"SELECT * FROM etf_iv_history WHERE etf_code='{etf_code}' ORDER BY trade_date"
        df_iv = pd.read_sql(iv_sql, engine)

        # 2. 获取 K 线数据 (使用别名映射)
        kline_sql = f"""
            SELECT trade_date, name,
                   open_price as open, high_price as high, 
                   low_price as low, close_price as close 
            FROM stock_price 
            WHERE ts_code='{etf_code}' 
            ORDER BY trade_date DESC LIMIT {limit}
        """
        df_kline = pd.read_sql(kline_sql, engine).sort_values('trade_date')

        return df_kline, df_iv

    except Exception as e:
        print(f"获取绘图数据出错: {e}")
        return pd.DataFrame(), pd.DataFrame()