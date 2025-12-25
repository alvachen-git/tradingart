import pandas as pd
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from langchain_core.tools import tool
# 引入 Pydantic 進行嚴格的參數控制
from pydantic import BaseModel, Field
import symbol_map
import traceback
import streamlit as st

# 1. 初始化環境
load_dotenv(override=True)
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")


def get_db_engine():
    if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]): return None
    db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(db_url)


engine = get_db_engine()


# --- 2. 定義 AI 調用工具時的參數結構 ---
class PriceStatsInput(BaseModel):
    query_list: str = Field(description="品種名稱，例如 '黃金' 或 '寧德時代,茅台'")
    start_date: str = Field(description="開始日期，格式必須是 YYYYMMDD，例如 '20231001'")
    end_date: str = Field(description="結束日期，格式必須是 YYYYMMDD，例如 '20231031'")


# --- 3. 輔助函數：清洗日期格式 ---
def clean_date_str(date_str: str) -> str:
    """防止 AI 傳入 '2023-01-01' 導致 SQL 查詢失敗，統一轉為 '20230101'"""
    if not date_str: return ""
    return date_str.replace("-", "").replace("/", "").replace(".", "").strip()


# --- 4. 核心工具：歷史價格統計 ---
@tool(args_schema=PriceStatsInput)
def get_price_statistics(query_list: str, start_date: str, end_date: str):
    """
    【区间行情統計工具】
    用於統計某段時間內的：最高价、最低价、區区间漲跌幅。
    适用于回答："上月最高价多少"、"上周誰漲得好"等问题。
    """
    if engine is None: return "❌ 數據庫連接失敗，請檢查 .env 配置"

    # A. 參數預處理
    s_date = clean_date_str(start_date)
    e_date = clean_date_str(end_date)
    queries = [q.strip() for q in query_list.split(',') if q.strip()]
    results = []

    for q in queries:
        # B. 解析代碼
        symbol_code, asset_type = symbol_map.resolve_symbol(q)
        if not symbol_code:
            results.append(f"⚠️ 未找到品種: {q}")
            continue

        try:
            target_code = symbol_code.upper()
            df = pd.DataFrame()

            # C. 構建查詢 (增加 pct_chg 字段)
            if asset_type == 'stock':
                codes_to_try = [target_code]
                if "." in target_code:
                    codes_to_try.append(target_code.split('.')[0])
                else:
                    codes_to_try.extend([f"{target_code}.SZ", f"{target_code}.SH"])
                code_str = "','".join(codes_to_try)

                # 【修改点 1】增加 pct_chg
                sql = text(f"""
                    SELECT trade_date, close_price as close, high_price as high, low_price as low, open_price as open,pct_chg 
                    FROM stock_price 
                    WHERE ts_code IN ('{code_str}')
                      AND trade_date >= :s_date 
                      AND trade_date <= :e_date
                    ORDER BY trade_date ASC
                """)
                df = pd.read_sql(sql, engine, params={"s_date": s_date, "e_date": e_date})

            else:
                # 【修改点 2】增加 pct_chg
                sql = text("""
                           SELECT trade_date, close_price as close, high_price as high, low_price as low, pct_chg
                           FROM futures_price
                           WHERE ts_code = :code
                             AND trade_date >= :s_date
                             AND trade_date <= :e_date
                           ORDER BY trade_date ASC
                           """)
                df = pd.read_sql(sql, engine, params={"code": target_code, "s_date": s_date, "e_date": e_date})

            # D. 數據計算
            if df.empty:
                results.append(f"⚠️ {q} ({target_code}): 在區間 {s_date}-{e_date} 無交易數據")
                continue

            start_price = df.iloc[0]['close']
            end_price = df.iloc[-1]['close']

            # 【核心修复逻辑】
            # 如果只查了一天 (start_date == end_date)，或者结果只有一行
            # 直接使用数据库里的 pct_chg (这是官方算好的，包含昨收信息)
            if len(df) == 1:
                period_chg = df.iloc[0]['pct_chg']
                if period_chg is None: period_chg = 0.0  # 防止空值
            else:
                # 如果是多天区间，计算累计涨幅
                if start_price == 0:
                    period_chg = 0.0
                else:
                    period_chg = (end_price - start_price) / start_price * 100

            max_price = df['high'].max()
            min_price = df['low'].min()
            max_date_row = df.loc[df['high'].idxmax()]
            max_date_str = str(max_date_row['trade_date'])

            # 优化输出文案
            results.append(f"""
📊 **{q} 行情统计 ({s_date} - {e_date})**
- **期间涨跌**: {period_chg:+.2f}% {'🔥' if period_chg > 0 else '💧'}
- **收盘价格**: {end_price}
- **最高价格**: {max_price} (出现在 {max_date_str})
- **最低价格**: {min_price}
            """)

        except Exception as e:
            print(f"計算 {q} 時出錯: {traceback.format_exc()}")
            results.append(f"❌ 計算 {q} 時發生錯誤: {e}")

    return "\n".join(results)


# --- 5. 另一個工具：獲取最新快照 (保持簡單可用) ---
@tool
def get_market_snapshot(query: str):
    """
    【最新行情查詢】
    用於查询最新价格
    輸入：品種名稱（如 "豆粕"、"茅台"）。
    """
    if engine is None: return "數據庫未連接"
    symbol_code, asset_type = symbol_map.resolve_symbol(query)
    if not symbol_code: return f"未找到 {query}"

    try:
        target_code = symbol_code.upper()
        if asset_type == 'stock':
            # 模糊匹配查詢最新一條
            sql = text(
                f"SELECT * FROM stock_price WHERE ts_code LIKE '{target_code}%' ORDER BY trade_date DESC LIMIT 1")
            df = pd.read_sql(sql, engine)
        else:
            sql = text("SELECT * FROM futures_price WHERE ts_code=:code ORDER BY trade_date DESC LIMIT 1")
            df = pd.read_sql(sql, engine, params={"code": target_code})

        if df.empty: return f"暫無 {query} 最新數據"

        row = df.iloc[0]
        price = row.get('close') or row.get('close_price')
        date = row['trade_date']
        return f"📍 **{query} 最新行情**\n日期: {date}\n价格: {price}\n(如需更多历史数据请询问具体时间段)"

    except Exception as e:
        return f"查询错误: {e}"

