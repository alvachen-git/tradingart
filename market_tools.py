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
    return create_engine(
        db_url,
        pool_pre_ping=True,      # 每次查询前检查连接是否有效
        pool_recycle=7200,       # 1小时回收连接
        pool_size=5,             # 连接池大小
        max_overflow=10          # 最大溢出连接数
    )


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
                is_hk = target_code.endswith('.HK')

                if is_hk:
                    # 🔥【港股】使用精确匹配，避免01810.HK和81810.HK混淆
                    sql = text("""
                               SELECT trade_date,
                                      close_price as close, high_price as high, 
                               low_price as low, open_price as open, pct_chg
                               FROM stock_price
                               WHERE ts_code = :code
                                 AND trade_date >= :s_date
                                 AND trade_date <= :e_date
                               ORDER BY trade_date ASC
                               """)
                    df = pd.read_sql(sql, engine, params={
                        "code": target_code,
                        "s_date": s_date,
                        "e_date": e_date
                    })
                else:
                    # 【A股】保持原逻辑
                    codes_to_try = [target_code]
                    if "." in target_code:
                        codes_to_try.append(target_code.split('.')[0])
                    else:
                        codes_to_try.extend([f"{target_code}.SZ", f"{target_code}.SH"])
                    code_str = "','".join(set(codes_to_try))

                    sql = text(f"""
                        SELECT trade_date, close_price as close, high_price as high, 
                               low_price as low, open_price as open, pct_chg 
                        FROM stock_price 
                        WHERE ts_code IN ('{code_str}')
                          AND trade_date >= :s_date 
                          AND trade_date <= :e_date
                        ORDER BY trade_date ASC
                    """)
                    df = pd.read_sql(sql, engine, params={"s_date": s_date, "e_date": e_date})

            # --- 分支 2: 指数 (新增) ---
            elif asset_type == 'index':
                # 指数表结构现在包含了 high_price, low_price
                sql = text("""
                    SELECT trade_date, 
                           close_price as close, 
                           high_price as high, 
                           low_price as low, 
                           pct_chg 
                    FROM index_price 
                    WHERE ts_code = :code
                      AND trade_date >= :s_date 
                      AND trade_date <= :e_date
                    ORDER BY trade_date ASC
                """)
                df = pd.read_sql(sql, engine, params={"code": target_code, "s_date": s_date, "e_date": e_date})

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
- **收盘价**: {end_price}
- **最高价**: {max_price} (出现在 {max_date_str})
- **最低价**: {min_price}
            """)

        except Exception as e:
            print(f"計算 {q} 時出錯: {traceback.format_exc()}")
            results.append(f"❌ 計算 {q} 時發生錯誤: {e}")

    return "\n".join(results)


# --- 5. 另一個工具：獲取最新快照 (保持簡單可用) ---
@tool
def get_market_snapshot(query: str):
    """
    【数据库最新日线查询】
    查询数据库中已收录的**最新一条日线记录**
    输入：品种名称（如 "豆粕"、"茅台"）。
    """
    if engine is None: return "數據庫未連接"
    symbol_code, asset_type = symbol_map.resolve_symbol(query)
    if not symbol_code: return f"未找到 {query}"

    try:
        target_code = symbol_code.upper()
        if asset_type == 'stock':
            # 模糊匹配查詢最新一條
            sql = text("""
                       SELECT ts_code, name, trade_date, close_price, pct_chg
                       FROM stock_price
                       WHERE ts_code = :code
                       ORDER BY trade_date DESC LIMIT 1
                       """)
            df = pd.read_sql(sql, engine, params={"code": target_code})

        elif asset_type == 'index':
            # 新增指数查询
            sql = text(f"SELECT * FROM index_price WHERE ts_code = :code ORDER BY trade_date DESC LIMIT 1")
            df = pd.read_sql(sql, engine, params={"code": target_code})

        else:
            sql = text("SELECT * FROM futures_price WHERE ts_code LIKE :code AND ts_code NOT LIKE '%TAS%' ORDER BY trade_date DESC LIMIT 1")
            df = pd.read_sql(sql, engine, params={"code": f"{target_code}%"})

        if df.empty: return f"暫無 {query} 最新數據"

        row = df.iloc[0]
        price = row.get('close') or row.get('close_price')
        date = row['trade_date']
        return f"📍 **{query} 行情**\n日期: {date}\n价格: {price}\n(如需更多历史数据请询问具体时间段)"

    except Exception as e:
        return f"查询错误: {e}"


# market_tools.py 末尾添加

# ==========================================
#   🔥 新增功能：精准期权价格查询工具
# ==========================================
@tool
def tool_query_specific_option(query: str):
    """
    【期权价格查询专用】
    当用户询问具体的期权合约价格时使用。
    例如："50ETF 1月 3.1 认购价格"、"300ETF 12月 4.0 认沽"、"创业板 2月 2.0 看涨"。
    """
    # 确保 engine 可用 (market_tools 头部已经创建了 engine)
    if engine is None: return "❌ 数据库未连接"

    import re
    from datetime import datetime

    # 1. 定义标的映射 (覆盖主要 ETF)
    ETF_CODE_MAP = {
        '50ETF': '510050.SH', '上证50': '510050.SH',
        '300ETF': '510300.SH', '沪深300': '510300.SH',
        '500ETF': '510500.SH', '中证500': '510500.SH',
        '创业板': '159915.SZ', '创业板ETF': '159915.SZ',
        '科创50': '588000.SH', '科创板': '588000.SH','科创50ETF': '588000.SH'
    }

    # --- A. 解析标的 ---
    target_code = None
    target_name = ""
    query_upper = query.upper()
    for name, code in ETF_CODE_MAP.items():
        if name in query_upper:
            target_code = code
            target_name = name
            break

    if not target_code:
        return "⚠️ 抱歉，我只支持查询 50ETF、300ETF、500ETF、创业板ETF、科创50ETF 的期权价格。"

    # --- B. 解析方向 (认购/认沽) ---
    cp_type = None
    if any(x in query for x in ['认购', '看涨', 'Call', 'C']):
        cp_type = 'C'
        cp_name = "认购"
    elif any(x in query for x in ['认沽', '看跌', 'Put', 'P']):
        cp_type = 'P'
        cp_name = "认沽"

    if not cp_type:
        return f"⚠️ 请指明是【认购】还是【认沽】？(例如：{target_name} 3.0 认购)"

    # --- C. 解析行权价 ---
    numbers = re.findall(r"\d+\.?\d*", query)
    strike_price = None

    for num in numbers:
        val = float(num)
        # 排除年份月份特征
        if "." in num and val < 20:
            strike_price = val
            break
        if "." not in num and val < 10 and "月" not in query.split(num)[1][:1]:
            strike_price = val
            break

    if strike_price is None:
        return f"⚠️ 未在问题中识别到行权价。(例如：{target_name} 3.2 认购)"

    # --- D. 解析月份 ---
    match_month = re.search(r'(\d+)月', query)
    search_month_str = ""

    if match_month:
        m = int(match_month.group(1))
        search_month_str = f"{m:02d}"  # 变成 "01", "12"
    else:
        search_month_str = datetime.now().strftime("%m")

    try:
        # --- E. 数据库查询：找合约代码 (Option Basic) ---
        sql_find_contract = f"""
            SELECT ts_code, exercise_price, delist_date 
            FROM option_basic 
            WHERE underlying = '{target_code}' 
              AND call_put = '{cp_type}'
              AND delist_date >= '{datetime.now().strftime('%Y%m%d')}'
            ORDER BY delist_date ASC
        """
        df_basic = pd.read_sql(sql_find_contract, engine)

        if df_basic.empty:
            return f"❌ 数据库中未找到 {target_name} 的基础合约信息。"

        # 1. 筛选月份
        df_basic['month_str'] = df_basic['delist_date'].astype(str).str[4:6]
        target_contracts = df_basic[df_basic['month_str'] == search_month_str]

        if target_contracts.empty:
            target_contracts = df_basic  # 兜底

        if target_contracts.empty:
            return f"⚠️ 未找到 {search_month_str} 月份到期的合约。"

        # 2. 筛选行权价
        target_contracts = target_contracts.copy()
        target_contracts['diff'] = abs(target_contracts['exercise_price'] - strike_price)
        best_match = target_contracts.sort_values('diff').iloc[0]

        if best_match['diff'] > 0.2:
            return f"⚠️ 未找到行权价为 {strike_price} 的合约。最接近的是 {best_match['exercise_price']}。"

        final_ts_code = best_match['ts_code']
        real_maturity = best_match['delist_date']

        # --- F. 数据库查询：找最新价格 (Option Daily) ---
        sql_price = f"""
            SELECT trade_date, close,vol, oi 
            FROM option_daily 
            WHERE ts_code = '{final_ts_code}' 
            ORDER BY trade_date DESC LIMIT 2
        """
        df_price = pd.read_sql(sql_price, engine)

        if df_price.empty:
            return f"✅ 找到合约代码 **{final_ts_code}**，但暂无最新交易数据。"

        curr_row = df_price.iloc[0]

        # 🔥【修改点 2】计算涨跌逻辑：今收 - 昨收
        change_val = 0.0
        change_text = "0.0000"

        if len(df_price) >= 2:
            prev_close = df_price.iloc[1]['close']  # 昨天的收盘价
            change_val = curr_row['close'] - prev_close
            change_text = f"{change_val:+.4f}"  # 带符号显示，如 +0.0012
        else:
            # 如果只有 1 条数据 (比如刚上市第一天)，暂时无法计算涨跌
            change_text = "新上市"

        return f"""
-------------------------
📝 **合约**: {target_name} {cp_name} @ {best_match['exercise_price']:.3f}
📅 **月份**: {search_month_str}月
-------------------------
💰 **最新价**: **{curr_row['close']:.4f}**
📅 **数据日期**: {curr_row['trade_date']}
📊 **涨跌**: **{change_text}** (较昨收)
📈 **持仓量**: {curr_row['oi']}
        """

    except Exception as e:
        return f"查询过程发生错误: {e}"


# ==========================================
#   🔥 新增：查询历史某一天的价格
# ==========================================
@tool
def get_historical_price(query: str, trade_date: str):
    """
    【历史价格查询】
    查询股票(A股/港股)、期货、指数在某一天的价格。
    适用于回答："阿里在12月2日价格多少"、"茅台上周五收盘价"、"黄金在2025年11月1日的价格"。

    参数:
    - query: 品种名称，如 "阿里"、"茅台"、"黄金"、"小米"
    - trade_date: 查询日期，格式 YYYYMMDD，如 "20251202"
    """
    if engine is None:
        return "❌ 数据库未连接"

    # 清洗日期
    clean_date = trade_date.replace("-", "").replace("/", "").replace(".", "").strip()

    # 解析品种
    symbol_code, asset_type = symbol_map.resolve_symbol(query)
    if not symbol_code:
        return f"⚠️ 未找到品种: {query}"

    try:
        target_code = symbol_code.upper()
        df = pd.DataFrame()

        if asset_type == 'stock':
            is_hk = target_code.endswith('.HK')

            if is_hk:
                # 港股精确匹配
                sql = text("""
                           SELECT ts_code,
                                  name,
                                  trade_date,
                                  open_price,
                                  high_price,
                                  low_price,
                                  close_price,
                                  pct_chg
                           FROM stock_price
                           WHERE ts_code = :code
                             AND trade_date = :date
                           """)
                df = pd.read_sql(sql, engine, params={"code": target_code, "date": clean_date})
            else:
                # A股
                codes_to_try = [target_code]
                if "." in target_code:
                    codes_to_try.append(target_code.split('.')[0])
                else:
                    codes_to_try.extend([f"{target_code}.SZ", f"{target_code}.SH"])
                code_str = "','".join(set(codes_to_try))

                sql = text(f"""
                    SELECT ts_code, name, trade_date, open_price, high_price, 
                           low_price, close_price, pct_chg
                    FROM stock_price 
                    WHERE ts_code IN ('{code_str}') AND trade_date = :date
                """)
                df = pd.read_sql(sql, engine, params={"date": clean_date})

        elif asset_type == 'index':
            sql = text("""
                       SELECT ts_code,
                              trade_date,
                              open_price,
                              high_price,
                              low_price,
                              close_price,
                              pct_chg
                       FROM index_price
                       WHERE ts_code = :code
                         AND trade_date = :date
                       """)
            df = pd.read_sql(sql, engine, params={"code": target_code, "date": clean_date})

        else:  # future
            sql = text("""
                       SELECT ts_code,
                              trade_date,
                              open_price,
                              high_price,
                              low_price,
                              close_price,
                              pct_chg
                       FROM futures_price
                       WHERE ts_code = :code
                         AND trade_date = :date
                       """)
            df = pd.read_sql(sql, engine, params={"code": target_code, "date": clean_date})

        if df.empty:
            return f"⚠️ {query} ({target_code}) 在 {clean_date} 无交易数据（可能是休市日）"

        row = df.iloc[0]
        ts_code = row.get('ts_code', target_code)
        name = row.get('name', query)
        open_p = row.get('open_price')
        high_p = row.get('high_price')
        low_p = row.get('low_price')
        close_p = row.get('close_price')
        pct_chg = row.get('pct_chg', 0)

        # 港股显示货币单位
        currency = "港元" if '.HK' in str(ts_code) else "元"

        # 涨跌幅
        if pct_chg and pct_chg != 0:
            chg_str = f"{float(pct_chg):+.2f}%"
            chg_emoji = "🔴" if float(pct_chg) < 0 else "🟢"
        else:
            chg_str = "0.00%"
            chg_emoji = "⚪"

        return f"""📅 **{name} ({ts_code}) {clean_date} 行情**

💰 **收盘价**: {close_p} {currency}
📈 **开盘价**: {open_p} {currency}
🔺 **最高价**: {high_p} {currency}
🔻 **最低价**: {low_p} {currency}
{chg_emoji} **涨跌幅**: {chg_str}
"""

    except Exception as e:
        print(f"查询错误: {traceback.format_exc()}")
        return f"❌ 查询出错: {e}"