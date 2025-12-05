import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from langchain_core.tools import tool
import symbol_map  # 復用名稱解析工具

# 1. 初始化
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


@tool
def get_market_snapshot(query: str):
    """
    【行情與波动率查询】
    輸入：品種名稱（如 "50ETF"、"螺纹"、"茅台"）。
    輸出：最新价格、涨跌幅、隐含波动率(IV)、历史波动率(HV)及IV等级。
    當用戶問 "价格多少"、"波动率高吗"、"期权贵不贵" 時使用此工具。
    """
    if engine is None: return "数据库未連接"

    # 1. 解析代碼
    symbol_code, asset_type = symbol_map.resolve_symbol(query)
    if not symbol_code:
        return f"未找到商品 '{query}'。"

    try:
        snapshot = {}

        # --- A. 獲取價格數據 ---
        if asset_type == 'stock':  # 股票/ETF
            # 補全後綴
            if "." not in symbol_code:
                symbol_code += ".SH" if symbol_code.startswith(('5', '6')) else ".SZ"

            sql_price = f"SELECT * FROM stock_price WHERE ts_code='{symbol_code}' ORDER BY trade_date DESC LIMIT 60"
            df_price = pd.read_sql(sql_price, engine)

        else:  # 期貨
            # 處理後綴
            code_no_digit = ''.join([i for i in symbol_code if not i.isdigit()])
            sql_price = f"SELECT * FROM futures_price WHERE ts_code='{code_no_digit}0' ORDER BY trade_date DESC LIMIT 60"
            df_price = pd.read_sql(sql_price, engine)

        if df_price.empty: return f"暫無 {query} 的價格數據。"

        curr = df_price.iloc[0]
        # 統一列名 (期貨表叫 close_price, 股票表叫 close)
        close_price = curr.get('close') if 'close' in curr else curr.get('close_price')
        trade_date = curr['trade_date']

        # 計算漲跌幅
        if len(df_price) > 1:
            prev = df_price.iloc[1]
            prev_close = prev.get('close') if 'close' in prev else prev.get('close_price')
            chg_pct = (close_price - prev_close) / prev_close
        else:
            chg_pct = 0

        # 計算 HV (20日歷史波動率)
        # 確保按時間升序
        df_hist = df_price.sort_values('trade_date')
        price_col = 'close' if 'close' in df_price.columns else 'close_price'
        df_hist['log_ret'] = np.log(df_hist[price_col] / df_hist[price_col].shift(1))
        hv_20 = df_hist['log_ret'].tail(20).std() * np.sqrt(252) * 100

        snapshot.update({
            "名称": query,
            "代码": symbol_code,
            "日期": trade_date,
            "价格": close_price,
            "涨跌幅": f"{chg_pct * 100:.2f}%",
            "历史波动率(HV20)": f"{hv_20:.2f}%"
        })

        # --- B. 獲取 IV 數據 (僅限 ETF) ---
        if asset_type == 'stock' and ("510" in symbol_code or "159" in symbol_code):
            # 去 etf_iv_history 表查
            sql_iv = f"""
                SELECT * FROM etf_iv_history 
                WHERE etf_code='{symbol_code}' 
                ORDER BY trade_date DESC LIMIT 252
            """
            df_iv = pd.read_sql(sql_iv, engine)

            if not df_iv.empty:
                curr_iv = df_iv.iloc[0]['iv']
                # 計算 IV Rank
                max_iv = df_iv['iv'].max()
                min_iv = df_iv['iv'].min()
                iv_rank = (curr_iv - min_iv) / (max_iv - min_iv) * 100 if max_iv != min_iv else 0

                snapshot.update({
                    "隐含波动率(IV)": f"{curr_iv:.2f}%",
                    "IV Rank": f"{iv_rank:.1f}% (過去一年)",
                    "评价": "期权很贵(适合卖方)" if iv_rank > 80 else "期权很便宜(适合买方)" if iv_rank < 10 else "价格适中"
                })

        # --- C. 生成報告 ---
        report = "📊 **行情与波动率数据**\n"
        for k, v in snapshot.items():
            report += f"- **{k}**: {v}\n"

        return report

    except Exception as e:
        return f"查詢出錯: {e}"


@tool
def get_price_statistics(query_list: str, start_date: str, end_date: str):
    """
    【區間行情統計與對比】
    用于回答"上个月最高价"、"最近一周谁涨得多"、"今年以来黄金的表现"等统计类问题。

    輸入參數：
    - query_list: 品种名称列表，用逗号分隔（例如："黄金,白银" 或 "茅台"）。
    - start_date: 开始日期，格式 YYYYMMDD（例如："20250101"）。
    - end_date: 结束日期，格式 YYYYMMDD。
    """
    if engine is None: return "數據庫連接失敗"

    # 1. 解析所有品種
    queries = [q.strip() for q in query_list.split(',') if q.strip()]
    results = []

    for q in queries:
        symbol_code, asset_type = symbol_map.resolve_symbol(q)
        if not symbol_code:
            results.append(f"❌ 未找到商品: {q}")
            continue

        try:
            # 2. 根據類型構建 SQL
            # 我們需要查區間內的開高低收，用於計算
            if asset_type == 'stock':
                # 補全後綴
                if "." not in symbol_code:
                    symbol_code += ".SH" if symbol_code.startswith(('5', '6')) else ".SZ"

                sql = f"""
                    SELECT trade_date, close, high, low 
                    FROM stock_price 
                    WHERE ts_code='{symbol_code}' 
                      AND trade_date >= '{start_date}' 
                      AND trade_date <= '{end_date}'
                    ORDER BY trade_date ASC
                """
            else:  # 期貨
                code_no_digit = ''.join([i for i in symbol_code if not i.isdigit()])
                # 兼容 lc 和 lc0
                sql = f"""
                    SELECT trade_date, close_price as close, high_price as high, low_price as low
                    FROM futures_price 
                    WHERE (ts_code='{code_no_digit}0' OR ts_code='{code_no_digit}')
                      AND trade_date >= '{start_date}' 
                      AND trade_date <= '{end_date}'
                    ORDER BY trade_date ASC
                """

            df = pd.read_sql(sql, engine)

            if df.empty:
                results.append(f"⚠️ {q} ({symbol_code}): 該時間段無數據")
                continue

            # 3. 核心統計計算
            start_price = df.iloc[0]['close']
            end_price = df.iloc[-1]['close']

            # 區間漲跌幅
            period_chg_pct = (end_price - start_price) / start_price * 100

            # 區間極值
            max_price = df['high'].max()
            min_price = df['low'].min()
            max_date = df.loc[df['high'].idxmax()]['trade_date']

            results.append(f"""
            📊 **{q} ({symbol_code}) 统计数据**
            - 区间：{start_date} 至 {end_date}
            - 涨跌幅：{period_chg_pct:+.2f}% {'🔥' if period_chg_pct > 0 else '💧'}
            - 最高价：{max_price} (出现在 {max_date})
            - 最低价：{min_price}
            - 期初价：{start_price} -> 期末价：{end_price}
            """)

        except Exception as e:
            results.append(f"❌ 計算 {q} 時出錯: {e}")

    return "\n".join(results)


# 測試
if __name__ == "__main__":
    # 測試：對比黃金和白銀最近的表現
    print(get_price_statistics.invoke({
        "query_list": "黄金, 白银",
        "start_date": "20251101",
        "end_date": "20251128"
    }))