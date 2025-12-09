import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from langchain_core.tools import tool
import symbol_map  # 复用名称解析工具

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
    【行情与波动率查询】
    输入：品种名称（如 "豆粕"、"M2505"、"50ETF"）。
    输出：最新价格、涨跌幅、隐含波动率(IV)、历史波动率(HV)及IV等级。
    当用户问 "价格多少"、"波动率高吗"、"期权贵不贵" 时使用此工具。
    """
    if engine is None: return "数据库未连接"

    # 1. 解析代码 (如 "豆粕" -> "M", "豆粕2505" -> "M2505")
    symbol_code, asset_type = symbol_map.resolve_symbol(query)
    if not symbol_code:
        return f"未找到商品 '{query}'。"

    try:
        snapshot = {}
        target_code = symbol_code.upper()  # 数据库存的大写

        # --- A. 获取价格数据 ---
        if asset_type == 'stock':  # 股票/ETF
            # 补全后缀
            if "." not in target_code:
                target_code += ".SH" if target_code.startswith(('5', '6')) else ".SZ"

            sql_price = text("SELECT * FROM stock_price WHERE ts_code=:code ORDER BY trade_date DESC LIMIT 60")
            df_price = pd.read_sql(sql_price, engine, params={"code": target_code})

        else:  # 期货 (关键修改：直接使用解析出的代码)
            # 不再通过 code_no_digit + '0' 拼接，而是直接查 M 或 M2505
            sql_price = text("SELECT * FROM futures_price WHERE ts_code=:code ORDER BY trade_date DESC LIMIT 60")
            df_price = pd.read_sql(sql_price, engine, params={"code": target_code})

        if df_price.empty: return f"暂无 {query} ({target_code}) 的价格数据。"

        curr = df_price.iloc[0]
        # 统一列名
        close_price = curr.get('close') if 'close' in curr else curr.get('close_price')
        trade_date = curr['trade_date']

        # 计算涨跌幅
        if len(df_price) > 1:
            prev = df_price.iloc[1]
            prev_close = prev.get('close') if 'close' in prev else prev.get('close_price')
            chg_pct = (close_price - prev_close) / prev_close
        else:
            chg_pct = 0

        # 计算 HV (20日历史波动率) - 实时计算兜底
        df_hist = df_price.sort_values('trade_date')
        price_col = 'close' if 'close' in df_price.columns else 'close_price'
        df_hist['log_ret'] = np.log(df_hist[price_col] / df_hist[price_col].shift(1))
        hv_20 = df_hist['log_ret'].tail(20).std() * np.sqrt(252) * 100

        snapshot.update({
            "名称": query,
            "代码": target_code,
            "日期": trade_date,
            "价格": close_price,
            "涨跌幅": f"{chg_pct * 100:.2f}%",
            "历史波动率(HV20)": f"{hv_20:.2f}%"
        })

        # --- B. 获取 IV 数据 (ETF & 期货) ---
        df_iv = pd.DataFrame()

        # 1. ETF IV
        if asset_type == 'stock' and ("510" in target_code or "159" in target_code):
            sql_iv = text("SELECT * FROM etf_iv_history WHERE etf_code=:code ORDER BY trade_date DESC LIMIT 252")
            df_iv = pd.read_sql(sql_iv, engine, params={"code": target_code})

        # 2. 期货 IV (新增支持)
        elif asset_type == 'future':
            sql_iv = text("SELECT * FROM commodity_iv_history WHERE ts_code=:code ORDER BY trade_date DESC LIMIT 252")
            df_iv = pd.read_sql(sql_iv, engine, params={"code": target_code})

        # 统一处理 IV 显示
        if not df_iv.empty:
            curr_iv_row = df_iv.iloc[0]
            curr_iv = curr_iv_row['iv']

            # 如果表里有算好的 HV，优先展示表里的 (通常更准)
            if 'hv' in curr_iv_row and curr_iv_row['hv'] > 0:
                snapshot["历史波动率(HV20)"] = f"{curr_iv_row['hv']:.2f}%"

            # 计算 IV Rank
            max_iv = df_iv['iv'].max()
            min_iv = df_iv['iv'].min()
            iv_rank = (curr_iv - min_iv) / (max_iv - min_iv) * 100 if max_iv != min_iv else 0

            snapshot.update({
                "隐含波动率(IV)": f"{curr_iv:.2f}%",
                "IV Rank": f"{iv_rank:.1f}% (过去一年)",
                "评价": "期权很贵(适合卖方)" if iv_rank > 80 else "期权很便宜(适合买方)" if iv_rank < 10 else "价格适中"
            })

            # 如果是主连，提示底层合约
            if 'used_contract' in curr_iv_row and curr_iv_row['used_contract']:
                snapshot["主力合约"] = curr_iv_row['used_contract']

        # --- C. 生成报告 ---
        report = f"📊 **{query} 行情与波动率**\n"
        for k, v in snapshot.items():
            report += f"- **{k}**: {v}\n"

        return report

    except Exception as e:
        return f"查询出错: {e}"


@tool
def get_price_statistics(query_list: str, start_date: str, end_date: str):
    """
    【区间行情统计与对比】
    用于回答"上个月最高价"、"最近一周谁涨得多"、"今年以来黄金的表现"等统计类问题。

    输入参数：
    - query_list: 品种名称列表，用逗号分隔（例如："黄金,白银" 或 "茅台"）。
    - start_date: 开始日期，格式 YYYYMMDD（例如："20250101"）。
    - end_date: 结束日期，格式 YYYYMMDD。
    """
    if engine is None: return "数据库连接失败"

    queries = [q.strip() for q in query_list.split(',') if q.strip()]
    results = []

    for q in queries:
        symbol_code, asset_type = symbol_map.resolve_symbol(q)
        if not symbol_code:
            results.append(f"❌ 未找到商品: {q}")
            continue

        try:
            target_code = symbol_code.upper()

            # 2. 根据类型构建 SQL
            if asset_type == 'stock':
                if "." not in target_code:
                    target_code += ".SH" if target_code.startswith(('5', '6')) else ".SZ"

                sql = text("""
                           SELECT trade_date, close, high, low
                           FROM stock_price
                           WHERE ts_code=:code
                             AND trade_date >= :
                           start
                             AND trade_date <= :
                           end
                    ORDER BY trade_date ASC
                           """)
            else:  # 期货
                # 直接使用 target_code (M 或 M2505)，不再拼接 0
                sql = text("""
                           SELECT trade_date, close_price as close, high_price as high, low_price as low
                           FROM futures_price
                           WHERE ts_code=:code
                             AND trade_date >= :
                           start
                             AND trade_date <= :
                           end
                    ORDER BY trade_date ASC
                           """)

            df = pd.read_sql(sql, engine, params={"code": target_code, "start": start_date, "end": end_date})

            if df.empty:
                results.append(f"⚠️ {q} ({target_code}): 该时间段无数据")
                continue

            # 3. 核心统计计算
            start_price = df.iloc[0]['close']
            end_price = df.iloc[-1]['close']

            # 区间涨跌幅
            period_chg_pct = (end_price - start_price) / start_price * 100

            # 区间极值
            max_price = df['high'].max()
            min_price = df['low'].min()
            max_date = df.loc[df['high'].idxmax()]['trade_date']

            results.append(f"""
            📊 **{q} ({target_code}) 统计数据**
            - 区间：{start_date} 至 {end_date}
            - 涨跌幅：{period_chg_pct:+.2f}% {'🔥' if period_chg_pct > 0 else '💧'}
            - 最高价：{max_price} (出现在 {max_date})
            - 最低价：{min_price}
            - 期初价：{start_price} -> 期末价：{end_price}
            """)

        except Exception as e:
            results.append(f"❌ 计算 {q} 时出错: {e}")

    return "\n".join(results)


# 测试
if __name__ == "__main__":
    # 测试代码
    print(get_market_snapshot.invoke("豆粕"))
    print(get_market_snapshot.invoke("豆粕2601"))