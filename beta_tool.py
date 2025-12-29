import pandas as pd
import numpy as np
import statsmodels.api as sm
import os
import streamlit as st
from dotenv import load_dotenv
from langchain_core.tools import tool
from sqlalchemy import create_engine, text

load_dotenv(override=True)


# 1. 数据库连接
def get_db_engine():
    try:
        # 请确保 .env 文件中配置了正确的数据库信息
        db_url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        return create_engine(db_url)
    except:
        return None


# 2. 股票代码智能解析 (输入容错)
def resolve_stock_code(code_input: str, engine):
    """
    将用户输入的 '茅台'、'600519' 等转换为标准代码 '600519.SH'
    基于用户提供的 stock_price 表 (含 name 字段)
    """
    code_input = code_input.strip()

    # A. 如果已经是标准格式 (如 000001.SZ)
    if len(code_input) >= 9 and '.' in code_input:
        return code_input

    # B. 如果是纯数字 (6位) -> 尝试补全后缀
    if code_input.isdigit() and len(code_input) == 6:
        # 简单规则补全，或者去数据库查一下这也行，这里先用规则补全以提高速度
        if code_input.startswith(('60', '68')):
            return f"{code_input}.SH"
        elif code_input.startswith(('00', '30')):
            return f"{code_input}.SZ"
        elif code_input.startswith(('8', '4')):
            return f"{code_input}.BJ"

    # C. 如果是中文名称 (如 '贵州茅台') -> 查 stock_price 表
    # 注意：在价格表中查名称效率较低，建议加 LIMIT 1
    try:
        # 假设 stock_price 表里每一行都有 name，或者至少有一行有
        sql = text("SELECT ts_code FROM stock_price WHERE name LIKE :name LIMIT 1")
        with engine.connect() as conn:
            res = conn.execute(sql, {'name': f"%{code_input}%"}).scalar()
            if res:
                return res
    except Exception as e:
        print(f"解析股票名称失败: {e}")
        pass

    return code_input


# 3. 通用行情获取函数
def get_price_data_from_db(ts_code: str, start_date: str, end_date: str, is_index: bool, engine):
    """
    从本地数据库获取行情
    用户环境:
    - 股票表: stock_price (ts_code, trade_date, close_price)
    - 指数表: index_price (ts_code, trade_date, close_price)
    """
    try:
        table_name = 'index_price' if is_index else 'stock_price'

        # SQL 查询：直接取 trade_date 和 close_price
        # 别名 close_price -> close 方便后续 pandas 处理
        sql = text(f"""
            SELECT trade_date, close_price as close 
            FROM {table_name} 
            WHERE ts_code = :ts_code 
            AND trade_date BETWEEN :start_date AND :end_date 
            ORDER BY trade_date ASC
        """)

        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params={
                'ts_code': ts_code,
                'start_date': start_date,
                'end_date': end_date
            })

        return df
    except Exception as e:
        return pd.DataFrame()


# 4. LangChain 工具定义
@tool
def calculate_hedging_beta(stock_input: str, benchmark_code: str = "000300.SH", window: int = 60):
    """
    【Beta 对冲计算器】
    用于计算股票相对于大盘指数的 Beta 系数，并给出期货对冲建议。

    参数:
    - stock_input: 股票代码或简称 (如 "600519", "茅台", "宁德时代")。
    - benchmark_code: 对冲基准，默认 "000300.SH" (沪深300)。
                      中证500用 "000905.SH"，上证50用 "000016.SH"。
    - window: 回测天数，默认 60 天。
    """
    engine = get_db_engine()
    if not engine:
        return "❌ 数据库连接失败，请检查 .env 配置"

    try:
        # --- 步骤 1: 解析股票代码 ---
        stock_code = resolve_stock_code(stock_input, engine)

        # --- 步骤 2: 确定日期范围 ---
        # 假设数据库存的是 '20251125' 这种字符串格式
        now = pd.Timestamp.now()
        end_date = now.strftime('%Y%m%d')
        start_date = (now - pd.Timedelta(days=window * 2.5)).strftime('%Y%m%d')  # 多取一些防停牌

        # --- 步骤 3: 获取数据 ---
        # A. 获取基准指数 (表: index_price)
        df_bench = get_price_data_from_db(benchmark_code, start_date, end_date, is_index=True, engine=engine)
        if df_bench.empty:
            return f"❌ 数据库 index_price 表中未找到指数 {benchmark_code} 的数据。"

        # B. 获取个股 (表: stock_price)
        df_stock = get_price_data_from_db(stock_code, start_date, end_date, is_index=False, engine=engine)
        if df_stock.empty:
            return f"❌ 数据库 stock_price 表中未找到股票 {stock_input} ({stock_code}) 的数据。"

        # --- 步骤 4: 数据清洗与合并 ---
        # 确保日期格式统一 (转为 datetime 以便排序和对齐)
        df_bench['trade_date'] = pd.to_datetime(df_bench['trade_date'].astype(str))
        df_stock['trade_date'] = pd.to_datetime(df_stock['trade_date'].astype(str))

        # 重命名列以便合并
        df_bench = df_bench.rename(columns={'close': 'bench_close'})
        df_stock = df_stock.rename(columns={'close': 'stock_close'})

        # 合并 (Inner Join 取交集)
        df = pd.merge(df_bench, df_stock, on='trade_date', how='inner')
        df = df.sort_values('trade_date')

        # 截取最新的 window 天
        df = df.tail(window)

        if len(df) < 20:
            return f"⚠️ 有效重叠数据不足 (仅 {len(df)} 天)，无法计算 Beta。可能该股近期停牌或刚上市。"

        # --- 步骤 5: 计算收益率与回归 ---
        df['bench_ret'] = df['bench_close'].pct_change()
        df['stock_ret'] = df['stock_close'].pct_change()
        df = df.dropna()

        X = df['bench_ret']
        y = df['stock_ret']
        X_sm = sm.add_constant(X)

        model = sm.OLS(y, X_sm).fit()

        beta = model.params['bench_ret']
        alpha = model.params['const'] * 250 * 100  # 年化 Alpha
        r_squared = model.rsquared

        # --- 步骤 6: 生成自然语言报告 ---
        # 尝试查一下股票中文名用于展示
        stock_name_display = stock_code
        try:
            name_sql = text("SELECT name FROM stock_price WHERE ts_code = :code LIMIT 1")
            with engine.connect() as conn:
                res = conn.execute(name_sql, {'code': stock_code}).scalar()
                if res:
                    stock_name_display = f"{res} ({stock_code})"
        except:
            pass

        bench_map = {
            "000300.SH": "沪深300", "000905.SH": "中证500","399006.SH": "创业板指","932000.CSI": "中证2000",
            "000016.SH": "上证50", "000852.SH": "中证1000"
        }
        bench_name = bench_map.get(benchmark_code, benchmark_code)

        # 话术逻辑
        hedge_advice = ""
        if beta > 0:
            hedge_val = 100 * beta
            hedge_advice = f"做空市值约 **{hedge_val:.1f} 万元** 的 {bench_name} 期货"
        else:
            hedge_advice = "该股与大盘负相关，理论上无需做空期货对冲，反而具有避险属性"

        return f"""
🎯 **Beta 对冲分析报告**
---------------------------
**分析标的**: {stock_name_display}
**对冲基准**: {bench_name}
**数据样本**: {len(df)} 个交易日 ({df['trade_date'].min().strftime('%Y-%m-%d')} 至 {df['trade_date'].max().strftime('%Y-%m-%d')})

📊 **核心指标**:
1. **Beta 系数**: **{beta:.4f}**
   - *解读*: 大盘每涨跌 1%，该股倾向于涨跌 {beta:.2f}%。
   - *类型*: {"高波动进攻型" if beta > 1.1 else "低波动防守型" if beta < 0.9 else "跟随大盘型"}
2. **Alpha (年化)**: {alpha:.2f}% (超额收益)
3. **R² (拟合度)**: {r_squared:.2%}
   - *解读*: {"拟合度高，对冲效果好" if r_squared > 0.6 else "拟合度低，个股走势独立，用指数对冲效果有限"}

🛡️ **操作建议**:
如果您持有 **100万元** 该股票市值，为了完全对冲系统性风险：
👉 建议 **{hedge_advice}**。
"""

    except Exception as e:
        return f"❌ 计算过程出错: {str(e)}"