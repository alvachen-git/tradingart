import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from langchain_core.tools import tool
from langchain.agents import create_agent
import tushare as ts
from datetime import datetime, timedelta
import akshare as ak
import streamlit as st

# --- AI 模块 ---
from langchain_community.chat_models import ChatTongyi
from langchain_core.messages import HumanMessage

# Tushare 初始化 (确保已配置 Token)
ts_token = os.getenv("TUSHARE_TOKEN")
if ts_token:
    ts.set_token(ts_token)
    pro = ts.pro_api()


# 1. 初始化
load_dotenv(override=True)

# --- 【安全修正】从环境变量读取数据库配置 ---
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# 检查配置是否读取成功 (防止 .env 没配好报错)
if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
    raise ValueError("❌ 数据库配置缺失！请检查 .env 文件中是否包含 DB_HOST, DB_USER, DB_PASSWORD 等信息。")

# 【修改点】加上这个装饰器
@st.cache_resource
def get_db_engine():
    db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    # 增加连接池配置，防止连接断开
    return create_engine(db_url, pool_recycle=3600, pool_pre_ping=True)

engine = get_db_engine()


# --- 新增：定义查库工具 ---
@tool
def query_broker_history(broker_name: str):
    """
    查询指定期货商(broker_name)最近 5 个交易日的持仓明细。
    输入必须是完整的期货商名称，例如 '中信期货'。
    """
    print(f"[*] Agent 正在查询: {broker_name}")
    try:
        # 简单直接的 SQL
        sql = f"""
            SELECT trade_date, net_vol, long_vol, short_vol 
            FROM futures_holding 
            WHERE broker = '{broker_name}' 
            ORDER BY trade_date DESC 
            LIMIT 5
        """
        df = pd.read_sql(sql, engine)

        if df.empty:
            return f"未找到【{broker_name}】的近期数据。"

        # 返回 Markdown 格式的表格，AI 读起来更容易
        return df.to_markdown(index=False)

    except Exception as e:
        return f"数据库查询出错: {e}"



# 2. 核心计算 (移除默认值，强制要求传入 symbol)
def calculate_broker_rankings(symbol, lookback_days=150):  # <-这里设置扫描过去的天数
    """
    计算指定品种(symbol)的期货商得分
    symbol: 如 'lc0', 'si0', 'fg0'
    """
    try:
        # A. 获取价格
        # 计算一年前的日期
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y%m%d')

        # 【修改 SQL】增加日期过滤
        query_price = f"""
                SELECT trade_date, close_price, oi, pct_chg 
                FROM futures_price 
                WHERE ts_code='{symbol}' AND trade_date >= '{start_date}' 
                ORDER BY trade_date
            """
        df_price = pd.read_sql(query_price, engine).drop_duplicates(subset=['trade_date'])

        if df_price.empty:
            print(f"Warning: No price data for {symbol}")
            return pd.DataFrame()

        df_price = pd.read_sql(query_price, engine).drop_duplicates(subset=['trade_date'])

        # ❌ 删除或注释掉这一行 (不再重新计算)
        # df_price['pct_chg'] = df_price['close_price'].pct_change()

        # ✅ 改为：处理一下可能的空值 (第一天数据可能是 NaN)
        df_price['pct_chg'] = df_price['pct_chg'].fillna(0)

        # B. 获取持仓 (处理 lc0 -> lc 的逻辑)
        # 去掉末尾的数字 '0' (主连标志)，因为持仓表里通常存的是 'lc', 'si'
        holding_symbol = ''.join([i for i in symbol if not i.isdigit()])

        query_holding = f"SELECT trade_date, broker, net_vol FROM futures_holding WHERE ts_code='{holding_symbol}'AND trade_date >= '{start_date}'"
        df_holding = pd.read_sql(query_holding, engine).drop_duplicates(subset=['trade_date', 'broker'])

        if df_holding.empty:
            print(f"Warning: No holding data for {holding_symbol}")
            return pd.DataFrame()

        # C. 合并数据
        df_merge = pd.merge(df_holding, df_price[['trade_date', 'pct_chg', 'oi']], on='trade_date', how='inner')

        # D. 评分逻辑
        def _score_logic(row):
            net_pos = row['net_vol']
            chg = row['pct_chg']
            total_oi = row['oi']

            if pd.isna(chg) or abs(net_pos) < 500:
                return pd.Series([0.0, 1.0, "忽略"])

            base_score = 0
            if net_pos > 0:
                if chg > 0:
                    base_score = 1 + (2 if chg > 0.02 else 0)
                else:
                    base_score = -1 - (2 if chg < -0.02 else 0)
            elif net_pos < 0:
                if chg < 0:
                    base_score = 1 + (2 if chg < -0.02 else 0)
                else:
                    base_score = -1 - (2 if chg > 0.02 else 0)

            weight = 1.0
            reason = "普通"
            if total_oi > 0:
                ratio = abs(net_pos) / total_oi
                if ratio > 0.1:
                    weight = 1.5
                    reason = "重仓(>10%)"
                elif ratio > 0.05:
                    weight = 1.2
                    reason = "中仓(>5%)"

            return pd.Series([base_score * weight, weight, reason])

        df_merge[['score', 'weight', 'type']] = df_merge.apply(_score_logic, axis=1)
        return df_merge

    except Exception as e:
        print(f"Error in calculation: {e}")
        return pd.DataFrame()


# ==========================================
#   核心功能 5: ETF 期权分析 (本地数据库 + 智能平滑版)
# ==========================================
@st.cache_data(ttl=600)  # 读本地库快，缓存设短点方便刷新
def get_etf_option_analysis(etf_code="510050", days=20):
    """
    从本地数据库读取 ETF 期权数据，并应用智能平滑算法。
    逻辑：读取数据库 -> 找出每日持仓前3名 -> 应用平滑过滤 -> 返回趋势。
    """
    if engine is None: return None

    # 1. 智能后缀补全 (匹配数据库里的 underlying)
    # 数据库存的是 510050.SH 或 159915.SZ
    if "." not in etf_code:
        if etf_code.startswith("15") or etf_code.startswith("16"):
            etf_code += ".SZ"
        else:
            etf_code += ".SH"

    print(f"[*] 正在从数据库分析 {etf_code} (智能平滑)...")

    try:
        # --- 步骤 A: 从数据库一次性拉取所需数据 ---
        # 我们需要：日期、类型、行权价、持仓量、收盘价
        # 关联 option_daily (行情) 和 option_basic (基础信息)

        # 1. 确定日期范围
        date_limit_sql = f"SELECT DISTINCT trade_date FROM option_daily ORDER BY trade_date DESC LIMIT {days}"
        dates_df = pd.read_sql(date_limit_sql, engine)
        if dates_df.empty:
            print(" [-] 数据库 option_daily 表为空")
            return None
        min_date = dates_df['trade_date'].min()

        # 2. 执行 SQL 查询 (只查该 ETF 的数据)
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

        if df_raw.empty:
            print(f" [-] 未查到 {etf_code} 的期权数据")
            return None

        # 统一类型名称
        df_raw['type'] = df_raw['call_put'].map({'C': '认购', 'P': '认沽'})

        # --- 步骤 B: 每日候选池构建 ---
        # 我们需要每一天、每种类型的持仓量前 3 名

        daily_candidates_map = {}

        # 按日期和类型分组
        grouped = df_raw.groupby(['date', 'type'])

        for (date, otype), group in grouped:
            if group.empty: continue

            # 取 OI 最大的前 3 个
            top3 = group.nlargest(3, 'oi')

            candidates = []
            for _, row in top3.iterrows():
                candidates.append({
                    'strike': row['strike'],
                    'oi': row['oi'],
                    'price': row['price'],
                    'code': row['code']
                })

            if date not in daily_candidates_map:
                daily_candidates_map[date] = {}
            daily_candidates_map[date][otype] = candidates

        # --- 步骤 C: 核心算法 - 智能平滑选择 ---
        final_results = []
        sorted_dates = sorted(daily_candidates_map.keys())

        for otype_raw in ['认购', '认沽']:
            type_label = f"{otype_raw} ({'压力' if otype_raw == '认购' else '支撑'})"
            last_strike = None

            for date in sorted_dates:
                day_data = daily_candidates_map[date]
                if otype_raw not in day_data: continue

                candidates = day_data[otype_raw]
                # 默认选第一名
                selected = candidates[0]

                # 平滑逻辑
                if last_strike is not None:
                    diff1 = abs(selected['strike'] - last_strike) / last_strike

                    # 如果跳变 > 5%，尝试备选
                    if diff1 > 0.05:
                        if len(candidates) > 1:
                            cand2 = candidates[1]
                            diff2 = abs(cand2['strike'] - last_strike) / last_strike
                            if diff2 <= 0.05:
                                selected = cand2
                            elif len(candidates) > 2:
                                cand3 = candidates[2]
                                diff3 = abs(cand3['strike'] - last_strike) / last_strike
                                if diff3 <= 0.05:
                                    selected = cand3

                last_strike = selected['strike']

                final_results.append({
                    'date': date,
                    'type': type_label,
                    'strike': selected['strike'],
                    'oi': selected['oi'],
                    'price': selected['price'],
                    'code': selected['code']
                })

        return pd.DataFrame(final_results)

    except Exception as e:
        print(f" [!] 数据库分析出错: {e}")
        return None

# 3. 获取专家观点
def get_expert_sentiment(date_str, symbol):
    try:
        # 清洗 symbol (lc0 -> lc)
        clean_symbol = ''.join([i for i in symbol if not i.isdigit()])
        sql = f"SELECT score, reason FROM market_sentiment WHERE trade_date='{date_str}' AND ts_code='{clean_symbol}'"
        df = pd.read_sql(sql, engine)
        return df.iloc[0].to_dict() if not df.empty else None
    except:
        return None



# 4. AI 生成报告 (新增 commodity_name 参数)
def generate_ai_report_agent(rank_df, expert_data, date_str, commodity_name):
    """
    使用 Agent 模式生成报告 (更智能，融合专家分数)
    """
    if not os.getenv("DASHSCOPE_API_KEY"):
        return "错误：未配置 API KEY"

    # 1. 初始化 LLM
    chat = ChatTongyi(model="qwen-plus", temperature=0.3)

    # 2. 准备工具箱
    tools = [query_broker_history]

    # 3. 准备提示词
    # 提取排行榜数据
    top_winner = rank_df.sort_values('总积分', ascending=False).iloc[0]['期货商']
    top_loser = rank_df.sort_values('总积分', ascending=True).iloc[0]['期货商']

    # --- 【关键修改】处理专家分数 ---
    if expert_data:
        exp_score = expert_data.get('score', 0)
        exp_reason = expert_data.get('reason', '无理由')

        # 将分数转化为文字描述，辅助 AI 理解
        if exp_score >= 2:
            score_desc = "强烈看涨 (极度乐观)"
        elif exp_score == 1:
            score_desc = "看小涨 (乐观)"
        elif exp_score == 0:
            score_desc = "中性 (横盘震荡)"
        elif exp_score == -1:
            score_desc = "看小跌 (悲观)"
        else:
            score_desc = "强烈看跌 (极度悲观)"

        expert_context = f"专家观点得分：{exp_score} ({score_desc})。\n专家理由：{exp_reason}"
    else:
        expert_context = "专家暂无观点 (视为中性)。"

    system_prompt = f"""
    你是一位资深交易员，遵守顺势交易的纪律。你的任务是撰写《{commodity_name} 行情复盘》，帮助投资者看清市场。

    【专家观点系统说明】
    你将获得一个“专家观点分数”，范围从 -2 到 2：
    * 2: 强烈看涨 (做多信号强烈)
    * 1: 看涨
    * 0: 中性/震荡
    * -1: 看跌
    * -2: 强烈看跌 (做空信号强烈)

    【你的任务】
    1. 首先，使用工具 `query_broker_history` 查询榜单上亏损最严重的【{top_loser}】（反向指标）的近期操作。
    2. 然后，使用工具 `query_broker_history` 查询榜单上东方财富期货和中信建投期货最近的净持仓，因为这两个是反指标，如果他们今天净持仓是多头增加，那代表行情可能继续跌，如果净持仓是多头减少，那代表行情可能继续涨，而在给客户的报告里不要把这两个期货商名字写出来，可以用反指标这词代替。
    3. 再来，结合【专家观点分数】（这是核心依据，权重占 70%）和你的查询结果（作为验证或反驳），生成分析。
    4. 如果反向指标的操作方向与专家观点相反（例如专家看涨，反向指标在做空），则信心增强；如果一致，则提示风险。

    【输出要求】
    * 给出明确的多空方向建议。
    * 引用专家分数作为论据，但不要把专家的分数说出来，也不要把专家占你判断权重的70%说出来。
    * 文字排版整齐，表达方式不要太学术，要平易近人，带点幽默。
    * 字数 500 字以内。
    """

    # 4. 创建 Agent
    agent = create_agent(
        model=chat,
        tools=tools,
        system_prompt=system_prompt
    )

    # 5. 执行
    try:
        response = agent.invoke({
            "messages": [
                HumanMessage(content=f"今天是 {date_str}。\n\n【专家数据】\n{expert_context}\n\n请开始分析并写报告。")]
        })
        return response["messages"][-1].content
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"=== Agent 详细错误 ===\n{error_details}")
        return f"Agent 思考失败。详细错误: {e}"