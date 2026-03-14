import pandas as pd
import numpy as np
from scipy import stats
import re
import json
from symbol_match import sql_prefix_condition
import os
import sys
from sqlalchemy import create_engine, text
from kline_tools import analyze_kline_pattern
from dotenv import load_dotenv
from langchain_core.tools import tool
from sqlalchemy.exc import SQLAlchemyError
from langchain.agents import create_agent
import tushare as ts
from datetime import datetime, timedelta
import streamlit as st

# --- AI 模块 ---
from langchain_community.chat_models import ChatTongyi
from langchain_core.messages import HumanMessage

# Tushare 初始化 (确保已配置 Token)
pro = None
ts_token = os.getenv("TUSHARE_TOKEN")
if ts_token:
    try:
        ts.set_token(ts_token)
        pro = ts.pro_api()
    except Exception as e:
        print(f"⚠️ Tushare 初始化失败，已降级运行: {e}")
        pro = None


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

_TOOL_STARTUP_SELF_CHECK_LOGGED = False


def _log_tool_startup_self_check():
    """
    启动自检日志（每个进程打印一次）：
    用于确认当前进程加载的 data_engine/symbol_match 是否为最新代码。
    """
    global _TOOL_STARTUP_SELF_CHECK_LOGGED
    if _TOOL_STARTUP_SELF_CHECK_LOGGED:
        return

    try:
        lc_condition = sql_prefix_condition("LC")
        m_condition = sql_prefix_condition("M")
        like_percent_escaped = "LIKE 'LC%%'" in lc_condition

        print(
            f"[ToolSelfCheck] data_engine loaded | pid={os.getpid()} | file={__file__} | cwd={os.getcwd()}",
            file=sys.stderr,
        )
        print(f"[ToolSelfCheck] sql_prefix_condition('LC') = {lc_condition}", file=sys.stderr)
        print(f"[ToolSelfCheck] sql_prefix_condition('M') = {m_condition}", file=sys.stderr)
        print(f"[ToolSelfCheck] like_percent_escaped = {'YES' if like_percent_escaped else 'NO'}", file=sys.stderr)
    except Exception as e:
        print(f"[ToolSelfCheck] startup self-check failed: {e}", file=sys.stderr)
    finally:
        _TOOL_STARTUP_SELF_CHECK_LOGGED = True


_log_tool_startup_self_check()


# --- 新增：定义查库工具 ---
@tool
def query_broker_history(broker_name: str):
    """
    查询指定期货商最近 5 个交易日的持仓明细。
    输入必须完整的期货商名称，例如 '中信期货'。
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
@st.cache_data(ttl=3600)
def calculate_broker_rankings(symbol, lookback_days=150):  # <-这里设置扫描过去的天数
    """
    计算指定品种(symbol)的期货商得分
    symbol: 如 'lc0', 'si0', 'fg0'
    """
    try:
        # A. 获取价格
        # 计算一年前的日期
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y%m%d')

        # 优先尝试命中复合索引，若线上索引名不一致则自动回退到无 hint 版本
        query_price_force = f"""
                SELECT trade_date, close_price, oi, pct_chg
                FROM futures_price FORCE INDEX(idx_price_code_date)
                WHERE ts_code='{symbol}' AND trade_date >= '{start_date}'
                ORDER BY trade_date
            """
        query_price_plain = f"""
                SELECT trade_date, close_price, oi, pct_chg
                FROM futures_price
                WHERE ts_code='{symbol}' AND trade_date >= '{start_date}'
                ORDER BY trade_date
            """
        try:
            df_price = pd.read_sql(query_price_force, engine).drop_duplicates(subset=['trade_date'])
        except Exception:
            df_price = pd.read_sql(query_price_plain, engine).drop_duplicates(subset=['trade_date'])

        if df_price.empty:
            print(f"Warning: No price data for {symbol}")
            return pd.DataFrame()

        # ❌ 删除或注释掉这一行 (不再重新计算)
        # df_price['pct_chg'] = df_price['close_price'].pct_change()

        # ✅ 改为：处理一下可能的空值 (第一天数据可能是 NaN)
        df_price['pct_chg'] = df_price['pct_chg'].fillna(0)

        # B. 获取持仓 (处理 lc0 -> lc 的逻辑)
        # 去掉末尾的数字 '0' (主连标志)，因为持仓表里通常存的是 'lc', 'si'
        holding_symbol = ''.join([i for i in symbol if not i.isdigit()])

        # B. 获取持仓
        query_holding = (
            f"SELECT trade_date, broker, net_vol "
            f"FROM futures_holding "
            f"WHERE ts_code='{holding_symbol}' AND trade_date >= '{start_date}'"
        )
        df_holding = pd.read_sql(query_holding, engine)

        # --- 【关键清洗】去除 "(代客)" 后缀 ---
        # 1. 替换中文括号、英文括号、以及“代客”字样
        df_holding['broker'] = df_holding['broker'].astype(str).str.replace(r'[（\(]代客[）\)]', '',
                                                                            regex=True).str.strip()

        # 2. 重新聚合 (Groupby)
        # 因为清洗后，原本分开的两行 "永安" 和 "永安(代客)" 会变成两个 "永安"
        # 我们必须把它们的持仓量加起来！
        df_holding = df_holding.groupby(['trade_date', 'broker'])['net_vol'].sum().reset_index()

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
 # 读本地库快，缓存设短点方便刷新
def get_etf_option_analysis(etf_code="510300", days=20):
    """
    从本地数据库读取 ETF 期权数据，并应用智能平滑算法。
    逻辑：读取数据库 -> 找出每日持仓前3名 -> 应用平滑过滤 -> 返回趋势。
    """
    if engine is None:
        raise ValueError("数据库连接未初始化")

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
            return pd.DataFrame()
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
        raise e

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

#極速的查排行榜函數
@st.cache_data(ttl=6000)
def get_cross_market_ranking(days=200, top_n=5):
    """
    直接從數據庫讀取預計算好的全市場排行榜
    """
    if engine is None: return pd.DataFrame(), pd.DataFrame()

    try:
        # 獲取最新的統計日期
        date_sql = "SELECT MAX(trade_date) FROM market_rank_daily"
        with engine.connect() as conn:
            latest_date = conn.execute(text(date_sql)).scalar()

        if not latest_date:
            # 如果表是空的，臨時回退到實時計算 (作為兜底)
            # 或者直接返回空
            print("[-] 緩存表為空")
            return pd.DataFrame(), pd.DataFrame()

        # 查詢數據
        sql = f"SELECT * FROM market_rank_daily WHERE trade_date='{latest_date}'"
        df = pd.read_sql(sql, engine)

        # 拆分
        top_winners = df[df['rank_type'] == 'WIN'].sort_values('score', ascending=False).head(top_n)
        top_losers = df[df['rank_type'] == 'LOSE'].sort_values('score', ascending=True).head(top_n)

        return top_winners, top_losers

    except Exception as e:
        print(f"查詢排行出錯: {e}")
        return pd.DataFrame(), pd.DataFrame()


# 4. AI 生成报告 (新增 commodity_name 参数)
def generate_ai_report_agent(rank_df, expert_data, date_str, commodity_name):
    """
    使用 Agent 模式生成报告 (更智能，融合专家分数)
    """
    if not os.getenv("DASHSCOPE_API_KEY"):
        return "错误：未配置 API KEY"

    # 1. 初始化 LLM
    chat = ChatTongyi(model="qwen-turbo", temperature=0.3)

    # 2. 准备工具箱
    tools = [query_broker_history,analyze_kline_pattern]

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



     【你的任务】
    1. 首先，使用工具 `query_broker_history` 查询榜单上亏损最严重的【{top_loser}】（反向指标）的近期操作。
    2. 然后，使用工具 `query_broker_history` 查询榜单上东方财富期货和中信建投期货最近的净持仓，因为这两个是反指标，如果他们今天净持仓是多头增加，那代表行情可能继续跌，如果净持仓是多头减少，那代表行情可能继续涨。
    3. 结合【专家观点分数】或者工具`analyze_kline_pattern`和你的查询结果（作为验证或反驳），生成分析。
    4. 如果专家没有给出观点，必须调用工具来分析行情取代专家观点 ->用 `analyze_kline_pattern`


    【输出要求】
    * 结合行情分析和期货商持仓情况，综合给出明确的多空方向建议。
    * 在给客户的报告里不要把东方财富期货和中信建投期货的名字写出来，可以用反指标这词代替
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


# ==========================================
#   核心功能 8: 用户画像与记忆系统 (User Memory)
# ==========================================

def get_user_profile(user_id='default_user'):
    """读取用户画像"""
    if engine is None: return {}
    try:
        sql = f"SELECT * FROM user_profile WHERE user_id='{user_id}'"
        df = pd.read_sql(sql, engine)
        if not df.empty:
            return df.iloc[0].to_dict()
        # 如果查不到，返回默认空字典
        return {}
    except:
        return {}


def update_user_memory_async(user_id, user_input):
    """
    【旁路分析】調用 AI 分析用戶的這句話，提取特徵並更新數據庫
    (優化版：只保留最近關注的 5 個品種)
    """
    # 1. 檢查 Key
    if not os.getenv("DASHSCOPE_API_KEY"): return

    print(f"\n[🧠] 正在後台分析用戶({user_id})的潛台詞: {user_input} ...")

    # 2. 強制清除代理
    for key in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"]:
        if key in os.environ: os.environ.pop(key, None)


        # 3. 讀取舊畫像
        old_profile = get_user_profile(user_id)
        old_assets = old_profile.get('focus_assets', '')  # 假設格式是 "茅台,寧德時代,螺紋鋼"
        old_risk = old_profile.get('risk_preference', '未知')

        # 4. 讓 AI 進行側寫
        chat = ChatTongyi(model="qwen-turbo", temperature=0.1)

        prompt = f"""
        你是一个用户行为分析师。请分析用户说的这句话："{user_input}"

        參考用戶舊畫像：
        - 风险偏好：{old_risk}
        - 已关注品种：{old_assets}

        【风险偏好判断标准】（必须严格遵守）：

        ✅ **保守型** - 符合以下任一特征：
        - 提到：稳妥、低风险、保本、止损严格、小仓位、怕亏、谨慎、安全第一
        - 示例："我比较怕亏，想找低风险的"、"我只想做稳妥的套利"

        ✅ **稳健型** - 符合以下任一特征：
        - 提到：合理、适度、控制风险、对冲、保护、风险收益平衡、备兑
        - 示例："我想做个对冲保护一下"、"有没有风险可控的策略"

        ✅ **激进型** - 符合以下任一特征：
        - 提到：梭哈、重仓、激进、all in、赌、翻倍、暴涨、买虚值期权、深虚、末日期权
        - 示例："我想重仓做多翻倍！"、"给我推荐虚值期权"

        ❓ **未知** - 如果这句话无法判断风险偏好（如纯查询型问题），返回"未知"

        【情绪识别标准】：
        - 贪婪：提到暴涨、暴富、翻倍、抄底、满仓
        - 焦虑：提到被套、亏损、怎么办、救命、担心
        - 愤怒：提到坑、骗、垃圾、又亏了、黑心
        - 平静：正常询问，无明显情绪词

        請提取以下信息：
        1. risk: 风险偏好（保守/稳健/激进/未知）- 必须根据上述标准严格判断
        2. mood: 當前情緒（焦虑/贪婪/平静/愤怒/开心/伤心/未知）
        3. assets: 用户本次对话提到的、感兴趣的品种（如果没提到就不填）
        4. style: 投资风格简评

        請僅返回 JSON 格式數據，例如：
        {{ "risk": "激进", "mood": "贪婪", "assets": "rb, 螺纹钢", "style": "短线" }}
        """

        response = chat.invoke([HumanMessage(content=prompt)])
        content = response.content

        # 正則提取 JSON
        json_match = re.search(r'\{.*\}', content.replace('\n', ''), re.DOTALL)
        if not json_match: return

        import json
        data = json.loads(json_match.group())

        # 5. 🔥【多轮积累】風險偏好判斷邏輯
        current_risk_signal = data.get('risk', '未知')

        # 從 Redis 讀取歷史風險信號 (最近5次)
        risk_history_key = f"user_risk_signals:{user_id}"
        try:
            import redis
            redis_client = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)

            # 獲取歷史信號列表
            risk_signals = redis_client.lrange(risk_history_key, 0, 4)  # 最近5次

            # 如果本次信號不是"未知"，則加入歷史
            if current_risk_signal != '未知':
                redis_client.lpush(risk_history_key, current_risk_signal)  # 左側推入（最新的在前）
                redis_client.ltrim(risk_history_key, 0, 4)  # 只保留最近5次
                redis_client.expire(risk_history_key, 2592000)  # 30天過期

                # 重新獲取（包含本次）
                risk_signals = redis_client.lrange(risk_history_key, 0, 4)

            # 統計各類型的出現次數
            from collections import Counter
            if risk_signals:
                signal_counts = Counter(risk_signals)
                total_signals = len(risk_signals)

                # 找出最多的類型及其占比
                most_common_risk, count = signal_counts.most_common(1)[0]
                ratio = count / total_signals

                # 🔥 只有當某個類型占比 >= 60% 時，才更新風險偏好
                if ratio >= 0.6:
                    new_risk = most_common_risk
                    print(f"📊 风险信号积累: {signal_counts} | 判定为: {new_risk} (占比{ratio:.0%})")
                else:
                    # 信號不一致，保留舊值
                    new_risk = old_risk
                    print(f"📊 风险信号不一致: {signal_counts} | 保留旧值: {old_risk}")
            else:
                # 沒有歷史信號，保留舊值
                new_risk = old_risk

        except Exception as e:
            print(f"⚠️ Redis 風險信號存取失敗，使用单次判断: {e}")
            new_risk = current_risk_signal if current_risk_signal != '未知' else old_risk

        new_mood = data.get('mood', '平靜')
        new_style = data.get('style', '')

        # --- 【核心優化】關注品種：LRU 策略 (最近最少使用) ---
        # A. 提取新舊列表
        new_assets_str = data.get('assets', '')
        # 將字符串轉為列表，並去除空值
        new_items = [x.strip() for x in new_assets_str.split(',') if
                     x.strip() and x.strip() not in ['...', '未知', '无']]
        old_items = [x.strip() for x in old_assets.split(',') if x.strip()]

        # B. 合併與去重 (保持順序：新 -> 舊)
        final_list = []
        seen = set()

        # 先加新的 (它們是最新的關注點)
        for item in new_items:
            if item not in seen:
                final_list.append(item)
                seen.add(item)

        # 再加舊的 (如果舊的在新的裡出現過，這裡就會跳過，相當於把舊的提到了前面)
        for item in old_items:
            if item not in seen:
                final_list.append(item)
                seen.add(item)

        # C. 截斷：只保留前 5 個
        final_list = final_list[:10]
        final_assets_str = ','.join(final_list)

        # 6. 寫入數據庫
        try:
            with engine.connect() as conn:
                # 开启事务
                trans = conn.begin()
                try:
                    # A. 检查用户是否存在
                    check = conn.execute(text(f"SELECT 1 FROM user_profile WHERE user_id=:u"),
                                         {"u": user_id}).fetchone()
                    if not check:
                        conn.execute(text("INSERT INTO user_profile (user_id) VALUES (:u)"), {"u": user_id})

                    # B. 更新画像
                    sql = text("""
                               UPDATE user_profile
                               SET risk_preference=:risk,
                                   current_mood=:mood,
                                   focus_assets=:assets,
                                   investment_style=:style
                               WHERE user_id = :uid
                               """)
                    conn.execute(sql, {
                        "risk": new_risk,
                        "mood": new_mood,
                        "assets": final_assets_str,
                        "style": new_style,
                        "uid": user_id
                    })

                    # C. 提交事务
                    trans.commit()
                    print(f" [✅] 記憶更新成功！关注列表(Top5): {final_assets_str}")

                except SQLAlchemyError as db_err:
                    # 这里的代码专门处理数据库报错，比如回滚
                    if 'trans' in locals():
                        trans.rollback()
                    print(f" [!] 数据库操作失败: {db_err}")
                    raise db_err  # 抛出异常，中断程序

        except Exception as e:
            print(f" [X] 記憶更新失敗: {e}")


# --- 静态字典：品种代码 -> 中文名称 ---
PRODUCT_MAP = {
    # 金融
    'IF': '沪深300', 'IH': '上证50', 'IM': '中证1000', 'IC': '中证500','PD': '钯金','PT': '铂金',
    'TS': '2年国债', 'TF': '5年国债', 'T': '10年国债', 'TL': '30年国债','EC': '欧线',
    'MO': '中证1000', 'HO': '上证50', 'IO': '沪深300',
    # 黑色
    'RB': '螺纹钢', 'HC': '热卷', 'J': '焦炭', 'JM': '焦煤', 'I': '铁矿石','WR': '线材',
    'SS': '不锈钢', 'SM': '锰硅', 'SF': '硅铁','NR': '20号胶','OP': '双胶纸','SP': '纸浆',
    # 有色/贵金属
    'AU': '黄金', 'AG': '白银', 'CU': '铜', 'AO': '氧化铝', 'AL': '铝', 'ZN': '锌','AD': '铝合金',
    'PB': '铅', 'NI': '镍', 'SN': '锡', 'LC': '碳酸锂', 'SI': '工业硅','PS': '多晶硅',
    # 农产品
    'M': '豆粕', 'Y': '豆油', 'P': '棕榈油', 'OI': '菜油', 'RM': '菜粕','A': '豆一','B': '豆二',
    'C': '玉米', 'CS': '淀粉', 'CF': '棉花', 'SR': '白糖', 'AP': '苹果','LG': '原木','PF': '短纤',
    'JD': '鸡蛋', 'LH': '生猪', 'PK': '花生', 'CJ': '红枣','CY': '棉纱','PM': '普麦','WH': '强麦',
    # 能化
    'SC': '原油', 'FU': '燃料油', 'PG': '液化气', 'TA': 'PTA', 'MA': '甲醇','BU': '沥青','LU': 'LU燃油','SH': '烧碱','RU': '橡胶',
    'PP': '聚丙烯', 'L': '塑料', 'V': 'PVC', 'EB': '苯乙烯', 'EG': '乙二醇','BZ': '纯苯','PL': '丙烯','PR': '瓶片',
    'UR': '尿素', 'SA': '纯碱', 'FG': '玻璃', 'PX': '对二甲苯', 'BR': 'BR橡胶'
}
ETF_MAP = {
        '50ETF': '510050.SH', '上证50ETF': '510050.SH',
        '300ETF': '510300.SH', '沪深300ETF': '510300.SH',
        '500ETF': '510500.SH', '中证500ETF': '510500.SH',
        '创业板ETF': '159915.SZ', '创业板': '159915.SZ', 'CYB': '159915.SZ',
        '科创50ETF': '588000.SH', '科创板': '588000.SH', '科创50': '588000.SH',
        '深100ETF': '159901.SZ'
    }
# 反向映射表 (中文 -> 代码)
CN_TO_CODE = {v: k for k, v in PRODUCT_MAP.items()}

# 期货商名单
BROKERS_DUMB = ['中信建投', '东方财富', '方正中期', '中信建投期货（代客）', '东方财富期货（代客）', '方正中期（代客）']
BROKERS_SMART = ['海通期货', '东证期货', '国泰君安', '海通期货（代客）', '东证期货（代客）', '国泰君安（代客）','海通期货(代客)','东证期货(代客)','国泰君安(代客)']


def fmt_date(d):
    return str(d).replace('-', '').replace('/', '').split(' ')[0]


def get_join_key(ts_code):
    """标准 Join Key 生成器"""
    if not isinstance(ts_code, str): return ""
    base = ts_code.strip().upper().split('.')[0]
    if '-' in base: base = base.split('-')[0]
    match = re.search(r'([A-Z]+)(\d{3,4})$', base)
    if match:
        product = match.group(1)
        month = match.group(2)
        mapping = {'IO': 'IF', 'HO': 'IH', 'MO': 'IM'}
        final_product = mapping.get(product, product)
        return f"{final_product}{month}"
    return ""


def get_product_code(raw_code):
    """提取纯品种代码 (锰硅SM -> SM)"""
    if not isinstance(raw_code, str): return ""
    base = raw_code.strip().upper().split('.')[0]
    return "".join(re.findall("[A-Z]", base))


# --- 【新增】判断合约是否快到期 (用于过滤不准确的IV) ---
def check_expiry_validity(row, current_date_str):
    """
    逻辑：
    1. 中金所期权 (IF/IH/IM/IO/HO/MO)：
       【硬规则】只要当前日期到了当月 15 号 (含)，就强制切换到下月合约。
       (例如今天是 12月15日，IF2512 必须下榜，IF2601 上榜)

    2. 商品期权：通常在期货月份的前一个月上旬到期。
       保留原有逻辑：(估算到期日 - 当前日期) <= 1天 则过滤。
    """
    try:
        # 1. 解析年份和月份 (RB2505 -> 2025, 5 / IF2512 -> 2025, 12)
        m = re.search(r'(\d{3,4})$', row['join_key'])
        if not m: return True  # 解析失败默认保留

        ym = m.group(1)
        if len(ym) == 3: ym = '2' + ym  # 处理 505 -> 2505

        contract_year = int('20' + ym[:2])
        contract_month = int(ym[2:])

        current_date = pd.to_datetime(current_date_str)
        curr_year = current_date.year
        curr_month = current_date.month
        curr_day = current_date.day

        # === 分支 A: 金融期货/期权 (IF/IH/IM/IO/HO/MO) ===
        if row['product'] in ['IF', 'IH', 'IM', 'IO', 'HO', 'MO']:
            # 规则：如果是"过去"的合约，肯定不要
            if curr_year > contract_year: return False
            if curr_year == contract_year and curr_month > contract_month: return False

            # 规则：如果是"当月"合约，且今天 >= 15号，强制过滤
            if curr_year == contract_year and curr_month == contract_month:
                if curr_day >= 15:
                    return False

            # 其他情况（下个月及以后的合约），保留
            return True

        # === 分支 B: 商品期货/期权 ===
        else:
            # 商品期权通常在期货月份的前一个月上旬到期
            # 例如 RB2505 (5月)，期权在 4月初到期
            fut_date = pd.Timestamp(year=contract_year, month=contract_month, day=15)
            # 估算期权到期日为：交割月前一个月的 12 号
            expiry_approx = (fut_date - pd.DateOffset(months=1)).replace(day=12)

            # 计算剩余天数
            days_left = (expiry_approx - current_date).days

            # 必须大于 1 天才算有效
            return days_left > 1

    except Exception as e:
        print(f"Expiry check error: {e}")
        return True  # 出错时默认不过滤，防止数据全空

@st.cache_data(ttl=3600)
def get_comprehensive_market_data():
    """
    优化版全市场监控数据
    主要优化：
    1. 合并SQL查询，减少数据库往返
    2. 使用IN替代LIKE模糊匹配
    3. 优化数据处理流程
    """
    if engine is None:
        return pd.DataFrame()

    try:
        # === 第1步：获取日期（保持不变）===
        dates_df = pd.read_sql(
            "SELECT DISTINCT trade_date FROM futures_price ORDER BY trade_date DESC LIMIT 10",
            engine
        )
        if len(dates_df) < 6:
            return pd.DataFrame()

        today = fmt_date(dates_df.iloc[0]['trade_date'])
        prev_day = fmt_date(dates_df.iloc[1]['trade_date'])
        day_5_ago = fmt_date(dates_df.iloc[5]['trade_date'])

        # === 【优化1】合并价格查询 - 一次性获取3天数据 ===
        sql_price_all = f"""
        SELECT ts_code, close_price, oi, REPLACE(trade_date, '-', '') as trade_date 
        FROM futures_price 
        WHERE REPLACE(trade_date, '-', '') IN ('{today}', '{prev_day}', '{day_5_ago}')
        """
        df_prices_all = pd.read_sql(sql_price_all, engine)

        # 分离数据
        df_now = df_prices_all[df_prices_all['trade_date'] == today].copy()
        df_hp = df_prices_all[df_prices_all['trade_date'].isin([prev_day, day_5_ago])].copy()

        # 处理join_key
        df_now['join_key'] = df_now['ts_code'].apply(get_join_key)
        df_now = df_now[df_now['join_key'] != ""]

        # === 【优化2】合并IV查询 - 一次性获取历史和最新数据 ===
        date_7d = (pd.to_datetime(today) - pd.Timedelta(days=7)).strftime('%Y%m%d')
        date_1y = (pd.to_datetime(today) - pd.Timedelta(days=252)).strftime('%Y%m%d')

        sql_iv_all = f"""
        SELECT ts_code, iv, REPLACE(trade_date, '-', '') as trade_date 
        FROM commodity_iv_history 
        WHERE REPLACE(trade_date, '-', '') >= '{date_1y}'
        """
        df_iv_all = pd.read_sql(sql_iv_all, engine)
        df_iv_all['join_key'] = df_iv_all['ts_code'].apply(get_join_key)
        df_iv_all = df_iv_all[df_iv_all['join_key'] != ""]

        # 分离最新7天和全年数据
        df_iv_recent = df_iv_all[df_iv_all['trade_date'] >= date_7d].copy()
        df_iv_latest = df_iv_recent.sort_values('trade_date').groupby('join_key').tail(1)[['join_key', 'iv']]

        # === 第4步：智能选择合约（保持不变）===
        df_cand = df_now.merge(df_iv_latest, on='join_key', how='left')
        df_cand['product'] = df_cand['join_key'].apply(lambda x: re.match(r"([a-zA-Z]+)", x).group(1))
        df_cand['iv'] = df_cand['iv'].fillna(0)
        df_cand['has_iv'] = df_cand['iv'] > 0.0001

        def get_m_num(k):
            m = re.search(r'\d+$', k)
            if not m: return 99999
            v = int(m.group(0))
            return v + 20000 if v < 1000 else v

        df_cand['m_num'] = df_cand['join_key'].apply(get_m_num)
        # 1. 过滤快到期的合约
        # row 必须包含 'join_key' 和 'product'，并且需要传入当前日期 today
        df_cand['is_valid'] = df_cand.apply(lambda row: check_expiry_validity(row, today), axis=1)
        df_valid = df_cand[df_cand['is_valid']].copy()

        # 2. 挑选逻辑：每种商品选 [持仓最大] 和 [月份最近] 两个

        # A. 选持仓最大的 (OI Max)
        top_oi = df_valid.sort_values('oi', ascending=False).groupby('product').head(1)

        # B. 选月份最近的 (m_num Min)
        # m_num 是之前代码里计算出来的数字月份，越小代表越近月
        top_near = df_valid.sort_values('m_num', ascending=True).groupby('product').head(1)

        # 3. 合并并去重
        # 如果主力合约刚好也是近月合约，drop_duplicates 会自动把它们变成一条
        df_selected = pd.concat([top_oi, top_near]).drop_duplicates(subset=['join_key'])

        # === 第5步：计算IV Rank（使用已加载的全年数据）===
        keys = df_selected['join_key'].unique().tolist()
        if keys:
            date_1y = (pd.to_datetime(today) - pd.Timedelta(days=252)).strftime('%Y%m%d')
            sql_h = f"SELECT ts_code, iv FROM commodity_iv_history WHERE REPLACE(trade_date, '-', '') >= '{date_1y}'"
            df_h = pd.read_sql(sql_h, engine)
            df_h['join_key'] = df_h['ts_code'].apply(get_join_key)
            df_h = df_h[df_h['join_key'].isin(keys)]

            # --- 【核心修改】 过滤掉 IV 为 0 的异常值 ---
            # 只有大于 0.0001 的 IV 才参与统计
            # 这样 Min 值就是“历史最低的有效IV”，而不是 0
            df_h_valid = df_h[df_h['iv'] > 0.0001]

            if not df_h_valid.empty:
                stats = df_h_valid.groupby('join_key')['iv'].agg(['min', 'max']).reset_index()
                df_final = df_selected.merge(stats, on='join_key', how='left')
            else:
                # 如果全是 0，给个空列防止报错
                df_final = df_selected.copy()
                df_final['min'] = 0
                df_final['max'] = 0

            # 计算 Rank
            df_final['iv_range'] = df_final['max'] - df_final['min']
            # 分母极小时保护
            df_final['iv_rank'] = np.where(
                df_final['iv_range'] > 0.0001,
                (df_final['iv'] - df_final['min']) / df_final['iv_range'] * 100,
                0
            )
            df_final['iv_rank'] = df_final['iv_rank'].replace([np.inf, -np.inf], 0).fillna(0)
        else:
            df_final = df_selected
            df_final['iv_rank'] = 0

        # === 第6步：历史IV数据（使用已加载的数据）===
        df_hiv = df_iv_all[df_iv_all['trade_date'].isin([prev_day, day_5_ago])].copy()
        df_hiv = df_hiv.groupby(['join_key', 'trade_date'])['iv'].mean().reset_index()

        # === 【优化3】持仓数据 - 使用IN替代LIKE ===
        date_15d = (pd.to_datetime(today) - pd.Timedelta(days=20)).strftime('%Y%m%d')

        # 精确匹配列表（比LIKE快10倍+）
        target_brokers = BROKERS_DUMB + BROKERS_SMART
        broker_placeholders = ','.join([f"'{b}'" for b in target_brokers])

        try:
            sql_hold = f"""
            SELECT ts_code, broker, long_vol, short_vol, REPLACE(trade_date, '-', '') as trade_date
            FROM futures_holding 
            WHERE REPLACE(trade_date, '-', '') >= '{date_15d}'
              AND broker IN ({broker_placeholders})
            """
            df_hold = pd.read_sql(sql_hold, engine)

            if not df_hold.empty:
                df_hold['product'] = df_hold['ts_code'].apply(get_product_code)
                df_hold['net_vol'] = df_hold['long_vol'] - df_hold['short_vol']

                def get_type(b):
                    for name in BROKERS_DUMB:
                        if name == b: return 'dumb'  # 精确匹配
                    for name in BROKERS_SMART:
                        if name == b: return 'smart'
                    return 'other'

                df_hold['type'] = df_hold['broker'].apply(get_type)
                df_h_agg = df_hold.groupby(['product', 'trade_date', 'type'])['net_vol'].sum().unstack(
                    fill_value=0).reset_index()

                if 'dumb' not in df_h_agg.columns: df_h_agg['dumb'] = 0
                if 'smart' not in df_h_agg.columns: df_h_agg['smart'] = 0
                df_h_agg.rename(columns={'dumb': 'dumb_net', 'smart': 'smart_net'}, inplace=True)

                df_h_agg = df_h_agg.sort_values(['product', 'trade_date'])
                df_h_agg['dumb_chg_1d'] = df_h_agg.groupby('product')['dumb_net'].diff(1).fillna(0)
                df_h_agg['smart_chg_1d'] = df_h_agg.groupby('product')['smart_net'].diff(1).fillna(0)
                df_h_agg['dumb_chg_5d'] = df_h_agg.groupby('product')['dumb_net'].diff(5).fillna(0)
                df_h_agg['smart_chg_5d'] = df_h_agg.groupby('product')['smart_net'].diff(5).fillna(0)

                df_h_final = df_h_agg.groupby('product').tail(1)
            else:
                df_h_final = pd.DataFrame()
        except Exception as e:
            print(f"Holding Error: {e}")
            df_h_final = pd.DataFrame()

        # === 第8步：数据合并（优化版）===
        def get_hist_data(date):
            p = df_hp[df_hp['trade_date'] == date][['ts_code', 'close_price']]
            i = df_hiv[df_hiv['trade_date'] == date][['join_key', 'iv']]
            return p, i

        p_prev, i_prev = get_hist_data(prev_day)
        p_5d, i_5d = get_hist_data(day_5_ago)

        df_final = df_final.merge(p_prev, on='ts_code', suffixes=('', '_prev'), how='left')
        df_final = df_final.merge(p_5d, on='ts_code', suffixes=('', '_5d'), how='left')
        df_final = df_final.merge(i_prev, on='join_key', suffixes=('', '_prev'), how='left')
        df_final = df_final.merge(i_5d, on='join_key', suffixes=('', '_5d'), how='left')

        if not df_h_final.empty:
            df_final = df_final.merge(
                df_h_final[['product', 'dumb_chg_1d', 'dumb_chg_5d', 'smart_chg_1d', 'smart_chg_5d']],
                on='product', how='left'
            )

        # === 第9步：计算指标（保持不变）===
        df_final['当日涨跌%'] = ((df_final['close_price'] - df_final['close_price_prev']) /
                                 df_final['close_price_prev'] * 100).fillna(0)
        df_final['5日涨跌%'] = ((df_final['close_price'] - df_final['close_price_5d']) /
                                df_final['close_price_5d'] * 100).fillna(0)

        for c in ['iv', 'iv_prev', 'iv_5d']:
            df_final[c] = df_final[c].fillna(0)
        df_final['当日IV变动'] = np.where(df_final['iv_prev'] > 0.0001, df_final['iv'] - df_final['iv_prev'], 0)
        df_final['5日IV变动'] = np.where(df_final['iv_5d'] > 0.0001, df_final['iv'] - df_final['iv_5d'], 0)

        for c in ['dumb_chg_1d', 'dumb_chg_5d', 'smart_chg_1d', 'smart_chg_5d']:
            if c not in df_final.columns: df_final[c] = 0
            df_final[c] = df_final[c].fillna(0)

        df_final['反指变动(日)'] = df_final['dumb_chg_1d']
        df_final['反指变动(5日)'] = df_final['dumb_chg_5d']
        df_final['正指变动(日)'] = df_final['smart_chg_1d']
        df_final['正指变动(5日)'] = df_final['smart_chg_5d']

        # === 第10步：格式化输出（保持不变）===
        def fmt_name(row):
            code = row['join_key']
            prod = row['product']
            cn = PRODUCT_MAP.get(prod, "")
            return f"{code} ({cn})" if cn else code

        df_final['合约'] = df_final.apply(fmt_name, axis=1)

        curr_yymm = int(pd.to_datetime(today).strftime('%y%m'))

        def fmt_rank(row):
            if row['iv'] < 0.0001:
                m = re.search(r'\d{3,4}$', row['join_key'])
                if m:
                    m_str = m.group(0)
                    if len(m_str) == 3: m_str = "2" + m_str
                    if int(m_str) <= curr_yymm + 1: return "快到期"
                return 0
            return int(round(row['iv_rank'], 0))

        df_final['iv_rank_display'] = df_final.apply(fmt_rank, axis=1)

        cols = ['合约', 'iv', 'iv_rank_display', '当日IV变动', '5日IV变动', '当日涨跌%', '5日涨跌%',
                '反指变动(日)', '反指变动(5日)', '正指变动(日)', '正指变动(5日)']
        res = df_final[cols].copy()
        res.columns = ['合约', '当前IV', 'IV Rank', 'IV变动(日)', 'IV变动(5日)', '涨跌%(日)', '涨跌%(5日)',
                       '散户变动(日)', '散户变动(5日)', '机构变动(日)', '机构变动(5日)']

        return res.round(2)

    except Exception as e:
        print(f"DataEngine Error: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


# ==========================================
#   核心功能：AI 专用 IV 查询工具
# ==========================================

# ==========================================
#   核心功能：AI 专用 IV 查询工具（商品 + ETF 期权）
# ==========================================

@tool
def get_commodity_iv_info(query: str):
    """
    查询指定商品或ETF的隐含波动率(IV)数据。

    逻辑：
    1. 默认返回：最新IV数值 + 近期变动趋势（节省资源）。
    2. 深度返回：只有当用户问题包含"IV等级"、"贵"、"便宜"、"分位"等词时，才计算IV Rank。
    """
    if engine is None:
        return "❌ 数据库未连接"

    # --- 1. 意图识别：判断用户是否需要 Rank 数据 ---
    keywords_rank = ['rank', '排名', '分位', '贵', '便宜', '位置', '历史', '高', '低', '水平']
    need_rank = any(k in query.lower() for k in keywords_rank)

    # 决定查询天数：只要趋势查5天就够，要排名才查250天
    limit_days = 252 if need_rank else 5

    # --- 2. ETF 识别（新增逻辑）---
    ETF_MAP = {
        '50ETF': '510050.SH', '上证50ETF': '510050.SH', '上证50': '510050.SH',
        '300ETF': '510300.SH', '沪深300ETF': '510300.SH', '沪深300': '510300.SH',
        '500ETF': '510500.SH', '中证500ETF': '510500.SH', '中证500': '510500.SH',
        '创业板ETF': '159915.SZ', '创业板': '159915.SZ', 'CYB': '159915.SZ',
        '科创50ETF': '588000.SH', '科创板': '588000.SH', '科创50': '588000.SH',
        '深100ETF': '159901.SZ', '深100': '159901.SZ'
    }

    etf_code = None
    etf_name = None

    # 【调试日志】打印原始查询
    print(f"[ETF识别] 原始查询: {query}")

    # 【修复版】ETF 识别逻辑：优先匹配最长关键词
    query_upper = query.upper()
    matches = []

    for name, code in ETF_MAP.items():
        # 1. 尝试完全匹配 (例如 "科创50ETF")
        if name.upper() in query_upper:
            matches.append({'name': name, 'code': code, 'len': len(name)})
            continue  # 如果全名匹配了，就不用试简写了

        # 2. 尝试去后缀匹配 (例如 "50ETF" -> "50")
        short_name = name.upper().replace('ETF', '')
        if len(short_name) > 0 and short_name in query_upper:
            matches.append({'name': name, 'code': code, 'len': len(short_name)})

    if matches:
        # 核心修复：按匹配词的长度降序排列，取最长的一个
        # 这样 "科创50" (长度4) 就会排在 "50" (长度2) 前面
        best_match = sorted(matches, key=lambda x: x['len'], reverse=True)[0]
        etf_code = best_match['code']
        etf_name = best_match['name']
        print(f"[ETF识别] ✅ 最终匹配: {etf_name} ({etf_code}) (匹配长度: {best_match['len']})")

    # 也支持直接输入代码（如 510050、510300）
    if not etf_code:
        import re
        match_code = re.search(r'(510\d{3}|159\d{3}|588\d{3})', query)
        if match_code:
            raw_code = match_code.group(1)
            # 根据开头判断交易所
            etf_code = f"{raw_code}.SZ" if raw_code.startswith('159') else f"{raw_code}.SH"
            etf_name = f"{raw_code}ETF"
            print(f"[ETF识别] 代码匹配成功 - {etf_code}")

    # 【调试日志】打印识别结果
    if etf_code:
        print(f"[ETF识别] ✅ 最终结果 - 名称: {etf_name}, 代码: {etf_code}")
    else:
        print(f"[ETF识别] ❌ 未识别为ETF，将按商品期权处理")

    # --- 3. 分支处理 ---
    if etf_code:
        # ========== ETF 期权查询 ==========
        return _query_etf_iv(etf_code, etf_name, query, need_rank, limit_days)
    else:
        # ========== 商品期权查询（保持原逻辑）==========
        return _query_commodity_iv(query, need_rank, limit_days)


# ==========================================
#   子函数 A: ETF 期权 IV 查询
# ==========================================
def _query_etf_iv(etf_code, etf_name, query, need_rank, limit_days):
    """查询 ETF 期权的 IV 数据"""
    try:
        # 【调试日志】打印查询信息
        print(f"[ETF IV 查询] 标的: {etf_name}, 代码: {etf_code}, 查询天数: {limit_days}")

        # 1. 【修复】直接查询历史 IV 数据（ETF不需要找主力合约）
        sql_iv = f"""
            SELECT REPLACE(trade_date, '-', '') as trade_date, iv 
            FROM etf_iv_history 
            WHERE etf_code = '{etf_code}' 
            ORDER BY trade_date DESC 
            LIMIT {limit_days}
        """
        df_iv = pd.read_sql(sql_iv, engine)

        # 【调试日志】打印查询结果
        print(f"[ETF IV 查询] 查到 {len(df_iv)} 条记录")
        if len(df_iv) > 0:
            print(f"[ETF IV 查询] 最新日期: {df_iv.iloc[0]['trade_date']}, IV: {df_iv.iloc[0]['iv']}")

        # 2. 检查是否有数据
        if df_iv.empty:
            # 【增强错误提示】告知用户可能的原因
            return f"""
⚠️ 未找到 ETF【{etf_name}】的波动率数据。

可能原因：
1. 数据库中该 ETF 代码为: {etf_code}，请确认是否正确
2. etf_iv_history 表中可能还没有该标的的数据
3. 请检查数据采集脚本是否正常运行

💡 提示：可以尝试查询其他 ETF（如"300ETF波动率"）来验证功能是否正常。
"""

        # 3. 提取最新日期和 IV
        latest_date = df_iv.iloc[0]['trade_date']
        date_str = str(latest_date).replace('-', '')
        curr_iv = df_iv.iloc[0]['iv']

        # 【调试日志】打印处理逻辑
        print(f"[ETF IV 查询] 当前IV: {curr_iv}%, 是否需要Rank: {need_rank}")

        # --- 分支 A: 仅回复近期趋势 (省流模式) ---
        if not need_rank:
            # 计算日变动
            iv_change_text = "持平"
            trend_text = "波动平稳"

            if len(df_iv) > 1:
                prev_iv = df_iv.iloc[1]['iv']
                diff = curr_iv - prev_iv
                if diff > 0.5:
                    iv_change_text = f"大幅上升 (+{diff:.2f}%)"
                elif diff > 0:
                    iv_change_text = f"小幅回升 (+{diff:.2f}%)"
                elif diff < -0.5:
                    iv_change_text = f"大幅回落 ({diff:.2f}%)"
                else:
                    iv_change_text = f"微跌 ({diff:.2f}%)"

            if len(df_iv) >= 5:
                iv_5d_ago = df_iv.iloc[4]['iv']
                diff_5d = curr_iv - iv_5d_ago
                if diff_5d > 2:
                    trend_text = "🌊 近期波动率显著放大，市场激情"
                elif diff_5d < -2:
                    trend_text = "💤 近期波动率持续走低，行情平淡"
                else:
                    trend_text = "➡️ 近一周波动率维持窄幅震荡"

            return f"""
📊 **{etf_name} ({etf_code}) 波动率**
--------------------------------
📅 日期: {date_str}
🔥 **当前 IV: {curr_iv:.2f}%**
📈 **较昨日: {iv_change_text}**
🌊 **近期趋势**: {trend_text}
--------------------------------
            """

        # --- 分支 B: 回复 Rank 和策略 (详细模式) ---
        else:
            max_iv = df_iv['iv'].max()
            min_iv = df_iv['iv'].min()

            if max_iv != min_iv:
                iv_rank = (curr_iv - min_iv) / (max_iv - min_iv) * 100
            else:
                iv_rank = 0

            if iv_rank < 20:
                status = "📉 极低 (权利金便宜)"
            elif iv_rank < 50:
                status = "☁️ 偏低"
            elif iv_rank < 80:
                status = "📈 偏高"
            else:
                status = "🔥 极高 (权利金昂贵)"

            return f"""
📊 **{etf_name} ({etf_code}) 深度波动率分析**
--------------------------------
📅 日期: {date_str}
🌊 **当前 IV: {curr_iv:.2f}%**
🏆 **IV Rank: {iv_rank:.1f}% ({status})**

🔍 统计周期: 过去 {len(df_iv)} 个交易日
📺 历史最高: {max_iv:.2f}%
📻 历史最低: {min_iv:.2f}%
--------------------------------
💡 *策略参考: 当前IV处于{'历史高位，权利金较贵，卖方有统计上优势' if iv_rank > 50 else '历史低位，权利金便宜，买方有潜力'}。*
            """

    except Exception as e:
        return f"ETF波动率查询发生错误: {e}"


# ==========================================
#   子函数 B: 商品期权 IV 查询（原有逻辑）
# ==========================================
def _query_commodity_iv(query, need_rank, limit_days):
    """查询商品期权的 IV 数据"""

    # 1. 商品代码映射
    target_code = None
    target_name = query
    clean_query = re.sub(r'[^a-zA-Z]', '', query).upper()

    if clean_query in PRODUCT_MAP:
        target_code = clean_query
        target_name = PRODUCT_MAP[clean_query]

    if not target_code:
        for code, name in PRODUCT_MAP.items():
            if name in query:
                target_code = code
                target_name = name
                break

    if not target_code:
        match = re.match(r'([a-zA-Z]+)', query)
        if match:
            target_code = match.group(1).upper()

    if not target_code:
        return f"⚠️ 未找到商品【{query}】。"

    try:
        # 🔥【关键修复开始】金融期权映射表
        # 解释：数据库里 futures_price 只有期货(IM)，没有期权(MO)
        # 所以查主力合约时，必须把 MO 映射为 IM 去查活跃月份
        FIN_OPT_MAP = {
            'MO': 'IM',  # 中证1000期权 -> 查 IM 期货
            'HO': 'IH',  # 上证50期权   -> 查 IH 期货
            'IO': 'IF'  # 沪深300期权 -> 查 IF 期货
        }

        # 如果是 MO，search_code 变成 IM；否则保持原样 (如 M, RB)
        search_code = FIN_OPT_MAP.get(target_code, target_code)

        # 2. 寻找主力合约 (使用 search_code 去查期货表)
        sql_main = f"""
            SELECT ts_code, close_price, REPLACE(trade_date, '-', '') as trade_date 
            FROM futures_price 
            WHERE {sql_prefix_condition(search_code)}
              AND ts_code NOT LIKE '%%TAS%%'
              AND trade_date = (SELECT MAX(trade_date) FROM futures_price)
            ORDER BY oi DESC LIMIT 1
        """
        df_main = pd.read_sql(sql_main, engine)

        if df_main.empty:
            return f"⚠️ 暂无品种【{target_code}】(关联期货 {search_code}) 的数据。"

        # 获取主力合约，例如 "IM2502"
        main_contract_future = df_main.iloc[0]['ts_code']
        curr_price = df_main.iloc[0]['close_price']
        date_str = df_main.iloc[0]['trade_date']

        # 3. 确定 IV 查询用的合约代码
        # 假设您的 IV 表里存的是和期货一样的代码 (IM2502)，或者我们先查 IM2502
        iv_search_code = main_contract_future

        # 查询 IV 数据
        sql_iv = f"""
            SELECT trade_date, iv 
            FROM commodity_iv_history 
            WHERE ts_code = '{iv_search_code}' 
            ORDER BY trade_date DESC 
            LIMIT {limit_days}
        """
        df_iv = pd.read_sql(sql_iv, engine)

        # 兜底：如果查 IM2502 没查到 IV，尝试替换前缀查 MO2502 (防止数据库存的是 MO 开头)
        if df_iv.empty and target_code in FIN_OPT_MAP:
            alt_code = main_contract_future.replace(search_code, target_code)  # IM2502 -> MO2502
            sql_iv_alt = f"SELECT trade_date, iv FROM commodity_iv_history WHERE ts_code = '{alt_code}' ORDER BY trade_date DESC LIMIT {limit_days}"
            df_iv = pd.read_sql(sql_iv_alt, engine)
            if not df_iv.empty:
                iv_search_code = alt_code  # 修正为实际查到的代码
        # 🔥【关键修复结束】

        if df_iv.empty:
            return f"⚠️ 合约【{iv_search_code}】暂无IV数据。"

        curr_iv = df_iv.iloc[0]['iv']

        # --- 分支 A: 仅回复近期趋势 ---
        if not need_rank:
            iv_change_text = "持平"
            trend_text = "波动平稳"

            if len(df_iv) > 1:
                prev_iv = df_iv.iloc[1]['iv']
                diff = curr_iv - prev_iv
                if diff > 0.5:
                    iv_change_text = f"大幅上升 (+{diff:.2f}%)"
                elif diff > 0:
                    iv_change_text = f"小幅回升 (+{diff:.2f}%)"
                elif diff < -0.5:
                    iv_change_text = f"大幅回落 ({diff:.2f}%)"
                else:
                    iv_change_text = f"微跌 ({diff:.2f}%)"

            if len(df_iv) >= 5:
                iv_5d_ago = df_iv.iloc[4]['iv']
                diff_5d = curr_iv - iv_5d_ago
                if diff_5d > 2:
                    trend_text = "🌊 近期波动率放大，市场激情"
                elif diff_5d < -2:
                    trend_text = "💤 近期波动率走低，行情平淡"
                else:
                    trend_text = "➡️ 近一周波动率维持窄幅震荡"

            return f"""
📊 **{target_name} ({iv_search_code}) 波动率**
--------------------------------
📅 日期: {date_str}
🔥 **当前 IV: {curr_iv:.2f}%**
📈 **较昨日: {iv_change_text}**
🌊 **近期趋势**: {trend_text}
--------------------------------
            """

        # --- 分支 B: 回复 Rank 和策略 ---
        else:
            max_iv = df_iv['iv'].max()
            min_iv = df_iv['iv'].min()

            if max_iv != min_iv:
                iv_rank = (curr_iv - min_iv) / (max_iv - min_iv) * 100
            else:
                iv_rank = 0

            if iv_rank < 20:
                status = "📉 极低 (权利金便宜)"
            elif iv_rank < 50:
                status = "☁️ 偏低"
            elif iv_rank < 80:
                status = "📈 偏高"
            else:
                status = "🔥 极高 (权利金昂贵)"

            return f"""
📊 **{target_name} ({iv_search_code}) 深度波动率分析**
--------------------------------
📅 日期: {date_str}
🌊 **当前 IV: {curr_iv:.2f}%**
🏆 **IV Rank: {iv_rank:.1f}% ({status})**

🔍 统计周期: 过去 {len(df_iv)} 个交易日
📺 历史最高: {max_iv:.2f}%
📻 历史最低: {min_iv:.2f}%
--------------------------------
            """

    except Exception as e:
        return f"商品波动率查询发生错误: {e}"


# --- AI 工具: 查期权到期 (高性能版) ---
@tool
def check_option_expiry_status(query: str):
    """
    查询 商品期权 或 ETF期权 的到期日。
    """
    # --- 1. 内置字典 ---
    LOCAL_PRODUCT_MAP = {
        'IF': '沪深300', 'IH': '上证50', 'IM': '中证1000', 'IC': '中证500',
        'MO': '中证1000股指期权', 'HO': '上证50股指期权', 'IO': '沪深300股指期权',
        'TS': '2年国债', 'TF': '5年国债', 'T': '10年国债', 'TL': '30年国债',
        'RB': '螺纹钢', 'HC': '热卷', 'J': '焦炭', 'JM': '焦煤', 'I': '铁矿石',
        'M': '豆粕', 'Y': '豆油', 'P': '棕榈油', 'OI': '菜油', 'RM': '菜粕',
        'C': '玉米', 'CF': '棉花', 'SR': '白糖', 'AP': '苹果', 'JD': '鸡蛋',
        'LH': '生猪', 'PK': '花生', 'SC': '原油', 'FU': '燃油', 'PG': '液化气',
        'TA': 'PTA', 'MA': '甲醇', 'PP': '聚丙烯', 'L': '塑料', 'V': 'PVC',
        'EB': '苯乙烯', 'EG': '乙二醇', 'UR': '尿素', 'SA': '纯碱', 'FG': '玻璃','RU': '橡胶',
        'PX': '对二甲苯', 'BR': 'BR橡胶', 'LC': '碳酸锂', 'SI': '工业硅', 'AO': '氧化铝',
        'SS': '不锈钢', 'SM': '锰硅', 'SF': '硅铁', 'WR': '线材', 'CU': '铜',
        'AL': '铝', 'ZN': '锌', 'PB': '铅', 'NI': '镍', 'SN': '锡', 'AU': '黄金', 'AG': '白银'
    }

    ETF_MAP = {
        '50ETF': '510050.SH', '上证50ETF': '510050.SH',
        '300ETF': '510300.SH', '沪深300ETF': '510300.SH',
        '500ETF': '510500.SH', '中证500ETF': '510500.SH',
        '创业板ETF': '159915.SZ', '创业板': '159915.SZ', 'CYB': '159915.SZ',
        '科创50ETF': '588000.SH', '科创板': '588000.SH', '科创50': '588000.SH',
        '深100ETF': '159901.SZ'
    }

    local_engine = get_db_engine()
    if local_engine is None: return "❌ 数据库未连接"

    try:
        clean_query = re.sub(r'[^a-zA-Z0-9\u4e00-\u9fa5]', '', query).upper()

        # ==========================================
        #  分支 A: ETF 期权查询 (使用新字段)
        # ==========================================
        etf_code = None
        etf_name = None

        for name, code in ETF_MAP.items():
            if name in clean_query:
                etf_code = code;
                etf_name = name;
                break

        if not etf_code:
            match_etf = re.search(r'(510\d{3}|159\d{3}|588\d{3})', clean_query)
            if match_etf:
                raw_code = match_etf.group(1)
                etf_code = f"{raw_code}.SZ" if raw_code.startswith('159') else f"{raw_code}.SH"
                etf_name = raw_code

        if etf_code:
            today_str = datetime.now().strftime('%Y%m%d')

            # 【核心修改】使用 underlying 和 delist_date
            # 我们直接把 delist_date 重命名为 maturity_date，方便后面统一处理
            sql_etf = f"""
                SELECT DISTINCT delist_date as maturity_date 
                FROM option_basic 
                WHERE underlying = '{etf_code}' 
                  AND delist_date >= '{today_str}'
                ORDER BY delist_date ASC
                LIMIT 1
            """

            try:
                df_etf = pd.read_sql(sql_etf, local_engine)
            except Exception as e:
                return f"❌ ETF查询SQL错误: {e}"

            if df_etf.empty:
                return f"⚠️ 未找到 ETF【{etf_name} ({etf_code})】的期权到期日。\n(已查询表: option_basic, 字段: underlying='{etf_code}')"

            expiry_date = df_etf.iloc[0]['maturity_date']
            target_obj = f"{etf_name} ({etf_code}) 当月合约"

        else:
            # ==========================================
            #  分支 B: 商品期权 (保持原有的健壮逻辑)
            # ==========================================
            text_part = re.sub(r'\d', '', clean_query)
            product_code = None
            product_name = text_part

            if text_part in LOCAL_PRODUCT_MAP:
                product_code = text_part;
                product_name = LOCAL_PRODUCT_MAP[text_part]
            else:
                for k, v in LOCAL_PRODUCT_MAP.items():
                    if v in text_part: product_code = k; product_name = v; break
                if not product_code:
                    m_head = re.match(r'^([A-Z]+)', clean_query)
                    if m_head: product_code = m_head.group(1)

            if not product_code:
                return f"⚠️ 未识别商品【{query}】，若是ETF请提供准确名称。"

            # 提取 Key
            match_digits = re.search(r'(\d{3,4})', clean_query)
            user_month = match_digits.group(1) if match_digits else None

            target_key = None
            is_main = False
            expiry_date = None

            if user_month:
                # 用户指定了月份
                # 【修复】郑商所品种使用3位月份格式（如MA602而非MA2602）
                CZCE_PRODUCTS = {'CF', 'SR', 'TA', 'MA', 'OI', 'RM', 'FG', 'ZC', 'SF', 'SM',
                                 'AP', 'CJ', 'UR', 'SA', 'PF', 'PK', 'PX', 'SH', 'WH', 'PM'}

                is_czce = product_code.upper() in CZCE_PRODUCTS

                if len(user_month) <= 2:
                    if is_czce:
                        year_digit = str(datetime.now().year % 10)
                        target_key = f"{product_code}{year_digit}{int(user_month):02d}"
                    else:
                        curr = datetime.now().year % 100
                        target_key = f"{product_code}{curr}{int(user_month):02d}"
                elif len(user_month) == 4 and is_czce:
                    # 郑商所：2602 → 602
                    target_key = f"{product_code}{user_month[1:]}"
                else:
                    target_key = f"{product_code}{user_month}"

                # 查询指定月份的到期日
                sql_d = f"""
                    SELECT maturity_date 
                    FROM commodity_option_basic 
                    WHERE {sql_prefix_condition(target_key)} AND ts_code NOT LIKE '%%TAS%%'
                    ORDER BY maturity_date ASC 
                    LIMIT 1
                """
                df_d = pd.read_sql(sql_d, local_engine)
                today_str = datetime.now().strftime('%Y%m%d')
                if df_d.empty:
                    # 备选兜底：如果上面都没查到，尝试宽泛查询
                    sql_fallback = f"""
                                    SELECT ts_code, maturity_date 
                                    FROM commodity_option_basic 
                                    WHERE {sql_prefix_condition(product_code)}
                                    ORDER BY maturity_date ASC 
                                    LIMIT 10
                                """
                    df_fallback = pd.read_sql(sql_fallback, local_engine)
                    # 过滤过期和 TAS
                    df_fallback = df_fallback[
                        (df_fallback['maturity_date'] >= today_str) &
                        (~df_fallback['ts_code'].str.contains('TAS'))
                        ]

                    # 正则二次匹配
                    pattern = re.compile(f"^{product_code}\\d", re.IGNORECASE)
                    df_opt = df_fallback[df_fallback['ts_code'].apply(lambda x: bool(pattern.match(x)))].head(1)

                if df_d.empty:
                    return f"⚠️ 未找到合约【{target_key}】的到期日，请确认月份是否正确。"

                expiry_date = df_d.iloc[0]['maturity_date']

            else:
# ==========================================
                # 【核心修复3】MySQL 正则精准查询
                # 解决 M(豆粕) 被 MA(甲醇) 淹没的问题
                # ==========================================
                today_str = datetime.now().strftime('%Y%m%d')

                # 使用 MySQL 的 REGEXP 操作符
                # ^{product_code}[0-9] 意思是以 "代码+数字" 开头
                # 例如：^M[0-9] 能匹配 M2601，但不能匹配 MA2601
                # 🔥【修复】同时查询大写(UR)和小写(ur)，防止漏掉郑商所数据
                sql_opt = f"""
                    SELECT ts_code, maturity_date 
                    FROM commodity_option_basic 
                    WHERE {sql_prefix_condition(product_code)}
                      AND ts_code NOT LIKE '%%TAS%%'
                      AND maturity_date >= '{today_str}'
                    ORDER BY maturity_date ASC 
                    LIMIT 1
                """

                # 如果数据库不支持 REGEXP (比如是 SQLite)，则使用备选方案：大幅增加 LIMIT
                # sql_opt = f"""
                #     SELECT ts_code, maturity_date
                #     FROM commodity_option_basic
                #     WHERE ts_code LIKE '{product_code}%%'
                #       AND maturity_date >= '{today_str}'
                #     ORDER BY maturity_date ASC
                #     LIMIT 3000
                # """

                df_opt = pd.read_sql(sql_opt, local_engine)

                if df_opt.empty:
                    # 备选尝试：有些数据源代码可能是小写，或者格式不同，尝试宽泛查询再过滤
                    # 只有在精准查询失败时才跑这个兜底逻辑
                    sql_fallback = f"""
                        SELECT ts_code, maturity_date 
                        FROM commodity_option_basic 
                        WHERE {sql_prefix_condition(product_code)} AND ts_code NOT LIKE '%%TAS%%'
                          AND maturity_date >= '{today_str}'
                        ORDER BY maturity_date ASC 
                        LIMIT 1000
                    """
                    df_fallback = pd.read_sql(sql_fallback, local_engine)

                    # Python 端二次过滤
                    pattern = re.compile(f"^{product_code}\\d", re.IGNORECASE)
                    df_opt = df_fallback[df_fallback['ts_code'].apply(lambda x: bool(pattern.match(x)))].head(1)

                if df_opt.empty:
                    return f"⚠️ 暂无【{product_name}】未到期的期权合约。"

                # 3. 提取结果
                raw = df_opt.iloc[0]['ts_code'].upper()
                # 再次用正则提取纯代码，确保展示美观 (M2601.DCE -> M2601)
                m = re.match(r'^([A-Z]+)(\d{3,4})', raw)
                target_key = m.group(0) if m else raw.split('.')[0]
                expiry_date = df_opt.iloc[0]['maturity_date']

            target_obj = f"{product_name} ({target_key})"

        # ==========================================
        #  统一计算
        # ==========================================
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)  # 只保留日期
        # 兼容处理: 如果数据库里是数字(20250325)或字符串
        expiry_date = pd.to_datetime(str(expiry_date))
        days_left = (expiry_date - today).days

        if days_left > 50:
            phase = "🌙 远期"
            advice = "时间价值衰减慢，主要受隐含波动率影响。"
        elif 20 < days_left <= 50:
            phase = "🌓 中期"
            advice = "时间和波动率影响都重要，卖方虚值收租黄金期。"
        elif 5 < days_left <= 20:
            phase = "🌔 近期"
            advice = "时间衰减加快，Gamma造成的方向加速开始体现。"
        elif 0 < days_left <= 5:
            phase = "⚡ 末日轮"
            advice = "买卖方决战集中在平值附近，⚠️ Gamma的暴击是最大。"
        else:
            phase = "💀 已过期"
            advice = "合约已到期。"

        return f"""
✅ **{target_obj}**
📅 到期: {expiry_date.strftime('%Y-%m-%d')}
⏱️ 剩余: **{days_left}天** ({phase})
💡 建议: {advice}
        """

    except Exception as e:
        return f"查询出错: {str(e)}"

# --- 【新增/保留】高性能静态数据缓存 (12小时只查一次库) ---
@st.cache_data(ttl=36000)
def get_static_maturity_map():
    """
    【高速缓存】加载期权到期日。
    修复：增强日期格式兼容性，防止因格式问题导致数据为空。
    """
    # 1. 获取连接
    local_engine = get_db_engine()
    if local_engine is None:
        print("❌ [Cache] 数据库连接失败")
        return {}

    try:
        # 2. 全量拉取
        print("🔄 [System] 正在刷新期权到期日缓存...")
        sql = "SELECT ts_code, maturity_date FROM commodity_option_basic"
        df = pd.read_sql(sql, local_engine)

        if df.empty:
            print("❌ [Cache] commodity_option_basic 表为空！")
            return {}

        # 3. 【核心修复】强制日期格式转换
        # 兼容: 20250520(int), "20250520"(str), "2025-05-20"(str)
        df['maturity_date'] = pd.to_datetime(df['maturity_date'], format='%Y%m%d', errors='coerce').fillna(
            pd.to_datetime(df['maturity_date'], errors='coerce'))

        # 4. 过滤无效日期
        df = df.dropna(subset=['maturity_date'])
        # 【修复】放宽过滤条件，保留近30天内到期的合约
        cutoff_date = pd.Timestamp.now().normalize() - pd.Timedelta(days=30)
        df = df[df['maturity_date'] >= cutoff_date]

        # 5. 智能提取 Key (正则 + 强制大写)
        def _extract_key(code):
            try:
                import re
                # 【关键修复】先统一转大写
                clean = str(code).upper().split('.')[0].replace('-', '')

                match = re.match(r'^([A-Z]+)(\d{3,4})', clean)
                if match:
                    return match.group(0)
                return None
            except:
                return None

        df['join_key'] = df['ts_code'].apply(_extract_key)
        df = df.dropna(subset=['join_key'])

        # 6. 生成字典 (取每个合约最早的到期日)
        expiry_map = df.groupby('join_key')['maturity_date'].min().to_dict()

        # 7. 【调试日志】
        print(f"✅ [System] 成功缓存 {len(expiry_map)} 条到期日数据")
        if len(expiry_map) > 0:
            print(f"   🔎 样例 Key: {list(expiry_map.keys())[:5]}")

        return expiry_map

    except Exception as e:
        print(f"❌ [Cache Error] 缓存建立失败: {e}")
        return {}


@tool
def search_broker_holdings_on_date(broker_name: str, date: str, symbol: str = None):
    """
    查持仓数据，支持两种模式：
    1. 查询【某家期货商】的持仓
    2. 查询【所有期货商】在某天针对【某品种】的持仓排名。

    参数:
    - broker_name: 期货商名称 ，例如国泰君安。如果要查全市场排名，请填 '所有' 或 'All'。
    - date: 查询日期 (YYYYMMDD)。
    - symbol: (可选) 品种代码，如 'RB', 'CU'。当 broker_name='所有' 时，此项必填。
    """
    # 1. 预处理参数
    date = date.replace('-', '').replace('/', '')

    # 识别是否查全市场
    is_search_all = broker_name in ['所有', '全部', 'All', 'all', '各个', '哪个']

    print(
        f"[*] Query Holdings: Broker={broker_name}, Date={date}, Symbol={symbol}, Mode={'ALL' if is_search_all else 'SINGLE'}")

    try:
        # ==========================================
        #  模式 A: 查【所有期货商】的排名 (横向对比)
        # ==========================================
        if is_search_all:
            if not symbol:
                return "❌ 查询所有期货商时，必须指定品种（例如：'查询所有期货商在铜上的持仓'）。"

            # 转换品种代码 (如 螺纹钢 -> RB)
            from data_engine import CN_TO_CODE
            code_prefix = CN_TO_CODE.get(symbol, symbol).upper()

            # SQL: 查该品种下，各期货商的持仓，按总持仓量(活跃度)排序
            # 注意: 这里使用 %% 来转义 %，防止 Python 字符串格式化报错
            sql = f"""
                SELECT 
                    broker as 期货商, 
                    long_vol as 多单, 
                    short_vol as 空单, 
                    net_vol as 净持仓,
                    (long_vol + short_vol) as 总持仓
                FROM futures_holding 
                WHERE REPLACE(trade_date, '-', '') = '{date}'
                  AND {sql_prefix_condition(code_prefix)} AND ts_code NOT LIKE '%%TAS%%'
                ORDER BY 总持仓 DESC
                LIMIT 6
            """

        # ==========================================
        #  模式 B: 查【某家期货商】的持仓 (纵向详情)
        # ==========================================
        else:
            sql = f"""
                SELECT 
                    ts_code as 合约, 
                    long_vol as 多单, 
                    short_vol as 空单, 
                    net_vol as 净持仓
                FROM futures_holding 
                WHERE broker = '{broker_name}' 
                  AND REPLACE(trade_date, '-', '') = '{date}'
            """
            # 如果指定了品种，加筛选
            if symbol:
                from data_engine import CN_TO_CODE
                code_prefix = CN_TO_CODE.get(symbol, symbol).upper()
                sql += f" AND {sql_prefix_condition(code_prefix)} AND ts_code NOT LIKE '%%TAS%%'"

            # 按净持仓绝对值排序
            sql += " ORDER BY ABS(net_vol) DESC LIMIT 20"

        # 3. 执行查询
        df = pd.read_sql(sql, engine)

        if df.empty:
            if is_search_all:
                return f"未找到 {date} 关于 {symbol} 的持仓数据。\n可能是当天该品种未进入龙虎榜，或数据未更新。"
            else:
                return f"未找到【{broker_name}】在 {date} 的持仓数据。\n请检查该期货商当天是否上榜。"

        # ==========================================
        # 🔥【核心修复】数据增强：把代码翻译成人话
        # ==========================================
        # 如果结果里有 '合约' 这一列 (模式 B)，我们要把它解析出中文名
        if '合约' in df.columns:
            # 引入映射表
            from data_engine import PRODUCT_MAP

            def enrich_name(row):
                raw_code = row['合约']  # e.g., eb2601.DCE
                if not isinstance(raw_code, str): return raw_code

                # 提取字母部分: eb2601 -> EB
                # 使用正则把数字和点去掉
                base_code = re.split(r'\d', raw_code)[0].upper()

                # 查字典
                cn_name = PRODUCT_MAP.get(base_code, base_code)

                # 返回格式: "苯乙烯 (EB)"
                # AI 看到这个格式，就不会自己去瞎编 "PS" 了
                return f"{cn_name} ({base_code})"

            # 新增一列 '品种' 并放到第一位
            df['品种'] = df.apply(enrich_name, axis=1)

            # 调整列顺序，把 '品种' 放在最前面，更直观
            cols = ['品种', '合约', '多单', '空单', '净持仓']
            # 确保列都存在
            final_cols = [c for c in cols if c in df.columns]
            df = df[final_cols]

        # 4. 返回 Markdown 表格
        return f"📊 **查询结果 ({date})**:\n" + df.to_markdown(index=False)

    except Exception as e:
        return f"查询持仓失败: {str(e)}"


@tool
def tool_analyze_position_change(symbol: str, start_date: str, end_date: str, sort_by: str = "long"):
    """
    【持仓变动分析器】
    计算某品种在一段时间内（start_date 到 end_date）某期货商的持仓变化。

    参数:
    - symbol: 品种名称或代码，如 '铜', 'RB', '510050'。
    - start_date: 开始日期 (YYYYMMDD)，如 '20251210'。
    - end_date: 结束日期 (YYYYMMDD)，通常是今天或昨天。
    - sort_by: 排序方式。'long' (按多单增量排序), 'short' (按空单增量排序), 'net' (按净持仓变动排序)。默认 'long'。
    """
    print(f"[*] 分析持仓变化: {symbol} ({start_date} -> {end_date})")

    try:
        # 1. 转换品种代码 (如 铜 -> CU)
        from data_engine import CN_TO_CODE
        code_prefix = CN_TO_CODE.get(symbol, symbol).upper()

        # 2. 清洗日期
        d1 = start_date.replace('-', '').replace('/', '')
        d2 = end_date.replace('-', '').replace('/', '')

        # 3. 一次性查出两天的所有相关数据
        # 注意：这里我们只查该品种(LIKE '{code_prefix}%')
        sql = f"""
            SELECT 
                broker, 
                long_vol, 
                short_vol, 
                REPLACE(trade_date, '-', '') as t_date
            FROM futures_holding 
            WHERE REPLACE(trade_date, '-', '') IN ('{d1}', '{d2}')
              AND {sql_prefix_condition(code_prefix)} AND ts_code NOT LIKE '%%TAS%%'
        """
        df = pd.read_sql(sql, engine)

        if df.empty:
            return f"未找到 {symbol} 在 {start_date} 或 {end_date} 的数据，无法对比。"

        # 4. 数据聚合 (关键步骤！)
        # 因为一家期货商可能同时持有 CU2503 和 CU2504，必须先把它们加起来，变成该期货商在"铜"上的总头寸
        df_agg = df.groupby(['broker', 't_date'])[['long_vol', 'short_vol']].sum().reset_index()

        # 5. 拆分成两个表进行对比
        df_start = df_agg[df_agg['t_date'] == d1].set_index('broker')
        df_end = df_agg[df_agg['t_date'] == d2].set_index('broker')

        if df_start.empty or df_end.empty:
            return f"数据缺失：找到的数据不足以进行首尾对比 (可能某一天休市或未上榜)。"

        # 6. 计算差值 (End - Start)
        # 使用 align 确保期货商对齐 (有的期货商可能只在某一天上榜，fillna(0) 处理)
        df_end, df_start = df_end.align(df_start, join='outer', fill_value=0)

        df_diff = pd.DataFrame()
        df_diff['多单变化'] = df_end['long_vol'] - df_start['long_vol']
        df_diff['空单变化'] = df_end['short_vol'] - df_start['short_vol']
        df_diff['净增仓'] = (df_end['long_vol'] - df_end['short_vol']) - (df_start['long_vol'] - df_start['short_vol'])

        # 结果美化：只保留变动不为0的
        df_diff = df_diff[(df_diff['多单变化'] != 0) | (df_diff['空单变化'] != 0)]

        # 7. 排序逻辑
        if sort_by == 'short':
            df_diff = df_diff.sort_values('空单变化', ascending=False)
            filter_desc = "空单增加前20名"
        elif sort_by == 'net':
            df_diff = df_diff.sort_values('净增仓', ascending=False)
            filter_desc = "净多头增加前20名"
        else:  # default long
            df_diff = df_diff.sort_values('多单变化', ascending=False)
            filter_desc = "多单增加前20名"

        # 取前 6
        res = df_diff.head(6).reset_index()
        res.columns = ['期货商', '多单变化', '空单变化', '净增仓变动']

        return f"📊 **{symbol} 持仓变动分析 ({start_date} vs {end_date})**\n📉 排序依据: {filter_desc}\n" + res.to_markdown(
            index=False)

    except Exception as e:
        return f"分析失败: {str(e)}"


def tool_analyze_broker_positions(broker_name: str, start_date: str, end_date: str, sort_by: str = "long"):
    """
    【期货商持仓分析器】
    查询某期货商在一段时间内各品种的持仓变化情况。

    适用场景：
    - "国泰君安最近在做多什么品种"
    - "中信期货这周增仓了哪些"
    - "永安期货最近的持仓变化"

    参数:
    - broker_name: 期货商名称，如 '国泰君安', '中信期货', '永安期货'。
    - start_date: 开始日期 (YYYYMMDD)，如 '20260110'。
    - end_date: 结束日期 (YYYYMMDD)，如 '20260120'。
    - sort_by: 排序方式。'long' (按多单增量), 'short' (按空单增量), 'net' (按净持仓变动)。默认 'long'。

    返回: 该期货商在各品种上的持仓变化表格。
    """
    print(f"[*] 分析期货商持仓: {broker_name} ({start_date} -> {end_date})")

    try:
        # 1. 清洗日期
        d1 = start_date.replace('-', '').replace('/', '')
        d2 = end_date.replace('-', '').replace('/', '')

        # 2. 查询该期货商在两个日期的所有持仓
        sql = f"""
            SELECT 
                ts_code,
                long_vol, 
                short_vol, 
                REPLACE(trade_date, '-', '') as t_date
            FROM futures_holding 
            WHERE broker = '{broker_name}'
              AND REPLACE(trade_date, '-', '') IN ('{d1}', '{d2}')
              AND ts_code NOT LIKE '%%TAS%%'
        """
        df = pd.read_sql(sql, engine)

        if df.empty:
            return f"未找到【{broker_name}】在 {start_date} 或 {end_date} 的持仓数据。\n请检查期货商名称是否正确，或该期货商当天是否上榜。"

        # 3. 提取品种代码（去掉合约月份，如 CU2503 -> CU）
        def extract_product(ts_code):
            # 提取字母部分作为品种代码
            import re
            match = re.match(r'([A-Za-z]+)', ts_code)
            return match.group(1).upper() if match else ts_code

        df['product'] = df['ts_code'].apply(extract_product)

        # 4. 按品种和日期聚合（因为一个品种可能有多个合约月份）
        df_agg = df.groupby(['product', 't_date'])[['long_vol', 'short_vol']].sum().reset_index()

        # 5. 拆分成两个表进行对比
        df_start = df_agg[df_agg['t_date'] == d1].set_index('product')
        df_end = df_agg[df_agg['t_date'] == d2].set_index('product')

        if df_start.empty and df_end.empty:
            return f"数据缺失：【{broker_name}】在这两天都没有持仓数据。"

        # 6. 计算差值 (End - Start)
        df_end, df_start = df_end.align(df_start, join='outer', fill_value=0)

        df_diff = pd.DataFrame()
        df_diff['多单变化'] = df_end['long_vol'] - df_start['long_vol']
        df_diff['空单变化'] = df_end['short_vol'] - df_start['short_vol']
        df_diff['净持仓变化'] = df_diff['多单变化'] - df_diff['空单变化']
        df_diff['当前多单'] = df_end['long_vol']
        df_diff['当前空单'] = df_end['short_vol']
        df_diff['当前净持仓'] = df_end['long_vol'] - df_end['short_vol']

        # 7. 过滤掉无变化的品种
        df_diff = df_diff[(df_diff['多单变化'] != 0) | (df_diff['空单变化'] != 0)]

        if df_diff.empty:
            return f"【{broker_name}】在 {start_date} 到 {end_date} 期间持仓无明显变化。"

        # 8. 排序逻辑
        if sort_by == 'short':
            df_diff = df_diff.sort_values('空单变化', ascending=False)
            filter_desc = "空单增加排序"
            direction = "做空"
        elif sort_by == 'net':
            df_diff = df_diff.sort_values('净持仓变化', ascending=False)
            filter_desc = "净多头增加排序"
            direction = "净多"
        else:  # default long
            df_diff = df_diff.sort_values('多单变化', ascending=False)
            filter_desc = "多单增加排序"
            direction = "做多"

        # 9. 取前 10
        res = df_diff.head(10).reset_index()

        # 10. 翻译品种代码为中文
        from data_engine import PRODUCT_MAP
        res['品种'] = res['product'].apply(lambda x: PRODUCT_MAP.get(x, x))

        # 整理输出列
        output_cols = ['品种', '净持仓变化', '当前净持仓', '多单变化', '空单变化']
        res = res[output_cols]

        # 11. 生成简评
        top_products = res.head(3)['品种'].tolist()
        summary = f"【{broker_name}】近期主要{direction}品种: {', '.join(top_products)}"

        return f"📊 **{broker_name} 持仓变动分析** ({start_date} vs {end_date})\n📉 排序: {filter_desc}\n\n" + res.to_markdown(
            index=False) + f"\n\n💡 {summary}"

    except Exception as e:
        return f"分析失败: {str(e)}"


# [新增] 获取数据库中实际存在的最新日期
@st.cache_data(ttl=300) # 缓存 5 分钟
def get_latest_data_date():
    """获取数据库中最新的交易日期 (返回 YYYYMMDD 字符串)"""
    if engine is None: return datetime.now().strftime('%Y%m%d')
    try:
        # 查期货表 (因为期货更新最快)
        sql = "SELECT MAX(trade_date) as last_date FROM futures_price"
        with engine.connect() as conn:
            result = conn.execute(text(sql)).fetchone()
            if result and result[0]:
                return str(result[0]).replace('-', '').replace('/', '')
    except Exception as e:
        print(f"获取最新日期失败: {e}")
    # 如果查库失败，兜底返回当前日期
    return datetime.now().strftime('%Y%m%d')


def log_token_usage(username, model_name, input_tokens, output_tokens, query_text=""):
    """记录 Token 消耗到数据库"""
    if not engine: return

    try:
        # 1. 准备数据
        today_str = datetime.now().strftime('%Y%m%d')
        total = input_tokens + output_tokens

        # 简单截取前50个字作为摘要，防止数据库太占空间
        snippet = query_text[:50].replace("'", "").replace('"', "") if query_text else ""

        # 2. 插入 SQL
        sql = text(f"""
            INSERT INTO token_usage_log 
            (trade_date, username, model_name, input_tokens, output_tokens, total_tokens, query_snippet)
            VALUES 
            (:d, :u, :m, :i, :o, :t, :s)
        """)

        # 3. 执行
        with engine.connect() as conn:
            conn.execute(sql, {
                "d": today_str,
                "u": username,
                "m": model_name,
                "i": input_tokens,
                "o": output_tokens,
                "t": total,
                "s": snippet
            })
            conn.commit()

        # print(f"📝 Token 已记录: {total}") # 调试用

    except Exception as e:
        print(f"❌ Token 记录失败: {e}")


@tool
def get_stock_valuation(symbol: str):
    """
    【估值分析】支持股票和指数。
    查询当前的估值指标(PE/PB)以及在历史(过去3-10年)中的分位水平。
    用于判断是"便宜"还是"贵"。

    Args:
        symbol: 名称或代码，如 '茅台', '600519', '沪深300', '000300.SH'
    """
    if engine is None: return "数据库连接失败"

    # 1. 智能识别代码
    import symbol_map
    res = symbol_map.resolve_symbol(symbol)

    # 【修复点1】增强识别逻辑，允许 index 类型通过
    if not res:
        return f"未找到 {symbol}，请确认名称。"

    ts_code, asset_type = res

    # 如果识别出来既不是 stock 也不是 index (比如是期货)，直接返回不支持
    if asset_type not in ['stock', 'index']:
        return f"品种 {symbol} ({asset_type}) 不支持估值分析（通常只有股票和指数有PE）。"

    try:
        # 【修复点2】根据类型决定查哪张表
        table_name = 'stock_valuation'
        if asset_type == 'index':
            table_name = 'index_valuation'

        # 2. 查询历史估值数据
        # 限制 1250 条大约是 5 年的数据
        sql = f"""
            SELECT trade_date, pe_ttm, pb, total_mv
            FROM {table_name} 
            WHERE ts_code = '{ts_code}' 
            ORDER BY trade_date DESC 
            LIMIT 2000
        """
        df = pd.read_sql(sql, engine)

        if df.empty:
            return f"暂无 {symbol} ({ts_code}) 的估值数据。请确认是否已运行 update_{asset_type}_valuation.py 更新数据。"

        # 取最新一天的数据
        curr = df.iloc[0]

        # 容错：指数表可能有时候 pe_ttm 是 0，尝试用 pe 字段（如果表里有的话，这里假设入库时已处理）
        curr_pe = curr['pe_ttm']
        curr_pb = curr['pb']

        # --- 3. 计算历史分位 ---
        from scipy import stats

        # 过滤有效数据 (PE>0)
        valid_pe_history = df[df['pe_ttm'] > 0]['pe_ttm']
        valid_pb_history = df[df['pb'] > 0]['pb']

        pe_rank = 0
        pe_desc = "数据不足"

        if len(valid_pe_history) > 100:
            pe_rank = stats.percentileofscore(valid_pe_history, curr_pe)

            if pe_rank < 10:
                pe_desc = "历史极值低位 (地板价 🔥)"
            elif pe_rank < 30:
                pe_desc = "偏低 (低估区域 ✅)"
            elif pe_rank < 70:
                pe_desc = "合理区间 (中枢震荡)"
            elif pe_rank < 90:
                pe_desc = "偏高 (高估区域 ⚠️)"
            else:
                pe_desc = "历史极值高位 (泡沫风险 ❌)"
        elif curr_pe <= 0:
            pe_desc = "亏损/无效"

        # --- 4. 生成报告 ---
        mv_val = curr['total_mv'] / 10000.0  # 假设单位是万元 -> 亿元
        mv_unit = "亿"

        # 指数的市值通常巨大，单位调整一下
        if asset_type == 'index' and mv_val > 10000:
            mv_val = mv_val / 10000.0
            mv_unit = "万亿"

        report = f"📊 **{symbol} ({ts_code}) 估值分析** ({curr['trade_date']})\n"
        report += f"- **类型**: {'指数' if asset_type == 'index' else '个股'}\n"
        report += f"- **总市值**: {mv_val:.2f} {mv_unit}\n"
        report += "--------------------------------\n"

        # PE 部分
        report += f"💎 **市盈率 (PE-TTM)**: {curr_pe:.2f}\n"
        if len(valid_pe_history) > 100:
            report += f"   - **历史分位**: {pe_rank:.1f}% ({pe_desc})\n"
            report += f"   - **近{len(df) // 250}年最高**: {valid_pe_history.max():.2f} | **最低**: {valid_pe_history.min():.2f}\n"
        else:
            report += f"   - 状态: {pe_desc}\n"

        # PB 部分
        report += f"🏠 **市净率 (PB)**: {curr_pb:.2f}\n"
        if len(valid_pb_history) > 100:
            pb_rank = stats.percentileofscore(valid_pb_history, curr_pb)
            report += f"   - **历史分位**: {pb_rank:.1f}%\n"

        return report

    except Exception as e:
        return f"估值分析出错: {e}"


@tool
def tool_compare_stocks(stock_list: str):
    """
    用于对比多只股票的市值、市盈率(PE)、市净率(PB)等指标。
    当用户问“对比A和B”、“谁的市值更高”、“给这些股票排个序”时使用。

    Args:
        stock_list: 股票名称字符串，用逗号或空格分隔。例如: "茅台,五粮液,泸州老窖" 或 "宁德时代 比亚迪"
    """
    if engine is None: return "数据库连接失败"

    # 1. 解析输入的股票名单
    # 简单的清洗逻辑：把逗号、顿号、空格都换成统一分隔符
    raw_names = stock_list.replace("，", ",").replace("、", ",").replace(" ", ",").split(",")
    valid_codes = []
    valid_names = []

    import symbol_map  # 复用您的映射库

    for name in raw_names:
        name = name.strip()
        if not name: continue

        # 解析代码
        res = symbol_map.resolve_symbol(name)
        if res and res[1] == 'stock':
            valid_codes.append(res[0])  # ts_code
            valid_names.append(name)  # 原名

    if not valid_codes:
        return "未能识别任何有效股票，请检查名称。"

    try:
        # 2. 批量查询数据库 (使用 IN 语法)
        code_str = "'" + "','".join(valid_codes) + "'"

        # 我们查最新的那一天的数据
        # 这里用了一个子查询来确保每只股票都取到它最新的一条
        sql = f"""
            SELECT v.ts_code, v.trade_date, v.total_mv, v.pe_ttm, v.pb, v.dv_ratio
            FROM stock_valuation v
            WHERE v.ts_code IN ({code_str})
            AND v.trade_date = (
                SELECT MAX(trade_date) FROM stock_valuation WHERE ts_code = v.ts_code
            )
        """
        df = pd.read_sql(sql, engine)

        if df.empty:
            return "未找到这些股票的估值数据，请确认是否已运行 update_valuation.py 入库。"

        # 3. 数据美化与计算
        # 把代码转回中文名 (为了给 AI 看得更清楚，也可以做个映射，这里简单处理)
        # 这里建议再读一次 stock_basic 表或者复用 symbol_map 的反向映射，为了演示简单，直接用 ts_code

        df['市值(亿)'] = (df['total_mv'] / 10000).round(2)
        df['PE(动)'] = df['pe_ttm'].round(2)
        df['PB'] = df['pb'].round(2)
        df['股息率%'] = df['dv_ratio'].round(2)

        # 4. 按市值降序排列 (默认逻辑：对比时通常看谁老大)
        df = df.sort_values('市值(亿)', ascending=False)

        # 5. 生成 Markdown 表格
        # 选取 AI 需要的核心列
        final_df = df[['ts_code', '市值(亿)', 'PE(动)', 'PB', '股息率%']]

        return f"📊 **多股同台竞技** (按市值排名):\n" + final_df.to_markdown(index=False)

    except Exception as e:
        return f"对比失败: {e}"


def check_user_email_status(username):
    """
    检查用户是否绑定了邮箱
    返回: (bool) True表示有邮箱, False表示无
    """
    try:
        # 示例 SQL，请根据你的表结构修改 table_users 和 email 字段名
        sql = text("SELECT email FROM users WHERE username = :user")
        with engine.connect() as conn:
            result = conn.execute(sql, {"user": username}).fetchone()

        if result and result[0] and str(result[0]).strip() != "":
            return True
        return False
    except Exception as e:
        print(f"Check email error: {e}")
        return False


def update_newsletter_subscription(username, is_subscribed):
    """
    更新用户的订阅状态
    is_subscribed: True (订阅) / False (取消)
    """
    try:
        # 假设你的 users 表里有一个字段叫 is_newsletter_active (布尔值)
        status_val = 1 if is_subscribed else 0
        sql = text("UPDATE users SET is_subscribed = :status WHERE username = :user")

        with engine.connect() as conn:
            conn.execute(sql, {"status": status_val, "user": username})
            conn.commit()
        return True
    except Exception as e:
        print(f"Update subscription error: {e}")
        return False


@tool
def get_iv_range_stats(symbol: str, start_date: str = None, end_date: str = None, days: int = 365):
    """
    查询期权IV在指定时间区间的统计数据（最高、最低、平均值）。

    使用场景：
    - "创业板期权过去一年的IV波动区间是多少"
    - "白银期权在2025年2月到2026年2月的IV范围"
    - "查询豆粕期权最近半年的IV高低点"

    参数:
        symbol: 品种名称或代码，如 '创业板ETF'、'159915'、'白银'、'AG'、'豆粕'、'M'
        start_date: 起始日期 'YYYY-MM-DD' 或 'YYYYMMDD'，不传则自动计算
        end_date: 结束日期 'YYYY-MM-DD' 或 'YYYYMMDD'，不传则为当前日期
        days: 当不传start_date时，向前查询的天数，默认365天

    返回:
        str: IV区间统计报告（最高值、最低值、平均值、当前值）
    """

    if engine is None:
        return "❌ 数据库连接失败"

    try:
        # 1. 处理日期参数
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        else:
            end_date = end_date.replace('-', '').replace('/', '')

        if start_date is None:
            start_dt = datetime.now() - timedelta(days=days)
            start_date = start_dt.strftime('%Y%m%d')
        else:
            start_date = start_date.replace('-', '').replace('/', '')

        # 2. 识别品种类型（复用 get_commodity_iv_info 中的 ETF_MAP）
        ETF_MAP = {
            '50ETF': '510050.SH', '上证50ETF': '510050.SH', '上证50': '510050.SH',
            '300ETF': '510300.SH', '沪深300ETF': '510300.SH', '沪深300': '510300.SH',
            '500ETF': '510500.SH', '中证500ETF': '510500.SH', '中证500': '510500.SH',
            '创业板ETF': '159915.SZ', '创业板': '159915.SZ', 'CYB': '159915.SZ',
            '科创50ETF': '588000.SH', '科创板': '588000.SH', '科创50': '588000.SH',
            '深100ETF': '159901.SZ', '深100': '159901.SZ'
        }

        etf_code = None
        etf_name = None
        commodity_code = None
        commodity_name = None

        # ETF识别（复用逻辑）
        query_upper = symbol.upper()
        matches = []

        for name, code in ETF_MAP.items():
            if name.upper() in query_upper:
                matches.append({'name': name, 'code': code, 'len': len(name)})
                continue
            short_name = name.upper().replace('ETF', '')
            if len(short_name) > 0 and short_name in query_upper:
                matches.append({'name': name, 'code': code, 'len': len(short_name)})

        if matches:
            best_match = sorted(matches, key=lambda x: x['len'], reverse=True)[0]
            etf_code = best_match['code']
            etf_name = best_match['name']

        # 也支持直接输入代码
        if not etf_code:
            match_code = re.search(r'(510\d{3}|159\d{3}|588\d{3})', symbol)
            if match_code:
                raw_code = match_code.group(1)
                etf_code = f"{raw_code}.SZ" if raw_code.startswith('159') else f"{raw_code}.SH"
                etf_name = f"{raw_code}ETF"

        # 如果不是ETF，按商品处理（复用PRODUCT_MAP）
        if not etf_code:
            clean_query = re.sub(r'[^a-zA-Z]', '', symbol).upper()

            if clean_query in PRODUCT_MAP:
                commodity_code = clean_query
                commodity_name = PRODUCT_MAP[clean_query]
            else:
                for code, name in PRODUCT_MAP.items():
                    if name in symbol:
                        commodity_code = code
                        commodity_name = name
                        break

        # 3. 查询数据库
        if etf_code:
            # ETF期权查询
            print(f"[*] 查询ETF IV区间: {etf_name} ({etf_code}), {start_date} - {end_date}")

            sql = f"""
                SELECT trade_date, iv 
                FROM etf_iv_history 
                WHERE etf_code = '{etf_code}' 
                  AND REPLACE(trade_date, '-', '') >= '{start_date}'
                  AND REPLACE(trade_date, '-', '') <= '{end_date}'
                ORDER BY trade_date DESC
            """
            df = pd.read_sql(sql, engine)
            display_name = f"{etf_name} ({etf_code})"

        elif commodity_code:
            # 商品期权查询
            print(f"[*] 查询商品 IV区间: {commodity_name} ({commodity_code}), {start_date} - {end_date}")

            sql = f"""
                SELECT trade_date, iv 
                FROM commodity_iv_history 
                WHERE commodity_code = '{commodity_code}' 
                  AND trade_date >= '{start_date}'
                  AND trade_date <= '{end_date}'
                ORDER BY trade_date DESC
            """
            df = pd.read_sql(sql, engine)
            display_name = f"{commodity_name} ({commodity_code})"

        else:
            return f"❌ 无法识别品种【{symbol}】，请使用正确的名称或代码。\n支持的格式：创业板ETF、159915、白银、AG等"

        # 4. 检查数据
        if df.empty:
            return f"""
⚠️ 未查询到【{display_name}】在 {start_date} 至 {end_date} 期间的IV数据。

可能原因：
1. 该时间段内无交易数据
2. 数据库表 {'etf_iv_history' if etf_code else 'commodity_iv_history'} 中暂无该品种数据
3. 请检查数据采集脚本是否正常运行
"""

        # 5. 计算统计数据
        max_iv = df['iv'].max()
        min_iv = df['iv'].min()
        avg_iv = df['iv'].mean()
        median_iv = df['iv'].median()
        current_iv = df.iloc[0]['iv']  # 最新的IV
        latest_date = df.iloc[0]['trade_date']

        # 计算标准差
        std_iv = df['iv'].std()

        # 当前IV在历史中的位置
        if max_iv != min_iv:
            iv_percentile = ((current_iv - min_iv) / (max_iv - min_iv)) * 100
        else:
            iv_percentile = 50

        # 6. 生成报告
        report = f"""
📊 **{display_name} IV区间统计报告**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📅 **统计区间**: {start_date[:4]}-{start_date[4:6]}-{start_date[6:]} 至 {end_date[:4]}-{end_date[4:6]}-{end_date[6:]}
📈 **数据天数**: {len(df)} 个交易日

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## 📉 IV统计数据

- **最高IV**: {max_iv:.2f}%
- **最低IV**: {min_iv:.2f}%
- **平均IV**: {avg_iv:.2f}%
- **中位数IV**: {median_iv:.2f}%
- **标准差**: {std_iv:.2f}%

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## 🎯 当前IV水平

- **最新日期**: {str(latest_date).replace('-', '')}
- **当前IV**: {current_iv:.2f}%
- **历史分位**: {iv_percentile:.1f}%

"""

        # 评级
        if iv_percentile < 10:
            report += "📌 **评级**: 🔥 **极低水平** - IV处于历史底部，期权便宜\n"
        elif iv_percentile < 30:
            report += "📌 **评级**: ✅ **偏低** - IV低于历史均值，适合买入期权\n"
        elif iv_percentile < 70:
            report += "📌 **评级**: 🔄 **正常区间** - IV处于历史中枢\n"
        elif iv_percentile < 90:
            report += "📌 **评级**: ⚠️ **偏高** - IV高于历史均值，期权较贵\n"
        else:
            report += "📌 **评级**: 🔴 **极高水平** - IV处于历史顶部，市场波动预期强烈\n"

        report += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"

        return report

    except Exception as e:
        return f"❌ 查询IV区间统计时出错: {str(e)}"


def get_user_portfolio_snapshot(user_id: str):
    """
    获取用户最新持仓分析总览快照。
    返回 dict，字段包含 industry_allocation / portfolio_corr 等解析后的结构。
    """
    if engine is None or not user_id:
        return {}
    try:
        sql = text("SELECT * FROM user_portfolio_snapshot WHERE user_id = :uid LIMIT 1")
        with engine.connect() as conn:
            row = conn.execute(sql, {"uid": user_id}).mappings().fetchone()
        if not row:
            return {}
        data = dict(row)
        try:
            data["industry_allocation"] = json.loads(data.get("industry_allocation_json") or "[]")
        except Exception:
            data["industry_allocation"] = []
        try:
            data["portfolio_corr"] = json.loads(data.get("portfolio_corr_json") or "{}")
        except Exception:
            data["portfolio_corr"] = {}
        return data
    except Exception as e:
        print(f"获取用户持仓快照失败: {e}")
        return {}


def get_user_portfolio_positions(user_id: str) -> pd.DataFrame:
    """
    获取用户当前持仓明细（结构化当前态）。
    """
    if engine is None or not user_id:
        return pd.DataFrame()
    try:
        sql = text(
            """
            SELECT symbol, name, market, quantity, market_value, price, cost_price,
                   industry, technical_grade, technical_reason, index_corr_json,
                   last_seen_at, updated_at
            FROM user_portfolio_positions
            WHERE user_id = :uid
            ORDER BY last_seen_at DESC, market_value DESC
            """
        )
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params={"uid": user_id})
        if df.empty:
            return df

        # 统一“当前态”口径：优先只保留最新 last_seen_at 批次；若筛选失败则回退全量。
        if "last_seen_at" in df.columns:
            seen_ts = pd.to_datetime(df["last_seen_at"], errors="coerce")
            if seen_ts.notna().any():
                latest_ts = seen_ts.max()
                batch_df = df[seen_ts == latest_ts].copy()
                if not batch_df.empty:
                    df = batch_df

        if "market_value" in df.columns:
            df["market_value"] = pd.to_numeric(df["market_value"], errors="coerce")
            df = df.sort_values("market_value", ascending=False, na_position="last")

        if "index_corr_json" in df.columns:
            def _parse_one(raw):
                if not raw:
                    return {}
                try:
                    return json.loads(raw)
                except Exception:
                    return {}
            df["index_corr"] = df["index_corr_json"].apply(_parse_one)
        return df
    except Exception as e:
        print(f"获取用户持仓明细失败: {e}")
        return pd.DataFrame()


def clear_user_portfolio_snapshot(user_id: str) -> bool:
    """
    清理用户持仓总览快照（不删除明细表）。
    用于修复“快照存在但明细为空”的异常展示。
    """
    if engine is None or not user_id:
        return False
    try:
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM user_portfolio_snapshot WHERE user_id = :uid"),
                {"uid": user_id},
            )
        return True
    except Exception as e:
        print(f"清理用户持仓快照失败: {e}")
        return False


def get_portfolio_momentum_scores(symbols, window_days: int = 10):
    """
    从 daily_stock_screener 读取每个持仓股票最近 N 条评分均值（动能分数）。
    返回: { "601126.SH": 63.2, ... }
    """
    if engine is None:
        return {}
    try:
        symbol_list = []
        for raw in symbols or []:
            code = str(raw or "").strip().upper()
            if code:
                symbol_list.append(code)
        if not symbol_list:
            return {}

        # 同时兼容带后缀和不带后缀两种 ts_code 存储口径
        candidates = []
        for sym in symbol_list:
            base = sym.split(".")[0]
            for code in (sym, base):
                if code and code not in candidates:
                    candidates.append(code)

        placeholders = []
        params = {}
        for i, code in enumerate(candidates):
            k = f"c{i}"
            placeholders.append(f":{k}")
            params[k] = code

        max_n = max(int(window_days or 10), 1)
        with engine.connect() as conn:
            recent_dates_df = pd.read_sql(
                text(
                    """
                    SELECT DISTINCT trade_date
                    FROM daily_stock_screener
                    ORDER BY trade_date DESC
                    LIMIT :n
                    """
                ),
                conn,
                params={"n": max_n},
            )
            if recent_dates_df.empty:
                return {}
            recent_dates = [str(x) for x in recent_dates_df["trade_date"].tolist() if str(x).strip()]
            if not recent_dates:
                return {}

            date_placeholders = []
            for i, d in enumerate(recent_dates):
                k = f"d{i}"
                date_placeholders.append(f":{k}")
                params[k] = d

            sql = text(
                f"""
                SELECT ts_code, trade_date, score
                FROM daily_stock_screener
                WHERE ts_code IN ({",".join(placeholders)})
                  AND trade_date IN ({",".join(date_placeholders)})
                ORDER BY trade_date DESC
                """
            )
            df = pd.read_sql(sql, conn, params=params)
        if df.empty:
            return {}

        df["score"] = pd.to_numeric(df["score"], errors="coerce")
        df["trade_date"] = df["trade_date"].astype(str)
        df = df.dropna(subset=["score"])
        if df.empty:
            return {}

        out = {}
        for sym in symbol_list:
            base = sym.split(".")[0]
            part = df[df["ts_code"].isin([sym, base])].sort_values("trade_date", ascending=False)
            if part.empty:
                continue
            out[sym] = round(float(part["score"].head(max_n).mean()), 2)
        return out
    except Exception as e:
        print(f"获取动能分数失败: {e}")
        return {}


def get_latest_hkd_cny_rate(default_rate: float = 0.92) -> float:
    """
    获取最新港币兑人民币汇率（HKDCNY）。
    读取 macro_daily.indicator_code='HKDCNY' 的最新 close_value。
    失败时返回 default_rate。
    """
    if engine is None:
        return float(default_rate)
    try:
        sql = text(
            """
            SELECT close_value
            FROM macro_daily
            WHERE indicator_code = 'HKDCNY'
            ORDER BY trade_date DESC
            LIMIT 1
            """
        )
        with engine.connect() as conn:
            row = conn.execute(sql).mappings().fetchone()
        if not row:
            return float(default_rate)
        val = row.get("close_value")
        rate = float(val)
        # 异常值保护，避免脏数据把总市值放大/缩小
        if rate <= 0 or rate > 2:
            return float(default_rate)
        return rate
    except Exception as e:
        print(f"获取 HKDCNY 汇率失败: {e}")
        return float(default_rate)
