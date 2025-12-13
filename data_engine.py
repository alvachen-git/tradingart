import pandas as pd
import numpy as np
import re
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from langchain_core.tools import tool
from sqlalchemy.exc import SQLAlchemyError
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

        # B. 获取持仓
        query_holding = f"SELECT trade_date, broker, net_vol FROM futures_holding WHERE ts_code='{holding_symbol}'"
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

        請提取以下信息：
        1. risk: 风险偏好（保守/稳健/激进/未知）
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

        # 5. 數據合併邏輯
        new_risk = data.get('risk', old_risk)
        if new_risk == '未知': new_risk = old_risk

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
    # 黑色
    'RB': '螺纹钢', 'HC': '热卷', 'J': '焦炭', 'JM': '焦煤', 'I': '铁矿石','WR': '线材',
    'SS': '不锈钢', 'SM': '锰硅', 'SF': '硅铁','NR': '20号胶','OP': '双胶纸','SP': '纸浆',
    # 有色/贵金属
    'AU': '黄金', 'AG': '白银', 'CU': '铜', 'AL': '铝', 'ZN': '锌','AD': '铝合金',
    'PB': '铅', 'NI': '镍', 'SN': '锡', 'AO': '氧化铝', 'LC': '碳酸锂', 'SI': '工业硅','PS': '多晶硅',
    # 农产品
    'M': '豆粕', 'Y': '豆油', 'P': '棕榈油', 'OI': '菜油', 'RM': '菜粕','A': '豆一','B': '豆二',
    'C': '玉米', 'CS': '淀粉', 'CF': '棉花', 'SR': '白糖', 'AP': '苹果','LG': '原木','PF': '短纤',
    'JD': '鸡蛋', 'LH': '生猪', 'PK': '花生', 'CJ': '红枣','CY': '棉纱','PM': '普麦','WH': '强麦',
    # 能化
    'SC': '原油', 'FU': '燃料油', 'PG': '液化气', 'TA': 'PTA', 'MA': '甲醇','BU': '沥青','LU': 'LU燃油','SH': '烧碱',
    'PP': '聚丙烯', 'L': '塑料', 'V': 'PVC', 'EB': '苯乙烯', 'EG': '乙二醇','BZ': '纯苯','PL': '丙烯','PR': '瓶片',
    'UR': '尿素', 'SA': '纯碱', 'FG': '玻璃', 'PX': '对二甲苯', 'BR': 'BR橡胶'
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
    1. 商品期权：通常在期货月份的前一个月上旬到期 (如 M2505 期权在 4月7日左右到期)。
    2. 金融期权(IO/MO/HO)：在期货月份当月的第三个周五到期。
    3. 如果 (估算到期日 - 当前日期) < 2天，则认为无效。
    """
    try:
        # 1. 解析年份和月份 (RB2505 -> 2025, 5)
        m = re.search(r'(\d{3,4})$', row['join_key'])
        if not m: return False
        ym = m.group(1)
        if len(ym) == 3: ym = '2' + ym  # 处理 505 -> 2505

        year = int('20' + ym[:2])
        month = int(ym[2:])

        # 构造期货合约的大致月份时间 (每月15号作为基准)
        fut_date = pd.Timestamp(year=year, month=month, day=15)
        current_date = pd.to_datetime(current_date_str)

        # 2. 估算期权到期日
        product = row['product']
        if product in ['IO', 'MO', 'HO', 'IF', 'IH', 'IM']:
            # 金融期权：当月到期 (保守按当月10号计算临近)
            expiry_approx = fut_date.replace(day=10)
        else:
            # 商品期权：前一个月到期 (保守按前一个月5号计算临近)
            # 例如 RB2505，期权在 4月初到期
            expiry_approx = (fut_date - pd.DateOffset(months=1)).replace(day=5)

        # 3. 计算剩余天数
        days_left = (expiry_approx - current_date).days

        # 必须大于 2 天才算有效
        return days_left > 2
    except:
        return True  # 解析失败默认不过滤，防止误杀

@st.cache_data(ttl=1800)
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
        date_1y = (pd.to_datetime(today) - pd.Timedelta(days=365)).strftime('%Y%m%d')

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
            df_h = df_iv_all[df_iv_all['join_key'].isin(keys)].copy()
            stats = df_h.groupby('join_key')['iv'].agg(['min', 'max']).reset_index()

            df_final = df_selected.merge(stats, on='join_key', how='left')
            df_final['iv_range'] = df_final['max'] - df_final['min']
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
        df_final['当日IV变动'] = df_final['iv'] - df_final['iv_prev']
        df_final['5日IV变动'] = df_final['iv'] - df_final['iv_5d']

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

@tool
def get_commodity_iv_info(query: str):
    """
    【期权专用】查询指定商品的隐含波动率(IV)数据。
    逻辑：
    1. 默认返回：最新IV数值 + 近期变动趋势（节省资源）。
    2. 深度返回：只有当用户问题包含"IV等级"、"贵"、"便宜"、"分位"等词时，才计算IV Rank。
    """
    if engine is None: return "❌ 数据库未连接"

    # --- 1. 意图识别：判断用户是否需要 Rank 数据 ---
    keywords_rank = ['rank', '排名', '分位', '贵', '便宜', '位置', '历史', '高', '低', '水平']
    need_rank = any(k in query.lower() for k in keywords_rank)

    # 决定查询天数：只要趋势查5天就够，要排名才查250天
    limit_days = 252 if need_rank else 5

    # --- 2. 商品代码映射 (保持不变) ---
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
        if match: target_code = match.group(1).upper()

    if not target_code:
        return f"⚠️ 未找到商品【{query}】。"

    try:
        # --- 3. 寻找主力合约 ---
        sql_main = f"""
            SELECT ts_code, close_price, REPLACE(trade_date, '-', '') as trade_date 
            FROM futures_price 
            WHERE ts_code LIKE '{target_code}%%' 
              AND trade_date = (SELECT MAX(trade_date) FROM futures_price)
            ORDER BY oi DESC LIMIT 1
        """
        df_main = pd.read_sql(sql_main, engine)

        if df_main.empty:
            return f"⚠️ 暂无品种【{target_code}】的数据。"

        main_contract = df_main.iloc[0]['ts_code']
        curr_price = df_main.iloc[0]['close_price']
        date_str = df_main.iloc[0]['trade_date']

        # --- 4. 动态查询 IV 数据 ---
        # 这里的 LIMIT 是动态的，大大减少了不必要的数据传输
        sql_iv = f"""
            SELECT trade_date, iv 
            FROM commodity_iv_history 
            WHERE ts_code = '{main_contract}' 
            ORDER BY trade_date DESC 
            LIMIT {limit_days}
        """
        df_iv = pd.read_sql(sql_iv, engine)

        if df_iv.empty:
            return f"⚠️ 合约【{main_contract}】暂无IV数据。"

        # 基础数据
        curr_iv = df_iv.iloc[0]['iv']

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
📊 **{target_name} ({main_contract}) 波动率速报**
--------------------------------
📅 日期: {date_str}
🔥 **当前 IV: {curr_iv:.2f}%**
📈 **较昨日: {iv_change_text}**
🌊 **近期趋势**: {trend_text}
--------------------------------
💡 *提示: 如需查询IV历史排位或策略建议，请问“{target_name}的IV贵吗？”或“{target_name} IV排名”*
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
📊 **{target_name} ({main_contract}) 深度波动率分析**
--------------------------------
📅 日期: {date_str}
🌊 **当前 IV: {curr_iv:.2f}%**
🏆 **IV Rank: {iv_rank:.1f}% ({status})**

📏 统计周期: 过去 {len(df_iv)} 个交易日
🔺 历史最高: {max_iv:.2f}%
🔻 历史最低: {min_iv:.2f}%
--------------------------------
💡 *策略参考: 当前IV处于{'历史高位，权利金较贵，卖方策略具有统计上优势' if iv_rank > 50 else '历史低位，权利金便宜，买方策略风险收益比更佳'}。*
            """

    except Exception as e:
        return f"数据查询发生错误: {e}"