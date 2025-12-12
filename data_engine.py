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
def get_etf_option_analysis(etf_code="510050", days=20):
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


def fmt_date(d):
    return str(d).replace('-', '').replace('/', '').split(' ')[0]


def get_join_key(ts_code):
    """提取 '品种+月份' 并映射期权代码"""
    if not isinstance(ts_code, str): return ""

    # 基础清理
    base = ts_code.strip().upper().split('.')[0]
    if '-' in base: base = base.split('-')[0]

    # 正则提取：字母+数字
    match = re.search(r'([A-Z]+)(\d{3,4})$', base)

    if match:
        product = match.group(1)
        month = match.group(2)

        # 映射金融期货 (IO->IF)
        mapping = {'IO': 'IF', 'HO': 'IH', 'MO': 'IM'}
        final_product = mapping.get(product, product)

        return f"{final_product}{month}"

    return ""


def get_comprehensive_market_data():
    if engine is None: return pd.DataFrame()

    try:
        # 1. 获取日期
        dates_df = pd.read_sql("SELECT DISTINCT trade_date FROM futures_price ORDER BY trade_date DESC LIMIT 10",
                               engine)
        if len(dates_df) < 6: return pd.DataFrame()

        today = fmt_date(dates_df.iloc[0]['trade_date'])
        prev_day = fmt_date(dates_df.iloc[1]['trade_date'])
        day_5_ago = fmt_date(dates_df.iloc[5]['trade_date'])

        date_str = "', '".join([today, prev_day, day_5_ago])

        # 2. 拉取 Price
        sql_price = f"""
        SELECT ts_code, close_price, oi, REPLACE(trade_date, '-', '') as trade_date
        FROM futures_price 
        WHERE REPLACE(trade_date, '-', '') IN ('{date_str}')
        """
        df_price = pd.read_sql(sql_price, engine)

        # 生成 Key 并过滤
        df_price['join_key'] = df_price['ts_code'].apply(get_join_key)
        df_price = df_price[df_price['join_key'] != ""]

        # 3. 拉取 IV
        sql_iv = f"""
        SELECT ts_code, iv, REPLACE(trade_date, '-', '') as trade_date
        FROM commodity_iv_history 
        WHERE REPLACE(trade_date, '-', '') IN ('{date_str}')
        """
        df_iv_raw = pd.read_sql(sql_iv, engine)

        # 映射 Key
        df_iv_raw['join_key'] = df_iv_raw['ts_code'].apply(get_join_key)
        df_iv_raw = df_iv_raw[df_iv_raw['join_key'] != ""]
        df_iv = df_iv_raw.groupby(['join_key', 'trade_date'])['iv'].mean().reset_index()

        # 4. 拉取多空
        sql_s = f"""
        SELECT symbol as ts_code, dumb_net, smart_net, REPLACE(trade_date, '-', '') as trade_date
        FROM market_conflict_daily 
        WHERE REPLACE(trade_date, '-', '') IN ('{date_str}')
        """
        df_s = pd.read_sql(sql_s, engine)
        df_s['join_key'] = df_s['ts_code'].apply(get_join_key)

        # 5. 合并逻辑
        def merge_data(d):
            p = df_price[df_price['trade_date'] == d].copy()
            i = df_iv[df_iv['trade_date'] == d].copy()
            s = df_s[df_s['trade_date'] == d].copy()

            merged = p.merge(i, on='join_key', how='left')

            if not s.empty and s['join_key'].iloc[0] != "":
                merged = merged.merge(s[['join_key', 'dumb_net', 'smart_net']], on='join_key', how='left')
            else:
                merged = merged.merge(s[['ts_code', 'dumb_net', 'smart_net']], on='ts_code', how='left')

            return merged

        df_now = merge_data(today)
        df_prev = merge_data(prev_day)
        df_5d = merge_data(day_5_ago)

        if df_now.empty: return pd.DataFrame()

        # 6. 筛选主力
        df_now['product'] = df_now['join_key'].apply(lambda x: re.match(r"([a-zA-Z]+)", x).group(1))
        df_now = df_now.sort_values(['product', 'oi'], ascending=[True, False])
        df_now = df_now.groupby('product').head(2)

        # 7. 计算 IV Rank
        date_1y = (pd.to_datetime(today) - pd.Timedelta(days=365)).strftime('%Y%m%d')
        sql_hist = f"""
        SELECT ts_code, iv 
        FROM commodity_iv_history 
        WHERE REPLACE(trade_date, '-', '') >= '{date_1y}'
        """
        df_hist = pd.read_sql(sql_hist, engine)
        df_hist['join_key'] = df_hist['ts_code'].apply(get_join_key)
        df_hist = df_hist[df_hist['join_key'] != ""]

        target_keys = df_now['join_key'].unique()
        df_hist = df_hist[df_hist['join_key'].isin(target_keys)]

        iv_stats = df_hist.groupby('join_key')['iv'].agg(['min', 'max']).reset_index()

        df_now = df_now.merge(iv_stats, on='join_key', how='left')
        df_now['iv_rank'] = (df_now['iv'] - df_now['min']) / (df_now['max'] - df_now['min']) * 100
        # 【修改点】IV Rank 替换 inf 并填充 0
        df_now['iv_rank'] = df_now['iv_rank'].replace([np.inf, -np.inf], 0).fillna(0)

        # 8. 计算变动
        cols = ['join_key', 'ts_code', 'close_price', 'iv', 'iv_rank', 'dumb_net', 'smart_net', 'product']
        base = df_now[cols].copy()

        final = base.merge(df_prev[['join_key', 'close_price', 'iv', 'dumb_net', 'smart_net']],
                           on='join_key', suffixes=('', '_prev'), how='left')
        final = final.merge(df_5d[['join_key', 'close_price', 'iv', 'dumb_net', 'smart_net']],
                            on='join_key', suffixes=('', '_5d'), how='left')

        final['当日涨跌%'] = (
                    (final['close_price'] - final['close_price_prev']) / final['close_price_prev'] * 100).fillna(0)
        final['5日涨跌%'] = ((final['close_price'] - final['close_price_5d']) / final['close_price_5d'] * 100).fillna(0)
        final['当日IV变动'] = (final['iv'] - final['iv_prev']).fillna(0)
        final['5日IV变动'] = (final['iv'] - final['iv_5d']).fillna(0)

        for c in ['dumb_net', 'dumb_net_prev', 'dumb_net_5d', 'smart_net', 'smart_net_prev', 'smart_net_5d']:
            if c not in final.columns: final[c] = 0
            final[c] = final[c].fillna(0)

        final['反指变动(日)'] = final['dumb_net'] - final['dumb_net_prev']
        final['反指变动(5日)'] = final['dumb_net'] - final['dumb_net_5d']
        final['正指变动(日)'] = final['smart_net'] - final['smart_net_prev']
        final['正指变动(5日)'] = final['smart_net'] - final['smart_net_5d']

        # --- 9. 【修改点】格式化显示名称 (英文+中文) ---
        def format_name(row):
            code = row['join_key']
            prod = row['product']
            cn_name = PRODUCT_MAP.get(prod, "")
            return f"{code} ({cn_name})" if cn_name else code

        final['合约'] = final.apply(format_name, axis=1)

        # 10. 处理 "快到期" 和 IV Rank 取整
        curr_yymm = int(pd.to_datetime(today).strftime('%y%m'))

        def process_rank(row):
            # 快到期逻辑
            if row['iv'] == 0 or pd.isna(row['iv']):
                m = re.search(r'\d{3,4}$', row['join_key'])
                if m:
                    m_str = m.group(0)
                    if len(m_str) == 3: m_str = "2" + m_str
                    if int(m_str) <= curr_yymm + 1:
                        return "快到期"

            # 【修改点】IV Rank 取整
            val = row['iv_rank']
            if pd.isna(val): return 0
            return int(round(val, 0))  # 强制转整数

        final['iv_rank_display'] = final.apply(process_rank, axis=1)

        # 整理输出 (保留1位小数)
        out_cols = ['合约', 'iv_rank_display', '当日IV变动', '5日IV变动', '当日涨跌%', '5日涨跌%',
                    '反指变动(日)', '反指变动(5日)', '正指变动(日)', '正指变动(5日)']

        res = final[out_cols].copy()
        res.columns = ['合约', 'IV Rank', 'IV变动(日)', 'IV变动(5日)', '涨跌%(日)', '涨跌%(5日)', '散户变动(日)',
                       '散户变动(5日)', '机构变动(日)', '机构变动(5日)']

        return res.round(1)

    except Exception as e:
        print(f"Engine Error: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


