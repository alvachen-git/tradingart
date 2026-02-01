from typing import TypedDict, Annotated, List, Union, Literal
from datetime import datetime
import random
import operator
import re
import os
import glob
import json
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage, AIMessage
from langgraph.prebuilt import create_react_agent
from langchain_core.prompts import ChatPromptTemplate
from macro_tools import get_macro_indicator, get_macro_overview, analyze_yield_curve
from pydantic import BaseModel, Field
from langchain_openai import ChatOpenAI  # 或 ChatTongyi
from langgraph.graph import StateGraph, END

# --- 引入你的工具库 ---
# 请确保这些文件名和你项目里的一致
from kline_tools import analyze_kline_pattern
from screener_tool import search_top_stocks, get_available_patterns
from news_tools import get_financial_news
from fund_flow_tools import tool_get_retail_money_flow
from polymarket_tool import tool_get_polymarket_sentiment
from plot_tools import draw_chart_tool,draw_macro_compare_chart
from futures_fund_flow_tools import get_futures_fund_flow, get_futures_fund_ranking
from volume_oi_tools import get_volume_oi, get_futures_oi_ranking, get_option_oi_ranking, get_option_volume_abnormal, get_option_oi_abnormal, analyze_etf_option_sentiment, get_etf_option_strikes
from market_tools import get_market_snapshot, get_price_statistics,tool_query_specific_option,get_historical_price,get_trending_hotspots,get_today_hotlist,analyze_keyword_trend,get_finance_related_trends,search_hotlist_history
from data_engine import get_commodity_iv_info, check_option_expiry_status,search_broker_holdings_on_date,tool_analyze_position_change,tool_compare_stocks,get_stock_valuation,get_latest_data_date,tool_analyze_broker_positions
from search_tools import search_web
from market_correlation import tool_stock_hedging_analysis, tool_futures_correlation_check,tool_stock_correlation_check
from beta_tool import calculate_hedging_beta
from knowledge_tools import search_investment_knowledge

# ==========================================
# 1. 定义共享记忆 (The State)
# ==========================================
class AgentState(TypedDict):
    # --- 基础信息 ---
    messages: Annotated[List[BaseMessage], operator.add]
    user_query: str

    # --- 调度控制 ---
    plan: List[str]  # 任务队列，如 ["analyst", "monitor"]
    current_step: str  # 当前正在执行的步骤

    # --- 专家结论 (黑板) ---
    symbol: str  # 标的代码 (如 "IM2606")
    symbol_name: str
    trend_signal: str  # "Bullish"(多), "Bearish"(空), "Neutral"(震荡)
    technical_summary: str  # 例如 "出现大阳线突破，且均线多头排列"
    key_levels: str  # "支撑3400, 压力3600"
    fund_data: str  # 资金流向描述
    news_summary: str  # 新闻摘要
    option_strategy: str  # 期权策略建议
    chart_img: str  # 图片路径
    risk_preference: str
    knowledge_context: str
    memory_context: str

    news_summary: str  # 情报员填入：新闻摘要 (CPI/非农/美联储)
    macro_view: str  # 宏观分析师填入：宏观定调 (宽松/紧缩)
    macro_chart: str  # 宏观分析师填入：生成的宏观对比图路径


# 期权合约乘数表（每张期权对应的标的数量）
OPTION_MULTIPLIER = {
    # ETF期权 (份)
    "510050": (10000, "份"), "510300": (10000, "份"), "510500": (10000, "份"),
    "159901": (10000, "份"), "159915": (10000, "份"), "159919": (10000, "份"),
    "159922": (10000, "份"), "588000": (10000, "份"), "588080": (10000, "份"),

    # 股指期权 (点×100元)
    "IO": (100, "点"), "MO": (100, "点"), "HO": (100, "点"),

    # 贵金属
    "AU": (1000, "克"), "AG": (15, "千克"),

    # 有色
    "CU": (5, "吨"), "AL": (5, "吨"), "ZN": (5, "吨"), "PB": (5, "吨"), "SN": (1, "吨"), "NI": (1, "吨"),

    # 黑色
    "I": (100, "吨"), "RB": (10, "吨"), "HC": (10, "吨"), "J": (100, "吨"), "JM": (60, "吨"),"SM": (5, "吨"),"SF": (5, "吨"),
    "PS": (3, "吨"),"LC": (1, "吨"),"SI": (5, "吨"),"PT": (1000, "吨"),"PD": (1000, "吨"),"SH": (30, "吨"),"AO": (20, "吨"),

    # 能化
    "SC": (1000, "桶"), "FU": (10, "吨"), "LU": (10, "吨"), "PG": (20, "吨"),
    "MA": (10, "吨"), "TA": (5, "吨"), "PP": (5, "吨"), "L": (5, "吨"),
    "V": (5, "吨"), "EB": (5, "吨"), "EG": (10, "吨"), "RU": (10, "吨"), "NR": (10, "吨"),"BR": (5, "吨"),
    "BU": (10, "吨"), "SA": (20, "吨"), "FG": (20, "吨"), "UR": (20, "吨"),

    # 农产品
    "M": (10, "吨"), "Y": (10, "吨"), "P": (10, "吨"), "OI": (10, "吨"), "RM": (10, "吨"),
    "C": (10, "吨"), "A": (10, "吨"), "CF": (5, "吨"), "SR": (10, "吨"),
    "AP": (10, "吨"), "PK": (5, "吨"), "CJ": (5, "吨"), "LH": (16, "吨"),
}


def get_option_multiplier(symbol: str) -> str:
    """
    根据标的代码获取期权合约乘数，返回精简的提示字符串
    支持单品种和多品种（逗号分隔）
    """
    import re
    if not symbol:
        return ""

    # 处理多品种情况 (如 "M,SR" 或 "豆粕,白糖")
    symbols = re.split(r'[,，、/\s]+', symbol.strip())
    results = []

    for sym in symbols:
        sym = sym.strip().upper()
        if not sym:
            continue

        # 1. 直接匹配 ETF (6位数字)
        if sym.isdigit() and len(sym) == 6:
            if sym in OPTION_MULTIPLIER:
                m, u = OPTION_MULTIPLIER[sym]
                results.append(f"{sym}={m}{u}")
            else:
                results.append(f"{sym}=10000份")  # ETF默认
            continue

        # 2. 股指期权 (IO/MO/HO开头)
        for prefix in ["IO", "MO", "HO"]:
            if sym.startswith(prefix):
                results.append(f"{prefix}=100点(每点100元)")
                break
        else:
            # 3. 商品期权 - 提取字母部分
            match = re.match(r'^([A-Za-z]+)', sym)
            if match:
                code = match.group(1).upper()
                if code in OPTION_MULTIPLIER:
                    m, u = OPTION_MULTIPLIER[code]
                    results.append(f"{code}={m}{u}")

    if not results:
        return ""

    # 返回精简格式
    return "、".join(results)


# ==========================================
# 2. 定义 Supervisor (大管家)
# ==========================================
# 定义输出结构，强制 LLM 返回 JSON 格式的任务列表
class PlanningOutput(BaseModel):
    plan: List[Literal["analyst", "researcher", "monitor", "strategist", "chatter", "generalist", "screener", "macro_analyst","roaster"]] = Field(
        description="执行步骤列表。注意依赖关系：期权(strategist)必须排在分析(analyst)之后。"
    )
    symbol: str = Field(description="核心标的代码。如果是对比问题或无法提取单一标的，请留空或填'MULTI'", default="")


def supervisor_node(state: AgentState, llm):
    """
    大管家节点：分析用户意图，生成任务清单
    """
    query = state["user_query"]
    messages = state.get("messages", [])
    history_text = ""
    if len(messages) > 1:  # 如果有历史消息
        history_lines = []
        for msg in messages[:-1]:  # 排除最后一条（当前问题）
            if isinstance(msg, HumanMessage):
                history_lines.append(f"用户: {msg.content[:200]}")  # 截取防止太长
            elif isinstance(msg, AIMessage):
                history_lines.append(f"AI: {msg.content[:150]}...")
        if history_lines:
            history_text = "\n".join(history_lines[-2:])  # 只取最近 2 条

    system_prompt = """
    你是交易团队的主管，根据问题制定计划。

    【可用员工】
    - analyst: 技术分析师 (看K线、定趋势),分析如何操作
    - monitor: 资金监控员 (看股票和期货资金流、期货商持仓、查持仓量和成交量、查价格、查合约)
    - researcher: 情报研究员 (看新闻、宏观、热点、地缘政治、货币政策、Polymarket上的概率分析、抖音热搜)
    - strategist: 期权策略员 (给策略，**必须依赖 analyst**) 
    - screener: 选股大师 (当用户问"推荐股票"、"什么股票好"、"选股"时使用)
    - chatter: 知识问答和闲聊 (例如解释一下IV，什么是牛市价差，"最近美联储什么时候开会")
    - generalist: 【王牌分析师】处理对比(A和B谁强)、多品种分析、画价差图或深度复杂问题。
    - macro_analyst: 宏观策略师 (分析美联储、美债、美元、通胀、CPI、非农、画利率图)
    - roaster: *毒舌分析师* (当用户要求"吐槽"、"挑战我"、"毒舌模式"时使用)。

    【调度规则 (严格遵守)】
    1. **追求效率**: 只问资金流就只派 `monitor`；只问持仓量或价格就只派 `monitor`；只问新闻或热点就只派 `researcher`；只问技术分析就只派`analyst`。
    2. **全套服务**: 如果用户问"全面分析"或"怎么做"，默认路径: ["analyst", "monitor", "researcher","strategist"]。
    3. **单品种期权问题**: "500ETF适合价差还是裸买"、"推荐白银期权策略" -> 
       - 只要标的明确(500ETF)，且涉及期权交易，一律走流水线。
       - Plan: `['analyst', 'strategist']` (必须先分析再出策略)。
    4. **多品种/对比**: 问"白银和黄金谁强"、"分析一下螺纹和热卷" -> 
       - symbol 填 "白银,黄金" (用逗号分隔)
       - plan 派 `['generalist']` (让王牌去处理多品种)。
    5. **宏观/大宗/贵金属**: 
       - 问 "现在宏观环境怎么样"、"美联储降息了吗" -> Plan: `['researcher', 'macro_analyst']` (先找新闻，再分析数据)。
       - 问 "黄金/白银/能买吗" -> Plan: `['analyst', 'researcher', 'macro_analyst', 'strategist']` (黄金对宏观极度敏感，必须加宏观分析)。
       - 问 "利率/美元走势" -> Plan: `['researcher', 'macro_analyst']`。
    6. 如果客户要画图，
       - plan 派 `['generalist']` 。
    7. **知识/百科/闲聊**: 问概念、问人名、问名词 -> 派 ['chatter']。
    8. 如果用户想分析行情但**没说名字** (如"帮我分析一下") -> plan=['chatter'] (让Chatter去问用户要代码)。
    """

    # 🔥 [修改] 将历史对话也包含在 query 中
    full_query = query
    if history_text:
        full_query = f"【近期对话历史】\n{history_text}\n\n【当前问题】\n{query}"

    # 使用 structured_output 强制输出 JSON
    planner = llm.with_structured_output(PlanningOutput)
    prompt = ChatPromptTemplate.from_messages([
        ("system", system_prompt),
        ("human", "{query}")
    ])

    chain = prompt | planner
    result = chain.invoke({"query": full_query})

    # 🔥🔥🔥 [新增防崩溃检查]
    # 如果 LLM 没有返回有效的结构化数据 (None)，默认转给 Chatter
    if not result:
        return {
            "plan": ["chatter"],
            "symbol": "",
            "messages": [SystemMessage(content="主管未生成计划，默认转入闲聊")]
        }

    final_plan = result.plan
    final_symbol = str(result.symbol).strip()

    # 简单的正则判断是否为 A 股个股代码 (6开头沪市主板/科创, 0开头深市, 3开头创业板, 8/4开头北交所)
    # ETF 通常是 51, 56, 58, 159 开头
    # 期货 通常是 字母开头

    # 提取纯数字代码
    match_code = re.search(r'\d{6}', final_symbol)
    if match_code:
        code_num = match_code.group(0)

        # 判断逻辑：如果是 6/0/3/8/4 开头的6位数字，视为个股 -> 剔除 strategist
        # (ETF 一般是 5 或 1 开头，保留)
        if code_num.startswith(('6', '0', '3', '8', '4')):
            if "strategist" in final_plan:
                final_plan.remove("strategist")

    if 'screener' in final_plan:
        final_symbol = ""

    return {
        "plan": final_plan,
        "symbol": final_symbol,
        "messages": [SystemMessage(content=f"已制定计划: {final_plan}")]
    }


# ==========================================
# 🔥 [新增] 王牌分析师 (Generalist / Fallback)
# 职责：处理对比、综合、模糊或 Supervisor 搞不定的复杂问题
# ==========================================
def generalist_node(state: AgentState, llm):
    query = state["user_query"]
    symbol_input = state.get("symbol", "")
    current_date = datetime.now().strftime("%Y年%m月%d日 %A")
    tools = [
        analyze_kline_pattern, search_investment_knowledge, get_market_snapshot, get_commodity_iv_info,
        search_broker_holdings_on_date, tool_analyze_position_change,
        tool_query_specific_option, get_historical_price, get_volume_oi, get_futures_oi_ranking,
        get_option_oi_ranking, get_option_volume_abnormal, get_option_oi_abnormal,
        get_price_statistics, check_option_expiry_status, tool_stock_hedging_analysis,
        tool_futures_correlation_check, tool_stock_correlation_check, calculate_hedging_beta,
        tool_get_retail_money_flow, draw_chart_tool, get_stock_valuation, tool_compare_stocks,
        get_futures_fund_flow, get_futures_fund_ranking, get_available_patterns, analyze_etf_option_sentiment,
        get_etf_option_strikes,search_web,tool_analyze_broker_positions,
        get_macro_indicator,get_macro_overview,analyze_yield_curve
    ]


    prompt = f"""
        你是一位王牌量化分析师。交易理念是顺势而为。
        【当前日期】：{current_date}。
        客户需求：{query}。
        分析品种：{symbol_input}。
   
        
        【工具使用表】
        1. **估值/便宜/贵吗/抄底** -> get_stock_valuation 
        2. **对比/PK/谁强/选哪个** -> tool_compare_stocks (多股横评)
        3. **对冲/相关性/联动** -> tool_stock_correlation_check
        4. **历史统计价格** -> get_price_statistics
        5. **画图/走势图** -> draw_chart_tool
        6. **概念/策略解释** -> search_investment_knowledge
        7. 相关性分析 -> tool_futures_correlation_check或tool_stock_correlation_check
        8. 对冲分析 -> calculate_hedging_beta
        9. 搜寻辅助分析的信息 ->search_web
        10.查某期货资金流动 -> get_futures_fund_flow
        11.查全部期货资金沉淀排名 -> get_futures_fund_ranking
        12.查商品龙虎榜/期货商持仓 -> search_broker_holdings_on_date  
        13.查某期货商最近持仓变化情况 -> tool_analyze_position_change
        14.查成交量和持仓量 -> get_volume_oi
        15.查期货持仓量排名 -> get_futures_oi_ranking
        16.查期权波动率-> get_commodity_iv_info
        17.查期权合约价格-> tool_query_specific_option
        18.查ETF期权有哪些合约-> get_etf_option_strikes
        19.查宏观指标 -> get_macro_indicator
        20.查宏观环境总览 -> get_macro_overview 
        21.分析收益率曲线 -> analyze_yield_curve 

        【行为准则】
        1. 先给结论，然后解释理由。
        2. 不要简单复述，要有深度洞察。
        3. 禁止空谈，必须用工具获取的数据说话。
        4. 不要编造数据，如果没查到数据就说不知道。
        """

    general_agent = create_react_agent(llm, tools, prompt=prompt)

    # 🔥 用于在异常时恢复部分结果
    partial_response = ""
    chart_img = ""

    try:
        # 给予足够的递归步数，但不要太高避免 GeneratorExit
        result = general_agent.invoke(
            {"messages": state["messages"]},
            {"recursion_limit": 100}
            # 降低到 15，足够完成大部分任务
        )

        last_response = result["messages"][-1].content
        partial_response = last_response

        # 🔥 从响应中提取图表路径
        chart_match = re.search(r'!\[.*?\]\((chart_[a-zA-Z0-9_]+\.json)\)', last_response)
        if chart_match:
            chart_img = chart_match.group(1)

        # 🔥 如果响应中没找到，尝试从所有消息中查找
        if not chart_img:
            for msg in result.get("messages", []):
                content = getattr(msg, 'content', str(msg))
                chart_match = re.search(r'(chart_[a-zA-Z0-9_]+\.json)', content)
                if chart_match:
                    chart_img = chart_match.group(1)
                    break

        return {
            "messages": [HumanMessage(content=f"【王牌分析】\n{last_response}")],
            "chart_img": chart_img
        }
    except GeneratorExit:
        # 🔥 GeneratorExit 通常发生在图表已生成之后，尝试查找最近生成的图表
        charts_dir = os.path.join(os.path.dirname(__file__), "static", "charts")
        if os.path.exists(charts_dir):
            chart_files = glob.glob(os.path.join(charts_dir, "chart_*.json"))
            if chart_files:
                # 获取最新的图表文件
                latest_chart = max(chart_files, key=os.path.getmtime)
                chart_img = os.path.basename(latest_chart)

        return {
            "messages": [HumanMessage(
                content=f"【王牌分析】分析完成\n{partial_response}" if partial_response else "【王牌分析】图表已生成，请查看下方")],
            "chart_img": chart_img
        }
    except Exception as e:
        # 优雅降级
        return {
            "messages": [HumanMessage(content=f"【王牌分析】思考过程中断: {e}")],
            "chart_img": chart_img  # 仍然尝试返回可能的图表
        }



# ==========================================
# 3. 定义各个专家节点 (Workers)
# ==========================================

# 🟢 1. 技术分析师
def analyst_node(state: AgentState, llm):
    symbol = state["symbol"]
    query = state["user_query"]
    mem_context = state.get("memory_context", "")
    current_date = datetime.now().strftime("%Y年%m月%d日 %A")
    tools = [
        analyze_kline_pattern,  # 核心：形态与趋势
        get_market_snapshot,
        get_price_statistics,  # 辅助：历史波动数据
        draw_chart_tool
    ]

    target_option_etfs = [
        "510050",  # 50ETF
        "510300",  # 300ETF
        "510500",  # 500ETF
        "159915",  # 创业板ETF
        "588000"  # 科创50ETF
    ]
    # 检查当前 symbol 是否命中上述列表
    # 动态判断是否加入ETF期权工具 (保持之前的逻辑)
    target_option_etfs = ["510050", "510300", "510500", "159915", "588000"]
    is_target_etf = any(code in symbol for code in target_option_etfs)

    extra_instruction = ""
    if is_target_etf:
        from volume_oi_tools import analyze_etf_option_sentiment
        tools.append(analyze_etf_option_sentiment)
        extra_instruction = """
            🎯 **期权主力持仓验证**：
            - 调用 `analyze_etf_option_sentiment` 查看期权最大持仓位作为支撑压力参考。
            """

    # 2. 注入“严谨”人设进行润色
    is_chart_only = any(kw in query for kw in ["K线图", "k线图"])

    if is_chart_only:
        # 🔥 画图快速模式 - 简化 prompt
        persona_prompt = f"""
            你是一位技术分析画图师。【当前日期】：{current_date}。
            【当前标的】：{symbol}
            【客户需求】：{query}

            【任务】：
            用户想要看图表，请直接调用 `draw_chart_tool` 画图。

            【回复要求】：
            1. 画完图后，只要简短说明图表关键信息（如当前价格、涨跌幅）。
            2. 绝对不要做冗长的分析！
            """
    else:
        # 正常分析模式
        persona_prompt = f"""
            你是一位严谨的技术分析师。遵循趋势交易。
            【当前日期】：{current_date}。
            【当前标的】：{symbol}
            【客户历史记忆】：{mem_context}

            【ETF期权持仓数据】：{extra_instruction}
            【客户需求】：{query}

            【可调用工具】
            1. 技术面分析-> `analyze_kline_pattern` ，获取K线形态和趋势。
            2. 获取标的一段时间价格-> `get_price_statistics` 。
            3. 分析的品种如果只有1个，只能调用1次`analyze_kline_pattern` 
            4. 客户没要求画图，就不要用`draw_chart_tool`
            5. 获取股票名字和价格用 get_market_snapshot

            【任务】：
            1. 描述K线和技术面情况
            2. 发掘突破进场机会。
            3. 如果有反转的风险，可以提醒。
            4. 如果没有明显机会，直说"建议观望"。
            5. 如果用户的指令模糊，可参考上文历史确认分析对象。
            """


    # 简单的方向提取 (给策略员用)
    analyst_agent = create_react_agent(llm, tools, prompt=persona_prompt)

    partial_response = ""
    chart_img = ""
    symbol_name = ""

    try:
        # 执行推理 (给予足够的递归次数，因为处理价差可能需要调2次工具)
        result = analyst_agent.invoke(
            {"messages": state["messages"]},
            {"recursion_limit": 20}
        )

        last_response = result["messages"][-1].content
        partial_response = last_response
        # 🔥 提取公司名称（从get_market_snapshot的返回中）
        symbol_name = ""
        # 格式："📍 **乾照光电(300102.SZ) 行情**"
        name_match = re.search(r'📍 \*\*(.+?)\((.+?)\) 行情\*\*', last_response)
        if name_match:
            symbol_name = name_match.group(1)
        else:
            # 兜底：如果没提取到，用symbol
            symbol_name = state.get('symbol', '')

        chart_match = re.search(r'IMAGE_CREATED:(chart_[a-zA-Z0-9_]+\.json)', last_response)
        if chart_match:
            chart_img = chart_match.group(1)
            print(f"📊 analyst_node 提取到图表: {chart_img}")
        if not chart_img:
            for msg in result.get("messages", []):
                content = getattr(msg, 'content', str(msg))
                chart_match = re.search(r'IMAGE_CREATED:(chart_[a-zA-Z0-9_]+\.json)', content)
                if chart_match:
                    chart_img = chart_match.group(1)
                    break

        # 提取信号
        trend_signal = "震荡"
        if any(x in last_response for x in ["看涨", "多头", "上行", "突破"]):
            trend_signal = "看涨"
        elif any(x in last_response for x in ["看跌", "空头", "下行", "破位"]):
            trend_signal = "看跌"

        levels = re.findall(r'(支撑|压力|阻力).*?(\d+[\.\d]*)', last_response)
        key_levels_str = " ".join([f"{k}{v}" for k, v in levels]) if levels else ""

        # 提取前 100 个字作为技术摘要
        tech_summary_str = last_response.replace("\n", " ")[:100]

        return {
            "messages": [HumanMessage(content=f"【技术分析】\n{last_response}")],
            "symbol_name": symbol_name,
            "trend_signal": trend_signal,  # 存入趋势
            "key_levels": key_levels_str,  # 存入点位 (新增)
            "technical_summary": tech_summary_str,  # 存入摘要 (新增)
            "chart_img": chart_img
        }

    except GeneratorExit:
        # 🔥 GeneratorExit 不是 Exception 子类，需要单独捕获
        # 尝试从已调用的工具中提取结果
        fallback_msg = partial_response if partial_response else f"技术分析已完成对 {symbol} 的初步分析"
        return {
            "messages": [HumanMessage(content=f"【技术分析】\n{fallback_msg}")],
            "symbol_name": symbol_name,
            "trend_signal": "震荡"
        }
    except Exception as e:
        print(f"Analyst Node Error: {e}")
        return {
            "messages": [HumanMessage(content=f"【技术分析】分析受阻: {e}")],
            "symbol_name": state.get('symbol', ''),
            "trend_signal": "未知"
        }


# 🟡 2. 资金监控员 (容错跳过)
def monitor_node(state: AgentState, llm):
    user_q = state["user_query"]
    symbol = state.get("symbol", "")
    symbol_name = state.get("symbol_name", "")
    current_date = datetime.now().strftime("%Y年%m月%d日 %A")
    latest_trade_date = get_latest_data_date()

    # 1. 装备所有数据类工具
    tools = [
        tool_get_retail_money_flow,  # 股票行业资金
        get_futures_fund_flow,  # 期货资金流
        get_futures_fund_ranking, # 期货沉淀资金排名
        get_commodity_iv_info,  # IV/波动率/Rank
        search_broker_holdings_on_date,  # 期货商持仓排名
        tool_analyze_position_change,  # 持仓变动分析
        get_option_volume_abnormal,
        get_option_oi_abnormal,
        get_option_oi_ranking,
        get_volume_oi,
        get_market_snapshot,
        tool_analyze_broker_positions,
        get_futures_oi_ranking,
        get_macro_indicator
    ]

    # 判断是否为 ETF (51/159开头) 或 股票
    is_etf_or_stock = False
    import re
    if re.search(r'\d{6}', symbol):  # 只要包含6位数字，大概率是证券
        is_etf_or_stock = True

    tool_instruction = ""
    if is_etf_or_stock:
        tool_instruction = """
        ⚠️ **特别注意 (ETF/股票)**：
        1. `tool_get_retail_money_flow` 只能查全市场行业概况，不支持查单只股票/ETF代码。
        2. 如果是 ETF，可以尝试查 `get_commodity_iv_info` 。
        3. 如果没有合适工具，直接回答 "暂无该品种资金数据"，不要编造数据。
                """
    else:
        tool_instruction = """
        ⚠️ **特别注意 (期货)**：
        1. 查某品种当天的期货商多空排名 -> search_broker_holdings_on_date(broker_name='所有', symbol='品种名', date='日期')
        2. 查某品种一段时间各期货商的持仓变化 -> tool_analyze_position_change(symbol='品种名', start_date, end_date)
        3. 查期货资金流 -> get_futures_fund_flow(symbol='品种名')
        4. 如果工具返回"未找到数据"，如实告知用户，不要编造假数据！
                """
    # 2. 定义 Prompt
    # 告诉他只做数据搬运工，不要给建议
    prompt = f"""
    你是一位追求效率的市场数据监控官**。。只负责查数据给结果。
    - 今天日期：{current_date}
    - 数据库最新交易日：{latest_trade_date}

    【你的工具箱 - 根据问题类型选择正确的工具】
    - 查波动率/IV -> get_commodity_iv_info
    - 查股票行业资金 -> tool_get_retail_money_flow
    - 查某期货资金流动 -> get_futures_fund_flow
    - 查全部期货资金沉淀排名 -> get_futures_fund_ranking
    - 查某天某品种的期货商持仓排名（龙虎榜） -> search_broker_holdings_on_date 
    - 查某品种一段时间内各期货商的持仓变化 -> tool_analyze_position_change 
    - 查某期货商在各品种的持仓变化 -> tool_analyze_broker_positions （当前净持仓代表期货商对这品种的趋势判断）
    - 查期权成交量异常(放量/异动) -> get_option_volume_abnormal
    - 查期权持仓量异常(大单增仓) -> get_option_oi_abnormal
    - 查期权持仓量排名 -> get_option_oi_ranking
    - 查成交量和持仓量 -> get_volume_oi
    - 查期货持仓量排名 -> get_futures_oi_ranking
    - 查标的价格 -> get_market_snapshot
    - 查宏观指标 -> get_macro_indicator(indicator_code='US10Y')  
    
    {tool_instruction}

    【要求】
    1. 精准使用工具，不要乱调用，除非客户有要求全面分析。
    2. **只陈述数据事实**，不要进行复杂的行情预测或给交易建议。
    3. 如果用户没有指定日期，**必须使用 {latest_trade_date}** 作为查询日期！
    4. 如果工具返回了 Markdown 表格，请原样输出。
    """

    # 3. 创建临时 Agent (ReAct 模式)
    # 使用 bind_tools 让 LLM 可以自动选择用哪个工具
    monitor_agent = create_react_agent(llm, tools, prompt=prompt)

    partial_response = ""

    try:
        # 限制迭代次数，防止死循环
        result = monitor_agent.invoke(
            {"messages": [HumanMessage(content=user_q)]},
            {"recursion_limit": 20}  # 降低到 15
        )
        last_response = result["messages"][-1].content
        partial_response = last_response

        return {
            "messages": [HumanMessage(content=f"【数据监控】\n{last_response}")],
            "fund_data": last_response
        }

    except GeneratorExit:
        # 🔥 GeneratorExit 不是 Exception 子类，需要单独捕获
        fallback_msg = partial_response if partial_response else f"资金数据查询完成"
        return {
            "messages": [HumanMessage(content=f"【数据监控】\n{fallback_msg}")],
            "fund_data": fallback_msg
        }
    except Exception as e:
        # 🛑 只要出错，立马优雅降级，返回空数据，保证 Supervisor 和 Finalizer 能继续工作
        error_msg = f"数据查询暂不可用 (Monitor Error)"
        print(f"Monitor Node Crash: {e}")  # 后台打印日志方便调试
        return {
            "messages": [HumanMessage(content=f"【数据监控】{error_msg}")],
            "fund_data": "无数据"
        }


# 🟠 3. 期权策略员 (逻辑硬编码 + LLM润色)
def strategist_node(state: AgentState, llm):
    """
    期权策略员 - 使用 ReAct 模式让 LLM 自主决定调用工具
    """
    symbol = state["symbol"]
    user_q = state.get("user_query", "")
    risk_pref = state.get("risk_preference", "稳健型")
    fund = state.get("fund_data", "暂无明显资金流向")
    trend = state.get("trend_signal", "Neutral")
    mem_context = state.get("memory_context", "")
    tech_view = state.get("technical_summary", "")
    current_date = datetime.now().strftime("%Y年%m月%d日")
    key_level = state.get("key_levels", "")

    # [新增] 获取合约乘数
    multiplier_str = get_option_multiplier(symbol)
    multiplier_hint = f"\n        【合约乘数】：{multiplier_str}（计算盈亏时必须乘以此数）" if multiplier_str else ""

    # === 🔥 期权策略专用工具集 ===
    tools = [
        # 期权数据工具
        get_commodity_iv_info,  # IV排名/波动率
        check_option_expiry_status,  # 到期日状态
        tool_query_specific_option,  # 查询特定期权合约
        get_option_volume_abnormal,  # 期权成交异动
        get_option_oi_abnormal,  # 期权持仓异动
        get_etf_option_strikes,  # ETF期权行权价
        # 标的分析工具
        get_market_snapshot,  # 标的快照/现价
        # 辅助工具
        search_investment_knowledge,  # 知识库检索
    ]

    # === 🔥 ReAct Prompt - 引导期权策略推理 ===
    prompt = f"""
        你是一位**资深期权交易策略师**，擅长根据市场数据设计期权策略。

        【当前日期】：{current_date}
        【分析标的】：{symbol}{multiplier_hint}
        【客户问题】：{user_q}
        【客户风险偏好】：{risk_pref} 
        【客户历史记忆】：{mem_context}
        【技术面参考】：{trend} 、 {tech_view}

        【工作流程】
        **第一步：获取标的价格和波动率**
        - 用 `get_market_snapshot` 获取现价，用`get_commodity_iv_info` 看IV，用`check_option_expiry_status` 看到期日。
              
        **第二步：设计策略**
        - **期权策略**：根据技术面趋势+IV+距离到期日+客户风险偏好来选择策略，可以查知识库辅助`search_investment_knowledge`。
        
        **第三步：思考行权价合约 (Strikes)**
        - 如果客户有指定行权价合约，就直接根据客户需求，但可以给出合适的不同建议。
        - 如果用户没指定，就根据设计的策略和客户风险偏好来看需要哪些合约。

        **第四步：确定策略的执行合约**
        - **合约选择**：一定要根据标的现价，再来找合适的行权价合约。
        - **查询合约**：用 `tool_query_specific_option` 查具体期权价格（格式："标的 行权价 认购/认沽"），权利金价格也要乘上合约乘数。
        - 只有当工具返回了有效的价格数据时，才能推荐该合约。
        - 如果工具返回“未找到”，请尝试调整行权价再次查询，或者诚实告知用户该档位无合约。

        【风险偏好适配】：
           - 【保守型】：只推荐风险有限的策略（牛市价差、熊市价差、比率价差），禁止裸卖
           - 【稳健型】：可以适度进攻（买平值期权、顺势卖虚值期权、价差策略、备兑策略、合成期货）
           - 【激进型】：可以用积极策略（有趋势时买深虚期权、买末日期权、飞龙在天，没趋势时就双卖期权，或者卖末日期权）

        【输出要求】
        1. 给出 1-2 个具体的期权策略建议，行权价必须用工具查过。
        2. **计算盈亏示例时，必须乘以合约乘数**
        3. 解释为什么这个策略适合市场或客户，可以查知识库辅助`search_investment_knowledge`
        4. 给出止损/止盈建议
        5. 禁止自己编造假数据！

        """

    # === 🔥 创建 ReAct Agent ===
    strategist_agent = create_react_agent(llm, tools, prompt=prompt)

    # 用于异常恢复
    partial_response = ""

    try:
        result = strategist_agent.invoke(
            {"messages": [HumanMessage(content=user_q)]},
            {"recursion_limit": 50}  # 期权分析可能需要多轮工具调用
        )

        last_response = result["messages"][-1].content
        partial_response = last_response

        return {
            "messages": [HumanMessage(content=f"【期权策略】\n{last_response}")],
            "option_strategy": last_response
        }

    except GeneratorExit:
        # 流被中断时的优雅降级
        fallback_msg = partial_response if partial_response else f"期权策略分析已完成，关于{symbol}的建议请参考上文。"
        return {
            "messages": [HumanMessage(content=f"【期权策略】\n{fallback_msg}")],
            "option_strategy": fallback_msg
        }

    except Exception as e:
        # 其他异常的降级处理
        error_msg = f"期权策略分析遇到问题: {e}"
        print(f"⚠️ strategist_node 错误: {e}")
        return {
            "messages": [HumanMessage(content=f"【期权策略】\n{error_msg}")],
            "option_strategy": ""
        }



# 🟤 5. 情报研究员
def researcher_node(state: AgentState,llm=None):
    symbol = state["symbol"]
    symbol_name = state.get("symbol_name", "")
    query = state["user_query"]
    current_date = datetime.now().strftime("%Y年%m月%d日 %A")
    # 1. 装备舆情与搜索工具
    tools = [
        get_finance_related_trends,  # 查财经类热点 (同花顺/东方财富热榜)
        get_today_hotlist,  # 查全网热搜 (抖音/微博/百度)
        tool_get_polymarket_sentiment,  # 查预测市场胜率 (Polymarket)
        analyze_keyword_trend,  # 查特定关键词热度趋势
        search_hotlist_history,  # 查热点历史回溯
        search_web,  # 兜底：通用联网搜索
        get_financial_news,  # 兜底：传统财经新闻
        get_trending_hotspots
    ]
    system_prompt = f"""
        你是一位**顶级市场情报官 (Market Intelligence Officer)**。
        你的职责不仅仅是看新闻，更是捕捉**市场情绪、热点风口和宏观预期**。
        【当前真实日期】：{current_date} 

        【客户需求】: "{query}"
        【标的】: {symbol_name}({symbol})  

        【工具调用策略】：

        1. 🎲 **宏观预期/大事件/胜率** (如 "大选谁赢"、"降息概率"、"地缘政治"、"战争"):
           - **必须调用** `tool_get_polymarket_sentiment`。
           - Polymarket 的真金白银押注数据比新闻更准。

        2. 🔥 **市场风口/散户热度** (如 "现在炒什么"、"最近的热点"):
           - **优先调用** `get_finance_related_trends` (看财经圈在关注什么)。
           - **辅助调用** `get_today_hotlist` (看抖音/微博等全网流量在哪)。
           - 关注关键词：概念板块、突发事件、政策利好。

        3. 📈 **特定概念热度验证** (如 "低空经济最近热吗"):
           - **调用** `analyze_keyword_trend`。
           - 用数据证明该话题是处于"升温期"还是"退潮期"。
           - **调用** get_trending_hotspots 查最近有什么热点趋势
        
        4. 📈 **当天财经快讯** :
           - **调用** `get_financial_news`。
           
        5. 📰 **具体资讯/事实核查**:
           - 如果以上工具查不到，或者需要更多细节，调用 `search_web``，只能使用1次，绝对不要重复使用search_web。

        【输出要求】
        - 如果是预测数据，必须给出**概率/胜率** (如 "特朗普胜率 65%")。
        - 如果是热点数据，必须指出**热度排名** (如 "抖音热搜 Top3")。
        - 为其他分析师（如策略师）提供简短的情绪总结（看多/看空/避险）。
        """

    # 3. 创建 Agent
    researcher_agent = create_react_agent(llm, tools, prompt=system_prompt)

    partial_response = ""

    try:
        # 舆情查询可能需要多步（先查热榜，再搜细节），给足步数
        result = researcher_agent.invoke(
            {"messages": [HumanMessage(content=query)]},
            {"recursion_limit": 30}
        )

        last_response = result["messages"][-1].content

        # 🔥 检测是否是 "need more steps" 错误
        if "need more steps" in last_response.lower() or "sorry" in last_response.lower():
            # 尝试从之前的工具调用结果中提取有用信息
            tool_results = []
            for msg in result.get("messages", []):
                msg_type = getattr(msg, 'type', '')
                content = getattr(msg, 'content', '')
                # 收集工具返回的内容
                if msg_type == 'tool' and content and len(content) > 50:
                    tool_results.append(content)

            if tool_results:
                # 拼接所有工具结果
                combined = "\n\n".join(tool_results[-3:])  # 取最近3个工具结果
                last_response = f"根据已收集的信息：\n\n{combined}"

        # 加上前缀，方便 Finalizer 整合
        return {
            "messages": [HumanMessage(content=f"【情报与舆情】\n{last_response}")]
        }

    except GeneratorExit:
        # 🔥 GeneratorExit 处理：尝试返回已收集的部分结果
        fallback_msg = partial_response if partial_response else f"情报查询已完成，关于：{query[:50]}"
        return {
            "messages": [HumanMessage(content=f"【情报与舆情】\n{fallback_msg}")]
        }
    except Exception as e:
        return {
            "messages": [HumanMessage(content=f"【情报】查询受阻: {e}")]
        }


def macro_analyst_node(state: AgentState, llm):
    """
    宏观策略师：全景扫描宏观数据，结合收益率曲线和新闻，判断全球流动性周期。
    """
    user_q = state.get("user_query", "")
    news_context = state.get("news_summary", "暂无最新宏观新闻")
    current_date = datetime.now().strftime("%Y年%m月%d日")

    # 引入宏观工具 (请确保在文件头部 import 这些工具)
    # from plot_tools import draw_macro_compare_chart
    # from macro_tools import get_macro_indicator

    tools = [
        get_macro_indicator,  # 来自 macro_tools (已升级支持多查)
        get_macro_overview,  # 来自 macro_tools (看全局)
        analyze_yield_curve,  # 来自 macro_tools (看倒挂)
        draw_macro_compare_chart,  # 来自 plot_tools (看双轴走势)
        get_financial_news  # 来自 news_tools (看新闻找原因)
    ]

    prompt = f"""
        你是一位**首席宏观策略师**，信奉 "Don't fight the Fed"。
        你的核心任务是利用【数据全景 + 收益率曲线 + 核心指标】模型判断全球流动性环境。

        【当前日期】：{current_date}
        【情报员提供的新闻】：
        {news_context}

        【分析逻辑与工具调用顺序】

        **第一步：全景与衰退诊断 (必须执行)**
        1. 调用 `get_macro_overview(category='all')`：
           - 快速扫一眼全球市场，看是否有异常板块（如BDI暴跌暗示需求不足，非美货币集体暴跌暗示美元虹吸）。
        2. 调用 `analyze_yield_curve()`：
           - **这是最关键的一步**。检查美债 10Y-2Y 是否**倒挂**。
           - 倒挂 = 衰退预警/降息预期升温；陡峭化 = 复苏或通胀预期。

        **第二步：核心锚点验证**
        1. 调用 `get_macro_indicator(codes='US10Y,DXY,US2Y')`：
           - 获取精确的最新报价和趋势。
        2. 结合 `Researcher` 的新闻（CPI/非农/FOMC），解释数据为何波动。

        **第三步：可视化 **
        - 调用 `draw_macro_compare_chart` 绘制 US10Y vs DXY 的对比图，直观展示流动性收紧还是放松。

        【决策矩阵 (结合收益率曲线)】
        - **紧缩交易**：US10Y 上行 + DXY 强势 + 曲线正常 -> 经济过热，美联储加息，杀估值。
        - **衰退交易**：US10Y 下行 + 曲线倒挂(或倒挂加深) -> 市场恐慌，押注降息，利好黄金/美债。
        - **避险交易 (Risk-Off)**：US10Y 下行 + DXY 强势 -> 衰退恐慌，股市暴跌，美元美债双牛。
        - **复苏交易**：US10Y 温和上行 + 曲线陡峭化 -> 经济复苏，利好商品/股票。
        - **滞胀/信任危机**：US10Y 上行 + DXY 弱势 -> 比较罕见，利好实物商品和黄金资产。

        【输出要求】
        请输出一份逻辑严密的宏观研报：
        1. **【周期定调】**：明确当前是“紧缩”、“衰退恐慌”还是“复苏”模式。
        2. **【收益率曲线监测】**：专门一段分析倒挂情况及其隐含的经济衰退概率。
        3. **【资产影响】**：基于上述判断，对 黄金/股市/大宗商品 的具体影响。
        """

    macro_agent = create_react_agent(llm, tools, prompt=prompt)

    try:
        result = macro_agent.invoke(
            {"messages": [HumanMessage(content=f"请分析当前的宏观流动性环境。用户问题：{user_q}")]},
            {"recursion_limit": 30}
        )
        last_response = result["messages"][-1].content

        # 提取图表
        chart_img = ""
        chart_match = re.search(r'(macro_chart_[a-zA-Z0-9_]+\.json)', last_response)
        if chart_match:
            chart_img = chart_match.group(1)

        return {
            "messages": [HumanMessage(content=f"【宏观策略】\n{last_response}")],
            "macro_view": last_response,
            "macro_chart": chart_img
        }
    except Exception as e:
        return {
            "messages": [HumanMessage(content=f"【宏观策略】分析受阻: {e}")]
        }

# ==========================================
# 🟣 6. 聊天/知识问答员 (Chatter)
# 职责：闲聊 + 百科知识问答 (RAG + Web)
# ==========================================
def chatter_node(state: AgentState, llm=None):
    """
    聊天/知识问答员 - 使用 ReAct 模式自主思考和检索
    优先使用内部知识库，必要时辅以网络搜索
    """
    user_query = state["user_query"]
    mem_context = state.get("memory_context", "")
    current_date = datetime.now().strftime("%Y年%m月%d日")

    # 判断是否是简单问候（不需要工具）
    is_greeting = len(user_query) < 5 and any(x in user_query for x in ["你好", "嗨", "早", "谢", "hello", "hi", "嘿", "晚上好", "早上好", "早安", "中午好", "下午好"])

    if is_greeting:
        # 简单问候直接回复，不启动 ReAct
        response = llm.invoke(f"用户说：{user_query}。请热情回应，并引导用户询问行情、策略或金融知识。")
        return {
            "messages": [HumanMessage(content=f"【闲聊】\n{response.content}")],
            "knowledge_context": ""
        }

    # === 🔥 知识问答专用工具集 ===
    tools = [
        # 知识检索工具（优先）
        search_investment_knowledge,  # 内部知识库 - 最高优先级

        # 网络搜索工具（辅助）
        get_financial_news,  # 财经新闻

        # 市场数据工具（如果用户问行情相关）
        get_market_snapshot,  # 快速获取标的价格

    ]

    # === 🔥 ReAct Prompt - 强调知识库优先 ===
    prompt = f"""
        你是一位热情、博学的**金融导师**，负责解答用户的金融知识问题和闲聊。

        【当前日期】：{current_date}
        【用户问题】：{user_query}
        【历史对话记录】{mem_context}

        【⚠️ 核心原则：知识库优先】
        1. **第一步必须**：先用 `search_investment_knowledge` 检索内部知识库
        2. **第二步可选**：如果知识库信息不足或需要最新数据，再用其他工具补充
           - `get_financial_news`：获取财经新闻
           - `get_market_snapshot`：获取实时行情

        【回答风格】
        1. 语气要轻松、易懂，像朋友聊天一样
        2. 如果是概念解释，用通俗的例子帮助理解
        3. 如果是策略问题，结合实际场景说明
        4. 适当引导用户深入探讨相关话题


        【禁止事项】
        - 不要编造数据或策略
        - 知识库内容要优先参考
        """

    # === 🔥 创建 ReAct Agent ===
    chatter_agent = create_react_agent(llm, tools, prompt=prompt)

    # 用于异常恢复
    partial_response = ""
    kb_content = ""

    try:
        result = chatter_agent.invoke(
            {"messages": [HumanMessage(content=user_query)]},
            {"recursion_limit": 15}  # 知识问答通常不需要太多轮
        )

        last_response = result["messages"][-1].content
        partial_response = last_response

        # 尝试从消息中提取知识库内容（用于后续流程）
        for msg in result.get("messages", []):
            content = getattr(msg, 'content', '')
            if "知识库" in content or "投资笔记" in content:
                kb_content = content[:500]
                break

        return {
            "messages": [HumanMessage(content=f"【知识问答】\n{last_response}")],
            "knowledge_context": kb_content
        }

    except GeneratorExit:
        # 流被中断时的优雅降级
        fallback_msg = partial_response if partial_response else "让我来回答你的问题..."
        return {
            "messages": [HumanMessage(content=f"【知识问答】\n{fallback_msg}")],
            "knowledge_context": kb_content
        }

    except Exception as e:
        # 其他异常 - 降级到简单回答
        print(f"⚠️ chatter_node 错误: {e}")
        try:
            # 尝试不用工具直接回答
            simple_response = llm.invoke(f"用户问：{user_query}。请基于你的知识回答，语气轻松友好。")
            return {
                "messages": [HumanMessage(content=f"【闲聊】\n{simple_response.content}")],
                "knowledge_context": ""
            }
        except:
            return {
                "messages": [HumanMessage(content=f"【闲聊】\n抱歉，我遇到了一些问题。请稍后再试或换个方式提问。")],
                "knowledge_context": ""
            }


# 🟣 =选股员 (Screener)
# 职责：结合【行业资金风向】和【个股技术形态】进行选股
def screener_node(state: AgentState, llm):
    # --- 1. 获取宏观资金风向 (Sector Flow) ---
    sector_flow_info = ""
    query = state["user_query"]

    # === 🔥 [新增] 形态查询快速通道 ===
    # 当用户明确询问"有什么形态"时，直接返回形态统计，不走选股流程
    pattern_inquiry_keywords = ["有什么形态", "什么形态", "哪些形态", "形态有哪些", "形态统计", "今天形态", "市场形态"]
    default_bearish_patterns = ["空头吞噬", "下降三法", "破位", "均线空头", "夜星"]

    if any(kw in query for kw in pattern_inquiry_keywords):
        try:
            patterns_info = get_available_patterns.invoke({})
            return {
                "messages": [HumanMessage(content=f"【形态统计】\n{patterns_info}")],
                "symbol": state.get("symbol", "")
            }
        except Exception as e:
            print(f"⚠️ 形态统计查询失败: {e}")
            # 失败时继续走正常选股流程

    risk_keywords = [
        "危险", "风险", "不要买", "别买", "卖掉", "要卖", "卖出",
        "避开", "规避", "远离", "小心", "警惕", "注意", "出场",
        "差的", "最差", "垃圾", "烂股", "坑", "雷", "暴雷",
        "分数最低", "评分最低", "最弱", "弱势股",
        "不好", "不行", "不要", "别碰", "跑路", "清仓",
        "下跌", "亏损", "套牢", "割肉", "止损"
    ]
    pattern_keywords = [
        "红三兵", "三红兵", "连阳", "三连阳", "创新高",
        "金针探底", "锤子", "锤子线", "下影线",
        "多头吞噬", "看涨吞噬", "阳包阴",
        "早晨之星", "启明星", "晨星",
        "V型反转", "反转", "假突破", "假跌破",
        "大阳线", "涨停", "放量突破", "突破",
        "三只乌鸦", "黑三兵", "连阴", "三连阴", "下降三法",
        "空头吞噬", "看跌吞噬", "阴包阳",
        "吊人线", "上吊线", "倒锤子", "射击之星", "流星",
        "黄昏之星", "倒V", "见顶",
        "大阴线", "跌停", "放量下跌",
        "十字星", "波动收窄", "震荡", "蓄势", "反击",
        "均线多头", "多头排列", "均线空头", "空头排列",
        "上升通道", "下降通道"
    ]

    industry_keywords = [
        "银行", "证券", "保险", "房地产", "医药", "医疗", "半导体", "芯片",
        "新能源", "光伏", "锂电", "汽车", "白酒", "酿酒", "食品", "饮料",
        "军工", "航空", "船舶", "钢铁", "煤炭", "石油", "化工", "有色",
        "电力", "水泥", "建材", "机械", "电子", "通信", "计算机", "软件", "航天",
        "传媒", "游戏", "教育", "旅游", "酒店", "零售", "电商", "物流",
        "农业", "养殖", "纺织", "服装", "家电", "家具", "建筑", "装饰"
    ]

    is_risk_query = any(kw in query for kw in risk_keywords)

    detected_pattern = None
    for pattern in pattern_keywords:
        if pattern in query:
            detected_pattern = pattern
            break

    # 🔥 如果是危险查询但没有指定形态，自动选择看跌形态
    if is_risk_query and not detected_pattern:
        detected_pattern = random.choice(default_bearish_patterns)

    detected_industry = None
    for industry in industry_keywords:
        if industry in query:
            detected_industry = industry
            break

    # === 🔥 [新增] 判断是否需要进入 ReAct 模式 ===
    # 如果没有匹配到任何快速通道条件，说明可能是概念/主题类查询
    need_react_mode = (not is_risk_query and
                       not detected_pattern and
                       not detected_industry)

    if need_react_mode:
        print(f"🤖 选股进入 ReAct 模式: {query}")

        # ReAct 模式工具集
        react_tools = [
            search_top_stocks,  # 按形态/行业/分数筛选
            search_web,  # 搜索概念股、热点信息
            search_investment_knowledge,  # 查知识库
            get_available_patterns,  # 查形态统计
            tool_get_retail_money_flow,  # 资金流向
        ]

        current_date = datetime.now().strftime("%Y年%m月%d日")

        react_prompt = f"""
            你是一位资深选股专家，擅长根据用户需求寻找相关股票。

            【当前日期】：{current_date}
            【用户需求】：{query}

            【你的工作流程】
            1. **理解需求**：分析用户想要什么类型的股票（概念股？行业股？主题股？）

            2. **信息收集**：
               - 如果是概念/主题类（如"马斯克概念"、"AI概念"、"低空经济"）：
                 → 先用 `search_web` 搜索"xxx概念股有哪些"或"xxx相关A股"，最多使用2次，必须遵守
                 → 获取相关股票名单

            3. **技术验证**：
               - 用 `search_top_stocks` 查询相关股票的技术面评分
               - 用 `tool_get_retail_money_flow` 查看相关板块资金流向

            4. **综合推荐**：
               - 推荐 3-5 只技术面较好的相关股票
               - 说明推荐理由（概念相关性 + 技术面 + 资金面）

            【工具使用说明】
            - `search_top_stocks`: 筛选股票，参数 condition(形态)、industry(行业)、limit(数量)
            - `search_investment_knowledge`: 查询内部知识库
            - `tool_get_retail_money_flow`: 查看行业资金流向
            - `get_available_patterns`: 查看今日市场有哪些K线形态

            【禁止事项】
            - 不要编造股票代码或名称
            - 如果搜索不到相关信息，诚实告知用户
            """

        screener_react_agent = create_react_agent(llm, react_tools, prompt=react_prompt)

        partial_response = ""

        try:
            result = screener_react_agent.invoke(
                {"messages": [HumanMessage(content=query)]},
                {"recursion_limit": 40}
            )

            last_response = result["messages"][-1].content
            partial_response = last_response

            # 提取股票代码
            codes = re.findall(r'[0-9]{6}\.[A-Z]{2}', last_response)
            if not codes:
                codes = re.findall(r'[0-9]{6}', last_response)
            next_symbol = codes[0] if codes else state.get("symbol", "")

            return {
                "messages": [HumanMessage(content=f"【精选股票】\n{last_response}")],
                "symbol": next_symbol
            }

        except GeneratorExit:
            fallback_msg = partial_response if partial_response else f"关于'{query}'的选股分析已完成，请参考上文。"
            return {
                "messages": [HumanMessage(content=f"【精选股票】\n{fallback_msg}")],
                "symbol": state.get("symbol", "")
            }

        except Exception as e:
            print(f"⚠️ screener ReAct 模式错误: {e}")
            # 降级到默认选股
            pass

    # === 快速通道：正常选股流程 ===
    try:
        # 🔥 [修复] 强制使用字典传参，确保 days 被识别为整数
        sector_flow_info = tool_get_retail_money_flow.invoke({"days": 2})
    except Exception as e:
        sector_flow_info = f"暂无行业资金流数据: {e}"

    # --- 2. 获取技术面强势股 (Technical Screener) ---
    raw_stocks = ""
    try:
        invoke_params = {"limit": 15}

        if detected_pattern:
            invoke_params["condition"] = detected_pattern
        else:
            invoke_params["condition"] = "综合评分"

        if detected_industry:
            invoke_params["industry"] = detected_industry

        # 🔥 [核心] 危险模式下，按分数从低到高排序
        if is_risk_query:
            invoke_params["sort_order"] = "asc"
        else:
            invoke_params["sort_order"] = "desc"

        print(f"📊 调用选股工具: {invoke_params}")
        raw_stocks = search_top_stocks.invoke(invoke_params)
    except Exception as e:
        raw_stocks = f"选股工具调用失败: {e}"

    # --- 3. LLM 综合决策 (Intersection Logic) ---
    # 让 AI 找出“资金风口”和“技术强势”的交集

    if is_risk_query:
        screen_prompt = f"""
                你是一位资深选股专家。用户想知道**哪些股票有风险，应该规避或卖出**。

                【数据源 A：市场资金流向】
                {sector_flow_info}

                【数据源 B：风险股票池（技术面较弱）】
                {raw_stocks}

                【用户原始需求】: "{query}"

                【你的任务】
                1. 从【数据源B】中选出 3-5 只最危险的股票，**不要编造**！
                2. 解释为什么这些股票有风险：
                   - 📉 **技术面风险**：出现什么看跌形态
                   - 💸 **资金面风险**：所属板块是否在资金流出
                   - ⚠️ **综合风险等级**：高/中/低
                3. 给出操作建议：
                   - 持有者：是否应该止损/减仓
                   - 观望者：为什么不要买入

                【输出格式】
                ⚠️ **风险股票警示**

                1. **股票名称** (代码) - 风险等级：🔴高
                   - 📉 形态风险：xxx
                   - 💸 资金风险：xxx
                   - 💡 操作建议：xxx
                """
    elif detected_pattern:
        screen_prompt = f"""
                你是一位资深选股专家。用户想找符合【{detected_pattern}】形态的股票。

                【数据源 A：市场资金风向】
                {sector_flow_info}

                【数据源 B：符合"{detected_pattern}"形态的股票】
                {raw_stocks}

                【用户原始需求】: "{query}"

                【你的任务】
                1. 如果【数据源B】中有符合形态的股票，**直接展示这些股票**，不要编造！
                2. 结合【数据源A】的资金流向，判断这些股票所属板块是否有资金支持。
                3. 如果【数据源B】为空，告诉用户"当前市场暂无明显的{detected_pattern}形态股票"。

                【输出格式】
                - 推荐 3-5 只符合条件的股票
                - 每只股票说明：形态特征 + 所属板块资金情况
                - ⚠️ 不要编造股票！
                """
    else:
        screen_prompt = f"""
                你是一位资深选股专家。请结合【资金风向】和【技术形态】为客户精选股票。

                【数据源 A：市场资金风向 (行业)】
                {sector_flow_info}

                【数据源 B：技术面强势股池】
                {raw_stocks}

                【当前用户需求】: "{query}"

                【选股逻辑】
                1. **寻找交集**：观察【数据源A】中资金净流入靠前的板块，然后在【数据源B】中寻找属于这些板块的个股。
                2. **优中选优**：如果数据源B里没有匹配风口的股票，则优先推荐数据源B里分数最高的。
                3. 仔细检查用户需求是否包含特定板块或行业。

                【输出任务】
                1. 推荐 5 只最值得关注的股票。
                2. **推荐理由必须包含两点**：
                   - 🌪️ **风口**：该股所属板块的资金流情况。
                   - 📈 **形态**：该股的技术面特征。
                3. 给出止损建议。
                """

    response = llm.invoke(screen_prompt)

    # 尝试提取第一只股票的代码作为 symbol
    codes = re.findall(r'[0-9]{6}\.[A-Z]{2}', response.content)
    if not codes:
        codes = re.findall(r'[0-9]{6}', response.content)

    next_symbol = codes[0] if codes else state["symbol"]

    return {
        "messages": [HumanMessage(content=f"【精选股票】\n{response.content}")],
        "symbol": next_symbol
    }


# ==========================================
#  🌶️ 7. 毒舌分析师 (Roaster Node) - 创意功能
# ==========================================
def roaster_node(state: AgentState, llm):
    query = state["user_query"]
    symbol = state.get("symbol", "")

    # 1. 给他一些基本工具，让他吐槽时有理有据
    tools = [
        get_market_snapshot,  # 看价格 (跌了才能嘲笑)
        get_stock_valuation,  # 看估值 (贵了才能骂韭菜)
        analyze_kline_pattern,  # 看形态 (破位了才能补刀)
        get_price_statistics,
        tool_get_retail_money_flow,  # 股票行业资金
        get_futures_fund_flow,
        tool_compare_stocks
    ]

    # 2. 注入“毒舌”人设 Prompt
    prompt = f"""
    你现在是**金融界的“脱口秀演员”兼“毒舌评论员”**。
    用户把持仓或关注的股票给你看，你的任务是**无情吐槽、犀利点评**。

    【当前关注】：{symbol if symbol else "用户的这个标的"}
    【用户问题】："{query}"

    【行为准则】：
    1. **人设风格**：
       - 幽默、讽刺、使用网络热梗，比喻要夸张（例如：“这K线走得像心电图停了一样”）。
    3. **数据驱动的吐槽**：
       - 先调用工具看数据。
       - 如果 **PE很高** -> 举例吐槽：“你买的是股票还是梦？这泡沫戳破了能淹死人。”
       - 如果 **下跌趋势** -> 举例吐槽：“这种飞刀你也敢接？手不想要了？”

    【输出要求】：
    - 不要写长篇大论，要短小精悍，字字扎心。
    - 结尾给一个“韭菜指数”评分（0-100，越高越韭）。
    """

    # 3. 创建 Agent
    roaster_agent = create_react_agent(llm, tools, prompt=prompt)

    try:
        result = roaster_agent.invoke({"messages": state["messages"]})
        last_response = result["messages"][-1].content

        return {
            "messages": [HumanMessage(content=f"【毒舌点评】\n{last_response}")]
        }
    except Exception as e:
        return {
            "messages": [HumanMessage(content=f"【毒舌点评】\n槽点太多，我都无语了... (系统错误: {e})")]
        }



# ⚪ 6. 聊天/总结节点 (Finalizer)
# 🔥 [升级]：单信源模式(审核不重写) vs 多信源模式(统稿)
def finalizer_node(state: AgentState, llm):
    # 1. 收集所有 AI 产生的实质性报告 (过滤掉用户的话和空的)
    # 也就是 Analyst, Monitor, Strategist 等人的发言
    all_messages = state["messages"]
    today_str = datetime.now().strftime("%Y年%m月%d日 %H:%M")

    # 过滤出 AI 的回复 (HumanMessage 在这里是 Worker 的输出载体)
    # 且排除掉可能是空的或者无关的
    worker_msgs = [m for m in all_messages if isinstance(m, HumanMessage) and len(m.content) > 10]

    # 获取知识库内容

    # 拼接到一起用于输入
    context_text = "\n".join([f"{m.content}" for m in worker_msgs])
    if "【精选股票】" in context_text:
        # 直接返回 PASS，不做任何 LLM 思考，毫秒级响应
        return {
            "messages": [HumanMessage(content="PASS")]
        }
    if "【毒舌点评】" in context_text:
        # 🔥 roaster 的毒舌吐槽，直接返回原文，不要改写！
        print("🔥 检测到毒舌点评，跳过 finalizer 整合")

        # 🔥 [修复] 只提取【毒舌点评】部分，不要包含用户原始提问
        roaster_content = ""
        for msg in worker_msgs:
            if "【毒舌点评】" in msg.content:
                roaster_content = msg.content
                break

        # 提取图表路径（如果有）
        chart_img = state.get("chart_img", "")
        if not chart_img:
            chart_match = re.search(r'IMAGE_CREATED:(chart_[a-zA-Z0-9_]+\.json)', context_text)
            if chart_match:
                chart_img = chart_match.group(1)
        return {
            "messages": [HumanMessage(content=roaster_content if roaster_content else context_text)],
            "chart_img": chart_img
        }
    # === 判断逻辑：单兵还是团战？ ===
    # 如果只有 1 个工种发言（或者没有发言），且不是王牌分析师（王牌本来就是总结好的）
    symbol = state.get("symbol", "")
    symbol_name = state.get("symbol_name", "")
    mem_context = state.get("memory_context", "")
    macro_view = state.get("macro_view", "无宏观分析")
    trend = state.get("trend_signal", "")  # 例如 "看涨"
    key_levels = state.get("key_levels", "")  # 例如 "压力3000"
    is_single_source = len(worker_msgs) <= 1
    has_chart = "chart_" in context_text or "![" in context_text
    user_query = state.get("user_query", "")
    complex_keywords = ["画", "图", "对比", "分析", "价差", "相关性", "走势"]
    is_complex_task = any(kw in user_query for kw in complex_keywords)

    display_name = f"{symbol_name}({symbol})" if symbol_name else symbol

    # 获取当前最后一次执行的计划（用于判断是不是王牌）
    # (由于 state plan 被 pop 了，我们用简单的长度判断通常够用，或者看 context)


    if is_single_source and not has_chart and not is_complex_task:
        # === 模式 A：质检员 (Audit Mode) ===
        # 目标：保留原汁原味的排版，只查错
        audit_prompt = f"""
        你是一位交易风控官。团队提交了一份分析报告（如下）。

        【待审核报告】：
        {context_text}

        【任务】：
        1. 检查报告是否存在**致命的常识性错误**（如把标的搞错、逻辑完全相反）。
        2. **如果报告无误**：请直接输出四个字 "DIRECT_PASS" (不要输出其他符号)。这意味着直接采用原报告，保留其完美的 Markdown 排版。
        3. **如果有致命错误**：请修改错误后，重写一份正确的报告。
        4. 如果发生数据缺失或语法错误，不要把错误写出来。
        """
        response = llm.invoke(audit_prompt)

        # 如果 LLM 觉得没问题，返回特定标记
        if "DIRECT_PASS" in response.content:
            return {
                "messages": [HumanMessage(content=context_text)]
            }
        else:
            # 如果有错被重写了，就返回重写的内容
            return {
                "messages": [HumanMessage(content=f"【风控修正】\n{response.content}")]
            }

    else:
        # === 模式 B：总编辑 (Editor Mode) ===
        # 目标：多源信息整合，但要根据用户问题类型调整输出风格

        # 🔥 [新增] 获取用户原始问题，判断问题类型
        user_query = state.get("user_query", "")

        # 判断是否为"纯数据查询"类问题
        data_query_keywords = ["持仓", "排名", "资金", "流入", "流出", "多少", "哪些", "哪个", "前几", "前3", "前三",
                               "前5", "前五", "top", "龙虎榜", "增仓", "减仓", "净持仓", "最多", "最大"]
        is_data_query = any(kw in user_query for kw in data_query_keywords)

        # 判断是否为"综合分析"类问题
        analysis_keywords = ["分析", "怎么看", "怎么做", "策略", "建议", "操作", "行情", "走势", "如何","趋势", "全面"]
        is_analysis_query = any(kw in user_query for kw in analysis_keywords)

        # 🎯 根据问题类型选择不同的 Prompt
        if is_data_query and not is_analysis_query:
            # === 数据查询模式：简洁直接 ===
            cio_prompt = f"""
                你是一位数据检查师。
                【当前日期】：{today_str}
                【用户问题】：{user_query}
                【分析标的】: {display_name} 

                【团队收集的数据】：
                {context_text}

                【输出要求】：
                1. **直接回答用户的问题**，不要跑题！用户问持仓就答持仓，问排名就答排名。
                2. **突出数据本身**：用表格或列表清晰展示数据。
                3. **简短点评**：可以加 1-2 句对数据的解读（如"XX 在大幅增仓，可能看多"），但不要扯到技术面K线分析。
                4. 数据不要编造和修改。
                5. 不要写成投资报告，文字要简洁有力。
                6. 如果发生数据缺失或语法错误，不要把错误写出来。
                7. 数据是每天下午5点后更新。


                【格式示例】：
                📊 **东证期货持仓前3大品种** ({today_str})

                | 排名 | 品种 | 净持仓 | 方向 |
                |-----|------|-------|-----|
                | 1 | 螺纹钢 | -33,272 | 空头 |
                | ... | ... | ... | ... |

                💡 **简评**：东证在黑色系整体偏空，螺纹空单最重...
                """
        else:
            # === 综合分析模式：完整报告 ===
            enhanced_query = f"{state['user_query']} {symbol} {trend}"
            kb_context = "暂无内部知识库匹配内容"
            try:
                # 使用增强后的 query 去搜
                kb_context = search_investment_knowledge.invoke(enhanced_query)
            except Exception as e:
                print(f"CIO知识库检索失败: {e}")
            cio_prompt = f"""
                你是这家交易公司的**首席投资官 (CIO)**。
                你的团队（分析师、策略员、监控员等）提交了多份分散的报告。
                【当前日期】：{today_str}
                【用户问题】：{user_query}
                【分析标的】: {display_name} 

                【团队报告池，必须优先采用！】：
                {context_text}
                
                【客户对话历史记忆】{mem_context}

                【📚 内部知识库 (基于"{enhanced_query}"检索)】：
                {kb_context}

                【任务】：
                请将上述零散报告整合成一份《深度投资决策书》，要求**排版精美、逻辑结构化**。
                1. 技术面分析以K线为主，均线为辅。
                2. 知识要参考{kb_context}，但要根据当下市场情况，自己理解后输出。
                3. 如果记忆{mem_context}有客户的持仓或偏好，在报告里可以针对性的写。               
                4. 所有价格数据（当前价、支撑位、压力位、均线值），必须使用来自【团队报告池】！
                
                【注意事项】：
                1. 中国的股票没有期权，客户问股票时，不要给期权策略，除非是用ETF期权来对冲股票。
                2. 商品期货都有期权！
                3. 如果某品种有利好消息但却下跌，要提醒利多不涨，可能反转，而如果有坏消息但却不跌，要提醒利空不跌，可能阶段底部到了。
                4. 价格数据是每天中午11点半和下午5点后更新。
                5. 2026年春节长假是2月16日才开始！
                
                【必须遵守的数据准则】
                1. **绝对禁止捏造数据**。如果没有数据就回答不知道。
                2. 在分析宏观前，要引用【宏观分析报告】的结论。
                
                【已有的宏观分析报告】
                {macro_view}
                
                【数据采信最高原则】
                当不同分析师提供的数据冲突时，**必须**按以下优先级采信：
                1. **宏观数据 (美元/美债/利率)**：
                   - ✅ **唯一权威来源**：【宏观策略 (Macro Analyst)】。

                

                【排版强制要求】：
                1. **头部信息**：使用引用块 `>` 展示签发人、日期和心情。
                2. **核心结论**：必须在最前面，使用 `### 🎯 核心结论` 标题，并用列表展示 3 个关键点。
                3. **分节标题**：使用 `###` 标题，并在标题前加上 Emoji (如 📈, 💰, ⚖️)。
                4. **重要警示**：如果涉及风险，使用 `> ⚠️ **风险提示**：...` 的格式高亮。
                5. **数据表格**：如果涉及多组数据对比（如支撑压力位、资金流），尽量整理成 Markdown 表格。
                6. **语气**：专业、自信、干练。不要堆砌废话。
                7. 报告里不要参考MACD指标！
                8. 引用知识库内容时，不要把文章标题写出来。

                【报告结构模板】：
                > 📅 日期：{today_str}
                > ✍️ 签发：交易汇首席

                ### 🎯 核心结论 (Executive Summary)
                * ...
                * ...

                ### 📈 市场深度解析
                (融合技术面和资金面...)

                ### ⚖️ 交易策略部署
                (具体的期权或现货操作建议...)

                ### 🛡️ 风控与对冲
                (止损位、风险提示...)
                """

        final_verdict = llm.invoke(cio_prompt)

        # 🔥 [新增] 从原始报告中提取图表路径（因为 finalizer 是整理者，图表在前面节点生成）
        chart_img = state.get("chart_img", "")
        if state.get("macro_chart"):  # 如果有宏观图，优先用宏观图
            chart_img = state.get("macro_chart")

        # 如果 state 中没有，尝试从报告内容中提取
        if not chart_img:
            chart_match = re.search(r'IMAGE_CREATED:(chart_[a-zA-Z0-9_]+\.json)', context_text)
            if chart_match:
                chart_img = chart_match.group(1)
                print(f"📊 finalizer 从报告中提取到图表: {chart_img}")

        return {
            "messages": [HumanMessage(content=f"【最终决策】\n{final_verdict.content}")],
            "chart_img": chart_img  # 🔥 返回图表路径
        }


# ==========================================
# 4. 构建图 (The Graph)
# ==========================================

def build_trading_graph(fast_llm, mid_llm, smart_llm):
    """
    构建并编译 LangGraph
    """
    workflow = StateGraph(AgentState)

    # 1. 注册节点
    # 主管 -> 用 Turbo (快)
    workflow.add_node("supervisor", lambda state: supervisor_node(state, fast_llm))
    # 分析师 -> 用 Plus (均衡)
    workflow.add_node("analyst", lambda state: analyst_node(state, mid_llm))
    # 策略员 -> 用 Max (聪明)
    workflow.add_node("strategist", lambda state: strategist_node(state, smart_llm))
    # 王牌 -> 用 Max (聪明)
    workflow.add_node("generalist", lambda state: generalist_node(state, smart_llm))
    # CIO -> 用 Max (聪明)
    workflow.add_node("finalizer", lambda state: finalizer_node(state, mid_llm))
    # 其他工具人 (不需要 LLM，或者随便给一个)
    workflow.add_node("monitor", lambda state: monitor_node(state, mid_llm))
    workflow.add_node("researcher", lambda state: researcher_node(state, mid_llm))
    workflow.add_node("chatter", lambda state: chatter_node(state, mid_llm))
    workflow.add_node("screener", lambda state: screener_node(state, mid_llm))
    workflow.add_node("roaster", lambda state: roaster_node(state, mid_llm))
    workflow.add_node("macro_analyst", lambda state: macro_analyst_node(state, mid_llm))

    # 2. 设置入口
    workflow.set_entry_point("supervisor")

    # 3. 定义动态路由逻辑 (The Router)
    def route_next_step(state: AgentState):
        plan = state.get("plan", [])
        if not plan:
            return "finalizer"

        # 取出计划中的下一个，并从计划中移除
        next_node = plan[0]
        # 更新 plan (这一步其实比较 trick，LangGraph 的 state 是 immutable 的更新流)
        # 我们需要在节点内部更新 plan，或者在这里只做路由
        # 简化做法：我们让每个节点跑完后，自己去检查 plan 并路由
        return next_node

    # ⚠️ LangGraph 的标准做法是：每个节点跑完，返回更新后的 State
    # 为了实现"流水线"，我们需要一个中间人或者让每个节点都指向 "scheduler"
    # 这里我们采用 "Scheduler" 模式，即 Supervisor -> Scheduler -> Node -> Scheduler...

    # 但为了简单，我们采用 add_conditional_edges 从 supervisor 直接分发是做不到串行的
    # 我们需要引入一个名为 "orchestrator" 的隐藏节点，或者修改 supervisor 逻辑

    # --- 修正后的串行逻辑 ---
    # 我们把 plan 的执行逻辑放在 edge 里

    def executor_router(state: AgentState):
        plan = state.get("plan", [])
        if not plan:
            return "end"  # 这里的 end 指向 finalizer
        return plan[0]  # 返回 list 中的第一个元素作为节点名

    # 定义每个 Worker 执行完后，都要回到 Router (或者在这里就是直接去下一个)
    # 为了实现 state['plan'].pop(0)，我们需要在每个 worker 里处理 plan
    # 这会很繁琐。

    # 🔥 最佳实践：使用一个 "Manager" 节点来循环
    workflow.add_node("manager", lambda state: {"current_step": "managing"})

    workflow.add_edge("supervisor", "manager")

    def manager_router(state: AgentState):
        plan = state.get("plan", [])
        if not plan:
            return "finalizer"

        # 获取下一个任务
        next_task = plan[0]

        # 这里的关键是：我们需要在路由的同时，把这个任务从 plan 里删掉
        # 但 router 函数不能修改 state。
        # 所以必须在 worker 节点里修改 plan，或者有一个专门的 step 节点。

        return next_task

    # 4. 连接 Edge
    # Manager 决定去哪
    workflow.add_conditional_edges(
        "manager",
        manager_router,
        {
            "analyst": "analyst",
            "monitor": "monitor",
            "strategist": "strategist",
            "researcher": "researcher",
            "generalist": "generalist",
            "chatter": "chatter",
            "roaster": "roaster",
            "screener": "screener",
            "macro_analyst": "macro_analyst",
            "finalizer": "finalizer"
        }
    )

    # 5. Worker 回流逻辑
    # 每个 Worker 跑完，必须把自己的名字从 plan 里通过代码删掉 (pop)，然后回到 manager
    def worker_complete(state):
        new_plan = state["plan"][1:]  # 移除已完成的第一个
        return {"plan": new_plan}

    # 我们需要包装一下 worker 节点，让它们能更新 plan
    # 但上面定义 worker 时已经写死了。
    # 简便起见，我们在 add_edge 时指定：
    # Analyst -> Manager (但在进入 Manager 前，State 已经被 Analyst 更新了吗？是的)
    # 问题是 Analyst 代码里没有 pop plan。

    # 解决方案：修改所有 Worker 节点，或者增加一个通用的后处理节点。
    # 我们修改上面的 Worker 定义太麻烦，不如在 edge 逻辑里做？不支持。

    # 👉 最终方案：让 Manager 节点负责 POP Plan
    # 修改 Manager Node 逻辑：
    workflow.add_node("manager_pop", lambda state: {"plan": state["plan"][1:]})

    # 流程变成：Manager(路由) -> Worker -> Manager_Pop(删除任务) -> Manager(路由)

    # 重新定义 Edge:
    for node_name in ["analyst", "monitor", "strategist", "researcher", "generalist","screener","roaster", "macro_analyst"]:
        workflow.add_edge(node_name, "manager_pop")

    workflow.add_edge("chatter", END)
    workflow.add_edge("manager_pop", "manager")

    workflow.add_edge("finalizer", END)

    return workflow.compile()