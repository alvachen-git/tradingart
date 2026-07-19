from typing import TypedDict, Annotated, List, Union, Literal, Dict, Any, Mapping
from datetime import datetime
from contextlib import contextmanager
import random
import operator
import re
import os
import glob
import json
import signal
import threading
import time
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage, AIMessage
from langgraph.prebuilt import create_react_agent
from langchain_core.prompts import ChatPromptTemplate
from macro_tools import (
    get_macro_indicator,
    get_macro_overview,
    analyze_yield_curve,
    get_macro_health_snapshot,
    get_us_debt_gdp_snapshot,
)
from pydantic import BaseModel, Field
from langgraph.graph import StateGraph, END
from langgraph.types import Send

# --- 引入你的工具库 ---
# 请确保这些文件名和你项目里的一致
from chart_annotation_tools import draw_pattern_annotation_chart, draw_forecast_chart
from kline_tools import analyze_kline_pattern
from screener_tool import search_top_stocks, get_available_patterns, search_us_stocks_by_technical_setup
from us_stock_multifactor_screener import (
    compare_semantic_plan_to_rules,
    compile_screen_plan_with_llm,
    is_us_multifactor_screen_query,
    screen_us_stocks,
)
from news_tools import get_financial_news
from news_rag_interpreter import interpret_market_news_tool
from fund_flow_tools import tool_get_retail_money_flow
from polymarket_tool import reset_polymarket_task_guard, tool_get_polymarket_sentiment
from plot_tools import draw_chart_tool,draw_macro_compare_chart
from futures_fund_flow_tools import get_futures_fund_flow, get_futures_fund_ranking, get_futures_margin_profile
from futures_structure_tools import (
    get_futures_basis_profile,
    get_futures_inventory_receipt_profile,
    get_futures_delivery_tospot_profile,
)
from volume_oi_tools import get_volume_oi, get_futures_oi_ranking, get_option_oi_ranking, get_option_volume_abnormal, get_option_oi_abnormal, analyze_etf_option_sentiment, get_etf_option_strikes
from market_tools import get_market_snapshot, get_price_statistics,tool_query_specific_option,get_historical_price,get_recent_price_series,get_trending_hotspots,get_today_hotlist,analyze_keyword_trend,get_finance_related_trends,search_hotlist_history
from data_engine import (
    BROKER_SIGNAL_NEGATIVE,
    BROKER_SIGNAL_POSITIVE,
    CN_TO_CODE,
    PRODUCT_MAP,
    _build_futures_broker_group_position_moves,
    _build_futures_broker_indicator_profile,
    _build_futures_broker_position_signal,
    check_option_expiry_status,
    get_commodity_iv_info,
    scan_iv_change_ranking,
    scan_volatility_divergence,
    get_futures_broker_indicator_profile,
    get_futures_broker_group_position_moves,
    get_futures_broker_position_signal,
    get_latest_data_date,
    get_stock_valuation,
    parse_account_total_capital,
    normalize_account_total_capital,
    search_broker_holdings_on_date,
    tool_analyze_broker_positions,
    tool_analyze_position_change,
    tool_compare_stocks,
)
from search_tools import search_web, is_search_answer_acceptable
from market_correlation import tool_stock_hedging_analysis, tool_futures_correlation_check,tool_stock_correlation_check
from beta_tool import calculate_hedging_beta
from knowledge_tools import search_investment_knowledge
from stock_volume_tools import query_stock_volume, search_volume_anomalies
from backtest_tools import run_option_strategy_backtest
from option_delta_tools import (
    compute_option_delta_cash,
    fetch_underlying_spot_map,
    DELTA_EXECUTION_COVERAGE_THRESHOLD,
)
from us_options_ai_tools import get_us_option_market_profile, get_us_option_strategy_candidates
from cn_margin_ai_tools import get_cn_margin_market_signal
from portfolio_tools import (
    get_user_portfolio_summary,
    get_user_portfolio_details,
    analyze_user_trading_style,
    check_portfolio_risks
)
from chat_routing import (
    is_market_data_query,
    is_pure_option_data_query,
    is_us_option_market_profile_query,
    is_volatility_divergence_query,
    is_volatility_market_view_query,
)
from simple_chat_runtime import (
    build_simple_runtime_context,
    format_simple_runtime_context,
    maybe_answer_simple_runtime_question,
)
from agent_prompt_policy import (
    TASK_TYPE_LINK_ARTICLE_STOCK_MAPPING,
    TASK_TYPE_NORMAL,
    TASK_TYPE_OPTION_STRATEGY_NEEDS_SUBJECT,
    TASK_TYPE_OPTION_STRATEGY_WITH_SUBJECT,
    TASK_TYPE_FUTURES_BROKER_SIGNAL,
    TASK_TYPE_SINGLE_STOCK_ANALYSIS,
    TASK_TYPE_STOCK_SELECTION,
    TASK_TYPE_TECHNICAL_CONCEPT,
    build_data_policy_context,
    build_profile_policy,
    build_subject_policy,
    classify_analysis_task_type,
    extract_us_option_underlying_symbol,
    has_explicit_option_underlying,
    is_generic_option_strategy_question,
    is_option_strategy_question,
)
from followup_task_policy import apply_followup_supervisor_policy
from option_scenario_policy import (
    build_finalizer_scenario_context,
    build_strategist_scenario_context,
    detect_option_hypothetical_scenario,
)
from option_strategy_policy import build_option_strategy_policy
from chat_context_layers import append_chat_trace_event, has_agent_context, render_agent_context
from agent_expert_router import build_route_decision


def _disable_langsmith_tracing_by_default() -> None:
    if str(os.getenv("ENABLE_LANGSMITH_TRACING", "")).strip().lower() in {"1", "true", "yes", "on"}:
        return
    os.environ["LANGCHAIN_TRACING_V2"] = "false"
    os.environ["LANGSMITH_TRACING"] = "false"
    os.environ["LANGCHAIN_CALLBACKS_BACKGROUND"] = "false"


_disable_langsmith_tracing_by_default()

# ==========================================
# 1. 定义共享记忆 (The State)
# ==========================================
class AgentState(TypedDict):
    # --- 基础信息 ---
    messages: Annotated[List[BaseMessage], operator.add]
    user_query: str
    completed_steps: Annotated[List[str], operator.add]
    agent_reports: Annotated[Dict[str, str], operator.or_]
    route_decision: Dict[str, Any]
    route_confidence: float
    route_mode: str

    # --- 调度控制 ---
    plan: List[str]  # 任务队列，如 ["analyst", "monitor"]
    execution_batches: List[List[str]]  # 并行/串行混合执行批次
    current_batch_index: int  # 当前执行到的批次下标
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
    profile_context: str
    conversation_memory_query: bool
    conversation_memory_label: str
    is_followup: bool
    recent_context: str
    conversation_id: str
    focus_entity: str
    focus_topic: str
    focus_aspect: str
    focus_mode_hint: str
    followup_goal: str
    followup_action_context: str
    followup_task_policy: Dict[str, Any]
    followup_route_context: str
    context_layers: List[Dict[str, Any]]
    context_layer_summary: List[Dict[str, Any]]
    task_id: str
    delivery_mode: str
    quick_answer_scenario: str
    quick_answer_target: str
    quick_answer_direction: str
    freshness_required: bool
    freshness_quick_status: str
    freshness_query_target: str
    link_context: Dict[str, Any]

    news_summary: str  # 情报员填入：新闻摘要 (CPI/非农/美联储)
    macro_view: str  # 宏观分析师填入：宏观定调 (宽松/紧缩)
    macro_chart: str  # 宏观分析师填入：生成的宏观对比图路径

    # --- 持仓相关 (Portfolio Analyst) ---
    user_id: str  # 用户ID
    has_portfolio: bool  # 是否有持仓数据
    portfolio_summary: str  # 持仓摘要
    portfolio_risks: str  # 风险提示
    trading_style: str  # 交易风格
    portfolio_top_corr_index: str  # 最相关指数名称
    portfolio_top_corr_value: str  # 最相关指数的相关系数
    option_delta_cash_report: str  # ETF期权Delta Cash预计算报告
    option_delta_cash_meta: Dict[str, Any]  # ETF期权Delta Cash结构化结果
    option_delta_cash_gap_note: str  # Delta无法计算时的数据缺口说明
    account_total_capital: float  # 用户账户总资金（元），用于账户口径Delta计算
    vision_position_payload: Dict[str, Any]  # 上传截图识别后的结构化持仓（仅会话内）
    vision_position_domain: str  # stock|option|mixed|unknown
    canonical_option_legs_block: str  # 识别持仓锁定表（防止方向被改写）
    option_direction_conflict_count: int  # 报告方向术语/事实冲突次数
    authoritative_underlying_quotes: Dict[str, Any]  # 多标的权威现价（代码/价格/日期/来源）
    authoritative_quote_block: str  # 权威现价固定展示块
    price_conflict_count: int  # 报告现价冲突次数（与权威行情相比）
    option_delta_cash_per_underlying: Dict[str, Any]  # DeltaCash 分标的结果
    option_delta_cash_portfolio_summary: Dict[str, Any]  # DeltaCash 组合汇总
    option_rebalance_priority_queue: List[Dict[str, Any]]  # 调仓优先队列（P1/P2/P3）
    option_delta_displayable: bool  # Delta 是否可展示（部分可算也为True）
    option_delta_execution_ready: bool  # Delta 是否可给金额级动作
    option_delta_coverage_ratio: float  # Delta 覆盖率


AUTHORITATIVE_PRICE_CONFLICT_THRESHOLD = 0.01
GENERALIST_SMART_KEYWORDS = (
    "回测",
    "胜率",
    "盈亏比",
    "最大回撤",
    "相关性",
    "对冲",
    "beta",
    "收益率曲线",
    "宏观",
    "美联储",
    "通胀",
    "cpi",
    "非农",
    "利率",
    "美元",
    "价差图",
    "压力测试",
    "情景分析",
)

PARALLEL_BATCH_AGENTS = {"analyst", "monitor", "researcher", "macro_analyst"}
SERIAL_BATCH_AGENTS = {"strategist", "generalist", "portfolio_analyst", "chatter", "screener", "roaster"}


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


MARGIN_QUERY_KEYWORDS = [
    "保证金", "合约乘数", "乘数", "一手多少钱", "每手多少钱", "资金占用", "开仓占用", "保证金率",
]
CN_MARGIN_QUERY_KEYWORDS = [
    "融资余额", "融资买入", "融资净买入", "融资净增", "融资净减", "融资杠杆", "融资动能",
    "融资占比", "融资资金", "融资融券", "两融", "杠杆资金",
]
CN_MARGIN_CONTEXT_KEYWORDS = [
    "余额", "买入", "净买入", "净增", "净减", "杠杆", "动能", "连续增加", "连续下降",
    "创新高", "新高", "占比", "比例", "撤退", "降温", "升温", "过热", "沪深", "A股", "a股", "大盘",
]
CN_MARGIN_EXCLUDED_KEYWORDS = [
    "再融资", "融资轮", "股权融资", "债务融资", "融资计划", "融资方案", "融资用途", "融资租赁",
    "融资成本", "定增", "增发", "发债", "配股",
]
CN_MARGIN_MARKET_SUBJECT_KEYWORDS = [
    "A股", "a股", "大盘", "沪深", "上证指数", "深证成指", "沪深300", "中证500", "中证1000",
    "创业板指", "科创50", "上证50", "300ETF", "500ETF", "1000ETF", "创业板ETF", "科创50ETF",
    "上证50ETF", "ETF期权", "etf期权", "510050", "510300", "510500", "159915", "588000",
    "588080", "159845", "512100",
]
CN_MARGIN_MARKET_INTENT_KEYWORDS = [
    "行情", "走势", "怎么看", "分析", "风险", "市场环境", "资金面", "风险偏好", "过热", "多空",
    "涨跌", "策略", "建议", "怎么做", "适合", "买方", "卖方", "配置",
]
CN_MARGIN_ANALYSIS_KEYWORDS = [
    "怎么看", "分析", "影响", "为什么", "判断", "说明", "风险", "过热", "撤退", "行情", "走势",
    "适合", "策略", "建议", "怎么做", "利好", "利空",
]
STRATEGY_QUERY_KEYWORDS = [
    "策略", "建议", "怎么做", "怎么操作", "开仓", "平仓", "做多", "做空", "对冲", "仓位",
]
STOCK_SELECTION_QUERY_KEYWORDS = [
    "选股", "推荐股票", "精选股票", "股票池", "筛选", "帮我选", "选几只", "哪些股票", "哪只股票",
    "买什么股", "有什么好股票", "找股票", "挖股票", "龙头股", "概念股有哪些", "帮我找",
    "帮我筛", "找几只", "找一下", "候选股",
]
OPTION_QUERY_KEYWORDS = [
    "期权", "认购", "认沽", "行权价", "波动率", "iv", "delta", "gamma", "vega", "theta",
    "升波", "降波", "牛市价差", "熊市价差", "跨式", "宽跨", "勒式",
]
OPTION_ACTION_QUERY_KEYWORDS = [
    "策略", "建议", "怎么做", "怎么调", "如何调", "如何做", "怎么操作", "调仓", "仓位", "对冲", "持仓",
]
RESEARCH_ROUTE_KEYWORDS = [
    "基本面", "财报", "公告", "近期动态", "最近动态", "公司动态", "消息面", "新闻", "资讯",
    "消息", "最新消息",
    "业绩", "一季报", "半年报", "中报", "三季报", "年报", "业绩快报", "业绩预告",
    "利好", "利空", "催化", "为什么涨", "为什么跌", "为什么大涨", "为什么大跌", "影响什么",
]
MACRO_POLICY_KEYWORDS = [
    "美联储", "fed", "fomc", "加息", "降息", "利率", "实际利率", "美元", "美债",
]
MACRO_POLICY_ASSET_KEYWORDS = [
    "黄金", "白银", "金银", "贵金属", "铜", "原油", "股市", "纳指", "美股", "a股", "债券", "美债",
]
MACRO_POLICY_IMPACT_KEYWORDS = [
    "影响", "利好", "利空", "传导", "会怎样", "怎么样", "怎么看", "压制", "支撑",
]
MACRO_POLICY_FRESH_RESEARCH_KEYWORDS = [
    "最新", "最近", "新闻", "消息", "刚刚", "今天", "昨晚", "昨夜", "会议", "纪要", "概率", "预期", "数据",
]
TECHNICAL_ROUTE_KEYWORDS = [
    "技术面", "技术分析", "K线", "k线", "均线", "走势", "趋势", "支撑", "压力", "阻力",
    "突破", "破位", "形态",
]
UNAUTHORIZED_TECHNICAL_INDICATOR_KEYWORDS = [
    "RSI", "MACD", "KDJ", "BOLL", "布林", "量能突破", "放量突破",
]
EXPLICIT_STOCK_PORTFOLIO_COUPLING_KEYWORDS = [
    "结合我的股票持仓", "结合我股票持仓", "结合我的持仓", "结合我持仓", "基于我的股票持仓", "基于我持仓",
    "根据我的股票持仓", "根据我持仓", "按我持仓", "按我的股票组合", "结合我的组合", "对冲我的股票持仓",
]


def _contains_any(text: str, keywords: list) -> bool:
    text_value = str(text or "")
    return any(k in text_value for k in keywords)


def _get_agent_recursion_limit(agent_name: str, default: int) -> int:
    env_name = f"AGENT_RECURSION_{str(agent_name or '').strip().upper()}"
    try:
        value = int(float(str(os.getenv(env_name, "")).strip()))
    except Exception:
        value = int(default)
    return max(2, value)


def _dedupe_plan(plan: List[str]) -> List[str]:
    deduped_plan: List[str] = []
    for step in plan:
        if step and step not in deduped_plan:
            deduped_plan.append(step)
    return deduped_plan


def _enforce_research_analyst_routing(query: str, plan: List[str]) -> List[str]:
    text = str(query or "")
    current_plan = list(plan or [])
    has_research_need = _contains_any(text, RESEARCH_ROUTE_KEYWORDS)
    has_technical_need = _contains_any(text, TECHNICAL_ROUTE_KEYWORDS)
    has_option_or_strategy_need = _contains_any(text, OPTION_QUERY_KEYWORDS) or _contains_any(text, STRATEGY_QUERY_KEYWORDS)

    if not has_research_need and not has_technical_need:
        return current_plan

    if has_research_need and not has_technical_need and not has_option_or_strategy_need and not _wants_chart(text):
        return ["researcher"]

    if has_technical_need and not has_research_need and not has_option_or_strategy_need and not _wants_chart(text):
        return ["analyst"]

    enforced: List[str] = []
    if has_technical_need:
        enforced.append("analyst")
    if has_research_need:
        enforced.append("researcher")
    enforced.extend(current_plan)
    return _dedupe_plan(enforced)


def _enforce_volatility_market_view_routing(query: str, plan: List[str]) -> List[str]:
    """
    升波/降波是窄口径行情判断：先查行情/IV数据，直接给波动率方向结论。
    默认不派 analyst，避免把“升降波”问法扩写成完整K线技术分析。
    """
    if not is_volatility_market_view_query(query):
        return list(plan or [])

    wants_strategy = _contains_any(
        str(query or ""),
        STRATEGY_QUERY_KEYWORDS + ["买购", "买认购", "买沽", "买认沽", "卖购", "卖认购", "卖沽", "卖认沽", "期权策略"],
    )
    enforced = ["monitor"]
    if wants_strategy:
        task_type = classify_analysis_task_type(query).task_type
        if task_type == TASK_TYPE_OPTION_STRATEGY_WITH_SUBJECT:
            enforced = ["analyst", "monitor", "strategist"]
        elif task_type == TASK_TYPE_OPTION_STRATEGY_NEEDS_SUBJECT:
            enforced = ["chatter"]
        else:
            enforced.append("strategist")
    return _dedupe_plan(enforced)


def _enforce_volatility_divergence_routing(query: str, plan: List[str]) -> List[str]:
    """
    波动率背离是价格/IV数据扫描任务：先由 monitor 用确定性工具计算。
    只有用户追问原因/消息或策略时，才串联 researcher/strategist。
    """
    if not is_volatility_divergence_query(query):
        return list(plan or [])

    text = str(query or "")
    wants_research = _contains_any(
        text,
        RESEARCH_ROUTE_KEYWORDS + ["为什么", "为何", "原因", "消息", "新闻", "事件", "背后"],
    )
    wants_strategy = _contains_any(
        text,
        STRATEGY_QUERY_KEYWORDS + ["买购", "买认购", "买沽", "买认沽", "卖购", "卖认购", "卖沽", "卖认沽", "期权策略"],
    )
    enforced = ["monitor"]
    task_type = classify_analysis_task_type(query).task_type
    if wants_strategy and task_type == TASK_TYPE_OPTION_STRATEGY_WITH_SUBJECT:
        enforced.insert(0, "analyst")
    if wants_research:
        enforced.append("researcher")
    if wants_strategy:
        enforced.append("strategist")
    return _dedupe_plan(enforced)


def _enforce_hybrid_background_routing(
    query: str,
    plan: List[str],
    *,
    delivery_mode: str = "",
    quick_answer_scenario: str = "",
) -> List[str]:
    current_plan = list(plan or [])
    if str(delivery_mode or "").strip() != "hybrid":
        return current_plan

    scenario = str(quick_answer_scenario or "").strip()
    has_technical_need = _contains_any(str(query or ""), TECHNICAL_ROUTE_KEYWORDS)
    if scenario == "technical" or has_technical_need:
        return ["analyst"]

    if scenario == "market_move" and "researcher" in current_plan:
        return ["analyst"]
    return current_plan


_HYBRID_BACKGROUND_SUBJECT_ALIASES: tuple[tuple[str, str, str], ...] = (
    ("创业板ETF", "159915.SZ", "创业板ETF"),
    ("创业板指数", "159915.SZ", "创业板ETF"),
    ("创业板指", "159915.SZ", "创业板ETF"),
    ("创业板", "159915.SZ", "创业板ETF"),
    ("科创50ETF", "588000.SH", "科创50ETF"),
    ("科创板50", "588000.SH", "科创50ETF"),
    ("科创50", "588000.SH", "科创50ETF"),
    ("沪深300ETF", "510300.SH", "300ETF"),
    ("300ETF", "510300.SH", "300ETF"),
    ("沪深300", "510300.SH", "300ETF"),
    ("中证500ETF", "510500.SH", "500ETF"),
    ("500ETF", "510500.SH", "500ETF"),
    ("中证500", "510500.SH", "500ETF"),
    ("上证50ETF", "510050.SH", "50ETF"),
    ("50ETF", "510050.SH", "50ETF"),
    ("上证50", "510050.SH", "50ETF"),
)


def _resolve_hybrid_background_subject_lock(
    query: str,
    *,
    delivery_mode: str = "",
    quick_answer_target: str = "",
    focus_entity: str = "",
) -> tuple[str, str]:
    if str(delivery_mode or "").strip() != "hybrid":
        return "", ""

    candidates = [
        str(quick_answer_target or "").strip(),
        str(focus_entity or "").strip(),
        str(query or "").strip(),
    ]
    for text in candidates:
        if not text:
            continue
        for alias, symbol, name in _HYBRID_BACKGROUND_SUBJECT_ALIASES:
            if alias in text:
                return symbol, name

    explicit_code = re.search(r"(?<!\d)(\d{6})(?:\.(SH|SZ|BJ))?(?!\d)", str(query or ""), flags=re.I)
    if explicit_code:
        code = explicit_code.group(1)
        suffix = (explicit_code.group(2) or "").upper()
        return f"{code}.{suffix}" if suffix else code, ""

    return "", ""


def _is_macro_policy_asset_impact_query(query: str) -> bool:
    text = str(query or "").strip()
    lowered = text.lower()
    if not text:
        return False
    has_policy = _contains_any(text, MACRO_POLICY_KEYWORDS) or _contains_any(lowered, MACRO_POLICY_KEYWORDS)
    has_asset = _contains_any(text, MACRO_POLICY_ASSET_KEYWORDS) or _contains_any(lowered, MACRO_POLICY_ASSET_KEYWORDS)
    has_impact = _contains_any(text, MACRO_POLICY_IMPACT_KEYWORDS) or _contains_any(lowered, MACRO_POLICY_IMPACT_KEYWORDS)
    return bool(has_policy and has_asset and has_impact)


def _has_stock_selection_intent_for_macro_guard(query: str) -> bool:
    text = str(query or "").strip()
    if not text:
        return False
    if _contains_any(text, STOCK_SELECTION_QUERY_KEYWORDS):
        return True
    stock_terms = r"(?:美股|股票|个股|候选股|标的|股票名单|股票池)"
    action_terms = r"(?:推荐|筛选|选|找|列|给我|帮我|挑|挖)"
    quantity_terms = r"(?:哪些|哪几只|几只|一批|名单|候选)"
    return bool(
        re.search(action_terms + r".{0,10}" + stock_terms, text)
        or re.search(quantity_terms + r".{0,10}" + stock_terms, text)
        or re.search(stock_terms + r".{0,10}" + quantity_terms, text)
    )


def _is_macro_policy_impact_query(query: str) -> bool:
    text = str(query or "").strip()
    lowered = text.lower()
    if not _is_macro_policy_asset_impact_query(text):
        return False
    if _has_stock_selection_intent_for_macro_guard(text):
        return False
    has_fresh_research_need = _contains_any(text, MACRO_POLICY_FRESH_RESEARCH_KEYWORDS) or _contains_any(
        lowered,
        MACRO_POLICY_FRESH_RESEARCH_KEYWORDS,
    )
    return not has_fresh_research_need


def _enforce_macro_policy_impact_routing(query: str, plan: List[str]) -> List[str]:
    current_plan = list(plan or [])
    if not _is_macro_policy_impact_query(query):
        return current_plan
    return ["macro_analyst"]


def _is_stock_selection_query_for_agent(query: str) -> bool:
    policy = classify_analysis_task_type(query)
    return policy.task_type == TASK_TYPE_STOCK_SELECTION


def _enforce_stock_selection_routing(query: str, plan: List[str]) -> List[str]:
    policy = classify_analysis_task_type(query)
    if policy.task_type == TASK_TYPE_STOCK_SELECTION:
        return list(policy.recommended_plan)
    return list(plan or [])


def _enforce_named_stock_analysis_screener_isolation(query: str, plan: List[str]) -> List[str]:
    policy = classify_analysis_task_type(query)
    current_plan = list(plan or [])
    if policy.task_type == TASK_TYPE_SINGLE_STOCK_ANALYSIS:
        return [step for step in current_plan if step != "screener"]
    return current_plan


def _apply_analysis_task_policy(
    query: str,
    plan: List[str],
    symbol: str = "",
    *,
    is_followup: bool = False,
    recent_context: str = "",
) -> tuple[List[str], str]:
    # 不采信 planner 生成的 symbol 来判定“是否有标的”，它可能正是模型自行补出的默认对象。
    if is_volatility_divergence_query(query):
        return _enforce_volatility_divergence_routing(query, plan), str(symbol or "").strip()

    if is_volatility_market_view_query(query):
        return _enforce_volatility_market_view_routing(query, plan), str(symbol or "").strip()

    policy = classify_analysis_task_type(
        query,
        symbol_hint="",
        is_followup=is_followup,
        recent_context=recent_context,
    )
    if is_pure_option_data_query(query):
        return ["monitor"], str(symbol or "").strip()
    if policy.task_type in {TASK_TYPE_STOCK_SELECTION, TASK_TYPE_OPTION_STRATEGY_NEEDS_SUBJECT}:
        return list(policy.recommended_plan), ""

    current_plan = list(plan or [])
    current_symbol = str(symbol or "").strip()

    if policy.task_type == TASK_TYPE_LINK_ARTICLE_STOCK_MAPPING:
        return list(policy.recommended_plan), "" if policy.clear_symbol else current_symbol

    if policy.task_type == TASK_TYPE_FUTURES_BROKER_SIGNAL:
        return list(policy.recommended_plan), "" if policy.clear_symbol else current_symbol

    if policy.task_type == TASK_TYPE_TECHNICAL_CONCEPT:
        return ["chatter"], "" if policy.clear_symbol else current_symbol

    if policy.task_type == TASK_TYPE_SINGLE_STOCK_ANALYSIS:
        filtered = [step for step in current_plan if step != "screener"]
        if not filtered and policy.recommended_plan:
            filtered = list(policy.recommended_plan)
        if not filtered:
            filtered = ["analyst"]
        return filtered, "" if policy.clear_symbol else current_symbol

    if policy.task_type == TASK_TYPE_OPTION_STRATEGY_WITH_SUBJECT:
        us_option_symbol = extract_us_option_underlying_symbol(query, symbol_hint=current_symbol)
        if us_option_symbol:
            current_symbol = us_option_symbol
        optional_experts = [
            step for step in current_plan
            if step in {"researcher", "macro_analyst", "portfolio_analyst"}
        ]
        return _dedupe_plan(["analyst", "monitor"] + optional_experts + ["strategist"]), current_symbol

    if is_market_data_query(query):
        return ["monitor"], str(symbol or "").strip()

    if _is_macro_policy_impact_query(query):
        return ["macro_analyst"], ""

    return current_plan, "" if policy.clear_symbol else current_symbol


def _is_futures_broker_signal_task(query: str) -> bool:
    return classify_analysis_task_type(query).task_type == TASK_TYPE_FUTURES_BROKER_SIGNAL


def _extract_futures_broker_signal_broker(query: str) -> str:
    text = str(query or "")
    candidates = []
    for broker in BROKER_SIGNAL_POSITIVE + BROKER_SIGNAL_NEGATIVE:
        candidates.append((broker, broker))
        short_name = broker.replace("期货", "")
        if short_name:
            candidates.append((short_name, broker))
    for alias, broker in sorted(candidates, key=lambda item: len(item[0]), reverse=True):
        if alias and alias in text:
            return broker
    for broker in BROKER_SIGNAL_POSITIVE + BROKER_SIGNAL_NEGATIVE:
        if broker in text:
            return broker
    return ""


def _extract_futures_broker_signal_product(query: str) -> str:
    text = str(query or "").replace("螺纹刚", "螺纹钢")
    lower = text.lower()
    for name, code in sorted(CN_TO_CODE.items(), key=lambda item: len(item[0]), reverse=True):
        if name and name in text:
            return name
    for code, name in sorted(PRODUCT_MAP.items(), key=lambda item: len(item[0]), reverse=True):
        code_lower = str(code or "").lower()
        if code_lower and re.search(rf"(?<![a-z0-9]){re.escape(code_lower)}(?![a-z0-9])", lower):
            return name or code
    return ""


def _extract_futures_broker_group_and_direction(query: str) -> tuple[str, str]:
    text = str(query or "")
    if "反指标" in text or "反向指标" in text:
        group = "negative"
    elif "正指标" in text or "正向指标" in text:
        group = "positive"
    else:
        group = ""

    if any(keyword in text for keyword in ("做空", "加空", "空单")):
        direction = "short"
    elif any(keyword in text for keyword in ("净持仓", "净多", "净变化")):
        direction = "net"
    else:
        direction = "long"
    return group, direction


def _user_requested_unauthorized_indicator(query: str) -> bool:
    return _contains_any(str(query or "").upper(), UNAUTHORIZED_TECHNICAL_INDICATOR_KEYWORDS)


def _sanitize_unauthorized_technical_indicators(text: str, query: str = "") -> str:
    lines = str(text or "").splitlines()
    kept_lines: List[str] = []
    removed = False
    requested_indicator = _user_requested_unauthorized_indicator(query)
    disclaimer_markers = ["不覆盖", "不展开", "不支持", "暂不", "不能", "当前技术分析标准", "当前产品口径"]
    for line in lines:
        upper_line = line.upper()
        if any(keyword in upper_line or keyword in line for keyword in UNAUTHORIZED_TECHNICAL_INDICATOR_KEYWORDS):
            if requested_indicator and any(marker in line for marker in disclaimer_markers):
                kept_lines.append(line)
                continue
            removed = True
            continue
        kept_lines.append(line)
    cleaned = "\n".join(kept_lines).strip()
    if removed:
        if requested_indicator:
            note = "> 注：当前技术分析标准只覆盖 K 线和均线，暂不展开 RSI、MACD、KDJ、BOLL 等指标。"
        else:
            note = "> 注：技术面已按当前产品口径仅保留 K 线与均线信息，未展开非授权技术指标。"
        cleaned = f"{cleaned}\n\n{note}".strip() if cleaned else note
    return cleaned


def _normalize_plan_for_execution_batches(plan: List[str]) -> List[str]:
    normalized = [str(step) for step in (plan or []) if str(step).strip()]
    if "strategist" not in normalized:
        return normalized

    normalized = [step for step in normalized if step != "strategist"]
    insert_after = -1
    for dependency in ("analyst", "monitor", "researcher", "macro_analyst"):
        if dependency in normalized:
            insert_after = max(insert_after, normalized.index(dependency))

    if insert_after >= 0:
        normalized.insert(insert_after + 1, "strategist")
    else:
        normalized.append("strategist")
    return normalized


def _build_execution_batches(plan: List[str]) -> List[List[str]]:
    normalized = _normalize_plan_for_execution_batches(plan)
    batches: List[List[str]] = []
    pending_parallel: List[str] = []

    def flush_parallel() -> None:
        nonlocal pending_parallel
        if pending_parallel:
            batches.append(pending_parallel[:])
            pending_parallel = []

    for step in normalized:
        if step in PARALLEL_BATCH_AGENTS:
            pending_parallel.append(step)
            continue

        flush_parallel()
        if step in SERIAL_BATCH_AGENTS:
            batches.append([step])
        else:
            batches.append([step])

    flush_parallel()
    return batches


FINALIZER_BYPASS_SINGLE_AGENTS = {"monitor", "screener", "macro_analyst"}


def _can_bypass_finalizer(state: AgentState) -> bool:
    plan = [str(step) for step in (state.get("plan") or []) if str(step).strip()]
    execution_batches = state.get("execution_batches") or _build_execution_batches(plan)
    if len(plan) != 1 or len(execution_batches) != 1 or len(execution_batches[0]) != 1:
        return False

    step = execution_batches[0][0]
    if step not in FINALIZER_BYPASS_SINGLE_AGENTS:
        return False

    reports = state.get("agent_reports") or {}
    return bool(str(reports.get(step, "") or "").strip())


def _ensure_analyst_then_strategist(plan: List[str]) -> List[str]:
    out = [p for p in plan if p not in {"analyst", "strategist"}]
    insert_at = 0
    out[insert_at:insert_at] = ["analyst", "strategist"]
    return out


def _enforce_option_portfolio_isolation(query: str, plan: List[str]) -> List[str]:
    """
    期权问题默认隔离股票持仓分析：
    - 未显式要求“结合我的股票持仓/组合”时，移除 portfolio_analyst，避免串仓。
    - 对“期权操作/调仓/建议”类问题，优先保障 analyst -> strategist 链路。
    """
    text = str(query or "")
    if not _contains_any(text, OPTION_QUERY_KEYWORDS):
        return plan

    explicit_stock_portfolio_coupling = _contains_any(text, EXPLICIT_STOCK_PORTFOLIO_COUPLING_KEYWORDS)
    filtered = list(plan)
    if not explicit_stock_portfolio_coupling:
        filtered = [p for p in filtered if p != "portfolio_analyst"]

    if _contains_any(text, OPTION_ACTION_QUERY_KEYWORDS):
        filtered = _ensure_analyst_then_strategist(filtered)

    return filtered


def _is_option_position_query(query: str) -> bool:
    text = str(query or "")
    if not _contains_any(text, OPTION_QUERY_KEYWORDS):
        return False
    return _contains_any(text, ["持仓", "仓位", "持有", "组合", "怎么调", "如何调", "调整"])


def _tag_monitor_worker_response(response: str) -> str:
    content = str(response or "")
    report_tag = "【美股期权体检】" if "【美股期权体检】" in content else "【数据监控】"
    if content.lstrip().startswith(report_tag):
        return content
    return f"{report_tag}\n{content}"


def _strip_stock_portfolio_sections(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"【持仓分析】[\s\S]*?(?=(?:\n【|$))", "", text).strip()


def _build_delta_cash_gap_note(reason: str, trend_signal: str, risk_preference: str) -> str:
    from option_delta_tools import get_delta_target_band

    band = get_delta_target_band(trend_signal=trend_signal, risk_preference=risk_preference)
    return (
        f"DeltaCash暂不输出：{reason}。"
        f"当前技术面目标区间参考为 `[{band['low']:+.2f}, {band['high']:+.2f}]`，"
        "请补齐IV与最新价后再计算。"
    )


def _coerce_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _derive_option_cn_labels_from_leg(leg: Dict[str, Any]) -> Dict[str, str]:
    cp_raw = str(
        leg.get("cp")
        or leg.get("option_flag")
        or leg.get("call_put")
        or leg.get("cp_cn")
        or leg.get("cp_text")
        or ""
    ).strip().lower()
    direction_raw = str(leg.get("direction_cn") or "").strip()
    if "认购" in direction_raw:
        cp_cn = "认购"
    elif "认沽" in direction_raw:
        cp_cn = "认沽"
    elif cp_raw in {"call", "c", "认购"}:
        cp_cn = "认购"
    elif cp_raw in {"put", "p", "认沽"}:
        cp_cn = "认沽"
    else:
        cp_cn = "待确认"

    side_raw = str(leg.get("side") or leg.get("side_text") or leg.get("side_cn") or "").strip().lower()
    if "买" in direction_raw:
        side_cn = "买方"
    elif "卖" in direction_raw:
        side_cn = "卖方"
    elif side_raw in {"long", "买方", "买入"}:
        side_cn = "买方"
    elif side_raw in {"short", "卖方", "卖出"}:
        side_cn = "卖方"
    else:
        signed_qty = _coerce_float(leg.get("signed_qty"))
        if signed_qty is None:
            side_cn = "待确认"
        else:
            side_cn = "买方" if signed_qty >= 0 else "卖方"

    if cp_cn == "待确认" or side_cn == "待确认":
        direction_cn = "待确认"
    else:
        direction_cn = ("买" if side_cn == "买方" else "卖") + cp_cn
    return {"cp_cn": cp_cn, "side_cn": side_cn, "direction_cn": direction_cn}


def _build_canonical_option_legs(vision_option_legs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for leg in vision_option_legs or []:
        if not isinstance(leg, dict):
            continue
        row = dict(leg)
        labels = _derive_option_cn_labels_from_leg(row)
        row.update(labels)
        signed_qty = _coerce_float(row.get("signed_qty"))
        qty_val = _coerce_float(row.get("qty"))
        if qty_val is None:
            qty_val = abs(signed_qty) if signed_qty is not None else None
        qty = int(abs(qty_val)) if qty_val is not None else 0
        if qty <= 0:
            continue
        if signed_qty is None:
            signed_qty = float(qty) if row["side_cn"] == "买方" else -float(qty)
        month_raw = row.get("month")
        month = None
        if month_raw not in (None, ""):
            try:
                month = int(float(month_raw))
            except Exception:
                month = None
        strike = _coerce_float(row.get("strike") or row.get("exercise_price") or row.get("行权价"))
        contract_code = str(row.get("contract_code") or row.get("ts_code") or "").strip().upper()
        underlying = str(row.get("underlying_hint") or row.get("underlying") or "").strip().upper() or "待确认"
        out.append(
            {
                "underlying_hint": underlying,
                "contract_code": contract_code or "待确认",
                "month": month,
                "strike": strike,
                "qty": qty,
                "signed_qty": int(round(float(signed_qty))),
                "cp_cn": row["cp_cn"],
                "side_cn": row["side_cn"],
                "direction_cn": row["direction_cn"],
            }
        )
    return out


def _build_canonical_option_legs_block(canonical_legs: List[Dict[str, Any]]) -> str:
    legs = [x for x in (canonical_legs or []) if isinstance(x, dict)]
    if not legs:
        return ""
    lines = [
        "1. 持仓拆解表",
        "- 以下持仓已按截图结构化识别结果锁定（方向不可改写）。",
        "",
        "| 序号 | 标的 | 合约代码 | 到期月 | 行权价 | 方向 | 张数 |",
        "|---:|---|---|---|---:|---|---:|",
    ]
    for i, leg in enumerate(legs, start=1):
        month_val = leg.get("month")
        month_text = f"{int(month_val)}月" if isinstance(month_val, int) else "待确认"
        strike_val = _coerce_float(leg.get("strike"))
        strike_text = f"{strike_val:.3f}" if strike_val is not None else "待确认"
        lines.append(
            f"| {i} | {leg.get('underlying_hint') or '待确认'} | {leg.get('contract_code') or '待确认'} | "
            f"{month_text} | {strike_text} | {leg.get('direction_cn') or '待确认'} | {int(leg.get('qty') or 0)} |"
        )
    return "\n".join(lines)


OPTION_SECTION_ORDER = [
    "summary",
    "quotes",
    "holdings",
    "delta",
    "exposure",
    "scenarios",
    "plans",
    "risk",
    "checklist",
]

OPTION_SECTION_WHITELIST = set(OPTION_SECTION_ORDER)


def _normalize_option_section_id(title: str) -> str:
    raw = str(title or "").strip()
    if not raw:
        return ""
    stripped = re.sub(r"^[#>\-\s]+", "", raw)
    stripped = re.sub(r"^[0-9一二三四五六七八九十]+[、\.\)]\s*", "", stripped)
    compact = re.sub(r"[\s`*_:\-\[\]【】()（）]", "", stripped).lower()
    raw_no_space = re.sub(r"\s+", "", stripped)

    if any(x in raw_no_space for x in ["情报与舆情", "财经热点", "市场背景补充", "今日财经热点", "市场情绪"]):
        return "ignore"
    if "deltacash" in compact or ("delta" in compact and "cash" in compact):
        return "delta"
    if any(x in raw_no_space for x in ["持仓拆解", "持仓拆解表", "持仓结构"]) or "holdingsbreakdown" in compact:
        return "holdings"
    if any(x in raw_no_space for x in ["标的现价", "权威数据", "权威现价", "现价（权威数据）"]):
        return "quotes"
    if any(x in raw_no_space for x in ["组合净暴露", "净暴露", "到期错配", "市场深度解析", "市场深度", "技术分析", "市场分析"]):
        return "exposure"
    if any(x in raw_no_space for x in ["三情景", "情景分支", "关键触发位"]):
        return "scenarios"
    if any(x in raw_no_space for x in ["两套可执行调整方案", "交易策略部署", "保守方案", "进攻方案", "调整方案"]):
        return "plans"
    if any(x in raw_no_space for x in ["风控阈值", "失效条件", "风险提示", "风控与对冲", "风控"]):
        return "risk"
    if any(x in raw_no_space for x in ["当日执行清单", "执行清单"]):
        return "checklist"
    if any(x in raw_no_space for x in ["核心结论", "综合研判", "执行摘要"]) or "executivesummary" in compact:
        return "summary"
    if "核心指标建议" in raw_no_space or "核心指标" in raw_no_space:
        return "delta"
    return ""


def _split_markdown_sections(text: str) -> Dict[str, Any]:
    body = str(text or "").strip()
    if not body:
        return {"preamble": "", "sections": []}

    lines = body.splitlines()
    preamble: List[str] = []
    sections: List[Dict[str, str]] = []
    current_title = ""
    current_lines: List[str] = []

    def _flush_current() -> None:
        nonlocal current_title, current_lines, sections
        if not current_lines:
            return
        content = "\n".join(current_lines).strip()
        if content:
            sections.append(
                {
                    "title": current_title,
                    "section_id": _normalize_option_section_id(current_title),
                    "content": content,
                }
            )
        current_title = ""
        current_lines = []

    def _extract_heading_title(line: str) -> str:
        line_stripped = str(line or "").strip()
        if not line_stripped:
            return ""
        m_md = re.match(r"^#{1,6}\s+(.+)$", line_stripped)
        if m_md:
            return str(m_md.group(1) or "").strip()
        m_num = re.match(r"^(?:[0-9]+|[一二三四五六七八九十]+)[、\.]\s*(.+)$", line_stripped)
        if m_num:
            return str(m_num.group(1) or "").strip()
        if re.match(r"^(?:【?\s*Delta\s*Cash\s*】?|【?\s*DeltaCash\s*】?)$", line_stripped, flags=re.IGNORECASE):
            return "DeltaCash"
        return ""

    for line in lines:
        heading = _extract_heading_title(line)
        if heading:
            _flush_current()
            current_title = heading
            current_lines = [line]
            continue
        if current_lines:
            current_lines.append(line)
        else:
            preamble.append(line)
    _flush_current()

    return {"preamble": "\n".join(preamble).strip(), "sections": sections}


def _render_markdown_sections(preamble: str, sections: List[str]) -> str:
    blocks = [str(x or "").strip() for x in sections if str(x or "").strip()]
    pre = str(preamble or "").strip()
    if pre and blocks:
        return f"{pre}\n\n" + "\n\n".join(blocks)
    if pre:
        return pre
    return "\n\n".join(blocks).strip()


def _collect_option_section_ids(text: str) -> set[str]:
    parsed = _split_markdown_sections(text)
    ids = set()
    for sec in parsed.get("sections", []):
        sid = str(sec.get("section_id") or "")
        if sid:
            ids.add(sid)
    return ids


def _compose_option_sections(
    text: str,
    structured_sections: Dict[str, str] | None = None,
    keep_only_whitelist: bool = True,
) -> str:
    parsed = _split_markdown_sections(text)
    sections = parsed.get("sections", [])
    preamble = str(parsed.get("preamble") or "").strip()
    structured = structured_sections or {}

    if not sections:
        blocks = [str(structured.get(sid) or "").strip() for sid in OPTION_SECTION_ORDER if str(structured.get(sid) or "").strip()]
        if blocks:
            return _render_markdown_sections(preamble=str(text or "").strip(), sections=blocks).strip()
        return str(text or "").strip()

    first_by_id: Dict[str, str] = {}
    passthrough: List[str] = []
    for sec in sections:
        sid = str(sec.get("section_id") or "").strip()
        content = str(sec.get("content") or "").strip()
        if not content:
            continue
        if sid == "ignore":
            continue
        if sid and ((not keep_only_whitelist) or sid in OPTION_SECTION_WHITELIST):
            first_by_id.setdefault(sid, content)
            continue
        if not keep_only_whitelist:
            passthrough.append(content)

    for sid, block in structured.items():
        block_text = str(block or "").strip()
        if not block_text:
            continue
        if sid in OPTION_SECTION_WHITELIST:
            first_by_id[sid] = block_text

    ordered: List[str] = []
    for sid in OPTION_SECTION_ORDER:
        if sid in first_by_id:
            ordered.append(first_by_id[sid])
    if not keep_only_whitelist:
        ordered.extend(passthrough)
    if not ordered:
        return str(text or "").strip()
    return _render_markdown_sections(preamble=preamble, sections=ordered).strip()


def _sanitize_option_direction_terms(text: str) -> str:
    out = str(text or "")
    replacements = [
        (r"(?i)\bshort\s*call\b", "卖认购"),
        (r"(?i)\blong\s*call\b", "买认购"),
        (r"(?i)\bshort\s*put\b", "卖认沽"),
        (r"(?i)\blong\s*put\b", "买认沽"),
        (r"(?i)\bshortcall\b", "卖认购"),
        (r"(?i)\blongcall\b", "买认购"),
        (r"(?i)\bshortput\b", "卖认沽"),
        (r"(?i)\blongput\b", "买认沽"),
        (r"长\s*call", "买认购"),
        (r"短\s*call", "卖认购"),
        (r"长\s*put", "买认沽"),
        (r"短\s*put", "卖认沽"),
    ]
    for pattern, repl in replacements:
        out = re.sub(pattern, repl, out)
    return out


def _validate_option_direction_consistency(final_text: str, canonical_legs: List[Dict[str, Any]]) -> Dict[str, Any]:
    text = str(final_text or "")
    reasons: List[str] = []
    term_hits = re.findall(
        r"(?i)\b(?:long|short)\s*(?:call|put)\b|长\s*call|短\s*call|长\s*put|短\s*put",
        text,
    )
    if term_hits:
        reasons.append("发现Long/Short方向术语")

    direction_tokens = set(re.findall(r"(买认购|卖认购|买认沽|卖认沽)", text))
    canonical_dirs = {str(leg.get("direction_cn") or "") for leg in (canonical_legs or []) if str(leg.get("direction_cn") or "")}
    extra_dirs = sorted([d for d in direction_tokens if d and d not in canonical_dirs])
    if extra_dirs:
        reasons.append(f"存在非识别持仓方向术语: {','.join(extra_dirs)}")

    contract_conflicts = 0
    all_dirs = {"买认购", "卖认购", "买认沽", "卖认沽"}
    for leg in canonical_legs or []:
        contract_code = str(leg.get("contract_code") or "").strip()
        if not contract_code or contract_code == "待确认":
            continue
        expected = str(leg.get("direction_cn") or "").strip()
        if not expected or expected == "待确认":
            continue
        for m in re.finditer(re.escape(contract_code), text):
            segment = text[max(0, m.start() - 24): min(len(text), m.end() + 24)]
            found = set(re.findall(r"(买认购|卖认购|买认沽|卖认沽)", segment))
            if any(x in found for x in (all_dirs - {expected})):
                contract_conflicts += 1
                break
    if contract_conflicts:
        reasons.append(f"合约方向邻域冲突 {contract_conflicts} 处")

    return {"conflict_count": len(term_hits) + len(extra_dirs) + contract_conflicts, "reasons": reasons}


def _replace_option_legs_section(text: str, canonical_block: str) -> str:
    body = str(text or "").strip()
    if not canonical_block:
        return body
    section_ids = _collect_option_section_ids(body)
    has_holdings_heading = "holdings" in section_ids
    patterns = [
        re.compile(
            r"(?:^|\n)(?:#{1,6}\s*)?1[\.、]\s*持仓拆解表[\s\S]*?(?=\n(?:#{1,6}\s*)?[2２][\.、]\s*组合净暴露与到期错配|\Z)",
            flags=re.MULTILINE,
        ),
        re.compile(
            r"(?:^|\n)(?:#{1,6}\s*)?[^\n]*持仓拆解(?:表)?[^\n]*\n[\s\S]*?(?=\n#{1,6}\s|\Z)",
            flags=re.MULTILINE,
        ),
    ]
    for pattern in patterns:
        if pattern.search(body):
            return pattern.sub("\n" + canonical_block + "\n", body, count=1).strip()
    if has_holdings_heading:
        # 已存在“持仓拆解表”但未命中替换规则时，不再额外前置，避免重复章节。
        return body
    # 未发现可替换章节时不强制前置，避免将锁定表插到报告最顶部造成冗长。
    return body


def _apply_option_fact_lock(
    text: str,
    canonical_legs: List[Dict[str, Any]],
    strict_cover: bool = True,
) -> Dict[str, Any]:
    sanitized = _sanitize_option_direction_terms(text)
    canonical_block = _build_canonical_option_legs_block(canonical_legs)
    validation = _validate_option_direction_consistency(sanitized, canonical_legs)
    conflict_count = int(validation.get("conflict_count") or 0)
    out = sanitized
    if strict_cover and canonical_block:
        out = _replace_option_legs_section(out, canonical_block)
        if conflict_count > 0 and "已按识别持仓自动纠偏" not in out:
            out = (
                "> ⚠️ **已按识别持仓自动纠偏，避免方向误判。**\n\n"
                + out
            ).strip()
    return {
        "text": out,
        "canonical_option_legs_block": canonical_block,
        "option_direction_conflict_count": conflict_count,
    }


def _normalize_underlying_code_for_quote(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if not raw:
        return ""
    mapping = {
        "IO": "000300.SH",
        "HO": "000016.SH",
        "MO": "000852.SH",
        "000300": "000300.SH",
        "000016": "000016.SH",
        "000852": "000852.SH",
    }
    if raw in mapping:
        return mapping[raw]
    if re.fullmatch(r"\d{6}", raw):
        return f"{raw}.SZ" if raw.startswith("159") else f"{raw}.SH"
    if re.fullmatch(r"\d{6}\.(SH|SZ)", raw):
        return raw
    return ""


def _collect_quote_underlyings_from_canonical_legs(canonical_legs: List[Dict[str, Any]], symbol_hint: str = "") -> List[str]:
    codes: List[str] = []
    for leg in canonical_legs or []:
        code = _normalize_underlying_code_for_quote(leg.get("underlying_hint"))
        if code and code not in codes:
            codes.append(code)
    hint_code = _normalize_underlying_code_for_quote(symbol_hint)
    if hint_code and hint_code not in codes:
        codes.append(hint_code)
    return codes


def _build_authoritative_quote_block(authoritative_quotes: Dict[str, Any]) -> str:
    quote_map = authoritative_quotes or {}
    if not quote_map:
        return ""
    lines = [
        "### 标的现价（权威数据）",
        "",
        "| 标的 | 收盘价 | 交易日 | 来源 |",
        "|---|---:|---|---|",
    ]
    for code, payload in quote_map.items():
        row = payload if isinstance(payload, dict) else {}
        is_missing = bool(row.get("missing"))
        price_raw = row.get("close_price")
        trade_date = str(row.get("trade_date") or "缺失")
        source = str(row.get("source") or "unknown")
        if is_missing or price_raw in (None, ""):
            price_text = "缺失"
        else:
            try:
                price_text = f"{float(price_raw):.3f}"
            except Exception:
                price_text = "缺失"
        lines.append(f"| {code} | {price_text} | {trade_date} | {source} |")
    return "\n".join(lines)


def _build_underlying_trend_map(
    technical_summary: str,
    underlyings: List[str],
    default_trend: str,
) -> Dict[str, str]:
    text = str(technical_summary or "")
    out: Dict[str, str] = {}
    for code in underlyings or []:
        base = str(code or "").strip().upper()
        if not base:
            continue
        aliases = {base, base.split(".")[0]}
        if base.startswith("510500"):
            aliases.update({"中证500", "500ETF"})
        elif base.startswith("159915"):
            aliases.update({"创业板", "创业板ETF"})
        elif base.startswith("588000"):
            aliases.update({"科创50", "科创50ETF"})
        trend = ""
        for alias in aliases:
            m = re.search(rf"{re.escape(alias)}[^\n，。；]*?(看涨|看跌|震荡)", text)
            if m:
                trend = str(m.group(1))
                break
        out[base] = trend or str(default_trend or "震荡")
    return out


def _replace_delta_cash_section(text: str, delta_report: str) -> str:
    body = str(text or "").strip()
    delta_block = str(delta_report or "").strip()
    if not delta_block:
        return body
    parsed = _split_markdown_sections(body)
    sections = parsed.get("sections", [])
    if not sections:
        return body
    replaced = False
    rendered_sections: List[str] = []
    for sec in sections:
        sid = str(sec.get("section_id") or "")
        if sid == "delta":
            if not replaced:
                rendered_sections.append(delta_block)
                replaced = True
            continue
        content = str(sec.get("content") or "").strip()
        if content:
            rendered_sections.append(content)
    if replaced:
        return _render_markdown_sections(
            preamble=str(parsed.get("preamble") or ""),
            sections=rendered_sections,
        ).strip()
    # 缺失时不强制前置，避免Delta块与正文中的Delta章节重复。
    return body


def _extract_delta_cash_block(text: str) -> str:
    body = str(text or "").strip()
    if not body:
        return ""
    parsed = _split_markdown_sections(body)
    for sec in parsed.get("sections", []):
        if str(sec.get("section_id") or "") == "delta":
            return str(sec.get("content") or "").strip()
    pattern = re.compile(
        r"(?:^|\n)###\s*【DeltaCash】[\s\S]*?(?=\n(?:###\s|[1-6][\.、]\s)|\Z)",
        flags=re.MULTILINE,
    )
    m = pattern.search(body)
    if not m:
        return ""
    return str(m.group(0)).strip()


def _build_min_delta_block_from_meta(delta_meta: Dict[str, Any]) -> str:
    meta = delta_meta if isinstance(delta_meta, dict) else {}
    portfolio = meta.get("portfolio_summary") or meta.get("metrics") or {}
    adjustment = portfolio.get("adjustment") or meta.get("adjustment") or {}
    ratio = portfolio.get("effective_delta_ratio", portfolio.get("delta_ratio", 0.0))
    lines = [
        "### 【DeltaCash】",
        f"- 组合 Total Delta Cash: `{float(portfolio.get('total_delta_cash', 0.0)):,.0f}` 元",
        f"- 组合执行口径 Delta Ratio: `{float(ratio):+.4f}`",
    ]
    band = adjustment.get("band") if isinstance(adjustment, dict) else {}
    if isinstance(band, dict) and {"low", "high"} <= set(band.keys()):
        lines.append(
            f"- 技术面目标区间: `[{float(band.get('low', 0.0)):+.2f}, {float(band.get('high', 0.0)):+.2f}]`"
        )
    if isinstance(adjustment, dict) and adjustment.get("action"):
        lines.append(f"- 建议方向: `{str(adjustment.get('action'))}`")
    return "\n".join(lines)


def _ensure_delta_section_from_meta(
    text: str,
    delta_report: str,
    delta_meta: Dict[str, Any],
    delta_gap_note: str,
    displayable: bool,
) -> str:
    out = str(text or "").strip()
    if "delta" in _collect_option_section_ids(out):
        return out
    block = _extract_delta_cash_block(delta_report)
    if not block:
        block = _extract_delta_cash_block(str((delta_meta or {}).get("report") or ""))
    if not block and displayable and isinstance(delta_meta, dict):
        block = _build_min_delta_block_from_meta(delta_meta)
    if not block and delta_gap_note:
        block = f"### 【DeltaCash】\n- 数据缺口: {delta_gap_note}"
    if not block:
        return out
    return f"{block}\n\n{out}".strip()


def _dedupe_option_position_sections(text: str) -> str:
    out = str(text or "").strip()
    if not out:
        return out
    parsed = _split_markdown_sections(out)
    sections = parsed.get("sections", [])
    if not sections:
        return out
    seen: set[str] = set()
    rendered_sections: List[str] = []
    for sec in sections:
        sid = str(sec.get("section_id") or "")
        content = str(sec.get("content") or "").strip()
        if not content:
            continue
        if sid == "ignore":
            continue
        if sid and sid in OPTION_SECTION_WHITELIST:
            if sid in seen:
                continue
            seen.add(sid)
        rendered_sections.append(content)
    deduped = _render_markdown_sections(
        preamble=str(parsed.get("preamble") or ""),
        sections=rendered_sections,
    )
    return re.sub(r"\n{3,}", "\n\n", deduped).strip()


def _replace_authoritative_quote_section(text: str, quote_block: str) -> str:
    body = str(text or "").strip()
    block = str(quote_block or "").strip()
    if not block:
        return body
    pattern = re.compile(
        r"(?:^|\n)###\s*标的现价（权威数据）[\s\S]*?(?=\n###\s|\Z)",
        flags=re.MULTILINE,
    )
    if pattern.search(body):
        return pattern.sub("\n" + block + "\n", body, count=1).strip()
    # 缺失时不强制前置，避免权威现价块被插在【最终决策】前方。
    return body


def _extract_price_mentions_for_symbol(text: str, symbol: str) -> List[float]:
    values: List[float] = []
    symbol_u = str(symbol or "").strip().upper()
    if not symbol_u:
        return values
    aliases = {symbol_u}
    if "." in symbol_u:
        aliases.add(symbol_u.split(".")[0])
    alias_pattern = "|".join(re.escape(x) for x in sorted(aliases, key=len, reverse=True))
    if not alias_pattern:
        return values
    patterns = [
        re.compile(
            rf"(?:{alias_pattern})[^\n]{{0,20}}(?:现价|收盘价|价格|close)[^\d\-]{{0,8}}(\d+(?:\.\d+)?)",
            flags=re.IGNORECASE,
        ),
        re.compile(rf"(?:{alias_pattern})\s*[:：=]\s*(\d+(?:\.\d+)?)(?:\s*元)?", flags=re.IGNORECASE),
    ]
    for pattern in patterns:
        for match in pattern.finditer(text):
            try:
                values.append(float(match.group(1)))
            except Exception:
                continue
    return values


def _validate_authoritative_price_consistency(
    final_text: str,
    authoritative_quotes: Dict[str, Any],
    threshold: float = AUTHORITATIVE_PRICE_CONFLICT_THRESHOLD,
) -> Dict[str, Any]:
    text = str(final_text or "")
    conflicts: List[Dict[str, Any]] = []
    for ts_code, payload in (authoritative_quotes or {}).items():
        row = payload if isinstance(payload, dict) else {}
        if bool(row.get("missing")):
            continue
        try:
            auth_price = float(row.get("close_price"))
        except Exception:
            continue
        if auth_price <= 0:
            continue
        mentions = _extract_price_mentions_for_symbol(text, ts_code)
        if not mentions:
            continue
        max_diff = 0.0
        observed = None
        for value in mentions:
            diff = abs(value - auth_price) / auth_price
            if diff > max_diff:
                max_diff = diff
                observed = value
        if observed is not None and max_diff > float(threshold):
            conflicts.append(
                {
                    "ts_code": ts_code,
                    "observed_price": float(observed),
                    "authoritative_price": float(auth_price),
                    "diff_ratio": float(max_diff),
                }
            )
    return {"conflict_count": len(conflicts), "conflicts": conflicts}


def _replace_symbol_price_mentions(text: str, ts_code: str, authoritative_price: float) -> str:
    out = str(text or "")
    symbol = str(ts_code or "").strip().upper()
    if not symbol:
        return out
    aliases = {symbol}
    if "." in symbol:
        aliases.add(symbol.split(".")[0])
    alias_pattern = "|".join(re.escape(x) for x in sorted(aliases, key=len, reverse=True))
    if not alias_pattern:
        return out
    price_text = f"{float(authoritative_price):.3f}"
    patterns = [
        re.compile(
            rf"((?:{alias_pattern})[^\n]{{0,20}}(?:现价|收盘价|价格|close)[^\d\-]{{0,8}})(\d+(?:\.\d+)?)",
            flags=re.IGNORECASE,
        ),
        re.compile(
            rf"((?:{alias_pattern})\s*[:：=]\s*)(\d+(?:\.\d+)?)(\s*元)?",
            flags=re.IGNORECASE,
        ),
    ]
    for pattern in patterns:
        out = pattern.sub(lambda m: f"{m.group(1)}{price_text}{m.group(3) if m.lastindex and m.lastindex >= 3 and m.group(3) else ''}", out)
    return out


def _apply_authoritative_quote_lock(
    text: str,
    authoritative_quotes: Dict[str, Any],
    authoritative_quote_block: str = "",
    strict_cover: bool = True,
    threshold: float = AUTHORITATIVE_PRICE_CONFLICT_THRESHOLD,
) -> Dict[str, Any]:
    out = str(text or "").strip()
    quotes = authoritative_quotes or {}
    block = authoritative_quote_block or _build_authoritative_quote_block(quotes)
    if strict_cover and block:
        out = _replace_authoritative_quote_section(out, block)
    validation = _validate_authoritative_price_consistency(out, quotes, threshold=threshold)
    conflicts = validation.get("conflicts") or []
    for item in conflicts:
        ts_code = str(item.get("ts_code") or "")
        auth_price = item.get("authoritative_price")
        try:
            out = _replace_symbol_price_mentions(out, ts_code=ts_code, authoritative_price=float(auth_price))
        except Exception:
            continue
    if conflicts and "已按权威行情自动纠偏" not in out:
        out = (
            "> ⚠️ **已按权威行情自动纠偏（代码、价格、日期已锁定）。**\n\n"
            + out
        ).strip()
    return {
        "text": out,
        "authoritative_quote_block": block,
        "price_conflict_count": int(validation.get("conflict_count") or 0),
    }


def _ensure_option_position_structure(
    text: str,
    delta_cash_report: str,
    delta_cash_gap_note: str,
    trend_signal: str,
    risk_preference: str,
    key_levels: str,
) -> str:
    out = str(text or "").strip()
    delta_block = str(delta_cash_report or "").strip()
    current_ids = _collect_option_section_ids(out)
    if delta_block and "delta" not in current_ids:
        out = f"{delta_block}\n\n{out}".strip()
        current_ids = _collect_option_section_ids(out)

    section_titles = {
        1: "1. 持仓拆解表",
        2: "2. 组合净暴露与到期错配",
        3: "3. 关键触发位与三情景分支",
        4: "4. 两套可执行调整方案",
        5: "5. 风控阈值与失效条件",
        6: "6. 当日执行清单",
    }
    section_id_map = {
        1: "holdings",
        2: "exposure",
        3: "scenarios",
        4: "plans",
        5: "risk",
        6: "checklist",
    }
    has_section = {idx: (section_id_map[idx] in current_ids) for idx in section_id_map}
    if all(has_section.values()):
        return out

    key_text = key_levels if key_levels else "关键位待确认（请结合最新K线支撑/压力位）"
    section_bodies = {
        1: f"""{section_titles[1]}
- {"已在【DeltaCash】区块展示核心腿信息；若单腿成本/已实现收益缺失，请补录后复算。" if delta_block else "本轮DeltaCash未输出，请先补齐缺失行情后再量化。"}""",
        2: f"""{section_titles[2]}
- {"先依据 Total Delta Cash 与 Delta Ratio 判定净方向，再检查近月到期腿是否集中。" if delta_block else "先按方向腿结构与近月到期分布判定净风险，避免伪精确估算。"}
- 近月到期腿优先降风险，避免时间价值快速衰减。""",
        3: f"""{section_titles[3]}
- 技术面参考：{trend_signal}；关键位：{key_text}
- 上涨分支：若突破关键压力位，按目标区间上沿控制Delta，避免过度追涨。
- 震荡分支：若维持区间震荡，维持中性或轻方向暴露，减少无效Theta损耗。
- 下跌分支：若跌破关键支撑位，按目标区间下沿收敛Delta，优先防极端波动风险。""",
        4: f"""{section_titles[4]}
- 保守方案：先削减风险最大的短近月腿，目标是把Delta收敛至区间中轴附近。
- 进攻方案：在趋势确认后再补方向腿，把Delta推进到区间同向一侧。""",
        5: f"""{section_titles[5]}
- 若Delta Ratio持续偏离目标区间，触发再平衡。
- 若到期剩余天数快速下降且仓位集中，触发展期或减仓。
- 若IV和价格方向同时不利，判定策略失效并降低总敞口。""",
        6: f"""{section_titles[6]}
- [ ] 核对每腿到期日与张数
- [ ] {"复核DeltaCash与目标区间偏离" if delta_block else "记录DeltaCash数据缺口并补齐IV/最新价"}
- [ ] 执行第一步减风险/调仓动作
- [ ] 设定盘中与收盘复核点""",
    }
    missing_sections = [idx for idx in section_titles if not has_section.get(idx)]
    if missing_sections:
        skeleton = "\n\n---\n\n" + "\n\n".join(section_bodies[idx] for idx in missing_sections)
        out = f"{out}{skeleton}".strip()
    if delta_cash_gap_note and delta_cash_gap_note not in out:
        out = f"{out}\n\n> ⚠️ **数据缺口**：{delta_cash_gap_note}".strip()
    return out


def _enforce_margin_monitor_routing(query: str, plan: List[str]) -> List[str]:
    """
    保证金/合约乘数问题强制优先 monitor。
    若用户同时要策略，则固定 monitor -> strategist，再接其余节点。
    """
    if not _contains_any(query, MARGIN_QUERY_KEYWORDS):
        return plan

    want_strategy = _contains_any(query, STRATEGY_QUERY_KEYWORDS)
    tail = [p for p in plan if p not in {"monitor", "strategist"}]
    enforced = ["monitor", "strategist"] + tail if want_strategy else ["monitor"] + tail

    deduped = []
    for step in enforced:
        if step not in deduped:
            deduped.append(step)
    return deduped


def _is_cn_margin_explicit_query(query: str) -> bool:
    text = str(query or "").strip()
    if not text or _contains_any(text, CN_MARGIN_EXCLUDED_KEYWORDS):
        return False
    if _contains_any(text, CN_MARGIN_QUERY_KEYWORDS):
        return True
    return "融资" in text and _contains_any(text, CN_MARGIN_CONTEXT_KEYWORDS)


def _is_cn_margin_auto_context_query(query: str) -> bool:
    text = str(query or "").strip()
    if not text or _contains_any(text, CN_MARGIN_EXCLUDED_KEYWORDS):
        return False
    return _contains_any(text, CN_MARGIN_MARKET_SUBJECT_KEYWORDS) and _contains_any(
        text, CN_MARGIN_MARKET_INTENT_KEYWORDS
    )


def _is_cn_margin_analysis_query(query: str) -> bool:
    return _contains_any(str(query or ""), CN_MARGIN_ANALYSIS_KEYWORDS)


def _extract_cn_margin_as_of_date(query: str) -> str:
    text = str(query or "")
    full = re.search(r"(?<!\d)(20\d{2})[-/.年](\d{1,2})[-/.月](\d{1,2})日?(?!\d)", text)
    if full:
        year, month, day = (int(value) for value in full.groups())
    else:
        compact = re.search(r"(?<!\d)(20\d{6})(?!\d)", text)
        if compact:
            try:
                return datetime.strptime(compact.group(1), "%Y%m%d").strftime("%Y%m%d")
            except ValueError:
                return ""
        short = re.search(r"(?<!\d)(\d{1,2})月(\d{1,2})日?", text)
        if not short:
            return ""
        year = datetime.now().year
        month, day = (int(value) for value in short.groups())
    try:
        return datetime(year, month, day).strftime("%Y%m%d")
    except ValueError:
        return ""


def _enforce_cn_margin_monitor_routing(query: str, plan: List[str]) -> List[str]:
    explicit = _is_cn_margin_explicit_query(query)
    automatic = _is_cn_margin_auto_context_query(query)
    if not explicit and not automatic:
        return list(plan or [])

    task_type = classify_analysis_task_type(query).task_type
    wants_strategy = task_type in {
        TASK_TYPE_OPTION_STRATEGY_WITH_SUBJECT,
        TASK_TYPE_OPTION_STRATEGY_NEEDS_SUBJECT,
    } and not is_pure_option_data_query(query)
    if explicit and not _is_cn_margin_analysis_query(query) and not wants_strategy:
        return ["monitor"]

    tail = [step for step in list(plan or []) if step not in {"monitor", "strategist"}]
    enforced = ["monitor"]
    if wants_strategy:
        enforced.append("strategist")
    enforced.extend(tail)
    return _dedupe_plan(enforced)


def _enforce_option_data_monitor_routing(query: str, plan: List[str]) -> List[str]:
    """
    明确的数据查询问题只派 monitor，避免误上知识解释或宏观/技术/策略整链路。
    例如：IV/波动率高低、分位、到期日、保证金、乘数、价格、最新价。
    """
    if is_volatility_divergence_query(query):
        return plan
    if is_volatility_market_view_query(query):
        return plan
    if is_pure_option_data_query(query):
        return ["monitor"]
    task_type = classify_analysis_task_type(query).task_type
    if task_type in {TASK_TYPE_OPTION_STRATEGY_WITH_SUBJECT, TASK_TYPE_OPTION_STRATEGY_NEEDS_SUBJECT}:
        return plan
    if not is_market_data_query(query):
        return plan
    return ["monitor"]


def _is_option_strategy_question(query: str) -> bool:
    return is_option_strategy_question(query)


def _has_explicit_option_underlying(query: str) -> bool:
    return has_explicit_option_underlying(query)


def _is_generic_option_strategy_question(query: str) -> bool:
    return is_generic_option_strategy_question(query)


def _enforce_unspecified_option_strategy_routing(query: str, plan: List[str], symbol: str = "") -> tuple[List[str], str]:
    return _apply_analysis_task_policy(query, plan, symbol)


CHART_REQUEST_KEYWORDS = (
    "画图",
    "画一下",
    "画出来",
    "出图",
    "图表",
    "走势图",
    "走势可视化",
    "K线图",
    "k线图",
    "K 线图",
    "k 线图",
    "candlestick",
    "chart",
    "plot",
)


def _wants_chart(query: str) -> bool:
    q = str(query or "")
    q_lower = q.lower()
    return any(keyword.lower() in q_lower for keyword in CHART_REQUEST_KEYWORDS)


def _select_generalist_model_tier(state: AgentState) -> Literal["mid", "smart"]:
    """
    为 generalist 做轻量分级：
    - 常规对比、估值、单一综合问题 -> mid
    - 画图、回测、对冲/相关性、宏观深分析、复杂承接 -> smart
    """
    query = str(state.get("user_query", "") or "").strip()
    lowered = query.lower()

    if _wants_chart(query):
        return "smart"

    if any(keyword in lowered for keyword in GENERALIST_SMART_KEYWORDS):
        return "smart"

    is_followup = bool(state.get("is_followup", False))
    has_context = bool(str(state.get("recent_context", "") or "").strip() or str(state.get("memory_context", "") or "").strip())
    if is_followup and has_context:
        return "smart"

    return "mid"


NEWS_IMPACT_DIRECT_PATTERNS = (
    "为什么涨",
    "为什么跌",
    "为何涨",
    "为何跌",
    "涨什么原因",
    "跌什么原因",
    "上涨原因",
    "下跌原因",
    "怎么传导",
    "如何传导",
    "会怎么传导",
    "先交易什么",
    "先反应什么",
    "市场在交易什么",
    "怎么定价",
)

NEWS_IMPACT_EVENT_KEYWORDS = (
    "新闻",
    "消息",
    "事件",
    "宏观",
    "地缘",
    "冲突",
    "战争",
    "制裁",
    "中东",
    "日本央行",
    "日银",
    "boj",
    "央行",
    "欧央行",
    "欧洲央行",
    "ecb",
    "美联储",
    "非农",
    "cpi",
    "pce",
    "关税",
    "降息",
    "加息",
    "鹰派",
    "鸽派",
    "财报",
    "停火",
    "通胀",
)

NEWS_IMPACT_IMPACT_KEYWORDS = (
    "有什么影响",
    "什么影响",
    "如何影响",
    "影响多大",
    "怎么看",
    "怎么传导",
    "如何传导",
    "会怎么传导",
    "先交易什么",
    "先反应什么",
    "市场在交易什么",
    "怎么定价",
    "利多还是利空",
    "利多利空",
    "怎么解读",
    "怎么理解",
    "对a股",
    "对港股",
    "对黄金",
    "对原油",
    "对美股",
)


def _is_news_impact_query(query: str) -> bool:
    text = str(query or "").strip().lower()
    if not text:
        return False
    if any(pattern in text for pattern in NEWS_IMPACT_DIRECT_PATTERNS):
        return True
    has_event = any(keyword in text for keyword in NEWS_IMPACT_EVENT_KEYWORDS)
    has_impact = any(keyword in text for keyword in NEWS_IMPACT_IMPACT_KEYWORDS)
    return has_event and has_impact


_RECENT_COMPANY_NEWS_TERMS = (
    "最近有什么利好",
    "最近有啥利好",
    "最近有没有利好",
    "最近有什么消息",
    "最近有啥消息",
    "最近有什么动态",
    "最近有没有消息",
    "最近进展",
    "近期动态",
    "近期消息",
    "近期利好",
)


def _is_recent_company_news_query(query: str) -> bool:
    text = str(query or "").strip()
    if not text:
        return False
    return any(term in text for term in _RECENT_COMPANY_NEWS_TERMS)


def _is_link_article_stock_mapping_task(query: str) -> bool:
    return classify_analysis_task_type(query).task_type == TASK_TYPE_LINK_ARTICLE_STOCK_MAPPING


def _extract_link_context_from_query(query: str) -> Dict[str, Any]:
    text = str(query or "")
    if "【链接参考内容】" not in text:
        return {}
    source_match = re.search(r"来源:\s*(.+)", text)
    title_match = re.search(r"标题:\s*(.+)", text)
    snippet = ""
    snippet_match = re.search(r"摘要:\s*(.*?)(?:\n请优先基于以上链接内容回答|$)", text, re.DOTALL)
    if snippet_match:
        snippet = snippet_match.group(1).strip()
    return {
        "ok": bool(snippet),
        "url": source_match.group(1).strip() if source_match else "",
        "title": title_match.group(1).strip() if title_match else "",
        "snippet": snippet,
        "snippet_len": len(snippet),
        "source": "prompt_link_block",
    }


def _get_link_article_context(state: Mapping[str, Any]) -> Dict[str, Any]:
    link_ctx = state.get("link_context") if isinstance(state.get("link_context"), dict) else {}
    if link_ctx.get("ok") and str(link_ctx.get("snippet") or "").strip():
        return dict(link_ctx)
    return _extract_link_context_from_query(str(state.get("user_query", "") or ""))


_ARTICLE_COMPANY_SUFFIXES = (
    "股份",
    "集团",
    "科技",
    "电气",
    "实业",
    "控股",
    "通信",
    "药业",
    "银行",
    "证券",
)
_ARTICLE_COMPANY_PATTERN = re.compile(
    r"[A-Za-z\u4e00-\u9fff]{2,20}(?:%s)" % "|".join(_ARTICLE_COMPANY_SUFFIXES)
)
_ARTICLE_CHAIN_KEYWORDS = (
    "电子特气",
    "高纯钨",
    "钨制品",
    "半导体材料",
    "半导体",
    "芯片",
    "材料",
    "设备",
    "制品",
    "产品",
    "化工",
    "新能源",
    "光伏",
    "储能",
    "电池",
    "机器人",
    "通信",
)


def _short_text(value: Any, limit: int = 240) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def _dedupe_text_items(items: Any, *, limit: int = 6, max_chars: int = 80) -> List[str]:
    if isinstance(items, str):
        raw_items = re.split(r"[\n,，、;；|]+", items)
    elif isinstance(items, (list, tuple, set)):
        raw_items = list(items)
    else:
        raw_items = []
    out: List[str] = []
    seen = set()
    for item in raw_items:
        value = _short_text(item, max_chars)
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
        if len(out) >= limit:
            break
    return out


def _extract_article_json_object(text: str) -> Dict[str, Any]:
    cleaned = str(text or "").strip()
    cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    candidates = [cleaned]
    brace_match = re.search(r"\{[\s\S]*\}", cleaned)
    if brace_match:
        candidates.append(brace_match.group(0))
    for candidate in candidates:
        try:
            data = json.loads(candidate)
        except Exception:
            continue
        if isinstance(data, dict):
            return data
    return {}


def _clean_company_candidate(value: str) -> str:
    text = str(value or "").strip(" ，,。；;：:")
    for marker in ("提到", "包括", "例如", "比如", "关注", "涉及"):
        if marker in text:
            text = text.rsplit(marker, 1)[-1].strip(" ，,。；;：:")
    return text


def _extract_mentioned_companies(text: str) -> List[str]:
    matches = [_clean_company_candidate(item) for item in _ARTICLE_COMPANY_PATTERN.findall(str(text or ""))]
    return _dedupe_text_items(matches, limit=8)


def _extract_industry_chain_items(text: str) -> List[str]:
    source = re.sub(r"\s+", " ", str(text or "")).strip()
    candidates: List[str] = []
    for keyword in _ARTICLE_CHAIN_KEYWORDS:
        if keyword not in source:
            continue
        pattern = rf"[\u4e00-\u9fffA-Za-z0-9]{{0,8}}{re.escape(keyword)}[\u4e00-\u9fffA-Za-z0-9]{{0,8}}"
        matches = re.findall(pattern, source)
        candidates.extend(matches or [keyword])
    cleaned = []
    for item in candidates:
        value = re.sub(r"^(文章|报道|主线|指向|利好|受益|以及|和|及|的)+", "", item).strip()
        value = re.sub(r"(企业|公司|板块|方向)$", "", value).strip() or item
        cleaned.append(value)
    return _dedupe_text_items(cleaned, limit=8, max_chars=40)


def _build_fallback_article_profile(link_ctx: Mapping[str, Any]) -> Dict[str, Any]:
    title = _short_text(link_ctx.get("title"), 160)
    snippet = str(link_ctx.get("snippet") or "").strip()
    first_sentence = re.split(r"[。！？!?]\s*", snippet, maxsplit=1)[0].strip()
    main_event = _short_text(first_sentence or title or "链接文章摘要不足", 220)
    merged_text = f"{title}\n{snippet}"
    chain_items = _extract_industry_chain_items(merged_text)
    mentioned_companies = _extract_mentioned_companies(merged_text)
    missing_info = [
        "候选公司的主营业务占比",
        "订单/产能/客户验证",
        "股价是否已提前反应",
    ]
    if not snippet:
        missing_info.insert(0, "完整正文")
    return {
        "main_event": main_event,
        "industry_chain_items": chain_items,
        "mentioned_companies": mentioned_companies,
        "key_claims": _dedupe_text_items([first_sentence or title], limit=4, max_chars=160),
        "missing_info": missing_info,
        "source": "fallback_parser",
    }


def build_article_event_profile(link_ctx: Mapping[str, Any], llm=None) -> Dict[str, Any]:
    fallback = _build_fallback_article_profile(link_ctx)
    snippet = str(link_ctx.get("snippet") or "").strip()
    if not llm or not snippet:
        return fallback

    prompt = f"""
请把用户提供的财经文章摘要结构化为 JSON。只允许使用摘要里的事实，不要补充常识。

【标题】
{str(link_ctx.get("title") or "").strip()}

【正文摘要】
{snippet[:3000]}

输出 JSON，字段固定为：
{{
  "main_event": "一句话概括文章主线",
  "industry_chain_items": ["文章直接指向的产业链环节或方向"],
  "mentioned_companies": ["文章摘要中明确出现的公司名，没有则空数组"],
  "key_claims": ["价格/供需/政策/订单等关键事实"],
  "missing_info": ["继续做A股映射前需要核验的信息"]
}}
""".strip()
    try:
        response = llm.invoke(prompt)
        data = _extract_article_json_object(str(getattr(response, "content", response) or ""))
    except Exception:
        data = {}
    if not data:
        return fallback

    return {
        "main_event": _short_text(data.get("main_event") or fallback.get("main_event"), 220),
        "industry_chain_items": _dedupe_text_items(
            data.get("industry_chain_items") or fallback.get("industry_chain_items"),
            limit=8,
            max_chars=60,
        ),
        "mentioned_companies": _dedupe_text_items(
            data.get("mentioned_companies") or fallback.get("mentioned_companies"),
            limit=8,
            max_chars=40,
        ),
        "key_claims": _dedupe_text_items(
            data.get("key_claims") or fallback.get("key_claims"),
            limit=6,
            max_chars=160,
        ),
        "missing_info": _dedupe_text_items(
            data.get("missing_info") or fallback.get("missing_info"),
            limit=6,
            max_chars=100,
        ),
        "source": "llm_structured" if data else fallback.get("source", "fallback_parser"),
    }


def _candidate_key(candidate: Mapping[str, Any]) -> str:
    return _short_text(candidate.get("name_or_direction"), 60).lower()


def _candidate_is_company_like(value: str) -> bool:
    return bool(_ARTICLE_COMPANY_PATTERN.fullmatch(str(value or "").strip()))


def _build_candidate(
    name_or_direction: str,
    *,
    source_type: str,
    benefit_logic: str,
    verification_needed: str,
    confidence: str,
) -> Dict[str, Any]:
    return {
        "name_or_direction": _short_text(name_or_direction, 60),
        "source_type": source_type,
        "benefit_logic": _short_text(benefit_logic, 180),
        "verification_needed": _short_text(verification_needed, 140),
        "confidence": confidence,
    }


def map_article_to_a_share_candidates(
    article_profile: Mapping[str, Any],
    search_result: str = "",
) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    seen = set()

    for company in _dedupe_text_items(article_profile.get("mentioned_companies"), limit=8, max_chars=60):
        candidate = _build_candidate(
            company,
            source_type="文章明确提到",
            benefit_logic="文章摘要直接出现该公司名；需核验文章事件与公司业务/订单的实际关系。",
            verification_needed="公告、主营业务、业务占比、事件影响是否已落地",
            confidence="中",
        )
        key = _candidate_key(candidate)
        if key:
            candidates.append(candidate)
            seen.add(key)

    for item in _dedupe_text_items(article_profile.get("industry_chain_items"), limit=8, max_chars=60):
        candidate = _build_candidate(
            item,
            source_type="产业链推导",
            benefit_logic=f"文章主线指向“{item}”，相关 A 股只能作为产业链受益方向继续核验。",
            verification_needed="该方向对应上市公司、主营业务占比、订单/产能弹性",
            confidence="低-中",
        )
        key = _candidate_key(candidate)
        if key and key not in seen:
            candidates.append(candidate)
            seen.add(key)

    for company in _extract_mentioned_companies(search_result):
        if company.lower() in seen:
            continue
        candidate = _build_candidate(
            company,
            source_type="待核验",
            benefit_logic="补证搜索结果中出现该公司，尚不能视为文章明确点名。",
            verification_needed="主营业务是否覆盖文章所述产业链、公告/财报是否有对应披露",
            confidence="低",
        )
        key = _candidate_key(candidate)
        if key:
            candidates.append(candidate)
            seen.add(key)

    if not candidates:
        candidates.append(
            _build_candidate(
                "文章主线相关产业链",
                source_type="待核验",
                benefit_logic="文章摘要不足以定位明确公司，先保留方向性研究线索。",
                verification_needed="补充完整正文、产业链环节和对应上市公司",
                confidence="低",
            )
        )
    return candidates[:10]


def _search_tool_invoke(search_tool: Any, query: str) -> str:
    if not search_tool:
        return ""
    if hasattr(search_tool, "invoke"):
        return str(search_tool.invoke({"query": query}) or "").strip()
    return str(search_tool(query) or "").strip()


def verify_a_share_candidates(
    candidates: List[Mapping[str, Any]],
    *,
    article_profile: Mapping[str, Any] | None = None,
    search_tool: Any = None,
    max_checks: int = 3,
) -> List[Dict[str, Any]]:
    verified: List[Dict[str, Any]] = []
    profile = article_profile or {}
    main_event = _short_text(profile.get("main_event"), 80)
    for index, raw_candidate in enumerate(candidates or []):
        candidate = dict(raw_candidate)
        name = _short_text(candidate.get("name_or_direction"), 60)
        source_type = str(candidate.get("source_type") or "").strip()
        candidate["verification_summary"] = "尚未执行补证检索；只能作为研究线索。"
        candidate["verification_evidence"] = ""

        should_check = index < max(0, int(max_checks or 0)) and bool(search_tool)
        if should_check:
            query = f"{name} 主营业务 公告 财报 {main_event}".strip()
            try:
                evidence = _search_tool_invoke(search_tool, query)
            except Exception as exc:
                evidence = f"补证搜索暂不可用: {exc}"
            evidence = _short_text(evidence, 700)
            candidate["verification_evidence"] = evidence
            if evidence.startswith("补证搜索暂不可用"):
                candidate["verification_summary"] = evidence
            elif evidence:
                candidate["verification_summary"] = "已做轻量补证，仍需核验主营占比、公告原文和事件落地程度。"
                if source_type == "文章明确提到":
                    candidate["confidence"] = "中-高"
                elif _candidate_is_company_like(name):
                    candidate["confidence"] = "中"
            elif source_type == "产业链推导":
                candidate["verification_summary"] = "方向性线索，暂未检索到可直接落到公司的证据。"

        verified.append(candidate)
    return verified


def _build_article_mapping_search_query(
    article_profile: Mapping[str, Any],
    link_ctx: Mapping[str, Any],
) -> str:
    chain = " ".join(_dedupe_text_items(article_profile.get("industry_chain_items"), limit=4, max_chars=30))
    main_event = _short_text(article_profile.get("main_event"), 80)
    title = _short_text(link_ctx.get("title"), 80)
    base = " ".join(item for item in [title, main_event, chain] if item).strip()
    return f"{base[:120]} A股 受益 公司 主营业务".strip()


def _format_article_bullets(items: Any, *, empty: str = "暂无明确可用信息") -> List[str]:
    values = _dedupe_text_items(items, limit=6, max_chars=100)
    if not values:
        return [f"- {empty}"]
    return [f"- {item}" for item in values]


def _format_candidate_table(candidates: List[Mapping[str, Any]]) -> List[str]:
    lines = [
        "| 公司/方向 | 怎么看 | 可信度 | 下一步核验 |",
        "| --- | --- | --- | --- |",
    ]
    for candidate in candidates[:8]:
        lines.append(
            "| {name} | {source}：{logic} | {confidence} | {verify} |".format(
                name=_short_text(candidate.get("name_or_direction"), 40),
                source=_short_text(candidate.get("source_type"), 20),
                logic=_short_text(candidate.get("benefit_logic"), 70),
                confidence=_short_text(candidate.get("confidence"), 16),
                verify=_short_text(candidate.get("verification_needed"), 55),
            )
        )
    return lines


def _format_verification_lines(candidates: List[Mapping[str, Any]]) -> List[str]:
    lines: List[str] = []
    for candidate in candidates[:5]:
        name = _short_text(candidate.get("name_or_direction"), 40)
        source_type = str(candidate.get("source_type") or "").strip()
        summary = _short_text(candidate.get("verification_summary"), 96)
        evidence = _short_text(candidate.get("verification_evidence"), 120)
        if source_type == "产业链推导":
            prefix = "偏方向线索"
        elif source_type == "文章明确提到":
            prefix = "文章点到"
        else:
            prefix = "先放观察池"
        if evidence:
            lines.append(f"- {name}：{prefix}，{summary}")
        else:
            lines.append(f"- {name}：{prefix}，{summary or '还缺补证材料'}")
    return lines or ["- 暂时没有形成可核验的公司线索。"]


def _build_article_mapping_report(
    query: str,
    link_ctx: Mapping[str, Any],
    article_profile: Mapping[str, Any],
    candidates: List[Mapping[str, Any]],
    *,
    search_result: str = "",
) -> str:
    title = _short_text(link_ctx.get("title") or "未提取到标题", 120)
    url = _short_text(link_ctx.get("url"), 160)
    main_event = _short_text(article_profile.get("main_event") or title, 220)
    lines = [
        "【情报与舆情】",
        f"一句话先说：这篇文章真正的看点是“{main_event}”。A股这边先别急着当名单买，比较适合沿着产业链找线索、再逐家公司核验。",
        "",
        "### 这篇文章在说什么",
        f"- 来源：{url or '用户提供链接'}",
        f"- 标题：{title}",
        f"- 主线：{main_event}",
        "",
        "### 先抓住几个关键点",
        *_format_article_bullets(article_profile.get("key_claims"), empty="文章摘要没有给出更多可核验事实"),
        "",
        "### A股可以顺着这些方向看",
        *_format_article_bullets(article_profile.get("industry_chain_items"), empty="文章摘要不足以提取明确产业链方向"),
        "",
        "### 候选公司/方向映射",
        *_format_candidate_table(candidates),
        "",
        "### 核验线索",
        *_format_verification_lines(candidates),
    ]
    missing = _dedupe_text_items(article_profile.get("missing_info"), limit=5, max_chars=100)
    if missing:
        lines.extend(["", "### 还差哪些信息"])
        lines.extend(f"- {item}" for item in missing)
    if search_result:
        lines.extend(["", "### 补证搜索怎么用", "- 我只把搜索结果当作辅助线索，不直接照搬长段原文；公司能不能落地，还要看主营占比和公告。"])
    return "\n".join(lines).strip()


def _build_link_article_mapping_fallback(query: str, link_ctx: Mapping[str, Any], search_result: str = "") -> str:
    title = str(link_ctx.get("title") or "未提取到标题").strip()
    url = str(link_ctx.get("url") or "").strip()
    snippet = str(link_ctx.get("snippet") or "").strip()
    mainline = snippet.split("\n", 1)[0][:180] if snippet else title
    lines = [
        "【情报与舆情】",
        f"一句话先说：这题要先看文章主线，别直接套普通选股模板。{mainline}",
        "",
        "### 这篇文章在说什么",
        f"- 来源：{url or '用户提供链接'}",
        f"- 标题：{title}",
        f"- 摘要：{snippet[:500] if snippet else '链接摘要不足，需要用户补充正文后才能精确映射。'}",
        "",
        "### A股怎么顺着看",
        "- 先看文章直接指向的产业链方向，再找可能相关的 A 股公司。",
        "- 如果文章没明确点名公司，公司层面只能写成“产业链推导/待核验”。",
    ]
    if search_result:
        lines.extend(["", "### 补证线索", f"- {_short_text(search_result, 180)}"])
    return "\n".join(lines).strip()


def _answer_link_article_stock_mapping(state: Mapping[str, Any], llm) -> Dict[str, Any]:
    query = str(state.get("user_query", "") or "").strip()
    link_ctx = _get_link_article_context(state)
    snippet = str(link_ctx.get("snippet") or "").strip()
    if not snippet:
        return {
            "messages": [
                HumanMessage(content=_build_link_article_mapping_fallback(query, link_ctx))
            ]
        }

    article_profile = build_article_event_profile(link_ctx, llm)

    search_result = ""
    search_query = _build_article_mapping_search_query(article_profile, link_ctx)
    if search_query:
        try:
            search_result = str(search_web.invoke({"query": search_query}) or "").strip()
        except Exception as exc:
            search_result = f"补证搜索暂不可用: {exc}"

    candidates = map_article_to_a_share_candidates(article_profile, search_result)
    verified_candidates = verify_a_share_candidates(
        candidates,
        article_profile=article_profile,
        search_tool=search_web,
        max_checks=2,
    )
    content = _build_article_mapping_report(
        query,
        link_ctx,
        article_profile,
        verified_candidates,
        search_result=search_result,
    )
    return {
        "messages": [HumanMessage(content=content)]
    }


def _extract_recent_company_subject(query: str, *, symbol: str = "", symbol_name: str = "") -> str:
    for candidate in (symbol_name, symbol):
        text = str(candidate or "").strip()
        if text and text not in {"未知标的", "无", "None"}:
            return text
    text = re.sub(r"^(帮我|帮忙|请|麻烦)?(看看|看一下|查一下|查查|分析一下)?", "", str(query or "").strip())
    for marker in ("最近", "近期"):
        if marker in text:
            subject = text.split(marker, 1)[0].strip(" ，,。？?的")
            if subject:
                return subject
    return ""


def _clean_finalizer_internal_labels(text: str) -> str:
    cleaned = str(text or "")
    for marker in ("【修正后报告】", "修正后报告：", "修正后报告:"):
        if marker in cleaned:
            cleaned = cleaned.split(marker, 1)[1]
            break
    cleaned = re.sub(r"^【风控修正】\s*", "", cleaned).strip()
    cleaned = re.sub(r"^经审核，原报告未通过。[^\n]*\n*", "", cleaned).strip()
    while True:
        next_cleaned = re.sub(r"^(?:报告审核结论|直接回答|依据说明)[:：][^\n]*(?:\n|$)", "", cleaned).strip()
        if next_cleaned == cleaned:
            break
        cleaned = next_cleaned
    return cleaned


def _invoke_search_web_for_researcher(query: str) -> str:
    return str(search_web.invoke({"query": query}) or "")


class _NodeTimeoutError(TimeoutError):
    pass


def _get_researcher_node_timeout_seconds() -> int:
    raw_value = str(os.getenv("RESEARCHER_NODE_TIMEOUT_SECONDS", "120") or "").strip()
    try:
        value = int(float(raw_value))
    except Exception:
        value = 120
    return max(10, value)


@contextmanager
def _wall_clock_timeout(seconds: int, label: str = "node"):
    safe_seconds = int(seconds or 0)
    if (
        safe_seconds <= 0
        or threading.current_thread() is not threading.main_thread()
        or not hasattr(signal, "SIGALRM")
        or not hasattr(signal, "setitimer")
    ):
        yield
        return

    previous_handler = signal.getsignal(signal.SIGALRM)
    previous_timer = signal.setitimer(signal.ITIMER_REAL, 0)

    def _handle_timeout(_signum, _frame):
        raise _NodeTimeoutError(f"{label} exceeded {safe_seconds}s")

    signal.signal(signal.SIGALRM, _handle_timeout)
    signal.setitimer(signal.ITIMER_REAL, safe_seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        if previous_timer and previous_timer[0] > 0:
            signal.setitimer(signal.ITIMER_REAL, previous_timer[0], previous_timer[1])


def build_generalist_tools():
    return [
        interpret_market_news_tool,
        analyze_kline_pattern, search_investment_knowledge, get_market_snapshot, get_commodity_iv_info,
        scan_iv_change_ranking, scan_volatility_divergence, get_us_option_market_profile, get_us_option_strategy_candidates,
        search_broker_holdings_on_date, tool_analyze_position_change,
        tool_query_specific_option, get_historical_price, get_volume_oi, get_futures_oi_ranking,
        get_option_oi_ranking, get_option_volume_abnormal, get_option_oi_abnormal,
        get_price_statistics, check_option_expiry_status, tool_stock_hedging_analysis,
        tool_futures_correlation_check, tool_stock_correlation_check, calculate_hedging_beta,
        tool_get_retail_money_flow, draw_chart_tool, get_stock_valuation, tool_compare_stocks,
        get_cn_margin_market_signal,
        get_futures_fund_flow, get_futures_fund_ranking, get_futures_margin_profile,
        get_futures_basis_profile, get_futures_inventory_receipt_profile, get_futures_delivery_tospot_profile,
        get_available_patterns, analyze_etf_option_sentiment, get_etf_option_strikes,
        tool_analyze_broker_positions, get_futures_broker_position_signal, get_futures_broker_group_position_moves,
        get_futures_broker_indicator_profile, run_option_strategy_backtest,
        get_macro_indicator, get_macro_overview, analyze_yield_curve
    ]


def build_monitor_tools():
    return [
        tool_get_retail_money_flow,  # 股票行业资金
        get_cn_margin_market_signal,  # A股融资杠杆与风险偏好
        get_futures_fund_flow,  # 期货资金流
        get_futures_fund_ranking,  # 期货沉淀资金排名
        get_futures_margin_profile,  # 保证金/合约乘数
        get_futures_basis_profile,  # 基差/现期结构
        get_futures_inventory_receipt_profile,  # 库存/仓单
        get_futures_delivery_tospot_profile,  # 交割/期转现
        get_commodity_iv_info,  # IV/波动率/Rank
        get_us_option_market_profile,  # 美股/美股ETF期权波动率体检
        scan_iv_change_ranking,  # IV增幅/降幅排名/扫描
        scan_volatility_divergence,  # 价格/IV波动率背离扫描
        search_broker_holdings_on_date,  # 期货商持仓排名
        tool_analyze_position_change,  # 持仓变动分析
        get_option_volume_abnormal,
        get_option_oi_abnormal,
        get_option_oi_ranking,
        get_volume_oi,
        get_market_snapshot,
        get_historical_price,
        get_price_statistics,
        get_recent_price_series,
        check_option_expiry_status,
        tool_analyze_broker_positions,
        get_futures_broker_position_signal,
        get_futures_broker_group_position_moves,
        get_futures_broker_indicator_profile,
        get_futures_oi_ranking,
        query_stock_volume,
        get_macro_indicator
    ]


def build_strategist_tools():
    return [
        get_commodity_iv_info,  # IV排名/波动率
        get_us_option_market_profile,  # 美股/美股ETF期权波动率体检
        get_us_option_strategy_candidates,  # 美股/美股ETF期权策略候选合约
        scan_iv_change_ranking,  # IV增幅/降幅排名/扫描
        scan_volatility_divergence,  # 价格/IV波动率背离扫描
        check_option_expiry_status,  # 到期日状态
        tool_query_specific_option,  # 查询特定期权合约
        get_option_volume_abnormal,  # 期权成交异动
        get_option_oi_abnormal,  # 期权持仓异动
        get_etf_option_strikes,  # ETF期权行权价
        get_market_snapshot,  # 标的快照/现价
        search_investment_knowledge,  # 知识库检索
        run_option_strategy_backtest,  # 期权回测
    ]


def build_chatter_tools():
    return [
        search_investment_knowledge,  # 内部知识库
        search_web,  # 通用联网搜索
        get_market_snapshot,  # 行情快照
        get_futures_margin_profile,  # 保证金/合约乘数
        get_futures_basis_profile,  # 基差/现期结构
        get_futures_inventory_receipt_profile,  # 库存/仓单
        get_futures_delivery_tospot_profile,  # 交割/期转现
    ]


def _select_knowledge_chat_strategy(state: AgentState) -> str:
    focus_mode_hint = str(state.get("focus_mode_hint", "") or "").strip().lower()
    focus_topic = str(state.get("focus_topic", "") or "").strip()
    user_query = str(state.get("user_query", "") or "").strip().lower()
    recent_context = str(state.get("recent_context", "") or "").strip().lower()

    if _is_freshness_required_state(state):
        return "company_news"
    if focus_mode_hint == "company_news" or focus_topic == "公司近期动态":
        return "company_news"
    if any(keyword in user_query for keyword in ("最近有什么好消息", "最近有没有好消息", "最近有什么动态", "最近进展", "最近催化")):
        return "company_news"
    if any(keyword in user_query for keyword in ("最近财报", "最新财报", "最近公告", "最新公告", "最近新闻", "最新新闻", "最近消息", "最新消息", "最近业绩", "最新业绩")):
        return "company_news"
    if _looks_like_listing_status_query(user_query):
        return "company_news"
    if _looks_like_latest_company_fact_query(user_query):
        return "company_news"
    if any(keyword in recent_context for keyword in ("最近有什么好消息", "最近有没有好消息", "最近有什么动态", "最近进展", "最近催化")):
        return "company_news"
    return "concept_explain"


_AGENT_TRACE_REDIS = None
_AGENT_TRACE_REDIS_INIT_ATTEMPTED = False


def _get_agent_trace_redis():
    global _AGENT_TRACE_REDIS, _AGENT_TRACE_REDIS_INIT_ATTEMPTED
    if _AGENT_TRACE_REDIS_INIT_ATTEMPTED:
        return _AGENT_TRACE_REDIS
    _AGENT_TRACE_REDIS_INIT_ATTEMPTED = True
    try:
        import redis

        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        _AGENT_TRACE_REDIS = redis.from_url(redis_url, decode_responses=True)
    except Exception:
        _AGENT_TRACE_REDIS = None
    return _AGENT_TRACE_REDIS


def _append_agent_trace_event(state: Mapping[str, Any], event: str, payload: Mapping[str, Any] | None = None) -> None:
    task_id = str(state.get("task_id", "") or "").strip()
    if not task_id:
        return
    append_chat_trace_event(_get_agent_trace_redis(), task_id, event, payload or {})


def _state_context_payload(state: Mapping[str, Any], *, recent_context_override: str = "") -> Dict[str, Any]:
    payload = {
        "recent_context": recent_context_override if recent_context_override else str(state.get("recent_context", "") or ""),
        "memory_context": str(state.get("memory_context", "") or ""),
        "profile_context": str(state.get("profile_context", "") or ""),
        "conversation_memory_query": bool(state.get("conversation_memory_query", False)),
        "conversation_memory_label": str(state.get("conversation_memory_label", "") or ""),
        "is_followup": bool(state.get("is_followup", False)),
        "focus_entity": str(state.get("focus_entity", "") or ""),
        "focus_topic": str(state.get("focus_topic", "") or ""),
        "focus_aspect": str(state.get("focus_aspect", "") or ""),
        "focus_mode_hint": str(state.get("focus_mode_hint", "") or ""),
        "followup_goal": str(state.get("followup_goal", "") or ""),
        "followup_action_context": str(state.get("followup_action_context", "") or ""),
        "followup_task_policy": state.get("followup_task_policy") or {},
        "followup_route_context": str(state.get("followup_route_context", "") or ""),
        "correction_intent": bool(state.get("correction_intent", False)),
        "intent_domain": str(state.get("intent_domain", "") or ""),
        "quick_answer_target": str(state.get("quick_answer_target", "") or ""),
        "quick_answer_direction": str(state.get("quick_answer_direction", "") or ""),
        "quick_answer_scenario": str(state.get("quick_answer_scenario", "") or ""),
        "freshness_required": bool(state.get("freshness_required", False)),
        "freshness_quick_status": str(state.get("freshness_quick_status", "") or ""),
        "freshness_query_target": str(state.get("freshness_query_target", "") or ""),
        "link_context": state.get("link_context") if isinstance(state.get("link_context"), dict) else {},
    }
    context_layers = state.get("context_layers")
    if isinstance(context_layers, list):
        payload["context_layers"] = context_layers
    context_layer_summary = state.get("context_layer_summary")
    if isinstance(context_layer_summary, list):
        payload["context_layer_summary"] = context_layer_summary
    return payload


def _simple_context_payload(
    *,
    recent_context: str = "",
    memory_context: str = "",
    profile_context: str = "",
    is_followup: bool = False,
    focus_entity: str = "",
    focus_topic: str = "",
    focus_aspect: str = "",
    conversation_memory_query: bool = False,
    conversation_memory_label: str = "",
    context_payload: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    payload = dict(context_payload or {})
    legacy_values = {
        "recent_context": recent_context,
        "memory_context": memory_context,
        "profile_context": profile_context,
        "is_followup": is_followup,
        "focus_entity": focus_entity,
        "focus_topic": focus_topic,
        "focus_aspect": focus_aspect,
        "conversation_memory_query": conversation_memory_query,
        "conversation_memory_label": conversation_memory_label,
    }
    for key, value in legacy_values.items():
        if key not in payload or payload.get(key) in (None, ""):
            payload[key] = value
    return payload


_LATEST_COMPANY_FACT_KEYWORDS = (
    "最近",
    "最新",
    "今天",
    "今日",
    "当前",
    "财报",
    "公告",
    "新闻",
    "消息",
    "动态",
    "业绩",
    "IPO",
    "ipo",
    "上市",
    "挂牌",
    "交易",
    "股票代码",
)
_LATEST_FACT_TIME_KEYWORDS = (
    "最近", "最新", "近期", "今天", "今日", "当前", "今年", "本年", "本月", "这个月", "一季度", "第一季度",
)
_COMPANY_FACT_TOPIC_KEYWORDS = (
    "财报", "公告", "新闻", "消息", "动态", "业绩", "年报", "季报", "一季报", "半年报", "中报",
    "IPO", "ipo", "上市", "挂牌", "交易", "股票代码", "纳斯达克", "NASDAQ", "Nasdaq", "NYSE", "SEC", "S-1", "招股书",
)
_LISTING_STATUS_QUERY_KEYWORDS = (
    "IPO", "ipo", "上市", "挂牌", "交易", "股票代码", "ticker", "纳斯达克", "nasdaq", "NYSE", "nyse", "SEC", "S-1", "招股书",
)
_LISTING_EVIDENCE_KEYWORDS = (
    "SPCX", "股票代码", "ticker", "纳斯达克", "NASDAQ", "Nasdaq", "NYSE", "IPO价格", "IPO price",
    "开始交易", "上市交易", "公开交易", "listed", "trading", "shares", "stock", "纳斯达克100",
)
_LISTING_QUOTE_ALIASES = {
    "spacex": {
        "ticker": "SPCX",
        "name": "SpaceX",
        "market": "美股",
        "exchange": "NASDAQ",
    },
}
_LISTING_QUOTE_ALLOWED_TYPES = {"EQUITY"}
_LISTING_QUOTE_EXCLUDED_SYMBOL_PARTS = ("=", "-USD", "ZZX")
_LISTING_QUOTE_EXCLUDED_NAME_HINTS = (
    "etf", "fund", "mutual fund", "tokenized", "prestock", "prestocks",
    "derivatives", "company level", "2x", "3x", "short daily", "short spacex",
    "proshares", "ultra", "highshares", "graniteshares", "tradr",
)
_LISTING_QUOTE_PREFERRED_EXCHANGES = {"NMS", "NYQ", "NGM", "NCM", "ASE"}
_FRESHNESS_STALE_ANSWER_HINTS = (
    "知识更新时间", "训练数据", "截至我", "无法实时", "不能实时", "没有实时联网", "目前没有能力",
    "仍是私营", "仍然是私营", "还是私营", "保持私有", "没有正式上市", "尚未确定IPO",
    "尚未宣布IPO", "尚未提交任何IPO", "未在Nasdaq", "未在NYSE", "没有近期IPO计划", "暂无IPO计划",
    "私营公司", "尚未公开宣布IPO", "尚未进行首次公开募股", "尚未上市", "尚未选择在纳斯达克",
)


def _looks_like_latest_company_fact_query(query: str) -> bool:
    query_text = str(query or "")
    return (
        any(keyword in query_text for keyword in _LATEST_FACT_TIME_KEYWORDS)
        and any(keyword in query_text for keyword in _COMPANY_FACT_TOPIC_KEYWORDS)
    )


def _looks_like_listing_status_query(query: str) -> bool:
    query_text = str(query or "")
    if not query_text:
        return False
    has_listing_topic = any(keyword in query_text for keyword in _LISTING_STATUS_QUERY_KEYWORDS)
    has_time_or_question = any(keyword in query_text for keyword in _LATEST_FACT_TIME_KEYWORDS) or any(
        keyword in query_text for keyword in ("是不是", "是否", "有没有", "要上市", "已上市", "上市了吗")
    )
    return has_listing_topic and has_time_or_question


def _is_freshness_required_state(state: Mapping[str, Any] | None) -> bool:
    payload = state or {}
    if bool(payload.get("freshness_required", False)):
        return True
    return str(payload.get("quick_answer_scenario") or "").strip().lower() == "freshness"


def _freshness_query_target_from_state(state: Mapping[str, Any] | None, query: str) -> str:
    payload = state or {}
    for key in ("freshness_query_target", "quick_answer_target", "focus_entity", "symbol_name", "symbol"):
        value = str(payload.get(key) or "").strip()
        if value:
            return value[:40]
    text = str(query or "").strip()
    for keyword in (
        "今天", "今日", "当前", "最新", "是不是", "是否", "有没有", "要不要", "上市", "IPO", "ipo",
        "挂牌", "交易", "股票代码", "吗", "呢", "么",
    ):
        text = text.replace(keyword, " ")
    text = re.sub(r"[，。！？、,.!?；;：:\s]+", " ", text).strip()
    for chunk in [part.strip(" -_/（）()[]【】") for part in text.split(" ") if part.strip()]:
        if 2 <= len(chunk) <= 40:
            return chunk
    return str(query or "").strip()[:40]


def _build_freshness_deep_search_query(query: str, state: Mapping[str, Any] | None = None) -> str:
    target = _freshness_query_target_from_state(state, query)
    today = datetime.now().strftime("%Y-%m-%d")
    if _looks_like_listing_status_query(query):
        subject = target or query
        return f"{subject} IPO stock ticker exchange Nasdaq NYSE listed latest {today}"
    if target:
        return f"{target} 最新 官方公告 新闻 {today} {query}"
    return f"{query} 最新 官方公告 新闻 {today}"


def _freshness_search_answer_has_evidence(query: str, answer: str) -> bool:
    answer_text = str(answer or "").strip()
    if not answer_text:
        return False
    stale_cutoff = datetime.now().year - 1
    if any(int(year) < stale_cutoff for year in re.findall(r"截至\s*((?:19|20)\d{2})", answer_text)):
        return False
    if any(hint in answer_text for hint in _FRESHNESS_STALE_ANSWER_HINTS):
        return False
    if not is_search_answer_acceptable(query, answer_text):
        return False
    if _looks_like_listing_status_query(query):
        return any(keyword.lower() in answer_text.lower() for keyword in _LISTING_EVIDENCE_KEYWORDS)
    return True


def _normalize_listing_quote_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def _make_yfinance_session() -> Any:
    try:
        from curl_cffi import requests as curl_requests

        session = curl_requests.Session(impersonate="chrome")
        session.trust_env = False
        return session
    except Exception:
        return None


def _resolve_listing_quote_alias(query: str, state: Mapping[str, Any] | None = None) -> Dict[str, str]:
    candidates = [
        _freshness_query_target_from_state(state, query),
        str(query or ""),
    ]
    for candidate in candidates:
        key = _normalize_listing_quote_key(candidate)
        if key in _LISTING_QUOTE_ALIASES:
            return dict(_LISTING_QUOTE_ALIASES[key])
        for alias_key, meta in _LISTING_QUOTE_ALIASES.items():
            if alias_key and alias_key in key:
                return dict(meta)
    return {}


def _listing_quote_target(query: str, state: Mapping[str, Any] | None = None) -> str:
    target = _freshness_query_target_from_state(state, query)
    if target:
        text = target
    else:
        text = str(query or "").strip()
    for keyword in ("已经", "已", "是否", "是不是", "有没有", "上市", "IPO", "ipo", "了吗", "吗", "了"):
        text = re.sub(re.escape(keyword), " ", text, flags=re.I)
    text = re.sub(r"[，。！？、,.!?；;：:\s]+", " ", text).strip()
    return text or target or str(query or "").strip()


def _listing_quote_candidate_score(candidate: Mapping[str, Any], target: str) -> float:
    symbol = str(candidate.get("symbol") or "").strip().upper()
    quote_type = str(candidate.get("quoteType") or "").strip().upper()
    if quote_type not in _LISTING_QUOTE_ALLOWED_TYPES or not symbol:
        return -1.0
    if any(part in symbol for part in _LISTING_QUOTE_EXCLUDED_SYMBOL_PARTS):
        return -1.0

    name_text = " ".join(
        str(candidate.get(key) or "")
        for key in ("shortname", "longname", "typeDisp", "exchDisp")
    ).strip()
    lower_name = name_text.lower()
    if any(hint in lower_name for hint in _LISTING_QUOTE_EXCLUDED_NAME_HINTS):
        return -1.0

    target_norm = _normalize_listing_quote_key(target)
    candidate_norm = _normalize_listing_quote_key(f"{symbol} {name_text}")
    if len(target_norm) < 4 and target_norm not in candidate_norm:
        return -1.0

    try:
        score = float(candidate.get("score") or 0)
    except Exception:
        score = 0.0
    if str(candidate.get("exchange") or "").strip().upper() in _LISTING_QUOTE_PREFERRED_EXCHANGES:
        score += 5000.0
    if target_norm and target_norm in candidate_norm:
        score += 10000.0
    return score


def _filter_listing_quote_candidates(candidates: Any, target: str, *, max_candidates: int = 3) -> List[Dict[str, str]]:
    scored: List[tuple[float, Dict[str, str]]] = []
    for raw in candidates or []:
        if not isinstance(raw, Mapping):
            continue
        score = _listing_quote_candidate_score(raw, target)
        if score < 0:
            continue
        item = {
            "ticker": str(raw.get("symbol") or "").strip().upper(),
            "name": str(raw.get("longname") or raw.get("shortname") or raw.get("symbol") or "").strip(),
            "market": str(raw.get("exchDisp") or raw.get("exchange") or "公开市场").strip(),
            "exchange": str(raw.get("exchange") or "").strip(),
        }
        scored.append((score, item))
    scored.sort(key=lambda pair: pair[0], reverse=True)
    output: List[Dict[str, str]] = []
    seen: set[str] = set()
    for _score, item in scored:
        ticker = item["ticker"]
        if ticker in seen:
            continue
        seen.add(ticker)
        output.append(item)
        if len(output) >= max_candidates:
            break
    return output


def _search_listing_quote_candidates(target: str, *, timeout_seconds: float) -> List[Dict[str, str]]:
    if not str(target or "").strip():
        return []
    try:
        import yfinance as yf

        search = yf.Search(
            str(target).strip(),
            max_results=8,
            news_count=0,
            lists_count=0,
            timeout=timeout_seconds,
            raise_errors=False,
            session=_make_yfinance_session(),
        )
        return _filter_listing_quote_candidates(getattr(search, "quotes", []) or [], target)
    except Exception as exc:
        print(f"[knowledge freshness gate] listing quote search failed for {target}: {exc}")
        return []


def _download_listing_quote_frame(ticker: str, *, timeout_seconds: float) -> Any:
    import yfinance as yf

    return yf.download(
        str(ticker or "").strip().upper(),
        period="10d",
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
        timeout=timeout_seconds,
        session=_make_yfinance_session(),
    )


def _extract_latest_close_from_yfinance_frame(frame: Any) -> tuple[str, float | None]:
    try:
        if frame is None or frame.empty:
            return "", None
        close_obj = frame["Close"]
        if hasattr(close_obj, "columns"):
            close_series = close_obj.iloc[:, 0]
        else:
            close_series = close_obj
        close_series = close_series.dropna()
        if close_series.empty:
            return "", None
        last_date = close_series.index[-1]
        date_text = last_date.strftime("%Y-%m-%d") if hasattr(last_date, "strftime") else str(last_date)[:10]
        return date_text, float(close_series.iloc[-1])
    except Exception:
        return "", None


def _try_listing_quote_status_answer(query: str, state: Mapping[str, Any] | None = None) -> str:
    if not _looks_like_listing_status_query(query):
        return ""
    target = _listing_quote_target(query, state)
    try:
        timeout_seconds = float(str(os.getenv("FRESHNESS_LISTING_QUOTE_TIMEOUT_SECONDS", "5")).strip() or 5)
        candidates: List[Dict[str, str]] = []
        alias = _resolve_listing_quote_alias(query, state)
        if alias:
            alias_item = {
                "ticker": str(alias.get("ticker") or "").strip().upper(),
                "name": str(alias.get("name") or alias.get("ticker") or "").strip(),
                "market": str(alias.get("market") or "公开市场").strip(),
                "exchange": str(alias.get("exchange") or "").strip(),
            }
            if alias_item["ticker"]:
                candidates.append(alias_item)
        for item in _search_listing_quote_candidates(target, timeout_seconds=timeout_seconds):
            if all(existing.get("ticker") != item.get("ticker") for existing in candidates):
                candidates.append(item)

        for candidate in candidates[:3]:
            ticker = str(candidate.get("ticker") or "").strip().upper()
            if not ticker:
                continue
            frame = _download_listing_quote_frame(ticker, timeout_seconds=timeout_seconds)
            quote_date, close_price = _extract_latest_close_from_yfinance_frame(frame)
            if not quote_date:
                continue
            name = str(candidate.get("name") or target or ticker).strip()
            market = str(candidate.get("market") or "公开市场").strip()
            price_text = f"，最近收盘价约 {close_price:.2f} 美元" if close_price is not None else ""
            return (
                f"【实时核验】\n{name} 已能通过公开行情源查到{market}股票代码 {ticker} 的近期交易数据；"
                f"最近交易日为 {quote_date}{price_text}。"
                "所以对“是否已经上市/公开交易”这类问题，当前应回答：已上市/已公开交易。"
                "上市方式、发行价和交易所公告仍建议继续以交易所、SEC 或公司公告核验。\n\n"
                "仅供研究参考，不构成投资建议。"
            )
    except Exception as exc:
        print(f"[knowledge freshness gate] listing quote lookup failed for {target}: {exc}")
        return ""
    return ""


def _is_latest_company_fact_query(knowledge_strategy: str, query: str) -> bool:
    if knowledge_strategy != "company_news":
        return False
    query_text = str(query or "")
    return (
        any(keyword in query_text for keyword in _LATEST_COMPANY_FACT_KEYWORDS)
        or _looks_like_latest_company_fact_query(query_text)
        or _looks_like_listing_status_query(query_text)
    )


def _insufficient_latest_company_fact_message() -> str:
    return "我这轮没有检索到足够新的公开资料。你可以指定报告期、公告来源或股票代码，我再帮你精确查。"


def _insufficient_freshness_fact_message(query: str) -> str:
    if _looks_like_listing_status_query(query):
        return (
            "我这轮没有检索到足够新的上市/IPO公开证据，不能凭模型记忆判断是否已上市。"
            "请稍后再试，或补充股票代码、交易所公告/SEC链接，我再精确核验。"
        )
    return (
        "我这轮没有检索到足够新的公开资料，不能凭模型记忆下实时事实结论。"
        "请稍后再试，或补充公告/新闻链接，我再精确核验。"
    )


def _invoke_search_web_direct(query: str) -> str:
    try:
        if hasattr(search_web, "invoke"):
            return str(search_web.invoke({"query": query}) or "").strip()
        return str(search_web(query) or "").strip()
    except TypeError:
        return str(search_web(query) or "").strip()
    except Exception as exc:
        print(f"[knowledge fast path] search_web failed: {exc}")
        return ""


def _try_direct_company_fact_search(query: str, knowledge_strategy: str) -> str | None:
    if not _is_latest_company_fact_query(knowledge_strategy, query):
        return None
    if _looks_like_listing_status_query(query):
        return _run_freshness_search_with_gate(query, {})

    answer = _invoke_search_web_direct(query)
    if answer and is_search_answer_acceptable(query, answer):
        return answer
    if answer:
        preview = answer.replace("\n", " ")[:160]
        print(f"[knowledge fast path] rejected stale/low-quality search answer: {preview}")
    return _insufficient_latest_company_fact_message()


def _run_freshness_search_with_gate(
    query: str,
    state: Mapping[str, Any] | None = None,
) -> str:
    quote_answer = _try_listing_quote_status_answer(query, state)
    if quote_answer:
        return quote_answer

    search_query = _build_freshness_deep_search_query(query, state)
    answer = _invoke_search_web_direct(search_query)
    if answer and _freshness_search_answer_has_evidence(query, answer):
        return f"【实时核验】\n{answer}"
    if answer:
        preview = answer.replace("\n", " ")[:160]
        print(f"[knowledge freshness gate] rejected stale/low-quality search answer: {preview}")
    return _insufficient_freshness_fact_message(query)


def _try_direct_freshness_fact_search(
    query: str,
    state: Mapping[str, Any] | None = None,
) -> str | None:
    if not _is_freshness_required_state(state):
        return None
    return _run_freshness_search_with_gate(query, state)


def simple_chatter_reply(
    user_query: str,
    llm,
    *,
    recent_context: str = "",
    memory_context: str = "",
    profile_context: str = "",
    is_followup: bool = False,
    focus_entity: str = "",
    focus_topic: str = "",
    focus_aspect: str = "",
    conversation_memory_query: bool = False,
    conversation_memory_label: str = "",
    messages: List[BaseMessage] | None = None,
    runtime_context: Mapping[str, Any] | None = None,
    context_payload: Mapping[str, Any] | None = None,
) -> str:
    direct_answer = maybe_answer_simple_runtime_question(user_query, runtime_context)
    if direct_answer:
        return direct_answer

    simple_payload = _simple_context_payload(
        recent_context=recent_context,
        memory_context=memory_context,
        profile_context=profile_context,
        is_followup=is_followup,
        focus_entity=focus_entity,
        focus_topic=focus_topic,
        focus_aspect=focus_aspect,
        conversation_memory_query=conversation_memory_query,
        conversation_memory_label=conversation_memory_label,
        context_payload=context_payload,
    )
    history_text = str(simple_payload.get("recent_context") or "").strip()
    runtime_text = format_simple_runtime_context(runtime_context)

    if not history_text and messages:
        history_lines = []
        for msg in messages[:-1]:
            if isinstance(msg, HumanMessage):
                history_lines.append(f"用户: {msg.content[:220]}")
            elif isinstance(msg, AIMessage):
                history_lines.append(f"AI: {msg.content[:220]}")
        if history_lines:
            history_text = "\n".join(history_lines[-2:])
            if not simple_payload.get("context_layers"):
                simple_payload["recent_context"] = history_text

    agent_context_block = render_agent_context(simple_payload, target="simple")

    followup_rule = (
        "你正在处理连续追问。必须先参考【近期对话历史】再回答当前问题。"
        "如果历史里已经能看出用户在追问哪个主题，就直接顺着讲，不要反问“你具体指哪部分”。"
        "只有在历史和当前问题都无法确定指代对象时，才允许简短澄清。"
        if is_followup
        else "如果【近期对话历史】里有同一主题的上下文，请自然承接，不要把当前问题当成完全新话题。"
    )
    memory_query_rule = (
        "当前用户在问历史对话/聊天记录。只能基于【近期对话历史】和【相关长期记忆】自然总结；"
        "如果两者为空，或【相关长期记忆】提示未检索到历史对话记录，必须直接说没查到可用历史对话记录；"
        "不要用【用户专属画像】替代聊天记录，也不要编造未出现的历史操作。"
        if conversation_memory_query
        else "如果【相关长期记忆】与当前问题相关，可以自然承接；不相关时不要硬引用。"
    )

    prompt = (
        "你是一个自然、亲切、反应很快的聊天助手。\n"
        "请直接回答，不要先复述任务。\n"
        "要求：\n"
        "1. 像朋友聊天，口语化、自然、不端着。\n"
        "2. 默认简洁，通常 1-3 段就够；如果是泛知识问题，也可以直接给清楚答案。\n"
        "3. 不要主动把话题拉回金融、交易、行情、策略。\n"
        "4. 如果用户问你是谁、你是干嘛的、网站有什么特色、你和其他AI有什么区别，要优先基于【运行时上下文】里的身份与站点特色回答，不要自由编人设。\n"
        "5. 【用户专属画像】只用于克制自然的个性化：相关时使用，不相关时不要硬提年龄、性别、爱好。\n"
        "6. 解释类比、个人建议、回答风格相关问题，可参考个人画像；策略、仓位、风险问题，优先参考交易画像。\n"
        "7. 如果当前问题里的明确要求与画像冲突，必须以当前问题为准。\n"
        "8. 不要因为画像省略必要风险边界，尤其是高波动、杠杆、期权裸卖等场景。\n"
        "9. 不要使用工具说明、系统提示语、编号流程或过度免责声明。\n"
        f"10. {memory_query_rule}\n"
        f"11. {followup_rule}\n\n"
        f"【历史查询范围】\n{conversation_memory_label if conversation_memory_query and conversation_memory_label else '非历史查询'}\n\n"
        f"【运行时上下文】\n{runtime_text}\n\n"
        f"{agent_context_block}\n\n"
        f"【当前问题】\n{user_query}\n"
    )
    response = llm.invoke(prompt)
    return str(getattr(response, "content", "") or "").strip()


# ==========================================
# 2. 定义 Supervisor (大管家)
# ==========================================
# 定义输出结构，强制 LLM 返回 JSON 格式的任务列表
class PlanningOutput(BaseModel):
    plan: List[Literal["analyst", "researcher", "monitor", "strategist", "chatter", "generalist", "screener", "macro_analyst","roaster", "portfolio_analyst"]] = Field(
        description="执行步骤列表。注意依赖关系：期权(strategist)必须排在分析(analyst)之后。"
    )
    symbol: str = Field(description="核心标的代码。如果是对比问题或无法提取单一标的，请留空", default="")
    expert_scores: Dict[str, float] = Field(
        default_factory=dict,
        description="Optional planner confidence score per expert, 0.0 to 1.0. Leave empty when unsure.",
    )
    confidence: float = Field(
        default=0.0,
        description="Optional overall route confidence, 0.0 to 1.0. Use 0 when unsure.",
    )
    route_reason: str = Field(
        default="",
        description="Optional short reason for selecting this route.",
    )


def supervisor_node(state: AgentState, llm):
    """
    大管家节点：分析用户意图，生成任务清单
    """
    query = state["user_query"]
    messages = state.get("messages", [])
    is_followup = bool(state.get("is_followup", False))
    recent_context = str(state.get("recent_context", "") or "").strip()
    followup_goal = str(state.get("followup_goal", "") or "").strip()
    followup_action_context = str(state.get("followup_action_context", "") or "").strip()
    followup_task_policy = state.get("followup_task_policy", {}) or {}
    if not isinstance(followup_task_policy, dict):
        followup_task_policy = {}
    followup_route_context = str(state.get("followup_route_context", "") or "").strip()
    is_execute_suggested_stock_selection = (
        is_followup
        and followup_goal == "execute_suggested_action"
        and "stock_selection" in followup_action_context
    )
    has_portfolio = bool(state.get("has_portfolio", False))  # 🔥 新增：获取持仓状态

    history_text = recent_context
    if not history_text and len(messages) > 1:
        # 兜底：若前端未传 recent_context，退回到消息列表抽取最近两条
        history_lines = []
        for msg in messages[:-1]:
            if isinstance(msg, HumanMessage):
                history_lines.append(f"用户: {msg.content[:220]}")
            elif isinstance(msg, AIMessage):
                history_lines.append(f"AI: {msg.content[:220]}")
        if history_lines:
            history_text = "\n".join(history_lines[-2:])

    context_payload = _state_context_payload(state, recent_context_override=history_text)
    agent_context_block = render_agent_context(context_payload, target="supervisor")

    # 🔥 新增：持仓状态提示
    portfolio_status = f"\n【重要】用户{'已上传' if has_portfolio else '未上传'}持仓数据。" if has_portfolio else ""
    subject_policy = build_subject_policy(query)
    subject_policy_context = subject_policy.as_prompt_context()

    system_prompt = f"""
    你是交易团队的主管，根据问题制定计划。
    {portfolio_status}
    {agent_context_block}
    {subject_policy_context}

    【可用员工】
    - analyst: 技术分析师 (看K线、定趋势),分析如何操作
    - monitor: 数据监控员 (看期货资金流、基差/库存仓单/交割期转现、期货商持仓、查期货持仓量、查价格)
    - researcher: 情报研究员 (看新闻、宏观、热点、地缘政治、货币政策、Polymarket上的概率分析)
    - strategist: 期权策略员 (给策略，**必须依赖 analyst**)
    - screener: 股票大师 (协助"推荐股票"、"选股"、查股票成交量、资金流)
    - portfolio_analyst: 持仓分析师 (分析用户持仓结构、风险、交易风格，给个性化建议) {'✅ 用户已上传持仓，可用' if has_portfolio else '❌ 用户未上传持仓，不可用'}
    - chatter: 知识问答和闲聊 (例如解释一下IV，什么是牛市价差，"最近美联储什么时候开会")
    - generalist: 【王牌分析师】处理对比(A和B谁强)、多品种分析、画价差图或深度复杂问题。
    - macro_analyst: 宏观策略师 (分析美联储、美债、美元、通胀、CPI、非农、画利率图)
    - roaster: *毒舌分析师* (当用户要求"吐槽"、"挑战我"、"毒舌模式"时使用)。

    【调度规则 (严格遵守)】
    1. **追求效率**: 问股票成交量就只派 `screener`；只问期货持仓量或价格就只派 `monitor`；只问新闻或热点就只派 `researcher`；只问技术分析就只派`analyst`；只问行情分析就只派`analyst`。
    1.0 **画像使用**: 【用户专属画像】只辅助路由和个性化表达；“适合我/我的风格/给我讲简单点/用我喜欢的方式解释”等问题，可参考画像决定是否派 `portfolio_analyst`、`strategist` 或 `chatter`。当前问题明确表达优先。
    1.1 **纯期权数据问题**：只问波动率/IV/IV Rank/到期日/剩余天数/行权价/保证金/合约乘数/一手资金占用，这类问题一律只派 `monitor`，不要派 `macro_analyst`、`analyst`、`strategist`、`researcher`。
    1.2 **升波/降波判断**：凡是问“现在/最近/当前上涨或下跌后会升波还是降波”，这是窄口径行情+IV方向判断，默认只派 `['monitor']` 查 IV/价格并直接回答；不要派 `chatter` 做知识解释，也不要派 `analyst` 展开完整K线技术分析。只有用户同时问“策略/怎么做/开仓/对冲”时，才派 `['monitor', 'strategist']`。
    2. **全套服务**: 如果用户问"全面分析"或"详细分析"，默认路径: ["analyst", "monitor", "researcher","strategist"]。
    3. **持仓相关** (仅当用户已上传持仓时): 如果用户提到"我的持仓"、"我的股票"、"仓位"、"持仓风险"、"持仓分析"、"适合我"、"个性化建议"、"我的风格"、"持仓建议"、"调仓"、"加仓"、"减仓"等关键词，**必须**派 `portfolio_analyst`。
    4. **期权交易决策**: "500ETF适合价差还是裸买"、"推荐白银期权策略" ->
       - 只要标的明确(500ETF)，且涉及期权交易，一律走流水线。
       - Plan: `['analyst', 'monitor', 'strategist']`（方向与行情/IV并行核验后再出策略）。
       - 决策表达即使同时出现价格、IV、到期日等数据词，也不得降级为纯数据查询。
    4.1 **标的上下文**: 遵守【标的上下文策略】；概念题交给 `chatter`，需要落地但缺少交易对象时也先由 `chatter` 澄清。
    5. **多品种/对比**: 问"白银和黄金谁强"、"分析一下螺纹和热卷" ->
       - symbol 填 "白银,黄金" (用逗号分隔)
       - plan 派 `['generalist']` (让王牌去处理多品种)。
    6. **宏观/大宗/贵金属**:
       - 问 "现在宏观环境怎么样"、"美联储降息了吗" -> Plan: `['researcher', 'macro_analyst']` (先找新闻，再分析数据)。
       - 问 "黄金/白银/能买吗" -> Plan: `['analyst', 'researcher', 'macro_analyst', 'strategist']` (黄金对宏观极度敏感，必须加宏观分析)。
       - 问 "利率/美元走势" -> Plan: `['researcher', 'macro_analyst']`。
    7. **客户提到股票**，但没有明确说明标的名字，需要选股时，只 Plan:['screener']
    8. **知识/百科/闲聊**: 问概念、问人名、问名词 -> 派 ['chatter']。
    9. 如果用户的问题很模糊 (例如"帮我分析一下"，"黄金怎么看")，要先派chatter去问清楚问题 -> plan=['chatter']。
    10. 只问K线或技术面分析时，只要派analyst，不要再派其他人
    11. 如果客户要画图，派 `['generalist']` 。
    12. 用户问保证金/合约乘数/一手资金占用，或基差/库存仓单/交割期转现等数据问题，优先派 `monitor`；若同时要策略建议，可派 `['monitor','strategist']`。
    12.1 用户问A股融资余额/融资买入/两融/杠杆资金，或分析大盘、A股指数、A股ETF及ETF期权市场环境时，必须派 `monitor` 调用融资市场信号工具；涉及期权策略时再派 `strategist`。
    13. 如果【追问派工策略】给出了推荐专家，优先遵守；override_level 为 force 时必须直接采用推荐专家。
    """

    if is_followup:
        system_prompt += """

    【连续追问模式（强约束）】
    1. 当前问题是对上一轮的追问，必须先承接上一轮关键结论，再回答当前问题。
    2. 不要把所有追问默认交给 `generalist`；只有跨节点综合、对比、画图、复杂复盘才优先 `generalist`。
    3. 禁止把“知识库命中为空”当作默认回答模板。
    4. 若【上一轮可执行建议】给出了可执行筛选动作，当前问题是在执行该建议，应直接按建议动作派工。
        """

    full_query = query
    if is_followup:
        full_query = (
            f"【连续追问模式】是\n"
            f"【当前问题】\n{query}"
        )
    elif history_text:
        full_query = f"【当前问题】\n{query}"

    # 使用 structured_output 强制输出 JSON
    planner = llm.with_structured_output(PlanningOutput)
    prompt = ChatPromptTemplate.from_messages([
        SystemMessage(content=system_prompt),
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

    # 回测问题 -> 直接交给通才处理，避免无关节点
    if "回测" in query or "策略回测" in query:
        final_plan = ["generalist"]

    final_plan = apply_followup_supervisor_policy(
        final_plan,
        is_followup=is_followup,
        has_context=has_agent_context(context_payload),
        followup_task_policy=followup_task_policy,
        is_execute_suggested_stock_selection=is_execute_suggested_stock_selection,
    )

    final_symbol = str(result.symbol).strip()
    final_plan = _enforce_research_analyst_routing(query, final_plan)
    final_plan = _enforce_hybrid_background_routing(
        query,
        final_plan,
        delivery_mode=str(state.get("delivery_mode", "") or ""),
        quick_answer_scenario=str(state.get("quick_answer_scenario", "") or ""),
    )
    final_plan = _enforce_macro_policy_impact_routing(query, final_plan)
    final_plan = _enforce_option_portfolio_isolation(query, final_plan)
    final_plan = _enforce_volatility_divergence_routing(query, final_plan)
    final_plan = _enforce_volatility_market_view_routing(query, final_plan)
    final_plan = _enforce_margin_monitor_routing(query, final_plan)
    final_plan = _enforce_cn_margin_monitor_routing(query, final_plan)
    final_plan = _enforce_option_data_monitor_routing(query, final_plan)
    if _wants_chart(query) and not any(p in final_plan for p in ("analyst", "generalist")):
        final_plan = ["generalist"] + list(final_plan)

    # 去重并保持顺序，避免路由重复
    final_plan = _dedupe_plan(final_plan)
    final_plan, final_symbol = _apply_analysis_task_policy(
        query,
        final_plan,
        final_symbol,
        is_followup=is_followup,
        recent_context="\n".join(part for part in (recent_context, followup_action_context, followup_route_context) if part),
    )
    final_plan = _dedupe_plan(final_plan)
    locked_symbol, locked_symbol_name = _resolve_hybrid_background_subject_lock(
        query,
        delivery_mode=str(state.get("delivery_mode", "") or ""),
        quick_answer_target=str(state.get("quick_answer_target", "") or ""),
        focus_entity=str(state.get("focus_entity", "") or ""),
    )
    if locked_symbol and any(step in final_plan for step in ("analyst", "monitor", "strategist", "generalist")):
        final_symbol = locked_symbol

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

    route_tags = []
    if is_market_data_query(query):
        route_tags.append("market_data")
    if is_pure_option_data_query(query):
        route_tags.append("pure_option_data")
    if is_volatility_market_view_query(query):
        route_tags.append("volatility_market_view")
    if _wants_chart(query):
        route_tags.append("chart")
    if has_portfolio and "portfolio_analyst" in final_plan:
        route_tags.append("portfolio")
    if is_followup:
        route_tags.append("followup")

    result_confidence = float(getattr(result, "confidence", 0.0) or 0.0)
    if 0 < result_confidence < 0.45:
        route_tags.append("low_confidence")

    route_decision = build_route_decision(
        query=query,
        plan=final_plan,
        symbol=final_symbol,
        planner_expert_scores=getattr(result, "expert_scores", {}) or {},
        planner_confidence=result_confidence,
        planner_reason=str(getattr(result, "route_reason", "") or ""),
        route_tags=route_tags,
    )
    final_plan = route_decision.plan
    final_symbol = route_decision.symbol
    route_debug = route_decision.as_dict()
    print(
        f"[expert-router] mode={route_decision.route_mode} "
        f"confidence={route_decision.confidence:.2f} "
        f"experts={route_decision.selected_expert_count} "
        f"plan={final_plan}"
    )

    return {
        "plan": final_plan,
        "symbol": final_symbol,
        "symbol_name": locked_symbol_name,
        "route_decision": route_debug,
        "route_confidence": route_decision.confidence,
        "route_mode": route_decision.route_mode,
        "messages": [SystemMessage(content=f"已制定计划: {final_plan}")]
    }


# ==========================================
# 🔥 [新增] 王牌分析师 (Generalist / Fallback)
# 职责：处理对比、综合、模糊或 Supervisor 搞不定的复杂问题
# ==========================================
def generalist_node(state: AgentState, llm):
    query = state["user_query"]
    symbol_input = state.get("symbol", "")
    wants_chart = _wants_chart(query)
    is_followup = bool(state.get("is_followup", False))
    current_date = datetime.now().strftime("%Y年%m月%d日 %A")
    context_payload = _state_context_payload(state)
    followup_context_block = render_agent_context(context_payload, target="generalist")

    if is_followup and not has_agent_context(context_payload):
        return {
            "messages": [HumanMessage(content="【王牌分析】未检索到上一轮关键结论。请贴出上一轮结论（方向、关键位或策略），我再承接展开。")],
            "chart_img": ""
        }

    tools = build_generalist_tools()


    prompt = f"""
        你是一位王牌量化分析师。交易理念是顺势而为。
        【当前日期】：{current_date}。
        客户需求：{query}。
        分析品种：{symbol_input}。
        【连续追问模式】：{"是" if is_followup else "否"}。
        【历史承接上下文】：
        {followup_context_block}
   
        
        【工具使用表】
        1. **估值/便宜/贵吗/抄底** -> get_stock_valuation 
        2. **对比/PK/谁强/选哪个** -> tool_compare_stocks (多股横评)
        3. **对冲/相关性/联动** -> tool_stock_correlation_check
        4. **历史统计价格** -> get_price_statistics
        5. **画图/走势图** -> draw_chart_tool
        6. **概念/策略解释** -> search_investment_knowledge
        7. 相关性分析 -> tool_futures_correlation_check或tool_stock_correlation_check
        8. 对冲分析 -> calculate_hedging_beta
        9. 查某期货资金流动 -> get_futures_fund_flow
        10.查全部期货资金沉淀排名 -> get_futures_fund_ranking
        10.05 查A股融资余额、融资杠杆、两融资金或大盘/ETF市场环境 -> get_cn_margin_market_signal
        10.1 查期货保证金/合约乘数/资金占用 -> get_futures_margin_profile
        10.2 查期货基差/现期结构 -> get_futures_basis_profile
        10.3 查期货库存与仓单 -> get_futures_inventory_receipt_profile
        10.4 查期货交割/期转现 -> get_futures_delivery_tospot_profile
        11.查商品龙虎榜/期货商持仓 -> search_broker_holdings_on_date  
        12.查某期货商最近持仓变化情况 -> tool_analyze_position_change
        12.1 查期货商正反指标/席位信号/龙虎榜辅助判断 -> get_futures_broker_position_signal
        12.2 查“正指标/反指标最近在哪些商品上做多/做空” -> get_futures_broker_group_position_moves
        12.3 只问某期货商加多/加空是否利多利空，且没有给具体品种 -> get_futures_broker_indicator_profile
        13.查成交量和持仓量 -> get_volume_oi
        14.查期货持仓量排名 -> get_futures_oi_ranking
        15.查单个标的最新IV/IV Rank/近期趋势 -> get_commodity_iv_info
        15.05 查美股/美股ETF期权体检（IV Rank、RV、期限结构、skew、Put/Call、0DTE、OI防线） -> get_us_option_market_profile；只限美股期权，不用于A股ETF/商品/股指期权。
        15.06 查美股/美股ETF期权策略候选合约（卖put、卖call、备兑、双卖、铁鹰、信用价差等） -> get_us_option_strategy_candidates；只有该工具返回候选时，才能引用具体行权价、EOD权利金、估算Delta、盈亏平衡或最大亏损。
        15.1 查IV增幅/降幅排行、IV扫描、指定日期区间ATM IV变化排序 -> scan_iv_change_ranking
        15.2 查价格与IV是否出现波动率背离 -> scan_volatility_divergence（只在复杂多品种/综合任务中使用；简单背离扫描应由 monitor 处理）
        16.查期权合约价格-> tool_query_specific_option
        17.查ETF期权有哪些合约-> get_etf_option_strikes
        18.查宏观指标 -> get_macro_indicator
        19.查宏观环境总览 -> get_macro_overview 
        20.分析收益率曲线 -> analyze_yield_curve 
        21.查单只股票的成交量详情 -> query_stock_volume
        22.期权策略回测 -> run_option_strategy_backtest
        23. 用户问“某策略在某时间段的胜率/盈亏比/回撤”时，必须调用回测工具，禁止口头估算。
        24. 用户问“为什么涨跌/新闻影响/宏观消息怎么看/最近消息如何影响行情”时，优先调用 interpret_market_news_tool，用交易员口吻回答主线、盘面验证、反向风险和接下来盯什么。
        【行为准则】
        1. 先给结论，然后解释理由。
        2. 不要简单复述，要有深度洞察。
        3. 禁止空谈，必须用工具获取的数据说话。
        4. 不要编造数据，如果没查到数据就说不知道。
        5. 若处于连续追问模式，第一段必须先承接上一轮关键结论，再回答当前问题。
        6. 东证期货、海通期货、中信期货是正指标期货商；中信建投、东方财富、方正中期是反指标期货商。反指标做多是一种利空，反指标做空是一种利多；只问某期货商加多/加空时，必须先调用 get_futures_broker_indicator_profile；问正/反指标组最近在哪些商品上做多/做空时，必须调用 get_futures_broker_group_position_moves。
        7. 用户问“波动率背离/IV背离/价格和IV背离”时，必须调用 scan_volatility_divergence；禁止用 scan_iv_change_ranking 替代背离判断。
        8. 用户问美股/美股ETF期权（如 SPY、QQQ、NVDA、TSLA、AAPL）的skew、0DTE、Put/Call、OI防线、期限结构、IV Rank时，才可调用 get_us_option_market_profile；其他期权市场禁止调用该工具。
        9. 用户问美股/美股ETF期权策略执行、行权价、权利金、双卖/铁鹰/信用价差候选时，必须调用 get_us_option_strategy_candidates；缺候选时只能给筛选条件和风险框架。
        """

    if wants_chart:
        prompt += """
        【强制画图】
        用户明确要求图表/走势图/K线图，必须调用 `draw_chart_tool` 生成图表。
        禁止只输出“无法渲染图表”之类的文字降级说明。
        """

    prompt += """
        【标的识别补充】
        - 支持美股代码：AAPL / TSLA / NVDA / MSFT / AMZN / GOOG / META / AVGO / AMD / INTC / TSM
        - 支持美股中文别名：苹果 / 特斯拉 / 英伟达 / 微软 / 亚马逊 / 谷歌等
        - 美股数据默认是日线收盘数据（EOD）
        """

    general_agent = create_react_agent(llm, tools, prompt=prompt)

    # 🔥 用于在异常时恢复部分结果
    partial_response = ""
    chart_img = ""

    try:
        # 给予足够的递归步数，但不要太高避免 GeneratorExit
        result = general_agent.invoke(
            {"messages": state["messages"]},
            {"recursion_limit": _get_agent_recursion_limit("generalist", 40)}
        )

        last_response = result["messages"][-1].content
        partial_response = last_response

        # 🔥 从响应中提取图表路径
        chart_matches = re.findall(r'!\[.*?\]\((chart_[a-zA-Z0-9_]+\.json)\)', last_response)
        if chart_matches:
            chart_img = chart_matches[-1]

        # 🔥 如果响应中没找到，尝试从所有消息中查找
        if not chart_img:
            base_msg_count = len(state.get("messages", []))
            new_msgs = result.get("messages", [])[base_msg_count:]
            all_chart_matches = []
            for msg in new_msgs:
                content = getattr(msg, 'content', str(msg))
                all_chart_matches.extend(re.findall(r'(chart_[a-zA-Z0-9_]+\.json)', content))
            if all_chart_matches:
                chart_img = all_chart_matches[-1]

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
    profile_context = str(state.get("profile_context", "") or "").strip()
    current_date = datetime.now().strftime("%Y年%m月%d日 %A")
    tools = [
        analyze_kline_pattern,  # 核心：形态与趋势
        get_market_snapshot,
        get_price_statistics,  # 辅助：历史波动数据
        draw_chart_tool,
        draw_pattern_annotation_chart,  # 形态标注图（破底翻/吞噬/晨星等）
        draw_forecast_chart,            # 关键价位图（支撑/压力/目标价）
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
    is_chart_only = _wants_chart(query)

    if is_chart_only:
        # 🔥 画图快速模式 - 简化 prompt
        persona_prompt = f"""
            你是一位技术分析画图师。【当前日期】：{current_date}。
            【当前标的】：{symbol}
            【客户需求】：{query}

            【任务】：
            用户想要看图表，请必须调用 `draw_chart_tool` 画图。禁止只输出“无法渲染图表”之类的文字降级说明。

            【回复要求】：
            1. 画完图后，只要简短说明图表关键信息（如当前价格、涨跌幅）。
            2. 绝对不要做冗长的分析！
            3. 技术面默认只允许引用工具返回的 K 线、均线、OHLC 与价格事实；禁止输出 RSI、MACD、KDJ、BOLL、布林、量能突破等非授权指标。
            """
    else:
        # 正常分析模式
        persona_prompt = f"""
            你是一位严谨的技术分析师。遵循趋势交易。
            【当前日期】：{current_date}。
            【当前标的】：{symbol}
            【客户历史记忆】：{mem_context}
            【用户专属画像】：{profile_context if profile_context else "无"}

            【ETF期权持仓数据】：{extra_instruction}
            【客户需求】：{query}

            【可调用工具】
            1. 技术面分析-> `analyze_kline_pattern` ，获取K线形态和趋势。
            2. 获取标的一段时间价格-> `get_price_statistics` 。
            3. 分析的品种如果只有1个，只能调用1次`analyze_kline_pattern`
            4. 获取股票名字和价格用 `get_market_snapshot`

            【画图规则（重要，必须遵守优先级）】
            - 优先级1：用户要求画图，且 `analyze_kline_pattern` 返回了形态信号 → 必须调用 `draw_pattern_annotation_chart`，禁止用 `draw_chart_tool`
              ⚠️ 调用时必须把 `analyze_kline_pattern` 识别到的形态名称填入 `pattern_name` 参数（如 pattern_name="5日平台突破"），不要留空
            - 优先级2：用户明确给出支撑/压力/目标价，或你做完分析后算出了关键价位 → 调用 `draw_forecast_chart`
            - 优先级3：用户只要求看K线走势图、没有形态信号、也没有价位数据 → 才用 `draw_chart_tool`
            - 用户没有要求画图时，三个画图工具都不调用

            【任务】：
            1. 只描述K线和技术面情况；基本面、财报、公告、行业逻辑交给 researcher，不要在技术分析里自行展开。
            2. 发掘突破进场机会。
            3. 如果连续几天累积涨幅过大，要提醒防范突然下跌，如果连续几天累积跌幅过大，要提醒可能突然报复反弹
            4. K线的重要性大于均线，反转或突破要看K线，均线反应会比较慢。
            5. 如果没有明显机会，直说"建议观望"。
            6. 如果用户的指令模糊，可参考上文历史确认分析对象。
            7. 必须以 `analyze_kline_pattern` 返回的【今日K线事实】为准描述影线方向：吊人线/锤子线都是长下影小实体，不能说成长上影；射击之星是长上影小实体，不能说成长下影。若形态名与OHLC事实冲突，优先相信OHLC事实并说明“形态需复核”。
            8. 【用户专属画像】只用于调整表达和风险边界；当前问题明确表达优先，不要因为年龄/性别做交易判断。
            9. 【数据采信硬边界】当前价、涨跌幅、支撑/压力、均线值必须来自工具返回；工具未查到时必须写“当前数据源未查到”，禁止用模型记忆补数。
            10. 【技术指标边界】默认只使用 K 线和均线；禁止输出 RSI、MACD、KDJ、BOLL、布林、量能突破等未授权指标。即使你知道这些概念，本产品当前技术分析也不展开。
            """


    # 简单的方向提取 (给策略员用)
    persona_prompt += """
            【标的识别补充】
            - 支持美股代码：AAPL / TSLA / NVDA / MSFT / AMZN / GOOG / META / AVGO / AMD / INTC / TSM
            - 支持美股中文别名：苹果 / 特斯拉 / 英伟达 / 微软 / 亚马逊 / 谷歌等
            - 美股行情默认是日线收盘数据（EOD），不是盘中实时逐笔
            """

    analyst_agent = create_react_agent(llm, tools, prompt=persona_prompt)

    partial_response = ""
    chart_img = ""
    symbol_name = ""

    try:
        # 执行推理 (给予足够的递归次数，因为处理价差可能需要调2次工具)
        result = analyst_agent.invoke(
            {"messages": state["messages"]},
            {"recursion_limit": _get_agent_recursion_limit("analyst", 18)}
        )

        last_response = _sanitize_unauthorized_technical_indicators(
            result["messages"][-1].content,
            query=query,
        )
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

        chart_matches = re.findall(r'IMAGE_CREATED:(chart_[a-zA-Z0-9_]+\.json)', last_response)
        if chart_matches:
            chart_img = chart_matches[-1]
            print(f"📊 analyst_node 提取到图表: {chart_img}")
        if not chart_img:
            base_msg_count = len(state.get("messages", []))
            new_msgs = result.get("messages", [])[base_msg_count:]
            all_chart_matches = []
            for msg in new_msgs:
                content = getattr(msg, 'content', str(msg))
                all_chart_matches.extend(re.findall(r'IMAGE_CREATED:(chart_[a-zA-Z0-9_]+\.json)', content))
            if all_chart_matches:
                chart_img = all_chart_matches[-1]

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
_MONITOR_DIRECT_DATA_BLOCKERS = tuple(
    dict.fromkeys(
        STRATEGY_QUERY_KEYWORDS
        + OPTION_ACTION_QUERY_KEYWORDS
        + [
            "怎么看", "分析", "影响", "为什么", "如果", "假如", "假设", "推演",
            "适合", "能买吗", "能不能", "如何处理", "利好", "利空",
        ]
    )
)


def _has_monitor_direct_data_blocker(query: str) -> bool:
    text = str(query or "").strip().lower()
    if not text:
        return False
    task_type = classify_analysis_task_type(query).task_type
    if task_type != TASK_TYPE_NORMAL and not is_pure_option_data_query(query):
        return True
    return any(keyword.lower() in text for keyword in _MONITOR_DIRECT_DATA_BLOCKERS)


def _invoke_monitor_direct_tool(tool_obj: Any, payload: Dict[str, Any]) -> str:
    try:
        if hasattr(tool_obj, "invoke"):
            return str(tool_obj.invoke(payload) or "").strip()
        return str(tool_obj(**payload) or "").strip()
    except Exception as exc:
        tool_name = str(getattr(tool_obj, "name", "") or getattr(tool_obj, "__name__", "tool"))
        return f"结论：数据不足\n- 原因：{tool_name} 调用失败：{exc}"


def _try_monitor_direct_data_query(query: str, *, symbol: str = "") -> str | None:
    text = str(query or "").strip()
    lowered = text.lower()
    if not text:
        return None

    if _is_cn_margin_explicit_query(text) and not _is_cn_margin_analysis_query(text):
        return _invoke_monitor_direct_tool(
            get_cn_margin_market_signal,
            {"as_of_date": _extract_cn_margin_as_of_date(text)},
        )

    if _has_monitor_direct_data_blocker(text):
        return None

    has_margin_intent = _contains_any(text, MARGIN_QUERY_KEYWORDS)
    has_option_context = "期权" in text or "option" in lowered
    if has_margin_intent and not has_option_context:
        return _invoke_monitor_direct_tool(get_futures_margin_profile, {"query": text})

    if any(keyword in text for keyword in ("基差", "现期结构", "现货升贴水")):
        return _invoke_monitor_direct_tool(get_futures_basis_profile, {"query": text})

    if any(keyword in text for keyword in ("库存", "仓单")):
        return _invoke_monitor_direct_tool(get_futures_inventory_receipt_profile, {"query": text})

    if any(keyword in text for keyword in ("期转现", "交割")):
        return _invoke_monitor_direct_tool(get_futures_delivery_tospot_profile, {"query": text})

    has_iv_data_intent = any(keyword in lowered for keyword in ("iv", "iv rank", "ivrank")) or any(
        keyword in text for keyword in ("波动率", "隐含波动率")
    )
    if has_iv_data_intent and not is_volatility_market_view_query(text):
        if is_us_option_market_profile_query(text):
            underlying = extract_us_option_underlying_symbol(text, symbol_hint=str(symbol or ""))
            if underlying:
                return _invoke_monitor_direct_tool(get_us_option_market_profile, {"underlying": underlying})
        if is_pure_option_data_query(text) or is_market_data_query(text):
            return _invoke_monitor_direct_tool(get_commodity_iv_info, {"query": text})

    return None


def monitor_node(state: AgentState, llm):
    user_q = state["user_query"]
    symbol = state.get("symbol", "")
    symbol_name = state.get("symbol_name", "")
    current_date = datetime.now().strftime("%Y年%m月%d日 %A")
    latest_trade_date = get_latest_data_date()
    is_pure_option_data = is_pure_option_data_query(user_q)
    is_volatility_market_view = is_volatility_market_view_query(user_q)

    if _is_futures_broker_signal_task(user_q):
        product = _extract_futures_broker_signal_product(user_q)
        broker = _extract_futures_broker_signal_broker(user_q)
        group, direction = _extract_futures_broker_group_and_direction(user_q)
        if product:
            last_response = _build_futures_broker_position_signal(product, lookback_days=5)
        elif broker:
            last_response = _build_futures_broker_indicator_profile(broker)
        elif group:
            last_response = _build_futures_broker_group_position_moves(
                signal_group=group,
                direction=direction,
                lookback_days=5,
            )
        else:
            last_response = (
                "结论：数据不足\n"
                "- 冲突与风险：已识别为期货商正反指标问题，但没有识别到具体品种、期货商名称或正/反指标组。"
            )
        return {
            "messages": [HumanMessage(content=f"【数据监控】\n{last_response}")],
            "fund_data": last_response,
        }

    direct_data_response = _try_monitor_direct_data_query(user_q, symbol=str(symbol or symbol_name or ""))
    if direct_data_response:
        return {
            "messages": [HumanMessage(content=_tag_monitor_worker_response(direct_data_response))],
            "fund_data": direct_data_response,
        }

    # 1. 装备所有数据类工具
    tools = build_monitor_tools()

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
    pure_option_data_instruction = ""
    if is_pure_option_data:
        pure_option_data_instruction = """
    8. 如果是纯期权数据问法（如 IV/波动率/到期日/保证金/乘数/持仓量），必须用短格式回答：
       - 第一行只写：`结论：...`
       - 然后最多 4 条数据要点，每条一行，优先回答用户真正问的数据
       - 禁止展开宏观、技术面、策略、风险偏好、长篇背景
       - 禁止使用 `【最终决策】`、`Executive Summary`、`市场深度解析`、`交易策略部署`、`签发` 这类长报告标题
       - 如果缺数据，就直接写“暂无该项数据”
       - 结尾最多补 1 句：`如果你想结合策略或行情，我再继续展开。`
    9. 纯数据问法下，整段回答控制在 6 行以内，越短越好。
        """
    volatility_market_view_instruction = ""
    if is_volatility_market_view:
        volatility_market_view_instruction = """
    10. 如果用户问“升波还是降波 / IV会升还是会降”：
       - 这是窄口径波动率方向判断，不是完整技术分析。
       - 必须优先调用 `get_commodity_iv_info` 查询当前 IV/IV变化/IV Rank；必要时再调用 `get_market_snapshot` 或 `get_recent_price_series` 确认标的涨跌。
       - 第一行必须直接回答：`结论：当前更偏【降波/升波/中性不确定】。`
       - 后面最多 3 条证据，只能引用工具返回的数据：标的涨跌、IV水平/变化、IV Rank或近期变化。
       - 禁止展开K线、均线、支撑压力、策略部署、Executive Summary、市场深度解析。
       - 不要给交易建议；最多写 1 句“需要继续盯价格是否放量突破/IV是否反向抬升”。
       - 整段控制在 6 行以内。
        """
    cn_margin_context_instruction = ""
    if _is_cn_margin_explicit_query(user_q) or _is_cn_margin_auto_context_query(user_q):
        cn_margin_context_instruction = """
    11. 当前问题涉及A股融资资金或A股大盘/ETF/ETF期权市场环境，必须调用 `get_cn_margin_market_signal`：
       - 只能引用工具返回的数据日、余额、分位、连续方向和市场确认，不自行补数字或更改信号阈值。
       - “过热、去杠杆、撤退、修复”是资金状态，不得直接改写成指数必涨或必跌。
       - 若工具标记数据陈旧或样本不足，必须明确降级，不能继续给确定性融资结论。
       - 个股分析只有在用户明确问整体市场环境时才使用该工具，不把全市场融资状态当成个股基本面。
        """

    prompt = f"""
    你是一位追求效率的市场数据监控官**。。只负责查数据给结果。
    - 今天日期：{current_date}
    - 数据库最新交易日：{latest_trade_date}

    【你的工具箱 - 根据问题类型选择正确的工具】
    - 查单个标的最新IV/IV Rank/近期趋势 -> get_commodity_iv_info
    - 查美股/美股ETF期权体检（IV Rank、RV、期限结构、skew、Put/Call、0DTE、OI防线） -> get_us_option_market_profile
    - 查IV增幅/降幅排行、IV扫描、指定日期区间ATM IV变化排序 -> scan_iv_change_ranking
    - 查价格与IV是否出现波动率背离 -> scan_volatility_divergence
    - 查股票行业资金 -> tool_get_retail_money_flow
    - 查A股融资余额、融资杠杆、两融资金及大盘/ETF市场环境 -> get_cn_margin_market_signal
    - 查某期货资金流动 -> get_futures_fund_flow
    - 查全部期货资金沉淀排名 -> get_futures_fund_ranking
    - 查期货保证金/合约乘数/资金占用 -> get_futures_margin_profile
    - 查期货基差/现期结构 -> get_futures_basis_profile
    - 查期货库存与仓单 -> get_futures_inventory_receipt_profile
    - 查期货交割/期转现 -> get_futures_delivery_tospot_profile
    - 查某天某品种的期货商持仓排名（龙虎榜） -> search_broker_holdings_on_date 
    - 查某品种一段时间内各期货商的持仓变化 -> tool_analyze_position_change 
    - 查某期货商在各品种的持仓变化 -> tool_analyze_broker_positions （当前净持仓代表期货商对这品种的趋势判断）
    - 查期货商正反指标/席位信号/龙虎榜辅助判断 -> get_futures_broker_position_signal
    - 查正指标/反指标最近在哪些商品上做多/做空 -> get_futures_broker_group_position_moves
    - 只问某期货商加多/加空是否利多利空，且没有给具体品种 -> get_futures_broker_indicator_profile
    - 查期权成交量异常(放量/异动) -> get_option_volume_abnormal
    - 查期权持仓量异常(大单增仓) -> get_option_oi_abnormal
    - 查期权持仓量排名 -> get_option_oi_ranking
    - 查期权合约价格 -> tool_query_specific_option,
    - 查成交量和持仓量 -> get_volume_oi
    - 查期货持仓量排名 -> get_futures_oi_ranking
    - 查标的价格 -> get_market_snapshot
    - 查某一天历史价格 -> get_historical_price
    - 查区间统计(最高/最低/区间涨跌) -> get_price_statistics
    - 查最近N个交易日逐日明细表 -> get_recent_price_series
    - 查宏观指标 -> get_macro_indicator(indicator_code='US10Y')  
    
    
    {tool_instruction}

    【要求】
    1. 精准使用工具，不要乱调用，除非客户有要求全面分析。
    2. **只陈述数据事实**，不要进行复杂的行情预测或给交易建议。
    3. 如果用户没有指定日期，**必须使用 {latest_trade_date}** 作为查询日期！
    4. 如果工具返回了 Markdown 表格，请原样输出。
    4.1 用户问 IV增幅/降幅/排行/扫描/哪些合约升波最多或降波最多/指定日期区间ATM IV变化排序时，必须调用 `scan_iv_change_ranking`。
    4.2 用户说“由大到小/增幅最大/升波最多”时，`direction="increase"`；用户说“由小到大/降幅最大/回落最多/降波最多”时，`direction="decrease"`。
    4.3 用户问“波动率背离/IV背离/隐波背离/价格和IV背离”时，必须调用 `scan_volatility_divergence`；禁止用 `scan_iv_change_ranking` 替代背离判断。
    4.4 `get_us_option_market_profile` 只允许用于美股/美股ETF/美股代码的期权问题（如 SPY、QQQ、NVDA、TSLA、AAPL）；A股ETF期权、商品期权、股指期权继续使用原有期权/IV工具，禁止误调用美股期权工具。
    5. 商品都有期权，禁止说商品没有场内期权。
    6. 用户要“某天价格”优先用 `get_historical_price`；用户要“区间统计”优先用 `get_price_statistics`。
    7. 用户要“最近N天/最近N个交易日/列表/逐日明细/走势数据表”时，优先用 `get_recent_price_series`，不要只返回 `get_market_snapshot`。
    8. 东证期货、海通期货、中信期货是正指标期货商；中信建投、东方财富、方正中期是反指标期货商。反指标做多是一种利空，反指标做空是一种利多；问正/反指标组最近在哪些商品上做多/做空时，必须调用 get_futures_broker_group_position_moves；只问某期货商加多/加空时，必须先调用 get_futures_broker_indicator_profile。
    {pure_option_data_instruction}
    {volatility_market_view_instruction}
    {cn_margin_context_instruction}
    """

    # 3. 创建临时 Agent (ReAct 模式)
    # 使用 bind_tools 让 LLM 可以自动选择用哪个工具
    prompt += """
    【标的识别补充】
    - 支持美股代码：AAPL / TSLA / NVDA / MSFT / AMZN / GOOG / META / AVGO / AMD / INTC / TSM
    - 支持美股中文别名：苹果 / 特斯拉 / 英伟达 / 微软 / 亚马逊 / 谷歌等
    - 美股数据默认是日线收盘数据（EOD）
    """

    monitor_agent = create_react_agent(llm, tools, prompt=prompt)

    partial_response = ""

    try:
        # 限制迭代次数，防止死循环
        result = monitor_agent.invoke(
            {"messages": [HumanMessage(content=user_q)]},
            {"recursion_limit": _get_agent_recursion_limit("monitor", 10)}
        )
        last_response = result["messages"][-1].content
        partial_response = last_response

        return {
            "messages": [HumanMessage(content=_tag_monitor_worker_response(last_response))],
            "fund_data": last_response
        }

    except GeneratorExit:
        # 🔥 GeneratorExit 不是 Exception 子类，需要单独捕获
        fallback_msg = partial_response if partial_response else f"资金数据查询完成"
        return {
            "messages": [HumanMessage(content=_tag_monitor_worker_response(fallback_msg))],
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
    option_scenario = detect_option_hypothetical_scenario(user_q)
    raw_risk_pref = state.get("risk_preference", "稳健型")
    fund = state.get("fund_data", "暂无明显资金流向")
    trend = state.get("trend_signal", "Neutral")
    mem_context = state.get("memory_context", "")
    profile_context = str(state.get("profile_context", "") or "").strip()
    tech_view = state.get("technical_summary", "")
    profile_policy = build_profile_policy(
        risk_preference=raw_risk_pref,
        profile_context=profile_context,
        user_query=user_q,
    )
    option_policy = build_option_strategy_policy(
        risk_preference=profile_policy.risk_label,
        profile_context=profile_context,
        user_query=user_q,
        trend_signal=trend,
        technical_summary=tech_view,
    )
    risk_pref = option_policy.risk_label
    current_date = datetime.now().strftime("%Y年%m月%d日")
    key_level = state.get("key_levels", "")
    vision_position_domain = str(state.get("vision_position_domain", "") or "").strip().lower()
    vision_position_payload = state.get("vision_position_payload") or {}
    vision_option_legs: List[Dict[str, Any]] = []
    if isinstance(vision_position_payload, dict):
        raw_legs = vision_position_payload.get("option_legs") or []
        if isinstance(raw_legs, list):
            vision_option_legs = [x for x in raw_legs if isinstance(x, dict)]
    canonical_option_legs = _build_canonical_option_legs(vision_option_legs)
    canonical_option_legs_block = _build_canonical_option_legs_block(canonical_option_legs)
    option_position_mode = _is_option_position_query(user_q) or vision_position_domain in {"option", "mixed"}
    authoritative_underlying_quotes: Dict[str, Any] = {}
    authoritative_quote_block = ""
    quote_underlyings: List[str] = []
    underlying_trend_map: Dict[str, str] = {}
    if option_position_mode:
        quote_underlyings = _collect_quote_underlyings_from_canonical_legs(
            canonical_legs=canonical_option_legs,
            symbol_hint=symbol,
        )
        if quote_underlyings:
            try:
                authoritative_underlying_quotes = fetch_underlying_spot_map(underlyings=quote_underlyings)
            except Exception as e:
                print(f"⚠️ 权威现价加载失败: {e}")
                authoritative_underlying_quotes = {}
        state_trend_map = state.get("underlying_trend_map") or {}
        if isinstance(state_trend_map, dict) and state_trend_map:
            underlying_trend_map = {str(k).upper(): str(v) for k, v in state_trend_map.items() if str(k).strip()}
        else:
            underlying_trend_map = _build_underlying_trend_map(
                technical_summary=tech_view,
                underlyings=quote_underlyings,
                default_trend=str(trend or "震荡"),
            )
        authoritative_quote_block = _build_authoritative_quote_block(authoritative_underlying_quotes)
    account_total_capital = normalize_account_total_capital(state.get("account_total_capital"))
    # 兜底：避免上下文传递丢失时退回组合口径，直接从本轮问句再提取一次账户资金
    if option_position_mode and not account_total_capital:
        try:
            account_total_capital = normalize_account_total_capital(parse_account_total_capital(user_q))
        except Exception:
            account_total_capital = None
    delta_cash_report = ""
    delta_cash_meta: Dict[str, Any] = {}
    delta_cash_gap_note = ""
    delta_cash_per_underlying: Dict[str, Any] = {}
    delta_cash_portfolio_summary: Dict[str, Any] = {}
    option_rebalance_priority_queue: List[Dict[str, Any]] = []
    option_delta_displayable = False
    option_delta_execution_ready = False
    option_delta_coverage_ratio = 0.0

    # 期权持仓问题优先给出可复核的Delta Cash量化结果
    if option_position_mode:
        try:
            symbol_hint_for_delta = symbol
            if vision_option_legs:
                first_hint = str((vision_option_legs[0] or {}).get("underlying_hint") or "").strip()
                if first_hint:
                    symbol_hint_for_delta = first_hint
            delta_cash_meta = compute_option_delta_cash(
                user_query=user_q,
                symbol_hint=symbol_hint_for_delta,
                vision_legs=vision_option_legs or None,
                vision_domain=vision_position_domain,
                trend_signal=trend,
                trend_map=underlying_trend_map or None,
                risk_preference=risk_pref,
                account_total_capital=account_total_capital,
            ) or {}
            raw_report = str(delta_cash_meta.get("report") or "").strip()
            delta_cash_per_underlying = delta_cash_meta.get("per_underlying") or {}
            delta_cash_portfolio_summary = delta_cash_meta.get("portfolio_summary") or {}
            option_rebalance_priority_queue = delta_cash_meta.get("risk_contribution_ranking") or []
            delta_asset_class = str(delta_cash_meta.get("asset_class") or "").strip().lower()
            supports_delta = bool(delta_asset_class in {"etf", "index", "multi"} or delta_cash_meta.get("is_etf"))
            metrics = delta_cash_meta.get("metrics") or {}
            portfolio_summary = delta_cash_meta.get("portfolio_summary") or {}
            coverage_raw = metrics.get("coverage_ratio")
            if coverage_raw is None:
                coverage_raw = portfolio_summary.get("coverage_ratio")
            coverage = float(coverage_raw) if coverage_raw is not None else None
            missing_notes = [str(x) for x in (delta_cash_meta.get("missing_notes") or []) if str(x).strip()]
            blocking_notes = [
                str(x) for x in (delta_cash_meta.get("blocking_missing_notes") or []) if str(x).strip()
            ]
            displayable_flag = delta_cash_meta.get("displayable")
            if displayable_flag is None:
                displayable_flag = portfolio_summary.get("displayable")
            if displayable_flag is None:
                if coverage is not None:
                    displayable_flag = bool(coverage > 0.0)
                else:
                    displayable_flag = bool(raw_report and "【DeltaCash】" in raw_report)

            execution_ready_flag = delta_cash_meta.get("execution_ready")
            if execution_ready_flag is None:
                execution_ready_flag = portfolio_summary.get("execution_ready")
            if execution_ready_flag is None:
                publishable_flag = delta_cash_meta.get("publishable")
                if publishable_flag is not None:
                    execution_ready_flag = bool(publishable_flag)
                elif coverage is not None:
                    execution_ready_flag = bool(coverage >= float(DELTA_EXECUTION_COVERAGE_THRESHOLD))
                else:
                    execution_ready_flag = False

            option_delta_displayable = bool(displayable_flag)
            option_delta_execution_ready = bool(execution_ready_flag)
            option_delta_coverage_ratio = float(coverage) if coverage is not None else 0.0

            if supports_delta and option_delta_displayable:
                delta_cash_report = raw_report
                delta_cash_gap_note = ""
            elif supports_delta:
                reason_candidates = blocking_notes or missing_notes
                if not reason_candidates and coverage is not None:
                    if coverage <= 0:
                        reason_candidates = ["Delta覆盖率为0，暂无可展示结果"]
                    elif coverage < float(DELTA_EXECUTION_COVERAGE_THRESHOLD):
                        reason_candidates = [
                            f"Delta覆盖率仅{coverage * 100:.1f}%，低于执行阈值{int(DELTA_EXECUTION_COVERAGE_THRESHOLD * 100)}%"
                        ]
                if not reason_candidates:
                    reason_candidates = ["Delta覆盖率不足或关键数据缺失"]
                reason = "；".join(reason_candidates)
                delta_cash_gap_note = _build_delta_cash_gap_note(
                    reason=reason,
                    trend_signal=trend,
                    risk_preference=risk_pref,
                )
            if option_position_mode and not authoritative_underlying_quotes:
                fallback_underlying = str(delta_cash_meta.get("underlying_code") or "").strip().upper()
                fallback_code = _normalize_underlying_code_for_quote(fallback_underlying)
                if fallback_code:
                    authoritative_underlying_quotes = fetch_underlying_spot_map(underlyings=[fallback_code])
                    authoritative_quote_block = _build_authoritative_quote_block(authoritative_underlying_quotes)
        except Exception as e:
            print(f"⚠️ DeltaCash预计算失败: {e}")
            delta_cash_meta = {}
            delta_cash_report = ""
            delta_cash_per_underlying = {}
            delta_cash_portfolio_summary = {}
            option_rebalance_priority_queue = []
            option_delta_displayable = False
            option_delta_execution_ready = False
            option_delta_coverage_ratio = 0.0
            delta_cash_gap_note = _build_delta_cash_gap_note(
                reason=f"Delta计算失败（{e}）",
                trend_signal=trend,
                risk_preference=risk_pref,
            )

    # [新增] 获取合约乘数
    multiplier_str = get_option_multiplier(symbol)
    multiplier_hint = f"\n        【合约乘数】：{multiplier_str}（计算盈亏时必须乘以此数）" if multiplier_str else ""

    # 🔥 [新增] 获取持仓上下文
    portfolio_corr_index = state.get("portfolio_top_corr_index", "")
    portfolio_corr_value = state.get("portfolio_top_corr_value", "")
    portfolio_summary = state.get("portfolio_summary", "")

    # 构建持仓上下文提示
    portfolio_context = ""
    if portfolio_corr_index and portfolio_corr_value:
        portfolio_context = f"\n        【客户持仓信息】：客户持仓组合与{portfolio_corr_index}指数相关度最高（相关系数{portfolio_corr_value}）"
        if portfolio_summary:
            portfolio_context += f"\n        持仓概况：{portfolio_summary[:100]}"

    delta_cash_prompt = ""
    if delta_cash_report:
        delta_cash_prompt = f"\n        【Delta Cash预计算（优先采信）】\n{delta_cash_report}"
    elif delta_cash_gap_note:
        delta_cash_prompt = f"\n        【DeltaCash数据缺口说明】\n{delta_cash_gap_note}"
    authoritative_quote_prompt = ""
    if authoritative_quote_block:
        authoritative_quote_prompt = f"\n        【标的现价（权威数据，仅可引用以下数值）】\n{authoritative_quote_block}"
    profile_policy_context = profile_policy.as_prompt_context()
    option_strategy_policy_context = option_policy.as_prompt_context()

    option_position_requirements = ""
    if option_position_mode:
        locked_fact_requirement = ""
        if canonical_option_legs_block:
            locked_fact_requirement = (
                "- 本轮已识别到结构化持仓，持仓拆解表必须逐行沿用以下锁定事实（不可改写）：\n"
                f"{canonical_option_legs_block}"
            )
        option_position_requirements = f"""
        【期权持仓深度模板（强制）】
        你必须严格按以下 6 个章节输出，章节名不可缺失：
        1. **持仓拆解表**：按“到期月/行权价/方向(认购认沽)/张数/单腿成本或已实现收益”逐项列出；信息缺失项要标注“待确认”。
        2. **组合净暴露与到期错配**：明确净方向（偏多/偏空/中性）与近月到期风险，说明哪一腿是主要风险来源。
        3. **关键触发位与三情景分支**：给出上涨/震荡/下跌三种情景下的应对动作（减仓/移仓/展期/对冲）。
        4. **两套可执行调整方案**：分别给“保守方案、进攻方案”，每套写清执行腿、目标、代价、适用条件。
        5. **风控阈值与失效条件**：必须给出止损阈值、仓位上限和策略失效触发条件（价格/时间/波动率任一维度）。
        6. **当日执行清单**：用 checklist 列出今天能执行的 3-5 个动作。

        补充要求：
        - 如果缺少关键行情数据，仍输出完整 6 章节，但要在对应章节明确“数据缺口”。
        - 所有权利金或盈亏示例必须注明“已按合约乘数换算”。
        - 关键位优先引用上游技术面传入信息：{key_level if key_level else "未提供关键位，需先用工具确认"}。
        - 若已提供【Delta Cash预计算】，必须在你的正文中保留其核心数字：Total Delta Cash、Delta Ratio、目标区间。
        - {"当前覆盖率达标，必须给出建议调整量（元）并对应执行方向。" if option_delta_execution_ready else f"当前覆盖率未达{int(DELTA_EXECUTION_COVERAGE_THRESHOLD * 100)}%，只允许给方向性动作与补数清单，不得输出金额级调仓量。"}
        - 若【DeltaCash】区块包含“调仓优先队列（P1/P2/P3）”，执行建议必须按该优先顺序展开，不得反向。
        - 市场现价只能引用【标的现价（权威数据）】区块，禁止从行权价推导/缩放现价。
        - 禁止输出“9.000视为3.000档”这类口径修正语句。
        - 方向术语只允许使用：买认购/卖认购/买认沽/卖认沽。禁止出现 Long/Short Call/Put、长Put、短Call 等写法。
        {locked_fact_requirement}
        """

    # === 🔥 期权策略专用工具集 ===
    tools = build_strategist_tools()

    # === 🔥 ReAct Prompt - 引导期权策略推理 ===
    prompt = f"""
        你是一位**资深期权交易策略师**，擅长根据市场数据设计期权策略。

        【当前日期】：{current_date}
        【分析标的】：{symbol}{multiplier_hint}
        【客户问题】：{user_q}
        【客户风险偏好】：{risk_pref}
        【客户历史记忆】：{mem_context}
        【用户专属画像】：{profile_context if profile_context else "无"}
        {profile_policy_context}
        【市场资金面/保证金信息】：{fund}
        【技术面参考】：{trend} 、 {tech_view}{portfolio_context}{delta_cash_prompt}{authoritative_quote_prompt}
        {option_strategy_policy_context}
        {"【上传截图识别】：本轮来自混合持仓截图，已自动切换到期权主线，股票体检不展开。" if vision_position_domain == "mixed" else ""}

        【边界说明】
        - 基差/库存仓单/交割期转现类数据由上游 monitor 汇总后传入，不在本节点直接查询。

        【工作流程】
        **第一步：判断是否需要查数据**
        - 普通知识解释、概念类比、宽泛讨论，不要为了预检索而查工具。
        - 只要进入“适合我怎么做 / 推荐策略 / 买卖哪个合约 / 仓位风险 / 具体执行”的回答，必须按需检查三件事：标的现价、IV Rank、距离到期日。
        - 用 `get_market_snapshot` 获取现价，用 `get_commodity_iv_info` 看IV/IV Rank，用 `check_option_expiry_status` 看到期日；工具失败时改为条件式建议，不编造数据。
        - 如果是美股/美股ETF期权（如 SPY、QQQ、NVDA、TSLA、AAPL）的 IV Rank、skew、0DTE、Put/Call、OI防线、期限结构或策略适配问题，必须先用 `get_us_option_market_profile` 读取本地EOD体检数据。
        - 美股期权策略适配问题包括“适合什么期权策略/怎么操作/卖put/卖call/双卖/铁鹰/covered call/cash-secured put/备兑/担保卖沽”等问法；回答开头必须先引用工具返回的数据日期、ATM IV/IV Rank、期限结构、skew或Put/Call、OI/0DTE和数据缺口，再解释策略适配。
        - 美股期权只要进入策略执行、行权价、权利金、价差宽度、双卖或卖方收租候选，就必须继续调用 `get_us_option_strategy_candidates`；只有该候选工具返回有效结果时，才允许给具体行权价、EOD权利金、估算Delta、盈亏平衡或最大亏损。
        - 卖方策略可以被正式推荐：当 IV Rank/IV Percentile 较高、流动性足够、趋势与客户风险偏好匹配时，可优先考虑卖认沽、备兑、信用价差、双卖、铁鹰等收权利金策略；激进偏好可讨论裸卖/双卖，但必须说明保证金、极端风险、止损/移仓条件和有限风险替代方案。
        - `get_us_option_market_profile` 或 `get_us_option_strategy_candidates` 返回缺数据或覆盖不足时，只能给条件式策略框架，禁止补造 IV、OI、0DTE、合约报价、权利金、Delta或实时行情。
        - `get_us_option_market_profile` 禁止用于 A股ETF期权、商品期权、股指期权；这些标的继续使用 `get_commodity_iv_info`、`check_option_expiry_status`、`tool_query_specific_option` 等原有工具。
        - 如果用户问 IV增幅/降幅排行、IV扫描、哪些合约升波最多/降波最多，必须用 `scan_iv_change_ranking`，不要用单标的 IV 查询替代。
        - 如果用户问波动率背离/IV背离并进一步要求策略，必须使用 monitor 传入的背离结论；需要补查时调用 `scan_volatility_divergence`，禁止用 `scan_iv_change_ranking` 替代背离判断。

        **第二步：设计策略**
        - **期权策略**：根据技术面趋势+IV+距离到期日+客户风险偏好来选择策略，可以查知识库辅助`search_investment_knowledge`。
        - **个性化规则**：遵守【画像优先级】与【个性化期权策略规则】；规则块给边界，未覆盖细节由工具结果和交易常识判断。
        - **策略方向**：如果技术面参考是做多或看涨，就不要给做空策略，如果技术面参考是做空或看跌，就不要给做多策略。
        - **持仓关联**：如果客户有持仓信息（见【客户持仓信息】），且当前标的与持仓相关指数有关，需要明确说明策略如何辅助或对冲现有持仓风险。例如："考虑到您的股票持仓与{portfolio_corr_index if portfolio_corr_index else 'XX指数'}高度相关，建议用该期权策略来..."
        
        **第三步：思考行权价合约 (Strikes)**
        - 如果客户有指定行权价合约，就直接根据客户需求，但可以给出合适的不同建议。
        - 如果用户没指定，就根据设计的策略和客户风险偏好来看需要哪些合约。

        **第四步：确定策略的执行合约**
        - **合约选择**：一定要根据标的现价，再来找合适的行权价合约。
        - **查询合约**：用 `tool_query_specific_option` 查具体期权价格（格式："标的 行权价 认购/认沽"），权利金价格也要乘上合约乘数。
        - 只有当工具返回了有效的价格数据时，才能推荐该合约。
        - 对美股/美股ETF期权，如果 `get_us_option_strategy_candidates` 没有返回具体候选，禁止给精确合约、精确行权价、权利金或收益测算；只能给 DTE、Delta/虚实程度、OTM百分比和风险上限这类筛选条件。
        - 如果工具返回“未找到”，请尝试调整行权价再次查询，或者诚实告知用户该档位无合约。
        - 如果客户问“回测/策略表现”，优先用 `run_option_strategy_backtest` 给出回测结果。
        - 如果客户问“某策略在某时间段的胜率/盈亏比/最大回撤”，必须调用回测工具并传入 `start_date/end_date` 或 `time_expr`，禁止口头估算。

        【策略规则使用】
        - 【个性化期权策略规则】是本轮期权策略边界，优先于自由发挥。
        - 工具查到的 IV Rank、DTE、现价和合约价格负责确定执行参数；查不到时只给条件式策略，不编造合约。
           
        【工具使用特殊提醒】：
        1. 中证1000有股指期权，不要用get_etf_option_strikes，必须用tool_query_specific_option查期权合约

        【输出要求】
        1. 默认给出“首选策略 + 条件”，只有当你输出具体合约、行权价、权利金或盈亏示例时，相关行权价/价格必须用工具查过。
        2. **计算盈亏示例时，必须乘以合约乘数**
        3. 必须说明策略选择依据来自风险偏好、DTE、IV、趋势强弱中的哪些项，可以查知识库辅助`search_investment_knowledge`
        4. 给出止损/止盈建议
        5. 禁止自己编造假数据！
        6. 禁止使用 Long/Short Call/Put 术语，统一使用中文交易口径：买认购/卖认购/买认沽/卖认沽。
        7. 不允许只给固定模板；必须输出“首选策略 + 不适合策略 + 触发/失效条件”。
        {option_position_requirements}

        """
    if option_scenario.active:
        prompt += build_strategist_scenario_context(option_scenario)

    # === 🔥 创建 ReAct Agent ===
    strategist_agent = create_react_agent(llm, tools, prompt=prompt)

    # 用于异常恢复
    partial_response = ""

    try:
        result = strategist_agent.invoke(
            {"messages": [HumanMessage(content=user_q)]},
            {"recursion_limit": _get_agent_recursion_limit("strategist", 28)}
        )

        last_response = result["messages"][-1].content
        if option_position_mode:
            last_response = _ensure_option_position_structure(
                text=last_response,
                delta_cash_report=delta_cash_report,
                delta_cash_gap_note=delta_cash_gap_note,
                trend_signal=trend,
                risk_preference=risk_pref,
                key_levels=key_level,
            )
            lock_result = _apply_option_fact_lock(
                text=last_response,
                canonical_legs=canonical_option_legs,
                strict_cover=True,
            )
            last_response = str(lock_result.get("text") or last_response)
            canonical_option_legs_block = str(lock_result.get("canonical_option_legs_block") or canonical_option_legs_block)
            option_direction_conflict_count = int(lock_result.get("option_direction_conflict_count") or 0)
            quote_lock_result = _apply_authoritative_quote_lock(
                text=last_response,
                authoritative_quotes=authoritative_underlying_quotes,
                authoritative_quote_block=authoritative_quote_block,
                strict_cover=bool(authoritative_underlying_quotes),
            )
            last_response = str(quote_lock_result.get("text") or last_response)
            authoritative_quote_block = str(quote_lock_result.get("authoritative_quote_block") or authoritative_quote_block)
            price_conflict_count = int(quote_lock_result.get("price_conflict_count") or 0)
        elif delta_cash_report:
            last_response = f"{delta_cash_report}\n\n{last_response}"
            option_direction_conflict_count = 0
            price_conflict_count = 0
        else:
            option_direction_conflict_count = 0
            price_conflict_count = 0
        partial_response = last_response

        return {
            "messages": [HumanMessage(content=f"【期权策略】\n{last_response}")],
            "option_strategy": last_response,
            "option_delta_cash_report": delta_cash_report,
            "option_delta_cash_meta": delta_cash_meta,
            "option_delta_cash_gap_note": delta_cash_gap_note,
            "option_delta_cash_per_underlying": delta_cash_per_underlying,
            "option_delta_cash_portfolio_summary": delta_cash_portfolio_summary,
            "option_rebalance_priority_queue": option_rebalance_priority_queue,
            "option_delta_displayable": option_delta_displayable,
            "option_delta_execution_ready": option_delta_execution_ready,
            "option_delta_coverage_ratio": option_delta_coverage_ratio,
            "canonical_option_legs_block": canonical_option_legs_block,
            "option_direction_conflict_count": option_direction_conflict_count,
            "authoritative_underlying_quotes": authoritative_underlying_quotes,
            "authoritative_quote_block": authoritative_quote_block,
            "price_conflict_count": price_conflict_count,
        }

    except GeneratorExit:
        # 流被中断时的优雅降级
        fallback_msg = partial_response if partial_response else f"期权策略分析已完成，关于{symbol}的建议请参考上文。"
        if option_position_mode:
            fallback_msg = _ensure_option_position_structure(
                text=fallback_msg,
                delta_cash_report=delta_cash_report,
                delta_cash_gap_note=delta_cash_gap_note,
                trend_signal=trend,
                risk_preference=risk_pref,
                key_levels=key_level,
            )
            lock_result = _apply_option_fact_lock(
                text=fallback_msg,
                canonical_legs=canonical_option_legs,
                strict_cover=True,
            )
            fallback_msg = str(lock_result.get("text") or fallback_msg)
            canonical_option_legs_block = str(lock_result.get("canonical_option_legs_block") or canonical_option_legs_block)
            option_direction_conflict_count = int(lock_result.get("option_direction_conflict_count") or 0)
            quote_lock_result = _apply_authoritative_quote_lock(
                text=fallback_msg,
                authoritative_quotes=authoritative_underlying_quotes,
                authoritative_quote_block=authoritative_quote_block,
                strict_cover=bool(authoritative_underlying_quotes),
            )
            fallback_msg = str(quote_lock_result.get("text") or fallback_msg)
            authoritative_quote_block = str(quote_lock_result.get("authoritative_quote_block") or authoritative_quote_block)
            price_conflict_count = int(quote_lock_result.get("price_conflict_count") or 0)
        elif delta_cash_report and "DeltaCash" not in fallback_msg:
            fallback_msg = f"{delta_cash_report}\n\n{fallback_msg}"
            option_direction_conflict_count = 0
            price_conflict_count = 0
        else:
            option_direction_conflict_count = 0
            price_conflict_count = 0
        return {
            "messages": [HumanMessage(content=f"【期权策略】\n{fallback_msg}")],
            "option_strategy": fallback_msg,
            "option_delta_cash_report": delta_cash_report,
            "option_delta_cash_meta": delta_cash_meta,
            "option_delta_cash_gap_note": delta_cash_gap_note,
            "option_delta_cash_per_underlying": delta_cash_per_underlying,
            "option_delta_cash_portfolio_summary": delta_cash_portfolio_summary,
            "option_rebalance_priority_queue": option_rebalance_priority_queue,
            "option_delta_displayable": option_delta_displayable,
            "option_delta_execution_ready": option_delta_execution_ready,
            "option_delta_coverage_ratio": option_delta_coverage_ratio,
            "canonical_option_legs_block": canonical_option_legs_block,
            "option_direction_conflict_count": option_direction_conflict_count,
            "authoritative_underlying_quotes": authoritative_underlying_quotes,
            "authoritative_quote_block": authoritative_quote_block,
            "price_conflict_count": price_conflict_count,
        }

    except Exception as e:
        # 其他异常的降级处理
        error_msg = f"期权策略分析遇到问题: {e}"
        print(f"⚠️ strategist_node 错误: {e}")
        return {
            "messages": [HumanMessage(content=f"【期权策略】\n{error_msg}")],
            "option_strategy": "",
            "option_delta_cash_report": delta_cash_report,
            "option_delta_cash_meta": delta_cash_meta,
            "option_delta_cash_gap_note": delta_cash_gap_note,
            "option_delta_cash_per_underlying": delta_cash_per_underlying,
            "option_delta_cash_portfolio_summary": delta_cash_portfolio_summary,
            "option_rebalance_priority_queue": option_rebalance_priority_queue,
            "option_delta_displayable": option_delta_displayable,
            "option_delta_execution_ready": option_delta_execution_ready,
            "option_delta_coverage_ratio": option_delta_coverage_ratio,
            "canonical_option_legs_block": canonical_option_legs_block,
            "option_direction_conflict_count": 0,
            "authoritative_underlying_quotes": authoritative_underlying_quotes,
            "authoritative_quote_block": authoritative_quote_block,
            "price_conflict_count": 0,
        }



# 🟤 5. 情报研究员
def researcher_node(state: AgentState,llm=None):
    timeout_seconds = _get_researcher_node_timeout_seconds()
    try:
        with _wall_clock_timeout(timeout_seconds, "researcher"):
            return _researcher_node_impl(state, llm)
    except _NodeTimeoutError:
        query = str(state.get("user_query", "") or "").strip()
        return {
            "messages": [
                HumanMessage(
                    content=(
                        "【情报】\n"
                        f"情报研究超过 {timeout_seconds} 秒，已停止深度联网扩展。"
                        "先保留快速判断，后续可单独追问更具体的新闻、政策或板块线索。"
                        f"\n\n本次问题：{query[:120]}"
                    )
                )
            ]
        }


def _researcher_node_impl(state: AgentState,llm=None):
    reset_polymarket_task_guard()
    symbol = state["symbol"]
    symbol_name = state.get("symbol_name", "")
    query = state["user_query"]
    current_date = datetime.now().strftime("%Y年%m月%d日 %A")
    if _is_link_article_stock_mapping_task(query):
        return _answer_link_article_stock_mapping(state, llm)

    if _is_recent_company_news_query(query):
        subject = _extract_recent_company_subject(query, symbol=symbol, symbol_name=symbol_name)
        if subject:
            search_query = f"{subject} 最近 利好 消息 公告 财报 进展"
            try:
                web_result = _invoke_search_web_for_researcher(search_query)
            except Exception as e:
                web_result = f"联网检索失败: {e}"
            web_text = str(web_result or "").strip()
            if not web_text or any(hint in web_text for hint in ("未搜索到相关内容", "搜索出错", "未配置")):
                return {
                    "messages": [
                        HumanMessage(
                            content=(
                                f"【情报与舆情】\n暂时没有在限定时间内检索到 {subject} 的可靠近期利好。"
                                "建议稍后再查公告、财报或交易所披露。"
                            )
                        )
                    ]
                }
            summary_prompt = f"""
            你是市场情报员。请基于下面联网检索结果，回答用户关于公司近期利好/消息的问题。
            只能使用检索结果，不要编造具体金额、日期、诉讼结论或财务占比。
            若信息只是市场传闻、条件式推演或尚未被权威公告确认，必须明确写出“不算确认”。

            【当前日期】{current_date}
            【用户问题】{query}
            【公司】{subject}
            【检索结果】
            {web_text[:4000]}

            【输出要求】
            - 先用一句话回答“有没有明确利好”。
            - 分 3-5 点列出可能相关的近期信息。
            - 每点标注：明确公告 / 媒体报道 / 条件式推演 / 待核验。
            - 最后给一句风险提醒，不要给买卖建议。
            """
            try:
                response = llm.invoke(summary_prompt)
                summary = str(getattr(response, "content", response) or "").strip()
            except Exception as e:
                summary = f"{subject} 的近期检索结果如下，AI 汇总暂时失败（{e}）：\n{web_text[:1200]}"
            return {
                "messages": [HumanMessage(content=f"【情报与舆情】\n{summary}")]
            }

    # 1. 装备舆情与搜索工具
    tools = [
        interpret_market_news_tool,  # 新闻RAG解释器：补背景、查行情验证、用交易员口吻解读新闻影响
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

        0. 🧭 **新闻/事件影响解读** (如 "为什么涨跌"、"这条新闻影响什么"、"宏观消息怎么看"):
           - **优先调用** `interpret_market_news_tool`。
           - 它会先补背景，再查行情/知识库/预测市场验证，最后用交易员口吻给出主线、反向风险和接下来盯什么。

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
        - 把查到的信息做个整理归纳。
        - 没有查到的信息就说不知道，绝对不要乱编造数据，只能依照查到的数据说话。
        """

    # 3. 创建 Agent
    researcher_agent = create_react_agent(llm, tools, prompt=system_prompt)

    partial_response = ""

    try:
        # 舆情查询可能需要多步（先查热榜，再搜细节），给足步数
        result = researcher_agent.invoke(
            {"messages": [HumanMessage(content=query)]},
            {"recursion_limit": _get_agent_recursion_limit("researcher", 14)}
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


def _invoke_macro_fast_tool(tool_obj, params: Dict[str, Any] | None = None, label: str = "宏观工具") -> str:
    try:
        if hasattr(tool_obj, "invoke"):
            return str(tool_obj.invoke(params or {}) or "").strip()
        return str(tool_obj(**(params or {})) or "").strip()
    except Exception as exc:
        return f"⚠️ {label}获取失败: {exc}"


def _extract_macro_report_stance(data_text: str) -> str:
    text = str(data_text or "")
    has_positive_spread = bool(re.search(r"10Y-2Y(?:利差)?:\s*\+", text))
    has_inversion = "倒挂" in text and "未出现倒挂" not in text
    has_dxy_up = "DXY" in text and "趋势: 上行" in text
    has_us10y_up = ("US10Y" in text or "10Y" in text) and "趋势: 上行" in text

    if has_inversion:
        return "衰退/降息预期升温：曲线倒挂会削弱单纯加息逻辑，避险属性可能托底金银。"
    if has_positive_spread and (has_dxy_up or has_us10y_up):
        return "紧缩交易：高利率与强美元仍是主线，对利率敏感资产偏压制。"
    if has_positive_spread:
        return "高利率但曲线未倒挂：先按紧缩环境处理，等待实际利率或美元转弱确认。"
    return "中性观察：当前需要同时看实际利率、DXY 与收益率曲线，避免只按单一加息标签下结论。"


def _macro_policy_asset_impact_lines(query: str) -> List[str]:
    text = str(query or "").lower()
    lines: List[str] = []

    if "黄金" in text or "金银" in text or "gold" in text:
        lines.append("- 黄金：加息或维持高利率会抬高实际利率和持有成本，通常压制金价；若曲线倒挂、信用压力或避险情绪升温，黄金会获得防守买盘。")
    if "白银" in text or "金银" in text or "silver" in text:
        lines.append("- 白银：方向大多跟随黄金，但工业属性和波动率更高；紧缩环境下通常比黄金更容易被需求预期拖累。")
    if "股市" in text or "美股" in text or "纳指" in text or "a股" in text:
        lines.append("- 股市：利率上行会压估值，成长股与高久期资产更敏感；若市场转向降息预期，则先看盈利下修是否抵消估值修复。")
    if "原油" in text or "铜" in text or "商品" in text or "大宗" in text:
        lines.append("- 大宗商品：紧缩会压需求预期，但供给冲击或美元转弱时可能出现反向支撑。")
    if not lines:
        lines.append("- 资产影响：先看实际利率和美元方向。实际利率/DXY上行偏利空，二者回落或避险升温偏利多。")
    return lines


def _build_fast_macro_policy_impact_report(
    query: str,
    symbol: str = "",
    symbol_name: str = "",
    news_context: str = "",
) -> str:
    started_at = time.perf_counter()
    health = _invoke_macro_fast_tool(
        get_macro_health_snapshot,
        {"indicator_code": "FEDFUNDS,SOFR,US10Y,US2Y,DXY,DFII10"},
        "宏观健康快照",
    )
    curve = _invoke_macro_fast_tool(analyze_yield_curve, {}, "收益率曲线")
    anchors = _invoke_macro_fast_tool(
        get_macro_indicator,
        {"indicator_code": "US10Y,US2Y,DXY,DFII10", "days": 30},
        "核心宏观锚点",
    )

    combined = "\n".join([health, curve, anchors])
    stance = _extract_macro_report_stance(combined)
    asset_lines = _macro_policy_asset_impact_lines(query)
    elapsed_ms = int((time.perf_counter() - started_at) * 1000)
    target = f"{symbol_name}({symbol})" if symbol_name or symbol else "宏观资产"
    context = str(news_context or "").strip()
    if not context or context.startswith("暂无"):
        context = "本快答未额外联网追新闻，主要使用本地宏观数据库与收益率曲线快照。"

    return "\n".join(
        [
            "### 宏观快答",
            f"- 标的/资产：{target}",
            f"- 模式：宏观政策影响快路径，耗时约 {elapsed_ms}ms",
            f"- 结论：{stance}",
            "",
            "### 传导链条",
            "- 加息/高利率 -> 实际利率上行 -> 无息资产持有成本上升 -> 黄金、白银承压。",
            "- 加息/高利率 -> 美元走强 -> 以美元计价的大宗商品承压。",
            "- 如果曲线倒挂、信用压力或衰退担忧升温，避险需求会抵消一部分利率压制。",
            "",
            "### 资产影响",
            *asset_lines,
            "",
            "### 数据验证",
            health,
            "",
            curve,
            "",
            anchors,
            "",
            "### 新闻/上下文",
            context,
            "",
            "### 反向条件",
            "- 若实际利率（DFII10）明显回落、DXY转弱，金银压力会缓解。",
            "- 若通胀或就业数据重新强化加息预期，金银反弹更容易遇到压制。",
        ]
    )


def macro_analyst_node(state: AgentState, llm):
    """
    宏观策略师：全景扫描宏观数据，结合收益率曲线和新闻，判断全球流动性周期。
    """
    user_q = state.get("user_query", "")
    symbol = state["symbol"]
    symbol_name = state.get("symbol_name", "")
    news_context = state.get("news_summary", "暂无最新宏观新闻")
    current_date = datetime.now().strftime("%Y年%m月%d日")

    if _is_macro_policy_asset_impact_query(user_q) and not _wants_chart(user_q):
        fast_report = _build_fast_macro_policy_impact_report(
            user_q,
            symbol=symbol,
            symbol_name=symbol_name,
            news_context=news_context,
        )
        return {
            "messages": [HumanMessage(content=f"【宏观策略】\n{fast_report}")],
            "macro_view": fast_report,
            "macro_chart": "",
        }

    # 引入宏观工具 (请确保在文件头部 import 这些工具)
    # from plot_tools import draw_macro_compare_chart
    # from macro_tools import get_macro_indicator

    tools = [
        get_macro_health_snapshot,  # 先做数据可用性/新鲜度体检
        get_us_debt_gdp_snapshot,  # 债务/GDP专用快照
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
        【标的】: {symbol_name}({symbol})  
        【情报员提供的新闻】：
        {news_context}

        【分析逻辑与工具调用顺序】
        0. 先调用 `get_macro_health_snapshot()` 检查关键宏观数据可用性和新鲜度，若存在缺失/陈旧必须在结论里明确说明。
        0.1 如果用户问题包含“债务/GDP/联邦债务”关键词，必须调用 `get_us_debt_gdp_snapshot()`；必要时再调用
            `get_macro_indicator(indicator_code='GFDEBTN,GDP,GFDEGDQ188S')` 补充细节，并可用 `draw_macro_compare_chart` 画 GFDEBTN vs GDP。

        **第一步：全景与衰退诊断 (必须执行)**
        1. 调用 `get_macro_overview(category='all')`：
           - 快速扫一眼全球市场，看是否有异常板块（如BDI暴跌暗示需求不足，非美货币集体暴跌暗示美元虹吸）。
        2. 调用 `analyze_yield_curve()`：
           - **这是最关键的一步**。检查美债 10Y-2Y 是否**倒挂**。
           - 倒挂 = 衰退预警/降息预期升温；陡峭化 = 复苏或通胀预期。

        **第二步：核心锚点验证**
        1. 调用 `get_macro_indicator(indicator_code='US10Y,DXY,US2Y')`：
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
            {"recursion_limit": 10}
        )
        last_response = result["messages"][-1].content

        # 提取图表
        chart_img = ""
        macro_matches = re.findall(r'(macro_chart_[a-zA-Z0-9_]+\.json)', last_response)
        if macro_matches:
            chart_img = macro_matches[-1]

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
def knowledge_chatter_node(state: AgentState, llm=None):
    """
    知识问答员 - 使用 ReAct 模式自主思考和检索
    优先使用内部知识库，必要时辅以网络搜索
    """
    user_query = state["user_query"]
    is_followup = bool(state.get("is_followup", False))
    knowledge_strategy = _select_knowledge_chat_strategy(state)
    subject_policy = build_subject_policy(user_query, symbol_hint=str(state.get("symbol", "") or ""))
    subject_policy_context = subject_policy.as_prompt_context()
    data_policy_context = build_data_policy_context(symbol=str(state.get("symbol", "") or ""), mode="chatter")
    current_date = datetime.now().strftime("%Y年%m月%d日")
    context_payload = _state_context_payload(state)
    combined_context = render_agent_context(context_payload, target="knowledge")

    if is_followup and not has_agent_context(context_payload):
        return {
            "messages": [HumanMessage(content="【知识问答】\n我这轮没有检索到上一轮关键结论，暂时无法安全承接。请补充上一轮的核心结论（例如方向、关键位、策略），我立刻继续。")],
            "knowledge_context": ""
        }

    # === 🔥 知识问答专用工具集 ===
    direct_freshness_fact_answer = _try_direct_freshness_fact_search(user_query, state)
    if direct_freshness_fact_answer:
        return {
            "messages": [HumanMessage(content=f"【知识问答】\n{direct_freshness_fact_answer}")],
            "knowledge_context": ""
        }

    direct_company_fact_answer = _try_direct_company_fact_search(user_query, knowledge_strategy)
    if direct_company_fact_answer:
        return {
            "messages": [HumanMessage(content=f"【知识问答】\n{direct_company_fact_answer}")],
            "knowledge_context": ""
        }

    tools = build_chatter_tools()

    if knowledge_strategy == "company_news":
        core_rules = """
        【⚠️ 核心原则：公司/个股近期动态问答】
        1. 优先使用 `search_web` 查近期动态、财报、公告、公开报道；必要时再用 `get_market_snapshot` 辅助确认标的或盘面。
        2. `search_web` 最多只允许调用 3 次；每次都要围绕同一家公司/业务线收窄关键词，不要重复搜同义词。
        3. 如果第一轮搜索已经拿到清晰答案，禁止继续为了“搜更多”而重复联网。
        4. 如果用户问的是某条业务线，必须优先围绕该业务线整理信息，不要泛泛介绍整个行业。
        5. 默认回答结构：
           - 最近 2-3 条最相关动态
           - 每条落在哪条业务线
           - 一句判断：更像常规进展 / 明确催化 / 暂未检到清晰利好
        6. 如果没查到清晰、近期、可信的利好或催化，要直接明说“目前没检到清晰的近期利好/催化”。
        7. 禁止用“持续发力”“市场反馈不错”这类行业套话填空。
        8. 不负责估值高低、基本面优劣、值不值得买、股价影响推演；用户若追问这些，请回答最后补一句“如果你想看对股价、估值、买点的影响，我可以继续从分析角度展开”。
        """
        if is_followup:
            core_rules += """
        9. 当前是连续追问，必须优先承接上一轮的公司实体和业务线，不要再问“你是说哪个公司/哪块业务”。
        """
    else:
        core_rules = """
        【⚠️ 核心原则：知识库优先】
        1. **第一步必须**：先用 `search_investment_knowledge` 检索内部知识库
        2. **第二步可选**：如果知识库信息不足或需要最新公开事实，再用其他工具补充
           - `search_web`：联网查公开资料（最多 3 次）
           - `get_market_snapshot`：获取实时行情
        3. 如果用户问期货保证金/合约乘数/一手资金占用，优先调用 `get_futures_margin_profile`
        4. 如果用户问基差/现期结构，调用 `get_futures_basis_profile`
        5. 如果用户问库存/仓单，调用 `get_futures_inventory_receipt_profile`
        6. 如果用户问交割/期转现，调用 `get_futures_delivery_tospot_profile`
        7. `search_web` 只用于知识库不足时补公开事实，不要把它当成默认第一步。
        """
        if is_followup:
            core_rules = """
        【⚠️ 核心原则：连续承接优先】
        1. 第一段必须先引用上一轮关键结论（1-2句），再回答当前问题。
        2. 承接说明要具体，不得只说“根据上文”。
        3. 仅在需要补充事实时再调用工具；可以查知识库，但不是必须第一步；如需联网，`search_web` 最多使用 3 次。
        4. 禁止把“知识库命中为空”当作默认模板回答。
        """

    # === 🔥 ReAct Prompt - 按模式切换规则 ===
    prompt = f"""
        你是一位热情、博学的**金融导师**，负责解答用户的金融知识问题和资讯问答。

        【当前日期】：{current_date}
        【用户问题】：{user_query}
        【连续追问模式】：{"是" if is_followup else "否"}
        【当前回答策略】：{"公司近期动态" if knowledge_strategy == "company_news" else "概念解释/知识问答"}
        【历史承接上下文】：
        {combined_context}

        {core_rules}
        {subject_policy_context}
        {data_policy_context}

        【回答风格】
        1. 语气要轻松、易懂，像朋友聊天一样
        2. 如果是概念解释，用通俗的例子帮助理解
        3. 如果是公司近期动态，先给信息，再做一句轻判断，不要越权分析
        4. 如果是策略问题，结合实际场景说明
        5. 如果【用户专属画像】里有爱好、回答偏好或厌恶点，只有在解释类比或表达风格相关时自然使用，不要硬提身份信息。
        6. 适当引导用户深入探讨相关话题

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
            {"recursion_limit": _get_agent_recursion_limit("chatter", 10)}
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
            fallback_prompt = f"用户问：{user_query}。请基于你的知识回答，语气轻松友好。"
            if is_followup and combined_context != "无":
                fallback_prompt = (
                    f"用户在连续追问。\n"
                    f"历史上下文：\n{combined_context}\n\n"
                    f"当前问题：{user_query}\n"
                    f"请先承接上一轮关键结论，再回答当前问题。"
                )
            if _is_latest_company_fact_query(knowledge_strategy, user_query):
                return {
                    "messages": [HumanMessage(content=f"【知识问答】\n{_insufficient_latest_company_fact_message()}")],
                    "knowledge_context": kb_content
                }
            simple_response = llm.invoke(fallback_prompt)
            return {
                "messages": [HumanMessage(content=f"【闲聊】\n{simple_response.content}")],
                "knowledge_context": ""
            }
        except:
            return {
                "messages": [HumanMessage(content=f"【闲聊】\n抱歉，我遇到了一些问题。请稍后再试或换个方式提问。")],
                "knowledge_context": ""
            }


def chatter_node(state: AgentState, llm=None):
    """
    聊天/知识问答员 - 图内兜底
    简单问候用轻量直答，其他问题进入知识问答模式
    """
    user_query = state["user_query"]
    is_followup = bool(state.get("is_followup", False))
    is_greeting = len(user_query) < 5 and any(
        x in user_query for x in ["你好", "嗨", "早", "谢", "hello", "hi", "嘿", "晚上好", "早上好", "早安", "中午好", "下午好"]
    )

    if is_greeting and not is_followup:
        try:
            reply = simple_chatter_reply(
                user_query,
                llm,
                recent_context=str(state.get("recent_context", "") or ""),
                memory_context=str(state.get("memory_context", "") or ""),
                profile_context=str(state.get("profile_context", "") or ""),
                is_followup=is_followup,
                focus_entity=str(state.get("focus_entity", "") or ""),
                focus_topic=str(state.get("focus_topic", "") or ""),
                focus_aspect=str(state.get("focus_aspect", "") or ""),
                conversation_memory_query=bool(state.get("conversation_memory_query", False)),
                conversation_memory_label=str(state.get("conversation_memory_label", "") or ""),
                messages=state.get("messages", []),
                runtime_context=build_simple_runtime_context(current_user_label=str(state.get("user_id", "") or "访客")),
                context_payload=_state_context_payload(state),
            )
            return {
                "messages": [HumanMessage(content=f"【闲聊】\n{reply}")],
                "knowledge_context": "",
            }
        except Exception as e:
            print(f"⚠️ chatter_node 简单回复失败: {e}")

    return knowledge_chatter_node(state, llm=llm)


# 🟣 =选股员 (Screener)
_SCREENER_RISK_KEYWORDS = [
    "危险", "不要买", "别买", "卖掉", "要卖", "卖出",
    "避开", "规避", "远离", "警惕", "出场",
    "差的", "最差", "垃圾", "烂股", "坑", "雷", "暴雷",
    "分数最低", "评分最低", "最弱", "弱势股",
    "不好", "不行", "别碰", "跑路", "清仓",
    "下跌", "亏损", "套牢", "割肉", "止损",
]

_SCREENER_RISK_PATTERNS = [
    r"风险(?:股票|股|预警|警示|较高|很高|高|大|很大|偏高)",
    r"(?:有|存在|哪些|哪个|哪只|哪几只).{0,4}风险",
    r"高风险",
    r"不要(?:买|买入|碰|追|接|参与|介入)",
    r"别(?:买|买入|碰|追|接|参与|介入)",
]

_SCREENER_NON_RISK_CONSTRAINT_PATTERNS = [
    r"不要(?:涨|涨幅|涨得|上涨|已经涨|累计涨|短期涨)",
    r"(?:涨幅|涨得|上涨|累计涨幅|短期涨幅).{0,8}(?:不要|别|不).{0,4}(?:太多|过大|过高)",
    r"(?:低风险|风险低|风险不高|风险较低|风险小|风险可控|控制风险)",
]


def _is_screener_risk_query(query: str) -> bool:
    """Detect requests for dangerous/avoid-list stocks without catching constraints."""
    compact_query = re.sub(r"\s+", "", query or "")
    if not compact_query:
        return False

    option_selling_phrases = (
        "卖出认购",
        "卖认购",
        "卖出认沽",
        "卖认沽",
        "期权卖方",
    )
    if any(phrase in compact_query for phrase in option_selling_phrases):
        return False

    if any(keyword in compact_query for keyword in _SCREENER_RISK_KEYWORDS):
        return True

    if any(re.search(pattern, compact_query) for pattern in _SCREENER_RISK_PATTERNS):
        return True

    if any(re.search(pattern, compact_query) for pattern in _SCREENER_NON_RISK_CONSTRAINT_PATTERNS):
        return False

    return False


def _is_us_stock_selection_query(query: str) -> bool:
    text = str(query or "").strip()
    lower = text.lower()
    if not text:
        return False
    has_us_subject = any(keyword in text for keyword in ("美股", "纳斯达克", "纽交所")) or "us stock" in lower
    if not has_us_subject:
        return False
    us_pool_action = bool(re.search(
        r"(?:从|在)?美股(?:股票)?(?:池)?(?:里|中|内)?(?:帮我)?(?:找|筛选|选|挑)",
        text,
    ))
    has_action = any(keyword in text for keyword in ("推荐", "筛选", "帮我找", "帮我选", "找几只", "选几只", "哪些", "候选股", "股票池"))
    has_stock_word = any(keyword in text for keyword in ("股票", "个股", "标的", "候选"))
    return bool(us_pool_action or has_action or has_stock_word)


_US_STOCK_BEARISH_SETUP_KEYWORDS = (
    "做空", "看跌", "空头", "破位", "弱势", "下跌", "下行", "short", "bearish",
)


def _infer_us_stock_technical_setup(query: str) -> str:
    text = str(query or "").strip()
    lower = text.lower()
    if any(keyword in text or keyword in lower for keyword in _US_STOCK_BEARISH_SETUP_KEYWORDS):
        return "bearish_breakdown"
    return "bottom_breakout"


def _extract_requested_candidate_limit(query: str, default: int = 10) -> int:
    text = str(query or "")
    # 先识别明确的结果数量，避免把“过去60个交易日”误当成要返回60个候选。
    match = re.search(
        r"(?:只看|仅看|展示|返回|给我|列出|最多)\s*(?:前)?\s*(\d{1,2})\s*(?:只|支|个)?",
        text,
        re.IGNORECASE,
    )
    if not match:
        match = re.search(r"(?:前|top)\s*(\d{1,2})\s*(?:只|支|个)?", text, re.IGNORECASE)
    if not match:
        match = re.search(r"(\d{1,2})\s*(?:只|支|个名称|只名称|支名称)", text)
    if match:
        return max(1, min(20, int(match.group(1))))

    chinese_digits = {
        "一": 1,
        "二": 2,
        "两": 2,
        "三": 3,
        "四": 4,
        "五": 5,
        "六": 6,
        "七": 7,
        "八": 8,
        "九": 9,
        "十": 10,
    }
    for digit, value in chinese_digits.items():
        if re.search(rf"{digit}\s*(?:只|支|个名称|只名称|支名称)", text):
            return value
    return default


def _should_reason_us_stock_screener_result(result_text: str) -> bool:
    text = str(result_text or "").strip()
    if not text:
        return False
    if "结论：数据不足" in text or "结论：暂无符合条件候选" in text:
        return False
    return "| 代码" in text and ".US" in text


def _build_us_stock_screener_reasoning_prompt(query: str, result_text: str, setup: str = "bottom_breakout") -> str:
    is_bearish = str(setup or "").strip().lower() == "bearish_breakdown"
    if is_bearish:
        task_rules = """
【看跌候选要求】
1. 结论必须围绕“看跌/做空观察候选”，不得改写成底部突破或做多观察。
2. 不要给直接做空指令，只能说“候选观察”“更适合跟踪”“需要确认”。
3. 必须提醒：做空还需确认券源、借券成本、止损位和隔夜跳空风险。

【输出格式】
【精选股票】
结论：一句话概括这批候选是否贴近用户要的“做空/看跌观察”。
- 数据日期：沿用工具里的日期，并注明美股日线 EOD。
- 优先观察：列用户要求数量内最贴近的候选，说明破位/弱势理由。
- 需要等待确认：只列急跌或量能不足的代表。
- 怎么看下一步：1 句，强调反抽、止损和券源确认。
""".strip()
    else:
        task_rules = """
【做多候选要求】
1. 结论必须围绕“底部起来刚突破/做多观察候选”，不得改写成做空观察。
2. 不要给买入指令，只能说“候选观察”“更适合跟踪”“需要确认”。
3. 如果候选属于“强势延续但不算底部刚启动”，要明确说它技术面强，但不完全符合“底部刚突破”。
4. 如果优先观察层为空，要明确说“当前库里没有特别标准的底部刚突破，只能看相对接近的观察名单”。

【输出格式】
【精选股票】
结论：一句话概括这批候选是否真正贴近用户要的“底部起来刚突破”。
- 数据日期：沿用工具里的日期，并注明美股日线 EOD。
- 优先观察：列 1-4 只最贴近的，说明理由。
- 偏强但已不早/量能不足：只列需要提醒的代表，不要展开太长。
- 怎么看下一步：1 句，强调回踩和量能确认。
""".strip()
    return f"""
你是 TradingArt 的美股技术筛选解释员。现在已经有一个确定性筛选工具给出了候选事实表。

【用户问题】
{query}

【确定性筛选结果】
{result_text}

【硬规则】
1. 只能使用“确定性筛选结果”里的股票代码、数字和分层，不得新增股票，不得改写数字。
2. 必须保留数据日期，并说明这是美股日线 EOD 数据，不是盘中实时行情。
3. 回答要简洁，不要写通用技术课，不要解释一堆无关术语。

{task_rules}
""".strip()


def _reason_us_stock_screener_result(query: str, result_text: str, llm, setup: str = "bottom_breakout") -> str:
    if not _should_reason_us_stock_screener_result(result_text):
        return f"【精选股票】\n{result_text}"
    try:
        response = llm.invoke(_build_us_stock_screener_reasoning_prompt(query, result_text, setup=setup))
        content = str(getattr(response, "content", response) or "").strip()
        if content:
            return content if content.startswith("【精选股票】") else f"【精选股票】\n{content}"
    except Exception as exc:
        print(f"⚠️ 美股筛选 AI 推理失败，回退确定性结果: {exc}")
    return f"【精选股票】\n{result_text}"


def screener_node(state: AgentState, llm, compiler_llm=None):
    # --- 1. 获取宏观资金风向 (Sector Flow) ---
    sector_flow_info = ""
    query = state["user_query"]
    followup_action_context = str(state.get("followup_action_context", "") or "").strip()
    followup_action_block = (
        f"\n【上一轮可执行建议】\n{followup_action_context}\n"
        "【承接要求】用户当前请求是在执行上一轮建议，不要反问筛选什么；应按上一轮建议中的条件筛选。\n"
        if followup_action_context
        else ""
    )
    followup_route_context = str(state.get("followup_route_context", "") or "").strip()
    if followup_route_context:
        followup_action_block += (
            f"\n{followup_route_context}\n"
            "【追问承接要求】当前请求是短期承接，不要反问上下文中已经明确的对象或条件。\n"
        )

    if _is_link_article_stock_mapping_task(query):
        return _answer_link_article_stock_mapping(state, llm)

    # 多维筛选器自身已经同时校验“美股市场 + 筛选动作 + 可执行维度”。
    # 这里不再叠加另一套意图判断，避免“从美股池里找……”因两套关键词不一致而落入A股工具。
    if is_us_multifactor_screen_query(query):
        try:
            limit = _extract_requested_candidate_limit(query, default=10)
            compiler_mode = str(os.getenv("US_STOCK_SCREEN_LLM_MODE", "on") or "on").strip().lower()
            compiled_plan = None
            compile_error = ""
            if compiler_mode != "off":
                compile_outcome = compile_screen_plan_with_llm(
                    query,
                    compiler_llm or llm,
                    limit=limit,
                )
                if hasattr(compile_outcome, "plan"):
                    compiled_plan = compile_outcome.plan
                    compile_status = str(getattr(compile_outcome, "status", "") or "")
                    compile_error = str(getattr(compile_outcome, "error", "") or "")
                    compile_model = str(getattr(compile_outcome, "model", "") or "")
                    compile_ms = int(getattr(compile_outcome, "elapsed_ms", 0) or 0)
                    has_tool_call = bool(getattr(compile_outcome, "has_tool_call", False))
                else:
                    # Temporary compatibility for older compiler implementations and test doubles.
                    compiled_plan, compile_error = compile_outcome
                    compile_status = "success" if compiled_plan is not None else "provider_error"
                    compile_model = str(
                        getattr(compiler_llm or llm, "model_name", "")
                        or getattr(compiler_llm or llm, "model", "")
                        or ""
                    )
                    compile_ms = 0
                    has_tool_call = compiled_plan is not None
                fallback_reason = "" if compiled_plan is not None else (compile_status or "compile_failed")
                print(
                    f"[USStockScreen] semantic_compile mode={compiler_mode} "
                    f"compile_status={compile_status or 'unknown'} model={compile_model or 'unknown'} "
                    f"compile_ms={compile_ms} has_tool_call={has_tool_call} "
                    f"fallback_reason={fallback_reason or '-'}"
                )
                _append_agent_trace_event(
                    state,
                    "screen_compile",
                    {
                        "mode": compiler_mode,
                        "status": compile_status or "unknown",
                        "model": compile_model or "unknown",
                        "duration_ms": compile_ms,
                        "has_tool_call": has_tool_call,
                        "fallback_reason": fallback_reason,
                    },
                )
                if compiled_plan is not None:
                    plan_payload = compiled_plan.model_dump() if hasattr(compiled_plan, "model_dump") else compiled_plan.dict()
                    print(f"[USStockScreen] semantic_plan mode={compiler_mode} plan={plan_payload}")
                    if compiler_mode == "shadow":
                        comparison = compare_semantic_plan_to_rules(query, compiled_plan, limit=limit)
                        print(f"[USStockScreen] shadow_comparison={comparison}")
                elif compile_error:
                    print(f"[USStockScreen] semantic_compile_failed fallback=rules reason={compile_error}")
            payload = {"query": query, "limit": limit}
            if compiled_plan is not None and compiler_mode not in {"off", "shadow"}:
                payload["plan"] = compiled_plan.model_dump() if hasattr(compiled_plan, "model_dump") else compiled_plan.dict()
            result = screen_us_stocks.invoke(payload)
        except Exception as exc:
            result = f"【美股多维筛选】\n结论：数据不足\n- 原因：美股多维筛选工具调用失败：{exc}"
        return {
            "messages": [HumanMessage(content=result)],
            "symbol": "",
        }

    if _is_us_stock_selection_query(query):
        try:
            setup = _infer_us_stock_technical_setup(query)
            limit = _extract_requested_candidate_limit(query, default=10)
            us_result = search_us_stocks_by_technical_setup.invoke({"setup": setup, "limit": limit})
        except Exception as e:
            us_result = f"结论：数据不足\n- 原因：美股筛选工具调用失败：{e}"
            setup = "bottom_breakout"
        final_result = _reason_us_stock_screener_result(query, us_result, llm, setup=setup)
        return {
            "messages": [HumanMessage(content=final_result)],
            "symbol": state.get("symbol", ""),
        }

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

    # 🔥 [新增] 成交量/量能类关键词 - 用于触发成交量专用快速通道

    volume_keywords = [
        "成交量异常", "成交量增加", "成交量减少", "成交量放大", "成交量萎缩", "成交金额异常",
        "成交量选", "按成交量", "用成交量", "看成交量", "查成交量",
        "成交量突然", "成交量前", "成交量排名", "成交量最大", "成交量TOP",
        "放量", "缩量", "天量", "地量", "巨量", "爆量",
        "量异常", "量能异常", "量能放大", "量价齐升", "量价背离",
        "放量异动", "量异动", "缩量下跌", "放量上涨", "放量突破",
        "换手率异常", "换手率最高", "换手率排名",
        "资金抢筹", "主力流入", "主力抢筹", "主力埋伏","埋伏",
        "成交异常", "交易异常活跃", "异常活跃"
    ]

    is_risk_query = _is_screener_risk_query(query)

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

    is_volume_query = any(kw in query for kw in volume_keywords)

    # === 🔥 [新增] 成交量选股快速通道 ===
    # 当检测到成交量相关关键词时，直接调用 search_volume_anomalies，不依赖 LLM 选工具
    # ⚠️ 必须排除已被"形态/行业/风险"通道命中的查询，避免误拦截
    #    例如"放量突破的股票"应走形态通道，"半导体放量"应走行业通道
    if is_volume_query and not detected_pattern and not detected_industry and not is_risk_query:
        print(f"📊 成交量选股快速通道触发: {query}")
        volume_result = ""
        try:
            volume_result = search_volume_anomalies.invoke({"days": 1, "min_score": 30, "limit": 15})
        except Exception as e:
            volume_result = f"成交量异动数据查询失败: {e}"

        # 辅助拉取行业资金流向
        sector_flow_for_vol = ""
        try:
            sector_flow_for_vol = tool_get_retail_money_flow.invoke({"days": 2})
        except Exception as e:
            sector_flow_for_vol = f"暂无行业资金流数据: {e}"

        vol_screen_prompt = f"""
                   你是一位资深选股专家。用户想找**成交量异常/放量/量能异动**的股票。

                   【数据源 A：成交量异动股票（按异动评分排序）】
                   {volume_result}

                   【数据源 B：市场资金风向 (行业)】
                   {sector_flow_for_vol}

                   【用户原始需求】: "{query}"
                   {followup_action_block}

                   【你的任务】
                   1. 从【数据源A】中展示成交量异动的股票，**不要编造数据**！
                   2. 结合【数据源B】分析这些股票所属板块的资金流向是否支持。
                   3. 区分分析：
                      - 📈 **放量上涨**：可能是主力资金进场突破信号
                      - 📉 **放量下跌**：可能是主力出货或恐慌抛售
                      - ⚖️ **放量横盘**：可能是换手充分，关注后续方向
                   4. 给出风险提示。

                   【输出格式】
                   📊 **成交量异动选股结果**

                   1. **股票名称** (代码) - 异动评分：XX
                      - 📊 量能情况：xxx
                      - 💰 资金面：所属板块资金xxx
                      - 💡 操作建议：xxx

                   ⚠️ **风险提示**：放量不等于利好，需结合价格走势判断。
                   """

        response = llm.invoke(vol_screen_prompt)

        codes = re.findall(r'[0-9]{6}\.[A-Z]{2}', response.content)
        if not codes:
            codes = re.findall(r'[0-9]{6}', response.content)
        next_symbol = codes[0] if codes else state.get("symbol", "")

        return {
            "messages": [HumanMessage(content=f"【精选股票】\n{response.content}")],
            "symbol": next_symbol
        }

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
            search_volume_anomalies,
            query_stock_volume,
            tool_get_retail_money_flow  # 资金流向
        ]

        current_date = datetime.now().strftime("%Y年%m月%d日")

        react_prompt = f"""
            你是一位资深选股专家，擅长通过概念主题、市场热点来挖掘投资机会。

            【当前日期】：{current_date}
            【用户需求】：{query}
            {followup_action_block}

            【你的核心能力：概念/主题选股】
            你主要处理的是"概念类"、"主题类"、"热点类"选股需求，例如：
            - "AI概念股有哪些" → 先 search_web 查概念股名单，再用 search_top_stocks 验证技术面
            - "低空经济相关的股票" → search_web 查相关个股，再用 search_top_stocks 交叉验证
            - "最近有什么好股票" → tool_get_retail_money_flow 看资金风口 + search_top_stocks 看强势股
            - "帮我选几只稳健的股票" → search_top_stocks(condition="综合评分") 选高分股

            【工具使用指南】
            1. `search_web`：搜索概念股名单、热点信息、行业新闻（搜索关键词如"xxx概念股 龙头"）
            2. `search_top_stocks`：按技术形态或综合评分筛选股票（condition 可填"综合评分"或具体形态名）
            3. `tool_get_retail_money_flow`：查看行业资金流向，判断哪些板块有资金支持
            4. `get_available_patterns`：查看今日市场有哪些K线形态可供筛选
            5. `search_investment_knowledge`：查询内部知识库获取投资参考
            6. `search_volume_anomalies`：查成交量异动股票（备用，成交量查询通常已被快速通道处理）
            7. `query_stock_volume`：查单只股票的成交量详情

            【标准流程】
            1. 理解用户想找什么类型的股票
            2. 选择最合适的 1-2 个工具获取数据
            3. 整理结果，说明推荐理由 + 风险提示

            【禁止事项】
            - 不要编造股票代码或名称
            - 如果搜索不到相关信息，诚实告知用户
            - 不要重复调用同一个工具
            """

        screener_react_agent = create_react_agent(llm, react_tools, prompt=react_prompt)

        partial_response = ""

        try:
            result = screener_react_agent.invoke(
                {"messages": [HumanMessage(content=query)]},
                {"recursion_limit": _get_agent_recursion_limit("screener_react", 20)}
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

    # ✅ 新增：拉取放量异动数据
    volume_anomaly_info = ""
    try:
        volume_anomaly_info = search_volume_anomalies.invoke({"days": 1, "min_score": 50, "limit": 10})
    except Exception as e:
        volume_anomaly_info = f"暂无放量数据: {e}"

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

                【数据源 C：今日成交量异动股（按评分排序）】
                {volume_anomaly_info}

                【用户原始需求】: "{query}"
                {followup_action_block}

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
                {followup_action_block}

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
                {followup_action_block}

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
    # 🔥 [修复] 只收集有标签的 worker 输出，排除用户原始消息
    WORKER_TAGS = [
        "【技术分析】", "【数据监控】", "【美股期权体检】", "【期权策略】", "【情报与舆情】",
        "【宏观策略】", "【知识问答】", "【精选股票】", "【毒舌点评】",
        "【王牌分析】", "【闲聊】", "【风控修正】", "【持仓分析】"
    ]

    agent_reports = state.get("agent_reports") or {}
    ordered_agent_names = [
        "macro_analyst",
        "analyst",
        "monitor",
        "researcher",
        "strategist",
        "portfolio_analyst",
        "screener",
        "generalist",
        "chatter",
        "roaster",
    ]
    worker_msgs = [
        HumanMessage(content=str(agent_reports[name]).strip())
        for name in ordered_agent_names
        if str(agent_reports.get(name, "") or "").strip()
    ]
    if not worker_msgs:
        worker_msgs = [
            m for m in all_messages
            if isinstance(m, HumanMessage)
               and len(m.content) > 10
               and any(tag in m.content for tag in WORKER_TAGS)
        ]

    # 🔥 [新增] 如果没有有效的 worker 输出，返回友好提示
    if not worker_msgs:
        symbol = state.get("symbol", "未知标的")
        return {
            "messages": [HumanMessage(
                content=f"⚠️ 抱歉，AI 团队未能生成关于 **{symbol}** 的有效分析报告。\n\n可能原因：\n- 标的代码不正确或不在支持范围内\n- 数据源暂时无法访问\n\n请检查代码后重试。")],
            "chart_img": ""
        }

    # 获取知识库内容

    # 拼接到一起用于输入
    context_text = "\n".join([f"{m.content}" for m in worker_msgs])
    user_query = state.get("user_query", "")
    option_scenario = detect_option_hypothetical_scenario(user_query)
    if _is_futures_broker_signal_task(user_query) and "【数据监控】" in context_text:
        return {
            "messages": [HumanMessage(content=context_text)],
            "chart_img": state.get("chart_img", ""),
        }

    vision_position_domain = str(state.get("vision_position_domain", "") or "").strip().lower()
    vision_position_payload = state.get("vision_position_payload") or {}
    vision_option_legs: List[Dict[str, Any]] = []
    if isinstance(vision_position_payload, dict):
        raw_legs = vision_position_payload.get("option_legs") or []
        if isinstance(raw_legs, list):
            vision_option_legs = [x for x in raw_legs if isinstance(x, dict)]
    canonical_option_legs = _build_canonical_option_legs(vision_option_legs)
    canonical_option_legs_block = _build_canonical_option_legs_block(canonical_option_legs)
    is_vision_option_mode = vision_position_domain in {"option", "mixed"}
    is_option_query = _contains_any(user_query, OPTION_QUERY_KEYWORDS) or is_vision_option_mode
    is_option_position_mode = _is_option_position_query(user_query) or is_vision_option_mode
    option_delta_cash_report = str(state.get("option_delta_cash_report", "") or "").strip()
    option_delta_cash_meta = state.get("option_delta_cash_meta") or {}
    option_delta_cash_gap_note = str(state.get("option_delta_cash_gap_note", "") or "").strip()
    option_delta_displayable = bool(state.get("option_delta_displayable", False))
    option_delta_execution_ready = bool(state.get("option_delta_execution_ready", False))
    option_delta_coverage_ratio = float(state.get("option_delta_coverage_ratio", 0.0) or 0.0)
    authoritative_underlying_quotes = state.get("authoritative_underlying_quotes") or {}
    authoritative_quote_block = str(state.get("authoritative_quote_block", "") or "")
    if is_option_position_mode and not authoritative_underlying_quotes:
        fallback_underlyings = _collect_quote_underlyings_from_canonical_legs(
            canonical_legs=canonical_option_legs,
            symbol_hint=state.get("symbol", ""),
        )
        if fallback_underlyings:
            try:
                authoritative_underlying_quotes = fetch_underlying_spot_map(underlyings=fallback_underlyings)
            except Exception as e:
                print(f"⚠️ finalizer 权威现价加载失败: {e}")
                authoritative_underlying_quotes = {}
    if not authoritative_quote_block and authoritative_underlying_quotes:
        authoritative_quote_block = _build_authoritative_quote_block(authoritative_underlying_quotes)

    def _lock_option_and_price(text: str) -> Dict[str, Any]:
        working_text = str(text or "")
        if is_option_position_mode:
            # finalizer 可能压缩掉结构化章节，这里只补缺失章节，避免“去重后缺章”。
            working_text = _ensure_option_position_structure(
                text=working_text,
                delta_cash_report=option_delta_cash_report,
                delta_cash_gap_note=option_delta_cash_gap_note,
                trend_signal=str(state.get("trend_signal", "")),
                risk_preference=str(state.get("risk_preference", "稳健型")),
                key_levels=str(state.get("key_levels", "")),
            )
        working_text = _ensure_delta_section_from_meta(
            text=working_text,
            delta_report=option_delta_cash_report,
            delta_meta=option_delta_cash_meta,
            delta_gap_note=option_delta_cash_gap_note,
            displayable=option_delta_displayable,
        )
        locked_delta_text = _replace_delta_cash_section(text=working_text, delta_report=option_delta_cash_report)
        lock_result = _apply_option_fact_lock(
            text=locked_delta_text,
            canonical_legs=canonical_option_legs,
            strict_cover=is_option_position_mode and bool(canonical_option_legs),
        )
        locked_text = str(lock_result.get("text") or locked_delta_text)
        quote_lock_result = _apply_authoritative_quote_lock(
            text=locked_text,
            authoritative_quotes=authoritative_underlying_quotes,
            authoritative_quote_block=authoritative_quote_block,
            strict_cover=is_option_position_mode and bool(authoritative_underlying_quotes),
        )
        locked_canonical_block = str(lock_result.get("canonical_option_legs_block") or canonical_option_legs_block)
        direction_conflict_count = int(lock_result.get("option_direction_conflict_count") or 0)
        final_quote_block = str(quote_lock_result.get("authoritative_quote_block") or authoritative_quote_block)
        price_conflict_count = int(quote_lock_result.get("price_conflict_count") or 0)
        locked_text = _dedupe_option_position_sections(str(quote_lock_result.get("text") or locked_text))
        if is_option_position_mode:
            delta_block_for_compose = ""
            if option_delta_displayable:
                delta_block_for_compose = (
                    _extract_delta_cash_block(option_delta_cash_report)
                    or _extract_delta_cash_block(locked_text)
                    or _extract_delta_cash_block(str((option_delta_cash_meta or {}).get("report") or ""))
                )
                if not delta_block_for_compose and isinstance(option_delta_cash_meta, dict):
                    delta_block_for_compose = _build_min_delta_block_from_meta(option_delta_cash_meta)
            elif option_delta_cash_gap_note:
                delta_block_for_compose = f"### 【DeltaCash】\n- 数据缺口: {option_delta_cash_gap_note}"
            structured_sections = {
                "holdings": locked_canonical_block if canonical_option_legs else "",
                "quotes": final_quote_block if authoritative_underlying_quotes else "",
                "delta": delta_block_for_compose,
            }
            locked_text = _compose_option_sections(
                text=locked_text,
                structured_sections=structured_sections,
                keep_only_whitelist=True,
            )
        # 期权持仓模式且Delta可展示时，强制保证最终正文含有唯一的Delta区块
        if is_option_position_mode and option_delta_displayable and "delta" not in _collect_option_section_ids(locked_text):
            fallback_structured = _ensure_option_position_structure(
                text=str(text or ""),
                delta_cash_report=option_delta_cash_report,
                delta_cash_gap_note=option_delta_cash_gap_note,
                trend_signal=str(state.get("trend_signal", "")),
                risk_preference=str(state.get("risk_preference", "稳健型")),
                key_levels=str(state.get("key_levels", "")),
            )
            fallback_structured = _ensure_delta_section_from_meta(
                text=fallback_structured,
                delta_report=option_delta_cash_report,
                delta_meta=option_delta_cash_meta,
                delta_gap_note=option_delta_cash_gap_note,
                displayable=True,
            )
            fallback_locked = _apply_option_fact_lock(
                text=fallback_structured,
                canonical_legs=canonical_option_legs,
                strict_cover=bool(canonical_option_legs),
            )
            fallback_quote_locked = _apply_authoritative_quote_lock(
                text=str(fallback_locked.get("text") or fallback_structured),
                authoritative_quotes=authoritative_underlying_quotes,
                authoritative_quote_block=authoritative_quote_block,
                strict_cover=bool(authoritative_underlying_quotes),
            )
            locked_text = _dedupe_option_position_sections(
                str(fallback_quote_locked.get("text") or fallback_structured)
            )
            locked_text = _compose_option_sections(
                text=locked_text,
                structured_sections={
                    "holdings": locked_canonical_block if canonical_option_legs else "",
                    "quotes": final_quote_block if authoritative_underlying_quotes else "",
                    "delta": _extract_delta_cash_block(option_delta_cash_report)
                    or _extract_delta_cash_block(str((option_delta_cash_meta or {}).get("report") or "")),
                },
                keep_only_whitelist=True,
            )
            locked_canonical_block = str(
                fallback_locked.get("canonical_option_legs_block") or locked_canonical_block
            )
            direction_conflict_count = int(
                fallback_locked.get("option_direction_conflict_count") or direction_conflict_count
            )
            final_quote_block = str(
                fallback_quote_locked.get("authoritative_quote_block") or final_quote_block
            )
            price_conflict_count = int(
                fallback_quote_locked.get("price_conflict_count") or price_conflict_count
            )
        return {
            "text": locked_text,
            "canonical_option_legs_block": locked_canonical_block,
            "option_direction_conflict_count": direction_conflict_count,
            "authoritative_quote_block": final_quote_block,
            "price_conflict_count": price_conflict_count,
        }
    explicit_stock_portfolio_coupling = _contains_any(user_query, EXPLICIT_STOCK_PORTFOLIO_COUPLING_KEYWORDS)
    allow_stock_portfolio_blend = (not is_option_query) or explicit_stock_portfolio_coupling
    if is_option_query and not explicit_stock_portfolio_coupling:
        context_text = _strip_stock_portfolio_sections(context_text)
    if option_delta_cash_report and "DeltaCash" not in context_text:
        context_text = f"{context_text}\n\n{option_delta_cash_report}"
    elif option_delta_cash_gap_note and option_delta_cash_gap_note not in context_text:
        context_text = f"{context_text}\n\n> ⚠️ **Delta数据缺口**：{option_delta_cash_gap_note}"
    is_pure_screener_source = bool(worker_msgs) and all(
        str(getattr(msg, "content", "") or "").strip().startswith("【精选股票】")
        for msg in worker_msgs
    )
    if "【精选股票】" in context_text and is_pure_screener_source:
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
            chart_matches = re.findall(r'IMAGE_CREATED:(chart_[a-zA-Z0-9_]+\.json)', context_text)
            if chart_matches:
                chart_img = chart_matches[-1]
        return {
            "messages": [HumanMessage(content=roaster_content if roaster_content else context_text)],
            "chart_img": chart_img
        }
    is_pure_researcher_source = bool(worker_msgs) and all(
        str(getattr(msg, "content", "") or "").strip().startswith("【情报与舆情】")
        for msg in worker_msgs
    )
    if is_pure_researcher_source and not _is_news_impact_query(user_query):
        sanitized_context_text = _clean_finalizer_internal_labels(
            _sanitize_unauthorized_technical_indicators(context_text, query=user_query)
        )
        return {
            "messages": [HumanMessage(content=sanitized_context_text)],
            "chart_img": state.get("chart_img", ""),
        }
    # === 判断逻辑：单兵还是团战？ ===
    # 如果只有 1 个工种发言（或者没有发言），且不是王牌分析师（王牌本来就是总结好的）
    symbol = state.get("symbol", "")
    symbol_name = state.get("symbol_name", "")
    mem_context = state.get("memory_context", "")
    profile_context = state.get("profile_context", "")
    macro_view = state.get("macro_view", "无宏观分析")
    trend = state.get("trend_signal", "")  # 例如 "看涨"
    key_levels = state.get("key_levels", "")  # 例如 "压力3000"

    # 🔥 [新增] 获取持仓上下文
    portfolio_corr_index = state.get("portfolio_top_corr_index", "")
    portfolio_corr_value = state.get("portfolio_top_corr_value", "")
    portfolio_risks = state.get("portfolio_risks", "")

    risk_pref = state.get("risk_preference", "稳健型")
    is_single_source = len(worker_msgs) <= 1
    has_chart = "chart_" in context_text or "![" in context_text
    complex_keywords = ["画", "图", "对比", "分析", "价差", "相关性", "走势"]
    is_complex_task = any(kw in user_query for kw in complex_keywords)
    is_news_impact = _is_news_impact_query(user_query)

    display_name = f"{symbol_name}({symbol})" if symbol_name else symbol
    finalizer_data_policy = build_data_policy_context(symbol=display_name, mode="finalizer")

    def _extract_finalizer_chart_img() -> str:
        local_chart_img = state.get("chart_img", "")
        if state.get("macro_chart"):
            local_chart_img = state.get("macro_chart")
        if not local_chart_img:
            chart_matches = re.findall(r'IMAGE_CREATED:(chart_[a-zA-Z0-9_]+\.json)', context_text)
            if chart_matches:
                local_chart_img = chart_matches[-1]
                print(f"📊 finalizer 从报告中提取到图表: {local_chart_img}")
        return local_chart_img

    def _normalize_symbol_text(text: str) -> str:
        return re.sub(r'[^A-Z0-9]', '', (text or "").upper())

    def _build_symbol_aliases(raw_symbol: str) -> set[str]:
        aliases = set()
        normalized = _normalize_symbol_text(raw_symbol)
        if not normalized:
            return aliases
        aliases.add(normalized)
        alpha_prefix = re.match(r'^[A-Z]+', normalized)
        if alpha_prefix:
            aliases.add(alpha_prefix.group(0))
        if normalized.isdigit() and len(normalized) == 6:
            aliases.add(f"{normalized}ETF")
            if normalized.startswith("51"):
                try:
                    aliases.add(f"{int(normalized[-3:])}ETF")
                except ValueError:
                    pass
        return {item for item in aliases if len(item) >= 2}

    if is_news_impact and not is_option_position_mode:
        news_prompt = f"""
                你是交易台的首席投资官，但当前只处理“新闻/事件影响解读”。
                这次不要写成长篇投研报告，不要使用“Executive Summary / 市场深度解析 / 交易策略部署 / 首席投资官寄语”这套长模板。

                【当前日期】：{today_str}
                【用户问题】：{user_query}
                【分析标的】：{display_name}
                【客户风险偏好】：{risk_pref}

                【团队报告池，必须优先采信】：
                {context_text}

                【已有的宏观分析报告】：
                {macro_view}

                {finalizer_data_policy}

                【输出目标】：
                用交易员风格，给一份短、清楚、能落地的“事件快评”。
                先说主线，再说盘面验证，再说反向风险，最后说接下来盯什么和怎么应对。

                【强制要求】：
                1. 不要写成长报告，不要超过 6 个小节。
                2. 每个小节 1-3 条，短句，口语化，别学术化。
                3. 只用团队报告池里已有的信息，不要臆造新数据。
                4. 如果证据不够，就明确说“这波先按交易假设看，不算确认”。
                5. 如果是宏观事件对市场影响，重点回答传导链，不要展开大而全资产配置长文。

                【格式模板】：
                > 📅 日期：{today_str}
                > ✍️ 签发：交易台CIO | 🎯 模式：事件快评

                ### 交易台一句话
                - 用一句话先回答用户。

                ### 主线
                - 现在市场主要在交易什么。

                ### 盘面验证
                - 哪些价格/利率/美元/情绪/IV 信号在配合。

                ### 反向风险
                - 什么情况会让这条逻辑失效或先回吐。

                ### 接下来盯什么
                - 未来 1-3 个最重要的观察点。

                ### 交易应对
                - 给偏稳健的应对方式，讲清楚是不追、等回踩、还是只做轻仓确认。
                """
        final_verdict = llm.invoke(news_prompt)
        final_text = f"【最终决策】\n{_sanitize_unauthorized_technical_indicators(final_verdict.content, query=user_query)}"
        lock_result = _lock_option_and_price(final_text)
        return {
            "messages": [HumanMessage(content=str(lock_result.get("text") or final_text))],
            "chart_img": _extract_finalizer_chart_img(),
            "canonical_option_legs_block": str(lock_result.get("canonical_option_legs_block") or canonical_option_legs_block),
            "option_direction_conflict_count": int(lock_result.get("option_direction_conflict_count") or 0),
            "authoritative_underlying_quotes": authoritative_underlying_quotes,
            "authoritative_quote_block": str(lock_result.get("authoritative_quote_block") or authoritative_quote_block),
            "price_conflict_count": int(lock_result.get("price_conflict_count") or 0),
        }

    # 获取当前最后一次执行的计划（用于判断是不是王牌）
    # (由于 state plan 被 pop 了，我们用简单的长度判断通常够用，或者看 context)


    if is_single_source and not has_chart and not is_complex_task and not is_option_position_mode:
        # === 模式 A：质检员 (Audit Mode) ===
        # 目标：保留原汁原味的排版，只查错
        symbol_aliases = _build_symbol_aliases(symbol)
        if not symbol_aliases:
            # symbol 兜底：尝试从单信源报告中抓标题代码
            match = re.search(r'([A-Za-z]{1,6}\d{0,4}|\d{6}(?:ETF)?)\s*(?:技术面|技术分析|走势)', context_text, re.IGNORECASE)
            if match:
                symbol_aliases = _build_symbol_aliases(match.group(1))
        source_norm = _normalize_symbol_text(context_text)
        enforce_symbol_lock = bool(symbol_aliases) and any(alias in source_norm for alias in symbol_aliases)
        symbol_lock_hint = (
            f"\n        5. **标的锁定**：本报告标的是 {symbol or '/'.join(sorted(symbol_aliases))}。"
            f" 你不能改成其他标的。若无法确认，请输出 DIRECT_PASS。"
            if enforce_symbol_lock else ""
        )
        audit_prompt = f"""
        你是一位交易风控官。团队提交了一份分析报告（如下）。

        【待审核报告】：
        {context_text}

        {finalizer_data_policy}

        【任务】：
        1. 检查报告是否存在**致命的常识性错误**（如把标的搞错、逻辑完全相反）。
        2. **如果报告无误**：请直接输出四个字 "DIRECT_PASS" (不要输出其他符号)。这意味着直接采用原报告，保留其完美的 Markdown 排版。
        3. **如果有致命错误**：请修改错误后，重写一份正确的报告。
        4. 如果发生数据缺失或语法错误，不要把错误写出来。
        {symbol_lock_hint}
        """
        response = llm.invoke(audit_prompt)

        # 如果 LLM 觉得没问题，返回特定标记
        if "DIRECT_PASS" in response.content:
            sanitized_context_text = _sanitize_unauthorized_technical_indicators(context_text, query=user_query)
            lock_result = _lock_option_and_price(sanitized_context_text)
            return {
                "messages": [HumanMessage(content=str(lock_result.get("text") or sanitized_context_text))],
                "canonical_option_legs_block": str(lock_result.get("canonical_option_legs_block") or canonical_option_legs_block),
                "option_direction_conflict_count": int(lock_result.get("option_direction_conflict_count") or 0),
                "authoritative_underlying_quotes": authoritative_underlying_quotes,
                "authoritative_quote_block": str(lock_result.get("authoritative_quote_block") or authoritative_quote_block),
                "price_conflict_count": int(lock_result.get("price_conflict_count") or 0),
            }
        else:
            revised_text = response.content or ""
            if enforce_symbol_lock:
                revised_norm = _normalize_symbol_text(revised_text)
                keep_symbol = any(alias in revised_norm for alias in symbol_aliases)
                if not keep_symbol:
                    print(f"⚠️ finalizer 审校疑似串标，回退原报告。locked={symbol_aliases}")
                    sanitized_context_text = _sanitize_unauthorized_technical_indicators(context_text, query=user_query)
                    lock_result = _lock_option_and_price(sanitized_context_text)
                    return {
                        "messages": [HumanMessage(content=str(lock_result.get("text") or sanitized_context_text))],
                        "canonical_option_legs_block": str(lock_result.get("canonical_option_legs_block") or canonical_option_legs_block),
                        "option_direction_conflict_count": int(lock_result.get("option_direction_conflict_count") or 0),
                        "authoritative_underlying_quotes": authoritative_underlying_quotes,
                        "authoritative_quote_block": str(lock_result.get("authoritative_quote_block") or authoritative_quote_block),
                        "price_conflict_count": int(lock_result.get("price_conflict_count") or 0),
                    }
            revised_text = _sanitize_unauthorized_technical_indicators(revised_text, query=user_query)
            revised_text = _clean_finalizer_internal_labels(revised_text)
            lock_result = _lock_option_and_price(revised_text)
            # 如果有错被重写了，就返回重写的内容
            return {
                "messages": [HumanMessage(content=str(lock_result.get("text") or revised_text))],
                "canonical_option_legs_block": str(lock_result.get("canonical_option_legs_block") or canonical_option_legs_block),
                "option_direction_conflict_count": int(lock_result.get("option_direction_conflict_count") or 0),
                "authoritative_underlying_quotes": authoritative_underlying_quotes,
                "authoritative_quote_block": str(lock_result.get("authoritative_quote_block") or authoritative_quote_block),
                "price_conflict_count": int(lock_result.get("price_conflict_count") or 0),
            }

    else:
        # === 模式 B：总编辑 (Editor Mode) ===
        # 目标：多源信息整合，但要根据用户问题类型调整输出风格

        # 判断是否为"纯数据查询"类问题
        data_query_keywords = ["持仓", "排名", "资金", "流入", "流出", "多少", "哪些", "哪个", "前几", "前3", "前三",
                               "前5", "前五", "top", "龙虎榜", "增仓", "减仓", "净持仓", "最多", "最大"]
        is_data_query = any(kw in user_query for kw in data_query_keywords)

        # 判断是否为"综合分析"类问题
        analysis_keywords = ["分析", "怎么看", "怎么做", "策略", "建议", "操作", "行情", "走势", "如何","趋势", "全面"]
        is_analysis_query = any(kw in user_query for kw in analysis_keywords)
        force_option_deep_mode = is_option_position_mode or option_scenario.active

        # 🎯 根据问题类型选择不同的 Prompt
        if is_data_query and not is_analysis_query and not force_option_deep_mode:
            # === 数据查询模式：简洁直接 ===
            cio_prompt = f"""
                你是一位数据检查师。
                【当前日期】：{today_str}
                【用户问题】：{user_query}
                【分析标的】: {display_name}

                【团队收集的数据】：
                {context_text}

                {finalizer_data_policy}

                【输出要求】：
                1. **直接回答用户的问题**，不要跑题！用户问持仓就答持仓，问排名就答排名。
                2. **突出数据本身**：用表格或列表清晰展示数据。
                3. **简短点评**：可以加 1-2 句对数据的解读（如"XX 在大幅增仓，可能看多"），但不要扯到技术面K线分析。
                4. 数据不要编造和修改。
                5. 不要写成投资报告，文字要简洁有力。
                6. 如果发生数据缺失或语法错误，不要把错误写出来。
                7. 数据是每天下午5点后更新。
                8. {"当前是期权问题，只回答期权持仓/期权数据，不要展开股票持仓体检内容。" if (is_option_query and not allow_stock_portfolio_blend) else "按用户问题口径输出，不要扩展无关持仓模块。"}
                9. {"如果团队报告中有【DeltaCash】区块，必须保留其中关键数值并输出目标区间与建议调整量。" if (option_delta_cash_report and option_delta_execution_ready) else ("如果团队报告中有【DeltaCash】区块，但覆盖率不足，请只给方向性建议与补数清单，不要输出金额级调整量。" if option_delta_cash_report else ("如果出现Delta数据缺口，只说明缺口，不要编造Delta数值。" if option_delta_cash_gap_note else "无额外量化模块要求。"))}


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
            # 🔥 [新增] 构建持仓上下文提示
            portfolio_context_prompt = ""
            if allow_stock_portfolio_blend and portfolio_corr_index and portfolio_corr_value:
                portfolio_context_prompt = f"""
                【客户持仓关键信息】：
                - 持仓组合与{portfolio_corr_index}指数相关度最高（相关系数{portfolio_corr_value}）
                - 持仓风险：{portfolio_risks if portfolio_risks else "未提及"}

                ⚠️ **重要**：如果团队报告中既有【持仓分析】又有【期权策略】，你必须在整合时明确说明两者的逻辑关联！
                例如："考虑到您的持仓与{portfolio_corr_index}高度相关，策略团队建议的{symbol}期权策略可以作为对冲/增强工具..."
                """
            option_focus_prompt = ""
            if is_option_query and not allow_stock_portfolio_blend:
                option_focus_prompt = """
                【期权优先整合模式】：
                - 当前问题属于期权域，禁止展开“股票持仓体检”段落。
                - 只允许整合与期权持仓、期权策略、期权风险直接相关的信息。
                - 若出现跨域冲突，以【期权策略】与【技术分析】中与期权相关内容为准。
                - 方向术语必须统一为：买认购/卖认购/买认沽/卖认沽。禁止输出 Long/Short Call/Put。
                """
                if vision_position_domain == "mixed":
                    option_focus_prompt += """
                - 已识别到混合持仓截图，必须用一句话提示“股票持仓已识别，本轮未展开股票体检”。
                    """
                if is_option_position_mode:
                    option_focus_prompt += """
                    - 回答必须包含：持仓拆解、净暴露/到期错配、三情景分支、两套调整方案、风控阈值、当日执行清单。
                    - “标的现价”只能引用系统给定的权威现价表，禁止从行权价推导/缩放现价。
                    """
                    if canonical_option_legs_block:
                        option_focus_prompt += f"""
                    - “1. 持仓拆解表”必须逐行使用以下识别锁定事实（不得改写方向/购沽）：
{canonical_option_legs_block}
                        """
                    if authoritative_quote_block:
                        option_focus_prompt += f"""
                    - 标的现价（权威数据）必须按下表引用：
{authoritative_quote_block}
                        """
                    if option_delta_cash_report:
                        option_focus_prompt += """
                    - 必须保留并明确展示【DeltaCash】核心数值：Total Delta Cash、Delta Ratio、技术面目标区间。
                    - 若【DeltaCash】包含“调仓优先队列”，必须沿用该顺序，不得自由改写优先级。
                        """
                        if option_delta_execution_ready:
                            option_focus_prompt += """
                    - 覆盖率达标时，必须输出金额级建议调整量（元）并匹配执行方向。
                            """
                        else:
                            option_focus_prompt += f"""
                    - 覆盖率仅 {option_delta_coverage_ratio * 100:.1f}%（阈值 {int(DELTA_EXECUTION_COVERAGE_THRESHOLD * 100)}%），只允许给方向性建议和补数清单，不得给金额级执行量。
                            """
                    elif option_delta_cash_gap_note:
                        option_focus_prompt += """
                    - Delta数据缺口时只说明缺口与补数动作，不要输出伪精确DeltaCash数值。
                        """

            cio_prompt = f"""
                你是这家交易公司的**首席投资官 (CIO)**。
                你的团队（分析师、策略员、监控员等）提交了多份分散的报告。
                【当前日期】：{today_str}
                【用户问题】：{user_query}
                【分析标的】: {display_name}
                【客户风险偏好】：{risk_pref}（请在【交易策略部署】和操作建议中，根据此风险偏好调整建议的激进程度）

                【团队报告池，必须优先采用！】：
                {context_text}

                【客户对话历史记忆】{mem_context}

                【用户专属画像】{profile_context if profile_context else "无"}

                {portfolio_context_prompt}
                {option_focus_prompt}
                {finalizer_data_policy}

                【📚 内部知识库 (基于"{enhanced_query}"检索)】：
                {kb_context}

                【任务】：
                请将上述零散报告整合成一份《深度投资决策书》，要求**排版精美、逻辑结构化**。
                1. 技术面分析以K线为主，均线为辅。如果没有数据，技术面这区块就省略。
                2. 知识要参考{kb_context}，但要根据当下市场情况，自己理解后输出。
                3. 如果记忆或画像里有客户的持仓、偏好、风险边界，在报告里可以针对性地写；如果当前问题与画像冲突，以当前问题为准。
                3.1 【用户专属画像】采用克制自然口径：策略、仓位、风险问题参考交易画像；解释类比或个人化表达才参考个人画像；年龄、性别不得作为交易判断依据。
                4. 所有价格数据（当前价、涨跌幅、支撑位、压力位、均线值），必须使用来自【团队报告池】；团队报告池没有就写“当前数据源未查到”，禁止补数。
                4.1 所有基本面事实（财报数字、公告、机构目标价、业务进展）必须来自【情报与舆情】或团队报告池；没有研究员资料时，不得自行编造。
                4.2 技术面默认只允许 K 线和均线；禁止输出 RSI、MACD、KDJ、BOLL、布林、量能突破等非授权指标。
                5. **【关键】**：如果报告池中包含持仓分析和策略建议，必须在整合时解释清楚策略如何服务于持仓管理（对冲/增强/风控），不要让两部分孤立存在。
                
                【数据与交易边界】
                1. 股票自身不展开场内期权策略；若涉及对冲，只能说明可通过相关 ETF 期权表达。
                2. 商品期货存在期权；如果遇到数据矛盾，以 strategist 与确定性工具结果为主。
                3. 消息与价格背离时，用“利多不涨/利空不跌”的交易验证框架提醒反向风险。
                4. 在分析宏观前，要引用【宏观分析报告】的结论。
                
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
                7. 报告里不要参考 RSI、MACD、KDJ、BOLL、布林、量能突破等非授权技术指标！
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

        if option_scenario.active:
            cio_prompt += build_finalizer_scenario_context(option_scenario)

        final_verdict = llm.invoke(cio_prompt)

        # 🔥 [新增] 从原始报告中提取图表路径（因为 finalizer 是整理者，图表在前面节点生成）
        chart_img = state.get("chart_img", "")
        if state.get("macro_chart"):  # 如果有宏观图，优先用宏观图
            chart_img = state.get("macro_chart")

        # 如果 state 中没有，尝试从报告内容中提取
        if not chart_img:
            chart_matches = re.findall(r'IMAGE_CREATED:(chart_[a-zA-Z0-9_]+\.json)', context_text)
            if chart_matches:
                chart_img = chart_matches[-1]
                print(f"📊 finalizer 从报告中提取到图表: {chart_img}")

        final_text = f"【最终决策】\n{_sanitize_unauthorized_technical_indicators(final_verdict.content, query=user_query)}"
        lock_result = _lock_option_and_price(final_text)
        return {
            "messages": [HumanMessage(content=str(lock_result.get("text") or final_text))],
            "chart_img": chart_img,  # 🔥 返回图表路径
            "canonical_option_legs_block": str(lock_result.get("canonical_option_legs_block") or canonical_option_legs_block),
            "option_direction_conflict_count": int(lock_result.get("option_direction_conflict_count") or 0),
            "authoritative_underlying_quotes": authoritative_underlying_quotes,
            "authoritative_quote_block": str(lock_result.get("authoritative_quote_block") or authoritative_quote_block),
            "price_conflict_count": int(lock_result.get("price_conflict_count") or 0),
        }


# ==========================================
# 📊 持仓分析师 (Portfolio Analyst)
# ==========================================
def portfolio_analyst_node(state: AgentState, llm):
    """
    持仓分析专家：分析用户持仓结构、风险特征和交易风格
    """
    query = state["user_query"]
    user_id = state.get("user_id", "")
    has_portfolio = state.get("has_portfolio", False)
    current_date = datetime.now().strftime("%Y年%m月%d日 %A")

    # 如果用户没有持仓数据，直接返回
    if not has_portfolio or not user_id:
        return {
            "messages": [HumanMessage(content="【持仓分析】用户暂无持仓数据，无法提供持仓相关分析。")],
            "portfolio_summary": "",
            "portfolio_risks": "",
            "trading_style": "",
            "portfolio_top_corr_index": "",
            "portfolio_top_corr_value": ""
        }

    # 配置工具
    tools = [
        get_user_portfolio_summary,
        get_user_portfolio_details,
        analyze_user_trading_style,
        check_portfolio_risks
    ]

    persona_prompt = f"""
    你是一位专业的持仓分析师，专注于：
    1. 分析用户当前持仓结构和风险特征
    2. 评估持仓与市场的相关度
    3. 识别用户的交易风格和偏好
    4. 结合用户实际持仓给出个性化建议

    【当前日期】：{current_date}
    【用户ID】：{user_id}
    【客户需求】：{query}

    【可调用工具】
    1. get_user_portfolio_summary - 获取持仓摘要（轻量级）
    2. get_user_portfolio_details - 获取持仓详情（完整数据）
    3. analyze_user_trading_style - 分析交易风格
    4. check_portfolio_risks - 检查持仓风险

    【任务】：
    1. 首先调用 get_user_portfolio_summary 了解用户持仓概况
    2. 根据查询需求，选择性调用其他工具获取详细信息
    3. 分析用户持仓特点、风险点和改进建议
    4. 如果用户查询涉及特定标的，分析该标的在用户组合中的占比和作用
    5. 提供专业、客观的分析，避免过度乐观或悲观
    6. **重要**：如果工具返回了portfolio_corr（组合相关度），必须明确指出"您的持仓与XX指数相关度最高，达到X.XX"

    【输出格式要求】：
    - 如果涉及指数相关性，用这样的格式：【指数相关性】您的持仓组合与XX指数相关度最高，相关系数为X.XX
    - 风险提示用：【风险提示】xxx
    - 交易风格用：【交易风格】xxx

    【注意】：
    - 数据来自用户上传的持仓截图分析结果
    - 如果持仓数据较旧（超过7天），提醒用户更新
    - 风险提示要明确具体，避免模糊表述
    """

    portfolio_agent = create_react_agent(llm, tools, prompt=persona_prompt)

    try:
        result = portfolio_agent.invoke(
            {"messages": state["messages"]},
            {"recursion_limit": 20}
        )

        last_response = result["messages"][-1].content

        # 提取关键信息（用于其他节点参考）
        portfolio_summary = ""
        portfolio_risks = ""
        trading_style = ""
        portfolio_top_corr_index = ""
        portfolio_top_corr_value = ""

        # 简单提取（可以更智能）
        if "总市值" in last_response or "持仓" in last_response:
            portfolio_summary = last_response[:200]  # 前200字作为摘要

        if "风险" in last_response:
            risk_match = re.search(r'【风险提示】(.*?)(?:【|$)', last_response, re.DOTALL)
            if risk_match:
                portfolio_risks = risk_match.group(1).strip()[:150]

        if "风格" in last_response or "偏好" in last_response:
            style_match = re.search(r'(稳健型|激进型|平衡型|保守型)', last_response)
            if style_match:
                trading_style = style_match.group(1)

        # 🔥 提取指数相关性信息（用于策略推荐）
        if "指数相关性" in last_response or "相关系数" in last_response:
            corr_match = re.search(r'【指数相关性】.*?与(.+?)指数.*?相关系数为?([\d\.]+)', last_response)
            if corr_match:
                portfolio_top_corr_index = corr_match.group(1).strip()
                portfolio_top_corr_value = corr_match.group(2).strip()
                print(f"✅ 提取到指数相关性: {portfolio_top_corr_index} = {portfolio_top_corr_value}")

        return {
            "messages": [HumanMessage(content=f"【持仓分析】\n{last_response}")],
            "portfolio_summary": portfolio_summary,
            "portfolio_risks": portfolio_risks,
            "trading_style": trading_style,
            "portfolio_top_corr_index": portfolio_top_corr_index,
            "portfolio_top_corr_value": portfolio_top_corr_value
        }

    except Exception as e:
        print(f"Portfolio Analyst Node Error: {e}")
        return {
            "messages": [HumanMessage(content=f"【持仓分析】分析受阻: {e}")],
            "portfolio_summary": "",
            "portfolio_risks": "",
            "trading_style": "",
            "portfolio_top_corr_index": "",
            "portfolio_top_corr_value": ""
        }


# ==========================================
# 4. 构建图 (The Graph)
# ==========================================

def build_trading_graph(fast_llm, mid_llm, smart_llm, screen_compiler_llm=None):
    """
    构建并编译 LangGraph
    """
    workflow = StateGraph(AgentState)

    def _query_preview(state: AgentState, limit: int = 80) -> str:
        return str(state.get("user_query", "") or "").strip().replace("\n", " ")[:limit]

    def _extract_latest_report(update: Dict[str, Any]) -> str:
        for msg in reversed(list(update.get("messages", []) or [])):
            content = str(getattr(msg, "content", "") or "").strip()
            if content:
                return content
        return ""

    def _wrap_worker(name: str, fn):
        def _runner(state: AgentState):
            batch_index = int(state.get("current_batch_index", 0) or 0)
            started_at = time.perf_counter()
            print(
                f"[analysis-node-start] batch={batch_index} "
                f"step={name} query={_query_preview(state)}"
            )
            update = fn(state)
            report = _extract_latest_report(update)
            duration_ms = int((time.perf_counter() - started_at) * 1000)
            wrapped = dict(update)
            wrapped["completed_steps"] = [name]
            if report:
                wrapped["agent_reports"] = {name: report}
            _append_agent_trace_event(
                state,
                "node_done",
                {
                    "node": name,
                    "batch_index": batch_index,
                    "duration_ms": duration_ms,
                    "report_len": len(report),
                    "has_report": bool(report),
                },
            )
            print(
                f"[analysis-node-done] batch={batch_index} "
                f"step={name} report_len={len(report)} duration_ms={duration_ms}"
            )
            return wrapped

        return _runner

    def _run_supervisor(state: AgentState):
        update = supervisor_node(state, fast_llm)
        plan = list(update.get("plan", []) or [])
        execution_batches = _build_execution_batches(plan)
        route_decision = update.get("route_decision", {}) or {}
        print(
            f"[analysis-batches] plan={plan} "
            f"batches={execution_batches} "
            f"route_mode={route_decision.get('route_mode', '')} "
            f"route_confidence={route_decision.get('confidence', '')} "
            f"query={_query_preview(state)}"
        )
        wrapped = dict(update)
        wrapped["execution_batches"] = execution_batches
        wrapped["current_batch_index"] = 0
        wrapped["completed_steps"] = []
        wrapped["agent_reports"] = {}
        _append_agent_trace_event(
            state,
            "supervisor_plan",
            {
                "plan": plan,
                "batches": execution_batches,
                "symbol": str(update.get("symbol", "") or ""),
                "route_mode": route_decision.get("route_mode", ""),
                "route_confidence": route_decision.get("confidence", ""),
                "selected_expert_count": route_decision.get("selected_expert_count", ""),
            },
        )
        return wrapped

    def _run_generalist(state: AgentState):
        chosen_tier = _select_generalist_model_tier(state)
        chosen_llm = smart_llm if chosen_tier == "smart" else mid_llm
        query_preview = str(state.get("user_query", "") or "").strip().replace("\n", " ")[:120]
        print(
            f"[generalist-tier] tier={chosen_tier} "
            f"is_followup={bool(state.get('is_followup', False))} "
            f"wants_chart={_wants_chart(state.get('user_query', ''))} "
            f"query={query_preview}"
        )
        return generalist_node(state, chosen_llm)

    # 1. 注册节点
    # 主管 -> 用 Turbo (快)
    workflow.add_node("supervisor", _run_supervisor)
    # 分析师 -> 用 Plus (均衡)
    workflow.add_node("analyst", _wrap_worker("analyst", lambda state: analyst_node(state, mid_llm)))
    # 策略员 -> 用 Plus (均衡，优先提速)
    workflow.add_node("strategist", _wrap_worker("strategist", lambda state: strategist_node(state, mid_llm)))
    # 王牌 -> 按场景分级选模型
    workflow.add_node("generalist", _wrap_worker("generalist", _run_generalist))
    # CIO -> 用 Max (聪明)
    workflow.add_node("finalizer", _wrap_worker("finalizer", lambda state: finalizer_node(state, mid_llm)))
    # 其他工具人 (不需要 LLM，或者随便给一个)
    workflow.add_node("monitor", _wrap_worker("monitor", lambda state: monitor_node(state, mid_llm)))
    workflow.add_node("researcher", _wrap_worker("researcher", lambda state: researcher_node(state, mid_llm)))
    workflow.add_node("chatter", _wrap_worker("chatter", lambda state: chatter_node(state, mid_llm)))
    workflow.add_node(
        "screener",
        _wrap_worker(
            "screener",
            lambda state: screener_node(
                state,
                mid_llm,
                compiler_llm=screen_compiler_llm,
            ),
        ),
    )
    workflow.add_node("roaster", _wrap_worker("roaster", lambda state: roaster_node(state, mid_llm)))
    workflow.add_node("macro_analyst", _wrap_worker("macro_analyst", lambda state: macro_analyst_node(state, mid_llm)))
    # 持仓分析师 -> 用 Plus (均衡)
    workflow.add_node("portfolio_analyst", _wrap_worker("portfolio_analyst", lambda state: portfolio_analyst_node(state, mid_llm)))

    # 2. 设置入口
    workflow.set_entry_point("supervisor")

    workflow.add_node("manager", lambda state: {"current_step": "managing"})

    def _batch_complete(state: AgentState):
        current_batch_index = int(state.get("current_batch_index", 0) or 0)
        execution_batches = state.get("execution_batches") or _build_execution_batches(state.get("plan", []))
        completed_steps = list(state.get("completed_steps", []) or [])
        current_batch = execution_batches[current_batch_index] if current_batch_index < len(execution_batches) else []
        print(
            f"[analysis-batch-done] batch={current_batch_index} "
            f"expected={current_batch} completed={completed_steps}"
        )
        return {"current_batch_index": current_batch_index + 1}

    workflow.add_node("batch_complete", _batch_complete)

    workflow.add_edge("supervisor", "manager")

    def manager_router(state: AgentState):
        execution_batches = state.get("execution_batches") or _build_execution_batches(state.get("plan", []))
        current_batch_index = int(state.get("current_batch_index", 0) or 0)
        if current_batch_index >= len(execution_batches):
            if _can_bypass_finalizer(state):
                plan = list(state.get("plan", []) or [])
                print(
                    f"[analysis-batch-next] batch={current_batch_index} "
                    f"next=end reason=single_expert_bypass plan={plan} "
                    f"query={_query_preview(state)}"
                )
                return END
            print(
                f"[analysis-batch-next] batch={current_batch_index} "
                f"next=finalizer query={_query_preview(state)}"
            )
            return "finalizer"

        current_batch = execution_batches[current_batch_index]
        print(
            f"[analysis-batch-start] batch={current_batch_index} "
            f"steps={current_batch} query={_query_preview(state)}"
        )
        base_state = dict(state)
        return [
            Send(step, {**base_state, "current_step": step})
            for step in current_batch
        ]

    workflow.add_conditional_edges("manager", manager_router)

    for node_name in ["analyst", "monitor", "strategist", "researcher", "generalist", "screener", "roaster", "macro_analyst", "portfolio_analyst", "chatter"]:
        workflow.add_edge(node_name, "batch_complete")

    workflow.add_edge("batch_complete", "manager")

    workflow.add_edge("finalizer", END)

    return workflow.compile()
