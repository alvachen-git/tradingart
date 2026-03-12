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
from langgraph.graph import StateGraph, END

# --- 寮曞叆浣犵殑宸ュ叿搴?---
# 璇风‘淇濊繖浜涙枃浠跺悕鍜屼綘椤圭洰閲岀殑涓€鑷?
from chart_annotation_tools import draw_pattern_annotation_chart, draw_forecast_chart
from kline_tools import analyze_kline_pattern
from screener_tool import search_top_stocks, get_available_patterns
from news_tools import get_financial_news
from fund_flow_tools import tool_get_retail_money_flow
from polymarket_tool import tool_get_polymarket_sentiment
from plot_tools import draw_chart_tool,draw_macro_compare_chart
from futures_fund_flow_tools import get_futures_fund_flow, get_futures_fund_ranking
from volume_oi_tools import get_volume_oi, get_futures_oi_ranking, get_option_oi_ranking, get_option_volume_abnormal, get_option_oi_abnormal, analyze_etf_option_sentiment, get_etf_option_strikes
from market_tools import get_market_snapshot, get_price_statistics,tool_query_specific_option,get_historical_price,get_recent_price_series,get_trending_hotspots,get_today_hotlist,analyze_keyword_trend,get_finance_related_trends,search_hotlist_history
from data_engine import get_commodity_iv_info, check_option_expiry_status,search_broker_holdings_on_date,tool_analyze_position_change,tool_compare_stocks,get_stock_valuation,get_latest_data_date,tool_analyze_broker_positions
from search_tools import search_web
from market_correlation import tool_stock_hedging_analysis, tool_futures_correlation_check,tool_stock_correlation_check
from beta_tool import calculate_hedging_beta
from knowledge_tools import search_investment_knowledge
from stock_volume_tools import query_stock_volume, search_volume_anomalies
from backtest_tools import run_option_backtest
from portfolio_tools import (
    get_user_portfolio_summary,
    get_user_portfolio_details,
    analyze_user_trading_style,
    check_portfolio_risks
)

# ==========================================
# 1. 瀹氫箟鍏变韩璁板繂 (The State)
# ==========================================
class AgentState(TypedDict):
    # --- 鍩虹淇℃伅 ---
    messages: Annotated[List[BaseMessage], operator.add]
    user_query: str

    # --- 璋冨害鎺у埗 ---
    plan: List[str]  # 浠诲姟闃熷垪锛屽 ["analyst", "monitor"]
    current_step: str  # 褰撳墠姝ｅ湪鎵ц鐨勬楠?

    # --- 涓撳缁撹 (榛戞澘) ---
    symbol: str  # 鏍囩殑浠ｇ爜 (濡?"IM2606")
    symbol_name: str
    trend_signal: str  # "Bullish"(澶?, "Bearish"(绌?, "Neutral"(闇囪崱)
    technical_summary: str  # 渚嬪 "鍑虹幇澶ч槼绾跨獊鐮达紝涓斿潎绾垮澶存帓鍒?
    key_levels: str  # "鏀拺3400, 鍘嬪姏3600"
    fund_data: str  # 璧勯噾娴佸悜鎻忚堪
    news_summary: str  # 鏂伴椈鎽樿
    option_strategy: str  # 鏈熸潈绛栫暐寤鸿
    chart_img: str  # 鍥剧墖璺緞
    risk_preference: str
    knowledge_context: str
    memory_context: str
    is_followup: bool
    recent_context: str
    conversation_id: str

    news_summary: str  # 鎯呮姤鍛樺～鍏ワ細鏂伴椈鎽樿 (CPI/闈炲啘/缇庤仈鍌?
    macro_view: str  # 瀹忚鍒嗘瀽甯堝～鍏ワ細瀹忚瀹氳皟 (瀹芥澗/绱х缉)
    macro_chart: str  # 瀹忚鍒嗘瀽甯堝～鍏ワ細鐢熸垚鐨勫畯瑙傚姣斿浘璺緞

    # --- 鎸佷粨鐩稿叧 (Portfolio Analyst) ---
    user_id: str  # 鐢ㄦ埛ID
    has_portfolio: bool  # 鏄惁鏈夋寔浠撴暟鎹?
    portfolio_summary: str  # 鎸佷粨鎽樿
    portfolio_risks: str  # 椋庨櫓鎻愮ず
    trading_style: str  # 浜ゆ槗椋庢牸
    portfolio_top_corr_index: str  # 鏈€鐩稿叧鎸囨暟鍚嶇О
    portfolio_top_corr_value: str  # 鏈€鐩稿叧鎸囨暟鐨勭浉鍏崇郴鏁?


# 鏈熸潈鍚堢害涔樻暟琛紙姣忓紶鏈熸潈瀵瑰簲鐨勬爣鐨勬暟閲忥級
OPTION_MULTIPLIER = {
    # ETF鏈熸潈 (浠?
    "510050": (10000, "浠?), "510300": (10000, "浠?), "510500": (10000, "浠?),
    "159901": (10000, "浠?), "159915": (10000, "浠?), "159919": (10000, "浠?),
    "159922": (10000, "浠?), "588000": (10000, "浠?), "588080": (10000, "浠?),

    # 鑲℃寚鏈熸潈 (鐐姑?00鍏?
    "IO": (100, "鐐?), "MO": (100, "鐐?), "HO": (100, "鐐?),

    # 璐甸噾灞?
    "AU": (1000, "鍏?), "AG": (15, "鍗冨厠"),

    # 鏈夎壊
    "CU": (5, "鍚?), "AL": (5, "鍚?), "ZN": (5, "鍚?), "PB": (5, "鍚?), "SN": (1, "鍚?), "NI": (1, "鍚?),

    # 榛戣壊
    "I": (100, "鍚?), "RB": (10, "鍚?), "HC": (10, "鍚?), "J": (100, "鍚?), "JM": (60, "鍚?),"SM": (5, "鍚?),"SF": (5, "鍚?),
    "PS": (3, "鍚?),"LC": (1, "鍚?),"SI": (5, "鍚?),"PT": (1000, "鍚?),"PD": (1000, "鍚?),"SH": (30, "鍚?),"AO": (20, "鍚?),

    # 鑳藉寲
    "SC": (1000, "妗?), "FU": (10, "鍚?), "LU": (10, "鍚?), "PG": (20, "鍚?),
    "MA": (10, "鍚?), "TA": (5, "鍚?), "PP": (5, "鍚?), "L": (5, "鍚?),
    "V": (5, "鍚?), "EB": (5, "鍚?), "EG": (10, "鍚?), "RU": (10, "鍚?), "NR": (10, "鍚?),"BR": (5, "鍚?),
    "BU": (10, "鍚?), "SA": (20, "鍚?), "FG": (20, "鍚?), "UR": (20, "鍚?),

    # 鍐滀骇鍝?
    "M": (10, "鍚?), "Y": (10, "鍚?), "P": (10, "鍚?), "OI": (10, "鍚?), "RM": (10, "鍚?),
    "C": (10, "鍚?), "A": (10, "鍚?), "CF": (5, "鍚?), "SR": (10, "鍚?),
    "AP": (10, "鍚?), "PK": (5, "鍚?), "CJ": (5, "鍚?), "LH": (16, "鍚?),
}


def get_option_multiplier(symbol: str) -> str:
    """
    鏍规嵁鏍囩殑浠ｇ爜鑾峰彇鏈熸潈鍚堢害涔樻暟锛岃繑鍥炵簿绠€鐨勬彁绀哄瓧绗︿覆
    鏀寔鍗曞搧绉嶅拰澶氬搧绉嶏紙閫楀彿鍒嗛殧锛?
    """
    import re
    if not symbol:
        return ""

    # 澶勭悊澶氬搧绉嶆儏鍐?(濡?"M,SR" 鎴?"璞嗙矔,鐧界硸")
    symbols = re.split(r'[,锛屻€?\s]+', symbol.strip())
    results = []

    for sym in symbols:
        sym = sym.strip().upper()
        if not sym:
            continue

        # 1. 鐩存帴鍖归厤 ETF (6浣嶆暟瀛?
        if sym.isdigit() and len(sym) == 6:
            if sym in OPTION_MULTIPLIER:
                m, u = OPTION_MULTIPLIER[sym]
                results.append(f"{sym}={m}{u}")
            else:
                results.append(f"{sym}=10000浠?)  # ETF榛樿
            continue

        # 2. 鑲℃寚鏈熸潈 (IO/MO/HO寮€澶?
        for prefix in ["IO", "MO", "HO"]:
            if sym.startswith(prefix):
                results.append(f"{prefix}=100鐐?姣忕偣100鍏?")
                break
        else:
            # 3. 鍟嗗搧鏈熸潈 - 鎻愬彇瀛楁瘝閮ㄥ垎
            match = re.match(r'^([A-Za-z]+)', sym)
            if match:
                code = match.group(1).upper()
                if code in OPTION_MULTIPLIER:
                    m, u = OPTION_MULTIPLIER[code]
                    results.append(f"{code}={m}{u}")

    if not results:
        return ""

    # 杩斿洖绮剧畝鏍煎紡
    return "銆?.join(results)


# ==========================================
# 2. 瀹氫箟 Supervisor (澶х瀹?
# ==========================================
# 瀹氫箟杈撳嚭缁撴瀯锛屽己鍒?LLM 杩斿洖 JSON 鏍煎紡鐨勪换鍔″垪琛?
class PlanningOutput(BaseModel):
    plan: List[Literal["analyst", "researcher", "monitor", "strategist", "chatter", "generalist", "screener", "macro_analyst","roaster", "portfolio_analyst"]] = Field(
        description="鎵ц姝ラ鍒楄〃銆傛敞鎰忎緷璧栧叧绯伙細鏈熸潈(strategist)蹇呴』鎺掑湪鍒嗘瀽(analyst)涔嬪悗銆?
    )
    symbol: str = Field(description="鏍稿績鏍囩殑浠ｇ爜銆傚鏋滄槸瀵规瘮闂鎴栨棤娉曟彁鍙栧崟涓€鏍囩殑锛岃鐣欑┖鎴栧～'榛勯噾'", default="")


def supervisor_node(state: AgentState, llm):
    """
    澶х瀹惰妭鐐癸細鍒嗘瀽鐢ㄦ埛鎰忓浘锛岀敓鎴愪换鍔℃竻鍗?
    """
    query = state["user_query"]
    messages = state.get("messages", [])
    is_followup = bool(state.get("is_followup", False))
    recent_context = str(state.get("recent_context", "") or "").strip()
    memory_context = str(state.get("memory_context", "") or "").strip()
    has_portfolio = bool(state.get("has_portfolio", False))  # 馃敟 鏂板锛氳幏鍙栨寔浠撶姸鎬?

    history_text = recent_context
    if not history_text and len(messages) > 1:
        # 鍏滃簳锛氳嫢鍓嶇鏈紶 recent_context锛岄€€鍥炲埌娑堟伅鍒楄〃鎶藉彇鏈€杩戜袱鏉?
        history_lines = []
        for msg in messages[:-1]:
            if isinstance(msg, HumanMessage):
                history_lines.append(f"鐢ㄦ埛: {msg.content[:220]}")
            elif isinstance(msg, AIMessage):
                history_lines.append(f"AI: {msg.content[:220]}")
        if history_lines:
            history_text = "\n".join(history_lines[-2:])

    # 馃敟 鏂板锛氭寔浠撶姸鎬佹彁绀?
    portfolio_status = f"\n銆愰噸瑕併€戠敤鎴穥'宸蹭笂浼? if has_portfolio else '鏈笂浼?}鎸佷粨鏁版嵁銆? if has_portfolio else ""

    system_prompt = f"""
    浣犳槸浜ゆ槗鍥㈤槦鐨勪富绠★紝鏍规嵁闂鍒跺畾璁″垝銆?
    {portfolio_status}

    銆愬彲鐢ㄥ憳宸ャ€?
    - analyst: 鎶€鏈垎鏋愬笀 (鐪婯绾裤€佸畾瓒嬪娍),鍒嗘瀽濡備綍鎿嶄綔
    - monitor: 鏁版嵁鐩戞帶鍛?(鐪嬫湡璐ц祫閲戞祦銆佹湡璐у晢鎸佷粨銆佹煡鏈熻揣鎸佷粨閲忋€佹煡浠锋牸)
    - researcher: 鎯呮姤鐮旂┒鍛?(鐪嬫柊闂汇€佸畯瑙傘€佺儹鐐广€佸湴缂樻斂娌汇€佽揣甯佹斂绛栥€丳olymarket涓婄殑姒傜巼鍒嗘瀽)
    - strategist: 鏈熸潈绛栫暐鍛?(缁欑瓥鐣ワ紝**蹇呴』渚濊禆 analyst**)
    - screener: 鑲＄エ澶у笀 (鍗忓姪"鎺ㄨ崘鑲＄エ"銆?閫夎偂"銆佹煡鑲＄エ鎴愪氦閲忋€佽祫閲戞祦)
    - portfolio_analyst: 鎸佷粨鍒嗘瀽甯?(鍒嗘瀽鐢ㄦ埛鎸佷粨缁撴瀯銆侀闄┿€佷氦鏄撻鏍硷紝缁欎釜鎬у寲寤鸿) {'鉁?鐢ㄦ埛宸蹭笂浼犳寔浠擄紝鍙敤' if has_portfolio else '鉂?鐢ㄦ埛鏈笂浼犳寔浠擄紝涓嶅彲鐢?}
    - chatter: 鐭ヨ瘑闂瓟鍜岄棽鑱?(渚嬪瑙ｉ噴涓€涓婭V锛屼粈涔堟槸鐗涘競浠峰樊锛?鏈€杩戠編鑱斿偍浠€涔堟椂鍊欏紑浼?)
    - generalist: 銆愮帇鐗屽垎鏋愬笀銆戝鐞嗗姣?A鍜孊璋佸己)銆佸鍝佺鍒嗘瀽銆佺敾浠峰樊鍥炬垨娣卞害澶嶆潅闂銆?
    - macro_analyst: 瀹忚绛栫暐甯?(鍒嗘瀽缇庤仈鍌ㄣ€佺編鍊恒€佺編鍏冦€侀€氳儉銆丆PI銆侀潪鍐溿€佺敾鍒╃巼鍥?
    - roaster: *姣掕垖鍒嗘瀽甯? (褰撶敤鎴疯姹?鍚愭Ы"銆?鎸戞垬鎴?銆?姣掕垖妯″紡"鏃朵娇鐢?銆?

    銆愯皟搴﹁鍒?(涓ユ牸閬靛畧)銆?
    1. **杩芥眰鏁堢巼**: 闂偂绁ㄦ垚浜ら噺灏卞彧娲?`screener`锛涘彧闂湡璐ф寔浠撻噺鎴栦环鏍煎氨鍙淳 `monitor`锛涘彧闂柊闂绘垨鐑偣灏卞彧娲?`researcher`锛涘彧闂妧鏈垎鏋愬氨鍙淳`analyst`锛涘彧闂鎯呭垎鏋愬氨鍙淳`analyst`銆?
    2. **鍏ㄥ鏈嶅姟**: 濡傛灉鐢ㄦ埛闂?鍏ㄩ潰鍒嗘瀽"鎴?璇︾粏鍒嗘瀽"锛岄粯璁よ矾寰? ["analyst", "monitor", "researcher","strategist"]銆?
    3. **鎸佷粨鐩稿叧** (浠呭綋鐢ㄦ埛宸蹭笂浼犳寔浠撴椂): 濡傛灉鐢ㄦ埛鎻愬埌"鎴戠殑鎸佷粨"銆?鎴戠殑鑲＄エ"銆?浠撲綅"銆?鎸佷粨椋庨櫓"銆?鎸佷粨鍒嗘瀽"銆?閫傚悎鎴?銆?涓€у寲寤鸿"銆?鎴戠殑椋庢牸"銆?鎸佷粨寤鸿"銆?璋冧粨"銆?鍔犱粨"銆?鍑忎粨"绛夊叧閿瘝锛?*蹇呴』**娲?`portfolio_analyst`銆?
    3. **鍗曞搧绉嶆湡鏉冮棶棰?*: "500ETF閫傚悎浠峰樊杩樻槸瑁镐拱"銆?鎺ㄨ崘鐧介摱鏈熸潈绛栫暐" -> 
       - 鍙鏍囩殑鏄庣‘(500ETF)锛屼笖娑夊強鏈熸潈浜ゆ槗锛屼竴寰嬭蛋娴佹按绾裤€?
       - Plan: `['analyst', 'strategist']` (蹇呴』鍏堝垎鏋愬啀鍑虹瓥鐣?銆?
    4. **澶氬搧绉?瀵规瘮**: 闂?鐧介摱鍜岄粍閲戣皝寮?銆?鍒嗘瀽涓€涓嬭灪绾瑰拰鐑嵎" -> 
       - symbol 濉?"鐧介摱,榛勯噾" (鐢ㄩ€楀彿鍒嗛殧)
       - plan 娲?`['generalist']` (璁╃帇鐗屽幓澶勭悊澶氬搧绉?銆?
    5. **瀹忚/澶у畻/璐甸噾灞?*: 
       - 闂?"鐜板湪瀹忚鐜鎬庝箞鏍?銆?缇庤仈鍌ㄩ檷鎭簡鍚? -> Plan: `['researcher', 'macro_analyst']` (鍏堟壘鏂伴椈锛屽啀鍒嗘瀽鏁版嵁)銆?
       - 闂?"榛勯噾/鐧介摱/鑳戒拱鍚? -> Plan: `['analyst', 'researcher', 'macro_analyst', 'strategist']` (榛勯噾瀵瑰畯瑙傛瀬搴︽晱鎰燂紝蹇呴』鍔犲畯瑙傚垎鏋?銆?
       - 闂?"鍒╃巼/缇庡厓璧板娍" -> Plan: `['researcher', 'macro_analyst']`銆?
    6. **瀹㈡埛鎻愬埌鑲＄エ**锛屼絾娌℃湁鏄庣‘璇存槑鏍囩殑鍚嶅瓧锛岄渶瑕侀€夎偂鏃讹紝鍙?Plan:['screener']
    7. **鐭ヨ瘑/鐧剧/闂茶亰**: 闂蹇点€侀棶浜哄悕銆侀棶鍚嶈瘝 -> 娲?['chatter']銆?
    8. 濡傛灉鐢ㄦ埛鐨勯棶棰樺緢妯＄硦 (渚嬪"甯垜鍒嗘瀽涓€涓?锛?榛勯噾鎬庝箞鐪?)锛岃鍏堟淳chatter鍘婚棶娓呮闂 -> plan=['chatter']銆?
    9. 鍙棶K绾挎垨鎶€鏈潰鍒嗘瀽鏃讹紝鍙娲綼nalyst锛屼笉瑕佸啀娲惧叾浠栦汉
    10.濡傛灉瀹㈡埛瑕佺敾鍥撅紝娲?`['generalist']` 銆?
    """

    if is_followup:
        system_prompt += """

    銆愯繛缁拷闂ā寮忥紙寮虹害鏉燂級銆?
    1. 褰撳墠闂鏄涓婁竴杞殑杩介棶锛屽繀椤诲厛鎵挎帴涓婁竴杞叧閿粨璁猴紝鍐嶅洖绛斿綋鍓嶉棶棰樸€?
    2. 鎵挎帴闂浼樺厛娲?`generalist`锛涜嫢涓婁笅鏂囦笉瓒冲垯娲?`chatter` 鍏堟緞娓咃紝涓嶅緱缂栭€犮€?
    3. 绂佹鎶娾€滅煡璇嗗簱鍛戒腑涓虹┖鈥濆綋浣滈粯璁ゅ洖绛旀ā鏉裤€?
        """

    full_query = query
    if is_followup:
        full_query = (
            f"銆愯繛缁拷闂ā寮忋€戞槸\n"
            f"銆愯繎鏈熷璇濆巻鍙层€慭n{history_text if history_text else '鏃?}\n\n"
            f"銆愮浉鍏抽暱鏈熻蹇嗐€慭n{memory_context if memory_context else '鏃?}\n\n"
            f"銆愬綋鍓嶉棶棰樸€慭n{query}"
        )
    elif history_text:
        full_query = f"銆愯繎鏈熷璇濆巻鍙层€慭n{history_text}\n\n銆愬綋鍓嶉棶棰樸€慭n{query}"

    # 浣跨敤 structured_output 寮哄埗杈撳嚭 JSON
    planner = llm.with_structured_output(PlanningOutput)
    prompt = ChatPromptTemplate.from_messages([
        ("system", system_prompt),
        ("human", "{query}")
    ])

    chain = prompt | planner
    result = chain.invoke({"query": full_query})

    # 馃敟馃敟馃敟 [鏂板闃插穿婧冩鏌
    # 濡傛灉 LLM 娌℃湁杩斿洖鏈夋晥鐨勭粨鏋勫寲鏁版嵁 (None)锛岄粯璁よ浆缁?Chatter
    if not result:
        return {
            "plan": ["chatter"],
            "symbol": "",
            "messages": [SystemMessage(content="涓荤鏈敓鎴愯鍒掞紝榛樿杞叆闂茶亰")]
        }

    final_plan = result.plan

    # 鍥炴祴闂 -> 鐩存帴浜ょ粰閫氭墠澶勭悊锛岄伩鍏嶆棤鍏宠妭鐐?
    if "鍥炴祴" in query or "绛栫暐鍥炴祴" in query:
        final_plan = ["generalist"]

    if is_followup:
        if not history_text and not memory_context:
            final_plan = ["chatter"]
        elif not final_plan:
            final_plan = ["generalist"]
        elif final_plan[0] not in ["generalist", "chatter"]:
            final_plan = ["generalist"] + [p for p in final_plan if p != "generalist"]

    # 鍘婚噸骞朵繚鎸侀『搴忥紝閬垮厤璺敱閲嶅
    deduped_plan = []
    for p in final_plan:
        if p not in deduped_plan:
            deduped_plan.append(p)
    final_plan = deduped_plan
    final_symbol = str(result.symbol).strip()

    # 绠€鍗曠殑姝ｅ垯鍒ゆ柇鏄惁涓?A 鑲′釜鑲′唬鐮?(6寮€澶存勃甯備富鏉?绉戝垱, 0寮€澶存繁甯? 3寮€澶村垱涓氭澘, 8/4寮€澶村寳浜ゆ墍)
    # ETF 閫氬父鏄?51, 56, 58, 159 寮€澶?
    # 鏈熻揣 閫氬父鏄?瀛楁瘝寮€澶?

    # 鎻愬彇绾暟瀛椾唬鐮?
    match_code = re.search(r'\d{6}', final_symbol)
    if match_code:
        code_num = match_code.group(0)

        # 鍒ゆ柇閫昏緫锛氬鏋滄槸 6/0/3/8/4 寮€澶寸殑6浣嶆暟瀛楋紝瑙嗕负涓偂 -> 鍓旈櫎 strategist
        # (ETF 涓€鑸槸 5 鎴?1 寮€澶达紝淇濈暀)
        if code_num.startswith(('6', '0', '3', '8', '4')):
            if "strategist" in final_plan:
                final_plan.remove("strategist")

    if 'screener' in final_plan:
        final_symbol = ""

    return {
        "plan": final_plan,
        "symbol": final_symbol,
        "messages": [SystemMessage(content=f"宸插埗瀹氳鍒? {final_plan}")]
    }


# ==========================================
# 馃敟 [鏂板] 鐜嬬墝鍒嗘瀽甯?(Generalist / Fallback)
# 鑱岃矗锛氬鐞嗗姣斻€佺患鍚堛€佹ā绯婃垨 Supervisor 鎼炰笉瀹氱殑澶嶆潅闂
# ==========================================
def generalist_node(state: AgentState, llm):
    query = state["user_query"]
    symbol_input = state.get("symbol", "")
    is_followup = bool(state.get("is_followup", False))
    recent_context = str(state.get("recent_context", "") or "").strip()
    mem_context = str(state.get("memory_context", "") or "").strip()
    current_date = datetime.now().strftime("%Y骞?m鏈?d鏃?%A")
    context_parts = []
    if recent_context:
        context_parts.append(f"銆愭渶杩戜袱杞細璇濄€慭n{recent_context}")
    if mem_context:
        context_parts.append(f"銆愮浉鍏抽暱鏈熻蹇嗐€慭n{mem_context}")
    followup_context_block = "\n\n".join(context_parts) if context_parts else "鏃?

    if is_followup and not context_parts:
        return {
            "messages": [HumanMessage(content="銆愮帇鐗屽垎鏋愩€戞湭妫€绱㈠埌涓婁竴杞叧閿粨璁恒€傝璐村嚭涓婁竴杞粨璁猴紙鏂瑰悜銆佸叧閿綅鎴栫瓥鐣ワ級锛屾垜鍐嶆壙鎺ュ睍寮€銆?)],
            "chart_img": ""
        }

    tools = [
        analyze_kline_pattern, search_investment_knowledge, get_market_snapshot, get_commodity_iv_info,
        search_broker_holdings_on_date, tool_analyze_position_change,
        tool_query_specific_option, get_historical_price, get_volume_oi, get_futures_oi_ranking,
        get_option_oi_ranking, get_option_volume_abnormal, get_option_oi_abnormal,
        get_price_statistics, check_option_expiry_status, tool_stock_hedging_analysis,
        tool_futures_correlation_check, tool_stock_correlation_check, calculate_hedging_beta,
        tool_get_retail_money_flow, draw_chart_tool, get_stock_valuation, tool_compare_stocks,
        get_futures_fund_flow, get_futures_fund_ranking, get_available_patterns, analyze_etf_option_sentiment,
        get_etf_option_strikes,tool_analyze_broker_positions, run_option_backtest,
        get_macro_indicator,get_macro_overview,analyze_yield_curve
    ]


    prompt = f"""
        浣犳槸涓€浣嶇帇鐗岄噺鍖栧垎鏋愬笀銆備氦鏄撶悊蹇垫槸椤哄娍鑰屼负銆?
        銆愬綋鍓嶆棩鏈熴€戯細{current_date}銆?
        瀹㈡埛闇€姹傦細{query}銆?
        鍒嗘瀽鍝佺锛歿symbol_input}銆?
        銆愯繛缁拷闂ā寮忋€戯細{"鏄? if is_followup else "鍚?}銆?
        銆愬巻鍙叉壙鎺ヤ笂涓嬫枃銆戯細
        {followup_context_block}
   
        
        銆愬伐鍏蜂娇鐢ㄨ〃銆?
        1. **浼板€?渚垮疁/璐靛悧/鎶勫簳** -> get_stock_valuation 
        2. **瀵规瘮/PK/璋佸己/閫夊摢涓?* -> tool_compare_stocks (澶氳偂妯瘎)
        3. **瀵瑰啿/鐩稿叧鎬?鑱斿姩** -> tool_stock_correlation_check
        4. **鍘嗗彶缁熻浠锋牸** -> get_price_statistics
        5. **鐢诲浘/璧板娍鍥?* -> draw_chart_tool
        6. **姒傚康/绛栫暐瑙ｉ噴** -> search_investment_knowledge
        7. 鐩稿叧鎬у垎鏋?-> tool_futures_correlation_check鎴杢ool_stock_correlation_check
        8. 瀵瑰啿鍒嗘瀽 -> calculate_hedging_beta
        9. 鏌ユ煇鏈熻揣璧勯噾娴佸姩 -> get_futures_fund_flow
        10.鏌ュ叏閮ㄦ湡璐ц祫閲戞矇娣€鎺掑悕 -> get_futures_fund_ranking
        11.鏌ュ晢鍝侀緳铏庢/鏈熻揣鍟嗘寔浠?-> search_broker_holdings_on_date  
        12.鏌ユ煇鏈熻揣鍟嗘渶杩戞寔浠撳彉鍖栨儏鍐?-> tool_analyze_position_change
        13.鏌ユ垚浜ら噺鍜屾寔浠撻噺 -> get_volume_oi
        14.鏌ユ湡璐ф寔浠撻噺鎺掑悕 -> get_futures_oi_ranking
        15.鏌ユ湡鏉冩尝鍔ㄧ巼-> get_commodity_iv_info
        16.鏌ユ湡鏉冨悎绾︿环鏍?> tool_query_specific_option
        17.鏌TF鏈熸潈鏈夊摢浜涘悎绾?> get_etf_option_strikes
        18.鏌ュ畯瑙傛寚鏍?-> get_macro_indicator
        19.鏌ュ畯瑙傜幆澧冩€昏 -> get_macro_overview 
        20.鍒嗘瀽鏀剁泭鐜囨洸绾?-> analyze_yield_curve 
        21.鏌ュ崟鍙偂绁ㄧ殑鎴愪氦閲忚鎯?-> query_stock_volume
        22.鏈熸潈绛栫暐鍥炴祴 -> run_option_backtest

        銆愯涓哄噯鍒欍€?
        1. 鍏堢粰缁撹锛岀劧鍚庤В閲婄悊鐢便€?
        2. 涓嶈绠€鍗曞杩帮紝瑕佹湁娣卞害娲炲療銆?
        3. 绂佹绌鸿皥锛屽繀椤荤敤宸ュ叿鑾峰彇鐨勬暟鎹璇濄€?
        4. 涓嶈缂栭€犳暟鎹紝濡傛灉娌℃煡鍒版暟鎹氨璇翠笉鐭ラ亾銆?
        5. 鑻ュ浜庤繛缁拷闂ā寮忥紝绗竴娈靛繀椤诲厛鎵挎帴涓婁竴杞叧閿粨璁猴紝鍐嶅洖绛斿綋鍓嶉棶棰樸€?
        """

    general_agent = create_react_agent(llm, tools, prompt=prompt)

    # 馃敟 鐢ㄤ簬鍦ㄥ紓甯告椂鎭㈠閮ㄥ垎缁撴灉
    partial_response = ""
    chart_img = ""

    try:
        # 缁欎簣瓒冲鐨勯€掑綊姝ユ暟锛屼絾涓嶈澶珮閬垮厤 GeneratorExit
        result = general_agent.invoke(
            {"messages": state["messages"]},
            {"recursion_limit": 100}
            # 闄嶄綆鍒?15锛岃冻澶熷畬鎴愬ぇ閮ㄥ垎浠诲姟
        )

        last_response = result["messages"][-1].content
        partial_response = last_response

        # 馃敟 浠庡搷搴斾腑鎻愬彇鍥捐〃璺緞
        chart_match = re.search(r'!\[.*?\]\((chart_[a-zA-Z0-9_]+\.json)\)', last_response)
        if chart_match:
            chart_img = chart_match.group(1)

        # 馃敟 濡傛灉鍝嶅簲涓病鎵惧埌锛屽皾璇曚粠鎵€鏈夋秷鎭腑鏌ユ壘
        if not chart_img:
            for msg in result.get("messages", []):
                content = getattr(msg, 'content', str(msg))
                chart_match = re.search(r'(chart_[a-zA-Z0-9_]+\.json)', content)
                if chart_match:
                    chart_img = chart_match.group(1)
                    break

        return {
            "messages": [HumanMessage(content=f"銆愮帇鐗屽垎鏋愩€慭n{last_response}")],
            "chart_img": chart_img
        }
    except GeneratorExit:
        # 馃敟 GeneratorExit 閫氬父鍙戠敓鍦ㄥ浘琛ㄥ凡鐢熸垚涔嬪悗锛屽皾璇曟煡鎵炬渶杩戠敓鎴愮殑鍥捐〃
        charts_dir = os.path.join(os.path.dirname(__file__), "static", "charts")
        if os.path.exists(charts_dir):
            chart_files = glob.glob(os.path.join(charts_dir, "chart_*.json"))
            if chart_files:
                # 鑾峰彇鏈€鏂扮殑鍥捐〃鏂囦欢
                latest_chart = max(chart_files, key=os.path.getmtime)
                chart_img = os.path.basename(latest_chart)

        return {
            "messages": [HumanMessage(
                content=f"銆愮帇鐗屽垎鏋愩€戝垎鏋愬畬鎴怽n{partial_response}" if partial_response else "銆愮帇鐗屽垎鏋愩€戝浘琛ㄥ凡鐢熸垚锛岃鏌ョ湅涓嬫柟")],
            "chart_img": chart_img
        }
    except Exception as e:
        # 浼橀泤闄嶇骇
        return {
            "messages": [HumanMessage(content=f"銆愮帇鐗屽垎鏋愩€戞€濊€冭繃绋嬩腑鏂? {e}")],
            "chart_img": chart_img  # 浠嶇劧灏濊瘯杩斿洖鍙兘鐨勫浘琛?
        }



# ==========================================
# 3. 瀹氫箟鍚勪釜涓撳鑺傜偣 (Workers)
# ==========================================

# 馃煝 1. 鎶€鏈垎鏋愬笀
def analyst_node(state: AgentState, llm):
    symbol = state["symbol"]
    query = state["user_query"]
    mem_context = state.get("memory_context", "")
    current_date = datetime.now().strftime("%Y骞?m鏈?d鏃?%A")
    tools = [
        analyze_kline_pattern,  # 鏍稿績锛氬舰鎬佷笌瓒嬪娍
        get_market_snapshot,
        get_price_statistics,  # 杈呭姪锛氬巻鍙叉尝鍔ㄦ暟鎹?
        draw_chart_tool,
        draw_pattern_annotation_chart,  # 褰㈡€佹爣娉ㄥ浘锛堢牬搴曠炕/鍚炲櫖/鏅ㄦ槦绛夛級
        draw_forecast_chart,            # 鍏抽敭浠蜂綅鍥撅紙鏀拺/鍘嬪姏/鐩爣浠凤級
    ]

    target_option_etfs = [
        "510050",  # 50ETF
        "510300",  # 300ETF
        "510500",  # 500ETF
        "159915",  # 鍒涗笟鏉縀TF
        "588000"  # 绉戝垱50ETF
    ]
    # 妫€鏌ュ綋鍓?symbol 鏄惁鍛戒腑涓婅堪鍒楄〃
    # 鍔ㄦ€佸垽鏂槸鍚﹀姞鍏TF鏈熸潈宸ュ叿 (淇濇寔涔嬪墠鐨勯€昏緫)
    target_option_etfs = ["510050", "510300", "510500", "159915", "588000"]
    is_target_etf = any(code in symbol for code in target_option_etfs)

    extra_instruction = ""
    if is_target_etf:
        from volume_oi_tools import analyze_etf_option_sentiment
        tools.append(analyze_etf_option_sentiment)
        extra_instruction = """
            馃幆 **鏈熸潈涓诲姏鎸佷粨楠岃瘉**锛?
            - 璋冪敤 `analyze_etf_option_sentiment` 鏌ョ湅鏈熸潈鏈€澶ф寔浠撲綅浣滀负鏀拺鍘嬪姏鍙傝€冦€?
            """

    # 2. 娉ㄥ叆鈥滀弗璋ㄢ€濅汉璁捐繘琛屾鼎鑹?
    is_chart_only = any(kw in query for kw in ["K绾垮浘", "k绾垮浘"])

    if is_chart_only:
        # 馃敟 鐢诲浘蹇€熸ā寮?- 绠€鍖?prompt
        persona_prompt = f"""
            浣犳槸涓€浣嶆妧鏈垎鏋愮敾鍥惧笀銆傘€愬綋鍓嶆棩鏈熴€戯細{current_date}銆?
            銆愬綋鍓嶆爣鐨勩€戯細{symbol}
            銆愬鎴烽渶姹傘€戯細{query}

            銆愪换鍔°€戯細
            鐢ㄦ埛鎯宠鐪嬪浘琛紝璇风洿鎺ヨ皟鐢?`draw_chart_tool` 鐢诲浘銆?

            銆愬洖澶嶈姹傘€戯細
            1. 鐢诲畬鍥惧悗锛屽彧瑕佺畝鐭鏄庡浘琛ㄥ叧閿俊鎭紙濡傚綋鍓嶄环鏍笺€佹定璺屽箙锛夈€?
            2. 缁濆涓嶈鍋氬啑闀跨殑鍒嗘瀽锛?
            """
    else:
        # 姝ｅ父鍒嗘瀽妯″紡
        persona_prompt = f"""
            浣犳槸涓€浣嶄弗璋ㄧ殑鎶€鏈垎鏋愬笀銆傞伒寰秼鍔夸氦鏄撱€?
            銆愬綋鍓嶆棩鏈熴€戯細{current_date}銆?
            銆愬綋鍓嶆爣鐨勩€戯細{symbol}
            銆愬鎴峰巻鍙茶蹇嗐€戯細{mem_context}

            銆怑TF鏈熸潈鎸佷粨鏁版嵁銆戯細{extra_instruction}
            銆愬鎴烽渶姹傘€戯細{query}

            銆愬彲璋冪敤宸ュ叿銆?
            1. 鎶€鏈潰鍒嗘瀽-> `analyze_kline_pattern` 锛岃幏鍙朘绾垮舰鎬佸拰瓒嬪娍銆?
            2. 鑾峰彇鏍囩殑涓€娈垫椂闂翠环鏍?> `get_price_statistics` 銆?
            3. 鍒嗘瀽鐨勫搧绉嶅鏋滃彧鏈?涓紝鍙兘璋冪敤1娆analyze_kline_pattern`
            4. 鑾峰彇鑲＄エ鍚嶅瓧鍜屼环鏍肩敤 `get_market_snapshot`

            銆愮敾鍥捐鍒欙紙閲嶈锛屽繀椤婚伒瀹堜紭鍏堢骇锛夈€?
            - 浼樺厛绾?锛氱敤鎴疯姹傜敾鍥撅紝涓?`analyze_kline_pattern` 杩斿洖浜嗗舰鎬佷俊鍙?鈫?蹇呴』璋冪敤 `draw_pattern_annotation_chart`锛岀姝㈢敤 `draw_chart_tool`
              鈿狅笍 璋冪敤鏃跺繀椤绘妸 `analyze_kline_pattern` 璇嗗埆鍒扮殑褰㈡€佸悕绉板～鍏?`pattern_name` 鍙傛暟锛堝 pattern_name="5鏃ュ钩鍙扮獊鐮?锛夛紝涓嶈鐣欑┖
            - 浼樺厛绾?锛氱敤鎴锋槑纭粰鍑烘敮鎾?鍘嬪姏/鐩爣浠凤紝鎴栦綘鍋氬畬鍒嗘瀽鍚庣畻鍑轰簡鍏抽敭浠蜂綅 鈫?璋冪敤 `draw_forecast_chart`
            - 浼樺厛绾?锛氱敤鎴峰彧瑕佹眰鐪婯绾胯蛋鍔垮浘銆佹病鏈夊舰鎬佷俊鍙枫€佷篃娌℃湁浠蜂綅鏁版嵁 鈫?鎵嶇敤 `draw_chart_tool`
            - 鐢ㄦ埛娌℃湁瑕佹眰鐢诲浘鏃讹紝涓変釜鐢诲浘宸ュ叿閮戒笉璋冪敤

            銆愪换鍔°€戯細
            1. 鎻忚堪K绾垮拰鎶€鏈潰鎯呭喌
            2. 鍙戞帢绐佺牬杩涘満鏈轰細銆?
            3. 濡傛灉杩炵画鍑犲ぉ绱Н娑ㄥ箙杩囧ぇ锛岃鎻愰啋闃茶寖绐佺劧涓嬭穼锛屽鏋滆繛缁嚑澶╃疮绉穼骞呰繃澶э紝瑕佹彁閱掑彲鑳界獊鐒舵姤澶嶅弽寮?
            4. K绾跨殑閲嶈鎬уぇ浜庡潎绾匡紝鍙嶈浆鎴栫獊鐮磋鐪婯绾匡紝鍧囩嚎鍙嶅簲浼氭瘮杈冩參銆?
            5. 濡傛灉娌℃湁鏄庢樉鏈轰細锛岀洿璇?寤鸿瑙傛湜"銆?
            6. 濡傛灉鐢ㄦ埛鐨勬寚浠ゆā绯婏紝鍙弬鑰冧笂鏂囧巻鍙茬‘璁ゅ垎鏋愬璞°€?
            """


    # 绠€鍗曠殑鏂瑰悜鎻愬彇 (缁欑瓥鐣ュ憳鐢?
    analyst_agent = create_react_agent(llm, tools, prompt=persona_prompt)

    partial_response = ""
    chart_img = ""
    symbol_name = ""

    try:
        # 鎵ц鎺ㄧ悊 (缁欎簣瓒冲鐨勯€掑綊娆℃暟锛屽洜涓哄鐞嗕环宸彲鑳介渶瑕佽皟2娆″伐鍏?
        result = analyst_agent.invoke(
            {"messages": state["messages"]},
            {"recursion_limit": 30}
        )

        last_response = result["messages"][-1].content
        partial_response = last_response
        # 馃敟 鎻愬彇鍏徃鍚嶇О锛堜粠get_market_snapshot鐨勮繑鍥炰腑锛?
        symbol_name = ""
        # 鏍煎紡锛?馃搷 **涔剧収鍏夌數(300102.SZ) 琛屾儏**"
        name_match = re.search(r'馃搷 \*\*(.+?)\((.+?)\) 琛屾儏\*\*', last_response)
        if name_match:
            symbol_name = name_match.group(1)
        else:
            # 鍏滃簳锛氬鏋滄病鎻愬彇鍒帮紝鐢╯ymbol
            symbol_name = state.get('symbol', '')

        chart_match = re.search(r'IMAGE_CREATED:(chart_[a-zA-Z0-9_]+\.json)', last_response)
        if chart_match:
            chart_img = chart_match.group(1)
            print(f"馃搳 analyst_node 鎻愬彇鍒板浘琛? {chart_img}")
        if not chart_img:
            for msg in result.get("messages", []):
                content = getattr(msg, 'content', str(msg))
                chart_match = re.search(r'IMAGE_CREATED:(chart_[a-zA-Z0-9_]+\.json)', content)
                if chart_match:
                    chart_img = chart_match.group(1)
                    break

        # 鎻愬彇淇″彿
        trend_signal = "闇囪崱"
        if any(x in last_response for x in ["鐪嬫定", "澶氬ご", "涓婅", "绐佺牬"]):
            trend_signal = "鐪嬫定"
        elif any(x in last_response for x in ["鐪嬭穼", "绌哄ご", "涓嬭", "鐮翠綅"]):
            trend_signal = "鐪嬭穼"

        levels = re.findall(r'(鏀拺|鍘嬪姏|闃诲姏).*?(\d+[\.\d]*)', last_response)
        key_levels_str = " ".join([f"{k}{v}" for k, v in levels]) if levels else ""

        # 鎻愬彇鍓?100 涓瓧浣滀负鎶€鏈憳瑕?
        tech_summary_str = last_response.replace("\n", " ")[:100]

        return {
            "messages": [HumanMessage(content=f"銆愭妧鏈垎鏋愩€慭n{last_response}")],
            "symbol_name": symbol_name,
            "trend_signal": trend_signal,  # 瀛樺叆瓒嬪娍
            "key_levels": key_levels_str,  # 瀛樺叆鐐逛綅 (鏂板)
            "technical_summary": tech_summary_str,  # 瀛樺叆鎽樿 (鏂板)
            "chart_img": chart_img
        }

    except GeneratorExit:
        # 馃敟 GeneratorExit 涓嶆槸 Exception 瀛愮被锛岄渶瑕佸崟鐙崟鑾?
        # 灏濊瘯浠庡凡璋冪敤鐨勫伐鍏蜂腑鎻愬彇缁撴灉
        fallback_msg = partial_response if partial_response else f"鎶€鏈垎鏋愬凡瀹屾垚瀵?{symbol} 鐨勫垵姝ュ垎鏋?
        return {
            "messages": [HumanMessage(content=f"銆愭妧鏈垎鏋愩€慭n{fallback_msg}")],
            "symbol_name": symbol_name,
            "trend_signal": "闇囪崱"
        }
    except Exception as e:
        print(f"Analyst Node Error: {e}")
        return {
            "messages": [HumanMessage(content=f"銆愭妧鏈垎鏋愩€戝垎鏋愬彈闃? {e}")],
            "symbol_name": state.get('symbol', ''),
            "trend_signal": "鏈煡"
        }


# 馃煛 2. 璧勯噾鐩戞帶鍛?(瀹归敊璺宠繃)
def monitor_node(state: AgentState, llm):
    user_q = state["user_query"]
    symbol = state.get("symbol", "")
    symbol_name = state.get("symbol_name", "")
    current_date = datetime.now().strftime("%Y骞?m鏈?d鏃?%A")
    latest_trade_date = get_latest_data_date()

    # 1. 瑁呭鎵€鏈夋暟鎹被宸ュ叿
    tools = [
        tool_get_retail_money_flow,  # 鑲＄エ琛屼笟璧勯噾
        get_futures_fund_flow,  # 鏈熻揣璧勯噾娴?
        get_futures_fund_ranking, # 鏈熻揣娌夋穩璧勯噾鎺掑悕
        get_commodity_iv_info,  # IV/娉㈠姩鐜?Rank
        search_broker_holdings_on_date,  # 鏈熻揣鍟嗘寔浠撴帓鍚?
        tool_analyze_position_change,  # 鎸佷粨鍙樺姩鍒嗘瀽
        get_option_volume_abnormal,
        get_option_oi_abnormal,
        get_option_oi_ranking,
        get_volume_oi,
        get_market_snapshot,
        get_historical_price,
        get_price_statistics,
        get_recent_price_series,
        tool_analyze_broker_positions,
        get_futures_oi_ranking,
        query_stock_volume,
        get_macro_indicator
    ]

    # 鍒ゆ柇鏄惁涓?ETF (51/159寮€澶? 鎴?鑲＄エ
    is_etf_or_stock = False
    import re
    if re.search(r'\d{6}', symbol):  # 鍙鍖呭惈6浣嶆暟瀛楋紝澶ф鐜囨槸璇佸埜
        is_etf_or_stock = True

    tool_instruction = ""
    if is_etf_or_stock:
        tool_instruction = """
        鈿狅笍 **鐗瑰埆娉ㄦ剰 (ETF/鑲＄エ)**锛?
        1. `tool_get_retail_money_flow` 鍙兘鏌ュ叏甯傚満琛屼笟姒傚喌锛屼笉鏀寔鏌ュ崟鍙偂绁?ETF浠ｇ爜銆?
        2. 濡傛灉鏄?ETF锛屽彲浠ュ皾璇曟煡 `get_commodity_iv_info` 銆?
        3. 濡傛灉娌℃湁鍚堥€傚伐鍏凤紝鐩存帴鍥炵瓟 "鏆傛棤璇ュ搧绉嶈祫閲戞暟鎹?锛屼笉瑕佺紪閫犳暟鎹€?
                """
    else:
        tool_instruction = """
        鈿狅笍 **鐗瑰埆娉ㄦ剰 (鏈熻揣)**锛?
        1. 鏌ユ煇鍝佺褰撳ぉ鐨勬湡璐у晢澶氱┖鎺掑悕 -> search_broker_holdings_on_date(broker_name='鎵€鏈?, symbol='鍝佺鍚?, date='鏃ユ湡')
        2. 鏌ユ煇鍝佺涓€娈垫椂闂村悇鏈熻揣鍟嗙殑鎸佷粨鍙樺寲 -> tool_analyze_position_change(symbol='鍝佺鍚?, start_date, end_date)
        3. 鏌ユ湡璐ц祫閲戞祦 -> get_futures_fund_flow(symbol='鍝佺鍚?)
        4. 濡傛灉宸ュ叿杩斿洖"鏈壘鍒版暟鎹?锛屽瀹炲憡鐭ョ敤鎴凤紝涓嶈缂栭€犲亣鏁版嵁锛?
                """
    # 2. 瀹氫箟 Prompt
    # 鍛婅瘔浠栧彧鍋氭暟鎹惉杩愬伐锛屼笉瑕佺粰寤鸿
    prompt = f"""
    浣犳槸涓€浣嶈拷姹傛晥鐜囩殑甯傚満鏁版嵁鐩戞帶瀹?*銆傘€傚彧璐熻矗鏌ユ暟鎹粰缁撴灉銆?
    - 浠婂ぉ鏃ユ湡锛歿current_date}
    - 鏁版嵁搴撴渶鏂颁氦鏄撴棩锛歿latest_trade_date}

    銆愪綘鐨勫伐鍏风 - 鏍规嵁闂绫诲瀷閫夋嫨姝ｇ‘鐨勫伐鍏枫€?
    - 鏌ユ尝鍔ㄧ巼/IV -> get_commodity_iv_info
    - 鏌ヨ偂绁ㄨ涓氳祫閲?-> tool_get_retail_money_flow
    - 鏌ユ煇鏈熻揣璧勯噾娴佸姩 -> get_futures_fund_flow
    - 鏌ュ叏閮ㄦ湡璐ц祫閲戞矇娣€鎺掑悕 -> get_futures_fund_ranking
    - 鏌ユ煇澶╂煇鍝佺鐨勬湡璐у晢鎸佷粨鎺掑悕锛堥緳铏庢锛?-> search_broker_holdings_on_date 
    - 鏌ユ煇鍝佺涓€娈垫椂闂村唴鍚勬湡璐у晢鐨勬寔浠撳彉鍖?-> tool_analyze_position_change 
    - 鏌ユ煇鏈熻揣鍟嗗湪鍚勫搧绉嶇殑鎸佷粨鍙樺寲 -> tool_analyze_broker_positions 锛堝綋鍓嶅噣鎸佷粨浠ｈ〃鏈熻揣鍟嗗杩欏搧绉嶇殑瓒嬪娍鍒ゆ柇锛?
    - 鏌ユ湡鏉冩垚浜ら噺寮傚父(鏀鹃噺/寮傚姩) -> get_option_volume_abnormal
    - 鏌ユ湡鏉冩寔浠撻噺寮傚父(澶у崟澧炰粨) -> get_option_oi_abnormal
    - 鏌ユ湡鏉冩寔浠撻噺鎺掑悕 -> get_option_oi_ranking
    - 鏌ユ湡鏉冨悎绾︿环鏍?-> tool_query_specific_option,
    - 鏌ユ垚浜ら噺鍜屾寔浠撻噺 -> get_volume_oi
    - 鏌ユ湡璐ф寔浠撻噺鎺掑悕 -> get_futures_oi_ranking
    - 鏌ユ爣鐨勪环鏍?-> get_market_snapshot
    - 鏌ユ煇涓€澶╁巻鍙蹭环鏍?-> get_historical_price
    - 鏌ュ尯闂寸粺璁?鏈€楂?鏈€浣?鍖洪棿娑ㄨ穼) -> get_price_statistics
    - 鏌ユ渶杩慛涓氦鏄撴棩閫愭棩鏄庣粏琛?-> get_recent_price_series
    - 鏌ュ畯瑙傛寚鏍?-> get_macro_indicator(indicator_code='US10Y')  
    
    
    {tool_instruction}

    銆愯姹傘€?
    1. 绮惧噯浣跨敤宸ュ叿锛屼笉瑕佷贡璋冪敤锛岄櫎闈炲鎴锋湁瑕佹眰鍏ㄩ潰鍒嗘瀽銆?
    2. **鍙檲杩版暟鎹簨瀹?*锛屼笉瑕佽繘琛屽鏉傜殑琛屾儏棰勬祴鎴栫粰浜ゆ槗寤鸿銆?
    3. 濡傛灉鐢ㄦ埛娌℃湁鎸囧畾鏃ユ湡锛?*蹇呴』浣跨敤 {latest_trade_date}** 浣滀负鏌ヨ鏃ユ湡锛?
    4. 濡傛灉宸ュ叿杩斿洖浜?Markdown 琛ㄦ牸锛岃鍘熸牱杈撳嚭銆?
    5. 鍟嗗搧閮芥湁鏈熸潈锛岀姝㈣鍟嗗搧娌℃湁鍦哄唴鏈熸潈銆?
    6. 鐢ㄦ埛瑕佲€滄煇澶╀环鏍尖€濅紭鍏堢敤 `get_historical_price`锛涚敤鎴疯鈥滃尯闂寸粺璁♀€濅紭鍏堢敤 `get_price_statistics`銆?
    7. 鐢ㄦ埛瑕佲€滄渶杩慛澶?鏈€杩慛涓氦鏄撴棩/鍒楄〃/閫愭棩鏄庣粏/璧板娍鏁版嵁琛ㄢ€濇椂锛屼紭鍏堢敤 `get_recent_price_series`锛屼笉瑕佸彧杩斿洖 `get_market_snapshot`銆?
    """

    # 3. 鍒涘缓涓存椂 Agent (ReAct 妯″紡)
    # 浣跨敤 bind_tools 璁?LLM 鍙互鑷姩閫夋嫨鐢ㄥ摢涓伐鍏?
    monitor_agent = create_react_agent(llm, tools, prompt=prompt)

    partial_response = ""

    try:
        # 闄愬埗杩唬娆℃暟锛岄槻姝㈡寰幆
        result = monitor_agent.invoke(
            {"messages": [HumanMessage(content=user_q)]},
            {"recursion_limit": 15}  # 闄嶄綆鍒?15
        )
        last_response = result["messages"][-1].content
        partial_response = last_response

        return {
            "messages": [HumanMessage(content=f"銆愭暟鎹洃鎺с€慭n{last_response}")],
            "fund_data": last_response
        }

    except GeneratorExit:
        # 馃敟 GeneratorExit 涓嶆槸 Exception 瀛愮被锛岄渶瑕佸崟鐙崟鑾?
        fallback_msg = partial_response if partial_response else f"璧勯噾鏁版嵁鏌ヨ瀹屾垚"
        return {
            "messages": [HumanMessage(content=f"銆愭暟鎹洃鎺с€慭n{fallback_msg}")],
            "fund_data": fallback_msg
        }
    except Exception as e:
        # 馃洃 鍙鍑洪敊锛岀珛椹紭闆呴檷绾э紝杩斿洖绌烘暟鎹紝淇濊瘉 Supervisor 鍜?Finalizer 鑳界户缁伐浣?
        error_msg = f"鏁版嵁鏌ヨ鏆備笉鍙敤 (Monitor Error)"
        print(f"Monitor Node Crash: {e}")  # 鍚庡彴鎵撳嵃鏃ュ織鏂逛究璋冭瘯
        return {
            "messages": [HumanMessage(content=f"銆愭暟鎹洃鎺с€憑error_msg}")],
            "fund_data": "鏃犳暟鎹?
        }


# 馃煚 3. 鏈熸潈绛栫暐鍛?(閫昏緫纭紪鐮?+ LLM娑﹁壊)
def strategist_node(state: AgentState, llm):
    """
    鏈熸潈绛栫暐鍛?- 浣跨敤 ReAct 妯″紡璁?LLM 鑷富鍐冲畾璋冪敤宸ュ叿
    """
    symbol = state["symbol"]
    user_q = state.get("user_query", "")
    risk_pref = state.get("risk_preference", "绋冲仴鍨?)
    fund = state.get("fund_data", "鏆傛棤鏄庢樉璧勯噾娴佸悜")
    trend = state.get("trend_signal", "Neutral")
    mem_context = state.get("memory_context", "")
    tech_view = state.get("technical_summary", "")
    current_date = datetime.now().strftime("%Y骞?m鏈?d鏃?)
    key_level = state.get("key_levels", "")

    # [鏂板] 鑾峰彇鍚堢害涔樻暟
    multiplier_str = get_option_multiplier(symbol)
    multiplier_hint = f"\n        銆愬悎绾︿箻鏁般€戯細{multiplier_str}锛堣绠楃泩浜忔椂蹇呴』涔樹互姝ゆ暟锛? if multiplier_str else ""

    # 馃敟 [鏂板] 鑾峰彇鎸佷粨涓婁笅鏂?
    portfolio_corr_index = state.get("portfolio_top_corr_index", "")
    portfolio_corr_value = state.get("portfolio_top_corr_value", "")
    portfolio_summary = state.get("portfolio_summary", "")

    # 鏋勫缓鎸佷粨涓婁笅鏂囨彁绀?
    portfolio_context = ""
    if portfolio_corr_index and portfolio_corr_value:
        portfolio_context = f"\n        銆愬鎴锋寔浠撲俊鎭€戯細瀹㈡埛鎸佷粨缁勫悎涓巤portfolio_corr_index}鎸囨暟鐩稿叧搴︽渶楂橈紙鐩稿叧绯绘暟{portfolio_corr_value}锛?
        if portfolio_summary:
            portfolio_context += f"\n        鎸佷粨姒傚喌锛歿portfolio_summary[:100]}"

    # === 馃敟 鏈熸潈绛栫暐涓撶敤宸ュ叿闆?===
    tools = [
        # 鏈熸潈鏁版嵁宸ュ叿
        get_commodity_iv_info,  # IV鎺掑悕/娉㈠姩鐜?
        check_option_expiry_status,  # 鍒版湡鏃ョ姸鎬?
        tool_query_specific_option,  # 鏌ヨ鐗瑰畾鏈熸潈鍚堢害
        get_option_volume_abnormal,  # 鏈熸潈鎴愪氦寮傚姩
        get_option_oi_abnormal,  # 鏈熸潈鎸佷粨寮傚姩
        get_etf_option_strikes,  # ETF鏈熸潈琛屾潈浠?
        # 鏍囩殑鍒嗘瀽宸ュ叿
        get_market_snapshot,  # 鏍囩殑蹇収/鐜颁环
        # 杈呭姪宸ュ叿
        search_investment_knowledge,  # 鐭ヨ瘑搴撴绱?
        run_option_backtest,  # 鏈熸潈鍥炴祴
    ]

    # === 馃敟 ReAct Prompt - 寮曞鏈熸潈绛栫暐鎺ㄧ悊 ===
    prompt = f"""
        浣犳槸涓€浣?*璧勬繁鏈熸潈浜ゆ槗绛栫暐甯?*锛屾搮闀挎牴鎹競鍦烘暟鎹璁℃湡鏉冪瓥鐣ャ€?

        銆愬綋鍓嶆棩鏈熴€戯細{current_date}
        銆愬垎鏋愭爣鐨勩€戯細{symbol}{multiplier_hint}
        銆愬鎴烽棶棰樸€戯細{user_q}
        銆愬鎴烽闄╁亸濂姐€戯細{risk_pref}
        銆愬鎴峰巻鍙茶蹇嗐€戯細{mem_context}
        銆愭妧鏈潰鍙傝€冦€戯細{trend} 銆?{tech_view}{portfolio_context}

        銆愬伐浣滄祦绋嬨€?
        **绗竴姝ワ細鑾峰彇鏍囩殑浠锋牸鍜屾尝鍔ㄧ巼**
        - 鐢?`get_market_snapshot` 鑾峰彇鐜颁环锛岀敤`get_commodity_iv_info` 鐪婭V锛岀敤`check_option_expiry_status` 鐪嬪埌鏈熸棩銆?

        **绗簩姝ワ細璁捐绛栫暐**
        - **鏈熸潈绛栫暐**锛氭牴鎹妧鏈潰瓒嬪娍+IV+璺濈鍒版湡鏃?瀹㈡埛椋庨櫓鍋忓ソ鏉ラ€夋嫨绛栫暐锛屽彲浠ユ煡鐭ヨ瘑搴撹緟鍔ーsearch_investment_knowledge`銆?
        - **绛栫暐鏂瑰悜**锛氬鏋滄妧鏈潰鍙傝€冩槸鍋氬鎴栫湅娑紝灏变笉瑕佺粰鍋氱┖绛栫暐锛屽鏋滄妧鏈潰鍙傝€冩槸鍋氱┖鎴栫湅璺岋紝灏变笉瑕佺粰鍋氬绛栫暐銆?
        - **鎸佷粨鍏宠仈**锛氬鏋滃鎴锋湁鎸佷粨淇℃伅锛堣銆愬鎴锋寔浠撲俊鎭€戯級锛屼笖褰撳墠鏍囩殑涓庢寔浠撶浉鍏虫寚鏁版湁鍏筹紝闇€瑕佹槑纭鏄庣瓥鐣ュ浣曡緟鍔╂垨瀵瑰啿鐜版湁鎸佷粨椋庨櫓銆備緥濡傦細"鑰冭檻鍒版偍鐨勮偂绁ㄦ寔浠撲笌{portfolio_corr_index if portfolio_corr_index else 'XX鎸囨暟'}楂樺害鐩稿叧锛屽缓璁敤璇ユ湡鏉冪瓥鐣ユ潵..."
        
        **绗笁姝ワ細鎬濊€冭鏉冧环鍚堢害 (Strikes)**
        - 濡傛灉瀹㈡埛鏈夋寚瀹氳鏉冧环鍚堢害锛屽氨鐩存帴鏍规嵁瀹㈡埛闇€姹傦紝浣嗗彲浠ョ粰鍑哄悎閫傜殑涓嶅悓寤鸿銆?
        - 濡傛灉鐢ㄦ埛娌℃寚瀹氾紝灏辨牴鎹璁＄殑绛栫暐鍜屽鎴烽闄╁亸濂芥潵鐪嬮渶瑕佸摢浜涘悎绾︺€?

        **绗洓姝ワ細纭畾绛栫暐鐨勬墽琛屽悎绾?*
        - **鍚堢害閫夋嫨**锛氫竴瀹氳鏍规嵁鏍囩殑鐜颁环锛屽啀鏉ユ壘鍚堥€傜殑琛屾潈浠峰悎绾︺€?
        - **鏌ヨ鍚堢害**锛氱敤 `tool_query_specific_option` 鏌ュ叿浣撴湡鏉冧环鏍硷紙鏍煎紡锛?鏍囩殑 琛屾潈浠?璁よ喘/璁ゆ步"锛夛紝鏉冨埄閲戜环鏍间篃瑕佷箻涓婂悎绾︿箻鏁般€?
        - 鍙湁褰撳伐鍏疯繑鍥炰簡鏈夋晥鐨勪环鏍兼暟鎹椂锛屾墠鑳芥帹鑽愯鍚堢害銆?
        - 濡傛灉宸ュ叿杩斿洖鈥滄湭鎵惧埌鈥濓紝璇峰皾璇曡皟鏁磋鏉冧环鍐嶆鏌ヨ锛屾垨鑰呰瘹瀹炲憡鐭ョ敤鎴疯妗ｄ綅鏃犲悎绾︺€?
        - 濡傛灉瀹㈡埛闂€滃洖娴?绛栫暐琛ㄧ幇鈥濓紝鍙敤 `run_option_backtest` 缁欏嚭鍥炴祴缁撴灉銆?

        銆愰闄╁亸濂介€傞厤銆戯細
           - 銆愪繚瀹堝瀷銆戯細鍙帹鑽愰闄╂湁闄愮殑绛栫暐锛堢墰甯備环宸€佺唺甯備环宸€佹瘮鐜囦环宸級锛岀姝㈣８鍗?
           - 銆愮ǔ鍋ュ瀷銆戯細鍙互閫傚害杩涙敾锛堜拱骞冲€兼湡鏉冦€侀『鍔垮崠铏氬€兼湡鏉冦€佷环宸瓥鐣ャ€佸鍏戠瓥鐣ャ€佸悎鎴愭湡璐э級
           - 銆愭縺杩涘瀷銆戯細鍙互鐢ㄧН鏋佺瓥鐣ワ紙鏈夎秼鍔挎椂涔版繁铏氭湡鏉冦€佷拱鏈棩鏈熸潈銆侀榫欏湪澶╋紝娌¤秼鍔挎椂灏卞弻鍗栨湡鏉冿紝鎴栬€呭崠鏈棩鏈熸潈锛?
           
        銆愬伐鍏蜂娇鐢ㄧ壒娈婃彁閱掋€戯細
        1. 涓瘉1000鏈夎偂鎸囨湡鏉冿紝涓嶈鐢╣et_etf_option_strikes锛屽繀椤荤敤tool_query_specific_option鏌ユ湡鏉冨悎绾?

        銆愯緭鍑鸿姹傘€?
        1. 缁欏嚭 1-2 涓叿浣撶殑鏈熸潈绛栫暐寤鸿锛岃鏉冧环蹇呴』鐢ㄥ伐鍏锋煡杩囥€?
        2. **璁＄畻鐩堜簭绀轰緥鏃讹紝蹇呴』涔樹互鍚堢害涔樻暟**
        3. 瑙ｉ噴涓轰粈涔堣繖涓瓥鐣ラ€傚悎甯傚満鎴栧鎴凤紝鍙互鏌ョ煡璇嗗簱杈呭姪`search_investment_knowledge`
        4. 缁欏嚭姝㈡崯/姝㈢泩寤鸿
        5. 绂佹鑷繁缂栭€犲亣鏁版嵁锛?

        """

    # === 馃敟 鍒涘缓 ReAct Agent ===
    strategist_agent = create_react_agent(llm, tools, prompt=prompt)

    # 鐢ㄤ簬寮傚父鎭㈠
    partial_response = ""

    try:
        result = strategist_agent.invoke(
            {"messages": [HumanMessage(content=user_q)]},
            {"recursion_limit": 40}  # 鏈熸潈鍒嗘瀽鍙兘闇€瑕佸杞伐鍏疯皟鐢?
        )

        last_response = result["messages"][-1].content
        partial_response = last_response

        return {
            "messages": [HumanMessage(content=f"銆愭湡鏉冪瓥鐣ャ€慭n{last_response}")],
            "option_strategy": last_response
        }

    except GeneratorExit:
        # 娴佽涓柇鏃剁殑浼橀泤闄嶇骇
        fallback_msg = partial_response if partial_response else f"鏈熸潈绛栫暐鍒嗘瀽宸插畬鎴愶紝鍏充簬{symbol}鐨勫缓璁鍙傝€冧笂鏂囥€?
        return {
            "messages": [HumanMessage(content=f"銆愭湡鏉冪瓥鐣ャ€慭n{fallback_msg}")],
            "option_strategy": fallback_msg
        }

    except Exception as e:
        # 鍏朵粬寮傚父鐨勯檷绾у鐞?
        error_msg = f"鏈熸潈绛栫暐鍒嗘瀽閬囧埌闂: {e}"
        print(f"鈿狅笍 strategist_node 閿欒: {e}")
        return {
            "messages": [HumanMessage(content=f"銆愭湡鏉冪瓥鐣ャ€慭n{error_msg}")],
            "option_strategy": ""
        }



# 馃煠 5. 鎯呮姤鐮旂┒鍛?
def researcher_node(state: AgentState,llm=None):
    symbol = state["symbol"]
    symbol_name = state.get("symbol_name", "")
    query = state["user_query"]
    current_date = datetime.now().strftime("%Y骞?m鏈?d鏃?%A")
    # 1. 瑁呭鑸嗘儏涓庢悳绱㈠伐鍏?
    tools = [
        get_finance_related_trends,  # 鏌ヨ储缁忕被鐑偣 (鍚岃姳椤?涓滄柟璐㈠瘜鐑)
        get_today_hotlist,  # 鏌ュ叏缃戠儹鎼?(鎶栭煶/寰崥/鐧惧害)
        tool_get_polymarket_sentiment,  # 鏌ラ娴嬪競鍦鸿儨鐜?(Polymarket)
        analyze_keyword_trend,  # 鏌ョ壒瀹氬叧閿瘝鐑害瓒嬪娍
        search_hotlist_history,  # 鏌ョ儹鐐瑰巻鍙插洖婧?
        search_web,  # 鍏滃簳锛氶€氱敤鑱旂綉鎼滅储
        get_financial_news,  # 鍏滃簳锛氫紶缁熻储缁忔柊闂?
        get_trending_hotspots
    ]
    system_prompt = f"""
        浣犳槸涓€浣?*椤剁骇甯傚満鎯呮姤瀹?(Market Intelligence Officer)**銆?
        浣犵殑鑱岃矗涓嶄粎浠呮槸鐪嬫柊闂伙紝鏇存槸鎹曟崏**甯傚満鎯呯华銆佺儹鐐归鍙ｅ拰瀹忚棰勬湡**銆?
        銆愬綋鍓嶇湡瀹炴棩鏈熴€戯細{current_date} 

        銆愬鎴烽渶姹傘€? "{query}"
        銆愭爣鐨勩€? {symbol_name}({symbol})  

        銆愬伐鍏疯皟鐢ㄧ瓥鐣ャ€戯細

        1. 馃幉 **瀹忚棰勬湡/澶т簨浠?鑳滅巼** (濡?"澶ч€夎皝璧?銆?闄嶆伅姒傜巼"銆?鍦扮紭鏀挎不"銆?鎴樹簤"):
           - **蹇呴』璋冪敤** `tool_get_polymarket_sentiment`銆?
           - Polymarket 鐨勭湡閲戠櫧閾舵娂娉ㄦ暟鎹瘮鏂伴椈鏇村噯銆?

        2. 馃敟 **甯傚満椋庡彛/鏁ｆ埛鐑害** (濡?"鐜板湪鐐掍粈涔?銆?鏈€杩戠殑鐑偣"):
           - **浼樺厛璋冪敤** `get_finance_related_trends` (鐪嬭储缁忓湀鍦ㄥ叧娉ㄤ粈涔?銆?
           - **杈呭姪璋冪敤** `get_today_hotlist` (鐪嬫姈闊?寰崥绛夊叏缃戞祦閲忓湪鍝?銆?
           - 鍏虫敞鍏抽敭璇嶏細姒傚康鏉垮潡銆佺獊鍙戜簨浠躲€佹斂绛栧埄濂姐€?

        3. 馃搱 **鐗瑰畾姒傚康鐑害楠岃瘉** (濡?"浣庣┖缁忔祹鏈€杩戠儹鍚?):
           - **璋冪敤** `analyze_keyword_trend`銆?
           - 鐢ㄦ暟鎹瘉鏄庤璇濋鏄浜?鍗囨俯鏈?杩樻槸"閫€娼湡"銆?
           - **璋冪敤** get_trending_hotspots 鏌ユ渶杩戞湁浠€涔堢儹鐐硅秼鍔?
        
        4. 馃搱 **褰撳ぉ璐㈢粡蹇** :
           - **璋冪敤** `get_financial_news`銆?
           
        5. 馃摪 **鍏蜂綋璧勮/浜嬪疄鏍告煡**:
           - 濡傛灉浠ヤ笂宸ュ叿鏌ヤ笉鍒帮紝鎴栬€呴渶瑕佹洿澶氱粏鑺傦紝璋冪敤 `search_web``锛屽彧鑳戒娇鐢?娆★紝缁濆涓嶈閲嶅浣跨敤search_web銆?

        銆愯緭鍑鸿姹傘€?
        - 鎶婃煡鍒扮殑淇℃伅鍋氫釜鏁寸悊褰掔撼銆?
        - 娌℃湁鏌ュ埌鐨勪俊鎭氨璇翠笉鐭ラ亾锛岀粷瀵逛笉瑕佷贡缂栭€犳暟鎹紝鍙兘渚濈収鏌ュ埌鐨勬暟鎹璇濄€?
        """

    # 3. 鍒涘缓 Agent
    researcher_agent = create_react_agent(llm, tools, prompt=system_prompt)

    partial_response = ""

    try:
        # 鑸嗘儏鏌ヨ鍙兘闇€瑕佸姝ワ紙鍏堟煡鐑锛屽啀鎼滅粏鑺傦級锛岀粰瓒虫鏁?
        result = researcher_agent.invoke(
            {"messages": [HumanMessage(content=query)]},
            {"recursion_limit": 20}
        )

        last_response = result["messages"][-1].content

        # 馃敟 妫€娴嬫槸鍚︽槸 "need more steps" 閿欒
        if "need more steps" in last_response.lower() or "sorry" in last_response.lower():
            # 灏濊瘯浠庝箣鍓嶇殑宸ュ叿璋冪敤缁撴灉涓彁鍙栨湁鐢ㄤ俊鎭?
            tool_results = []
            for msg in result.get("messages", []):
                msg_type = getattr(msg, 'type', '')
                content = getattr(msg, 'content', '')
                # 鏀堕泦宸ュ叿杩斿洖鐨勫唴瀹?
                if msg_type == 'tool' and content and len(content) > 50:
                    tool_results.append(content)

            if tool_results:
                # 鎷兼帴鎵€鏈夊伐鍏风粨鏋?
                combined = "\n\n".join(tool_results[-3:])  # 鍙栨渶杩?涓伐鍏风粨鏋?
                last_response = f"鏍规嵁宸叉敹闆嗙殑淇℃伅锛歕n\n{combined}"

        # 鍔犱笂鍓嶇紑锛屾柟渚?Finalizer 鏁村悎
        return {
            "messages": [HumanMessage(content=f"銆愭儏鎶ヤ笌鑸嗘儏銆慭n{last_response}")]
        }

    except GeneratorExit:
        # 馃敟 GeneratorExit 澶勭悊锛氬皾璇曡繑鍥炲凡鏀堕泦鐨勯儴鍒嗙粨鏋?
        fallback_msg = partial_response if partial_response else f"鎯呮姤鏌ヨ宸插畬鎴愶紝鍏充簬锛歿query[:50]}"
        return {
            "messages": [HumanMessage(content=f"銆愭儏鎶ヤ笌鑸嗘儏銆慭n{fallback_msg}")]
        }
    except Exception as e:
        return {
            "messages": [HumanMessage(content=f"銆愭儏鎶ャ€戞煡璇㈠彈闃? {e}")]
        }


def macro_analyst_node(state: AgentState, llm):
    """
    瀹忚绛栫暐甯堬細鍏ㄦ櫙鎵弿瀹忚鏁版嵁锛岀粨鍚堟敹鐩婄巼鏇茬嚎鍜屾柊闂伙紝鍒ゆ柇鍏ㄧ悆娴佸姩鎬у懆鏈熴€?
    """
    user_q = state.get("user_query", "")
    symbol = state["symbol"]
    symbol_name = state.get("symbol_name", "")
    news_context = state.get("news_summary", "鏆傛棤鏈€鏂板畯瑙傛柊闂?)
    current_date = datetime.now().strftime("%Y骞?m鏈?d鏃?)

    # 寮曞叆瀹忚宸ュ叿 (璇风‘淇濆湪鏂囦欢澶撮儴 import 杩欎簺宸ュ叿)
    # from plot_tools import draw_macro_compare_chart
    # from macro_tools import get_macro_indicator

    tools = [
        get_macro_indicator,  # 鏉ヨ嚜 macro_tools (宸插崌绾ф敮鎸佸鏌?
        get_macro_overview,  # 鏉ヨ嚜 macro_tools (鐪嬪叏灞€)
        analyze_yield_curve,  # 鏉ヨ嚜 macro_tools (鐪嬪€掓寕)
        draw_macro_compare_chart,  # 鏉ヨ嚜 plot_tools (鐪嬪弻杞磋蛋鍔?
        get_financial_news  # 鏉ヨ嚜 news_tools (鐪嬫柊闂绘壘鍘熷洜)
    ]

    prompt = f"""
        浣犳槸涓€浣?*棣栧腑瀹忚绛栫暐甯?*锛屼俊濂?"Don't fight the Fed"銆?
        浣犵殑鏍稿績浠诲姟鏄埄鐢ㄣ€愭暟鎹叏鏅?+ 鏀剁泭鐜囨洸绾?+ 鏍稿績鎸囨爣銆戞ā鍨嬪垽鏂叏鐞冩祦鍔ㄦ€х幆澧冦€?

        銆愬綋鍓嶆棩鏈熴€戯細{current_date}
        銆愭爣鐨勩€? {symbol_name}({symbol})  
        銆愭儏鎶ュ憳鎻愪緵鐨勬柊闂汇€戯細
        {news_context}

        銆愬垎鏋愰€昏緫涓庡伐鍏疯皟鐢ㄩ『搴忋€?

        **绗竴姝ワ細鍏ㄦ櫙涓庤“閫€璇婃柇 (蹇呴』鎵ц)**
        1. 璋冪敤 `get_macro_overview(category='all')`锛?
           - 蹇€熸壂涓€鐪煎叏鐞冨競鍦猴紝鐪嬫槸鍚︽湁寮傚父鏉垮潡锛堝BDI鏆磋穼鏆楃ず闇€姹備笉瓒筹紝闈炵編璐у竵闆嗕綋鏆磋穼鏆楃ず缇庡厓铏瑰惛锛夈€?
        2. 璋冪敤 `analyze_yield_curve()`锛?
           - **杩欐槸鏈€鍏抽敭鐨勪竴姝?*銆傛鏌ョ編鍊?10Y-2Y 鏄惁**鍊掓寕**銆?
           - 鍊掓寕 = 琛伴€€棰勮/闄嶆伅棰勬湡鍗囨俯锛涢櫋宄寲 = 澶嶈嫃鎴栭€氳儉棰勬湡銆?

        **绗簩姝ワ細鏍稿績閿氱偣楠岃瘉**
        1. 璋冪敤 `get_macro_indicator(indicator_code='US10Y,DXY,US2Y')`锛?
           - 鑾峰彇绮剧‘鐨勬渶鏂版姤浠峰拰瓒嬪娍銆?
        2. 缁撳悎 `Researcher` 鐨勬柊闂伙紙CPI/闈炲啘/FOMC锛夛紝瑙ｉ噴鏁版嵁涓轰綍娉㈠姩銆?

        **绗笁姝ワ細鍙鍖?**
        - 璋冪敤 `draw_macro_compare_chart` 缁樺埗 US10Y vs DXY 鐨勫姣斿浘锛岀洿瑙傚睍绀烘祦鍔ㄦ€ф敹绱ц繕鏄斁鏉俱€?

        銆愬喅绛栫煩闃?(缁撳悎鏀剁泭鐜囨洸绾?銆?
        - **绱х缉浜ゆ槗**锛歎S10Y 涓婅 + DXY 寮哄娍 + 鏇茬嚎姝ｅ父 -> 缁忔祹杩囩儹锛岀編鑱斿偍鍔犳伅锛屾潃浼板€笺€?
        - **琛伴€€浜ゆ槗**锛歎S10Y 涓嬭 + 鏇茬嚎鍊掓寕(鎴栧€掓寕鍔犳繁) -> 甯傚満鎭愭厡锛屾娂娉ㄩ檷鎭紝鍒╁ソ榛勯噾/缇庡€恒€?
        - **閬块櫓浜ゆ槗 (Risk-Off)**锛歎S10Y 涓嬭 + DXY 寮哄娍 -> 琛伴€€鎭愭厡锛岃偂甯傛毚璺岋紝缇庡厓缇庡€哄弻鐗涖€?
        - **澶嶈嫃浜ゆ槗**锛歎S10Y 娓╁拰涓婅 + 鏇茬嚎闄″抄鍖?-> 缁忔祹澶嶈嫃锛屽埄濂藉晢鍝?鑲＄エ銆?
        - **婊炶儉/淇′换鍗辨満**锛歎S10Y 涓婅 + DXY 寮卞娍 -> 姣旇緝缃曡锛屽埄濂藉疄鐗╁晢鍝佸拰榛勯噾璧勪骇銆?

        銆愯緭鍑鸿姹傘€?
        璇疯緭鍑轰竴浠介€昏緫涓ュ瘑鐨勫畯瑙傜爺鎶ワ細
        1. **銆愬懆鏈熷畾璋冦€?*锛氭槑纭綋鍓嶆槸鈥滅揣缂┾€濄€佲€滆“閫€鎭愭厡鈥濊繕鏄€滃鑻忊€濇ā寮忋€?
        2. **銆愭敹鐩婄巼鏇茬嚎鐩戞祴銆?*锛氫笓闂ㄤ竴娈靛垎鏋愬€掓寕鎯呭喌鍙婂叾闅愬惈鐨勭粡娴庤“閫€姒傜巼銆?
        3. **銆愯祫浜у奖鍝嶃€?*锛氬熀浜庝笂杩板垽鏂紝瀵?榛勯噾/鑲″競/澶у畻鍟嗗搧 鐨勫叿浣撳奖鍝嶃€?
        """

    macro_agent = create_react_agent(llm, tools, prompt=prompt)

    try:
        result = macro_agent.invoke(
            {"messages": [HumanMessage(content=f"璇峰垎鏋愬綋鍓嶇殑瀹忚娴佸姩鎬х幆澧冦€傜敤鎴烽棶棰橈細{user_q}")]},
            {"recursion_limit": 10}
        )
        last_response = result["messages"][-1].content

        # 鎻愬彇鍥捐〃
        chart_img = ""
        chart_match = re.search(r'(macro_chart_[a-zA-Z0-9_]+\.json)', last_response)
        if chart_match:
            chart_img = chart_match.group(1)

        return {
            "messages": [HumanMessage(content=f"銆愬畯瑙傜瓥鐣ャ€慭n{last_response}")],
            "macro_view": last_response,
            "macro_chart": chart_img
        }
    except Exception as e:
        return {
            "messages": [HumanMessage(content=f"銆愬畯瑙傜瓥鐣ャ€戝垎鏋愬彈闃? {e}")]
        }

# ==========================================
# 馃煟 6. 鑱婂ぉ/鐭ヨ瘑闂瓟鍛?(Chatter)
# 鑱岃矗锛氶棽鑱?+ 鐧剧鐭ヨ瘑闂瓟 (RAG + Web)
# ==========================================
def chatter_node(state: AgentState, llm=None):
    """
    鑱婂ぉ/鐭ヨ瘑闂瓟鍛?- 浣跨敤 ReAct 妯″紡鑷富鎬濊€冨拰妫€绱?
    浼樺厛浣跨敤鍐呴儴鐭ヨ瘑搴擄紝蹇呰鏃惰緟浠ョ綉缁滄悳绱?
    """
    user_query = state["user_query"]
    is_followup = bool(state.get("is_followup", False))
    recent_context = str(state.get("recent_context", "") or "").strip()
    mem_context = str(state.get("memory_context", "") or "").strip()
    current_date = datetime.now().strftime("%Y骞?m鏈?d鏃?)
    context_parts = []
    if recent_context:
        context_parts.append(f"銆愭渶杩戜袱杞細璇濄€慭n{recent_context}")
    if mem_context:
        context_parts.append(f"銆愮浉鍏抽暱鏈熻蹇嗐€慭n{mem_context}")
    combined_context = "\n\n".join(context_parts) if context_parts else "鏃?

    if is_followup and combined_context == "鏃?:
        return {
            "messages": [HumanMessage(content="銆愮煡璇嗛棶绛斻€慭n鎴戣繖杞病鏈夋绱㈠埌涓婁竴杞叧閿粨璁猴紝鏆傛椂鏃犳硶瀹夊叏鎵挎帴銆傝琛ュ厖涓婁竴杞殑鏍稿績缁撹锛堜緥濡傛柟鍚戙€佸叧閿綅銆佺瓥鐣ワ級锛屾垜绔嬪埢缁х画銆?)],
            "knowledge_context": ""
        }

    # 鍒ゆ柇鏄惁鏄畝鍗曢棶鍊欙紙涓嶉渶瑕佸伐鍏凤級
    is_greeting = len(user_query) < 5 and any(x in user_query for x in ["浣犲ソ", "鍡?, "鏃?, "璋?, "hello", "hi", "鍢?, "鏅氫笂濂?, "鏃╀笂濂?, "鏃╁畨", "涓崍濂?, "涓嬪崍濂?])

    if is_greeting and not is_followup:
        # 绠€鍗曢棶鍊欑洿鎺ュ洖澶嶏紝涓嶅惎鍔?ReAct
        response = llm.invoke(f"鐢ㄦ埛璇达細{user_query}銆傝鐑儏鍥炲簲锛屽苟寮曞鐢ㄦ埛璇㈤棶琛屾儏銆佺瓥鐣ユ垨閲戣瀺鐭ヨ瘑銆?)
        return {
            "messages": [HumanMessage(content=f"銆愰棽鑱娿€慭n{response.content}")],
            "knowledge_context": ""
        }

    # === 馃敟 鐭ヨ瘑闂瓟涓撶敤宸ュ叿闆?===
    tools = [
        # 鐭ヨ瘑妫€绱㈠伐鍏凤紙浼樺厛锛?
        search_investment_knowledge,  # 鍐呴儴鐭ヨ瘑搴?- 鏈€楂樹紭鍏堢骇

        # 缃戠粶鎼滅储宸ュ叿锛堣緟鍔╋級
        get_financial_news,  # 璐㈢粡鏂伴椈

        # 甯傚満鏁版嵁宸ュ叿锛堝鏋滅敤鎴烽棶琛屾儏鐩稿叧锛?
        get_market_snapshot,  # 蹇€熻幏鍙栨爣鐨勪环鏍?

    ]

    core_rules = """
        銆愨殸锔?鏍稿績鍘熷垯锛氱煡璇嗗簱浼樺厛銆?
        1. **绗竴姝ュ繀椤?*锛氬厛鐢?`search_investment_knowledge` 妫€绱㈠唴閮ㄧ煡璇嗗簱
        2. **绗簩姝ュ彲閫?*锛氬鏋滅煡璇嗗簱淇℃伅涓嶈冻鎴栭渶瑕佹渶鏂版暟鎹紝鍐嶇敤鍏朵粬宸ュ叿琛ュ厖
           - `get_financial_news`锛氳幏鍙栬储缁忔柊闂?
           - `get_market_snapshot`锛氳幏鍙栧疄鏃惰鎯?
    """
    if is_followup:
        core_rules = """
        銆愨殸锔?鏍稿績鍘熷垯锛氳繛缁壙鎺ヤ紭鍏堛€?
        1. 绗竴娈靛繀椤诲厛寮曠敤涓婁竴杞叧閿粨璁猴紙1-2鍙ワ級锛屽啀鍥炵瓟褰撳墠闂銆?
        2. 鎵挎帴璇存槑瑕佸叿浣擄紝涓嶅緱鍙鈥滄牴鎹笂鏂団€濄€?
        3. 浠呭湪闇€瑕佽ˉ鍏呬簨瀹炴椂鍐嶈皟鐢ㄥ伐鍏凤紱鍙互鏌ョ煡璇嗗簱锛屼絾涓嶆槸蹇呴』绗竴姝ャ€?
        4. 绂佹鎶娾€滅煡璇嗗簱鍛戒腑涓虹┖鈥濆綋浣滈粯璁ゆā鏉垮洖绛斻€?
        """

    # === 馃敟 ReAct Prompt - 鎸夋ā寮忓垏鎹㈣鍒?===
    prompt = f"""
        浣犳槸涓€浣嶇儹鎯呫€佸崥瀛︾殑**閲戣瀺瀵煎笀**锛岃礋璐ｈВ绛旂敤鎴风殑閲戣瀺鐭ヨ瘑闂鍜岄棽鑱娿€?

        銆愬綋鍓嶆棩鏈熴€戯細{current_date}
        銆愮敤鎴烽棶棰樸€戯細{user_query}
        銆愯繛缁拷闂ā寮忋€戯細{"鏄? if is_followup else "鍚?}
        銆愬巻鍙叉壙鎺ヤ笂涓嬫枃銆戯細
        {combined_context}

        {core_rules}

        銆愬洖绛旈鏍笺€?
        1. 璇皵瑕佽交鏉俱€佹槗鎳傦紝鍍忔湅鍙嬭亰澶╀竴鏍?
        2. 濡傛灉鏄蹇佃В閲婏紝鐢ㄩ€氫織鐨勪緥瀛愬府鍔╃悊瑙?
        3. 濡傛灉鏄瓥鐣ラ棶棰橈紝缁撳悎瀹為檯鍦烘櫙璇存槑
        4. 閫傚綋寮曞鐢ㄦ埛娣卞叆鎺㈣鐩稿叧璇濋


        銆愮姝簨椤广€?
        - 涓嶈缂栭€犳暟鎹垨绛栫暐
        - 鐭ヨ瘑搴撳唴瀹硅浼樺厛鍙傝€?
        """

    # === 馃敟 鍒涘缓 ReAct Agent ===
    chatter_agent = create_react_agent(llm, tools, prompt=prompt)

    # 鐢ㄤ簬寮傚父鎭㈠
    partial_response = ""
    kb_content = ""

    try:
        result = chatter_agent.invoke(
            {"messages": [HumanMessage(content=user_query)]},
            {"recursion_limit": 15}  # 鐭ヨ瘑闂瓟閫氬父涓嶉渶瑕佸お澶氳疆
        )

        last_response = result["messages"][-1].content
        partial_response = last_response

        # 灏濊瘯浠庢秷鎭腑鎻愬彇鐭ヨ瘑搴撳唴瀹癸紙鐢ㄤ簬鍚庣画娴佺▼锛?
        for msg in result.get("messages", []):
            content = getattr(msg, 'content', '')
            if "鐭ヨ瘑搴? in content or "鎶曡祫绗旇" in content:
                kb_content = content[:500]
                break

        return {
            "messages": [HumanMessage(content=f"銆愮煡璇嗛棶绛斻€慭n{last_response}")],
            "knowledge_context": kb_content
        }

    except GeneratorExit:
        # 娴佽涓柇鏃剁殑浼橀泤闄嶇骇
        fallback_msg = partial_response if partial_response else "璁╂垜鏉ュ洖绛斾綘鐨勯棶棰?.."
        return {
            "messages": [HumanMessage(content=f"銆愮煡璇嗛棶绛斻€慭n{fallback_msg}")],
            "knowledge_context": kb_content
        }

    except Exception as e:
        # 鍏朵粬寮傚父 - 闄嶇骇鍒扮畝鍗曞洖绛?
        print(f"鈿狅笍 chatter_node 閿欒: {e}")
        try:
            # 灏濊瘯涓嶇敤宸ュ叿鐩存帴鍥炵瓟
            fallback_prompt = f"鐢ㄦ埛闂細{user_query}銆傝鍩轰簬浣犵殑鐭ヨ瘑鍥炵瓟锛岃姘旇交鏉惧弸濂姐€?
            if is_followup and combined_context != "鏃?:
                fallback_prompt = (
                    f"鐢ㄦ埛鍦ㄨ繛缁拷闂€俓n"
                    f"鍘嗗彶涓婁笅鏂囷細\n{combined_context}\n\n"
                    f"褰撳墠闂锛歿user_query}\n"
                    f"璇峰厛鎵挎帴涓婁竴杞叧閿粨璁猴紝鍐嶅洖绛斿綋鍓嶉棶棰樸€?
                )
            simple_response = llm.invoke(fallback_prompt)
            return {
                "messages": [HumanMessage(content=f"銆愰棽鑱娿€慭n{simple_response.content}")],
                "knowledge_context": ""
            }
        except:
            return {
                "messages": [HumanMessage(content=f"銆愰棽鑱娿€慭n鎶辨瓑锛屾垜閬囧埌浜嗕竴浜涢棶棰樸€傝绋嶅悗鍐嶈瘯鎴栨崲涓柟寮忔彁闂€?)],
                "knowledge_context": ""
            }


# 馃煟 =閫夎偂鍛?(Screener)
def screener_node(state: AgentState, llm):
    # --- 1. 鑾峰彇瀹忚璧勯噾椋庡悜 (Sector Flow) ---
    sector_flow_info = ""
    query = state["user_query"]

    # === 馃敟 [鏂板] 褰㈡€佹煡璇㈠揩閫熼€氶亾 ===
    # 褰撶敤鎴锋槑纭闂?鏈変粈涔堝舰鎬?鏃讹紝鐩存帴杩斿洖褰㈡€佺粺璁★紝涓嶈蛋閫夎偂娴佺▼
    pattern_inquiry_keywords = ["鏈変粈涔堝舰鎬?, "浠€涔堝舰鎬?, "鍝簺褰㈡€?, "褰㈡€佹湁鍝簺", "褰㈡€佺粺璁?, "浠婂ぉ褰㈡€?, "甯傚満褰㈡€?]
    default_bearish_patterns = ["绌哄ご鍚炲櫖", "涓嬮檷涓夋硶", "鐮翠綅", "鍧囩嚎绌哄ご", "澶滄槦"]

    if any(kw in query for kw in pattern_inquiry_keywords):
        try:
            patterns_info = get_available_patterns.invoke({})
            return {
                "messages": [HumanMessage(content=f"銆愬舰鎬佺粺璁°€慭n{patterns_info}")],
                "symbol": state.get("symbol", "")
            }
        except Exception as e:
            print(f"鈿狅笍 褰㈡€佺粺璁℃煡璇㈠け璐? {e}")
            # 澶辫触鏃剁户缁蛋姝ｅ父閫夎偂娴佺▼

    risk_keywords = [
        "鍗遍櫓", "椋庨櫓", "涓嶈涔?, "鍒拱", "鍗栨帀", "瑕佸崠", "鍗栧嚭",
        "閬垮紑", "瑙勯伩", "杩滅", "灏忓績", "璀︽儠", "娉ㄦ剰", "鍑哄満",
        "宸殑", "鏈€宸?, "鍨冨溇", "鐑傝偂", "鍧?, "闆?, "鏆撮浄",
        "鍒嗘暟鏈€浣?, "璇勫垎鏈€浣?, "鏈€寮?, "寮卞娍鑲?,
        "涓嶅ソ", "涓嶈", "涓嶈", "鍒", "璺戣矾", "娓呬粨",
        "涓嬭穼", "浜忔崯", "濂楃墷", "鍓茶倝", "姝㈡崯"
    ]
    pattern_keywords = [
        "绾笁鍏?, "涓夌孩鍏?, "杩為槼", "涓夎繛闃?, "鍒涙柊楂?,
        "閲戦拡鎺㈠簳", "閿ゅ瓙", "閿ゅ瓙绾?, "涓嬪奖绾?,
        "澶氬ご鍚炲櫖", "鐪嬫定鍚炲櫖", "闃冲寘闃?,
        "鏃╂櫒涔嬫槦", "鍚槑鏄?, "鏅ㄦ槦",
        "V鍨嬪弽杞?, "鍙嶈浆", "鍋囩獊鐮?, "鍋囪穼鐮?,
        "澶ч槼绾?, "娑ㄥ仠", "鏀鹃噺绐佺牬", "绐佺牬",
        "涓夊彧涔岄甫", "榛戜笁鍏?, "杩為槾", "涓夎繛闃?, "涓嬮檷涓夋硶",
        "绌哄ご鍚炲櫖", "鐪嬭穼鍚炲櫖", "闃村寘闃?,
        "鍚婁汉绾?, "涓婂悐绾?, "鍊掗敜瀛?, "灏勫嚮涔嬫槦", "娴佹槦",
        "榛勬槒涔嬫槦", "鍊扸", "瑙侀《",
        "澶ч槾绾?, "璺屽仠", "鏀鹃噺涓嬭穼",
        "鍗佸瓧鏄?, "娉㈠姩鏀剁獎", "闇囪崱", "钃勫娍", "鍙嶅嚮",
        "鍧囩嚎澶氬ご", "澶氬ご鎺掑垪", "鍧囩嚎绌哄ご", "绌哄ご鎺掑垪",
        "涓婂崌閫氶亾", "涓嬮檷閫氶亾"
    ]

    industry_keywords = [
        "閾惰", "璇佸埜", "淇濋櫓", "鎴垮湴浜?, "鍖昏嵂", "鍖荤枟", "鍗婂浣?, "鑺墖",
        "鏂拌兘婧?, "鍏変紡", "閿傜數", "姹借溅", "鐧介厭", "閰块厭", "椋熷搧", "楗枡",
        "鍐涘伐", "鑸┖", "鑸硅埗", "閽㈤搧", "鐓ょ偔", "鐭虫补", "鍖栧伐", "鏈夎壊",
        "鐢靛姏", "姘存偿", "寤烘潗", "鏈烘", "鐢靛瓙", "閫氫俊", "璁＄畻鏈?, "杞欢", "鑸ぉ",
        "浼犲獟", "娓告垙", "鏁欒偛", "鏃呮父", "閰掑簵", "闆跺敭", "鐢靛晢", "鐗╂祦",
        "鍐滀笟", "鍏绘畺", "绾虹粐", "鏈嶈", "瀹剁數", "瀹跺叿", "寤虹瓚", "瑁呴グ"
    ]

    # 馃敟 [鏂板] 鎴愪氦閲?閲忚兘绫诲叧閿瘝 - 鐢ㄤ簬瑙﹀彂鎴愪氦閲忎笓鐢ㄥ揩閫熼€氶亾

    volume_keywords = [
        "鎴愪氦閲忓紓甯?, "鎴愪氦閲忓鍔?, "鎴愪氦閲忓噺灏?, "鎴愪氦閲忔斁澶?, "鎴愪氦閲忚悗缂?, "鎴愪氦閲戦寮傚父",
        "鎴愪氦閲忛€?, "鎸夋垚浜ら噺", "鐢ㄦ垚浜ら噺", "鐪嬫垚浜ら噺", "鏌ユ垚浜ら噺",
        "鎴愪氦閲忕獊鐒?, "鎴愪氦閲忓墠", "鎴愪氦閲忔帓鍚?, "鎴愪氦閲忔渶澶?, "鎴愪氦閲廡OP",
        "鏀鹃噺", "缂╅噺", "澶╅噺", "鍦伴噺", "宸ㄩ噺", "鐖嗛噺",
        "閲忓紓甯?, "閲忚兘寮傚父", "閲忚兘鏀惧ぇ", "閲忎环榻愬崌", "閲忎环鑳岀",
        "鏀鹃噺寮傚姩", "閲忓紓鍔?, "缂╅噺涓嬭穼", "鏀鹃噺涓婃定", "鏀鹃噺绐佺牬",
        "鎹㈡墜鐜囧紓甯?, "鎹㈡墜鐜囨渶楂?, "鎹㈡墜鐜囨帓鍚?,
        "璧勯噾鎶㈢", "涓诲姏娴佸叆", "涓诲姏鎶㈢", "涓诲姏鍩嬩紡","鍩嬩紡",
        "鎴愪氦寮傚父", "浜ゆ槗寮傚父娲昏穬", "寮傚父娲昏穬"
    ]

    is_risk_query = any(kw in query for kw in risk_keywords)

    detected_pattern = None
    for pattern in pattern_keywords:
        if pattern in query:
            detected_pattern = pattern
            break

    # 馃敟 濡傛灉鏄嵄闄╂煡璇絾娌℃湁鎸囧畾褰㈡€侊紝鑷姩閫夋嫨鐪嬭穼褰㈡€?
    if is_risk_query and not detected_pattern:
        detected_pattern = random.choice(default_bearish_patterns)

    detected_industry = None
    for industry in industry_keywords:
        if industry in query:
            detected_industry = industry
            break

    is_volume_query = any(kw in query for kw in volume_keywords)

    # === 馃敟 [鏂板] 鎴愪氦閲忛€夎偂蹇€熼€氶亾 ===
    # 褰撴娴嬪埌鎴愪氦閲忕浉鍏冲叧閿瘝鏃讹紝鐩存帴璋冪敤 search_volume_anomalies锛屼笉渚濊禆 LLM 閫夊伐鍏?
    # 鈿狅笍 蹇呴』鎺掗櫎宸茶"褰㈡€?琛屼笟/椋庨櫓"閫氶亾鍛戒腑鐨勬煡璇紝閬垮厤璇嫤鎴?
    #    渚嬪"鏀鹃噺绐佺牬鐨勮偂绁?搴旇蛋褰㈡€侀€氶亾锛?鍗婂浣撴斁閲?搴旇蛋琛屼笟閫氶亾
    if is_volume_query and not detected_pattern and not detected_industry and not is_risk_query:
        print(f"馃搳 鎴愪氦閲忛€夎偂蹇€熼€氶亾瑙﹀彂: {query}")
        volume_result = ""
        try:
            volume_result = search_volume_anomalies.invoke({"days": 1, "min_score": 30, "limit": 15})
        except Exception as e:
            volume_result = f"鎴愪氦閲忓紓鍔ㄦ暟鎹煡璇㈠け璐? {e}"

        # 杈呭姪鎷夊彇琛屼笟璧勯噾娴佸悜
        sector_flow_for_vol = ""
        try:
            sector_flow_for_vol = tool_get_retail_money_flow.invoke({"days": 2})
        except Exception as e:
            sector_flow_for_vol = f"鏆傛棤琛屼笟璧勯噾娴佹暟鎹? {e}"

        vol_screen_prompt = f"""
                   浣犳槸涓€浣嶈祫娣遍€夎偂涓撳銆傜敤鎴锋兂鎵?*鎴愪氦閲忓紓甯?鏀鹃噺/閲忚兘寮傚姩**鐨勮偂绁ㄣ€?

                   銆愭暟鎹簮 A锛氭垚浜ら噺寮傚姩鑲＄エ锛堟寜寮傚姩璇勫垎鎺掑簭锛夈€?
                   {volume_result}

                   銆愭暟鎹簮 B锛氬競鍦鸿祫閲戦鍚?(琛屼笟)銆?
                   {sector_flow_for_vol}

                   銆愮敤鎴峰師濮嬮渶姹傘€? "{query}"

                   銆愪綘鐨勪换鍔°€?
                   1. 浠庛€愭暟鎹簮A銆戜腑灞曠ず鎴愪氦閲忓紓鍔ㄧ殑鑲＄エ锛?*涓嶈缂栭€犳暟鎹?*锛?
                   2. 缁撳悎銆愭暟鎹簮B銆戝垎鏋愯繖浜涜偂绁ㄦ墍灞炴澘鍧楃殑璧勯噾娴佸悜鏄惁鏀寔銆?
                   3. 鍖哄垎鍒嗘瀽锛?
                      - 馃搱 **鏀鹃噺涓婃定**锛氬彲鑳芥槸涓诲姏璧勯噾杩涘満绐佺牬淇″彿
                      - 馃搲 **鏀鹃噺涓嬭穼**锛氬彲鑳芥槸涓诲姏鍑鸿揣鎴栨亹鎱屾姏鍞?
                      - 鈿栵笍 **鏀鹃噺妯洏**锛氬彲鑳芥槸鎹㈡墜鍏呭垎锛屽叧娉ㄥ悗缁柟鍚?
                   4. 缁欏嚭椋庨櫓鎻愮ず銆?

                   銆愯緭鍑烘牸寮忋€?
                   馃搳 **鎴愪氦閲忓紓鍔ㄩ€夎偂缁撴灉**

                   1. **鑲＄エ鍚嶇О** (浠ｇ爜) - 寮傚姩璇勫垎锛歑X
                      - 馃搳 閲忚兘鎯呭喌锛歺xx
                      - 馃挵 璧勯噾闈細鎵€灞炴澘鍧楄祫閲憍xx
                      - 馃挕 鎿嶄綔寤鸿锛歺xx

                   鈿狅笍 **椋庨櫓鎻愮ず**锛氭斁閲忎笉绛変簬鍒╁ソ锛岄渶缁撳悎浠锋牸璧板娍鍒ゆ柇銆?
                   """

        response = llm.invoke(vol_screen_prompt)

        codes = re.findall(r'[0-9]{6}\.[A-Z]{2}', response.content)
        if not codes:
            codes = re.findall(r'[0-9]{6}', response.content)
        next_symbol = codes[0] if codes else state.get("symbol", "")

        return {
            "messages": [HumanMessage(content=f"銆愮簿閫夎偂绁ㄣ€慭n{response.content}")],
            "symbol": next_symbol
        }

    # === 馃敟 [鏂板] 鍒ゆ柇鏄惁闇€瑕佽繘鍏?ReAct 妯″紡 ===
    # 濡傛灉娌℃湁鍖归厤鍒颁换浣曞揩閫熼€氶亾鏉′欢锛岃鏄庡彲鑳芥槸姒傚康/涓婚绫绘煡璇?
    need_react_mode = (not is_risk_query and
                       not detected_pattern and
                       not detected_industry)

    if need_react_mode:
        print(f"馃 閫夎偂杩涘叆 ReAct 妯″紡: {query}")

        # ReAct 妯″紡宸ュ叿闆?
        react_tools = [
            search_top_stocks,  # 鎸夊舰鎬?琛屼笟/鍒嗘暟绛涢€?
            search_web,  # 鎼滅储姒傚康鑲°€佺儹鐐逛俊鎭?
            search_investment_knowledge,  # 鏌ョ煡璇嗗簱
            get_available_patterns,  # 鏌ュ舰鎬佺粺璁?
            search_volume_anomalies,
            query_stock_volume,
            tool_get_retail_money_flow  # 璧勯噾娴佸悜
        ]

        current_date = datetime.now().strftime("%Y骞?m鏈?d鏃?)

        react_prompt = f"""
            浣犳槸涓€浣嶈祫娣遍€夎偂涓撳锛屾搮闀块€氳繃姒傚康涓婚銆佸競鍦虹儹鐐规潵鎸栨帢鎶曡祫鏈轰細銆?

            銆愬綋鍓嶆棩鏈熴€戯細{current_date}
            銆愮敤鎴烽渶姹傘€戯細{query}

            銆愪綘鐨勬牳蹇冭兘鍔涳細姒傚康/涓婚閫夎偂銆?
            浣犱富瑕佸鐞嗙殑鏄?姒傚康绫?銆?涓婚绫?銆?鐑偣绫?閫夎偂闇€姹傦紝渚嬪锛?
            - "AI姒傚康鑲℃湁鍝簺" 鈫?鍏?search_web 鏌ユ蹇佃偂鍚嶅崟锛屽啀鐢?search_top_stocks 楠岃瘉鎶€鏈潰
            - "浣庣┖缁忔祹鐩稿叧鐨勮偂绁? 鈫?search_web 鏌ョ浉鍏充釜鑲★紝鍐嶇敤 search_top_stocks 浜ゅ弶楠岃瘉
            - "鏈€杩戞湁浠€涔堝ソ鑲＄エ" 鈫?tool_get_retail_money_flow 鐪嬭祫閲戦鍙?+ search_top_stocks 鐪嬪己鍔胯偂
            - "甯垜閫夊嚑鍙ǔ鍋ョ殑鑲＄エ" 鈫?search_top_stocks(condition="缁煎悎璇勫垎") 閫夐珮鍒嗚偂

            銆愬伐鍏蜂娇鐢ㄦ寚鍗椼€?
            1. `search_web`锛氭悳绱㈡蹇佃偂鍚嶅崟銆佺儹鐐逛俊鎭€佽涓氭柊闂伙紙鎼滅储鍏抽敭璇嶅"xxx姒傚康鑲?榫欏ご"锛?
            2. `search_top_stocks`锛氭寜鎶€鏈舰鎬佹垨缁煎悎璇勫垎绛涢€夎偂绁紙condition 鍙～"缁煎悎璇勫垎"鎴栧叿浣撳舰鎬佸悕锛?
            3. `tool_get_retail_money_flow`锛氭煡鐪嬭涓氳祫閲戞祦鍚戯紝鍒ゆ柇鍝簺鏉垮潡鏈夎祫閲戞敮鎸?
            4. `get_available_patterns`锛氭煡鐪嬩粖鏃ュ競鍦烘湁鍝簺K绾垮舰鎬佸彲渚涚瓫閫?
            5. `search_investment_knowledge`锛氭煡璇㈠唴閮ㄧ煡璇嗗簱鑾峰彇鎶曡祫鍙傝€?
            6. `search_volume_anomalies`锛氭煡鎴愪氦閲忓紓鍔ㄨ偂绁紙澶囩敤锛屾垚浜ら噺鏌ヨ閫氬父宸茶蹇€熼€氶亾澶勭悊锛?
            7. `query_stock_volume`锛氭煡鍗曞彧鑲＄エ鐨勬垚浜ら噺璇︽儏

            銆愭爣鍑嗘祦绋嬨€?
            1. 鐞嗚В鐢ㄦ埛鎯虫壘浠€涔堢被鍨嬬殑鑲＄エ
            2. 閫夋嫨鏈€鍚堥€傜殑 1-2 涓伐鍏疯幏鍙栨暟鎹?
            3. 鏁寸悊缁撴灉锛岃鏄庢帹鑽愮悊鐢?+ 椋庨櫓鎻愮ず

            銆愮姝簨椤广€?
            - 涓嶈缂栭€犺偂绁ㄤ唬鐮佹垨鍚嶇О
            - 濡傛灉鎼滅储涓嶅埌鐩稿叧淇℃伅锛岃瘹瀹炲憡鐭ョ敤鎴?
            - 涓嶈閲嶅璋冪敤鍚屼竴涓伐鍏?
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

            # 鎻愬彇鑲＄エ浠ｇ爜
            codes = re.findall(r'[0-9]{6}\.[A-Z]{2}', last_response)
            if not codes:
                codes = re.findall(r'[0-9]{6}', last_response)
            next_symbol = codes[0] if codes else state.get("symbol", "")

            return {
                "messages": [HumanMessage(content=f"銆愮簿閫夎偂绁ㄣ€慭n{last_response}")],
                "symbol": next_symbol
            }

        except GeneratorExit:
            fallback_msg = partial_response if partial_response else f"鍏充簬'{query}'鐨勯€夎偂鍒嗘瀽宸插畬鎴愶紝璇峰弬鑰冧笂鏂囥€?
            return {
                "messages": [HumanMessage(content=f"銆愮簿閫夎偂绁ㄣ€慭n{fallback_msg}")],
                "symbol": state.get("symbol", "")
            }

        except Exception as e:
            print(f"鈿狅笍 screener ReAct 妯″紡閿欒: {e}")
            # 闄嶇骇鍒伴粯璁ら€夎偂
            pass

    # === 蹇€熼€氶亾锛氭甯搁€夎偂娴佺▼ ===
    try:
        # 馃敟 [淇] 寮哄埗浣跨敤瀛楀吀浼犲弬锛岀‘淇?days 琚瘑鍒负鏁存暟
        sector_flow_info = tool_get_retail_money_flow.invoke({"days": 2})
    except Exception as e:
        sector_flow_info = f"鏆傛棤琛屼笟璧勯噾娴佹暟鎹? {e}"

    # 鉁?鏂板锛氭媺鍙栨斁閲忓紓鍔ㄦ暟鎹?
    volume_anomaly_info = ""
    try:
        volume_anomaly_info = search_volume_anomalies.invoke({"days": 1, "min_score": 50, "limit": 10})
    except Exception as e:
        volume_anomaly_info = f"鏆傛棤鏀鹃噺鏁版嵁: {e}"

    # --- 2. 鑾峰彇鎶€鏈潰寮哄娍鑲?(Technical Screener) ---
    raw_stocks = ""
    try:
        invoke_params = {"limit": 15}

        if detected_pattern:
            invoke_params["condition"] = detected_pattern
        else:
            invoke_params["condition"] = "缁煎悎璇勫垎"

        if detected_industry:
            invoke_params["industry"] = detected_industry

        # 馃敟 [鏍稿績] 鍗遍櫓妯″紡涓嬶紝鎸夊垎鏁颁粠浣庡埌楂樻帓搴?
        if is_risk_query:
            invoke_params["sort_order"] = "asc"
        else:
            invoke_params["sort_order"] = "desc"

        print(f"馃搳 璋冪敤閫夎偂宸ュ叿: {invoke_params}")
        raw_stocks = search_top_stocks.invoke(invoke_params)
    except Exception as e:
        raw_stocks = f"閫夎偂宸ュ叿璋冪敤澶辫触: {e}"

    # --- 3. LLM 缁煎悎鍐崇瓥 (Intersection Logic) ---
    # 璁?AI 鎵惧嚭鈥滆祫閲戦鍙ｂ€濆拰鈥滄妧鏈己鍔库€濈殑浜ら泦

    if is_risk_query:
        screen_prompt = f"""
                浣犳槸涓€浣嶈祫娣遍€夎偂涓撳銆傜敤鎴锋兂鐭ラ亾**鍝簺鑲＄エ鏈夐闄╋紝搴旇瑙勯伩鎴栧崠鍑?*銆?

                銆愭暟鎹簮 A锛氬競鍦鸿祫閲戞祦鍚戙€?
                {sector_flow_info}

                銆愭暟鎹簮 B锛氶闄╄偂绁ㄦ睜锛堟妧鏈潰杈冨急锛夈€?
                {raw_stocks}

                銆愭暟鎹簮 C锛氫粖鏃ユ垚浜ら噺寮傚姩鑲★紙鎸夎瘎鍒嗘帓搴忥級銆?
                {volume_anomaly_info}

                銆愮敤鎴峰師濮嬮渶姹傘€? "{query}"

                銆愪綘鐨勪换鍔°€?
                1. 浠庛€愭暟鎹簮B銆戜腑閫夊嚭 3-5 鍙渶鍗遍櫓鐨勮偂绁紝**涓嶈缂栭€?*锛?
                2. 瑙ｉ噴涓轰粈涔堣繖浜涜偂绁ㄦ湁椋庨櫓锛?
                   - 馃搲 **鎶€鏈潰椋庨櫓**锛氬嚭鐜颁粈涔堢湅璺屽舰鎬?
                   - 馃捀 **璧勯噾闈㈤闄?*锛氭墍灞炴澘鍧楁槸鍚﹀湪璧勯噾娴佸嚭
                   - 鈿狅笍 **缁煎悎椋庨櫓绛夌骇**锛氶珮/涓?浣?
                3. 缁欏嚭鎿嶄綔寤鸿锛?
                   - 鎸佹湁鑰咃細鏄惁搴旇姝㈡崯/鍑忎粨
                   - 瑙傛湜鑰咃細涓轰粈涔堜笉瑕佷拱鍏?

                銆愯緭鍑烘牸寮忋€?
                鈿狅笍 **椋庨櫓鑲＄エ璀︾ず**

                1. **鑲＄エ鍚嶇О** (浠ｇ爜) - 椋庨櫓绛夌骇锛氿煍撮珮
                   - 馃搲 褰㈡€侀闄╋細xxx
                   - 馃捀 璧勯噾椋庨櫓锛歺xx
                   - 馃挕 鎿嶄綔寤鸿锛歺xx
                """
    elif detected_pattern:
        screen_prompt = f"""
                浣犳槸涓€浣嶈祫娣遍€夎偂涓撳銆傜敤鎴锋兂鎵剧鍚堛€恵detected_pattern}銆戝舰鎬佺殑鑲＄エ銆?

                銆愭暟鎹簮 A锛氬競鍦鸿祫閲戦鍚戙€?
                {sector_flow_info}

                銆愭暟鎹簮 B锛氱鍚?{detected_pattern}"褰㈡€佺殑鑲＄エ銆?
                {raw_stocks}

                銆愮敤鎴峰師濮嬮渶姹傘€? "{query}"

                銆愪綘鐨勪换鍔°€?
                1. 濡傛灉銆愭暟鎹簮B銆戜腑鏈夌鍚堝舰鎬佺殑鑲＄エ锛?*鐩存帴灞曠ず杩欎簺鑲＄エ**锛屼笉瑕佺紪閫狅紒
                2. 缁撳悎銆愭暟鎹簮A銆戠殑璧勯噾娴佸悜锛屽垽鏂繖浜涜偂绁ㄦ墍灞炴澘鍧楁槸鍚︽湁璧勯噾鏀寔銆?
                3. 濡傛灉銆愭暟鎹簮B銆戜负绌猴紝鍛婅瘔鐢ㄦ埛"褰撳墠甯傚満鏆傛棤鏄庢樉鐨剓detected_pattern}褰㈡€佽偂绁?銆?

                銆愯緭鍑烘牸寮忋€?
                - 鎺ㄨ崘 3-5 鍙鍚堟潯浠剁殑鑲＄エ
                - 姣忓彧鑲＄エ璇存槑锛氬舰鎬佺壒寰?+ 鎵€灞炴澘鍧楄祫閲戞儏鍐?
                - 鈿狅笍 涓嶈缂栭€犺偂绁紒
                """
    else:
        screen_prompt = f"""
                浣犳槸涓€浣嶈祫娣遍€夎偂涓撳銆傝缁撳悎銆愯祫閲戦鍚戙€戝拰銆愭妧鏈舰鎬併€戜负瀹㈡埛绮鹃€夎偂绁ㄣ€?

                銆愭暟鎹簮 A锛氬競鍦鸿祫閲戦鍚?(琛屼笟)銆?
                {sector_flow_info}

                銆愭暟鎹簮 B锛氭妧鏈潰寮哄娍鑲℃睜銆?
                {raw_stocks}

                銆愬綋鍓嶇敤鎴烽渶姹傘€? "{query}"

                銆愰€夎偂閫昏緫銆?
                1. **瀵绘壘浜ら泦**锛氳瀵熴€愭暟鎹簮A銆戜腑璧勯噾鍑€娴佸叆闈犲墠鐨勬澘鍧楋紝鐒跺悗鍦ㄣ€愭暟鎹簮B銆戜腑瀵绘壘灞炰簬杩欎簺鏉垮潡鐨勪釜鑲°€?
                2. **浼樹腑閫変紭**锛氬鏋滄暟鎹簮B閲屾病鏈夊尮閰嶉鍙ｇ殑鑲＄エ锛屽垯浼樺厛鎺ㄨ崘鏁版嵁婧怋閲屽垎鏁版渶楂樼殑銆?
                3. 浠旂粏妫€鏌ョ敤鎴烽渶姹傛槸鍚﹀寘鍚壒瀹氭澘鍧楁垨琛屼笟銆?

                銆愯緭鍑轰换鍔°€?
                1. 鎺ㄨ崘 5 鍙渶鍊煎緱鍏虫敞鐨勮偂绁ㄣ€?
                2. **鎺ㄨ崘鐞嗙敱蹇呴』鍖呭惈涓ょ偣**锛?
                   - 馃尓锔?**椋庡彛**锛氳鑲℃墍灞炴澘鍧楃殑璧勯噾娴佹儏鍐点€?
                   - 馃搱 **褰㈡€?*锛氳鑲＄殑鎶€鏈潰鐗瑰緛銆?
                3. 缁欏嚭姝㈡崯寤鸿銆?
                """

    response = llm.invoke(screen_prompt)

    # 灏濊瘯鎻愬彇绗竴鍙偂绁ㄧ殑浠ｇ爜浣滀负 symbol
    codes = re.findall(r'[0-9]{6}\.[A-Z]{2}', response.content)
    if not codes:
        codes = re.findall(r'[0-9]{6}', response.content)

    next_symbol = codes[0] if codes else state["symbol"]

    return {
        "messages": [HumanMessage(content=f"銆愮簿閫夎偂绁ㄣ€慭n{response.content}")],
        "symbol": next_symbol
    }


# ==========================================
#  馃尪锔?7. 姣掕垖鍒嗘瀽甯?(Roaster Node) - 鍒涙剰鍔熻兘
# ==========================================
def roaster_node(state: AgentState, llm):
    query = state["user_query"]
    symbol = state.get("symbol", "")

    # 1. 缁欎粬涓€浜涘熀鏈伐鍏凤紝璁╀粬鍚愭Ы鏃舵湁鐞嗘湁鎹?
    tools = [
        get_market_snapshot,  # 鐪嬩环鏍?(璺屼簡鎵嶈兘鍢茬瑧)
        get_stock_valuation,  # 鐪嬩及鍊?(璐典簡鎵嶈兘楠傞煭鑿?
        analyze_kline_pattern,  # 鐪嬪舰鎬?(鐮翠綅浜嗘墠鑳借ˉ鍒€)
        get_price_statistics,
        tool_get_retail_money_flow,  # 鑲＄エ琛屼笟璧勯噾
        get_futures_fund_flow,
        tool_compare_stocks
    ]

    # 2. 娉ㄥ叆鈥滄瘨鑸屸€濅汉璁?Prompt
    prompt = f"""
    浣犵幇鍦ㄦ槸**閲戣瀺鐣岀殑鈥滆劚鍙ｇ婕斿憳鈥濆吋鈥滄瘨鑸岃瘎璁哄憳鈥?*銆?
    鐢ㄦ埛鎶婃寔浠撴垨鍏虫敞鐨勮偂绁ㄧ粰浣犵湅锛屼綘鐨勪换鍔℃槸**鏃犳儏鍚愭Ы銆佺妧鍒╃偣璇?*銆?

    銆愬綋鍓嶅叧娉ㄣ€戯細{symbol if symbol else "鐢ㄦ埛鐨勮繖涓爣鐨?}
    銆愮敤鎴烽棶棰樸€戯細"{query}"

    銆愯涓哄噯鍒欍€戯細
    1. **浜鸿椋庢牸**锛?
       - 骞介粯銆佽鍒恒€佷娇鐢ㄧ綉缁滅儹姊楋紝姣斿柣瑕佸じ寮狅紙渚嬪锛氣€滆繖K绾胯蛋寰楀儚蹇冪數鍥惧仠浜嗕竴鏍封€濓級銆?
    3. **鏁版嵁椹卞姩鐨勫悙妲?*锛?
       - 鍏堣皟鐢ㄥ伐鍏风湅鏁版嵁銆?
       - 濡傛灉 **PE寰堥珮** -> 涓句緥鍚愭Ы锛氣€滀綘涔扮殑鏄偂绁ㄨ繕鏄ⅵ锛熻繖娉℃搏鎴崇牬浜嗚兘娣规浜恒€傗€?
       - 濡傛灉 **涓嬭穼瓒嬪娍** -> 涓句緥鍚愭Ы锛氣€滆繖绉嶉鍒€浣犱篃鏁㈡帴锛熸墜涓嶆兂瑕佷簡锛熲€?

    銆愯緭鍑鸿姹傘€戯細
    - 涓嶈鍐欓暱绡囧ぇ璁猴紝瑕佺煭灏忕簿鎮嶏紝瀛楀瓧鎵庡績銆?
    - 缁撳熬缁欎竴涓€滈煭鑿滄寚鏁扳€濊瘎鍒嗭紙0-100锛岃秺楂樿秺闊級銆?
    """

    # 3. 鍒涘缓 Agent
    roaster_agent = create_react_agent(llm, tools, prompt=prompt)

    try:
        result = roaster_agent.invoke({"messages": state["messages"]})
        last_response = result["messages"][-1].content

        return {
            "messages": [HumanMessage(content=f"銆愭瘨鑸岀偣璇勩€慭n{last_response}")]
        }
    except Exception as e:
        return {
            "messages": [HumanMessage(content=f"銆愭瘨鑸岀偣璇勩€慭n妲界偣澶锛屾垜閮芥棤璇簡... (绯荤粺閿欒: {e})")]
        }



# 鈿?6. 鑱婂ぉ/鎬荤粨鑺傜偣 (Finalizer)
# 馃敟 [鍗囩骇]锛氬崟淇℃簮妯″紡(瀹℃牳涓嶉噸鍐? vs 澶氫俊婧愭ā寮?缁熺)
def finalizer_node(state: AgentState, llm):
    # 1. 鏀堕泦鎵€鏈?AI 浜х敓鐨勫疄璐ㄦ€ф姤鍛?(杩囨护鎺夌敤鎴风殑璇濆拰绌虹殑)
    # 涔熷氨鏄?Analyst, Monitor, Strategist 绛変汉鐨勫彂瑷€
    all_messages = state["messages"]
    today_str = datetime.now().strftime("%Y骞?m鏈?d鏃?%H:%M")

    # 杩囨护鍑?AI 鐨勫洖澶?(HumanMessage 鍦ㄨ繖閲屾槸 Worker 鐨勮緭鍑鸿浇浣?
    # 涓旀帓闄ゆ帀鍙兘鏄┖鐨勬垨鑰呮棤鍏崇殑
    # 馃敟 [淇] 鍙敹闆嗘湁鏍囩鐨?worker 杈撳嚭锛屾帓闄ょ敤鎴峰師濮嬫秷鎭?
    WORKER_TAGS = [
        "銆愭妧鏈垎鏋愩€?, "銆愭暟鎹洃鎺с€?, "銆愭湡鏉冪瓥鐣ャ€?, "銆愭儏鎶ヤ笌鑸嗘儏銆?,
        "銆愬畯瑙傜瓥鐣ャ€?, "銆愮煡璇嗛棶绛斻€?, "銆愮簿閫夎偂绁ㄣ€?, "銆愭瘨鑸岀偣璇勩€?,
        "銆愮帇鐗屽垎鏋愩€?, "銆愰棽鑱娿€?, "銆愰鎺т慨姝ｃ€?, "銆愭寔浠撳垎鏋愩€?
    ]

    worker_msgs = [
        m for m in all_messages
        if isinstance(m, HumanMessage)
           and len(m.content) > 10
           and any(tag in m.content for tag in WORKER_TAGS)
    ]

    # 馃敟 [鏂板] 濡傛灉娌℃湁鏈夋晥鐨?worker 杈撳嚭锛岃繑鍥炲弸濂芥彁绀?
    if not worker_msgs:
        symbol = state.get("symbol", "鏈煡鏍囩殑")
        return {
            "messages": [HumanMessage(
                content=f"鈿狅笍 鎶辨瓑锛孉I 鍥㈤槦鏈兘鐢熸垚鍏充簬 **{symbol}** 鐨勬湁鏁堝垎鏋愭姤鍛娿€俓n\n鍙兘鍘熷洜锛歕n- 鏍囩殑浠ｇ爜涓嶆纭垨涓嶅湪鏀寔鑼冨洿鍐匼n- 鏁版嵁婧愭殏鏃舵棤娉曡闂甛n\n璇锋鏌ヤ唬鐮佸悗閲嶈瘯銆?)],
            "chart_img": ""
        }

    # 鑾峰彇鐭ヨ瘑搴撳唴瀹?

    # 鎷兼帴鍒颁竴璧风敤浜庤緭鍏?
    context_text = "\n".join([f"{m.content}" for m in worker_msgs])
    if "銆愮簿閫夎偂绁ㄣ€? in context_text:
        # 鐩存帴杩斿洖 PASS锛屼笉鍋氫换浣?LLM 鎬濊€冿紝姣绾у搷搴?
        return {
            "messages": [HumanMessage(content="PASS")]
        }
    if "銆愭瘨鑸岀偣璇勩€? in context_text:
        # 馃敟 roaster 鐨勬瘨鑸屽悙妲斤紝鐩存帴杩斿洖鍘熸枃锛屼笉瑕佹敼鍐欙紒
        print("馃敟 妫€娴嬪埌姣掕垖鐐硅瘎锛岃烦杩?finalizer 鏁村悎")

        # 馃敟 [淇] 鍙彁鍙栥€愭瘨鑸岀偣璇勩€戦儴鍒嗭紝涓嶈鍖呭惈鐢ㄦ埛鍘熷鎻愰棶
        roaster_content = ""
        for msg in worker_msgs:
            if "銆愭瘨鑸岀偣璇勩€? in msg.content:
                roaster_content = msg.content
                break

        # 鎻愬彇鍥捐〃璺緞锛堝鏋滄湁锛?
        chart_img = state.get("chart_img", "")
        if not chart_img:
            chart_match = re.search(r'IMAGE_CREATED:(chart_[a-zA-Z0-9_]+\.json)', context_text)
            if chart_match:
                chart_img = chart_match.group(1)
        return {
            "messages": [HumanMessage(content=roaster_content if roaster_content else context_text)],
            "chart_img": chart_img
        }
    # === 鍒ゆ柇閫昏緫锛氬崟鍏佃繕鏄洟鎴橈紵 ===
    # 濡傛灉鍙湁 1 涓伐绉嶅彂瑷€锛堟垨鑰呮病鏈夊彂瑷€锛夛紝涓斾笉鏄帇鐗屽垎鏋愬笀锛堢帇鐗屾湰鏉ュ氨鏄€荤粨濂界殑锛?
    symbol = state.get("symbol", "")
    symbol_name = state.get("symbol_name", "")
    mem_context = state.get("memory_context", "")
    macro_view = state.get("macro_view", "鏃犲畯瑙傚垎鏋?)
    trend = state.get("trend_signal", "")  # 渚嬪 "鐪嬫定"
    key_levels = state.get("key_levels", "")  # 渚嬪 "鍘嬪姏3000"

    # 馃敟 [鏂板] 鑾峰彇鎸佷粨涓婁笅鏂?
    portfolio_corr_index = state.get("portfolio_top_corr_index", "")
    portfolio_corr_value = state.get("portfolio_top_corr_value", "")
    portfolio_risks = state.get("portfolio_risks", "")

    risk_pref = state.get("risk_preference", "绋冲仴鍨?)
    is_single_source = len(worker_msgs) <= 1
    has_chart = "chart_" in context_text or "![" in context_text
    user_query = state.get("user_query", "")
    complex_keywords = ["鐢?, "鍥?, "瀵规瘮", "鍒嗘瀽", "浠峰樊", "鐩稿叧鎬?, "璧板娍"]
    is_complex_task = any(kw in user_query for kw in complex_keywords)

    display_name = f"{symbol_name}({symbol})" if symbol_name else symbol

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

    # 鑾峰彇褰撳墠鏈€鍚庝竴娆℃墽琛岀殑璁″垝锛堢敤浜庡垽鏂槸涓嶆槸鐜嬬墝锛?
    # (鐢变簬 state plan 琚?pop 浜嗭紝鎴戜滑鐢ㄧ畝鍗曠殑闀垮害鍒ゆ柇閫氬父澶熺敤锛屾垨鑰呯湅 context)


    if is_single_source and not has_chart and not is_complex_task:
        # === 妯″紡 A锛氳川妫€鍛?(Audit Mode) ===
        # 鐩爣锛氫繚鐣欏師姹佸師鍛崇殑鎺掔増锛屽彧鏌ラ敊
        symbol_aliases = _build_symbol_aliases(symbol)
        if not symbol_aliases:
            # symbol 鍏滃簳锛氬皾璇曚粠鍗曚俊婧愭姤鍛婁腑鎶撴爣棰樹唬鐮?
            match = re.search(r'([A-Za-z]{1,6}\d{0,4}|\d{6}(?:ETF)?)\s*(?:鎶€鏈潰|鎶€鏈垎鏋恷璧板娍)', context_text, re.IGNORECASE)
            if match:
                symbol_aliases = _build_symbol_aliases(match.group(1))
        source_norm = _normalize_symbol_text(context_text)
        enforce_symbol_lock = bool(symbol_aliases) and any(alias in source_norm for alias in symbol_aliases)
        symbol_lock_hint = (
            f"\n        5. **鏍囩殑閿佸畾**锛氭湰鎶ュ憡鏍囩殑鏄?{symbol or '/'.join(sorted(symbol_aliases))}銆?
            f" 浣犱笉鑳芥敼鎴愬叾浠栨爣鐨勩€傝嫢鏃犳硶纭锛岃杈撳嚭 DIRECT_PASS銆?
            if enforce_symbol_lock else ""
        )
        audit_prompt = f"""
        浣犳槸涓€浣嶄氦鏄撻鎺у畼銆傚洟闃熸彁浜や簡涓€浠藉垎鏋愭姤鍛婏紙濡備笅锛夈€?

        銆愬緟瀹℃牳鎶ュ憡銆戯細
        {context_text}

        銆愪换鍔°€戯細
        1. 妫€鏌ユ姤鍛婃槸鍚﹀瓨鍦?*鑷村懡鐨勫父璇嗘€ч敊璇?*锛堝鎶婃爣鐨勬悶閿欍€侀€昏緫瀹屽叏鐩稿弽锛夈€?
        2. **濡傛灉鎶ュ憡鏃犺**锛氳鐩存帴杈撳嚭鍥涗釜瀛?"DIRECT_PASS" (涓嶈杈撳嚭鍏朵粬绗﹀彿)銆傝繖鎰忓懗鐫€鐩存帴閲囩敤鍘熸姤鍛婏紝淇濈暀鍏跺畬缇庣殑 Markdown 鎺掔増銆?
        3. **濡傛灉鏈夎嚧鍛介敊璇?*锛氳淇敼閿欒鍚庯紝閲嶅啓涓€浠芥纭殑鎶ュ憡銆?
        4. 濡傛灉鍙戠敓鏁版嵁缂哄け鎴栬娉曢敊璇紝涓嶈鎶婇敊璇啓鍑烘潵銆?
        {symbol_lock_hint}
        """
        response = llm.invoke(audit_prompt)

        # 濡傛灉 LLM 瑙夊緱娌￠棶棰橈紝杩斿洖鐗瑰畾鏍囪
        if "DIRECT_PASS" in response.content:
            return {
                "messages": [HumanMessage(content=context_text)]
            }
        else:
            revised_text = response.content or ""
            if enforce_symbol_lock:
                revised_norm = _normalize_symbol_text(revised_text)
                keep_symbol = any(alias in revised_norm for alias in symbol_aliases)
                if not keep_symbol:
                    print(f"鈿狅笍 finalizer 瀹℃牎鐤戜技涓叉爣锛屽洖閫€鍘熸姤鍛娿€俵ocked={symbol_aliases}")
                    return {
                        "messages": [HumanMessage(content=context_text)]
                    }
            # 濡傛灉鏈夐敊琚噸鍐欎簡锛屽氨杩斿洖閲嶅啓鐨勫唴瀹?
            return {
                "messages": [HumanMessage(content=f"銆愰鎺т慨姝ｃ€慭n{revised_text}")]
            }

    else:
        # === 妯″紡 B锛氭€荤紪杈?(Editor Mode) ===
        # 鐩爣锛氬婧愪俊鎭暣鍚堬紝浣嗚鏍规嵁鐢ㄦ埛闂绫诲瀷璋冩暣杈撳嚭椋庢牸

        # 馃敟 [鏂板] 鑾峰彇鐢ㄦ埛鍘熷闂锛屽垽鏂棶棰樼被鍨?
        user_query = state.get("user_query", "")

        # 鍒ゆ柇鏄惁涓?绾暟鎹煡璇?绫婚棶棰?
        data_query_keywords = ["鎸佷粨", "鎺掑悕", "璧勯噾", "娴佸叆", "娴佸嚭", "澶氬皯", "鍝簺", "鍝釜", "鍓嶅嚑", "鍓?", "鍓嶄笁",
                               "鍓?", "鍓嶄簲", "top", "榫欒檸姒?, "澧炰粨", "鍑忎粨", "鍑€鎸佷粨", "鏈€澶?, "鏈€澶?]
        is_data_query = any(kw in user_query for kw in data_query_keywords)

        # 鍒ゆ柇鏄惁涓?缁煎悎鍒嗘瀽"绫婚棶棰?
        analysis_keywords = ["鍒嗘瀽", "鎬庝箞鐪?, "鎬庝箞鍋?, "绛栫暐", "寤鸿", "鎿嶄綔", "琛屾儏", "璧板娍", "濡備綍","瓒嬪娍", "鍏ㄩ潰"]
        is_analysis_query = any(kw in user_query for kw in analysis_keywords)

        # 馃幆 鏍规嵁闂绫诲瀷閫夋嫨涓嶅悓鐨?Prompt
        if is_data_query and not is_analysis_query:
            # === 鏁版嵁鏌ヨ妯″紡锛氱畝娲佺洿鎺?===
            cio_prompt = f"""
                浣犳槸涓€浣嶆暟鎹鏌ュ笀銆?
                銆愬綋鍓嶆棩鏈熴€戯細{today_str}
                銆愮敤鎴烽棶棰樸€戯細{user_query}
                銆愬垎鏋愭爣鐨勩€? {display_name} 

                銆愬洟闃熸敹闆嗙殑鏁版嵁銆戯細
                {context_text}

                銆愯緭鍑鸿姹傘€戯細
                1. **鐩存帴鍥炵瓟鐢ㄦ埛鐨勯棶棰?*锛屼笉瑕佽窇棰橈紒鐢ㄦ埛闂寔浠撳氨绛旀寔浠擄紝闂帓鍚嶅氨绛旀帓鍚嶃€?
                2. **绐佸嚭鏁版嵁鏈韩**锛氱敤琛ㄦ牸鎴栧垪琛ㄦ竻鏅板睍绀烘暟鎹€?
                3. **绠€鐭偣璇?*锛氬彲浠ュ姞 1-2 鍙ュ鏁版嵁鐨勮В璇伙紙濡?XX 鍦ㄥぇ骞呭浠擄紝鍙兘鐪嬪"锛夛紝浣嗕笉瑕佹壇鍒版妧鏈潰K绾垮垎鏋愩€?
                4. 鏁版嵁涓嶈缂栭€犲拰淇敼銆?
                5. 涓嶈鍐欐垚鎶曡祫鎶ュ憡锛屾枃瀛楄绠€娲佹湁鍔涖€?
                6. 濡傛灉鍙戠敓鏁版嵁缂哄け鎴栬娉曢敊璇紝涓嶈鎶婇敊璇啓鍑烘潵銆?
                7. 鏁版嵁鏄瘡澶╀笅鍗?鐐瑰悗鏇存柊銆?


                銆愭牸寮忕ず渚嬨€戯細
                馃搳 **涓滆瘉鏈熻揣鎸佷粨鍓?澶у搧绉?* ({today_str})

                | 鎺掑悕 | 鍝佺 | 鍑€鎸佷粨 | 鏂瑰悜 |
                |-----|------|-------|-----|
                | 1 | 铻虹汗閽?| -33,272 | 绌哄ご |
                | ... | ... | ... | ... |

                馃挕 **绠€璇?*锛氫笢璇佸湪榛戣壊绯绘暣浣撳亸绌猴紝铻虹汗绌哄崟鏈€閲?..
                """
        else:
            # === 缁煎悎鍒嗘瀽妯″紡锛氬畬鏁存姤鍛?===
            enhanced_query = f"{state['user_query']} {symbol} {trend}"
            kb_context = "鏆傛棤鍐呴儴鐭ヨ瘑搴撳尮閰嶅唴瀹?
            try:
                # 浣跨敤澧炲己鍚庣殑 query 鍘绘悳
                kb_context = search_investment_knowledge.invoke(enhanced_query)
            except Exception as e:
                print(f"CIO鐭ヨ瘑搴撴绱㈠け璐? {e}")
            # 馃敟 [鏂板] 鏋勫缓鎸佷粨涓婁笅鏂囨彁绀?
            portfolio_context_prompt = ""
            if portfolio_corr_index and portfolio_corr_value:
                portfolio_context_prompt = f"""
                銆愬鎴锋寔浠撳叧閿俊鎭€戯細
                - 鎸佷粨缁勫悎涓巤portfolio_corr_index}鎸囨暟鐩稿叧搴︽渶楂橈紙鐩稿叧绯绘暟{portfolio_corr_value}锛?
                - 鎸佷粨椋庨櫓锛歿portfolio_risks if portfolio_risks else "鏈彁鍙?}

                鈿狅笍 **閲嶈**锛氬鏋滃洟闃熸姤鍛婁腑鏃㈡湁銆愭寔浠撳垎鏋愩€戝張鏈夈€愭湡鏉冪瓥鐣ャ€戯紝浣犲繀椤诲湪鏁村悎鏃舵槑纭鏄庝袱鑰呯殑閫昏緫鍏宠仈锛?
                渚嬪锛?鑰冭檻鍒版偍鐨勬寔浠撲笌{portfolio_corr_index}楂樺害鐩稿叧锛岀瓥鐣ュ洟闃熷缓璁殑{symbol}鏈熸潈绛栫暐鍙互浣滀负瀵瑰啿/澧炲己宸ュ叿..."
                """

            cio_prompt = f"""
                浣犳槸杩欏浜ゆ槗鍏徃鐨?*棣栧腑鎶曡祫瀹?(CIO)**銆?
                浣犵殑鍥㈤槦锛堝垎鏋愬笀銆佺瓥鐣ュ憳銆佺洃鎺у憳绛夛級鎻愪氦浜嗗浠藉垎鏁ｇ殑鎶ュ憡銆?
                銆愬綋鍓嶆棩鏈熴€戯細{today_str}
                銆愮敤鎴烽棶棰樸€戯細{user_query}
                銆愬垎鏋愭爣鐨勩€? {display_name}
                銆愬鎴烽闄╁亸濂姐€戯細{risk_pref}锛堣鍦ㄣ€愪氦鏄撶瓥鐣ラ儴缃层€戝拰鎿嶄綔寤鸿涓紝鏍规嵁姝ら闄╁亸濂借皟鏁村缓璁殑婵€杩涚▼搴︼級

                銆愬洟闃熸姤鍛婃睜锛屽繀椤讳紭鍏堥噰鐢紒銆戯細
                {context_text}

                銆愬鎴峰璇濆巻鍙茶蹇嗐€憑mem_context}

                {portfolio_context_prompt}

                銆愷煋?鍐呴儴鐭ヨ瘑搴?(鍩轰簬"{enhanced_query}"妫€绱?銆戯細
                {kb_context}

                銆愪换鍔°€戯細
                璇峰皢涓婅堪闆舵暎鎶ュ憡鏁村悎鎴愪竴浠姐€婃繁搴︽姇璧勫喅绛栦功銆嬶紝瑕佹眰**鎺掔増绮剧編銆侀€昏緫缁撴瀯鍖?*銆?
                1. 鎶€鏈潰鍒嗘瀽浠绾夸负涓伙紝鍧囩嚎涓鸿緟銆傚鏋滄病鏈夋暟鎹紝鎶€鏈潰杩欏尯鍧楀氨鐪佺暐銆?
                2. 鐭ヨ瘑瑕佸弬鑰儃kb_context}锛屼絾瑕佹牴鎹綋涓嬪競鍦烘儏鍐碉紝鑷繁鐞嗚В鍚庤緭鍑恒€?
                3. 濡傛灉璁板繂{mem_context}鏈夊鎴风殑鎸佷粨鎴栧亸濂斤紝鍦ㄦ姤鍛婇噷鍙互閽堝鎬х殑鍐欍€?
                4. 鎵€鏈変环鏍兼暟鎹紙褰撳墠浠枫€佹敮鎾戜綅銆佸帇鍔涗綅銆佸潎绾垮€硷級锛屽繀椤讳娇鐢ㄦ潵鑷€愬洟闃熸姤鍛婃睜銆戯紒
                5. **銆愬叧閿€?*锛氬鏋滄姤鍛婃睜涓寘鍚寔浠撳垎鏋愬拰绛栫暐寤鸿锛屽繀椤诲湪鏁村悎鏃惰В閲婃竻妤氱瓥鐣ュ浣曟湇鍔′簬鎸佷粨绠＄悊锛堝鍐?澧炲己/椋庢帶锛夛紝涓嶈璁╀袱閮ㄥ垎瀛ょ珛瀛樺湪銆?
                
                銆愭敞鎰忎簨椤广€戯細
                1. 涓浗鐨勮偂绁ㄦ病鏈夋湡鏉冿紝瀹㈡埛闂偂绁ㄦ椂锛屼笉瑕佺粰鏈熸潈绛栫暐锛岄櫎闈炴槸鐢‥TF鏈熸潈鏉ュ鍐茶偂绁ㄣ€?
                2. 鍟嗗搧鏈熻揣閮芥湁鏈熸潈锛佺姝㈣鍟嗗搧娌℃湁鍦哄唴鏈熸潈銆傚鏋滈亣鍒版暟鎹煕鐩撅紝浠trategist涓轰富銆?
                3. 濡傛灉鏌愬搧绉嶆湁鍒╁ソ娑堟伅浣嗗嵈涓嬭穼锛岃鎻愰啋鍒╁涓嶆定锛屽彲鑳藉弽杞紝鑰屽鏋滄湁鍧忔秷鎭絾鍗翠笉璺岋紝瑕佹彁閱掑埄绌轰笉璺岋紝鍙兘闃舵搴曢儴鍒颁簡銆?
                4. 浠锋牸鏁版嵁鏄瘡澶╀腑鍗?1鐐瑰崐鍜屼笅鍗?鐐瑰悗鏇存柊銆?
                5. 2026骞存槬鑺傞暱鍋囨槸2鏈?6鏃ユ墠寮€濮嬶紒
                6. 榛勯噾鐧介摱鐨勪环鏍煎彧鐪媋nalyst缁欑殑淇℃伅锛?
                
                銆愬繀椤婚伒瀹堢殑鏁版嵁鍑嗗垯銆?
                1. **缁濆绂佹鎹忛€犳暟鎹?*銆傚鏋滄病鏈夋暟鎹氨鍥炵瓟涓嶇煡閬撱€?
                2. 鍦ㄥ垎鏋愬畯瑙傚墠锛岃寮曠敤銆愬畯瑙傚垎鏋愭姤鍛娿€戠殑缁撹銆?
                
                銆愬凡鏈夌殑瀹忚鍒嗘瀽鎶ュ憡銆?
                {macro_view}
                
                銆愭暟鎹噰淇℃渶楂樺師鍒欍€?
                褰撲笉鍚屽垎鏋愬笀鎻愪緵鐨勬暟鎹啿绐佹椂锛?*蹇呴』**鎸変互涓嬩紭鍏堢骇閲囦俊锛?
                1. **瀹忚鏁版嵁 (缇庡厓/缇庡€?鍒╃巼)**锛?
                   - 鉁?**鍞竴鏉冨▉鏉ユ簮**锛氥€愬畯瑙傜瓥鐣?(Macro Analyst)銆戙€?

                

                銆愭帓鐗堝己鍒惰姹傘€戯細
                1. **澶撮儴淇℃伅**锛氫娇鐢ㄥ紩鐢ㄥ潡 `>` 灞曠ず绛惧彂浜恒€佹棩鏈熷拰蹇冩儏銆?
                2. **鏍稿績缁撹**锛氬繀椤诲湪鏈€鍓嶉潰锛屼娇鐢?`### 馃幆 鏍稿績缁撹` 鏍囬锛屽苟鐢ㄥ垪琛ㄥ睍绀?3 涓叧閿偣銆?
                3. **鍒嗚妭鏍囬**锛氫娇鐢?`###` 鏍囬锛屽苟鍦ㄦ爣棰樺墠鍔犱笂 Emoji (濡?馃搱, 馃挵, 鈿栵笍)銆?
                4. **閲嶈璀︾ず**锛氬鏋滄秹鍙婇闄╋紝浣跨敤 `> 鈿狅笍 **椋庨櫓鎻愮ず**锛?..` 鐨勬牸寮忛珮浜€?
                5. **鏁版嵁琛ㄦ牸**锛氬鏋滄秹鍙婂缁勬暟鎹姣旓紙濡傛敮鎾戝帇鍔涗綅銆佽祫閲戞祦锛夛紝灏介噺鏁寸悊鎴?Markdown 琛ㄦ牸銆?
                6. **璇皵**锛氫笓涓氥€佽嚜淇°€佸共缁冦€備笉瑕佸爢鐮屽簾璇濄€?
                7. 鎶ュ憡閲屼笉瑕佸弬鑰僊ACD鎸囨爣锛?
                8. 寮曠敤鐭ヨ瘑搴撳唴瀹规椂锛屼笉瑕佹妸鏂囩珷鏍囬鍐欏嚭鏉ャ€?

                銆愭姤鍛婄粨鏋勬ā鏉裤€戯細
                > 馃搮 鏃ユ湡锛歿today_str}
                > 鉁嶏笍 绛惧彂锛氫氦鏄撴眹棣栧腑

                ### 馃幆 鏍稿績缁撹 (Executive Summary)
                * ...
                * ...

                ### 馃搱 甯傚満娣卞害瑙ｆ瀽
                (铻嶅悎鎶€鏈潰鍜岃祫閲戦潰...)

                ### 鈿栵笍 浜ゆ槗绛栫暐閮ㄧ讲
                (鍏蜂綋鐨勬湡鏉冩垨鐜拌揣鎿嶄綔寤鸿...)

                ### 馃洝锔?椋庢帶涓庡鍐?
                (姝㈡崯浣嶃€侀闄╂彁绀?..)
                """

        final_verdict = llm.invoke(cio_prompt)

        # 馃敟 [鏂板] 浠庡師濮嬫姤鍛婁腑鎻愬彇鍥捐〃璺緞锛堝洜涓?finalizer 鏄暣鐞嗚€咃紝鍥捐〃鍦ㄥ墠闈㈣妭鐐圭敓鎴愶級
        chart_img = state.get("chart_img", "")
        if state.get("macro_chart"):  # 濡傛灉鏈夊畯瑙傚浘锛屼紭鍏堢敤瀹忚鍥?
            chart_img = state.get("macro_chart")

        # 濡傛灉 state 涓病鏈夛紝灏濊瘯浠庢姤鍛婂唴瀹逛腑鎻愬彇
        if not chart_img:
            chart_match = re.search(r'IMAGE_CREATED:(chart_[a-zA-Z0-9_]+\.json)', context_text)
            if chart_match:
                chart_img = chart_match.group(1)
                print(f"馃搳 finalizer 浠庢姤鍛婁腑鎻愬彇鍒板浘琛? {chart_img}")

        return {
            "messages": [HumanMessage(content=f"銆愭渶缁堝喅绛栥€慭n{final_verdict.content}")],
            "chart_img": chart_img  # 馃敟 杩斿洖鍥捐〃璺緞
        }


# ==========================================
# 馃搳 鎸佷粨鍒嗘瀽甯?(Portfolio Analyst)
# ==========================================
def portfolio_analyst_node(state: AgentState, llm):
    """
    鎸佷粨鍒嗘瀽涓撳锛氬垎鏋愮敤鎴锋寔浠撶粨鏋勩€侀闄╃壒寰佸拰浜ゆ槗椋庢牸
    """
    query = state["user_query"]
    user_id = state.get("user_id", "")
    has_portfolio = state.get("has_portfolio", False)
    current_date = datetime.now().strftime("%Y骞?m鏈?d鏃?%A")

    # 濡傛灉鐢ㄦ埛娌℃湁鎸佷粨鏁版嵁锛岀洿鎺ヨ繑鍥?
    if not has_portfolio or not user_id:
        return {
            "messages": [HumanMessage(content="銆愭寔浠撳垎鏋愩€戠敤鎴锋殏鏃犳寔浠撴暟鎹紝鏃犳硶鎻愪緵鎸佷粨鐩稿叧鍒嗘瀽銆?)],
            "portfolio_summary": "",
            "portfolio_risks": "",
            "trading_style": "",
            "portfolio_top_corr_index": "",
            "portfolio_top_corr_value": ""
        }

    # 閰嶇疆宸ュ叿
    tools = [
        get_user_portfolio_summary,
        get_user_portfolio_details,
        analyze_user_trading_style,
        check_portfolio_risks
    ]

    persona_prompt = f"""
    浣犳槸涓€浣嶄笓涓氱殑鎸佷粨鍒嗘瀽甯堬紝涓撴敞浜庯細
    1. 鍒嗘瀽鐢ㄦ埛褰撳墠鎸佷粨缁撴瀯鍜岄闄╃壒寰?
    2. 璇勪及鎸佷粨涓庡競鍦虹殑鐩稿叧搴?
    3. 璇嗗埆鐢ㄦ埛鐨勪氦鏄撻鏍煎拰鍋忓ソ
    4. 缁撳悎鐢ㄦ埛瀹為檯鎸佷粨缁欏嚭涓€у寲寤鸿

    銆愬綋鍓嶆棩鏈熴€戯細{current_date}
    銆愮敤鎴稩D銆戯細{user_id}
    銆愬鎴烽渶姹傘€戯細{query}

    銆愬彲璋冪敤宸ュ叿銆?
    1. get_user_portfolio_summary - 鑾峰彇鎸佷粨鎽樿锛堣交閲忕骇锛?
    2. get_user_portfolio_details - 鑾峰彇鎸佷粨璇︽儏锛堝畬鏁存暟鎹級
    3. analyze_user_trading_style - 鍒嗘瀽浜ゆ槗椋庢牸
    4. check_portfolio_risks - 妫€鏌ユ寔浠撻闄?

    銆愪换鍔°€戯細
    1. 棣栧厛璋冪敤 get_user_portfolio_summary 浜嗚В鐢ㄦ埛鎸佷粨姒傚喌
    2. 鏍规嵁鏌ヨ闇€姹傦紝閫夋嫨鎬ц皟鐢ㄥ叾浠栧伐鍏疯幏鍙栬缁嗕俊鎭?
    3. 鍒嗘瀽鐢ㄦ埛鎸佷粨鐗圭偣銆侀闄╃偣鍜屾敼杩涘缓璁?
    4. 濡傛灉鐢ㄦ埛鏌ヨ娑夊強鐗瑰畾鏍囩殑锛屽垎鏋愯鏍囩殑鍦ㄧ敤鎴风粍鍚堜腑鐨勫崰姣斿拰浣滅敤
    5. 鎻愪緵涓撲笟銆佸瑙傜殑鍒嗘瀽锛岄伩鍏嶈繃搴︿箰瑙傛垨鎮茶
    6. **閲嶈**锛氬鏋滃伐鍏疯繑鍥炰簡portfolio_corr锛堢粍鍚堢浉鍏冲害锛夛紝蹇呴』鏄庣‘鎸囧嚭"鎮ㄧ殑鎸佷粨涓嶺X鎸囨暟鐩稿叧搴︽渶楂橈紝杈惧埌X.XX"

    銆愯緭鍑烘牸寮忚姹傘€戯細
    - 濡傛灉娑夊強鎸囨暟鐩稿叧鎬э紝鐢ㄨ繖鏍风殑鏍煎紡锛氥€愭寚鏁扮浉鍏虫€с€戞偍鐨勬寔浠撶粍鍚堜笌XX鎸囨暟鐩稿叧搴︽渶楂橈紝鐩稿叧绯绘暟涓篨.XX
    - 椋庨櫓鎻愮ず鐢細銆愰闄╂彁绀恒€憍xx
    - 浜ゆ槗椋庢牸鐢細銆愪氦鏄撻鏍笺€憍xx

    銆愭敞鎰忋€戯細
    - 鏁版嵁鏉ヨ嚜鐢ㄦ埛涓婁紶鐨勬寔浠撴埅鍥惧垎鏋愮粨鏋?
    - 濡傛灉鎸佷粨鏁版嵁杈冩棫锛堣秴杩?澶╋級锛屾彁閱掔敤鎴锋洿鏂?
    - 椋庨櫓鎻愮ず瑕佹槑纭叿浣擄紝閬垮厤妯＄硦琛ㄨ堪
    """

    portfolio_agent = create_react_agent(llm, tools, prompt=persona_prompt)

    try:
        result = portfolio_agent.invoke(
            {"messages": state["messages"]},
            {"recursion_limit": 20}
        )

        last_response = result["messages"][-1].content

        # 鎻愬彇鍏抽敭淇℃伅锛堢敤浜庡叾浠栬妭鐐瑰弬鑰冿級
        portfolio_summary = ""
        portfolio_risks = ""
        trading_style = ""
        portfolio_top_corr_index = ""
        portfolio_top_corr_value = ""

        # 绠€鍗曟彁鍙栵紙鍙互鏇存櫤鑳斤級
        if "鎬诲競鍊? in last_response or "鎸佷粨" in last_response:
            portfolio_summary = last_response[:200]  # 鍓?00瀛椾綔涓烘憳瑕?

        if "椋庨櫓" in last_response:
            risk_match = re.search(r'銆愰闄╂彁绀恒€?.*?)(?:銆恷$)', last_response, re.DOTALL)
            if risk_match:
                portfolio_risks = risk_match.group(1).strip()[:150]

        if "椋庢牸" in last_response or "鍋忓ソ" in last_response:
            style_match = re.search(r'(绋冲仴鍨媩婵€杩涘瀷|骞宠　鍨媩淇濆畧鍨?', last_response)
            if style_match:
                trading_style = style_match.group(1)

        # 馃敟 鎻愬彇鎸囨暟鐩稿叧鎬т俊鎭紙鐢ㄤ簬绛栫暐鎺ㄨ崘锛?
        if "鎸囨暟鐩稿叧鎬? in last_response or "鐩稿叧绯绘暟" in last_response:
            corr_match = re.search(r'銆愭寚鏁扮浉鍏虫€с€?*?涓?.+?)鎸囨暟.*?鐩稿叧绯绘暟涓?([\d\.]+)', last_response)
            if corr_match:
                portfolio_top_corr_index = corr_match.group(1).strip()
                portfolio_top_corr_value = corr_match.group(2).strip()
                print(f"鉁?鎻愬彇鍒版寚鏁扮浉鍏虫€? {portfolio_top_corr_index} = {portfolio_top_corr_value}")

        return {
            "messages": [HumanMessage(content=f"銆愭寔浠撳垎鏋愩€慭n{last_response}")],
            "portfolio_summary": portfolio_summary,
            "portfolio_risks": portfolio_risks,
            "trading_style": trading_style,
            "portfolio_top_corr_index": portfolio_top_corr_index,
            "portfolio_top_corr_value": portfolio_top_corr_value
        }

    except Exception as e:
        print(f"Portfolio Analyst Node Error: {e}")
        return {
            "messages": [HumanMessage(content=f"銆愭寔浠撳垎鏋愩€戝垎鏋愬彈闃? {e}")],
            "portfolio_summary": "",
            "portfolio_risks": "",
            "trading_style": "",
            "portfolio_top_corr_index": "",
            "portfolio_top_corr_value": ""
        }


# ==========================================
# 4. 鏋勫缓鍥?(The Graph)
# ==========================================

def build_trading_graph(fast_llm, mid_llm, smart_llm):
    """
    鏋勫缓骞剁紪璇?LangGraph
    """
    workflow = StateGraph(AgentState)

    # 1. 娉ㄥ唽鑺傜偣
    # 涓荤 -> 鐢?Turbo (蹇?
    workflow.add_node("supervisor", lambda state: supervisor_node(state, fast_llm))
    # 鍒嗘瀽甯?-> 鐢?Plus (鍧囪　)
    workflow.add_node("analyst", lambda state: analyst_node(state, mid_llm))
    # 绛栫暐鍛?-> 鐢?Max (鑱槑)
    workflow.add_node("strategist", lambda state: strategist_node(state, smart_llm))
    # 鐜嬬墝 -> 鐢?Max (鑱槑)
    workflow.add_node("generalist", lambda state: generalist_node(state, smart_llm))
    # CIO -> 鐢?Max (鑱槑)
    workflow.add_node("finalizer", lambda state: finalizer_node(state, mid_llm))
    # 鍏朵粬宸ュ叿浜?(涓嶉渶瑕?LLM锛屾垨鑰呴殢渚跨粰涓€涓?
    workflow.add_node("monitor", lambda state: monitor_node(state, mid_llm))
    workflow.add_node("researcher", lambda state: researcher_node(state, mid_llm))
    workflow.add_node("chatter", lambda state: chatter_node(state, mid_llm))
    workflow.add_node("screener", lambda state: screener_node(state, mid_llm))
    workflow.add_node("roaster", lambda state: roaster_node(state, mid_llm))
    workflow.add_node("macro_analyst", lambda state: macro_analyst_node(state, mid_llm))
    # 鎸佷粨鍒嗘瀽甯?-> 鐢?Plus (鍧囪　)
    workflow.add_node("portfolio_analyst", lambda state: portfolio_analyst_node(state, mid_llm))

    # 2. 璁剧疆鍏ュ彛
    workflow.set_entry_point("supervisor")

    # 3. 瀹氫箟鍔ㄦ€佽矾鐢遍€昏緫 (The Router)
    def route_next_step(state: AgentState):
        plan = state.get("plan", [])
        if not plan:
            return "finalizer"

        # 鍙栧嚭璁″垝涓殑涓嬩竴涓紝骞朵粠璁″垝涓Щ闄?
        next_node = plan[0]
        # 鏇存柊 plan (杩欎竴姝ュ叾瀹炴瘮杈?trick锛孡angGraph 鐨?state 鏄?immutable 鐨勬洿鏂版祦)
        # 鎴戜滑闇€瑕佸湪鑺傜偣鍐呴儴鏇存柊 plan锛屾垨鑰呭湪杩欓噷鍙仛璺敱
        # 绠€鍖栧仛娉曪細鎴戜滑璁╂瘡涓妭鐐硅窇瀹屽悗锛岃嚜宸卞幓妫€鏌?plan 骞惰矾鐢?
        return next_node

    # 鈿狅笍 LangGraph 鐨勬爣鍑嗗仛娉曟槸锛氭瘡涓妭鐐硅窇瀹岋紝杩斿洖鏇存柊鍚庣殑 State
    # 涓轰簡瀹炵幇"娴佹按绾?锛屾垜浠渶瑕佷竴涓腑闂翠汉鎴栬€呰姣忎釜鑺傜偣閮芥寚鍚?"scheduler"
    # 杩欓噷鎴戜滑閲囩敤 "Scheduler" 妯″紡锛屽嵆 Supervisor -> Scheduler -> Node -> Scheduler...

    # 浣嗕负浜嗙畝鍗曪紝鎴戜滑閲囩敤 add_conditional_edges 浠?supervisor 鐩存帴鍒嗗彂鏄仛涓嶅埌涓茶鐨?
    # 鎴戜滑闇€瑕佸紩鍏ヤ竴涓悕涓?"orchestrator" 鐨勯殣钘忚妭鐐癸紝鎴栬€呬慨鏀?supervisor 閫昏緫

    # --- 淇鍚庣殑涓茶閫昏緫 ---
    # 鎴戜滑鎶?plan 鐨勬墽琛岄€昏緫鏀惧湪 edge 閲?

    def executor_router(state: AgentState):
        plan = state.get("plan", [])
        if not plan:
            return "end"  # 杩欓噷鐨?end 鎸囧悜 finalizer
        return plan[0]  # 杩斿洖 list 涓殑绗竴涓厓绱犱綔涓鸿妭鐐瑰悕

    # 瀹氫箟姣忎釜 Worker 鎵ц瀹屽悗锛岄兘瑕佸洖鍒?Router (鎴栬€呭湪杩欓噷灏辨槸鐩存帴鍘讳笅涓€涓?
    # 涓轰簡瀹炵幇 state['plan'].pop(0)锛屾垜浠渶瑕佸湪姣忎釜 worker 閲屽鐞?plan
    # 杩欎細寰堢箒鐞愩€?

    # 馃敟 鏈€浣冲疄璺碉細浣跨敤涓€涓?"Manager" 鑺傜偣鏉ュ惊鐜?
    workflow.add_node("manager", lambda state: {"current_step": "managing"})

    workflow.add_edge("supervisor", "manager")

    def manager_router(state: AgentState):
        plan = state.get("plan", [])
        if not plan:
            return "finalizer"

        # 鑾峰彇涓嬩竴涓换鍔?
        next_task = plan[0]

        # 杩欓噷鐨勫叧閿槸锛氭垜浠渶瑕佸湪璺敱鐨勫悓鏃讹紝鎶婅繖涓换鍔′粠 plan 閲屽垹鎺?
        # 浣?router 鍑芥暟涓嶈兘淇敼 state銆?
        # 鎵€浠ュ繀椤诲湪 worker 鑺傜偣閲屼慨鏀?plan锛屾垨鑰呮湁涓€涓笓闂ㄧ殑 step 鑺傜偣銆?

        return next_task

    # 4. 杩炴帴 Edge
    # Manager 鍐冲畾鍘诲摢
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
            "portfolio_analyst": "portfolio_analyst",
            "finalizer": "finalizer"
        }
    )

    # 5. Worker 鍥炴祦閫昏緫
    # 姣忎釜 Worker 璺戝畬锛屽繀椤绘妸鑷繁鐨勫悕瀛椾粠 plan 閲岄€氳繃浠ｇ爜鍒犳帀 (pop)锛岀劧鍚庡洖鍒?manager
    def worker_complete(state):
        new_plan = state["plan"][1:]  # 绉婚櫎宸插畬鎴愮殑绗竴涓?
        return {"plan": new_plan}

    # 鎴戜滑闇€瑕佸寘瑁呬竴涓?worker 鑺傜偣锛岃瀹冧滑鑳芥洿鏂?plan
    # 浣嗕笂闈㈠畾涔?worker 鏃跺凡缁忓啓姝讳簡銆?
    # 绠€渚胯捣瑙侊紝鎴戜滑鍦?add_edge 鏃舵寚瀹氾細
    # Analyst -> Manager (浣嗗湪杩涘叆 Manager 鍓嶏紝State 宸茬粡琚?Analyst 鏇存柊浜嗗悧锛熸槸鐨?
    # 闂鏄?Analyst 浠ｇ爜閲屾病鏈?pop plan銆?

    # 瑙ｅ喅鏂规锛氫慨鏀规墍鏈?Worker 鑺傜偣锛屾垨鑰呭鍔犱竴涓€氱敤鐨勫悗澶勭悊鑺傜偣銆?
    # 鎴戜滑淇敼涓婇潰鐨?Worker 瀹氫箟澶夯鐑︼紝涓嶅鍦?edge 閫昏緫閲屽仛锛熶笉鏀寔銆?

    # 馃憠 鏈€缁堟柟妗堬細璁?Manager 鑺傜偣璐熻矗 POP Plan
    # 淇敼 Manager Node 閫昏緫锛?
    workflow.add_node("manager_pop", lambda state: {"plan": state["plan"][1:]})

    # 娴佺▼鍙樻垚锛歁anager(璺敱) -> Worker -> Manager_Pop(鍒犻櫎浠诲姟) -> Manager(璺敱)

    # 閲嶆柊瀹氫箟 Edge:
    for node_name in ["analyst", "monitor", "strategist", "researcher", "generalist","screener","roaster", "macro_analyst", "portfolio_analyst"]:
        workflow.add_edge(node_name, "manager_pop")

    workflow.add_edge("chatter", END)
    workflow.add_edge("manager_pop", "manager")

    workflow.add_edge("finalizer", END)

    return workflow.compile()

