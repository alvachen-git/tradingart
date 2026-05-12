import re
from typing import Final

from chat_context_utils import infer_correction_intent, infer_followup_goal


CHAT_MODE_SIMPLE: Final[str] = "simple_chat"
CHAT_MODE_KNOWLEDGE: Final[str] = "knowledge_chat"
CHAT_MODE_ANALYSIS: Final[str] = "analysis_chat"

FOLLOWUP_HINTS = (
    "继续", "展开", "详细说", "详细讲", "再说", "那为什么", "为什么呢",
    "然后呢", "接下来", "具体点", "说说看", "讲讲", "进一步", "补充一下",
    "查一下", "帮我查", "具体原因", "到底为什么", "详细原因", "那具体呢",
)

COMPANY_ENTITY_SUFFIXES = (
    "股份", "集团", "科技", "技术", "控股", "电子", "电气", "机械", "汽车", "能源",
    "药业", "银行", "证券", "制造", "动力", "材料", "智能", "软件", "通信", "航空",
    "医药", "生物", "实业", "新材",
)

COMPANY_ENTITY_PATTERN = re.compile(
    r"[一-龥]{2,10}(?:%s)" % "|".join(COMPANY_ENTITY_SUFFIXES)
)

COMPANY_NEWS_KEYWORDS = (
    "最近有什么好消息", "最近有没有好消息", "最近有什么动态", "最近动态", "最近进展",
    "最近催化", "最近有没有催化", "最近有什么消息", "最近消息", "最新消息", "近期动态",
    "近期进展", "近期催化", "最近公告", "最近怎么样", "业务最近怎么样", "业务最近如何",
)

COMPANY_ANALYSIS_KEYWORDS = (
    "估值", "基本面", "值不值得买", "值得买吗", "能不能买", "能买吗", "对股价影响", "影响股价",
    "怎么看", "会不会涨", "算不算利好", "算不算催化", "股价影响", "业绩弹性", "景气度",
)

COMPANY_ASPECT_KEYWORDS = (
    "机器人业务", "汽车业务", "机器人", "汽车", "工业自动化", "工业软件", "协作机器人",
    "服务机器人", "工业机器人", "业务线", "这块业务", "这个业务",
)

SIMPLE_CHAT_KEYWORDS = (
    "你好", "您好", "嗨", "hello", "hi", "hey", "哈喽", "早安", "早上好", "下午好", "晚上好",
    "谢谢", "感谢", "thank you", "thanks", "你是谁", "你叫什么", "你是干嘛的", "陪我聊聊",
    "聊聊天", "鼓励我", "安慰我", "有点慌", "有点焦虑", "有点难受", "给我打打气",
)

KNOWLEDGE_PREFIXES = (
    "什么是", "解释", "解释一下", "科普一下", "说说", "讲讲", "区别", "原理", "举例",
    "通俗说", "什么意思", "是什么", "怎么理解",
)

CONVERSATIONAL_KNOWLEDGE_PREFIXES = (
    "你知道", "你了解", "听过", "有没有听过", "知道", "了解",
)

ANALYSIS_INTENT_KEYWORDS = (
    "建议", "怎么做", "怎么办", "怎么看", "能买吗", "能不能买", "分析", "复盘",
    "行情", "走势", "涨跌", "对比", "比较", "宏观", "仓位", "持仓", "调仓", "加仓", "减仓",
    "对冲", "买入", "卖出", "开仓", "平仓", "新闻", "影响", "利好", "利空",
    "基本面", "交易什么", "先交易", "先反应", "怎么传导", "如何传导",
    "适合做吗", "适合吗", "现在适合",
)

PORTFOLIO_META_QUERY_KEYWORDS = (
    "你记得我持仓吗",
    "你记得我的持仓吗",
    "你有我的持仓吗",
    "我上传过持仓吗",
    "你知道我现在持仓吗",
    "你知道我的持仓吗",
    "你记住我持仓了吗",
)

PORTFOLIO_META_QUERY_EXCLUDED_KEYWORDS = (
    "分析", "判断", "建议", "风险大吗", "调仓", "加仓", "减仓", "怎么做", "怎么办",
)

TECHNICAL_KNOWLEDGE_KEYWORDS = (
    "k线", "均线", "图表", "技术面", "技术分析", "真假突破", "假突破", "假跌破",
    "支撑位", "阻力位", "成交量", "回踩", "趋势线", "多头陷阱", "空头陷阱",
    "止损", "止盈", "仓位管理", "突破四原则",
)

MARKET_SUBJECT_KEYWORDS = (
    "策略", "期权", "认购", "认沽", "iv", "delta", "gamma", "vega", "theta",
    "牛市价差", "熊市价差", "跨式", "宽跨", "勒式", "美联储", "降息", "黄金",
    "白银", "创业板", "etf", "股票", "指数", "铜价", "原油", "波动率",
) + TECHNICAL_KNOWLEDGE_KEYWORDS

OPTION_DATA_SUBJECT_KEYWORDS = (
    "期权", "认购", "认沽", "波动率", "隐含波动率", "iv", "iv rank", "ivrank",
    "delta", "gamma", "vega", "theta", "行权价", "到期日", "到期", "剩余天数",
    "保证金", "合约乘数", "乘数", "一手多少钱", "资金占用", "持仓量", "成交量",
)

OPTION_DATA_INTENT_KEYWORDS = (
    "多少", "多高", "高吗", "低吗", "大吗", "小吗", "几天", "多久", "什么水平", "分位",
    "rank", "几档", "多不多", "够不够", "贵吗", "便宜吗",
)

OPTION_DATA_EXCLUDED_KEYWORDS = (
    "策略", "建议", "怎么做", "怎么办", "怎么看", "适合", "能买吗", "能不能买", "买入", "卖出",
    "开仓", "平仓", "移仓", "调仓", "对冲", "行情", "走势", "技术面", "基本面", "宏观", "新闻",
    "影响", "利好", "利空", "如何处理",
)

MARKET_DATA_SUBJECT_KEYWORDS = (
    "iv", "隐含波动率", "波动率", "iv rank", "ivrank", "价格", "现价", "最新价", "报价", "收盘",
    "开盘", "昨收", "昨结", "结算价", "成交量", "持仓量", "持仓", "到期", "剩余天数", "到期日",
    "保证金", "合约乘数", "乘数", "一手", "行权价", "权利金",
)

MARKET_DATA_INTENT_KEYWORDS = (
    "查看", "查", "查下", "查一下", "看", "看下", "看一下", "给我", "多少", "多高", "高吗", "低吗",
    "几天", "多久", "多少点", "多少钱", "是什么水平", "分位", "rank", "报价", "数值",
)

MARKET_DATA_EXCLUDED_KEYWORDS = OPTION_DATA_EXCLUDED_KEYWORDS + (
    "解释", "什么是", "什么意思", "原理", "为什么", "原因", "意味着", "举例", "护城河", "竞争对手",
    "隐忧", "值不值得", "该怎么做",
)

FINANCE_BASE_KEYWORDS = (
    "金融", "交易", "投资", "理财", "基金", "债券", "国债", "利率", "通胀", "cpi", "pmi",
    "非农", "美债", "美元", "汇率", "外汇", "a股", "港股", "美股", "期货", "现货",
    "商品", "贵金属", "券商", "财报", "估值", "资产配置",
    "科创50", "上证50", "沪深300", "中证500", "创业板指", "科创板", "纳斯达克100",
)

FINANCE_DOMAIN_KEYWORDS = FINANCE_BASE_KEYWORDS + ANALYSIS_INTENT_KEYWORDS + MARKET_SUBJECT_KEYWORDS

RECENT_FINANCE_CONTEXT_HINTS = FINANCE_BASE_KEYWORDS + MARKET_SUBJECT_KEYWORDS

RECENT_ANALYSIS_HINTS = (
    "怎么看", "怎么做", "怎么办", "能不能买", "能买吗", "策略", "建议", "买入", "卖出",
    "仓位", "调仓", "加仓", "减仓", "对冲", "开仓", "平仓", "技术面", "基本面",
    "比较", "对比", "先交易", "交易什么", "先反应", "怎么传导", "如何传导",
    "适合做吗", "适合吗", "现在适合", "涨这么多", "跌这么多", "异动原因", "上涨原因", "下跌原因",
)
CORRECTION_ANALYSIS_HINTS = (
    "护城河", "隐忧", "竞争对手", "竞争格局", "竞争", "逻辑", "影响", "估值", "比较", "对比",
    "相关度", "相关系数", "权重", "占比", "比例", "数值", "数据", "统计", "指标", "判断",
    "为什么", "意味着", "怎么做", "适合做吗", "值不值得", "哪个更强",
)
CORRECTION_FACT_HINTS = (
    "公司名", "名称", "名字", "全称", "简称", "实体", "有这家公司", "有这个公司", "不是中微公司",
    "定义", "意思", "是什么", "来源", "出处", "日期", "年份", "时间", "公告", "名单", "板块",
)
RUNTIME_FACT_HINTS = ("时间", "几点", "几号", "日期", "星期", "周几")

SYMBOL_PATTERN = re.compile(r"\b[A-Z]{1,5}\d{0,4}\b|(?<!\d)\d{6}(?!\d)")
CONTRACT_PATTERN = re.compile(r"[一-龥]{2,8}\d{3,4}|[A-Za-z]{1,4}\d{3,4}")
URL_PATTERN = re.compile(r"https?://|www\.", re.IGNORECASE)


def infer_followup(prompt_text: str) -> bool:
    text = str(prompt_text or "").strip().lower()
    if not text:
        return False
    return any(hint in text for hint in FOLLOWUP_HINTS)


def has_url_like_text(prompt_text: str) -> bool:
    return bool(URL_PATTERN.search(str(prompt_text or "")))


def is_finance_or_trading_domain(prompt_text: str, *, has_symbol: bool = False) -> bool:
    text = str(prompt_text or "").strip().lower()
    if not text:
        return bool(has_symbol)
    return bool(has_symbol) or any(keyword in text for keyword in FINANCE_DOMAIN_KEYWORDS)


def _recent_context_suggests_analysis(recent_context: str) -> bool:
    text = str(recent_context or "").strip().lower()
    if not text:
        return False
    return any(keyword in text for keyword in RECENT_ANALYSIS_HINTS)


def _recent_context_suggests_knowledge(recent_context: str) -> bool:
    text = str(recent_context or "").strip().lower()
    if not text:
        return False
    return any(keyword in text for keyword in KNOWLEDGE_PREFIXES) or any(
        phrase in text for phrase in ("什么意思", "是什么", "原理", "举例", "科普", "定义", "区别")
    )


def _recent_context_has_finance_subject(recent_context: str) -> bool:
    text = str(recent_context or "").strip().lower()
    if not text:
        return False
    return any(keyword in text for keyword in RECENT_FINANCE_CONTEXT_HINTS)


def _looks_like_runtime_fact_correction(prompt_text: str, recent_context: str) -> bool:
    text = str(prompt_text or "").strip().lower()
    recent = str(recent_context or "").strip().lower()
    if not text or not recent:
        return False
    if not any(keyword in text for keyword in RUNTIME_FACT_HINTS):
        return False
    return any(keyword in recent for keyword in ("北京时间", "现在是", "今天是", "星期"))


def _looks_like_company_subject(prompt_text: str, *, has_symbol: bool = False, focus_entity: str = "") -> bool:
    text = str(prompt_text or "").strip()
    if not text:
        return bool(has_symbol or focus_entity)
    if has_symbol or str(focus_entity or "").strip():
        return True
    if "公司" in text or "个股" in text or "业务" in text:
        return True
    return bool(COMPANY_ENTITY_PATTERN.search(text))


def _is_company_news_query(
    prompt_text: str,
    *,
    has_symbol: bool = False,
    focus_entity: str = "",
    focus_mode_hint: str = "",
    is_followup: bool = False,
) -> bool:
    text = str(prompt_text or "").strip().lower()
    if not text:
        return False
    has_company_subject = _looks_like_company_subject(
        prompt_text,
        has_symbol=has_symbol,
        focus_entity=focus_entity,
    )
    has_news_intent = any(keyword in text for keyword in COMPANY_NEWS_KEYWORDS)
    has_aspect_reference = any(keyword in text for keyword in COMPANY_ASPECT_KEYWORDS)
    if has_company_subject and (has_news_intent or has_aspect_reference):
        return True
    if is_followup and str(focus_mode_hint or "").strip().lower() == "company_news":
        if any(keyword in text for keyword in FOLLOWUP_HINTS):
            return True
        if "业务" in text or "消息" in text or "动态" in text or "进展" in text:
            return True
    return False


def _is_company_analysis_query(
    prompt_text: str,
    *,
    has_symbol: bool = False,
    focus_entity: str = "",
    focus_mode_hint: str = "",
) -> bool:
    text = str(prompt_text or "").strip().lower()
    if not text:
        return False
    has_company_subject = _looks_like_company_subject(
        prompt_text,
        has_symbol=has_symbol,
        focus_entity=focus_entity,
    ) or str(focus_mode_hint or "").strip().lower() == "company_news"
    if not has_company_subject:
        return False
    return any(keyword in text for keyword in COMPANY_ANALYSIS_KEYWORDS)


def _is_price_move_reason_query(
    prompt_text: str,
    *,
    focus_topic: str = "",
    focus_mode_hint: str = "",
    is_followup: bool = False,
) -> bool:
    text = str(prompt_text or "").strip().lower()
    if not text:
        return False
    if ("为什么" in text or "原因" in text) and any(
        keyword in text for keyword in ("涨", "跌", "拉升", "跳水", "异动", "大涨", "大跌")
    ):
        return True
    if str(focus_mode_hint or "").strip().lower() == "price_move_reason" or str(focus_topic or "").strip() == "异动原因":
        if is_followup and any(keyword in text for keyword in FOLLOWUP_HINTS):
            return True
        if any(keyword in text for keyword in ("查", "找", "具体", "原因", "为什么")):
            return True
    return False


def is_pure_option_data_query(prompt_text: str) -> bool:
    text = str(prompt_text or "").strip().lower()
    if not text:
        return False

    has_subject = any(keyword in text for keyword in OPTION_DATA_SUBJECT_KEYWORDS)
    has_intent = any(keyword in text for keyword in OPTION_DATA_INTENT_KEYWORDS)
    has_excluded = any(keyword in text for keyword in OPTION_DATA_EXCLUDED_KEYWORDS)

    if not has_subject:
        return False
    if has_excluded:
        return False
    return has_intent


def is_market_data_query(prompt_text: str) -> bool:
    text = str(prompt_text or "").strip().lower()
    if not text:
        return False

    if is_pure_option_data_query(prompt_text):
        return True

    has_subject = any(keyword in text for keyword in MARKET_DATA_SUBJECT_KEYWORDS)
    has_intent = any(keyword in text for keyword in MARKET_DATA_INTENT_KEYWORDS)
    has_excluded = any(keyword in text for keyword in MARKET_DATA_EXCLUDED_KEYWORDS)
    has_contract = bool(CONTRACT_PATTERN.search(str(prompt_text or "")))
    has_symbol = bool(SYMBOL_PATTERN.search(str(prompt_text or "").upper()))

    if has_excluded:
        return False
    if not (has_subject or has_contract or has_symbol):
        return False
    if has_subject and has_intent:
        return True
    if has_contract and any(keyword in text for keyword in ("iv", "隐含波动率", "波动率", "价格", "现价", "最新价", "报价")):
        return True
    if has_symbol and has_subject and has_intent:
        return True
    return False


def classify_chat_mode(
    prompt_text: str,
    *,
    is_followup: bool = False,
    recent_context: str = "",
    focus_entity: str = "",
    focus_topic: str = "",
    focus_aspect: str = "",
    focus_mode_hint: str = "",
    followup_goal: str = "",
    correction_intent: bool = False,
    has_uploaded_image: bool = False,
    has_structured_payload: bool = False,
    vision_position_domain: str = "",
) -> str:
    text = str(prompt_text or "").strip()
    if not text:
        return CHAT_MODE_ANALYSIS

    text_lower = text.lower()
    if any(keyword in text for keyword in PORTFOLIO_META_QUERY_KEYWORDS) and not any(
        keyword in text for keyword in PORTFOLIO_META_QUERY_EXCLUDED_KEYWORDS
    ):
        return CHAT_MODE_SIMPLE
    recent_context_lower = str(recent_context or "").strip().lower()
    has_url = has_url_like_text(text)
    has_symbol = bool(SYMBOL_PATTERN.search(text.upper()))
    domain = str(vision_position_domain or "").strip().lower()
    has_special_input = has_uploaded_image or has_structured_payload or domain in {"stock", "option", "mixed"}

    if has_special_input or has_url:
        return CHAT_MODE_ANALYSIS

    if is_market_data_query(text):
        return CHAT_MODE_ANALYSIS

    has_knowledge_prefix = any(keyword in text_lower for keyword in KNOWLEDGE_PREFIXES)
    has_conversational_knowledge_prefix = any(
        keyword in text_lower for keyword in CONVERSATIONAL_KNOWLEDGE_PREFIXES
    )
    has_analysis_intent = any(keyword in text_lower for keyword in ANALYSIS_INTENT_KEYWORDS)
    has_market_subject = any(keyword in text_lower for keyword in MARKET_SUBJECT_KEYWORDS) or has_symbol
    has_finance_subject = is_finance_or_trading_domain(text_lower, has_symbol=has_symbol)
    recent_has_finance_subject = _recent_context_has_finance_subject(recent_context_lower)
    recent_suggests_analysis = _recent_context_suggests_analysis(recent_context_lower)
    recent_suggests_knowledge = _recent_context_suggests_knowledge(recent_context_lower)
    has_simple_keyword = any(keyword in text_lower for keyword in SIMPLE_CHAT_KEYWORDS)
    effective_followup_goal = str(followup_goal or "").strip().lower() or infer_followup_goal(
        text,
        recent_context=recent_context_lower,
        recent_focus_topic=focus_topic,
    )
    effective_correction_intent = bool(correction_intent) or infer_correction_intent(
        text,
        recent_context=recent_context_lower,
        recent_focus_topic=focus_topic,
    )
    has_company_news = _is_company_news_query(
        text,
        has_symbol=has_symbol,
        focus_entity=focus_entity,
        focus_mode_hint=focus_mode_hint,
        is_followup=is_followup,
    )
    has_company_analysis = _is_company_analysis_query(
        text,
        has_symbol=has_symbol,
        focus_entity=focus_entity,
        focus_mode_hint=focus_mode_hint,
    )
    has_price_move_reason = _is_price_move_reason_query(
        text,
        focus_topic=focus_topic,
        focus_mode_hint=focus_mode_hint,
        is_followup=is_followup,
    )
    correction_signal_text = "\n".join(
        part
        for part in (
            text_lower,
            recent_context_lower,
            str(focus_topic or "").strip().lower(),
            str(focus_aspect or "").strip().lower(),
        )
        if part
    )
    correction_prompt_text = "\n".join(
        part
        for part in (
            text_lower,
            str(focus_aspect or "").strip().lower(),
        )
        if part
    )
    correction_suggests_analysis = (
        effective_followup_goal in {"fetch_numeric", "analyze_reason", "analyze_impact"}
        or any(keyword in correction_signal_text for keyword in CORRECTION_ANALYSIS_HINTS)
    )
    correction_suggests_facts = any(keyword in correction_signal_text for keyword in CORRECTION_FACT_HINTS)
    correction_prompt_suggests_facts = any(keyword in correction_prompt_text for keyword in CORRECTION_FACT_HINTS)
    correction_prompt_suggests_analysis = any(keyword in correction_prompt_text for keyword in CORRECTION_ANALYSIS_HINTS)

    if is_followup:
        if effective_correction_intent:
            if _looks_like_runtime_fact_correction(text, recent_context_lower):
                return CHAT_MODE_SIMPLE
            if correction_prompt_suggests_facts and not correction_prompt_suggests_analysis:
                return CHAT_MODE_KNOWLEDGE
            if correction_suggests_analysis:
                return CHAT_MODE_ANALYSIS
            if (
                correction_suggests_facts
                or recent_has_finance_subject
                or has_finance_subject
                or has_market_subject
                or bool(focus_topic)
                or bool(focus_entity)
            ):
                return CHAT_MODE_KNOWLEDGE
            return CHAT_MODE_SIMPLE
        if has_price_move_reason:
            return CHAT_MODE_ANALYSIS
        if has_company_analysis:
            return CHAT_MODE_ANALYSIS
        if has_company_news:
            return CHAT_MODE_KNOWLEDGE
        if effective_followup_goal == "fetch_numeric":
            if recent_has_finance_subject or has_finance_subject or has_market_subject:
                return CHAT_MODE_ANALYSIS
            return CHAT_MODE_KNOWLEDGE
        if effective_followup_goal in {"analyze_reason", "analyze_impact"}:
            return CHAT_MODE_ANALYSIS
        if effective_followup_goal == "fetch_facts":
            return CHAT_MODE_KNOWLEDGE
        if effective_followup_goal == "explain_more":
            if recent_has_finance_subject or has_finance_subject:
                if str(focus_topic or "").strip() == "概念解释" or recent_suggests_knowledge:
                    return CHAT_MODE_KNOWLEDGE
                if recent_suggests_analysis or has_analysis_intent:
                    return CHAT_MODE_ANALYSIS
                return CHAT_MODE_KNOWLEDGE
            return CHAT_MODE_SIMPLE
        if recent_has_finance_subject or has_finance_subject:
            if has_analysis_intent:
                return CHAT_MODE_ANALYSIS
            if has_knowledge_prefix and not has_analysis_intent:
                return CHAT_MODE_KNOWLEDGE
            if recent_suggests_analysis:
                return CHAT_MODE_ANALYSIS
            if recent_suggests_knowledge or recent_has_finance_subject:
                return CHAT_MODE_KNOWLEDGE
            return CHAT_MODE_ANALYSIS
        if has_simple_keyword or not has_finance_subject:
            return CHAT_MODE_SIMPLE
        return CHAT_MODE_ANALYSIS

    if has_knowledge_prefix and has_finance_subject and not has_analysis_intent:
        return CHAT_MODE_KNOWLEDGE

    if has_conversational_knowledge_prefix and has_finance_subject and not has_analysis_intent:
        return CHAT_MODE_KNOWLEDGE

    if has_simple_keyword and len(text) <= 24 and not has_finance_subject:
        return CHAT_MODE_SIMPLE

    if has_company_analysis:
        return CHAT_MODE_ANALYSIS

    if has_company_news:
        return CHAT_MODE_KNOWLEDGE

    if has_price_move_reason:
        return CHAT_MODE_ANALYSIS

    if has_knowledge_prefix and not has_analysis_intent and has_market_subject:
        return CHAT_MODE_KNOWLEDGE

    if has_analysis_intent or has_market_subject:
        return CHAT_MODE_ANALYSIS

    if has_finance_subject:
        return CHAT_MODE_KNOWLEDGE

    return CHAT_MODE_SIMPLE


def default_progress_for_chat_mode(chat_mode: str, status: str = "processing") -> str:
    mode = str(chat_mode or CHAT_MODE_ANALYSIS).strip()
    current = str(status or "").strip().lower()
    if mode == CHAT_MODE_KNOWLEDGE:
        return "任务排队中..." if current == "pending" else "正在整理知识回答..."
    if mode == CHAT_MODE_SIMPLE:
        return "正在回复..."
    return "任务排队中..." if current == "pending" else "团队正在协作分析..."
