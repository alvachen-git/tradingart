import re
from typing import Final


CHAT_MODE_SIMPLE: Final[str] = "simple_chat"
CHAT_MODE_KNOWLEDGE: Final[str] = "knowledge_chat"
CHAT_MODE_ANALYSIS: Final[str] = "analysis_chat"

FOLLOWUP_HINTS = (
    "继续", "展开", "详细说", "详细讲", "再说", "那为什么", "为什么呢",
    "然后呢", "接下来", "具体点", "说说看", "讲讲", "进一步", "补充一下",
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

ANALYSIS_INTENT_KEYWORDS = (
    "建议", "怎么做", "怎么办", "怎么看", "能买吗", "能不能买", "分析", "复盘",
    "行情", "走势", "涨跌", "对比", "比较", "宏观", "仓位", "持仓", "调仓", "加仓", "减仓",
    "对冲", "买入", "卖出", "开仓", "平仓", "新闻", "影响", "利好", "利空", "技术面",
    "基本面", "k线", "图表", "交易什么", "先交易", "先反应", "怎么传导", "如何传导",
    "适合做吗", "适合吗", "现在适合",
)

MARKET_SUBJECT_KEYWORDS = (
    "策略", "期权", "认购", "认沽", "iv", "delta", "gamma", "vega", "theta",
    "牛市价差", "熊市价差", "跨式", "宽跨", "勒式", "美联储", "降息", "黄金",
    "白银", "创业板", "etf", "股票", "指数", "铜价", "原油", "波动率",
)

FINANCE_BASE_KEYWORDS = (
    "金融", "交易", "投资", "理财", "基金", "债券", "国债", "利率", "通胀", "cpi", "pmi",
    "非农", "美债", "美元", "汇率", "外汇", "a股", "港股", "美股", "期货", "现货",
    "商品", "贵金属", "券商", "财报", "估值", "资产配置",
)

FINANCE_DOMAIN_KEYWORDS = FINANCE_BASE_KEYWORDS + ANALYSIS_INTENT_KEYWORDS + MARKET_SUBJECT_KEYWORDS

RECENT_FINANCE_CONTEXT_HINTS = FINANCE_BASE_KEYWORDS + MARKET_SUBJECT_KEYWORDS

RECENT_ANALYSIS_HINTS = (
    "怎么看", "怎么做", "怎么办", "能不能买", "能买吗", "策略", "建议", "买入", "卖出",
    "仓位", "调仓", "加仓", "减仓", "对冲", "开仓", "平仓", "技术面", "基本面",
    "比较", "对比", "先交易", "交易什么", "先反应", "怎么传导", "如何传导",
    "适合做吗", "适合吗", "现在适合",
)

SYMBOL_PATTERN = re.compile(r"\b[A-Z]{1,5}\d{0,4}\b|(?<!\d)\d{6}(?!\d)")
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


def classify_chat_mode(
    prompt_text: str,
    *,
    is_followup: bool = False,
    recent_context: str = "",
    has_uploaded_image: bool = False,
    has_structured_payload: bool = False,
    vision_position_domain: str = "",
) -> str:
    text = str(prompt_text or "").strip()
    if not text:
        return CHAT_MODE_ANALYSIS

    text_lower = text.lower()
    recent_context_lower = str(recent_context or "").strip().lower()
    has_url = has_url_like_text(text)
    has_symbol = bool(SYMBOL_PATTERN.search(text.upper()))
    domain = str(vision_position_domain or "").strip().lower()
    has_special_input = has_uploaded_image or has_structured_payload or domain in {"stock", "option", "mixed"}

    if has_special_input or has_url:
        return CHAT_MODE_ANALYSIS

    has_knowledge_prefix = any(keyword in text_lower for keyword in KNOWLEDGE_PREFIXES)
    has_analysis_intent = any(keyword in text_lower for keyword in ANALYSIS_INTENT_KEYWORDS)
    has_market_subject = any(keyword in text_lower for keyword in MARKET_SUBJECT_KEYWORDS) or has_symbol
    has_finance_subject = is_finance_or_trading_domain(text_lower, has_symbol=has_symbol)
    recent_has_finance_subject = _recent_context_has_finance_subject(recent_context_lower)
    recent_suggests_analysis = _recent_context_suggests_analysis(recent_context_lower)
    recent_suggests_knowledge = _recent_context_suggests_knowledge(recent_context_lower)
    has_simple_keyword = any(keyword in text_lower for keyword in SIMPLE_CHAT_KEYWORDS)

    if is_followup:
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

    if has_simple_keyword and len(text) <= 24 and not has_finance_subject:
        return CHAT_MODE_SIMPLE

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
