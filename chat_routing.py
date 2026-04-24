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
    "基本面", "k线", "图表",
)

MARKET_SUBJECT_KEYWORDS = (
    "策略", "期权", "认购", "认沽", "iv", "delta", "gamma", "vega", "theta",
    "牛市价差", "熊市价差", "跨式", "宽跨", "勒式", "美联储", "降息", "黄金",
    "白银", "创业板", "etf", "股票", "指数", "铜价", "原油", "波动率",
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


def classify_chat_mode(
    prompt_text: str,
    *,
    is_followup: bool = False,
    has_uploaded_image: bool = False,
    has_structured_payload: bool = False,
    vision_position_domain: str = "",
) -> str:
    text = str(prompt_text or "").strip()
    if not text:
        return CHAT_MODE_ANALYSIS

    text_lower = text.lower()
    has_url = has_url_like_text(text)
    has_symbol = bool(SYMBOL_PATTERN.search(text.upper()))
    domain = str(vision_position_domain or "").strip().lower()
    has_special_input = has_uploaded_image or has_structured_payload or domain in {"stock", "option", "mixed"}

    if has_special_input or has_url:
        return CHAT_MODE_ANALYSIS

    has_knowledge_prefix = any(keyword in text_lower for keyword in KNOWLEDGE_PREFIXES)
    has_analysis_intent = any(keyword in text_lower for keyword in ANALYSIS_INTENT_KEYWORDS)
    has_market_subject = any(keyword in text_lower for keyword in MARKET_SUBJECT_KEYWORDS) or has_symbol
    has_simple_keyword = any(keyword in text_lower for keyword in SIMPLE_CHAT_KEYWORDS)

    if is_followup:
        if has_knowledge_prefix and not has_analysis_intent and has_market_subject:
            return CHAT_MODE_KNOWLEDGE
        return CHAT_MODE_ANALYSIS

    if has_knowledge_prefix and not has_analysis_intent:
        return CHAT_MODE_KNOWLEDGE

    if has_simple_keyword and len(text) <= 24:
        return CHAT_MODE_SIMPLE

    if has_analysis_intent or has_market_subject:
        return CHAT_MODE_ANALYSIS

    return CHAT_MODE_ANALYSIS


def default_progress_for_chat_mode(chat_mode: str, status: str = "processing") -> str:
    mode = str(chat_mode or CHAT_MODE_ANALYSIS).strip()
    current = str(status or "").strip().lower()
    if mode == CHAT_MODE_KNOWLEDGE:
        return "任务排队中..." if current == "pending" else "正在整理知识回答..."
    if mode == CHAT_MODE_SIMPLE:
        return "正在回复..."
    return "任务排队中..." if current == "pending" else "团队正在协作分析..."
