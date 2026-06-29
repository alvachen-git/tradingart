import os
import re
import time
import json
from dataclasses import dataclass
from datetime import date
from io import BytesIO
from typing import Any, Dict, List, Optional
from urllib import parse as url_parse
from urllib import request as url_request
try:
    import requests
except Exception:
    requests = None
if requests is None or not hasattr(requests, "post"):
    class _RequestsFallback:
        def post(self, *args, **kwargs):
            raise RuntimeError("requests package is unavailable")
    requests = _RequestsFallback()
try:
    from langchain_core.tools import tool
except Exception:
    def tool(func):
        func.name = getattr(func, "__name__", "")
        return func
try:
    from zhipuai import ZhipuAI
except Exception:
    class ZhipuAI:
        def __init__(self, *args, **kwargs):
            raise RuntimeError("zhipuai package is unavailable")
try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv(*args, **kwargs):
        return False
try:
    from langchain_community.chat_models import ChatTongyi # 引入通义千问
except Exception:
    ChatTongyi = None
try:
    from langchain_core.prompts import ChatPromptTemplate
except Exception:
    ChatPromptTemplate = None

# 初始化智谱客户端
# 建议将 key 放入 .env 文件: ZHIPUAI_API_KEY=...

load_dotenv(override=True)
if str(os.getenv("ENABLE_LANGSMITH_TRACING", "")).strip().lower() not in {"1", "true", "yes", "on"}:
    os.environ["LANGCHAIN_TRACING_V2"] = "false"
    os.environ["LANGSMITH_TRACING"] = "false"
    os.environ["LANGCHAIN_CALLBACKS_BACKGROUND"] = "false"

ZHIPU_API_KEY = os.getenv("ZHIPUAI_API_KEY")
_SEARCH_WEB_TIMEOUT_SECONDS = float(str(os.getenv("SEARCH_WEB_TIMEOUT_SECONDS", "12")).strip() or 12)
_DEFAULT_ZHIPU_SEARCH_MODEL = "glm-4-air"
_DEFAULT_ZHIPU_SEARCH_ENGINE = "search_std"
_DEFAULT_ZHIPU_SEARCH_MAX_ATTEMPTS = 2
_DEFAULT_ZHIPU_SEARCH_COUNT = 4
_DEFAULT_ZHIPU_SEARCH_DEEP_COUNT = 6
_DEFAULT_FILING_PROBE_TIMEOUT_SECONDS = 4.0
_DEFAULT_FILING_PROBE_MAX_RESULTS = 5
_DEFAULT_FILING_PDF_PARSE_TIMEOUT_SECONDS = 6.0
_DEFAULT_FILING_PDF_MAX_PAGES = 4
_DEFAULT_FILING_PDF_MAX_BYTES = 8_000_000
_CNINFO_ANNOUNCEMENT_QUERY_URL = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
_CNINFO_STATIC_BASE_URL = "https://static.cninfo.com.cn/"
_ALLOWED_SEARCH_ENGINES = {
    "search_std",
    "search_pro",
    "search_pro_sogou",
    "search_pro_quark",
}


@dataclass
class FilingReport:
    title: str
    date: str
    url: str
    report_type: str


@dataclass
class FilingProbeResult:
    company: str
    reports: List[FilingReport]
    source: str = "巨潮资讯"


@dataclass
class FilingMetrics:
    revenue: str = ""
    net_profit_parent: str = ""
    deducted_net_profit: str = ""
    operating_cashflow: str = ""
    eps: str = ""
    period: str = ""
    source_title: str = ""
    source_date: str = ""

_COMPANY_ENTITY_SUFFIXES = (
    "技术", "股份", "集团", "银行", "药业", "控股", "能源", "电子", "科技",
    "汽车", "证券", "实业", "制造", "电气", "电器", "机械", "通信", "传媒",
    "通讯", "电源",
)
_COMPANY_ENTITY_PATTERN = re.compile(
    rf"[A-Za-z\u4e00-\u9fff]{{2,20}}(?:{'|'.join(map(re.escape, _COMPANY_ENTITY_SUFFIXES))})"
)
_RECENT_NEWS_KEYWORDS = (
    "最近有什么好消息", "最近有没有好消息", "最近有什么动态", "最近进展", "最近催化",
    "最近公告", "最近财报", "最近业绩", "最近怎么样", "近期动态", "近期进展", "业务最近怎么样",
)
_LATEST_KEYWORDS = (
    "最近", "近期", "最新", "今天", "今日", "本周", "本月", "消息", "新闻", "动态", "公告", "财报", "业绩",
)
_FILING_KEYWORDS = (
    "财报", "年报", "季报", "一季报", "半年报", "中报", "三季报",
    "公告", "业绩快报", "业绩预告", "财务报告",
)
_GLOBAL_LISTING_KEYWORDS = (
    "IPO", "ipo", "上市", "挂牌", "股票代码", "ticker", "stock symbol",
    "Nasdaq", "NASDAQ", "NYSE", "nyse", "listed", "listing", "交易所",
)
_GLOBAL_LISTING_CN_EXCLUDE_MARKERS = (
    "A股", "a股", "沪市", "深市", "上交所", "深交所", "北交所", "科创板", "创业板",
)
_GLOBAL_LISTING_ALIAS_TICKERS = {
    "spacex": "SPCX",
}
_GLOBAL_LISTING_STOPWORDS = {
    "IPO", "ipo", "Nasdaq", "NASDAQ", "NYSE", "nyse", "stock", "ticker", "symbol",
    "listed", "listing", "exchange", "latest", "official", "announcement",
}
_GLOBAL_LISTING_STALE_NEGATIVE_HINTS = (
    "尚未确定IPO", "尚未宣布IPO", "尚未发布任何关于IPO", "尚未提交任何IPO", "未在Nasdaq",
    "未在NYSE", "没有近期IPO计划", "暂无IPO计划", "未确定具体上市", "私营公司",
    "尚未公开宣布IPO", "尚未进行首次公开募股", "尚未上市", "尚未选择在纳斯达克",
)
_GLOBAL_LISTING_POSITIVE_EVIDENCE_HINTS = (
    "已上市", "已经上市", "已公开交易", "公开交易股票", "开始交易", "上市交易",
    "股票代码为", "股票代码是", "ticker is", "began trading", "started trading",
    "listed on", "trades on",
)
_GLOBAL_LISTING_ENTITY_PATTERN = re.compile(r"\b[A-Za-z][A-Za-z0-9&.\-]{1,40}\b")
_REPORT_PERIOD_KEYWORDS = (
    "一季报", "第一季度", "半年报", "中报", "三季报", "第三季度",
)
_ASPECT_KEYWORDS = (
    "机器人业务", "汽车业务", "新能源汽车", "工业自动化", "电梯", "电机",
    "伺服", "控制器", "储能", "光伏", "人形机器人",
)
_SEARCH_MISS_HINTS = (
    "未搜索到相关内容", "没搜到", "没有搜到", "未查到", "暂无明确", "暂无相关",
    "抱歉", "无法找到", "未找到", "未检索到近期有效来源",
    "无法实时进行网络搜索", "无法实时搜索", "无法直接访问", "无法访问",
    "不能直接访问", "不能实时访问", "不能实时搜索",
)
_MAX_SEARCH_QUERIES = int(str(os.getenv("SEARCH_WEB_MAX_QUERIES", "2")).strip() or 2)
_A_SHARE_FILING_SITES = (
    "cninfo.com.cn",
    "sse.com.cn",
    "szse.cn",
)
_A_SHARE_NEWS_SITES = (
    "eastmoney.com",
    "10jqka.com.cn",
)
_AUTHORITY_SOURCE_KEYWORDS = (
    "官方", "官网", "交易所", "深交所", "上交所", "巨潮", "巨潮资讯",
    "原文", "公告原文", "披露原文", "PDF", "pdf", "链接",
)
_MACRO_KEYWORDS = (
    "宏观", "政策", "央行", "美联储", "降息", "加息", "利率", "CPI", "PPI", "PMI", "GDP",
    "通胀", "就业", "非农", "国债", "美元", "人民币", "汇率", "财政", "货币", "风险雷达",
)
_CONCEPT_KEYWORDS = (
    "概念股", "龙头", "相关股票", "相关个股", "受益股", "板块", "产业链", "行业名单", "有哪些",
)
_DEEP_CONTENT_KEYWORDS = (
    "深度", "报告", "研究", "复盘", "梳理", "风险雷达",
)
_LOW_QUALITY_ANSWER_HINTS = (
    "建议你直接去", "建议直接去", "建议查看", "建议查询", "建议关注", "官方数据尚未",
    "尚未完全同步", "公开检索库中", "无法确认最新", "未能确认最新",
)
_LATEST_FILING_FALSE_NEGATIVE_HINTS = (
    "尚未发布", "尚未披露", "还未发布", "还未披露", "无法获取", "未获取到",
    "无法查到", "未能获取", "没有检索到", "没有找到",
)
_FILING_METRIC_QUERY_KEYWORDS = (
    "赚钱吗", "盈利", "亏损", "好不好", "好吗", "如何", "怎么样", "财报好", "利润", "净利润", "营收",
)
_FILING_METRIC_ANSWER_KEYWORDS = (
    "营业收入", "营收", "净利润", "归母", "扣非", "每股收益", "EPS", "盈利", "亏损", "赚钱", "利润",
)
_PARTIAL_FILING_RESULT_HINTS = (
    "已找到最新披露文件", "未能在限时内解析出核心财务指标",
)
_QUERY_ENTITY_NOISE_TERMS = (
    "最近有什么好消息", "最近有没有好消息", "最近有什么动态", "业务最近怎么样",
    "最近", "近期", "最新", "今天", "今日", "本周", "本月",
    "财务报告", "年度报告", "季度报告", "定期报告", "第一季度报告", "第三季度报告",
    "财报", "年报", "季报", "一季报", "半年报", "中报", "三季报", "公告",
    "业绩快报", "业绩预告", "业绩", "消息", "新闻", "动态", "进展", "催化",
    "怎么样", "怎么", "如何", "情况", "数据", "报告", "分析", "查询", "搜索",
    "帮我", "看看", "一下", "原文", "披露", "链接", "PDF", "pdf", "的", "吗", "呢",
)


def _get_env_int(name: str, default: int, *, min_value: int, max_value: int) -> int:
    raw_value = os.getenv(name, "")
    try:
        value = int(str(raw_value).strip()) if raw_value else default
    except ValueError:
        return default
    return max(min_value, min(value, max_value))


def _get_env_float(name: str, default: float, *, min_value: float, max_value: float) -> float:
    raw_value = os.getenv(name, "")
    try:
        value = float(str(raw_value).strip()) if raw_value else default
    except ValueError:
        return default
    return max(min_value, min(value, max_value))


def _get_env_bool(name: str, default: bool) -> bool:
    raw_value = os.getenv(name, "")
    if raw_value == "":
        return default
    return str(raw_value).strip().lower() in {"1", "true", "yes", "on"}


def _get_zhipu_search_model() -> str:
    return (os.getenv("ZHIPU_SEARCH_MODEL") or _DEFAULT_ZHIPU_SEARCH_MODEL).strip() or _DEFAULT_ZHIPU_SEARCH_MODEL


def _get_zhipu_search_engine() -> str:
    engine = (os.getenv("ZHIPU_SEARCH_ENGINE") or _DEFAULT_ZHIPU_SEARCH_ENGINE).strip()
    return engine if engine in _ALLOWED_SEARCH_ENGINES else _DEFAULT_ZHIPU_SEARCH_ENGINE


def _get_zhipu_search_max_attempts() -> int:
    return _get_env_int(
        "ZHIPU_SEARCH_MAX_ATTEMPTS",
        _DEFAULT_ZHIPU_SEARCH_MAX_ATTEMPTS,
        min_value=1,
        max_value=_MAX_SEARCH_QUERIES,
    )


def _get_zhipu_search_count() -> int:
    return _get_env_int("ZHIPU_SEARCH_COUNT", _DEFAULT_ZHIPU_SEARCH_COUNT, min_value=1, max_value=50)


def _get_zhipu_search_deep_count() -> int:
    return _get_env_int("ZHIPU_SEARCH_DEEP_COUNT", _DEFAULT_ZHIPU_SEARCH_DEEP_COUNT, min_value=1, max_value=50)


def _get_filing_probe_timeout_seconds() -> float:
    return _get_env_float(
        "FILING_PROBE_TIMEOUT_SECONDS",
        _DEFAULT_FILING_PROBE_TIMEOUT_SECONDS,
        min_value=0.5,
        max_value=10.0,
    )


def _get_filing_probe_max_results() -> int:
    return _get_env_int("FILING_PROBE_MAX_RESULTS", _DEFAULT_FILING_PROBE_MAX_RESULTS, min_value=1, max_value=20)


def _official_filing_probe_enabled() -> bool:
    return _get_env_bool("ENABLE_OFFICIAL_FILING_PROBE", True)


def _filing_pdf_parse_enabled() -> bool:
    return _get_env_bool("ENABLE_FILING_PDF_PARSE", True)


def _get_filing_pdf_parse_timeout_seconds() -> float:
    return _get_env_float(
        "FILING_PDF_PARSE_TIMEOUT_SECONDS",
        _DEFAULT_FILING_PDF_PARSE_TIMEOUT_SECONDS,
        min_value=1.0,
        max_value=15.0,
    )


def _get_filing_pdf_max_pages() -> int:
    return _get_env_int("FILING_PDF_MAX_PAGES", _DEFAULT_FILING_PDF_MAX_PAGES, min_value=1, max_value=20)


def _get_filing_pdf_max_bytes() -> int:
    return _get_env_int("FILING_PDF_MAX_BYTES", _DEFAULT_FILING_PDF_MAX_BYTES, min_value=100_000, max_value=50_000_000)


def _dedupe_keep_order(items: List[str]) -> List[str]:
    seen = set()
    result = []
    for item in items:
        normalized = str(item or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def _extract_stock_code(query: str) -> str:
    match = re.search(r"(?<!\d)(\d{6})(?!\d)", str(query or ""))
    return match.group(1) if match else ""


def _normalize_a_share_code(value: str) -> str:
    text = str(value or "").strip().upper()
    match = re.search(r"(?<!\d)(\d{6})(?:\.(?:SH|SZ|BJ))?(?!\d)", text)
    return match.group(1) if match else ""


def _resolve_a_share_code_from_symbol_map_source(terms: List[str]) -> str:
    try:
        with open(os.path.join(os.path.dirname(__file__), "symbol_map.py"), "r", encoding="utf-8") as handle:
            source = handle.read()
    except Exception:
        return ""
    for term in terms:
        if not term:
            continue
        pattern = rf"['\"]{re.escape(term)}['\"]\s*:\s*['\"](\d{{6}}\.(?:SH|SZ|BJ))['\"]"
        match = re.search(pattern, source)
        if match:
            return _normalize_a_share_code(match.group(1))
    return ""


def _resolve_a_share_code(query: str, company: str = "") -> str:
    explicit_code = _extract_stock_code(query)
    if explicit_code:
        return explicit_code

    probe_terms = _dedupe_keep_order([company, *(_extract_company_or_query_entities(query) if query else [])])
    proxy_keys = ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy")
    proxy_snapshot = {key: os.environ.get(key) for key in proxy_keys}
    try:
        from symbol_map import resolve_symbol
    except Exception as exc:
        print(f"[filing probe warning] symbol resolver unavailable: {exc}")
        return _resolve_a_share_code_from_symbol_map_source(probe_terms)
    finally:
        for key, value in proxy_snapshot.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    for term in probe_terms:
        try:
            code, asset_type = resolve_symbol(term)
        except Exception as exc:
            print(f"[filing probe warning] symbol resolve failed, term='{term}' err={exc}")
            continue
        if asset_type == "stock":
            normalized = _normalize_a_share_code(str(code or ""))
            if normalized:
                return normalized
    return _resolve_a_share_code_from_symbol_map_source(probe_terms)


def _current_year() -> int:
    return date.today().year


def _latest_a_share_filing_should_be_available() -> bool:
    today = date.today()
    return today.month >= 5


def _has_explicit_historical_time(query: str) -> bool:
    text = str(query or "")
    if re.search(r"(?:19|20)\d{2}\s*(?:年|Q[1-4]|q[1-4])?", text):
        return True
    if re.search(r"(?:Q|q)[1-4]", text):
        return True
    if any(keyword in text for keyword in ("去年", "前年", "上年", "上一年", "历史", "过往")):
        return True
    return False


def _expand_relative_time_terms(query: str, intent: str) -> List[str]:
    text = str(query or "")
    year_now = _current_year()
    year_prev = year_now - 1
    month_now = date.today().month
    day_now = date.today().day
    terms: List[str] = []

    if any(keyword in text for keyword in ("今天", "今日", "当天", "盘中", "刚刚")):
        terms.append(f"{year_now}年{month_now}月{day_now}日")
    if any(keyword in text for keyword in ("这个月", "本月", "最近", "近期", "最新")):
        terms.append(f"{year_now}年{month_now}月")
    if any(keyword in text for keyword in ("今年", "本年", "年内")):
        terms.append(f"{year_now}年")
    if any(keyword in text for keyword in ("一季度", "第一季度", "一季报")):
        terms.append(f"{year_now} 第一季度")
    if any(keyword in text for keyword in ("去年", "上一年", "上年")):
        terms.append(f"{year_prev}年")

    if intent == "filing" and _should_prioritize_latest(query, intent):
        terms.extend([
            f"{year_prev} 年度报告",
            f"{year_now} 第一季度报告",
        ])

    return _dedupe_keep_order(terms)


def _append_time_context(search_query: str, original_query: str, intent: str) -> str:
    terms = _expand_relative_time_terms(original_query, intent)
    if not terms:
        return str(search_query or "").strip()
    query_text = str(search_query or "").strip()
    missing_terms = [term for term in terms if term not in query_text]
    if not missing_terms:
        return query_text
    return f"{query_text} {' '.join(missing_terms)}".strip()


def _should_prioritize_latest(query: str, intent: str) -> bool:
    if _has_explicit_historical_time(query):
        return False
    if intent in {"filing", "stock_news", "macro", "concept"}:
        return True
    return any(keyword in str(query or "") for keyword in _LATEST_KEYWORDS)


def _wants_authority_source(query: str) -> bool:
    return any(keyword in str(query or "") for keyword in _AUTHORITY_SOURCE_KEYWORDS)


def _extract_company_entities(query: str) -> List[str]:
    return _dedupe_keep_order([m.group(0) for m in _COMPANY_ENTITY_PATTERN.finditer(str(query or ""))])


def _infer_company_entities_from_query(query: str) -> List[str]:
    text = re.sub(r"(?<!\d)\d{6}(?!\d)", " ", str(query or ""))
    for term in sorted(set(_QUERY_ENTITY_NOISE_TERMS), key=len, reverse=True):
        text = text.replace(term, " ")
    text = re.sub(r"[（）()【】\[\]《》<>“”\"'，,。.!！?？、:：；;\s]+", " ", text)
    candidates = [
        token.strip()
        for token in text.split()
        if 2 <= len(token.strip()) <= 12 and re.search(r"[\u4e00-\u9fffA-Za-z]", token)
    ]
    return _dedupe_keep_order(candidates[:2])


def _extract_company_or_query_entities(query: str) -> List[str]:
    entities = _extract_company_entities(query)
    return entities or _infer_company_entities_from_query(query)


def _extract_aspect_keywords(query: str) -> List[str]:
    text = str(query or "")
    return [keyword for keyword in _ASPECT_KEYWORDS if keyword in text]


def _looks_like_precise_finance_query(query: str) -> bool:
    text = str(query or "").strip()
    if not text:
        return False
    if _looks_like_global_listing_query(text):
        return False
    if _extract_stock_code(text):
        return True
    has_company = bool(_extract_company_or_query_entities(text))
    has_recent_news = any(keyword in text for keyword in _RECENT_NEWS_KEYWORDS)
    has_filing = any(keyword in text for keyword in _FILING_KEYWORDS)
    return has_company and (has_recent_news or has_filing)


def _extract_global_listing_entity(query: str) -> str:
    for match in _GLOBAL_LISTING_ENTITY_PATTERN.findall(str(query or "")):
        token = match.strip()
        if token and token not in _GLOBAL_LISTING_STOPWORDS:
            return token
    return ""


def _looks_like_global_listing_query(query: str) -> bool:
    text = str(query or "").strip()
    if not text:
        return False
    if any(marker in text for marker in _GLOBAL_LISTING_CN_EXCLUDE_MARKERS):
        return False
    has_listing_intent = any(keyword in text for keyword in _GLOBAL_LISTING_KEYWORDS)
    if not has_listing_intent:
        return False
    has_global_entity = bool(_extract_global_listing_entity(text))
    has_global_market = any(keyword in text for keyword in ("Nasdaq", "NASDAQ", "NYSE", "nyse", "ticker", "stock symbol"))
    return has_global_entity or has_global_market


def _build_global_listing_queries(query: str) -> List[str]:
    text = str(query or "").strip()
    entity = _extract_global_listing_entity(text)
    subject = entity or text
    alias = _GLOBAL_LISTING_ALIAS_TICKERS.get(re.sub(r"[^a-z0-9]", "", subject.lower()), "")
    alias_part = f" {alias}" if alias else ""
    year_now = _current_year()
    candidates = [
        f"{subject}{alias_part} IPO stock ticker Nasdaq NYSE listed shares trading June {year_now}",
        f"{subject}{alias_part} stock symbol ticker exchange listed trading latest {year_now}",
        f"{subject}{alias_part} investor relations IPO listing stock ticker {year_now}",
    ]
    return _dedupe_keep_order(candidates)[:_MAX_SEARCH_QUERIES]


def _classify_search_intent(query: str) -> str:
    text = str(query or "").strip()
    if not text:
        return "generic"
    if _looks_like_global_listing_query(text):
        return "global_listing"
    if any(keyword in text for keyword in _FILING_KEYWORDS):
        return "filing"
    if any(keyword in text for keyword in _MACRO_KEYWORDS):
        return "macro"
    if any(keyword in text for keyword in _CONCEPT_KEYWORDS):
        return "concept"
    if _looks_like_precise_finance_query(text):
        return "stock_news"
    return "generic"


def _infer_search_recency_filter(query: str, intent: str) -> str:
    text = str(query or "")
    if any(keyword in text for keyword in ("今天", "今日", "当天", "盘中", "刚刚")):
        return "oneDay"
    if intent == "global_listing":
        return "oneYear"
    if intent == "filing":
        return "oneYear"
    if any(keyword in text for keyword in ("本周", "一周", "最近", "近期", "最新")):
        return "oneWeek" if intent in {"stock_news", "macro"} else "oneMonth"
    if intent in {"macro", "generic"}:
        return "oneMonth"
    return "noLimit"


def _extract_site_filter(search_query: str) -> str:
    match = re.search(r"(?:^|\s)site:([^\s]+)", str(search_query or ""))
    if not match:
        return ""
    return match.group(1).strip()


def _strip_site_filters(search_query: str) -> str:
    return re.sub(r"(?:^|\s)site:[^\s]+", " ", str(search_query or "")).strip()


def _build_finance_search_prompt(intent: str) -> str:
    focus = {
        "filing": "优先核对公告、财报、交易所或公司官方来源。",
        "macro": "优先核对政策、央行、统计数据、权威媒体和原始发布机构。",
        "concept": "优先交叉验证概念股名单、产业链位置和近期热点来源。",
        "stock_news": "优先核对公司公告、权威财经媒体和近期公开报道。",
        "global_listing": "优先核对交易所、SEC、公司官网、投资者关系页面和权威财经媒体，确认股票代码、交易所、上市/开始交易日期。",
    }.get(intent, "优先核对权威、近期、可追溯的信息来源。")
    latest_instruction = (
        "如果用户没有明确指定历史年份、季度或报告期，必须优先检索并回答当前能找到的最新公开信息。"
        "不要用旧报告或旧新闻代替最新资料。"
    )
    filing_instruction = ""
    if intent == "filing":
        filing_instruction = "财报/公告问题必须先确认最新报告期、公告标题、披露日期和来源；如果没有找到最新报告期，请明确说明。"
    elif intent == "stock_news":
        filing_instruction = "新闻/消息问题必须优先近期来源，并说明来源日期；如果没有近期有效来源，请明确说明未检索到近期有效来源。"
    elif intent == "global_listing":
        filing_instruction = (
            "上市/IPO状态问题必须回答当前状态，不得使用旧年份信息替代当前核验；"
            "如果结果只显示旧年份的未上市说法，必须继续寻找股票代码、交易所或最新权威报道。"
        )
    return f"""
你是一位谨慎的财经信息检索助手。请基于网络搜索结果回答用户问题，不要编造未被搜索结果支持的信息。
{focus}
{latest_instruction}
{filing_instruction}
输出要求：
1. 先给出一句话结论。
2. 按重要性列出关键事实，并尽量标注来源名称与发布日期。
3. 如信息对标的或市场有影响，请说明偏正面、偏负面或中性，以及不确定性。
4. 如果搜索结果不足，请明确说明不足之处，不要强行下结论。
5. 末尾保留“仅供研究参考，不构成投资建议”。
""".strip()


def _build_web_search_options(original_query: str, search_query: str, *, attempt_index: int = 0) -> Dict[str, Any]:
    intent = _classify_search_intent(original_query)
    engine = _get_zhipu_search_engine()
    count = _get_zhipu_search_count()
    content_size = "medium"

    if intent in {"macro", "concept", "global_listing"}:
        engine = "search_pro"
        count = _get_zhipu_search_deep_count()
        if intent == "global_listing" or any(keyword in str(original_query or "") for keyword in _DEEP_CONTENT_KEYWORDS):
            content_size = "high"
    elif intent == "filing":
        count = _get_zhipu_search_deep_count()
        content_size = "high"
    elif attempt_index > 0:
        engine = "search_pro"
        count = _get_zhipu_search_deep_count()

    web_search: Dict[str, Any] = {
        "enable": True,
        "search_result": True,
        "search_query": _strip_site_filters(search_query) or search_query,
        "search_engine": engine,
        "count": count,
        "content_size": content_size,
        "search_recency_filter": _infer_search_recency_filter(original_query, intent),
        "search_prompt": _build_finance_search_prompt(intent),
    }

    site_filter = _extract_site_filter(search_query)
    if site_filter:
        web_search["search_domain_filter"] = site_filter

    return web_search


def _optimize_search_query(raw_query: str) -> str:
    # 使用便宜且快的模型 (qwen-turbo) 来做关键词提取
    # 注意：这里需要你有 DASHSCOPE_API_KEY
    llm_optimizer = ChatTongyi(model="qwen-turbo", temperature=0.1)

    prompt = ChatPromptTemplate.from_template("""
    你是一个搜索引擎优化专家(SEO)。你的任务是将用户的复杂问题转换为【最适合搜索引擎】的关键词。

    【规则】
    1. 去除“帮我查”、“分析一下”、“最新的”等无关词汇。
    2. 提取核心实体和时间。
    3. 如果包含多个不同主题，用空格分隔。
    4. **直接输出优化后的关键词，不要任何解释。**

    用户问题: {raw_query}
    优化后的搜索词:
    """)

    chain = prompt | llm_optimizer
    return chain.invoke({"raw_query": raw_query}).content.strip()


def _build_precise_finance_queries(query: str) -> List[str]:
    text = str(query or "").strip()
    base_tokens = _extract_company_or_query_entities(text)
    stock_code = _extract_stock_code(text)
    if stock_code:
        base_tokens.append(stock_code)
    base_tokens = _dedupe_keep_order(base_tokens)
    if not base_tokens:
        return [text] if text else []

    base = " ".join(base_tokens)
    aspects = _extract_aspect_keywords(text)
    aspect = aspects[0] if aspects else ""
    has_filing = any(keyword in text for keyword in _FILING_KEYWORDS)
    intent = _classify_search_intent(text)
    prioritize_latest = _should_prioritize_latest(text, intent)
    wants_authority_source = _wants_authority_source(text)
    time_context = " ".join(_expand_relative_time_terms(text, intent))
    year_now = _current_year()
    year_prev = year_now - 1

    broad_candidates: List[str] = []
    if aspect:
        if has_filing:
            if prioritize_latest:
                broad_candidates.extend([
                    f"{base} {aspect} {year_now} 第一季度报告 营收 净利润 归母净利润 最新财务数据",
                    f"{base} {aspect} {year_prev} 年年度报告 营收 净利润 归母净利润",
                    f"{base} {aspect} {year_now} 一季度报告 PDF finalpage 巨潮资讯 深交所",
                ])
            else:
                broad_candidates.extend([
                    f"{base} {aspect} {text}",
                    f"{base} {aspect} 财报 公告",
                    f"{base} {aspect} 年报 季报",
                ])
        else:
            broad_candidates.extend([
                f"{base} {aspect} 最新消息 近期动态 {time_context}",
                f"{base} {aspect} 公告 {time_context}",
                f"{base} {aspect} 财报",
            ])
    else:
        if has_filing:
            if prioritize_latest:
                broad_candidates.extend([
                    f"{base} {year_now} 第一季度报告 营收 净利润 归母净利润 最新财务数据",
                    f"{base} {year_prev} 年年度报告 营收 净利润 归母净利润",
                    f"{base} {year_now} 一季度报告 PDF finalpage 巨潮资讯 深交所",
                ])
            else:
                broad_candidates.extend([
                    f"{base} {text}",
                    f"{base} 财报 公告",
                    f"{base} 年报 季报",
                ])
        else:
            broad_candidates.extend([
                f"{base} 最新消息 近期动态 {time_context}",
                f"{base} 财报 公告 {time_context}",
                f"{base} 最新 公告",
            ])

    primary_query = broad_candidates[0] if broad_candidates else text
    secondary_query = broad_candidates[1] if len(broad_candidates) > 1 else primary_query
    fallback_query = (
        broad_candidates[2]
        if has_filing and prioritize_latest and len(broad_candidates) > 2
        else text
    )

    if has_filing and prioritize_latest:
        if wants_authority_source:
            site_candidates = [
                f"site:{_A_SHARE_FILING_SITES[0]} {primary_query}",
                f"site:{_A_SHARE_FILING_SITES[2]} {secondary_query}",
                fallback_query,
            ]
        else:
            site_candidates = [
                primary_query,
                secondary_query,
                fallback_query,
            ]
    else:
        if wants_authority_source:
            site_candidates = [
                f"site:{_A_SHARE_FILING_SITES[0]} {primary_query}",
                f"site:{_A_SHARE_NEWS_SITES[0]} {secondary_query}",
                fallback_query,
            ]
        else:
            site_candidates = [
                primary_query,
                secondary_query,
                fallback_query,
            ]
    return _dedupe_keep_order(site_candidates)[:_MAX_SEARCH_QUERIES]


def _build_search_queries(query: str) -> List[str]:
    raw_query = str(query or "").strip()
    if not raw_query:
        return []

    if _looks_like_global_listing_query(raw_query):
        queries = _build_global_listing_queries(raw_query)
        print(f"[search plan] global listing query, using template queries: {queries}")
        return queries

    if _looks_like_precise_finance_query(raw_query):
        queries = _build_precise_finance_queries(raw_query)
        print(f"[search plan] precise finance query, using template queries: {queries}")
        return queries

    try:
        optimized_query = _optimize_search_query(raw_query)
        print(f"[search optimize] raw='{raw_query}' -> optimized='{optimized_query}'")
    except Exception as e:
        print(f"[search optimize warning] failed, falling back to raw query: {e}")
        optimized_query = raw_query

    intent = _classify_search_intent(raw_query)
    return _dedupe_keep_order([
        _append_time_context(optimized_query, raw_query, intent),
        _append_time_context(raw_query, raw_query, intent),
    ])[:_MAX_SEARCH_QUERIES]


def _extract_answer_text(response) -> str:
    try:
        return str(response.choices[0].message.content or "").strip()
    except Exception:
        return ""


def _looks_like_search_miss(answer: str) -> bool:
    text = str(answer or "").strip()
    if not text:
        return True
    return any(hint in text for hint in _SEARCH_MISS_HINTS)


def _extract_years(text: str) -> List[int]:
    return [int(match) for match in re.findall(r"(?:19|20)\d{2}", str(text or ""))]


def _contains_current_or_previous_year(text: str) -> bool:
    years = set(_extract_years(text))
    return bool(years & {_current_year(), _current_year() - 1})


def _wants_filing_metric_answer(query: str) -> bool:
    return any(keyword in str(query or "") for keyword in _FILING_METRIC_QUERY_KEYWORDS)


def _has_metric_answer_signal(answer: str) -> bool:
    text = str(answer or "")
    return any(keyword in text for keyword in _FILING_METRIC_ANSWER_KEYWORDS) and bool(re.search(r"\d", text))


def _is_partial_filing_result(answer: str) -> bool:
    text = str(answer or "")
    return any(keyword in text for keyword in _PARTIAL_FILING_RESULT_HINTS)


def _looks_like_low_quality_latest_answer(original_query: str, answer: str) -> bool:
    query = str(original_query or "")
    text = str(answer or "").strip()
    if not text:
        return True

    intent = _classify_search_intent(query)
    if intent == "global_listing":
        years = set(_extract_years(text))
        stale_cutoff = _current_year() - 1
        has_stale_as_of = any(int(year) < stale_cutoff for year in re.findall(r"截至\s*((?:19|20)\d{2})", text))
        lower_text = text.lower()
        has_positive_listing_evidence = any(keyword.lower() in lower_text for keyword in _GLOBAL_LISTING_POSITIVE_EVIDENCE_HINTS)
        if has_stale_as_of:
            return True
        if any(hint in text for hint in _GLOBAL_LISTING_STALE_NEGATIVE_HINTS) and not has_positive_listing_evidence:
            return True
        if years and not (years & {_current_year(), _current_year() - 1}) and not has_positive_listing_evidence:
            return True

    if not _should_prioritize_latest(query, intent):
        return False

    if intent == "filing":
        if _latest_a_share_filing_should_be_available() and any(hint in text for hint in _LATEST_FILING_FALSE_NEGATIVE_HINTS):
            return True
        years = set(_extract_years(text))
        if years and not (years & {_current_year(), _current_year() - 1}):
            return True
        if any(hint in text for hint in _LOW_QUALITY_ANSWER_HINTS) and not _contains_current_or_previous_year(text):
            return True
        if "最新完整财报" in text and not _contains_current_or_previous_year(text):
            return True
        if _wants_filing_metric_answer(query) and not _has_metric_answer_signal(text) and not _is_partial_filing_result(text):
            return True

    if intent in {"stock_news", "macro", "concept", "generic"}:
        if any(hint in text for hint in _LOW_QUALITY_ANSWER_HINTS) and not _contains_current_or_previous_year(text):
            return True

    return False


def _is_acceptable_search_answer(original_query: str, answer: str) -> bool:
    return not _looks_like_search_miss(answer) and not _looks_like_low_quality_latest_answer(original_query, answer)


def is_search_answer_acceptable(query: str, answer: str) -> bool:
    return _is_acceptable_search_answer(query, answer)


def _clean_cninfo_title(title: str) -> str:
    text = re.sub(r"<[^>]+>", "", str(title or ""))
    return re.sub(r"\s+", " ", text).strip()


def _normalize_cninfo_date(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp = timestamp / 1000
        try:
            return date.fromtimestamp(timestamp).isoformat()
        except Exception:
            return str(value)
    text = str(value).strip()
    match = re.search(r"(?:19|20)\d{2}[-/年.]\d{1,2}[-/月.]\d{1,2}", text)
    if match:
        return match.group(0).replace("年", "-").replace("月", "-").replace("/", "-").replace(".", "-").rstrip("日")
    return text[:10]


def _normalize_cninfo_url(value: str) -> str:
    url = str(value or "").strip()
    if not url:
        return ""
    if url.startswith(("http://", "https://")):
        return url
    return f"{_CNINFO_STATIC_BASE_URL}{url.lstrip('/')}"


def _classify_filing_report_type(title: str) -> str:
    text = str(title or "")
    if "第一季度" in text or "一季度" in text:
        return "一季报"
    if "半年度" in text or "半年报" in text or "中报" in text:
        return "半年报"
    if "第三季度" in text or "三季度" in text:
        return "三季报"
    if "年度报告" in text or "年报" in text:
        return "年报"
    return "公告"


def _is_target_latest_filing_title(title: str) -> bool:
    text = str(title or "")
    current_year = _current_year()
    previous_year = current_year - 1
    is_current_q1 = (
        str(current_year) in text
        and ("第一季度报告" in text or "一季度报告" in text or "第 一 季度报告" in text)
    )
    is_previous_annual = (
        str(previous_year) in text
        and ("年度报告" in text or "年报" in text)
    )
    return is_current_q1 or is_previous_annual


def _announcement_to_filing_report(item: Dict[str, Any]) -> Optional[FilingReport]:
    title = _clean_cninfo_title(item.get("announcementTitle") or item.get("title") or "")
    if not title or not _is_target_latest_filing_title(title):
        return None
    url = _normalize_cninfo_url(item.get("adjunctUrl") or item.get("url") or "")
    date_text = _normalize_cninfo_date(item.get("announcementTime") or item.get("date") or item.get("publishTime"))
    return FilingReport(
        title=title,
        date=date_text,
        url=url,
        report_type=_classify_filing_report_type(title),
    )


def _post_cninfo_json(data: Dict[str, Any], *, headers: Dict[str, str], timeout: float) -> Dict[str, Any]:
    post = getattr(requests, "post", None)
    if callable(post):
        try:
            response = post(
                _CNINFO_ANNOUNCEMENT_QUERY_URL,
                data=data,
                headers=headers,
                timeout=timeout,
            )
            response.raise_for_status()
            payload = response.json()
            return payload if isinstance(payload, dict) else {}
        except RuntimeError:
            pass

    encoded = url_parse.urlencode(data).encode("utf-8")
    req = url_request.Request(_CNINFO_ANNOUNCEMENT_QUERY_URL, data=encoded, headers=headers, method="POST")
    with url_request.urlopen(req, timeout=timeout) as response:
        raw = response.read().decode("utf-8", errors="replace")
    payload = json.loads(raw)
    return payload if isinstance(payload, dict) else {}


def _fetch_cninfo_announcements(search_key: str, *, timeout_seconds: float, max_results: int) -> List[Dict[str, Any]]:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search",
        "Accept": "application/json, text/plain, */*",
    }
    deadline = time.monotonic() + max(0.5, timeout_seconds)
    items: List[Dict[str, Any]] = []
    for column in ("szse", "sse"):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        data = {
            "pageNum": "1",
            "pageSize": str(max(10, min(max_results * 4, 30))),
            "column": column,
            "tabName": "fulltext",
            "plate": "",
            "stock": "",
            "searchkey": search_key,
            "secid": "",
            "category": "",
            "trade": "",
            "seDate": "",
            "sortName": "",
            "sortType": "",
            "isHLtitle": "true",
        }
        try:
            payload = _post_cninfo_json(data, headers=headers, timeout=min(2.0, max(0.5, remaining)))
        except Exception as exc:
            print(f"[filing probe warning] cninfo query failed, column={column} err={exc}")
            continue
        announcements = payload.get("announcements") if isinstance(payload, dict) else None
        if isinstance(announcements, list):
            items.extend([item for item in announcements if isinstance(item, dict)])
    return items


def _build_filing_probe_search_keys(query: str, company: str, stock_code: str = "") -> List[str]:
    resolved_code = stock_code or _resolve_a_share_code(query, company)
    return _dedupe_keep_order([resolved_code, company])


def _collect_filing_reports(raw_items: List[Dict[str, Any]], max_results: int) -> List[FilingReport]:
    reports: List[FilingReport] = []
    seen = set()
    for item in raw_items:
        report = _announcement_to_filing_report(item)
        if not report:
            continue
        dedupe_key = (report.title, report.date)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        reports.append(report)
    priority = {"一季报": 0, "年报": 1, "半年报": 2, "三季报": 3, "公告": 4}
    reports.sort(key=lambda item: (priority.get(item.report_type, 9), item.date), reverse=False)
    return reports[:max_results]


def _official_filing_probe(query: str) -> Optional[FilingProbeResult]:
    if not _official_filing_probe_enabled():
        return None
    intent = _classify_search_intent(query)
    if intent != "filing" or not _should_prioritize_latest(query, intent):
        return None

    entities = _extract_company_or_query_entities(query)
    stock_code = _extract_stock_code(query)
    company = entities[0] if entities else stock_code
    if not company:
        return None

    search_keys = _build_filing_probe_search_keys(query, company, stock_code)
    max_results = _get_filing_probe_max_results()
    reports: List[FilingReport] = []
    for search_key in search_keys:
        raw_items = _fetch_cninfo_announcements(
            search_key,
            timeout_seconds=_get_filing_probe_timeout_seconds(),
            max_results=max_results,
        )
        reports = _collect_filing_reports(raw_items, max_results)
        if reports:
            break

    if not reports:
        return None

    return FilingProbeResult(company=company, reports=reports)


def _should_try_official_filing_probe(query: str, answer: str) -> bool:
    intent = _classify_search_intent(query)
    if intent != "filing" or not _should_prioritize_latest(query, intent):
        return False
    return _looks_like_search_miss(answer) or _looks_like_low_quality_latest_answer(query, answer)


def _should_probe_filing_immediately(query: str, answer: str) -> bool:
    intent = _classify_search_intent(query)
    if intent != "filing" or not _should_prioritize_latest(query, intent):
        return False
    text = str(answer or "")
    return any(hint in text for hint in _LATEST_FILING_FALSE_NEGATIVE_HINTS) or any(
        hint in text for hint in ("无法实时搜索", "无法实时进行网络搜索", "无法直接访问", "无法访问数据库")
    )


def _build_probe_metric_query(probe_result: FilingProbeResult) -> str:
    report = probe_result.reports[0]
    return f"{probe_result.company} {report.title} 营收 净利润 归母净利润 毛利率"


def _download_pdf_bytes(url: str, *, timeout_seconds: float, max_bytes: int) -> bytes:
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/pdf,*/*"}
    req = url_request.Request(str(url or ""), headers=headers)
    with url_request.urlopen(req, timeout=timeout_seconds) as response:
        content_length = response.headers.get("Content-Length")
        if content_length and int(content_length) > max_bytes:
            raise ValueError(f"PDF too large: {content_length} bytes")
        data = response.read(max_bytes + 1)
    if len(data) > max_bytes:
        raise ValueError(f"PDF too large: >{max_bytes} bytes")
    return data


def _extract_pdf_text(pdf_bytes: bytes, *, max_pages: int, deadline: float) -> str:
    try:
        from pypdf import PdfReader
    except Exception:
        from PyPDF2 import PdfReader

    reader = PdfReader(BytesIO(pdf_bytes))
    chunks: List[str] = []
    for page in reader.pages[:max_pages]:
        if time.monotonic() > deadline:
            break
        try:
            chunks.append(page.extract_text() or "")
        except Exception as exc:
            print(f"[filing pdf warning] page text extraction failed: {exc}")
    return "\n".join(chunks)


def _metric_number_pattern() -> str:
    return r"[-+]?(?:\d{1,3}(?:[,，]\d{3})+|\d+)(?:\.\d+)?\s*(?:亿元|万元|元)?"


def _extract_metric_value(text: str, labels: List[str]) -> str:
    normalized = re.sub(r"[ \t]+", " ", str(text or ""))
    compact = re.sub(r"\s+", "", normalized)
    number = _metric_number_pattern()
    for label in labels:
        escaped = re.escape(label)
        patterns = [
            rf"{escaped}[：:\s]*({number})",
            rf"{escaped}.{{0,40}}?({number})",
        ]
        for pattern in patterns:
            match = re.search(pattern, normalized, flags=re.S)
            if match:
                return match.group(1).replace("，", ",").strip()
            match = re.search(pattern, compact, flags=re.S)
            if match:
                return match.group(1).replace("，", ",").strip()
    return ""


def _infer_period_from_title(title: str) -> str:
    text = str(title or "")
    match = re.search(r"((?:19|20)\d{2}\s*年?\s*(?:第一季度|一季度|半年度|半年|第三季度|三季度|年度))", text)
    return re.sub(r"\s+", "", match.group(1)) if match else ""


def _parse_filing_metrics_from_text(text: str, report: FilingReport) -> FilingMetrics:
    return FilingMetrics(
        revenue=_extract_metric_value(text, ["营业收入", "营业总收入"]),
        net_profit_parent=_extract_metric_value(text, ["归属于上市公司股东的净利润", "归母净利润"]),
        deducted_net_profit=_extract_metric_value(text, ["归属于上市公司股东的扣除非经常性损益的净利润", "扣非归母净利润", "扣非净利润"]),
        operating_cashflow=_extract_metric_value(text, ["经营活动产生的现金流量净额", "经营现金流量净额"]),
        eps=_extract_metric_value(text, ["基本每股收益"]),
        period=_infer_period_from_title(report.title),
        source_title=report.title,
        source_date=report.date,
    )


def _has_filing_metrics(metrics: Optional[FilingMetrics]) -> bool:
    return bool(metrics and any([
        metrics.revenue,
        metrics.net_profit_parent,
        metrics.deducted_net_profit,
        metrics.operating_cashflow,
        metrics.eps,
    ]))


def _download_and_parse_filing_pdf(report: FilingReport) -> Optional[FilingMetrics]:
    if not _filing_pdf_parse_enabled() or not report.url:
        return None
    timeout_seconds = _get_filing_pdf_parse_timeout_seconds()
    deadline = time.monotonic() + timeout_seconds
    try:
        pdf_bytes = _download_pdf_bytes(
            report.url,
            timeout_seconds=min(4.0, timeout_seconds),
            max_bytes=_get_filing_pdf_max_bytes(),
        )
        remaining = max(0.5, deadline - time.monotonic())
        text = _extract_pdf_text(pdf_bytes, max_pages=_get_filing_pdf_max_pages(), deadline=time.monotonic() + remaining)
        metrics = _parse_filing_metrics_from_text(text, report)
        return metrics if _has_filing_metrics(metrics) else None
    except Exception as exc:
        print(f"[filing pdf warning] parse failed, title='{report.title}' err={exc}")
        return None


def _format_metric_line(label: str, value: str) -> str:
    return f"- {label}：{value}" if value else ""


def _looks_negative_value(value: str) -> bool:
    text = str(value or "").strip()
    return text.startswith("-") or text.startswith("－") or text.startswith("(") or "亏" in text


def _build_filing_metrics_conclusion(query: str, metrics: FilingMetrics) -> str:
    profit_value = metrics.net_profit_parent or metrics.deducted_net_profit
    period = metrics.period or "最新报告期"
    if profit_value:
        if _looks_negative_value(profit_value):
            return f"{period}仍处于亏损状态，核心依据是归母/扣非净利润为负。"
        return f"{period}是盈利的，核心依据是归母/扣非净利润为正。"
    if metrics.revenue:
        return f"{period}已披露营收数据，但本轮未解析到净利润指标，暂不能直接判断是否赚钱。"
    return f"{period}已披露，但本轮未解析到足够的核心财务指标。"


def _format_filing_metrics_summary(query: str, metrics: FilingMetrics) -> str:
    lines = [_build_filing_metrics_conclusion(query, metrics), ""]
    lines.append("核心财务指标：")
    for line in [
        _format_metric_line("营业收入", metrics.revenue),
        _format_metric_line("归母净利润", metrics.net_profit_parent),
        _format_metric_line("扣非归母净利润", metrics.deducted_net_profit),
        _format_metric_line("经营活动现金流量净额", metrics.operating_cashflow),
        _format_metric_line("基本每股收益", metrics.eps),
    ]:
        if line:
            lines.append(line)
    source = metrics.source_title
    if metrics.source_date:
        source += f"，披露日期：{metrics.source_date}"
    if source:
        lines.extend(["", f"来源：{source}"])
    return "\n".join(lines)


def _format_filing_probe_result(
    probe_result: FilingProbeResult,
    metric_answer: str = "",
    metrics: Optional[FilingMetrics] = None,
    original_query: str = "",
) -> str:
    lines = [
        "根据官方公告检索，已找到最新披露文件：",
    ]
    for report in probe_result.reports:
        line = f"- {report.report_type}：{report.title}"
        if report.date:
            line += f"，披露日期：{report.date}"
        if report.url:
            line += f"，链接：{report.url}"
        lines.append(line)

    if _has_filing_metrics(metrics):
        lines.extend(["", _format_filing_metrics_summary(original_query, metrics)])
    elif str(metric_answer or "").strip():
        lines.extend(["", "补充检索到的财务摘要：", str(metric_answer or "").strip()])
    else:
        lines.extend([
            "",
            "已找到最新披露文件，但本轮未能在限时内解析出核心财务指标；因此不编造营收、净利润等数字。",
        ])
    lines.append("仅供研究参考，不构成投资建议。")
    return "\n".join(lines)


def _invoke_search_once(client: ZhipuAI, *, original_query: str, search_query: str, attempt_index: int = 0) -> str:
    tools = [{
        "type": "web_search",
        "web_search": _build_web_search_options(original_query, search_query, attempt_index=attempt_index)
    }]

    messages = [
        {
            "role": "user",
            "content": f"你可以实时的网络搜索，搜索以下内容：{search_query}。原始问题背景：{original_query}"
        }
    ]

    response = client.chat.completions.create(
        model=_get_zhipu_search_model(),
        messages=messages,
        tools=tools,
        timeout=_SEARCH_WEB_TIMEOUT_SECONDS,
    )
    return _extract_answer_text(response)


def _search_web_impl(query: str) -> str:
    if not ZHIPU_API_KEY:
        return "❌ 错误：未配置 ZHIPUAI_API_KEY"

    search_queries = _build_search_queries(query)
    if not search_queries:
        return "📭 未搜索到相关内容。"

    try:
        client = ZhipuAI(api_key=ZHIPU_API_KEY)
        fallback_answer = ""

        for attempt_index, search_query in enumerate(search_queries[:_get_zhipu_search_max_attempts()]):
            try:
                answer = _invoke_search_once(
                    client,
                    original_query=query,
                    search_query=search_query,
                    attempt_index=attempt_index,
                )
            except Exception as inner_exc:
                print(f"[search warning] single web search failed, query='{search_query}' err={inner_exc}")
                continue

            if answer and not fallback_answer:
                fallback_answer = answer
            if _is_acceptable_search_answer(query, answer):
                return answer
            if _should_probe_filing_immediately(query, answer):
                break

        if _should_try_official_filing_probe(query, fallback_answer):
            probe_result = _official_filing_probe(query)
            if probe_result and probe_result.reports:
                metrics = _download_and_parse_filing_pdf(probe_result.reports[0])
                metric_answer = ""
                if not _has_filing_metrics(metrics):
                    try:
                        metric_query = _build_probe_metric_query(probe_result)
                        metric_candidate = _invoke_search_once(
                            client,
                            original_query=query,
                            search_query=metric_query,
                            attempt_index=1,
                        )
                        if _is_acceptable_search_answer(query, metric_candidate):
                            metric_answer = metric_candidate
                    except Exception as inner_exc:
                        print(f"[filing probe warning] precise metric search failed: {inner_exc}")
                return _format_filing_probe_result(
                    probe_result,
                    metric_answer,
                    metrics=metrics,
                    original_query=query,
                )

        return fallback_answer or "📭 未搜索到相关内容。"
    except Exception as e:
        return f"搜索出错: {e}"

@tool
def search_web(query: str) -> str:
    """
    【互联网搜索工具】
    使用智谱 AI 的内置联网功能进行搜索。
    适用于：查询财经和政治新闻、宏观政策、具体事件细节。
    """
    return _search_web_impl(query)


# 测试代码
if __name__ == "__main__":
    # 需要先设置环境变量才能运行测试
    # os.environ["ZHIPUAI_API_KEY"] = "你的key"
    print(search_web.invoke("最近的铝价格走势原因"))
