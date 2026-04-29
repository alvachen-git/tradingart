import os
import re
from typing import List
from langchain_core.tools import tool
from zhipuai import ZhipuAI
from dotenv import load_dotenv
from langchain_community.chat_models import ChatTongyi # 引入通义千问
from langchain_core.prompts import ChatPromptTemplate

# 初始化智谱客户端
# 建议将 key 放入 .env 文件: ZHIPUAI_API_KEY=...

load_dotenv(override=True)
ZHIPU_API_KEY = os.getenv("ZHIPUAI_API_KEY")

_COMPANY_ENTITY_SUFFIXES = (
    "技术", "股份", "集团", "银行", "药业", "控股", "能源", "电子", "科技",
    "汽车", "证券", "实业", "制造", "电气", "电器", "机械", "通信", "传媒",
)
_COMPANY_ENTITY_PATTERN = re.compile(
    rf"[A-Za-z\u4e00-\u9fff]{{2,20}}(?:{'|'.join(map(re.escape, _COMPANY_ENTITY_SUFFIXES))})"
)
_RECENT_NEWS_KEYWORDS = (
    "最近有什么好消息", "最近有没有好消息", "最近有什么动态", "最近进展", "最近催化",
    "最近公告", "最近财报", "最近业绩", "最近怎么样", "近期动态", "近期进展", "业务最近怎么样",
)
_FILING_KEYWORDS = (
    "财报", "年报", "季报", "一季报", "半年报", "中报", "三季报",
    "公告", "业绩快报", "业绩预告", "财务报告",
)
_ASPECT_KEYWORDS = (
    "机器人业务", "汽车业务", "新能源汽车", "工业自动化", "电梯", "电机",
    "伺服", "控制器", "储能", "光伏", "人形机器人",
)
_SEARCH_MISS_HINTS = (
    "未搜索到相关内容", "没搜到", "没有搜到", "未查到", "暂无明确", "暂无相关",
    "抱歉", "无法找到", "未找到",
)
_MAX_SEARCH_QUERIES = 3
_A_SHARE_FILING_SITES = (
    "cninfo.com.cn",
    "sse.com.cn",
    "szse.cn",
)
_A_SHARE_NEWS_SITES = (
    "eastmoney.com",
    "10jqka.com.cn",
)


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


def _extract_company_entities(query: str) -> List[str]:
    return _dedupe_keep_order([m.group(0) for m in _COMPANY_ENTITY_PATTERN.finditer(str(query or ""))])


def _extract_aspect_keywords(query: str) -> List[str]:
    text = str(query or "")
    return [keyword for keyword in _ASPECT_KEYWORDS if keyword in text]


def _looks_like_precise_finance_query(query: str) -> bool:
    text = str(query or "").strip()
    if not text:
        return False
    if _extract_stock_code(text):
        return True
    has_company = bool(_extract_company_entities(text))
    has_recent_news = any(keyword in text for keyword in _RECENT_NEWS_KEYWORDS)
    has_filing = any(keyword in text for keyword in _FILING_KEYWORDS)
    return has_company and (has_recent_news or has_filing)


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
    base_tokens = _extract_company_entities(text)
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

    broad_candidates: List[str] = []
    if aspect:
        if has_filing:
            broad_candidates.extend([
                f"{base} {aspect} 财报 公告",
                f"{base} {aspect} 年报 季报",
                f"{base} {aspect} 最新 公告",
            ])
        else:
            broad_candidates.extend([
                f"{base} {aspect} 最近动态",
                f"{base} {aspect} 公告",
                f"{base} {aspect} 财报",
            ])
    else:
        if has_filing:
            broad_candidates.extend([
                f"{base} 财报 公告",
                f"{base} 年报 季报",
                f"{base} 一季报 业绩快报",
            ])
        else:
            broad_candidates.extend([
                f"{base} 最近动态",
                f"{base} 财报 公告",
                f"{base} 最新 公告",
            ])

    primary_query = broad_candidates[0] if broad_candidates else text
    secondary_query = broad_candidates[1] if len(broad_candidates) > 1 else primary_query

    site_candidates = [
        f"site:{_A_SHARE_FILING_SITES[0]} {primary_query}",
        f"site:{_A_SHARE_NEWS_SITES[0]} {secondary_query}",
        text,
    ]
    return _dedupe_keep_order(site_candidates)[:_MAX_SEARCH_QUERIES]


def _build_search_queries(query: str) -> List[str]:
    raw_query = str(query or "").strip()
    if not raw_query:
        return []

    if _looks_like_precise_finance_query(raw_query):
        queries = _build_precise_finance_queries(raw_query)
        print(f"🔍 [搜索规划] 精准财经查询，直接使用模板词: {queries}")
        return queries

    try:
        optimized_query = _optimize_search_query(raw_query)
        print(f"🔍 [搜索优化] 原词: '{raw_query}' -> 优化词: '{optimized_query}'")
    except Exception as e:
        print(f"⚠️ 关键词优化失败，降级使用原词: {e}")
        optimized_query = raw_query

    return _dedupe_keep_order([optimized_query, raw_query])[:_MAX_SEARCH_QUERIES]


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


def _invoke_search_once(client: ZhipuAI, *, original_query: str, search_query: str) -> str:
    tools = [{
        "type": "web_search",
        "web_search": {
            "enable": True,
            "search_result": True,
            "search_query": search_query,
        }
    }]

    messages = [
        {
            "role": "user",
            "content": f"你可以实时的网络搜索，搜索以下内容：{search_query}。原始问题背景：{original_query}"
        }
    ]

    response = client.chat.completions.create(
        model="glm-4-air",
        messages=messages,
        tools=tools
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

        for search_query in search_queries[:_MAX_SEARCH_QUERIES]:
            try:
                answer = _invoke_search_once(client, original_query=query, search_query=search_query)
            except Exception as inner_exc:
                print(f"⚠️ 单次联网搜索失败，query='{search_query}' err={inner_exc}")
                continue

            if answer and not fallback_answer:
                fallback_answer = answer
            if not _looks_like_search_miss(answer):
                return answer

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
