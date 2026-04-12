import os
import warnings
from typing import Optional
from langchain_community.embeddings import DashScopeEmbeddings
from langchain_core.documents import Document
from datetime import datetime
from chromadb.config import Settings
import chromadb

try:
    from langchain_chroma import Chroma  # type: ignore
    _CHROMA_BACKEND = "langchain_chroma"
except ImportError:
    try:
        from langchain_core._api.deprecation import LangChainDeprecationWarning  # type: ignore
    except Exception:
        LangChainDeprecationWarning = Warning  # type: ignore

    # Fallback for environments that haven't installed langchain-chroma yet.
    # Suppress import-time deprecation noise while keeping runtime behavior.
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            category=LangChainDeprecationWarning,
            message=r".*langchain_community\.vectorstores\.Chroma.*",
        )
        from langchain_community.vectorstores import Chroma  # type: ignore

    _CHROMA_BACKEND = "langchain_community"

# 1. 初始化 Embedding 模型
if not os.getenv("DASHSCOPE_API_KEY"):
    print("⚠️ 警告: 未检测到 DASHSCOPE_API_KEY，记忆功能将无法使用")

embeddings = DashScopeEmbeddings(
    model="text-embedding-v3",
    dashscope_api_key=os.getenv("DASHSCOPE_API_KEY")
)

PERSIST_DIRECTORY = "./chroma_memory_db"
TOPIC_OPTION = "option"
TOPIC_STOCK_PORTFOLIO = "stock_portfolio"
TOPIC_GENERAL = "general"

OPTION_TOPIC_KEYWORDS = (
    "期权", "认购", "认沽", "行权价", "牛市价差", "熊市价差", "跨式", "宽跨", "勒式",
    "call", "put", "delta", "gamma", "vega", "theta", "iv", "波动率", "权利金",
)
STOCK_PORTFOLIO_TOPIC_KEYWORDS = (
    "持仓体检", "自动持仓体检", "我的持仓", "我的股票", "股票持仓", "持仓分析", "仓位", "调仓",
    "加仓", "减仓", "股票组合", "股票账户", "前3大持仓", "行业分布",
)


def _needs_manual_persist() -> bool:
    """
    Chroma 0.4+ persists automatically. Older versions may still require persist().
    """
    version = getattr(chromadb, "__version__", "")
    try:
        parts = str(version).split(".")
        major = int(parts[0])
        minor = int(parts[1]) if len(parts) > 1 else 0
        return (major, minor) < (0, 4)
    except Exception:
        return False


def get_vector_store():
    """获取或创建向量数据库实例"""
    # 修复逻辑：显式定义 Settings 对象，防止版本兼容性问题
    settings = Settings(
        anonymized_telemetry=False,
        is_persistent=True  # <--- [新增] 明确告诉它我要持久化
    )

    return Chroma(
        collection_name="user_chat_history",
        embedding_function=embeddings,
        persist_directory=PERSIST_DIRECTORY,
        client_settings=settings,
        # 强制指定 Cosine 距离
        collection_metadata={"hnsw:space": "cosine"}
    )


def classify_memory_topic(text: str) -> str:
    text_norm = str(text or "").strip().lower()
    if not text_norm:
        return TOPIC_GENERAL
    if any(kw in text_norm for kw in OPTION_TOPIC_KEYWORDS):
        return TOPIC_OPTION
    if any(kw in text_norm for kw in STOCK_PORTFOLIO_TOPIC_KEYWORDS):
        return TOPIC_STOCK_PORTFOLIO
    return TOPIC_GENERAL


def _normalize_topic(topic: str, fallback_text: str = "") -> str:
    topic_norm = str(topic or "").strip().lower()
    if topic_norm in {TOPIC_OPTION, TOPIC_STOCK_PORTFOLIO, TOPIC_GENERAL}:
        return topic_norm
    inferred = classify_memory_topic(fallback_text)
    return inferred if inferred else TOPIC_GENERAL


def save_interaction(user_id: str, user_input: str, ai_response: str, topic: str = "", source: str = ""):
    """
    [写入记忆] 带有显式持久化和错误检查
    """
    if not user_id: return

    print(f"💾 [记忆系统] 正在尝试为用户 {user_id} 写入记忆...")

    try:
        vector_store = get_vector_store()

        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
        content = f"[{timestamp}] 用户问: {user_input}\nAI回答: {ai_response}"
        topic_norm = _normalize_topic(topic, fallback_text=f"{user_input}\n{ai_response}")

        doc = Document(
            page_content=content,
            metadata={
                "user_id": str(user_id),
                "timestamp": timestamp,
                "topic": topic_norm,
                "source": str(source or ""),
            }
        )

        # 1. 添加文档
        vector_store.add_documents([doc])

        # Chroma 0.4+ 自动持久化；仅对老版本保留手动 persist。
        if _needs_manual_persist():
            try:
                vector_store.persist()
            except AttributeError:
                pass

        print("✅ [记忆系统] 写入成功！")

    except Exception as e:
        print(f"❌ [记忆系统] 写入失败 (CRITICAL): {e}")
        # 这里打印详细错误，方便我们在控制台看到原因


def retrieve_relevant_memory(
    user_id: str,
    query: str,
    k: int = 3,
    score_threshold: float = 0.5,
    query_topic: str = "",
    strict_topic: bool = False,
) -> str:
    """
    [读取记忆]
    注意：切换到 Cosine 后，Score 的范围变了：
    - 0.0 ~ 0.3: 高度相关 (几乎一样)
    - 0.3 ~ 0.6: 中度相关
    - > 0.7: 不太相关
    建议阈值设为 0.5 或 0.6
    """
    if not user_id:
        return ""

    try:
        vector_store = get_vector_store()
        query_topic_norm = _normalize_topic(query_topic, fallback_text=query)
        should_topic_filter = query_topic_norm != TOPIC_GENERAL

        results_with_score = vector_store.similarity_search_with_score(
            query,
            k=max(int(k or 3), 3) * (3 if (strict_topic and should_topic_filter) else 1),
            filter={"user_id": str(user_id)}
        )

        valid_memories = []
        print(f"🔍 [记忆检索] 用户问题: {query} | query_topic={query_topic_norm} | strict_topic={strict_topic}")

        for doc, score in results_with_score:
            doc_topic = _normalize_topic(
                str((doc.metadata or {}).get("topic", "")),
                fallback_text=doc.page_content,
            )
            # Cosine 距离：越小越相似
            if score >= score_threshold:
                print(f"  ❌ 忽略 (Dist: {score:.3f} > {score_threshold}): {doc.page_content[:30]}...")
                continue

            if should_topic_filter:
                if strict_topic and doc_topic != query_topic_norm:
                    print(f"  ❌ 主题不符 (doc_topic={doc_topic} != {query_topic_norm})")
                    continue
                if (not strict_topic) and doc_topic not in {query_topic_norm, TOPIC_GENERAL}:
                    print(f"  ❌ 主题不符 (doc_topic={doc_topic})")
                    continue

            valid_memories.append(doc.page_content)
            print(f"  ✅ 命中 (Dist: {score:.3f}, topic={doc_topic}): {doc.page_content[:30]}...")
            if len(valid_memories) >= max(int(k or 3), 1):
                break

        return "\n".join([f"- {m}" for m in valid_memories])

    except Exception as e:
        print(f"⚠️ [记忆检索] 读取出错: {e}")
        return ""

def search_memory(query: str, user_id: str = "default_user", top_k: int = 3):
    """
    [别名函数] 方便外部调用，底层复用 retrieve_relevant_memory
    """
    # 直接调用已有的 retrieve_relevant_memory
    # 注意：原函数的参数顺序是 (user_id, query, k, score_threshold)
    return retrieve_relevant_memory(
        user_id=user_id,
        query=query,
        k=top_k,
        score_threshold=0.5
    )
