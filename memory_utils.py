import os
from langchain_community.vectorstores import Chroma
from langchain_community.embeddings import DashScopeEmbeddings
from langchain_core.documents import Document
from datetime import datetime
from chromadb.config import Settings
import chromadb

# 1. 初始化 Embedding 模型
if not os.getenv("DASHSCOPE_API_KEY"):
    print("⚠️ 警告: 未检测到 DASHSCOPE_API_KEY，记忆功能将无法使用")

embeddings = DashScopeEmbeddings(
    model="text-embedding-v3",
    dashscope_api_key=os.getenv("DASHSCOPE_API_KEY")
)

PERSIST_DIRECTORY = "./chroma_memory_db"


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


def save_interaction(user_id: str, user_input: str, ai_response: str):
    """
    [写入记忆] 带有显式持久化和错误检查
    """
    if not user_id: return

    print(f"💾 [记忆系统] 正在尝试为用户 {user_id} 写入记忆...")

    try:
        vector_store = get_vector_store()

        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
        content = f"[{timestamp}] 用户问: {user_input}\nAI回答: {ai_response}"

        doc = Document(
            page_content=content,
            metadata={"user_id": str(user_id), "timestamp": timestamp}
        )

        # 1. 添加文档
        vector_store.add_documents([doc])

        # 2. [关键修复] 显式强制持久化 (解决文件夹不生成的问题)
        # 尝试调用 persist，如果版本太新没有这个方法，则忽略(新版会自动存)
        try:
            vector_store.persist()
        except AttributeError:
            pass

        print("✅ [记忆系统] 写入成功！")

    except Exception as e:
        print(f"❌ [记忆系统] 写入失败 (CRITICAL): {e}")
        # 这里打印详细错误，方便我们在控制台看到原因


def retrieve_relevant_memory(user_id: str, query: str, k=3, score_threshold=0.5) -> str:
    """
    [读取记忆]
    注意：切换到 Cosine 后，Score 的范围变了：
    - 0.0 ~ 0.3: 高度相关 (几乎一样)
    - 0.3 ~ 0.6: 中度相关
    - > 0.7: 不太相关
    建议阈值设为 0.5 或 0.6
    """
    if not user_id: return ""

    try:
        vector_store = get_vector_store()

        results_with_score = vector_store.similarity_search_with_score(
            query,
            k=k,
            filter={"user_id": str(user_id)}
        )

        valid_memories = []
        print(f"🔍 [记忆检索] 用户问题: {query}")

        for doc, score in results_with_score:
            # Cosine 距离：越小越相似
            if score < score_threshold:
                valid_memories.append(doc.page_content)
                print(f"  ✅ 命中 (Dist: {score:.3f}): {doc.page_content[:30]}...")
            else:
                print(f"  ❌ 忽略 (Dist: {score:.3f} > {score_threshold}): {doc.page_content[:30]}...")

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
