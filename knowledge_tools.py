import os
from qdrant_client import QdrantClient
from langchain_community.embeddings import DashScopeEmbeddings
from langchain_core.tools import tool
from dotenv import load_dotenv

# 1. 初始化
load_dotenv(override=True)

# --- 【关键修复】强制清除代理，确保直连阿里云 DashScope ---
# 避免 SSLError: EOF occurred in violation of protocol
for key in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"]:
    if key in os.environ:
        del os.environ[key]
# -------------------------------------------------------

if not os.getenv("DASHSCOPE_API_KEY"):
    print("❌ 错误：未找到 DASHSCOPE_API_KEY")



# 初始化 Embedding 模型 (用于把问题变成向量)
embeddings = DashScopeEmbeddings(model="text-embedding-v1")


# 2. 定义工具 (Tool)
@tool
def search_investment_knowledge(query: str):
    """
    【投资知识库检索】
    当用户询问具体的金融概念、交易策略、行业研报观点或宏观经济逻辑时，**必须**调用此工具。
    输入：搜索关键词（例如 "MACD战法"、"碳酸锂供需"）。
    输出：相关的知识片段。
    """
    if not os.path.exists("./qdrant_db"):
        return "知识库尚未构建，请联系管理员运行 build_knowledge.py。"

    try:
        # --- A. 初始化原生客户端 ---
        client = QdrantClient(path="./qdrant_db")
        collection_name = "finance_knowledge"

        # --- B. 生成查询向量 ---
        # (这一步需要连 DashScope，清除代理后应该就稳了)
        query_vector = embeddings.embed_query(query)

        # --- C. 执行搜索 (原生方法) ---
        # 新版 QdrantClient 用 query_points 替代了 search
        hits = client.query_points(
            collection_name=collection_name,
            query=query_vector,  # 参数名变成了 query
            limit=3
        ).points  # 注意：返回值里要取 .points 属性

        if not hits:
            return f"在知识库中未找到关于 '{query}' 的内容。"

        # --- D. 解析结果 ---
        # hits 是 ScoredPoint 对象列表
        result = f"📚 关于 '{query}' 的参考资料：\n"
        for i, hit in enumerate(hits):
            # 从 payload 中提取内容
            payload = hit.payload
            if payload:
                content = payload.get('page_content', '')
                metadata = payload.get('metadata', {})
                source = metadata.get('source', '未知来源')
                source_name = os.path.basename(source)

                result += f"\n--- 来源: {source_name} (匹配度: {hit.score:.2f}) ---\n{content}\n"

        return result

    except AttributeError as ae:
        return f"客户端版本不兼容: {ae}。请尝试运行 `pip install -U qdrant-client` 更新。"

    except Exception as e:
        # 【关键修改】在控制台打印详细错误堆栈，方便在 PyCharm 中查看
        import traceback
        print(f"❌ 知识库检索出错: {e}")
        traceback.print_exc()
        # 返回给 AI 的信息
        return f"当前知识库检索功能暂时异常: {str(e)}"


# 测试用
if __name__ == "__main__":
    # 确保已运行过 build_knowledge.py
    print(search_investment_knowledge.invoke({"query": "期权"}))