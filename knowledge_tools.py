import os
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv
from langchain_community.embeddings import DashScopeEmbeddings
from langchain_core.tools import tool
from qdrant_client import QdrantClient


load_dotenv(override=True)

# --- 强制清理代理，避免 DashScope 走代理导致握手异常 ---
for key in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"]:
    if key in os.environ:
        del os.environ[key]

if not os.getenv("DASHSCOPE_API_KEY"):
    print("❌ 错误：未找到 DASHSCOPE_API_KEY")

embeddings = DashScopeEmbeddings(model="text-embedding-v1")
COLLECTION_NAME = "finance_knowledge"


def _resolve_qdrant_path() -> str:
    """兼容本地与服务器目录结构。"""
    base_dir = Path(__file__).resolve().parent
    candidates = [
        base_dir / "qdrant_db",
        base_dir.parent / "future-app" / "qdrant_db",
        Path("../future-app/qdrant_db"),
    ]
    for path in candidates:
        if path.exists():
            return str(path)
    return str(candidates[0])


def _safe_score(hit: Any) -> float:
    try:
        return float(getattr(hit, "score", 0.0))
    except Exception:
        return 0.0


def _parse_hit(hit: Any) -> Dict[str, Any]:
    payload = getattr(hit, "payload", {}) or {}
    metadata = payload.get("metadata", {})
    if not isinstance(metadata, dict):
        metadata = {}

    score = _safe_score(hit)
    source = metadata.get("source") or payload.get("source") or "未知来源"
    content = str(payload.get("page_content", "") or "").strip()
    doc_type = payload.get("doc_type") or metadata.get("doc_type")
    image_id = payload.get("image_id") or metadata.get("image_id")
    oss_key = payload.get("oss_key") or metadata.get("oss_key")

    if not doc_type:
        doc_type = "image" if (image_id or oss_key) else "text"

    title = (
        payload.get("title")
        or metadata.get("title")
        or os.path.basename(str(source))
        or "图片知识"
    )
    tags = payload.get("tags") or metadata.get("tags") or []
    if not isinstance(tags, list):
        tags = [str(tags)]

    return {
        "score": score,
        "doc_type": doc_type,
        "source": str(source),
        "content": content,
        "image_id": image_id,
        "oss_key": oss_key,
        "title": str(title),
        "tags": [str(t).strip() for t in tags if str(t).strip()],
        "summary_text": str(payload.get("summary_text") or metadata.get("summary_text") or "").strip(),
        "ocr_text": str(payload.get("ocr_text") or "").strip(),
    }


def search_knowledge_structured(
    query: str,
    limit: int = 6,
    image_limit: int = 3,
    min_score: float = 0.0,
) -> Dict[str, Any]:
    """
    结构化检索：返回 text_hits + image_hits，供任务层做附件组装。
    """
    qdrant_path = _resolve_qdrant_path()
    if not os.path.exists(qdrant_path):
        return {
            "query": query,
            "text_hits": [],
            "image_hits": [],
            "error": "知识库尚未构建，请联系管理员运行 build_knowledge.py。",
        }

    try:
        client = QdrantClient(path=qdrant_path)
        query_vector = embeddings.embed_query(query)
        hits = client.query_points(
            collection_name=COLLECTION_NAME,
            query=query_vector,
            limit=max(limit, image_limit),
        ).points
    except Exception as e:
        return {
            "query": query,
            "text_hits": [],
            "image_hits": [],
            "error": f"当前知识库检索功能暂时异常: {e}",
        }

    text_hits: List[Dict[str, Any]] = []
    image_hits: List[Dict[str, Any]] = []

    for hit in hits:
        parsed = _parse_hit(hit)
        if parsed["score"] < min_score:
            continue
        if parsed["doc_type"] == "image":
            image_hits.append(parsed)
        else:
            text_hits.append(parsed)

    text_hits = sorted(text_hits, key=lambda x: x["score"], reverse=True)[:limit]
    image_hits = sorted(image_hits, key=lambda x: x["score"], reverse=True)[:image_limit]

    return {
        "query": query,
        "text_hits": text_hits,
        "image_hits": image_hits,
        "error": None,
    }


@tool
def search_investment_knowledge(query: str):
    """
    【投资知识库检索】
    当用户询问期权策略、波动率、K线、商品基本面、交易理念时，建议调用此工具。
    输入：用户的搜索关键词或完整问题。
    输出：面向模型的文本参考片段（兼容旧流程）。
    """
    data = search_knowledge_structured(query=query, limit=6, image_limit=3, min_score=0.0)
    if data.get("error"):
        return data["error"]

    text_hits = data.get("text_hits", [])
    image_hits = data.get("image_hits", [])
    if not text_hits and not image_hits:
        return f"在知识库中未找到关于 '{query}' 的内容。"

    result = [f"📚 关于 '{query}' 的参考资料："]

    for hit in text_hits:
        source_name = os.path.basename(hit.get("source", "未知来源"))
        content = hit.get("content", "")
        result.append(
            f"\n--- 来源: {source_name} (匹配度: {hit.get('score', 0.0):.2f}) ---\n{content}"
        )

    if image_hits:
        result.append("\n--- 图片参考 ---")
        for hit in image_hits:
            source_name = os.path.basename(hit.get("source", "未知来源"))
            result.append(
                f"🖼️ {hit.get('title', '图片知识')} | 来源: {source_name} | 匹配度: {hit.get('score', 0.0):.2f}"
            )

    return "\n".join(result).strip()


if __name__ == "__main__":
    print(search_investment_knowledge.invoke({"query": "期权"}))
