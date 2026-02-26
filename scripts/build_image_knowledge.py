#!/usr/bin/env python3
"""
离线构建图片知识库：
1) 扫描 knowledge_docs/images 下的 PNG/JPG/JPEG
2) 上传原图到 OSS（私有桶）
3) 用 qwen-vl-plus 抽取 OCR/摘要/标签
4) 将抽取文本写入 Qdrant finance_knowledge 集合（文本向量）
"""

from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
import os
import re
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List

import dashscope
from dotenv import load_dotenv
from langchain_community.embeddings import DashScopeEmbeddings
from qdrant_client import QdrantClient
from qdrant_client.http import models

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.oss_utils import upload_bytes, is_oss_configured


def load_env_with_fallback() -> str:
    candidates = [REPO_ROOT / ".env", REPO_ROOT.parent / ".env"]
    for path in candidates:
        if path.exists():
            load_dotenv(dotenv_path=path, override=False)
            return str(path)
    return "NOT_FOUND"


def compute_image_id(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def build_point_uuid(image_id: str) -> str:
    """将业务 image_id 稳定映射为 Qdrant 可接受的 UUID。"""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"knowledge-image:{image_id}"))


def parse_mm_text(resp: Any) -> str:
    try:
        content = resp.output.choices[0].message.content
        if isinstance(content, list):
            return "\n".join(item.get("text", "") for item in content if isinstance(item, dict)).strip()
        return str(content).strip()
    except Exception:
        return ""


def extract_image_knowledge(data: bytes, suffix: str, model: str) -> Dict[str, Any]:
    # dashscope 支持 data url，使用 base64 编码。
    import base64

    b64 = base64.b64encode(data).decode("utf-8")
    mime = mimetypes.types_map.get(suffix.lower(), "image/jpeg")
    img_url = f"data:{mime};base64,{b64}"

    prompt = (
        "请对这张金融相关图片做结构化抽取，并仅返回JSON："
        '{"title":"", "summary_text":"", "ocr_text":"", "tags":[""]}。'
        "要求：summary_text 100-300字，tags 3-8个，禁止输出JSON以外文本。"
    )

    resp = dashscope.MultiModalConversation.call(
        model=model,
        messages=[{"role": "user", "content": [{"image": img_url}, {"text": prompt}]}],
        api_key=os.getenv("DASHSCOPE_API_KEY"),
    )
    if int(getattr(resp, "status_code", 0)) != 200:
        raise RuntimeError(f"视觉抽取失败: {getattr(resp, 'code', '')} {getattr(resp, 'message', '')}")

    text = parse_mm_text(resp)
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?|```$", "", text, flags=re.IGNORECASE | re.MULTILINE).strip()

    try:
        obj = json.loads(text)
    except Exception:
        # 兜底：把原文本压成摘要
        obj = {
            "title": "图片知识",
            "summary_text": text[:260],
            "ocr_text": text[:500],
            "tags": [],
        }

    title = str(obj.get("title", "")).strip() or "图片知识"
    summary = str(obj.get("summary_text", "")).strip()[:600]
    ocr_text = str(obj.get("ocr_text", "")).strip()[:4000]
    tags_raw = obj.get("tags", [])
    tags = [str(x).strip() for x in tags_raw if str(x).strip()] if isinstance(tags_raw, list) else []
    return {
        "title": title,
        "summary_text": summary,
        "ocr_text": ocr_text,
        "tags": tags,
    }


def build_retrieval_text(meta: Dict[str, Any], source: str) -> str:
    tags = "、".join(meta.get("tags", []))
    return (
        f"标题：{meta.get('title', '')}\n"
        f"来源：{source}\n"
        f"标签：{tags}\n"
        f"摘要：{meta.get('summary_text', '')}\n"
        f"OCR：{meta.get('ocr_text', '')}"
    ).strip()


def ensure_collection(client: QdrantClient, collection_name: str, embeddings: DashScopeEmbeddings) -> None:
    try:
        client.get_collection(collection_name)
        return
    except Exception:
        pass

    sample_vec = embeddings.embed_query("finance image knowledge")
    client.recreate_collection(
        collection_name=collection_name,
        vectors_config=models.VectorParams(size=len(sample_vec), distance=models.Distance.COSINE),
    )


def maybe_clear_image_docs(client: QdrantClient, collection_name: str) -> None:
    try:
        client.delete(
            collection_name=collection_name,
            points_selector=models.FilterSelector(
                filter=models.Filter(
                    must=[models.FieldCondition(key="doc_type", match=models.MatchValue(value="image"))]
                )
            ),
        )
    except Exception as e:
        print(f"[WARN] 清理旧图片文档失败（可忽略）: {e}")


def upsert_image_doc(
    client: QdrantClient,
    collection_name: str,
    point_id: str,
    vector: List[float],
    payload: Dict[str, Any],
) -> None:
    point = models.PointStruct(id=point_id, vector=vector, payload=payload)
    client.upsert(collection_name=collection_name, points=[point], wait=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build image knowledge into Qdrant")
    parser.add_argument("--source-dir", default=str(REPO_ROOT / "knowledge_docs" / "images"))
    parser.add_argument("--qdrant-path", default=str(REPO_ROOT / "qdrant_db"))
    parser.add_argument("--collection", default="finance_knowledge")
    parser.add_argument("--model", default="qwen-vl-plus")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--rebuild", action="store_true")
    args = parser.parse_args()

    env_path = load_env_with_fallback()
    print("=== Build Image Knowledge ===")
    print(f"Repo: {REPO_ROOT}")
    print(f"Dotenv: {env_path}")
    print(f"Source dir: {args.source_dir}")
    print(f"Qdrant path: {args.qdrant_path}")
    print(f"Collection: {args.collection}")
    print(f"Dry-run: {args.dry_run}")
    print(f"Rebuild: {args.rebuild}")

    if not os.getenv("DASHSCOPE_API_KEY"):
        print("❌ 缺少 DASHSCOPE_API_KEY")
        return 2
    if not is_oss_configured() and not args.dry_run:
        print("❌ OSS 配置不完整，无法上传图片。可先用 --dry-run 预检。")
        return 2

    src_dir = Path(args.source_dir)
    if not src_dir.exists():
        src_dir.mkdir(parents=True, exist_ok=True)
        print(f"ℹ️ 图片目录不存在，已自动创建: {src_dir}")
        print("请放入 PNG/JPG/JPEG 图片后重新运行。")
        return 0

    all_images: List[Path] = []
    for pattern in ("**/*.png", "**/*.jpg", "**/*.jpeg", "**/*.PNG", "**/*.JPG", "**/*.JPEG"):
        all_images.extend(src_dir.glob(pattern))
    all_images = sorted({p.resolve() for p in all_images})
    if args.limit and args.limit > 0:
        all_images = all_images[: args.limit]

    print(f"待处理图片数: {len(all_images)}")
    if not all_images:
        return 0

    embeddings = DashScopeEmbeddings(model="text-embedding-v1")
    client = QdrantClient(path=args.qdrant_path)
    ensure_collection(client, args.collection, embeddings)
    if args.rebuild:
        maybe_clear_image_docs(client, args.collection)

    ok = 0
    failed = 0
    start = time.time()

    for idx, image_path in enumerate(all_images, start=1):
        try:
            raw = image_path.read_bytes()
            image_id = compute_image_id(raw)
            point_id = build_point_uuid(image_id)
            suffix = image_path.suffix.lower()
            content_type = mimetypes.types_map.get(suffix, "application/octet-stream")
            rel_source = str(image_path.relative_to(REPO_ROOT))
            oss_key = f"knowledge-images/{image_id}{suffix}"

            print(f"[{idx}/{len(all_images)}] {rel_source} -> {oss_key}")
            if args.dry_run:
                ok += 1
                continue

            uploaded = upload_bytes(oss_key, raw, content_type=content_type)
            if not uploaded:
                failed += 1
                print("  ❌ OSS 上传失败")
                continue

            meta = extract_image_knowledge(raw, suffix=suffix, model=args.model)
            retrieval_text = build_retrieval_text(meta, rel_source)
            vector = embeddings.embed_query(retrieval_text)

            payload = {
                "doc_type": "image",
                "image_id": image_id,
                "point_id": point_id,
                "oss_key": oss_key,
                "title": meta["title"],
                "tags": meta["tags"],
                "source": rel_source,
                "ocr_text": meta["ocr_text"],
                "summary_text": meta["summary_text"],
                "page_content": retrieval_text,
                "metadata": {
                    "doc_type": "image",
                    "image_id": image_id,
                    "point_id": point_id,
                    "oss_key": oss_key,
                    "title": meta["title"],
                    "tags": meta["tags"],
                    "source": rel_source,
                    "summary_text": meta["summary_text"],
                },
            }

            upsert_image_doc(
                client=client,
                collection_name=args.collection,
                point_id=point_id,
                vector=vector,
                payload=payload,
            )
            ok += 1
        except Exception as e:
            failed += 1
            print(f"  ❌ 处理失败: {e}")

    spent = time.time() - start
    print("=== Done ===")
    print(f"成功: {ok} | 失败: {failed} | 耗时: {spent:.1f}s")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
