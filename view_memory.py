import argparse
import os
import re
import textwrap
from datetime import datetime
from typing import Any, Optional

import pandas as pd
from dotenv import load_dotenv


def _clean_text(value: Any) -> str:
    text = str(value or "")
    text = text.replace("\u3000", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _truncate(text: str, max_len: int) -> str:
    max_len = max(int(max_len or 0), 0)
    normalized = _clean_text(text)
    if max_len <= 0 or len(normalized) <= max_len:
        return normalized
    return textwrap.shorten(normalized, width=max_len, placeholder="...")


def _parse_qa_fields(content: str) -> tuple[str, str]:
    text = str(content or "")
    q = ""
    a = ""
    if "用户问:" in text and "AI回答:" in text:
        try:
            left, right = text.split("AI回答:", 1)
            q = left.split("用户问:", 1)[1].strip()
            a = right.strip()
        except Exception:
            q = ""
            a = ""
    if "【回答片段】" in a:
        try:
            a = a.split("【回答片段】", 1)[1].strip()
        except Exception:
            pass
    if "【结构化摘要】" in a and "【回答片段】" not in a:
        try:
            a = a.split("【结构化摘要】", 1)[1].strip()
        except Exception:
            pass
    if not q:
        q = text[:120].strip()
    if not a:
        a = text[:240].strip()
    return _clean_text(q), _clean_text(a)


def _parse_ts(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y/%m/%d",
    ]
    for fmt in fmts:
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    try:
        parsed = pd.to_datetime(raw, errors="coerce")
        if pd.notna(parsed):
            return parsed.to_pydatetime()
    except Exception:
        pass
    return None


def _build_rows(data: dict) -> list[dict]:
    ids = data.get("ids") or []
    docs = data.get("documents") or []
    metas = data.get("metadatas") or []
    rows = []
    for i, _ in enumerate(ids):
        doc = str(docs[i] if i < len(docs) else "")
        meta = metas[i] if i < len(metas) and isinstance(metas[i], dict) else {}
        ts_raw = str(meta.get("timestamp", "") or "")
        question, answer = _parse_qa_fields(doc)
        rows.append(
            {
                "user_id": str(meta.get("user_id", "Unknown")),
                "time": ts_raw or "N/A",
                "ts": _parse_ts(ts_raw),
                "question": question,
                "answer": answer,
                "raw": doc,
            }
        )
    return rows


def _filter_rows(
    rows: list[dict],
    user: str = "",
    contains: str = "",
    since: str = "",
    until: str = "",
    limit: int = 200,
    order: str = "asc",
) -> list[dict]:
    out = list(rows)
    user = str(user or "").strip()
    contains = str(contains or "").strip().lower()

    if user:
        out = [r for r in out if str(r.get("user_id", "")).strip() == user]

    since_dt = _parse_ts(since)
    until_dt = _parse_ts(until)
    since_raw = str(since or "").strip()
    until_raw = str(until or "").strip()
    if until_dt and until_raw and (":" not in until_raw) and ("T" not in until_raw):
        until_dt = until_dt.replace(hour=23, minute=59, second=59)
    if since_dt and since_raw and (":" not in since_raw) and ("T" not in since_raw):
        since_dt = since_dt.replace(hour=0, minute=0, second=0)
    if since_dt:
        out = [
            r for r in out
            if r.get("ts") is None or r.get("ts") >= since_dt
        ]
    if until_dt:
        out = [
            r for r in out
            if r.get("ts") is None or r.get("ts") <= until_dt
        ]

    if contains:
        out = [
            r for r in out
            if contains in str(r.get("question", "")).lower()
            or contains in str(r.get("answer", "")).lower()
            or contains in str(r.get("raw", "")).lower()
        ]

    sort_desc = str(order or "asc").strip().lower() == "desc"
    out.sort(key=lambda x: x.get("ts") or datetime.min, reverse=sort_desc)
    if limit > 0:
        out = out[:limit]
    return out


def _load_vector_store():
    load_dotenv(override=True)
    if not os.getenv("DASHSCOPE_API_KEY"):
        raise RuntimeError("未读取到 DASHSCOPE_API_KEY，请检查 .env 配置")
    from memory_utils import get_vector_store

    return get_vector_store()


def _render_cards(rows: list[dict], max_question: int, max_answer: int) -> None:
    for idx, row in enumerate(rows, start=1):
        user_id = str(row.get("user_id", "Unknown")).strip() or "Unknown"
        ts = str(row.get("time", "N/A")).strip() or "N/A"
        question = _truncate(row.get("question", ""), max_question)
        answer = _truncate(row.get("answer", ""), max_answer)
        print(f"[{idx}] user={user_id}  time={ts}")
        print(f"Q: {question}")
        print(f"A: {answer}")
        print("-" * 80)


def _render_table(rows: list[dict], max_question: int, max_answer: int) -> None:
    df = pd.DataFrame(
        [
            {
                "User ID": str(r.get("user_id", "")),
                "Time": str(r.get("time", "")),
                "Question": _truncate(r.get("question", ""), max_question),
                "Answer Snippet": _truncate(r.get("answer", ""), max_answer),
            }
            for r in rows
        ]
    )
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_colwidth", max(max_question, max_answer))
    pd.set_option("display.width", 200)
    print(df.to_string(index=False))


def view_all_memories(
    user: str = "",
    limit: int = 200,
    contains: str = "",
    since: str = "",
    until: str = "",
    output_format: str = "card",
    max_question: int = 80,
    max_answer: int = 160,
    order: str = "asc",
) -> int:
    print("=== 📖 正在读取本地向量记忆库... ===")
    vector_store = _load_vector_store()
    data = vector_store.get(limit=20000)
    ids = data.get("ids") or []
    if not ids:
        print("📭 记忆库是空的。")
        return 0

    rows = _build_rows(data)
    rows = _filter_rows(
        rows,
        user=user,
        contains=contains,
        since=since,
        until=until,
        limit=limit,
        order=order,
    )
    if not rows:
        print("📭 未匹配到记录。")
        return 0

    print(f"✅ 共匹配到 {len(rows)} 条记忆：")
    print("-" * 80)
    fmt = str(output_format or "card").strip().lower()
    if fmt == "table":
        _render_table(rows, max_question=max_question, max_answer=max_answer)
    else:
        _render_cards(rows, max_question=max_question, max_answer=max_answer)
    print("-" * 80)
    return len(rows)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="查看向量资料库中的历史问答记录")
    parser.add_argument("--user", default="", help="按用户名过滤")
    parser.add_argument("--limit", type=int, default=200, help="最多输出条数，默认 200")
    parser.add_argument("--contains", default="", help="按关键词过滤（问题/回答/原文）")
    parser.add_argument("--since", default="", help="起始时间（如 2026-03-01）")
    parser.add_argument("--until", default="", help="结束时间（如 2026-03-31）")
    parser.add_argument("--format", default="card", choices=["card", "table"], help="输出模式：card(默认) 或 table")
    parser.add_argument("--max-question", type=int, default=80, help="问题最大展示长度，默认 80")
    parser.add_argument("--max-answer", type=int, default=160, help="回答最大展示长度，默认 160")
    parser.add_argument("--order", default="asc", choices=["asc", "desc"], help="时间排序：asc=旧到新(最新在最下)，desc=新到旧")
    return parser


def main(argv=None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        view_all_memories(
            user=args.user,
            limit=max(int(args.limit or 0), 0),
            contains=args.contains,
            since=args.since,
            until=args.until,
            output_format=args.format,
            max_question=max(int(args.max_question or 0), 20),
            max_answer=max(int(args.max_answer or 0), 20),
            order=args.order,
        )
        return 0
    except Exception as e:
        print(f"❌ 读取失败: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
