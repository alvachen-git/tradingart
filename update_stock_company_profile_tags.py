from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

import pandas as pd
import requests
import tushare as ts
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(override=True)


def _configure_langsmith_tracing() -> None:
    """
    默认关闭 LangSmith tracing，避免无权限环境下 403 日志风暴干扰批处理。
    如需开启，可设置 ENABLE_LANGSMITH_TRACING=1。
    """
    enabled = str(os.getenv("ENABLE_LANGSMITH_TRACING", "0")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if enabled:
        return
    os.environ["LANGCHAIN_TRACING_V2"] = "false"
    os.environ["LANGSMITH_TRACING"] = "false"
    os.environ["LANGCHAIN_CALLBACKS_BACKGROUND"] = "false"


_configure_langsmith_tracing()

from llm_compat import ChatTongyiCompat
from industry_chain_tools import load_chain_templates, fetch_stage_members_from_tushare

TEMPLATE_PATH = Path(__file__).resolve().parent / "static" / "industry_chain_templates.json"
INSIGHT_MODEL_ENV = "DOMAIN_INSIGHT_MODEL"
DEFAULT_INSIGHT_MODEL = "qwen-plus"

OFFICIAL_DOMAINS = {
    "cninfo.com.cn",
    "sse.com.cn",
    "szse.cn",
    "szse.com.cn",
}

MEDIA_DOMAINS = {
    "stcn.com",
    "cnstock.com",
    "cs.com.cn",
    "cls.cn",
    "finance.sina.com.cn",
    "eastmoney.com",
    "10jqka.com.cn",
}


@dataclass
class EvidenceItem:
    url: str
    title: str
    domain: str
    published_at: str
    fetched_at: str
    snippet: str
    source_type: str  # official | media


# --------------------------
# 基础连接
# --------------------------
def get_db_engine():
    user = os.getenv("DB_USER")
    pwd = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "3306")
    name = os.getenv("DB_NAME")
    if not all([user, pwd, host, name]):
        return None
    url = f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{name}"
    return create_engine(url, pool_pre_ping=True, pool_recycle=3600)


def get_tushare_pro():
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        return None
    ts.set_token(token)
    return ts.pro_api()


def build_tag_llm():
    api_key = os.getenv("DASHSCOPE_API_KEY")
    if not api_key:
        return None
    model = os.getenv("DOMAIN_TAG_MODEL", DEFAULT_INSIGHT_MODEL)
    try:
        return ChatTongyiCompat(model=model, api_key=api_key, temperature=0.0, streaming=False)
    except Exception:
        return None


def build_insight_llm():
    api_key = os.getenv("DASHSCOPE_API_KEY")
    if not api_key:
        return None
    model = os.getenv(INSIGHT_MODEL_ENV, DEFAULT_INSIGHT_MODEL)
    try:
        return ChatTongyiCompat(model=model, api_key=api_key, temperature=0.1, streaming=False)
    except Exception:
        return None


# --------------------------
# DDL
# --------------------------
def _ensure_column(engine, table: str, column_ddl: str) -> None:
    try:
        with engine.begin() as conn:
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column_ddl}"))
    except Exception:
        # 已存在/数据库差异均忽略
        return


def _ensure_index(engine, table: str, index_sql: str) -> None:
    try:
        with engine.begin() as conn:
            conn.execute(text(index_sql))
    except Exception:
        return


def ensure_stock_company_profile_cache_table(engine):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS stock_company_profile_cache (
                  ts_code VARCHAR(16) NOT NULL,
                  company_name VARCHAR(128) DEFAULT '',
                  exchange VARCHAR(16) DEFAULT '',
                  main_business TEXT,
                  business_scope LONGTEXT,
                  domain_tags VARCHAR(255) DEFAULT '',
                  tags_model VARCHAR(64) DEFAULT '',
                  profile_hash VARCHAR(64) DEFAULT '',
                  source_updated_at DATETIME NULL,
                  tags_updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                  domain_insight_text TEXT,
                  tech_highlights VARCHAR(512) DEFAULT '',
                  customer_profile VARCHAR(512) DEFAULT '',
                  moat_note VARCHAR(512) DEFAULT '',
                  boundary_risk VARCHAR(512) DEFAULT '',
                  insight_sources_json LONGTEXT,
                  insight_hash VARCHAR(64) DEFAULT '',
                  insight_model VARCHAR(64) DEFAULT '',
                  insight_updated_at DATETIME NULL,
                  insight_quality_score DOUBLE DEFAULT 0,
                  PRIMARY KEY (ts_code),
                  KEY idx_tags_updated_at (tags_updated_at),
                  KEY idx_insight_updated_at (insight_updated_at),
                  KEY idx_insight_hash (insight_hash)
                ) DEFAULT CHARSET=utf8mb4
                """
            )
        )

    # 兼容历史已存在表
    _ensure_column(engine, "stock_company_profile_cache", "domain_insight_text TEXT")
    _ensure_column(engine, "stock_company_profile_cache", "tech_highlights VARCHAR(512) DEFAULT ''")
    _ensure_column(engine, "stock_company_profile_cache", "customer_profile VARCHAR(512) DEFAULT ''")
    _ensure_column(engine, "stock_company_profile_cache", "moat_note VARCHAR(512) DEFAULT ''")
    _ensure_column(engine, "stock_company_profile_cache", "boundary_risk VARCHAR(512) DEFAULT ''")
    _ensure_column(engine, "stock_company_profile_cache", "insight_sources_json LONGTEXT")
    _ensure_column(engine, "stock_company_profile_cache", "insight_hash VARCHAR(64) DEFAULT ''")
    _ensure_column(engine, "stock_company_profile_cache", "insight_model VARCHAR(64) DEFAULT ''")
    _ensure_column(engine, "stock_company_profile_cache", "insight_updated_at DATETIME NULL")
    _ensure_column(engine, "stock_company_profile_cache", "insight_quality_score DOUBLE DEFAULT 0")

    _ensure_index(
        engine,
        "stock_company_profile_cache",
        "CREATE INDEX idx_insight_updated_at ON stock_company_profile_cache(insight_updated_at)",
    )
    _ensure_index(
        engine,
        "stock_company_profile_cache",
        "CREATE INDEX idx_insight_hash ON stock_company_profile_cache(insight_hash)",
    )


# --------------------------
# 通用工具
# --------------------------
def _norm_code(code: Any) -> str:
    return str(code or "").strip().upper()


def normalize_domain_tags(tags: List[str], max_tags: int = 3) -> List[str]:
    out: List[str] = []
    seen = set()
    for tag in tags:
        t = str(tag or "").strip()
        t = re.sub(r"[\s\-_/]+", "", t)
        t = re.sub(r"[，。,；;、|]+", "", t)
        if not t:
            continue
        if len(t) < 2:
            continue
        if len(t) > 8:
            t = t[:8]
        if t not in seen:
            out.append(t)
            seen.add(t)
        if len(out) >= max(1, int(max_tags)):
            break
    return out


def normalize_point_items(items: Sequence[str], max_items: int = 2) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in items:
        s = str(item or "").strip()
        s = re.sub(r"\s+", "", s)
        s = s.strip("，。；;、")
        if not s:
            continue
        if len(s) > 64:
            s = s[:64]
        if s not in seen:
            out.append(s)
            seen.add(s)
        if len(out) >= max(1, int(max_items)):
            break
    return out


def fallback_extract_domain_tags(text_value: str) -> List[str]:
    text_l = str(text_value or "").lower()

    mapping = [
        ("芯片设计", ["芯片设计", "ic设计", "soc", "eda", "ip核", "数字芯片", "模拟芯片"]),
        ("晶圆制造", ["晶圆", "代工", "wafer", "fab"]),
        ("封装测试", ["封装", "封测", "测试服务"]),
        ("半导体材料", ["光刻胶", "硅片", "靶材", "材料", "电子特气", "抛光液"]),
        ("半导体设备", ["刻蚀", "薄膜", "清洗", "检测设备", "设备"]),
        ("功率器件", ["igbt", "mosfet", "功率半导体", "功率器件"]),
        ("存储芯片", ["dram", "nand", "存储"]),
        ("传感器", ["传感器", "cmos", "光学"]),
        ("汽车电子", ["车规", "汽车电子", "新能源车"]),
        ("消费电子", ["消费电子", "手机", "pc", "可穿戴"]),
        ("工业控制", ["工业控制", "工控", "自动化"]),
        ("AI算力", ["ai", "算力", "服务器", "数据中心"]),
    ]

    hit = [tag for tag, kws in mapping if any(k in text_l for k in kws)]
    hit = normalize_domain_tags(hit)
    if hit:
        return hit

    words = re.findall(r"[\u4e00-\u9fa5]{2,8}", str(text_value or ""))
    words = [w for w in words if w not in {"公司", "业务", "产品", "技术", "服务", "客户", "市场", "以及"}]
    return normalize_domain_tags(words[:6]) or ["综合业务"]


def parse_llm_tags(content: str) -> List[str]:
    s = str(content or "").strip()
    if not s:
        return []

    if "[" in s and "]" in s:
        try:
            arr = json.loads(s[s.find("[") : s.rfind("]") + 1])
            if isinstance(arr, list):
                return normalize_domain_tags([str(x) for x in arr])
        except Exception:
            pass

    for sep in ["|", ",", "，", "、", "/", ";", "；", "\n"]:
        s = s.replace(sep, "|")
    return normalize_domain_tags([x.strip() for x in s.split("|") if x.strip()])


def ai_extract_domain_tags(text_value: str, llm) -> List[str]:
    if llm is None:
        return []
    prompt = (
        "请根据以下公司主营描述，提取1-3个中文短标签表示其擅长业务领域。"
        "要求：每个标签2-8字，不要解释，不要编号，只输出JSON数组。\n\n"
        f"描述：{text_value[:1500]}"
    )
    try:
        resp = llm.invoke(prompt)
        content = getattr(resp, "content", resp)
        return parse_llm_tags(str(content))
    except Exception:
        return []


def compute_profile_hash(main_business: str, business_scope: str) -> str:
    raw = f"{main_business or ''}\n{business_scope or ''}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _safe_datetime(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    s = str(v).strip()
    if not s:
        return None
    s = s.replace("T", " ")
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y%m%d"]:
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    try:
        return datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _extract_json_object(raw: str) -> Dict[str, Any]:
    s = str(raw or "").strip()
    if not s:
        return {}
    if "{" in s and "}" in s:
        s = s[s.find("{") : s.rfind("}") + 1]
    try:
        obj = json.loads(s)
        if isinstance(obj, dict):
            return obj
    except Exception:
        return {}
    return {}


def _normalize_url(url: str) -> str:
    u = str(url or "").strip()
    if not u:
        return ""
    if u.startswith("//"):
        u = "https:" + u
    return u


def _extract_domain(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower().strip()
    except Exception:
        return ""
    return host[4:] if host.startswith("www.") else host


def _is_allowed_domain(domain: str) -> bool:
    d = str(domain or "").lower().strip()
    if not d:
        return False
    for suffix in OFFICIAL_DOMAINS | MEDIA_DOMAINS:
        if d == suffix or d.endswith("." + suffix):
            return True
    return False


def _source_type_from_domain(domain: str) -> str:
    d = str(domain or "").lower().strip()
    for suffix in OFFICIAL_DOMAINS:
        if d == suffix or d.endswith("." + suffix):
            return "official"
    return "media"


def _strip_html_text(html: str, max_len: int = 1800) -> str:
    soup = BeautifulSoup(html or "", "html.parser")
    parts: List[str] = []

    title = (soup.title.get_text(" ", strip=True) if soup.title else "").strip()
    if title:
        parts.append(title)

    for p in soup.find_all(["p", "article", "section", "div"]):
        txt = p.get_text(" ", strip=True)
        txt = re.sub(r"\s+", " ", txt)
        if not txt:
            continue
        if len(txt) < 20:
            continue
        parts.append(txt)
        if sum(len(x) for x in parts) >= max_len:
            break

    merged = " ".join(parts)
    merged = re.sub(r"\s+", " ", merged).strip()
    return merged[:max_len]


def _extract_published_at(text_blob: str) -> str:
    s = str(text_blob or "")
    m = re.search(r"(20\d{2})[-/.年](\d{1,2})[-/.月](\d{1,2})", s)
    if not m:
        return ""
    y, mm, dd = m.groups()
    return f"{int(y):04d}-{int(mm):02d}-{int(dd):02d}"


def _snippet_from_text(text_blob: str, min_len: int = 200, max_len: int = 400) -> str:
    s = re.sub(r"\s+", "", str(text_blob or ""))
    if len(s) <= max_len:
        return s
    if len(s) < min_len:
        return s
    return s[:max_len]


def dedupe_evidences(items: List[EvidenceItem]) -> List[EvidenceItem]:
    out: List[EvidenceItem] = []
    seen_url = set()
    seen_title = set()
    for item in items:
        u = _normalize_url(item.url)
        t = str(item.title or "").strip()
        if not u or not t:
            continue
        if u in seen_url or t in seen_title:
            continue
        seen_url.add(u)
        seen_title.add(t)
        out.append(item)
    return out


def _rank_evidences(items: List[EvidenceItem]) -> List[EvidenceItem]:
    def _score(x: EvidenceItem) -> Tuple[int, int, str]:
        st = 1 if x.source_type == "official" else 0
        pub = str(x.published_at or "")
        return (st, int(pub.replace("-", "") or "0"), x.url)

    return sorted(items, key=_score, reverse=True)


def _search_web_urls(query: str, max_results: int = 8) -> List[str]:
    try:
        from ddgs import DDGS
    except Exception:
        return []

    urls: List[str] = []
    try:
        with DDGS() as ddgs:
            for row in ddgs.text(query, max_results=max_results):
                u = _normalize_url(str((row or {}).get("href") or ""))
                if u:
                    urls.append(u)
    except Exception:
        return []
    return urls


def _fetch_single_evidence(
    session: requests.Session,
    url: str,
    company_name: str,
    business_hint: str,
    timeout_sec: int = 8,
) -> Optional[EvidenceItem]:
    domain = _extract_domain(url)
    if not _is_allowed_domain(domain):
        return None

    try:
        resp = session.get(url, timeout=timeout_sec)
        if resp.status_code != 200:
            return None
        html = resp.text
    except Exception:
        return None

    text_blob = _strip_html_text(html)
    if not text_blob:
        return None

    title = text_blob[:120]
    # 命中条件：公司名 + 业务关键词至少其一
    low = text_blob.lower()
    if company_name and company_name not in text_blob:
        return None

    keywords = ["晶圆", "封装", "封测", "设备", "材料", "车规", "客户端", "代工", "技术", "产能"]
    if business_hint:
        keywords.extend([x for x in re.findall(r"[\u4e00-\u9fa5]{2,6}", business_hint) if len(x) >= 2][:6])
    if not any(k.lower() in low for k in keywords):
        return None

    pub = _extract_published_at(text_blob)
    snippet = _snippet_from_text(text_blob)
    if len(snippet) < 80:
        return None

    return EvidenceItem(
        url=url,
        title=title,
        domain=domain,
        published_at=pub,
        fetched_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        snippet=snippet,
        source_type=_source_type_from_domain(domain),
    )


def _evidence_in_recent_months(published_at: str, months: int = 24) -> bool:
    s = str(published_at or "").strip()
    if not s:
        return True
    dt = _safe_datetime(s)
    if dt is None:
        return True
    return (datetime.now() - dt).days <= months * 31


def collect_company_evidence(
    company_name: str,
    main_business: str,
    business_scope: str,
    evidence_per_company: int = 4,
) -> List[EvidenceItem]:
    if not company_name:
        return []

    business_hint = " ".join(
        normalize_domain_tags(
            fallback_extract_domain_tags((main_business or "") + "\n" + (business_scope or "")),
            max_tags=5,
        )
    )
    queries = [
        f"{company_name} 半导体 官方 公告 年报",
        f"{company_name} {business_hint} 证券时报",
        f"{company_name} {business_hint} 上证报",
        f"{company_name} {business_hint} 中证网",
        f"{company_name} {business_hint} 财联社",
        f"{company_name} {business_hint} 新浪财经",
    ]

    session = requests.Session()
    session.trust_env = False
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    raw_urls: List[str] = []
    for q in queries:
        raw_urls.extend(_search_web_urls(q, max_results=6))

    # 先按域名白名单过滤，再抓取
    filtered_urls = [u for u in raw_urls if _is_allowed_domain(_extract_domain(u))]

    evidences: List[EvidenceItem] = []
    for url in filtered_urls:
        item = _fetch_single_evidence(
            session=session,
            url=url,
            company_name=company_name,
            business_hint=business_hint,
        )
        if item is None:
            continue
        if not _evidence_in_recent_months(item.published_at, months=24):
            continue
        evidences.append(item)
        if len(evidences) >= 12:
            break

    deduped = dedupe_evidences(evidences)
    ranked = _rank_evidences(deduped)

    # 尽量保证 2 官方 + 2 媒体
    official = [x for x in ranked if x.source_type == "official"]
    media = [x for x in ranked if x.source_type == "media"]

    selected: List[EvidenceItem] = []
    selected.extend(official[:2])
    selected.extend(media[:2])

    if len(selected) < evidence_per_company:
        used = {x.url for x in selected}
        for item in ranked:
            if item.url in used:
                continue
            selected.append(item)
            if len(selected) >= evidence_per_company:
                break

    return selected[: max(1, int(evidence_per_company))]


def evidence_hash(profile_hash: str, evidences: Sequence[EvidenceItem]) -> str:
    payload = {
        "profile_hash": profile_hash,
        "evidences": [
            {
                "url": x.url,
                "title": x.title,
                "domain": x.domain,
                "published_at": x.published_at,
                "snippet": x.snippet,
                "source_type": x.source_type,
            }
            for x in evidences
        ],
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _calc_quality_score(
    insight_text: str,
    evidences: Sequence[EvidenceItem],
    confidence: float,
    is_fallback: bool,
) -> float:
    score = 0.0
    length = len(re.sub(r"\s+", "", str(insight_text or "")))
    if 120 <= length <= 180:
        score += 40.0
    elif length >= 80:
        score += 20.0

    ev_cnt = len(evidences)
    score += min(30.0, ev_cnt * 7.5)

    official_cnt = sum(1 for x in evidences if x.source_type == "official")
    media_cnt = sum(1 for x in evidences if x.source_type == "media")
    if official_cnt >= 1:
        score += 10.0
    if media_cnt >= 1:
        score += 10.0

    score += max(0.0, min(10.0, float(confidence or 0.0) * 10.0))

    if is_fallback:
        score = min(score, 55.0)
    return round(min(100.0, score), 2)


def parse_insight_output(raw: str) -> Dict[str, Any]:
    obj = _extract_json_object(raw)
    if not obj:
        return {}

    tags = normalize_domain_tags([str(x) for x in (obj.get("domain_tags") or [])], max_tags=5)
    tech = normalize_point_items([str(x) for x in (obj.get("tech_highlights") or [])], max_items=2)

    customer = str(obj.get("customer_profile") or "").strip()
    moat = str(obj.get("moat_note") or "").strip()
    risk = str(obj.get("boundary_risk") or "").strip()
    insight_text = str(obj.get("domain_insight_text") or "").strip()
    confidence = obj.get("confidence", 0)

    try:
        confidence = float(confidence)
    except Exception:
        confidence = 0.0

    return {
        "domain_tags": tags,
        "tech_highlights": tech,
        "customer_profile": customer,
        "moat_note": moat,
        "boundary_risk": risk,
        "domain_insight_text": insight_text,
        "confidence": max(0.0, min(1.0, confidence)),
    }


def is_valid_insight(parsed: Dict[str, Any]) -> bool:
    if not parsed:
        return False
    insight_text = str(parsed.get("domain_insight_text") or "").strip()
    length = len(re.sub(r"\s+", "", insight_text))
    if length < 120 or length > 180:
        return False

    if not parsed.get("domain_tags"):
        return False
    if not parsed.get("tech_highlights"):
        return False
    if not str(parsed.get("customer_profile") or "").strip():
        return False
    if not str(parsed.get("moat_note") or "").strip():
        return False
    if not str(parsed.get("boundary_risk") or "").strip():
        return False

    weak_words = ["可能", "或许", "大概", "较为", "一定程度"]
    weak_hits = sum(1 for w in weak_words if w in insight_text)
    if weak_hits >= 3:
        return False
    return True


def _trim_text_len(s: str, min_len: int = 120, max_len: int = 180) -> str:
    text_value = re.sub(r"\s+", "", str(s or "")).strip()
    if len(text_value) > max_len:
        return text_value[:max_len]
    if len(text_value) < min_len:
        pad = "信息仍在补全，后续将结合季度披露持续修订。"
        text_value = (text_value + pad)[:min_len]
    return text_value


def fallback_build_insight(
    company_name: str,
    main_business: str,
    business_scope: str,
    tags: Sequence[str],
    evidences: Sequence[EvidenceItem],
) -> Dict[str, Any]:
    tags = normalize_domain_tags(list(tags), max_tags=5) or fallback_extract_domain_tags(
        (main_business or "") + "\n" + (business_scope or "")
    )

    tech = normalize_point_items(tags[:2], max_items=2) or ["工艺与产品协同"]
    customer = "面向产业链头部客户与高可靠应用场景"
    moat = "依托认证周期、工艺积累与交付稳定性形成壁垒"
    risk = "需关注景气波动、扩产节奏与下游需求不确定性"

    evidence_note = ""
    if len(evidences) < 2:
        evidence_note = "当前公开证据较少，"

    summary = (
        f"{company_name}在{ '、'.join(tags[:3]) }方向具备持续投入，"
        f"技术侧突出{ '、'.join(tech) }；客户结构上{customer}；"
        f"护城河主要体现在{moat}；{evidence_note}边界上{risk}。"
    )
    summary = _trim_text_len(summary, 120, 180)

    return {
        "domain_tags": tags[:5],
        "tech_highlights": tech[:2],
        "customer_profile": customer,
        "moat_note": moat,
        "boundary_risk": risk,
        "domain_insight_text": summary,
        "confidence": 0.35 if len(evidences) < 2 else 0.5,
        "is_fallback": True,
    }


def generate_insight_with_llm(
    company_name: str,
    main_business: str,
    business_scope: str,
    evidences: Sequence[EvidenceItem],
    llm,
) -> Dict[str, Any]:
    tags_fallback = fallback_extract_domain_tags((main_business or "") + "\n" + (business_scope or ""))

    if llm is None:
        return fallback_build_insight(company_name, main_business, business_scope, tags_fallback, evidences)

    evidence_lines = []
    for i, ev in enumerate(evidences, 1):
        evidence_lines.append(
            f"[{i}] 域名:{ev.domain} 日期:{ev.published_at or '-'} 标题:{ev.title[:70]} 摘要:{ev.snippet[:240]}"
        )

    prompt = (
        "你是半导体行业研究助手。请严格基于给定主营文本与证据摘要，输出JSON对象，不得输出额外文字。\n"
        "字段要求：\n"
        "domain_tags: 3-5个标签，每个2-8字\n"
        "tech_highlights: 1-2条\n"
        "customer_profile: 1条\n"
        "moat_note: 1条\n"
        "boundary_risk: 1条\n"
        "domain_insight_text: 120-180字中文，必须同时覆盖技术能力/客户属性/护城河/边界风险\n"
        "confidence: 0-1\n"
        "若证据不足，也需给出可用结果，但confidence降低。\n\n"
        f"公司: {company_name}\n"
        f"主营: {(main_business or '')[:900]}\n"
        f"经营范围: {(business_scope or '')[:900]}\n"
        "证据:\n"
        f"{chr(10).join(evidence_lines)[:4000]}\n"
    )

    try:
        resp = llm.invoke(prompt)
        content = getattr(resp, "content", resp)
        parsed = parse_insight_output(str(content))
    except Exception:
        parsed = {}

    if is_valid_insight(parsed):
        parsed["is_fallback"] = False
        return parsed

    fb_tags = parsed.get("domain_tags") or tags_fallback
    return fallback_build_insight(company_name, main_business, business_scope, fb_tags, evidences)


# --------------------------
# 数据读取/候选
# --------------------------
def fetch_company_profiles(pro) -> pd.DataFrame:
    all_rows = []
    for ex in ["SSE", "SZSE"]:
        df = pro.stock_company(exchange=ex)
        if df is None or df.empty:
            continue
        df = df.copy()
        df["ts_code"] = df["ts_code"].astype(str).str.upper().str.strip()
        df = df[df["ts_code"].str.endswith((".SH", ".SZ"))]
        all_rows.append(df)

    if not all_rows:
        return pd.DataFrame(
            columns=["ts_code", "com_name", "main_business", "business_scope", "exchange"]
        )

    out = pd.concat(all_rows, ignore_index=True)
    out = out.drop_duplicates(subset=["ts_code"], keep="last")
    return out


def load_existing_cache(engine, ts_codes: List[str]) -> pd.DataFrame:
    if not ts_codes:
        return pd.DataFrame(
            columns=[
                "ts_code",
                "domain_tags",
                "profile_hash",
                "tags_updated_at",
                "insight_hash",
                "insight_updated_at",
                "domain_insight_text",
            ]
        )

    params = {f"c{i}": code for i, code in enumerate(ts_codes)}
    placeholders = ",".join(f":c{i}" for i in range(len(ts_codes)))
    sql = text(
        f"""
        SELECT ts_code, domain_tags, profile_hash, tags_updated_at,
               insight_hash, insight_updated_at, domain_insight_text
        FROM stock_company_profile_cache
        WHERE ts_code IN ({placeholders})
        """
    )
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params=params)


def should_refresh(
    existing_row: Optional[Dict[str, Any]],
    profile_hash: str,
    now_dt: datetime,
    refresh_missing: bool,
    refresh_expired: bool,
    expire_days: int,
) -> bool:
    if existing_row is None:
        return True

    old_hash = str(existing_row.get("profile_hash") or "").strip()
    if old_hash != profile_hash:
        return True

    old_tags = str(existing_row.get("domain_tags") or "").strip()
    if refresh_missing and not old_tags:
        return True

    if refresh_expired:
        old_dt = _safe_datetime(existing_row.get("tags_updated_at"))
        if old_dt is None:
            return True
        if (now_dt - old_dt) >= timedelta(days=max(1, int(expire_days))):
            return True

    return False


def should_refresh_insight(
    existing_row: Optional[Dict[str, Any]],
    profile_hash: str,
    insight_hash_value: str,
    now_dt: datetime,
    expire_days: int,
    force: bool,
) -> bool:
    if force:
        return True
    if existing_row is None:
        return True

    old_profile_hash = str(existing_row.get("profile_hash") or "").strip()
    if old_profile_hash != profile_hash:
        return True

    old_insight_hash = str(existing_row.get("insight_hash") or "").strip()
    if old_insight_hash != insight_hash_value:
        return True

    old_insight = str(existing_row.get("domain_insight_text") or "").strip()
    if not old_insight:
        return True

    old_dt = _safe_datetime(existing_row.get("insight_updated_at"))
    if old_dt is None:
        return True
    if (now_dt - old_dt) >= timedelta(days=max(1, int(expire_days))):
        return True

    return False


def pick_candidates(
    profiles_df: pd.DataFrame,
    existing_df: pd.DataFrame,
    refresh_missing: bool,
    refresh_expired: bool,
    expire_days: int,
) -> pd.DataFrame:
    if profiles_df.empty:
        return profiles_df

    existing_map = {}
    if not existing_df.empty:
        for _, row in existing_df.iterrows():
            existing_map[_norm_code(row.get("ts_code"))] = row.to_dict()

    now_dt = datetime.now()
    selected = []
    for _, row in profiles_df.iterrows():
        code = _norm_code(row.get("ts_code"))
        mb = str(row.get("main_business") or "")
        bs = str(row.get("business_scope") or "")
        profile_hash = compute_profile_hash(mb, bs)
        old = existing_map.get(code)
        if should_refresh(
            existing_row=old,
            profile_hash=profile_hash,
            now_dt=now_dt,
            refresh_missing=refresh_missing,
            refresh_expired=refresh_expired,
            expire_days=expire_days,
        ):
            r = row.copy()
            r["profile_hash"] = profile_hash
            selected.append(r)

    if not selected:
        return profiles_df.head(0).copy()
    return pd.DataFrame(selected)


def load_sector_component_codes(pro, sector_name: str) -> List[str]:
    try:
        templates = load_chain_templates(str(TEMPLATE_PATH))
    except Exception:
        return []

    sector = templates.get(sector_name) or {}
    stages = sector.get("stages") or []

    if pro is None:
        return []

    stage_member_map, _ = fetch_stage_members_from_tushare(
        stages=stages,
        pro=pro,
        sector_name=sector_name,
        collect_meta=False,
    )
    code_name_map: Dict[str, str] = {}
    for members in stage_member_map.values():
        for row in members or []:
            c = _norm_code(row.get("ts_code"))
            if c.endswith((".SH", ".SZ")):
                code_name_map[c] = str(row.get("name") or "").strip()

    return sorted(code_name_map.keys())


# --------------------------
# 写库
# --------------------------
def generate_domain_tags(main_business: str, business_scope: str, llm) -> Tuple[List[str], str]:
    raw = (main_business or "") + "\n" + (business_scope or "")
    ai_tags = ai_extract_domain_tags(raw, llm)
    if ai_tags:
        return ai_tags, os.getenv("DOMAIN_TAG_MODEL", DEFAULT_INSIGHT_MODEL)

    fb = fallback_extract_domain_tags(raw)
    return fb, "fallback-rules"


def upsert_profiles(engine, rows: List[Dict[str, Any]], include_insight: bool = True) -> None:
    if not rows:
        return

    if include_insight:
        sql = text(
            """
            INSERT INTO stock_company_profile_cache (
                ts_code, company_name, exchange, main_business, business_scope,
                domain_tags, tags_model, profile_hash, source_updated_at, tags_updated_at,
                domain_insight_text, tech_highlights, customer_profile, moat_note, boundary_risk,
                insight_sources_json, insight_hash, insight_model, insight_updated_at, insight_quality_score
            ) VALUES (
                :ts_code, :company_name, :exchange, :main_business, :business_scope,
                :domain_tags, :tags_model, :profile_hash, :source_updated_at, :tags_updated_at,
                :domain_insight_text, :tech_highlights, :customer_profile, :moat_note, :boundary_risk,
                :insight_sources_json, :insight_hash, :insight_model, :insight_updated_at, :insight_quality_score
            )
            ON DUPLICATE KEY UPDATE
                company_name=VALUES(company_name),
                exchange=VALUES(exchange),
                main_business=VALUES(main_business),
                business_scope=VALUES(business_scope),
                domain_tags=VALUES(domain_tags),
                tags_model=VALUES(tags_model),
                profile_hash=VALUES(profile_hash),
                source_updated_at=VALUES(source_updated_at),
                tags_updated_at=VALUES(tags_updated_at),
                domain_insight_text=VALUES(domain_insight_text),
                tech_highlights=VALUES(tech_highlights),
                customer_profile=VALUES(customer_profile),
                moat_note=VALUES(moat_note),
                boundary_risk=VALUES(boundary_risk),
                insight_sources_json=VALUES(insight_sources_json),
                insight_hash=VALUES(insight_hash),
                insight_model=VALUES(insight_model),
                insight_updated_at=VALUES(insight_updated_at),
                insight_quality_score=VALUES(insight_quality_score)
            """
        )
    else:
        sql = text(
            """
            INSERT INTO stock_company_profile_cache (
                ts_code, company_name, exchange, main_business, business_scope,
                domain_tags, tags_model, profile_hash, source_updated_at, tags_updated_at
            ) VALUES (
                :ts_code, :company_name, :exchange, :main_business, :business_scope,
                :domain_tags, :tags_model, :profile_hash, :source_updated_at, :tags_updated_at
            )
            ON DUPLICATE KEY UPDATE
                company_name=VALUES(company_name),
                exchange=VALUES(exchange),
                main_business=VALUES(main_business),
                business_scope=VALUES(business_scope),
                domain_tags=VALUES(domain_tags),
                tags_model=VALUES(tags_model),
                profile_hash=VALUES(profile_hash),
                source_updated_at=VALUES(source_updated_at),
                tags_updated_at=VALUES(tags_updated_at)
            """
        )

    with engine.begin() as conn:
        for row in rows:
            conn.execute(sql, row)


# --------------------------
# 主流程
# --------------------------
def run_update_tags(
    refresh_missing: bool,
    refresh_expired: bool,
    expire_days: int,
    limit: int,
) -> int:
    if not refresh_missing and not refresh_expired:
        refresh_missing = True

    engine = get_db_engine()
    if engine is None:
        raise RuntimeError("数据库配置缺失")

    pro = get_tushare_pro()
    if pro is None:
        raise RuntimeError("TUSHARE_TOKEN 缺失")

    ensure_stock_company_profile_cache_table(engine)

    print(
        f"🚀 更新公司业务标签缓存 | refresh_missing={refresh_missing} "
        f"refresh_expired={refresh_expired} expire_days={expire_days} limit={limit}"
    )

    profiles = fetch_company_profiles(pro)
    if profiles.empty:
        raise RuntimeError("未拉取到 stock_company 数据")

    existing = load_existing_cache(engine, profiles["ts_code"].astype(str).tolist())
    candidates = pick_candidates(
        profiles_df=profiles,
        existing_df=existing,
        refresh_missing=refresh_missing,
        refresh_expired=refresh_expired,
        expire_days=expire_days,
    )

    if limit > 0:
        candidates = candidates.head(limit)

    print(f"📊 全量公司={len(profiles)} | 待刷新={len(candidates)}")
    if candidates.empty:
        print("✅ 没有需要刷新的公司")
        return 0

    llm = build_tag_llm()
    if llm is None:
        print("⚠️ 未配置 DASHSCOPE_API_KEY 或模型不可用，将使用规则提取标签")

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    upsert_rows: List[Dict[str, Any]] = []
    ai_cnt = 0
    fb_cnt = 0

    for i, (_, row) in enumerate(candidates.iterrows(), 1):
        ts_code = _norm_code(row.get("ts_code"))
        company_name = str(row.get("com_name") or "").strip()
        exchange = str(row.get("exchange") or "").strip()
        main_business = str(row.get("main_business") or "").strip()
        business_scope = str(row.get("business_scope") or "").strip()
        profile_hash = str(row.get("profile_hash") or compute_profile_hash(main_business, business_scope))

        tags, model_name = generate_domain_tags(main_business, business_scope, llm=llm)
        if model_name == "fallback-rules":
            fb_cnt += 1
        else:
            ai_cnt += 1

        upsert_rows.append(
            {
                "ts_code": ts_code,
                "company_name": company_name,
                "exchange": exchange,
                "main_business": main_business,
                "business_scope": business_scope,
                "domain_tags": "|".join(tags),
                "tags_model": model_name,
                "profile_hash": profile_hash,
                "source_updated_at": now_str,
                "tags_updated_at": now_str,
            }
        )

        if i % 100 == 0:
            print(f"... 已处理 {i}/{len(candidates)}")
            time.sleep(0.05)

    upsert_profiles(engine, upsert_rows, include_insight=False)
    print(f"✅ 完成写入 {len(upsert_rows)} 条 | AI={ai_cnt} | fallback={fb_cnt}")
    return len(upsert_rows)


def run_update_insight(
    sector_name: str,
    expire_days: int,
    limit: int,
    dry_run: bool,
    force: bool,
    evidence_per_company: int = 4,
) -> int:
    engine = get_db_engine()
    if engine is None:
        raise RuntimeError("数据库配置缺失")

    pro = get_tushare_pro()
    if pro is None:
        raise RuntimeError("TUSHARE_TOKEN 缺失")

    ensure_stock_company_profile_cache_table(engine)

    print(
        f"🚀 季度更新公司业务精华说明 | sector={sector_name} days={expire_days} "
        f"limit={limit} dry_run={dry_run} force={force}"
    )

    component_codes = load_sector_component_codes(pro, sector_name)
    if not component_codes:
        raise RuntimeError(f"未获取到板块 {sector_name} 成分股")

    profiles = fetch_company_profiles(pro)
    profiles = profiles[profiles["ts_code"].astype(str).str.upper().isin(set(component_codes))].copy()
    if profiles.empty:
        raise RuntimeError("产业链成分股在 stock_company 中无记录")

    existing = load_existing_cache(engine, profiles["ts_code"].astype(str).tolist())
    existing_map: Dict[str, Dict[str, Any]] = {}
    if not existing.empty:
        for _, row in existing.iterrows():
            existing_map[_norm_code(row.get("ts_code"))] = row.to_dict()

    llm = build_insight_llm()
    if llm is None:
        print("⚠️ 未配置 DASHSCOPE_API_KEY 或模型不可用，将使用规则回退生成说明")

    now_dt = datetime.now()
    now_str = now_dt.strftime("%Y-%m-%d %H:%M:%S")

    upsert_rows: List[Dict[str, Any]] = []
    skipped = 0
    fallback_cnt = 0

    iter_df = profiles
    if limit > 0:
        iter_df = iter_df.head(limit)

    for i, (_, row) in enumerate(iter_df.iterrows(), 1):
        ts_code = _norm_code(row.get("ts_code"))
        company_name = str(row.get("com_name") or "").strip()
        exchange = str(row.get("exchange") or "").strip()
        main_business = str(row.get("main_business") or "").strip()
        business_scope = str(row.get("business_scope") or "").strip()
        profile_hash = compute_profile_hash(main_business, business_scope)

        evidences = collect_company_evidence(
            company_name=company_name,
            main_business=main_business,
            business_scope=business_scope,
            evidence_per_company=evidence_per_company,
        )
        insight_hash_value = evidence_hash(profile_hash, evidences)

        old = existing_map.get(ts_code)
        if not should_refresh_insight(
            existing_row=old,
            profile_hash=profile_hash,
            insight_hash_value=insight_hash_value,
            now_dt=now_dt,
            expire_days=expire_days,
            force=force,
        ):
            skipped += 1
            continue

        parsed = generate_insight_with_llm(
            company_name=company_name,
            main_business=main_business,
            business_scope=business_scope,
            evidences=evidences,
            llm=llm,
        )

        is_fallback = bool(parsed.get("is_fallback", False))
        if is_fallback:
            fallback_cnt += 1

        domain_tags = normalize_domain_tags(parsed.get("domain_tags") or [], max_tags=5)
        if not domain_tags:
            domain_tags = fallback_extract_domain_tags(main_business + "\n" + business_scope)

        tech_items = normalize_point_items(parsed.get("tech_highlights") or [], max_items=2)
        customer_profile = str(parsed.get("customer_profile") or "").strip()
        moat_note = str(parsed.get("moat_note") or "").strip()
        boundary_risk = str(parsed.get("boundary_risk") or "").strip()
        insight_text = _trim_text_len(str(parsed.get("domain_insight_text") or ""), 120, 180)
        confidence = float(parsed.get("confidence") or 0.0)

        quality_score = _calc_quality_score(
            insight_text=insight_text,
            evidences=evidences,
            confidence=confidence,
            is_fallback=is_fallback,
        )

        source_json = json.dumps(
            [
                {
                    "url": x.url,
                    "title": x.title,
                    "domain": x.domain,
                    "published_at": x.published_at,
                    "fetched_at": x.fetched_at,
                    "snippet": x.snippet,
                    "source_type": x.source_type,
                }
                for x in evidences
            ],
            ensure_ascii=False,
        )

        item = {
            "ts_code": ts_code,
            "company_name": company_name,
            "exchange": exchange,
            "main_business": main_business,
            "business_scope": business_scope,
            "domain_tags": "|".join(domain_tags),
            "tags_model": os.getenv("DOMAIN_TAG_MODEL", DEFAULT_INSIGHT_MODEL) if llm else "fallback-rules",
            "profile_hash": profile_hash,
            "source_updated_at": now_str,
            "tags_updated_at": now_str,
            "domain_insight_text": insight_text,
            "tech_highlights": "|".join(tech_items),
            "customer_profile": customer_profile,
            "moat_note": moat_note,
            "boundary_risk": boundary_risk,
            "insight_sources_json": source_json,
            "insight_hash": insight_hash_value,
            "insight_model": os.getenv(INSIGHT_MODEL_ENV, DEFAULT_INSIGHT_MODEL) if llm else "fallback-rules",
            "insight_updated_at": now_str,
            "insight_quality_score": quality_score,
        }

        if dry_run:
            print(
                f"[DRY-RUN] {ts_code} {company_name} | tags={item['domain_tags']} | quality={quality_score} | "
                f"evidence={len(evidences)}"
            )
            print(f"         insight: {insight_text}")
        else:
            upsert_rows.append(item)

        if i % 20 == 0:
            print(f"... 已处理 {i}/{len(iter_df)}")
            time.sleep(0.05)

    if not dry_run:
        upsert_profiles(engine, upsert_rows, include_insight=True)

    written = 0 if dry_run else len(upsert_rows)
    print(
        f"✅ 说明更新完成 | candidates={len(iter_df)} write={written} skipped={skipped} fallback={fallback_cnt}"
    )
    return written


def run_update(
    refresh_missing: bool,
    refresh_expired: bool,
    expire_days: int,
    limit: int,
    refresh_insight: bool,
    sector_name: str,
    dry_run: bool,
    force: bool,
) -> int:
    if refresh_insight:
        return run_update_insight(
            sector_name=sector_name,
            expire_days=expire_days,
            limit=limit,
            dry_run=dry_run,
            force=force,
        )

    return run_update_tags(
        refresh_missing=refresh_missing,
        refresh_expired=refresh_expired,
        expire_days=expire_days,
        limit=limit,
    )


def main():
    parser = argparse.ArgumentParser(description="更新 A 股公司主营信息、业务标签与精华说明缓存")
    parser.add_argument("--refresh-missing", action="store_true", help="刷新缺失标签的公司")
    parser.add_argument("--refresh-expired", action="store_true", help="刷新过期标签的公司")
    parser.add_argument("--refresh-insight", action="store_true", help="刷新产业链公司精华说明")
    parser.add_argument("--sector", type=str, default="半导体", help="产业链板块名，默认半导体")
    parser.add_argument("--days", type=int, default=0, help="过期阈值天数；说明默认90，标签默认180")
    parser.add_argument("--limit", type=int, default=0, help="限制处理数量，0=不限制")
    parser.add_argument("--dry-run", action="store_true", help="仅打印候选与示例，不写库")
    parser.add_argument("--force", action="store_true", help="忽略阈值与hash，强制重算")
    args = parser.parse_args()

    expire_days = int(args.days) if int(args.days) > 0 else (90 if args.refresh_insight else 180)

    run_update(
        refresh_missing=args.refresh_missing,
        refresh_expired=args.refresh_expired,
        expire_days=expire_days,
        limit=args.limit,
        refresh_insight=args.refresh_insight,
        sector_name=args.sector,
        dry_run=args.dry_run,
        force=args.force,
    )


if __name__ == "__main__":
    main()
