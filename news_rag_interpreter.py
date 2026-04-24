from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional

from langchain_core.tools import tool


HORIZON_LABELS = {
    "intraday": "日内",
    "swing": "波段",
    "weekly": "周度",
}

EVENT_TOOL_RULES = {
    "geopolitical": {
        "keywords": ["地缘", "冲突", "战争", "制裁", "中东", "伊朗", "以色列", "俄乌", "停火", "避险", "geopolitical", "middle east", "safe-haven", "war", "sanction"],
        "tools": ["geopolitical_risk", "polymarket", "market"],
    },
    "monetary": {
        "keywords": ["美联储", "降息", "加息", "利率", "通胀", "cpi", "非农", "美元", "收益率", "fed", "rate", "inflation"],
        "tools": ["knowledge", "market", "web_search"],
    },
    "commodity_supply": {
        "keywords": ["库存", "产量", "减产", "增产", "供需", "opec", "eia", "仓单", "基差", "期限结构"],
        "tools": ["knowledge", "market", "structure"],
    },
    "equity_policy": {
        "keywords": ["a股", "政策", "监管", "刺激", "财政", "行业", "概念", "补贴", "审批"],
        "tools": ["knowledge", "market", "web_search"],
    },
    "option_vol": {
        "keywords": ["期权", "iv", "波动率", "iv rank", "隐波", "持仓", "成交"],
        "tools": ["knowledge", "market", "volatility"],
    },
}

MARKET_QUERY_HINTS = [
    ("黄金", ["黄金", "金价", "gold", "避险"]),
    ("白银", ["白银", "silver"]),
    ("原油", ["原油", "油价", "oil", "opec", "eia"]),
    ("铜", ["铜", "copper"]),
    ("螺纹", ["螺纹", "钢材", "黑色"]),
    ("500ETF", ["500etf", "中证500", "510500"]),
    ("沪深300", ["沪深300", "300etf", "510300"]),
    ("纳指", ["纳指", "nasdaq", "科技股"]),
    ("美元指数", ["美元", "dxy", "美元指数"]),
    ("10年美债", ["美债", "收益率", "10y", "treasury"]),
]

BULLISH_HINTS = [
    "上涨",
    "走强",
    "拉升",
    "反弹",
    "推高",
    "支撑",
    "利多",
    "提振",
    "降息",
    "risk on",
    "risk-on",
    "rally",
    "bullish",
]

BEARISH_HINTS = [
    "下跌",
    "走弱",
    "回落",
    "跳水",
    "压制",
    "利空",
    "抛售",
    "冲突升级",
    "制裁",
    "加息",
    "risk off",
    "risk-off",
    "selloff",
    "bearish",
]


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _compact_text(value: Any, limit: int = 700) -> str:
    text = re.sub(r"\s+", " ", _safe_text(value)).strip()
    if len(text) <= limit:
        return text
    return text[:limit].rstrip() + "..."


def _invoke_tool(tool_obj: Any, payload: Dict[str, Any]) -> str:
    try:
        if hasattr(tool_obj, "invoke"):
            return _safe_text(tool_obj.invoke(payload))
        return _safe_text(tool_obj(**payload))
    except TypeError:
        try:
            if len(payload) == 1:
                return _safe_text(tool_obj(next(iter(payload.values()))))
        except Exception:
            return ""
    except Exception as exc:
        return f"tool_error: {exc}"
    return ""


def _dedupe(items: List[str], limit: int) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in items:
        value = _safe_text(item)
        key = value.lower()
        if not value or key in seen:
            continue
        seen.add(key)
        out.append(value)
        if len(out) >= limit:
            break
    return out


def _classify_event_type(text: str) -> str:
    lower = _safe_text(text).lower()
    for event_type, rule in EVENT_TOOL_RULES.items():
        if any(keyword.lower() in lower for keyword in rule["keywords"]):
            return event_type
    return "market_news"


def _infer_market_queries(query: str, event: Dict[str, Any], symbols: Optional[List[str]]) -> List[str]:
    candidates: List[str] = []
    if symbols:
        candidates.extend([_safe_text(item) for item in symbols])

    merged = " ".join(
        [
            query,
            _safe_text(event.get("title")),
            _safe_text(event.get("summary")),
            _safe_text(event.get("raw_content")),
            " ".join([_safe_text(x) for x in event.get("affected_assets", []) if x]),
        ]
    ).lower()

    for target, hints in MARKET_QUERY_HINTS:
        if any(h.lower() in merged for h in hints):
            candidates.append(target)

    if not candidates:
        affected = event.get("affected_assets") if isinstance(event.get("affected_assets"), list) else []
        for item in affected:
            label = _safe_text(item)
            if label in {"大宗商品", "多资产"}:
                continue
            candidates.append(label)

    if not candidates:
        candidates.append(query)

    return _dedupe(candidates, limit=3)


def _build_source_coverage() -> Dict[str, bool]:
    return {
        "news": False,
        "market": False,
        "knowledge": False,
        "polymarket": False,
        "web_search": False,
        "geopolitical_risk": False,
    }


def _mark_coverage(coverage: Dict[str, bool], key: str, value: str) -> None:
    text = _safe_text(value)
    if text and "tool_error" not in text and "未配置" not in text and "暂无" not in text:
        coverage[key] = True


def _load_ingest(query: str, horizon: str, use_external_news: bool) -> Dict[str, Any]:
    try:
        from event_ingest_tool import ingest_event_timeline

        payload = ingest_event_timeline(
            query=query,
            analysis_horizon=horizon,
            use_external_news=use_external_news,
        )
        return payload if isinstance(payload, dict) else {}
    except Exception as exc:
        return {
            "timeline": [],
            "market_context": {},
            "ingest_meta": {
                "query": query,
                "analysis_horizon": horizon,
                "use_external_news": bool(use_external_news),
                "event_count": 0,
                "market_errors": [f"ingest_error: {exc}"],
            },
        }


def _extract_and_score(ingest_payload: Dict[str, Any], query: str, horizon: str) -> Dict[str, Any]:
    try:
        from event_extract_tool import extract_event_elements
        from impact_scoring_tool import score_event_impacts

        extracted = extract_event_elements(ingest_payload=ingest_payload, query=query)
        _calibrate_event_direction(extracted)
        scored = score_event_impacts(extracted_payload=extracted, analysis_horizon=horizon)
        return {"extracted": extracted, "scored": scored}
    except Exception as exc:
        return {
            "extracted": {"events": [], "event_briefing": [], "source_ledger": []},
            "scored": {
                "impact_matrix": [],
                "aggregate_bias": {"market_bias": "中性", "bull_strength": 0.0, "bear_strength": 0.0, "net_score": 0.0},
                "conflict_analysis": {"has_conflict": False, "conflicts": [], "dominant_events": []},
                "conflict_ranking": [],
                "event_priority_board": [],
                "error": f"score_error: {exc}",
            },
        }


def _calibrate_event_direction(extracted_payload: Dict[str, Any]) -> None:
    events = extracted_payload.get("events") if isinstance(extracted_payload, dict) else []
    if not isinstance(events, list):
        return

    for event in events:
        if not isinstance(event, dict):
            continue
        merged = " ".join(
            [
                _safe_text(event.get("title")),
                _safe_text(event.get("summary")),
                _safe_text(event.get("raw_content")),
            ]
        ).lower()
        if any(hint.lower() in merged for hint in BULLISH_HINTS):
            event["direction_hint"] = "risk_on"
        elif any(hint.lower() in merged for hint in BEARISH_HINTS):
            event["direction_hint"] = "risk_off"


def _load_context_for_event(
    query: str,
    event: Dict[str, Any],
    symbols: Optional[List[str]],
    coverage: Dict[str, bool],
) -> Dict[str, Any]:
    title = _safe_text(event.get("title")) or _safe_text(event.get("summary")) or query
    raw = " ".join([query, title, _safe_text(event.get("raw_content"))])
    event_type = _classify_event_type(raw)
    tools = set(EVENT_TOOL_RULES.get(event_type, {}).get("tools", ["knowledge", "market"]))
    tools.add("knowledge")
    tools.add("market")

    checks: List[Dict[str, str]] = []

    if "knowledge" in tools:
        try:
            from knowledge_tools import search_investment_knowledge

            knowledge = _invoke_tool(search_investment_knowledge, {"query": title})
            _mark_coverage(coverage, "knowledge", knowledge)
            if knowledge:
                checks.append({"tool": "knowledge", "target": title, "result": _compact_text(knowledge, 360)})
        except Exception as exc:
            checks.append({"tool": "knowledge", "target": title, "result": f"tool_error: {exc}"})

    if "market" in tools:
        try:
            from market_tools import get_market_snapshot, get_recent_price_series

            for target in _infer_market_queries(query, event, symbols):
                snapshot = _invoke_tool(get_market_snapshot, {"query": target})
                _mark_coverage(coverage, "market", snapshot)
                if snapshot:
                    checks.append({"tool": "market_snapshot", "target": target, "result": _compact_text(snapshot, 260)})
                series = _invoke_tool(get_recent_price_series, {"query": target, "days": 5})
                _mark_coverage(coverage, "market", series)
                if series:
                    checks.append({"tool": "recent_price_series", "target": target, "result": _compact_text(series, 360)})
        except Exception as exc:
            checks.append({"tool": "market", "target": title, "result": f"tool_error: {exc}"})

    if "polymarket" in tools:
        try:
            from polymarket_tool import tool_get_polymarket_sentiment

            poly = _invoke_tool(tool_get_polymarket_sentiment, {"keywords": title})
            _mark_coverage(coverage, "polymarket", poly)
            if poly:
                checks.append({"tool": "polymarket", "target": title, "result": _compact_text(poly, 420)})
        except Exception as exc:
            checks.append({"tool": "polymarket", "target": title, "result": f"tool_error: {exc}"})

    if "geopolitical_risk" in tools:
        try:
            import data_engine as de

            geo = de.get_latest_geopolitical_risk_snapshot()
            if isinstance(geo, dict) and geo:
                coverage["geopolitical_risk"] = True
                checks.append(
                    {
                        "tool": "geopolitical_risk",
                        "target": "世界混乱指数",
                        "result": _compact_text(
                            f"score={geo.get('score_raw')}, band={geo.get('band')}, drivers={geo.get('top_markets', [])[:3]}",
                            420,
                        ),
                    }
                )
        except Exception as exc:
            checks.append({"tool": "geopolitical_risk", "target": "世界混乱指数", "result": f"tool_error: {exc}"})

    if "web_search" in tools:
        try:
            from search_tools import search_web

            web = _invoke_tool(search_web, {"query": title})
            _mark_coverage(coverage, "web_search", web)
            if web:
                checks.append({"tool": "web_search", "target": title, "result": _compact_text(web, 420)})
        except Exception as exc:
            checks.append({"tool": "web_search", "target": title, "result": f"tool_error: {exc}"})

    return {"event_type": event_type, "checks": checks[:8]}


def _direction_word(value: Any) -> str:
    text = _safe_text(value)
    if text in {"偏多", "偏空", "中性"}:
        return text
    lower = text.lower()
    if "bull" in lower or "risk_on" in lower or "positive" in lower:
        return "偏多"
    if "bear" in lower or "risk_off" in lower or "negative" in lower:
        return "偏空"
    return "中性"


def _build_points(impact_matrix: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    bullish: List[str] = []
    bearish: List[str] = []
    neutral: List[str] = []
    for item in impact_matrix[:8]:
        title = _safe_text(item.get("title")) or _safe_text(item.get("event_id")) or "事件"
        strength = item.get("strength", "-")
        confidence = item.get("confidence", "-")
        line = f"{title}，强度{strength}，置信度{confidence}"
        direction = _direction_word(item.get("direction"))
        if direction == "偏多":
            bullish.append(line)
        elif direction == "偏空":
            bearish.append(line)
        else:
            neutral.append(line)
    return {"bullish": bullish[:4], "bearish": bearish[:4], "neutral": neutral[:3]}


def _build_summary(query: str, market_bias: str, confidence: float, points: Dict[str, List[str]], degraded: bool) -> str:
    bias = _safe_text(market_bias) or "中性"
    if bias == "偏多":
        opening = f"{query} 这条线现在偏暖，主线暂时偏多。"
    elif bias == "偏空":
        opening = f"{query} 这条线现在偏冷，主线暂时偏空。"
    else:
        opening = f"{query} 现在不是单边逻辑，多空都有理由。"

    if degraded:
        opening += " 不过有些数据源没打满，结论要打折看。"

    if points.get("bullish") and points.get("bearish"):
        opening += " 关键是别只看利多，反向压力也在。"
    elif confidence < 0.55:
        opening += " 证据还不够硬，先按观察处理。"
    return opening


def _build_watch_triggers(query: str, events: List[Dict[str, Any]]) -> List[str]:
    merged = " ".join([query] + [_safe_text(e.get("title")) for e in events]).lower()
    triggers: List[str] = []
    if any(x in merged for x in ["黄金", "gold", "地缘", "冲突", "中东"]):
        triggers.extend(["地缘消息有没有继续升级", "美元和美债实际利率是否反向压制", "黄金/白银IV是否继续抬升"])
    if any(x in merged for x in ["原油", "oil", "opec", "库存", "eia"]):
        triggers.extend(["EIA库存和成品油需求是否同步验证", "OPEC供给口径有没有变化", "油价涨幅是否已经提前反映利多"])
    if any(x in merged for x in ["美联储", "fed", "降息", "加息", "cpi", "通胀"]):
        triggers.extend(["美元指数方向", "美债收益率变化", "市场对降息路径的重新定价"])
    if any(x in merged for x in ["a股", "政策", "财政", "刺激"]):
        triggers.extend(["政策是否有后续细则", "成交量和北向/资金流是否跟上", "相关板块是否扩散"])
    if not triggers:
        triggers.extend(["后续新闻有没有继续发酵", "价格是否跟新闻方向一致", "成交量/波动率有没有同步放大"])
    return _dedupe(triggers, limit=5)


def _build_risk_notes(points: Dict[str, List[str]], degraded: bool) -> List[str]:
    notes: List[str] = []
    if points.get("bullish") and points.get("bearish"):
        notes.append("这不是单边盘，多空消息互相打架，别把一条新闻当成唯一依据。")
    if degraded:
        notes.append("部分数据源不可用，当前判断更适合做盘中线索，不适合当最终结论。")
    notes.append("这里只做新闻和行情解读，不给买卖指令。")
    return notes


def _format_trader_brief(payload: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append(_safe_text(payload.get("summary")))
    lines.append("")
    bullish = payload.get("bullish_points") or []
    bearish = payload.get("bearish_points") or []
    watch = payload.get("what_to_watch") or []
    risks = payload.get("risk_notes") or []

    if bullish:
        lines.append("偏多的点：")
        lines.extend([f"- {x}" for x in bullish])
        lines.append("")
    if bearish:
        lines.append("压制/反向风险：")
        lines.extend([f"- {x}" for x in bearish])
        lines.append("")
    if watch:
        lines.append("接下来盯：")
        lines.extend([f"{idx + 1}. {x}" for idx, x in enumerate(watch)])
        lines.append("")
    if risks:
        lines.append("提醒：")
        lines.extend([f"- {x}" for x in risks])

    return "\n".join(lines).strip()


def interpret_market_news(
    query: str,
    symbols: Optional[List[str]] = None,
    horizon: str = "swing",
    use_external_news: bool = True,
    max_events: int = 8,
) -> Dict[str, Any]:
    safe_query = _safe_text(query) or "市场新闻怎么看"
    safe_horizon = _safe_text(horizon).lower() or "swing"
    if safe_horizon not in HORIZON_LABELS:
        safe_horizon = "swing"
    safe_max_events = max(1, min(int(max_events or 8), 12))

    coverage = _build_source_coverage()
    ingest_payload = _load_ingest(safe_query, safe_horizon, bool(use_external_news))
    timeline = ingest_payload.get("timeline") if isinstance(ingest_payload, dict) else []
    if isinstance(timeline, list) and any(_safe_text(x.get("source")) != "市场工具" for x in timeline if isinstance(x, dict)):
        coverage["news"] = True

    scored_payload = _extract_and_score(ingest_payload, safe_query, safe_horizon)
    extracted = scored_payload["extracted"]
    scored = scored_payload["scored"]

    events = extracted.get("events") if isinstance(extracted, dict) else []
    events = [item for item in events if isinstance(item, dict)][:safe_max_events]

    enriched_events: List[Dict[str, Any]] = []
    tool_checks: List[Dict[str, str]] = []
    for event in events:
        context = _load_context_for_event(safe_query, event, symbols, coverage)
        enriched = dict(event)
        enriched["event_type"] = context.get("event_type")
        enriched["tool_check_count"] = len(context.get("checks") or [])
        enriched_events.append(enriched)
        tool_checks.extend(context.get("checks") or [])

    impact_matrix = scored.get("impact_matrix") if isinstance(scored, dict) else []
    impact_matrix = [item for item in impact_matrix if isinstance(item, dict)]
    aggregate = scored.get("aggregate_bias") if isinstance(scored, dict) else {}
    market_bias = _safe_text(aggregate.get("market_bias")) or "中性"

    if impact_matrix:
        confidence = round(
            sum(float(item.get("confidence") or 0.0) for item in impact_matrix[:safe_max_events])
            / max(1, min(len(impact_matrix), safe_max_events)),
            3,
        )
    else:
        confidence = 0.35

    degrade_reasons: List[str] = []
    ingest_meta = ingest_payload.get("ingest_meta") if isinstance(ingest_payload, dict) else {}
    market_errors = ingest_meta.get("market_errors") if isinstance(ingest_meta, dict) else []
    if market_errors:
        degrade_reasons.extend([_safe_text(x) for x in market_errors if _safe_text(x)])
    if not events:
        degrade_reasons.append("没有抓到足够清晰的新闻事件")
    if not coverage["market"]:
        degrade_reasons.append("行情验证不足")
    if not coverage["knowledge"]:
        degrade_reasons.append("知识库上下文不足")

    degraded = bool(degrade_reasons)
    points = _build_points(impact_matrix)
    summary = _build_summary(safe_query, market_bias, confidence, points, degraded)
    what_to_watch = _build_watch_triggers(safe_query, enriched_events)
    risk_notes = _build_risk_notes(points, degraded)

    payload: Dict[str, Any] = {
        "query": safe_query,
        "horizon": safe_horizon,
        "horizon_label": HORIZON_LABELS[safe_horizon],
        "summary": summary,
        "market_bias": market_bias,
        "confidence": confidence,
        "events": enriched_events,
        "impact_matrix": impact_matrix[:safe_max_events],
        "bullish_points": points["bullish"],
        "bearish_points": points["bearish"],
        "neutral_points": points["neutral"],
        "what_to_watch": what_to_watch,
        "risk_notes": risk_notes,
        "tool_checks": tool_checks[:18],
        "source_coverage": coverage,
        "degraded": degraded,
        "degrade_reasons": degrade_reasons[:6],
    }
    payload["trader_brief"] = _format_trader_brief(payload)
    return payload


@tool
def interpret_market_news_tool(query: str) -> str:
    """
    新闻RAG解释器。用于回答“为什么涨跌、新闻有什么影响、宏观事件怎么看、最近消息如何影响行情”。
    返回交易员口吻的新闻解读：主线、偏多/偏空点、盘面验证、反向风险、接下来盯什么。
    """
    payload = interpret_market_news(query=query, horizon="swing", use_external_news=True, max_events=6)
    return _safe_text(payload.get("trader_brief")) or json.dumps(payload, ensure_ascii=False)
