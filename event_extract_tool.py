from __future__ import annotations

import re
from typing import Any, Dict, List

REGION_KEYWORDS = {
    "china": ["china", "cn", "a-share", "shanghai", "shenzhen", "csi300", "中国", "沪深", "a股", "上证", "深证", "沪深300"],
    "us": ["united states", "u.s.", "us ", "fomc", "fed", "nasdaq", "spx", "美国", "美股", "纳指", "标普", "美联储"],
    "europe": ["europe", "eu", "ecb", "eurozone", "欧洲", "欧元区", "欧央行"],
    "middle_east": ["middle east", "iran", "israel", "gulf", "opec", "中东", "伊朗", "以色列", "海湾"],
}

POLICY_KEYWORDS = {
    "monetary": ["rate", "fomc", "fed", "easing", "tightening", "liquidity", "qe", "qt", "降息", "加息", "货币", "流动性", "央行"],
    "fiscal": ["fiscal", "budget", "stimulus", "deficit", "tax", "subsidy", "财政", "预算", "赤字", "税收", "补贴", "刺激"],
    "regulatory": ["regulation", "penalty", "restriction", "approval", "license", "compliance", "监管", "处罚", "限制", "审批", "合规"],
    "geopolitical": ["conflict", "sanction", "negotiation", "military", "ceasefire", "trade war", "冲突", "制裁", "谈判", "军事", "停火", "地缘"],
}

ASSET_KEYWORDS = {
    "equity_cn": ["csi300", "a-share", "shanghai", "shenzhen", "chiNext", "a股", "沪深300", "创业板"],
    "equity_us": ["nasdaq", "spx", "s&p", "dow", "us equity", "美股", "纳指", "标普", "道指"],
    "commodities": ["oil", "gold", "copper", "coal", "gas", "commodity", "原油", "黄金", "铜", "煤炭", "大宗"],
    "fx_rates": ["fx", "dxy", "usdcny", "eurusd", "exchange rate", "汇率", "美元指数", "人民币汇率"],
    "rates": ["yield", "treasury", "bond", "rates", "10y", "2y", "国债", "利率", "收益率"],
}

TIME_WINDOW_PATTERNS = [
    (r"(today|intraday|intra-day|日内|今天)", "intraday"),
    (r"(this week|weekly|5 days|本周|周内)", "weekly"),
    (r"(this month|monthly|本月|月内)", "monthly"),
]


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _find_bucket(text: str, keyword_map: Dict[str, List[str]], default: str = "unknown") -> str:
    lower = text.lower()
    for name, keywords in keyword_map.items():
        for keyword in keywords:
            if keyword.lower() in lower:
                return name
    return default


def _find_assets(text: str) -> List[str]:
    lower = text.lower()
    assets: List[str] = []
    for name, keywords in ASSET_KEYWORDS.items():
        if any(keyword.lower() in lower for keyword in keywords):
            assets.append(name)
    if not assets:
        assets.append("multi_asset")
    return assets


def _extract_time_window(text: str) -> str:
    lower = text.lower()
    for pattern, tag in TIME_WINDOW_PATTERNS:
        if re.search(pattern, lower):
            return tag
    return "swing"


def _event_confidence(source: str, content: str) -> float:
    s = source.lower()
    base = 0.58
    if "market_tools" in s or "市场工具" in source or "市场" in source:
        base += 0.12
    if "news" in s or "新闻" in source:
        base += 0.05
    if "error" in content.lower() or "unavailable" in content.lower():
        base -= 0.2
    return max(0.2, min(0.95, base))


def extract_event_elements(ingest_payload: Dict[str, Any], query: str) -> Dict[str, Any]:
    timeline = ingest_payload.get("timeline") if isinstance(ingest_payload, dict) else []
    events_out: List[Dict[str, Any]] = []
    source_stats: Dict[str, Dict[str, Any]] = {}

    for idx, event in enumerate(timeline if isinstance(timeline, list) else []):
        if not isinstance(event, dict):
            continue
        title = _safe_text(event.get("title"))
        content = _safe_text(event.get("content"))
        source = _safe_text(event.get("source")) or "unknown"
        merged = f"{title}\n{content}\n{query}"

        region = _find_bucket(merged, REGION_KEYWORDS)
        policy_type = _find_bucket(merged, POLICY_KEYWORDS, default="market")
        assets = _find_assets(merged)
        time_window = _extract_time_window(merged)

        direction_hint = "neutral"
        if any(
            x in merged.lower()
            for x in ["up", "rally", "stimulus", "easing", "ceasefire", "上涨", "反弹", "提振", "缓和", "停火", "降息"]
        ):
            direction_hint = "risk_on"
        elif any(
            x in merged.lower()
            for x in ["down", "selloff", "sanction", "conflict", "tightening", "下跌", "回落", "抛售", "制裁", "冲突", "加息"]
        ):
            direction_hint = "risk_off"

        event_id = _safe_text(event.get("event_id")) or f"evt_{idx + 1}"
        confidence = _event_confidence(source, content)
        summary = title or content[:120] or "event"

        region_zh = {
            "china": "中国",
            "us": "美国",
            "europe": "欧洲",
            "middle_east": "中东",
            "unknown": "未知",
        }.get(region, "未知")
        policy_zh = {
            "monetary": "货币政策",
            "fiscal": "财政政策",
            "regulatory": "监管政策",
            "geopolitical": "地缘政治",
            "market": "市场事件",
        }.get(policy_type, "市场事件")
        window_zh = {
            "intraday": "日内",
            "weekly": "周内",
            "monthly": "月内",
            "swing": "波段",
        }.get(time_window, "波段")

        asset_zh = {
            "equity_cn": "中国权益",
            "equity_us": "美国权益",
            "commodities": "大宗商品",
            "fx_rates": "外汇汇率",
            "rates": "利率债券",
            "multi_asset": "多资产",
        }

        events_out.append(
            {
                "event_id": event_id,
                "title": title or summary,
                "summary": summary,
                "source": source,
                "timestamp": _safe_text(event.get("timestamp")),
                "region": region_zh,
                "policy_type": policy_zh,
                "affected_assets": [asset_zh.get(a, a) for a in assets],
                "time_window": time_window,
                "time_window_label": window_zh,
                "direction_hint": direction_hint,
                "confidence": confidence,
                "raw_content": content[:400],
            }
        )

        stat = source_stats.setdefault(source, {"source": source, "event_count": 0, "avg_confidence": 0.0})
        stat["event_count"] += 1
        stat["avg_confidence"] += confidence

    source_ledger: List[Dict[str, Any]] = []
    for source, stat in source_stats.items():
        cnt = max(1, int(stat["event_count"]))
        avg = round(float(stat["avg_confidence"]) / cnt, 3)
        credibility = "高" if avg >= 0.75 else "中" if avg >= 0.58 else "低"
        source_ledger.append(
            {
                "source": source,
                "event_count": cnt,
                "avg_confidence": avg,
                "credibility": credibility,
            }
        )

    source_ledger.sort(key=lambda x: (x.get("credibility") != "高", -int(x.get("event_count", 0))))
    event_briefing = [f"{item['title']} ({item['region']}/{item['policy_type']})" for item in events_out[:6]]

    return {
        "events": events_out,
        "event_briefing": event_briefing,
        "source_ledger": source_ledger,
    }
