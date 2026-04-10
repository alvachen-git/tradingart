from __future__ import annotations

import json
import math
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests
from sqlalchemy import text

from risk_index_config import (
    EVENT_BASKET_V1,
    FOCUSED_CONFLICT_WATCHLIST_V1,
    ONGOING_CHAOS_CLUSTERS_V1,
    RISK_CATEGORIES,
    RISK_INDEX_CONFIG,
    FocusedConflictWatchConfig,
    OngoingChaosClusterConfig,
    RiskEventConfig,
)

BEIJING_TZ = timezone(timedelta(hours=8))
POLYMARKET_EVENTS_API = "https://gamma-api.polymarket.com/events"
GEOPOLITICAL_RISK_TABLE = "geopolitical_risk_snapshot"
_TABLES_READY_BY_ENGINE: dict[int, bool] = {}
_TOKEN_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "at",
    "before",
    "by",
    "et",
    "for",
    "from",
    "in",
    "into",
    "is",
    "of",
    "on",
    "or",
    "the",
    "their",
    "to",
    "will",
}
_GLOBAL_EXCLUDE_PHRASES = (
    "world cup",
    "fifa",
    "eurovision",
    "gta vi",
    "ipo",
    "atp paris",
    "nothing ever happens",
    "vs ",
)
_MONTH_NAME_TO_NUM = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}
_EXPLICIT_DEADLINE_PATTERNS = (
    re.compile(
        r"\b(?:by|before|until|through|on)\s+"
        r"(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|"
        r"aug(?:ust)?|sep(?:t|tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
        r"\s+(\d{1,2})(?:st|nd|rd|th)?(?:,?\s*(\d{4}))?\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:by|before|until|through)\s+end\s+of\s+"
        r"(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|"
        r"aug(?:ust)?|sep(?:t|tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
        r"(?:\s+(\d{4}))?\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:in|during)\s+"
        r"(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|"
        r"aug(?:ust)?|sep(?:t|tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
        r"(?:\s+(\d{4}))?\b",
        re.IGNORECASE,
    ),
)
_COUNTRY_LABELS_ZH = {
    "USA": "美国",
    "CHN": "中国",
    "RUS": "俄罗斯",
    "IND": "印度",
    "JPN": "日本",
    "CUB": "古巴",
    "MEX": "墨西哥",
    "ISR": "以色列",
    "TUR": "土耳其",
    "IRN": "伊朗",
    "UKR": "乌克兰",
    "PAK": "巴基斯坦",
    "PRK": "朝鲜",
    "ROK": "韩国",
    "COL": "哥伦比亚",
    "TWN": "台湾",
    "SAU": "沙特",
    "NATO": "北约",
}
_COUNTRY_PRIORITY = {
    "USA": 0,
    "CHN": 1,
    "RUS": 2,
    "IND": 3,
}


def _now_beijing(now: Optional[datetime] = None) -> datetime:
    if now is None:
        return datetime.now(BEIJING_TZ)
    if now.tzinfo is None:
        return now.replace(tzinfo=BEIJING_TZ)
    return now.astimezone(BEIJING_TZ)


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return default
        return out
    except Exception:
        return default


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _days_in_month(year: int, month: int) -> int:
    if month == 2:
        is_leap = year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
        return 29 if is_leap else 28
    if month in {4, 6, 9, 11}:
        return 30
    return 31


def _parse_iso_datetime(value: Any) -> Optional[datetime]:
    raw = _safe_text(value)
    if not raw:
        return None
    normalized = raw.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S"):
            try:
                parsed = datetime.strptime(raw, fmt)
                break
            except ValueError:
                parsed = None
        if parsed is None:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=BEIJING_TZ)
    return parsed.astimezone(BEIJING_TZ)


def _extract_textual_deadline(text: str, now: datetime) -> Optional[datetime]:
    raw = _safe_text(text)
    if not raw:
        return None
    for pattern in _EXPLICIT_DEADLINE_PATTERNS:
        match = pattern.search(raw)
        if not match:
            continue
        month_name = _safe_text(match.group(1)).lower()
        month = _MONTH_NAME_TO_NUM.get(month_name)
        if not month:
            continue
        year_group = match.groups()[-1]
        year = int(year_group) if year_group and year_group.isdigit() else now.year
        if pattern is _EXPLICIT_DEADLINE_PATTERNS[0]:
            day = int(match.group(2))
        else:
            day = _days_in_month(year, month)
        try:
            return datetime(year, month, day, 23, 59, 59, tzinfo=BEIJING_TZ)
        except ValueError:
            continue
    return None


def _candidate_deadline(candidate: Dict[str, Any], now: datetime) -> Optional[datetime]:
    direct_keys = (
        "endDate",
        "end_date",
        "endDateIso",
        "endDatetime",
        "resolutionDate",
        "resolveDate",
        "closedTime",
    )
    parsed_dates = [_parse_iso_datetime(candidate.get(key)) for key in direct_keys]
    parsed_dates = [item for item in parsed_dates if item is not None]
    if parsed_dates:
        return min(parsed_dates)

    combined_text = " | ".join(
        [
            _safe_text(candidate.get("market_title")),
            _safe_text(candidate.get("event_title")),
            _safe_text(candidate.get("question")),
            _safe_text(candidate.get("market_slug")),
            _safe_text(candidate.get("event_slug")),
        ]
    )
    return _extract_textual_deadline(combined_text, now)


def _is_candidate_expired(candidate: Dict[str, Any], now: datetime) -> bool:
    deadline = _candidate_deadline(candidate, now)
    if deadline is None:
        return False
    return deadline < now


def _normalize_text(value: Any) -> str:
    text_value = _safe_text(value).lower()
    replacements = {
        "u.s.": "usa",
        "u.s": "usa",
        "us ": "usa ",
        " us": " usa",
        "prc": "china",
        "h5n1": "avianflu",
        "bird flu": "avianflu",
        "avian flu": "avianflu",
        "covid-19": "covid",
        "north korean": "north korea",
        "south korean": "south korea",
        "treasuries": "treasury",
        "treasurys": "treasury",
    }
    for src, dst in replacements.items():
        text_value = text_value.replace(src, dst)
    text_value = text_value.replace("-", " ").replace("_", " ").replace("/", " ")
    text_value = re.sub(r"[^a-z0-9\s]", " ", text_value)
    text_value = re.sub(r"\s+", " ", text_value).strip()
    return text_value


def _normalize_token(token: str) -> str:
    token = _normalize_text(token)
    if token.endswith("ies") and len(token) > 4:
        token = token[:-3] + "y"
    elif token.endswith("ing") and len(token) > 5:
        token = token[:-3]
    elif token.endswith("ed") and len(token) > 4:
        token = token[:-2]
    elif token.endswith("es") and len(token) > 4 and token[:-2].endswith(("sh", "ch", "x", "z", "s", "o")):
        token = token[:-2]
    elif token.endswith("s") and len(token) > 3:
        token = token[:-1]
    return token.strip()


def _tokenize(value: Any) -> List[str]:
    tokens = []
    for token in _normalize_text(value).split():
        norm = _normalize_token(token)
        if not norm or norm in _TOKEN_STOPWORDS:
            continue
        tokens.append(norm)
    return tokens


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _json_loads(raw: Any, default: Any) -> Any:
    raw_text = _safe_text(raw)
    if not raw_text:
        return default
    try:
        return json.loads(raw_text)
    except Exception:
        return default


def _band_for_score(score: float) -> str:
    score = max(0.0, min(100.0, _to_float(score, 0.0)))
    if score < 25:
        return "nothing_happens"
    if score < 50:
        return "something_might_happen"
    if score < 75:
        return "something_is_brewing"
    return "things_are_happening"


def normalize_probability(value: Any) -> float:
    prob = _to_float(value, 0.0)
    if prob > 1.0:
        prob /= 100.0
    return max(0.0, min(1.0, prob))


def calc_liquidity_factor(liquidity_usd: Any) -> float:
    floor = float(RISK_INDEX_CONFIG["liquidity_floor"])
    ceiling = float(RISK_INDEX_CONFIG["liquidity_ceiling"])
    ref = float(RISK_INDEX_CONFIG["liquidity_ref_usd"])
    liquidity = max(0.0, _to_float(liquidity_usd, 0.0))
    ratio = math.log10(1.0 + liquidity) / math.log10(1.0 + ref)
    factor = floor + (ceiling - floor) * ratio
    return max(floor, min(ceiling, factor))


def cap_category_scores(
    category_raw: Dict[str, float],
    cap_ratio: float = 0.5,
    min_distinct_categories: int = 2,
) -> Tuple[Dict[str, float], bool]:
    normalized = {
        str(category): max(0.0, _to_float(value, 0.0))
        for category, value in (category_raw or {}).items()
        if str(category).strip()
    }
    return dict(normalized), False


def _keyword_hits(text: str, keywords: Iterable[str]) -> int:
    lower = text.lower()
    return sum(1 for keyword in keywords if _safe_text(keyword).lower() in lower)


def _phrase_match_score(combined_text: str, combined_tokens: set[str], phrase: str) -> float:
    normalized_phrase = _normalize_text(phrase)
    if not normalized_phrase:
        return 0.0
    if normalized_phrase in combined_text:
        return 10.0

    phrase_tokens = {_normalize_token(token) for token in normalized_phrase.split() if _normalize_token(token)}
    phrase_tokens = {token for token in phrase_tokens if token and token not in _TOKEN_STOPWORDS}
    if not phrase_tokens:
        return 0.0

    overlap = phrase_tokens & combined_tokens
    if not overlap:
        return 0.0

    if len(phrase_tokens) == 1:
        return 2.0

    coverage = len(overlap) / len(phrase_tokens)
    if len(overlap) >= 2 or coverage >= 0.66:
        return 3.0 + coverage
    return 0.0


def _contains_phrase_or_tokens(combined_text: str, combined_tokens: set[str], phrase: str) -> bool:
    return _phrase_match_score(combined_text, combined_tokens, phrase) > 0


def _extract_outcome_prices(market: Dict[str, Any]) -> List[float]:
    raw_prices = market.get("outcomePrices")
    if isinstance(raw_prices, str):
        try:
            raw_prices = json.loads(raw_prices)
        except Exception:
            raw_prices = []
    if not isinstance(raw_prices, list):
        return []
    return [normalize_probability(value) for value in raw_prices]


def _extract_text_list(raw_value: Any) -> List[str]:
    if isinstance(raw_value, str):
        try:
            parsed = json.loads(raw_value)
            raw_value = parsed
        except Exception:
            raw_value = [raw_value]
    if not isinstance(raw_value, list):
        return []
    out: List[str] = []
    for item in raw_value:
        if isinstance(item, dict):
            text_value = _safe_text(
                item.get("name")
                or item.get("label")
                or item.get("title")
                or item.get("value")
            )
        else:
            text_value = _safe_text(item)
        if text_value:
            out.append(text_value)
    return out


def _extract_outcome_names(market: Dict[str, Any]) -> List[str]:
    for key in ("outcomes", "outcomeLabels", "outcomeNames", "options"):
        names = _extract_text_list(market.get(key))
        if names:
            return names
    return []


def _extract_outcome_deltas(market: Dict[str, Any]) -> List[float]:
    for key in ("oneDayPriceChange", "oneDayPriceChangePercent", "priceChange24h", "delta24h"):
        raw_value = market.get(key)
        if raw_value in (None, ""):
            continue
        if isinstance(raw_value, str):
            try:
                raw_value = json.loads(raw_value)
            except Exception:
                return []
        if not isinstance(raw_value, list):
            return []
        deltas: List[float] = []
        for item in raw_value:
            delta = _to_float(item, 0.0)
            if abs(delta) > 1.0:
                delta /= 100.0
            deltas.append(max(-1.0, min(1.0, delta)))
        if deltas:
            return deltas
    return []


def _configured_market_structure(event: Optional[Dict[str, Any]]) -> str:
    structure = _safe_text((event or {}).get("market_structure")).lower()
    if structure in {"binary", "binary_market"}:
        return "binary_market"
    if structure in {"conditional_outcome", "conditional_outcome_market"}:
        return "conditional_outcome_market"
    if structure in {"multi_outcome_range", "multi_outcome_range_market"}:
        return "multi_outcome_range_market"
    return ""


def _market_structure_for_candidate(candidate: Dict[str, Any], event: Optional[Dict[str, Any]] = None) -> str:
    configured = _configured_market_structure(event)
    if configured:
        return configured

    outcome_count = max(
        len(_extract_outcome_names(candidate)),
        len(_extract_outcome_prices(candidate)),
    )
    min_options = int(RISK_INDEX_CONFIG.get("multi_outcome_min_options", 3))
    if outcome_count >= max(3, min_options):
        return "multi_outcome_range_market"
    return "binary_market"


def _first_number(text: str) -> Optional[float]:
    match = re.search(r"(\d+(?:\.\d+)?)", _safe_text(text))
    if not match:
        return None
    try:
        return float(match.group(1))
    except Exception:
        return None


def _outcome_direction(label: str) -> str:
    normalized = _normalize_text(label)
    if "↑" in label or any(token in normalized for token in ("above", "over", "greater", "at least", "gte", "hit ")):
        return "up"
    if "↓" in label or any(token in normalized for token in ("below", "under", "less", "at most", "lte")):
        return "down"
    return "neutral"


def _target_outcome_keywords(event: Optional[Dict[str, Any]]) -> List[str]:
    return [_safe_text(item) for item in (event or {}).get("target_outcome_keywords") or [] if _safe_text(item)]


def _target_outcome_match_score(label: str, event: Optional[Dict[str, Any]]) -> float:
    mode = _safe_text((event or {}).get("target_outcome_mode")).lower()
    keywords = _target_outcome_keywords(event)
    normalized = _normalize_text(label)
    tokens = set(_tokenize(label))
    if not mode or not keywords:
        return 0.0

    if mode in {"exact_match", "keyword_match"}:
        best = 0.0
        for keyword in keywords:
            best = max(best, _phrase_match_score(normalized, tokens, keyword))
        return best

    threshold = _first_number(" ".join(keywords))
    label_num = _first_number(label)
    if threshold is None or label_num is None:
        return 0.0

    direction = _outcome_direction(label)
    if mode == "threshold_gte":
        if direction == "down" or label_num < threshold:
            return 0.0
        return 1000.0 - abs(label_num - threshold)
    if mode == "threshold_lte":
        if direction == "up" or label_num > threshold:
            return 0.0
        return 1000.0 - abs(label_num - threshold)
    return 0.0


def _multi_outcome_fallback_mode(event: Optional[Dict[str, Any]]) -> str:
    fallback = _safe_text((event or {}).get("fallback_if_outcome_missing")).lower()
    return fallback or "skip_scoring"


def _resolve_target_outcome_selection(candidate: Dict[str, Any], event: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    if _market_structure_for_candidate(candidate, event) != "multi_outcome_range_market":
        return None

    label_candidates = []
    for key in ("groupItemTitle", "question", "market_title", "title"):
        label = _safe_text(candidate.get(key))
        if label:
            label_candidates.append(label)
    best_label = ""
    best_score = 0.0
    for label in label_candidates:
        score = _target_outcome_match_score(label, event)
        if score > best_score:
            best_score = score
            best_label = label
    if best_score > 0:
        return {
            "outcome_label": best_label,
            "probability": extract_probability_from_market({k: v for k, v in candidate.items() if k not in {"outcomes", "outcomePrices"}}),
            "delta_24h": extract_delta_24h_from_market({k: v for k, v in candidate.items() if k not in {"outcomes", "outcomePrices"}}),
            "match_score": best_score,
        }

    outcome_labels = _extract_outcome_names(candidate)
    outcome_prices = _extract_outcome_prices(candidate)
    outcome_deltas = _extract_outcome_deltas(candidate)
    if not outcome_labels or not outcome_prices:
        return None

    best_index = -1
    best_index_score = 0.0
    for idx, label in enumerate(outcome_labels):
        score = _target_outcome_match_score(label, event)
        if score > best_index_score:
            best_index_score = score
            best_index = idx
    if best_index < 0 or best_index >= len(outcome_prices) or best_index_score <= 0:
        return None

    return {
        "outcome_label": _safe_text(outcome_labels[best_index]),
        "probability": normalize_probability(outcome_prices[best_index]),
        "delta_24h": outcome_deltas[best_index] if best_index < len(outcome_deltas) else 0.0,
        "match_score": best_index_score,
    }


def extract_probability_from_market(market: Dict[str, Any], event: Optional[Dict[str, Any]] = None) -> float:
    structure = _market_structure_for_candidate(market, event)
    if structure == "multi_outcome_range_market":
        resolved = _resolve_target_outcome_selection(market, event)
        if resolved is not None:
            return normalize_probability(resolved.get("probability"))
        if _multi_outcome_fallback_mode(event) in {"skip_scoring", "monitor_only"}:
            return 0.0

    for key in ("probability", "yesPrice", "lastTradePrice", "price", "groupPrice"):
        if market.get(key) not in (None, ""):
            prob = normalize_probability(market.get(key))
            if prob > 0:
                return prob

    prices = _extract_outcome_prices(market)
    if prices:
        return normalize_probability(prices[0])
    return 0.0


def extract_delta_24h_from_market(market: Dict[str, Any], event: Optional[Dict[str, Any]] = None) -> float:
    structure = _market_structure_for_candidate(market, event)
    if structure == "multi_outcome_range_market":
        resolved = _resolve_target_outcome_selection(market, event)
        if resolved is not None:
            delta = _to_float(resolved.get("delta_24h"), 0.0)
            if abs(delta) > 1.0:
                delta /= 100.0
            return max(-1.0, min(1.0, delta))
        if _multi_outcome_fallback_mode(event) in {"skip_scoring", "monitor_only"}:
            return 0.0

    for key in ("oneDayPriceChange", "oneDayPriceChangePercent", "priceChange24h", "delta24h"):
        if market.get(key) not in (None, ""):
            delta = _to_float(market.get(key), 0.0)
            if abs(delta) > 1.0:
                delta /= 100.0
            return max(-1.0, min(1.0, delta))
    return 0.0


def extract_liquidity_usd_from_market(market: Dict[str, Any]) -> float:
    for key in ("volume24hr", "volume24Hours", "volume", "liquidity"):
        if market.get(key) not in (None, ""):
            return max(0.0, _to_float(market.get(key), 0.0))
    return 10000.0


def _candidate_identity(candidate: Dict[str, Any]) -> str:
    return "|".join(
        [
            _safe_text(candidate.get("event_id")),
            _safe_text(candidate.get("event_slug")),
            _safe_text(candidate.get("market_slug")),
            _safe_text(candidate.get("market_title")),
        ]
    ).strip("|")


def _conflict_country_aliases() -> Dict[str, List[str]]:
    aliases = dict(RISK_INDEX_CONFIG.get("dynamic_conflict_country_aliases") or {})
    return {str(code).upper(): [str(item).strip().lower() for item in items if str(item).strip()] for code, items in aliases.items()}


def _detect_candidate_countries(candidate: Dict[str, Any]) -> List[str]:
    aliases = _conflict_country_aliases()
    combined_raw = " ".join(
        [
            _safe_text(candidate.get("market_title")),
            _safe_text(candidate.get("event_title")),
            _safe_text(candidate.get("market_slug")),
            _safe_text(candidate.get("event_slug")),
        ]
    )
    combined = _normalize_text(combined_raw)
    combined_tokens = set(_tokenize(combined_raw))
    found: List[str] = []
    for code, names in aliases.items():
        if any(_contains_phrase_or_tokens(combined, combined_tokens, name) for name in names):
            found.append(code)
    return found


def _ordered_candidate_countries(candidate: Dict[str, Any]) -> List[str]:
    aliases = _conflict_country_aliases()
    combined_raw = " ".join(
        [
            _safe_text(candidate.get("market_title")),
            _safe_text(candidate.get("event_title")),
            _safe_text(candidate.get("market_slug")),
            _safe_text(candidate.get("event_slug")),
        ]
    )
    combined = _normalize_text(combined_raw)
    combined_tokens = set(_tokenize(combined_raw))
    ranked: List[Tuple[int, str]] = []
    for code, names in aliases.items():
        best_pos: Optional[int] = None
        for name in names:
            normalized_name = _normalize_text(name)
            if not normalized_name:
                continue
            pos = combined.find(normalized_name)
            if pos >= 0:
                best_pos = pos if best_pos is None else min(best_pos, pos)
            elif _contains_phrase_or_tokens(combined, combined_tokens, name):
                best_pos = 9999 if best_pos is None else min(best_pos, 9999)
        if best_pos is not None:
            ranked.append((best_pos, code))
    ranked.sort(key=lambda item: item[0])
    return [code for _, code in ranked]


def _is_dynamic_conflict_candidate(candidate: Dict[str, Any], now: Optional[datetime] = None) -> bool:
    current_ts = _now_beijing(now)
    if _is_candidate_expired(candidate, current_ts):
        return False
    combined = " ".join(
        [
            _safe_text(candidate.get("market_title")),
            _safe_text(candidate.get("event_title")),
            _safe_text(candidate.get("market_slug")),
            _safe_text(candidate.get("event_slug")),
        ]
    )
    normalized = _normalize_text(combined)
    exclude_terms = [
        _normalize_text(item)
        for item in RISK_INDEX_CONFIG.get("dynamic_conflict_exclude_keywords", [])
        if _safe_text(item)
    ]
    if any(term and term in normalized for term in exclude_terms):
        return False
    actions = [
        _normalize_text(item)
        for item in RISK_INDEX_CONFIG.get("dynamic_conflict_action_keywords", [])
        if _safe_text(item)
    ]
    if not any(action and action in normalized for action in actions):
        return False
    countries = [code for code in _detect_candidate_countries(candidate) if code != "NATO"]
    if len(set(countries)) >= 2:
        return True
    if "NATO" in _detect_candidate_countries(candidate) and countries:
        return True
    return False


def _dynamic_country_weight(country_codes: List[str]) -> float:
    weights = dict(RISK_INDEX_CONFIG.get("dynamic_conflict_country_weights") or {})
    default_weight = _to_float(weights.get("default"), 0.35)
    if not country_codes:
        return default_weight
    return max(_to_float(weights.get(code, default_weight), default_weight) for code in set(country_codes))


def _canonical_country_codes(country_codes: List[str]) -> List[str]:
    unique = sorted(
        set(code for code in country_codes if code),
        key=lambda code: (_COUNTRY_PRIORITY.get(code, 100), code),
    )
    return unique


def _canonical_pair_tag_from_codes(country_codes: List[str]) -> str:
    unique = _canonical_country_codes(country_codes)
    if not unique:
        return "DYNAMIC_CONFLICT"
    return "_".join(unique[:3])


def _dynamic_pair_tag(country_codes: List[str]) -> str:
    return _canonical_pair_tag_from_codes(country_codes)


def _dynamic_region_tag(country_codes: List[str]) -> str:
    code_set = set(country_codes)
    if {"ISR", "IRN", "TUR", "SAU"} & code_set:
        return "middle_east"
    if {"CHN", "JPN", "PRK", "ROK", "TWN"} & code_set:
        return "east_asia"
    if {"RUS", "UKR", "NATO"} & code_set:
        return "europe"
    if {"USA", "CUB", "MEX", "COL"} & code_set:
        return "north_america"
    if "IND" in code_set or "PAK" in code_set:
        return "global"
    return "global"


def _dynamic_market_semantics(candidate: Dict[str, Any], country_codes: List[str]) -> str:
    if len(country_codes) < 2:
        return "direct_conflict"
    combined = _normalize_text(
        " ".join(
            [
                _safe_text(candidate.get("market_title")),
                _safe_text(candidate.get("event_title")),
                _safe_text(candidate.get("market_slug")),
                _safe_text(candidate.get("event_slug")),
            ]
        )
    )
    conditional_terms = [
        _normalize_text(item)
        for item in RISK_INDEX_CONFIG.get("dynamic_conflict_conditional_keywords", [])
        if _safe_text(item)
    ]
    action_terms = [
        _normalize_text(item)
        for item in RISK_INDEX_CONFIG.get("dynamic_conflict_action_keywords", [])
        if _safe_text(item)
    ]
    has_action = any(term and term in combined for term in action_terms)
    has_conditional = any(term and term in combined for term in conditional_terms)
    if not (has_action and has_conditional):
        return "direct_conflict"
    if "survive" in combined and "strike" in combined:
        return "conditional_outcome"
    if "regime" in combined and ("strike" in combined or "attack" in combined or "military action" in combined):
        return "conditional_outcome"
    if ("collapse" in combined or "fall" in combined or "removed" in combined or "overthrown" in combined) and (
        "after" in combined or "following" in combined
    ):
        return "conditional_outcome"
    return "direct_conflict"


def _market_theme_key(item: Dict[str, Any]) -> str:
    event_slug = _safe_text(item.get("event_slug")).lower()
    if event_slug:
        return event_slug
    market_slug = _safe_text(item.get("market_slug")).lower()
    if market_slug:
        return market_slug
    title = _normalize_text(_safe_text(item.get("market_title")) or _safe_text(item.get("display_title")))
    title = re.sub(r"\b(april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec)\b", " ", title)
    title = re.sub(r"\b\d{1,2}\b", " ", title)
    title = re.sub(r"\b2026\b|\b2027\b", " ", title)
    title = re.sub(r"\s+", " ", title).strip()
    return title


def _localized_dynamic_title(candidate: Dict[str, Any], country_codes: List[str], market_semantics: str = "direct_conflict") -> str:
    ordered = _ordered_candidate_countries(candidate)
    countries = ordered or country_codes
    countries = [code for code in countries if code in _COUNTRY_LABELS_ZH]
    combined = _normalize_text(
        " ".join(
            [
                _safe_text(candidate.get("market_title")),
                _safe_text(candidate.get("event_title")),
                _safe_text(candidate.get("market_slug")),
                _safe_text(candidate.get("event_slug")),
            ]
        )
    )
    if len(countries) >= 2:
        a = _COUNTRY_LABELS_ZH.get(countries[0], countries[0])
        b = _COUNTRY_LABELS_ZH.get(countries[1], countries[1])
        if market_semantics == "conditional_outcome":
            actor = b
            target = a
            if "survive" in combined and "regime" in combined and "strike" in combined:
                return f"{actor}打击{target}后的政权存续风险"
            if "survive" in combined and "strike" in combined:
                return f"{actor}打击{target}后的结果风险"
            if "collapse" in combined or "fall" in combined or "removed" in combined or "overthrown" in combined:
                return f"{actor}-{target}冲突后果风险"
            return f"{actor}-{target}结果风险"
        if "invade" in combined or "invasion" in combined:
            return f"{a}入侵{b}风险"
        if "strike" in combined:
            return f"{a}打击{b}风险"
        if "military action" in combined or "ground operation" in combined or "operation" in combined:
            return f"{a}对{b}军事行动"
        if "capture" in combined:
            return f"{a}-{b}战场推进"
        if "blockade" in combined:
            return f"{a}-{b}封锁风险"
        return f"{a}-{b}冲突风险"
    if len(countries) == 1:
        a = _COUNTRY_LABELS_ZH.get(countries[0], countries[0])
        return f"{a}相关冲突风险"
    return "国家间冲突风险"


def _pair_codes_from_tag(pair_tag: str) -> List[str]:
    parts = [part for part in _safe_text(pair_tag).split("_") if part]
    codes = [part for part in parts if part.isupper() and part not in {"BASE", "GLOBAL", "INTERNAL"}]
    return codes


def _scoring_dedupe_key(item: Dict[str, Any]) -> str:
    pair_codes = list(item.get("country_codes") or []) or _pair_codes_from_tag(_safe_text(item.get("pair_tag")))
    if len(pair_codes) >= 2:
        return f"PAIR::{_canonical_pair_tag_from_codes(pair_codes)}"
    return f"THEME::{_market_theme_key(item)}"


def _build_dynamic_conflict_markets(
    candidates: List[Dict[str, Any]],
    used_candidate_ids: set[str],
    now: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    limit = max(0, int(RISK_INDEX_CONFIG.get("dynamic_conflict_limit", 36)))
    base_impact = _to_float(RISK_INDEX_CONFIG.get("dynamic_conflict_base_impact"), 0.55)
    ranked_by_theme: Dict[str, Dict[str, Any]] = {}

    for candidate in candidates or []:
        identity = _candidate_identity(candidate)
        if identity and identity in used_candidate_ids:
            continue
        if not _is_dynamic_conflict_candidate(candidate, now=now):
            continue

        countries = _detect_candidate_countries(candidate)
        market_semantics = _dynamic_market_semantics(candidate, countries)
        country_weight = _dynamic_country_weight(countries)
        probability = extract_probability_from_market(candidate)
        if probability <= 0:
            continue
        liquidity_usd = extract_liquidity_usd_from_market(candidate)
        liquidity_factor = calc_liquidity_factor(liquidity_usd)
        semantics_multiplier = (
            _to_float(RISK_INDEX_CONFIG.get("conditional_outcome_weight_multiplier"), 0.35)
            if market_semantics == "conditional_outcome"
            else 1.0
        )
        event_raw = probability * base_impact * country_weight * liquidity_factor * semantics_multiplier
        item = {
            "event_key": f"dynamic::{_safe_text(candidate.get('market_slug') or candidate.get('event_slug') or identity)}",
            "display_title": _localized_dynamic_title(candidate, countries, market_semantics=market_semantics),
            "category": "military_conflict",
            "region_tag": _dynamic_region_tag(countries),
            "pair_tag": _dynamic_pair_tag(countries),
            "probability": probability,
            "delta_24h": extract_delta_24h_from_market(candidate),
            "impact_weight": round(base_impact * country_weight * semantics_multiplier, 4),
            "liquidity_usd": liquidity_usd,
            "liquidity_factor": liquidity_factor,
            "event_raw": event_raw,
            "market_title": _safe_text(candidate.get("market_title")),
            "source_url": _safe_text(candidate.get("source_url")),
            "market_slug": _safe_text(candidate.get("market_slug")),
            "event_slug": _safe_text(candidate.get("event_slug")),
            "country_codes": sorted(set(countries)),
            "is_dynamic_conflict": True,
            "market_semantics": market_semantics,
            "semantics_weight_multiplier": round(semantics_multiplier, 4),
        }
        theme_key = _canonical_pair_tag_from_codes(countries)
        if not theme_key or theme_key == "DYNAMIC_CONFLICT":
            theme_key = _safe_text(candidate.get("event_slug") or candidate.get("market_slug") or identity).lower()
        existing = ranked_by_theme.get(theme_key)
        if existing is None or item["event_raw"] > existing.get("event_raw", 0.0):
            ranked_by_theme[theme_key] = item

    ranked = sorted(ranked_by_theme.values(), key=lambda item: item.get("event_raw", 0.0), reverse=True)
    return ranked[:limit]


def _dedupe_scored_markets(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    best_by_theme: Dict[str, Dict[str, Any]] = {}
    for item in items or []:
        theme_key = _scoring_dedupe_key(item)
        existing = best_by_theme.get(theme_key)
        if existing is None:
            best_by_theme[theme_key] = item
            continue
        current_score = _to_float(item.get("event_raw"), 0.0)
        existing_score = _to_float(existing.get("event_raw"), 0.0)
        if current_score > existing_score:
            best_by_theme[theme_key] = item
        elif abs(current_score - existing_score) < 1e-9 and not item.get("is_dynamic_conflict") and existing.get("is_dynamic_conflict"):
            best_by_theme[theme_key] = item
    out = list(best_by_theme.values())
    out.sort(key=lambda item: item.get("event_raw", 0.0), reverse=True)
    return out


def _flatten_polymarket_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    flattened: List[Dict[str, Any]] = []
    for event in events or []:
        event_title = _safe_text(event.get("title"))
        event_slug = _safe_text(event.get("slug") or event.get("eventSlug"))
        event_id = _safe_text(event.get("id"))
        event_volume = _to_float(event.get("volume24hr") or event.get("volume"), 0.0)
        event_tags = event.get("tags") or []
        event_tag_slugs = [
            _safe_text(item.get("slug")).lower()
            for item in event_tags
            if isinstance(item, dict) and _safe_text(item.get("slug"))
        ]
        event_tag_labels = [
            _safe_text(item.get("label"))
            for item in event_tags
            if isinstance(item, dict) and _safe_text(item.get("label"))
        ]
        for market in event.get("markets") or []:
            item = dict(market)
            item["event_title"] = event_title
            item["event_slug"] = event_slug
            item["event_id"] = event_id
            item["event_volume24hr"] = event_volume
            item["event_tag_slugs"] = event_tag_slugs
            item["event_tag_labels"] = event_tag_labels
            item["market_slug"] = _safe_text(market.get("slug") or market.get("marketSlug"))
            item["market_title"] = _safe_text(
                market.get("question") or market.get("groupItemTitle") or market.get("title") or event_title
            )
            item["source_url"] = _safe_text(market.get("url"))
            if not item["source_url"] and event_slug:
                item["source_url"] = f"https://polymarket.com/event/{event_slug}"
            elif not item["source_url"] and item["market_slug"]:
                item["source_url"] = f"https://polymarket.com/event/{item['market_slug']}"
            flattened.append(item)
    return flattened


def _event_tag_slugs(event: Dict[str, Any]) -> set[str]:
    slugs = set()
    for item in event.get("tags") or []:
        if not isinstance(item, dict):
            continue
        slug = _safe_text(item.get("slug")).lower()
        if slug:
            slugs.add(slug)
    return slugs


def _event_search_text(event: Dict[str, Any]) -> str:
    parts = [
        _safe_text(event.get("title")),
        _safe_text(event.get("description")),
        _safe_text(event.get("slug")),
        _safe_text(event.get("ticker")),
    ]
    for item in event.get("tags") or []:
        if not isinstance(item, dict):
            continue
        parts.append(_safe_text(item.get("label")))
        parts.append(_safe_text(item.get("slug")))
    return _normalize_text(" ".join(parts))


def _watchlist_configs() -> List[FocusedConflictWatchConfig]:
    return [item for item in FOCUSED_CONFLICT_WATCHLIST_V1 if item.get("active", True)]


def _watchlist_event_key(event: Dict[str, Any], watch: FocusedConflictWatchConfig) -> bool:
    searchable = _event_search_text(event)
    searchable_tokens = set(_tokenize(searchable))
    detected_codes = set(_detect_candidate_countries(event))
    watch_codes = {str(code).upper() for code in (watch.get("country_codes") or []) if _safe_text(code)}

    if len(watch_codes) >= 2:
        if len(detected_codes & watch_codes) >= 2:
            return True
    elif watch_codes and detected_codes & watch_codes:
        return True

    for keyword in watch.get("query_keywords") or []:
        if _contains_phrase_or_tokens(searchable, searchable_tokens, keyword):
            return True
    return False


def _event_watch_hits(event: Dict[str, Any]) -> List[str]:
    hits: List[str] = []
    for watch in _watchlist_configs():
        if _watchlist_event_key(event, watch):
            hits.append(_safe_text(watch.get("watch_key")))
    return hits


def _event_priority_tuple(event: Dict[str, Any]) -> Tuple[int, float]:
    watch_hits = event.get("_watch_hits") or []
    watch_weight = 1 if watch_hits else 0
    event_volume = _to_float(event.get("volume24hr") or event.get("volume"), 0.0)
    return watch_weight, event_volume


def _is_relevant_polymarket_event(event: Dict[str, Any]) -> bool:
    allowed_tags = {str(item).strip().lower() for item in RISK_INDEX_CONFIG.get("polymarket_allowed_tag_slugs", []) if str(item).strip()}
    excluded_tags = {str(item).strip().lower() for item in RISK_INDEX_CONFIG.get("polymarket_excluded_tag_slugs", []) if str(item).strip()}
    allowed_keywords = [
        _normalize_text(item)
        for item in RISK_INDEX_CONFIG.get("polymarket_allowed_text_keywords", [])
        if _safe_text(item)
    ]
    tag_slugs = _event_tag_slugs(event)
    if tag_slugs & excluded_tags:
        return False
    if tag_slugs & allowed_tags:
        return True

    searchable = _event_search_text(event)
    if not searchable:
        return False
    return any(keyword and keyword in searchable for keyword in allowed_keywords)


def fetch_polymarket_events(limit: int = 250, timeout: int = 12) -> List[Dict[str, Any]]:
    headers = {"User-Agent": "Mozilla/5.0"}
    page_size = max(50, min(int(RISK_INDEX_CONFIG.get("polymarket_fetch_page_size", 200)), 500))
    max_pages = max(1, int(RISK_INDEX_CONFIG.get("polymarket_fetch_max_pages", 8)))
    supplemental_max_pages = max(0, int(RISK_INDEX_CONFIG.get("polymarket_supplemental_max_pages", 0)))
    target = max(20, int(limit))
    events: List[Dict[str, Any]] = []
    supplemental_events: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()
    pages_scanned = 0

    def _append_event(event: Dict[str, Any], bucket: List[Dict[str, Any]]) -> bool:
        event_id = _safe_text(event.get("id") or event.get("slug") or event.get("ticker"))
        if event_id and event_id in seen_ids:
            return False
        if event_id:
            seen_ids.add(event_id)
        if not _is_relevant_polymarket_event(event):
            return False
        watch_hits = _event_watch_hits(event)
        event_copy = dict(event)
        event_copy["_watch_hits"] = watch_hits
        bucket.append(event_copy)
        return True

    for page_idx in range(max_pages):
        params = {
            "limit": page_size,
            "offset": page_idx * page_size,
            "active": "true",
            "closed": "false",
            "archived": "false",
            "order": "volume24hr",
            "ascending": "false",
        }
        resp = requests.get(POLYMARKET_EVENTS_API, params=params, headers=headers, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        batch = data if isinstance(data, list) else []
        if not batch:
            break
        pages_scanned = page_idx + 1

        for event in batch:
            _append_event(event, events)

        if len(events) >= target:
            break
        if len(batch) < page_size:
            break

    pending_watch_keys = {
        _safe_text(item.get("watch_key"))
        for item in _watchlist_configs()
        if _safe_text(item.get("watch_key"))
    }
    found_watch_keys = {
        hit
        for event in events
        for hit in (event.get("_watch_hits") or [])
        if _safe_text(hit)
    }
    missing_watch_keys = pending_watch_keys - found_watch_keys

    for page_offset in range(supplemental_max_pages):
        if not missing_watch_keys:
            break
        page_idx = pages_scanned + page_offset
        params = {
            "limit": page_size,
            "offset": page_idx * page_size,
            "active": "true",
            "closed": "false",
            "archived": "false",
            "order": "volume24hr",
            "ascending": "false",
        }
        resp = requests.get(POLYMARKET_EVENTS_API, params=params, headers=headers, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        batch = data if isinstance(data, list) else []
        if not batch:
            break

        for event in batch:
            watch_hits = _event_watch_hits(event)
            if not watch_hits or not (set(watch_hits) & missing_watch_keys):
                event_id = _safe_text(event.get("id") or event.get("slug") or event.get("ticker"))
                if event_id and event_id not in seen_ids:
                    seen_ids.add(event_id)
                continue
            appended = _append_event(event, supplemental_events)
            if appended:
                missing_watch_keys -= set(watch_hits)

        if len(batch) < page_size:
            break

    events.sort(key=_event_priority_tuple, reverse=True)
    supplemental_events.sort(key=_event_priority_tuple, reverse=True)
    combined_events: List[Dict[str, Any]] = []
    seen_combined: set[str] = set()
    for event in supplemental_events + events:
        event_id = _safe_text(event.get("id") or event.get("slug") or event.get("ticker"))
        if event_id and event_id in seen_combined:
            continue
        if event_id:
            seen_combined.add(event_id)
        combined_events.append(event)
    return combined_events[:target]


def fetch_polymarket_candidates(limit: int = 250, timeout: int = 12) -> List[Dict[str, Any]]:
    return _flatten_polymarket_events(fetch_polymarket_events(limit=limit, timeout=timeout))


def _candidate_match_tuple(event: Dict[str, Any], candidate: Dict[str, Any], now: Optional[datetime] = None) -> Tuple[int, float, int]:
    current_ts = _now_beijing(now)
    market_slug = _safe_text(candidate.get("market_slug")).lower()
    event_slug = _safe_text(candidate.get("event_slug")).lower()
    combined_raw = " ".join(
        [
            _safe_text(candidate.get("groupItemTitle")),
            _safe_text(candidate.get("market_title")),
            _safe_text(candidate.get("title")),
            _safe_text(candidate.get("question")),
            _safe_text(candidate.get("event_title")),
            market_slug,
            event_slug,
        ]
    )
    combined = _normalize_text(combined_raw)
    combined_tokens = set(_tokenize(combined_raw))

    market_allow = {slug.lower() for slug in event.get("market_slug_allowlist") or [] if _safe_text(slug)}
    event_allow = {slug.lower() for slug in event.get("event_slug_allowlist") or [] if _safe_text(slug)}
    exclude_keywords = [item for item in event.get("exclude_keywords") or [] if _safe_text(item)]
    must_contain_any = [item for item in event.get("must_contain_any") or [] if _safe_text(item)]
    must_contain_all = [item for item in event.get("must_contain_all") or [] if _safe_text(item)]
    must_contain_any_group = [
        [item for item in (group or []) if _safe_text(item)]
        for group in event.get("must_contain_any_group") or []
        if group
    ]

    if any(phrase in combined for phrase in _GLOBAL_EXCLUDE_PHRASES):
        return 0, 0.0, 0
    if _is_candidate_expired(candidate, current_ts):
        return 0, 0.0, 0
    if exclude_keywords and any(_contains_phrase_or_tokens(combined, combined_tokens, item) for item in exclude_keywords):
        return 0, 0.0, 0
    if must_contain_all and not all(_contains_phrase_or_tokens(combined, combined_tokens, item) for item in must_contain_all):
        return 0, 0.0, 0
    if must_contain_any_group and not any(
        all(_contains_phrase_or_tokens(combined, combined_tokens, item) for item in group)
        for group in must_contain_any_group
    ):
        return 0, 0.0, 0
    if must_contain_any and not any(_contains_phrase_or_tokens(combined, combined_tokens, item) for item in must_contain_any):
        return 0, 0.0, 0

    structure = _market_structure_for_candidate(candidate, event)
    if structure == "multi_outcome_range_market":
        resolved = _resolve_target_outcome_selection(candidate, event)
        if resolved is None and _multi_outcome_fallback_mode(event) in {"skip_scoring", "monitor_only"}:
            return 0, 0.0, 0

    if market_slug and any(allow in market_slug for allow in market_allow):
        bonus = int((_resolve_target_outcome_selection(candidate, event) or {}).get("match_score", 0))
        return 3, extract_liquidity_usd_from_market(candidate), 999 + bonus
    if event_slug and any(allow in event_slug for allow in event_allow):
        bonus = int((_resolve_target_outcome_selection(candidate, event) or {}).get("match_score", 0))
        return 2, extract_liquidity_usd_from_market(candidate), 999 + bonus

    phrase_score = 0.0
    matched_phrases = 0
    for phrase in event.get("query_keywords") or []:
        score = _phrase_match_score(combined, combined_tokens, phrase)
        if score > 0:
            matched_phrases += 1
            phrase_score += score

    if matched_phrases <= 0:
        return 0, 0.0, 0
    semantics_bonus = int((_resolve_target_outcome_selection(candidate, event) or {}).get("match_score", 0))
    return 1, extract_liquidity_usd_from_market(candidate), int(phrase_score * 100 + matched_phrases + semantics_bonus)


def select_representative_market(
    event: Dict[str, Any],
    candidates: List[Dict[str, Any]],
    now: Optional[datetime] = None,
) -> Optional[Dict[str, Any]]:
    ranked: List[Tuple[Tuple[int, float, int], Dict[str, Any]]] = []
    for candidate in candidates or []:
        match_tuple = _candidate_match_tuple(event, candidate, now=now)
        if match_tuple[0] <= 0:
            continue
        ranked.append((match_tuple, candidate))
    if not ranked:
        return None
    ranked.sort(key=lambda item: (item[0][0], item[0][2], item[0][1]), reverse=True)
    return dict(ranked[0][1])
def _make_explanation_result(top_market: Dict[str, Any], use_external_news: bool) -> Tuple[Dict[str, Any], str]:
    title = _safe_text(top_market.get("display_title") or top_market.get("market_title") or top_market.get("event_key"))
    probability = normalize_probability(top_market.get("probability"))
    delta = _to_float(top_market.get("delta_24h"), 0.0)
    delta_text = f"{delta * 100:+.1f}%"
    market_semantics = _safe_text(top_market.get("market_semantics")) or "direct_conflict"
    target_outcome_label = _safe_text(top_market.get("target_outcome_label"))
    if market_semantics == "conditional_outcome":
        conditional_reason = (
            f"{title} 当前市场定价约为 {probability * 100:.1f}%，24 小时变化 {delta_text}，"
            f"这是对潜在冲突后果的定价，不等于冲突本身发生概率。"
        )
        return (
            {
                "event_key": _safe_text(top_market.get("event_key")),
                "one_line_reason": conditional_reason,
                "source_links": [],
            },
            "fallback",
        )
    if _safe_text(top_market.get("market_structure")) == "multi_outcome_range_market":
        range_reason = (
            f"{title} 当前跟踪的是 {target_outcome_label or '目标档位'}，市场定价约为 {probability * 100:.1f}%，"
            f"24 小时变化 {delta_text}，说明对应区间风险{'上升' if delta > 0 else '回落' if delta < 0 else '维持稳定'}。"
        )
        return (
            {
                "event_key": _safe_text(top_market.get("event_key")),
                "one_line_reason": range_reason,
                "source_links": [],
            },
            "fallback",
        )
    fallback_reason = (
        f"{title} 当前市场定价约为 {probability * 100:.1f}%，24 小时变化 {delta_text}，"
        f"属于 {RISK_CATEGORIES.get(_safe_text(top_market.get('category')), '风险')} 的核心扰动。"
    )

    if not use_external_news:
        return (
            {
                "event_key": _safe_text(top_market.get("event_key")),
                "one_line_reason": fallback_reason,
                "source_links": [],
            },
            "fallback",
        )

    try:
        from event_extract_tool import extract_event_elements
        from event_ingest_tool import ingest_event_timeline

        ingest_payload = ingest_event_timeline(query=title, analysis_horizon="weekly", use_external_news=True)
        extracted = extract_event_elements(ingest_payload, title)
        events = extracted.get("events") or []
        if events:
            summary = _safe_text(events[0].get("summary") or events[0].get("title"))
            if summary:
                return (
                    {
                        "event_key": _safe_text(top_market.get("event_key")),
                        "one_line_reason": summary,
                        "source_links": [],
                    },
                    "external",
                )
    except Exception:
        pass

    return (
        {
            "event_key": _safe_text(top_market.get("event_key")),
            "one_line_reason": fallback_reason,
            "source_links": [],
        },
        "fallback",
    )


def _make_explanation(top_market: Dict[str, Any], use_external_news: bool) -> Dict[str, Any]:
    explanation, _ = _make_explanation_result(top_market, use_external_news=use_external_news)
    return explanation


def _should_run_news_explainer(top_markets: List[Dict[str, Any]], use_news_explainer: bool) -> bool:
    if not use_news_explainer:
        return False
    threshold = max(0.0, float(RISK_INDEX_CONFIG.get("news_explainer_delta_threshold", 0.05)))
    return any(abs(_to_float(item.get("delta_24h"), 0.0)) >= threshold for item in top_markets or [])


def _candidate_horizon_weight(candidate: Dict[str, Any], now: datetime) -> float:
    deadline = _candidate_deadline(candidate, now)
    if deadline is None:
        return 0.20
    days_to_deadline = max(0.0, (deadline - now).total_seconds() / 86400.0)
    if days_to_deadline <= 45:
        return 0.45
    if days_to_deadline <= 120:
        return 0.35
    return 0.20


def _candidate_horizon_bucket(candidate: Dict[str, Any], now: datetime) -> str:
    deadline = _candidate_deadline(candidate, now)
    if deadline is None:
        return "unknown"
    days_to_deadline = max(0.0, (deadline - now).total_seconds() / 86400.0)
    if days_to_deadline <= 45:
        return "near"
    if days_to_deadline <= 120:
        return "mid"
    return "far"


def _select_cluster_reverse_markets(
    cluster: OngoingChaosClusterConfig,
    candidates: List[Dict[str, Any]],
    now: datetime,
) -> List[Dict[str, Any]]:
    ranked_by_bucket: Dict[str, Tuple[Tuple[int, float, int], Dict[str, Any]]] = {}
    for candidate in candidates or []:
        match_tuple = _candidate_match_tuple(cluster, candidate, now=now)
        if match_tuple[0] <= 0:
            continue
        bucket = _candidate_horizon_bucket(candidate, now)
        best = ranked_by_bucket.get(bucket)
        if best is None or match_tuple > best[0]:
            ranked_by_bucket[bucket] = (match_tuple, candidate)

    ranked = [item[1] for item in ranked_by_bucket.values()]
    ranked.sort(
        key=lambda item: (
            _candidate_horizon_weight(item, now),
            extract_liquidity_usd_from_market(item),
            extract_probability_from_market(item),
        ),
        reverse=True,
    )
    return [dict(item) for item in ranked[:3]]


def _build_ongoing_baseline(
    candidates: List[Dict[str, Any]],
    now: datetime,
) -> Tuple[List[Dict[str, Any]], float]:
    clusters: List[Dict[str, Any]] = []
    baseline_total = 0.0
    for cluster in ONGOING_CHAOS_CLUSTERS_V1:
        if not cluster.get("active", True):
            continue
        reverse_markets = _select_cluster_reverse_markets(cluster, candidates, now)
        if not reverse_markets:
            continue

        weighted_sum = 0.0
        weight_total = 0.0
        selected_markets = []
        for market in reverse_markets:
            probability_end = extract_probability_from_market(market)
            liquidity_factor = calc_liquidity_factor(extract_liquidity_usd_from_market(market))
            horizon_weight = _candidate_horizon_weight(market, now)
            combined_weight = horizon_weight * liquidity_factor
            weighted_sum += combined_weight * (1.0 - probability_end)
            weight_total += combined_weight
            selected_markets.append(
                {
                    "market_title": _safe_text(market.get("market_title")),
                    "source_url": _safe_text(market.get("source_url")),
                    "probability_end": round(probability_end, 4),
                    "probability_persist": round(1.0 - probability_end, 4),
                    "horizon_weight": round(horizon_weight, 4),
                }
            )

        persistence_score = 0.0 if weight_total <= 0 else max(0.0, min(1.0, weighted_sum / weight_total))
        contribution = float(cluster["max_points"]) * persistence_score * float(cluster.get("intensity_multiplier", 1.0))
        baseline_total += contribution
        clusters.append(
            {
                "cluster_key": cluster["cluster_key"],
                "display_title": cluster["display_title"],
                "category": cluster["category"],
                "region_tag": cluster["region_tag"],
                "pair_tag": cluster["pair_tag"],
                "persistence_score": round(persistence_score, 4),
                "max_points": float(cluster["max_points"]),
                "contribution": round(contribution, 4),
                "reverse_markets": selected_markets,
            }
        )

    clusters.sort(key=lambda item: item.get("contribution", 0.0), reverse=True)
    baseline_cap = float(RISK_INDEX_CONFIG.get("ongoing_score_max", 60.0))
    return clusters, max(0.0, min(baseline_cap, baseline_total))


def _calculate_escalation_score(raw_total: float) -> float:
    max_score = float(RISK_INDEX_CONFIG.get("escalation_score_max", 30.0))
    curve_scale = max(0.2, float(RISK_INDEX_CONFIG.get("pressure_curve_scale", 1.35)))
    return max(0.0, min(max_score, max_score * (1.0 - math.exp(-max(0.0, raw_total) / curve_scale))))


def _calculate_contagion_bonus(
    resolved_events: List[Dict[str, Any]],
    ongoing_clusters: List[Dict[str, Any]],
) -> float:
    max_bonus = float(RISK_INDEX_CONFIG.get("contagion_bonus_max", 10.0))
    min_probability = float(RISK_INDEX_CONFIG.get("cluster_min_probability", 0.08))
    region_activity: Dict[str, float] = {}

    for cluster in ongoing_clusters:
        region_tag = _safe_text(cluster.get("region_tag")) or "global"
        region_activity[region_tag] = region_activity.get(region_tag, 0.0) + (_to_float(cluster.get("contribution"), 0.0) / 10.0)

    for item in resolved_events:
        if normalize_probability(item.get("probability")) < min_probability:
            continue
        region_tag = _safe_text(item.get("region_tag")) or "global"
        region_activity[region_tag] = region_activity.get(region_tag, 0.0) + (_to_float(item.get("event_raw"), 0.0) * 3.0)

    active_regions = {region for region, score in region_activity.items() if score >= 0.6}
    if not active_regions:
        return 0.0

    intensity = sum(min(3.0, score) for score in region_activity.values())
    bonus = max(0.0, len(active_regions) - 1) * 1.8 + max(0.0, intensity - 2.0) * 0.9
    return max(0.0, min(max_bonus, bonus))


def build_risk_snapshot(
    candidates: List[Dict[str, Any]],
    previous_snapshot: Optional[Dict[str, Any]] = None,
    now: Optional[datetime] = None,
    use_news_explainer: bool = True,
) -> Dict[str, Any]:
    current_ts = _now_beijing(now)
    ongoing_clusters, ongoing_baseline_score = _build_ongoing_baseline(candidates or [], current_ts)
    resolved_events: List[Dict[str, Any]] = []
    used_candidate_ids: set[str] = set()

    for event in EVENT_BASKET_V1[: int(RISK_INDEX_CONFIG["max_events"])]:
        if not event.get("active", True):
            continue
        event_candidates = [
            candidate for candidate in (candidates or [])
            if _candidate_identity(candidate) not in used_candidate_ids
        ]
        selected = select_representative_market(event, event_candidates, now=current_ts)
        if not selected:
            continue
        identity = _candidate_identity(selected)
        if identity:
            used_candidate_ids.add(identity)
        selected_structure = _market_structure_for_candidate(selected, event)
        target_outcome = _resolve_target_outcome_selection(selected, event) if selected_structure == "multi_outcome_range_market" else None
        probability = extract_probability_from_market(selected, event=event)
        if probability <= 0:
            continue
        liquidity_usd = extract_liquidity_usd_from_market(selected)
        liquidity_factor = calc_liquidity_factor(liquidity_usd)
        event_raw = probability * float(event["impact_weight"]) * liquidity_factor
        resolved_events.append(
            {
                "event_key": event["event_key"],
                "display_title": event["display_title"],
                "category": event["category"],
                "region_tag": event["region_tag"],
                "pair_tag": event["pair_tag"],
                "probability": probability,
                "delta_24h": extract_delta_24h_from_market(selected, event=event),
                "impact_weight": float(event["impact_weight"]),
                "liquidity_usd": liquidity_usd,
                "liquidity_factor": liquidity_factor,
                "event_raw": event_raw,
                "market_title": _safe_text(selected.get("market_title")),
                "source_url": _safe_text(selected.get("source_url")),
                "market_slug": _safe_text(selected.get("market_slug")),
                "event_slug": _safe_text(selected.get("event_slug")),
                "is_dynamic_conflict": False,
                "market_semantics": "direct_conflict",
                "market_structure": selected_structure,
                "target_outcome_label": _safe_text((target_outcome or {}).get("outcome_label")),
            }
        )

    dynamic_conflicts = _build_dynamic_conflict_markets(candidates or [], used_candidate_ids, now=current_ts)
    resolved_events.extend(dynamic_conflicts)
    resolved_events = _dedupe_scored_markets(resolved_events)
    category_raw = {key: 0.0 for key in RISK_CATEGORIES}
    category_baseline = {key: 0.0 for key in RISK_CATEGORIES}
    pair_raw: Dict[str, float] = {}

    for item in resolved_events:
        category_raw[item["category"]] = category_raw.get(item["category"], 0.0) + float(item["event_raw"])
        pair_tag = _safe_text(item.get("pair_tag"))
        pair_raw[pair_tag] = pair_raw.get(pair_tag, 0.0) + float(item["event_raw"])

    for cluster in ongoing_clusters:
        category_baseline[cluster["category"]] = category_baseline.get(cluster["category"], 0.0) + _to_float(cluster.get("contribution"), 0.0)
        pair_tag = _safe_text(cluster.get("pair_tag"))
        pair_raw[pair_tag] = pair_raw.get(pair_tag, 0.0) + _to_float(cluster.get("contribution"), 0.0)

    escalation_raw_total = float(sum(category_raw.values()))
    escalation_score = _calculate_escalation_score(escalation_raw_total)
    contagion_bonus = _calculate_contagion_bonus(resolved_events, ongoing_clusters)
    score_raw = max(0.0, min(100.0, ongoing_baseline_score + escalation_score + contagion_bonus))
    low_diversity = len({item["region_tag"] for item in ongoing_clusters if _to_float(item.get("contribution"), 0.0) > 0.0}) < 1 and len(
        {item["category"] for item in resolved_events if _to_float(item.get("event_raw"), 0.0) > 0.0}
    ) < 2

    prev_display = _to_float((previous_snapshot or {}).get("score_display"), score_raw)
    alpha = float(RISK_INDEX_CONFIG["ema_alpha"])
    score_display = score_raw if not previous_snapshot else max(0.0, min(100.0, alpha * score_raw + (1.0 - alpha) * prev_display))

    category_breakdown = []
    for category_key, category_label in RISK_CATEGORIES.items():
        escalation_value = float(category_raw.get(category_key, 0.0))
        baseline_value = float(category_baseline.get(category_key, 0.0))
        total_value = escalation_value + baseline_value
        category_breakdown.append(
            {
                "category": category_key,
                "label": category_label,
                "raw": total_value,
                "capped": total_value,
                "baseline": baseline_value,
                "escalation": escalation_value,
                "was_capped": False,
            }
        )
    category_breakdown.sort(key=lambda item: item["raw"], reverse=True)

    pair_breakdown = []
    for pair_tag, raw_value in sorted(pair_raw.items(), key=lambda item: item[1], reverse=True):
        pair_breakdown.append(
            {
                "pair_tag": pair_tag,
                "raw": float(raw_value),
                "share_of_total": 0.0 if score_raw <= 0 else min(1.0, float(raw_value) / score_raw),
            }
        )

    top_markets = resolved_events[:10]
    monitored_market_limit = max(8, int(RISK_INDEX_CONFIG.get("monitored_markets_limit", 24)))
    monitored_markets: List[Dict[str, Any]] = []
    monitored_theme_keys: set[str] = set()
    monitored_pair_counts: Dict[str, int] = {}
    for item in sorted(
        resolved_events,
        key=lambda row: (normalize_probability(row.get("probability")), row.get("event_raw", 0.0)),
        reverse=True,
    ):
        theme_key = _scoring_dedupe_key(item)
        if theme_key and theme_key in monitored_theme_keys:
            continue
        pair_tag = _safe_text(item.get("pair_tag")) or "UNKNOWN"
        if monitored_pair_counts.get(pair_tag, 0) >= 1:
            continue
        if theme_key:
            monitored_theme_keys.add(theme_key)
        monitored_pair_counts[pair_tag] = monitored_pair_counts.get(pair_tag, 0) + 1
        monitored_markets.append(item)
        if len(monitored_markets) >= monitored_market_limit:
            break
    should_run_news_explainer = _should_run_news_explainer(top_markets, use_news_explainer=use_news_explainer)
    explanation_results = [
        _make_explanation_result(item, use_external_news=should_run_news_explainer)
        for item in top_markets[:3]
    ]
    explanations = [item[0] for item in explanation_results]
    for cluster in ongoing_clusters[:2]:
        explanations.append(
            {
                "event_key": _safe_text(cluster.get("cluster_key")),
                "one_line_reason": f"{cluster.get('display_title', '-')}" f" 的持续混乱底座贡献约 {float(cluster.get('contribution', 0.0)):.1f} 分，" f"说明市场并不预期它会很快结束。",
                "source_links": [item.get("source_url") for item in cluster.get("reverse_markets", []) if item.get("source_url")][:2],
            }
        )

    matched_cluster_count = len([item for item in ongoing_clusters if _to_float(item.get("contribution"), 0.0) > 0.0])
    if len(resolved_events) >= 10 and matched_cluster_count >= 3:
        confidence = "high"
    elif len(resolved_events) >= 5 or matched_cluster_count >= 2:
        confidence = "medium"
    else:
        confidence = "low"

    if should_run_news_explainer:
        news_explainer_status = "ok" if any(item[1] == "external" for item in explanation_results) else "fallback"
    elif use_news_explainer:
        news_explainer_status = "skipped_low_delta"
    else:
        news_explainer_status = "fallback"

    source_status = {
        "polymarket": "ok" if candidates else "empty",
        "news_explainer": news_explainer_status,
        "score_components": {
            "ongoing_baseline": round(ongoing_baseline_score, 4),
            "escalation_pressure": round(escalation_score, 4),
            "contagion_bonus": round(contagion_bonus, 4),
            "escalation_raw_total": round(escalation_raw_total, 6),
        },
        "ongoing_clusters": ongoing_clusters,
        "monitored_markets": monitored_markets,
        "dynamic_conflict_count": len(dynamic_conflicts),
    }

    return {
        "snapshot_ts": current_ts.strftime("%Y%m%d%H%M%S"),
        "snapshot_date": current_ts.strftime("%Y%m%d"),
        "score_raw": round(score_raw, 4),
        "score_display": round(score_display, 4),
        "band": _band_for_score(score_display),
        "updated_at": current_ts.isoformat(),
        "stale": False,
        "confidence": confidence,
        "low_diversity": bool(low_diversity),
        "raw_total_uncapped": round(escalation_raw_total, 6),
        "raw_total_capped": round(ongoing_baseline_score, 6),
        "max_possible_capped": 100.0,
        "top_markets": top_markets,
        "category_breakdown": category_breakdown,
        "pair_breakdown": pair_breakdown,
        "headline_explanations": explanations,
        "source_status": source_status,
        "methodology_version": str(RISK_INDEX_CONFIG["methodology_version"]),
    }


def _ensure_risk_tables(engine) -> None:
    if engine is None:
        return
    key = id(engine)
    if _TABLES_READY_BY_ENGINE.get(key):
        return

    ddl = f"""
        CREATE TABLE IF NOT EXISTS {GEOPOLITICAL_RISK_TABLE} (
            snapshot_ts VARCHAR(14) NOT NULL,
            snapshot_date VARCHAR(8) NOT NULL,
            score_raw FLOAT NOT NULL DEFAULT 0,
            score_display FLOAT NOT NULL DEFAULT 0,
            band VARCHAR(48) NOT NULL DEFAULT 'nothing_happens',
            updated_at VARCHAR(40) NOT NULL DEFAULT '',
            stale TINYINT(1) NOT NULL DEFAULT 0,
            confidence VARCHAR(16) NOT NULL DEFAULT 'low',
            low_diversity TINYINT(1) NOT NULL DEFAULT 0,
            raw_total_uncapped FLOAT NOT NULL DEFAULT 0,
            raw_total_capped FLOAT NOT NULL DEFAULT 0,
            max_possible_capped FLOAT NOT NULL DEFAULT 0,
            top_markets_json LONGTEXT NULL,
            category_breakdown_json LONGTEXT NULL,
            pair_breakdown_json LONGTEXT NULL,
            headline_explanations_json LONGTEXT NULL,
            source_status_json LONGTEXT NULL,
            methodology_version VARCHAR(32) NOT NULL DEFAULT '',
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (snapshot_ts),
            KEY idx_geopol_risk_date (snapshot_date, snapshot_ts),
            KEY idx_geopol_risk_created (created_at)
        ) DEFAULT CHARSET=utf8mb4;
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
    _TABLES_READY_BY_ENGINE[key] = True


def _row_to_snapshot(row: Dict[str, Any]) -> Dict[str, Any]:
    if not row:
        return {}
    out = dict(row)
    out["stale"] = bool(out.get("stale"))
    out["low_diversity"] = bool(out.get("low_diversity"))
    out["top_markets"] = _json_loads(out.pop("top_markets_json", ""), [])
    out["category_breakdown"] = _json_loads(out.pop("category_breakdown_json", ""), [])
    out["pair_breakdown"] = _json_loads(out.pop("pair_breakdown_json", ""), [])
    out["headline_explanations"] = _json_loads(out.pop("headline_explanations_json", ""), [])
    out["source_status"] = _json_loads(out.pop("source_status_json", ""), {})
    return out


def persist_risk_snapshot(engine, snapshot: Dict[str, Any]) -> Dict[str, Any]:
    if engine is None or not snapshot:
        return snapshot or {}
    _ensure_risk_tables(engine)
    payload = dict(snapshot)
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                INSERT INTO {GEOPOLITICAL_RISK_TABLE} (
                    snapshot_ts, snapshot_date, score_raw, score_display, band, updated_at, stale,
                    confidence, low_diversity, raw_total_uncapped, raw_total_capped, max_possible_capped,
                    top_markets_json, category_breakdown_json, pair_breakdown_json,
                    headline_explanations_json, source_status_json, methodology_version
                ) VALUES (
                    :snapshot_ts, :snapshot_date, :score_raw, :score_display, :band, :updated_at, :stale,
                    :confidence, :low_diversity, :raw_total_uncapped, :raw_total_capped, :max_possible_capped,
                    :top_markets_json, :category_breakdown_json, :pair_breakdown_json,
                    :headline_explanations_json, :source_status_json, :methodology_version
                )
                ON DUPLICATE KEY UPDATE
                    score_raw = VALUES(score_raw),
                    score_display = VALUES(score_display),
                    band = VALUES(band),
                    updated_at = VALUES(updated_at),
                    stale = VALUES(stale),
                    confidence = VALUES(confidence),
                    low_diversity = VALUES(low_diversity),
                    raw_total_uncapped = VALUES(raw_total_uncapped),
                    raw_total_capped = VALUES(raw_total_capped),
                    max_possible_capped = VALUES(max_possible_capped),
                    top_markets_json = VALUES(top_markets_json),
                    category_breakdown_json = VALUES(category_breakdown_json),
                    pair_breakdown_json = VALUES(pair_breakdown_json),
                    headline_explanations_json = VALUES(headline_explanations_json),
                    source_status_json = VALUES(source_status_json),
                    methodology_version = VALUES(methodology_version)
                """
            ),
            {
                "snapshot_ts": _safe_text(payload.get("snapshot_ts")),
                "snapshot_date": _safe_text(payload.get("snapshot_date")),
                "score_raw": _to_float(payload.get("score_raw"), 0.0),
                "score_display": _to_float(payload.get("score_display"), 0.0),
                "band": _safe_text(payload.get("band")),
                "updated_at": _safe_text(payload.get("updated_at")),
                "stale": 1 if payload.get("stale") else 0,
                "confidence": _safe_text(payload.get("confidence") or "low"),
                "low_diversity": 1 if payload.get("low_diversity") else 0,
                "raw_total_uncapped": _to_float(payload.get("raw_total_uncapped"), 0.0),
                "raw_total_capped": _to_float(payload.get("raw_total_capped"), 0.0),
                "max_possible_capped": _to_float(payload.get("max_possible_capped"), 0.0),
                "top_markets_json": _json_dumps(payload.get("top_markets") or []),
                "category_breakdown_json": _json_dumps(payload.get("category_breakdown") or []),
                "pair_breakdown_json": _json_dumps(payload.get("pair_breakdown") or []),
                "headline_explanations_json": _json_dumps(payload.get("headline_explanations") or []),
                "source_status_json": _json_dumps(payload.get("source_status") or {}),
                "methodology_version": _safe_text(payload.get("methodology_version")),
            },
        )
    return payload


def get_latest_geopolitical_risk_snapshot(engine) -> Dict[str, Any]:
    if engine is None:
        return {}
    _ensure_risk_tables(engine)
    sql = text(
        f"""
        SELECT *
        FROM {GEOPOLITICAL_RISK_TABLE}
        ORDER BY snapshot_ts DESC
        LIMIT 1
        """
    )
    with engine.connect() as conn:
        row = conn.execute(sql).mappings().first()
    return _row_to_snapshot(dict(row)) if row else {}


def get_geopolitical_risk_history(engine, days: int = 7) -> pd.DataFrame:
    if engine is None:
        return pd.DataFrame()
    _ensure_risk_tables(engine)
    days = max(1, min(int(days), 90))
    cutoff = _now_beijing() - timedelta(days=days)
    sql = text(
        f"""
        SELECT snapshot_ts, snapshot_date, score_raw, score_display, band, updated_at, stale, methodology_version
        FROM {GEOPOLITICAL_RISK_TABLE}
        WHERE snapshot_date >= :cutoff_date
        ORDER BY snapshot_ts ASC
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"cutoff_date": cutoff.strftime("%Y%m%d")})
    return df if df is not None else pd.DataFrame()


def get_recent_geopolitical_risk_snapshots(engine, limit: int = 8) -> List[Dict[str, Any]]:
    if engine is None:
        return []
    _ensure_risk_tables(engine)
    limit = max(2, min(int(limit), 48))
    sql = text(
        f"""
        SELECT *
        FROM {GEOPOLITICAL_RISK_TABLE}
        ORDER BY snapshot_ts DESC
        LIMIT :limit_rows
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, {"limit_rows": limit}).mappings().all()
    snapshots = [_row_to_snapshot(dict(row)) for row in rows]
    snapshots.reverse()
    return snapshots


def refresh_geopolitical_risk_snapshot(
    engine,
    now: Optional[datetime] = None,
    persist: bool = True,
    use_news_explainer: bool = True,
) -> Dict[str, Any]:
    previous = get_latest_geopolitical_risk_snapshot(engine) if engine is not None else {}
    try:
        candidates = fetch_polymarket_candidates(
            limit=int(RISK_INDEX_CONFIG.get("polymarket_candidate_limit", 600)),
            timeout=12,
        )
        snapshot = build_risk_snapshot(
            candidates=candidates,
            previous_snapshot=previous,
            now=now,
            use_news_explainer=use_news_explainer,
        )
        if persist and engine is not None:
            persist_risk_snapshot(engine, snapshot)
        return snapshot
    except Exception as exc:
        if previous:
            fallback = dict(previous)
            fallback["stale"] = True
            source_status = dict(fallback.get("source_status") or {})
            source_status["polymarket"] = f"error:{type(exc).__name__}"
            fallback["source_status"] = source_status
            return fallback
        current_ts = _now_beijing(now)
        return {
            "snapshot_ts": current_ts.strftime("%Y%m%d%H%M%S"),
            "snapshot_date": current_ts.strftime("%Y%m%d"),
            "score_raw": 0.0,
            "score_display": 0.0,
            "band": "nothing_happens",
            "updated_at": current_ts.isoformat(),
            "stale": True,
            "confidence": "low",
            "low_diversity": False,
            "raw_total_uncapped": 0.0,
            "raw_total_capped": 0.0,
            "max_possible_capped": 0.0,
            "top_markets": [],
            "category_breakdown": [],
            "pair_breakdown": [],
            "headline_explanations": [],
            "source_status": {"polymarket": f"error:{type(exc).__name__}", "news_explainer": "fallback"},
            "methodology_version": str(RISK_INDEX_CONFIG["methodology_version"]),
        }
