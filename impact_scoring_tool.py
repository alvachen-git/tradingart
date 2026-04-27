from __future__ import annotations

from typing import Any, Dict, List


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _direction_to_score(direction_hint: str) -> int:
    d = _safe_text(direction_hint).lower()
    if d == "risk_on":
        return 1
    if d == "risk_off":
        return -1
    return 0


def _time_weight(time_window: str) -> Dict[str, float]:
    w = _safe_text(time_window).lower()
    if w == "intraday":
        return {"short": 0.9, "mid": 0.5, "long": 0.25}
    if w == "weekly":
        return {"short": 0.75, "mid": 0.6, "long": 0.35}
    if w == "monthly":
        return {"short": 0.4, "mid": 0.75, "long": 0.6}
    return {"short": 0.6, "mid": 0.6, "long": 0.45}


def _strength_from_event(event: Dict[str, Any]) -> int:
    confidence = float(event.get("confidence") or 0.5)
    content = _safe_text(event.get("raw_content")).lower()
    base = int(45 + confidence * 35)
    if any(x in content for x in ["breaking", "urgent", "war", "sanction"]):
        base += 10
    if any(x in content for x in ["rumor", "unavailable", "error"]):
        base -= 12
    return max(20, min(95, base))


def _build_conflict_ranking(matrix: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ranked: List[Dict[str, Any]] = []
    if len(matrix) < 2:
        return ranked

    conflict_id = 1
    for i in range(len(matrix)):
        a = matrix[i]
        a_dir = int(a.get("direction_score") or 0)
        if a_dir == 0:
            continue
        for j in range(i + 1, len(matrix)):
            b = matrix[j]
            b_dir = int(b.get("direction_score") or 0)
            if b_dir == 0:
                continue
            if a_dir == b_dir:
                continue

            a_strength = float(a.get("strength") or 0.0) * float(a.get("confidence") or 0.0)
            b_strength = float(b.get("strength") or 0.0) * float(b.get("confidence") or 0.0)
            spread = abs(a_strength - b_strength)
            offset_ratio = 0.0
            if (a_strength + b_strength) > 0:
                offset_ratio = 1.0 - spread / (a_strength + b_strength)
            severity = round(min(1.0, max(0.05, offset_ratio)) * 100, 1)

            if severity >= 70:
                handling = "冲突强，短期结论不稳定，建议优先等待新增证据确认。"
            elif severity >= 40:
                handling = "冲突中等，维持中性判断，关注下一条主导事件。"
            else:
                handling = "冲突较弱，以高置信事件为主线，保留反向风险提示。"

            dominant = a if a_strength >= b_strength else b
            ranked.append(
                {
                    "conflict_id": f"冲突_{conflict_id}",
                    "event_a": a.get("event_id"),
                    "event_b": b.get("event_id"),
                    "title_a": a.get("title"),
                    "title_b": b.get("title"),
                    "severity": severity,
                    "offset_ratio": round(offset_ratio, 3),
                    "dominant_event": dominant.get("event_id"),
                    "dominant_direction": dominant.get("direction"),
                    "handling_strategy": handling,
                }
            )
            conflict_id += 1

    ranked.sort(key=lambda x: (-float(x.get("severity") or 0), _safe_text(x.get("conflict_id"))))
    return ranked


def _build_priority_board(matrix: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    board: List[Dict[str, Any]] = []
    for item in matrix[:8]:
        board.append(
            {
                "event_id": item.get("event_id"),
                "priority": item.get("event_priority"),
                "title": item.get("title"),
                "direction": item.get("direction"),
                "strength": item.get("strength"),
                "confidence": item.get("confidence"),
                "short_term": item.get("short_term"),
                "immediacy": item.get("immediacy"),
            }
        )
    return board


def score_event_impacts(extracted_payload: Dict[str, Any], analysis_horizon: str = "swing") -> Dict[str, Any]:
    events = extracted_payload.get("events") if isinstance(extracted_payload, dict) else []
    matrix: List[Dict[str, Any]] = []
    bull_strength = 0.0
    bear_strength = 0.0

    for event in events if isinstance(events, list) else []:
        if not isinstance(event, dict):
            continue

        direction = _direction_to_score(_safe_text(event.get("direction_hint")))
        strength = _strength_from_event(event)
        confidence = float(event.get("confidence") or 0.5)
        weights = _time_weight(_safe_text(event.get("time_window")) or analysis_horizon)

        short_term = round(direction * strength * weights["short"], 2)
        mid_term = round(direction * strength * weights["mid"], 2)
        long_term = round(direction * strength * weights["long"], 2)
        immediacy = "高" if weights["short"] >= 0.75 else "中" if weights["short"] >= 0.5 else "低"

        if direction > 0:
            bull_strength += strength * confidence
        elif direction < 0:
            bear_strength += strength * confidence

        matrix.append(
            {
                "event_id": _safe_text(event.get("event_id")),
                "title": _safe_text(event.get("title")),
                "region": _safe_text(event.get("region")),
                "policy_type": _safe_text(event.get("policy_type")),
                "impacted_assets": event.get("affected_assets") if isinstance(event.get("affected_assets"), list) else [],
                "direction": "偏多" if direction > 0 else "偏空" if direction < 0 else "中性",
                "direction_score": direction,
                "strength": strength,
                "immediacy": immediacy,
                "confidence": round(confidence, 3),
                "short_term": short_term,
                "mid_term": mid_term,
                "long_term": long_term,
                "reasoning": f"{event.get('policy_type', '市场')}事件，短期影响强度{immediacy}。",
            }
        )

    matrix.sort(key=lambda x: (-abs(float(x.get("short_term") or 0.0)), -float(x.get("confidence") or 0.0)))
    for idx, item in enumerate(matrix):
        item["event_priority"] = idx + 1

    net_score = round(bull_strength - bear_strength, 2)
    if net_score > 8:
        market_bias = "偏多"
    elif net_score < -8:
        market_bias = "偏空"
    else:
        market_bias = "中性"

    conflicts: List[Dict[str, Any]] = []
    has_bull = any((i.get("direction_score") or 0) > 0 for i in matrix)
    has_bear = any((i.get("direction_score") or 0) < 0 for i in matrix)
    conflict_ranking = _build_conflict_ranking(matrix)
    if has_bull and has_bear:
        conflicts.append(
            {
                "type": "方向冲突",
                "description": "偏多与偏空事件并存，结论存在冲突。",
                "conflict_pairs": len(conflict_ranking),
            }
        )

    priority_board = _build_priority_board(matrix)

    return {
        "impact_matrix": matrix,
        "aggregate_bias": {
            "market_bias": market_bias,
            "bull_strength": round(bull_strength, 2),
            "bear_strength": round(bear_strength, 2),
            "net_score": net_score,
        },
        "conflict_analysis": {
            "has_conflict": len(conflicts) > 0,
            "conflicts": conflicts,
            "dominant_events": [i.get("event_id") for i in matrix[:3]],
        },
        "conflict_ranking": conflict_ranking,
        "event_priority_board": priority_board,
    }
