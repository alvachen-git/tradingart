from __future__ import annotations

import json
import os
import statistics
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import text

from kline_algo import calculate_kline_signals

ANALYSIS_VERSION = "v1"

OPEN_LONG_ACTIONS = {"open_long", "add_long", "buy"}
OPEN_SHORT_ACTIONS = {"open_short", "add_short", "sell_short"}
ADD_ACTIONS = {"add_long", "add_short"}
CLOSE_LONG_ACTIONS = {"close_long", "close_long_partial", "close_long_all", "sell_long", "close"}
CLOSE_SHORT_ACTIONS = {"close_short", "close_short_partial", "close_short_all", "buy_to_cover"}

STRONG_LONG_THRESHOLD = 70.0
STRONG_SHORT_THRESHOLD = 30.0
REVERSAL_LOOKAHEAD_TRADES = 2
PATTERN_DIRECTION_BONUS = 12.0
ENGULF_DIRECTION_BONUS = 10.0
TRADE_BREAKOUT_EPS = 0.0


def _to_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _json_load(v: Any, default: Any) -> Any:
    if v is None:
        return default
    if isinstance(v, (dict, list)):
        return v
    try:
        return json.loads(str(v))
    except Exception:
        return default


def _json_dump(v: Any) -> str:
    try:
        return json.dumps(v, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return "{}"


def _normalize_date(v: Any) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    s = str(v).strip()
    if not s:
        return None
    s = s[:10]
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        pass
    try:
        return datetime.strptime(s, "%Y%m%d").date()
    except Exception:
        return None


def _clip(v: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, v))


def _table_by_symbol_type(symbol_type: str) -> str:
    t = str(symbol_type or "").lower()
    if t == "future":
        return "futures_price"
    if t == "index":
        return "index_price"
    return "stock_price"


def ensure_review_tables(db_engine) -> None:
    with db_engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS kline_game_trade_evaluations (
                    game_id BIGINT NOT NULL,
                    user_id VARCHAR(128) NOT NULL,
                    trade_seq INT NOT NULL,
                    action VARCHAR(32) NOT NULL,
                    bar_date DATE NULL,
                    symbol VARCHAR(32) NULL,
                    symbol_type VARCHAR(16) NULL,
                    rule_score DECIMAL(10,2) NOT NULL DEFAULT 50,
                    market_bias VARCHAR(16) NOT NULL DEFAULT 'neutral',
                    alignment VARCHAR(32) NOT NULL DEFAULT 'observe',
                    confidence DECIMAL(10,4) NOT NULL DEFAULT 0,
                    direction_points DECIMAL(10,2) NOT NULL DEFAULT 0,
                    direction_reasons_json TEXT NULL,
                    evidence_patterns_json TEXT NULL,
                    evidence_trends_json TEXT NULL,
                    violation_tags_json TEXT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (game_id, trade_seq)
                )
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS kline_game_analysis_reports (
                    game_id BIGINT NOT NULL,
                    user_id VARCHAR(128) NOT NULL,
                    analysis_version VARCHAR(16) NOT NULL,
                    overall_score DECIMAL(10,2) NOT NULL DEFAULT 0,
                    direction_score DECIMAL(10,2) NOT NULL DEFAULT 0,
                    risk_score DECIMAL(10,2) NOT NULL DEFAULT 0,
                    execution_score DECIMAL(10,2) NOT NULL DEFAULT 0,
                    metrics_json TEXT NULL,
                    mistakes_json TEXT NULL,
                    strengths_json TEXT NULL,
                    ai_report_json TEXT NULL,
                    ai_status VARCHAR(32) NOT NULL DEFAULT 'rule_only',
                    ai_model VARCHAR(64) NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (game_id, analysis_version)
                )
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS kline_user_trade_analysis_reports (
                    user_id VARCHAR(128) NOT NULL,
                    scope_type VARCHAR(32) NOT NULL,
                    scope_value INT NOT NULL,
                    analysis_version VARCHAR(16) NOT NULL,
                    metrics_json TEXT NULL,
                    radar_json TEXT NULL,
                    habit_summary_json TEXT NULL,
                    ai_report_json TEXT NULL,
                    ai_status VARCHAR(32) NOT NULL DEFAULT 'rule_only',
                    ai_model VARCHAR(64) NULL,
                    source_trade_count INT NOT NULL DEFAULT 0,
                    source_game_count INT NOT NULL DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, scope_type, scope_value, analysis_version)
                )
                """
            )
        )

        # Backward-compatible columns for existing deployments.
        try:
            conn.execute(
                text(
                    """
                    ALTER TABLE kline_game_trade_evaluations
                    ADD COLUMN direction_points DECIMAL(10,2) NOT NULL DEFAULT 0
                    """
                )
            )
        except Exception:
            pass
        try:
            conn.execute(
                text(
                    """
                    ALTER TABLE kline_game_trade_evaluations
                    ADD COLUMN direction_reasons_json TEXT NULL
                    """
                )
            )
        except Exception:
            pass


def _load_game_row(conn, game_id: int, user_id: str) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        text(
            """
            SELECT *
            FROM kline_game_records
            WHERE id = :gid
              AND user_id = :uid
            LIMIT 1
            """
        ),
        {"gid": int(game_id), "uid": str(user_id)},
    ).mappings().fetchone()
    return dict(row) if row else None


def _load_game_trades(conn, game_id: int, user_id: str) -> List[Dict[str, Any]]:
    rows = conn.execute(
        text(
            """
            SELECT *
            FROM kline_game_trades
            WHERE game_id = :gid
              AND user_id = :uid
            ORDER BY
                CASE WHEN trade_seq IS NULL OR trade_seq = 0 THEN 2147483647 ELSE trade_seq END,
                id ASC
            """
        ),
        {"gid": int(game_id), "uid": str(user_id)},
    ).mappings().fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        one = dict(r)
        one["position_before"] = _json_load(one.get("position_before"), {})
        one["position_after"] = _json_load(one.get("position_after"), {})
        one["action"] = str(one.get("action") or "").strip().lower()
        out.append(one)
    return out


def _fetch_kline_window(
    conn,
    symbol: str,
    symbol_type: str,
    bar_date: Optional[date],
    window: int = 90,
) -> pd.DataFrame:
    if not symbol or not bar_date:
        return pd.DataFrame()

    table_name = _table_by_symbol_type(symbol_type)
    d_norm = bar_date.strftime("%Y%m%d")
    sql = text(
        f"""
        SELECT trade_date, open_price, high_price, low_price, close_price
        FROM {table_name}
        WHERE ts_code = :code
          AND REPLACE(CAST(trade_date AS CHAR), '-', '') <= :d_norm
          AND open_price IS NOT NULL
          AND close_price IS NOT NULL
        ORDER BY REPLACE(CAST(trade_date AS CHAR), '-', '') DESC
        LIMIT :lim
        """
    )
    try:
        df = pd.read_sql(sql, conn, params={"code": str(symbol), "d_norm": d_norm, "lim": int(window)})
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return df
    try:
        df["trade_date"] = df["trade_date"].apply(_normalize_date)
        df = df[df["trade_date"].notna()].copy()
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        return df.sort_values("trade_date").reset_index(drop=True)
    except Exception:
        return pd.DataFrame()


def _score_to_bias(score: float) -> str:
    if score >= 60:
        return "long_bias"
    if score <= 40:
        return "short_bias"
    return "neutral"


def _score_to_strong_bias(score: float) -> str:
    if score >= STRONG_LONG_THRESHOLD:
        return "long"
    if score <= STRONG_SHORT_THRESHOLD:
        return "short"
    return ""


def _action_direction(action: str) -> str:
    a = str(action or "").strip().lower()
    if a in OPEN_LONG_ACTIONS:
        return "long"
    if a in OPEN_SHORT_ACTIONS:
        return "short"
    return ""


def _pattern_flags(patterns: List[str], trends: List[str], bias: str) -> Dict[str, bool]:
    pat = [str(x or "") for x in (patterns or [])]
    trd = [str(x or "") for x in (trends or [])]

    up_breakout = any(
        ("突破" in p and "跌破" not in p and "假突破" not in p)
        or ("创新高" in p)
        for p in pat
    )
    down_breakout = any(("跌破" in p and "假跌破" not in p) for p in pat)

    has_bullish_engulf = any("多头吞噬" in p for p in pat)
    has_bearish_engulf = any("空头吞噬" in p for p in pat)

    bull_trend = (
        str(bias) == "long_bias"
        or any(("均线多头排列" in t) or ("中多" in t) for t in trd)
    )
    bear_trend = (
        str(bias) == "short_bias"
        or any(("均线空头排列" in t) or ("中空" in t) for t in trd)
    )
    return {
        "up_breakout": bool(up_breakout),
        "down_breakout": bool(down_breakout),
        "bullish_engulf": bool(has_bullish_engulf),
        "bearish_engulf": bool(has_bearish_engulf),
        "bull_trend": bool(bull_trend),
        "bear_trend": bool(bear_trend),
    }


def _trade_price_breakout_flags(trade: Dict[str, Any], window_df: pd.DataFrame) -> Dict[str, Any]:
    if window_df is None or window_df.empty:
        return {"up_breakout": False, "down_breakout": False, "labels": []}
    need_cols = {"trade_date", "high_price", "low_price"}
    if not need_cols.issubset(set(window_df.columns)):
        return {"up_breakout": False, "down_breakout": False, "labels": []}

    trade_price = _to_float(trade.get("price"), 0.0)
    if trade_price <= 0:
        return {"up_breakout": False, "down_breakout": False, "labels": []}

    try:
        df = window_df.copy()
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df = df.sort_values("trade_date").reset_index(drop=True)
    except Exception:
        return {"up_breakout": False, "down_breakout": False, "labels": []}

    if len(df) < 12:
        return {"up_breakout": False, "down_breakout": False, "labels": []}
    prev = df.iloc[:-1].copy()
    if prev.empty:
        return {"up_breakout": False, "down_breakout": False, "labels": []}

    up_hit_n: Optional[int] = None
    down_hit_n: Optional[int] = None
    for n in (5, 10):
        if len(prev) < n:
            continue
        seg = prev.iloc[-n:]
        high_n = _to_float(seg["high_price"].max(), 0.0)
        low_n = _to_float(seg["low_price"].min(), 0.0)
        if high_n > 0 and trade_price >= high_n * (1.0 + TRADE_BREAKOUT_EPS):
            up_hit_n = n if up_hit_n is None else min(up_hit_n, n)
        if low_n > 0 and trade_price <= low_n * (1.0 - TRADE_BREAKOUT_EPS):
            down_hit_n = n if down_hit_n is None else min(down_hit_n, n)

    labels: List[str] = []
    if up_hit_n is not None:
        labels.append(f"交易价上破{up_hit_n}日区间")
    if down_hit_n is not None:
        labels.append(f"交易价下破{down_hit_n}日区间")
    return {
        "up_breakout": up_hit_n is not None,
        "down_breakout": down_hit_n is not None,
        "labels": labels,
    }


def _pattern_direction_adjustment(
    action: str,
    pattern_flags: Dict[str, bool],
) -> Tuple[float, List[str], str]:
    a = str(action or "").strip().lower()
    points = 0.0
    reasons: List[str] = []
    action_hint = "neutral"
    official_up_breakout = bool(pattern_flags.get("up_breakout"))
    official_down_breakout = bool(pattern_flags.get("down_breakout"))
    trade_up_breakout = bool(pattern_flags.get("trade_up_breakout"))
    trade_down_breakout = bool(pattern_flags.get("trade_down_breakout"))

    if official_up_breakout or trade_up_breakout:
        if a in OPEN_LONG_ACTIONS:
            points += PATTERN_DIRECTION_BONUS
            reasons.append("up_breakout_long_plus")
            action_hint = "aligned"
        elif a in OPEN_SHORT_ACTIONS and official_up_breakout:
            points -= PATTERN_DIRECTION_BONUS
            reasons.append("up_breakout_short_minus")
            action_hint = "counter"

    if official_down_breakout or trade_down_breakout:
        if a in OPEN_SHORT_ACTIONS:
            points += PATTERN_DIRECTION_BONUS
            reasons.append("down_breakout_short_plus")
            action_hint = "aligned"
        elif a in OPEN_LONG_ACTIONS and official_down_breakout:
            points -= PATTERN_DIRECTION_BONUS
            reasons.append("down_breakout_long_minus")
            action_hint = "counter"

    if pattern_flags.get("bearish_engulf") and pattern_flags.get("bull_trend"):
        if a in OPEN_LONG_ACTIONS:
            points -= ENGULF_DIRECTION_BONUS
            reasons.append("bull_trend_bearish_engulf_long_minus")
            action_hint = "counter"
        elif a in OPEN_SHORT_ACTIONS or a in CLOSE_LONG_ACTIONS:
            points += ENGULF_DIRECTION_BONUS
            reasons.append("bull_trend_bearish_engulf_defense_plus")
            if a in OPEN_SHORT_ACTIONS:
                action_hint = "aligned"

    if pattern_flags.get("bullish_engulf") and pattern_flags.get("bear_trend"):
        if a in OPEN_SHORT_ACTIONS:
            points -= ENGULF_DIRECTION_BONUS
            reasons.append("bear_trend_bullish_engulf_short_minus")
            action_hint = "counter"
        if a in OPEN_LONG_ACTIONS:
            points -= ENGULF_DIRECTION_BONUS
            reasons.append("bear_trend_bullish_engulf_cover_or_long_minus")
            if a in OPEN_LONG_ACTIONS:
                action_hint = "counter"
        elif a in CLOSE_SHORT_ACTIONS:
            points += ENGULF_DIRECTION_BONUS
            reasons.append("bear_trend_bullish_engulf_cover_plus")

    return points, reasons, action_hint


def _has_reversal_evidence(before_dir: str, patterns: List[str], pattern_flags: Dict[str, bool]) -> bool:
    bdir = str(before_dir or "").strip().lower()
    pats = [str(x or "") for x in (patterns or [])]

    if bdir == "short":
        if (
            pattern_flags.get("bullish_engulf")
            or pattern_flags.get("up_breakout")
            or pattern_flags.get("trade_up_breakout")
        ):
            return True
        for p in pats:
            if ("波动转折(多头)" in p) or ("小区间突破" in p) or ("创新高" in p):
                return True
        return False

    if bdir == "long":
        if (
            pattern_flags.get("bearish_engulf")
            or pattern_flags.get("down_breakout")
            or pattern_flags.get("trade_down_breakout")
        ):
            return True
        for p in pats:
            if ("波动转折(空头)" in p) or ("平台跌破" in p):
                return True
        return False

    return False


def _is_reversal_response(target_dir: str, ev: Dict[str, Any]) -> bool:
    action = str(ev.get("action") or "").strip().lower()
    before_dir = str(ev.get("before_dir") or "").strip().lower()
    t = str(target_dir or "").strip().lower()
    if t == "long":
        if action in OPEN_LONG_ACTIONS:
            return True
        if action in CLOSE_SHORT_ACTIONS and before_dir == "short":
            return True
        return False
    if t == "short":
        if action in OPEN_SHORT_ACTIONS:
            return True
        if action in CLOSE_LONG_ACTIONS and before_dir == "long":
            return True
        return False
    return False


def _compute_direction_score(evals: List[Dict[str, Any]]) -> Tuple[float, Dict[str, Any]]:
    if not evals:
        return 50.0, {
            "strong_follow_count": 0,
            "strong_counter_count": 0,
            "pattern_plus_count": 0,
            "pattern_minus_count": 0,
            "reversal_events": 0,
            "reversal_responded": 0,
            "reversal_missed": 0,
            "direction_points": 0.0,
        }

    points = 0.0
    strong_follow_count = 0
    strong_counter_count = 0
    pattern_plus_count = 0
    pattern_minus_count = 0
    reversal_events = 0
    reversal_responded = 0
    reversal_missed = 0
    prev_strong_bias = ""

    for idx, e in enumerate(evals):
        action = str(e.get("action") or "").strip().lower()
        action_dir = _action_direction(action)
        score = _to_float(e.get("rule_score"), 50.0)
        strong_bias = _score_to_strong_bias(score)
        normal_bias = str(e.get("market_bias") or "neutral")
        target_dir = strong_bias
        if not target_dir:
            if normal_bias == "long_bias":
                target_dir = "long"
            elif normal_bias == "short_bias":
                target_dir = "short"

        if action_dir and target_dir:
            if action_dir == target_dir:
                if strong_bias:
                    points += 8.0
                    strong_follow_count += 1
                else:
                    points += 3.0
            else:
                if strong_bias:
                    points -= 8.0
                    strong_counter_count += 1
                else:
                    points -= 3.0

        pattern_points = _to_float(e.get("direction_points"), 0.0)
        if pattern_points != 0.0:
            points += pattern_points
            if pattern_points > 0:
                pattern_plus_count += 1
            else:
                pattern_minus_count += 1

        if strong_bias and prev_strong_bias and strong_bias != prev_strong_bias:
            reversal_events += 1
            responded = _is_reversal_response(strong_bias, e)
            if not responded:
                for step in range(1, REVERSAL_LOOKAHEAD_TRADES + 1):
                    next_idx = idx + step
                    if next_idx >= len(evals):
                        break
                    if _is_reversal_response(strong_bias, evals[next_idx]):
                        responded = True
                        break
            if responded:
                points += 10.0
                reversal_responded += 1
            else:
                points -= 10.0
                reversal_missed += 1

        if strong_bias:
            prev_strong_bias = strong_bias

    return _clip(50.0 + points), {
        "strong_follow_count": int(strong_follow_count),
        "strong_counter_count": int(strong_counter_count),
        "pattern_plus_count": int(pattern_plus_count),
        "pattern_minus_count": int(pattern_minus_count),
        "reversal_events": int(reversal_events),
        "reversal_responded": int(reversal_responded),
        "reversal_missed": int(reversal_missed),
        "direction_points": round(points, 2),
    }


def _build_in_clause(values: List[Any], prefix: str) -> Tuple[str, Dict[str, Any]]:
    placeholders: List[str] = []
    params: Dict[str, Any] = {}
    for idx, val in enumerate(values):
        key = f"{prefix}{idx}"
        placeholders.append(f":{key}")
        params[key] = val
    return (", ".join(placeholders) if placeholders else "NULL"), params


def _build_rule_only_review(
    metrics: Dict[str, Any],
    mistakes: List[Dict[str, Any]],
    strengths: List[Dict[str, Any]],
    evals: List[Dict[str, Any]],
) -> Dict[str, Any]:
    check_items = [
        "开仓前先确认当前方向偏向（多/空/中性）",
        "若出现逆势信号，优先减仓或等待确认，不盲目加仓",
        "交易次数超过20次时，强制暂停并复核纪律",
    ]
    examples = []
    for e in evals[:3]:
        examples.append(
            {
                "trade_seq": int(e.get("trade_seq") or 0),
                "action": str(e.get("action") or ""),
                "bar_date": str(e.get("bar_date") or ""),
                "market_bias": str(e.get("market_bias") or "neutral"),
                "judgement": str(e.get("alignment") or "observe"),
            }
        )

    overall = "本局整体符合度一般，建议先强化方向一致性与仓位纪律。"
    if _to_float(metrics.get("overall_score"), 0) >= 75:
        overall = "本局整体符合交易体系，建议延续当前纪律。"
    elif _to_float(metrics.get("overall_score"), 0) <= 40:
        overall = "本局偏离交易体系较多，优先修正逆势和加亏仓行为。"

    return {
        "overall_judgement": overall,
        "what_was_right": [s.get("title") for s in strengths if s.get("title")],
        "mistakes_to_fix": [
            {
                "tag": m.get("tag"),
                "problem": m.get("title"),
                "fix": m.get("suggestion"),
                "count": int(m.get("count") or 0),
            }
            for m in mistakes
        ],
        "next_game_checklist": check_items,
        "key_examples": examples,
    }


def _build_user_global_rule_only_report(aggregate: Dict[str, Any], radar: Dict[str, Any]) -> Dict[str, Any]:
    habits = list((aggregate.get("habit_summary") or {}).get("top_habits") or [])
    dims = list(radar.get("dimensions") or [])
    values = list(radar.get("values") or [])
    dim_diag = []
    for i in range(min(len(dims), len(values))):
        dim_diag.append(
            {
                "dimension": str(dims[i]),
                "score": round(_to_float(values[i], 0.0), 2),
                "note": "高于80保持优势，低于60优先修正。",
            }
        )
    return {
        "profile_summary": "已完成最近交易行为总分析，建议先修正高频错误，再提升稳定性。",
        "core_habits": [
            {"tag": str(h.get("tag") or ""), "count": int(h.get("count") or 0)}
            for h in habits[:5]
        ],
        "dimension_diagnosis": dim_diag,
        "improvement_plan_7d": [
            "每次开仓前先确认方向偏向，偏向冲突时等待确认。",
            "若出现转折信号，2笔内必须执行应对动作（减仓/反手/平反向仓）。",
            "单日连续出现2次逆势入场后，当日停止新开仓。",
        ],
        "improvement_plan_30d": [
            "建立每周复盘清单，记录逆势开仓与亏损加仓的触发情境。",
            "每周统计五维得分变化，连续2周低于60的维度设为主修项。",
        ],
        "watchlist_risks": [
            "亏损中继续加仓",
            "转折出现后反应迟缓",
            "过度交易导致执行纪律下滑",
        ],
        "representative_cases": list(aggregate.get("samples") or [])[:8],
    }


def _try_generate_ai_review(
    metrics: Dict[str, Any],
    mistakes: List[Dict[str, Any]],
    strengths: List[Dict[str, Any]],
    evals: List[Dict[str, Any]],
) -> Tuple[Dict[str, Any], str, str]:
    if os.getenv("KLINE_REVIEW_AI_ENABLE", "0") != "1":
        return _build_rule_only_review(metrics, mistakes, strengths, evals), "rule_only", ""

    try:
        from llm_compat import ChatTongyiCompat

        model = os.getenv("KLINE_REVIEW_AI_MODEL", "qwen3.5-plus")
        llm = ChatTongyiCompat(model=model, temperature=0.2)

        payload = {
            "metrics": metrics,
            "mistakes": mistakes,
            "strengths": strengths,
            "sample_evaluations": evals[:12],
        }
        prompt = (
            "你是交易复盘教练。请只返回JSON，字段必须包含: "
            "overall_judgement, what_was_right, mistakes_to_fix, next_game_checklist, key_examples。"
            "不要输出额外文字。\n"
            f"输入数据:\n{json.dumps(payload, ensure_ascii=False)}"
        )
        resp = llm.invoke(prompt)
        content = getattr(resp, "content", resp)
        if isinstance(content, list):
            content = "".join(str(x) for x in content)
        text_content = str(content or "").strip()
        parsed = json.loads(text_content)
        if not isinstance(parsed, dict):
            raise ValueError("ai review is not dict")
        return parsed, "ai", model
    except Exception:
        return _build_rule_only_review(metrics, mistakes, strengths, evals), "rule_only", ""


def _try_generate_user_global_ai_report(
    aggregate: Dict[str, Any],
    radar: Dict[str, Any],
) -> Tuple[Dict[str, Any], str, str]:
    fallback = _build_user_global_rule_only_report(aggregate, radar)
    if os.getenv("KLINE_REVIEW_AI_ENABLE", "0") != "1":
        return fallback, "rule_only", ""

    try:
        from llm_compat import ChatTongyiCompat

        model = os.getenv("KLINE_REVIEW_AI_MODEL", "qwen3.5-plus")
        llm = ChatTongyiCompat(model=model, temperature=0.2)
        payload = {
            "aggregate": aggregate,
            "radar": radar,
        }
        prompt = (
            "你是交易复盘教练。请只返回JSON，字段必须包含: "
            "profile_summary, core_habits, dimension_diagnosis, improvement_plan_7d, "
            "improvement_plan_30d, watchlist_risks, representative_cases。"
            "不要输出额外文字。\n"
            f"输入数据:\n{json.dumps(payload, ensure_ascii=False)}"
        )
        resp = llm.invoke(prompt)
        content = getattr(resp, "content", resp)
        if isinstance(content, list):
            content = "".join(str(x) for x in content)
        parsed = json.loads(str(content or "").strip())
        if not isinstance(parsed, dict):
            raise ValueError("user global ai review is not dict")
        return parsed, "ai", model
    except Exception:
        return fallback, "rule_only", ""


def _evaluate_one_trade(trade: Dict[str, Any], window_df: pd.DataFrame) -> Dict[str, Any]:
    action = str(trade.get("action") or "").strip().lower()
    score = 50.0
    patterns: List[str] = []
    trends: List[str] = []
    if not window_df.empty and len(window_df) >= 30:
        try:
            signals = calculate_kline_signals(window_df.copy())
            raw_score = _to_float(signals.get("score"), 50.0)
            score = _clip(raw_score, 0.0, 100.0)
            patterns = list(signals.get("patterns") or [])
            trends = list(signals.get("trends") or [])
        except Exception:
            score = 50.0
    score = round(_clip(score, 0.0, 100.0), 2)
    bias = _score_to_bias(score)
    pattern_flags = _pattern_flags(patterns, trends, bias)
    price_breakout = _trade_price_breakout_flags(trade, window_df)
    pattern_flags["trade_up_breakout"] = bool(price_breakout.get("up_breakout"))
    pattern_flags["trade_down_breakout"] = bool(price_breakout.get("down_breakout"))
    if pattern_flags.get("trade_up_breakout"):
        s = "交易价上破5日区间"
        if action in OPEN_LONG_ACTIONS.union(CLOSE_SHORT_ACTIONS) and s not in patterns:
            patterns.append(s)
    if pattern_flags.get("trade_down_breakout"):
        s = "交易价下破5日区间"
        if action in OPEN_SHORT_ACTIONS.union(CLOSE_LONG_ACTIONS) and s not in patterns:
            patterns.append(s)

    pos_before = _json_load(trade.get("position_before"), {})
    pos_after = _json_load(trade.get("position_after"), {})
    before_dir = str(pos_before.get("direction") or "").lower()
    after_dir = str(pos_after.get("direction") or "").lower()

    alignment = "observe"
    tags: List[str] = []
    reversal_evidence = _has_reversal_evidence(before_dir, patterns, pattern_flags)

    if action in OPEN_LONG_ACTIONS:
        if bias == "long_bias":
            alignment = "aligned"
        elif bias == "short_bias":
            alignment = "counter"
            tags.append("counter_trend_entry")
        else:
            alignment = "observe"
    elif action in OPEN_SHORT_ACTIONS:
        if bias == "short_bias":
            alignment = "aligned"
        elif bias == "long_bias":
            alignment = "counter"
            tags.append("counter_trend_entry")
        else:
            alignment = "observe"
    elif action in CLOSE_LONG_ACTIONS or action in CLOSE_SHORT_ACTIONS:
        if before_dir == "long":
            if reversal_evidence:
                alignment = "risk_control_good"
            elif bias == "short_bias":
                alignment = "risk_control_good"
            elif bias == "long_bias" and _to_float(trade.get("realized_pnl_after"), 0.0) >= 0:
                alignment = "premature_take_profit"
                tags.append("premature_take_profit")
            else:
                alignment = "risk_control_neutral"
        elif before_dir == "short":
            if reversal_evidence:
                alignment = "risk_control_good"
            elif bias == "long_bias":
                alignment = "risk_control_good"
            elif bias == "short_bias" and _to_float(trade.get("realized_pnl_after"), 0.0) >= 0:
                alignment = "premature_take_profit"
                tags.append("premature_take_profit")
            else:
                alignment = "risk_control_neutral"
        else:
            alignment = "observe"

    direction_points, direction_reasons, action_hint = _pattern_direction_adjustment(action, pattern_flags)
    if action in OPEN_LONG_ACTIONS.union(OPEN_SHORT_ACTIONS):
        if direction_points > 0 or action_hint == "aligned":
            alignment = "aligned"
            tags = [t for t in tags if t != "counter_trend_entry"]
        elif direction_points < 0 or action_hint == "counter":
            alignment = "counter"
            tags.append("counter_trend_entry")
    elif action in CLOSE_LONG_ACTIONS.union(CLOSE_SHORT_ACTIONS):
        if direction_points > 0:
            alignment = "risk_control_good"
            tags = [t for t in tags if t != "premature_take_profit"]
        elif direction_points < 0:
            alignment = "risk_control_bad"

    if action in ADD_ACTIONS and _to_float(trade.get("floating_pnl_after"), 0.0) < 0:
        tags.append("add_to_loser")

    conf = round(abs(score - 50.0) / 50.0, 4)
    bar_date = _normalize_date(trade.get("bar_date"))

    return {
        "trade_seq": _to_int(trade.get("trade_seq"), 0),
        "action": action,
        "bar_date": bar_date,
        "symbol": str(trade.get("symbol") or "")[:32],
        "symbol_type": str(trade.get("symbol_type") or "")[:16],
        "rule_score": score,
        "market_bias": bias,
        "alignment": alignment,
        "confidence": conf,
        "direction_points": round(_to_float(direction_points, 0.0), 2),
        "direction_reasons": list(direction_reasons),
        "before_dir": before_dir,
        "after_dir": after_dir,
        "evidence_patterns": patterns,
        "evidence_trends": trends,
        "violation_tags": sorted(set(tags)),
    }


def _build_mistakes_and_strengths(evals: List[Dict[str, Any]], trade_count: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    counts: Dict[str, int] = {
        "counter_trend_entry": 0,
        "add_to_loser": 0,
        "premature_take_profit": 0,
        "overtrading": 1 if int(trade_count) > 20 else 0,
    }
    aligned_entry = 0

    for e in evals:
        tags = list(e.get("violation_tags") or [])
        for t in tags:
            if t in counts:
                counts[t] += 1
        if e.get("alignment") == "aligned":
            aligned_entry += 1

    mistakes: List[Dict[str, Any]] = []
    if counts["counter_trend_entry"] > 0:
        mistakes.append(
            {
                "tag": "counter_trend_entry",
                "count": counts["counter_trend_entry"],
                "title": "逆势开仓/加仓",
                "suggestion": "先确认方向偏向再执行开仓，偏向冲突时优先等待确认。",
            }
        )
    if counts["add_to_loser"] > 0:
        mistakes.append(
            {
                "tag": "add_to_loser",
                "count": counts["add_to_loser"],
                "title": "亏损中继续加仓",
                "suggestion": "出现浮亏时先减仓或止损，避免摊薄型加仓。",
            }
        )
    if counts["premature_take_profit"] > 0:
        mistakes.append(
            {
                "tag": "premature_take_profit",
                "count": counts["premature_take_profit"],
                "title": "趋势延续中提前止盈",
                "suggestion": "强趋势里分批止盈，保留跟随仓位，避免过早离场。",
            }
        )
    if counts["overtrading"] > 0:
        mistakes.append(
            {
                "tag": "overtrading",
                "count": int(trade_count),
                "title": "交易频次过高",
                "suggestion": "单局超过20笔时强制暂停，复核是否在重复无效操作。",
            }
        )

    strengths: List[Dict[str, Any]] = []
    if aligned_entry > 0:
        strengths.append({"tag": "aligned_entry", "title": f"顺势开仓 {aligned_entry} 次"})
    risk_ctrl_good = sum(1 for e in evals if e.get("alignment") == "risk_control_good")
    if risk_ctrl_good > 0:
        strengths.append({"tag": "risk_control_good", "title": f"风险控制有效 {risk_ctrl_good} 次"})

    return mistakes, strengths


def analyze_game(
    db_engine,
    game_id: int,
    user_id: str,
    force: bool = False,
    generate_ai: bool = False,
    force_ai: bool = False,
    analysis_version: str = ANALYSIS_VERSION,
) -> Dict[str, Any]:
    gid = int(game_id or 0)
    uid = str(user_id or "").strip()
    if gid <= 0 or not uid:
        return {"ok": False, "message": "invalid game_id/user_id"}

    ensure_review_tables(db_engine)

    with db_engine.begin() as conn:
        game = _load_game_row(conn, gid, uid)
        if not game:
            return {"ok": False, "message": "game not found"}

        existed = conn.execute(
            text(
                """
                SELECT game_id
                FROM kline_game_analysis_reports
                WHERE game_id = :gid
                  AND analysis_version = :ver
                LIMIT 1
                """
            ),
            {"gid": gid, "ver": str(analysis_version)},
        ).fetchone()
        if existed and not force:
            row = conn.execute(
                text(
                    """
                    SELECT *
                    FROM kline_game_analysis_reports
                    WHERE game_id = :gid
                      AND analysis_version = :ver
                    LIMIT 1
                    """
                ),
                {"gid": gid, "ver": str(analysis_version)},
            ).mappings().fetchone()
            return {"ok": True, "already": True, "report": dict(row) if row else {}}

        conn.execute(
            text("DELETE FROM kline_game_trade_evaluations WHERE game_id = :gid"),
            {"gid": gid},
        )
        conn.execute(
            text(
                """
                DELETE FROM kline_game_analysis_reports
                WHERE game_id = :gid
                  AND analysis_version = :ver
                """
            ),
            {"gid": gid, "ver": str(analysis_version)},
        )

        trades = _load_game_trades(conn, gid, uid)
        evals: List[Dict[str, Any]] = []
        for t in trades:
            bar_date = _normalize_date(t.get("bar_date"))
            symbol = str(t.get("symbol") or game.get("symbol") or "")
            symbol_type = str(t.get("symbol_type") or game.get("symbol_type") or "stock")
            window_df = _fetch_kline_window(conn, symbol=symbol, symbol_type=symbol_type, bar_date=bar_date, window=90)
            ev = _evaluate_one_trade(t, window_df)
            ev["symbol"] = symbol[:32]
            ev["symbol_type"] = symbol_type[:16]
            evals.append(ev)

        for e in evals:
            conn.execute(
                text(
                    """
                    INSERT INTO kline_game_trade_evaluations
                    (game_id, user_id, trade_seq, action, bar_date, symbol, symbol_type,
                     rule_score, market_bias, alignment, confidence, direction_points, direction_reasons_json,
                     evidence_patterns_json, evidence_trends_json, violation_tags_json)
                    VALUES
                    (:gid, :uid, :seq, :action, :bar_date, :symbol, :symbol_type,
                     :rule_score, :market_bias, :alignment, :confidence, :direction_points, :direction_reasons,
                     :patterns, :trends, :tags)
                    """
                ),
                {
                    "gid": gid,
                    "uid": uid,
                    "seq": int(e.get("trade_seq") or 0),
                    "action": str(e.get("action") or "")[:32],
                    "bar_date": e.get("bar_date"),
                    "symbol": str(e.get("symbol") or "")[:32],
                    "symbol_type": str(e.get("symbol_type") or "")[:16],
                    "rule_score": _to_float(e.get("rule_score"), 50.0),
                    "market_bias": str(e.get("market_bias") or "neutral")[:16],
                    "alignment": str(e.get("alignment") or "observe")[:32],
                    "confidence": _to_float(e.get("confidence"), 0.0),
                    "direction_points": _to_float(e.get("direction_points"), 0.0),
                    "direction_reasons": _json_dump(list(e.get("direction_reasons") or [])),
                    "patterns": _json_dump(list(e.get("evidence_patterns") or [])),
                    "trends": _json_dump(list(e.get("evidence_trends") or [])),
                    "tags": _json_dump(list(e.get("violation_tags") or [])),
                },
            )

        entry_actions = [e for e in evals if e.get("action") in OPEN_LONG_ACTIONS.union(OPEN_SHORT_ACTIONS)]
        aligned_entries = sum(1 for e in entry_actions if e.get("alignment") == "aligned")
        counter_entries = sum(1 for e in entry_actions if e.get("alignment") == "counter")
        direction_score, direction_components = _compute_direction_score(evals)

        max_dd = _to_float(game.get("max_drawdown"), 0.0)
        risk_score = _clip(100.0 - max_dd * 100.0)

        trade_count = _to_int(game.get("trade_count"), len(evals))
        add_to_loser_cnt = sum(1 for e in evals if "add_to_loser" in list(e.get("violation_tags") or []))
        premature_cnt = sum(1 for e in evals if "premature_take_profit" in list(e.get("violation_tags") or []))
        overtrade_penalty = 15.0 if trade_count > 20 else 0.0
        execution_score = _clip(100.0 - overtrade_penalty - add_to_loser_cnt * 5.0 - premature_cnt * 4.0)

        overall_score = _clip(direction_score * 0.55 + risk_score * 0.30 + execution_score * 0.15)

        mistakes, strengths = _build_mistakes_and_strengths(evals, trade_count)

        metrics = {
            "overall_score": round(overall_score, 2),
            "direction_score": round(direction_score, 2),
            "risk_score": round(risk_score, 2),
            "execution_score": round(execution_score, 2),
            "trade_count": int(trade_count),
            "aligned_entries": int(aligned_entries),
            "counter_entries": int(counter_entries),
            "max_drawdown": float(max_dd),
            "profit": _to_float(game.get("profit"), 0.0),
            "profit_rate": _to_float(game.get("profit_rate"), 0.0),
            "direction_components": direction_components,
        }

        ai_report = _build_rule_only_review(metrics, mistakes, strengths, evals)
        ai_status = "rule_only"
        ai_model = ""
        if generate_ai:
            ai_report, ai_status, ai_model = _try_generate_ai_review(metrics, mistakes, strengths, evals)
            if force_ai and ai_status != "ai":
                # force_ai=true 下也保证有可展示内容
                ai_status = "rule_only"

        now = datetime.now()
        conn.execute(
            text(
                """
                INSERT INTO kline_game_analysis_reports
                (game_id, user_id, analysis_version,
                 overall_score, direction_score, risk_score, execution_score,
                 metrics_json, mistakes_json, strengths_json,
                 ai_report_json, ai_status, ai_model,
                 created_at, updated_at)
                VALUES
                (:gid, :uid, :ver,
                 :overall, :direction, :risk, :execution,
                 :metrics, :mistakes, :strengths,
                 :ai_report, :ai_status, :ai_model,
                 :created_at, :updated_at)
                """
            ),
            {
                "gid": gid,
                "uid": uid,
                "ver": str(analysis_version),
                "overall": round(overall_score, 2),
                "direction": round(direction_score, 2),
                "risk": round(risk_score, 2),
                "execution": round(execution_score, 2),
                "metrics": _json_dump(metrics),
                "mistakes": _json_dump(mistakes),
                "strengths": _json_dump(strengths),
                "ai_report": _json_dump(ai_report),
                "ai_status": str(ai_status)[:32],
                "ai_model": str(ai_model)[:64],
                "created_at": now,
                "updated_at": now,
            },
        )

    return {
        "ok": True,
        "analysis_version": str(analysis_version),
        "game_id": gid,
        "user_id": uid,
        "summary": {
            "overall_score": round(overall_score, 2),
            "direction_score": round(direction_score, 2),
            "risk_score": round(risk_score, 2),
            "execution_score": round(execution_score, 2),
            "ai_status": ai_status,
        },
    }


def fetch_report(db_engine, game_id: int, analysis_version: str = ANALYSIS_VERSION) -> Optional[Dict[str, Any]]:
    ensure_review_tables(db_engine)
    with db_engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT *
                FROM kline_game_analysis_reports
                WHERE game_id = :gid
                  AND analysis_version = :ver
                LIMIT 1
                """
            ),
            {"gid": int(game_id), "ver": str(analysis_version)},
        ).mappings().fetchone()
        if not row:
            return None
        out = dict(row)
        out["metrics"] = _json_load(out.get("metrics_json"), {})
        out["mistakes"] = _json_load(out.get("mistakes_json"), [])
        out["strengths"] = _json_load(out.get("strengths_json"), [])
        out["ai_report"] = _json_load(out.get("ai_report_json"), {})
        return out


def fetch_evaluations(db_engine, game_id: int) -> List[Dict[str, Any]]:
    ensure_review_tables(db_engine)
    with db_engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT *
                FROM kline_game_trade_evaluations
                WHERE game_id = :gid
                ORDER BY trade_seq ASC
                """
            ),
            {"gid": int(game_id)},
        ).mappings().fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        one = dict(r)
        one["direction_points"] = _to_float(one.get("direction_points"), 0.0)
        one["direction_reasons"] = _json_load(one.get("direction_reasons_json"), [])
        one["evidence_patterns"] = _json_load(one.get("evidence_patterns_json"), [])
        one["evidence_trends"] = _json_load(one.get("evidence_trends_json"), [])
        one["violation_tags"] = _json_load(one.get("violation_tags_json"), [])
        out.append(one)
    return out


def build_habit_profile(
    db_engine,
    user_id: str,
    lookback_games: int = 20,
    analysis_version: str = ANALYSIS_VERSION,
) -> Dict[str, Any]:
    uid = str(user_id or "").strip()
    if not uid:
        return {"ok": False, "message": "missing user_id"}

    ensure_review_tables(db_engine)
    with db_engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT game_id, mistakes_json, overall_score
                FROM kline_game_analysis_reports
                WHERE user_id = :uid
                  AND analysis_version = :ver
                ORDER BY updated_at DESC
                LIMIT :lim
                """
            ),
            {"uid": uid, "ver": str(analysis_version), "lim": int(max(1, lookback_games))},
        ).mappings().fetchall()

    tag_counts: Dict[str, int] = {}
    scores: List[float] = []
    for r in rows:
        scores.append(_to_float(r.get("overall_score"), 0.0))
        for m in _json_load(r.get("mistakes_json"), []):
            tag = str((m or {}).get("tag") or "").strip()
            if not tag:
                continue
            tag_counts[tag] = tag_counts.get(tag, 0) + int((m or {}).get("count") or 1)

    top_habits = sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)[:5]
    avg_score = sum(scores) / len(scores) if scores else 0.0

    return {
        "ok": True,
        "user_id": uid,
        "lookback_games": len(rows),
        "avg_overall_score": round(avg_score, 2),
        "top_habits": [{"tag": k, "count": int(v)} for k, v in top_habits],
    }


def fetch_user_global_report(
    db_engine,
    user_id: str,
    max_trades: int = 2000,
    analysis_version: str = ANALYSIS_VERSION,
) -> Optional[Dict[str, Any]]:
    uid = str(user_id or "").strip()
    if not uid:
        return None
    scope_value = int(max(1, min(2000, _to_int(max_trades, 2000))))
    ensure_review_tables(db_engine)
    with db_engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT *
                FROM kline_user_trade_analysis_reports
                WHERE user_id = :uid
                  AND scope_type = 'last_n_trades'
                  AND scope_value = :scope_value
                  AND analysis_version = :ver
                LIMIT 1
                """
            ),
            {"uid": uid, "scope_value": scope_value, "ver": str(analysis_version)},
        ).mappings().fetchone()
    if not row:
        return None
    out = dict(row)
    out["metrics"] = _json_load(out.get("metrics_json"), {})
    out["radar"] = _json_load(out.get("radar_json"), {})
    out["habit_summary"] = _json_load(out.get("habit_summary_json"), {})
    out["ai_report"] = _json_load(out.get("ai_report_json"), {})
    return out


def build_user_trade_aggregate(
    db_engine,
    user_id: str,
    max_trades: int = 2000,
    analysis_version: str = ANALYSIS_VERSION,
) -> Dict[str, Any]:
    uid = str(user_id or "").strip()
    if not uid:
        return {"ok": False, "message": "missing user_id"}

    lim = int(max(1, min(2000, _to_int(max_trades, 2000))))
    ensure_review_tables(db_engine)

    with db_engine.connect() as conn:
        trade_rows = conn.execute(
            text(
                """
                SELECT t.id, t.game_id, t.trade_seq, t.action, t.bar_date, t.trade_time, t.symbol, t.symbol_type,
                       t.price, t.lots, t.realized_pnl_after, t.floating_pnl_after, g.game_end_time
                FROM kline_game_trades t
                JOIN kline_game_records g
                  ON g.id = t.game_id
                 AND g.user_id = t.user_id
                WHERE t.user_id = :uid
                  AND g.status = 'finished'
                  AND g.end_reason = 'completed'
                ORDER BY COALESCE(t.trade_time, g.game_end_time) DESC, t.id DESC
                LIMIT :lim
                """
            ),
            {"uid": uid, "lim": lim},
        ).mappings().fetchall()

    trades = [dict(r) for r in trade_rows]
    if not trades:
        empty_metrics = {
            "avg_overall_score": 0.0,
            "avg_direction_score": 0.0,
            "avg_risk_score": 0.0,
            "avg_execution_score": 0.0,
            "aligned_entry_count": 0,
            "counter_entry_count": 0,
            "reversal_events": 0,
            "reversal_responded": 0,
            "reversal_response_rate": 0.0,
            "overall_score_std": 0.0,
            "violation_total": 0,
            "top_violation_ratio": 0.0,
            "stability_score": 60.0,
        }
        return {
            "ok": True,
            "user_id": uid,
            "max_trades": lim,
            "source_trade_count": 0,
            "source_game_count": 0,
            "metrics": empty_metrics,
            "habit_summary": {"top_habits": []},
            "samples": [],
        }

    game_ids = sorted({int(_to_int(t.get("game_id"), 0)) for t in trades if _to_int(t.get("game_id"), 0) > 0})

    # 优先补齐缺失的单局规则分析（不触发 AI）
    with db_engine.connect() as conn:
        in_sql, in_params = _build_in_clause(game_ids, "gid_")
        rows = conn.execute(
            text(
                f"""
                SELECT game_id
                FROM kline_game_analysis_reports
                WHERE analysis_version = :ver
                  AND game_id IN ({in_sql})
                """
            ),
            {"ver": str(analysis_version), **in_params},
        ).mappings().fetchall()
        existed_report_ids = {int(_to_int(r.get("game_id"), 0)) for r in rows}

        owner_rows = conn.execute(
            text(
                f"""
                SELECT id, user_id
                FROM kline_game_records
                WHERE id IN ({in_sql})
                """
            ),
            in_params,
        ).mappings().fetchall()
        game_owner = {int(_to_int(r.get("id"), 0)): str(r.get("user_id") or "") for r in owner_rows}

    missing_report_ids = [gid for gid in game_ids if gid not in existed_report_ids]
    for gid in missing_report_ids:
        owner = str(game_owner.get(gid) or uid)
        analyze_game(
            db_engine=db_engine,
            game_id=gid,
            user_id=owner,
            force=False,
            generate_ai=False,
            analysis_version=analysis_version,
        )

    with db_engine.connect() as conn:
        in_sql, in_params = _build_in_clause(game_ids, "gid2_")
        eval_rows = conn.execute(
            text(
                f"""
                SELECT game_id, trade_seq, action, bar_date, symbol, symbol_type,
                       rule_score, market_bias, alignment, confidence, violation_tags_json,
                       evidence_patterns_json, evidence_trends_json
                FROM kline_game_trade_evaluations
                WHERE game_id IN ({in_sql})
                ORDER BY game_id DESC, trade_seq DESC
                """
            ),
            in_params,
        ).mappings().fetchall()

        report_rows = conn.execute(
            text(
                f"""
                SELECT game_id, overall_score, direction_score, risk_score, execution_score, metrics_json, mistakes_json
                FROM kline_game_analysis_reports
                WHERE analysis_version = :ver
                  AND game_id IN ({in_sql})
                """
            ),
            {"ver": str(analysis_version), **in_params},
        ).mappings().fetchall()

    eval_map: Dict[Tuple[int, int], Dict[str, Any]] = {}
    for r in eval_rows:
        game_id = int(_to_int(r.get("game_id"), 0))
        seq = int(_to_int(r.get("trade_seq"), 0))
        eval_map[(game_id, seq)] = {
            "action": str(r.get("action") or ""),
            "rule_score": _to_float(r.get("rule_score"), 50.0),
            "market_bias": str(r.get("market_bias") or "neutral"),
            "alignment": str(r.get("alignment") or "observe"),
            "confidence": _to_float(r.get("confidence"), 0.0),
            "violation_tags": _json_load(r.get("violation_tags_json"), []),
            "evidence_patterns": _json_load(r.get("evidence_patterns_json"), []),
            "evidence_trends": _json_load(r.get("evidence_trends_json"), []),
            "bar_date": _normalize_date(r.get("bar_date")),
            "symbol": str(r.get("symbol") or ""),
            "symbol_type": str(r.get("symbol_type") or ""),
        }

    aligned_entry = 0
    counter_entry = 0
    risk_control_good = 0
    violation_counts: Dict[str, int] = {}
    samples: List[Dict[str, Any]] = []

    for t in trades:
        gid = int(_to_int(t.get("game_id"), 0))
        seq = int(_to_int(t.get("trade_seq"), 0))
        ev = eval_map.get((gid, seq))
        if not ev:
            continue
        action = str(ev.get("action") or "").lower()
        if action in OPEN_LONG_ACTIONS.union(OPEN_SHORT_ACTIONS):
            if str(ev.get("alignment")) == "aligned":
                aligned_entry += 1
            elif str(ev.get("alignment")) == "counter":
                counter_entry += 1
        if str(ev.get("alignment")) == "risk_control_good":
            risk_control_good += 1
        for tag in list(ev.get("violation_tags") or []):
            tkey = str(tag or "").strip()
            if not tkey:
                continue
            violation_counts[tkey] = violation_counts.get(tkey, 0) + 1
        if len(samples) < 16:
            bd = ev.get("bar_date")
            if isinstance(bd, date):
                bd = bd.strftime("%Y-%m-%d")
            samples.append(
                {
                    "game_id": gid,
                    "trade_seq": seq,
                    "action": str(ev.get("action") or ""),
                    "bar_date": str(bd or ""),
                    "rule_score": round(_to_float(ev.get("rule_score"), 50.0), 2),
                    "market_bias": str(ev.get("market_bias") or "neutral"),
                    "alignment": str(ev.get("alignment") or "observe"),
                    "tags": list(ev.get("violation_tags") or []),
                }
            )

    overall_scores: List[float] = []
    direction_scores: List[float] = []
    risk_scores: List[float] = []
    execution_scores: List[float] = []
    reversal_events = 0
    reversal_responded = 0
    reversal_missed = 0

    for r in report_rows:
        overall_scores.append(_to_float(r.get("overall_score"), 0.0))
        direction_scores.append(_to_float(r.get("direction_score"), 0.0))
        risk_scores.append(_to_float(r.get("risk_score"), 0.0))
        execution_scores.append(_to_float(r.get("execution_score"), 0.0))
        m = _json_load(r.get("metrics_json"), {})
        dc = dict((m or {}).get("direction_components") or {})
        reversal_events += int(_to_int(dc.get("reversal_events"), 0))
        reversal_responded += int(_to_int(dc.get("reversal_responded"), 0))
        reversal_missed += int(_to_int(dc.get("reversal_missed"), 0))

    avg_overall = sum(overall_scores) / len(overall_scores) if overall_scores else 0.0
    avg_direction = sum(direction_scores) / len(direction_scores) if direction_scores else 0.0
    avg_risk = sum(risk_scores) / len(risk_scores) if risk_scores else 0.0
    avg_execution = sum(execution_scores) / len(execution_scores) if execution_scores else 0.0
    overall_std = statistics.pstdev(overall_scores) if len(overall_scores) > 1 else 0.0

    violation_total = int(sum(int(v) for v in violation_counts.values()))
    top_violation_ratio = (max(violation_counts.values()) / violation_total) if violation_total > 0 else 0.0
    stability_score = _clip(100.0 - overall_std * 2.0 - top_violation_ratio * 25.0)
    reversal_response_rate = (100.0 * reversal_responded / reversal_events) if reversal_events > 0 else 60.0

    top_habits = sorted(violation_counts.items(), key=lambda x: x[1], reverse=True)[:8]
    metrics = {
        "avg_overall_score": round(avg_overall, 2),
        "avg_direction_score": round(avg_direction, 2),
        "avg_risk_score": round(avg_risk, 2),
        "avg_execution_score": round(avg_execution, 2),
        "aligned_entry_count": int(aligned_entry),
        "counter_entry_count": int(counter_entry),
        "risk_control_good_count": int(risk_control_good),
        "reversal_events": int(reversal_events),
        "reversal_responded": int(reversal_responded),
        "reversal_missed": int(reversal_missed),
        "reversal_response_rate": round(reversal_response_rate, 2),
        "overall_score_std": round(overall_std, 2),
        "violation_total": int(violation_total),
        "top_violation_ratio": round(top_violation_ratio, 4),
        "stability_score": round(stability_score, 2),
    }
    habit_summary = {"top_habits": [{"tag": k, "count": int(v)} for k, v in top_habits]}
    return {
        "ok": True,
        "user_id": uid,
        "max_trades": lim,
        "source_trade_count": int(len(trades)),
        "source_game_count": int(len(game_ids)),
        "metrics": metrics,
        "habit_summary": habit_summary,
        "samples": samples,
    }


def compute_user_radar_scores(aggregate: Dict[str, Any]) -> Dict[str, Any]:
    metrics = dict((aggregate or {}).get("metrics") or {})
    aligned = int(_to_int(metrics.get("aligned_entry_count"), 0))
    counter = int(_to_int(metrics.get("counter_entry_count"), 0))
    entry_total = max(1, aligned + counter)
    direction_consistency = _clip(100.0 * aligned / entry_total) if (aligned + counter) > 0 else 60.0

    reversal_events = int(_to_int(metrics.get("reversal_events"), 0))
    reversal_responded = int(_to_int(metrics.get("reversal_responded"), 0))
    reversal_response = _clip(100.0 * reversal_responded / reversal_events) if reversal_events > 0 else 60.0

    risk_control = _clip(_to_float(metrics.get("avg_risk_score"), 0.0))
    execution_discipline = _clip(_to_float(metrics.get("avg_execution_score"), 0.0))
    stability = _clip(_to_float(metrics.get("stability_score"), 60.0))

    dimensions = ["方向一致性", "转折应对", "风险控制", "执行纪律", "稳定性"]
    values = [
        round(direction_consistency, 2),
        round(reversal_response, 2),
        round(risk_control, 2),
        round(execution_discipline, 2),
        round(stability, 2),
    ]
    return {
        "dimensions": dimensions,
        "values": values,
        "scores": {dimensions[i]: values[i] for i in range(len(dimensions))},
    }


def generate_game_ai_review(
    db_engine,
    game_id: int,
    user_id: str,
    force: bool = False,
    analysis_version: str = ANALYSIS_VERSION,
) -> Dict[str, Any]:
    gid = int(_to_int(game_id, 0))
    uid = str(user_id or "").strip()
    if gid <= 0 or not uid:
        return {"ok": False, "message": "invalid game_id/user_id"}

    ensure_review_tables(db_engine)
    report = fetch_report(db_engine, gid, analysis_version=analysis_version)
    if not report:
        analyze_res = analyze_game(
            db_engine=db_engine,
            game_id=gid,
            user_id=uid,
            force=False,
            generate_ai=False,
            analysis_version=analysis_version,
        )
        if not analyze_res.get("ok"):
            return {"ok": False, "message": analyze_res.get("message", "analysis failed")}
        report = fetch_report(db_engine, gid, analysis_version=analysis_version)
    if not report:
        return {"ok": False, "message": "analysis report unavailable"}

    ai_status = str(report.get("ai_status") or "")
    ai_report = dict(report.get("ai_report") or {})
    if (not force) and ai_status == "ai" and ai_report:
        return {"ok": True, "cached": True, "game_id": gid, "user_id": uid, "report": report}

    metrics = dict(report.get("metrics") or {})
    mistakes = list(report.get("mistakes") or [])
    strengths = list(report.get("strengths") or [])
    evals = fetch_evaluations(db_engine, gid)
    ai_report, ai_status, ai_model = _try_generate_ai_review(metrics, mistakes, strengths, evals)

    with db_engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE kline_game_analysis_reports
                SET ai_report_json = :ai_report,
                    ai_status = :ai_status,
                    ai_model = :ai_model,
                    updated_at = :updated_at
                WHERE game_id = :gid
                  AND analysis_version = :ver
                """
            ),
            {
                "ai_report": _json_dump(ai_report),
                "ai_status": str(ai_status)[:32],
                "ai_model": str(ai_model)[:64],
                "updated_at": datetime.now(),
                "gid": gid,
                "ver": str(analysis_version),
            },
        )

    latest = fetch_report(db_engine, gid, analysis_version=analysis_version) or {}
    return {
        "ok": True,
        "cached": False,
        "game_id": gid,
        "user_id": uid,
        "ai_status": str(latest.get("ai_status") or ai_status),
        "report": latest,
    }


def generate_user_global_ai_report(
    db_engine,
    user_id: str,
    max_trades: int = 2000,
    force: bool = False,
    analysis_version: str = ANALYSIS_VERSION,
) -> Dict[str, Any]:
    uid = str(user_id or "").strip()
    if not uid:
        return {"ok": False, "message": "missing user_id"}
    scope_value = int(max(1, min(2000, _to_int(max_trades, 2000))))
    ensure_review_tables(db_engine)

    cached = fetch_user_global_report(
        db_engine=db_engine,
        user_id=uid,
        max_trades=scope_value,
        analysis_version=analysis_version,
    )
    if cached and (not force) and str(cached.get("ai_status") or "") == "ai":
        return {"ok": True, "cached": True, "user_id": uid, "report": cached}

    aggregate = build_user_trade_aggregate(
        db_engine=db_engine,
        user_id=uid,
        max_trades=scope_value,
        analysis_version=analysis_version,
    )
    if not aggregate.get("ok"):
        return aggregate
    radar = compute_user_radar_scores(aggregate)
    ai_report, ai_status, ai_model = _try_generate_user_global_ai_report(aggregate, radar)

    with db_engine.begin() as conn:
        conn.execute(
            text(
                """
                DELETE FROM kline_user_trade_analysis_reports
                WHERE user_id = :uid
                  AND scope_type = 'last_n_trades'
                  AND scope_value = :scope_value
                  AND analysis_version = :ver
                """
            ),
            {"uid": uid, "scope_value": scope_value, "ver": str(analysis_version)},
        )
        now = datetime.now()
        conn.execute(
            text(
                """
                INSERT INTO kline_user_trade_analysis_reports
                (user_id, scope_type, scope_value, analysis_version,
                 metrics_json, radar_json, habit_summary_json, ai_report_json, ai_status, ai_model,
                 source_trade_count, source_game_count, created_at, updated_at)
                VALUES
                (:uid, 'last_n_trades', :scope_value, :ver,
                 :metrics, :radar, :habit_summary, :ai_report, :ai_status, :ai_model,
                 :source_trade_count, :source_game_count, :created_at, :updated_at)
                """
            ),
            {
                "uid": uid,
                "scope_value": scope_value,
                "ver": str(analysis_version),
                "metrics": _json_dump(aggregate.get("metrics") or {}),
                "radar": _json_dump(radar),
                "habit_summary": _json_dump(aggregate.get("habit_summary") or {}),
                "ai_report": _json_dump(ai_report),
                "ai_status": str(ai_status)[:32],
                "ai_model": str(ai_model)[:64],
                "source_trade_count": int(_to_int(aggregate.get("source_trade_count"), 0)),
                "source_game_count": int(_to_int(aggregate.get("source_game_count"), 0)),
                "created_at": now,
                "updated_at": now,
            },
        )

    latest = fetch_user_global_report(
        db_engine=db_engine,
        user_id=uid,
        max_trades=scope_value,
        analysis_version=analysis_version,
    ) or {}
    return {"ok": True, "cached": False, "user_id": uid, "report": latest}


def _symbol_roots(symbol: str) -> List[str]:
    s = str(symbol or "").strip()
    if not s:
        return []
    base = s.upper().split(".")[0].strip()
    roots = [s, s.upper(), base]
    root_alpha = ""
    for ch in base:
        if "A" <= ch <= "Z":
            root_alpha += ch
        else:
            break
    if root_alpha:
        roots.append(root_alpha)
    out: List[str] = []
    seen = set()
    for x in roots:
        v = str(x or "").strip()
        if v and v not in seen:
            out.append(v)
            seen.add(v)
    return out


def _query_chart_df(
    conn,
    table_name: str,
    symbol: str,
    start_date: Optional[date],
    end_date: Optional[date],
    like_prefix: bool = False,
) -> pd.DataFrame:
    params: Dict[str, Any] = {}
    if like_prefix:
        cond = ["UPPER(ts_code) LIKE :code_like", "open_price IS NOT NULL", "close_price IS NOT NULL"]
        params["code_like"] = f"{str(symbol or '').upper()}%"
    else:
        cond = ["ts_code = :code", "open_price IS NOT NULL", "close_price IS NOT NULL"]
        params["code"] = str(symbol or "")
    if start_date:
        cond.append("REPLACE(CAST(trade_date AS CHAR), '-', '') >= :start_ds")
        params["start_ds"] = start_date.strftime("%Y%m%d")
    if end_date:
        cond.append("REPLACE(CAST(trade_date AS CHAR), '-', '') <= :end_ds")
        params["end_ds"] = end_date.strftime("%Y%m%d")
    sql = text(
        f"""
        SELECT trade_date, open_price, high_price, low_price, close_price
        FROM {table_name}
        WHERE {' AND '.join(cond)}
        ORDER BY trade_date ASC
        LIMIT 600
        """
    )
    try:
        return pd.read_sql(sql, conn, params=params)
    except Exception:
        return pd.DataFrame()


def fetch_chart_bars(
    db_engine,
    symbol: str,
    symbol_type: str,
    start_date: Optional[date],
    end_date: Optional[date],
) -> List[Dict[str, Any]]:
    sym = str(symbol or "").strip()
    if not sym:
        return []

    primary_table = _table_by_symbol_type(symbol_type)
    tables = [primary_table, "stock_price", "index_price", "futures_price"]
    ordered_tables: List[str] = []
    seen_tables = set()
    for t in tables:
        if t not in seen_tables:
            ordered_tables.append(t)
            seen_tables.add(t)
    candidates = _symbol_roots(sym)
    if not candidates:
        return []

    df = pd.DataFrame()
    with db_engine.connect() as conn:
        for table_name in ordered_tables:
            for code in candidates:
                df = _query_chart_df(conn, table_name, code, start_date, end_date, like_prefix=False)
                if not df.empty:
                    break
            if not df.empty:
                break

            # 期货代码兼容：根代码前缀匹配
            if table_name == "futures_price":
                for code in candidates:
                    df = _query_chart_df(conn, table_name, code, start_date, end_date, like_prefix=True)
                    if not df.empty:
                        break
                if not df.empty:
                    break
    if df.empty:
        return []

    out: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        d = _normalize_date(r.get("trade_date"))
        if not d:
            continue
        out.append(
            {
                "date": d.strftime("%Y-%m-%d"),
                "open": _to_float(r.get("open_price"), 0.0),
                "high": _to_float(r.get("high_price"), 0.0),
                "low": _to_float(r.get("low_price"), 0.0),
                "close": _to_float(r.get("close_price"), 0.0),
            }
        )
    return out
