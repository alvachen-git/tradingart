#!/usr/bin/env python3
"""
14:25 突破引擎 V2（完全脱离 kline_tools）
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

SCAN_WINDOWS_V2: Tuple[int, ...] = (10, 20, 30)
DEFAULT_TOP_K = 6


def _normalize_trade_date(value: Any) -> str:
    text = "".join(ch for ch in str(value or "") if ch.isdigit())
    return text[:8] if len(text) >= 8 else ""


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _safe_div(a: float, b: float) -> float:
    if abs(float(b)) < 1e-12:
        return 0.0
    return float(a) / float(b)


def _calc_atr14(df: pd.DataFrame) -> pd.Series:
    work = df.copy()
    work["h_l"] = work["high_price"] - work["low_price"]
    work["h_pc"] = (work["high_price"] - work["close_price"].shift(1)).abs()
    work["l_pc"] = (work["low_price"] - work["close_price"].shift(1)).abs()
    work["tr"] = work[["h_l", "h_pc", "l_pc"]].max(axis=1)
    return work["tr"].rolling(window=14).mean()


def _linear_slope(values: List[float]) -> float:
    n = len(values)
    if n < 2:
        return 0.0
    xs = list(range(n))
    x_mean = sum(xs) / n
    y_mean = sum(values) / n
    num = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, values))
    den = sum((x - x_mean) ** 2 for x in xs)
    return _safe_div(num, den)


def _rank_percentile(values: List[float], higher_better: bool) -> List[float]:
    n = len(values)
    if n == 0:
        return []
    if n == 1:
        return [1.0]

    idx_vals = list(enumerate(values))
    idx_vals.sort(key=lambda x: x[1])
    scores = [0.0] * n

    i = 0
    while i < n:
        j = i + 1
        while j < n and idx_vals[j][1] == idx_vals[i][1]:
            j += 1
        avg_rank = (i + (j - 1)) / 2.0
        pct = avg_rank / float(n - 1)
        if not higher_better:
            pct = 1.0 - pct
        for k in range(i, j):
            scores[idx_vals[k][0]] = pct
        i = j
    return scores


def detect_rising_three_methods_v2(df: pd.DataFrame) -> bool:
    if df is None or len(df) < 6:
        return False
    work = df.copy().reset_index(drop=True)
    for col in ("open_price", "high_price", "low_price", "close_price"):
        work[col] = pd.to_numeric(work[col], errors="coerce")
    work = work.dropna(subset=["open_price", "high_price", "low_price", "close_price"]).reset_index(drop=True)
    if len(work) < 6:
        return False

    work["MA5"] = work["close_price"].rolling(window=5).mean()
    work["MA10"] = work["close_price"].rolling(window=10).mean()

    curr = work.iloc[-1]
    prev = work.iloc[-2]
    pprev = work.iloc[-3]
    tprev = work.iloc[-4]
    pppprev = work.iloc[-5]
    fprev = work.iloc[-6]

    close = float(curr["close_price"])
    prev_close = float(prev["close_price"])
    pprev_close = float(pprev["close_price"])
    prev_open = float(prev["open_price"])
    pprev_open = float(pprev["open_price"])
    tprev_close = float(tprev["close_price"])
    tprev_open = float(tprev["open_price"])
    pppprev_close = float(pppprev["close_price"])
    pppprev_open = float(pppprev["open_price"])
    fprev_close = float(fprev["close_price"])
    fprev_open = float(fprev["open_price"])

    prev_high = float(prev["high_price"])
    tprev_high = float(tprev["high_price"])
    pppprev_high = float(pppprev["high_price"])
    fprev_high = float(fprev["high_price"])

    chg_pct = _safe_div(close - prev_close, prev_close)
    pprev_chg_pct = _safe_div(pprev_close - pprev_open, pprev_open)
    tprev_chg_pct = _safe_div(tprev_close - tprev_open, tprev_open)
    pppprev_chg_pct = _safe_div(pppprev_close - pppprev_open, pppprev_open)
    fprev_chg_pct = _safe_div(fprev_close - fprev_open, fprev_open)

    prev_2_days_high = float(work["high_price"].iloc[-3:-1].max())
    prev_3_days_high = float(work["high_price"].iloc[-4:-1].max())
    prev_4_days_high = float(work["high_price"].iloc[-5:-1].max())
    prev_5_days_high = float(work["high_price"].iloc[-6:-1].max())

    ma5 = _safe_float(curr.get("MA5", 0.0))
    ma10 = _safe_float(curr.get("MA10", 0.0))
    if not (ma5 > ma10 and chg_pct > 0.01):
        return False

    if pprev_chg_pct > 0.015 and prev_close < pprev_close and prev_open < pprev_close and close > prev_2_days_high:
        return True
    if tprev_chg_pct > 0.015 and pprev_close < tprev_high and prev_close < tprev_high and close > prev_3_days_high:
        return True
    if (
        pppprev_chg_pct > 0.015
        and tprev_close < pppprev_high
        and pprev_close < pppprev_high
        and prev_close < pppprev_high
        and close > prev_4_days_high
    ):
        return True
    if (
        fprev_chg_pct > 0.015
        and pppprev_close < fprev_high
        and tprev_close < fprev_high
        and pprev_close < fprev_high
        and prev_close < fprev_high
        and close > prev_5_days_high
    ):
        return True
    return False


def detect_falling_three_methods_v2(df: pd.DataFrame) -> bool:
    if df is None or len(df) < 6:
        return False
    work = df.copy().reset_index(drop=True)
    for col in ("open_price", "high_price", "low_price", "close_price"):
        work[col] = pd.to_numeric(work[col], errors="coerce")
    work = work.dropna(subset=["open_price", "high_price", "low_price", "close_price"]).reset_index(drop=True)
    if len(work) < 6:
        return False

    work["MA5"] = work["close_price"].rolling(window=5).mean()
    work["MA10"] = work["close_price"].rolling(window=10).mean()

    curr = work.iloc[-1]
    prev = work.iloc[-2]
    pprev = work.iloc[-3]
    tprev = work.iloc[-4]
    pppprev = work.iloc[-5]
    fprev = work.iloc[-6]

    close = float(curr["close_price"])
    prev_close = float(prev["close_price"])
    pprev_close = float(pprev["close_price"])
    prev_open = float(prev["open_price"])
    pprev_open = float(pprev["open_price"])
    tprev_close = float(tprev["close_price"])
    tprev_open = float(tprev["open_price"])
    pppprev_close = float(pppprev["close_price"])
    pppprev_open = float(pppprev["open_price"])
    fprev_close = float(fprev["close_price"])
    fprev_open = float(fprev["open_price"])

    pprev_low = float(pprev["low_price"])
    tprev_low = float(tprev["low_price"])
    pppprev_low = float(pppprev["low_price"])
    fprev_low = float(fprev["low_price"])

    chg_pct = _safe_div(close - prev_close, prev_close)
    pprev_chg_pct = _safe_div(pprev_close - pprev_open, pprev_open)
    tprev_chg_pct = _safe_div(tprev_close - tprev_open, tprev_open)
    pppprev_chg_pct = _safe_div(pppprev_close - pppprev_open, pppprev_open)
    fprev_chg_pct = _safe_div(fprev_close - fprev_open, fprev_open)

    prev_2_days_low = float(work["low_price"].iloc[-3:-1].min())
    prev_3_days_low = float(work["low_price"].iloc[-4:-1].min())
    prev_4_days_low = float(work["low_price"].iloc[-5:-1].min())
    prev_5_days_low = float(work["low_price"].iloc[-6:-1].min())

    ma5 = _safe_float(curr.get("MA5", 0.0))
    ma10 = _safe_float(curr.get("MA10", 0.0))
    if not (ma5 < ma10 and chg_pct < -0.01):
        return False

    if pprev_chg_pct < -0.015 and prev_close > pprev_close and prev_open > pprev_close and close < prev_2_days_low:
        return True
    if tprev_chg_pct < -0.015 and pprev_close > tprev_low and prev_close > tprev_low and close < prev_3_days_low:
        return True
    if (
        pppprev_chg_pct < -0.015
        and tprev_close > pppprev_low
        and pprev_close > pppprev_low
        and prev_close > pppprev_low
        and close < prev_4_days_low
    ):
        return True
    if (
        fprev_chg_pct < -0.015
        and pppprev_close > fprev_low
        and tprev_close > fprev_low
        and pprev_close > fprev_low
        and prev_close > fprev_low
        and close < prev_5_days_low
    ):
        return True
    return False


class BreakoutEngineV2:
    def __init__(self, top_k: int = DEFAULT_TOP_K, scan_windows: Tuple[int, ...] = SCAN_WINDOWS_V2):
        self.top_k = max(1, int(top_k))
        self.scan_windows = tuple(int(x) for x in scan_windows if int(x) > 1) or SCAN_WINDOWS_V2

    def _pick_window(self, windows: List[Dict[str, Any]], target_period: int = 20) -> Optional[Dict[str, Any]]:
        if not windows:
            return None
        for w in windows:
            if int(w.get("period", 0)) == int(target_period):
                return w
        return windows[0]

    def select_candidates(
        self,
        history_map: Dict[str, pd.DataFrame],
        realtime_map: Dict[str, Optional[Dict[str, Any]]],
        symbol_name_map: Optional[Dict[str, str]] = None,
        trade_date: str = "",
        scan_symbols: Optional[List[str]] = None,
        debug: bool = False,
    ) -> Dict[str, Any]:
        symbol_name_map = symbol_name_map or {}
        symbols = [str(x).upper() for x in (scan_symbols or list(history_map.keys()))]

        seeds: List[Dict[str, Any]] = []
        debug_rows: List[Dict[str, Any]] = []

        for symbol in symbols:
            row: Dict[str, Any] = {
                "symbol": symbol,
                "symbol_name": str(symbol_name_map.get(symbol, "") or symbol_name_map.get(symbol.upper(), "") or ""),
                "windows": [],
                "three_method": {"up": False, "down": False},
                "drop_reasons": [],
                "selected": False,
            }

            rt = realtime_map.get(symbol)
            if not rt:
                row["drop_reasons"].append("no_realtime")
                debug_rows.append(row)
                continue

            df = history_map.get(symbol)
            if df is None or df.empty:
                row["drop_reasons"].append("no_history")
                debug_rows.append(row)
                continue

            work = df.copy().reset_index(drop=True)
            for col in ("open_price", "high_price", "low_price", "close_price"):
                work[col] = pd.to_numeric(work[col], errors="coerce")
            work = work.dropna(subset=["open_price", "high_price", "low_price", "close_price"]).reset_index(drop=True)
            if work.empty:
                row["drop_reasons"].append("history_invalid")
                debug_rows.append(row)
                continue

            atr_series = _calc_atr14(work)
            latest_trade_date = trade_date or _normalize_trade_date(work.iloc[-1]["trade_date"])
            has_current_bar = _normalize_trade_date(work.iloc[-1]["trade_date"]) == latest_trade_date
            if atr_series.empty:
                row["drop_reasons"].append("atr_empty")
                debug_rows.append(row)
                continue
            atr_idx = -2 if has_current_bar and len(atr_series) >= 2 else -1
            atr_ref = _safe_float(atr_series.iloc[atr_idx], 0.0)
            if atr_ref <= 0:
                row["drop_reasons"].append("atr_non_positive")
                debug_rows.append(row)
                continue

            realtime_price = _safe_float(rt.get("price", 0.0), 0.0)
            latest_close = _safe_float(work.iloc[-1]["close_price"], 0.0)
            row["contract_code"] = str(rt.get("contract_code", "") or "")
            row["contract_name"] = str(rt.get("name", "") or "")
            row["realtime_price"] = realtime_price
            row["latest_close"] = latest_close
            row["atr14"] = atr_ref

            rising_three = detect_rising_three_methods_v2(work)
            falling_three = detect_falling_three_methods_v2(work)
            row["three_method"] = {"up": bool(rising_three), "down": bool(falling_three)}

            window_infos: List[Dict[str, Any]] = []
            breakout_up_count = 0
            breakout_down_count = 0
            for period in self.scan_windows:
                required = period + 1 if has_current_bar else period
                if len(work) < required:
                    row["windows"].append(
                        {
                            "period": int(period),
                            "skip": "insufficient_rows",
                            "required_rows": int(required),
                            "available_rows": int(len(work)),
                        }
                    )
                    continue

                window = work.iloc[-(period + 1) : -1] if has_current_bar else work.iloc[-period:]
                box_high = _safe_float(window["high_price"].max(), 0.0)
                box_low = _safe_float(window["low_price"].min(), 0.0)
                width_atr = _safe_div(box_high - box_low, atr_ref)
                closes = [float(x) for x in window["close_price"].tolist()]
                slope_abs_atr = abs(_linear_slope(closes)) / max(atr_ref, 1e-9)
                up_break = realtime_price > box_high
                down_break = realtime_price < box_low
                up_distance = _safe_div(realtime_price - box_high, atr_ref)
                down_distance = _safe_div(box_low - realtime_price, atr_ref)

                win_info = {
                    "period": int(period),
                    "box_high": box_high,
                    "box_low": box_low,
                    "distance_atr_up": float(max(0.0, up_distance)),
                    "distance_atr_down": float(max(0.0, down_distance)),
                    "width_atr": float(width_atr),
                    "slope_abs_atr": float(slope_abs_atr),
                    "up_break": bool(up_break),
                    "down_break": bool(down_break),
                }
                row["windows"].append(win_info)
                window_infos.append(win_info)

                if up_break:
                    breakout_up_count += 1
                    seeds.append(
                        {
                            "symbol": symbol,
                            "symbol_name": row["symbol_name"],
                            "direction": "up",
                            "period": int(period),
                            "trigger_type": "box_breakout",
                            "distance_atr": float(max(0.0, up_distance)),
                            "width_atr": float(width_atr),
                            "slope_abs_atr": float(slope_abs_atr),
                            "pattern_score": 1.0 if rising_three else 0.0,
                            "pattern_hit": bool(rising_three),
                            "realtime_price": realtime_price,
                            "box_high": box_high,
                            "box_low": box_low,
                            "atr14": atr_ref,
                            "latest_close": latest_close,
                            "contract_code": row["contract_code"],
                            "contract_name": row["contract_name"],
                            "reason_prefilter": f"上破{period}日箱体",
                        }
                    )
                if down_break:
                    breakout_down_count += 1
                    seeds.append(
                        {
                            "symbol": symbol,
                            "symbol_name": row["symbol_name"],
                            "direction": "down",
                            "period": int(period),
                            "trigger_type": "box_breakout",
                            "distance_atr": float(max(0.0, down_distance)),
                            "width_atr": float(width_atr),
                            "slope_abs_atr": float(slope_abs_atr),
                            "pattern_score": 1.0 if falling_three else 0.0,
                            "pattern_hit": bool(falling_three),
                            "realtime_price": realtime_price,
                            "box_high": box_high,
                            "box_low": box_low,
                            "atr14": atr_ref,
                            "latest_close": latest_close,
                            "contract_code": row["contract_code"],
                            "contract_name": row["contract_name"],
                            "reason_prefilter": f"下破{period}日箱体",
                        }
                    )

            fallback_window = self._pick_window(window_infos, target_period=20)
            pattern_distance = _safe_div(abs(realtime_price - latest_close), atr_ref)
            if rising_three and breakout_up_count == 0 and fallback_window is not None:
                seeds.append(
                    {
                        "symbol": symbol,
                        "symbol_name": row["symbol_name"],
                        "direction": "up",
                        "period": int(fallback_window["period"]),
                        "trigger_type": "three_method",
                        "distance_atr": float(max(0.0, pattern_distance)),
                        "width_atr": float(fallback_window["width_atr"]),
                        "slope_abs_atr": float(fallback_window["slope_abs_atr"]),
                        "pattern_score": 1.0,
                        "pattern_hit": True,
                        "realtime_price": realtime_price,
                        "box_high": float(fallback_window["box_high"]),
                        "box_low": float(fallback_window["box_low"]),
                        "atr14": atr_ref,
                        "latest_close": latest_close,
                        "contract_code": row["contract_code"],
                        "contract_name": row["contract_name"],
                        "reason_prefilter": "上升三法触发",
                    }
                )
            if falling_three and breakout_down_count == 0 and fallback_window is not None:
                seeds.append(
                    {
                        "symbol": symbol,
                        "symbol_name": row["symbol_name"],
                        "direction": "down",
                        "period": int(fallback_window["period"]),
                        "trigger_type": "three_method",
                        "distance_atr": float(max(0.0, pattern_distance)),
                        "width_atr": float(fallback_window["width_atr"]),
                        "slope_abs_atr": float(fallback_window["slope_abs_atr"]),
                        "pattern_score": 1.0,
                        "pattern_hit": True,
                        "realtime_price": realtime_price,
                        "box_high": float(fallback_window["box_high"]),
                        "box_low": float(fallback_window["box_low"]),
                        "atr14": atr_ref,
                        "latest_close": latest_close,
                        "contract_code": row["contract_code"],
                        "contract_name": row["contract_name"],
                        "reason_prefilter": "下降三法触发",
                    }
                )

            if breakout_up_count == 0 and breakout_down_count == 0 and not rising_three and not falling_three:
                row["drop_reasons"].append("no_box_break_no_three_method")
            debug_rows.append(row)

        if not seeds:
            return {
                "candidate_count": 0,
                "candidates": [],
                "all_ranked": [],
                "ranking_table": [],
                "debug_rows": debug_rows if debug else [],
            }

        up_idxs = [i for i, x in enumerate(seeds) if str(x.get("direction")) == "up"]
        down_idxs = [i for i, x in enumerate(seeds) if str(x.get("direction")) == "down"]

        up_vals = [float(seeds[i]["distance_atr"]) for i in up_idxs]
        down_vals = [float(seeds[i]["distance_atr"]) for i in down_idxs]
        up_ranks = _rank_percentile(up_vals, higher_better=True)
        down_ranks = _rank_percentile(down_vals, higher_better=True)
        width_ranks = _rank_percentile([float(x.get("width_atr", 0.0)) for x in seeds], higher_better=False)
        slope_ranks = _rank_percentile([float(x.get("slope_abs_atr", 0.0)) for x in seeds], higher_better=False)

        up_map = {idx: up_ranks[k] for k, idx in enumerate(up_idxs)}
        down_map = {idx: down_ranks[k] for k, idx in enumerate(down_idxs)}

        scored: List[Dict[str, Any]] = []
        for i, item in enumerate(seeds):
            direction = str(item.get("direction", ""))
            distance_rank = up_map.get(i, 0.0) if direction == "up" else down_map.get(i, 0.0)
            range_quality = 0.5 * width_ranks[i] + 0.5 * slope_ranks[i]
            pattern_score = 1.0 if bool(item.get("pattern_score", 0.0)) else 0.0
            score = 0.65 * distance_rank + 0.25 * range_quality + 0.10 * pattern_score

            merged = dict(item)
            merged["distance_rank"] = float(distance_rank)
            merged["range_quality"] = float(range_quality)
            merged["pattern_score"] = float(pattern_score)
            merged["score"] = float(score)
            merged["strength"] = float(score)
            merged["strength_raw"] = float(item.get("distance_atr", 0.0))
            merged["atr_ratio"] = float(item.get("width_atr", 0.0))
            merged["reason_simple"] = str(item.get("reason_prefilter", ""))
            merged["summary_line"] = str(item.get("reason_prefilter", ""))
            scored.append(merged)

        dedup: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for item in scored:
            key = (str(item.get("symbol", "")), str(item.get("direction", "")))
            prev = dedup.get(key)
            if prev is None or float(item.get("score", 0.0)) > float(prev.get("score", 0.0)):
                dedup[key] = item

        ranked = sorted(dedup.values(), key=lambda x: float(x.get("score", 0.0)), reverse=True)
        top = ranked[: self.top_k]

        selected_keys = {(str(x.get("symbol", "")), str(x.get("direction", ""))) for x in top}
        for row in debug_rows:
            sym = str(row.get("symbol", ""))
            picks = [x for x in top if str(x.get("symbol", "")) == sym]
            if picks:
                row["selected"] = True
                row["selected_items"] = [
                    {
                        "direction": str(x.get("direction", "")),
                        "period": int(x.get("period", 0) or 0),
                        "score": float(x.get("score", 0.0) or 0.0),
                        "reason": str(x.get("reason_prefilter", "")),
                    }
                    for x in picks
                ]
            else:
                if not row.get("drop_reasons"):
                    if any((sym, d) in selected_keys for d in ("up", "down")):
                        pass
                    else:
                        row["drop_reasons"] = ["not_in_top_k"]

        ranking_table = [
            {
                "rank": i + 1,
                "symbol": str(x.get("symbol", "")),
                "symbol_name": str(x.get("symbol_name", "")),
                "direction": str(x.get("direction", "")),
                "period": int(x.get("period", 0) or 0),
                "score": round(float(x.get("score", 0.0)), 6),
                "distance_atr": round(float(x.get("distance_atr", 0.0)), 6),
                "range_quality": round(float(x.get("range_quality", 0.0)), 6),
                "pattern_score": round(float(x.get("pattern_score", 0.0)), 6),
                "trigger_type": str(x.get("trigger_type", "")),
                "contract_code": str(x.get("contract_code", "")),
            }
            for i, x in enumerate(ranked[:20])
        ]

        return {
            "candidate_count": len(ranked),
            "candidates": top,
            "all_ranked": ranked,
            "ranking_table": ranking_table,
            "debug_rows": debug_rows if debug else [],
        }
