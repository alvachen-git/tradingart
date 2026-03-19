#!/usr/bin/env python3
"""
14:25 期货突破提醒任务（两阶段提速版）
- 阶段1：形态+实时融合预筛（压缩箱体/上升三法/下降三法）
- 阶段2：LLM复核（仅候选）
- 推送：站内频道 + 邮件
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import text

try:
    from email_utils2 import send_email as send_email_html
except Exception:
    send_email_html = None

# 扫描与筛选参数
SCAN_PERIODS = (5, 10, 20, 30, 60)
HISTORY_BARS = 90
DEFAULT_MAX_CANDIDATES = 20
DEFAULT_MIN_CONFIDENCE = 0.60
DEFAULT_THRESHOLD_ATR = 0.30
DEFAULT_CHANNEL_CODE = "trade_signal"
DEFAULT_STATE_FILE = "/tmp/tradingart_breakout_state.json"
PATTERN_BONUS_PER_HIT = 0.10
MIN_CONSOLIDATION_BARS = 8
MAX_CONSOLIDATION_RANGE_ATR = 3.0
MAX_CONSOLIDATION_DRIFT_ATR = 0.5

BULLISH_PATTERN_KEYWORDS = (
    "平台突破",
    "上升三法",
)
BEARISH_PATTERN_KEYWORDS = (
    "平台跌破",
    "下降三法",
)


def _normalize_trade_date(value: Any) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    return digits[:8] if len(digits) >= 8 else ""


def _build_sina_session() -> requests.Session:
    session = requests.Session()
    session.trust_env = False
    session.headers.update(
        {
            "Referer": "http://finance.sina.com.cn/",
            "User-Agent": "Mozilla/5.0",
        }
    )
    return session


def _load_state(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_state(path: str, state: Dict[str, Any]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _signal_hash(trade_date: str, signals: List[Dict[str, Any]]) -> str:
    normalized = [
        {
            "symbol": str(x.get("symbol", "")),
            "direction": str(x.get("direction", "")),
            "period": int(x.get("period", 0)),
            "strength": round(float(x.get("strength", 0.0)), 4),
        }
        for x in sorted(signals, key=lambda v: (str(v.get("symbol", "")), str(v.get("direction", ""))))
    ]
    payload = json.dumps({"trade_date": trade_date, "signals": normalized}, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _split_sina_code(code: str) -> Tuple[str, bool]:
    upper_code = str(code or "").upper()
    is_fin = upper_code.startswith(("IF", "IH", "IC", "IM", "T", "TF", "TS"))
    return upper_code, is_fin


def _candidate_contracts(main_code: str, month_count: int = 14) -> List[str]:
    """为主力代码生成候选合约列表，按近月优先。"""
    code, is_fin = _split_sina_code(main_code)
    now = datetime.now()
    ym_list = []
    y = now.year
    m = now.month

    # 先从当月开始，覆盖未来月份，最后再补上月兜底，避免优先命中过期月。
    for i in range(0, month_count):
        nm = m + i
        ny = y
        while nm <= 0:
            nm += 12
            ny -= 1
        while nm > 12:
            nm -= 12
            ny += 1
        ym_list.append(f"{ny % 100:02d}{nm:02d}")
    prev_month = m - 1
    prev_year = y
    if prev_month <= 0:
        prev_month += 12
        prev_year -= 1
    ym_list.append(f"{prev_year % 100:02d}{prev_month:02d}")

    contracts = [f"{code}{ym}" for ym in ym_list]
    if is_fin:
        return [f"CFF_RE_{c}" for c in contracts]
    return [f"nf_{c.lower()}" for c in contracts]


def _extract_quote_date(content: str) -> str:
    m = re.search(r"(20\d{2}-\d{2}-\d{2})", str(content or ""))
    if m:
        return _normalize_trade_date(m.group(1))
    m2 = re.search(r"\b(20\d{6})\b", str(content or ""))
    if m2:
        return _normalize_trade_date(m2.group(1))
    return ""


def _contract_to_sina_code(contract_code: str) -> str:
    code = re.sub(r"[^A-Z0-9]", "", str(contract_code or "").upper())
    if not code:
        return ""
    _, is_fin = _split_sina_code(code)
    return f"CFF_RE_{code}" if is_fin else f"nf_{code.lower()}"


def _sina_to_contract_code(sina_code: str) -> str:
    return str(sina_code or "").replace("nf_", "").replace("CFF_RE_", "").upper().strip()


def _fetch_minute_close_fallback(contract_code: str) -> Optional[Dict[str, Any]]:
    """
    商品期货快照为空时的兜底行情：
    - 使用 akshare 的 5 分钟数据最后一根 close 作为实时价。
    - 金融期货在该接口稳定性较差，跳过该兜底。
    """
    code = re.sub(r"[^A-Z0-9]", "", str(contract_code or "").upper())
    if not code:
        return None
    _, is_fin = _split_sina_code(code)
    if is_fin:
        return None

    try:
        import akshare as ak

        df = ak.futures_zh_minute_sina(symbol=code.lower(), period="5")
        if df is None or df.empty:
            return None
        last = df.iloc[-1]
        price = float(last.get("close", 0.0) or 0.0)
        if price <= 0:
            return None
        quote_date = _normalize_trade_date(last.get("datetime", ""))
        return {
            "price": price,
            "quote_date": quote_date,
            "name": code,
            "contract_code": code,
            "source": "ak_minute",
        }
    except Exception:
        return None


def _parse_price_from_sina_line(sina_code: str, content: str) -> Optional[Tuple[str, float, str]]:
    parts = content.split(",")
    if not parts or len(parts) < 2:
        return None
    name = parts[0].strip()
    price = 0.0
    try:
        if "CFF_RE_" in sina_code:
            if len(parts) > 3:
                price = float(parts[3])
        else:
            if len(parts) > 8:
                price = float(parts[8])
            if price <= 0 and len(parts) > 6:
                price = float(parts[6])
    except Exception:
        return None
    if price <= 0:
        return None
    quote_date = _extract_quote_date(content)
    return name, price, quote_date


def _load_main_contract_map(
    engine: Any,
    latest_trade_date: str,
    symbols: List[str],
) -> Dict[str, str]:
    if not symbols:
        return {}
    sql = text(
        """
        SELECT UPPER(SUBSTRING_INDEX(ts_code, '.', 1)) AS contract_code, oi
        FROM futures_price
        WHERE trade_date = :trade_date
          AND ts_code NOT LIKE '%TAS%'
          AND UPPER(ts_code) REGEXP '^[A-Z]{1,4}[0-9]{3,4}(\\.[A-Z]+)?$'
        """
    )
    try:
        df = pd.read_sql(sql, engine, params={"trade_date": latest_trade_date})
    except Exception:
        return {}
    if df.empty:
        return {}

    target = {str(x).upper() for x in symbols}
    best: Dict[str, Tuple[str, float]] = {}
    for _, row in df.iterrows():
        contract = str(row.get("contract_code", "")).upper().strip()
        m = re.match(r"^([A-Z]{1,4})(\d{3,4})$", contract)
        if not m:
            continue
        symbol = m.group(1)
        if symbol not in target:
            continue
        try:
            oi = float(row.get("oi", 0.0) or 0.0)
        except Exception:
            oi = 0.0
        prev = best.get(symbol)
        if prev is None or oi > prev[1]:
            best[symbol] = (contract, oi)
    return {k: v[0] for k, v in best.items()}


def fetch_realtime_prices(
    main_codes: List[str],
    preferred_contracts: Optional[Dict[str, str]] = None,
    target_trade_date: str = "",
) -> Dict[str, Optional[Dict[str, Any]]]:
    result: Dict[str, Optional[Dict[str, Any]]] = {c: None for c in main_codes}
    if not main_codes:
        return result

    pref = {str(k).upper(): str(v).upper() for k, v in (preferred_contracts or {}).items()}
    task_map: Dict[str, List[str]] = {}
    for c in main_codes:
        candidates = _candidate_contracts(c)
        preferred = _contract_to_sina_code(pref.get(c.upper(), ""))
        if preferred:
            # 主力月优先，再回退候选月
            candidates = [preferred] + [x for x in candidates if x != preferred]
        task_map[c] = candidates
    all_sina_codes = sorted({item for candidates in task_map.values() for item in candidates})

    if not all_sina_codes:
        return result

    session = _build_sina_session()
    price_cache: Dict[str, Dict[str, Any]] = {}
    batch_size = 50

    for i in range(0, len(all_sina_codes), batch_size):
        chunk = all_sina_codes[i : i + batch_size]
        url = f"http://hq.sinajs.cn/list={','.join(chunk)}"
        try:
            resp = session.get(url, timeout=2)
            lines = resp.text.split(";")
        except Exception as exc:
            print(f"[realtime] batch request failed: {exc}")
            continue

        for line in lines:
            if '="' not in line:
                continue
            left, right = line.split('="', 1)
            sina_code = left.split("hq_str_")[-1].strip()
            content = right.strip().strip('"')
            if not content:
                continue
            parsed = _parse_price_from_sina_line(sina_code, content)
            if not parsed:
                continue
            name, price, quote_date = parsed
            price_cache[sina_code] = {
                "name": name,
                "price": price,
                "sina_code": sina_code,
                "contract_code": sina_code.replace("nf_", "").replace("CFF_RE_", "").upper(),
                "quote_date": quote_date,
            }

    for code, candidates in task_map.items():
        selected: Optional[Dict[str, Any]] = None
        for c in candidates:
            if c in price_cache:
                quote_date = str(price_cache[c].get("quote_date", "")).strip()
                if target_trade_date and quote_date and quote_date != target_trade_date:
                    continue
                selected = dict(price_cache[c])
                break
        if selected is not None:
            result[code] = selected
            continue

        # 新浪快照为空时，对商品主力合约做分钟线兜底
        fallback_contracts: List[str] = []
        preferred_contract = pref.get(code.upper(), "")
        if preferred_contract:
            fallback_contracts.append(preferred_contract)
        fallback_contracts.extend([_sina_to_contract_code(x) for x in candidates])
        seen = set()
        for contract in fallback_contracts:
            if not contract or contract in seen:
                continue
            seen.add(contract)
            fb = _fetch_minute_close_fallback(contract)
            if not fb:
                continue
            quote_date = str(fb.get("quote_date", "")).strip()
            if target_trade_date and quote_date and quote_date != target_trade_date:
                continue
            result[code] = {
                "name": str(fb.get("name", contract)),
                "price": float(fb.get("price", 0.0)),
                "sina_code": "",
                "contract_code": contract,
                "quote_date": quote_date,
                "source": str(fb.get("source", "ak_minute")),
            }
            break

    return result


def _fetch_history_df(engine: Any, symbol: str, end_date: str, bars: int = HISTORY_BARS) -> pd.DataFrame:
    sql = text(
        """
        SELECT trade_date, open_price, high_price, low_price, close_price
        FROM futures_price
        WHERE UPPER(ts_code) = :code
          AND trade_date <= :end_date
        ORDER BY trade_date DESC
        LIMIT :bars
        """
    )
    df = pd.read_sql(sql, engine, params={"code": symbol.upper(), "end_date": end_date, "bars": bars})
    if df.empty:
        return df
    df = df.sort_values("trade_date").reset_index(drop=True)
    for col in ["open_price", "high_price", "low_price", "close_price"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["open_price", "high_price", "low_price", "close_price"]).reset_index(drop=True)
    return df


def _calc_atr14(df: pd.DataFrame) -> pd.Series:
    work = df.copy()
    work["h_l"] = work["high_price"] - work["low_price"]
    work["h_pc"] = (work["high_price"] - work["close_price"].shift(1)).abs()
    work["l_pc"] = (work["low_price"] - work["close_price"].shift(1)).abs()
    work["tr"] = work[["h_l", "h_pc", "l_pc"]].max(axis=1)
    return work["tr"].rolling(window=14).mean()


def _max_atr_multiple(period: int) -> float:
    if period <= 5:
        return 2.5
    if period <= 10:
        return 4.0
    if period <= 20:
        return 6.0
    return 10.0


def _calc_consolidation_gate(df: pd.DataFrame, atr_latest: float, bars: int = MIN_CONSOLIDATION_BARS) -> Dict[str, Any]:
    """
    横盘硬约束：至少 bars 根K线，且区间宽度与漂移都受 ATR 约束。
    """
    if df.empty or atr_latest <= 0 or len(df) < bars:
        return {
            "ok": False,
            "bars": bars,
            "range_atr": 999.0,
            "drift_atr": 999.0,
        }

    window = df.iloc[-bars:]
    box_high = float(window["high_price"].max())
    box_low = float(window["low_price"].min())
    box_range = max(0.0, box_high - box_low)

    first_high = float(window["high_price"].iloc[0])
    last_high = float(window["high_price"].iloc[-1])
    first_low = float(window["low_price"].iloc[0])
    last_low = float(window["low_price"].iloc[-1])
    drift = max(abs(last_high - first_high), abs(last_low - first_low))

    range_atr = box_range / atr_latest
    drift_atr = drift / atr_latest
    ok = range_atr <= MAX_CONSOLIDATION_RANGE_ATR and drift_atr <= MAX_CONSOLIDATION_DRIFT_ATR
    return {
        "ok": bool(ok),
        "bars": bars,
        "range_atr": float(range_atr),
        "drift_atr": float(drift_atr),
    }


def _calc_kline_signals(df: pd.DataFrame) -> Dict[str, Any]:
    """
    复用既有 K 线算法（kline_algo.py），避免重写形态识别。
    """
    try:
        from kline_algo import calculate_kline_signals

        use_df = df[["open_price", "high_price", "low_price", "close_price"]].copy()
        return calculate_kline_signals(use_df)
    except Exception:
        return {"patterns": [], "trends": [], "score": 50}


def _build_intraday_kline_df(df: pd.DataFrame, realtime_price: float, trade_date: str) -> pd.DataFrame:
    """
    将实时价融合进日线序列：
    - 若最后一根就是目标交易日：更新该根 close/high/low。
    - 否则追加一根“盘中临时K”（open=昨收，close=实时价）。
    """
    work = df.copy().reset_index(drop=True)
    if work.empty:
        return work

    last_idx = len(work) - 1
    last_row = work.iloc[last_idx]
    last_close = float(last_row["close_price"])
    last_trade_date = _normalize_trade_date(last_row["trade_date"])

    if last_trade_date == trade_date:
        open_price = float(last_row["open_price"])
        high_price = max(float(last_row["high_price"]), realtime_price, open_price)
        low_price = min(float(last_row["low_price"]), realtime_price, open_price)
        work.at[last_idx, "open_price"] = open_price
        work.at[last_idx, "high_price"] = high_price
        work.at[last_idx, "low_price"] = low_price
        work.at[last_idx, "close_price"] = realtime_price
        return work

    open_price = last_close
    high_price = max(open_price, realtime_price)
    low_price = min(open_price, realtime_price)
    append_row = pd.DataFrame(
        [
            {
                "trade_date": trade_date,
                "open_price": open_price,
                "high_price": high_price,
                "low_price": low_price,
                "close_price": realtime_price,
            }
        ]
    )
    return pd.concat([work, append_row], ignore_index=True)


def _extract_pattern_signal(patterns: List[str]) -> Dict[str, Any]:
    up_hits = [p for p in patterns if any(k in p for k in BULLISH_PATTERN_KEYWORDS)]
    down_hits = [p for p in patterns if any(k in p for k in BEARISH_PATTERN_KEYWORDS)]
    up_platform_hits = [p for p in up_hits if "平台突破" in p]
    down_platform_hits = [p for p in down_hits if "平台跌破" in p]
    up_three_hits = [p for p in up_hits if "上升三法" in p]
    down_three_hits = [p for p in down_hits if "下降三法" in p]

    # 陷阱形态做方向否决
    veto_up = any("假突破" in p for p in patterns)
    veto_down = any("假跌破" in p for p in patterns)

    if len(up_hits) > len(down_hits):
        direction = "up"
    elif len(down_hits) > len(up_hits):
        direction = "down"
    else:
        direction = "none"

    return {
        "direction": direction,
        "up_hits": up_hits,
        "down_hits": down_hits,
        "up_platform_hits": up_platform_hits,
        "down_platform_hits": down_platform_hits,
        "up_three_hits": up_three_hits,
        "down_three_hits": down_three_hits,
        "veto_up": veto_up,
        "veto_down": veto_down,
    }


def build_prefilter_candidates(
    history_map: Dict[str, pd.DataFrame],
    realtime_map: Dict[str, Optional[Dict[str, Any]]],
    threshold_atr: float = DEFAULT_THRESHOLD_ATR,
    max_candidates: int = DEFAULT_MAX_CANDIDATES,
    trade_date: str = "",
) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []

    for symbol, df in history_map.items():
        rt_data = realtime_map.get(symbol)
        if not rt_data:
            continue
        if df.empty or len(df) < max(SCAN_PERIODS):
            continue

        atr_series = _calc_atr14(df)
        latest_trade_date = trade_date or _normalize_trade_date(df.iloc[-1]["trade_date"])
        has_current_bar = _normalize_trade_date(df.iloc[-1]["trade_date"]) == latest_trade_date
        if atr_series.empty:
            continue
        atr_ref_idx = -2 if has_current_bar and len(atr_series) >= 2 else -1
        atr_latest = float(atr_series.iloc[atr_ref_idx]) if pd.notna(atr_series.iloc[atr_ref_idx]) else 0.0
        if atr_latest <= 0:
            continue

        realtime_price = float(rt_data["price"])
        latest_close = float(df.iloc[-1]["close_price"])

        # 1) 先复用既有K线形态识别（融合实时价后的临时K）
        intraday_df = _build_intraday_kline_df(df, realtime_price=realtime_price, trade_date=latest_trade_date)
        kline_result = _calc_kline_signals(intraday_df)
        patterns = [str(x) for x in kline_result.get("patterns", []) if str(x).strip()]
        pattern_sig = _extract_pattern_signal(patterns)

        # 2) 再做“压缩箱体 + 实时越界”判定，避免把普通波动当突破
        consolidation = _calc_consolidation_gate(df, atr_latest=atr_latest, bars=MIN_CONSOLIDATION_BARS)
        best_up: Optional[Dict[str, Any]] = None
        best_down: Optional[Dict[str, Any]] = None
        best_up_platform: Optional[Dict[str, Any]] = None
        best_down_platform: Optional[Dict[str, Any]] = None
        for period in SCAN_PERIODS:
            required = period + 1 if has_current_bar else period
            if len(df) < required:
                continue
            window = df.iloc[-(period + 1) : -1] if has_current_bar else df.iloc[-period:]
            box_high = float(window["high_price"].max())
            box_low = float(window["low_price"].min())
            box_height = box_high - box_low
            atr_ratio = box_height / atr_latest if atr_latest > 0 else 999.0

            # 与 kline_tools 的动态箱体定义一致
            if atr_ratio > _max_atr_multiple(period):
                continue

            up_strength = (realtime_price - box_high) / atr_latest
            down_strength = (box_low - realtime_price) / atr_latest

            up_item = {
                "symbol": symbol,
                "direction": "up",
                "period": period,
                "strength_raw": float(up_strength),
                "realtime_price": realtime_price,
                "box_high": box_high,
                "box_low": box_low,
                "atr14": atr_latest,
                "atr_ratio": float(atr_ratio),
                "latest_close": latest_close,
                "latest_trade_date": latest_trade_date,
                "contract_code": str(rt_data.get("contract_code", "")),
                "contract_name": str(rt_data.get("name", "")),
                "patterns": patterns,
                "consolidation_ok": bool(consolidation["ok"]),
                "consolidation_bars": int(consolidation["bars"]),
                "consolidation_range_atr": float(consolidation["range_atr"]),
                "consolidation_drift_atr": float(consolidation["drift_atr"]),
            }
            down_item = dict(up_item)
            down_item["direction"] = "down"
            down_item["strength_raw"] = float(down_strength)

            if best_up is None or up_item["strength_raw"] > float(best_up["strength_raw"]):
                best_up = up_item
            if best_down is None or down_item["strength_raw"] > float(best_down["strength_raw"]):
                best_down = down_item
            if period >= MIN_CONSOLIDATION_BARS:
                if best_up_platform is None or up_item["strength_raw"] > float(best_up_platform["strength_raw"]):
                    best_up_platform = up_item
                if best_down_platform is None or down_item["strength_raw"] > float(best_down_platform["strength_raw"]):
                    best_down_platform = down_item

        options: List[Dict[str, Any]] = []
        if (
            best_up is not None
            and float(best_up["strength_raw"]) >= threshold_atr
            and bool(pattern_sig["up_three_hits"])
            and not bool(pattern_sig["veto_up"])
            and str(pattern_sig["direction"]) != "down"
        ):
            up_hits = list(pattern_sig["up_hits"])
            bonus = PATTERN_BONUS_PER_HIT * min(3, len(up_hits))
            item = dict(best_up)
            item["strength"] = float(item["strength_raw"]) + bonus
            item["pattern_hits"] = up_hits[:3]
            trigger_text = "上升三法"
            item["reason_prefilter"] = (
                f"{item['period']}日压缩箱体上破(压缩比{float(item['atr_ratio']):.1f}ATR, {trigger_text})"
            )
            options.append(item)
        if (
            best_up_platform is not None
            and float(best_up_platform["strength_raw"]) >= threshold_atr
            and bool(pattern_sig["up_platform_hits"])
            and not bool(pattern_sig["veto_up"])
            and str(pattern_sig["direction"]) != "down"
        ):
            up_hits = list(pattern_sig["up_hits"])
            bonus = PATTERN_BONUS_PER_HIT * min(3, len(up_hits))
            item = dict(best_up_platform)
            item["strength"] = float(item["strength_raw"]) + bonus
            item["pattern_hits"] = up_hits[:3]
            trigger_text = (
                f"平台突破+{int(consolidation['bars'])}bar观察"
                f"(区间{float(consolidation['range_atr']):.2f}ATR)"
            )
            item["reason_prefilter"] = (
                f"{item['period']}日压缩箱体上破(压缩比{float(item['atr_ratio']):.1f}ATR, {trigger_text})"
            )
            options.append(item)

        if (
            best_down is not None
            and float(best_down["strength_raw"]) >= threshold_atr
            and bool(pattern_sig["down_three_hits"])
            and not bool(pattern_sig["veto_down"])
            and str(pattern_sig["direction"]) != "up"
        ):
            down_hits = list(pattern_sig["down_hits"])
            bonus = PATTERN_BONUS_PER_HIT * min(3, len(down_hits))
            item = dict(best_down)
            item["strength"] = float(item["strength_raw"]) + bonus
            item["pattern_hits"] = down_hits[:3]
            trigger_text = "下降三法"
            item["reason_prefilter"] = (
                f"{item['period']}日压缩箱体下破(压缩比{float(item['atr_ratio']):.1f}ATR, {trigger_text})"
            )
            options.append(item)
        if (
            best_down_platform is not None
            and float(best_down_platform["strength_raw"]) >= threshold_atr
            and bool(pattern_sig["down_platform_hits"])
            and not bool(pattern_sig["veto_down"])
            and str(pattern_sig["direction"]) != "up"
        ):
            down_hits = list(pattern_sig["down_hits"])
            bonus = PATTERN_BONUS_PER_HIT * min(3, len(down_hits))
            item = dict(best_down_platform)
            item["strength"] = float(item["strength_raw"]) + bonus
            item["pattern_hits"] = down_hits[:3]
            trigger_text = (
                f"平台跌破+{int(consolidation['bars'])}bar观察"
                f"(区间{float(consolidation['range_atr']):.2f}ATR)"
            )
            item["reason_prefilter"] = (
                f"{item['period']}日压缩箱体下破(压缩比{float(item['atr_ratio']):.1f}ATR, {trigger_text})"
            )
            options.append(item)

        if options:
            options.sort(key=lambda x: float(x.get("strength", 0.0)), reverse=True)
            candidates.append(options[0])

    candidates.sort(key=lambda x: float(x.get("strength", 0.0)), reverse=True)
    return candidates[: max(0, int(max_candidates))]


def _extract_json_blob(raw_text: str) -> str:
    text_msg = str(raw_text or "").strip()
    if not text_msg:
        return ""

    if text_msg.startswith("```"):
        text_msg = re.sub(r"^```(?:json)?\s*", "", text_msg)
        text_msg = re.sub(r"\s*```$", "", text_msg)

    if text_msg.startswith("[") or text_msg.startswith("{"):
        return text_msg

    m = re.search(r"(\[.*\])", text_msg, flags=re.S)
    if m:
        return m.group(1)
    m = re.search(r"(\{.*\})", text_msg, flags=re.S)
    if m:
        return m.group(1)
    return ""


def parse_llm_json(raw_text: str) -> List[Dict[str, Any]]:
    blob = _extract_json_blob(raw_text)
    if not blob:
        return []
    try:
        data = json.loads(blob)
    except Exception:
        return []

    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list):
        return []

    parsed: List[Dict[str, Any]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol", "")).upper().strip()
        direction = str(item.get("direction", "none")).lower().strip()
        is_breakout = item.get("is_breakout", False)
        reason_simple = str(item.get("reason_simple", "")).strip()
        try:
            confidence = float(item.get("confidence", 0.0))
        except Exception:
            confidence = 0.0

        parsed.append(
            {
                "symbol": symbol,
                "direction": direction,
                "is_breakout": _to_bool(is_breakout),
                "reason_simple": reason_simple,
                "confidence": confidence,
            }
        )
    return parsed


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0

    text_msg = str(value).strip().lower()
    if text_msg in {"1", "true", "yes", "y", "on"}:
        return True
    if text_msg in {"0", "false", "no", "n", "off", "", "none", "null"}:
        return False
    return False


def llm_review_candidates(
    candidates: List[Dict[str, Any]],
    model_name: str,
    min_confidence: float,
) -> Tuple[List[Dict[str, Any]], str]:
    if not candidates:
        return [], ""

    api_key = os.getenv("DASHSCOPE_API_KEY", "").strip()
    if not api_key:
        return [], "missing DASHSCOPE_API_KEY"

    from llm_compat import ChatTongyiCompat

    llm = ChatTongyiCompat(model=model_name, api_key=api_key)
    compact_input = [
        {
            "symbol": c["symbol"],
            "direction_rule": c["direction"],
            "period": int(c["period"]),
            "strength": round(float(c["strength"]), 4),
            "strength_raw": round(float(c.get("strength_raw", c["strength"])), 4),
            "realtime_price": round(float(c["realtime_price"]), 4),
            "box_high": round(float(c["box_high"]), 4),
            "box_low": round(float(c["box_low"]), 4),
            "atr14": round(float(c["atr14"]), 4),
            "atr_ratio": round(float(c.get("atr_ratio", 0.0)), 4),
            "latest_close": round(float(c["latest_close"]), 4),
            "pattern_hits": c.get("pattern_hits", []),
            "reason_prefilter": c.get("reason_prefilter", ""),
        }
        for c in candidates
    ]

    prompt = f"""
你是期货技术面突破审核器。请对每个候选做是否突破的判断。

候选列表（JSON）：
{json.dumps(compact_input, ensure_ascii=False)}

输出要求：
1. 只输出 JSON 数组，不要 markdown，不要额外文字。
2. 每个元素字段必须是：
   - symbol: 字符串（如 RB）
   - direction: up/down/none
   - is_breakout: true/false
   - reason_simple: 一句话中文说明（不超过30字）
   - confidence: 0~1 数字
3. 若你不确认，direction=none 且 is_breakout=false。
""".strip()

    try:
        resp = llm.invoke(prompt)
        content = getattr(resp, "content", str(resp))
    except Exception as exc:
        return [], f"llm invoke failed: {exc}"

    parsed = parse_llm_json(str(content))
    if not parsed:
        return [], "llm output parse failed"

    by_symbol = {str(x.get("symbol", "")).upper(): x for x in parsed}
    approved: List[Dict[str, Any]] = []

    for c in candidates:
        symbol = str(c["symbol"]).upper()
        p = by_symbol.get(symbol)
        if not p:
            continue
        conf = float(p.get("confidence", 0.0))
        if not bool(p.get("is_breakout", False)):
            continue
        if conf < float(min_confidence):
            continue

        direction = str(p.get("direction", "none")).lower()
        if direction not in {"up", "down"}:
            continue

        merged = dict(c)
        merged["direction"] = direction
        merged["reason_simple"] = str(p.get("reason_simple", "")).strip() or "触发突破条件"
        merged["confidence"] = conf
        approved.append(merged)

    approved.sort(key=lambda x: float(x.get("strength", 0.0)), reverse=True)
    return approved, ""


def group_signals(signals: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped = {"up": [], "down": []}
    for s in signals:
        d = str(s.get("direction", "")).lower()
        if d in grouped:
            grouped[d].append(s)
    grouped["up"].sort(key=lambda x: float(x.get("strength", 0.0)), reverse=True)
    grouped["down"].sort(key=lambda x: float(x.get("strength", 0.0)), reverse=True)
    return grouped


def compose_grouped_summary(
    trade_date: str,
    scan_count: int,
    candidate_count: int,
    signals: List[Dict[str, Any]],
) -> str:
    grouped = group_signals(signals)
    header = (
        f"【14:25 技术突破提醒】{trade_date}\n"
        f"扫描品种: {scan_count} | 预筛候选: {candidate_count} | LLM确认: {len(signals)}"
    )

    if not signals:
        return header + "\n\n未发现有效突破信号。"

    lines = [header, "", "【上破组】"]
    if grouped["up"]:
        for x in grouped["up"]:
            lines.append(
                f"- {x['symbol']}({x.get('contract_code','')}) 现价{float(x['realtime_price']):.2f} "
                f"上破{x['period']}日箱体 | {x.get('reason_simple','')}"
            )
    else:
        lines.append("- 无")

    lines.append("")
    lines.append("【下破组】")
    if grouped["down"]:
        for x in grouped["down"]:
            lines.append(
                f"- {x['symbol']}({x.get('contract_code','')}) 现价{float(x['realtime_price']):.2f} "
                f"下破{x['period']}日箱体 | {x.get('reason_simple','')}"
            )
    else:
        lines.append("- 无")

    return "\n".join(lines)


def compose_grouped_brief(
    trade_date: str,
    scan_count: int,
    candidate_count: int,
    signals: List[Dict[str, Any]],
) -> str:
    grouped = group_signals(signals)
    return (
        f"{trade_date} 14:25 扫描{scan_count} | 预筛{candidate_count} | "
        f"确认{len(signals)} | 上破{len(grouped['up'])} | 下破{len(grouped['down'])}"
    )


def compose_grouped_html(
    trade_date: str,
    scan_count: int,
    candidate_count: int,
    signals: List[Dict[str, Any]],
) -> str:
    grouped = group_signals(signals)

    def _render_group(label: str, icon: str, color: str, items: List[Dict[str, Any]]) -> str:
        title_html = (
            f"<div style='font-size:18px;font-weight:700;margin:16px 0 10px 0;color:{color};'>"
            f"{icon} {label}（{len(items)}）</div>"
        )
        if not items:
            return title_html + "<div style='color:#9fb0c8;font-size:15px;padding:8px 2px;'>- 无</div>"

        rows: List[str] = []
        for x in items:
            symbol = html.escape(str(x.get("symbol", "")))
            contract = html.escape(str(x.get("contract_code", "")))
            reason = html.escape(str(x.get("reason_simple", "")))
            period = int(x.get("period", 0))
            price = float(x.get("realtime_price", 0.0))
            strength = float(x.get("strength_raw", x.get("strength", 0.0)))
            atr_ratio = float(x.get("atr_ratio", 0.0))
            rows.append(
                (
                    "<div style='margin:10px 0;padding:12px 14px;border:1px solid rgba(255,255,255,0.12);"
                    "border-radius:10px;background:rgba(255,255,255,0.03);'>"
                    f"<div style='font-size:16px;font-weight:700;color:#eef6ff;'>{symbol}({contract})</div>"
                    f"<div style='margin-top:6px;color:#cde0ff;font-size:14px;line-height:1.7;'>"
                    f"现价：<b>{price:.2f}</b> ｜ 箱体：{period}日 ｜ 越界强度：{strength:.2f} ATR ｜ 压缩比：{atr_ratio:.1f} ATR"
                    "</div>"
                    f"<div style='margin-top:6px;color:#f7fbff;font-size:15px;line-height:1.7;word-break:break-word;'>"
                    f"说明：{reason}"
                    "</div>"
                    "</div>"
                )
            )
        return title_html + "".join(rows)

    header_html = (
        "<div style='font-family:PingFang SC,Microsoft YaHei,Arial,sans-serif;"
        "color:#eaf3ff;line-height:1.7;padding:14px 16px;border-radius:12px;"
        "background:linear-gradient(135deg, rgba(20,36,64,0.96), rgba(8,18,38,0.96));"
        "border:1px solid rgba(255,255,255,0.12);'>"
        f"<div style='font-size:22px;font-weight:800;color:#ffe082;'>⚡ 14:25 技术突破提醒</div>"
        f"<div style='margin-top:4px;font-size:15px;color:#b8cced;'>交易日：{html.escape(trade_date)}</div>"
        f"<div style='margin-top:10px;font-size:15px;color:#d6e6ff;'>"
        f"扫描品种：<b>{scan_count}</b> ｜ 预筛候选：<b>{candidate_count}</b> ｜ LLM确认：<b>{len(signals)}</b>"
        "</div>"
    )

    if not signals:
        body_html = (
            "<div style='margin-top:16px;padding:14px;border-radius:10px;"
            "background:rgba(255,255,255,0.04);font-size:15px;color:#d8e7ff;'>"
            "今日未发现有效突破信号。"
            "</div>"
        )
        return header_html + body_html + "</div>"

    up_html = _render_group("上破组", "📈", "#76e4a8", grouped["up"])
    down_html = _render_group("下破组", "📉", "#ff9b9b", grouped["down"])
    return header_html + up_html + down_html + "</div>"


def _publish_station(
    channel_code: str,
    title: str,
    content: str,
    dry_run: bool,
    summary: str = "",
) -> Tuple[bool, str]:
    if dry_run:
        return True, "dry-run"

    import subscription_service as sub_svc

    ok, result = sub_svc.publish_content(
        channel_code=channel_code,
        title=title,
        content=content,
        summary=summary or title,
    )
    return bool(ok), str(result)


def _parse_email_recipients(raw_value: str) -> List[str]:
    raw = str(raw_value or "").replace(";", ",")
    items = [x.strip() for x in raw.split(",")]
    return [x for x in items if x]


def _send_email(recipients: List[str], subject: str, text_msg: str, dry_run: bool) -> Tuple[bool, str]:
    recipients = [x.strip() for x in (recipients or []) if str(x).strip()]
    if not recipients:
        return False, "email empty"
    if send_email_html is None:
        return False, "email sender unavailable"
    if dry_run:
        return True, f"dry-run recipients={len(recipients)}"

    html = f"<pre style='font-family:monospace'>{text_msg}</pre>"
    ok_count = 0
    for addr in recipients:
        try:
            if bool(send_email_html(addr, subject, html)):
                ok_count += 1
        except Exception:
            continue

    if ok_count <= 0:
        return False, f"all failed recipients={len(recipients)}"
    if ok_count < len(recipients):
        return True, f"partial success {ok_count}/{len(recipients)}"
    return True, f"success {ok_count}/{len(recipients)}"


def _resolve_email_recipients(channel_code: str, email_to_override: str = "") -> List[str]:
    manual = _parse_email_recipients(email_to_override)
    if manual:
        return sorted(set(manual))

    env_raw = os.getenv("BREAKOUT_ALERT_EMAIL_TO", "").strip() or os.getenv("ALERT_EMAIL_TO", "").strip()
    env_recipients = _parse_email_recipients(env_raw)
    if env_recipients:
        return sorted(set(env_recipients))

    try:
        import subscription_service as sub_svc

        rows = sub_svc.get_channel_email_subscribers(channel_code=channel_code)
        db_emails = [str(x.get("email", "")).strip() for x in rows if isinstance(x, dict)]
        return sorted(set([x for x in db_emails if x]))
    except Exception:
        return []


def _load_scan_symbols(
    engine: Any,
    latest_trade_date: str,
    product_codes: List[str],
    symbols_filter: Optional[List[str]],
    limit: int,
) -> List[str]:
    sql = text(
        """
        SELECT DISTINCT UPPER(ts_code) AS code
        FROM futures_price
        WHERE trade_date = :trade_date
          AND UPPER(ts_code) REGEXP '^[A-Z]{1,4}$'
        ORDER BY code
        """
    )
    df = pd.read_sql(sql, engine, params={"trade_date": latest_trade_date})
    if df.empty:
        return []

    available = [str(x).upper() for x in df["code"].tolist()]
    available_set = set(available)
    universe = sorted([x for x in product_codes if x in available_set])

    if symbols_filter:
        flt_set = {s.upper() for s in symbols_filter}
        universe = [x for x in universe if x in flt_set]

    if limit > 0:
        universe = universe[:limit]

    return universe


def _parse_symbols_arg(symbols_arg: str) -> List[str]:
    raw = [x.strip().upper() for x in str(symbols_arg or "").split(",")]
    return [x for x in raw if x]


def run_job(
    trade_date_arg: str,
    limit: int,
    symbols_arg: str,
    dry_run: bool,
) -> Dict[str, Any]:
    load_dotenv(override=True)

    from data_engine import PRODUCT_MAP, engine, get_latest_data_date

    now = datetime.now()
    today = now.strftime("%Y%m%d")
    latest = _normalize_trade_date(get_latest_data_date())
    target_trade_date = _normalize_trade_date(trade_date_arg) if trade_date_arg else latest

    if not latest:
        return {"status": "error", "error": "latest trading date unavailable"}

    if not trade_date_arg and latest != today:
        return {
            "status": "skipped",
            "reason": f"non-trading-day-or-data-not-ready today={today} latest={latest}",
            "trade_date": latest,
        }

    if trade_date_arg and target_trade_date != latest:
        return {
            "status": "skipped",
            "reason": f"trade_date_mismatch target={target_trade_date} latest={latest}",
            "trade_date": latest,
        }

    product_codes = sorted([str(k).upper() for k in PRODUCT_MAP.keys()])
    symbols_filter = _parse_symbols_arg(symbols_arg)

    universe = _load_scan_symbols(
        engine=engine,
        latest_trade_date=latest,
        product_codes=product_codes,
        symbols_filter=symbols_filter,
        limit=limit,
    )

    if not universe:
        return {
            "status": "skipped",
            "reason": "no symbols to scan",
            "trade_date": latest,
        }

    main_contract_map = _load_main_contract_map(engine=engine, latest_trade_date=latest, symbols=universe)
    realtime_map = fetch_realtime_prices(
        universe,
        preferred_contracts=main_contract_map,
        target_trade_date=latest,
    )

    history_map: Dict[str, pd.DataFrame] = {}
    for symbol in universe:
        df = _fetch_history_df(engine, symbol, latest, bars=HISTORY_BARS)
        if not df.empty:
            history_map[symbol] = df

    max_candidates = int(os.getenv("BREAKOUT_MAX_CANDIDATES", str(DEFAULT_MAX_CANDIDATES)))
    min_confidence = float(os.getenv("BREAKOUT_MIN_CONFIDENCE", str(DEFAULT_MIN_CONFIDENCE)))

    threshold_atr = float(os.getenv("BREAKOUT_THRESHOLD_ATR", str(DEFAULT_THRESHOLD_ATR)))
    candidates = build_prefilter_candidates(
        history_map=history_map,
        realtime_map=realtime_map,
        threshold_atr=threshold_atr,
        max_candidates=max_candidates,
        trade_date=latest,
    )

    model_name = os.getenv("BREAKOUT_LLM_MODEL", "qwen-plus")
    approved, llm_error = llm_review_candidates(
        candidates=candidates,
        model_name=model_name,
        min_confidence=min_confidence,
    )

    summary_text = compose_grouped_summary(
        trade_date=latest,
        scan_count=len(universe),
        candidate_count=len(candidates),
        signals=approved,
    )
    summary_brief = compose_grouped_brief(
        trade_date=latest,
        scan_count=len(universe),
        candidate_count=len(candidates),
        signals=approved,
    )
    summary_html = compose_grouped_html(
        trade_date=latest,
        scan_count=len(universe),
        candidate_count=len(candidates),
        signals=approved,
    )

    state_file = os.getenv("BREAKOUT_STATE_FILE", DEFAULT_STATE_FILE)
    state = _load_state(state_file)

    dedupe_hash = _signal_hash(latest, approved)
    sent_before = (
        state.get("last_trade_date") == latest
        and state.get("last_signal_hash") == dedupe_hash
    )

    channel_code = os.getenv("BREAKOUT_CHANNEL_CODE", DEFAULT_CHANNEL_CODE)
    email_recipients = _resolve_email_recipients(channel_code=channel_code)
    title = f"{latest} 14:25 技术突破提醒"

    publish_ok = False
    publish_msg = ""
    email_ok = False
    email_msg = ""

    if approved and not sent_before:
        publish_ok, publish_msg = _publish_station(
            channel_code=channel_code,
            title=title,
            content=summary_html,
            dry_run=dry_run,
            summary=summary_brief,
        )
        email_ok, email_msg = _send_email(email_recipients, title, summary_text, dry_run=dry_run)

        if (publish_ok or email_ok) and (not dry_run):
            state["last_trade_date"] = latest
            state["last_signal_hash"] = dedupe_hash
            state["last_sent_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            _save_state(state_file, state)
    elif sent_before:
        publish_msg = "duplicate-signal-skip"
        email_msg = "duplicate-signal-skip"

    status = "success"
    if llm_error and candidates:
        status = "partial"

    return {
        "status": status,
        "trade_date": latest,
        "scan_count": len(universe),
        "candidate_count": len(candidates),
        "signal_count": len(approved),
        "llm_error": llm_error,
        "sent_before": sent_before,
        "publish_ok": publish_ok,
        "publish_msg": publish_msg,
        "email_ok": email_ok,
        "email_msg": email_msg,
        "summary": summary_text,
    }


def run_test_push(dry_run: bool, test_message: str, email_to_arg: str) -> Dict[str, Any]:
    load_dotenv(override=True)

    channel_code = os.getenv("BREAKOUT_CHANNEL_CODE", DEFAULT_CHANNEL_CODE)
    email_recipients = _resolve_email_recipients(
        channel_code=channel_code,
        email_to_override=str(email_to_arg or "").strip(),
    )

    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    title = f"[联调] {now_text} 14:25 突破提醒站内+邮件测试"
    content = (
        test_message.strip()
        if str(test_message or "").strip()
        else f"【联调消息】{now_text}\n此消息用于验证 breakout 站内+邮件通道是否可达。"
    )

    publish_ok, publish_msg = _publish_station(channel_code, title, content, dry_run=dry_run)
    email_ok, email_msg = _send_email(email_recipients, title, content, dry_run=dry_run)

    status = "success" if (publish_ok or email_ok) else "error"
    return {
        "status": status,
        "mode": "test-push",
        "channel_code": channel_code,
        "email_configured": bool(email_recipients),
        "email_recipient_count": len(email_recipients),
        "publish_ok": publish_ok,
        "publish_msg": publish_msg,
        "email_ok": email_ok,
        "email_msg": email_msg,
        "message": content,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run 14:25 futures breakout alert job")
    parser.add_argument("--dry-run", action="store_true", help="仅打印结果，不实际发送")
    parser.add_argument("--trade-date", default="", help="指定交易日，格式 YYYYMMDD")
    parser.add_argument("--limit", type=int, default=0, help="仅扫描前N个品种（0表示不限制）")
    parser.add_argument("--symbols", default="", help="只扫描指定品种，逗号分隔，如 RB,AU")
    parser.add_argument("--send-test-message", action="store_true", help="发送联调测试消息（站内+邮件）")
    parser.add_argument("--test-message", default="", help="自定义联调消息正文")
    parser.add_argument("--email-to", default="", help="联调模式指定收件人，多个用逗号分隔")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    if args.send_test_message:
        result = run_test_push(
            dry_run=bool(args.dry_run),
            test_message=args.test_message,
            email_to_arg=args.email_to,
        )
    else:
        result = run_job(
            trade_date_arg=args.trade_date,
            limit=max(0, int(args.limit or 0)),
            symbols_arg=args.symbols,
            dry_run=bool(args.dry_run),
        )
    print(json.dumps(result, ensure_ascii=False, indent=2))

    if result.get("status") in {"success", "skipped", "partial"}:
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
