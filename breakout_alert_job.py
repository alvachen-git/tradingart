#!/usr/bin/env python3
"""
14:25 期货突破提醒任务
- V2：规则引擎直接出信号（区间突破 + 三法并联），LLM只补解释
- V1：兼容旧两阶段模式（可回滚）
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

from breakout_rules_v2 import BreakoutEngineV2

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
DEFAULT_ENGINE_MODE = "v2"
DEFAULT_TOP_K = 6
PATTERN_BONUS_PER_HIT = 0.10
MIN_CONSOLIDATION_BARS = 8
MAX_CONSOLIDATION_RANGE_ATR = 3.0
MAX_CONSOLIDATION_DRIFT_ATR = 0.5
DEFAULT_RULE_MAX_BOX_ATR = 2.2

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


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


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


def _empty_report_hash(trade_date: str, scan_count: int) -> str:
    payload = json.dumps({"trade_date": trade_date, "scan_count": int(scan_count), "type": "empty_report_v2"}, sort_keys=True)
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


def _load_replay_prices_from_db(
    engine: Any,
    trade_date: str,
    symbols: List[str],
    preferred_contracts: Optional[Dict[str, str]] = None,
) -> Dict[str, Optional[Dict[str, Any]]]:
    """
    历史回放价（用于 dry-run 历史交易日）：
    - 优先使用当日主力合约 close_price；
    - 主力缺失时回退到品种主码 close_price。
    """
    result: Dict[str, Optional[Dict[str, Any]]] = {str(s).upper(): None for s in symbols}
    if not symbols:
        return result

    pref = {str(k).upper(): str(v).upper() for k, v in (preferred_contracts or {}).items()}
    target_codes = set(str(x).upper() for x in symbols)
    target_codes.update([str(x).upper() for x in pref.values() if str(x).strip()])

    if not target_codes:
        return result

    params: Dict[str, Any] = {"trade_date": trade_date}
    placeholders: List[str] = []
    for i, code in enumerate(sorted(target_codes)):
        key = f"c{i}"
        params[key] = code
        placeholders.append(f":{key}")

    sql = text(
        f"""
        SELECT UPPER(SUBSTRING_INDEX(ts_code, '.', 1)) AS code, close_price
        FROM futures_price
        WHERE trade_date = :trade_date
          AND UPPER(SUBSTRING_INDEX(ts_code, '.', 1)) IN ({",".join(placeholders)})
        """
    )
    try:
        df = pd.read_sql(sql, engine, params=params)
    except Exception:
        return result
    if df.empty:
        return result

    close_map: Dict[str, float] = {}
    for _, r in df.iterrows():
        code = str(r.get("code", "")).upper().strip()
        px = _safe_float(r.get("close_price", 0.0), 0.0)
        if not code or px <= 0:
            continue
        close_map[code] = px

    for symbol in symbols:
        code = str(symbol).upper()
        contract = pref.get(code, "")
        if contract and contract in close_map:
            result[code] = {
                "name": contract,
                "price": float(close_map[contract]),
                "sina_code": "",
                "contract_code": contract,
                "quote_date": trade_date,
                "source": "db_replay_close",
            }
            continue
        if code in close_map:
            result[code] = {
                "name": code,
                "price": float(close_map[code]),
                "sina_code": "",
                "contract_code": code,
                "quote_date": trade_date,
                "source": "db_replay_close",
            }
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
    突破任务专用：按 kline_tools 关键阈值提取形态。
    """
    try:
        patterns = _calc_kline_patterns_tools_compatible(df)
        return {"patterns": patterns, "trends": [], "score": 50}
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
    # 无当日K线时仅有实时价，给合成K加保守上下影，避免 body_pct=1 造成平台突破误判。
    base_high = max(open_price, realtime_price)
    base_low = min(open_price, realtime_price)
    body_size = abs(realtime_price - open_price)
    shadow = max(body_size * 0.35, max(abs(open_price) * 0.0005, 1e-6))
    high_price = base_high + shadow
    low_price = max(0.0, base_low - shadow)
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


def _safe_div(numerator: float, denominator: float) -> float:
    d = float(denominator or 0.0)
    if abs(d) < 1e-12:
        return 0.0
    return float(numerator) / d


def _calc_kline_patterns_tools_compatible(df: pd.DataFrame) -> List[str]:
    """
    仅提取突破任务依赖的关键形态，阈值对齐 kline_tools.py：
    - 上升三法/下降三法
    - ATR 动态平台突破/跌破
    - 假突破/假跌破
    """
    if df is None or df.empty or len(df) < 7:
        return []

    work = df[["open_price", "high_price", "low_price", "close_price"]].copy().reset_index(drop=True)
    for col in ["open_price", "high_price", "low_price", "close_price"]:
        work[col] = pd.to_numeric(work[col], errors="coerce")
    work = work.dropna(subset=["open_price", "high_price", "low_price", "close_price"]).reset_index(drop=True)
    if len(work) < 7:
        return []

    work["MA5"] = work["close_price"].rolling(window=5).mean()
    work["MA10"] = work["close_price"].rolling(window=10).mean()

    curr = work.iloc[-1]
    prev = work.iloc[-2]
    pprev = work.iloc[-3]
    tprev = work.iloc[-4]
    pppprev = work.iloc[-5]
    fprev = work.iloc[-6]

    close = float(curr["close_price"])
    open_p = float(curr["open_price"])
    high = float(curr["high_price"])
    low = float(curr["low_price"])
    prev_close = float(prev["close_price"])
    prev_open = float(prev["open_price"])
    pprev_close = float(pprev["close_price"])
    pprev_open = float(pprev["open_price"])
    tprev_close = float(tprev["close_price"])
    tprev_open = float(tprev["open_price"])
    pppprev_close = float(pppprev["close_price"])
    pppprev_open = float(pppprev["open_price"])
    fprev_close = float(fprev["close_price"])
    fprev_open = float(fprev["open_price"])
    prev_high = float(prev["high_price"])
    prev_low = float(prev["low_price"])
    tprev_high = float(tprev["high_price"])
    tprev_low = float(tprev["low_price"])
    pppprev_high = float(pppprev["high_price"])
    pppprev_low = float(pppprev["low_price"])
    fprev_high = float(fprev["high_price"])
    fprev_low = float(fprev["low_price"])

    total_range = max(high - low, 0.01)
    body_size = abs(close - open_p)
    body_pct = _safe_div(body_size, total_range)
    chg_pct = _safe_div(close - prev_close, prev_close)
    prev_chg_pct = _safe_div(prev_close - prev_open, prev_open)
    pprev_chg_pct = _safe_div(pprev_close - pprev_open, pprev_open)
    tprev_chg_pct = _safe_div(tprev_close - tprev_open, tprev_open)
    pppprev_chg_pct = _safe_div(pppprev_close - pppprev_open, pppprev_open)
    fprev_chg_pct = _safe_div(fprev_close - fprev_open, fprev_open)

    prev_2_days_high = float(work["high_price"].iloc[-3:-1].max())
    prev_2_days_low = float(work["low_price"].iloc[-3:-1].min())
    prev_3_days_high = float(work["high_price"].iloc[-4:-1].max())
    prev_3_days_low = float(work["low_price"].iloc[-4:-1].min())
    prev_4_days_high = float(work["high_price"].iloc[-5:-1].max())
    prev_4_days_low = float(work["low_price"].iloc[-5:-1].min())
    prev_5_days_high = float(work["high_price"].iloc[-6:-1].max())
    prev_5_days_low = float(work["low_price"].iloc[-6:-1].min())

    patterns: List[str] = []
    ma5 = float(curr.get("MA5", 0.0) or 0.0)
    ma10 = float(curr.get("MA10", 0.0) or 0.0)

    # 上升三法（阈值对齐 kline_tools：0.01/0.015）
    if ma5 > ma10 and chg_pct > 0.01:
        if pprev_chg_pct > 0.015 and prev_close < pprev_close and prev_open < pprev_close and close > prev_2_days_high:
            patterns.append("【上升三法】(中继再涨，多头持续上攻！)")
        elif tprev_chg_pct > 0.015 and pprev_close < tprev_high and prev_close < tprev_high and close > prev_3_days_high:
            patterns.append("【上升三法】(中继再涨，多头持续上攻！！)")
        elif (
            pppprev_chg_pct > 0.015
            and tprev_close < pppprev_high
            and pprev_close < pppprev_high
            and prev_close < pppprev_high
            and close > prev_4_days_high
        ):
            patterns.append("【上升三法】(中继再涨，多头持续上攻！！)")
        elif (
            fprev_chg_pct > 0.015
            and pppprev_close < fprev_high
            and tprev_close < fprev_high
            and pprev_close < fprev_high
            and prev_close < fprev_high
            and close > prev_5_days_high
        ):
            patterns.append("【上升三法】(中继再涨，多头持续上攻！！)")

    # 下降三法（阈值对齐 kline_tools：-0.01/-0.015）
    if ma5 < ma10 and chg_pct < -0.01:
        if pprev_chg_pct < -0.015 and prev_close > pprev_close and prev_open > pprev_close and close < prev_2_days_low:
            patterns.append("【下降三法】(中继再跌，空头持续发力！)")
        elif tprev_chg_pct < -0.015 and pprev_close > tprev_low and prev_close > tprev_low and close < prev_3_days_low:
            patterns.append("【下降三法】(中继再跌，空头持续发力！)")
        elif (
            pppprev_chg_pct < -0.015
            and tprev_close > pppprev_low
            and pprev_close > pppprev_low
            and prev_close > pppprev_low
            and close < prev_4_days_low
        ):
            patterns.append("【下降三法】(中继再跌，空头持续发力！)")
        elif (
            fprev_chg_pct < -0.015
            and pppprev_close > fprev_low
            and tprev_close > fprev_low
            and pprev_close > fprev_low
            and prev_close > fprev_low
            and close < prev_5_days_low
        ):
            patterns.append("【下降三法】(中继再跌，空头持续发力！)")

    # ATR 动态平台突破 / 假突破
    work["h_l"] = work["high_price"] - work["low_price"]
    work["h_pc"] = (work["high_price"] - work["close_price"].shift(1)).abs()
    work["l_pc"] = (work["low_price"] - work["close_price"].shift(1)).abs()
    work["tr"] = work[["h_l", "h_pc", "l_pc"]].max(axis=1)
    work["atr"] = work["tr"].rolling(window=14).mean()

    ref_atr = float(work["atr"].iloc[-2]) if len(work) > 2 and pd.notna(work["atr"].iloc[-2]) else 0.0
    if ref_atr > 0:
        for period in SCAN_PERIODS:
            if len(work) <= period + 1:
                continue

            recent_box = work.iloc[-(period + 1) : -1]
            box_high = float(recent_box["high_price"].max())
            box_low = float(recent_box["low_price"].min())
            box_height = box_high - box_low
            atr_ratio = _safe_div(box_height, ref_atr)
            max_mul = _max_atr_multiple(period)

            if atr_ratio <= max_mul:
                if close > box_high and body_pct > 0.6:
                    patterns.append(f"{period}日平台突破")
                    break
                if close < box_low and body_pct > 0.6:
                    patterns.append(f"{period}日平台跌破")
                    break

            ref_atr_prev = float(work["atr"].iloc[-3]) if len(work) > 3 and pd.notna(work["atr"].iloc[-3]) else 0.0
            if ref_atr_prev <= 0 or len(work) <= period + 2:
                continue

            box_prev_days = work.iloc[-(period + 2) : -2]
            box_high_prev = float(box_prev_days["high_price"].max())
            box_low_prev = float(box_prev_days["low_price"].min())
            box_height_prev = box_high_prev - box_low_prev
            atr_ratio_prev = _safe_div(box_height_prev, ref_atr_prev)
            if atr_ratio_prev <= max_mul:
                if prev_close > box_high_prev and close < box_high_prev:
                    patterns.append("假突破(诱多)")
                    break
                if prev_close < box_low_prev and close > box_low_prev:
                    patterns.append("假跌破(诱空)")
                    break

    return patterns


def _allow_rule_breakout(item: Dict[str, Any], rule_max_box_atr: float, threshold_atr: float) -> bool:
    diag = _rule_breakout_diag(item=item, rule_max_box_atr=rule_max_box_atr, threshold_atr=threshold_atr)
    return bool(diag["pass"])


def _rule_breakout_diag(item: Optional[Dict[str, Any]], rule_max_box_atr: float, threshold_atr: float) -> Dict[str, Any]:
    """
    规则分支门禁：
    - 至少 8bar 周期；
    - 箱体宽度受控；
    - 若横盘硬门禁通过，直接允许；
    - 若硬门禁未通过，只放行“轻度未通过 + 足够强度”的边缘案例。
    """
    if not item:
        return {"pass": False, "reasons": ["missing_item"]}

    period = int(item.get("period", 0) or 0)
    if period < MIN_CONSOLIDATION_BARS:
        return {"pass": False, "reasons": [f"period_lt_{MIN_CONSOLIDATION_BARS}"]}

    atr_ratio = float(item.get("atr_ratio", 999.0) or 999.0)
    if atr_ratio > float(rule_max_box_atr):
        return {"pass": False, "reasons": [f"atr_ratio_gt_rule_max:{atr_ratio:.3f}>{float(rule_max_box_atr):.3f}"]}

    if bool(item.get("consolidation_ok", False)):
        return {"pass": True, "reasons": ["consolidation_ok"]}

    # 软放行仅用于短周期（5/10日）边缘破位，避免长周期趋势行情被误判成横盘突破。
    if period > 10:
        return {"pass": False, "reasons": ["soft_fallback_period_gt_10"]}

    range_atr = float(item.get("consolidation_range_atr", 999.0) or 999.0)
    drift_atr = float(item.get("consolidation_drift_atr", 999.0) or 999.0)
    strength_raw = abs(float(item.get("strength_raw", 0.0) or 0.0))
    soft_range_cap = min(float(rule_max_box_atr), 2.0)
    soft_strength_floor = float(threshold_atr)
    ok = range_atr <= soft_range_cap and drift_atr <= 0.8 and strength_raw >= soft_strength_floor
    reasons = []
    if range_atr > soft_range_cap:
        reasons.append(f"range_atr_gt_cap:{range_atr:.3f}>{soft_range_cap:.3f}")
    if drift_atr > 0.8:
        reasons.append(f"drift_atr_gt_cap:{drift_atr:.3f}>0.800")
    if strength_raw < soft_strength_floor:
        reasons.append(f"strength_lt_floor:{strength_raw:.3f}<{soft_strength_floor:.3f}")
    if ok:
        reasons = ["soft_fallback_pass"]
    return {"pass": bool(ok), "reasons": reasons}


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

    up_platform_periods = sorted(
        {
            int(m.group(1))
            for p in up_platform_hits
            for m in [re.search(r"(\d+)\s*日平台突破", str(p))]
            if m
        }
    )
    down_platform_periods = sorted(
        {
            int(m.group(1))
            for p in down_platform_hits
            for m in [re.search(r"(\d+)\s*日平台跌破", str(p))]
            if m
        }
    )

    return {
        "direction": direction,
        "up_hits": up_hits,
        "down_hits": down_hits,
        "up_platform_hits": up_platform_hits,
        "down_platform_hits": down_platform_hits,
        "up_platform_periods": up_platform_periods,
        "down_platform_periods": down_platform_periods,
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
    symbol_name_map: Optional[Dict[str, str]] = None,
    scan_symbols: Optional[List[str]] = None,
    debug_rows: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    symbol_name_map = symbol_name_map or {}
    rule_max_box_atr = float(os.getenv("BREAKOUT_RULE_MAX_BOX_ATR", str(DEFAULT_RULE_MAX_BOX_ATR)))
    symbols_iter = [str(s).upper() for s in (scan_symbols or list(history_map.keys()))]
    min_history_need = int(max(SCAN_PERIODS))

    for symbol in symbols_iter:
        df = history_map.get(symbol)
        debug_row: Dict[str, Any] = {
            "symbol": symbol,
            "symbol_name": str(symbol_name_map.get(symbol, "") or symbol_name_map.get(symbol.upper(), "") or ""),
            "threshold_atr": float(threshold_atr),
            "rule_max_box_atr": float(rule_max_box_atr),
            "drop_reasons": [],
            "period_checks": [],
            "branch_flags": {},
            "selected": False,
        }

        rt_data = realtime_map.get(symbol)
        if not rt_data:
            debug_row["drop_reasons"].append("no_realtime")
            if debug_rows is not None:
                debug_rows.append(debug_row)
            continue
        if df is None or df.empty:
            debug_row["drop_reasons"].append("no_history")
            if debug_rows is not None:
                debug_rows.append(debug_row)
            continue
        if len(df) < min_history_need:
            debug_row["drop_reasons"].append(f"insufficient_history:{len(df)}<{min_history_need}")
            if debug_rows is not None:
                debug_rows.append(debug_row)
            continue

        atr_series = _calc_atr14(df)
        latest_trade_date = trade_date or _normalize_trade_date(df.iloc[-1]["trade_date"])
        has_current_bar = _normalize_trade_date(df.iloc[-1]["trade_date"]) == latest_trade_date
        if atr_series.empty:
            debug_row["drop_reasons"].append("atr_series_empty")
            if debug_rows is not None:
                debug_rows.append(debug_row)
            continue
        atr_ref_idx = -2 if has_current_bar and len(atr_series) >= 2 else -1
        atr_latest = float(atr_series.iloc[atr_ref_idx]) if pd.notna(atr_series.iloc[atr_ref_idx]) else 0.0
        if atr_latest <= 0:
            debug_row["drop_reasons"].append(f"atr_invalid:{atr_latest:.6f}")
            if debug_rows is not None:
                debug_rows.append(debug_row)
            continue

        realtime_price = float(rt_data["price"])
        latest_close = float(df.iloc[-1]["close_price"])
        debug_row["realtime_price"] = realtime_price
        debug_row["contract_code"] = str(rt_data.get("contract_code", ""))
        debug_row["contract_name"] = str(rt_data.get("name", ""))
        debug_row["latest_close"] = latest_close
        debug_row["atr14"] = atr_latest

        # 1) 先复用既有K线形态识别（融合实时价后的临时K）
        intraday_df = _build_intraday_kline_df(df, realtime_price=realtime_price, trade_date=latest_trade_date)
        kline_result = _calc_kline_signals(intraday_df)
        patterns = [str(x) for x in kline_result.get("patterns", []) if str(x).strip()]
        pattern_sig = _extract_pattern_signal(patterns)
        debug_row["patterns"] = patterns[:8]
        up_platform_periods = set(pattern_sig.get("up_platform_periods", []))
        down_platform_periods = set(pattern_sig.get("down_platform_periods", []))
        intraday_ma5 = float(intraday_df["close_price"].rolling(window=5).mean().iloc[-1]) if len(intraday_df) >= 5 else latest_close
        intraday_ma10 = float(intraday_df["close_price"].rolling(window=10).mean().iloc[-1]) if len(intraday_df) >= 10 else latest_close

        # 2) 再做“压缩箱体 + 实时越界”判定，避免把普通波动当突破
        consolidation = _calc_consolidation_gate(df, atr_latest=atr_latest, bars=MIN_CONSOLIDATION_BARS)
        best_up: Optional[Dict[str, Any]] = None
        best_down: Optional[Dict[str, Any]] = None
        best_up_rule: Optional[Dict[str, Any]] = None
        best_down_rule: Optional[Dict[str, Any]] = None
        best_up_platform: Optional[Dict[str, Any]] = None
        best_down_platform: Optional[Dict[str, Any]] = None
        for period in SCAN_PERIODS:
            required = period + 1 if has_current_bar else period
            if len(df) < required:
                debug_row["period_checks"].append(
                    {
                        "period": int(period),
                        "required_rows": int(required),
                        "available_rows": int(len(df)),
                        "skip": "insufficient_rows",
                    }
                )
                continue
            window = df.iloc[-(period + 1) : -1] if has_current_bar else df.iloc[-period:]
            box_high = float(window["high_price"].max())
            box_low = float(window["low_price"].min())
            box_height = box_high - box_low
            atr_ratio = box_height / atr_latest if atr_latest > 0 else 999.0
            max_mul = _max_atr_multiple(period)
            up_strength = (realtime_price - box_high) / atr_latest
            down_strength = (box_low - realtime_price) / atr_latest

            # 与 kline_tools 的动态箱体定义一致
            if atr_ratio > max_mul:
                debug_row["period_checks"].append(
                    {
                        "period": int(period),
                        "box_high": box_high,
                        "box_low": box_low,
                        "atr_ratio": float(atr_ratio),
                        "max_atr_multiple": float(max_mul),
                        "up_strength_raw": float(up_strength),
                        "down_strength_raw": float(down_strength),
                        "dynamic_box_ok": False,
                    }
                )
                continue

            debug_row["period_checks"].append(
                {
                    "period": int(period),
                    "box_high": box_high,
                    "box_low": box_low,
                    "atr_ratio": float(atr_ratio),
                    "max_atr_multiple": float(max_mul),
                    "up_strength_raw": float(up_strength),
                    "down_strength_raw": float(down_strength),
                    "dynamic_box_ok": True,
                }
            )

            up_item = {
                "symbol": symbol,
                "symbol_name": str(symbol_name_map.get(symbol, "") or symbol_name_map.get(symbol.upper(), "") or ""),
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
                "intraday_ma5": intraday_ma5,
                "intraday_ma10": intraday_ma10,
            }
            down_item = dict(up_item)
            down_item["direction"] = "down"
            down_item["strength_raw"] = float(down_strength)

            if best_up is None or up_item["strength_raw"] > float(best_up["strength_raw"]):
                best_up = up_item
            if best_down is None or down_item["strength_raw"] > float(best_down["strength_raw"]):
                best_down = down_item
            if period >= MIN_CONSOLIDATION_BARS:
                if best_up_rule is None or up_item["strength_raw"] > float(best_up_rule["strength_raw"]):
                    best_up_rule = up_item
                if best_down_rule is None or down_item["strength_raw"] > float(best_down_rule["strength_raw"]):
                    best_down_rule = down_item
                if period in up_platform_periods:
                    if best_up_platform is None or up_item["strength_raw"] > float(best_up_platform["strength_raw"]):
                        best_up_platform = up_item
                if period in down_platform_periods:
                    if best_down_platform is None or down_item["strength_raw"] > float(best_down_platform["strength_raw"]):
                        best_down_platform = down_item

        options: List[Dict[str, Any]] = []
        up_rule_diag = _rule_breakout_diag(best_up_rule, rule_max_box_atr=rule_max_box_atr, threshold_atr=threshold_atr)
        down_rule_diag = _rule_breakout_diag(best_down_rule, rule_max_box_atr=rule_max_box_atr, threshold_atr=threshold_atr)
        debug_row["branch_flags"] = {
            "up_three_hit": bool(pattern_sig["up_three_hits"]),
            "down_three_hit": bool(pattern_sig["down_three_hits"]),
            "up_platform_hit": bool(pattern_sig["up_platform_hits"]),
            "down_platform_hit": bool(pattern_sig["down_platform_hits"]),
            "veto_up": bool(pattern_sig["veto_up"]),
            "veto_down": bool(pattern_sig["veto_down"]),
            "best_up_strength_raw": float(best_up["strength_raw"]) if best_up else None,
            "best_down_strength_raw": float(best_down["strength_raw"]) if best_down else None,
            "up_rule_gate": up_rule_diag,
            "down_rule_gate": down_rule_diag,
        }
        if (
            best_up is not None
            and float(best_up["strength_raw"]) >= threshold_atr
            and bool(pattern_sig["up_three_hits"])
            and not bool(pattern_sig["veto_up"])
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
            and bool(best_up_platform.get("consolidation_ok", False))
            and float(best_up_platform.get("atr_ratio", 999.0)) <= rule_max_box_atr
            and not bool(pattern_sig["veto_up"])
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
            and bool(best_down_platform.get("consolidation_ok", False))
            and float(best_down_platform.get("atr_ratio", 999.0)) <= rule_max_box_atr
            and not bool(pattern_sig["veto_down"])
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

        # 实时横盘区间突破分支：不依赖平台形态字符串，专门用于盘中破位识别
        if (
            best_up_rule is not None
            and float(best_up_rule["strength_raw"]) >= threshold_atr
            and bool(up_rule_diag["pass"])
            and float(best_up_rule.get("atr_ratio", 999.0)) <= rule_max_box_atr
            and not bool(pattern_sig["veto_up"])
        ):
            up_hits = list(pattern_sig["up_hits"])
            bonus = PATTERN_BONUS_PER_HIT * min(3, len(up_hits))
            item = dict(best_up_rule)
            item["strength"] = float(item["strength_raw"]) + bonus
            item["pattern_hits"] = up_hits[:3]
            item["reason_prefilter"] = (
                f"{item['period']}日横盘区间上破(压缩比{float(item['atr_ratio']):.1f}ATR, "
                f"{int(item['consolidation_bars'])}bar横盘)"
            )
            options.append(item)

        if (
            best_down_rule is not None
            and float(best_down_rule["strength_raw"]) >= threshold_atr
            and bool(down_rule_diag["pass"])
            and float(best_down_rule.get("atr_ratio", 999.0)) <= rule_max_box_atr
            and not bool(pattern_sig["veto_down"])
        ):
            down_hits = list(pattern_sig["down_hits"])
            bonus = PATTERN_BONUS_PER_HIT * min(3, len(down_hits))
            item = dict(best_down_rule)
            item["strength"] = float(item["strength_raw"]) + bonus
            item["pattern_hits"] = down_hits[:3]
            item["reason_prefilter"] = (
                f"{item['period']}日横盘区间下破(压缩比{float(item['atr_ratio']):.1f}ATR, "
                f"{int(item['consolidation_bars'])}bar横盘)"
            )
            options.append(item)

        if options:
            options.sort(key=lambda x: float(x.get("strength", 0.0)), reverse=True)
            pick = options[0]
            candidates.append(pick)
            debug_row["selected"] = True
            debug_row["selected_direction"] = str(pick.get("direction", ""))
            debug_row["selected_period"] = int(pick.get("period", 0) or 0)
            debug_row["selected_strength"] = float(pick.get("strength", 0.0) or 0.0)
            debug_row["selected_reason"] = str(pick.get("reason_prefilter", ""))
        else:
            dynamic_ok_count = sum(1 for x in debug_row["period_checks"] if bool(x.get("dynamic_box_ok")))
            if dynamic_ok_count == 0:
                debug_row["drop_reasons"].append("all_periods_dynamic_box_reject")
            up_raw = float(best_up["strength_raw"]) if best_up else None
            down_raw = float(best_down["strength_raw"]) if best_down else None
            if (up_raw is None or up_raw < threshold_atr) and (down_raw is None or down_raw < threshold_atr):
                debug_row["drop_reasons"].append("both_directions_strength_below_threshold")
            if not bool(pattern_sig["up_three_hits"] or pattern_sig["down_three_hits"] or pattern_sig["up_platform_hits"] or pattern_sig["down_platform_hits"]):
                debug_row["drop_reasons"].append("no_kline_trigger_pattern")
            if bool(pattern_sig["veto_up"]) and bool(pattern_sig["veto_down"]):
                debug_row["drop_reasons"].append("both_directions_vetoed")
            if not bool(up_rule_diag["pass"]) and not bool(down_rule_diag["pass"]):
                debug_row["drop_reasons"].append("rule_gate_reject")

        if debug_rows is not None:
            debug_rows.append(debug_row)

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


def parse_llm_explain_json(raw_text: str) -> List[Dict[str, Any]]:
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
        direction = str(item.get("direction", "")).lower().strip()
        reason_simple = str(item.get("reason_simple", "")).strip()
        summary_line = str(item.get("summary_line", "")).strip()
        period = int(item.get("period", 0) or 0)
        if not symbol or direction not in {"up", "down"}:
            continue
        parsed.append(
            {
                "symbol": symbol,
                "direction": direction,
                "period": period,
                "reason_simple": reason_simple,
                "summary_line": summary_line,
            }
        )
    return parsed


def engine_v2_select_candidates(
    history_map: Dict[str, pd.DataFrame],
    realtime_map: Dict[str, Optional[Dict[str, Any]]],
    symbol_name_map: Dict[str, str],
    trade_date: str,
    top_k: int,
    scan_symbols: Optional[List[str]] = None,
    debug: bool = False,
) -> Dict[str, Any]:
    engine = BreakoutEngineV2(top_k=top_k)
    return engine.select_candidates(
        history_map=history_map,
        realtime_map=realtime_map,
        symbol_name_map=symbol_name_map,
        trade_date=trade_date,
        scan_symbols=scan_symbols,
        debug=debug,
    )


def llm_explain_signals(
    signals: List[Dict[str, Any]],
    model_name: str,
) -> Tuple[List[Dict[str, Any]], str]:
    if not signals:
        return [], ""

    api_key = os.getenv("DASHSCOPE_API_KEY", "").strip()
    if not api_key:
        # 无 key 时仍返回规则信号，理由使用模板，保证稳定可复现。
        for item in signals:
            item["reason_simple"] = str(item.get("reason_simple", "") or item.get("reason_prefilter", "") or "规则触发")
            item["summary_line"] = str(item.get("summary_line", "") or item.get("reason_simple", ""))
        return signals, "missing DASHSCOPE_API_KEY"

    from llm_compat import ChatTongyiCompat

    llm = ChatTongyiCompat(model=model_name, api_key=api_key)
    compact = [
        {
            "symbol": str(x.get("symbol", "")),
            "symbol_name": str(x.get("symbol_name", "")),
            "direction": str(x.get("direction", "")),
            "period": int(x.get("period", 0) or 0),
            "trigger_type": str(x.get("trigger_type", "")),
            "realtime_price": round(float(x.get("realtime_price", 0.0)), 4),
            "box_high": round(float(x.get("box_high", 0.0)), 4),
            "box_low": round(float(x.get("box_low", 0.0)), 4),
            "distance_atr": round(float(x.get("distance_atr", 0.0)), 4),
            "width_atr": round(float(x.get("width_atr", 0.0)), 4),
            "score": round(float(x.get("score", 0.0)), 4),
            "reason_prefilter": str(x.get("reason_prefilter", "")),
        }
        for x in signals
    ]

    prompt = f"""
你是交易信号解释器。规则引擎已经确定以下都是有效突破信号，你只能解释，不可否决。

输入信号(JSON)：
{json.dumps(compact, ensure_ascii=False)}

请输出 JSON 数组，每个元素字段：
- symbol
- direction (up/down)
- period
- reason_simple（<=30字）
- summary_line（<=50字）

只输出JSON，不要其他文字。
""".strip()

    try:
        resp = llm.invoke(prompt)
        content = getattr(resp, "content", str(resp))
        parsed = parse_llm_explain_json(str(content))
    except Exception as exc:
        parsed = []
        llm_error = f"llm explain invoke failed: {exc}"
    else:
        llm_error = ""

    if not parsed:
        for item in signals:
            item["reason_simple"] = str(item.get("reason_simple", "") or item.get("reason_prefilter", "") or "规则触发")
            item["summary_line"] = str(item.get("summary_line", "") or item.get("reason_simple", ""))
        return signals, (llm_error or "llm explain parse failed")

    by_key = {
        (str(x.get("symbol", "")).upper(), str(x.get("direction", "")).lower(), int(x.get("period", 0) or 0)): x
        for x in parsed
    }
    by_symbol_direction = {
        (str(x.get("symbol", "")).upper(), str(x.get("direction", "")).lower()): x for x in parsed
    }

    merged: List[Dict[str, Any]] = []
    for item in signals:
        key = (
            str(item.get("symbol", "")).upper(),
            str(item.get("direction", "")).lower(),
            int(item.get("period", 0) or 0),
        )
        p = by_key.get(key)
        if p is None:
            p = by_symbol_direction.get((key[0], key[1]))
        out = dict(item)
        if p:
            out["reason_simple"] = str(p.get("reason_simple", "")).strip() or str(item.get("reason_prefilter", ""))
            out["summary_line"] = str(p.get("summary_line", "")).strip() or out["reason_simple"]
        else:
            out["reason_simple"] = str(item.get("reason_simple", "") or item.get("reason_prefilter", "") or "规则触发")
            out["summary_line"] = str(item.get("summary_line", "") or out["reason_simple"])
        merged.append(out)
    return merged, llm_error


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
            "symbol_name": c.get("symbol_name", ""),
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


def _format_signal_label(item: Dict[str, Any]) -> str:
    symbol = str(item.get("symbol", "")).upper()
    symbol_name = str(item.get("symbol_name", "") or "").strip()
    contract = str(item.get("contract_code", "") or "").strip()
    if symbol_name and contract:
        return f"{symbol_name} {symbol}({contract})"
    if symbol_name:
        return f"{symbol_name}({symbol})"
    if contract:
        return f"{symbol}({contract})"
    return symbol


def compose_grouped_summary(
    trade_date: str,
    scan_count: int,
    candidate_count: int,
    signals: List[Dict[str, Any]],
) -> str:
    grouped = group_signals(signals)
    header = (
        f"【14:25 技术突破提醒】{trade_date}\n"
        f"扫描品种: {scan_count} | 规则候选: {candidate_count} | 最终信号: {len(signals)}"
    )

    if not signals:
        return header + "\n\n未发现有效突破信号（已完成14:25扫描）。"

    lines = [header, "", "【上破组】"]
    if grouped["up"]:
        for x in grouped["up"]:
            trigger = str(x.get("trigger_type", "box_breakout"))
            action = f"上破{x['period']}日箱体" if trigger != "three_method" else "上升三法触发"
            lines.append(
                f"- {_format_signal_label(x)} 现价{float(x['realtime_price']):.2f} "
                f"{action} | {x.get('reason_simple','')}"
            )
    else:
        lines.append("- 无")

    lines.append("")
    lines.append("【下破组】")
    if grouped["down"]:
        for x in grouped["down"]:
            trigger = str(x.get("trigger_type", "box_breakout"))
            action = f"下破{x['period']}日箱体" if trigger != "three_method" else "下降三法触发"
            lines.append(
                f"- {_format_signal_label(x)} 现价{float(x['realtime_price']):.2f} "
                f"{action} | {x.get('reason_simple','')}"
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
        f"{trade_date} 14:25 扫描{scan_count} | 候选{candidate_count} | "
        f"信号{len(signals)} | 上破{len(grouped['up'])} | 下破{len(grouped['down'])}"
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
            display_label = html.escape(_format_signal_label(x))
            reason = html.escape(str(x.get("reason_simple", "")))
            period = int(x.get("period", 0))
            price = float(x.get("realtime_price", 0.0))
            strength = float(x.get("strength_raw", x.get("strength", 0.0)))
            atr_ratio = float(x.get("atr_ratio", 0.0))
            score = float(x.get("score", x.get("strength", 0.0)))
            trigger_type = str(x.get("trigger_type", "box_breakout"))
            trigger_text = f"{period}日箱体" if trigger_type != "three_method" else "三法形态"
            rows.append(
                (
                    "<div style='margin:10px 0;padding:12px 14px;border:1px solid rgba(255,255,255,0.12);"
                    "border-radius:10px;background:rgba(255,255,255,0.03);'>"
                    f"<div style='font-size:16px;font-weight:700;color:#eef6ff;'>{display_label}</div>"
                    f"<div style='margin-top:6px;color:#cde0ff;font-size:14px;line-height:1.7;'>"
                    f"现价：<b>{price:.2f}</b> ｜ 触发：{trigger_text} ｜ 越界强度：{strength:.2f} ATR ｜ 结构压缩：{atr_ratio:.1f} ATR ｜ 总分：{score:.3f}"
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
        f"扫描品种：<b>{scan_count}</b> ｜ 规则候选：<b>{candidate_count}</b> ｜ 最终信号：<b>{len(signals)}</b>"
        "</div>"
    )

    if not signals:
        body_html = (
            "<div style='margin-top:16px;padding:14px;border-radius:10px;"
            "background:rgba(255,255,255,0.04);font-size:15px;color:#d8e7ff;'>"
            "今日未发现有效突破信号（已完成14:25扫描）。"
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
    debug: bool = False,
    top_k_arg: int = 0,
    engine_mode_arg: str = "",
) -> Dict[str, Any]:
    load_dotenv(override=True)

    from data_engine import PRODUCT_MAP, engine, get_latest_data_date

    now = datetime.now()
    today = now.strftime("%Y%m%d")
    latest = _normalize_trade_date(get_latest_data_date())
    target_trade_date = _normalize_trade_date(trade_date_arg) if trade_date_arg else latest
    replay_mode = bool(target_trade_date and latest and target_trade_date != latest)

    if not latest:
        return {"status": "error", "error": "latest trading date unavailable"}

    if not trade_date_arg and latest != today:
        return {
            "status": "skipped",
            "reason": f"non-trading-day-or-data-not-ready today={today} latest={latest}",
            "trade_date": latest,
        }

    # 历史日期仅允许 dry-run 回放，避免误发历史信号。
    if replay_mode and (not dry_run):
        return {
            "status": "skipped",
            "reason": f"trade_date_mismatch_non_dry target={target_trade_date} latest={latest}",
            "trade_date": latest,
        }

    product_name_map = {str(k).upper(): str(v) for k, v in PRODUCT_MAP.items()}
    product_codes = sorted(product_name_map.keys())
    symbols_filter = _parse_symbols_arg(symbols_arg)

    universe = _load_scan_symbols(
        engine=engine,
        latest_trade_date=target_trade_date,
        product_codes=product_codes,
        symbols_filter=symbols_filter,
        limit=limit,
    )

    if not universe:
        return {
            "status": "skipped",
            "reason": "no symbols to scan",
            "trade_date": target_trade_date,
        }

    main_contract_map = _load_main_contract_map(engine=engine, latest_trade_date=target_trade_date, symbols=universe)
    if replay_mode:
        realtime_map = _load_replay_prices_from_db(
            engine=engine,
            trade_date=target_trade_date,
            symbols=universe,
            preferred_contracts=main_contract_map,
        )
    else:
        realtime_map = fetch_realtime_prices(
            universe,
            preferred_contracts=main_contract_map,
            target_trade_date=target_trade_date,
        )

    history_map: Dict[str, pd.DataFrame] = {}
    for symbol in universe:
        history_code = str(main_contract_map.get(symbol, symbol)).upper()
        df = _fetch_history_df(engine, history_code, target_trade_date, bars=HISTORY_BARS)
        if df.empty and history_code != symbol:
            df = _fetch_history_df(engine, symbol, target_trade_date, bars=HISTORY_BARS)
        if not df.empty:
            history_map[symbol] = df

    engine_mode = str(engine_mode_arg or os.getenv("BREAKOUT_ENGINE", DEFAULT_ENGINE_MODE)).strip().lower()
    if engine_mode not in {"v1", "v2"}:
        engine_mode = DEFAULT_ENGINE_MODE

    top_k = max(1, int(top_k_arg or os.getenv("BREAKOUT_TOP_K", str(DEFAULT_TOP_K))))
    model_name = os.getenv("BREAKOUT_LLM_MODEL", "qwen-plus")

    llm_error = ""
    candidates: List[Dict[str, Any]] = []
    signals: List[Dict[str, Any]] = []
    debug_payload: Dict[str, Any] = {}

    if engine_mode == "v1":
        # V1 兼容路径（回滚用）
        max_candidates = int(os.getenv("BREAKOUT_MAX_CANDIDATES", str(DEFAULT_MAX_CANDIDATES)))
        min_confidence = float(os.getenv("BREAKOUT_MIN_CONFIDENCE", str(DEFAULT_MIN_CONFIDENCE)))
        threshold_atr = float(os.getenv("BREAKOUT_THRESHOLD_ATR", str(DEFAULT_THRESHOLD_ATR)))
        prefilter_debug: Optional[List[Dict[str, Any]]] = [] if debug else None
        candidates = build_prefilter_candidates(
            history_map=history_map,
            realtime_map=realtime_map,
            threshold_atr=threshold_atr,
            max_candidates=max_candidates,
            trade_date=target_trade_date,
            symbol_name_map=product_name_map,
            scan_symbols=universe,
            debug_rows=prefilter_debug,
        )
        signals, llm_error = llm_review_candidates(
            candidates=candidates,
            model_name=model_name,
            min_confidence=min_confidence,
        )
        if debug:
            debug_payload = {
                "engine": "v1",
                "threshold_atr": threshold_atr,
                "min_confidence": min_confidence,
                "rule_max_box_atr": float(os.getenv("BREAKOUT_RULE_MAX_BOX_ATR", str(DEFAULT_RULE_MAX_BOX_ATR))),
                "prefilter": prefilter_debug or [],
            }
    else:
        # V2 主路径：规则直出 + LLM仅解释
        v2 = engine_v2_select_candidates(
            history_map=history_map,
            realtime_map=realtime_map,
            symbol_name_map=product_name_map,
            trade_date=target_trade_date,
            top_k=top_k,
            scan_symbols=universe,
            debug=debug,
        )
        candidates = list(v2.get("all_ranked", []))
        top_candidates = list(v2.get("candidates", []))
        signals, llm_error = llm_explain_signals(signals=top_candidates, model_name=model_name)
        if debug:
            debug_payload = {
                "engine": "v2",
                "top_k": top_k,
                "engine_v2": v2.get("debug_rows", []),
                "ranking_table": v2.get("ranking_table", []),
            }

    summary_text = compose_grouped_summary(
        trade_date=target_trade_date,
        scan_count=len(universe),
        candidate_count=len(candidates),
        signals=signals,
    )
    summary_brief = compose_grouped_brief(
        trade_date=target_trade_date,
        scan_count=len(universe),
        candidate_count=len(candidates),
        signals=signals,
    )
    summary_html = compose_grouped_html(
        trade_date=target_trade_date,
        scan_count=len(universe),
        candidate_count=len(candidates),
        signals=signals,
    )

    state_file = os.getenv("BREAKOUT_STATE_FILE", DEFAULT_STATE_FILE)
    state = _load_state(state_file)

    has_signal = bool(signals)
    report_hash = _signal_hash(target_trade_date, signals) if has_signal else _empty_report_hash(target_trade_date, len(universe))
    sent_before = False
    if has_signal:
        latest_signal = state.get("last_signal_report", {})
        sent_before = (
            str(latest_signal.get("trade_date", "")) == target_trade_date
            and str(latest_signal.get("hash", "")) == report_hash
        ) or (
            state.get("last_trade_date") == target_trade_date
            and state.get("last_signal_hash") == report_hash
        )
    else:
        latest_empty = state.get("last_empty_report", {})
        sent_before = (
            str(latest_empty.get("trade_date", "")) == target_trade_date
            and str(latest_empty.get("hash", "")) == report_hash
        )

    channel_code = os.getenv("BREAKOUT_CHANNEL_CODE", DEFAULT_CHANNEL_CODE)
    email_recipients = _resolve_email_recipients(channel_code=channel_code)
    title = f"{target_trade_date} 14:25 技术突破提醒"

    publish_ok = False
    publish_msg = ""
    email_ok = False
    email_msg = ""

    if not sent_before:
        publish_ok, publish_msg = _publish_station(
            channel_code=channel_code,
            title=title,
            content=summary_html,
            dry_run=dry_run,
            summary=summary_brief,
        )
        email_ok, email_msg = _send_email(email_recipients, title, summary_text, dry_run=dry_run)

        if (publish_ok or email_ok) and (not dry_run):
            if has_signal:
                state["last_trade_date"] = target_trade_date
                state["last_signal_hash"] = report_hash
                state["last_signal_report"] = {
                    "trade_date": target_trade_date,
                    "hash": report_hash,
                }
            else:
                state["last_empty_report"] = {
                    "trade_date": target_trade_date,
                    "hash": report_hash,
                }
            state["last_sent_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            _save_state(state_file, state)
    elif sent_before:
        publish_msg = "duplicate-signal-skip"
        email_msg = "duplicate-signal-skip"

    result = {
        "status": "success",
        "trade_date": target_trade_date,
        "engine": engine_mode,
        "scan_count": len(universe),
        "candidate_count": len(candidates),
        "signal_count": len(signals),
        "llm_error": llm_error,
        "sent_before": sent_before,
        "publish_ok": publish_ok,
        "publish_msg": publish_msg,
        "email_ok": email_ok,
        "email_msg": email_msg,
        "summary": summary_text,
    }
    if debug:
        result["debug"] = debug_payload
    return result


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
    parser.add_argument("--top-k", type=int, default=0, help="V2最终信号数量上限（0表示使用环境变量或默认6）")
    parser.add_argument("--engine", default="", help="突破引擎版本：v2|v1（默认v2）")
    parser.add_argument("--send-test-message", action="store_true", help="发送联调测试消息（站内+邮件）")
    parser.add_argument("--test-message", default="", help="自定义联调消息正文")
    parser.add_argument("--email-to", default="", help="联调模式指定收件人，多个用逗号分隔")
    parser.add_argument("--debug", action="store_true", help="输出预筛调试细节（每个品种的入选/淘汰原因）")
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
            debug=bool(args.debug),
            top_k_arg=max(0, int(args.top_k or 0)),
            engine_mode_arg=str(args.engine or "").strip(),
        )
    print(json.dumps(result, ensure_ascii=False, indent=2))

    if result.get("status") in {"success", "skipped", "partial"}:
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
