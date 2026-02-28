import hashlib
import json
import re
from datetime import datetime
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError

from data_engine import engine
from kline_tools import analyze_kline_pattern
from market_correlation import analyze_stock_market_correlation

MARKET_VALUE_REL_GAP_THRESHOLD = 0.30
QUANTITY_REL_GAP_THRESHOLD = 0.12


def _utc_now_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def ensure_portfolio_tables() -> None:
    if engine is None:
        raise ValueError("数据库连接不可用")

    sql_positions = """
    CREATE TABLE IF NOT EXISTS user_portfolio_positions (
        user_id VARCHAR(128) NOT NULL,
        symbol VARCHAR(32) NOT NULL,
        name VARCHAR(128) DEFAULT '',
        market VARCHAR(8) DEFAULT 'A',
        quantity DOUBLE NULL,
        market_value DOUBLE NULL,
        price DOUBLE NULL,
        cost_price DOUBLE NULL,
        industry VARCHAR(128) DEFAULT NULL,
        technical_grade VARCHAR(16) DEFAULT '持有',
        technical_reason TEXT,
        index_corr_json LONGTEXT,
        position_hash VARCHAR(64) NOT NULL,
        screenshot_hash VARCHAR(64) DEFAULT NULL,
        last_seen_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        PRIMARY KEY (user_id, symbol),
        KEY idx_user_updated (user_id, updated_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """

    sql_snapshot = """
    CREATE TABLE IF NOT EXISTS user_portfolio_snapshot (
        user_id VARCHAR(128) NOT NULL,
        industry_allocation_json LONGTEXT,
        portfolio_corr_json LONGTEXT,
        summary_text TEXT,
        snapshot_hash VARCHAR(64) DEFAULT NULL,
        screenshot_hash VARCHAR(64) DEFAULT NULL,
        recognized_count INT DEFAULT 0,
        missing_count INT DEFAULT 0,
        updated_at DATETIME NOT NULL,
        PRIMARY KEY (user_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """

    with engine.begin() as conn:
        conn.execute(text(sql_positions))
        conn.execute(text(sql_snapshot))


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    txt = str(value).strip()
    if not txt:
        return None
    txt = txt.replace(",", "").replace("，", "").replace("元", "").replace("股", "")
    txt = txt.replace("HK$", "").replace("$", "").replace("¥", "").replace("￥", "")

    scale = 1.0
    if txt.endswith("亿"):
        txt = txt[:-1]
        scale = 1e8
    elif txt.endswith("万"):
        txt = txt[:-1]
        scale = 1e4

    m = re.search(r"-?\d+(?:\.\d+)?", txt)
    if not m:
        return None
    return float(m.group(0)) * scale


def _first_numeric_value(item: Dict[str, Any], keys: List[str]) -> Optional[float]:
    for key in keys:
        if key not in item:
            continue
        value = item.get(key)
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        parsed = _to_float(value)
        if parsed is not None:
            return parsed
    return None


def _normalize_symbol_market(symbol: Any, market: Any = None) -> Tuple[str, str]:
    code = str(symbol or "").strip().upper().replace(" ", "")
    market_text = str(market or "").strip().upper()

    # OCR 可能把港股错加 A 股后缀（如 00988.SZ），这里先做格式纠偏。
    m_5_a = re.fullmatch(r"(\d{5})\.(SH|SZ|BJ)", code)
    if m_5_a:
        return f"{m_5_a.group(1)}.HK", "HK"
    m_4_hk = re.fullmatch(r"(\d{4})\.HK", code)
    if m_4_hk:
        return f"0{m_4_hk.group(1)}.HK", "HK"

    if re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", code):
        return code, "A"
    if re.fullmatch(r"\d{5}\.HK", code):
        return code, "HK"
    if re.fullmatch(r"\d{6}", code):
        if code.startswith(("6", "5", "9")):
            return f"{code}.SH", "A"
        if code.startswith(("0", "1", "2", "3")):
            return f"{code}.SZ", "A"
        return f"{code}.BJ", "A"
    if re.fullmatch(r"\d{5}", code):
        return f"{code}.HK", "HK"

    if market_text in ("HK", "HONGKONG"):
        return code, "HK"
    if market_text in ("A", "CN", "ASHARE", "CHINA"):
        return code, "A"

    if code.endswith(".HK"):
        return code, "HK"
    if code.endswith((".SH", ".SZ", ".BJ")):
        return code, "A"
    return code, "A"


def _symbol_quality(symbol: str, market: str) -> Tuple[float, int]:
    code = str(symbol or "").upper()
    mkt = str(market or "").upper()
    base = code.split(".")[0] if "." in code else code
    suffix = code.split(".")[1] if "." in code else ""
    try:
        base_num = int(base)
    except Exception:
        base_num = -1

    score = 0.0
    if mkt == "A":
        if re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", code):
            score += 3.0
        if suffix == "SH" and base.startswith(("6", "5", "9")):
            score += 0.5
        if suffix == "SZ" and base.startswith(("0", "1", "2", "3")):
            score += 0.5
    elif mkt == "HK":
        if re.fullmatch(r"\d{5}\.HK", code):
            score += 3.0
        if suffix == "HK":
            score += 0.5
    return score, base_num


def _dedup_positions_by_identity(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    去重策略：
    1) 先按 symbol 去重（保留质量更高者）；
    2) 再按 name+quantity+market_value 去重，消除同一标的多代码重复识别。
    """
    by_symbol: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        sym = str(row.get("symbol") or "")
        if not sym:
            continue
        prev = by_symbol.get(sym)
        if not prev:
            by_symbol[sym] = row
            continue
        cur_rank = _symbol_quality(row.get("symbol", ""), row.get("market", ""))
        prev_rank = _symbol_quality(prev.get("symbol", ""), prev.get("market", ""))
        if cur_rank > prev_rank:
            by_symbol[sym] = row

    by_identity: Dict[str, Dict[str, Any]] = {}
    for row in by_symbol.values():
        name = str(row.get("name") or "").strip()
        qty = row.get("quantity")
        mv = row.get("market_value")
        if name and qty is not None and mv is not None:
            identity = f"{name}|{round(float(qty),4)}|{round(float(mv),2)}"
        else:
            identity = f"SYM|{row.get('symbol')}"

        prev = by_identity.get(identity)
        if not prev:
            by_identity[identity] = row
            continue

        cur_rank = _symbol_quality(row.get("symbol", ""), row.get("market", ""))
        prev_rank = _symbol_quality(prev.get("symbol", ""), prev.get("market", ""))
        # 主排序：代码质量；次排序：数值更大的 base_num（优先 09988.HK over 00988.HK）。
        if cur_rank > prev_rank:
            by_identity[identity] = row

    return list(by_identity.values())


def _reconcile_market_value(
    quantity: Optional[float], price: Optional[float], market_value: Optional[float]
) -> Optional[float]:
    """
    纠偏 OCR 市值:
    - 若 quantity 和 price 可用，优先用 quantity*price 做一致性校验；
    - 当 OCR 市值与理论市值偏差过大（>30%）时，自动改用理论市值。
    """
    if quantity is None or price is None:
        return market_value
    try:
        implied = float(quantity) * float(price)
    except Exception:
        return market_value
    if implied <= 0:
        return market_value
    if market_value is None:
        return implied
    try:
        mv = float(market_value)
    except Exception:
        return implied
    if mv <= 0:
        return implied

    rel_gap = abs(mv - implied) / max(abs(implied), 1.0)
    if rel_gap > MARKET_VALUE_REL_GAP_THRESHOLD:
        return implied
    return mv


def _normalize_inferred_quantity(inferred: float) -> float:
    rounded = round(inferred)
    if abs(inferred - rounded) / max(abs(inferred), 1.0) <= 0.01:
        return float(rounded)
    return round(inferred, 4)


def _reconcile_quantity(
    quantity: Optional[float], price: Optional[float], market_value: Optional[float]
) -> Optional[float]:
    """
    纠偏 OCR 数量:
    - quantity 缺失/<=0 时，用 market_value/price 反推数量；
    - quantity 与反推数量偏差过大时，优先使用反推值。
    """
    if price is None or market_value is None:
        return quantity
    try:
        p = float(price)
        mv = float(market_value)
    except Exception:
        return quantity
    if p <= 0 or mv <= 0:
        return quantity

    inferred = mv / p
    if inferred <= 0:
        return quantity
    inferred_qty = _normalize_inferred_quantity(inferred)

    if quantity is None:
        return inferred_qty
    try:
        q = float(quantity)
    except Exception:
        return inferred_qty
    if q <= 0:
        return inferred_qty

    rel_gap = abs(q - inferred_qty) / max(abs(inferred_qty), 1.0)
    if rel_gap > QUANTITY_REL_GAP_THRESHOLD:
        return inferred_qty
    return q


def _normalize_positions(raw_positions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in raw_positions or []:
        if not isinstance(item, dict):
            continue

        symbol_raw = item.get("symbol") or item.get("code") or item.get("ticker")
        symbol, market = _normalize_symbol_market(symbol_raw, item.get("market"))
        if not symbol:
            continue

        quantity = _first_numeric_value(
            item,
            [
                "quantity",
                "actual_quantity",
                "实际数量",
                "实有数量",
                "持仓数量",
                "股票余额",
                "可用余额",
            ],
        )
        market_value = _first_numeric_value(item, ["market_value", "持仓市值", "市值"])
        price = _first_numeric_value(item, ["price", "最新价", "现价", "市价"])
        cost_price = _first_numeric_value(item, ["cost_price", "成本价", "买入价"])

        market_value = _reconcile_market_value(quantity, price, market_value)
        quantity = _reconcile_quantity(quantity, price, market_value)

        rows.append(
            {
                "symbol": symbol,
                "market": market,
                "name": str(item.get("name") or "").strip(),
                "quantity": quantity,
                "market_value": market_value,
                "price": price,
                "cost_price": cost_price,
            }
        )

    return _dedup_positions_by_identity(rows)


def _compute_position_hash(quantity: Optional[float], market_value: Optional[float]) -> str:
    qty = 0.0 if quantity is None else round(float(quantity), 4)
    mv = 0.0 if market_value is None else round(float(market_value), 2)
    payload = f"qty={qty:.4f}|mv={mv:.2f}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@lru_cache(maxsize=32)
def _table_exists(table_name: str) -> bool:
    if engine is None:
        return False
    try:
        inspector = inspect(engine)
        return bool(inspector.has_table(table_name))
    except Exception:
        return False


def _fetch_name_industry(symbol: str, market: str, fallback_name: str = "") -> Tuple[str, str]:
    if engine is None:
        return fallback_name or symbol, "未知行业"

    if market == "HK":
        sql_hk = text(
            """
            SELECT name
            FROM stock_price
            WHERE ts_code = :code
            ORDER BY trade_date DESC
            LIMIT 1
            """
        )
        row = None
        try:
            if _table_exists("stock_price"):
                with engine.connect() as conn:
                    row = conn.execute(sql_hk, {"code": symbol}).mappings().fetchone()
        except SQLAlchemyError:
            row = None
        name = (row or {}).get("name") if row else None
        return (str(name).strip() if name else (fallback_name or symbol), "港股其他")

    code_no_suffix = symbol.split(".")[0]
    sql_basic = text(
        """
        SELECT name, industry
        FROM stock_basic
        WHERE ts_code = :code OR ts_code = :code_no_suffix
        LIMIT 1
        """
    )
    sql_screener = text(
        """
        SELECT name, industry
        FROM daily_stock_screener
        WHERE ts_code = :code OR ts_code = :code_no_suffix
        ORDER BY trade_date DESC
        LIMIT 1
        """
    )

    row = None
    try:
        with engine.connect() as conn:
            if _table_exists("stock_basic"):
                row = conn.execute(
                    sql_basic, {"code": symbol, "code_no_suffix": code_no_suffix}
                ).mappings().fetchone()
            if (not row) and _table_exists("daily_stock_screener"):
                row = conn.execute(
                    sql_screener, {"code": symbol, "code_no_suffix": code_no_suffix}
                ).mappings().fetchone()
    except SQLAlchemyError:
        row = None

    name = (row or {}).get("name") if row else None
    industry = (row or {}).get("industry") if row else None
    return (
        str(name).strip() if name else (fallback_name or symbol),
        str(industry).strip() if industry else "未知行业",
    )


def _grade_from_kline_report(report: str) -> Tuple[str, str]:
    text_blob = str(report or "")
    lowered = text_blob.lower()

    bearish_terms = ["空头", "三只乌鸦", "空头吞噬", "跌破20日线", "破位", "下跌", "见顶"]
    bullish_terms = ["多头", "红三兵", "多头吞噬", "站稳20日线", "放量突破", "上涨", "反转"]

    bear_hits = sum(1 for t in bearish_terms if t in text_blob or t in lowered)
    bull_hits = sum(1 for t in bullish_terms if t in text_blob or t in lowered)

    if bear_hits >= bull_hits + 1:
        grade = "减仓"
    elif bull_hits >= bear_hits + 1:
        grade = "增持"
    else:
        grade = "持有"

    lines = [ln.strip() for ln in text_blob.splitlines() if ln.strip()]
    reason = lines[1] if len(lines) > 1 else (lines[0] if lines else "数据不足，建议持有观察")
    return grade, reason[:220]


def _compute_index_corr(symbol: str, lookback_days: int = 120) -> Dict[str, float]:
    df = analyze_stock_market_correlation(symbol, lookback_days=lookback_days)
    if df is None or df.empty:
        return {}
    corr_map: Dict[str, float] = {}
    for _, row in df.iterrows():
        name = str(row.get("指数名称") or "").strip()
        corr = row.get("相关系数")
        if not name:
            continue
        try:
            corr_map[name] = round(float(corr), 4)
        except Exception:
            continue
    return corr_map


def _analyze_technical(symbol: str, display_name: str = "") -> Tuple[str, str]:
    query = symbol
    report = ""
    try:
        report = analyze_kline_pattern.invoke({"query": query})
    except Exception:
        try:
            report = analyze_kline_pattern.invoke({"symbol": query})
        except Exception:
            if display_name:
                try:
                    report = analyze_kline_pattern.invoke({"query": display_name})
                except Exception:
                    report = "技术分析失败"
            else:
                report = "技术分析失败"

    return _grade_from_kline_report(report)


def _load_existing_hashes(user_id: str) -> Dict[str, str]:
    sql = text(
        """
        SELECT symbol, position_hash
        FROM user_portfolio_positions
        WHERE user_id = :uid
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, {"uid": user_id}).mappings().fetchall()
    return {str(r["symbol"]): str(r["position_hash"]) for r in rows}


def _upsert_position(user_id: str, row: Dict[str, Any], timestamp: str, screenshot_hash: str) -> None:
    sql = text(
        """
        INSERT INTO user_portfolio_positions (
            user_id, symbol, name, market, quantity, market_value, price, cost_price,
            industry, technical_grade, technical_reason, index_corr_json, position_hash,
            screenshot_hash, last_seen_at, updated_at
        ) VALUES (
            :user_id, :symbol, :name, :market, :quantity, :market_value, :price, :cost_price,
            :industry, :technical_grade, :technical_reason, :index_corr_json, :position_hash,
            :screenshot_hash, :last_seen_at, :updated_at
        )
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            market = VALUES(market),
            quantity = VALUES(quantity),
            market_value = VALUES(market_value),
            price = VALUES(price),
            cost_price = VALUES(cost_price),
            industry = VALUES(industry),
            technical_grade = VALUES(technical_grade),
            technical_reason = VALUES(technical_reason),
            index_corr_json = VALUES(index_corr_json),
            position_hash = VALUES(position_hash),
            screenshot_hash = VALUES(screenshot_hash),
            last_seen_at = VALUES(last_seen_at),
            updated_at = VALUES(updated_at)
        """
    )
    payload = dict(row)
    payload.update(
        {
            "user_id": user_id,
            "screenshot_hash": screenshot_hash,
            "last_seen_at": timestamp,
            "updated_at": timestamp,
            "index_corr_json": json.dumps(
                row.get("index_corr", {}), ensure_ascii=False, sort_keys=True
            ),
        }
    )
    with engine.begin() as conn:
        conn.execute(sql, payload)


def _touch_position_seen(user_id: str, symbol: str, timestamp: str, screenshot_hash: str) -> None:
    sql = text(
        """
        UPDATE user_portfolio_positions
        SET last_seen_at = :ts, screenshot_hash = :screenshot_hash
        WHERE user_id = :uid AND symbol = :symbol
        """
    )
    with engine.begin() as conn:
        conn.execute(
            sql,
            {
                "uid": user_id,
                "symbol": symbol,
                "ts": timestamp,
                "screenshot_hash": screenshot_hash,
            },
        )


def _build_industry_allocation(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    total_mv = sum(max(float(r.get("market_value") or 0.0), 0.0) for r in rows)
    by_industry: Dict[str, float] = {}
    for r in rows:
        industry = str(r.get("industry") or "未知行业")
        by_industry[industry] = by_industry.get(industry, 0.0) + max(
            float(r.get("market_value") or 0.0), 0.0
        )

    output: List[Dict[str, Any]] = []
    for industry, mv in by_industry.items():
        pct = (mv / total_mv * 100.0) if total_mv > 0 else 0.0
        output.append(
            {"industry": industry, "market_value": round(mv, 2), "weight_pct": round(pct, 2)}
        )
    output.sort(key=lambda x: x["market_value"], reverse=True)
    return output


def _build_weighted_corr(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    weight_sum = sum(max(float(r.get("market_value") or 0.0), 0.0) for r in rows)
    if weight_sum <= 0:
        weight_sum = float(len(rows) or 1)

    accum: Dict[str, float] = {}
    accum_w: Dict[str, float] = {}
    for r in rows:
        corr_map = r.get("index_corr") or {}
        if not isinstance(corr_map, dict):
            continue
        w = max(float(r.get("market_value") or 0.0), 0.0)
        if w <= 0:
            w = 1.0
        for idx_name, corr in corr_map.items():
            try:
                corr_val = float(corr)
            except Exception:
                continue
            accum[idx_name] = accum.get(idx_name, 0.0) + corr_val * w
            accum_w[idx_name] = accum_w.get(idx_name, 0.0) + w

    result: Dict[str, float] = {}
    for idx_name, val in accum.items():
        w = accum_w.get(idx_name, 0.0)
        if w > 0:
            result[idx_name] = round(val / w, 4)
    return dict(sorted(result.items(), key=lambda kv: kv[1], reverse=True))


def _build_snapshot_hash(rows: List[Dict[str, Any]]) -> str:
    parts = [
        f"{r.get('symbol')}|{r.get('position_hash')}"
        for r in sorted(rows, key=lambda x: str(x.get("symbol")))
    ]
    raw = "||".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _build_summary_text(
    recognized_count: int,
    missing_count: int,
    industry_alloc: List[Dict[str, Any]],
    portfolio_corr: Dict[str, float],
) -> str:
    top_industry = industry_alloc[0]["industry"] if industry_alloc else "未知行业"
    top_industry_pct = industry_alloc[0]["weight_pct"] if industry_alloc else 0
    corr_items = list(portfolio_corr.items())
    if corr_items:
        top_idx, top_corr = corr_items[0]
        corr_text = f"组合与{top_idx}相关度{top_corr:+.2f}"
    else:
        corr_text = "组合相关度数据不足"
    return (
        f"识别到{recognized_count}只股票，缺失{missing_count}只。"
        f"行业最大暴露为{top_industry}({top_industry_pct:.1f}%)，{corr_text}。"
    )


def _upsert_snapshot(
    user_id: str,
    industry_alloc: List[Dict[str, Any]],
    portfolio_corr: Dict[str, float],
    summary_text: str,
    snapshot_hash: str,
    screenshot_hash: str,
    recognized_count: int,
    missing_count: int,
    updated_at: str,
) -> None:
    sql = text(
        """
        INSERT INTO user_portfolio_snapshot (
            user_id, industry_allocation_json, portfolio_corr_json, summary_text,
            snapshot_hash, screenshot_hash, recognized_count, missing_count, updated_at
        ) VALUES (
            :user_id, :industry_allocation_json, :portfolio_corr_json, :summary_text,
            :snapshot_hash, :screenshot_hash, :recognized_count, :missing_count, :updated_at
        )
        ON DUPLICATE KEY UPDATE
            industry_allocation_json = VALUES(industry_allocation_json),
            portfolio_corr_json = VALUES(portfolio_corr_json),
            summary_text = VALUES(summary_text),
            snapshot_hash = VALUES(snapshot_hash),
            screenshot_hash = VALUES(screenshot_hash),
            recognized_count = VALUES(recognized_count),
            missing_count = VALUES(missing_count),
            updated_at = VALUES(updated_at)
        """
    )
    with engine.begin() as conn:
        conn.execute(
            sql,
            {
                "user_id": user_id,
                "industry_allocation_json": json.dumps(industry_alloc, ensure_ascii=False),
                "portfolio_corr_json": json.dumps(portfolio_corr, ensure_ascii=False),
                "summary_text": summary_text,
                "snapshot_hash": snapshot_hash,
                "screenshot_hash": screenshot_hash,
                "recognized_count": recognized_count,
                "missing_count": missing_count,
                "updated_at": updated_at,
            },
        )


def process_portfolio_snapshot(
    user_id: str,
    raw_positions: List[Dict[str, Any]],
    screenshot_hash: str = "",
    lookback_days: int = 120,
) -> Dict[str, Any]:
    if not user_id:
        return {"status": "error", "error": "缺少 user_id"}
    if engine is None:
        return {"status": "error", "error": "数据库连接不可用"}

    ensure_portfolio_tables()
    timestamp = _utc_now_str()
    normalized_positions = _normalize_positions(raw_positions)
    existing_hash_map = _load_existing_hashes(user_id)

    processed_rows: List[Dict[str, Any]] = []
    skipped_same_count = 0
    missing_count = 0

    for pos in normalized_positions:
        symbol = pos["symbol"]
        pos_hash = _compute_position_hash(pos.get("quantity"), pos.get("market_value"))
        old_hash = existing_hash_map.get(symbol)

        if old_hash and old_hash == pos_hash:
            _touch_position_seen(user_id, symbol, timestamp, screenshot_hash)
            skipped_same_count += 1
            continue

        name, industry = _fetch_name_industry(symbol, pos["market"], fallback_name=pos.get("name", ""))
        grade, reason = _analyze_technical(symbol=symbol, display_name=name)
        corr_map = _compute_index_corr(symbol, lookback_days=lookback_days)
        if not corr_map:
            missing_count += 1

        row = dict(pos)
        row.update(
            {
                "name": name or symbol,
                "industry": industry,
                "technical_grade": grade,
                "technical_reason": reason,
                "index_corr": corr_map,
                "position_hash": pos_hash,
            }
        )
        _upsert_position(user_id, row, timestamp, screenshot_hash)
        processed_rows.append(row)

    # 当前快照只读取本次 last_seen_at 批次，避免历史误识别 symbol 残留造成“重复持仓”。
    with engine.connect() as conn:
        all_rows = conn.execute(
            text(
                """
                SELECT symbol, name, market, quantity, market_value, price, cost_price,
                       industry, technical_grade, technical_reason, index_corr_json,
                       position_hash, last_seen_at, updated_at
                FROM user_portfolio_positions
                WHERE user_id = :uid AND last_seen_at = :ts
                ORDER BY market_value DESC
                """
            ),
            {"uid": user_id, "ts": timestamp},
        ).mappings().fetchall()

    position_rows: List[Dict[str, Any]] = []
    for r in all_rows:
        row = dict(r)
        corr_raw = row.get("index_corr_json")
        try:
            row["index_corr"] = json.loads(corr_raw) if corr_raw else {}
        except Exception:
            row["index_corr"] = {}
        position_rows.append(row)

    recognized_count = len(position_rows)
    industry_alloc = _build_industry_allocation(position_rows)
    portfolio_corr = _build_weighted_corr(position_rows)
    snapshot_hash = _build_snapshot_hash(position_rows)
    summary_text = _build_summary_text(
        recognized_count=recognized_count,
        missing_count=missing_count,
        industry_alloc=industry_alloc,
        portfolio_corr=portfolio_corr,
    )

    _upsert_snapshot(
        user_id=user_id,
        industry_alloc=industry_alloc,
        portfolio_corr=portfolio_corr,
        summary_text=summary_text,
        snapshot_hash=snapshot_hash,
        screenshot_hash=screenshot_hash,
        recognized_count=recognized_count,
        missing_count=missing_count,
        updated_at=timestamp,
    )

    retrieval_summary = (
        f"【持仓体检】用户{user_id}当前识别{recognized_count}只股票，"
        f"更新{len(processed_rows)}只，重复跳过{skipped_same_count}只。{summary_text}"
    )

    return {
        "status": "success",
        "recognized_count": recognized_count,
        "updated_count": len(processed_rows),
        "skipped_same_count": skipped_same_count,
        "missing_count": missing_count,
        "industry_allocation": industry_alloc,
        "portfolio_corr": portfolio_corr,
        "summary_text": summary_text,
        "snapshot_hash": snapshot_hash,
        "retrieval_summary": retrieval_summary,
        "positions": position_rows,
        "updated_at": timestamp,
    }


def get_user_portfolio_positions_df(user_id: str) -> pd.DataFrame:
    if engine is None or not user_id:
        return pd.DataFrame()
    ensure_portfolio_tables()
    sql = text(
        """
        SELECT symbol, name, market, quantity, market_value, price, cost_price,
               industry, technical_grade, technical_reason, index_corr_json,
               last_seen_at, updated_at
        FROM user_portfolio_positions
        WHERE user_id = :uid
          AND last_seen_at = (
              SELECT MAX(last_seen_at)
              FROM user_portfolio_positions
              WHERE user_id = :uid
          )
        ORDER BY market_value DESC
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"uid": user_id})
    if df.empty:
        return df
    if "index_corr_json" in df.columns:
        def _parse(v: Any) -> Dict[str, Any]:
            if not v:
                return {}
            try:
                return json.loads(v)
            except Exception:
                return {}
        df["index_corr"] = df["index_corr_json"].apply(_parse)
    return df


def get_user_portfolio_snapshot(user_id: str) -> Dict[str, Any]:
    if engine is None or not user_id:
        return {}
    ensure_portfolio_tables()
    sql = text("SELECT * FROM user_portfolio_snapshot WHERE user_id = :uid LIMIT 1")
    with engine.connect() as conn:
        row = conn.execute(sql, {"uid": user_id}).mappings().fetchone()
    if not row:
        return {}
    payload = dict(row)
    for key in ("industry_allocation_json", "portfolio_corr_json"):
        raw = payload.get(key)
        try:
            payload[key.replace("_json", "")] = json.loads(raw) if raw else {}
        except Exception:
            payload[key.replace("_json", "")] = {}
    return payload
