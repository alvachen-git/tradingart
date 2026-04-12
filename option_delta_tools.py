from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
import os
from pathlib import Path
import re
from typing import Any, Dict, List, Optional

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import text

try:
    from py_vollib_vectorized import vectorized_delta
except Exception:  # pragma: no cover
    vectorized_delta = None

ETF_UNDERLYING_MAP = {
    "50ETF": "510050.SH",
    "上证50": "510050.SH",
    "300ETF": "510300.SH",
    "沪深300": "510300.SH",
    "500ETF": "510500.SH",
    "中证500": "510500.SH",
    "创业板": "159915.SZ",
    "创业板ETF": "159915.SZ",
    "科创50": "588000.SH",
    "科创50ETF": "588000.SH",
}

ETF_MULTIPLIER = 10000

_RISK_PROFILE_MAP = {
    "保守型": "conservative",
    "稳健型": "balanced",
    "激进型": "aggressive",
}

_DELTA_TARGET_BANDS = {
    "conservative": {
        "bullish": (0.10, 0.25),
        "neutral": (-0.05, 0.05),
        "bearish": (-0.25, -0.10),
    },
    "balanced": {
        "bullish": (0.20, 0.40),
        "neutral": (-0.10, 0.10),
        "bearish": (-0.40, -0.20),
    },
    "aggressive": {
        "bullish": (0.50, 2.00),
        "neutral": (-0.20, 0.20),
        "bearish": (-2.00, -0.60),
    },
}

_SIDE_SIGN = {"买方": 1, "买入": 1, "卖方": -1, "卖出": -1}
_CP_MAP = {"认购": "c", "认沽": "p"}

_LEG_PATTERNS = [
    re.compile(
        r"(?:(?P<month>\d{1,2})月)?\s*(?P<strike>\d+(?:\.\d+)?)\s*"
        r"(?P<cp>认购|认沽)\s*(?P<side>买方|卖方|买入|卖出)\s*(?P<qty>\d+)\s*张"
    ),
    re.compile(
        r"(?:(?P<month>\d{1,2})月)?\s*(?P<strike>\d+(?:\.\d+)?)\s*"
        r"(?P<side>买方|卖方|买入|卖出)\s*(?P<cp>认购|认沽)\s*(?P<qty>\d+)\s*张"
    ),
]


def _load_project_env() -> None:
    """
    worktree 下运行时，.env 往往在上级主目录（.../tradingart/.env），
    这里做向上查找，避免因 cwd 不同导致 DB 配置丢失。
    """
    current_file = Path(__file__).resolve()
    for parent in [current_file.parent, *current_file.parents]:
        env_path = parent / ".env"
        if env_path.exists():
            load_dotenv(dotenv_path=env_path, override=False)
            return
    load_dotenv(override=False)


@lru_cache(maxsize=1)
def _get_db_engine_cached():
    _load_project_env()
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    if not all([db_user, db_password, db_host, db_name]):
        return None
    db_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    try:
        return create_engine(db_url, pool_pre_ping=True, pool_recycle=3600)
    except Exception:
        return None


@dataclass
class OptionLeg:
    month: Optional[int]
    strike: float
    cp_text: str
    side_text: str
    qty: int
    signed_qty: int
    option_flag: str
    source_text: str


def detect_etf_underlying(text: str, symbol_hint: str = "") -> str:
    text_u = f"{symbol_hint} {text}".upper()
    for name, code in ETF_UNDERLYING_MAP.items():
        if name.upper() in text_u:
            return code

    m = re.search(r"(510\d{3}|159\d{3}|588\d{3})", text_u)
    if m:
        raw = m.group(1)
        return f"{raw}.SZ" if raw.startswith("159") else f"{raw}.SH"

    hint = str(symbol_hint or "").strip().upper()
    if re.fullmatch(r"\d{6}(?:\.[A-Z]{2})?", hint):
        if "." in hint:
            return hint
        return f"{hint}.SZ" if hint.startswith("159") else f"{hint}.SH"
    return ""


def parse_etf_option_legs(text: str) -> List[Dict[str, Any]]:
    raw = str(text or "")
    legs: List[OptionLeg] = []
    taken_spans = set()

    for pattern in _LEG_PATTERNS:
        for m in pattern.finditer(raw):
            span = m.span()
            if span in taken_spans:
                continue
            taken_spans.add(span)
            month = m.group("month")
            cp_text = m.group("cp")
            side_text = m.group("side")
            qty = int(m.group("qty"))
            signed_qty = _SIDE_SIGN.get(side_text, 0) * qty
            flag = _CP_MAP.get(cp_text)
            if not flag or signed_qty == 0:
                continue
            legs.append(
                OptionLeg(
                    month=int(month) if month else None,
                    strike=float(m.group("strike")),
                    cp_text=cp_text,
                    side_text=side_text,
                    qty=qty,
                    signed_qty=signed_qty,
                    option_flag=flag,
                    source_text=m.group(0),
                )
            )

    legs.sort(key=lambda x: x.source_text)
    return [
        {
            "month": leg.month,
            "strike": leg.strike,
            "cp_text": leg.cp_text,
            "side_text": leg.side_text,
            "qty": leg.qty,
            "signed_qty": leg.signed_qty,
            "option_flag": leg.option_flag,
            "source_text": leg.source_text,
        }
        for leg in legs
    ]


class ETFOptionMarketLoader:
    def __init__(self, engine=None):
        self.engine = engine or _get_db_engine_cached()

    def get_underlying_spot(self, underlying_code: str) -> Optional[Dict[str, Any]]:
        if self.engine is None:
            return None
        try:
            sql = text(
                """
                SELECT ts_code, trade_date, close_price
                FROM stock_price
                WHERE ts_code = :code
                ORDER BY trade_date DESC
                LIMIT 1
                """
            )
            df = pd.read_sql(sql, self.engine, params={"code": underlying_code})
        except Exception:
            return None
        if df.empty:
            return None
        row = df.iloc[0]
        return {
            "ts_code": str(row.get("ts_code")),
            "trade_date": str(row.get("trade_date")),
            "close_price": float(row.get("close_price")),
        }

    def get_latest_iv(self, underlying_code: str) -> Optional[Dict[str, Any]]:
        if self.engine is None:
            return None
        try:
            sql = text(
                """
                SELECT etf_code, trade_date, iv
                FROM etf_iv_history
                WHERE etf_code = :code
                ORDER BY trade_date DESC
                LIMIT 1
                """
            )
            df = pd.read_sql(sql, self.engine, params={"code": underlying_code})
        except Exception:
            return None
        if df.empty:
            return None
        row = df.iloc[0]
        return {
            "etf_code": str(row.get("etf_code")),
            "trade_date": str(row.get("trade_date")),
            "iv": float(row.get("iv")),
        }

    def find_option_contract(
        self,
        underlying_code: str,
        option_flag: str,
        strike: float,
        month: Optional[int],
        as_of_yyyymmdd: str,
    ) -> Optional[Dict[str, Any]]:
        if self.engine is None:
            return None
        sql = text(
            """
            SELECT ts_code, call_put, exercise_price, delist_date
            FROM option_basic
            WHERE underlying = :underlying
              AND call_put = :cp
              AND delist_date >= :as_of_date
            ORDER BY delist_date ASC
            """
        )
        cp = option_flag.upper()
        try:
            df = pd.read_sql(
                sql,
                self.engine,
                params={"underlying": underlying_code, "cp": cp, "as_of_date": as_of_yyyymmdd},
            )
        except Exception:
            return {
                "missing_reason": "查询合约失败（数据源异常）",
                "status": "loader_error",
            }
        if df.empty:
            return None

        work = df.copy()
        work["strike_num"] = pd.to_numeric(work["exercise_price"], errors="coerce")
        work["month_num"] = work["delist_date"].astype(str).str[4:6].astype(int)
        work = work.dropna(subset=["strike_num"])
        if month is not None:
            month_work = work[work["month_num"] == int(month)]
            if month_work.empty:
                return {
                    "missing_reason": f"未找到{month}月可交易合约",
                    "status": "missing_month",
                }
            work = month_work

        work["diff"] = (work["strike_num"] - float(strike)).abs()
        best = work.sort_values(["diff", "delist_date"]).iloc[0]
        matched_exact = float(best["diff"]) < 1e-8

        px_sql = text(
            """
            SELECT trade_date, close, vol, oi
            FROM option_daily
            WHERE ts_code = :ts_code
            ORDER BY trade_date DESC
            LIMIT 1
            """
        )
        try:
            df_px = pd.read_sql(px_sql, self.engine, params={"ts_code": str(best["ts_code"])})
        except Exception:
            return {
                "missing_reason": f"合约{best['ts_code']}价格查询失败（数据源异常）",
                "status": "loader_error",
                "ts_code": str(best["ts_code"]),
                "delist_date": str(best["delist_date"]),
                "exercise_price": float(best["strike_num"]),
            }
        if df_px.empty:
            return {
                "missing_reason": f"合约{best['ts_code']}暂无最新收盘数据",
                "status": "missing_price",
                "ts_code": str(best["ts_code"]),
                "delist_date": str(best["delist_date"]),
                "exercise_price": float(best["strike_num"]),
            }
        row_px = df_px.iloc[0]
        return {
            "status": "ok",
            "ts_code": str(best["ts_code"]),
            "delist_date": str(best["delist_date"]),
            "exercise_price": float(best["strike_num"]),
            "is_exact_strike": matched_exact,
            "trade_date": str(row_px.get("trade_date")),
            "close": float(row_px.get("close")),
            "vol": float(row_px.get("vol") or 0),
            "oi": float(row_px.get("oi") or 0),
        }


def _normalize_iv(iv_value: Optional[float]) -> Optional[float]:
    if iv_value is None:
        return None
    iv = float(iv_value)
    if iv <= 0:
        return None
    if iv > 1:
        iv = iv / 100.0
    return max(iv, 1e-4)


def _norm_date_str(dt: Optional[str] = None) -> str:
    if dt:
        return str(dt).replace("-", "")[:8]
    return datetime.now().strftime("%Y%m%d")


def _annualized_t(as_of_yyyymmdd: str, delist_yyyymmdd: str) -> float:
    try:
        as_of_dt = datetime.strptime(as_of_yyyymmdd, "%Y%m%d")
        exp_dt = datetime.strptime(str(delist_yyyymmdd)[:8], "%Y%m%d")
    except Exception:
        return 1.0 / 365.0
    days = max((exp_dt - as_of_dt).days, 1)
    return max(days / 365.0, 1.0 / 365.0)


def compute_delta_cash_metrics(
    legs: List[Dict[str, Any]],
    underlying_price: float,
    multiplier: int = ETF_MULTIPLIER,
) -> Dict[str, float]:
    gross_notional = float(underlying_price) * float(multiplier) * sum(abs(int(l.get("signed_qty", 0))) for l in legs)
    total_delta_cash = 0.0
    valid = 0
    for leg in legs:
        delta = leg.get("delta")
        if delta is None:
            continue
        valid += 1
        total_delta_cash += float(leg.get("signed_qty", 0)) * float(delta) * float(underlying_price) * float(multiplier)
    delta_ratio = total_delta_cash / gross_notional if gross_notional > 0 else 0.0
    coverage_ratio = valid / len(legs) if legs else 0.0
    return {
        "total_delta_cash": total_delta_cash,
        "gross_notional": gross_notional,
        "delta_ratio": delta_ratio,
        "coverage_ratio": coverage_ratio,
    }


def _normalize_risk_profile(risk_preference: str) -> str:
    text = str(risk_preference or "")
    for cn, key in _RISK_PROFILE_MAP.items():
        if cn in text:
            return key
    text_l = text.lower()
    if "aggress" in text_l:
        return "aggressive"
    if "conserv" in text_l:
        return "conservative"
    return "balanced"


def _normalize_trend_signal(trend_signal: str) -> str:
    text = str(trend_signal or "")
    if any(k in text for k in ["看涨", "多头", "上涨", "bull", "Bull"]):
        return "bullish"
    if any(k in text for k in ["看跌", "空头", "下跌", "bear", "Bear"]):
        return "bearish"
    return "neutral"


def get_delta_target_band(trend_signal: str, risk_preference: str) -> Dict[str, Any]:
    risk_key = _normalize_risk_profile(risk_preference)
    trend_key = _normalize_trend_signal(trend_signal)
    low, high = _DELTA_TARGET_BANDS[risk_key][trend_key]
    return {
        "risk_key": risk_key,
        "trend_key": trend_key,
        "low": float(low),
        "high": float(high),
        "mid": float((low + high) / 2.0),
    }


def build_delta_adjustment(
    total_delta_cash: float,
    gross_notional: float,
    trend_signal: str,
    risk_preference: str,
) -> Dict[str, Any]:
    band = get_delta_target_band(trend_signal=trend_signal, risk_preference=risk_preference)
    current_ratio = float(total_delta_cash) / float(gross_notional) if gross_notional > 0 else 0.0
    mid = band["mid"]
    adjust_cash = (mid - current_ratio) * float(gross_notional) if gross_notional > 0 else 0.0
    in_band = band["low"] <= current_ratio <= band["high"]
    if gross_notional <= 0:
        action = "名义敞口为0，无法计算调整量"
    elif adjust_cash > 0:
        action = "需提高 Delta Cash（减空/加多）"
    elif adjust_cash < 0:
        action = "需降低 Delta Cash（减多/加空）"
    else:
        action = "当前已接近目标，无需调整"
    return {
        "band": band,
        "current_ratio": current_ratio,
        "target_mid": mid,
        "adjust_cash": adjust_cash,
        "in_band": in_band,
        "action": action,
    }


def _calc_delta(
    option_flag: str,
    spot: float,
    strike: float,
    t: float,
    r: float,
    sigma: float,
    q: float,
) -> Optional[float]:
    if vectorized_delta is None:
        return None
    try:
        res = vectorized_delta(
            flag=[option_flag],
            S=[spot],
            K=[strike],
            t=[t],
            r=[r],
            sigma=[sigma],
            q=[q],
            model="black_scholes",
            return_as="numpy",
        )
        if len(res) == 0:
            return None
        val = float(res[0])
        if val != val:  # NaN
            return None
        return val
    except Exception:
        return None


def _fmt_money(v: float) -> str:
    return f"{float(v):,.0f}"


def _build_delta_report(
    underlying_code: str,
    spot_info: Optional[Dict[str, Any]],
    iv_info: Optional[Dict[str, Any]],
    legs: List[Dict[str, Any]],
    metrics: Dict[str, float],
    adjustment: Dict[str, Any],
    missing_notes: List[str],
) -> str:
    lines = []
    lines.append("### 【DeltaCash】")
    if spot_info:
        lines.append(
            f"- 标的: `{underlying_code}` | 现价: `{spot_info['close_price']:.4f}` | 日期: `{spot_info['trade_date']}`"
        )
    if iv_info:
        iv_raw = float(iv_info.get("iv", 0))
        lines.append(f"- 使用IV: `{iv_raw:.2f}%` | 日期: `{iv_info.get('trade_date')}`")
    if missing_notes:
        for note in missing_notes:
            lines.append(f"- 数据缺口: {note}")

    lines.append("")
    lines.append("| 腿 | 月份 | 方向 | 张数 | 行权价 | Delta | Delta Cash(元) | 状态 |")
    lines.append("|---|---:|---|---:|---:|---:|---:|---|")
    for i, leg in enumerate(legs, start=1):
        month_text = f"{leg.get('month')}月" if leg.get("month") else "待确认"
        direction = f"{leg.get('cp_text')}{leg.get('side_text')}"
        delta = leg.get("delta")
        delta_text = f"{delta:+.4f}" if isinstance(delta, (int, float)) else "待确认"
        cash = leg.get("delta_cash")
        cash_text = _fmt_money(cash) if isinstance(cash, (int, float)) else "待确认"
        status = str(leg.get("status") or "")
        lines.append(
            f"| {i} | {month_text} | {direction} | {int(leg.get('signed_qty', 0)):+d} | "
            f"{float(leg.get('strike', 0)):.3f} | {delta_text} | {cash_text} | {status} |"
        )

    lines.append("")
    lines.append(f"- Total Delta Cash: `{_fmt_money(metrics.get('total_delta_cash', 0.0))}` 元")
    lines.append(f"- Gross Notional: `{_fmt_money(metrics.get('gross_notional', 0.0))}` 元")
    lines.append(f"- Delta Ratio: `{metrics.get('delta_ratio', 0.0):+.4f}`")
    lines.append(f"- 覆盖率: `{metrics.get('coverage_ratio', 0.0) * 100:.1f}%`")

    band = adjustment.get("band", {})
    lines.append(
        f"- 技术面目标区间: `[{band.get('low', 0.0):+.2f}, {band.get('high', 0.0):+.2f}]` "
        f"(目标中点 `{adjustment.get('target_mid', 0.0):+.2f}`)"
    )
    lines.append(
        f"- 当前偏离: `{adjustment.get('current_ratio', 0.0) - adjustment.get('target_mid', 0.0):+.4f}`"
    )
    lines.append(
        f"- 建议调整量: `{_fmt_money(adjustment.get('adjust_cash', 0.0))}` 元 | {adjustment.get('action', '')}"
    )
    return "\n".join(lines)


def compute_etf_option_delta_cash(
    user_query: str,
    symbol_hint: str = "",
    trend_signal: str = "",
    risk_preference: str = "稳健型",
    loader: Optional[ETFOptionMarketLoader] = None,
    as_of_date: Optional[str] = None,
    r: float = 0.015,
    q: float = 0.0,
) -> Dict[str, Any]:
    as_of_yyyymmdd = _norm_date_str(as_of_date)
    underlying_code = detect_etf_underlying(user_query, symbol_hint=symbol_hint)
    if not underlying_code:
        return {
            "is_etf": False,
            "legs": [],
            "metrics": {"total_delta_cash": 0.0, "gross_notional": 0.0, "delta_ratio": 0.0, "coverage_ratio": 0.0},
            "adjustment": build_delta_adjustment(0.0, 0.0, trend_signal, risk_preference),
            "missing_notes": [],
            "blocking_missing_notes": [],
            "publishable": False,
            "report": "",
        }

    parsed_legs = parse_etf_option_legs(user_query)
    if not parsed_legs:
        return {
            "is_etf": True,
            "underlying_code": underlying_code,
            "legs": [],
            "metrics": {"total_delta_cash": 0.0, "gross_notional": 0.0, "delta_ratio": 0.0, "coverage_ratio": 0.0},
            "adjustment": build_delta_adjustment(0.0, 0.0, trend_signal, risk_preference),
            "missing_notes": ["未识别到可计算的ETF期权持仓腿"],
            "blocking_missing_notes": ["未识别到可计算的ETF期权持仓腿"],
            "publishable": False,
            "report": "### 【DeltaCash】\n- 数据缺口: 未识别到可计算的ETF期权持仓腿，请补充“月份/行权价/认购认沽/买卖方向/张数”。",
        }

    market_loader = loader or ETFOptionMarketLoader()
    try:
        spot_info = market_loader.get_underlying_spot(underlying_code)
    except Exception:
        spot_info = None
    try:
        iv_info = market_loader.get_latest_iv(underlying_code)
    except Exception:
        iv_info = None
    sigma = _normalize_iv(iv_info.get("iv")) if iv_info else None

    missing_notes: List[str] = []
    blocking_notes: List[str] = []
    if not spot_info:
        msg = "未找到标的最新收盘价"
        missing_notes.append(msg)
        blocking_notes.append(msg)
    if sigma is None:
        msg = "未找到可用IV数据"
        missing_notes.append(msg)
        blocking_notes.append(msg)

    legs_out: List[Dict[str, Any]] = []
    for leg in parsed_legs:
        leg_out = dict(leg)
        leg_out.update({"status": "ok", "delta": None, "delta_cash": None})
        try:
            contract = market_loader.find_option_contract(
                underlying_code=underlying_code,
                option_flag=str(leg["option_flag"]),
                strike=float(leg["strike"]),
                month=leg.get("month"),
                as_of_yyyymmdd=as_of_yyyymmdd,
            )
        except Exception:
            contract = {"status": "loader_error", "missing_reason": "合约查询异常"}
        if not contract:
            leg_out["status"] = "缺合约"
            leg_out["missing_reason"] = "未找到可交易合约"
            blocking_notes.append("存在未匹配合约腿")
            legs_out.append(leg_out)
            continue

        leg_out.update(
            {
                "ts_code": contract.get("ts_code"),
                "matched_strike": contract.get("exercise_price"),
                "delist_date": contract.get("delist_date"),
                "option_close": contract.get("close"),
                "option_trade_date": contract.get("trade_date"),
                "match_exact": contract.get("is_exact_strike"),
            }
        )
        if contract.get("status") != "ok":
            leg_out["status"] = "数据缺口"
            leg_out["missing_reason"] = contract.get("missing_reason", "合约信息缺失")
            if contract.get("status") in {"missing_price", "loader_error"}:
                blocking_notes.append(str(leg_out["missing_reason"]))
            legs_out.append(leg_out)
            continue

        if not spot_info or sigma is None:
            leg_out["status"] = "数据缺口"
            leg_out["missing_reason"] = "缺现价或IV，无法计算Delta"
            blocking_notes.append(str(leg_out["missing_reason"]))
            legs_out.append(leg_out)
            continue

        t = _annualized_t(as_of_yyyymmdd, str(contract.get("delist_date")))
        delta = _calc_delta(
            option_flag=str(leg["option_flag"]),
            spot=float(spot_info["close_price"]),
            strike=float(contract.get("exercise_price")),
            t=t,
            r=float(r),
            sigma=float(sigma),
            q=float(q),
        )
        if delta is None:
            leg_out["status"] = "Delta失败"
            leg_out["missing_reason"] = "Delta计算失败"
            blocking_notes.append(str(leg_out["missing_reason"]))
            legs_out.append(leg_out)
            continue

        leg_out["delta"] = float(delta)
        leg_out["delta_cash"] = (
            float(leg["signed_qty"]) * float(delta) * float(spot_info["close_price"]) * float(ETF_MULTIPLIER)
        )
        if leg_out.get("match_exact") is False:
            leg_out["status"] = "近似行权价"
            leg_out["missing_reason"] = "未命中精确行权价，已就近匹配"
        else:
            leg_out["status"] = "已计算"
        legs_out.append(leg_out)

    spot_for_metrics = float(spot_info["close_price"]) if spot_info else 0.0
    metrics = compute_delta_cash_metrics(legs_out, underlying_price=spot_for_metrics, multiplier=ETF_MULTIPLIER)
    adjustment = build_delta_adjustment(
        total_delta_cash=metrics["total_delta_cash"],
        gross_notional=metrics["gross_notional"],
        trend_signal=trend_signal,
        risk_preference=risk_preference,
    )
    report = _build_delta_report(
        underlying_code=underlying_code,
        spot_info=spot_info,
        iv_info=iv_info,
        legs=legs_out,
        metrics=metrics,
        adjustment=adjustment,
        missing_notes=missing_notes,
    )
    if metrics.get("coverage_ratio", 0.0) < 1.0:
        blocking_notes.append("Delta覆盖率不足")

    # 去重并保持顺序
    dedup_blocking = list(dict.fromkeys([x for x in blocking_notes if str(x).strip()]))
    dedup_missing = list(dict.fromkeys([x for x in missing_notes if str(x).strip()]))
    if dedup_blocking:
        for note in dedup_blocking:
            if note not in dedup_missing:
                dedup_missing.append(note)

    return {
        "is_etf": True,
        "underlying_code": underlying_code,
        "legs": legs_out,
        "metrics": metrics,
        "adjustment": adjustment,
        "missing_notes": dedup_missing,
        "blocking_missing_notes": dedup_blocking,
        "publishable": (len(dedup_blocking) == 0 and bool(legs_out)),
        "report": report,
    }
