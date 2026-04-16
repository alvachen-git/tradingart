import os
import base64
from http import HTTPStatus
import json
import re
from io import BytesIO
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import dashscope
try:
    from PIL import Image
    from PIL import ImageEnhance
except Exception:  # pragma: no cover
    Image = None
    ImageEnhance = None


def _extract_json_text(raw_text: str) -> str:
    text = str(raw_text or "").strip()
    if not text:
        return ""

    code_block = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", text, flags=re.IGNORECASE)
    if code_block:
        return code_block.group(1).strip()

    if text.startswith("{") and text.endswith("}"):
        return text

    start = text.find("{")
    if start < 0:
        return ""

    depth = 0
    for idx in range(start, len(text)):
        ch = text[idx]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : idx + 1]
    return ""


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text:
        return None

    scale = 1.0
    if text.endswith("亿"):
        scale = 1e8
        text = text[:-1]
    elif text.endswith("万"):
        scale = 1e4
        text = text[:-1]

    text = text.replace(",", "")
    text = text.replace("，", "")
    text = text.replace("元", "")
    text = text.replace("股", "")
    text = text.replace("HK$", "")
    text = text.replace("$", "")
    text = text.replace("￥", "")
    text = text.replace("¥", "")
    text = text.strip()

    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(0)) * scale
    except Exception:
        return None


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


def _first_non_empty_value(item: Dict[str, Any], keys: List[str]) -> Any:
    for key in keys:
        if key not in item:
            continue
        value = item.get(key)
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def _normalize_symbol_and_market(symbol: Any, market: Any = None) -> Tuple[str, str]:
    raw_symbol = str(symbol or "").strip().upper().replace(" ", "")
    raw_market = str(market or "").strip().upper()
    raw_symbol = raw_symbol.replace("SHSE:", "").replace("SZSE:", "")

    if re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", raw_symbol):
        return raw_symbol, "A"
    if re.fullmatch(r"\d{5}\.HK", raw_symbol):
        return raw_symbol, "HK"
    if re.fullmatch(r"\d{6}", raw_symbol):
        if raw_symbol.startswith(("6", "5", "9")):
            return f"{raw_symbol}.SH", "A"
        if raw_symbol.startswith(("0", "1", "2", "3")):
            return f"{raw_symbol}.SZ", "A"
        return f"{raw_symbol}.BJ", "A"
    if re.fullmatch(r"\d{5}", raw_symbol):
        return f"{raw_symbol}.HK", "HK"

    if raw_market in ("HK", "HONGKONG"):
        if re.fullmatch(r"\d{5}", raw_symbol):
            return f"{raw_symbol}.HK", "HK"
        return raw_symbol, "HK"
    if raw_market in ("A", "CN", "CHINA", "ASHARE"):
        return raw_symbol, "A"

    if raw_symbol.endswith(".HK"):
        return raw_symbol, "HK"
    if raw_symbol.endswith((".SH", ".SZ", ".BJ")):
        return raw_symbol, "A"
    return raw_symbol, "A"


def _option_contract_symbol_like(symbol: Any) -> bool:
    token = str(symbol or "").strip().upper().replace(" ", "")
    token = token.replace("SHSE:", "").replace("SZSE:", "").replace(".SSE", ".SH").replace(".SS", ".SH")
    if re.fullmatch(r"\d{7,9}\.(SH|SZ)", token):
        return True
    if re.fullmatch(r"\d{7,9}", token):
        return True
    if re.fullmatch(r"(IO|HO|MO)\d{3,6}", token):
        return True
    return False


def _option_name_like(name: Any) -> bool:
    text = str(name or "").strip()
    if not text:
        return False
    return any(kw in text for kw in ["期权", "认购", "认沽", "ETF购", "ETF沽", "购", "沽", "CALL", "PUT"])


def _infer_option_cp_from_text(text: str) -> str:
    t = str(text or "").upper()
    if any(x in t for x in ["认购", "ETF购", "CALL", " C ", "购"]):
        return "call"
    if any(x in t for x in ["认沽", "ETF沽", "PUT", " P ", "沽"]):
        return "put"
    return ""


def _derive_option_cn_labels(cp: Any, side: Any, signed_qty: Any = None) -> Dict[str, str]:
    cp_raw = str(cp or "").strip().lower()
    side_raw = str(side or "").strip().lower()
    cp_cn = "认购" if cp_raw in {"call", "c", "认购"} else ("认沽" if cp_raw in {"put", "p", "认沽"} else "待确认")
    if side_raw in {"long", "买方", "买入"}:
        side_cn = "买方"
    elif side_raw in {"short", "卖方", "卖出"}:
        side_cn = "卖方"
    else:
        qty = _to_float(signed_qty)
        if qty is not None:
            side_cn = "买方" if qty >= 0 else "卖方"
        else:
            side_cn = "待确认"
    if cp_cn == "待确认" or side_cn == "待确认":
        direction_cn = "待确认"
    else:
        direction_cn = ("买" if side_cn == "买方" else "卖") + cp_cn
    return {"cp_cn": cp_cn, "side_cn": side_cn, "direction_cn": direction_cn}


def _attach_option_cn_labels(leg: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(leg or {})
    out.update(_derive_option_cn_labels(out.get("cp"), out.get("side"), out.get("signed_qty")))
    return out


def _normalize_option_contract_code(value: Any) -> str:
    raw = str(value or "").strip().upper().replace(" ", "")
    if not raw:
        return ""
    raw = raw.replace("SHSE:", "").replace("SZSE:", "")
    raw = raw.replace(".SSE", ".SH").replace(".SS", ".SH")
    return raw


def _decode_side_text(value: Any) -> str:
    txt = str(value or "").strip()
    if not txt:
        return ""
    t = txt.upper().replace(" ", "")
    if t in {"买卖", "买/卖", "方向", "--", "-"}:
        return ""
    if any(k in t for k in ["SHORT", "SELL", "卖方", "卖出", "做空", "空头"]):
        return "short"
    if any(k in t for k in ["LONG", "BUY", "买方", "买入", "做多", "多头"]):
        return "long"
    if "卖" in t and "买" not in t:
        return "short"
    if "买" in t and "卖" not in t:
        return "long"
    if t == "卖":
        return "short"
    if t == "买":
        return "long"
    return ""


def _infer_option_side_from_item(item: Dict[str, Any], quantity: Optional[float] = None) -> str:
    primary = _first_non_empty_value(
        item,
        [
            "side",
            "position_side",
            "买卖方向",
            "买卖",
            "方向",
            "持仓方向",
            "操作",
            "开平方向",
        ],
    )
    guessed = _decode_side_text(primary)
    if guessed:
        return guessed

    # 次级兜底：扫描所有字段里可疑的“买/卖”值，覆盖OCR键名漂移。
    for _k, v in item.items():
        guessed = _decode_side_text(v)
        if guessed:
            return guessed

    if quantity is not None:
        q = float(quantity)
        if q < 0:
            return "short"
    return ""


def _extract_month_and_strike_from_option_name(name: Any, underlying_hint: str = "") -> Tuple[Optional[int], Optional[float]]:
    text = str(name or "").strip()
    if not text:
        return None, None
    normalized = (
        text.upper()
        .replace("O", "0")
        .replace("〇", "0")
        .replace("I", "1")
        .replace("L", "1")
        .replace("B", "8")
    )
    month = None
    strike = None

    # 常见格式：创业板ETF购4月3400 / 50ETF沽4月2900 / IO购4月4000
    m = re.search(r"(?P<month>\d{1,2})\s*月\s*(?P<strike>\d{3,6}(?:\.\d+)?)", normalized)
    if not m:
        month_m = re.search(r"(?P<month>\d{1,2})\s*月", normalized)
        if month_m:
            try:
                month = int(month_m.group("month"))
            except Exception:
                month = None
        strike_candidates = re.findall(r"\d+(?:\.\d+)?", normalized)
        for token in reversed(strike_candidates):
            token_digits = token.replace(".", "")
            if len(token_digits) >= 3:
                m = re.match(r"(?P<strike>\d+(?:\.\d+)?)", token)
                break
    if m:
        if m.groupdict().get("month"):
            try:
                month = int(m.group("month"))
            except Exception:
                month = None
        strike_raw = m.group("strike") if "strike" in m.groupdict() else m.group(1)
        strike_num = _to_float(strike_raw)
        if strike_num is not None:
            hint = _normalize_underlying_hint(underlying_hint)
            # 股指期权行权价通常是整数点位；ETF期权常见为x1000编码（如3400->3.4）
            if hint in {"IO", "HO", "MO"}:
                strike = float(strike_num)
            else:
                strike = float(strike_num / 1000.0) if strike_num >= 1000 else float(strike_num)
    return month, strike


def _normalize_position_item(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    symbol_raw = _first_non_empty_value(item, ["symbol", "code", "ticker", "股票代码", "代码", "证券代码", "合约代码"])
    name = _first_non_empty_value(item, ["name", "stock_name", "security_name", "股票名称", "名称", "证券名称", "合约名称"]) or ""
    market_raw = _first_non_empty_value(item, ["market", "exchange", "市场", "市场名", "交易所"])

    # 期权合约编码或购沽名称，不应落入股票持仓解析。
    if _option_contract_symbol_like(symbol_raw) or _option_name_like(name):
        return None

    symbol, market = _normalize_symbol_and_market(symbol_raw, market_raw)
    if not symbol:
        return None

    # 数量优先取“实际数量/持仓数量”，最后才退化到股票余额/可用余额。
    quantity = _first_numeric_value(
        item,
        [
            "quantity",
            "actual_quantity",
            "实际数量",
            "实有数量",
            "持仓数量",
            "实际持仓",
            "shares",
            "qty",
            "股票余额",
            "可用余额",
        ],
    )
    market_value = _first_numeric_value(
        item,
        [
            "market_value",
            "position_value",
            "持仓市值",
            "市值",
        ],
    )
    cost_price = _first_numeric_value(item, ["cost_price", "成本价", "买入价"])
    price = _first_numeric_value(item, ["price", "市价", "最新价", "现价"])

    if quantity is None and market_value is None:
        return None

    return {
        "symbol": symbol,
        "name": str(name).strip(),
        "market": market,
        "quantity": quantity,
        "market_value": market_value,
        "cost_price": cost_price,
        "price": price,
    }


def _normalize_option_leg_from_stocklike_item(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    symbol_raw = _first_non_empty_value(item, ["symbol", "code", "ticker", "股票代码", "代码", "证券代码", "合约代码"]) or ""
    name = _first_non_empty_value(item, ["name", "stock_name", "security_name", "股票名称", "名称", "证券名称", "合约名称"]) or ""
    if not (_option_contract_symbol_like(symbol_raw) or _option_name_like(name)):
        return None

    quantity = _first_numeric_value(
        item,
        [
            "quantity",
            "actual_quantity",
            "实际数量",
            "实有数量",
            "持仓数量",
            "持仓",
            "持仓量",
            "数量",
            "shares",
            "qty",
        ],
    )
    if quantity is None:
        return None
    qty_int = int(round(abs(float(quantity))))
    if qty_int <= 0:
        return None

    strike = _first_numeric_value(item, ["strike", "exercise_price", "行权价", "执行价"])
    month = _first_numeric_value(item, ["month", "expiry_month", "到期月", "月份"])
    name_text = str(name or "")
    cp = _infer_option_cp_from_text(
        str(
            _first_non_empty_value(item, ["类别", "认购认沽", "cp", "option_type", "type", "方向"])
            or name_text
        )
    )
    side = _infer_option_side_from_item(item, quantity=quantity)
    underlying_hint = _normalize_underlying_hint(
        item.get("underlying_hint")
        or item.get("underlying")
        or item.get("标的")
        or _detect_underlying_hint_from_text(name_text)
    )
    month_from_name, strike_from_name = _extract_month_and_strike_from_option_name(name_text, underlying_hint=underlying_hint)
    if month is None and month_from_name is not None:
        month = month_from_name
    if strike is None and strike_from_name is not None:
        strike = strike_from_name
    contract_code = _normalize_option_contract_code(symbol_raw)
    leg = {
        "underlying_hint": underlying_hint,
        "month": int(month) if month is not None else None,
        "strike": float(strike) if strike is not None else None,
        "cp": cp or None,
        "side": side or None,
        "qty": qty_int,
        "signed_qty": (-qty_int if side == "short" else qty_int),
        "contract_code": contract_code or None,
    }
    return _attach_option_cn_labels(leg)


def _fallback_parse_positions(raw_text: str) -> List[Dict[str, Any]]:
    positions: List[Dict[str, Any]] = []
    lines = [ln.strip() for ln in str(raw_text or "").splitlines() if ln.strip()]
    code_pattern = re.compile(r"(\d{6}\.(?:SH|SZ|BJ)|\d{5}\.HK|\d{6}|\d{5})", re.IGNORECASE)
    num_pattern = re.compile(r"-?\d+(?:,\d{3})*(?:\.\d+)?")

    for line in lines:
        code_match = code_pattern.search(line)
        if not code_match:
            continue
        if _option_name_like(line):
            continue
        code = code_match.group(1).upper()
        numbers = [n.replace(",", "") for n in num_pattern.findall(line)]
        quantity = float(numbers[0]) if len(numbers) >= 1 else None
        market_value = float(numbers[1]) if len(numbers) >= 2 else None
        symbol, market = _normalize_symbol_and_market(code, None)
        left_text = line[: code_match.start()].strip()
        name = left_text if left_text else symbol
        pos = _normalize_position_item(
            {
                "symbol": symbol,
                "name": name,
                "market": market,
                "quantity": quantity,
                "market_value": market_value,
            }
        )
        if pos:
            positions.append(pos)
    return positions


def parse_portfolio_json_response(raw_text: str) -> Dict[str, Any]:
    parsed: Dict[str, Any] = {"positions": [], "warnings": []}
    json_text = _extract_json_text(raw_text)
    if not json_text:
        parsed["warnings"].append("视觉输出非JSON，已尝试文本兜底解析")
        parsed["positions"] = _fallback_parse_positions(raw_text)
        return parsed

    try:
        payload = json.loads(json_text)
    except Exception as e:
        parsed["warnings"].append(f"JSON解析失败: {e}")
        parsed["positions"] = _fallback_parse_positions(raw_text)
        return parsed

    raw_positions = payload.get("positions", [])
    if not isinstance(raw_positions, list):
        parsed["warnings"].append("positions字段缺失或类型错误")
        raw_positions = []

    normalized: List[Dict[str, Any]] = []
    for item in raw_positions:
        if not isinstance(item, dict):
            continue
        row = _normalize_position_item(item)
        if row:
            normalized.append(row)

    parsed["positions"] = normalized
    if not normalized:
        parsed["warnings"].append("结构化结果为空，已尝试文本兜底解析")
        parsed["positions"] = _fallback_parse_positions(raw_text)
    return parsed


_OPTION_CP_MAP = {
    "call": "call",
    "认购": "call",
    "购": "call",
    "c": "call",
    "put": "put",
    "认沽": "put",
    "沽": "put",
    "p": "put",
}

_OPTION_SIDE_MAP = {
    "long": "long",
    "买方": "long",
    "买入": "long",
    "买": "long",
    "buy": "long",
    "short": "short",
    "卖方": "short",
    "卖出": "short",
    "卖": "short",
    "sell": "short",
}

_OPTION_UNDERLYING_HINTS = {
    "创业板": "159915.SZ",
    "创业板ETF": "159915.SZ",
    "沪深300ETF": "510300.SH",
    "300ETF": "510300.SH",
    "510300": "510300.SH",
    "上证50ETF": "510050.SH",
    "50ETF": "510050.SH",
    "510050": "510050.SH",
    "中证500ETF": "510500.SH",
    "500ETF": "510500.SH",
    "510500": "510500.SH",
    "科创50": "588000.SH",
    "科创50ETF": "588000.SH",
    "588000": "588000.SH",
    "IO": "IO",
    "HO": "HO",
    "MO": "MO",
}


def _normalize_underlying_hint(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if not raw:
        return ""
    if raw in {"IO", "HO", "MO"}:
        return raw
    if raw in {"000300", "000300.SH"}:
        return "IO"
    if raw in {"000016", "000016.SH"}:
        return "HO"
    if raw in {"000852", "000852.SH"}:
        return "MO"
    if re.fullmatch(r"(510\d{3}|588\d{3})", raw):
        return f"{raw}.SH"
    if re.fullmatch(r"159\d{3}", raw):
        return f"{raw}.SZ"
    if re.fullmatch(r"\d{6}\.(SH|SZ)", raw):
        return raw
    return _OPTION_UNDERLYING_HINTS.get(raw, raw)


def _normalize_option_leg_item(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    name_text = str(
        _first_non_empty_value(item, ["name", "stock_name", "security_name", "股票名称", "名称", "证券名称", "合约名称"])
        or ""
    )
    cp_raw = (
        item.get("cp")
        or item.get("option_type")
        or item.get("type")
        or item.get("类别")
        or item.get("认购认沽")
        or item.get("方向")
    )
    side_raw = (
        item.get("side")
        or item.get("position_side")
        or item.get("买卖方向")
        or item.get("买卖")
    )
    strike = _first_numeric_value(item, ["strike", "exercise_price", "行权价", "执行价"])
    qty = _first_numeric_value(item, ["qty", "quantity", "张数", "手数", "持仓", "持仓量", "数量"])
    month = _first_numeric_value(item, ["month", "expiry_month", "到期月", "月份"])
    cp = _OPTION_CP_MAP.get(str(cp_raw or "").strip().lower(), "")
    if not cp:
        cp = _infer_option_cp_from_text(f"{cp_raw or ''} {name_text}")
    side = _OPTION_SIDE_MAP.get(str(side_raw or "").strip().lower(), "")
    if not side:
        side = _infer_option_side_from_item(item, quantity=qty)
    underlying_hint = _normalize_underlying_hint(
        item.get("underlying_hint")
        or item.get("underlying")
        or item.get("symbol")
        or item.get("标的")
        or item.get("标的代码")
        or _detect_underlying_hint_from_text(name_text)
    )
    contract_code = _normalize_option_contract_code(item.get("contract_code") or item.get("ts_code") or item.get("合约代码"))
    if not contract_code:
        contract_code = _normalize_option_contract_code(item.get("代码") or item.get("证券代码"))

    if strike is None:
        month_from_name, strike_from_name = _extract_month_and_strike_from_option_name(name_text, underlying_hint=underlying_hint)
        if strike_from_name is not None:
            strike = strike_from_name
        if month is None and month_from_name is not None:
            month = month_from_name

    if qty is None:
        return None
    if strike is None and not contract_code:
        return None
    if not cp:
        return None
    qty_int = int(round(abs(float(qty))))
    if qty_int <= 0:
        return None
    month_int = int(month) if month is not None else None
    signed_qty = -qty_int if side == "short" else qty_int
    return _attach_option_cn_labels(
        {
            "underlying_hint": underlying_hint,
            "month": month_int,
            "strike": float(strike) if strike is not None else None,
            "cp": cp,
            "side": side or None,
            "qty": qty_int,
            "signed_qty": signed_qty,
            "contract_code": contract_code or None,
        }
    )


def _detect_underlying_hint_from_text(raw_text: str) -> str:
    text = str(raw_text or "").upper()
    if not text:
        return ""
    text_n = (
        text.replace("Ｏ", "0")
        .replace("O", "0")
        .replace("I", "1")
        .replace("L", "1")
        .replace("B", "8")
        .replace("S", "5")
    )

    # 长词优先，避免 "500ETF" 被 "50ETF" 误命中等重叠别名问题。
    alias_items = sorted(_OPTION_UNDERLYING_HINTS.items(), key=lambda kv: len(str(kv[0])), reverse=True)
    for key, value in alias_items:
        key_u = str(key).upper()
        if not key_u:
            continue
        key_n = (
            key_u.replace("Ｏ", "0")
            .replace("O", "0")
            .replace("I", "1")
            .replace("L", "1")
            .replace("B", "8")
            .replace("S", "5")
        )
        if re.fullmatch(r"\d{6}", key_u):
            if re.search(rf"(?<!\d){re.escape(key_u)}(?!\d)", text) or re.search(rf"(?<!\d){re.escape(key_n)}(?!\d)", text_n):
                return value
            continue
        if key_u in text or key_n in text_n:
            return value
    code_match = re.search(r"(510\d{3}|159\d{3}|588\d{3})", text)
    if code_match:
        return _normalize_underlying_hint(code_match.group(1))
    if "沪深300" in text:
        return "IO"
    if "上证50" in text:
        return "HO"
    if "中证1000" in text:
        return "MO"
    return ""


def _fallback_parse_option_legs(raw_text: str) -> List[Dict[str, Any]]:
    text = str(raw_text or "")
    hint = _detect_underlying_hint_from_text(text)
    legs: List[Dict[str, Any]] = []
    patterns = [
        re.compile(
            r"(?:(?P<month>\d{1,2})月)?\s*(?P<strike>\d+(?:\.\d+)?)\s*(?P<cp>认购|认沽)\s*"
            r"(?P<side>买方|卖方|买入|卖出)\s*(?P<qty>\d+)\s*(?:张|手)"
        ),
        re.compile(
            r"(?P<underlying>IO|HO|MO)\s*\d{0,4}\s*(?:(?P<month>\d{1,2})月)?\s*"
            r"(?P<strike>\d+(?:\.\d+)?)\s*(?P<cp>认购|认沽|CALL|PUT|C|P)\s*"
            r"(?P<side>买方|卖方|买入|卖出|LONG|SHORT|BUY|SELL)\s*(?P<qty>\d+)\s*(?:张|手)?",
            flags=re.IGNORECASE,
        ),
    ]
    for pattern in patterns:
        for match in pattern.finditer(text):
            cp_raw = str(match.group("cp") or "").upper()
            side_raw = str(match.group("side") or "").upper()
            leg = _normalize_option_leg_item(
                {
                    "underlying_hint": match.groupdict().get("underlying") or hint,
                    "month": match.group("month"),
                    "strike": match.group("strike"),
                    "cp": "call" if cp_raw in {"认购", "CALL", "C"} else "put",
                    "side": "long" if side_raw in {"买方", "买入", "LONG", "BUY"} else "short",
                    "qty": match.group("qty"),
                }
            )
            if leg:
                legs.append(leg)

    # 兜底1.5：券商表格行（代码 名称 类别 买卖 持仓 ...）
    table_line = re.compile(
        r"(?P<code>\d{7,9}(?:\.(?:SH|SZ))?)\s+"
        r"(?P<name>[^\s\n]*?(?:购|沽)[^\s\n]*)\s*"
        r"(?P<cp>认购|认沽|购|沽|CALL|PUT)?\s*"
        r"(?P<side>买方|卖方|买入|卖出|买|卖)\s+"
        r"(?P<qty>\d+)",
        flags=re.IGNORECASE,
    )
    for m in table_line.finditer(text):
        name_text = str(m.group("name") or "")
        row_hint = _normalize_underlying_hint(_detect_underlying_hint_from_text(name_text) or hint)
        month, strike = _extract_month_and_strike_from_option_name(name_text, underlying_hint=row_hint)
        cp_raw = str(m.group("cp") or "") or _infer_option_cp_from_text(name_text)
        cp_upper = str(cp_raw).upper()
        if cp_upper in {"认购", "购", "CALL", "C"}:
            cp_mapped = "call"
        elif cp_upper in {"认沽", "沽", "PUT", "P"}:
            cp_mapped = "put"
        else:
            cp_mapped = ""
        side_raw = str(m.group("side") or "")
        side_mapped = "long" if side_raw in {"买方", "买入", "买"} else "short"
        leg = _normalize_option_leg_item(
            {
                "underlying_hint": row_hint,
                "month": month,
                "strike": strike,
                "cp": cp_mapped,
                "side": side_mapped,
                "qty": m.group("qty"),
                "contract_code": str(m.group("code") or "").upper(),
            }
        )
        if leg:
            legs.append(leg)

    # 兜底2：匹配纯合约代码（常见于券商持仓页），至少提取合约代码和张数，不回落股票体检。
    contract_line = re.compile(r"(?P<code>\d{7,9}\.(?:SH|SZ)|\d{7,9})[^\n]*?(?P<qty>-?\d+)\s*(?:张|手)?", re.IGNORECASE)
    for raw_line in text.splitlines():
        line = str(raw_line or "").strip()
        if not line:
            continue
        # 若该行已经包含购沽/认购认沽信息，优先使用上面的结构化规则，不再降级裸码抽取。
        if any(x in line for x in ["认购", "认沽", "购", "沽"]):
            continue
        m = contract_line.search(line)
        if not m:
            continue
        qty = int(m.group("qty"))
        if qty == 0:
            continue
        leg = {
            "underlying_hint": hint,
            "month": None,
            "strike": None,
            "cp": None,
            "side": "long" if qty > 0 else "short",
            "qty": abs(qty),
            "signed_qty": qty,
            "contract_code": str(m.group("code")).upper(),
        }
        legs.append(_attach_option_cn_labels(leg))
    deduped: List[Dict[str, Any]] = []
    seen = set()
    for leg in legs:
        key = (
            leg.get("underlying_hint"),
            leg.get("month"),
            leg.get("strike"),
            leg.get("cp"),
            leg.get("side"),
            leg.get("qty"),
            leg.get("contract_code"),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(_attach_option_cn_labels(leg))
    return deduped


def parse_position_image_response(raw_text: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "domain": "unknown",
        "stock_positions": [],
        "option_legs": [],
        "warnings": [],
        "gaps": [],
    }
    json_text = _extract_json_text(raw_text)
    payload = None
    if json_text:
        try:
            payload = json.loads(json_text)
        except Exception as e:
            out["warnings"].append(f"JSON解析失败: {e}")
    else:
        out["warnings"].append("视觉输出非JSON，已尝试文本兜底解析")

    stock_positions: List[Dict[str, Any]] = []
    option_legs: List[Dict[str, Any]] = []

    has_stock_key = False
    has_option_key = False
    if isinstance(payload, dict):
        raw_stock = payload.get("stock_positions")
        has_stock_key = "stock_positions" in payload
        if raw_stock is None:
            raw_stock = payload.get("positions")
            has_stock_key = has_stock_key or ("positions" in payload)
        if isinstance(raw_stock, list):
            for item in raw_stock:
                if not isinstance(item, dict):
                    continue
                pos = _normalize_position_item(item)
                if pos:
                    stock_positions.append(pos)
                    continue
                opt_leg = _normalize_option_leg_from_stocklike_item(item)
                if opt_leg:
                    option_legs.append(opt_leg)

        raw_legs = payload.get("option_legs")
        has_option_key = "option_legs" in payload
        if isinstance(raw_legs, list):
            for item in raw_legs:
                if not isinstance(item, dict):
                    continue
                leg = _normalize_option_leg_item(item)
                if leg:
                    option_legs.append(leg)

        payload_domain = str(payload.get("domain") or "").strip().lower()
        if payload_domain in {"stock", "option", "mixed", "unknown"}:
            out["domain"] = payload_domain

    if not stock_positions and not has_stock_key:
        stock_positions = _fallback_parse_positions(raw_text)
    if not option_legs and not has_option_key:
        option_legs = _fallback_parse_option_legs(raw_text)

    if stock_positions and option_legs:
        out["domain"] = "mixed"
    elif option_legs:
        out["domain"] = "option"
    elif stock_positions:
        out["domain"] = "stock"
    elif out["domain"] not in {"stock", "option", "mixed"}:
        out["domain"] = "unknown"

    for leg in option_legs:
        if not leg.get("underlying_hint"):
            out["gaps"].append("存在未识别标的的期权腿，请补充标的（如510300/IO）。")
            break

    out["stock_positions"] = stock_positions
    out["option_legs"] = [_attach_option_cn_labels(leg) for leg in option_legs]
    if not stock_positions and not option_legs:
        out["warnings"].append("结构化结果为空，兜底解析后仍未识别到持仓数据")
    return out


def _get_dashscope_vision_model_candidates() -> List[str]:
    configured = (
        os.getenv("DASHSCOPE_VISION_MODEL")
        or os.getenv("POSITION_VISION_MODEL")
        or ""
    )
    configured_model = str(configured).strip()
    strict = str(os.getenv("DASHSCOPE_VISION_STRICT", "")).strip().lower() in {"1", "true", "yes", "on"}
    if strict:
        return [configured_model or "qwen3-vl-plus"]

    candidates: List[str] = []
    if configured_model:
        candidates.append(configured_model)
    for m in ["qwen3-vl-plus", "qwen-vl-plus"]:
        if m not in candidates:
            candidates.append(m)
    return candidates


def _call_dashscope_vision(
    api_key: str,
    prompt: str,
    model: str,
    image_url: str = "",
) -> Dict[str, Any]:
    content = []
    if image_url:
        content.append({"image": image_url})
    content.append({"text": str(prompt or "")})
    messages = [{"role": "user", "content": content}]
    try:
        response = dashscope.MultiModalConversation.call(
            model=model,
            messages=messages,
            api_key=api_key,
        )
    except Exception as e:
        return {"ok": False, "text": "", "error": f"识别异常: {e}"}

    if response.status_code != HTTPStatus.OK:
        return {"ok": False, "text": "", "error": f"视觉模型错误: {response.code} - {response.message}"}

    try:
        text = response.output.choices[0].message.content[0]["text"]
    except Exception:
        text = ""
    return {"ok": True, "text": str(text or ""), "error": ""}


def _dedupe_option_legs(legs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen = set()
    for leg in legs or []:
        if not isinstance(leg, dict):
            continue
        key = (
            str(leg.get("underlying_hint") or ""),
            leg.get("month"),
            leg.get("strike"),
            str(leg.get("cp") or ""),
            str(leg.get("side") or ""),
            leg.get("qty"),
            str(leg.get("contract_code") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(_attach_option_cn_labels(leg))
    return out


def _dedupe_stock_positions(positions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen = set()
    for pos in positions or []:
        if not isinstance(pos, dict):
            continue
        key = (
            str(pos.get("symbol") or ""),
            str(pos.get("market") or ""),
            pos.get("quantity"),
            pos.get("market_value"),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(pos)
    return out


def _merge_position_parse_results(stage1: Dict[str, Any], stage2: Dict[str, Any]) -> Dict[str, Any]:
    stage1_stock = list(stage1.get("stock_positions") or [])
    stage1_legs = list(stage1.get("option_legs") or [])
    stage2_stock = list(stage2.get("stock_positions") or [])
    stage2_legs = list(stage2.get("option_legs") or [])

    # 结构化阶段优先，OCR阶段补充缺失腿。
    merged_stock = _dedupe_stock_positions(stage2_stock + stage1_stock)
    merged_legs = _dedupe_option_legs(stage2_legs + stage1_legs)

    if merged_stock and merged_legs:
        domain = "mixed"
    elif merged_legs:
        domain = "option"
    elif merged_stock:
        domain = "stock"
    else:
        domain = "unknown"

    warnings = list(dict.fromkeys([*list(stage1.get("warnings") or []), *list(stage2.get("warnings") or [])]))
    gaps = list(dict.fromkeys([*list(stage1.get("gaps") or []), *list(stage2.get("gaps") or [])]))
    return {
        "domain": domain,
        "stock_positions": merged_stock,
        "option_legs": merged_legs,
        "warnings": warnings,
        "gaps": gaps,
    }


def _enrich_option_legs_with_contract_metadata(
    option_legs: List[Dict[str, Any]],
    loader=None,
    as_of_yyyymmdd: str = "",
) -> Tuple[List[Dict[str, Any]], List[str]]:
    legs = [dict(x) for x in (option_legs or []) if isinstance(x, dict)]
    if not legs:
        return legs, []

    warnings: List[str] = []
    if loader is None:
        try:
            import option_delta_tools as odt
            loader = odt.ETFOptionMarketLoader()
        except Exception as e:
            return legs, [f"合约元数据增强跳过：loader初始化失败 ({e})"]

    as_of = str(as_of_yyyymmdd or datetime.now().strftime("%Y%m%d"))
    for leg in legs:
        code = _normalize_option_contract_code(leg.get("contract_code"))
        if not code:
            continue
        try:
            meta = loader.get_contract_by_ts_code(code, as_of_yyyymmdd=as_of)
        except Exception as e:
            warnings.append(f"合约{code}元数据查询异常: {e}")
            continue
        if not isinstance(meta, dict) or meta.get("status") != "ok":
            continue

        # 1) 标的按合约元数据矫正（优先级最高）
        before_underlying = str(leg.get("underlying_hint") or "").strip().upper()
        meta_underlying = _normalize_underlying_hint(meta.get("underlying"))
        if meta_underlying and meta_underlying != before_underlying:
            leg["underlying_hint"] = meta_underlying
            warnings.append(f"合约{code}标的已按元数据修正: {before_underlying or '空'} -> {meta_underlying}")

        # 2) 行权价按合约元数据矫正（防 OCR 漏小数/错位）
        meta_strike = _to_float(meta.get("exercise_price"))
        if meta_strike is not None:
            prev_strike = _to_float(leg.get("strike"))
            if prev_strike is None or abs(float(prev_strike) - float(meta_strike)) > 1e-6:
                leg["strike"] = float(meta_strike)
                if prev_strike is not None:
                    warnings.append(f"合约{code}行权价已按元数据修正: {prev_strike} -> {meta_strike}")

        # 3) 购沽按合约元数据修正
        cp = str(meta.get("call_put") or "").strip().upper()
        if cp == "C":
            leg["cp"] = "call"
        elif cp == "P":
            leg["cp"] = "put"

        # 4) 月份按到期日补齐
        if leg.get("month") in (None, ""):
            dd = str(meta.get("delist_date") or "")
            if len(dd) >= 6 and dd[:6].isdigit():
                try:
                    leg["month"] = int(dd[4:6])
                except Exception:
                    pass

        leg["contract_code"] = code
        leg.update(_derive_option_cn_labels(leg.get("cp"), leg.get("side"), leg.get("signed_qty")))

    legs = [_attach_option_cn_labels(leg) for leg in legs]
    return legs, list(dict.fromkeys([w for w in warnings if str(w).strip()]))


def _prepare_image_data_url(uploaded_file) -> Tuple[str, Dict[str, Any]]:
    uploaded_file.seek(0)
    image_bytes = uploaded_file.read()
    uploaded_file.seek(0)

    meta: Dict[str, Any] = {
        "raw_size_bytes": len(image_bytes or b""),
        "width": None,
        "height": None,
        "processed_size_bytes": None,
        "upscaled": False,
    }

    if Image is None:
        base64_data = base64.b64encode(image_bytes).decode("utf-8")
        meta["processed_size_bytes"] = len(image_bytes or b"")
        return f"data:image/png;base64,{base64_data}", meta

    try:
        im = Image.open(BytesIO(image_bytes))
        width, height = im.size
        meta["width"] = int(width)
        meta["height"] = int(height)

        # 期权表格OCR：低清截图先放大再轻微增强，降低小字漏识别概率。
        target_short_edge = 1400
        short_edge = max(1, min(width, height))
        scale = 1.0
        if short_edge < target_short_edge:
            scale = min(3.0, float(target_short_edge) / float(short_edge))
        if scale > 1.05:
            new_w = int(round(width * scale))
            new_h = int(round(height * scale))
            resample = getattr(getattr(Image, "Resampling", Image), "LANCZOS", getattr(Image, "LANCZOS", 1))
            im = im.resize((new_w, new_h), resample)
            meta["upscaled"] = True
            meta["width"] = new_w
            meta["height"] = new_h

        if ImageEnhance is not None:
            im = ImageEnhance.Contrast(im).enhance(1.15)
            im = ImageEnhance.Sharpness(im).enhance(1.20)

        out = BytesIO()
        im.save(out, format="PNG")
        processed = out.getvalue()
        meta["processed_size_bytes"] = len(processed)
        base64_data = base64.b64encode(processed).decode("utf-8")
        return f"data:image/png;base64,{base64_data}", meta
    except Exception:
        base64_data = base64.b64encode(image_bytes).decode("utf-8")
        meta["processed_size_bytes"] = len(image_bytes or b"")
        return f"data:image/png;base64,{base64_data}", meta


def analyze_financial_image(uploaded_file):
    """
    【全能金融眼 - 省钱版】
    Prompt 极简优化，降低 Token 消耗
    """
    api_key = os.getenv("DASHSCOPE_API_KEY")
    if not api_key: return "❌ 未配置 API Key"

    try:
        img_url, _meta = _prepare_image_data_url(uploaded_file)
    except Exception as e:
        return f"图片处理错误: {e}"

    # 🔥【优化点】极简指令 Prompt
    # 字数减少约 60%，去除所有废话，直接命中核心任务
    prompt = """
    任务：分析金融图片。
    根据图片类型执行对应逻辑：
    1. [持仓/账户]：OCR提取表格数据(标的/数量/盈亏)，评估仓位风险。
    2. [K线/走势]：判断标的名称，识别趋势(涨/跌/盘)，识别关键支撑压力位及形态。
    3. [文字/研报]：提取核心观点与策略逻辑。
    输出要求：直接输出数据与结论，严禁啰嗦。
    """

    errors: List[str] = []
    for model in _get_dashscope_vision_model_candidates():
        result = _call_dashscope_vision(
            api_key=api_key,
            prompt=prompt,
            model=model,
            image_url=img_url,
        )
        if result.get("ok"):
            return str(result.get("text") or "")
        errors.append(f"{model}: {result.get('error')}")
    return f"识别异常: {' | '.join(errors) if errors else 'unknown'}"


def analyze_portfolio_image(uploaded_file) -> Dict[str, Any]:
    """兼容旧股票持仓入口：仅返回股票持仓结构。"""
    unified = analyze_position_image(uploaded_file)
    domain = str(unified.get("domain", "unknown"))
    stock_positions = unified.get("stock_positions") or []
    warnings = list(unified.get("warnings") or [])
    if domain in {"option"} and not stock_positions:
        return {
            "ok": False,
            "error": "识别为期权持仓截图，股票体检入口仅支持股票持仓。",
            "positions": [],
            "raw_text": unified.get("raw_text", ""),
            "warnings": warnings,
        }
    if domain == "mixed":
        warnings.append("检测到混合持仓，股票体检入口仅使用股票持仓部分。")
    return {
        "ok": bool(stock_positions),
        "error": None if stock_positions else (unified.get("error") or "未识别到有效持仓数据"),
        "positions": stock_positions,
        "raw_text": unified.get("raw_text", ""),
        "warnings": warnings,
        "domain": domain,
        "option_legs": unified.get("option_legs") or [],
        "gaps": unified.get("gaps") or [],
    }


def analyze_position_image(uploaded_file) -> Dict[str, Any]:
    """统一识别股票/期权/混合持仓截图。"""
    api_key = os.getenv("DASHSCOPE_API_KEY")
    if not api_key:
        return {
            "ok": False,
            "error": "❌ 未配置 API Key",
            "domain": "unknown",
            "stock_positions": [],
            "option_legs": [],
            "raw_text": "",
            "warnings": [],
            "gaps": [],
        }

    try:
        img_url, image_meta = _prepare_image_data_url(uploaded_file)
    except Exception as e:
        return {
            "ok": False,
            "error": f"图片处理错误: {e}",
            "domain": "unknown",
            "stock_positions": [],
            "option_legs": [],
            "raw_text": "",
            "warnings": [],
            "gaps": [],
        }

    model_candidates = _get_dashscope_vision_model_candidates()
    model_used = ""
    stage1_errors: List[str] = []

    # 阶段1：先做 OCR / 表格抽取，优先保障“代码、名称、类别、买卖、持仓”原始行信息。
    stage1_prompt = """
你是金融持仓OCR抽取器。请只输出纯文本，不要Markdown，不要总结，不要解释。
逐行输出你识别到的持仓记录，尽量保留每行字段顺序（如：代码 名称 类别 买卖 持仓 可用）。
要求：
1) 只输出持仓明细行，忽略页眉、汇总、统计、备注、广告等无关文字。
2) 若表格有列：代码/名称/类别/买卖/持仓，请在每行尽量都保留这些值。
3) 字段无法识别可留空，但不要编造，不要翻译，不要改写证券名称。
"""
    stage1 = {"ok": False, "text": "", "error": "未执行"}
    for model in model_candidates:
        stage1 = _call_dashscope_vision(api_key=api_key, prompt=stage1_prompt, model=model, image_url=img_url)
        if stage1.get("ok"):
            model_used = model
            break
        stage1_errors.append(f"{model}: {stage1.get('error')}")
    if not stage1.get("ok"):
        return {
            "ok": False,
            "error": f"OCR阶段失败: {' | '.join(stage1_errors) if stage1_errors else stage1.get('error')}",
            "domain": "unknown",
            "stock_positions": [],
            "option_legs": [],
            "raw_text": "",
            "warnings": [],
            "gaps": [],
        }
    stage1_text = str(stage1.get("text") or "")
    stage1_parsed = parse_position_image_response(stage1_text)

    # 阶段2：基于 OCR 文本做结构化语义归一（股票 + 期权腿 JSON）。
    stage2_prompt = f"""
你是持仓结构化器。根据下面 OCR 文本提取结构化结果。
请只输出严格 JSON，不要任何额外文字，不要 Markdown。
输出格式：
{{
  "domain": "stock|option|mixed|unknown",
  "stock_positions": [
    {{
      "symbol": "股票代码，A股输出6位+交易所后缀(如600519.SH)，港股输出5位+HK后缀(如00700.HK)",
      "name": "股票名称",
      "market": "A或HK",
      "quantity": 数值,
      "market_value": 数值,
      "cost_price": 数值或null,
      "price": 数值或null
    }}
  ],
  "option_legs": [
    {{
      "underlying_hint": "ETF代码(510300/159915/510050/510500/588000)或股指前缀(IO/HO/MO)",
      "month": 4,
      "strike": 3.2,
      "cp": "call|put",
      "side": "long|short",
      "qty": 23,
      "contract_code": "可选，合约代码或null"
    }}
  ]
}}
要求：
1) domain 必须与识别结果一致。
2) option_legs 优先保留原始合约代码和买卖方向。
3) 无法识别字段填 null 或空字符串，不编造。

【OCR文本开始】
{stage1_text}
【OCR文本结束】
"""
    stage2 = {"ok": False, "text": "", "error": "未执行"}
    stage2_errors: List[str] = []
    stage2_try_models = [model_used] + [m for m in model_candidates if m != model_used]
    for model in stage2_try_models:
        stage2 = _call_dashscope_vision(api_key=api_key, prompt=stage2_prompt, model=model, image_url=img_url)
        if stage2.get("ok"):
            break
        stage2_errors.append(f"{model}: {stage2.get('error')}")
    stage2_text = str(stage2.get("text") or "")
    stage2_parsed = parse_position_image_response(stage2_text) if stage2_text else {
        "domain": "unknown",
        "stock_positions": [],
        "option_legs": [],
        "warnings": [],
        "gaps": [],
    }

    merged = _merge_position_parse_results(stage1_parsed, stage2_parsed)
    if not (merged.get("stock_positions") or merged.get("option_legs")):
        # 兜底：回退到单次“图片->严格JSON”旧路径，避免两阶段在某些模型上全空。
        legacy_prompt = """
你是持仓识别器。请只输出严格 JSON，不要任何额外文字，不要 Markdown。
输出格式：
{
  "domain": "stock|option|mixed|unknown",
  "stock_positions": [{"symbol":"","name":"","market":"","quantity":0,"market_value":0,"cost_price":null,"price":null}],
  "option_legs": [{"underlying_hint":"","month":4,"strike":3.2,"cp":"call|put","side":"long|short","qty":1,"contract_code":null}]
}
要求：
1) 识别股票持仓和期权持仓，忽略汇总项。
2) 无法识别字段填 null 或空字符串，不要编造。
3) domain 与识别结果一致。
"""
        legacy_errors: List[str] = []
        for model in stage2_try_models:
            legacy = _call_dashscope_vision(api_key=api_key, prompt=legacy_prompt, model=model, image_url=img_url)
            if not legacy.get("ok"):
                legacy_errors.append(f"{model}: {legacy.get('error')}")
                continue
            legacy_text = str(legacy.get("text") or "")
            legacy_parsed = parse_position_image_response(legacy_text)
            merged = _merge_position_parse_results(merged, legacy_parsed)
            if merged.get("stock_positions") or merged.get("option_legs"):
                break
        if legacy_errors:
            merged["warnings"] = list(merged.get("warnings") or []) + [f"legacy_fallback_errors: {' | '.join(legacy_errors)}"]

    # 合约级校正：按 contract_code 反查 option_basic，修正标的/行权价/购沽。
    enriched_legs, enrich_warnings = _enrich_option_legs_with_contract_metadata(
        merged.get("option_legs") or [],
        as_of_yyyymmdd=datetime.now().strftime("%Y%m%d"),
    )
    merged["option_legs"] = enriched_legs
    merged["warnings"] = list(merged.get("warnings") or []) + list(enrich_warnings or [])
    merged = _merge_position_parse_results(merged, {})

    warnings = list(merged.get("warnings") or [])
    gaps = list(merged.get("gaps") or [])
    if not stage2.get("ok"):
        warnings.append(
            "结构化阶段失败，已退回OCR兜底："
            + ((" | ".join(stage2_errors)) if stage2_errors else str(stage2.get("error") or "unknown"))
        )
    warnings.append(
        f"视觉模型: {model_used or model_candidates[0]} | 识别流水线: two_stage(ocr->normalize)"
    )
    if image_meta.get("width") and image_meta.get("height"):
        warnings.append(
            f"图片尺寸: {image_meta['width']}x{image_meta['height']} | upscaled={bool(image_meta.get('upscaled'))}"
        )
    warnings = list(dict.fromkeys([w for w in warnings if str(w).strip()]))

    stock_positions = merged.get("stock_positions") or []
    option_legs = merged.get("option_legs") or []
    domain = str(merged.get("domain") or "unknown")
    ok = bool(stock_positions or option_legs)
    raw_text = stage2_text or stage1_text

    err_msg = None if ok else "未识别到有效持仓数据"
    if not ok and image_meta.get("width") and image_meta.get("height"):
        w = int(image_meta["width"])
        h = int(image_meta["height"])
        if w < 1200 or h < 500:
            err_msg = f"{err_msg}（截图分辨率偏低：{w}x{h}，建议上传原图或放大后重传）"
    if not ok and not stage2.get("ok"):
        err_msg = f"{err_msg}（结构化阶段异常）"

    return {
        "ok": ok,
        "error": err_msg,
        "domain": domain,
        "stock_positions": stock_positions,
        "option_legs": option_legs,
        "raw_text": raw_text,
        "warnings": warnings,
        "gaps": gaps,
        "ocr_text": stage1_text,
        "image_meta": image_meta,
    }
