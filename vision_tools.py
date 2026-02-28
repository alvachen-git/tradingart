import os
import base64
from http import HTTPStatus
import json
import re
from typing import Any, Dict, List, Optional, Tuple
import dashscope


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


def _normalize_position_item(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    symbol_raw = (
        item.get("symbol")
        or item.get("code")
        or item.get("ticker")
        or item.get("股票代码")
    )
    name = (
        item.get("name")
        or item.get("stock_name")
        or item.get("security_name")
        or item.get("股票名称")
        or ""
    )
    market_raw = item.get("market") or item.get("exchange") or item.get("市场")

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


def _fallback_parse_positions(raw_text: str) -> List[Dict[str, Any]]:
    positions: List[Dict[str, Any]] = []
    lines = [ln.strip() for ln in str(raw_text or "").splitlines() if ln.strip()]
    code_pattern = re.compile(r"(\d{6}\.(?:SH|SZ|BJ)|\d{5}\.HK|\d{6}|\d{5})", re.IGNORECASE)
    num_pattern = re.compile(r"-?\d+(?:,\d{3})*(?:\.\d+)?")

    for line in lines:
        code_match = code_pattern.search(line)
        if not code_match:
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


def analyze_financial_image(uploaded_file):
    """
    【全能金融眼 - 省钱版】
    Prompt 极简优化，降低 Token 消耗
    """
    api_key = os.getenv("DASHSCOPE_API_KEY")
    if not api_key: return "❌ 未配置 API Key"

    try:
        uploaded_file.seek(0)
        image_bytes = uploaded_file.read()
        base64_data = base64.b64encode(image_bytes).decode('utf-8')
        img_url = f"data:image/png;base64,{base64_data}"
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

    messages = [
        {
            "role": "user",
            "content": [
                {"image": img_url},
                {"text": prompt}
            ]
        }
    ]

    try:
        # 💡 建议：如果心疼钱，可以将 model 改为 'qwen-vl-plus'
        # Plus 版本价格通常大幅低于 Max，且处理 OCR 任务能力仅稍弱一点点
        response = dashscope.MultiModalConversation.call(
            model='qwen-vl-plus',
            messages=messages,
            api_key=api_key
        )

        if response.status_code == HTTPStatus.OK:
            return response.output.choices[0].message.content[0]['text']
        else:
            return f"视觉模型错误: {response.code} - {response.message}"

    except Exception as e:
        return f"识别异常: {str(e)}"


def analyze_portfolio_image(uploaded_file) -> Dict[str, Any]:
    """识别持仓截图并输出结构化结果。"""
    api_key = os.getenv("DASHSCOPE_API_KEY")
    if not api_key:
        return {"ok": False, "error": "❌ 未配置 API Key", "positions": [], "raw_text": "", "warnings": []}

    try:
        uploaded_file.seek(0)
        image_bytes = uploaded_file.read()
        base64_data = base64.b64encode(image_bytes).decode("utf-8")
        img_url = f"data:image/png;base64,{base64_data}"
    except Exception as e:
        return {"ok": False, "error": f"图片处理错误: {e}", "positions": [], "raw_text": "", "warnings": []}

    prompt = """
你是持仓识别器。请只输出严格 JSON，不要任何额外文字，不要 Markdown。
输出格式：
{
  "positions": [
    {
      "symbol": "股票代码，A股输出6位+交易所后缀(如600519.SH)，港股输出5位+HK后缀(如00700.HK)",
      "name": "股票名称",
      "market": "A或HK",
      "quantity": 数值,
      "market_value": 数值,
      "cost_price": 数值或null,
      "price": 数值或null
    }
  ]
}
要求：
1) 只识别股票持仓行，忽略现金、基金、总资产等汇总项。
2) 无法识别的字段填 null，不要编造。
3) quantity / market_value / cost_price / price 必须是数字或 null。
4) quantity 必须对应“实际数量/持仓数量”列，不能使用“股票余额/可用余额/冻结数量”。
"""

    messages = [
        {
            "role": "user",
            "content": [
                {"image": img_url},
                {"text": prompt},
            ],
        }
    ]

    try:
        response = dashscope.MultiModalConversation.call(
            model="qwen-vl-plus",
            messages=messages,
            api_key=api_key,
        )
        if response.status_code != HTTPStatus.OK:
            return {
                "ok": False,
                "error": f"视觉模型错误: {response.code} - {response.message}",
                "positions": [],
                "raw_text": "",
                "warnings": [],
            }

        raw_text = response.output.choices[0].message.content[0]["text"]
        parsed = parse_portfolio_json_response(raw_text)
        positions = parsed.get("positions", [])
        warnings = parsed.get("warnings", [])

        return {
            "ok": bool(positions),
            "error": None if positions else "未识别到有效持仓数据",
            "positions": positions,
            "raw_text": raw_text,
            "warnings": warnings,
        }
    except Exception as e:
        return {"ok": False, "error": f"识别异常: {e}", "positions": [], "raw_text": "", "warnings": []}
