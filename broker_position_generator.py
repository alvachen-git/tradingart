"""
期货商持仓晚报生成器 v6.0
=====================================
更新内容：
- 从数据库动态获取品种价格（不再写死）
- 今日核心信号：优先展示正反指标分歧 + 技术面验证
- 机构5日累计布局：用资金金额（亿元）
- 反指标做多/做空区分正确
"""

import html
import json
import pandas as pd
import os
import random
import re
import sys
import time
from typing import Any, Callable
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from llm_compat import ChatTongyiCompat as ChatTongyi
from langchain_core.messages import HumanMessage
from langchain_core.tools import tool
from langgraph.prebuilt import create_react_agent

# ==========================================
# 1. 引入工具包
# ==========================================
from data_engine import (
    PRODUCT_MAP,
    search_broker_holdings_on_date,
    tool_analyze_broker_positions,
    tool_analyze_position_change,
    get_latest_data_date
)
from kline_tools import analyze_kline_pattern
from plot_tools import draw_chart_tool
import subscription_service as sub_svc

try:
    from requests.exceptions import ConnectionError as RequestsConnectionError
    from requests.exceptions import HTTPError as RequestsHTTPError
    from requests.exceptions import SSLError as RequestsSSLError
    from requests.exceptions import Timeout as RequestsTimeout
    RETRYABLE_REQUEST_EXCEPTIONS = (
        RequestsConnectionError,
        RequestsSSLError,
        RequestsTimeout,
        RequestsHTTPError,
    )
except Exception:
    RETRYABLE_REQUEST_EXCEPTIONS = tuple()


class RetryableOperationError(Exception):
    """Error that should be retried within the batch operation budget."""

    def __init__(self, category: str, message: str):
        super().__init__(message)
        self.category = category


def _read_int_env(name: str, default: int) -> int:
    raw = str(os.getenv(name, str(default))).strip()
    try:
        value = int(raw)
        return value if value > 0 else default
    except Exception:
        return default


def _configure_langsmith_tracing() -> None:
    """Default to disable LangSmith tracing for this batch script."""
    enabled = str(os.getenv("ENABLE_LANGSMITH_TRACING", "0")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if enabled:
        return
    os.environ["LANGCHAIN_TRACING_V2"] = "false"
    os.environ["LANGSMITH_TRACING"] = "false"
    os.environ["LANGCHAIN_CALLBACKS_BACKGROUND"] = "false"

# 避免本地 shell 代理变量触发 requests 的 SOCKS 依赖异常
load_dotenv(override=True)
_configure_langsmith_tracing()

BROKER_REPORT_MAX_RETRY_SECONDS = _read_int_env("BROKER_REPORT_MAX_RETRY_SECONDS", 480)
BROKER_REPORT_REQ_TIMEOUT_SECONDS = _read_int_env("BROKER_REPORT_REQ_TIMEOUT_SECONDS", 45)
_MAX_BACKOFF_SECONDS = 30

for _proxy_key in (
    "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
    "http_proxy", "https_proxy", "all_proxy",
):
    os.environ.pop(_proxy_key, None)

# 数据库连接
db_url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_url)

# 初始化 LLM
llm = ChatTongyi(
    model="qwen-plus",
    api_key=os.getenv("DASHSCOPE_API_KEY"),
    request_timeout=BROKER_REPORT_REQ_TIMEOUT_SECONDS,
    max_retries=1,
)


def _classify_retry(exc: Exception) -> tuple[str, bool]:
    if isinstance(exc, RetryableOperationError):
        return exc.category, True

    message = f"{type(exc).__name__}: {exc}"
    lower = message.lower()

    status_codes = re.findall(r"\b([1-5]\d{2})\b", lower)
    for code_str in status_codes:
        code = int(code_str)
        if code in (401, 403):
            return "auth_forbidden", False
        if code == 429:
            return "rate_limit", True
        if 500 <= code <= 599:
            return f"http_{code}", True

    if RETRYABLE_REQUEST_EXCEPTIONS and isinstance(exc, RETRYABLE_REQUEST_EXCEPTIONS):
        return "network_transport", True

    if isinstance(exc, (TimeoutError, ConnectionError)):
        return "network_transport", True

    retryable_hints = (
        "ssleoferror",
        "unexpected_eof_while_reading",
        "max retries exceeded",
        "connection aborted",
        "connection reset",
        "timed out",
        "timeout",
        "remote disconnected",
        "temporarily unavailable",
        "service unavailable",
    )
    if any(hint in lower for hint in retryable_hints):
        return "network_transport", True

    if isinstance(exc, (ValueError, TypeError, KeyError)):
        return "request_param_error", False

    return "unknown_non_retryable", False


def _compute_backoff_seconds(attempt: int, remaining_budget: float) -> float:
    base = min(_MAX_BACKOFF_SECONDS, 2 ** max(attempt - 1, 0))
    jitter = random.uniform(0.0, 1.0)
    wait_s = min(_MAX_BACKOFF_SECONDS, base + jitter)
    return max(0.0, min(wait_s, remaining_budget))


def _invoke_with_retry(fn: Callable[[], Any], op_name: str) -> dict:
    started_at = time.time()
    attempt = 0
    while True:
        attempt += 1
        try:
            result = fn()
            elapsed = time.time() - started_at
            return {
                "ok": True,
                "result": result,
                "attempts": attempt,
                "elapsed_seconds": elapsed,
            }
        except Exception as exc:
            elapsed = time.time() - started_at
            remaining = BROKER_REPORT_MAX_RETRY_SECONDS - elapsed
            category, retryable = _classify_retry(exc)
            message = f"{type(exc).__name__}: {exc}"

            if (not retryable) or remaining <= 0:
                return {
                    "ok": False,
                    "result": None,
                    "attempts": attempt,
                    "elapsed_seconds": elapsed,
                    "error_category": category,
                    "error_message": message,
                }

            wait_s = _compute_backoff_seconds(attempt, remaining)
            print(
                f"⚠️ [{op_name}] attempt {attempt} failed ({category}): {message} | "
                f"retry in {wait_s:.1f}s, remaining budget {remaining:.1f}s"
            )
            if wait_s > 0:
                time.sleep(wait_s)


def _write_material_debug(material: str, meta: dict = None) -> None:
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "meta": meta or {},
        "material_length": len(material or ""),
        "material_preview": (material or "")[:2000],
    }
    try:
        with open("broker_material_debug.txt", "w", encoding="utf-8") as f:
            f.write("=== broker material debug ===\n")
            f.write(json.dumps(payload, ensure_ascii=False, indent=2))
            f.write("\n\n=== material full text ===\n")
            f.write(material or "")
    except Exception as write_err:
        print(f"[debug] failed to write broker_material_debug.txt: {write_err}")


MIN_COLLECT_MATERIAL_CHARS = 100


def _material_quality_error(material: str) -> str:
    text = str(material or "").strip()
    lower = text.lower()
    if "need more steps" in lower:
        return "agent returned need_more_steps"
    if len(text) < MIN_COLLECT_MATERIAL_CHARS:
        return f"material too short: {len(text)} chars"
    return ""


def _extract_reporter_material(agent_result: Any) -> str:
    messages = agent_result.get("messages") if isinstance(agent_result, dict) else None
    if not messages:
        raise ValueError("missing messages in reporter_agent response")

    content = getattr(messages[-1], "content", "")
    if not isinstance(content, str):
        content = str(content or "")

    quality_error = _material_quality_error(content)
    if quality_error:
        preview = re.sub(r"\s+", " ", content).strip()[:160]
        raise RetryableOperationError(
            "collect_bad_material",
            f"{quality_error}; preview={preview!r}",
        )

    return content.strip()

# ==========================================
# 2. 期货商分类配置
# ==========================================
BROKER_CONFIG = {
    "正指标_机构": ["海通期货", "东证期货", "国泰君安"],
    "正指标_外资": ["乾坤期货", "摩根大通"],
    "反指标": ["中信建投", "东方财富", "方正中期"]
}

BROKER_DB_NAMES = {
    "海通期货": "海通期货（代客）",
    "东证期货": "东证期货（代客）",
    "国泰君安": "国泰君安（代客）",
    "乾坤期货": "乾坤期货",
    "摩根大通": "摩根大通",
    "中信建投": "中信建投（代客）",
    "东方财富": "东方财富",
    "方正中期": "方正中期（代客）"
}

CONTRA_BROKER_ALIASES = [
    "中信建投", "中信建投期货",
    "方正中期", "方正",
    "东方财富", "东财",
]

# ==========================================
# 3. 合约乘数配置（只存乘数，价格从数据库获取）
# ==========================================
CONTRACT_MULTIPLIER = {
    # 金属
    "CU": 5,  # 铜，5吨/手
    "AL": 5,  # 铝，5吨/手
    "ZN": 5,  # 锌，5吨/手
    "PB": 5,  # 铅，5吨/手
    "NI": 1,  # 镍，1吨/手
    "SN": 1,  # 锡，1吨/手
    "AU": 1000,  # 黄金，1000克/手
    "AG": 15,  # 白银，15千克/手

    # 黑色
    "RB": 10,  # 螺纹钢，10吨/手
    "HC": 10,  # 热卷，10吨/手
    "I": 100,  # 铁矿石，100吨/手
    "J": 100,  # 焦炭，100吨/手
    "JM": 60,  # 焦煤，60吨/手
    "SF": 5,  # 硅铁，5吨/手
    "SM": 5,  # 锰硅，5吨/手
    "SS": 5,  # 不锈钢，5吨/手

    # 能化
    "SC": 1000,  # 原油，1000桶/手
    "FU": 10,  # 燃料油，10吨/手
    "LU": 10,  # 低硫燃油，10吨/手
    "BU": 10,  # 沥青，10吨/手
    "TA": 5,  # PTA，5吨/手
    "EG": 10,  # 乙二醇，10吨/手
    "MA": 10,  # 甲醇，10吨/手
    "PP": 5,  # 聚丙烯，5吨/手
    "L": 5,  # 塑料，5吨/手
    "V": 5,  # PVC，5吨/手
    "EB": 5,  # 苯乙烯，5吨/手
    "PG": 20,  # LPG，20吨/手
    "SA": 20,  # 纯碱，20吨/手
    "SH": 30,  # 烧碱，30吨/手
    "FG": 20,  # 玻璃，20吨/手
    "SP": 10,  # 纸浆，10吨/手
    "UR": 20,  # 尿素，20吨/手

    # 农产品
    "M": 10,  # 豆粕，10吨/手
    "Y": 10,  # 豆油，10吨/手
    "A": 10,  # 豆一，10吨/手
    "C": 10,  # 玉米，10吨/手
    "CS": 10,  # 淀粉，10吨/手
    "P": 10,  # 棕榈油，10吨/手
    "OI": 10,  # 菜油，10吨/手
    "RM": 10,  # 菜粕，10吨/手
    "CF": 5,  # 棉花，5吨/手
    "SR": 10,  # 白糖，10吨/手
    "AP": 10,  # 苹果，10吨/手
    "CJ": 5,  # 红枣，5吨/手
    "JD": 5,  # 鸡蛋，5吨/手
    "LH": 16,  # 生猪，16吨/手
    "PK": 5,  # 花生，5吨/手

    # 股指
    "IF": 300,  # 沪深300，300元/点
    "IC": 200,  # 中证500，200元/点
    "IM": 200,  # 中证1000，200元/点
    "IH": 300,  # 上证50，300元/点

    # 新能源
    "LC": 1,  # 碳酸锂，1吨/手
    "SI": 5,  # 工业硅，5吨/手
    "AO": 20,  # 氧化铝，20吨/手
    "PS": 3,  # 多晶硅，3吨/手

    # 航运
    "EC": 50,  # 欧线集运，50点/手
}


def get_db_broker_name(broker_name: str) -> str:
    return BROKER_DB_NAMES.get(broker_name, broker_name)


def _normalize_broker_base_name(broker_name: str) -> str:
    name = str(broker_name or "").strip()
    name = name.replace("（代客）", "").replace("(代客)", "").strip()
    return name


def _expand_broker_names(broker_name: str) -> list[str]:
    """
    数据库中同一机构可能同时存在：
    - 无后缀（海通期货）
    - 全角代客（海通期货（代客））
    - 半角代客（海通期货(代客)）
    查询前统一展开，避免跨交易所品种被漏算。
    """
    primary = str(broker_name or "").strip()
    base = _normalize_broker_base_name(primary)
    candidates = {primary, base}
    if base:
        candidates.add(f"{base}（代客）")
        candidates.add(f"{base}(代客)")
    return [x for x in candidates if x]


def _expand_broker_list(brokers_db: list[str]) -> list[str]:
    expanded = set()
    for broker in brokers_db or []:
        expanded.update(_expand_broker_names(broker))
    return sorted(expanded)


def _extract_product_code(ts_code: str) -> str:
    match = re.match(r"([A-Za-z]+)", str(ts_code or ""))
    return match.group(1).upper() if match else ""


def _broker_alias(broker_name: str) -> str:
    alias = str(broker_name or "")
    alias = alias.replace("（代客）", "").replace("(代客)", "").replace("期货", "")
    return alias or broker_name


def _query_group_product_net_changes(brokers_db: list[str], start_date: str, end_date: str) -> list[dict]:
    """
    代码端确定性计算：
    给定期货商分组，返回各品种净持仓变化（end-start）及分期货商明细。
    """
    d1 = _normalize_trade_date(start_date)
    d2 = _normalize_trade_date(end_date)
    query_brokers = _expand_broker_list(brokers_db)
    if not query_brokers or len(d1) != 8 or len(d2) != 8:
        return []

    try:
        brokers_sql = ",".join("'" + str(b).replace("'", "''") + "'" for b in query_brokers)
        sql = f"""
            SELECT
                broker,
                ts_code,
                long_vol,
                short_vol,
                REPLACE(trade_date, '-', '') AS t_date
            FROM futures_holding
            WHERE broker IN ({brokers_sql})
              AND REPLACE(trade_date, '-', '') IN ('{d1}', '{d2}')
              AND ts_code NOT LIKE '%%TAS%%'
        """
        df = pd.read_sql(sql, engine)
        if df.empty:
            return []

        df["product"] = df["ts_code"].apply(_extract_product_code)
        df = df[df["product"] != ""]
        if df.empty:
            return []

        # Normalize aliases first to avoid cross-day key drift: "xx" vs "xx(代客)".
        df["broker_norm"] = df["broker"].apply(_normalize_broker_base_name)
        df_agg = (
            df.groupby(["broker_norm", "product", "t_date"])[["long_vol", "short_vol"]]
            .sum()
            .reset_index()
        )
        df_start = df_agg[df_agg["t_date"] == d1].set_index(["broker_norm", "product"])
        df_end = df_agg[df_agg["t_date"] == d2].set_index(["broker_norm", "product"])
        if df_start.empty or df_end.empty:
            print(
                f"[broker_net] insufficient_rows: start_rows={len(df_start)}, "
                f"end_rows={len(df_end)}, start={d1}, end={d2}"
            )
            return []

        start_keys = set(df_start.index.tolist())
        end_keys = set(df_end.index.tolist())
        common_keys = start_keys & end_keys
        if not common_keys:
            print(f"[broker_net] no_common_keys: start={d1}, end={d2}")
            return []

        # Critical rule: missing rows mean "unknown", not zero.
        if start_keys != end_keys:
            missing_in_end = len(start_keys - end_keys)
            missing_in_start = len(end_keys - start_keys)
            print(
                f"[broker_net] drop_unpaired_keys: missing_in_end={missing_in_end}, "
                f"missing_in_start={missing_in_start}, start={d1}, end={d2}"
            )

        df_end, df_start = df_end.align(df_start, join="inner")

        df_diff = pd.DataFrame(index=df_end.index)
        # 净持仓变化 = (end_long - end_short) - (start_long - start_short)
        df_diff["net_chg"] = (df_end["long_vol"] - df_end["short_vol"]) - (df_start["long_vol"] - df_start["short_vol"])
        df_diff = df_diff.reset_index()

        df_diff["name"] = df_diff["product"].apply(lambda x: PRODUCT_MAP.get(x, x))
        product_total = (
            df_diff.groupby("name", as_index=False)["net_chg"]
            .sum()
            .sort_values("net_chg", ascending=False)
        )

        records = []
        for _, row in product_total.iterrows():
            net_chg = int(row["net_chg"])
            if net_chg == 0:
                continue

            name = str(row["name"])
            detail_df = (
                df_diff[(df_diff["name"] == name) & (df_diff["net_chg"] != 0)]
                .groupby("broker_norm", as_index=False)["net_chg"]
                .sum()
                .assign(abs_chg=lambda x: x["net_chg"].abs())
                .sort_values("abs_chg", ascending=False)
            )
            component_df = (
                df_diff[(df_diff["name"] == name) & (df_diff["net_chg"] != 0)]
                .groupby("product", as_index=False)["net_chg"]
                .sum()
            )
            details = []
            for _, d in detail_df.iterrows():
                details.append(f"{_broker_alias(d['broker_norm'])} {int(d['net_chg']):+,}")
            components = {
                str(d["product"]).upper(): int(d["net_chg"])
                for _, d in component_df.iterrows()
                if int(d["net_chg"]) != 0
            }

            records.append({
                "product": name,
                "name": name,
                "net_chg": net_chg,
                "details": details,
                "components": components,
            })

        return records
    except Exception as e:
        print(f"⚠️ 机构净持仓确定性计算失败: {e}")
        return []


def _collect_missing_end_products(brokers_db: list[str], start_date: str, end_date: str) -> list[dict]:
    """
    Collect products that exist on start_date but are missing on end_date.
    """
    d1 = _normalize_trade_date(start_date)
    d2 = _normalize_trade_date(end_date)
    query_brokers = _expand_broker_list(brokers_db)
    if not query_brokers or len(d1) != 8 or len(d2) != 8:
        return []

    try:
        brokers_sql = ",".join("'" + str(b).replace("'", "''") + "'" for b in query_brokers)
        sql = f"""
            SELECT broker, ts_code, REPLACE(trade_date, '-', '') AS t_date
            FROM futures_holding
            WHERE broker IN ({brokers_sql})
              AND REPLACE(trade_date, '-', '') IN ('{d1}', '{d2}')
              AND ts_code NOT LIKE '%%TAS%%'
        """
        df = pd.read_sql(sql, engine)
        if df.empty:
            return []

        df["product"] = df["ts_code"].apply(_extract_product_code)
        df = df[df["product"] != ""].copy()
        if df.empty:
            return []

        df["broker_norm"] = df["broker"].apply(_normalize_broker_base_name)
        pairs = df.groupby(["broker_norm", "product", "t_date"]).size().reset_index(name="cnt")

        start_pairs = set(
            tuple(x)
            for x in pairs[pairs["t_date"] == d1][["broker_norm", "product"]].itertuples(index=False, name=None)
        )
        end_pairs = set(
            tuple(x)
            for x in pairs[pairs["t_date"] == d2][["broker_norm", "product"]].itertuples(index=False, name=None)
        )
        missing_pairs = sorted(start_pairs - end_pairs)
        if not missing_pairs:
            return []

        grouped: dict[str, dict] = {}
        for broker_norm, product in missing_pairs:
            name = PRODUCT_MAP.get(product, product)
            slot = grouped.setdefault(name, {"name": name, "brokers": set(), "codes": set()})
            slot["brokers"].add(_broker_alias(broker_norm))
            slot["codes"].add(str(product).upper())

        notes: list[dict] = []
        for name in sorted(grouped.keys()):
            slot = grouped[name]
            notes.append(
                {
                    "name": slot["name"],
                    "brokers": ", ".join(sorted(slot["brokers"])),
                    "codes": "/".join(sorted(slot["codes"])),
                }
            )
        return notes
    except Exception as e:
        print(f"[broker_net] collect_missing_end_products_failed: {e}")
        return []


def _build_institution_day_snapshot(start_date: str, end_date: str) -> dict:
    institution_brokers_db = [get_db_broker_name(b) for b in BROKER_CONFIG["正指标_机构"]]
    records = _query_group_product_net_changes(institution_brokers_db, start_date, end_date)
    missing_today = _collect_missing_end_products(institution_brokers_db, start_date, end_date)
    long_top = [r for r in records if r["net_chg"] > 0][:5]
    short_top = sorted([r for r in records if r["net_chg"] < 0], key=lambda x: x["net_chg"])[:5]
    return {
        "start_date": start_date,
        "end_date": end_date,
        "long_top": long_top,
        "short_top": short_top,
        "missing_today": missing_today,
    }


def _render_institution_rows(items: list[dict], css_class: str, empty_text: str) -> str:
    if not items:
        return (
            f'<tr><td class="text-gray">{html.escape(empty_text)}</td>'
            '<td class="text-gray">—</td>'
            '<td class="text-gray">—</td></tr>'
        )

    rows = []
    for item in items:
        details = "；".join(item.get("details", [])) if item.get("details") else "—"
        rows.append(
            f'<tr>'
            f'<td><strong>{html.escape(str(item.get("name", "")))}</strong></td>'
            f'<td class="{css_class}">{int(item.get("net_chg", 0)):+,}</td>'
            f'<td class="{css_class}">{html.escape(details)}</td>'
            f'</tr>'
        )
    return "\n".join(rows)


def enforce_institution_day_section(html_content: str, snapshot: dict) -> str:
    """
    强制覆盖“机构当日动向”板块，避免 LLM 对净多/净空方向误判。
    """
    if not html_content or not snapshot:
        return html_content

    start_tag = '<h2 class="section-title">机构当日动向</h2>'
    end_tag = '<!-- 机构5日累计布局 -->'
    s = html_content.find(start_tag)
    e = html_content.find(end_tag)
    if s == -1 or e == -1 or e <= s:
        return html_content

    long_rows = _render_institution_rows(snapshot.get("long_top", []), "text-red", "— 无显著净多增仓 —")
    short_rows = _render_institution_rows(snapshot.get("short_top", []), "text-green", "— 无显著净空增仓 —")

    section = f"""
<h2 class="section-title">机构当日动向</h2>
<p class="sub-text" style="margin:-8px 0 16px 0;">海通 · 东证 · 国泰君安 ｜ 当日净持仓变化</p>

<p style="font-size:14px; font-weight:600; margin:16px 0 8px 0;"><span class="text-red">●</span> 当日净多头增仓 TOP5</p>
<table class="data-table">
  <tr><th>品种</th><th style="text-align:right;">合计</th><th style="text-align:right;">明细</th></tr>
  {long_rows}
</table>

<p style="font-size:14px; font-weight:600; margin:24px 0 8px 0;"><span class="text-green">●</span> 当日净空头增仓 TOP5</p>
<table class="data-table">
  <tr><th>品种</th><th style="text-align:right;">合计</th><th style="text-align:right;">明细</th></tr>
  {short_rows}
</table>
</div>

"""
    print("🔧 已用代码端净持仓计算强制覆盖机构当日动向区块。")
    return html_content[:s] + section + html_content[e:]


_PRICE_CACHE: dict[str, float | None] = {}


def _get_latest_product_price(product_code: str) -> float | None:
    code = str(product_code or "").upper().strip()
    if not code:
        return None
    if code in _PRICE_CACHE:
        return _PRICE_CACHE[code]

    try:
        sql = f"""
            SELECT close_price
            FROM futures_price
            WHERE UPPER(ts_code) LIKE '{code}%%'
              AND ts_code NOT LIKE '%%TAS%%'
              AND ts_code REGEXP '[0-9]{{4}}$'
            ORDER BY trade_date DESC, oi DESC
            LIMIT 1
        """
        df = pd.read_sql(sql, engine)
        if df.empty:
            _PRICE_CACHE[code] = None
            return None
        px = float(df.iloc[0]["close_price"])
        _PRICE_CACHE[code] = px
        return px
    except Exception:
        _PRICE_CACHE[code] = None
        return None


def _calc_value_yi_for_components(components: dict[str, int]) -> float | None:
    total_yi = 0.0
    has_price = False
    for code, lots in components.items():
        if not lots:
            continue
        px = _get_latest_product_price(code)
        if px is None:
            continue
        multiplier = CONTRACT_MULTIPLIER.get(str(code).upper(), 10)
        total_yi += lots * multiplier * px / 100000000
        has_price = True
    return total_yi if has_price else None


def _enrich_records_with_value(records: list[dict]) -> list[dict]:
    enriched = []
    for r in records:
        item = dict(r)
        item["value_yi"] = _calc_value_yi_for_components(item.get("components", {}))
        enriched.append(item)
    return enriched


def _record_value_scale_sort_key(record: dict) -> float:
    value_yi = record.get("value_yi")
    if isinstance(value_yi, (int, float)):
        return abs(float(value_yi))
    try:
        return float(abs(int(record.get("net_chg", 0))))
    except Exception:
        return 0.0


def _format_value_yi(value_yi: float | None) -> str:
    if value_yi is None:
        return "(估值缺失)"
    return f"(约{value_yi:+.2f}亿)"


def _direction_of(v: int) -> int:
    if v > 0:
        return 1
    if v < 0:
        return -1
    return 0


def _build_institution_5d_snapshot(start_date: str, end_date: str) -> dict:
    institution_brokers_db = [get_db_broker_name(b) for b in BROKER_CONFIG["正指标_机构"]]
    records = _enrich_records_with_value(_query_group_product_net_changes(institution_brokers_db, start_date, end_date))
    long_top = sorted(
        [r for r in records if r["net_chg"] > 0],
        key=_record_value_scale_sort_key,
        reverse=True,
    )[:5]
    short_top = sorted(
        [r for r in records if r["net_chg"] < 0],
        key=_record_value_scale_sort_key,
        reverse=True,
    )[:5]
    return {
        "start_date": start_date,
        "end_date": end_date,
        "all_records": records,
        "long_top": long_top,
        "short_top": short_top,
    }


def _build_contra_day_snapshot(start_date: str, end_date: str) -> dict:
    contra_brokers_db = [get_db_broker_name(b) for b in BROKER_CONFIG["反指标"]]
    records = _query_group_product_net_changes(contra_brokers_db, start_date, end_date)
    long_top = [r for r in records if r["net_chg"] > 0][:5]
    short_top = sorted([r for r in records if r["net_chg"] < 0], key=lambda x: x["net_chg"])[:5]
    return {
        "start_date": start_date,
        "end_date": end_date,
        "all_records": records,
        "long_top": long_top,
        "short_top": short_top,
    }


def _build_divergence_snapshot(institution_5d: dict, contra_day: dict) -> dict:
    inst_map = {str(x["name"]): int(x["net_chg"]) for x in institution_5d.get("all_records", [])}
    contra_map = {str(x["name"]): int(x["net_chg"]) for x in contra_day.get("all_records", [])}

    divergences = []
    consensuses = []
    for name, inst_chg in inst_map.items():
        contra_chg = contra_map.get(name)
        if contra_chg is None:
            continue
        if _direction_of(inst_chg) == 0 or _direction_of(contra_chg) == 0:
            continue
        score = abs(inst_chg) + abs(contra_chg)
        row = {
            "name": name,
            "inst_chg": inst_chg,
            "contra_chg": contra_chg,
            "score": score,
        }
        if _direction_of(inst_chg) != _direction_of(contra_chg):
            divergences.append(row)
        else:
            consensuses.append(row)

    divergences.sort(key=lambda x: x["score"], reverse=True)
    consensuses.sort(key=lambda x: x["score"], reverse=True)
    return {
        "divergences": divergences[:3],
        "consensuses": consensuses[:2],
    }


_TECH_VIEW_CACHE: dict[str, dict] = {}


def _parse_tech_view(report_text: str) -> dict:
    text = str(report_text or "")
    trend_match = re.search(r"多日趋势：([^\n]+)", text)
    trend_text = trend_match.group(1).strip() if trend_match else "技术面待确认"

    combo_match = re.search(r"\*\*三、多日组合形态\*\*\s*([\s\S]*?)\n\s*\*\*四、趋势研判\*\*", text)
    combo_text = combo_match.group(1).strip() if combo_match else "暂无明显组合形态"

    lower = text.lower()
    if any(k in text for k in ["多头主导", "均线多头排列", "强势上涨", "放量突破"]) or "bull" in lower:
        bias = "bullish"
    elif any(k in text for k in ["空头主导", "均线空头排列", "持续下跌", "放量下跌"]) or "bear" in lower:
        bias = "bearish"
    elif "未找到" in text or "出错" in text or "无法" in text:
        bias = "unknown"
    else:
        bias = "neutral"

    return {
        "trend_text": trend_text,
        "combo_text": combo_text,
        "bias": bias,
    }


def _get_tech_view(product_name: str) -> dict:
    key = str(product_name or "").strip()
    if not key:
        return {"trend_text": "技术面待确认", "combo_text": "暂无明显组合形态", "bias": "unknown"}
    if key in _TECH_VIEW_CACHE:
        return _TECH_VIEW_CACHE[key]

    try:
        if hasattr(analyze_kline_pattern, "invoke"):
            raw = analyze_kline_pattern.invoke({"query": key})
        else:
            raw = analyze_kline_pattern(key)
        parsed = _parse_tech_view(str(raw))
        _TECH_VIEW_CACHE[key] = parsed
        return parsed
    except Exception as e:
        print(f"⚠️ 技术面提取失败({key}): {e}")
        fallback = {"trend_text": "技术面调用失败", "combo_text": "暂无明显组合形态", "bias": "unknown"}
        _TECH_VIEW_CACHE[key] = fallback
        return fallback


def _batch_get_tech_views(product_names: list[str]) -> dict[str, dict]:
    views = {}
    for name in product_names:
        key = str(name or "").strip()
        if not key:
            continue
        views[key] = _get_tech_view(key)
    return views


def enforce_core_signal_section(html_content: str, divergence_snapshot: dict, tech_views: dict[str, dict]) -> str:
    if not html_content or not divergence_snapshot:
        return html_content

    start_tag = '<h2 class="section-title">今日核心信号</h2>'
    end_tag = '<!-- 机构当日动向 -->'
    s = html_content.find(start_tag)
    e = html_content.find(end_tag)
    if s == -1 or e == -1 or e <= s:
        return html_content

    blocks = []
    for row in divergence_snapshot.get("divergences", []):
        name = html.escape(str(row["name"]))
        tech = tech_views.get(str(row["name"]), {"trend_text": "技术面待确认", "combo_text": "暂无明显组合形态", "bias": "unknown"})
        inst_chg = int(row["inst_chg"])
        contra_chg = int(row["contra_chg"])
        follow = "做多" if inst_chg > 0 else "做空"
        stage = "多头主场" if inst_chg > 0 else "空头主场"
        bias = str(tech.get("bias", "unknown"))
        if (inst_chg > 0 and bias == "bullish") or (inst_chg < 0 and bias == "bearish"):
            resonance = "✅ 资金和技术同台飙戏，趋势像开了追光灯。"
        elif bias == "unknown":
            resonance = "⚠️ 技术面演员临时缺席，先按资金主线推进。"
        else:
            resonance = "⚠️ 资金和技术在台上互怼，仓位别上头。"
        blocks.append(
            f'<div class="signal-item"><strong>{"🟢" if inst_chg > 0 else "🔴"} {name}｜高强度分歧 · {stage}</strong><br>'
            f'机构5日 {inst_chg:+,} 手，反指标当日 {contra_chg:+,} 手，两路资金正面对戏。<br>'
            f'交易剧本先站机构这边：优先{follow}。<br>'
            f'技术面：{html.escape(str(tech.get("trend_text", "技术面待确认")))}；形态：{html.escape(str(tech.get("combo_text", "暂无明显组合形态")))}。<br>'
            f'{resonance}</div>'
        )

    for row in divergence_snapshot.get("consensuses", []):
        name = html.escape(str(row["name"]))
        inst_chg = int(row["inst_chg"])
        contra_chg = int(row["contra_chg"])
        direction = "做多" if inst_chg > 0 else "做空"
        blocks.append(
            f'<div class="signal-item"><strong>🟡 {name}｜同向拥挤</strong><br>'
            f'机构5日 {inst_chg:+,} 手，反指标当日 {contra_chg:+,} 手，两边站到同一侧。<br>'
            f'这类剧情容易“人多踩踏”，方向虽是{direction}，但更适合等二次确认再上场。</div>'
        )

    if not blocks:
        blocks.append('<div class="signal-item">今天多空双方都比较克制，市场没有上演主线大戏。先控仓，等下一场重头戏开幕。</div>')

    section = f"""
<h2 class="section-title">今日核心信号</h2>
<div style="line-height:1.9; font-size:14px;">
  {"".join(blocks)}
</div>
</div>

"""
    print("🔧 已用代码端分歧结果强制覆盖今日核心信号区块。")
    return html_content[:s] + section + html_content[e:]


def _render_institution_5d_lines(items: list[dict], css_class: str, empty_text: str) -> str:
    if not items:
        return f'<div class="text-gray">{html.escape(empty_text)}</div>'

    lines = []
    for idx, item in enumerate(items, start=1):
        name = html.escape(str(item.get("name", "")))
        net_chg = int(item.get("net_chg", 0))
        value_text = _format_value_yi(item.get("value_yi"))
        lines.append(
            f'{idx}. {name} <span class="{css_class}">{net_chg:+,}手</span> '
            f'<span class="text-gray">{html.escape(value_text)}</span>'
        )
    return "<br>\n".join(lines)


def enforce_institution_5d_section(html_content: str, snapshot: dict) -> str:
    if not html_content or not snapshot:
        return html_content

    start_tag = '<h2 class="section-title">机构5日累计布局</h2>'
    end_tag = '<!-- 外资风向标 -->'
    s = html_content.find(start_tag)
    e = html_content.find(end_tag)
    if s == -1 or e == -1 or e <= s:
        return html_content

    long_lines = _render_institution_5d_lines(snapshot.get("long_top", []), "text-red", "— 无显著累计净多增仓 —")
    short_lines = _render_institution_5d_lines(snapshot.get("short_top", []), "text-green", "— 无显著累计净空增仓 —")

    section = f"""
<h2 class="section-title">机构5日累计布局</h2>
<p class="sub-text" style="margin:-8px 0 16px 0;">海通 · 东证 · 国泰君安 ｜ 近5个交易日累计 · 按资金规模排序</p>

<div style="display:grid; grid-template-columns:1fr 1fr; gap:16px;">
  <div>
    <p style="font-size:13px; font-weight:600; margin:0 0 8px 0;"><span class="text-red">●</span> 累计做多</p>
    <div style="font-size:13px; line-height:1.8;">
      {long_lines}
    </div>
  </div>
  <div>
    <p style="font-size:13px; font-weight:600; margin:0 0 8px 0;"><span class="text-green">●</span> 累计做空</p>
    <div style="font-size:13px; line-height:1.8;">
      {short_lines}
    </div>
  </div>
</div>
</div>

"""
    print("🔧 已用代码端净持仓与估值强制覆盖机构5日累计区块。")
    return html_content[:s] + section + html_content[e:]


def _render_contra_rows(items: list[dict], inst_map: dict[str, int], css_class: str, empty_text: str) -> str:
    if not items:
        return (
            f'<tr><td class="text-gray">{html.escape(empty_text)}</td>'
            '<td class="text-gray">—</td>'
            '<td class="text-gray">—</td></tr>'
        )

    rows = []
    for item in items:
        name = str(item.get("name", ""))
        net_chg = int(item.get("net_chg", 0))
        inst_chg = inst_map.get(name)
        if inst_chg is None:
            signal = "独立信号，观察持续性"
        elif _direction_of(inst_chg) != _direction_of(net_chg):
            signal = "与机构反向，分歧信号（优先看机构）"
        else:
            signal = "与机构同向，警惕拥挤"

        rows.append(
            f'<tr>'
            f'<td><strong>{html.escape(name)}</strong></td>'
            f'<td class="{css_class}">{net_chg:+,}</td>'
            f'<td>{html.escape(signal)}</td>'
            f'</tr>'
        )
    return "\n".join(rows)


def enforce_contra_signal_section(html_content: str, contra_snapshot: dict, institution_5d: dict) -> str:
    if not html_content or not contra_snapshot:
        return html_content

    start_tag = '<h2 class="section-title">反指标信号</h2>'
    end_tag = '<!-- AI毒舌点评 -->'
    s = html_content.find(start_tag)
    e = html_content.find(end_tag)
    if s == -1 or e == -1 or e <= s:
        return html_content

    inst_map = {str(x["name"]): int(x["net_chg"]) for x in institution_5d.get("all_records", [])}
    long_rows = _render_contra_rows(contra_snapshot.get("long_top", []), inst_map, "text-red", "— 无显著反指标净多信号 —")
    short_rows = _render_contra_rows(contra_snapshot.get("short_top", []), inst_map, "text-green", "— 无显著反指标净空信号 —")

    section = f"""
<h2 class="section-title">反指标信号</h2>
<p class="sub-text" style="margin:-8px 0 16px 0;">散户聚集地 ｜ 当日净持仓变化 · 反向参考</p>

<p style="font-size:14px; font-weight:600; margin:16px 0 8px 0;"><span class="tag tag-short">反着看</span> 反指标大幅做多</p>
<table class="data-table">
  <tr><th>品种</th><th style="text-align:right;">合计净多</th><th>潜在信号</th></tr>
  {long_rows}
</table>

<p style="font-size:14px; font-weight:600; margin:24px 0 8px 0;"><span class="tag tag-long">反着看</span> 反指标大幅做空</p>
<table class="data-table">
  <tr><th>品种</th><th style="text-align:right;">合计净空</th><th>潜在信号</th></tr>
  {short_rows}
</table>
</div>

"""
    print("🔧 已用代码端净持仓计算强制覆盖反指标信号区块。")
    return html_content[:s] + section + html_content[e:]


def sanitize_institution_section(html: str) -> str:
    """
    安全兜底：
    机构当日动向区块只允许海通/东证/国泰系明细，剔除反指标名称及其数值片段。
    """
    if not html:
        return html

    start_tag = '<h2 class="section-title">机构当日动向</h2>'
    end_tag = '<!-- 机构5日累计布局 -->'
    s = html.find(start_tag)
    e = html.find(end_tag)
    if s == -1 or e == -1 or e <= s:
        return html

    section = html[s:e]
    cleaned = section

    for alias in CONTRA_BROKER_ALIASES:
        # 删掉“| 中信建投 +12,977”这类明细片段
        cleaned = re.sub(rf"\s*\|\s*{re.escape(alias)}[^|<\n]*", "", cleaned)
        # 删掉“中信建投 +12,977 |”位于开头的片段
        cleaned = re.sub(rf"{re.escape(alias)}[^|<\n]*\s*\|\s*", "", cleaned)
        # 删掉仅剩一段的场景
        cleaned = re.sub(rf"{re.escape(alias)}[^|<\n]*", "", cleaned)

    # 清理可能残留的分隔符
    cleaned = re.sub(r"\|\s*\|", "|", cleaned)
    cleaned = re.sub(r"\s*\|\s*</td>", "</td>", cleaned)
    cleaned = re.sub(r"<td>\s*\|\s*", "<td>", cleaned)
    cleaned = re.sub(r"\s{2,}", " ", cleaned)

    if cleaned != section:
        print("🔧 已清洗机构区块中的反指标期货商名称。")

    return html[:s] + cleaned + html[e:]


def _normalize_trade_date(value) -> str:
    """将数据库日期值规范成 YYYYMMDD。"""
    digits = re.sub(r"\D", "", str(value or ""))
    return digits[:8] if len(digits) >= 8 else ""


def get_recent_trading_days(n_days: int = 5):
    """从数据库获取最近 n 个交易日（含当日）。"""
    try:
        sql = f"SELECT DISTINCT trade_date FROM futures_holding ORDER BY trade_date DESC LIMIT {n_days + 2}"
        df = pd.read_sql(sql, engine)
        if len(df) >= 2:
            dates = [_normalize_trade_date(d) for d in df["trade_date"].tolist()]
            dates = [d for d in dates if len(d) == 8]
            if len(dates) >= 2:
                today = dates[0]
                yesterday = dates[1]
                # 口径统一：n_days=5 => 取 [today ... 往前第4个交易日]，累计恰好5个交易日
                start_idx = min(max(n_days - 1, 0), len(dates) - 1)
                start_date = dates[start_idx]
                return today, yesterday, start_date
    except Exception as e:
        print(f"获取交易日失败: {e}")

    today = datetime.now()
    return (
        today.strftime("%Y%m%d"),
        (today - timedelta(days=1)).strftime("%Y%m%d"),
        (today - timedelta(days=7)).strftime("%Y%m%d")
    )


def should_skip_non_trading_publish() -> bool:
    """
    交易日门禁：
    当数据库最新交易日不是今天时，跳过发布。
    """
    try:
        latest_db_date = _normalize_trade_date(get_latest_data_date())
        today = datetime.now().strftime("%Y%m%d")
        if latest_db_date != today:
            print(f"⏭️ 非交易日或当日数据未就绪，跳过发布。today={today}, latest_db={latest_db_date or 'N/A'}")
            return True
        return False
    except Exception as e:
        print(f"⚠️ 交易日门禁检查失败，按保守策略跳过发布: {e}")
        return True


# ==========================================
# 4. 新增工具：获取品种价格和计算资金
# ==========================================
@tool
def get_futures_price_and_value(product_code: str, lots: int):
    """
    【期货持仓价值计算器】
    根据品种代码和手数，查询最新价格并计算持仓资金价值。

    参数:
    - product_code: 品种代码，如 'CU', 'RB', 'M', 'SA'
    - lots: 持仓手数（正数表示多头，负数表示空头）

    返回: 品种价格、合约价值、持仓资金（亿元）
    """
    if engine is None:
        return "数据库连接失败"

    code = product_code.upper().strip()

    try:
        # 1. 查询该品种主力合约的最新价格
        # 使用正则匹配品种代码开头的合约，按持仓量排序取主力
        sql = f"""
            SELECT ts_code, close_price, trade_date
            FROM futures_price
            WHERE UPPER(ts_code) LIKE '{code}%'
              AND ts_code NOT LIKE '%TAS%'
              AND ts_code REGEXP '[0-9]{{4}}$'
            ORDER BY trade_date DESC, oi DESC
            LIMIT 1
        """
        df = pd.read_sql(sql, engine)

        if df.empty:
            return f"未找到品种 {code} 的价格数据"

        price = float(df.iloc[0]['close_price'])
        contract = df.iloc[0]['ts_code']
        trade_date = str(df.iloc[0]['trade_date'])

        # 2. 获取合约乘数
        multiplier = CONTRACT_MULTIPLIER.get(code, 10)  # 默认10

        # 3. 计算资金价值
        # 资金(亿元) = |手数| × 乘数 × 价格 / 1亿
        value_yuan = abs(lots) * multiplier * price
        value_yi = value_yuan / 100000000

        # 4. 单手价值
        single_lot_value = multiplier * price

        return f"""
📊 **{code} 持仓价值计算**
- 主力合约: {contract}
- 最新价格: {price:,.2f} (日期: {trade_date})
- 合约乘数: {multiplier}
- 单手价值: {single_lot_value:,.0f} 元
- 持仓手数: {lots:+,} 手
- **持仓资金: {value_yi:.2f} 亿元**
"""

    except Exception as e:
        return f"查询价格出错: {e}"


@tool
def batch_calculate_position_value(positions: str):
    """
    【批量计算持仓资金】
    批量计算多个品种的持仓资金价值。

    参数:
    - positions: 品种和手数列表，格式为 "品种1:手数1,品种2:手数2"
                例如: "M:115000,SA:-96000,RB:50000"

    返回: 各品种的资金价值汇总
    """
    if engine is None:
        return "数据库连接失败"

    results = []
    total_long = 0  # 多头总资金
    total_short = 0  # 空头总资金

    try:
        items = positions.split(',')
        for item in items:
            parts = item.strip().split(':')
            if len(parts) != 2:
                continue

            code = parts[0].strip().upper()
            lots = int(parts[1].strip())

            # 查询价格
            sql = f"""
                SELECT close_price
                FROM futures_price
                WHERE UPPER(ts_code) LIKE '{code}%'
                  AND ts_code NOT LIKE '%TAS%'
                  AND ts_code REGEXP '[0-9]{{4}}$'
                ORDER BY trade_date DESC, oi DESC
                LIMIT 1
            """
            df = pd.read_sql(sql, engine)

            if df.empty:
                results.append(f"- {code}: 未找到价格")
                continue

            price = float(df.iloc[0]['close_price'])
            multiplier = CONTRACT_MULTIPLIER.get(code, 10)
            value_yi = abs(lots) * multiplier * price / 100000000

            direction = "多" if lots > 0 else "空"
            results.append(f"- {code}: {lots:+,}手 × {multiplier} × {price:,.0f} = **{value_yi:.2f}亿** ({direction})")

            if lots > 0:
                total_long += value_yi
            else:
                total_short += value_yi

        summary = f"""
📊 **批量持仓资金计算**

{chr(10).join(results)}

---
**汇总**:
- 多头总资金: {total_long:.2f} 亿
- 空头总资金: {total_short:.2f} 亿
- 净资金规模: {total_long - total_short:+.2f} 亿
"""
        return summary

    except Exception as e:
        return f"批量计算出错: {e}"


# ==========================================
# 5. AI 记者 - 数据采集
# ==========================================
def collect_broker_position_data():
    print("🕵️‍♂️ [持仓记者] 出发采集期货商持仓数据...")

    tools = [
        search_broker_holdings_on_date,
        tool_analyze_broker_positions,
        tool_analyze_position_change,
        analyze_kline_pattern,
        draw_chart_tool,
        # 新增：价格和资金计算工具
        get_futures_price_and_value,
        batch_calculate_position_value,
    ]

    today_str = datetime.now().strftime("%Y年%m月%d日")

    # 获取实际交易日
    today_date, yesterday, five_days_ago = get_recent_trading_days(5)
    print(f"📅 交易日：今日={today_date}, 昨日={yesterday}, 5日前={five_days_ago}")

    institution_brokers_db = [get_db_broker_name(b) for b in BROKER_CONFIG["正指标_机构"]]
    foreign_brokers_db = [get_db_broker_name(b) for b in BROKER_CONFIG["正指标_外资"]]
    contra_brokers_db = [get_db_broker_name(b) for b in BROKER_CONFIG["反指标"]]

    system_prompt = f"""
IMPORTANT:
- Do not use web search or news lookup.
- Use only database-backed tools in this workflow.
你是一位**期货商持仓分析专家**，为《爱波塔-期货商持仓晚报》采集数据。
当前日期：{today_str}

【期货商分类】
- 正指标（机构）：{', '.join(institution_brokers_db)}
- 正指标（外资）：{', '.join(foreign_brokers_db)}
- 反指标：{', '.join(contra_brokers_db)}

【重要提示 - 品种名称处理】
同一品种可能有不同的合约代码，在汇总时必须合并为同一品种！
- 纯碱sa → 纯碱
- 白糖sr → 白糖
- 玻璃fg → 玻璃
以此类推，按品种大类合并统计。

【采集流程】

## 第一步：机构当日持仓变化 ⭐必做
查询3家机构**当日**持仓变化：

调用 `tool_analyze_broker_positions`，参数：
- start_date: {yesterday}
- end_date: {today_date}
- sort_by: "net"

依次查询：
1. 海通期货（代客）
2. 东证期货（代客）
3. 国泰君安（代客）

**输出要求**：
- 将3家数据按品种合并
- 计算**当日净多头增仓TOP5**（品种、合计手数、各家明细）
- 计算**当日净空头增仓TOP5**（品种、合计手数、各家明细）

## 第二步：机构5日累计持仓变化 + 资金计算 ⭐必做
查询3家机构**近5个交易日**累计变化：

调用 `tool_analyze_broker_positions`，参数：
- start_date: {five_days_ago}
- end_date: {today_date}
- sort_by: "net"

**重要！计算资金价值**：
汇总完5日累计持仓后，调用 `batch_calculate_position_value` 计算资金：
- 格式: "M:115000,SA:-96000,RB:50000,..."
- 这个工具会从数据库查询最新价格，计算准确的资金金额

**输出要求**：
- 计算**5日累计净多头增仓TOP5品种**（手数 + 资金亿元）
- 计算**5日累计净空头增仓TOP5品种**（手数 + 资金亿元）

## 第三步：外资当日持仓变化 ⭐必做
查询2家外资的当日持仓变化：
- start_date: {yesterday}
- end_date: {today_date}

1. 乾坤期货
2. 摩根大通

## 第四步：反指标当日持仓变化 ⭐必做
查询3家反指标的当日持仓变化：
- start_date: {yesterday}
- end_date: {today_date}

1. 中信建投（代客）
2. 东方财富
3. 方正中期（代客）

**重要**：只汇总品种和合计方向，**不记录具体期货商名称**。
**注意区分**：
- 净持仓 > 0 的品种 → 反指标做多
- 净持仓 < 0 的品种 → 反指标做空

## 第五步：正反指标分歧分析 ⭐核心任务
对比机构和反指标的持仓方向，找出分歧品种：
- 【经典分歧A】机构做多 + 反指标做空 → 跟随机构做多
- 【经典分歧B】机构做空 + 反指标做多 → 跟随机构做空
- 【罕见共识】机构和反指标同方向 → 需要警惕

## 第六步：分歧品种技术面验证 ⭐必做
对正反分歧最明显的前2-3个品种，调用 `analyze_kline_pattern` 验证技术面。

【输出格式总结】：
1. 品种名称要统一（合并不同合约）
2. 5日累计要有资金金额（从数据库查询计算）
3. 必须有正反分歧分析
4. 必须有技术面验证结果
5. 反指标只汇总品种和方向，不显示期货商名称
6. 反指标数据要区分做多（净持仓>0）和做空（净持仓<0）
"""

    reporter_agent = create_react_agent(llm, tools, prompt=system_prompt)

    try:
        trigger_msg = f"""开始期货商持仓扫描：

交易日参数：
- 当日变化：{yesterday} vs {today_date}
- 5日累计：{five_days_ago} vs {today_date}

请按顺序完成：
1. 机构当日持仓变化（合并同品种）
2. 机构5日累计持仓变化（用 batch_calculate_position_value 计算资金）
3. 外资当日持仓变化
4. 反指标当日持仓变化（只汇总品种，区分做多和做空）
5. 正反指标分歧分析
6. 分歧品种技术面验证
"""

        def _do_collect():
            result = reporter_agent.invoke(
                {"messages": [HumanMessage(content=trigger_msg)]},
                {"recursion_limit": 200}
            )
            return _extract_reporter_material(result)

        invoke_result = _invoke_with_retry(_do_collect, "collect_material")
        if not invoke_result.get("ok"):
            print(
                f"[collect_material] failed after {invoke_result.get('attempts', 0)} attempts, "
                f"elapsed={invoke_result.get('elapsed_seconds', 0.0):.1f}s, "
                f"category={invoke_result.get('error_category')}, "
                f"error={invoke_result.get('error_message')}"
            )
            return {
                "ok": False,
                "material": "",
                "error_category": invoke_result.get("error_category", "collect_failed"),
                "error_message": invoke_result.get("error_message", ""),
                "attempts": invoke_result.get("attempts", 0),
                "elapsed_seconds": invoke_result.get("elapsed_seconds", 0.0),
            }

        collected_content = str(invoke_result.get("result") or "")

        print("✅[持仓记者] 采集完成。")
        return {
            "ok": True,
            "material": collected_content,
            "error_category": "",
            "error_message": "",
            "attempts": invoke_result.get("attempts", 0),
            "elapsed_seconds": invoke_result.get("elapsed_seconds", 0.0),
        }

    except Exception as e:
        print(f"❌ [持仓记者] 采集出错: {e}")
        import traceback
        traceback.print_exc()
        return {
            "ok": False,
            "material": "",
            "error_category": "collect_internal_error",
            "error_message": f"{type(e).__name__}: {e}",
            "attempts": 0,
            "elapsed_seconds": 0.0,
        }


# ==========================================
# 6. AI 主编 - 撰写报告
# ==========================================
def draft_broker_position_report(raw_material):
    print("✏️ [持仓主编] 正在撰写晚报...")

    today = datetime.now().strftime("%Y年%m月%d日")
    weekday = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][datetime.now().weekday()]

    prompt = f"""
你是【爱波塔持仓研究中心】主编，正在撰写《爱波塔-持仓数据流晚报》。

【记者提交的素材】：
{raw_material}

【期货商分类】：
- **机构**：海通期货、东证期货、国泰君安（正指标，跟随）
- **外资**：乾坤期货、摩根大通（正指标，跟随）
- **反指标**：散户聚集地（报告中**不显示具体名称**，反向参考）

【报告结构】（共6个板块）：
1. 今日核心信号 ← 聚焦正反分歧+技术验证
2. 机构当日动向
3. 机构5日累计布局 ← 用资金金额
4. 外资风向标
5. 反指标信号
6. AI毒舌点评

【设计规范】：
- 主色：#6366f1（紫色）
- 多头：#ef4444（红色）
- 空头：#22c55e（绿色）
- 辅助：#94a3b8（灰色）
- 风格：极简高端

【关键要求】：

### 1. 今日核心信号（最重要！）
必须包含：
- **正反分歧品种**：机构和反指标方向相反的品种
- **技术面验证结果**：是否形成"资金+技术"共振

### 2. 机构5日累计布局
- 必须显示**资金金额（亿元）**
- 格式：品种名 +XX万手 (约X.X亿)

### 3. 反指标信号区分
- **反指标做多区块**：只放净持仓 > 0 的品种（数字为正，红色）
- **反指标做空区块**：只放净持仓 < 0 的品种（数字为负，绿色）

### 4. 机构区块名单约束（强制）
- “机构当日动向”明细中，只允许出现海通/东证/国泰相关明细
- 禁止出现：中信建投、方正中期、东方财富（及简称）

### 4. AI毒舌点评
- 风格：幽默、毒舌、有梗
- 必须聚焦正反指标对比

【HTML模板】：

```html
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <style>
    * {{ box-sizing: border-box; }}
    body {{ margin:0; padding:0; background:#0f172a; font-family:'PingFang SC','Microsoft YaHei',system-ui,sans-serif; color:#e2e8f0; }}
    .container {{ max-width:680px; margin:0 auto; padding:32px 20px; }}
    .card {{ background:rgba(30,41,59,0.8); border:1px solid rgba(99,102,241,0.2); border-radius:12px; padding:20px; margin-bottom:24px; }}
    .section-title {{ color:#6366f1; font-size:17px; font-weight:600; margin:0 0 16px 0; display:flex; align-items:center; gap:8px; }}
    .section-title::before {{ content:''; width:3px; height:18px; background:#6366f1; border-radius:2px; }}
    .sub-text {{ color:#94a3b8; font-size:12px; }}
    .data-table {{ width:100%; border-collapse:collapse; margin-top:12px; }}
    .data-table th {{ text-align:left; padding:10px 8px; color:#94a3b8; font-size:12px; font-weight:500; border-bottom:1px solid rgba(255,255,255,0.06); }}
    .data-table td {{ padding:12px 8px; font-size:13px; border-bottom:1px solid rgba(255,255,255,0.03); }}
    .text-red {{ color:#ef4444; }}
    .text-green {{ color:#22c55e; }}
    .text-purple {{ color:#6366f1; }}
    .text-gray {{ color:#94a3b8; }}
    .tag {{ display:inline-block; padding:2px 8px; border-radius:4px; font-size:11px; margin-right:6px; }}
    .tag-long {{ background:rgba(239,68,68,0.15); color:#ef4444; }}
    .tag-short {{ background:rgba(34,197,94,0.15); color:#22c55e; }}
    .tag-warn {{ background:rgba(251,191,36,0.15); color:#fbbf24; }}
    .highlight-box {{ background:rgba(99,102,241,0.1); border-left:3px solid #6366f1; padding:16px; border-radius:0 8px 8px 0; margin:16px 0; }}
    .detail-text {{ font-size:11px; color:#94a3b8; margin-top:4px; }}
    .signal-item {{ margin-bottom:16px; padding:12px; background:rgba(99,102,241,0.05); border-radius:8px; }}
  </style>
</head>
<body>
<div class="container">

  <!-- 标题 -->
  <div style="text-align:center; margin-bottom:32px;">
    <h1 style="color:#6366f1; font-size:26px; margin:0 0 8px 0; font-weight:700;">爱波塔-持仓数据流晚报</h1>
    <p class="sub-text">{today} {weekday} · 追踪聪明钱动向</p>
  </div>

  <!-- 今日核心信号 -->
  <div class="card" style="border-color:rgba(99,102,241,0.4);">
    <h2 class="section-title">今日核心信号</h2>
    <div style="line-height:1.9; font-size:14px;">
      <!-- 核心信号：正反分歧 + 技术验证 -->
    </div>
  </div>

  <!-- 机构当日动向 -->
  <div class="card">
    <h2 class="section-title">机构当日动向</h2>
    <p class="sub-text" style="margin:-8px 0 16px 0;">海通 · 东证 · 国泰君安 ｜ 当日净持仓变化</p>

    <p style="font-size:14px; font-weight:600; margin:16px 0 8px 0;"><span class="text-red">●</span> 当日净多头增仓 TOP5</p>
    <table class="data-table">
      <tr><th>品种</th><th style="text-align:right;">合计</th><th style="text-align:right;">明细</th></tr>
    </table>

    <p style="font-size:14px; font-weight:600; margin:24px 0 8px 0;"><span class="text-green">●</span> 当日净空头增仓 TOP5</p>
    <table class="data-table">
      <tr><th>品种</th><th style="text-align:right;">合计</th><th style="text-align:right;">明细</th></tr>
    </table>
  </div>

  <!-- 机构5日累计布局 -->
  <div class="card">
    <h2 class="section-title">机构5日累计布局</h2>
    <p class="sub-text" style="margin:-8px 0 16px 0;">海通 · 东证 · 国泰君安 ｜ 近5个交易日累计 · 按资金规模排序</p>

    <div style="display:grid; grid-template-columns:1fr 1fr; gap:16px;">
      <div>
        <p style="font-size:13px; font-weight:600; margin:0 0 8px 0;"><span class="text-red">●</span> 累计做多</p>
        <div style="font-size:13px; line-height:1.8;">
          <!-- 格式：1. 豆粕 <span class="text-red">+11.5万手</span> <span class="text-gray">(约3.7亿)</span> -->
        </div>
      </div>
      <div>
        <p style="font-size:13px; font-weight:600; margin:0 0 8px 0;"><span class="text-green">●</span> 累计做空</p>
        <div style="font-size:13px; line-height:1.8;">
        </div>
      </div>
    </div>
  </div>

  <!-- 外资风向标 -->
  <div class="card">
    <h2 class="section-title">外资风向标</h2>
    <p class="sub-text" style="margin:-8px 0 16px 0;">乾坤期货 · 摩根大通 ｜ 当日净持仓变化</p>
    <div style="line-height:1.9; font-size:13px;">
    </div>
  </div>

  <!-- 反指标信号 -->
  <div class="card">
    <h2 class="section-title">反指标信号</h2>
    <p class="sub-text" style="margin:-8px 0 16px 0;">散户聚集地 ｜ 当日净持仓变化 · 反向参考</p>

    <!-- 反指标做多区块：只放净持仓 > 0 的品种 -->
    <p style="font-size:14px; font-weight:600; margin:16px 0 8px 0;"><span class="tag tag-short">反着看</span> 反指标大幅做多</p>
    <table class="data-table">
      <tr><th>品种</th><th style="text-align:right;">合计净多</th><th>潜在信号</th></tr>
      <!-- ⚠️ 只放净持仓 > 0 的品种，数字为正（红色） -->
    </table>

    <!-- 反指标做空区块：只放净持仓 < 0 的品种 -->
    <p style="font-size:14px; font-weight:600; margin:24px 0 8px 0;"><span class="tag tag-long">反着看</span> 反指标大幅做空</p>
    <table class="data-table">
      <tr><th>品种</th><th style="text-align:right;">合计净空</th><th>潜在信号</th></tr>
      <!-- ⚠️ 只放净持仓 < 0 的品种，数字为负（绿色） -->
    </table>
  </div>

  <!-- AI毒舌点评 -->
  <div class="card" style="border-color:rgba(99,102,241,0.4);">
    <h2 class="section-title">AI毒舌点评</h2>
    <div class="highlight-box" style="font-size:14px; line-height:2.0;">
      <!-- 2-3段毒舌点评，聚焦正反对比 -->
    </div>
  </div>

  <!-- 底部 -->
  <div style="text-align:center; padding:24px 0 0 0; border-top:1px solid rgba(255,255,255,0.06);">
    <p class="sub-text">⚠️ 本报告仅供参考，不构成投资建议</p>
    <p class="sub-text" style="margin-top:8px;">爱波塔 · 期货商持仓研究中心</p>
  </div>

</div>
</body>
</html>
```

【检查清单】：
- [ ] 今日核心信号：有正反分歧 + 技术验证
- [ ] 5日累计：有资金金额（亿元）
- [ ] 品种去重（无重复）
- [ ] 反指标不显示期货商名称
- [ ] 机构区块不出现反指标名称（中信建投/方正中期/东方财富）
- [ ] **反指标做多区块：只放净持仓>0的品种（数字为正，红色）**
- [ ] **反指标做空区块：只放净持仓<0的品种（数字为负，绿色）**
- [ ] AI毒舌点评：幽默+正反对比

【输出】：只返回HTML代码。
"""

    try:
        def _do_draft():
            return llm.invoke([HumanMessage(content=prompt)])

        invoke_result = _invoke_with_retry(_do_draft, "draft_report")
        if not invoke_result.get("ok"):
            print(
                f"[draft_report] failed after {invoke_result.get('attempts', 0)} attempts, "
                f"elapsed={invoke_result.get('elapsed_seconds', 0.0):.1f}s, "
                f"category={invoke_result.get('error_category')}, "
                f"error={invoke_result.get('error_message')}"
            )
            return {
                "ok": False,
                "report_html": "",
                "error_category": invoke_result.get("error_category", "draft_failed"),
                "error_message": invoke_result.get("error_message", ""),
                "attempts": invoke_result.get("attempts", 0),
                "elapsed_seconds": invoke_result.get("elapsed_seconds", 0.0),
            }

        res = invoke_result.get("result")
        html = getattr(res, "content", "")
        if not isinstance(html, str):
            html = str(html or "")
        html = html.replace("```html", "").replace("```", "").strip()
    except Exception as e:
        return {
            "ok": False,
            "report_html": "",
            "error_category": "draft_internal_error",
            "error_message": f"{type(e).__name__}: {e}",
            "attempts": 0,
            "elapsed_seconds": 0.0,
        }

    # 核心持仓与分歧信号全部改为代码端确定性计算，避免 LLM 误判方向
    today_date, yesterday, five_days_ago = get_recent_trading_days(5)
    institution_day = _build_institution_day_snapshot(yesterday, today_date)
    institution_5d = _build_institution_5d_snapshot(five_days_ago, today_date)
    contra_day = _build_contra_day_snapshot(yesterday, today_date)
    divergence = _build_divergence_snapshot(institution_5d, contra_day)
    focus_products = [str(x.get("name", "")) for x in divergence.get("divergences", []) if x.get("name")]
    tech_views = _batch_get_tech_views(focus_products[:3])

    html = enforce_core_signal_section(html, divergence, tech_views)
    html = enforce_institution_day_section(html, institution_day)
    html = enforce_institution_5d_section(html, institution_5d)
    html = enforce_contra_signal_section(html, contra_day, institution_5d)

    html = sanitize_institution_section(html)
    return {
        "ok": True,
        "report_html": html,
        "error_category": "",
        "error_message": "",
        "attempts": invoke_result.get("attempts", 0),
        "elapsed_seconds": invoke_result.get("elapsed_seconds", 0.0),
    }


# ==========================================
# 7. 发布
# ==========================================
def extract_summary_from_html(html_content: str) -> str:
    import re
    match = re.search(r'今日核心信号.*?<p[^>]*>(.*?)</p>', html_content, re.DOTALL)
    if match:
        summary = re.sub(r'<[^>]+>', '', match.group(1))
        return summary[:200].strip()
    return "今日期货商持仓动向分析"


def publish_broker_position_report(html_content: str):
    print("📤 [发布] 正在发布...")

    today_str = datetime.now().strftime("%m月%d日")
    weekday = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][datetime.now().weekday()]

    title = f"{today_str} {weekday} 期货商持仓晚报"
    summary = extract_summary_from_html(html_content)

    try:
        success, result = sub_svc.publish_content(
            channel_code="broker_position_report",
            title=title,
            content=html_content,
            summary=summary if summary else f"{today_str}期货商持仓动向分析"
        )
        if success:
            print(f"✅ [发布] 成功，ID: {result}")
            return True, result
        else:
            print(f"❌ [发布] 失败: {result}")
            return False, result
    except Exception as e:
        print(f"❌ [发布] 异常: {e}")
        return False, str(e)


# ==========================================
# 8. 主流程（旧版本保留）
# ==========================================
def _legacy_main():
    # Keep compatibility for any external caller that still imports legacy entrypoint.
    return main()

    start_t = time.time()
    print("=" * 60)
    print("🏛️ 期货商持仓晚报生成器 v6.0")
    print("=" * 60)

    if should_skip_non_trading_publish():
        print("✅ 任务已安全跳过（非交易日门禁）")
        return

    print("\n【第一步】数据采集...")
    material = collect_broker_position_data()

    if len(material) < 100:
        print(f"❌ 素材过少（{len(material)}字），内容：{repr(material[:300])}")
        return

    with open("broker_material_debug.txt", "w", encoding="utf-8") as f:
        f.write(material)
    print("📝 素材已保存: broker_material_debug.txt")

    print("\n【第二步】撰写报告...")
    report_html = draft_broker_position_report(material)

    preview_path = "preview_broker_position_report.html"
    with open(preview_path, "w", encoding="utf-8") as f:
        f.write(report_html)
    print(f"✅ 预览: {preview_path}")

    if len(report_html) < 300:
        print("❌ 报告过短")
        return

    print("\n【第三步】发布...")
    pub_success, pub_result = publish_broker_position_report(report_html)

    print(f"\n{'=' * 60}")
    print(f"📊 结果汇总")
    print(f"{'=' * 60}")
    print(f"采集: ✅ | 撰写: ✅ | 发布: {'✅' if pub_success else '❌'}")
    print(f"预览: {preview_path}")
    print(f"耗时: {time.time() - start_t:.1f}s")
    print("=" * 60)


def main():
    start_t = time.time()
    print("=" * 60)
    print("Broker Position Report Generator v6.0")
    print("=" * 60)

    if should_skip_non_trading_publish():
        print("Task skipped by non-trading-day gate.")
        return

    print("\n[Step 1] Collect material...")
    collect_result = collect_broker_position_data()
    if not isinstance(collect_result, dict):
        collect_result = {
            "ok": False,
            "material": "",
            "error_category": "collect_contract_error",
            "error_message": f"unexpected collect return type: {type(collect_result).__name__}",
            "attempts": 0,
            "elapsed_seconds": 0.0,
        }

    if not collect_result.get("ok"):
        meta = {
            "stage": "collect",
            "error_category": collect_result.get("error_category", "collect_failed"),
            "error_message": collect_result.get("error_message", ""),
            "attempts": collect_result.get("attempts", 0),
            "elapsed_seconds": collect_result.get("elapsed_seconds", 0.0),
        }
        _write_material_debug("", meta)
        print(
            f"[collect] failed | category={meta['error_category']} | "
            f"attempts={meta['attempts']} | elapsed={meta['elapsed_seconds']:.1f}s | "
            f"error={meta['error_message']}"
        )
        sys.exit(1)

    material = str(collect_result.get("material") or "").strip()
    if len(material) < 100:
        meta = {
            "stage": "collect_quality",
            "error_category": "material_too_short",
            "error_message": f"material length too short: {len(material)}",
            "attempts": collect_result.get("attempts", 0),
            "elapsed_seconds": collect_result.get("elapsed_seconds", 0.0),
        }
        _write_material_debug(material, meta)
        print(f"[collect] material too short ({len(material)} chars), abort.")
        sys.exit(1)

    _write_material_debug(
        material,
        {
            "stage": "collect_success",
            "attempts": collect_result.get("attempts", 0),
            "elapsed_seconds": collect_result.get("elapsed_seconds", 0.0),
        },
    )

    print("\n[Step 2] Draft report...")
    draft_result = draft_broker_position_report(material)
    if not isinstance(draft_result, dict):
        draft_result = {
            "ok": False,
            "report_html": "",
            "error_category": "draft_contract_error",
            "error_message": f"unexpected draft return type: {type(draft_result).__name__}",
            "attempts": 0,
            "elapsed_seconds": 0.0,
        }

    if not draft_result.get("ok"):
        meta = {
            "stage": "draft",
            "error_category": draft_result.get("error_category", "draft_failed"),
            "error_message": draft_result.get("error_message", ""),
            "attempts": draft_result.get("attempts", 0),
            "elapsed_seconds": draft_result.get("elapsed_seconds", 0.0),
        }
        _write_material_debug(material, meta)
        print(
            f"[draft] failed | category={meta['error_category']} | "
            f"attempts={meta['attempts']} | elapsed={meta['elapsed_seconds']:.1f}s | "
            f"error={meta['error_message']}"
        )
        sys.exit(2)

    report_html = str(draft_result.get("report_html") or "")
    preview_path = "preview_broker_position_report.html"
    with open(preview_path, "w", encoding="utf-8") as f:
        f.write(report_html)
    print(f"[draft] preview written: {preview_path}")

    if len(report_html) < 300:
        meta = {
            "stage": "draft_quality",
            "error_category": "report_too_short",
            "error_message": f"report html too short: {len(report_html)}",
            "attempts": draft_result.get("attempts", 0),
            "elapsed_seconds": draft_result.get("elapsed_seconds", 0.0),
        }
        _write_material_debug(material, meta)
        print("[draft] report too short, abort.")
        sys.exit(2)

    print("\n[Step 3] Publish report...")
    pub_success, pub_result = publish_broker_position_report(report_html)
    if not pub_success:
        _write_material_debug(
            material,
            {
                "stage": "publish",
                "error_category": "publish_failed",
                "error_message": str(pub_result),
                "attempts": 1,
                "elapsed_seconds": 0.0,
            },
        )
        print(f"[publish] failed: {pub_result}")
        sys.exit(3)

    print(f"\n{'=' * 60}")
    print("Result Summary")
    print(f"{'=' * 60}")
    print("collect: OK | draft: OK | publish: OK")
    print(f"preview: {preview_path}")
    print(f"elapsed: {time.time() - start_t:.1f}s")
    print("=" * 60)


if __name__ == "__main__":
    main()
