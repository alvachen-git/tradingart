from __future__ import annotations

import json
import math
import os
import re
import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import tushare as ts
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(override=True)

_DB_USER = os.getenv("DB_USER")
_DB_PASSWORD = os.getenv("DB_PASSWORD")
_DB_HOST = os.getenv("DB_HOST")
_DB_PORT = os.getenv("DB_PORT", "3306")
_DB_NAME = os.getenv("DB_NAME")

_TEMPLATE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "static", "industry_chain_templates.json"
)
_FLOW_EPS = 1e-6
_EXTERNAL_IN_ID = "external_in"
_EXTERNAL_OUT_ID = "external_out"


def get_db_engine():
    if not all([_DB_USER, _DB_PASSWORD, _DB_HOST, _DB_NAME]):
        return None
    db_url = (
        f"mysql+pymysql://{_DB_USER}:{_DB_PASSWORD}@{_DB_HOST}:{_DB_PORT}/{_DB_NAME}"
    )
    return create_engine(db_url, pool_pre_ping=True, pool_recycle=3600)


def get_tushare_pro():
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        return None
    try:
        ts.set_token(token)
        return ts.pro_api()
    except Exception:
        return None


def load_chain_templates(path: str = _TEMPLATE_PATH) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def parse_domain_tags(domain_tags_text: Any) -> List[str]:
    if not domain_tags_text:
        return []
    s = str(domain_tags_text).strip()
    if not s:
        return []
    if s.startswith("["):
        try:
            parsed = json.loads(s)
            tags = [str(x).strip() for x in parsed if str(x).strip()]
            return tags[:3]
        except Exception:
            pass
    for sep in ["|", ",", "，", "/", "、", ";", "；"]:
        s = s.replace(sep, "|")
    tags = [x.strip() for x in s.split("|") if x.strip()]
    return tags[:3]


def calc_fund_signal(main_net_amount_1d: float, main_net_amount_5d: float) -> str:
    if main_net_amount_5d > 0 and main_net_amount_1d > 0:
        return "持续流入"
    if main_net_amount_5d > 0 and main_net_amount_1d <= 0:
        return "短线分歧"
    if main_net_amount_5d <= 0 and main_net_amount_1d > 0:
        return "反抽修复"
    return "持续流出"


def _to_float(v: Any) -> float:
    try:
        if v is None:
            return 0.0
        if isinstance(v, str) and not v.strip():
            return 0.0
        return float(v)
    except Exception:
        return 0.0


def split_net_flow(net_value: float) -> Tuple[float, float]:
    net_value = _to_float(net_value)
    return max(net_value, 0.0), max(-net_value, 0.0)


def scale_flow_width(flow_value_abs: float, mode: str = "log", a: float = 0.6, b: float = 1.4) -> float:
    x = max(_to_float(flow_value_abs), 0.0)
    mode = str(mode or "log").lower()
    if mode == "linear":
        return x
    if mode in {"bucket", "tier", "分档"}:
        if x <= 0:
            return 0.0
        if x < 500:
            return 1.0
        if x < 2000:
            return 2.0
        if x < 10000:
            return 3.0
        if x < 50000:
            return 4.0
        return 5.0
    # 默认对数：增强中小流量可见度，保持单调。
    return a + b * math.log10(1.0 + x)


def _normalize_ts_code(code: Any) -> str:
    return str(code or "").strip().upper()


def _build_in_params(values: Iterable[str], prefix: str = "p") -> Tuple[str, Dict[str, str]]:
    vals = list(values)
    params: Dict[str, str] = {}
    placeholders: List[str] = []
    for i, value in enumerate(vals):
        key = f"{prefix}{i}"
        placeholders.append(f":{key}")
        params[key] = value
    return ",".join(placeholders), params


def _fetch_max_trade_date(engine, table_name: str) -> Optional[str]:
    if engine is None:
        return None
    try:
        with engine.connect() as conn:
            d = conn.execute(text(f"SELECT MAX(trade_date) FROM {table_name}")).scalar()
        if d is None:
            return None
        return str(d).replace("-", "")
    except Exception:
        return None


def _fetch_screener_trade_date(engine, prefer_le_trade_date: Optional[str] = None) -> Optional[str]:
    """
    选择 daily_stock_screener 的“完整交易日”：
    - 先看最近20个交易日的行数分布
    - 仅接受 >= max(500, max_cnt*0.6) 的日期，避免误选到盘中半成品数据
    - 如提供 prefer_le_trade_date（通常是资金日期），优先选 <= 该日期的最新完整日
    """
    if engine is None:
        return None
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT trade_date, COUNT(*) AS cnt
                    FROM daily_stock_screener
                    GROUP BY trade_date
                    ORDER BY trade_date DESC
                    LIMIT 20
                    """
                )
            ).fetchall()
    except Exception:
        return None

    if not rows:
        return None

    parsed = [(str(r[0]).replace("-", ""), int(r[1])) for r in rows]
    max_cnt = max(x[1] for x in parsed)
    min_cnt = max(500, int(max_cnt * 0.6))
    eligible = [x[0] for x in parsed if x[1] >= min_cnt]
    if not eligible:
        return parsed[0][0]

    if prefer_le_trade_date:
        for d in eligible:
            if d <= prefer_le_trade_date:
                return d
    return eligible[0]


def get_recent_screener_dates(engine=None, limit: int = 20) -> List[str]:
    engine = engine if engine is not None else get_db_engine()
    if engine is None:
        return []
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT DISTINCT trade_date
                    FROM daily_stock_screener
                    ORDER BY trade_date DESC
                    LIMIT :n
                    """
                ),
                {"n": int(limit)},
            ).fetchall()
        return [str(r[0]).replace("-", "") for r in rows if r and r[0]]
    except Exception:
        return []


def _fetch_profile_max_updated_at(engine) -> Optional[str]:
    if engine is None:
        return None
    try:
        with engine.connect() as conn:
            ts_max = conn.execute(text("SELECT MAX(tags_updated_at) FROM stock_company_profile_cache")).scalar()
        if ts_max is None:
            return None
        return str(ts_max)
    except Exception:
        return None


def _normalize_domain_tags(tags: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for tag in tags:
        t = str(tag or "").strip()
        t = re.sub(r"[\s\-_/,;，。；、|]+", "", t)
        if len(t) < 2:
            continue
        if len(t) > 8:
            t = t[:8]
        if t not in seen:
            seen.add(t)
            out.append(t)
        if len(out) >= 3:
            break
    return out


def _fallback_extract_domain_tags(text_value: str) -> List[str]:
    text_l = str(text_value or "").lower()
    mapping = [
        ("芯片设计", ["芯片设计", "ic设计", "soc", "eda", "数字芯片", "模拟芯片"]),
        ("晶圆制造", ["晶圆", "代工", "wafer", "fab"]),
        ("封装测试", ["封装", "封测", "测试服务"]),
        ("半导体材料", ["光刻胶", "硅片", "靶材", "电子特气", "抛光液", "半导体材料"]),
        ("半导体设备", ["刻蚀", "薄膜", "清洗", "检测设备", "半导体设备", "设备"]),
        ("功率器件", ["igbt", "mosfet", "功率半导体", "功率器件"]),
        ("存储芯片", ["dram", "nand", "存储"]),
        ("传感器", ["传感器", "cmos"]),
        ("汽车电子", ["车规", "汽车电子", "新能源车"]),
        ("消费电子", ["消费电子", "手机", "pc", "可穿戴"]),
        ("工业控制", ["工业控制", "工控", "自动化"]),
        ("AI算力", ["ai", "算力", "服务器", "数据中心"]),
    ]
    hit = [tag for tag, kws in mapping if any(k in text_l for k in kws)]
    hit = _normalize_domain_tags(hit)
    if hit:
        return hit
    words = re.findall(r"[\u4e00-\u9fa5]{2,8}", str(text_value or ""))
    words = [w for w in words if w not in {"公司", "业务", "产品", "技术", "服务", "客户", "市场", "以及"}]
    return _normalize_domain_tags(words[:6]) or ["综合业务"]


def _fetch_profiles_from_tushare_and_cache(engine, pro, codes: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    页面兜底：当缓存缺失时，按代码实时补拉主营信息并回填缓存。
    为避免页面超时，仅用于少量缺口代码。
    """
    if pro is None or not codes:
        return {}

    rows_to_save: List[Dict[str, Any]] = []
    out: Dict[str, Dict[str, Any]] = {}
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for code in codes:
        try:
            df = pro.stock_company(ts_code=code)
        except Exception:
            time.sleep(0.05)
            continue
        if df is None or df.empty:
            continue

        row = df.iloc[0]
        company_name = str(row.get("com_name") or "").strip()
        main_business = str(row.get("main_business") or "").strip()
        business_scope = str(row.get("business_scope") or "").strip()
        exchange = str(row.get("exchange") or "").strip()
        tags = _fallback_extract_domain_tags(main_business + "\n" + business_scope)

        out[code] = {
            "company_name": company_name,
            "main_business": main_business,
            "business_scope": business_scope,
            "domain_tags": tags,
            "domain_tags_text": " / ".join(tags),
            "tags_updated_at": now_str,
        }

        rows_to_save.append(
            {
                "ts_code": code,
                "company_name": company_name,
                "exchange": exchange,
                "main_business": main_business,
                "business_scope": business_scope,
                "domain_tags": "|".join(tags),
                "tags_model": "fallback-live",
                "profile_hash": "",
                "source_updated_at": now_str,
                "tags_updated_at": now_str,
            }
        )

    if engine is not None and rows_to_save:
        try:
            with engine.begin() as conn:
                sql = text(
                    """
                    INSERT INTO stock_company_profile_cache (
                        ts_code, company_name, exchange, main_business, business_scope,
                        domain_tags, tags_model, profile_hash, source_updated_at, tags_updated_at
                    ) VALUES (
                        :ts_code, :company_name, :exchange, :main_business, :business_scope,
                        :domain_tags, :tags_model, :profile_hash, :source_updated_at, :tags_updated_at
                    )
                    ON DUPLICATE KEY UPDATE
                        company_name=VALUES(company_name),
                        exchange=VALUES(exchange),
                        main_business=VALUES(main_business),
                        business_scope=VALUES(business_scope),
                        domain_tags=VALUES(domain_tags),
                        tags_model=VALUES(tags_model),
                        source_updated_at=VALUES(source_updated_at),
                        tags_updated_at=VALUES(tags_updated_at)
                    """
                )
                for row in rows_to_save:
                    conn.execute(sql, row)
        except Exception:
            pass

    return out


def fetch_stage_members_from_tushare(
    stages: List[Dict[str, Any]],
    pro,
) -> Tuple[Dict[str, List[Dict[str, str]]], List[str]]:
    members: Dict[str, List[Dict[str, str]]] = {}
    warnings: List[str] = []

    if pro is None:
        warnings.append("Tushare 不可用，无法拉取产业链成分股。")
        for stage in stages:
            members[stage["id"]] = []
        return members, warnings

    for stage in stages:
        stage_id = stage["id"]
        code_set: Dict[str, str] = {}
        for idx_code in stage.get("ths_index_codes", []):
            try:
                df = pro.ths_member(ts_code=idx_code)
            except Exception as e:
                warnings.append(f"阶段 {stage_id} 拉取 {idx_code} 失败: {e}")
                continue

            if df is None or df.empty:
                continue

            for _, row in df.iterrows():
                ts_code = _normalize_ts_code(row.get("con_code"))
                if not ts_code.endswith((".SH", ".SZ")):
                    continue
                name = str(row.get("con_name") or "").strip()
                if ts_code and ts_code not in code_set:
                    code_set[ts_code] = name

        members[stage_id] = [
            {"ts_code": code, "name": name}
            for code, name in sorted(code_set.items(), key=lambda x: x[0])
        ]

    # 处理排除关系（例如下游应用排除前面环节）
    for stage in stages:
        stage_id = stage["id"]
        excludes = stage.get("exclude_stage_ids") or []
        if not excludes:
            continue
        excluded_codes = set()
        for ex in excludes:
            excluded_codes.update(x["ts_code"] for x in members.get(ex, []))
        members[stage_id] = [x for x in members.get(stage_id, []) if x["ts_code"] not in excluded_codes]

    return members, warnings


def _query_screener_map(engine, codes: List[str], trade_date: Optional[str]) -> Dict[str, Dict[str, Any]]:
    if engine is None or not codes or not trade_date:
        return {}
    placeholders, params = _build_in_params(codes, prefix="s")
    params["trade_date"] = trade_date
    sql = text(
        f"""
        SELECT ts_code, name, industry, pattern, ma_trend, score
        FROM daily_stock_screener
        WHERE trade_date=:trade_date AND ts_code IN ({placeholders})
        """
    )
    try:
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params=params)
    except Exception:
        return {}

    data: Dict[str, Dict[str, Any]] = {}
    for _, row in df.iterrows():
        code = _normalize_ts_code(row.get("ts_code"))
        data[code] = {
            "name": str(row.get("name") or "").strip(),
            "industry": str(row.get("industry") or "").strip(),
            "pattern": str(row.get("pattern") or "").strip(),
            "ma_trend": str(row.get("ma_trend") or "").strip(),
            "score": int(_to_float(row.get("score"))),
        }
    return data


def _query_fund_map(engine, codes: List[str], fund_trade_date: Optional[str]) -> Dict[str, Dict[str, float]]:
    if engine is None or not codes or not fund_trade_date:
        return {}

    placeholders, params = _build_in_params(codes, prefix="f")
    params["fund_trade_date"] = fund_trade_date

    try:
        with engine.connect() as conn:
            date_df = pd.read_sql(
                text(
                    """
                    SELECT DISTINCT trade_date
                    FROM stock_moneyflow_daily
                    WHERE trade_date <= :fund_trade_date
                    ORDER BY trade_date DESC
                    LIMIT 5
                    """
                ),
                conn,
                params={"fund_trade_date": fund_trade_date},
            )

            if date_df.empty:
                return {}

            d5 = [str(x).replace("-", "") for x in date_df["trade_date"].tolist()]
            d5_placeholders, d5_params = _build_in_params(d5, prefix="d")

            q1 = text(
                f"""
                SELECT ts_code, main_net_amount
                FROM stock_moneyflow_daily
                WHERE trade_date=:fund_trade_date AND ts_code IN ({placeholders})
                """
            )
            q5 = text(
                f"""
                SELECT ts_code, SUM(main_net_amount) AS main_net_amount_5d
                FROM stock_moneyflow_daily
                WHERE trade_date IN ({d5_placeholders})
                  AND ts_code IN ({placeholders})
                GROUP BY ts_code
                """
            )

            df1 = pd.read_sql(q1, conn, params=params)
            df5 = pd.read_sql(q5, conn, params={**params, **d5_params})
    except Exception:
        return {}

    out: Dict[str, Dict[str, float]] = {}
    for _, row in df1.iterrows():
        code = _normalize_ts_code(row.get("ts_code"))
        out.setdefault(code, {})["main_net_amount_1d"] = _to_float(row.get("main_net_amount"))
    for _, row in df5.iterrows():
        code = _normalize_ts_code(row.get("ts_code"))
        out.setdefault(code, {})["main_net_amount_5d"] = _to_float(row.get("main_net_amount_5d"))

    for code in list(out.keys()):
        out[code].setdefault("main_net_amount_1d", 0.0)
        out[code].setdefault("main_net_amount_5d", 0.0)

    return out


def _query_recent_fund_dates(engine, fund_trade_date: Optional[str], limit: int = 12) -> List[str]:
    if engine is None:
        return []
    try:
        with engine.connect() as conn:
            if fund_trade_date:
                df = pd.read_sql(
                    text(
                        """
                        SELECT DISTINCT trade_date
                        FROM stock_moneyflow_daily
                        WHERE trade_date <= :fund_trade_date
                        ORDER BY trade_date DESC
                        LIMIT :n
                        """
                    ),
                    conn,
                    params={"fund_trade_date": fund_trade_date, "n": int(limit)},
                )
            else:
                df = pd.read_sql(
                    text(
                        """
                        SELECT DISTINCT trade_date
                        FROM stock_moneyflow_daily
                        ORDER BY trade_date DESC
                        LIMIT :n
                        """
                    ),
                    conn,
                    params={"n": int(limit)},
                )
    except Exception:
        return []
    if df is None or df.empty:
        return []
    return [str(x).replace("-", "") for x in df["trade_date"].tolist() if str(x).strip()]


def _query_fund_5d_history_map(
    engine,
    codes: List[str],
    fund_trade_date: Optional[str],
    history_days: int = 3,
) -> Tuple[List[str], Dict[str, Dict[str, float]]]:
    """
    返回最近 history_days 个交易日（含 fund_trade_date）对应的“5D主力净流”：
    - hist_dates: 例如 [T, T-1, T-2]
    - hist_map[ts_code][trade_date] = 截止该 trade_date 的近5日 main_net_amount 求和
    """
    if engine is None or not codes:
        return [], {}
    history_days = max(1, int(history_days))
    recent_dates = _query_recent_fund_dates(engine, fund_trade_date=fund_trade_date, limit=history_days + 8)
    if not recent_dates:
        return [], {}

    hist_dates = recent_dates[:history_days]
    window_dates = recent_dates[: history_days + 4]
    if not window_dates:
        return hist_dates, {}

    code_placeholders, code_params = _build_in_params(codes, prefix="c")
    date_placeholders, date_params = _build_in_params(window_dates, prefix="d")
    sql = text(
        f"""
        SELECT ts_code, trade_date, main_net_amount
        FROM stock_moneyflow_daily
        WHERE ts_code IN ({code_placeholders})
          AND trade_date IN ({date_placeholders})
        """
    )
    try:
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params={**code_params, **date_params})
    except Exception:
        return hist_dates, {}

    code_date_map: Dict[str, Dict[str, float]] = {}
    if df is not None and not df.empty:
        for _, row in df.iterrows():
            code = _normalize_ts_code(row.get("ts_code"))
            d = str(row.get("trade_date") or "").replace("-", "")
            if not code or not d:
                continue
            code_date_map.setdefault(code, {})
            code_date_map[code][d] = _to_float(row.get("main_net_amount"))

    hist_map: Dict[str, Dict[str, float]] = {}
    for code in codes:
        code_u = _normalize_ts_code(code)
        day_values = code_date_map.get(code_u, {})
        out_item: Dict[str, float] = {}
        for idx, d in enumerate(hist_dates):
            d5 = recent_dates[idx : idx + 5]
            out_item[d] = sum(_to_float(day_values.get(x, 0.0)) for x in d5)
        hist_map[code_u] = out_item

    return hist_dates, hist_map


def _query_profile_map(engine, codes: List[str]) -> Dict[str, Dict[str, Any]]:
    if engine is None or not codes:
        return {}

    placeholders, params = _build_in_params(codes, prefix="p")
    sql = text(
        f"""
        SELECT ts_code, company_name, main_business, business_scope, domain_tags, tags_updated_at
        FROM stock_company_profile_cache
        WHERE ts_code IN ({placeholders})
        """
    )

    try:
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params=params)
    except Exception:
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    for _, row in df.iterrows():
        code = _normalize_ts_code(row.get("ts_code"))
        tags = parse_domain_tags(row.get("domain_tags"))
        out[code] = {
            "company_name": str(row.get("company_name") or "").strip(),
            "main_business": str(row.get("main_business") or "").strip(),
            "business_scope": str(row.get("business_scope") or "").strip(),
            "domain_tags": tags,
            "domain_tags_text": " / ".join(tags),
            "tags_updated_at": str(row.get("tags_updated_at") or "").strip(),
        }
    return out


def _sort_stage_companies(rows: List[Dict[str, Any]], limit_per_stage: int) -> List[Dict[str, Any]]:
    rows = sorted(
        rows,
        key=lambda x: (
            -_to_float(x.get("market_cap", 0.0)),
            -_to_float(x.get("main_net_amount_5d", 0.0)),
            -int(_to_float(x.get("score", 0))),
            -_to_float(x.get("main_net_amount_1d", 0.0)),
            str(x.get("ts_code") or ""),
        ),
    )
    return rows[: max(1, int(limit_per_stage))]


def _query_market_cap_map(pro, codes: List[str], trade_date: Optional[str]) -> Dict[str, float]:
    """
    读取指定交易日总市值（total_mv，单位万元）。
    """
    if pro is None or not codes or not trade_date:
        return {}
    try:
        df = pro.daily_basic(
            trade_date=str(trade_date),
            fields="ts_code,total_mv,circ_mv",
        )
    except Exception:
        return {}
    if df is None or df.empty:
        return {}

    code_set = {str(c).upper() for c in codes}
    out: Dict[str, float] = {}
    for _, row in df.iterrows():
        code = _normalize_ts_code(row.get("ts_code"))
        if code not in code_set:
            continue
        total_mv = _to_float(row.get("total_mv"))
        circ_mv = _to_float(row.get("circ_mv"))
        out[code] = total_mv if total_mv > 0 else circ_mv
    return out


def _normalize_flow_window(flow_window: str) -> str:
    fw = str(flow_window or "5D").strip().upper()
    return "1D" if fw == "1D" else "5D"


def _build_flow_edges(
    stage_results: List[Dict[str, Any]],
    structure_edges: List[List[str]],
    flow_window: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, float], Dict[str, float]]:
    stage_ids = [str(x.get("id") or "") for x in stage_results]
    stage_id_set = set(stage_ids)

    outgoing: Dict[str, List[str]] = {sid: [] for sid in stage_ids}
    for edge in structure_edges:
        if len(edge) != 2:
            continue
        src, dst = str(edge[0]), str(edge[1])
        if src in stage_id_set and dst in stage_id_set:
            outgoing[src].append(dst)

    use_key = "net_flow_1d" if _normalize_flow_window(flow_window) == "1D" else "net_flow_5d"
    stage_net: Dict[str, float] = {}
    stage_pos: Dict[str, float] = {}
    stage_neg: Dict[str, float] = {}
    for stage in stage_results:
        sid = str(stage.get("id") or "")
        n = _to_float(stage.get(use_key, 0.0))
        p, o = split_net_flow(n)
        stage_net[sid] = n
        stage_pos[sid] = p
        stage_neg[sid] = o

    internal_flow: Dict[Tuple[str, str], float] = {}
    incoming_internal: Dict[str, float] = {sid: 0.0 for sid in stage_ids}
    for src, downstream in outgoing.items():
        p_src = stage_pos.get(src, 0.0)
        if p_src <= 0 or not downstream:
            continue
        # 仅把链内承接分配给“净流入”为正的下游，避免把大额正流量强行灌入净流出环节。
        eligible_downstream = [dst for dst in downstream if stage_pos.get(dst, 0.0) > _FLOW_EPS]
        if not eligible_downstream:
            continue
        weights = [stage_pos.get(dst, 0.0) for dst in eligible_downstream]
        weight_sum = sum(weights)
        if weight_sum <= 0:
            continue
        for dst, w in zip(eligible_downstream, weights):
            f_ij = p_src * w / weight_sum
            internal_flow[(src, dst)] = f_ij
            incoming_internal[dst] = incoming_internal.get(dst, 0.0) + f_ij

    flow_in_external: Dict[str, float] = {}
    flow_out_external: Dict[str, float] = {}
    for sid in stage_ids:
        p_i = stage_pos.get(sid, 0.0)
        o_i = stage_neg.get(sid, 0.0)
        ext_in = max(0.0, p_i - incoming_internal.get(sid, 0.0))
        flow_in_external[sid] = ext_in
        flow_out_external[sid] = o_i

    edge_results: List[Dict[str, Any]] = []
    for edge in structure_edges:
        if len(edge) != 2:
            continue
        src, dst = str(edge[0]), str(edge[1])
        if src not in stage_id_set or dst not in stage_id_set:
            continue
        f_ij = _to_float(internal_flow.get((src, dst), 0.0))
        edge_results.append(
            {
                "source": src,
                "target": dst,
                "flow_value": f_ij,
                "flow_value_abs": abs(f_ij),
                "flow_type": "internal",
                "is_estimated": True,
            }
        )

    for sid in stage_ids:
        ext_out = _to_float(flow_out_external.get(sid, 0.0))
        if ext_out > 0:
            edge_results.append(
                {
                    "source": sid,
                    "target": _EXTERNAL_OUT_ID,
                    "flow_value": -ext_out,
                    "flow_value_abs": ext_out,
                    "flow_type": "external_out",
                    "is_estimated": True,
                }
            )
        ext_in = _to_float(flow_in_external.get(sid, 0.0))
        if ext_in > 0:
            edge_results.append(
                {
                    "source": _EXTERNAL_IN_ID,
                    "target": sid,
                    "flow_value": ext_in,
                    "flow_value_abs": ext_in,
                    "flow_type": "external_in",
                    "is_estimated": True,
                }
            )

    return edge_results, flow_in_external, flow_out_external


def get_chain_snapshot(
    sector_name: str,
    limit_per_stage: int = 10,
    engine=None,
    pro=None,
    templates: Optional[Dict[str, Any]] = None,
    stage_member_map: Optional[Dict[str, List[Dict[str, str]]]] = None,
    force_screener_trade_date: Optional[str] = None,
    flow_window: str = "5D",
) -> Dict[str, Any]:
    templates = templates or load_chain_templates()
    if sector_name not in templates:
        raise ValueError(f"未配置产业链模板: {sector_name}")

    engine = engine if engine is not None else get_db_engine()
    pro = pro if pro is not None else get_tushare_pro()

    chain_cfg = templates[sector_name]
    stages = chain_cfg.get("stages") or []
    edges = chain_cfg.get("edges") or []

    warnings: List[str] = []
    flow_window = _normalize_flow_window(flow_window)
    flow_mode = "fund_flow"

    if stage_member_map is None:
        stage_member_map, stage_warnings = fetch_stage_members_from_tushare(stages=stages, pro=pro)
        warnings.extend(stage_warnings)

    all_codes: List[str] = []
    for stage in stages:
        for item in stage_member_map.get(stage["id"], []):
            code = _normalize_ts_code(item.get("ts_code"))
            if code:
                all_codes.append(code)
    all_codes = sorted(set(all_codes))

    fund_trade_date = _fetch_max_trade_date(engine, "stock_moneyflow_daily")
    if force_screener_trade_date:
        screener_trade_date = str(force_screener_trade_date).strip().replace("-", "")
    else:
        screener_trade_date = _fetch_screener_trade_date(engine, prefer_le_trade_date=fund_trade_date)
        if not screener_trade_date:
            screener_trade_date = _fetch_max_trade_date(engine, "daily_stock_screener")
    profile_updated_at = _fetch_profile_max_updated_at(engine)

    screener_map = _query_screener_map(engine, all_codes, screener_trade_date)
    fund_map = _query_fund_map(engine, all_codes, fund_trade_date)
    fund_history_dates, fund_5d_history_map = _query_fund_5d_history_map(
        engine=engine,
        codes=all_codes,
        fund_trade_date=fund_trade_date,
        history_days=3,
    )
    profile_map = _query_profile_map(engine, all_codes)
    market_cap_map = _query_market_cap_map(pro=pro, codes=all_codes, trade_date=screener_trade_date)

    stage_results: List[Dict[str, Any]] = []
    stage_count_map: Dict[str, int] = {}

    for stage in stages:
        stage_id = stage["id"]
        stage_name = stage["name"]
        stage_members = stage_member_map.get(stage_id, [])

        companies: List[Dict[str, Any]] = []
        for item in stage_members:
            code = _normalize_ts_code(item.get("ts_code"))
            if not code:
                continue
            screener_item = screener_map.get(code, {})
            fund_item = fund_map.get(code, {})
            profile_item = profile_map.get(code, {})

            main_1d = _to_float(fund_item.get("main_net_amount_1d", 0.0))
            main_5d = _to_float(fund_item.get("main_net_amount_5d", 0.0))

            company = {
                "ts_code": code,
                "name": (
                    screener_item.get("name")
                    or profile_item.get("company_name")
                    or str(item.get("name") or "")
                ),
                "industry": str(screener_item.get("industry") or ""),
                "pattern": str(screener_item.get("pattern") or ""),
                "ma_trend": str(screener_item.get("ma_trend") or ""),
                "score": int(_to_float(screener_item.get("score", 0))),
                "market_cap": _to_float(market_cap_map.get(code, 0.0)),
                "main_net_amount_1d": main_1d,
                "main_net_amount_5d": main_5d,
                "fund_signal": calc_fund_signal(main_1d, main_5d),
                "domain_tags": profile_item.get("domain_tags", []),
                "domain_tags_text": profile_item.get("domain_tags_text", ""),
                "main_business": profile_item.get("main_business", ""),
                "business_scope": profile_item.get("business_scope", ""),
                "tags_updated_at": profile_item.get("tags_updated_at", ""),
            }
            companies.append(company)

        companies = _sort_stage_companies(companies, limit_per_stage)

        # 业务标签兜底：仅对最终展示的少量公司按代码补拉并回填缓存
        missing_profile_codes = [
            x["ts_code"]
            for x in companies
            if not str(x.get("domain_tags_text") or "").strip()
        ]
        if missing_profile_codes:
            live_profile_map = _fetch_profiles_from_tushare_and_cache(
                engine=engine, pro=pro, codes=missing_profile_codes
            )
            for x in companies:
                if str(x.get("domain_tags_text") or "").strip():
                    continue
                p = live_profile_map.get(x["ts_code"])
                if not p:
                    continue
                x["name"] = p.get("company_name") or x["name"]
                x["domain_tags"] = p.get("domain_tags", [])
                x["domain_tags_text"] = p.get("domain_tags_text", "")
                x["main_business"] = p.get("main_business", "")
                x["business_scope"] = p.get("business_scope", "")
                x["tags_updated_at"] = p.get("tags_updated_at", "")

        missing_fund_count = sum(1 for x in companies if x["ts_code"] not in fund_map)
        if missing_fund_count > 0:
            warnings.append(
                f"{stage_name} 有 {missing_fund_count} 家公司缺少资金数据，按 0 处理。"
            )

        stage_net_flow_1d = sum(_to_float(x.get("main_net_amount_1d")) for x in companies)
        stage_net_flow_5d = sum(_to_float(x.get("main_net_amount_5d")) for x in companies)
        stage_5d_history: List[Dict[str, Any]] = []
        for d in fund_history_dates:
            d_total = sum(
                _to_float(fund_5d_history_map.get(_normalize_ts_code(x.get("ts_code")), {}).get(d, 0.0))
                for x in companies
            )
            stage_5d_history.append({"trade_date": d, "net_flow_5d": d_total})

        stage_count_map[stage_id] = len(companies)
        stage_results.append(
            {
                "id": stage_id,
                "name": stage_name,
                "companies": companies,
                "company_count": len(companies),
                "net_flow_1d": stage_net_flow_1d,
                "net_flow_5d": stage_net_flow_5d,
                "net_flow_5d_history": stage_5d_history,
                "flow_in_external": 0.0,
                "flow_out_external": 0.0,
            }
        )

    flow_edges, flow_in_external_map, flow_out_external_map = _build_flow_edges(
        stage_results=stage_results,
        structure_edges=edges,
        flow_window=flow_window,
    )

    for stage in stage_results:
        sid = str(stage.get("id") or "")
        stage["flow_in_external"] = _to_float(flow_in_external_map.get(sid, 0.0))
        stage["flow_out_external"] = _to_float(flow_out_external_map.get(sid, 0.0))

    internal_edge_map: Dict[Tuple[str, str], Dict[str, Any]] = {
        (str(x.get("source")), str(x.get("target"))): x
        for x in flow_edges
        if x.get("flow_type") == "internal"
    }

    edge_results: List[Dict[str, Any]] = []
    for edge in edges:
        if len(edge) != 2:
            continue
        src, dst = edge
        value = min(stage_count_map.get(src, 0), stage_count_map.get(dst, 0)) or 1
        flow_edge = internal_edge_map.get((str(src), str(dst)), {})
        flow_value = _to_float(flow_edge.get("flow_value", 0.0))
        edge_results.append(
            {
                "source": src,
                "target": dst,
                "value": float(value),
                "flow_value": flow_value,
                "flow_value_abs": abs(flow_value),
                "flow_type": "internal",
                "is_estimated": True,
            }
        )

    for edge in flow_edges:
        if edge.get("flow_type") == "internal":
            continue
        edge_results.append(
            {
                "source": edge.get("source"),
                "target": edge.get("target"),
                "value": 1.0,
                "flow_value": _to_float(edge.get("flow_value")),
                "flow_value_abs": _to_float(edge.get("flow_value_abs")),
                "flow_type": edge.get("flow_type"),
                "is_estimated": bool(edge.get("is_estimated", True)),
            }
        )

    # 仅保留唯一 warning（按出现顺序）
    dedup_warnings: List[str] = []
    seen_warnings = set()
    for w in warnings:
        ww = str(w or "").strip()
        if not ww or ww in seen_warnings:
            continue
        seen_warnings.add(ww)
        dedup_warnings.append(ww)

    return {
        "meta": {
            "sector": sector_name,
            "display_name": chain_cfg.get("display_name") or sector_name,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "screener_trade_date": screener_trade_date or "",
            "fund_trade_date": fund_trade_date or "",
            "fund_history_dates": fund_history_dates,
            "profile_updated_at": profile_updated_at or "",
            "limit_per_stage": int(limit_per_stage),
            "flow_mode": flow_mode,
            "flow_window": flow_window,
            "flow_semantics": "估算承接流量（非逐笔交易事实）",
            "warnings": dedup_warnings,
        },
        "stages": stage_results,
        "edges": edge_results,
    }
