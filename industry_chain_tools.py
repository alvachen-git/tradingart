from __future__ import annotations

import json
import math
import os
import re
import time
import copy
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
_DYNAMIC_CACHE_TTL_SEC = int(os.getenv("AI_CHAIN_DYNAMIC_CACHE_TTL_SEC", "21600"))
_DEFAULT_MAX_INDEX_CODES = 6
_DEFAULT_MIN_COMPANIES_BEFORE_FALLBACK = 8
_DEFAULT_COMPANY_KEEP_MAX_STAGES = 2
_DEFAULT_STAGE_RELEVANCE_THRESHOLD = 1
_STRONG_STAGE_FILTER_SECTORS = {"AI服务器", "AI算力"}
_SNAPSHOT_CACHE_TABLE = "industry_chain_snapshot_cache"

# 主题指数动态筛选规则（关键词）+ 白名单兜底（同花顺指数代码）
# 字段：
# include_keywords/exclude_keywords -> 指数名匹配规则
# company_include_keywords/company_exclude_keywords -> 公司文本二次分类规则
# max_index_codes -> 动态命中指数上限
# min_companies_before_fallback -> 动态候选公司数不足时启用白名单补足
# company_keep_max_stages -> 单公司跨环节最多保留数
AI_CHAIN_DYNAMIC_RULES: Dict[str, Dict[str, Dict[str, Any]]] = {
    "AI服务器": {
        "up_chip_storage": {
            "include_keywords": ["算力芯片", "AI芯片", "GPU", "CPU", "存储", "HBM", "DRAM", "NAND", "服务器芯片"],
            "exclude_keywords": ["PCB", "连接器", "CPO", "铜缆", "光模块", "交换机"],
            "company_include_keywords": ["GPU", "CPU", "AI芯片", "HBM", "DRAM", "NAND", "存储", "晶圆代工"],
            "company_exclude_keywords": ["PCB", "覆铜板", "连接器", "铜缆", "CPO", "光模块"],
            "whitelist_codes": ["884287.TI", "884288.TI"],
            "max_index_codes": 6,
            "min_companies_before_fallback": 10,
            "company_keep_max_stages": 2,
        },
        "up_pcb_connect": {
            "include_keywords": ["PCB", "覆铜板", "CCL", "连接器", "高速连接", "高速铜缆", "光模块", "硅光", "CPO"],
            "exclude_keywords": ["晶圆代工", "芯片设计", "GPU", "CPU", "NPU", "存储芯片", "存储器"],
            "company_include_keywords": ["PCB", "覆铜板", "连接器", "铜缆", "光模块", "硅光", "CPO"],
            "company_exclude_keywords": ["晶圆代工", "芯片设计", "GPU", "CPU", "NPU", "存储芯片"],
            "whitelist_codes": ["881121.TI"],
            "max_index_codes": 5,
            "min_companies_before_fallback": 8,
            "company_keep_max_stages": 2,
        },
        "mid_components": {
            "include_keywords": ["服务器电源", "机箱", "散热", "热管理", "风扇", "电源设备"],
            "exclude_keywords": ["GPU", "CPU", "晶圆代工"],
            "company_include_keywords": ["服务器电源", "机箱", "散热", "热管理", "风扇", "PDU"],
            "company_exclude_keywords": ["GPU", "CPU", "晶圆代工", "芯片设计"],
            "whitelist_codes": ["884229.TI"],
            "max_index_codes": 5,
            "min_companies_before_fallback": 8,
            "company_keep_max_stages": 2,
        },
        "mid_odm_oem": {
            "include_keywords": ["服务器", "ODM", "OEM", "整机", "算力服务器", "AI服务器"],
            "exclude_keywords": ["PCB", "覆铜板", "连接器", "晶圆代工"],
            "company_include_keywords": ["服务器", "整机", "ODM", "OEM", "机架", "集群"],
            "company_exclude_keywords": ["PCB", "连接器", "晶圆代工", "芯片设计"],
            "whitelist_codes": ["881121.TI"],
            "max_index_codes": 6,
            "min_companies_before_fallback": 8,
            "company_keep_max_stages": 2,
        },
        "mid_liquid_cooling": {
            "include_keywords": ["液冷", "温控", "冷板", "浸没式", "制冷", "热管理"],
            "exclude_keywords": ["GPU", "CPU", "存储芯片"],
            "company_include_keywords": ["液冷", "温控", "冷板", "浸没式", "制冷", "换热"],
            "company_exclude_keywords": ["GPU", "CPU", "存储芯片", "晶圆代工"],
            "whitelist_codes": ["884229.TI"],
            "max_index_codes": 5,
            "min_companies_before_fallback": 8,
            "company_keep_max_stages": 2,
        },
        "down_deploy_app": {
            "include_keywords": ["云计算", "运营商", "政企", "IDC", "数据中心", "算力租赁"],
            "exclude_keywords": ["PCB", "连接器", "晶圆代工", "封装测试"],
            "company_include_keywords": ["云计算", "政企", "运营商", "IDC", "数据中心", "算力租赁", "云服务"],
            "company_exclude_keywords": ["PCB", "连接器", "晶圆代工", "封装测试"],
            "whitelist_codes": ["881121.TI"],
            "max_index_codes": 6,
            "min_companies_before_fallback": 8,
            "company_keep_max_stages": 2,
        },
    },
    "AI算力": {
        "up_compute_chip": {
            "include_keywords": ["算力芯片", "GPU", "AI芯片", "ASIC", "NPU", "CPU"],
            "exclude_keywords": ["PCB", "连接器", "CPO", "光模块"],
            "company_include_keywords": ["GPU", "CPU", "AI芯片", "ASIC", "NPU", "加速卡"],
            "company_exclude_keywords": ["PCB", "连接器", "CPO", "光模块"],
            "whitelist_codes": ["884287.TI", "884288.TI"],
            "max_index_codes": 6,
            "min_companies_before_fallback": 8,
            "company_keep_max_stages": 2,
        },
        "up_packaging_hbm": {
            "include_keywords": ["先进封装", "HBM", "CoWoS", "封装测试", "封测"],
            "exclude_keywords": ["PCB", "连接器", "IDC"],
            "company_include_keywords": ["先进封装", "HBM", "封装测试", "封测", "CoWoS"],
            "company_exclude_keywords": ["PCB", "连接器", "IDC"],
            "whitelist_codes": ["884228.TI"],
            "max_index_codes": 6,
            "min_companies_before_fallback": 8,
            "company_keep_max_stages": 2,
        },
        "mid_server_cluster": {
            "include_keywords": ["AI服务器", "算力服务器", "智算", "集群", "服务器"],
            "exclude_keywords": ["PCB", "连接器", "晶圆代工"],
            "company_include_keywords": ["AI服务器", "算力服务器", "智算", "集群", "服务器"],
            "company_exclude_keywords": ["PCB", "连接器", "晶圆代工"],
            "whitelist_codes": ["881121.TI"],
            "max_index_codes": 6,
            "min_companies_before_fallback": 8,
            "company_keep_max_stages": 2,
        },
        "mid_interconnect": {
            "include_keywords": ["高速互联", "交换机", "光模块", "CPO", "光通信"],
            "exclude_keywords": ["晶圆代工", "CPU", "GPU"],
            "company_include_keywords": ["交换机", "光模块", "CPO", "光通信", "高速互联"],
            "company_exclude_keywords": ["晶圆代工", "CPU", "GPU"],
            "whitelist_codes": ["881121.TI"],
            "max_index_codes": 6,
            "min_companies_before_fallback": 8,
            "company_keep_max_stages": 2,
        },
        "mid_dc_infra": {
            "include_keywords": ["数据中心", "UPS", "配电", "制冷", "机柜", "温控"],
            "exclude_keywords": ["芯片设计", "GPU", "CPU"],
            "company_include_keywords": ["数据中心", "UPS", "配电", "制冷", "机柜", "温控"],
            "company_exclude_keywords": ["芯片设计", "GPU", "CPU"],
            "whitelist_codes": ["881121.TI"],
            "max_index_codes": 6,
            "min_companies_before_fallback": 8,
            "company_keep_max_stages": 2,
        },
        "down_model_service": {
            "include_keywords": ["大模型", "模型服务", "AI应用", "AIGC", "云服务"],
            "exclude_keywords": ["PCB", "连接器", "封装测试"],
            "company_include_keywords": ["大模型", "模型服务", "AI应用", "AIGC", "云服务"],
            "company_exclude_keywords": ["PCB", "连接器", "封装测试"],
            "whitelist_codes": ["881121.TI"],
            "max_index_codes": 6,
            "min_companies_before_fallback": 8,
            "company_keep_max_stages": 2,
        },
    },
}

_THS_CATALOG_CACHE: Dict[str, Any] = {"expires_at": 0.0, "df": pd.DataFrame()}
_THS_KEYWORD_CACHE: Dict[str, Dict[str, Any]] = {}
_STAGE_MEMBER_CACHE: Dict[str, Dict[str, Any]] = {}


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


def _normalize_trade_date(value: Any) -> str:
    s = str(value or "").strip().replace("-", "")
    return s if re.fullmatch(r"\d{8}", s) else ""


def ensure_industry_chain_snapshot_cache_table(engine=None) -> bool:
    engine = engine if engine is not None else get_db_engine()
    if engine is None:
        return False

    ddl = text(
        f"""
        CREATE TABLE IF NOT EXISTS {_SNAPSHOT_CACHE_TABLE} (
            trade_date VARCHAR(8) NOT NULL,
            sector_name VARCHAR(64) NOT NULL,
            flow_window VARCHAR(8) NOT NULL DEFAULT '5D',
            snapshot_json LONGTEXT NOT NULL,
            fund_trade_date VARCHAR(8) DEFAULT '',
            screener_trade_date VARCHAR(8) DEFAULT '',
            generated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, sector_name, flow_window)
        )
        """
    )
    idx1 = text(
        f"""
        CREATE INDEX idx_sector_generated
        ON {_SNAPSHOT_CACHE_TABLE} (sector_name, generated_at)
        """
    )
    idx2 = text(
        f"""
        CREATE INDEX idx_trade_date_sector
        ON {_SNAPSHOT_CACHE_TABLE} (trade_date, sector_name)
        """
    )

    try:
        with engine.begin() as conn:
            conn.execute(ddl)
            try:
                conn.execute(idx1)
            except Exception:
                pass
            try:
                conn.execute(idx2)
            except Exception:
                pass
        return True
    except Exception:
        return False


def save_chain_snapshot_cache(
    trade_date: str,
    sector_name: str,
    flow_window: str,
    snapshot: Dict[str, Any],
    engine=None,
) -> bool:
    engine = engine if engine is not None else get_db_engine()
    if engine is None:
        return False
    if not ensure_industry_chain_snapshot_cache_table(engine):
        return False

    flow_window = _normalize_flow_window(flow_window)
    trade_date = _normalize_trade_date(trade_date)
    if not trade_date:
        meta = snapshot.get("meta") or {}
        trade_date = (
            _normalize_trade_date(meta.get("fund_trade_date"))
            or _normalize_trade_date(meta.get("screener_trade_date"))
            or datetime.now().strftime("%Y%m%d")
        )

    payload = json.dumps(snapshot or {}, ensure_ascii=False)
    meta = snapshot.get("meta") or {}
    params = {
        "trade_date": trade_date,
        "sector_name": str(sector_name or "").strip(),
        "flow_window": flow_window,
        "snapshot_json": payload,
        "fund_trade_date": _normalize_trade_date(meta.get("fund_trade_date")),
        "screener_trade_date": _normalize_trade_date(meta.get("screener_trade_date")),
    }

    dialect = str(getattr(engine.dialect, "name", "")).lower()
    mysql_sql = text(
        f"""
        INSERT INTO {_SNAPSHOT_CACHE_TABLE}
        (trade_date, sector_name, flow_window, snapshot_json, fund_trade_date, screener_trade_date, generated_at)
        VALUES
        (:trade_date, :sector_name, :flow_window, :snapshot_json, :fund_trade_date, :screener_trade_date, CURRENT_TIMESTAMP)
        ON DUPLICATE KEY UPDATE
            snapshot_json=VALUES(snapshot_json),
            fund_trade_date=VALUES(fund_trade_date),
            screener_trade_date=VALUES(screener_trade_date),
            generated_at=CURRENT_TIMESTAMP
        """
    )
    sqlite_sql = text(
        f"""
        INSERT INTO {_SNAPSHOT_CACHE_TABLE}
        (trade_date, sector_name, flow_window, snapshot_json, fund_trade_date, screener_trade_date, generated_at)
        VALUES
        (:trade_date, :sector_name, :flow_window, :snapshot_json, :fund_trade_date, :screener_trade_date, CURRENT_TIMESTAMP)
        ON CONFLICT(trade_date, sector_name, flow_window) DO UPDATE SET
            snapshot_json=excluded.snapshot_json,
            fund_trade_date=excluded.fund_trade_date,
            screener_trade_date=excluded.screener_trade_date,
            generated_at=CURRENT_TIMESTAMP
        """
    )

    try:
        with engine.begin() as conn:
            conn.execute(sqlite_sql if dialect == "sqlite" else mysql_sql, params)
        return True
    except Exception:
        return False


def load_chain_snapshot_cache(
    sector_name: str,
    flow_window: str = "5D",
    trade_date: Optional[str] = None,
    engine=None,
) -> Optional[Dict[str, Any]]:
    engine = engine if engine is not None else get_db_engine()
    if engine is None:
        return None
    if not ensure_industry_chain_snapshot_cache_table(engine):
        return None

    flow_window = _normalize_flow_window(flow_window)
    trade_date = _normalize_trade_date(trade_date)
    base_params = {
        "sector_name": str(sector_name or "").strip(),
        "flow_window": flow_window,
    }

    if trade_date:
        sql = text(
            f"""
            SELECT snapshot_json
            FROM {_SNAPSHOT_CACHE_TABLE}
            WHERE sector_name=:sector_name
              AND flow_window=:flow_window
              AND trade_date=:trade_date
            LIMIT 1
            """
        )
        base_params["trade_date"] = trade_date
    else:
        sql = text(
            f"""
            SELECT snapshot_json
            FROM {_SNAPSHOT_CACHE_TABLE}
            WHERE sector_name=:sector_name
              AND flow_window=:flow_window
            ORDER BY trade_date DESC, generated_at DESC
            LIMIT 1
            """
        )

    try:
        with engine.connect() as conn:
            payload = conn.execute(sql, base_params).scalar()
    except Exception:
        return None

    if not payload:
        return None
    try:
        parsed = json.loads(payload)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def load_chain_templates(path: str = _TEMPLATE_PATH) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _normalize_text_for_match(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "").lower())


def _normalize_index_code(value: Any) -> str:
    code = str(value or "").strip().upper()
    if not code:
        return ""
    if "." not in code and code.isdigit():
        return f"{code}.TI"
    return code


def _query_ths_index_catalog(pro) -> pd.DataFrame:
    now_ts = time.time()
    cached_df = _THS_CATALOG_CACHE.get("df")
    if isinstance(cached_df, pd.DataFrame) and now_ts < float(_THS_CATALOG_CACHE.get("expires_at", 0)):
        return cached_df.copy()

    if pro is None:
        return pd.DataFrame()

    fetch_kwargs = [
        {"exchange": "A", "type": "N", "fields": "ts_code,name"},
        {"exchange": "", "type": "N", "fields": "ts_code,name"},
        {"type": "N", "fields": "ts_code,name"},
        {"fields": "ts_code,name"},
        {"exchange": "A", "type": "N"},
        {"exchange": "", "type": "N"},
        {"type": "N"},
        {},
    ]

    for kwargs in fetch_kwargs:
        try:
            df = pro.ths_index(**kwargs)
        except Exception:
            continue
        if df is None or df.empty:
            continue
        out = df.copy()
        if "ts_code" not in out.columns or "name" not in out.columns:
            continue
        out["ts_code"] = out["ts_code"].map(_normalize_index_code)
        out = out[out["ts_code"] != ""]
        if out.empty:
            continue
        _THS_CATALOG_CACHE["df"] = out[["ts_code", "name"]].copy()
        _THS_CATALOG_CACHE["expires_at"] = now_ts + _DYNAMIC_CACHE_TTL_SEC
        return _THS_CATALOG_CACHE["df"].copy()

    return pd.DataFrame()


def _as_keyword_list(raw: Any) -> List[str]:
    out: List[str] = []
    for item in (raw or []):
        s = str(item or "").strip()
        if s:
            out.append(s)
    return out


def _get_stage_dynamic_rule(sector_name: str, stage_id: str) -> Dict[str, Any]:
    return dict((AI_CHAIN_DYNAMIC_RULES.get(sector_name) or {}).get(stage_id) or {})


def _match_index_codes_by_keywords(
    catalog: pd.DataFrame,
    include_keywords: List[str],
    exclude_keywords: Optional[List[str]] = None,
    max_codes: int = 0,
) -> List[str]:
    if catalog is None or catalog.empty:
        return []
    include_norm = [_normalize_text_for_match(x) for x in (include_keywords or []) if str(x or "").strip()]
    exclude_norm = [_normalize_text_for_match(x) for x in (exclude_keywords or []) if str(x or "").strip()]
    if not include_norm:
        return []

    matched_codes: List[str] = []
    for _, row in catalog.iterrows():
        code = _normalize_index_code(row.get("ts_code"))
        if not code:
            continue
        hay = _normalize_text_for_match(row.get("name"))
        if not hay:
            continue
        include_hit = any(kw in hay for kw in include_norm)
        exclude_hit = any(kw in hay for kw in exclude_norm) if exclude_norm else False
        if include_hit and not exclude_hit:
            matched_codes.append(code)

    unique_codes = sorted(set(matched_codes))
    if int(max_codes or 0) > 0:
        return unique_codes[: int(max_codes)]
    return unique_codes


def _resolve_stage_index_codes_with_meta(
    stage: Dict[str, Any],
    sector_name: str,
    pro,
) -> Tuple[List[str], Dict[str, Any]]:
    fixed_codes = [_normalize_index_code(x) for x in (stage.get("ths_index_codes") or []) if _normalize_index_code(x)]
    stage_id = str(stage.get("id") or "")
    info: Dict[str, Any] = {
        "source_mode": "fixed" if fixed_codes else "empty",
        "keywords_hit": 0,
        "index_codes": 0,
        "whitelist_codes": 0,
        "final_index_codes": len(fixed_codes),
    }
    info.update(
        {
            "index_hit_count": len(fixed_codes),
            "candidate_company_count": 0,
            "filtered_company_count": 0,
            "fallback_company_count": 0,
        }
    )
    if fixed_codes:
        return sorted(set(fixed_codes)), info

    rule = _get_stage_dynamic_rule(sector_name=sector_name, stage_id=stage_id)
    include_keywords = _as_keyword_list(rule.get("include_keywords") or rule.get("keywords"))
    exclude_keywords = _as_keyword_list(rule.get("exclude_keywords"))
    whitelist_codes = sorted(
        set(_normalize_index_code(x) for x in (rule.get("whitelist_codes") or []) if _normalize_index_code(x))
    )
    max_index_codes = int(rule.get("max_index_codes") or _DEFAULT_MAX_INDEX_CODES)
    min_companies_before_fallback = int(
        rule.get("min_companies_before_fallback") or _DEFAULT_MIN_COMPANIES_BEFORE_FALLBACK
    )
    keep_max_stages = int(rule.get("company_keep_max_stages") or _DEFAULT_COMPANY_KEEP_MAX_STAGES)

    dynamic_codes: List[str] = []
    if include_keywords:
        cache_key = (
            f"{sector_name}|{stage_id}|"
            f"{'|'.join(sorted(set(include_keywords)))}|"
            f"{'|'.join(sorted(set(exclude_keywords)))}|{max_index_codes}"
        )
        cached = _THS_KEYWORD_CACHE.get(cache_key) or {}
        now_ts = time.time()
        if now_ts < float(cached.get("expires_at", 0)):
            dynamic_codes = list(cached.get("codes") or [])
        else:
            catalog = _query_ths_index_catalog(pro)
            dynamic_codes = _match_index_codes_by_keywords(
                catalog=catalog,
                include_keywords=include_keywords,
                exclude_keywords=exclude_keywords,
                max_codes=max_index_codes,
            )
            _THS_KEYWORD_CACHE[cache_key] = {
                "codes": dynamic_codes,
                "expires_at": now_ts + _DYNAMIC_CACHE_TTL_SEC,
            }

    merged_codes = sorted(set(dynamic_codes + whitelist_codes))
    info["keywords_hit"] = len(include_keywords)
    info["index_codes"] = len(dynamic_codes)
    info["index_hit_count"] = len(dynamic_codes)
    info["whitelist_codes"] = len(whitelist_codes)
    info["final_index_codes"] = len(merged_codes)
    info["dynamic_index_codes"] = dynamic_codes
    info["whitelist_index_codes"] = whitelist_codes
    info["min_companies_before_fallback"] = max(1, min_companies_before_fallback)
    info["company_keep_max_stages"] = max(1, keep_max_stages)
    if dynamic_codes and whitelist_codes:
        info["source_mode"] = "mixed"
    elif dynamic_codes:
        info["source_mode"] = "dynamic"
    elif whitelist_codes:
        info["source_mode"] = "whitelist"
    else:
        info["source_mode"] = "empty"
    return merged_codes, info


def _resolve_stage_index_codes(stage: Dict[str, Any], sector_name: str, pro) -> List[str]:
    codes, _ = _resolve_stage_index_codes_with_meta(stage=stage, sector_name=sector_name, pro=pro)
    return codes


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


def _parse_pipe_items(raw: Any, max_items: int = 3) -> List[str]:
    s = str(raw or "").strip()
    if not s:
        return []
    out: List[str] = []
    seen = set()
    for x in s.split("|"):
        item = str(x or "").strip()
        if not item:
            continue
        if item not in seen:
            out.append(item)
            seen.add(item)
        if len(out) >= max(1, int(max_items)):
            break
    return out


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
            try:
                ts_max = conn.execute(
                    text(
                        """
                        SELECT MAX(COALESCE(insight_updated_at, tags_updated_at))
                        FROM stock_company_profile_cache
                        """
                    )
                ).scalar()
            except Exception:
                ts_max = conn.execute(
                    text("SELECT MAX(tags_updated_at) FROM stock_company_profile_cache")
                ).scalar()
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
            "domain_insight_text": "",
            "insight_updated_at": "",
            "tech_highlights": [],
            "customer_profile": "",
            "moat_note": "",
            "boundary_risk": "",
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
    sector_name: str = "",
    collect_meta: bool = False,
) -> Tuple[Any, ...]:
    members: Dict[str, List[Dict[str, str]]] = {}
    warnings: List[str] = []
    dynamic_match_info: Dict[str, Dict[str, Any]] = {}
    stage_source_modes: List[str] = []

    if pro is None:
        warnings.append("Tushare 不可用，无法拉取产业链成分股。")
        for stage in stages:
            members[stage["id"]] = []
            dynamic_match_info[stage["id"]] = {
                "keywords_hit": 0,
                "index_codes": 0,
                "index_hit_count": 0,
                "whitelist_codes": 0,
                "final_index_codes": 0,
                "candidate_company_count": 0,
                "filtered_company_count": 0,
                "fallback_company_count": 0,
                "source_mode": "empty",
            }
        if collect_meta:
            return members, warnings, dynamic_match_info, "fixed"
        return members, warnings

    for stage in stages:
        stage_id = stage["id"]
        code_set: Dict[str, str] = {}
        code_source_map: Dict[str, str] = {}
        idx_codes, stage_meta = _resolve_stage_index_codes_with_meta(
            stage=stage,
            sector_name=sector_name,
            pro=pro,
        )
        dynamic_match_info[stage_id] = stage_meta
        has_fixed_codes = bool(stage.get("ths_index_codes"))
        has_dynamic_rule = bool(_get_stage_dynamic_rule(sector_name, stage_id))

        def _ingest_index_members(index_codes: List[str], source_label: str) -> None:
            for idx_code in index_codes:
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
                    if not ts_code:
                        continue
                    if ts_code not in code_set:
                        code_set[ts_code] = name
                        code_source_map[ts_code] = source_label
                    elif code_source_map.get(ts_code) != source_label:
                        code_source_map[ts_code] = "mixed"

        if has_fixed_codes:
            _ingest_index_members(idx_codes, "fixed")
            stage_meta["source_mode"] = "fixed"
        else:
            dynamic_codes = list(stage_meta.get("dynamic_index_codes") or [])
            whitelist_codes = list(stage_meta.get("whitelist_index_codes") or [])
            min_companies = int(stage_meta.get("min_companies_before_fallback") or _DEFAULT_MIN_COMPANIES_BEFORE_FALLBACK)

            if dynamic_codes:
                _ingest_index_members(dynamic_codes, "dynamic")
            else:
                if has_dynamic_rule:
                    warnings.append(f"阶段 {stage_id} 动态筛选无命中。")
                elif not idx_codes:
                    warnings.append(f"阶段 {stage_id} 未匹配到主题指数，返回空结果。")

            candidate_count = len(code_set)
            stage_meta["candidate_company_count"] = candidate_count

            fallback_added = 0
            fallback_used = False
            if whitelist_codes and candidate_count < max(1, min_companies):
                before_fallback = len(code_set)
                _ingest_index_members(whitelist_codes, "whitelist")
                fallback_added = max(0, len(code_set) - before_fallback)
                fallback_used = fallback_added > 0
                if fallback_used:
                    warnings.append(
                        f"阶段 {stage_id} 动态候选 {candidate_count} 家，白名单补足 {fallback_added} 家。"
                    )
                elif candidate_count == 0:
                    warnings.append(f"阶段 {stage_id} 动态筛选无命中且白名单未补足，返回空结果。")

            stage_meta["fallback_company_count"] = fallback_added
            if candidate_count == 0 and not fallback_used and whitelist_codes and has_dynamic_rule:
                stage_meta["source_mode"] = "whitelist"
            elif candidate_count > 0 and fallback_used:
                stage_meta["source_mode"] = "mixed"
            elif candidate_count > 0:
                stage_meta["source_mode"] = "dynamic"
            elif whitelist_codes:
                stage_meta["source_mode"] = "whitelist"
            else:
                stage_meta["source_mode"] = "empty"

        stage_meta["final_index_codes"] = len(
            set((stage_meta.get("dynamic_index_codes") or []) + (stage_meta.get("whitelist_index_codes") or []))
        )
        stage_meta["candidate_company_count"] = int(stage_meta.get("candidate_company_count") or len(code_set))
        stage_source_modes.append(str(stage_meta.get("source_mode") or "empty"))

        members[stage_id] = [
            {"ts_code": code, "name": name, "match_source": code_source_map.get(code, "unknown")}
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

    member_source_mode = "fixed"
    source_set = set(stage_source_modes)
    if any(x in source_set for x in {"dynamic", "whitelist", "mixed"}):
        member_source_mode = "mixed"
    elif source_set and source_set == {"fixed"}:
        member_source_mode = "fixed"

    if collect_meta:
        return members, warnings, dynamic_match_info, member_source_mode
    return members, warnings


def _get_stage_members_cached(
    stages: List[Dict[str, Any]],
    pro,
    sector_name: str,
    screener_trade_date: str,
) -> Tuple[Dict[str, List[Dict[str, str]]], List[str], Dict[str, Dict[str, Any]], str]:
    """
    成分股二级缓存（按板块+交易日）：
    - 缓存成员映射、告警、动态匹配信息、来源模式
    - TTL 复用 _DYNAMIC_CACHE_TTL_SEC
    """
    cache_key = f"{sector_name}|{str(screener_trade_date or '').strip()}"
    now_ts = time.time()
    cached = _STAGE_MEMBER_CACHE.get(cache_key) or {}
    if now_ts < float(cached.get("expires_at", 0)):
        return (
            copy.deepcopy(cached.get("members") or {}),
            list(cached.get("warnings") or []),
            copy.deepcopy(cached.get("dynamic_match_info") or {}),
            str(cached.get("member_source_mode") or "fixed"),
        )

    members, stage_warnings, dynamic_match_info, member_source_mode = fetch_stage_members_from_tushare(
        stages=stages,
        pro=pro,
        sector_name=sector_name,
        collect_meta=True,
    )
    _STAGE_MEMBER_CACHE[cache_key] = {
        "expires_at": now_ts + _DYNAMIC_CACHE_TTL_SEC,
        "members": copy.deepcopy(members),
        "warnings": list(stage_warnings or []),
        "dynamic_match_info": copy.deepcopy(dynamic_match_info or {}),
        "member_source_mode": str(member_source_mode or "fixed"),
    }
    return members, stage_warnings, dynamic_match_info, member_source_mode


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
    try:
        with engine.connect() as conn:
            try:
                df = pd.read_sql(
                    text(
                        f"""
                        SELECT ts_code, company_name, main_business, business_scope, domain_tags, tags_updated_at,
                               domain_insight_text, insight_updated_at, tech_highlights,
                               customer_profile, moat_note, boundary_risk
                        FROM stock_company_profile_cache
                        WHERE ts_code IN ({placeholders})
                        """
                    ),
                    conn,
                    params=params,
                )
            except Exception:
                df = pd.read_sql(
                    text(
                        f"""
                        SELECT ts_code, company_name, main_business, business_scope, domain_tags, tags_updated_at
                        FROM stock_company_profile_cache
                        WHERE ts_code IN ({placeholders})
                        """
                    ),
                    conn,
                    params=params,
                )
    except Exception:
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    for _, row in df.iterrows():
        code = _normalize_ts_code(row.get("ts_code"))
        tags = parse_domain_tags(row.get("domain_tags"))
        tech_items = _parse_pipe_items(row.get("tech_highlights"), max_items=2)
        out[code] = {
            "company_name": str(row.get("company_name") or "").strip(),
            "main_business": str(row.get("main_business") or "").strip(),
            "business_scope": str(row.get("business_scope") or "").strip(),
            "domain_tags": tags,
            "domain_tags_text": " / ".join(tags),
            "tags_updated_at": str(row.get("tags_updated_at") or "").strip(),
            "domain_insight_text": str(row.get("domain_insight_text") or "").strip(),
            "insight_updated_at": str(row.get("insight_updated_at") or "").strip(),
            "tech_highlights": tech_items,
            "customer_profile": str(row.get("customer_profile") or "").strip(),
            "moat_note": str(row.get("moat_note") or "").strip(),
            "boundary_risk": str(row.get("boundary_risk") or "").strip(),
        }
    return out


def _get_company_match_text(company: Dict[str, Any]) -> str:
    fields = [
        "domain_tags_text",
        "domain_insight_text",
        "main_business",
        "business_scope",
        "industry",
        "name",
    ]
    parts = [str(company.get(k) or "") for k in fields]
    return _normalize_text_for_match(" ".join(parts))


def _score_stage_relevance(company: Dict[str, Any], stage_rule: Dict[str, Any]) -> int:
    if not stage_rule:
        return 0
    text = _get_company_match_text(company)
    include_keywords = _as_keyword_list(stage_rule.get("company_include_keywords") or stage_rule.get("include_keywords"))
    exclude_keywords = _as_keyword_list(stage_rule.get("company_exclude_keywords") or stage_rule.get("exclude_keywords"))
    if not text:
        return 0

    score = 0
    include_hit = 0
    for kw in include_keywords:
        if _normalize_text_for_match(kw) in text:
            include_hit += 1
            score += 2

    for kw in exclude_keywords:
        if _normalize_text_for_match(kw) in text:
            score -= 3

    if include_keywords and include_hit == 0:
        score -= 1
    return score


def _apply_company_stage_cap(
    stage_company_map: Dict[str, List[Dict[str, Any]]],
    keep_max: int,
) -> Dict[str, int]:
    keep_max = max(1, int(keep_max))
    code_entries: Dict[str, List[Tuple[str, Dict[str, Any]]]] = {}
    for stage_id, rows in stage_company_map.items():
        for row in rows:
            code = _normalize_ts_code(row.get("ts_code"))
            if not code:
                continue
            code_entries.setdefault(code, []).append((stage_id, row))

    keep_assignment: Dict[str, set] = {}
    for code, entries in code_entries.items():
        ranked = sorted(
            entries,
            key=lambda x: (
                -int(_to_float(x[1].get("stage_relevance_score", 0))),
                -_to_float(x[1].get("market_cap", 0.0)),
                -_to_float(x[1].get("main_net_amount_5d", 0.0)),
                -_to_float(x[1].get("main_net_amount_1d", 0.0)),
                str(x[0]),
            ),
        )
        keep_assignment[code] = {sid for sid, _ in ranked[:keep_max]}

    removed_count_by_stage: Dict[str, int] = {k: 0 for k in stage_company_map.keys()}
    for sid, rows in list(stage_company_map.items()):
        kept_rows: List[Dict[str, Any]] = []
        for row in rows:
            code = _normalize_ts_code(row.get("ts_code"))
            allowed_stages = keep_assignment.get(code, set())
            if allowed_stages and sid not in allowed_stages:
                removed_count_by_stage[sid] = removed_count_by_stage.get(sid, 0) + 1
                continue
            kept_rows.append(row)
        stage_company_map[sid] = kept_rows
    return removed_count_by_stage


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
    member_source_mode = "fixed"
    dynamic_match_info: Dict[str, Dict[str, Any]] = {}

    fund_trade_date = _fetch_max_trade_date(engine, "stock_moneyflow_daily")
    if force_screener_trade_date:
        screener_trade_date = str(force_screener_trade_date).strip().replace("-", "")
    else:
        screener_trade_date = _fetch_screener_trade_date(engine, prefer_le_trade_date=fund_trade_date)
        if not screener_trade_date:
            screener_trade_date = _fetch_max_trade_date(engine, "daily_stock_screener")
    profile_updated_at = _fetch_profile_max_updated_at(engine)

    if stage_member_map is None:
        (
            stage_member_map,
            stage_warnings,
            dynamic_match_info,
            member_source_mode,
        ) = _get_stage_members_cached(
            stages=stages,
            pro=pro,
            sector_name=sector_name,
            screener_trade_date=screener_trade_date or "",
        )
        warnings.extend(stage_warnings)
    else:
        dynamic_match_info = {
            str(stage.get("id") or ""): {
                "keywords_hit": 0,
                "index_codes": 0,
                "index_hit_count": 0,
                "whitelist_codes": 0,
                "final_index_codes": len(stage_member_map.get(str(stage.get("id") or ""), [])),
                "candidate_company_count": len(stage_member_map.get(str(stage.get("id") or ""), [])),
                "filtered_company_count": 0,
                "fallback_company_count": 0,
                "source_mode": "fixed",
            }
            for stage in stages
        }

    all_codes: List[str] = []
    for stage in stages:
        for item in stage_member_map.get(stage["id"], []):
            code = _normalize_ts_code(item.get("ts_code"))
            if code:
                all_codes.append(code)
    all_codes = sorted(set(all_codes))

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
    stage_company_map: Dict[str, List[Dict[str, Any]]] = {}
    stage_name_map: Dict[str, str] = {}

    for stage in stages:
        stage_id = str(stage["id"])
        stage_name = str(stage["name"])
        stage_name_map[stage_id] = stage_name
        stage_members = stage_member_map.get(stage_id, [])
        member_source_map = {
            _normalize_ts_code(x.get("ts_code")): str(x.get("match_source") or "unknown")
            for x in stage_members
        }

        stage_meta = dynamic_match_info.setdefault(stage_id, {})
        stage_meta.setdefault("candidate_company_count", len(stage_members))
        stage_meta.setdefault("filtered_company_count", 0)
        stage_meta.setdefault("fallback_company_count", 0)
        stage_meta.setdefault("index_hit_count", int(stage_meta.get("index_codes") or 0))

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
                "main_net_amount_5d_hist": fund_5d_history_map.get(code, {}),
                "fund_signal": calc_fund_signal(main_1d, main_5d),
                "domain_tags": profile_item.get("domain_tags", []),
                "domain_tags_text": profile_item.get("domain_tags_text", ""),
                "domain_insight_text": profile_item.get("domain_insight_text", ""),
                "main_business": profile_item.get("main_business", ""),
                "business_scope": profile_item.get("business_scope", ""),
                "tags_updated_at": profile_item.get("tags_updated_at", ""),
                "insight_updated_at": profile_item.get("insight_updated_at", ""),
                "tech_highlights": profile_item.get("tech_highlights", []),
                "customer_profile": profile_item.get("customer_profile", ""),
                "moat_note": profile_item.get("moat_note", ""),
                "boundary_risk": profile_item.get("boundary_risk", ""),
                "stage_relevance_score": 0,
                "stage_match_source": member_source_map.get(code, "unknown"),
            }
            companies.append(company)

        if sector_name in _STRONG_STAGE_FILTER_SECTORS:
            rule = _get_stage_dynamic_rule(sector_name=sector_name, stage_id=stage_id)
            threshold = int(rule.get("stage_relevance_threshold") or _DEFAULT_STAGE_RELEVANCE_THRESHOLD)
            before_count = len(companies)
            filtered_rows: List[Dict[str, Any]] = []
            for c in companies:
                score = _score_stage_relevance(c, rule)
                c["stage_relevance_score"] = int(score)
                if score >= threshold:
                    filtered_rows.append(c)
            removed = max(0, before_count - len(filtered_rows))
            if removed > 0:
                stage_meta["filtered_company_count"] = int(stage_meta.get("filtered_company_count") or 0) + removed
            companies = filtered_rows

        stage_company_map[stage_id] = companies

    if sector_name in _STRONG_STAGE_FILTER_SECTORS:
        stage_rules = AI_CHAIN_DYNAMIC_RULES.get(sector_name) or {}
        keep_max_values = [
            int((stage_rules.get(str(stage.get("id") or "")) or {}).get("company_keep_max_stages") or 0)
            for stage in stages
        ]
        keep_max = max([x for x in keep_max_values if x > 0] or [_DEFAULT_COMPANY_KEEP_MAX_STAGES])
        removed_by_stage = _apply_company_stage_cap(stage_company_map=stage_company_map, keep_max=keep_max)
        for sid, removed in removed_by_stage.items():
            if removed <= 0:
                continue
            stage_meta = dynamic_match_info.setdefault(sid, {})
            stage_meta["filtered_company_count"] = int(stage_meta.get("filtered_company_count") or 0) + int(removed)

    for stage in stages:
        stage_id = str(stage["id"])
        stage_name = str(stage["name"])
        companies = _sort_stage_companies(stage_company_map.get(stage_id, []), limit_per_stage)

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
                x["domain_insight_text"] = p.get("domain_insight_text", "")
                x["main_business"] = p.get("main_business", "")
                x["business_scope"] = p.get("business_scope", "")
                x["tags_updated_at"] = p.get("tags_updated_at", "")
                x["insight_updated_at"] = p.get("insight_updated_at", "")
                x["tech_highlights"] = p.get("tech_highlights", [])
                x["customer_profile"] = p.get("customer_profile", "")
                x["moat_note"] = p.get("moat_note", "")
                x["boundary_risk"] = p.get("boundary_risk", "")

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
            "member_source_mode": member_source_mode,
            "dynamic_match_info": dynamic_match_info,
            "warnings": dedup_warnings,
        },
        "stages": stage_results,
        "edges": edge_results,
    }


def _rebuild_stage_aggregates_for_limit(snapshot: Dict[str, Any], limit_per_stage: int) -> Dict[str, Any]:
    snap = copy.deepcopy(snapshot or {})
    stages = snap.get("stages") or []
    meta = snap.get("meta") or {}
    hist_dates = [str(x).strip() for x in (meta.get("fund_history_dates") or []) if str(x).strip()]
    limit_n = max(1, int(limit_per_stage))

    for stage in stages:
        companies = list(stage.get("companies") or [])[:limit_n]
        stage["companies"] = companies
        stage["company_count"] = len(companies)
        stage["net_flow_1d"] = sum(_to_float(x.get("main_net_amount_1d")) for x in companies)
        stage["net_flow_5d"] = sum(_to_float(x.get("main_net_amount_5d")) for x in companies)
        if hist_dates:
            hist_rows: List[Dict[str, Any]] = []
            for d in hist_dates:
                d_total = sum(
                    _to_float((x.get("main_net_amount_5d_hist") or {}).get(d, 0.0))
                    for x in companies
                )
                hist_rows.append({"trade_date": d, "net_flow_5d": d_total})
            stage["net_flow_5d_history"] = hist_rows
    structure_edges: List[List[str]] = []
    for edge in (snap.get("edges") or []):
        src = str(edge.get("source") or "")
        dst = str(edge.get("target") or "")
        if not src or not dst:
            continue
        if src in {_EXTERNAL_IN_ID, _EXTERNAL_OUT_ID} or dst in {_EXTERNAL_IN_ID, _EXTERNAL_OUT_ID}:
            continue
        structure_edges.append([src, dst])

    flow_window = _normalize_flow_window(meta.get("flow_window") or "5D")
    flow_edges, flow_in_external_map, flow_out_external_map = _build_flow_edges(
        stage_results=stages,
        structure_edges=structure_edges,
        flow_window=flow_window,
    )

    for stage in stages:
        sid = str(stage.get("id") or "")
        stage["flow_in_external"] = _to_float(flow_in_external_map.get(sid, 0.0))
        stage["flow_out_external"] = _to_float(flow_out_external_map.get(sid, 0.0))

    stage_count_map = {str(s.get("id") or ""): int(s.get("company_count") or 0) for s in stages}
    internal_edge_map: Dict[Tuple[str, str], Dict[str, Any]] = {
        (str(x.get("source")), str(x.get("target"))): x
        for x in flow_edges
        if x.get("flow_type") == "internal"
    }
    edge_results: List[Dict[str, Any]] = []
    for src, dst in structure_edges:
        flow_edge = internal_edge_map.get((src, dst), {})
        flow_value = _to_float(flow_edge.get("flow_value", 0.0))
        value = min(stage_count_map.get(src, 0), stage_count_map.get(dst, 0)) or 1
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
                "flow_value": _to_float(edge.get("flow_value", 0.0)),
                "flow_value_abs": _to_float(edge.get("flow_value_abs", 0.0)),
                "flow_type": edge.get("flow_type"),
                "is_estimated": bool(edge.get("is_estimated", True)),
            }
        )

    snap["edges"] = edge_results
    snap.setdefault("meta", {})
    snap["meta"]["limit_per_stage"] = limit_n
    return snap


def get_chain_snapshot_with_cache(
    sector_name: str,
    limit_per_stage: int = 10,
    flow_window: str = "5D",
    screener_trade_date: Optional[str] = None,
    engine=None,
    pro=None,
    templates: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    engine = engine if engine is not None else get_db_engine()
    flow_window = _normalize_flow_window(flow_window)
    requested_trade_date = _normalize_trade_date(screener_trade_date)

    cached_snapshot = load_chain_snapshot_cache(
        sector_name=sector_name,
        flow_window=flow_window,
        trade_date=requested_trade_date or None,
        engine=engine,
    )
    if cached_snapshot:
        snap = _rebuild_stage_aggregates_for_limit(cached_snapshot, limit_per_stage=limit_per_stage)
        snap.setdefault("meta", {})
        snap["meta"]["snapshot_source"] = "cache"
        snap["meta"]["flow_window"] = flow_window
        snap["meta"]["cache_hit"] = True
        return snap

    force_date = requested_trade_date or None
    snap = get_chain_snapshot(
        sector_name=sector_name,
        limit_per_stage=limit_per_stage,
        engine=engine,
        pro=pro,
        templates=templates,
        force_screener_trade_date=force_date,
        flow_window=flow_window,
    )
    snap.setdefault("meta", {})
    meta_warnings = list(snap["meta"].get("warnings") or [])
    cache_msg = "今日快照未生成，已使用实时计算（可能较慢）。"
    if cache_msg not in meta_warnings:
        meta_warnings.insert(0, cache_msg)
    snap["meta"]["warnings"] = meta_warnings
    snap["meta"]["snapshot_source"] = "realtime"
    snap["meta"]["cache_hit"] = False
    return snap
