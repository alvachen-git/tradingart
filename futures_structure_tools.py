import os
import re
import time
import warnings
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

import akshare as ak
import pandas as pd
from langchain_core.tools import tool

from futures_fund_flow_tools import (
    FUTURES_NAME_MAP,
    parse_contract_input,
    _get_latest_price_for_margin_estimation,
)


_CACHE: Dict[Tuple[str, Tuple[Any, ...]], Dict[str, Any]] = {}
_SNAPSHOT: Dict[Tuple[str, str], Dict[str, Any]] = {}
CACHE_TTL_SECONDS = 300
PROXY_ENV_KEYS = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
)


SHFE_CODES = {
    "AU", "AG", "CU", "AL", "ZN", "PB", "NI", "SN", "RB", "HC", "SS", "RU", "FU", "BU", "SP", "AO", "WR"
}
DCE_CODES = {
    "A", "B", "M", "Y", "P", "C", "CS", "RR", "JD", "LH", "I", "J", "JM", "L", "V", "PP", "EG", "EB", "PG"
}
CZCE_CODES = {
    "SR", "CF", "OI", "RM", "AP", "CJ", "PK", "WH", "TA", "MA", "FG", "SA", "UR", "SF", "SM", "PF"
}
GFEX_CODES = {"SI", "LC", "PS"}
SPOT_STOCK_SECTORS = ("能源", "化工", "塑料", "纺织", "有色", "钢铁", "建材", "农副")


def _safe_int(value: Any, default: int, minimum: int = 1, maximum: int = 3650) -> int:
    try:
        out = int(float(value))
    except Exception:
        out = default
    return max(minimum, min(maximum, out))


def _normalize_date(date_text: str = "", default_today: bool = True) -> str:
    value = str(date_text or "").strip()
    if not value:
        return datetime.now().strftime("%Y%m%d") if default_today else ""

    clean = value.replace("-", "")
    if re.fullmatch(r"\d{8}", clean):
        return clean
    if re.fullmatch(r"\d{6}", clean):
        return f"{clean}01"
    if re.fullmatch(r"\d{4}", clean):
        return f"{clean}0101"
    return datetime.now().strftime("%Y%m%d") if default_today else ""


def _normalize_month(month_text: str = "") -> str:
    value = str(month_text or "").strip().replace("-", "")
    if not value:
        return datetime.now().strftime("%Y%m")
    if re.fullmatch(r"\d{8}", value):
        return value[:6]
    if re.fullmatch(r"\d{6}", value):
        return value
    if re.fullmatch(r"\d{4}", value):
        return f"{value}01"
    return datetime.now().strftime("%Y%m")


def _month_range(end_month: str, months: int) -> List[str]:
    months_value = _safe_int(months, default=6, minimum=1, maximum=24)
    end = _normalize_month(end_month)
    year = int(end[:4])
    month = int(end[4:6])

    out = []
    for _ in range(months_value):
        out.append(f"{year:04d}{month:02d}")
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    return out


def _cache_fetch(name: str, params: Tuple[Any, ...], loader: Callable[[], Any]) -> Any:
    key = (name, params)
    now = time.time()
    hit = _CACHE.get(key)
    if hit and now - hit["ts"] < CACHE_TTL_SECONDS:
        return hit["value"]

    with _without_proxy_env():
        value = loader()
    _CACHE[key] = {"ts": now, "value": value}
    return value


@contextmanager
def _without_proxy_env():
    backup = {}
    for key in PROXY_ENV_KEYS:
        if key in os.environ:
            backup[key] = os.environ[key]
            os.environ.pop(key, None)
    try:
        yield
    finally:
        for key, value in backup.items():
            os.environ[key] = value


def _save_snapshot(kind: str, code: str, df: pd.DataFrame, source: List[str], hit_date: str) -> None:
    if df is None or df.empty:
        return
    _SNAPSHOT[(kind, code)] = {
        "ts": time.time(),
        "df": df.copy(),
        "source": list(source),
        "hit_date": str(hit_date or ""),
    }


def _load_snapshot(kind: str, code: str) -> Optional[Dict[str, Any]]:
    snap = _SNAPSHOT.get((kind, code))
    if not snap:
        return None
    df = snap.get("df")
    if isinstance(df, pd.DataFrame) and not df.empty:
        return {
            "ts": snap.get("ts", 0),
            "df": df.copy(),
            "source": list(snap.get("source") or []),
            "hit_date": str(snap.get("hit_date") or ""),
        }
    return None


def _candidate_dates(anchor_date: str, fallback_days: int = 7) -> List[str]:
    clean = _normalize_date(anchor_date, default_today=True)
    try:
        anchor = datetime.strptime(clean, "%Y%m%d")
    except Exception:
        return [clean]

    out = []
    for offset in range(max(0, _safe_int(fallback_days, default=7, minimum=0, maximum=30)) + 1):
        out.append((anchor - timedelta(days=offset)).strftime("%Y%m%d"))
    return out


def _normalize_error(err: Exception) -> str:
    raw = str(err or "").replace("\n", " ").strip()
    lower = raw.lower()
    if "no tables found" in lower:
        return "No tables found"
    if "非交易日" in raw or "not trading day" in lower:
        return "非交易日"
    if "find_all" in lower and "nonetype" in lower:
        return "NoneType.find_all"
    if not raw:
        return err.__class__.__name__
    return raw[:200]


def _append_miss(miss_lines: List[str], source_name: str, date_text: str, err: Exception) -> None:
    suffix = f"({date_text})" if date_text else ""
    miss_lines.append(f"{source_name}{suffix}: {_normalize_error(err)}")


def _infer_hit_date(df: pd.DataFrame, fallback: str = "") -> str:
    if df is None or df.empty:
        return fallback

    for col in ["交易日期", "日期", "统计日期", "更新时间", "date", "Date"]:
        if col not in df.columns:
            continue
        try:
            series = df[col].dropna().astype(str)
        except Exception:
            continue
        if series.empty:
            continue
        raw = str(series.iloc[-1]).strip()
        norm = _normalize_date(raw, default_today=False)
        return norm or raw or fallback
    return fallback


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        if isinstance(value, str):
            text = value.replace(",", "").strip()
            if not text or text.lower() in {"nan", "none", "n/a"}:
                return None
            return float(text)
        return float(value)
    except Exception:
        return None


def _spot_price_from_spot_stock(code: str, name: str, miss_lines: List[str]) -> Tuple[Optional[float], str]:
    for sector in SPOT_STOCK_SECTORS:
        try:
            stock_df = _cache_fetch(
                "futures_spot_stock",
                (sector,),
                lambda s=sector: _safe_call_with_warnings(lambda: ak.futures_spot_stock(symbol=s)),
            )
            stock_df = _match_rows(_as_dataframe(stock_df), code, name)
            if stock_df.empty:
                continue

            price_col = ""
            for candidate in ["最新价格", "最新价", "现货价格"]:
                if candidate in stock_df.columns:
                    price_col = candidate
                    break
            if not price_col:
                continue

            for _, row in stock_df.iterrows():
                spot_price = _to_float(row.get(price_col))
                if spot_price is not None and spot_price > 0:
                    return spot_price, sector
        except Exception as e:
            _append_miss(miss_lines, f"futures_spot_stock[{sector}]", "", e)
    return None, ""


def _lookup_cn_name(code: str) -> str:
    c = str(code or "").upper().strip()
    if not c:
        return ""

    candidates = [name for name, v in FUTURES_NAME_MAP.items() if v.upper() == c]
    if not candidates:
        return c

    chinese_names = [x for x in candidates if re.search(r"[\u4e00-\u9fff]", x)]
    if chinese_names:
        return sorted(chinese_names, key=len)[0]
    return sorted(candidates, key=len)[0]


def _resolve_code_name(query: str) -> Tuple[str, str]:
    code, _ = parse_contract_input(str(query or ""))
    code = str(code or "").upper().strip()
    if not re.fullmatch(r"[A-Z]{1,3}", code):
        return "", ""
    return code, _lookup_cn_name(code)


def _as_dataframe(value: Any) -> pd.DataFrame:
    if isinstance(value, pd.DataFrame):
        return value
    if isinstance(value, list):
        return pd.DataFrame(value)
    if isinstance(value, dict):
        rows = []
        for k, v in value.items():
            if isinstance(v, pd.DataFrame):
                d = v.copy()
                d.insert(0, "section", str(k))
                rows.extend(d.to_dict("records"))
            elif isinstance(v, list):
                for item in v:
                    if isinstance(item, dict):
                        item_copy = dict(item)
                        item_copy.setdefault("section", str(k))
                        rows.append(item_copy)
                    else:
                        rows.append({"section": str(k), "value": str(item)})
            elif isinstance(v, dict):
                item_copy = dict(v)
                item_copy.setdefault("section", str(k))
                rows.append(item_copy)
            else:
                rows.append({"section": str(k), "value": str(v)})
        return pd.DataFrame(rows)
    return pd.DataFrame()


def _match_rows(df: pd.DataFrame, code: str, name: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    work = df.copy()
    tokens = [code.lower()]
    if name:
        tokens.append(str(name).lower())

    str_cols = []
    for col in work.columns:
        try:
            sample = work[col].dropna().astype(str)
            if not sample.empty:
                str_cols.append(col)
        except Exception:
            continue

    if not str_cols:
        return work

    mask = pd.Series(False, index=work.index)
    for col in str_cols:
        col_text = work[col].astype(str).str.lower()
        for token in tokens:
            if token:
                mask = mask | col_text.str.contains(re.escape(token), na=False)

    filtered = work[mask]
    return filtered if not filtered.empty else work


def _prefer_columns(df: pd.DataFrame, max_cols: int = 8) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    preferred = [
        "日期", "交易日期", "更新时间", "symbol", "品种", "品种代码", "商品", "合约", "主力合约",
        "现货价格", "现货", "期货价格", "主力价格", "最近交割合约收盘价", "最近交割合约", "基差", "基差值",
        "库存", "库存量", "增减", "变化", "交割量", "期转现量", "仓单", "仓单数量", "增减量",
        "section",
    ]
    keep = [c for c in preferred if c in df.columns]
    if keep:
        remain = [c for c in df.columns if c not in keep]
        keep.extend(remain[: max(0, max_cols - len(keep))])
    else:
        keep = list(df.columns[:max_cols])

    return df[keep]


def _df_to_markdown(df: pd.DataFrame, max_rows: int = 8, max_cols: int = 8) -> str:
    if df is None or df.empty:
        return "(无数据)"

    view = _prefer_columns(df, max_cols=max_cols).head(max_rows)
    try:
        return view.to_markdown(index=False)
    except Exception:
        header = "| " + " | ".join(map(str, view.columns)) + " |"
        sep = "|" + "|".join(["---"] * len(view.columns)) + "|"
        rows = [
            "| " + " | ".join(str(v) for v in row) + " |"
            for row in view.fillna("").astype(str).itertuples(index=False, name=None)
        ]
        return "\n".join([header, sep] + rows)


def _normalize_warehouse_df(value: Any) -> pd.DataFrame:
    df = _as_dataframe(value)
    if df.empty:
        return df

    # 对字典展开后的 section 列做容错保留
    if "section" in df.columns:
        cols = ["section"] + [c for c in df.columns if c != "section"]
        df = df[cols]
    return df


def _normalize_inventory_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()
    alias_keywords = {
        "日期": ["日期", "交易日期", "统计日期", "更新时间", "time"],
        "库存": ["库存", "库存量", "当日库存", "total stock", "stock"],
        "增减": ["增减", "变化", "日增减", "增减量", "change"],
    }

    lower_cols = {str(c).lower(): c for c in out.columns}
    for target, keywords in alias_keywords.items():
        if target in out.columns:
            continue
        for key in keywords:
            matched = None
            key_low = key.lower()
            for low_name, raw_name in lower_cols.items():
                if key_low in low_name:
                    matched = raw_name
                    break
            if matched:
                out = out.rename(columns={matched: target})
                lower_cols = {str(c).lower(): c for c in out.columns}
                break
    return out


def _exchange_from_code(code: str) -> str:
    c = str(code or "").upper()
    if c in SHFE_CODES:
        return "SHFE"
    if c in DCE_CODES:
        return "DCE"
    if c in CZCE_CODES:
        return "CZCE"
    if c in GFEX_CODES:
        return "GFEX"
    return ""


def _format_header(title: str, query: str, code: str, name: str) -> str:
    return (
        f"📌 **{title}**\n"
        f"- 查询输入: {query}\n"
        f"- 命中品种: {code} ({name or code})"
    )


def _safe_call_with_warnings(loader: Callable[[], Any]):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return loader()


@tool
def get_futures_basis_profile(query: str, date: str = "", recent_days: int = 20):
    """
    期货基差/现期结构查询。
    数据优先级: futures_spot_price -> futures_spot_price_previous -> futures_spot_sys。
    """
    clean_query = str(query or "").strip()
    if not clean_query:
        return "请输入品种或合约，例如：螺纹钢、RB、白银。"

    code, name = _resolve_code_name(clean_query)
    if not code:
        return f"未能识别有效期货品种: {clean_query}"

    date_raw = str(date or "").strip()
    date8 = _normalize_date(date_raw, default_today=True)
    date_provided = bool(date_raw)
    days = _safe_int(recent_days, default=20, minimum=1, maximum=240)
    candidate_dates = _candidate_dates(date8, fallback_days=7)

    source_lines = []
    miss_lines = []
    basis_df = pd.DataFrame()
    hit_date = ""
    local_price_used = False
    snapshot_used = False
    mixed_spot_stock_used = False
    ak_source_success = False

    for candidate in candidate_dates:
        try:
            df1 = _cache_fetch(
                "futures_spot_price",
                (candidate, code),
                lambda c=candidate: _safe_call_with_warnings(lambda: ak.futures_spot_price(date=c, vars_list=[code])),
            )
            df1 = _match_rows(_as_dataframe(df1), code, name)
            if not df1.empty:
                basis_df = df1
                source_lines.append("futures_spot_price")
                hit_date = _infer_hit_date(df1, fallback=candidate)
                ak_source_success = True
                break
        except Exception as e:
            _append_miss(miss_lines, "futures_spot_price", candidate, e)

    if basis_df.empty and not date_provided:
        try:
            df1_default = _cache_fetch(
                "futures_spot_price_default",
                (code,),
                lambda: _safe_call_with_warnings(lambda: ak.futures_spot_price(vars_list=[code])),
            )
            df1_default = _match_rows(_as_dataframe(df1_default), code, name)
            if not df1_default.empty:
                basis_df = df1_default
                source_lines.append("futures_spot_price(default)")
                hit_date = _infer_hit_date(df1_default, fallback="default")
                ak_source_success = True
        except Exception as e:
            _append_miss(miss_lines, "futures_spot_price(default)", "", e)

    if basis_df.empty:
        for candidate in candidate_dates:
            try:
                df2 = _cache_fetch(
                    "futures_spot_price_previous",
                    (candidate, code),
                    lambda c=candidate: _safe_call_with_warnings(lambda: ak.futures_spot_price_previous(date=c)),
                )
                df2 = _match_rows(_as_dataframe(df2), code, name)
                if not df2.empty:
                    basis_df = df2
                    source_lines.append("futures_spot_price_previous")
                    hit_date = _infer_hit_date(df2, fallback=candidate)
                    ak_source_success = True
                    break
            except Exception as e:
                _append_miss(miss_lines, "futures_spot_price_previous", candidate, e)

    if basis_df.empty and not date_provided:
        try:
            df2_default = _cache_fetch(
                "futures_spot_price_previous_default",
                (code,),
                lambda: _safe_call_with_warnings(lambda: ak.futures_spot_price_previous()),
            )
            df2_default = _match_rows(_as_dataframe(df2_default), code, name)
            if not df2_default.empty:
                basis_df = df2_default
                source_lines.append("futures_spot_price_previous(default)")
                hit_date = _infer_hit_date(df2_default, fallback="default")
                ak_source_success = True
        except Exception as e:
            _append_miss(miss_lines, "futures_spot_price_previous(default)", "", e)

    if basis_df.empty:
        for candidate in candidate_dates:
            try:
                dt = datetime.strptime(candidate, "%Y%m%d")
                start_day = (dt - pd.Timedelta(days=days)).strftime("%Y%m%d")
                end_day = candidate
                df_daily = _cache_fetch(
                    "futures_spot_price_daily",
                    (start_day, end_day, code),
                    lambda s=start_day, e=end_day: _safe_call_with_warnings(
                        lambda: ak.futures_spot_price_daily(start_day=s, end_day=e, vars_list=[code])
                    ),
                )
                df_daily = _match_rows(_as_dataframe(df_daily), code, name)
                if not df_daily.empty:
                    basis_df = df_daily
                    source_lines.append("futures_spot_price_daily")
                    hit_date = _infer_hit_date(df_daily, fallback=end_day)
                    ak_source_success = True
                    break
            except Exception as e:
                _append_miss(miss_lines, "futures_spot_price_daily", candidate, e)

    if basis_df.empty:
        try:
            df3 = _cache_fetch(
                "futures_spot_sys",
                (name or code,),
                lambda: _safe_call_with_warnings(
                    lambda: ak.futures_spot_sys(symbol=name or code, indicator="市场价格")
                ),
            )
            df3 = _match_rows(_as_dataframe(df3), code, name)
            if not df3.empty:
                basis_df = df3
                source_lines.append("futures_spot_sys")
                hit_date = _infer_hit_date(df3, fallback="spot_sys")
                ak_source_success = True
        except Exception as e:
            _append_miss(miss_lines, "futures_spot_sys", name or code, e)

    if basis_df.empty:
        # 东财现货备源: 用现货与股票提供的现货价格, 再结合本地期货主力价做参考基差
        spot_price, sector = _spot_price_from_spot_stock(code, name, miss_lines)
        if spot_price is not None:
            db_price = None
            db_contract = ""
            db_trade_date = ""
            try:
                db_price, db_contract, db_trade_date = _get_latest_price_for_margin_estimation(code, None)
            except Exception as e:
                _append_miss(miss_lines, "local_futures_price_for_basis", code, e)

            if db_price is not None and db_price > 0:
                basis_est = float(db_price) - float(spot_price)
                basis_df = pd.DataFrame(
                    [
                        {
                            "交易日期": db_trade_date or date8,
                            "合约": db_contract or code,
                            "现货价格": float(spot_price),
                            "期货价格": float(db_price),
                            "基差": round(basis_est, 6),
                            "说明": "现货来自东财现货与股票，基差为参考估算",
                        }
                    ]
                )
                source_lines.extend([f"futures_spot_stock({sector})", "local_futures_price_for_basis"])
                hit_date = db_trade_date or date8
                ak_source_success = True
                mixed_spot_stock_used = True

    if basis_df.empty:
        snap = _load_snapshot("basis", code)
        if snap:
            basis_df = snap["df"]
            source_lines.append("basis_snapshot_fallback")
            hit_date = snap.get("hit_date") or "snapshot"
            snapshot_used = True

    if basis_df.empty:
        db_price = None
        db_contract = ""
        db_trade_date = ""
        try:
            db_price, db_contract, db_trade_date = _get_latest_price_for_margin_estimation(code, None)
        except Exception as e:
            _append_miss(miss_lines, "local_futures_price", code, e)
        if db_price is not None and db_price > 0:
            basis_df = pd.DataFrame(
                [
                    {
                        "交易日期": db_trade_date or "",
                        "合约": db_contract or "",
                        "期货价格": float(db_price),
                        "现货价格": "N/A",
                        "基差": "N/A",
                        "说明": "基差源不可用，仅返回期货端价格参考",
                    }
                ]
            )
            source_lines.append("local_futures_price_fallback")
            hit_date = db_trade_date or "db_latest"
            local_price_used = True

    if ak_source_success:
        _save_snapshot("basis", code, basis_df, source_lines, hit_date)

    out = [
        _format_header("期货基差/现期结构", clean_query, code, name),
        f"- 默认窗口: 最近 {days} 日",
        f"- 数据来源: {', '.join(source_lines) if source_lines else '无可用来源'}",
        f"- 命中日期: {hit_date or 'N/A'}",
        "\n**核心数据**",
        _df_to_markdown(basis_df),
        "\n- 口径说明: 仅返回数据事实，不含交易建议。",
    ]
    if local_price_used:
        out.append("- 补充说明: 现货/基差源临时不可用，已降级返回期货端参考价格。")
    if mixed_spot_stock_used:
        out.append("- 补充说明: 当前基差为现货与股票数据和期货主力价的估算值，仅供参考。")
    if snapshot_used:
        out.append("- 补充说明: 当前返回为历史快照（非当日实时）。")

    if miss_lines:
        out.append("- 未取到来源: " + "；".join(miss_lines[:10]))

    return "\n".join(out)


@tool
def get_futures_inventory_receipt_profile(query: str, date: str = "", recent_days: int = 90):
    """
    期货库存与仓单查询。
    库存优先级: futures_inventory_em -> futures_inventory_99。
    仓单按交易所查询。
    """
    clean_query = str(query or "").strip()
    if not clean_query:
        return "请输入品种或合约，例如：螺纹钢、RB、豆粕。"

    code, name = _resolve_code_name(clean_query)
    if not code:
        return f"未能识别有效期货品种: {clean_query}"

    date8 = _normalize_date(date, default_today=True)
    candidate_dates = _candidate_dates(date8, fallback_days=7)
    days = _safe_int(recent_days, default=90, minimum=1, maximum=720)

    inventory_df = pd.DataFrame()
    inventory_source = []
    inventory_hit_date = ""
    receipt_df = pd.DataFrame()
    receipt_source = []
    receipt_hit_date = ""
    miss_lines = []
    inventory_snapshot_used = False
    receipt_snapshot_used = False

    try:
        inv1 = _cache_fetch("futures_inventory_em", (code.lower(),), lambda: ak.futures_inventory_em(symbol=code.lower()))
        inv1 = _normalize_inventory_df(_match_rows(_as_dataframe(inv1), code, name))
        if not inv1.empty:
            inventory_df = inv1
            inventory_source.append("futures_inventory_em")
            inventory_hit_date = _infer_hit_date(inv1, fallback="latest")
    except Exception as e:
        _append_miss(miss_lines, "futures_inventory_em", code.lower(), e)

    if inventory_df.empty:
        try:
            inv2 = _cache_fetch("futures_inventory_99", (name or code,), lambda: ak.futures_inventory_99(symbol=name or code))
            inv2 = _normalize_inventory_df(_match_rows(_as_dataframe(inv2), code, name))
            if not inv2.empty:
                inventory_df = inv2
                inventory_source.append("futures_inventory_99")
                inventory_hit_date = _infer_hit_date(inv2, fallback="latest")
        except Exception as e:
            _append_miss(miss_lines, "futures_inventory_99", name or code, e)

    if not inventory_df.empty:
        _save_snapshot("inventory", code, inventory_df, inventory_source, inventory_hit_date)
    else:
        inv_snap = _load_snapshot("inventory", code)
        if inv_snap:
            inventory_df = inv_snap["df"]
            inventory_source.append("inventory_snapshot_fallback")
            inventory_hit_date = inv_snap.get("hit_date") or "snapshot"
            inventory_snapshot_used = True

    exchange = _exchange_from_code(code)
    source_name = ""
    if exchange == "SHFE":
        source_name = "futures_shfe_warehouse_receipt"
    elif exchange == "DCE":
        source_name = "futures_warehouse_receipt_dce"
    elif exchange == "CZCE":
        source_name = "futures_warehouse_receipt_czce"
    elif exchange == "GFEX":
        source_name = "futures_gfex_warehouse_receipt"
    else:
        miss_lines.append(f"仓单交易所映射缺失: {code}")

    if source_name:
        for candidate in candidate_dates:
            try:
                if source_name == "futures_shfe_warehouse_receipt":
                    raw = _cache_fetch(source_name, (candidate,), lambda c=candidate: ak.futures_shfe_warehouse_receipt(date=c))
                elif source_name == "futures_warehouse_receipt_dce":
                    raw = _cache_fetch(source_name, (candidate,), lambda c=candidate: ak.futures_warehouse_receipt_dce(date=c))
                elif source_name == "futures_warehouse_receipt_czce":
                    raw = _cache_fetch(source_name, (candidate,), lambda c=candidate: ak.futures_warehouse_receipt_czce(date=c))
                else:
                    raw = _cache_fetch(source_name, (candidate,), lambda c=candidate: ak.futures_gfex_warehouse_receipt(date=c))

                local_receipt = _match_rows(_normalize_warehouse_df(raw), code, name)
                if not local_receipt.empty:
                    receipt_df = local_receipt
                    receipt_source.append(source_name)
                    receipt_hit_date = _infer_hit_date(local_receipt, fallback=candidate)
                    break
            except Exception as e:
                _append_miss(miss_lines, source_name, candidate, e)

    if not receipt_df.empty:
        _save_snapshot("receipt", code, receipt_df, receipt_source, receipt_hit_date)
    else:
        receipt_snap = _load_snapshot("receipt", code)
        if receipt_snap:
            receipt_df = receipt_snap["df"]
            receipt_source.append("receipt_snapshot_fallback")
            receipt_hit_date = receipt_snap.get("hit_date") or "snapshot"
            receipt_snapshot_used = True

    out = [
        _format_header("期货库存与仓单", clean_query, code, name),
        f"- 默认窗口: 最近 {days} 日",
        f"- 库存数据来源: {', '.join(inventory_source) if inventory_source else '无可用来源'}",
        f"- 库存命中日期: {inventory_hit_date or 'N/A'}",
        f"- 仓单数据来源: {', '.join(receipt_source) if receipt_source else '无可用来源'}",
        f"- 仓单命中日期: {receipt_hit_date or 'N/A'}",
        "\n**库存数据**",
        _df_to_markdown(inventory_df),
        "\n**仓单数据**",
        _df_to_markdown(receipt_df),
        "\n- 口径说明: 仅返回数据事实，不含交易建议。",
    ]

    if inventory_snapshot_used:
        out.append("- 补充说明: 库存外部源临时不可用，已降级为历史快照（非当日实时）。")
    if receipt_snapshot_used:
        out.append("- 补充说明: 仓单外部源临时不可用，已降级为历史快照（非当日实时）。")

    if miss_lines:
        out.append("- 未取到来源: " + "；".join(miss_lines[:8]))

    return "\n".join(out)


@tool
def get_futures_delivery_tospot_profile(query: str, end_month: str = "", months: int = 6):
    """
    期货交割与期转现查询。
    汇总近 N 个月各交易所的交割、期转现、交割配对数据。
    """
    clean_query = str(query or "").strip()
    if not clean_query:
        return "请输入品种或合约，例如：螺纹钢、RB、豆粕。"

    code, name = _resolve_code_name(clean_query)
    if not code:
        return f"未能识别有效期货品种: {clean_query}"

    month_list = _month_range(end_month, months)

    to_spot_parts = []
    delivery_parts = []
    match_parts = []
    source_lines = []
    miss_lines = []

    for month in month_list:
        # 期转现
        for src_name, loader in [
            ("futures_to_spot_shfe", lambda m=month: ak.futures_to_spot_shfe(date=m)),
            ("futures_to_spot_dce", lambda m=month: ak.futures_to_spot_dce(date=m)),
            ("futures_to_spot_czce", lambda m=month: ak.futures_to_spot_czce(date=f"{m}01")),
        ]:
            try:
                df = _cache_fetch(src_name, (month, code), loader)
                df = _match_rows(_as_dataframe(df), code, name)
                if not df.empty:
                    local = df.copy()
                    local.insert(0, "月份", month)
                    local.insert(1, "来源", src_name)
                    to_spot_parts.append(local)
                    if src_name not in source_lines:
                        source_lines.append(src_name)
            except Exception as e:
                miss_lines.append(f"{src_name}({month}): {e}")

        # 交割
        for src_name, loader in [
            ("futures_delivery_shfe", lambda m=month: ak.futures_delivery_shfe(date=m)),
            ("futures_delivery_dce", lambda m=month: ak.futures_delivery_dce(date=m)),
            ("futures_delivery_czce", lambda m=month: ak.futures_delivery_czce(date=f"{m}01")),
        ]:
            try:
                df = _cache_fetch(src_name, (month, code), loader)
                df = _match_rows(_as_dataframe(df), code, name)
                if not df.empty:
                    local = df.copy()
                    local.insert(0, "月份", month)
                    local.insert(1, "来源", src_name)
                    delivery_parts.append(local)
                    if src_name not in source_lines:
                        source_lines.append(src_name)
            except Exception as e:
                miss_lines.append(f"{src_name}({month}): {e}")

    # 交割配对单独查一次
    for src_name, loader in [
        ("futures_delivery_match_dce", lambda: ak.futures_delivery_match_dce(symbol=code.lower())),
        ("futures_delivery_match_czce", lambda: ak.futures_delivery_match_czce(date=f"{month_list[0]}01")),
    ]:
        try:
            df = _cache_fetch(src_name, (month_list[0], code), loader)
            df = _match_rows(_as_dataframe(df), code, name)
            if not df.empty:
                local = df.copy()
                local.insert(0, "月份", month_list[0])
                local.insert(1, "来源", src_name)
                match_parts.append(local)
                if src_name not in source_lines:
                    source_lines.append(src_name)
        except Exception as e:
            miss_lines.append(f"{src_name}({month_list[0]}): {e}")

    to_spot_df = pd.concat(to_spot_parts, ignore_index=True) if to_spot_parts else pd.DataFrame()
    delivery_df = pd.concat(delivery_parts, ignore_index=True) if delivery_parts else pd.DataFrame()
    match_df = pd.concat(match_parts, ignore_index=True) if match_parts else pd.DataFrame()

    out = [
        _format_header("期货交割与期转现", clean_query, code, name),
        f"- 窗口月份: {month_list[-1]} ~ {month_list[0]} (共 {len(month_list)} 个月)",
        f"- 数据来源: {', '.join(source_lines) if source_lines else '无可用来源'}",
        "\n**期转现数据**",
        _df_to_markdown(to_spot_df),
        "\n**交割数据**",
        _df_to_markdown(delivery_df),
        "\n**交割配对数据**",
        _df_to_markdown(match_df),
        "\n- 口径说明: 仅返回数据事实，不含交易建议。",
    ]

    if miss_lines:
        out.append("- 未取到来源: " + "；".join(miss_lines[:12]))

    return "\n".join(out)
