import streamlit as st
import pandas as pd
import plotly.express as px
import sys
import os
import time
import logging
from html import escape
# 【关键修改】导入新的独立工具模块，不再依赖 data_engine
import etf_option_tool as de


# 路径修复
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
sys.path.append(root_dir)
from option_kline_chart import lightweight_chart_loader_html, render_option_kline_chart
from ui_components import inject_option_page_header_style, render_option_page_title, render_option_sidebar_footer
from cn_market_climate_data import load_cn_market_climate_strip

# 1. 页面配置
st.set_page_config(
    page_title="爱波塔-ETF期权分析",
    page_icon="favicon.ico",
    layout="wide",
    initial_sidebar_state="expanded"
)


# 加载 CSS

css_path = os.path.join(root_dir, 'style.css')
with open(css_path, encoding='utf-8') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

# 🔥 添加统一的侧边栏导航
sys.path.insert(0, root_dir)
from sidebar_navigation import show_navigation
with st.sidebar:
    show_navigation()
    render_option_sidebar_footer("etf_option")

inject_option_page_header_style()

PAGE_NAME = "ETF期权"
_PAGE_T0 = time.perf_counter()
_PERF_LOGGER = logging.getLogger(__name__)
_CACHE_PROBE_SEEN = set()


def _perf_user_id() -> str:
    return str(
        st.session_state.get("username")
        or st.session_state.get("user")
        or st.session_state.get("current_user")
        or "anonymous"
    )


def _probe_cache(tag: str, signature: str) -> int:
    cache_key = f"{PAGE_NAME}::{tag}::{signature}"
    hit = 1 if cache_key in _CACHE_PROBE_SEEN else 0
    _CACHE_PROBE_SEEN.add(cache_key)
    return hit


def _perf_page_log(
    *,
    page: str,
    render_ms: float = 0.0,
    db_ms: float = 0.0,
    api_ms: float = 0.0,
    cache_hit: int = -1,
    stage: str = "main",
) -> None:
    msg = (
        f"PERF_PAGE page={page} stage={stage} "
        f"render_ms={render_ms:.1f} db_ms={db_ms:.1f} api_ms={api_ms:.1f} cache_hit={cache_hit}"
    )
    print(msg)
    _PERF_LOGGER.info(msg)


@st.cache_data(ttl=90, show_spinner=False)
def _cached_etf_option_analysis(
    user_id: str, page: str, symbol: str, date_window: str, days: int
):
    return de.get_etf_option_analysis(symbol, days=days)


@st.cache_data(ttl=120, show_spinner=False)
def _cached_iv_rank_data(
    user_id: str, page: str, symbol: str, date_window: str, window: int
):
    return de.get_iv_rank_data(symbol, window=window)


@st.cache_data(ttl=90, show_spinner=False)
def _cached_kline_and_iv_data(
    user_id: str, page: str, symbol: str, date_window: str, limit: int
):
    return de.get_kline_and_iv_data(symbol, limit=limit)


@st.cache_data(ttl=600, show_spinner=False)
def _cached_cn_market_climate_strip():
    return load_cn_market_climate_strip(de.engine)


def _inject_etf_lab_style() -> None:
    """Render the ETF option lab with the same calm hierarchy as US options."""
    st.markdown(
        """
        <style>
        [data-testid="stMain"],
        [data-testid="stMainBlockContainer"] {
            background: #f6f8fb !important;
        }

        [data-testid="stMainBlockContainer"] {
            max-width: 100% !important;
            padding-bottom: 52px !important;
        }

        .option-page-title-wrap {
            margin-bottom: 8px !important;
        }

        .etf-lab-page-nav {
            display: none;
        }

        div[data-testid="stHorizontalBlock"]:has(.etf-lab-page-nav) {
            align-items: center !important;
            gap: 18px !important;
            margin-bottom: 18px !important;
        }

        div[data-testid="stHorizontalBlock"]:has(.etf-lab-page-nav) > div[data-testid="stColumn"] {
            min-width: 0 !important;
        }

        div[data-testid="stHorizontalBlock"]:has(.etf-lab-page-nav) div[data-testid="stSelectbox"] {
            margin: 0 !important;
        }

        .etf-lab-kpi-strip {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 8px;
            margin: 8px 0 14px;
        }

        .etf-lab-kpi {
            min-width: 0;
            min-height: 82px;
            padding: 13px 14px 11px;
            border: 1px solid #e2e8f0;
            border-radius: 8px;
            background: #ffffff;
            box-shadow: 0 1px 2px rgba(15, 23, 42, .03);
        }

        .etf-lab-kpi-label-row {
            display: flex;
            align-items: center;
            gap: 5px;
            margin-bottom: 7px;
        }

        .etf-lab-kpi-label {
            color: #64758b;
            font-size: 12px;
            font-weight: 600;
            line-height: 1.2;
            white-space: nowrap;
        }

        .etf-lab-kpi-value {
            font-size: 21px;
            font-weight: 780;
            line-height: 1.05;
            white-space: nowrap;
        }

        .etf-lab-kpi-detail {
            margin-top: 6px;
            color: #708199;
            font-size: 11px;
            line-height: 1.3;
            white-space: normal;
        }

        .etf-lab-kpi-info {
            position: relative;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 14px;
            height: 14px;
            border: 1px solid #cbd5e1;
            border-radius: 999px;
            color: #64748b;
            background: #f8fafc;
            font-size: 10px;
            line-height: 1;
            font-weight: 700;
            cursor: help;
            flex: 0 0 auto;
        }

        .etf-lab-kpi-tooltip {
            position: absolute;
            z-index: 80;
            top: 18px;
            left: 0;
            transform: translateX(-6px);
            width: 340px;
            max-width: min(340px, calc(100vw - 32px));
            padding: 10px 12px;
            border-radius: 8px;
            background: #0f172a;
            color: #f8fafc;
            box-shadow: 0 12px 30px rgba(15, 23, 42, .22);
            font-size: 12px;
            line-height: 1.5;
            font-weight: 500;
            text-align: left;
            white-space: normal;
            opacity: 0;
            visibility: hidden;
            pointer-events: none;
            transition: opacity .12s ease, visibility .12s ease;
        }

        .etf-lab-kpi-info:hover .etf-lab-kpi-tooltip,
        .etf-lab-kpi-info:focus .etf-lab-kpi-tooltip {
            opacity: 1;
            visibility: visible;
        }

        .etf-lab-kpi:nth-child(n+6) .etf-lab-kpi-tooltip {
            left: auto;
            right: 0;
            transform: translateX(6px);
        }

        div[data-testid="stHorizontalBlock"]:has(.etf-lab-rail) {
            align-items: stretch !important;
            gap: 16px !important;
            margin-bottom: 16px !important;
        }

        div[data-testid="stHorizontalBlock"]:has(.etf-lab-rail) > div[data-testid="stColumn"] {
            min-width: 0 !important;
        }

        div[data-testid="stColumn"]:has(.etf-lab-panel-head) {
            padding: 16px 16px 10px !important;
            border: 1px solid #d8e0ea;
            border-radius: 8px;
            background: #ffffff;
            overflow: hidden;
        }

        .etf-lab-panel-head {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 12px;
            margin-bottom: 8px;
        }

        .etf-lab-panel-head h2,
        .etf-lab-section-head h2 {
            margin: 0;
            color: #111827;
            font-size: 20px;
            font-weight: 720;
            line-height: 1.25;
        }

        .etf-lab-panel-head span {
            color: #64748b;
            font-size: 12px;
            white-space: nowrap;
        }

        .etf-lab-rail {
            height: 100%;
            min-height: 652px;
            padding: 16px 18px;
            border: 1px solid #d8e0ea;
            border-radius: 8px;
            background: #ffffff;
            box-sizing: border-box;
        }

        .etf-lab-rail-head {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 8px;
            padding-bottom: 14px;
            border-bottom: 1px solid #d8e0ea;
        }

        .etf-lab-rail-head strong {
            color: #111827;
            font-size: 17px;
            font-weight: 720;
            line-height: 1.25;
            white-space: nowrap;
        }

        .etf-lab-freshness {
            position: relative;
            padding-right: 14px;
            color: #64748b;
            font-size: 11px;
            white-space: nowrap;
        }

        .etf-lab-freshness::after {
            content: "";
            position: absolute;
            right: 0;
            top: 50%;
            width: 7px;
            height: 7px;
            margin-top: -3.5px;
            border-radius: 50%;
            background: #10b981;
            box-shadow: 0 0 0 3px rgba(16, 185, 129, 0.12);
        }

        .etf-lab-iv-block {
            padding: 22px 0 20px;
            border-bottom: 1px solid #d8e0ea;
        }

        .etf-lab-iv-top,
        .etf-lab-rail-row {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 14px;
        }

        .etf-lab-iv-top span,
        .etf-lab-rail-label {
            color: #64748b;
            font-size: 13px;
            font-weight: 600;
        }

        .etf-lab-iv-top em {
            color: #111827;
            font-size: 14px;
            font-style: normal;
            font-weight: 650;
        }

        .etf-lab-iv-value {
            margin: 5px 0 14px;
            color: #dc2626;
            font-family: "SFMono-Regular", Menlo, Monaco, Consolas, monospace;
            font-size: 27px;
            font-weight: 760;
            line-height: 1.15;
        }

        .etf-lab-iv-value small {
            color: #64748b;
            font-size: 14px;
            font-weight: 650;
        }

        .etf-lab-meter {
            position: relative;
            height: 4px;
            border-radius: 999px;
            background: #e2e8f0;
        }

        .etf-lab-meter > span {
            display: block;
            height: 100%;
            border-radius: inherit;
            background: #dc2626;
        }

        .etf-lab-meter-scale {
            display: flex;
            justify-content: space-between;
            margin-top: 7px;
            color: #94a3b8;
            font-size: 10px;
        }

        .etf-lab-rail-row {
            min-height: 78px;
            padding: 14px 0;
            border-bottom: 1px solid #d8e0ea;
        }

        .etf-lab-rail-row:last-child {
            border-bottom: 0;
        }

        .etf-lab-rail-value {
            margin-top: 5px;
            color: #111827;
            font-family: "SFMono-Regular", Menlo, Monaco, Consolas, monospace;
            font-size: 21px;
            font-weight: 740;
            line-height: 1.15;
            white-space: nowrap;
        }

        .etf-lab-rail-value.danger {
            color: #dc2626;
        }

        .etf-lab-rail-value.success {
            color: #16a34a;
        }

        .etf-lab-rail-detail {
            color: #475569;
            font-family: "SFMono-Regular", Menlo, Monaco, Consolas, monospace;
            font-size: 12px;
            font-weight: 650;
            text-align: right;
            white-space: nowrap;
        }

        .etf-lab-section {
            margin-top: 10px;
            padding: 16px;
            border: 1px solid #d8e0ea;
            border-radius: 8px;
            background: #ffffff;
        }

        .etf-lab-section-head {
            margin-bottom: 4px;
        }

        .etf-lab-section-copy {
            margin: 5px 0 12px;
            color: #64748b;
            font-size: 12px;
            line-height: 1.5;
        }

        div[data-testid="stElementContainer"]:has(.etf-lab-kpi-strip),
        div[data-testid="stElementContainer"]:has(.etf-lab-rail),
        div[data-testid="stElementContainer"]:has(.etf-lab-section) {
            margin-bottom: 0 !important;
        }

        @media (max-width: 1360px) {
            .etf-lab-kpi-strip {
                grid-template-columns: repeat(4, minmax(0, 1fr));
            }

            .etf-lab-kpi:nth-child(n+6) .etf-lab-kpi-tooltip {
                left: 0;
                right: auto;
                transform: translateX(-6px);
            }

            .etf-lab-kpi:nth-child(4n) .etf-lab-kpi-tooltip {
                left: auto;
                right: 0;
                transform: translateX(6px);
            }

            div[data-testid="stHorizontalBlock"]:has(.etf-lab-rail) {
                flex-direction: column !important;
            }

            div[data-testid="stHorizontalBlock"]:has(.etf-lab-rail) > div[data-testid="stColumn"] {
                width: 100% !important;
                flex: 1 1 100% !important;
            }

            .etf-lab-rail {
                min-height: auto;
            }
        }

        @media (max-width: 900px) {
            .etf-lab-kpi-strip {
                grid-template-columns: repeat(3, minmax(0, 1fr));
            }

            .etf-lab-kpi:nth-child(4n) .etf-lab-kpi-tooltip {
                left: 0;
                right: auto;
                transform: translateX(-6px);
            }

            .etf-lab-kpi:nth-child(3n) .etf-lab-kpi-tooltip {
                left: auto;
                right: 0;
                transform: translateX(6px);
            }
        }

        @media (max-width: 640px) {
            div[data-testid="stHorizontalBlock"]:has(.etf-lab-page-nav) {
                flex-direction: column !important;
                align-items: stretch !important;
            }

            div[data-testid="stHorizontalBlock"]:has(.etf-lab-page-nav) > div[data-testid="stColumn"] {
                width: 100% !important;
                flex: 1 1 100% !important;
            }

            .etf-lab-kpi-strip {
                grid-template-columns: minmax(0, 1fr);
            }

            .etf-lab-kpi:nth-child(3n) .etf-lab-kpi-tooltip {
                left: 0;
                right: auto;
                transform: translateX(-6px);
            }

            .etf-lab-rail-row {
                align-items: flex-start;
                flex-direction: column;
            }

            .etf-lab-rail-detail {
                text-align: left;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _iv_regime(percentile: float | None) -> tuple[str, str]:
    if percentile is None:
        return "数据不足", "#64748b"
    if percentile >= 80:
        return "偏高", "#dc2626"
    if percentile <= 20:
        return "偏低", "#16a34a"
    return "中性", "#2563eb"


def _render_market_climate_strip(cards: list[dict]) -> None:
    def card_html(card: dict) -> str:
        hint = str(card.get("hint") or "")
        info = ""
        if hint:
            info = (
                '<span class="etf-lab-kpi-info" tabindex="0" aria-label="数据说明">i'
                f'<span class="etf-lab-kpi-tooltip">{escape(hint)}</span></span>'
            )
        return (
            '<div class="etf-lab-kpi">'
            '<div class="etf-lab-kpi-label-row">'
            f'<span class="etf-lab-kpi-label">{escape(str(card.get("label") or "--"))}</span>'
            f"{info}</div>"
            f'<div class="etf-lab-kpi-value" style="color:{escape(str(card.get("color") or "#0f172a"))}">'
            f'{escape(str(card.get("value") or "--"))}</div>'
            f'<div class="etf-lab-kpi-detail">{escape(str(card.get("detail") or ""))}</div>'
            "</div>"
        )

    st.markdown(
        '<div class="etf-lab-kpi-strip">' + "".join(card_html(card) for card in cards) + "</div>",
        unsafe_allow_html=True,
    )


def _render_volatility_rail(
    *,
    latest_date: str,
    pressure: float,
    pressure_oi: int,
    support: float,
    support_oi: int,
    iv_percentile: float | None,
) -> None:
    iv_value = max(0.0, min(100.0, float(iv_percentile or 0.0)))
    iv_text = f"{iv_percentile:.0f}" if iv_percentile is not None else "--"
    regime, regime_color = _iv_regime(iv_percentile)
    updated = latest_date.replace("-", "/")
    st.markdown(
        f"""
        <div class="etf-lab-rail">
            <div class="etf-lab-rail-head">
                <strong>波动率与区间速览</strong>
                <span class="etf-lab-freshness">更新至 {escape(updated)}</span>
            </div>
            <div class="etf-lab-iv-block">
                <div class="etf-lab-iv-top"><span>IV等级</span><em style="color:{regime_color}">{escape(regime)}</em></div>
                <div class="etf-lab-iv-value" style="color:{regime_color}">{escape(iv_text)}<small>/100</small></div>
                <div class="etf-lab-meter"><span style="width:{iv_value:.1f}%;background:{regime_color}"></span></div>
                <div class="etf-lab-meter-scale"><span>0</span><span>25</span><span>50</span><span>75</span><span>100</span></div>
            </div>
            <div class="etf-lab-rail-row">
                <div><div class="etf-lab-rail-label">压力位</div><div class="etf-lab-rail-value danger">{pressure:.3f}</div></div>
                <div class="etf-lab-rail-detail">持仓 {pressure_oi:,}</div>
            </div>
            <div class="etf-lab-rail-row">
                <div><div class="etf-lab-rail-label">支撑位</div><div class="etf-lab-rail-value success">{support:.3f}</div></div>
                <div class="etf-lab-rail-detail">持仓 {support_oi:,}</div>
            </div>
            <div class="etf-lab-rail-row">
                <div><div class="etf-lab-rail-label">当前区间</div><div class="etf-lab-rail-value">{support:.3f}–{pressure:.3f}</div></div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _prepare_price_iv_frames(
    df_kline: pd.DataFrame,
    df_iv: pd.DataFrame,
    period: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    k_df = df_kline.copy()
    if "volume" not in k_df.columns:
        k_df["volume"] = 0.0
    k_df = k_df[["trade_date", "open", "high", "low", "close", "volume"]].copy()
    k_df.columns = ["date", "open", "high", "low", "close", "volume"]
    k_df["date_obj"] = pd.to_datetime(k_df["date"], errors="coerce")
    for column in ("open", "high", "low", "close", "volume"):
        k_df[column] = pd.to_numeric(k_df[column], errors="coerce")
    k_df["volume"] = k_df["volume"].fillna(0.0)
    k_df = k_df.dropna(subset=["date_obj", "open", "high", "low", "close"]).sort_values("date_obj")

    if df_iv is None or df_iv.empty or not {"trade_date", "iv"}.issubset(df_iv.columns):
        iv_df = pd.DataFrame(columns=["date_obj", "iv"])
    else:
        iv_df = df_iv[["trade_date", "iv"]].copy()
        iv_df.columns = ["date", "iv"]
        iv_df["date_obj"] = pd.to_datetime(iv_df["date"], errors="coerce")
        iv_df["iv"] = pd.to_numeric(iv_df["iv"], errors="coerce")
        iv_df = iv_df.dropna(subset=["date_obj", "iv"])
        iv_df = iv_df[iv_df["iv"] > 0].sort_values("date_obj")

    if not k_df.empty and not iv_df.empty:
        iv_df = iv_df[
            (iv_df["date_obj"] >= k_df["date_obj"].min())
            & (iv_df["date_obj"] <= k_df["date_obj"].max())
        ]

    if period == "weekly":
        k_df = (
            k_df.set_index("date_obj")
            .resample("W-FRI")
            .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
            .dropna(subset=["open", "high", "low", "close"])
            .reset_index()
        )
        if not iv_df.empty:
            iv_df = (
                iv_df.set_index("date_obj")[["iv"]]
                .resample("W-FRI")
                .last()
                .dropna()
                .reset_index()
            )

    for window in (5, 20, 60):
        k_df[f"ma{window}"] = k_df["close"].rolling(window).mean()
    k_df["date"] = k_df["date_obj"].dt.strftime("%Y-%m-%d")
    if not iv_df.empty:
        iv_df["date"] = iv_df["date_obj"].dt.strftime("%Y-%m-%d")
    else:
        iv_df["date"] = pd.Series(dtype="object")

    return (
        k_df[["date", "open", "high", "low", "close", "volume", "ma5", "ma20", "ma60"]],
        iv_df[["date", "iv"]],
    )


def _line_records(frame: pd.DataFrame, value_column: str) -> list[dict[str, float | str]]:
    if frame is None or frame.empty or value_column not in frame.columns:
        return []
    rows: list[dict[str, float | str]] = []
    for _, row in frame[["date", value_column]].dropna().iterrows():
        rows.append({"time": str(row["date"]), "value": float(row[value_column])})
    return rows


def _build_etf_kline_dataset(
    k_df: pd.DataFrame,
    iv_df: pd.DataFrame,
) -> dict[str, object]:
    candles: list[dict[str, float | str]] = []
    volumes: list[dict[str, float | str]] = []
    for _, row in k_df.iterrows():
        open_price = float(row["open"])
        close_price = float(row["close"])
        time_value = str(row["date"])
        candles.append(
            {
                "time": time_value,
                "open": open_price,
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": close_price,
            }
        )
        volumes.append(
            {
                "time": time_value,
                "value": float(row.get("volume", 0.0) or 0.0),
                "color": "rgba(239, 68, 68, 0.34)"
                if close_price >= open_price
                else "rgba(16, 185, 129, 0.34)",
            }
        )

    latest: dict[str, float | str] = {}
    if candles:
        latest = {"time": candles[-1]["time"], "close": float(candles[-1]["close"])}
        if len(candles) >= 2 and float(candles[-2]["close"]):
            previous_close = float(candles[-2]["close"])
            change = float(candles[-1]["close"]) - previous_close
            latest["change"] = change
            latest["change_pct"] = change / previous_close * 100

    return {
        "candles": candles,
        "volumes": volumes,
        "ma5": _line_records(k_df, "ma5"),
        "ma20": _line_records(k_df, "ma20"),
        "ma60": _line_records(k_df, "ma60"),
        "iv": _line_records(iv_df, "iv"),
        "latest": latest,
    }


def _build_etf_kline_payload(
    df_kline: pd.DataFrame,
    df_iv: pd.DataFrame,
    *,
    symbol: str,
    pressure: float,
    support: float,
) -> dict[str, object]:
    daily_k, daily_iv = _prepare_price_iv_frames(df_kline, df_iv, "daily")
    weekly_k, weekly_iv = _prepare_price_iv_frames(df_kline, df_iv, "weekly")
    daily = _build_etf_kline_dataset(daily_k, daily_iv)
    weekly = _build_etf_kline_dataset(weekly_k, weekly_iv)
    return {
        "symbol": symbol,
        **daily,
        "datasets": {"daily": daily, "weekly": weekly},
        "referenceLines": [
            {"price": pressure, "color": "#dc2626", "title": "压力", "lineWidth": 1},
            {"price": support, "color": "#16a34a", "title": "支撑", "lineWidth": 1},
        ],
        "config": {
            "showTitle": False,
            "showLatest": False,
            "enablePeriodSwitch": True,
            "activePeriod": "daily",
            "priceDigits": 3,
            "useTimeVisibleRange": True,
            "storageNamespace": "etf-options-chart-drawings",
            "titleContext": "ETF 日线 · 本地数据库",
            "ivLabel": "平均IV",
        },
    }


def _render_price_iv_chart(
    df_kline: pd.DataFrame,
    df_iv: pd.DataFrame,
    *,
    symbol: str,
    pressure: float,
    support: float,
) -> None:
    payload = _build_etf_kline_payload(
        df_kline,
        df_iv,
        symbol=symbol,
        pressure=pressure,
        support=support,
    )
    render_option_kline_chart(
        payload,
        chart_loader_html=lightweight_chart_loader_html(),
        height=650,
    )


def _build_defense_figure(df: pd.DataFrame, target: str, *, height: int = 390):
    fig = px.line(
        df,
        x='date_obj',
        y='strike',
        color='type',
        markers=False,
        title=None,
        color_discrete_map={"认购 (压力)": "#dc2626", "认沽 (支撑)": "#16a34a"},
    )
    fig.update_traces(line=dict(width=2.2))
    fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='white',
        margin=dict(l=18, r=18, t=20, b=18),
        xaxis_title=None,
        yaxis_title=None,
        hovermode="x unified",
        height=height,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            title=None,
        ),
        font=dict(family="Arial, sans-serif", color="#475569", size=12),
        xaxis=dict(showgrid=False, tickformat="%m/%d", zeroline=False),
        yaxis=dict(showgrid=True, gridcolor='#e7edf4', zeroline=False),
    )
    return fig


def _render_defense_chart(df: pd.DataFrame, target: str, *, height: int = 390) -> None:
    st.markdown(
        """
        <div class="etf-lab-panel-head">
            <h2>主力持仓防线移动（近20日）</h2>
            <span>压力与支撑位置变化</span>
        </div>
        <div class="etf-lab-section-copy">观察最大持仓合约的移动：红线代表上方压力，绿线代表下方支撑。</div>
        """,
        unsafe_allow_html=True,
    )
    st.plotly_chart(_build_defense_figure(df, target, height=height), width="stretch")


_inject_etf_lab_style()


# --- 页面逻辑 ---
render_option_page_title("ETF期权")

view_options = ["总览", "持仓防线"]
if st.session_state.get("etf_option_active_view") not in view_options:
    st.session_state["etf_option_active_view"] = "总览"

nav_col, target_col = st.columns([0.72, 0.28], gap="small", vertical_alignment="center")
with nav_col:
    st.markdown('<div class="etf-lab-page-nav"></div>', unsafe_allow_html=True)
    active_view = st.segmented_control(
        "页面",
        options=view_options,
        label_visibility="collapsed",
        key="etf_option_active_view",
    ) or "总览"
with target_col:
    target = st.selectbox(
        "标的",
        ["510300 (300ETF)", "510050 (50ETF)", "510500 (500ETF)", "588000 (科创50ETF)", "159915 (创业板ETF)"],
        format_func=lambda value: value.replace(" (", " ").replace(")", ""),
        label_visibility="collapsed",
        key="etf_option_symbol",
    )

# 1. 获取基础代码
etf_code = target.split(' ')[0] # 得到 "510050"

# --- 【关键修复】补全后缀 (匹配数据库格式) ---
if "." not in etf_code:
    if etf_code.startswith("15") or etf_code.startswith("16"):
        etf_code = etf_code + ".SZ" # 深市
    else:
        etf_code = etf_code + ".SH" # 沪市 (50/300/500/科创)

# 数据获取
user_id = _perf_user_id()
main_window = "20d"
main_sig = f"{user_id}|{PAGE_NAME}|{etf_code}|{main_window}"
main_cache_hit = _probe_cache("etf_option_analysis", main_sig)
_api_t0 = time.perf_counter()
with st.spinner(f"正在扫描 {etf_code} 全市场持仓数据..."):
    df = _cached_etf_option_analysis(user_id, PAGE_NAME, etf_code, main_window, 20)
_perf_page_log(
    page=PAGE_NAME,
    api_ms=(time.perf_counter() - _api_t0) * 1000,
    cache_hit=main_cache_hit,
    stage="get_etf_option_analysis",
)

if df is None or df.empty:
    st.error("暂无数据，可能是非交易时间或 Tushare 接口受限。")
    st.stop()

# --- 【关键修复】健壮的日期处理 ---
try:
    # 1. 先统一转为字符串，去除可能的空格
    df['date'] = df['date'].astype(str).str.strip()

    # 2. 智能解析日期 (兼容 '20251121', '2025-11-21' 等多种格式)
    df['date_obj'] = pd.to_datetime(df['date'], errors='coerce')

    # 3. 再次格式化为我们想要的字符串 (用于表格显示)
    df['date_str'] = df['date_obj'].dt.strftime('%Y-%m-%d')

    # 4. 按时间排序
    df = df.sort_values('date_obj')

    # 5. 去除解析失败的坏数据
    df = df.dropna(subset=['date_obj'])

except Exception as e:
    st.error(f"日期处理出错: {e}")
    st.stop()

# --- 1. 核心洞察卡片 ---
latest_date = df['date_str'].max()  # 使用格式化后的字符串取最大值
latest_data = df[df['date_str'] == latest_date]

# --- 核心指标卡片 ---
# (此处调用 tool.get_iv_rank_data)
iv_window = "252d"
iv_sig = f"{user_id}|{PAGE_NAME}|{etf_code}|{iv_window}"
iv_cache_hit = _probe_cache("iv_rank", iv_sig)
_api_t0 = time.perf_counter()
iv_stats = _cached_iv_rank_data(user_id, PAGE_NAME, etf_code, iv_window, 252)
_perf_page_log(
    page=PAGE_NAME,
    api_ms=(time.perf_counter() - _api_t0) * 1000,
    cache_hit=iv_cache_hit,
    stage="get_iv_rank_data",
)

call_matches = latest_data[latest_data['type'].astype(str).str.contains('认购', na=False)]
put_matches = latest_data[latest_data['type'].astype(str).str.contains('认沽', na=False)]
if call_matches.empty or put_matches.empty:
    st.warning("当前交易日缺少完整的压力或支撑持仓数据，请稍后重试。")
    st.stop()

call_row = call_matches.iloc[0]
put_row = put_matches.iloc[0]
pressure = float(call_row['strike'])
support = float(put_row['strike'])
pressure_oi = int(call_row['oi'])
support_oi = int(put_row['oi'])
iv_percentile = None
if iv_stats and iv_stats.get('iv_percentile') is not None:
    iv_percentile = float(iv_stats['iv_percentile'])

# --- 3. 价格与波动率 (K线 + IV) ---
# 【核心调用】从新文件获取数据
kline_window = "500bars"
kline_sig = f"{user_id}|{PAGE_NAME}|{etf_code}|{kline_window}"
kline_cache_hit = _probe_cache("kline_iv", kline_sig)
_api_t0 = time.perf_counter()
df_kline, df_iv = _cached_kline_and_iv_data(user_id, PAGE_NAME, etf_code, kline_window, 500)
_perf_page_log(
    page=PAGE_NAME,
    api_ms=(time.perf_counter() - _api_t0) * 1000,
    cache_hit=kline_cache_hit,
    stage="get_kline_and_iv_data",
)



_render_market_climate_strip(_cached_cn_market_climate_strip())

price_data_ready = not df_kline.empty
main_col, rail_col = st.columns([2.55, 1.05], gap="small")
if active_view == "持仓防线":
    with main_col:
        _render_defense_chart(df, target, height=650)
else:
    with main_col:
        if price_data_ready:
            _render_price_iv_chart(
                df_kline,
                df_iv,
                symbol=etf_code,
                pressure=pressure,
                support=support,
            )
        else:
            st.info("暂无价格数据。")

with rail_col:
    _render_volatility_rail(
        latest_date=latest_date,
        pressure=pressure,
        pressure_oi=pressure_oi,
        support=support,
        support_oi=support_oi,
        iv_percentile=iv_percentile,
    )

_perf_page_log(
    page=PAGE_NAME,
    render_ms=(time.perf_counter() - _PAGE_T0) * 1000,
    cache_hit=-1,
    stage="page_done",
)
