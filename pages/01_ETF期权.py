import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
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
from global_index_valuation import (
    build_global_index_valuation_dashboard,
    get_global_index_valuation_cache_version,
)

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


@st.cache_data(ttl=120, show_spinner=False)
def _cached_cn_market_climate_strip():
    return load_cn_market_climate_strip(de.engine)


@st.cache_data(ttl=1800, show_spinner=False)
def _cached_global_index_valuation_dashboard(cache_version: str):
    return build_global_index_valuation_dashboard(de.engine)


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

        .global-valuation-dashboard {
            margin-top: 8px;
            color: #172235;
        }

        .global-valuation-title-row {
            display: flex;
            align-items: flex-start;
            justify-content: space-between;
            gap: 24px;
            margin-bottom: 14px;
        }

        .global-valuation-title-row h2 {
            margin: 0;
            color: #102039;
            font-size: 28px;
            font-weight: 760;
            letter-spacing: -.02em;
        }

        .global-valuation-title-row p {
            margin: 4px 0 0;
            color: #63748b;
            font-size: 13px;
            line-height: 1.55;
        }

        .global-valuation-title-row > span {
            padding-top: 8px;
            color: #6b7c93;
            font-size: 12px;
            white-space: nowrap;
        }

        .global-valuation-summary {
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            margin-bottom: 14px;
            overflow: hidden;
            border: 1px solid #dce4ee;
            border-radius: 9px;
            background: #ffffff;
        }

        .global-valuation-summary-item {
            min-width: 0;
            padding: 14px 18px 15px;
        }

        .global-valuation-summary-item + .global-valuation-summary-item {
            border-left: 1px solid #e3e9f1;
        }

        .global-valuation-status {
            display: inline-flex;
            align-items: center;
            padding: 2px 7px;
            border: 1px solid color-mix(in srgb, var(--valuation-color) 20%, white);
            border-radius: 999px;
            color: var(--valuation-color);
            background: color-mix(in srgb, var(--valuation-color) 8%, white);
            font-size: 10px;
            font-weight: 700;
        }

        .global-valuation-summary-main {
            display: grid;
            grid-template-columns: minmax(0, 1fr) minmax(150px, .8fr);
            align-items: center;
            gap: 22px;
            margin-top: 8px;
        }

        .global-valuation-summary-main strong {
            overflow: hidden;
            color: #1d2b40;
            font-size: 15px;
            text-overflow: ellipsis;
            white-space: nowrap;
        }

        .global-valuation-percentile {
            display: grid;
            grid-template-columns: 56px minmax(86px, 1fr);
            align-items: center;
            gap: 10px;
            min-width: 0;
        }

        .global-valuation-percentile > span {
            color: var(--valuation-color);
            font-family: "SFMono-Regular", Menlo, Monaco, Consolas, monospace;
            font-size: 12px;
            font-weight: 760;
            white-space: nowrap;
        }

        .global-valuation-percentile.is-compact {
            grid-template-columns: 62px minmax(90px, 1fr);
        }

        .global-valuation-percentile.is-compact > span {
            font-size: 17px;
            text-align: right;
        }

        .global-valuation-percentile progress {
            width: 100%;
            height: 5px;
            overflow: hidden;
            border: 0;
            border-radius: 999px;
            background: #e8edf4;
            appearance: none;
            -webkit-appearance: none;
        }

        .global-valuation-percentile progress::-webkit-progress-bar {
            border-radius: 999px;
            background: #e8edf4;
        }

        .global-valuation-percentile progress::-webkit-progress-value {
            border-radius: 999px;
            background: var(--valuation-color);
        }

        .global-valuation-percentile progress::-moz-progress-bar {
            border-radius: 999px;
            background: var(--valuation-color);
        }

        .global-valuation-table-shell {
            overflow-x: auto;
            border: 1px solid #dce4ee;
            border-radius: 9px;
            background: #ffffff;
        }

        .global-valuation-table-header {
            display: grid;
            grid-template-columns: 72px minmax(140px, 1.15fr) 100px minmax(230px, 1.5fr) 90px 112px;
            align-items: center;
            min-width: 790px;
            padding: 10px 16px;
            border-bottom: 1px solid #dce4ee;
            color: #6b7c93;
            background: #f8fafc;
            font-size: 11px;
            font-weight: 650;
        }

        .global-valuation-table-group {
            display: grid;
            grid-template-columns: 72px minmax(718px, 1fr);
            min-width: 790px;
            border-bottom: 1px solid #dce4ee;
        }

        .global-valuation-table-group:last-child {
            border-bottom: 0;
        }

        .global-valuation-table-market {
            display: flex;
            align-items: center;
            padding: 0 16px;
            color: #203047;
            font-size: 13px;
            font-weight: 740;
        }

        .global-valuation-table-row {
            display: grid;
            grid-template-columns: minmax(140px, 1.15fr) 100px minmax(230px, 1.5fr) 90px 112px;
            align-items: center;
            min-height: 38px;
            padding: 0 16px 0 0;
            border-bottom: 1px solid #edf1f5;
            color: #34445a;
            font-size: 12px;
        }

        .global-valuation-table-row:last-child {
            border-bottom: 0;
        }

        .global-valuation-table-row strong {
            color: #203047;
            font-size: 12px;
            font-weight: 680;
        }

        .global-valuation-table-pe,
        .global-valuation-table-date {
            font-family: "SFMono-Regular", Menlo, Monaco, Consolas, monospace;
            font-variant-numeric: tabular-nums;
        }

        .global-valuation-table-status {
            font-weight: 680;
        }

        .global-valuation-table-date {
            color: #73849a;
            font-size: 11px;
        }

        .global-valuation-history-section-head {
            margin: 24px 0 10px;
            color: #1b2a40;
            font-size: 17px;
            font-weight: 740;
            line-height: 1.35;
        }

        div[data-testid="stElementContainer"]:has(.global-valuation-controls-marker) {
            display: none;
        }

        div[data-testid="stHorizontalBlock"]:has(.global-valuation-controls-marker) {
            align-items: flex-end !important;
            gap: 18px !important;
            margin-bottom: 16px;
        }

        div[data-testid="stHorizontalBlock"]:has(.global-valuation-controls-marker) > div[data-testid="stColumn"]:first-child {
            width: min(520px, 100%) !important;
            max-width: 520px !important;
            flex: 0 1 520px !important;
        }

        div[data-testid="stHorizontalBlock"]:has(.global-valuation-controls-marker) > div[data-testid="stColumn"]:last-child {
            width: auto !important;
            flex: 0 0 auto !important;
            margin-left: auto;
        }

        .global-valuation-history-title {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 18px;
            margin-top: 0;
            padding: 14px 16px 4px;
            border: 1px solid #dce4ee;
            border-bottom: 0;
            border-radius: 9px 9px 0 0;
            background: #ffffff;
        }

        .global-valuation-history-title strong {
            color: #1b2a40;
            font-size: 16px;
        }

        .global-valuation-history-title span {
            color: #718198;
            font-size: 11px;
        }

        div[data-testid="stElementContainer"]:has(.global-valuation-history-title) + div[data-testid="stElementContainer"] {
            border-right: 1px solid #dce4ee;
            border-bottom: 1px solid #dce4ee;
            border-left: 1px solid #dce4ee;
            border-radius: 0 0 9px 9px;
            background: #ffffff;
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

            .global-valuation-summary {
                grid-template-columns: minmax(0, 1fr);
            }

            .global-valuation-summary-item + .global-valuation-summary-item {
                border-top: 1px solid #e3e9f1;
                border-left: 0;
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

            .global-valuation-title-row,
            .global-valuation-history-title {
                align-items: flex-start;
                flex-direction: column;
                gap: 6px;
            }

            div[data-testid="stHorizontalBlock"]:has(.global-valuation-controls-marker) {
                align-items: stretch !important;
                flex-direction: column !important;
                gap: 10px !important;
            }

            div[data-testid="stHorizontalBlock"]:has(.global-valuation-controls-marker) > div[data-testid="stColumn"]:first-child,
            div[data-testid="stHorizontalBlock"]:has(.global-valuation-controls-marker) > div[data-testid="stColumn"]:last-child {
                width: 100% !important;
                max-width: none !important;
                flex: 0 0 auto !important;
                margin-left: 0 !important;
            }

            .global-valuation-title-row h2 {
                font-size: 24px;
            }

            .global-valuation-title-row > span {
                padding-top: 0;
            }

            .global-valuation-summary-main {
                grid-template-columns: minmax(0, 1fr) minmax(140px, .9fr);
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
        markers=True,
        text='oi',
        title=None,
        color_discrete_map={"认购 (压力)": "#dc2626", "认沽 (支撑)": "#16a34a"},
    )
    fig.update_traces(
        line=dict(width=2.2),
        marker=dict(size=6, line=dict(width=1.4, color="white")),
        texttemplate="%{text:,.0f}",
        textfont=dict(size=10),
        cliponaxis=False,
    )
    fig.update_traces(textposition="top center", selector=dict(name="认购 (压力)"))
    fig.update_traces(textposition="bottom center", selector=dict(name="认沽 (支撑)"))
    fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='white',
        margin=dict(l=18, r=18, t=30, b=30),
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
        <div class="etf-lab-section-copy">观察最大持仓合约的移动：红线代表上方压力，绿线代表下方支撑，点旁数字为持仓量。</div>
        """,
        unsafe_allow_html=True,
    )
    st.plotly_chart(_build_defense_figure(df, target, height=height), width="stretch")


def _render_defense_detail_table(df: pd.DataFrame) -> None:
    with st.expander("查看详细数据表"):
        st.dataframe(
            df[['date_str', 'type', 'strike', 'oi', 'price', 'code']],
            column_config={
                "date_str": "日期",
                "type": "类型",
                "strike": st.column_config.NumberColumn("行权价", format="%.3f"),
                "oi": st.column_config.NumberColumn("持仓量(张)", format="%d"),
                "price": st.column_config.NumberColumn("期权价", format="%.4f"),
                "code": "合约代码",
            },
            width="stretch",
        )


def _render_global_valuation_dashboard(payload: dict) -> None:
    """Render the local-only global PE comparison dashboard."""
    label_colors = {
        "历史低位": "#16a34a", "偏低": "#4f9d69", "中性": "#2563eb",
        "偏高": "#d97706", "历史高位": "#dc2626",
        "样本不足": "#64748b", "暂无数据": "#64748b",
    }
    cards = payload.get("cards") or []
    populated_cards = [card for card in cards if card.get("percentile") is not None]
    latest_date = max(
        (str(card.get("data_date") or "") for card in cards),
        default="",
    ) or str(payload.get("as_of_date") or "")
    if latest_date and "-" not in latest_date and len(latest_date) == 8:
        latest_date = f"{latest_date[:4]}-{latest_date[4:6]}-{latest_date[6:]}"

    def percentile_html(card: dict, compact: bool = False) -> str:
        percentile = card.get("percentile")
        value = max(0.0, min(100.0, float(percentile))) if percentile is not None else 0.0
        rank_text = f"{value:.0f}/100" if percentile is not None else "--/100"
        label = str(card.get("percentile_label") or "暂无数据")
        color = label_colors.get(label, "#64748b")
        return (
            f'<div class="global-valuation-percentile{" is-compact" if compact else ""}" '
            f'style="--valuation-color:{color}">'
            f'<span>{escape(rank_text)}</span>'
            f'<progress value="{value:.1f}" max="100" aria-label="{escape(rank_text)}"></progress>'
            "</div>"
        )

    ranked_cards = sorted(
        populated_cards,
        key=lambda card: float(card.get("percentile") or 0),
        reverse=True,
    )
    summary_cards = []
    for card in (ranked_cards[:2] + ranked_cards[-1:]):
        if card and card not in summary_cards:
            summary_cards.append(card)

    summary_html = "".join(
        '<div class="global-valuation-summary-item">'
        f'<span class="global-valuation-status" style="--valuation-color:'
        f'{label_colors.get(str(card.get("percentile_label")), "#64748b")}">'
        f'{escape(str(card.get("percentile_label") or "暂无数据"))}</span>'
        '<div class="global-valuation-summary-main">'
        f'<strong>{escape(str(card.get("name") or "--"))}</strong>'
        f'{percentile_html(card, compact=True)}</div>'
        "</div>"
        for card in summary_cards
    )

    group_html = []
    for market in ("美国", "A股", "香港"):
        market_cards = [card for card in cards if card.get("market") == market]
        rows = []
        for card in market_cards:
            current_pe = card.get("current_pe")
            pe_text = f"{float(current_pe):.2f}" if current_pe is not None else "--"
            label = str(card.get("percentile_label") or "暂无数据")
            color = label_colors.get(label, "#64748b")
            rows.append(
                '<div class="global-valuation-table-row">'
                f'<strong>{escape(str(card.get("name") or "--"))}</strong>'
                f'<span class="global-valuation-table-pe">{escape(pe_text)}</span>'
                f'{percentile_html(card)}'
                f'<span class="global-valuation-table-status" style="color:{color}">{escape(label)}</span>'
                f'<span class="global-valuation-table-date">{escape(str(card.get("data_date") or "待更新"))}</span>'
                "</div>"
            )
        group_html.append(
            '<div class="global-valuation-table-group">'
            f'<div class="global-valuation-table-market">{escape(market)}</div>'
            f'<div class="global-valuation-table-rows">{"".join(rows)}</div>'
            "</div>"
        )

    st.markdown(
        '<div class="global-valuation-dashboard">'
        '<div class="global-valuation-title-row">'
        '<div><h2>股市估值</h2><p>当前市盈率（PE）在自身历史区间中的位置，分位越高表示越接近历史高位。</p></div>'
        f'<span>数据日期：{escape(latest_date or "待更新")}</span></div>'
        f'<div class="global-valuation-summary">{summary_html}</div>'
        '<div class="global-valuation-table-shell">'
        '<div class="global-valuation-table-header">'
        '<span>区域</span><span>指数</span><span>当前PE</span><span>历史分位（0–100）</span><span>状态</span><span>数据日期</span>'
        f'</div>{"".join(group_html)}</div></div>',
        unsafe_allow_html=True,
    )

    if not populated_cards:
        for note in payload.get("quality_notes") or ["暂无可用估值数据。"]:
            st.warning(str(note))
        return

    available_cards = [
        card for card in cards if (payload.get("series_by_code") or {}).get(card.get("code"))
    ]
    if not available_cards:
        st.info("暂无可绘制的本地历史数据。")
        return

    available_names = [card["name"] for card in available_cards]
    default_name = "科创50" if "科创50" in available_names else available_names[0]
    st.markdown(
        '<div class="global-valuation-history-section-head">历史走势</div>',
        unsafe_allow_html=True,
    )
    selected_col, range_col = st.columns([0.5, 0.5], gap="small")
    with selected_col:
        st.markdown('<span class="global-valuation-controls-marker"></span>', unsafe_allow_html=True)
        selected_name = st.selectbox(
            "选择指数", available_names, index=available_names.index(default_name),
            key="global_valuation_index_selector",
            label_visibility="collapsed",
        )
    range_options = ["近1年", "近3年", "近5年", "近10年", "全部"]
    if st.session_state.get("global_valuation_history_range") not in range_options:
        st.session_state["global_valuation_history_range"] = "近5年"
    with range_col:
        history_range = st.segmented_control(
            "历史区间", range_options, key="global_valuation_history_range",
            label_visibility="collapsed",
        ) or "近5年"
    selected = next(card for card in available_cards if card["name"] == selected_name)
    history = pd.DataFrame(payload["series_by_code"][selected["code"]])
    history["date"] = pd.to_datetime(history["date"], errors="coerce")
    history["pe"] = pd.to_numeric(history["pe"], errors="coerce")
    history = history.dropna(subset=["date", "pe"])
    range_years = {"近1年": 1, "近3年": 3, "近5年": 5, "近10年": 10}
    if history_range in range_years and not history.empty:
        history = history[
            history["date"] >= history["date"].max() - pd.DateOffset(years=range_years[history_range])
        ]

    st.markdown(
        f'<div class="global-valuation-history-title"><strong>{escape(selected_name)} 市盈率（PE）</strong>'
        '<span>蓝线为PE，虚线为历史中位数及20%/80%分位</span></div>',
        unsafe_allow_html=True,
    )
    history_fig = go.Figure(go.Scatter(
        x=history["date"], y=history["pe"], mode="lines",
        line=dict(color="#2563eb", width=2.2), name="PE",
        hovertemplate="%{x|%Y-%m}<br>PE %{y:.2f}<extra></extra>",
    ))
    line_specs = (
        (selected.get("median_pe"), "历史中位数", "#475569", "dash"),
        (selected.get("p20"), "20%分位", "#16a34a", "dot"),
        (selected.get("p80"), "80%分位", "#dc2626", "dot"),
    )
    for value, label, color, dash in line_specs:
        if value is not None:
            history_fig.add_hline(
                y=float(value), line_color=color, line_dash=dash, line_width=1.3,
                annotation_text=f"{label} {float(value):.2f}", annotation_position="top left",
            )
    history_fig.update_layout(
        height=410, margin=dict(l=20, r=25, t=26, b=25),
        paper_bgcolor="white", plot_bgcolor="white", showlegend=False,
        xaxis=dict(title=None, showgrid=False),
        yaxis=dict(title="PE", showgrid=True, gridcolor="#e7edf4"),
        font=dict(color="#475569", size=12),
    )
    st.plotly_chart(history_fig, width="stretch")


_inject_etf_lab_style()


# --- 页面逻辑 ---
render_option_page_title("ETF期权")

view_options = ["总览", "持仓防线", "股市估值"]
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
    if active_view != "股市估值":
        target = st.selectbox(
            "标的",
            ["510300 (300ETF)", "510050 (50ETF)", "510500 (500ETF)", "588000 (科创50ETF)", "159915 (创业板ETF)"],
            format_func=lambda value: value.replace(" (", " ").replace(")", ""),
            label_visibility="collapsed",
            key="etf_option_symbol",
        )

if active_view == "股市估值":
    valuation_cache_version = get_global_index_valuation_cache_version(de.engine)
    _render_global_valuation_dashboard(
        _cached_global_index_valuation_dashboard(valuation_cache_version)
    )
    _perf_page_log(
        page=PAGE_NAME,
        render_ms=(time.perf_counter() - _PAGE_T0) * 1000,
        cache_hit=-1,
        stage="global_valuation_done",
    )
    st.stop()

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

if active_view == "持仓防线":
    _render_defense_detail_table(df)

_perf_page_log(
    page=PAGE_NAME,
    render_ms=(time.perf_counter() - _PAGE_T0) * 1000,
    cache_hit=-1,
    stage="page_done",
)
