from __future__ import annotations

import datetime as dt
import importlib
import json
import math
import sys
from html import escape
from pathlib import Path
from string import Template
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components
from plotly.subplots import make_subplots


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

CHART_RENDER_WINDOW = 260
TODAY_LINE_COLOR = "#2563eb"
PREVIOUS_LINE_COLOR = "#f97316"

import us_market_dashboard_data as dashboard_data
from sidebar_navigation import show_navigation
from ui_components import inject_sidebar_toggle_style

dashboard_data = importlib.reload(dashboard_data)

from us_market_dashboard_data import (
    DEFAULT_DASHBOARD_UNDERLYINGS,
    UNDERLYING_DISPLAY_NAMES,
    build_otm_volatility_curve,
    build_volatility_cone_line,
    calculate_atm_iv_pct,
    calculate_overview_metrics_from_market_history,
    calculate_volatility_positioning_metrics,
    dashboard_engine,
    load_available_option_trade_dates,
    load_iv_history,
    load_oi_defense_history,
    load_latest_option_trade_date,
    load_market_climate_strip,
    load_market_metrics_history,
    load_option_chain_daily,
    load_option_chain_summary,
    load_option_surface_snapshot,
    load_stock_daily,
    load_volatility_cone_history,
    oi_defense_y_axis_range,
    selected_underlying_price,
    summarize_option_chain,
)
from us_options_polygon import default_trade_date


st.set_page_config(
    page_title="美股期权",
    page_icon="favicon.ico",
    layout="wide",
    initial_sidebar_state="expanded",
)


def _load_global_css() -> None:
    css_path = ROOT_DIR / "style.css"
    if css_path.exists():
        st.markdown(f"<style>{css_path.read_text(encoding='utf-8', errors='ignore')}</style>", unsafe_allow_html=True)


def _inject_page_style() -> None:
    st.markdown(
        """
        <style>
        #MainMenu,
        footer,
        [data-testid="stDecoration"] {
            display: none !important;
        }
        [data-testid="stHeader"] {
            background: transparent !important;
            pointer-events: auto !important;
        }
        [data-testid="stHeader"] > div {
            background: transparent !important;
        }
        [data-testid="stAppViewContainer"] > .main .block-container,
        .block-container {
            padding-top: 0.5rem !important;
            padding-bottom: 1.4rem !important;
            max-width: 100% !important;
        }
        .us-lab-page-head {
            display: grid;
            grid-template-columns: minmax(280px, 1fr) auto minmax(190px, 240px);
            align-items: center;
            gap: 18px;
            padding: 16px 0 14px;
            border-bottom: 1px solid #e2e8f0;
            margin-bottom: 14px;
        }
        .us-lab-topbar {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 18px;
            padding: 2px 2px 12px;
            border-bottom: 1px solid #e2e8f0;
            margin-bottom: 12px;
        }
        .us-lab-brand {
            display: flex;
            align-items: center;
            gap: 10px;
            min-width: 260px;
            margin: 0 0 10px !important;
        }
        .us-lab-mark {
            width: 30px;
            height: 30px;
            border-radius: 8px;
            background: linear-gradient(135deg, #2563eb 0%, #14b8a6 100%);
            box-shadow: inset 0 0 0 1px rgba(255,255,255,.42);
        }
        .us-lab-title {
            margin: 0;
            color: #0f172a;
            font-size: 27px !important;
            line-height: 1.1;
            font-weight: 760;
            letter-spacing: 0;
        }
        .us-lab-subtitle {
            margin-top: 3px;
            color: #64748b;
            font-size: 12px;
            line-height: 1.25;
        }
        .us-lab-sync {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 7px 11px;
            border-radius: 8px;
            border: 1px solid #bbf7d0;
            background: #f0fdf4;
            color: #15803d;
            font-size: 13px;
            font-weight: 650;
            white-space: nowrap;
        }
        .us-lab-source-line {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 12px;
            color: #64748b;
            font-size: 12px;
            line-height: 1.35;
            margin: -4px 0 10px;
        }
        .us-lab-source-pill {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            border: 1px solid #dbe3ef;
            background: #ffffff;
            border-radius: 8px;
            padding: 7px 10px;
            color: #334155;
            white-space: nowrap;
        }
        .us-lab-control-band {
            display: none;
        }
        .us-lab-kpi-strip {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(132px, 1fr));
            gap: 8px;
            border: 0;
            border-radius: 0;
            background: transparent;
            overflow: visible;
            margin: 8px 0 14px;
        }
        .us-lab-kpi {
            min-height: 76px;
            padding: 13px 14px 11px;
            border: 1px solid #e2e8f0;
            border-radius: 8px;
            background: #ffffff;
            box-shadow: 0 1px 2px rgba(15, 23, 42, .03);
        }
        .us-lab-kpi:last-child {
            border-right: 1px solid #e2e8f0;
        }
        .us-lab-kpi-label {
            color: #64758b;
            font-size: 12px;
            line-height: 1.2;
            white-space: nowrap;
        }
        .us-lab-kpi-label-row {
            display: flex;
            align-items: center;
            gap: 5px;
            margin-bottom: 7px;
            min-width: 0;
        }
        .us-lab-kpi-info {
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
        .us-lab-kpi-tooltip {
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
        .us-lab-kpi-info:hover .us-lab-kpi-tooltip,
        .us-lab-kpi-info:focus .us-lab-kpi-tooltip {
            opacity: 1;
            visibility: visible;
        }
        .us-lab-kpi:nth-child(n+6) .us-lab-kpi-tooltip {
            left: auto;
            right: 0;
            transform: translateX(6px);
        }
        .us-lab-kpi-value {
            color: #0f172a;
            font-size: 21px;
            line-height: 1.05;
            font-weight: 780;
            letter-spacing: 0;
            white-space: nowrap;
        }
        .us-lab-kpi-detail {
            color: #708199;
            font-size: 11px;
            line-height: 1.25;
            margin-top: 6px;
            white-space: nowrap;
        }
        .us-lab-tab-divider {
            height: 1px;
            background: #dbe3ef;
            margin: 10px 0 18px;
        }
        div[data-testid="stSegmentedControl"] {
            display: inline-flex;
            width: auto;
        }
        div[data-testid="stSegmentedControl"] > div {
            background: #ffffff;
            border: 1px solid #dbe3ef;
            border-radius: 8px;
            padding: 4px;
            gap: 2px;
            box-shadow: 0 1px 2px rgba(15, 23, 42, .04);
        }
        div[data-testid="stSegmentedControl"] button {
            min-height: 34px;
            border-radius: 6px !important;
            color: #475569 !important;
            font-weight: 700 !important;
        }
        div[data-testid="stSegmentedControl"] button[aria-pressed="true"] {
            background: #eff6ff !important;
            color: #2563eb !important;
            box-shadow: inset 0 -2px 0 #ff4b4b;
        }
        div[data-testid="stSelectbox"] [data-baseweb="select"] {
            min-height: 42px;
            border-radius: 8px;
            border-color: #dbe3ef;
            background: #ffffff;
            box-shadow: 0 1px 2px rgba(15, 23, 42, .04);
        }
        div[data-testid="stSelectbox"] [data-baseweb="select"] > div {
            font-weight: 700;
            color: #0f172a;
        }
        div[data-testid="column"]:has(div[data-testid="stSelectbox"]) {
            display: flex;
            justify-content: flex-end;
            align-items: flex-start;
        }
        div[data-testid="column"]:has(div[data-testid="stSelectbox"]) > div {
            width: min(280px, 100%);
        }
        .us-lab-panel {
            border: 1px solid #e2e8f0;
            border-radius: 8px;
            background: #ffffff;
            padding: 14px 14px 12px;
            margin-bottom: 12px;
        }
        .us-lab-panel-title {
            display: flex;
            align-items: baseline;
            justify-content: space-between;
            gap: 10px;
            margin-bottom: 8px;
        }
        .us-lab-panel-title strong {
            color: #0f172a;
            font-size: 15px;
            line-height: 1.2;
            font-weight: 720;
        }
        .us-lab-panel-title span {
            color: #64748b;
            font-size: 12px;
            line-height: 1.2;
        }
        .us-lab-rail {
            border: 1px solid #e2e8f0;
            border-radius: 8px;
            background: #ffffff;
            padding: 18px 18px 14px;
            min-height: 626px;
        }
        .us-lab-rail .us-lab-panel-title {
            align-items: center;
            margin-bottom: 14px;
        }
        .us-lab-rail .us-lab-panel-title strong {
            font-size: 18px;
            font-weight: 780;
        }
        .us-lab-rail-updated {
            display: inline-flex;
            align-items: center;
            gap: 7px;
            color: #64748b;
            font-size: 12px;
            white-space: nowrap;
        }
        .us-lab-rail-updated::after {
            content: "";
            width: 7px;
            height: 7px;
            border-radius: 999px;
            background: #10b981;
            box-shadow: 0 0 0 3px rgba(16,185,129,.12);
        }
        .us-lab-rail-section {
            padding: 14px 0;
            border-bottom: 1px solid #e2e8f0;
        }
        .us-lab-rail-section:first-child {
            padding-top: 0;
        }
        .us-lab-rail-section:last-child {
            border-bottom: 0;
            padding-bottom: 0;
        }
        .us-lab-rail-label {
            color: #64748b;
            font-size: 12px;
            line-height: 1.2;
            margin-bottom: 7px;
        }
        .us-lab-rail-value {
            color: #0f172a;
            font-size: 26px;
            line-height: 1.05;
            font-weight: 780;
        }
        .us-lab-rail-section-title {
            color: #0f172a;
            font-size: 12px;
            line-height: 1.2;
            font-weight: 760;
            margin: 0 0 10px;
        }
        .us-lab-rail-metric {
            min-width: 0;
        }
        .us-lab-rail-metric-value {
            color: #0f172a;
            font-size: 18px;
            line-height: 1.05;
            font-weight: 780;
            white-space: nowrap;
        }
        .us-lab-rail-metric-detail {
            color: #64748b;
            font-size: 11px;
            line-height: 1.25;
            margin-top: 5px;
        }
        .us-lab-rail-status {
            display: inline-flex;
            align-items: center;
            border: 1px solid #dbe3ef;
            border-radius: 999px;
            padding: 4px 8px;
            color: #334155;
            background: #f8fafc;
            font-size: 11px;
            line-height: 1;
            font-weight: 700;
            white-space: nowrap;
        }
        .us-lab-rail-empty {
            color: #64748b;
            font-size: 12px;
            line-height: 1.35;
            border: 1px dashed #cbd5e1;
            border-radius: 8px;
            padding: 9px 10px;
            background: #f8fafc;
        }
        .us-lab-rail-grid {
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 10px 14px;
        }
        .us-lab-ledger {
            display: grid;
            gap: 10px;
        }
        .us-lab-ledger-row {
            display: grid;
            grid-template-columns: minmax(118px, 1fr) minmax(92px, auto) minmax(82px, auto) 44px;
            align-items: center;
            gap: 14px;
            min-height: 104px;
            padding: 14px 14px;
            border: 1px solid #e2e8f0;
            border-radius: 8px;
            background: #fbfcfe;
            box-shadow: 0 1px 2px rgba(15, 23, 42, .035);
        }
        .us-lab-ledger-row.hot {
            border-color: #e2e8f0;
            background: #fbfcfe;
        }
        .us-lab-ledger-row.compact {
            min-height: 104px;
        }
        .us-lab-ledger-row.wide {
            grid-template-columns: minmax(132px, 1fr) minmax(92px, auto) minmax(82px, auto) 44px;
            min-height: 118px;
        }
        .us-lab-ledger-main {
            min-width: 0;
        }
        .us-lab-ledger-label {
            color: #111827;
            font-size: 14px;
            line-height: 1.2;
            font-weight: 760;
            white-space: nowrap;
        }
        .us-lab-ledger-sub {
            color: #64758b;
            font-size: 11px;
            line-height: 1.25;
            margin-top: 7px;
        }
        .us-lab-ledger-extra {
            display: flex;
            flex-wrap: wrap;
            gap: 6px;
            margin-top: 8px;
            max-width: 150px;
        }
        .us-lab-mini-chip {
            display: inline-flex;
            align-items: baseline;
            gap: 4px;
            max-width: 100%;
            border: 1px solid #dbe3ef;
            border-radius: 999px;
            background: #ffffff;
            padding: 4px 7px;
            color: #64758b;
            font-size: 10px;
            line-height: 1;
            white-space: nowrap;
        }
        .us-lab-mini-chip strong {
            color: #0f172a;
            font-size: 11px;
            font-weight: 760;
        }
        .us-lab-mini-chip.red strong {
            color: #dc2626;
        }
        .us-lab-mini-chip.blue strong {
            color: #2563eb;
        }
        .us-lab-mini-chip.orange strong {
            color: #ea580c;
        }
        .us-lab-ledger-value {
            color: #0f172a;
            font-size: 25px;
            line-height: 1;
            font-weight: 800;
            letter-spacing: 0;
            white-space: nowrap;
            text-align: right;
            min-width: 0;
        }
        .us-lab-ledger-value.small {
            font-size: 22px;
        }
        .us-lab-ledger-pct {
            color: #64758b;
            font-size: 11px;
            line-height: 1.2;
            min-width: 0;
            text-align: left;
        }
        .us-lab-ledger-pct strong {
            display: block;
            color: #0f172a;
            font-size: 18px;
            line-height: 1.05;
            margin-top: 6px;
            white-space: nowrap;
            font-weight: 800;
        }
        .us-lab-ledger-pct.insufficient strong {
            font-size: 17px;
            color: #64748b !important;
        }
        .us-lab-ledger-pct em {
            display: block;
            margin-top: 5px;
            color: #94a3b8;
            font-size: 10px;
            font-style: normal;
            white-space: nowrap;
        }
        .us-lab-thermo-wrap {
            display: grid;
            grid-template-columns: 20px 16px;
            align-items: center;
            gap: 8px;
            width: 44px;
        }
        .us-lab-thermo {
            position: relative;
            width: 16px;
            height: 70px;
            border-radius: 999px;
            background: #eef2f7;
            box-shadow: inset 0 0 0 1px #d6e0ee;
            overflow: hidden;
        }
        .us-lab-thermo > span {
            position: absolute;
            left: 3px;
            right: 3px;
            bottom: 4px;
            min-height: 4px;
            border-radius: 999px;
            background: #2563eb;
        }
        .us-lab-thermo-labels {
            display: flex;
            height: 70px;
            flex-direction: column;
            justify-content: space-between;
            color: #64748b;
            font-size: 10px;
            line-height: 1;
            text-align: left;
        }
        .us-lab-section-heading {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 12px;
            margin-bottom: 12px;
            color: #0f172a;
            font-size: 14px;
            font-weight: 780;
            line-height: 1.2;
        }
        .us-lab-mini-grid {
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 12px;
        }
        .us-lab-mini-metric {
            min-width: 0;
        }
        .us-lab-mini-label {
            color: #64748b;
            font-size: 12px;
            line-height: 1.2;
            margin-bottom: 7px;
            white-space: nowrap;
        }
        .us-lab-mini-value {
            color: #0f172a;
            font-size: 22px;
            line-height: 1.05;
            font-weight: 780;
            white-space: nowrap;
        }
        .us-lab-mini-detail {
            color: #64748b;
            font-size: 11px;
            line-height: 1.25;
            margin-top: 6px;
        }
        .us-lab-position-row {
            display: grid;
            grid-template-columns: minmax(0, 1fr) minmax(96px, 1fr);
            gap: 12px;
            padding: 11px 0;
            border-top: 1px dashed #dbe3ef;
        }
        .us-lab-position-row:first-child {
            border-top: 0;
            padding-top: 0;
        }
        .us-lab-inline-meter {
            height: 7px;
            border-radius: 999px;
            background: #e8eef6;
            overflow: hidden;
            margin-top: 9px;
        }
        .us-lab-inline-meter span {
            display: block;
            height: 100%;
            border-radius: 999px;
            background: #10b981;
        }
        .us-lab-progress {
            height: 7px;
            border-radius: 999px;
            background: #e2e8f0;
            overflow: hidden;
            margin: 8px 0 4px;
        }
        .us-lab-progress span {
            display: block;
            height: 100%;
            border-radius: 999px;
            background: #2563eb;
        }
        .us-lab-note {
            border-left: 3px solid #2563eb;
            background: #eff6ff;
            color: #1e3a8a;
            padding: 10px 12px;
            border-radius: 8px;
            margin: 8px 0 12px;
            font-size: 13px;
            line-height: 1.5;
        }
        .us-lab-warning {
            border-left-color: #f59e0b;
            background: #fffbeb;
            color: #92400e;
        }
        .us-lab-code {
            font-family: Consolas, "IBM Plex Mono", monospace;
            font-size: 12px;
            background: #0f172a;
            color: #e2e8f0;
            border-radius: 8px;
            padding: 10px 12px;
            overflow-x: auto;
        }
        .stTabs [data-baseweb="tab-list"] {
            gap: 6px;
            border-bottom: 1px solid #dbe3ef;
        }
        .stTabs [data-baseweb="tab"] {
            border-radius: 8px 8px 0 0;
            padding: 8px 12px;
        }
        .stTabs [aria-selected="true"] {
            background: #eff6ff;
            color: #1d4ed8;
        }
        @media (max-width: 1200px) {
            .us-lab-kpi-strip {
                grid-template-columns: repeat(3, minmax(0, 1fr));
            }
            .us-lab-kpi {
                border-bottom: 1px solid #e2e8f0;
            }
            .us-lab-kpi:nth-child(3n) .us-lab-kpi-tooltip {
                left: auto;
                right: 0;
                transform: translateX(6px);
            }
        }
        div.us-lab-control-band {
            display: none !important;
            height: 0 !important;
            padding: 0 !important;
            margin: 0 !important;
            border: 0 !important;
            overflow: hidden !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _inject_home_sidebar_button_style() -> None:
    st.markdown(
        """
        <style>
        button[data-testid="stExpandSidebarButton"],
        [data-testid="stSidebarHeader"] button[data-testid="stBaseButton-headerNoPadding"] {
            width: 36px !important;
            height: 36px !important;
            min-width: 36px !important;
            min-height: 36px !important;
            background: #2563eb !important;
            border: 2px solid rgba(255, 255, 255, 0.9) !important;
            border-radius: 10px !important;
            box-shadow: 0 6px 16px rgba(2, 6, 23, 0.65) !important;
            opacity: 1 !important;
            display: flex !important;
            align-items: center !important;
            justify-content: center !important;
            pointer-events: auto !important;
        }

        button[data-testid="stExpandSidebarButton"] {
            position: fixed !important;
            top: 14px !important;
            left: 14px !important;
            z-index: 999997 !important;
        }

        button[data-testid="stExpandSidebarButton"] *,
        [data-testid="stSidebarHeader"] button[data-testid="stBaseButton-headerNoPadding"] * {
            color: #ffffff !important;
            fill: #ffffff !important;
            stroke: #ffffff !important;
            opacity: 1 !important;
            text-shadow: 0 1px 2px rgba(0,0,0,0.55) !important;
        }

        button[data-testid="stExpandSidebarButton"],
        [data-testid="stSidebarHeader"] button[data-testid="stBaseButton-headerNoPadding"] {
            font-size: 0 !important;
            color: transparent !important;
        }

        button[data-testid="stExpandSidebarButton"] span,
        [data-testid="stSidebarHeader"] button[data-testid="stBaseButton-headerNoPadding"] span,
        button[data-testid="stExpandSidebarButton"] i,
        [data-testid="stSidebarHeader"] button[data-testid="stBaseButton-headerNoPadding"] i {
            font-family: "Material Symbols Rounded", "Material Symbols Outlined", "Material Icons", sans-serif !important;
            font-size: 20px !important;
            line-height: 20px !important;
            font-weight: 800 !important;
            letter-spacing: normal !important;
            text-transform: none !important;
            white-space: nowrap !important;
        }

        button[data-testid="stExpandSidebarButton"] svg,
        [data-testid="stSidebarHeader"] button[data-testid="stBaseButton-headerNoPadding"] svg {
            width: 20px !important;
            height: 20px !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _fmt_number(value: Any, digits: int = 2, suffix: str = "") -> str:
    if value is None:
        return "-"
    try:
        numeric = float(value)
    except Exception:
        return "-"
    if math.isnan(numeric):
        return "-"
    return f"{numeric:,.{digits}f}{suffix}"


def _fmt_int(value: Any) -> str:
    if value is None:
        return "-"
    try:
        numeric = float(value)
    except Exception:
        return "-"
    if math.isnan(numeric):
        return "-"
    return f"{int(numeric):,}"


def _fmt_pct(value: Any, digits: int = 1) -> str:
    return _fmt_number(value, digits, "%")


def _fmt_rank_pct(value: Any, digits: int = 1) -> str:
    return _fmt_number(value, digits, "%")


def _fmt_signed(value: Any, digits: int = 1, suffix: str = "") -> str:
    if value is None:
        return "-"
    try:
        numeric = float(value)
    except Exception:
        return "-"
    if math.isnan(numeric):
        return "-"
    return f"{numeric:+,.{digits}f}{suffix}"


def _fmt_signed_pct(value: Any, digits: int = 1) -> str:
    return _fmt_signed(value, digits, "%")


def _fmt_strike(value: Any) -> str:
    if value is None:
        return "-"
    try:
        numeric = float(value)
    except Exception:
        return "-"
    if math.isnan(numeric):
        return "-"
    if numeric.is_integer():
        return f"{int(numeric):,}"
    return f"{numeric:,.2f}"


def _clamp_percentile(value: Any) -> float | None:
    try:
        numeric = float(value)
    except Exception:
        return None
    if math.isnan(numeric):
        return None
    return max(0.0, min(100.0, numeric))


def _percentile_color(value: Any) -> str:
    pct = _clamp_percentile(value)
    if pct is None:
        return "#94a3b8"
    if pct >= 70:
        return "#dc2626"
    if pct <= 30:
        return "#2563eb"
    return "#0f172a"


def _thermo_html(value: Any) -> str:
    pct = _clamp_percentile(value)
    if pct is None:
        fill = 0.0
        color = "#cbd5e1"
    else:
        fill = pct
        color = _percentile_color(pct)
    return (
        '<div class="us-lab-thermo-wrap">'
        '<div class="us-lab-thermo">'
        f'<span style="height:{fill:.1f}%; background:{escape(color)}"></span>'
        "</div>"
        '<div class="us-lab-thermo-labels"><span>高</span><span>中</span><span>低</span></div>'
        "</div>"
    )


def _inline_meter_html(value: Any, color: str = "#10b981") -> str:
    pct = _clamp_percentile(value) or 0.0
    return f'<div class="us-lab-inline-meter"><span style="width:{pct:.1f}%; background:{escape(color)}"></span></div>'


def _mini_chip_html(label: str, value: str, tone: str = "") -> str:
    class_name = f"us-lab-mini-chip {tone}".strip()
    return f'<span class="{escape(class_name)}">{escape(label)} <strong>{escape(value)}</strong></span>'


def _rail_card_html(
    *,
    title: str,
    sub: str,
    value: str,
    pct_label: str,
    pct_value: Any,
    color: str,
    tone: str = "",
    history_count: Any = None,
    min_samples: Any = None,
    extra_html: str = "",
) -> str:
    row_class = f"us-lab-ledger-row {tone}".strip()
    sample_count = None
    try:
        if history_count is not None:
            sample_count = int(history_count)
    except Exception:
        sample_count = None
    sample_floor = None
    try:
        if min_samples is not None:
            sample_floor = int(min_samples)
    except Exception:
        sample_floor = None
    insufficient = (
        pct_value is None
        and sample_count is not None
        and sample_floor is not None
        and sample_count < sample_floor
    )
    pct_display = "样本不足" if insufficient else _fmt_pct(pct_value, 0)
    pct_detail = f"<em>{sample_count}/{sample_floor}</em>" if insufficient else ""
    pct_class = "us-lab-ledger-pct insufficient" if insufficient else "us-lab-ledger-pct"
    thermo_value = None if insufficient else pct_value
    pct_color = "#64748b" if insufficient else _percentile_color(pct_value)
    return f"""
        <div class="{escape(row_class)}">
            <div class="us-lab-ledger-main">
                <div class="us-lab-ledger-label">{escape(title)}</div>
                <div class="us-lab-ledger-sub">{escape(sub)}</div>
                {extra_html}
            </div>
            <div class="us-lab-ledger-value" style="color:{escape(color)}">{escape(value)}</div>
            <div class="{escape(pct_class)}">{escape(pct_label)}<strong style="color:{escape(pct_color)}">{escape(pct_display)}</strong>{pct_detail}</div>
            {_thermo_html(thermo_value)}
        </div>
    """


def _compact_html_fragment(markup: str) -> str:
    return "".join(line.strip() for line in markup.splitlines() if line.strip())


def _format_trade_date(value: str | dt.date | dt.datetime | None) -> str:
    raw = str(value or "").replace("-", "").replace("/", "")[:8]
    if len(raw) != 8:
        return "-"
    return f"{raw[:4]}/{raw[4:6]}/{raw[6:8]}"


def _underlying_option_label(symbol: str) -> str:
    code = str(symbol or "").upper()
    name = UNDERLYING_DISPLAY_NAMES.get(code)
    return f"{code}  {name}" if name else code


def _clean_float(value: Any) -> float | None:
    try:
        numeric = float(value)
    except Exception:
        return None
    if math.isnan(numeric):
        return None
    return numeric


def _pct(value: float, total: float) -> float:
    if total <= 0:
        return 0.0
    return max(0.0, min(100.0, value / total * 100.0))


def _weighted_average(values: pd.Series, weights: pd.Series | None = None) -> float | None:
    clean = pd.to_numeric(values, errors="coerce")
    if weights is not None:
        clean_weights = pd.to_numeric(weights, errors="coerce").fillna(0)
        valid = clean.notna() & (clean_weights > 0)
        if valid.any() and float(clean_weights[valid].sum()) > 0:
            return float((clean[valid] * clean_weights[valid]).sum() / clean_weights[valid].sum())
    valid_values = clean.dropna()
    if valid_values.empty:
        return None
    return float(valid_values.mean())


def _latest_change(stock_df: pd.DataFrame) -> tuple[float | None, float | None]:
    if stock_df is None or len(stock_df) < 2:
        return None, None
    close = pd.to_numeric(stock_df["close"], errors="coerce").dropna()
    if len(close) < 2:
        return None, None
    change = float(close.iloc[-1] - close.iloc[-2])
    change_pct = change / float(close.iloc[-2]) * 100 if float(close.iloc[-2]) else None
    return change, change_pct


def _atm_iv_pct(chain_df: pd.DataFrame, underlying_price: float | None) -> float | None:
    if chain_df is None or chain_df.empty or underlying_price is None:
        return None
    df = chain_df.copy()
    df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
    df["iv_pct"] = pd.to_numeric(df["iv_pct"], errors="coerce")
    df["open_interest"] = pd.to_numeric(df["open_interest"], errors="coerce")
    df = df.dropna(subset=["strike", "iv_pct"])
    if df.empty:
        return None
    df["distance"] = (df["strike"] - float(underlying_price)).abs()
    near = df.sort_values(["distance", "expiration_date"]).head(24)
    return _weighted_average(near["iv_pct"], near.get("open_interest"))


def _put_call_ratio(chain_df: pd.DataFrame) -> float | None:
    if chain_df is None or chain_df.empty:
        return None
    oi = pd.to_numeric(chain_df.get("open_interest"), errors="coerce").fillna(0)
    call_oi = float(oi[chain_df["call_put"] == "C"].sum())
    put_oi = float(oi[chain_df["call_put"] == "P"].sum())
    if call_oi <= 0:
        return None
    return put_oi / call_oi


def _kpi_html(label: str, value: str, detail: str = "", color: str = "#0f172a", hint: str = "") -> str:
    info_html = ""
    if hint:
        info_html = (
            '<span class="us-lab-kpi-info" tabindex="0" aria-label="数据说明">'
            "i"
            f'<span class="us-lab-kpi-tooltip">{escape(hint)}</span>'
            "</span>"
        )
    return (
        '<div class="us-lab-kpi">'
        '<div class="us-lab-kpi-label-row">'
        f'<span class="us-lab-kpi-label">{escape(label)}</span>'
        f"{info_html}"
        "</div>"
        f'<div class="us-lab-kpi-value" style="color:{escape(color)}">{escape(value)}</div>'
        f'<div class="us-lab-kpi-detail">{escape(detail)}</div>'
        "</div>"
    )


def _render_kpi_strip(rows: list[tuple[str, str, str, str, str]]) -> None:
    st.markdown(
        '<div class="us-lab-kpi-strip">' + "".join(_kpi_html(*row) for row in rows) + "</div>",
        unsafe_allow_html=True,
    )


def _empty_command_hint(symbol: str, trade_date: str) -> None:
    command = (
        "python update_us_options_polygon.py "
        f"--mode live-test --underlyings {symbol} --date {trade_date} --use-test-tables"
    )
    st.markdown(
        (
            '<div class="us-lab-note us-lab-warning">'
            "这一天暂无期权数据。可以先用 Options Starter 写入测试表，页面会继续只读数据库，不会在 UI 内触发 API。"
            "</div>"
        ),
        unsafe_allow_html=True,
    )
    st.markdown(f'<div class="us-lab-code">{escape(command)}</div>', unsafe_allow_html=True)


def _prepare_iv_series(iv_history: pd.DataFrame, trade_date: str, current_iv_pct: float | None) -> pd.DataFrame:
    if iv_history is not None and not iv_history.empty:
        out = iv_history.copy()
        out["date"] = pd.to_datetime(out["trade_date"], format="%Y%m%d", errors="coerce")
        out["iv_pct"] = pd.to_numeric(out["iv_pct"], errors="coerce")
        out = out.dropna(subset=["date", "iv_pct"])
        if not out.empty:
            return out[["date", "iv_pct"]].sort_values("date")
    if current_iv_pct is None:
        return pd.DataFrame(columns=["date", "iv_pct"])
    return pd.DataFrame(
        [{"date": pd.to_datetime(trade_date, format="%Y%m%d", errors="coerce"), "iv_pct": current_iv_pct}]
    )


def _iv_history_from_market_metrics(market_metrics_history: pd.DataFrame) -> pd.DataFrame:
    columns = ["trade_date", "iv", "iv_pct", "source_rows", "provider_rows", "computed_rows"]
    if (
        market_metrics_history is None
        or market_metrics_history.empty
        or "trade_date" not in market_metrics_history.columns
        or "atm_iv_pct" not in market_metrics_history.columns
    ):
        return pd.DataFrame(columns=columns)

    source = market_metrics_history.copy()
    out = pd.DataFrame()
    out["trade_date"] = source["trade_date"].astype(str).str.replace("-", "", regex=False).str[:8]
    out["iv_pct"] = pd.to_numeric(source["atm_iv_pct"], errors="coerce")
    out = out.dropna(subset=["trade_date", "iv_pct"])
    if out.empty:
        return pd.DataFrame(columns=columns)

    out["iv"] = out["iv_pct"] / 100
    out["source_rows"] = 0
    if "provider_iv_rows" in source.columns:
        out["provider_rows"] = pd.to_numeric(source["provider_iv_rows"], errors="coerce").fillna(0).astype(int)
    else:
        out["provider_rows"] = 0
    if "computed_iv_rows" in source.columns:
        out["computed_rows"] = pd.to_numeric(source["computed_iv_rows"], errors="coerce").fillna(0).astype(int)
    else:
        out["computed_rows"] = 0
    return out[columns].sort_values("trade_date").reset_index(drop=True)


def _compact_trade_date_value(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, (dt.date, dt.datetime, pd.Timestamp)):
        return pd.to_datetime(value).strftime("%Y%m%d")
    return str(value).replace("-", "")[:8]


def _latest_metric_trade_date(market_metrics_history: pd.DataFrame) -> str | None:
    if market_metrics_history is None or market_metrics_history.empty or "trade_date" not in market_metrics_history.columns:
        return None
    dates = market_metrics_history["trade_date"].apply(_compact_trade_date_value)
    dates = dates[dates.astype(bool)]
    return str(dates.max()) if not dates.empty else None


def _int_metric_value(row: pd.Series, key: str) -> int:
    value = row.get(key)
    if value is None or pd.isna(value):
        return 0
    try:
        return int(value)
    except Exception:
        return 0


def _summary_from_market_metrics(market_metrics_history: pd.DataFrame, trade_date: str) -> dict[str, int]:
    summary = {
        "rows": 0,
        "monthly": 0,
        "short_cycle": 0,
        "zero_dte": 0,
        "one_dte": 0,
        "expirations": 0,
        "provider_iv_rows": 0,
        "computed_iv_rows": 0,
        "open_interest_rows": 0,
    }
    if market_metrics_history is None or market_metrics_history.empty or "trade_date" not in market_metrics_history.columns:
        return summary

    history = market_metrics_history.copy()
    history["trade_date"] = history["trade_date"].apply(_compact_trade_date_value)
    history = history[history["trade_date"].astype(bool)].sort_values("trade_date")
    if trade_date:
        history = history[history["trade_date"] <= str(trade_date)]
    if history.empty:
        return summary

    exact = history[history["trade_date"] == str(trade_date)]
    row = exact.iloc[-1] if not exact.empty else history.iloc[-1]
    summary["monthly"] = _int_metric_value(row, "monthly_contract_count")
    summary["short_cycle"] = _int_metric_value(row, "short_cycle_contract_count")
    summary["provider_iv_rows"] = _int_metric_value(row, "provider_iv_rows")
    summary["computed_iv_rows"] = _int_metric_value(row, "computed_iv_rows")
    summary["open_interest_rows"] = _int_metric_value(row, "open_interest_rows")
    summary["rows"] = max(summary["monthly"] + summary["short_cycle"], summary["open_interest_rows"])
    return summary


def _lightweight_chart_data(
    stock_df: pd.DataFrame,
    render_window: int | None = None,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    empty_lines = {
        "ma5": pd.DataFrame(columns=["date", "MA5"]),
        "ma20": pd.DataFrame(columns=["date", "MA20"]),
        "ma60": pd.DataFrame(columns=["date", "MA60"]),
    }
    if stock_df is None or stock_df.empty:
        empty = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
        return empty, empty_lines

    df = stock_df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    for col in ("open", "high", "low", "close", "volume"):
        df[col] = pd.to_numeric(df.get(col), errors="coerce")
    df = df.dropna(subset=["date", "open", "high", "low", "close"]).sort_values("date").reset_index(drop=True)
    if df.empty:
        empty = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
        return empty, empty_lines

    df["MA5"] = df["close"].rolling(5).mean()
    df["MA20"] = df["close"].rolling(20).mean()
    df["MA60"] = df["close"].rolling(60).mean()

    candle_df = df[["date", "open", "high", "low", "close", "volume"]].copy()
    line_dfs = {
        "ma5": df[["date", "MA5"]].dropna().copy(),
        "ma20": df[["date", "MA20"]].dropna().copy(),
        "ma60": df[["date", "MA60"]].dropna().copy(),
    }
    if render_window is not None:
        window = max(int(render_window or CHART_RENDER_WINDOW), 80)
        if len(candle_df) > window:
            candle_df = candle_df.tail(window).copy()
            visible_dates = set(candle_df["date"].astype(str))
            line_dfs = {
                key: value[value["date"].astype(str).isin(visible_dates)].copy()
                for key, value in line_dfs.items()
            }
    return candle_df, line_dfs


@st.cache_data(show_spinner=False)
def _lightweight_charts_script() -> str:
    try:
        import lightweight_charts

        script_path = Path(lightweight_charts.__file__).resolve().parent / "js" / "lightweight-charts.js"
        return script_path.read_text(encoding="utf-8", errors="ignore").replace("</script>", "<\\/script>")
    except Exception:
        return ""


def _lightweight_chart_loader_html() -> str:
    chart_js = _lightweight_charts_script()
    if not chart_js:
        return ""
    return f"<script>{chart_js}</script>"


def _chart_line_records(df: pd.DataFrame, value_col: str) -> list[dict[str, Any]]:
    if df is None or df.empty or value_col not in df.columns:
        return []
    rows: list[dict[str, Any]] = []
    for _, row in df.dropna(subset=["date", value_col]).iterrows():
        value = _clean_float(row.get(value_col))
        if value is None:
            continue
        time_value = pd.to_datetime(row["date"], errors="coerce")
        if pd.isna(time_value):
            time_text = str(row["date"])
        else:
            time_text = time_value.strftime("%Y-%m-%d")
        rows.append({"time": time_text, "value": value})
    return rows


def _iv_chart_data(
    iv_history: pd.DataFrame,
    trade_date: str,
    current_iv_pct: float | None,
    *,
    start_date: str | None = None,
) -> pd.DataFrame:
    iv_series = _prepare_iv_series(iv_history, trade_date, current_iv_pct)
    if iv_series.empty:
        return pd.DataFrame(columns=["date", "IV"])
    out = iv_series.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out["IV"] = pd.to_numeric(out["iv_pct"], errors="coerce")
    out = out.dropna(subset=["date", "IV"])[["date", "IV"]]
    if start_date:
        out = out[out["date"] >= str(start_date)]
    return out


def _chart_payload(
    candle_df: pd.DataFrame,
    line_dfs: dict[str, pd.DataFrame],
    iv_df: pd.DataFrame,
    symbol: str,
) -> dict[str, Any]:
    candles: list[dict[str, Any]] = []
    volumes: list[dict[str, Any]] = []
    for _, row in candle_df.iterrows():
        open_price = _clean_float(row.get("open"))
        high = _clean_float(row.get("high"))
        low = _clean_float(row.get("low"))
        close = _clean_float(row.get("close"))
        if None in (open_price, high, low, close):
            continue
        time_value = str(row["date"])
        candles.append({"time": time_value, "open": open_price, "high": high, "low": low, "close": close})
        volume = _clean_float(row.get("volume")) or 0.0
        volumes.append(
            {
                "time": time_value,
                "value": volume,
                "color": "rgba(239, 68, 68, 0.34)" if close >= open_price else "rgba(16, 185, 129, 0.34)",
            }
        )

    latest: dict[str, Any] = {}
    if candles:
        latest = candles[-1].copy()
        if len(candles) >= 2:
            previous_close = _clean_float(candles[-2].get("close"))
            if previous_close:
                change = float(candles[-1]["close"]) - previous_close
                latest["change"] = change
                latest["change_pct"] = change / previous_close * 100

    return {
        "symbol": str(symbol or "").upper(),
        "candles": candles,
        "volumes": volumes,
        "ma5": _chart_line_records(line_dfs.get("ma5"), "MA5"),
        "ma20": _chart_line_records(line_dfs.get("ma20"), "MA20"),
        "ma60": _chart_line_records(line_dfs.get("ma60"), "MA60"),
        "iv": _chart_line_records(iv_df, "IV"),
        "latest": latest,
    }


def _render_lightweight_chart(
    stock_df: pd.DataFrame,
    iv_history: pd.DataFrame,
    symbol: str,
    trade_date: str,
    current_iv_pct: float | None,
    height: int = 650,
) -> None:
    candle_df, line_dfs = _lightweight_chart_data(stock_df, render_window=CHART_RENDER_WINDOW)
    if candle_df.empty:
        st.info("本地 stock_prices 暂无该标的日线，暂时无法渲染自研 K 线。")
        return

    chart_loader_html = _lightweight_chart_loader_html()
    if not chart_loader_html:
        st.warning("本地图表库加载失败，暂时无法渲染自研 K 线。")
        return

    chart_start_date = str(candle_df["date"].min()) if not candle_df.empty else None
    iv_df = _iv_chart_data(iv_history, trade_date, current_iv_pct, start_date=chart_start_date)
    payload = _chart_payload(candle_df, line_dfs, iv_df, symbol)
    if not payload["candles"]:
        st.info("本地 stock_prices 暂无有效 OHLC 数据，暂时无法渲染自研 K 线。")
        return

    payload_json = json.dumps(payload, ensure_ascii=False, allow_nan=False)
    chart_height = max(int(height or 650), 420)
    html = Template(
        """
        <!doctype html>
        <html lang="zh-CN">
        <head>
          <meta charset="utf-8" />
          <style>
            html, body {
              margin: 0;
              padding: 0;
              background: #ffffff;
              color: #0f172a;
              overflow: hidden;
              font-family: "Microsoft YaHei", "PingFang SC", Arial, sans-serif;
            }
            .lwc-shell {
              position: relative;
              height: ${height}px;
              min-height: ${height}px;
              border: 1px solid #e2e8f0;
              border-radius: 8px;
              background: #ffffff;
              overflow: hidden;
              box-sizing: border-box;
            }
            .lwc-head {
              position: absolute;
              left: 16px;
              right: 16px;
              top: 10px;
              z-index: 3;
              display: flex;
              align-items: center;
              justify-content: space-between;
              gap: 16px;
              pointer-events: none;
            }
            .lwc-left-head {
              display: inline-flex;
              align-items: center;
              gap: 10px;
              min-width: 0;
            }
            .lwc-title {
              display: inline-flex;
              align-items: baseline;
              gap: 9px;
              padding: 6px 9px;
              border: 1px solid rgba(226, 232, 240, .88);
              border-radius: 8px;
              background: rgba(255,255,255,.86);
              box-shadow: 0 8px 28px rgba(15,23,42,.05);
              backdrop-filter: blur(8px);
            }
            .lwc-title strong {
              font-size: 14px;
              line-height: 1;
              font-weight: 760;
            }
            .lwc-title span,
            .lwc-latest span {
              color: #64748b;
              font-size: 11px;
            }
            .lwc-latest {
              display: inline-flex;
              align-items: baseline;
              gap: 8px;
              padding: 6px 9px;
              border: 1px solid rgba(226, 232, 240, .88);
              border-radius: 8px;
              background: rgba(255,255,255,.86);
              box-shadow: 0 8px 28px rgba(15,23,42,.05);
              backdrop-filter: blur(8px);
              font-size: 12px;
              font-weight: 700;
            }
            .lwc-controls {
              display: inline-flex;
              align-items: center;
              gap: 6px;
              padding: 4px;
              border: 1px solid rgba(226, 232, 240, .88);
              border-radius: 8px;
              background: rgba(255,255,255,.86);
              box-shadow: 0 8px 28px rgba(15,23,42,.05);
              backdrop-filter: blur(8px);
              pointer-events: auto;
            }
            .lwc-tool-divider {
              width: 1px;
              height: 22px;
              margin: 0 2px;
              background: #e2e8f0;
            }
            .lwc-toggle {
              display: inline-flex;
              align-items: center;
              gap: 5px;
              height: 26px;
              padding: 0 8px;
              border: 0;
              border-radius: 6px;
              background: transparent;
              color: #64748b;
              font-size: 11px;
              font-weight: 760;
              line-height: 1;
              cursor: pointer;
              font-family: inherit;
            }
            .lwc-toggle::before {
              content: "";
              width: 7px;
              height: 7px;
              border-radius: 999px;
              background: var(--line-color, #94a3b8);
              opacity: .38;
            }
            .lwc-toggle.active {
              background: #eff6ff;
              color: #0f172a;
            }
            .lwc-toggle.active::before {
              opacity: 1;
            }
            .lwc-toggle:disabled {
              opacity: .42;
              cursor: not-allowed;
            }
            .lwc-draw-tool {
              height: 26px;
              padding: 0 8px;
              border: 0;
              border-radius: 6px;
              background: transparent;
              color: #475569;
              font-size: 11px;
              font-weight: 760;
              line-height: 1;
              cursor: pointer;
              font-family: inherit;
            }
            .lwc-draw-tool:hover,
            .lwc-draw-tool.active {
              background: #fff7ed;
              color: #ea580c;
            }
            .lwc-draw-tool.danger:hover,
            .lwc-draw-tool.danger.active {
              background: #fef2f2;
              color: #dc2626;
            }
            .lwc-readout {
              position: absolute;
              left: 16px;
              top: 52px;
              z-index: 3;
              display: none;
              flex-wrap: wrap;
              gap: 6px 10px;
              max-width: calc(100% - 32px);
              min-height: 26px;
              align-items: center;
              padding: 6px 10px;
              border: 1px solid rgba(226, 232, 240, .86);
              border-radius: 8px;
              background: rgba(255, 255, 255, .88);
              box-shadow: 0 10px 30px rgba(15, 23, 42, .05);
              backdrop-filter: blur(8px);
              color: #64748b;
              font-size: 11px;
              line-height: 1.2;
              pointer-events: none;
              box-sizing: border-box;
            }
            .lwc-readout.visible {
              display: flex;
            }
            .lwc-readout span {
              display: inline-flex;
              align-items: baseline;
              gap: 4px;
              white-space: nowrap;
            }
            .lwc-readout b {
              color: #0f172a;
              font-weight: 760;
            }
            .lwc-chart {
              position: absolute;
              inset: 0;
              background: #ffffff;
            }
            .lwc-chart.drawing-hover {
              cursor: grab;
            }
            .lwc-chart.dragging-drawing {
              cursor: grabbing;
            }
            .lwc-drawing-layer {
              position: absolute;
              inset: 0;
              z-index: 2;
              width: 100%;
              height: 100%;
              pointer-events: none;
              overflow: visible;
            }
            .lwc-drawing-layer.active {
              pointer-events: auto;
              cursor: crosshair;
            }
            .lwc-drawing-line {
              vector-effect: non-scaling-stroke;
              pointer-events: none;
            }
            .lwc-drawing-hit {
              stroke: transparent;
              stroke-width: 14;
              vector-effect: non-scaling-stroke;
              pointer-events: stroke;
              cursor: grab;
            }
            .lwc-drawing-label {
              fill: #475569;
              font-size: 11px;
              font-weight: 760;
              paint-order: stroke;
              stroke: rgba(255,255,255,.94);
              stroke-width: 4px;
              stroke-linejoin: round;
            }
            .lwc-draw-hint {
              position: absolute;
              right: 16px;
              bottom: 12px;
              z-index: 3;
              display: none;
              padding: 6px 9px;
              border: 1px solid rgba(251, 146, 60, .35);
              border-radius: 8px;
              background: rgba(255, 247, 237, .92);
              color: #9a3412;
              font-size: 11px;
              font-weight: 700;
              pointer-events: none;
              box-shadow: 0 8px 28px rgba(15,23,42,.05);
              backdrop-filter: blur(8px);
            }
            .lwc-draw-hint.visible {
              display: block;
            }
            .lwc-error {
              position: absolute;
              inset: 0;
              display: none;
              align-items: center;
              justify-content: center;
              padding: 24px;
              background: #ffffff;
              color: #475569;
              font-size: 13px;
              line-height: 1.45;
              text-align: center;
              box-sizing: border-box;
            }
          </style>
        </head>
        <body>
          <div class="lwc-shell">
            <div class="lwc-head">
              <div class="lwc-left-head">
                <div class="lwc-title"><strong id="lwc-symbol"></strong><span>日线 · 本地数据库</span></div>
                <div class="lwc-controls" aria-label="图表指标开关">
                  <button class="lwc-toggle" style="--line-color:#f59e0b" data-series="ma5" type="button">MA5</button>
                  <button class="lwc-toggle active" style="--line-color:#2563eb" data-series="ma20" type="button">MA20</button>
                  <button class="lwc-toggle" style="--line-color:#7c3aed" data-series="ma60" type="button">MA60</button>
                  <button class="lwc-toggle active" style="--line-color:#db2777" data-series="iv" type="button">ATM IV</button>
                  <span class="lwc-tool-divider"></span>
                  <button class="lwc-draw-tool" data-draw-mode="hline" type="button" title="点击图表价格位置添加水平线">水平线</button>
                  <button class="lwc-draw-tool" data-draw-mode="trend" type="button" title="点击两个位置添加趋势线">趋势线</button>
                  <button class="lwc-draw-tool danger" data-draw-mode="delete" type="button" title="点击已有画线删除">删除</button>
                  <button id="lwc-clear-drawings" class="lwc-draw-tool danger" type="button" title="清空当前标的全部本地画线">清空</button>
                </div>
              </div>
              <div class="lwc-latest"><span>最新</span><strong id="lwc-close"></strong><span id="lwc-change"></span></div>
            </div>
            <div id="lwc-readout" class="lwc-readout">移动十字光标查看每日 OHLC / 均线 / IV</div>
            <div id="lwc-chart" class="lwc-chart"></div>
            <svg id="lwc-drawing-layer" class="lwc-drawing-layer" aria-hidden="true"></svg>
            <div id="lwc-draw-hint" class="lwc-draw-hint"></div>
            <div id="lwc-error" class="lwc-error"></div>
          </div>
          ${chart_loader_html}
          <script>
          (function() {
            const payload = ${payload_json};
            const errorEl = document.getElementById("lwc-error");
            function showError(message) {
              errorEl.style.display = "flex";
              errorEl.textContent = "图表加载失败：" + message;
            }
            function fmt(value, digits) {
              if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
              return Number(value).toLocaleString("en-US", {
                minimumFractionDigits: digits,
                maximumFractionDigits: digits
              });
            }
            try {
              if (!window.LightweightCharts || !window.LightweightCharts.createChart) {
                throw new Error("本地 lightweight-charts 未正确加载");
              }
              const chartEl = document.getElementById("lwc-chart");
              const latest = payload.latest || {};
              const change = Number(latest.change || 0);
              document.getElementById("lwc-symbol").textContent = payload.symbol || "";
              document.getElementById("lwc-close").textContent = fmt(latest.close, 2);
              const changeEl = document.getElementById("lwc-change");
              changeEl.textContent = (change >= 0 ? "+" : "") + fmt(change, 2) + " (" + (change >= 0 ? "+" : "") + fmt(latest.change_pct, 2) + "%)";
              changeEl.style.color = change >= 0 ? "#dc2626" : "#059669";

              const rect = chartEl.getBoundingClientRect();
              const chart = LightweightCharts.createChart(chartEl, {
                width: Math.max(rect.width, 480),
                height: Math.max(rect.height, 420),
                layout: {
                  background: { type: LightweightCharts.ColorType.Solid, color: "#ffffff" },
                  textColor: "#334155",
                  fontSize: 12,
                  fontFamily: "Microsoft YaHei, PingFang SC, Arial, sans-serif"
                },
                grid: {
                  vertLines: { color: "rgba(148, 163, 184, 0.12)" },
                  horzLines: { color: "rgba(148, 163, 184, 0.16)" }
                },
                crosshair: {
                  mode: LightweightCharts.CrosshairMode.Normal,
                  vertLine: { color: "rgba(71, 85, 105, 0.45)", style: 2, width: 1 },
                  horzLine: { color: "rgba(71, 85, 105, 0.45)", style: 2, width: 1 }
                },
                rightPriceScale: {
                  borderVisible: false,
                  scaleMargins: { top: 0.08, bottom: 0.40 }
                },
                leftPriceScale: {
                  visible: false,
                  borderVisible: false,
                  scaleMargins: { top: 0.70, bottom: 0.15 }
                },
                timeScale: {
                  borderVisible: false,
                  rightOffset: 8,
                  barSpacing: 6,
                  minBarSpacing: 4,
                  timeVisible: true,
                  secondsVisible: false
                },
                localization: {
                  locale: "zh-CN",
                  priceFormatter: function(price) { return fmt(price, 2); }
                },
                handleScale: true,
                handleScroll: true
              });

              const candleSeries = chart.addCandlestickSeries({
                upColor: "#ef4444",
                downColor: "#10b981",
                borderUpColor: "#ef4444",
                borderDownColor: "#10b981",
                wickUpColor: "#ef4444",
                wickDownColor: "#10b981",
                priceLineColor: "#64748b",
                lastValueVisible: true
              });
              candleSeries.setData(payload.candles || []);

              const drawingLayer = document.getElementById("lwc-drawing-layer");
              const drawHintEl = document.getElementById("lwc-draw-hint");
              const clearDrawingsButton = document.getElementById("lwc-clear-drawings");
              const storageKey = "us-options-chart-drawings:" + String(payload.symbol || "UNKNOWN").toUpperCase();
              const drawingColors = { hline: "#f97316", trend: "#2563eb", draft: "#64748b" };
              let drawings = loadDrawings();
              let drawMode = "none";
              let pendingTrend = null;
              let draftPoint = null;
              let dragState = null;

              function loadDrawings() {
                try {
                  const raw = window.localStorage.getItem(storageKey);
                  const rows = raw ? JSON.parse(raw) : [];
                  if (!Array.isArray(rows)) return [];
                  return rows.filter((item) => item && (item.type === "hline" || item.type === "trend"));
                } catch (_) {
                  return [];
                }
              }
              function saveDrawings() {
                try {
                  window.localStorage.setItem(storageKey, JSON.stringify(drawings));
                } catch (_) {}
              }
              function drawingId() {
                return "d" + Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
              }
              function setDrawHint(text) {
                if (!text) {
                  drawHintEl.classList.remove("visible");
                  drawHintEl.textContent = "";
                  return;
                }
                drawHintEl.textContent = text;
                drawHintEl.classList.add("visible");
              }
              function setDrawMode(nextMode) {
                drawMode = drawMode === nextMode ? "none" : nextMode;
                pendingTrend = null;
                draftPoint = null;
                drawingLayer.classList.toggle("active", drawMode !== "none");
                document.querySelectorAll("[data-draw-mode]").forEach((button) => {
                  button.classList.toggle("active", button.dataset.drawMode === drawMode);
                });
                if (drawMode === "hline") setDrawHint("点击图表任意价格位置添加水平线；Esc 退出");
                else if (drawMode === "trend") setDrawHint("依次点击趋势线的起点和终点；Esc 退出");
                else if (drawMode === "delete") setDrawHint("点击已有画线删除；Esc 退出");
                else setDrawHint("");
                renderDrawings();
              }
              function pointFromEvent(event) {
                const box = chartEl.getBoundingClientRect();
                const x = event.clientX - box.left;
                const y = event.clientY - box.top;
                const time = chart.timeScale().coordinateToTime(x);
                const price = candleSeries.coordinateToPrice(y);
                if (!time || price === null || price === undefined || Number.isNaN(Number(price))) return null;
                return { x, y, time: timeKey(time), price: Number(price) };
              }
              function chartPointFromEvent(event) {
                const box = chartEl.getBoundingClientRect();
                return { x: event.clientX - box.left, y: event.clientY - box.top };
              }
              function createSvgLine(x1, y1, x2, y2, color, width, id, dashed) {
                const visibleLine = document.createElementNS("http://www.w3.org/2000/svg", "line");
                visibleLine.setAttribute("x1", x1);
                visibleLine.setAttribute("y1", y1);
                visibleLine.setAttribute("x2", x2);
                visibleLine.setAttribute("y2", y2);
                visibleLine.setAttribute("stroke", color);
                visibleLine.setAttribute("stroke-width", width);
                visibleLine.setAttribute("stroke-linecap", "round");
                visibleLine.setAttribute("class", "lwc-drawing-line");
                if (dashed) visibleLine.setAttribute("stroke-dasharray", "6 5");
                drawingLayer.appendChild(visibleLine);

                if (id) {
                  const hitLine = document.createElementNS("http://www.w3.org/2000/svg", "line");
                  hitLine.setAttribute("x1", x1);
                  hitLine.setAttribute("y1", y1);
                  hitLine.setAttribute("x2", x2);
                  hitLine.setAttribute("y2", y2);
                  hitLine.setAttribute("data-drawing-id", id);
                  hitLine.setAttribute("class", "lwc-drawing-hit");
                  drawingLayer.appendChild(hitLine);
                }
              }
              function createSvgLabel(text, x, y) {
                const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
                label.setAttribute("x", x);
                label.setAttribute("y", y);
                label.setAttribute("class", "lwc-drawing-label");
                label.textContent = text;
                drawingLayer.appendChild(label);
              }
              function renderDrawings() {
                drawingLayer.replaceChildren();
                const box = chartEl.getBoundingClientRect();
                const width = Math.max(box.width, 480);
                const height = Math.max(box.height, 420);
                drawingLayer.setAttribute("viewBox", "0 0 " + width + " " + height);
                drawingLayer.setAttribute("width", width);
                drawingLayer.setAttribute("height", height);

                drawings.forEach((drawing) => {
                  if (drawing.type === "hline") {
                    const y = candleSeries.priceToCoordinate(Number(drawing.price));
                    if (y === null || y === undefined || y < -40 || y > height + 40) return;
                    createSvgLine(0, y, width, y, drawing.color || drawingColors.hline, 1.6, drawing.id, false);
                    createSvgLabel(fmt(drawing.price, 2), Math.max(width - 70, 8), Math.max(y - 6, 14));
                    return;
                  }
                  if (drawing.type === "trend") {
                    const x1 = chart.timeScale().timeToCoordinate(drawing.time1);
                    const x2 = chart.timeScale().timeToCoordinate(drawing.time2);
                    const y1 = candleSeries.priceToCoordinate(Number(drawing.price1));
                    const y2 = candleSeries.priceToCoordinate(Number(drawing.price2));
                    if ([x1, x2, y1, y2].some((value) => value === null || value === undefined)) return;
                    createSvgLine(x1, y1, x2, y2, drawing.color || drawingColors.trend, 1.8, drawing.id, false);
                  }
                });

                if (pendingTrend && draftPoint) {
                  createSvgLine(pendingTrend.x, pendingTrend.y, draftPoint.x, draftPoint.y, drawingColors.draft, 1.5, null, true);
                }
              }
              function removeDrawing(id) {
                drawings = drawings.filter((item) => item.id !== id);
                saveDrawings();
                renderDrawings();
              }
              function drawingCoords(drawing) {
                if (!drawing) return null;
                if (drawing.type === "hline") {
                  const y = candleSeries.priceToCoordinate(Number(drawing.price));
                  if (y === null || y === undefined) return null;
                  return { y };
                }
                if (drawing.type === "trend") {
                  const x1 = chart.timeScale().timeToCoordinate(drawing.time1);
                  const x2 = chart.timeScale().timeToCoordinate(drawing.time2);
                  const y1 = candleSeries.priceToCoordinate(Number(drawing.price1));
                  const y2 = candleSeries.priceToCoordinate(Number(drawing.price2));
                  if ([x1, x2, y1, y2].some((value) => value === null || value === undefined)) return null;
                  return { x1, y1, x2, y2 };
                }
                return null;
              }
              function distanceToSegment(px, py, x1, y1, x2, y2) {
                const dx = x2 - x1;
                const dy = y2 - y1;
                if (dx === 0 && dy === 0) {
                  return Math.hypot(px - x1, py - y1);
                }
                const t = Math.max(0, Math.min(1, ((px - x1) * dx + (py - y1) * dy) / (dx * dx + dy * dy)));
                const projectionX = x1 + t * dx;
                const projectionY = y1 + t * dy;
                return Math.hypot(px - projectionX, py - projectionY);
              }
              function nearestDrawing(point, threshold) {
                let best = null;
                drawings.forEach((drawing) => {
                  const coords = drawingCoords(drawing);
                  if (!coords) return;
                  let distance = Infinity;
                  if (drawing.type === "hline") {
                    distance = Math.abs(point.y - coords.y);
                  } else if (drawing.type === "trend") {
                    distance = distanceToSegment(point.x, point.y, coords.x1, coords.y1, coords.x2, coords.y2);
                  }
                  if (distance <= threshold && (!best || distance < best.distance)) {
                    best = { drawing, coords, distance };
                  }
                });
                return best;
              }
              function startDrawingDrag(event, drawingId) {
                const point = chartPointFromEvent(event);
                const hit = drawingId
                  ? (() => {
                      const drawing = drawings.find((item) => item.id === drawingId);
                      const coords = drawingCoords(drawing);
                      return drawing && coords ? { drawing, coords, distance: 0 } : null;
                    })()
                  : nearestDrawing(point, 8);
                if (!hit) return false;
                event.preventDefault();
                event.stopPropagation();
                dragState = {
                  id: hit.drawing.id,
                  type: hit.drawing.type,
                  startX: point.x,
                  startY: point.y,
                  coords: { ...hit.coords },
                  drawing: { ...hit.drawing },
                };
                chartEl.classList.add("dragging-drawing");
                setDrawHint("拖动画线调整位置，松开后自动保存");
                window.addEventListener("pointermove", dragDrawing, true);
                window.addEventListener("pointerup", stopDrawingDrag, true);
                window.addEventListener("pointercancel", stopDrawingDrag, true);
                return true;
              }
              function dragDrawing(event) {
                if (!dragState) return;
                event.preventDefault();
                event.stopPropagation();
                const point = chartPointFromEvent(event);
                const dx = point.x - dragState.startX;
                const dy = point.y - dragState.startY;
                const drawing = drawings.find((item) => item.id === dragState.id);
                if (!drawing) return;
                if (dragState.type === "hline") {
                  const price = candleSeries.coordinateToPrice(dragState.coords.y + dy);
                  if (price !== null && price !== undefined && !Number.isNaN(Number(price))) {
                    drawing.price = Number(price);
                  }
                } else if (dragState.type === "trend") {
                  const time1 = chart.timeScale().coordinateToTime(dragState.coords.x1 + dx);
                  const time2 = chart.timeScale().coordinateToTime(dragState.coords.x2 + dx);
                  const price1 = candleSeries.coordinateToPrice(dragState.coords.y1 + dy);
                  const price2 = candleSeries.coordinateToPrice(dragState.coords.y2 + dy);
                  if (
                    time1 && time2 &&
                    price1 !== null && price1 !== undefined &&
                    price2 !== null && price2 !== undefined &&
                    !Number.isNaN(Number(price1)) &&
                    !Number.isNaN(Number(price2))
                  ) {
                    drawing.time1 = timeKey(time1);
                    drawing.time2 = timeKey(time2);
                    drawing.price1 = Number(price1);
                    drawing.price2 = Number(price2);
                  }
                }
                renderDrawings();
              }
              function stopDrawingDrag(event) {
                if (!dragState) return;
                event.preventDefault();
                event.stopPropagation();
                dragState = null;
                chartEl.classList.remove("dragging-drawing");
                saveDrawings();
                setDrawHint(drawMode === "hline" ? "点击图表任意价格位置添加水平线；Esc 退出"
                  : drawMode === "trend" ? "依次点击趋势线的起点和终点；Esc 退出"
                  : drawMode === "delete" ? "点击已有画线删除；Esc 退出"
                  : "");
                window.removeEventListener("pointermove", dragDrawing, true);
                window.removeEventListener("pointerup", stopDrawingDrag, true);
                window.removeEventListener("pointercancel", stopDrawingDrag, true);
              }
              function updateDrawingHover(event) {
                if (drawMode !== "none" || dragState) {
                  chartEl.classList.remove("drawing-hover");
                  return;
                }
                const hit = nearestDrawing(chartPointFromEvent(event), 8);
                chartEl.classList.toggle("drawing-hover", Boolean(hit));
              }
              document.querySelectorAll("[data-draw-mode]").forEach((button) => {
                button.addEventListener("click", () => setDrawMode(button.dataset.drawMode));
              });
              clearDrawingsButton.addEventListener("click", () => {
                if (!drawings.length) return;
                drawings = [];
                pendingTrend = null;
                draftPoint = null;
                saveDrawings();
                renderDrawings();
              });
              drawingLayer.addEventListener("pointerdown", (event) => {
                const target = event.target && event.target.closest ? event.target.closest("[data-drawing-id]") : null;
                if (drawMode === "none") {
                  if (target && target.dataset.drawingId) {
                    startDrawingDrag(event, target.dataset.drawingId);
                  }
                  return;
                }
                event.preventDefault();
                event.stopPropagation();
                if (drawMode === "delete") {
                  if (target && target.dataset.drawingId) removeDrawing(target.dataset.drawingId);
                  return;
                }
                if (target && target.dataset.drawingId && startDrawingDrag(event, target.dataset.drawingId)) {
                  return;
                }
                const point = pointFromEvent(event);
                if (!point) return;
                if (drawMode === "hline") {
                  drawings.push({
                    id: drawingId(),
                    type: "hline",
                    price: point.price,
                    color: drawingColors.hline
                  });
                  saveDrawings();
                  renderDrawings();
                  return;
                }
                if (drawMode === "trend") {
                  if (!pendingTrend) {
                    pendingTrend = point;
                    draftPoint = point;
                    setDrawHint("已选择起点，再点击一次设置终点；Esc 取消");
                    renderDrawings();
                  } else {
                    drawings.push({
                      id: drawingId(),
                      type: "trend",
                      time1: pendingTrend.time,
                      price1: pendingTrend.price,
                      time2: point.time,
                      price2: point.price,
                      color: drawingColors.trend
                    });
                    pendingTrend = null;
                    draftPoint = null;
                    setDrawHint("趋势线已添加，可继续画线；Esc 退出");
                    saveDrawings();
                    renderDrawings();
                  }
                }
              });
              drawingLayer.addEventListener("pointermove", (event) => {
                if (drawMode !== "trend" || !pendingTrend) return;
                const point = pointFromEvent(event);
                if (!point) return;
                draftPoint = point;
                renderDrawings();
              });
              window.addEventListener("keydown", (event) => {
                if (event.key === "Escape" && drawMode !== "none") setDrawMode("none");
              });
              chartEl.addEventListener("pointerdown", (event) => {
                if (drawMode !== "none") return;
                startDrawingDrag(event, null);
              }, true);
              chartEl.addEventListener("pointermove", updateDrawingHover, true);
              chartEl.addEventListener("mouseleave", () => {
                if (!dragState) chartEl.classList.remove("drawing-hover");
              });

              const volumeSeries = chart.addHistogramSeries({
                priceFormat: { type: "volume" },
                priceScaleId: "volume",
                priceLineVisible: false,
                lastValueVisible: false
              });
              volumeSeries.priceScale().applyOptions({ scaleMargins: { top: 0.86, bottom: 0.02 } });
              volumeSeries.setData(payload.volumes || []);

              const overlaySeries = {};
              const defaultVisible = { ma5: false, ma20: true, ma60: false, iv: true };
              function setSeriesVisible(item, visible) {
                item.visible = visible;
                if (item.button) item.button.classList.toggle("active", visible);
                try {
                  item.series.applyOptions({ visible: visible });
                } catch (_) {}
                item.series.setData(visible ? item.data : []);
                if (item.key === "iv") {
                  chart.applyOptions({
                    leftPriceScale: {
                      visible: visible,
                      borderVisible: false,
                      scaleMargins: { top: 0.70, bottom: 0.15 }
                    }
                  });
                }
              }
              function addToggleLine(key, title, color, priceScaleId, lineWidth) {
                const data = payload[key] || [];
                const button = document.querySelector('[data-series="' + key + '"]');
                if (!data.length) {
                  if (button) {
                    button.disabled = true;
                    button.classList.remove("active");
                  }
                  return null;
                }
                const series = chart.addLineSeries({
                  color,
                  lineWidth,
                  title,
                  priceScaleId,
                  priceLineVisible: false,
                  lastValueVisible: false
                });
                overlaySeries[key] = { key, series, data, visible: false, button };
                setSeriesVisible(overlaySeries[key], Boolean(defaultVisible[key]));
                return series;
              }
              addToggleLine("ma5", "MA5", "#f59e0b", "right", 1);
              addToggleLine("ma20", "MA20", "#2563eb", "right", 2);
              addToggleLine("ma60", "MA60", "#7c3aed", "right", 2);
              const ivSeries = addToggleLine("iv", "ATM IV", "#db2777", "left", 2);
              if (ivSeries) {
                ivSeries.priceScale().applyOptions({ scaleMargins: { top: 0.70, bottom: 0.15 } });
              }

              document.querySelectorAll(".lwc-toggle").forEach((button) => {
                const key = button.dataset.series;
                const item = overlaySeries[key];
                if (!item) return;
                button.addEventListener("click", () => {
                  setSeriesVisible(item, !item.visible);
                });
              });

              const readoutEl = document.getElementById("lwc-readout");
              function timeKey(time) {
                if (!time) return null;
                if (typeof time === "string") return time;
                if (typeof time === "object" && time.year && time.month && time.day) {
                  return String(time.year) + "-" + String(time.month).padStart(2, "0") + "-" + String(time.day).padStart(2, "0");
                }
                return String(time);
              }
              function valueMap(rows, field) {
                const map = new Map();
                (rows || []).forEach((row) => {
                  if (!row || !row.time) return;
                  map.set(String(row.time), field ? row[field] : row);
                });
                return map;
              }
              const lookup = {
                candles: valueMap(payload.candles || null),
                volume: valueMap(payload.volumes || null, "value"),
                ma5: valueMap(payload.ma5 || null, "value"),
                ma20: valueMap(payload.ma20 || null, "value"),
                ma60: valueMap(payload.ma60 || null, "value"),
                iv: valueMap(payload.iv || null, "value")
              };
              function hideReadout() {
                readoutEl.classList.remove("visible");
                readoutEl.replaceChildren();
              }
              function fmtVolume(value) {
                if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
                const number = Number(value);
                const abs = Math.abs(number);
                if (abs >= 1000000000) return fmt(number / 1000000000, 2) + "B";
                if (abs >= 1000000) return fmt(number / 1000000, 2) + "M";
                if (abs >= 1000) return fmt(number / 1000, 1) + "K";
                return fmt(number, 0);
              }
              function addReadoutItem(label, value) {
                const span = document.createElement("span");
                const strong = document.createElement("b");
                strong.textContent = label;
                span.appendChild(strong);
                span.appendChild(document.createTextNode(value));
                readoutEl.appendChild(span);
              }
              function renderReadout(time) {
                const key = timeKey(time);
                const candle = key ? lookup.candles.get(key) : null;
                readoutEl.replaceChildren();
                if (!key || !candle) {
                  hideReadout();
                  return;
                }
                readoutEl.classList.add("visible");
                addReadoutItem("日期", key);
                addReadoutItem("开", fmt(candle.open, 2));
                addReadoutItem("高", fmt(candle.high, 2));
                addReadoutItem("低", fmt(candle.low, 2));
                addReadoutItem("收", fmt(candle.close, 2));
                addReadoutItem("量", fmtVolume(lookup.volume.get(key)));
                addReadoutItem("MA5", fmt(lookup.ma5.get(key), 2));
                addReadoutItem("MA20", fmt(lookup.ma20.get(key), 2));
                addReadoutItem("MA60", fmt(lookup.ma60.get(key), 2));
                const ivValue = lookup.iv.get(key);
                addReadoutItem("ATM IV", ivValue === null || ivValue === undefined ? "-" : fmt(ivValue, 2) + "%");
              }
              let pointerInsideChart = false;
              chartEl.addEventListener("mouseenter", () => {
                pointerInsideChart = true;
              });
              chartEl.addEventListener("mouseleave", () => {
                pointerInsideChart = false;
                hideReadout();
              });
              chart.subscribeCrosshairMove((param) => {
                if (!pointerInsideChart || !param || !param.time || !param.point || param.point.x < 0 || param.point.y < 0) {
                  hideReadout();
                  return;
                }
                renderReadout(param.time);
              });
              hideReadout();

              chart.timeScale().fitContent();
              const length = (payload.candles || []).length;
              if (length > 130) {
                chart.timeScale().setVisibleLogicalRange({ from: length - 126, to: length + 6 });
              }
              renderDrawings();
              chart.timeScale().subscribeVisibleLogicalRangeChange(() => renderDrawings());

              function resize() {
                const box = chartEl.getBoundingClientRect();
                chart.resize(Math.max(box.width, 480), Math.max(box.height, 420));
                renderDrawings();
              }
              if ("ResizeObserver" in window) {
                new ResizeObserver(resize).observe(chartEl);
              } else {
                window.addEventListener("resize", resize);
              }
              requestAnimationFrame(resize);
            } catch (err) {
              showError(err && err.message ? err.message : String(err));
            }
          })();
          </script>
        </body>
        </html>
        """
    ).substitute(
        height=chart_height,
        chart_loader_html=chart_loader_html,
        payload_json=payload_json,
    )
    components.html(html, height=chart_height + 2, scrolling=False)


def _auto_option_source(symbol: str, engine) -> tuple[bool, str | None, str]:
    prod_date = load_latest_option_trade_date(symbol, use_test_tables=False, engine=engine)
    if prod_date:
        return False, prod_date, "正式表"
    test_date = load_latest_option_trade_date(symbol, use_test_tables=True, engine=engine)
    if test_date:
        return True, test_date, "测试表"
    return False, None, "正式表"


def _build_composite_figure(
    stock_df: pd.DataFrame,
    iv_history: pd.DataFrame,
    symbol: str,
    trade_date: str,
    current_iv_pct: float | None,
    put_call_ratio: float | None,
    iv_rank: dict[str, Any] | None,
) -> go.Figure:
    fig = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.035,
        row_heights=[0.65, 0.22, 0.13],
    )
    df = stock_df.copy()
    if not df.empty:
        df["ma5"] = pd.to_numeric(df["close"], errors="coerce").rolling(5).mean()
        df["ma20"] = pd.to_numeric(df["close"], errors="coerce").rolling(20).mean()
        df["ma60"] = pd.to_numeric(df["close"], errors="coerce").rolling(60).mean()
        fig.add_trace(
            go.Candlestick(
                x=df["date"],
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"],
                name="K线",
                increasing_line_color="#dc2626",
                increasing_fillcolor="#ef4444",
                decreasing_line_color="#059669",
                decreasing_fillcolor="#10b981",
            ),
            row=1,
            col=1,
        )
        for column, name, color, width in (
            ("ma5", "MA5", "#f59e0b", 1.2),
            ("ma20", "MA20", "#2563eb", 1.6),
            ("ma60", "MA60", "#7c3aed", 1.6),
        ):
            fig.add_trace(
                go.Scatter(
                    x=df["date"],
                    y=df[column],
                    mode="lines",
                    name=name,
                    line=dict(color=color, width=width),
                ),
                row=1,
                col=1,
            )
        bar_colors = ["#10b981" if c >= o else "#ef4444" for o, c in zip(df["open"], df["close"])]
        fig.add_trace(
            go.Bar(
                x=df["date"],
                y=df["volume"],
                name="成交量",
                marker_color=bar_colors,
                opacity=0.48,
            ),
            row=3,
            col=1,
        )

    iv_series = _prepare_iv_series(iv_history, trade_date, current_iv_pct)
    if not iv_series.empty:
        iv_min = float(iv_series["iv_pct"].min())
        iv_max = float(iv_series["iv_pct"].max())
        band_low = iv_min if iv_min != iv_max else max(0.0, iv_min - 1.5)
        band_high = iv_max if iv_min != iv_max else iv_max + 1.5
        fig.add_trace(
            go.Scatter(
                x=iv_series["date"],
                y=[band_high] * len(iv_series),
                mode="lines",
                line=dict(width=0),
                showlegend=False,
                hoverinfo="skip",
            ),
            row=2,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=iv_series["date"],
                y=[band_low] * len(iv_series),
                mode="lines",
                fill="tonexty",
                fillcolor="rgba(37, 99, 235, 0.08)",
                line=dict(width=0),
                name="IV区间",
                hoverinfo="skip",
            ),
            row=2,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=iv_series["date"],
                y=iv_series["iv_pct"],
                mode="lines+markers",
                name="ATM IV",
                line=dict(color="#2563eb", width=2),
                marker=dict(size=5),
            ),
            row=2,
            col=1,
        )

    chip_text = (
        f"{symbol} · 1D · IV {_fmt_pct(current_iv_pct, 2)} · "
        f"IV Rank {_fmt_rank_pct((iv_rank or {}).get('iv_rank'), 1)} · Put/Call {_fmt_number(put_call_ratio, 2)}"
    )
    fig.add_annotation(
        text=chip_text,
        xref="paper",
        yref="paper",
        x=0.99,
        y=0.99,
        xanchor="right",
        yanchor="top",
        showarrow=False,
        font=dict(size=12, color="#334155"),
        bgcolor="#f8fafc",
        bordercolor="#dbe3ef",
        borderpad=5,
    )
    fig.update_layout(
        height=626,
        template="plotly_white",
        margin=dict(l=16, r=14, t=42, b=18),
        hovermode="x unified",
        xaxis_rangeslider_visible=False,
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="left", x=0),
        paper_bgcolor="#ffffff",
        plot_bgcolor="#ffffff",
    )
    fig.update_xaxes(showgrid=False, rangebreaks=[dict(bounds=["sat", "mon"])])
    fig.update_yaxes(title_text="价格", row=1, col=1, gridcolor="#edf2f7")
    fig.update_yaxes(title_text="IV %", row=2, col=1, gridcolor="#edf2f7")
    fig.update_yaxes(title_text="量", row=3, col=1, gridcolor="#edf2f7")
    return fig


def _add_empty_chart_annotation(fig: go.Figure, text: str) -> None:
    fig.add_annotation(
        text=text,
        xref="paper",
        yref="paper",
        x=0.5,
        y=0.52,
        showarrow=False,
        font=dict(size=13, color="#64748b"),
        bgcolor="rgba(248,250,252,.9)",
        bordercolor="#dbe3ef",
        borderpad=8,
    )


def _add_cone_line(fig: go.Figure, line_df: pd.DataFrame, *, name: str, color: str, dash: str = "solid") -> None:
    if line_df is None or line_df.empty:
        return
    line = line_df.copy()
    for col in ("dte_target", "iv_pct", "dte"):
        line[col] = pd.to_numeric(line.get(col), errors="coerce")
    line = line.dropna(subset=["dte_target", "iv_pct"]).sort_values("dte_target")
    if line.empty:
        return
    custom = pd.DataFrame(
        {
            "expiration": line.get("expiration_date", pd.Series(["-"] * len(line))).fillna("-").astype(str),
            "actual_dte": pd.to_numeric(line.get("dte"), errors="coerce"),
        }
    ).to_numpy()
    fig.add_trace(
        go.Scatter(
            x=line["dte_target"],
            y=line["iv_pct"],
            customdata=custom,
            mode="lines+markers",
            name=name,
            line=dict(color=color, width=2.4, dash=dash),
            marker=dict(size=6, color=color),
            hovertemplate=(
                "目标DTE %{x}<br>"
                "IV %{y:.2f}%<br>"
                "实际DTE %{customdata[1]:.0f}<br>"
                "到期 %{customdata[0]}<extra></extra>"
            ),
        )
    )


def _build_volatility_cone_figure(
    cone_df: pd.DataFrame,
    today_line: pd.DataFrame,
    previous_line: pd.DataFrame,
    chart_id: str,
) -> go.Figure:
    fig = go.Figure()
    cone = cone_df.copy() if cone_df is not None else pd.DataFrame()
    if not cone.empty:
        for col in ("dte_target", "p10", "p25", "p50", "p75", "p90", "sample_count"):
            cone[col] = pd.to_numeric(cone.get(col), errors="coerce")
        cone = cone.dropna(subset=["dte_target"]).sort_values("dte_target")
    if not cone.empty and {"p10", "p25", "p50", "p75", "p90"}.issubset(cone.columns):
        x = cone["dte_target"]
        fig.add_trace(
            go.Scatter(x=x, y=cone["p90"], mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip")
        )
        fig.add_trace(
            go.Scatter(
                x=x,
                y=cone["p10"],
                mode="lines",
                fill="tonexty",
                fillcolor="rgba(37, 99, 235, 0.08)",
                line=dict(width=0),
                name="10-90%区间",
                hovertemplate="DTE %{x}<br>p10 %{y:.2f}%<extra></extra>",
            )
        )
        fig.add_trace(
            go.Scatter(x=x, y=cone["p75"], mode="lines", line=dict(width=0), showlegend=False, hoverinfo="skip")
        )
        fig.add_trace(
            go.Scatter(
                x=x,
                y=cone["p25"],
                mode="lines",
                fill="tonexty",
                fillcolor="rgba(37, 99, 235, 0.14)",
                line=dict(width=0),
                name="25-75%区间",
                hovertemplate="DTE %{x}<br>p25 %{y:.2f}%<extra></extra>",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=x,
                y=cone["p50"],
                mode="lines+markers",
                name="历史中位数",
                line=dict(color="#64748b", width=1.8),
                marker=dict(size=5, color="#64748b"),
                hovertemplate="DTE %{x}<br>中位数 %{y:.2f}%<extra></extra>",
            )
        )
        if pd.to_numeric(cone.get("sample_count"), errors="coerce").max() < 20:
            _add_empty_chart_annotation(fig, "历史样本不足：分位锥仅供参考")
    else:
        _add_empty_chart_annotation(fig, "历史样本不足：先显示今日/昨日曲线")
    _add_cone_line(fig, previous_line, name="昨日", color=PREVIOUS_LINE_COLOR, dash="dash")
    _add_cone_line(fig, today_line, name="今日", color=TODAY_LINE_COLOR)
    fig.update_layout(
        height=320,
        template="plotly_white",
        meta={"chart_id": chart_id},
        margin=dict(l=12, r=12, t=22, b=34),
        yaxis_title="IV %",
        xaxis_title="DTE",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
        paper_bgcolor="#ffffff",
        plot_bgcolor="#ffffff",
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="#edf2f7")
    return fig


def _add_otm_curve(fig: go.Figure, curve_df: pd.DataFrame, *, name: str, color: str, dash: str = "solid") -> None:
    if curve_df is None or curve_df.empty:
        return
    curve = curve_df.copy()
    for col in ("moneyness_pct", "iv_pct", "dte"):
        curve[col] = pd.to_numeric(curve.get(col), errors="coerce")
    curve = curve.dropna(subset=["moneyness_pct", "iv_pct"]).sort_values("moneyness_pct")
    if curve.empty:
        return
    negative = curve[curve["moneyness_pct"] < 0]
    positive = curve[curve["moneyness_pct"] > 0]
    if not negative.empty and not positive.empty:
        separator = pd.DataFrame([{col: None for col in curve.columns}])
        separator["moneyness_pct"] = 0.0
        separator["iv_pct"] = None
        separator["call_put"] = ""
        separator["expiration_date"] = "-"
        separator["dte"] = None
        curve = pd.concat([negative, separator, positive], ignore_index=True)
    custom = pd.DataFrame(
        {
            "side": curve.get("call_put", pd.Series([""] * len(curve))).fillna("").astype(str),
            "expiration": curve.get("expiration_date", pd.Series(["-"] * len(curve))).fillna("-").astype(str),
            "dte": pd.to_numeric(curve.get("dte"), errors="coerce"),
        }
    ).to_numpy()
    fig.add_trace(
        go.Scatter(
            x=curve["moneyness_pct"],
            y=curve["iv_pct"],
            customdata=custom,
            mode="lines+markers",
            name=name,
            line=dict(color=color, width=2.4, dash=dash),
            marker=dict(size=5.5, color=color),
            hovertemplate=(
                "相对ATM %{x:+.2f}%<br>"
                "IV %{y:.2f}%<br>"
                "%{customdata[0]} · DTE %{customdata[2]:.0f}<br>"
                "到期 %{customdata[1]}<extra></extra>"
            ),
        )
    )


def _otm_curve_x_range(*curves: pd.DataFrame) -> list[float]:
    values: list[float] = []
    for curve in curves:
        if curve is None or curve.empty or "moneyness_pct" not in curve.columns:
            continue
        series = pd.to_numeric(curve["moneyness_pct"], errors="coerce").abs().dropna()
        values.extend(float(value) for value in series if float(value) > 0)
    if not values:
        return [-5.5, 5.5]
    padded = math.ceil((max(values) + 0.35) * 2) / 2
    padded = min(10.5, max(3.5, padded))
    return [-padded, padded]


def _build_otm_volatility_curve_figure(
    today_curve: pd.DataFrame,
    previous_curve: pd.DataFrame,
    chart_id: str,
) -> go.Figure:
    fig = go.Figure()
    _add_otm_curve(fig, previous_curve, name="昨日OTM曲线", color=PREVIOUS_LINE_COLOR, dash="dash")
    _add_otm_curve(fig, today_curve, name="今日OTM曲线", color=TODAY_LINE_COLOR)
    if (today_curve is None or today_curve.empty) and (previous_curve is None or previous_curve.empty):
        _add_empty_chart_annotation(fig, "暂无可计算的 OTM 波动率曲线")
    elif previous_curve is None or previous_curve.empty:
        _add_empty_chart_annotation(fig, "暂无昨日曲线：仅显示今日")
    fig.add_vline(x=0, line_width=1, line_dash="dash", line_color="#94a3b8")
    fig.add_annotation(
        text="OTM Put",
        xref="paper",
        yref="paper",
        x=0.02,
        y=0.98,
        showarrow=False,
        font=dict(size=11, color="#64748b"),
    )
    fig.add_annotation(
        text="OTM Call",
        xref="paper",
        yref="paper",
        x=0.98,
        y=0.98,
        showarrow=False,
        font=dict(size=11, color="#64748b"),
    )
    fig.update_layout(
        height=320,
        template="plotly_white",
        meta={"chart_id": chart_id},
        margin=dict(l=12, r=12, t=22, b=34),
        yaxis_title="IV %",
        xaxis_title="相对 ATM %",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
        paper_bgcolor="#ffffff",
        plot_bgcolor="#ffffff",
    )
    fig.update_xaxes(showgrid=False, range=_otm_curve_x_range(today_curve, previous_curve), zeroline=False)
    fig.update_yaxes(gridcolor="#edf2f7")
    return fig


def _build_oi_distribution_figure(chain_df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    if chain_df is not None and not chain_df.empty:
        df = chain_df.copy()
        df["open_interest"] = pd.to_numeric(df["open_interest"], errors="coerce").fillna(0)
        df["dte"] = pd.to_numeric(df["dte"], errors="coerce")
        grouped = (
            df.groupby(["expiration_date", "dte"], dropna=False)["open_interest"]
            .sum()
            .reset_index()
            .sort_values("open_interest", ascending=False)
            .head(8)
            .sort_values("open_interest")
        )
        labels = grouped.apply(lambda row: f"{row['expiration_date']} ({int(row['dte'])}D)", axis=1)
        fig.add_trace(
            go.Bar(
                x=grouped["open_interest"],
                y=labels,
                orientation="h",
                marker_color="#2563eb",
                opacity=0.88,
                name="OI",
            )
        )
    fig.update_layout(
        height=280,
        template="plotly_white",
        margin=dict(l=12, r=12, t=22, b=26),
        xaxis_title="OI",
        yaxis_title="",
        showlegend=False,
    )
    return fig


def _build_oi_defense_figure(defense_df: pd.DataFrame, symbol: str) -> go.Figure:
    fig = go.Figure()
    y_axis_range = oi_defense_y_axis_range(defense_df)
    if defense_df is not None and not defense_df.empty:
        df = defense_df.copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        close = pd.to_numeric(df["underlying_close"], errors="coerce")
        if close.notna().any():
            fig.add_trace(
                go.Scatter(
                    x=df["date"],
                    y=close,
                    mode="lines",
                    name=f"{symbol} 收盘价",
                    line=dict(color="#64748b", width=2, dash="dot"),
                    hovertemplate="日期 %{x|%Y-%m-%d}<br>收盘价 %{y:,.2f}<extra></extra>",
                    cliponaxis=False,
                )
            )
        for side, name, color in [
            ("call", "Call 最大OI压力线", "#dc2626"),
            ("put", "Put 最大OI支撑线", "#16a34a"),
        ]:
            strike = pd.to_numeric(df[f"{side}_strike"], errors="coerce")
            if strike.notna().any():
                label_prefix = "Call最大OI" if side == "call" else "Put最大OI"
                labels = [""] * len(df)
                latest_idx = strike.dropna().index[-1]
                labels[df.index.get_loc(latest_idx)] = f"{label_prefix} {_fmt_strike(strike.loc[latest_idx])}"
                custom = pd.DataFrame(
                    {
                        "oi": pd.to_numeric(df[f"{side}_oi"], errors="coerce"),
                        "distance": pd.to_numeric(df[f"{side}_distance_pct"], errors="coerce"),
                        "expiration": df[f"{side}_expiration"].fillna("-").astype(str),
                    }
                ).to_numpy()
                fig.add_trace(
                    go.Scatter(
                        x=df["date"],
                        y=strike,
                        customdata=custom,
                        mode="lines+markers+text",
                        name=name,
                        text=labels,
                        textposition="top center" if side == "call" else "bottom center",
                        textfont=dict(color=color, size=12),
                        line=dict(color=color, width=3),
                        marker=dict(size=7, color=color),
                        cliponaxis=False,
                        hovertemplate=(
                            "日期 %{x|%Y-%m-%d}<br>"
                            "最大OI行权价 %{y:,.2f}<br>"
                            "最大持仓量 OI %{customdata[0]:,.0f}<br>"
                            "距现价 %{customdata[1]:+.2f}%<br>"
                            "主要到期 %{customdata[2]}<extra></extra>"
                        ),
                    )
                )
    fig.update_layout(
        height=460,
        template="plotly_white",
        margin=dict(l=14, r=18, t=58, b=34),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
        xaxis_title="日期",
        yaxis_title="行权价 / 收盘价",
        annotations=[
            dict(
                text="Call/Put 防线按近价 0-90D 期权最大持仓量（OI）所在行权价绘制",
                xref="paper",
                yref="paper",
                x=0,
                y=1.12,
                xanchor="left",
                yanchor="bottom",
                showarrow=False,
                font=dict(size=12, color="#64748b"),
            )
        ],
        paper_bgcolor="#ffffff",
        plot_bgcolor="#ffffff",
    )
    fig.update_xaxes(showgrid=False, tickformat="%m/%d")
    fig.update_yaxes(gridcolor="#edf2f7", range=y_axis_range)
    return fig


def _oi_defense_display(defense_df: pd.DataFrame) -> pd.DataFrame:
    if defense_df is None or defense_df.empty:
        return pd.DataFrame()
    out = defense_df.copy()
    out["日期"] = out["trade_date"].apply(_format_trade_date)
    out = out.rename(
        columns={
            "underlying_close": "收盘价",
            "call_strike": "Call 压力位",
            "call_oi": "Call OI",
            "call_distance_pct": "Call距现价%",
            "call_expiration": "Call主要到期",
            "put_strike": "Put 支撑位",
            "put_oi": "Put OI",
            "put_distance_pct": "Put距现价%",
            "put_expiration": "Put主要到期",
            "put_call_oi": "Put/Call OI",
        }
    )
    columns = [
        "日期",
        "收盘价",
        "Call 压力位",
        "Call OI",
        "Call距现价%",
        "Call主要到期",
        "Put 支撑位",
        "Put OI",
        "Put距现价%",
        "Put主要到期",
        "Put/Call OI",
    ]
    return out[[col for col in columns if col in out.columns]]


def _style_oi_defense_table(df: pd.DataFrame):
    return df.style.format(
        {
            "收盘价": "{:,.2f}",
            "Call 压力位": "{:,.2f}",
            "Call OI": "{:,.0f}",
            "Call距现价%": "{:+.2f}%",
            "Put 支撑位": "{:,.2f}",
            "Put OI": "{:,.0f}",
            "Put距现价%": "{:+.2f}%",
            "Put/Call OI": "{:,.2f}",
        },
        na_rep="-",
    )


def _option_display(df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "option_ticker",
        "strike",
        "cycle_label",
        "dte",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "open_interest",
        "iv_pct",
        "moneyness_pct",
        "iv_source",
    ]
    display = df[[col for col in columns if col in df.columns]].copy()
    display = display.rename(
        columns={
            "option_ticker": "合约",
            "strike": "行权价",
            "cycle_label": "类型",
            "dte": "DTE",
            "open": "开",
            "high": "高",
            "low": "低",
            "close": "收",
            "volume": "量",
            "open_interest": "OI",
            "iv_pct": "IV%",
            "moneyness_pct": "价差%",
            "iv_source": "IV来源",
        }
    )
    return display


def _style_option_table(df: pd.DataFrame):
    def style_row(row):
        moneyness = row.get("价差%")
        if pd.notna(moneyness) and abs(float(moneyness)) <= 1.0:
            return ["background-color: #fff7ed"] * len(row)
        return [""] * len(row)

    numeric_format = {
        "行权价": "{:,.2f}",
        "开": "{:,.2f}",
        "高": "{:,.2f}",
        "低": "{:,.2f}",
        "收": "{:,.2f}",
        "量": "{:,.0f}",
        "OI": "{:,.0f}",
        "IV%": "{:,.2f}",
        "价差%": "{:,.2f}",
    }
    return df.style.apply(style_row, axis=1).format(numeric_format, na_rep="-")


def _atm_chain_preview(chain_df: pd.DataFrame, selected_expiration: str, underlying_price: float | None) -> pd.DataFrame:
    if chain_df is None or chain_df.empty:
        return pd.DataFrame()
    df = chain_df.copy()
    if selected_expiration != "全部到期日":
        filtered = df[df["expiration_date"].astype(str) == selected_expiration]
        if not filtered.empty:
            df = filtered
    elif "dte" in df.columns:
        dte = pd.to_numeric(df["dte"], errors="coerce")
        target = df[(dte >= 20) & (dte <= 45)]
        if not target.empty:
            chosen_expiry = target.assign(dte_distance=(dte.loc[target.index] - 30).abs()).sort_values(
                "dte_distance"
            )["expiration_date"].iloc[0]
            df = df[df["expiration_date"] == chosen_expiry]
    if underlying_price is not None:
        df["distance"] = (pd.to_numeric(df["strike"], errors="coerce") - float(underlying_price)).abs()
        strikes = df.groupby("strike")["distance"].min().sort_values().head(9).index
        df = df[df["strike"].isin(strikes)]
    calls = df[df["call_put"] == "C"].copy()
    puts = df[df["call_put"] == "P"].copy()

    def side_frame(side: pd.DataFrame, prefix: str) -> pd.DataFrame:
        cols = ["strike", "iv_pct", "open_interest", "volume", "close", "moneyness_pct"]
        out = side[[col for col in cols if col in side.columns]].copy()
        return out.rename(
            columns={
                "iv_pct": f"{prefix} IV",
                "open_interest": f"{prefix} OI",
                "volume": f"{prefix} 量",
                "close": f"{prefix} Last",
                "moneyness_pct": "距ATM%",
            }
        )

    merged = pd.merge(side_frame(calls, "Call"), side_frame(puts, "Put"), on="strike", how="outer")
    merged = merged.sort_values("strike").rename(columns={"strike": "执行价"})
    if "距ATM%" not in merged.columns:
        if underlying_price:
            merged["距ATM%"] = (pd.to_numeric(merged["执行价"], errors="coerce") - float(underlying_price)) / float(
                underlying_price
            ) * 100
    ordered = [
        "Call IV",
        "Call OI",
        "Call 量",
        "Call Last",
        "执行价",
        "距ATM%",
        "Put Last",
        "Put 量",
        "Put OI",
        "Put IV",
    ]
    return merged[[col for col in ordered if col in merged.columns]]


def _render_rail(
    metrics: dict[str, Any],
    trade_date: str,
) -> None:
    term_state = str(metrics.get("term_state") or "样本不足")
    if term_state == "Backwardation":
        term_detail = "近月风险溢价偏强"
        term_color = "#dc2626"
    elif term_state == "Contango":
        term_detail = "远月 IV 高于近月"
        term_color = "#2563eb"
    elif term_state == "Flat":
        term_detail = "期限结构相对平坦"
        term_color = "#475569"
    else:
        term_detail = "期限样本不足"
        term_color = "#64748b"

    skew_expiration = metrics.get("skew_expiration")
    skew_detail = f"参考到期 {_format_trade_date(skew_expiration)}" if skew_expiration else "样本不足"
    term_sub = term_detail
    term_extra = (
        '<div class="us-lab-ledger-extra">'
        + _mini_chip_html("30D IV", _fmt_pct(metrics.get("iv_30d"), 2), "blue")
        + _mini_chip_html("60D IV", _fmt_pct(metrics.get("iv_60d"), 2), "blue")
        + "</div>"
    )
    put_skew = _clean_float(metrics.get("put_skew_5pct"))
    call_skew = _clean_float(metrics.get("call_skew_5pct"))
    skew_gap = _clean_float(metrics.get("put_call_skew_5pct"))
    if skew_gap is None and put_skew is not None and call_skew is not None:
        skew_gap = put_skew - call_skew
    skew_expiry_chip = (
        '<div class="us-lab-ledger-extra">'
        + _mini_chip_html("到期", _format_trade_date(skew_expiration), "blue")
        + "</div>"
    )
    skew_gap_extra = (
        '<div class="us-lab-ledger-extra">'
        + _mini_chip_html("Put", _fmt_signed_pct(put_skew, 1), "red")
        + _mini_chip_html("Call", _fmt_signed_pct(call_skew, 1), "blue")
        + "</div>"
    )
    positioning_sub = (
        f"0DTE成交 {_fmt_pct(metrics.get('zero_dte_volume_share_pct'), 1)} · Top OI {_fmt_strike(metrics.get('top_oi_strike'))}"
    )
    html = f"""
    <div class="us-lab-rail">
        <div class="us-lab-panel-title">
            <strong>波动率与持仓速览</strong>
            <span class="us-lab-rail-updated">更新至 {escape(_format_trade_date(trade_date))}</span>
        </div>

        <div class="us-lab-ledger">
            {_rail_card_html(title="今日 IV 变化", sub="较前一交易日", value=_fmt_signed_pct(metrics.get("iv_change_1d"), 1), pct_label="历史分位", pct_value=metrics.get("iv_change_1d_percentile"), color="#2563eb", history_count=metrics.get("iv_change_1d_history_count"), min_samples=metrics.get("historical_percentile_min_samples"))}
            {_rail_card_html(title="IV - RV20", sub="隐含波动率 - 实际波动率", value=_fmt_signed_pct(metrics.get("iv_rv20_spread"), 1), pct_label="历史分位", pct_value=metrics.get("iv_rv20_percentile"), color="#ea580c", tone="hot", history_count=metrics.get("iv_rv20_spread_history_count"), min_samples=metrics.get("historical_percentile_min_samples"))}
            {_rail_card_html(title="IV Rank", sub="月结算 IV 排名", value=_fmt_rank_pct(metrics.get("iv_rank"), 1), pct_label="历史分位", pct_value=metrics.get("iv_rank"), color="#2563eb", tone="compact", history_count=metrics.get("iv_history_days"), min_samples=metrics.get("historical_percentile_min_samples"))}
            {_rail_card_html(title="期限结构", sub=term_sub, value=_fmt_signed_pct(metrics.get("term_slope_30_60"), 1), pct_label="历史分位", pct_value=metrics.get("term_slope_percentile"), color=term_color, tone="compact wide", history_count=metrics.get("term_slope_30_60_history_count"), min_samples=metrics.get("historical_percentile_min_samples"), extra_html=term_extra)}
            {_rail_card_html(title="Put Skew", sub="下方保护相对 ATM", value=_fmt_signed_pct(put_skew, 1), pct_label="历史分位", pct_value=metrics.get("put_skew_5pct_percentile"), color="#dc2626", tone="compact", history_count=metrics.get("put_skew_5pct_history_count"), min_samples=metrics.get("historical_percentile_min_samples"), extra_html=skew_expiry_chip)}
            {_rail_card_html(title="Call Skew", sub="上方追涨相对 ATM", value=_fmt_signed_pct(call_skew, 1), pct_label="历史分位", pct_value=metrics.get("call_skew_5pct_percentile"), color="#2563eb", tone="compact", history_count=metrics.get("call_skew_5pct_history_count"), min_samples=metrics.get("historical_percentile_min_samples"), extra_html=skew_expiry_chip)}
            {_rail_card_html(title="Put-Call Skew", sub="保护需求减追涨需求", value=_fmt_signed_pct(skew_gap, 1), pct_label="历史分位", pct_value=metrics.get("put_call_skew_5pct_percentile"), color="#ea580c", tone="compact wide hot", history_count=metrics.get("put_call_skew_5pct_history_count"), min_samples=metrics.get("historical_percentile_min_samples"), extra_html=skew_gap_extra)}
            {_rail_card_html(title="Put/Call OI", sub=positioning_sub, value=_fmt_number(metrics.get("put_call_oi"), 2), pct_label="历史分位", pct_value=metrics.get("put_call_oi_percentile"), color="#ea580c", tone="compact hot", history_count=metrics.get("put_call_oi_history_count"), min_samples=metrics.get("historical_percentile_min_samples"))}
        </div>
    </div>
    """
    st.markdown(_compact_html_fragment(html), unsafe_allow_html=True)


@st.cache_resource(show_spinner=False)
def _cached_engine():
    return dashboard_engine()


@st.cache_data(show_spinner=False, ttl=600)
def _cached_auto_option_source(symbol: str) -> tuple[bool, str | None, str]:
    return _auto_option_source(symbol, _cached_engine())


@st.cache_data(show_spinner=False, ttl=600)
def _cached_stock_daily(symbol: str, limit: int, cache_version: str) -> pd.DataFrame:
    del cache_version
    return load_stock_daily(symbol, limit=limit, engine=_cached_engine())


def _stock_daily_cache_version(symbol: str) -> str:
    latest = load_stock_daily(symbol, limit=1, engine=_cached_engine())
    if latest.empty or "date" not in latest.columns:
        return "empty"
    row = latest.iloc[-1]
    date_value = pd.to_datetime(row.get("date"), errors="coerce")
    if pd.isna(date_value):
        date_text = "unknown"
    else:
        date_text = date_value.strftime("%Y%m%d")
    close_value = row.get("close", "")
    volume_value = row.get("volume", "")
    return f"{date_text}:{close_value}:{volume_value}"


@st.cache_data(show_spinner=False, ttl=600)
def _cached_available_option_trade_dates(symbol: str, use_test_tables: bool, limit: int) -> list[str]:
    return load_available_option_trade_dates(
        symbol,
        use_test_tables=use_test_tables,
        limit=limit,
        engine=_cached_engine(),
    )


@st.cache_data(show_spinner=False, ttl=600)
def _cached_option_chain_daily(
    symbol: str,
    trade_date: str,
    include_short_cycle: bool,
    use_test_tables: bool,
    underlying_price: float | None,
) -> pd.DataFrame:
    return load_option_chain_daily(
        symbol,
        trade_date,
        include_short_cycle=include_short_cycle,
        use_test_tables=use_test_tables,
        underlying_price=underlying_price,
        engine=_cached_engine(),
    )


@st.cache_data(show_spinner=False, ttl=600)
def _cached_option_surface_snapshot(
    symbol: str,
    trade_date: str,
    include_short_cycle: bool,
    use_test_tables: bool,
    underlying_price: float | None,
    moneyness_range: float,
    max_dte: int,
) -> pd.DataFrame:
    return load_option_surface_snapshot(
        symbol,
        trade_date,
        include_short_cycle=include_short_cycle,
        use_test_tables=use_test_tables,
        underlying_price=underlying_price,
        moneyness_range=moneyness_range,
        max_dte=max_dte,
        engine=_cached_engine(),
    )


@st.cache_data(show_spinner=False, ttl=600)
def _cached_option_chain_summary(
    symbol: str,
    trade_date: str,
    include_short_cycle: bool,
    include_iv_counts: bool,
    use_test_tables: bool,
) -> dict[str, int]:
    return load_option_chain_summary(
        symbol,
        trade_date,
        include_short_cycle=include_short_cycle,
        include_iv_counts=include_iv_counts,
        use_test_tables=use_test_tables,
        engine=_cached_engine(),
    )


@st.cache_data(show_spinner=False, ttl=600)
def _cached_iv_history(symbol: str, window: int, use_test_tables: bool) -> pd.DataFrame:
    return load_iv_history(symbol, window=window, use_test_tables=use_test_tables, engine=_cached_engine())


@st.cache_data(show_spinner=False, ttl=600)
def _cached_market_metrics_history(symbol: str, window: int, use_test_tables: bool) -> pd.DataFrame:
    return load_market_metrics_history(
        symbol,
        window=window,
        use_test_tables=use_test_tables,
        engine=_cached_engine(),
    )


@st.cache_data(show_spinner=False, ttl=600)
def _cached_volatility_cone_history(symbol: str, end_date: str, window: int, use_test_tables: bool) -> pd.DataFrame:
    return load_volatility_cone_history(
        symbol,
        end_date,
        window=window,
        use_test_tables=use_test_tables,
        engine=_cached_engine(),
    )


@st.cache_data(show_spinner=False, ttl=600)
def _cached_market_climate_strip() -> list[dict[str, Any]]:
    return load_market_climate_strip(engine=_cached_engine())


@st.cache_data(show_spinner=False, ttl=600)
def _cached_oi_defense_history(symbol: str, end_date: str, window: int, use_test_tables: bool) -> pd.DataFrame:
    return load_oi_defense_history(
        symbol,
        end_date,
        window=window,
        use_test_tables=use_test_tables,
        engine=_cached_engine(),
    )


_load_global_css()
_inject_page_style()
inject_sidebar_toggle_style(mode="high_contrast")
_inject_home_sidebar_button_style()

with st.sidebar:
    show_navigation()

engine_error = None
try:
    engine = _cached_engine()
except Exception as exc:
    engine = None
    engine_error = exc

if engine_error is not None:
    st.error(f"数据库连接初始化失败：{engine_error}")
if engine is None:
    st.warning("数据库环境变量未配置或数据库驱动不可用。请检查 DB_USER、DB_PASSWORD、DB_HOST、DB_PORT、DB_NAME。")

st.markdown(
    """
    <div class="us-lab-brand">
        <div class="us-lab-mark"></div>
        <div>
            <h1 class="us-lab-title">美股期权</h1>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

symbol_options = list(DEFAULT_DASHBOARD_UNDERLYINGS)
current_symbol = st.session_state.get("us_lab_symbol", symbol_options[0])
if current_symbol not in symbol_options:
    st.session_state.pop("us_lab_symbol", None)
    current_symbol = symbol_options[0]

view_options = ["总览", "波动率曲面", "持仓防线"]
if st.session_state.get("us_lab_active_view") not in view_options:
    st.session_state["us_lab_active_view"] = "总览"

nav_col, symbol_col = st.columns([0.74, 0.26], gap="small")
with nav_col:
    active_view = st.segmented_control(
        "页面",
        options=view_options,
        label_visibility="collapsed",
        key="us_lab_active_view",
    )
with symbol_col:
    symbol = st.selectbox(
        "标的",
        symbol_options,
        index=symbol_options.index(current_symbol),
        format_func=_underlying_option_label,
        label_visibility="collapsed",
        key="us_lab_symbol",
    )
active_view = active_view or "总览"

include_short_cycle = True
stock_limit = 420
market_metrics_history = _cached_market_metrics_history(symbol, 252, False)
if not market_metrics_history.empty:
    use_test_tables = False
    latest_option_trade_date = _latest_metric_trade_date(market_metrics_history)
    table_label = "正式表"
else:
    use_test_tables, latest_option_trade_date, table_label = _cached_auto_option_source(symbol)
    market_metrics_history = _cached_market_metrics_history(symbol, 252, use_test_tables)
stock_cache_version = _stock_daily_cache_version(symbol)
stock_df = _cached_stock_daily(symbol, stock_limit, stock_cache_version)
fallback_date = dt.datetime.strptime(default_trade_date(), "%Y%m%d").date()
if not stock_df.empty:
    latest_stock_date = pd.to_datetime(stock_df["date"].max()).date()
else:
    latest_stock_date = fallback_date
if latest_option_trade_date:
    selected_date = dt.datetime.strptime(latest_option_trade_date, "%Y%m%d").date()
else:
    selected_date = latest_stock_date
trade_date = selected_date.strftime("%Y%m%d")
underlying_price = selected_underlying_price(stock_df, trade_date)
if market_metrics_history.empty:
    summary = _cached_option_chain_summary(
        symbol,
        trade_date,
        include_short_cycle,
        True,
        use_test_tables,
    )
else:
    summary = _summary_from_market_metrics(market_metrics_history, trade_date)
iv_history = _iv_history_from_market_metrics(market_metrics_history)
vol_position_metrics = calculate_overview_metrics_from_market_history(
    stock_df=stock_df,
    market_metrics_history=market_metrics_history,
    trade_date=trade_date,
)
current_iv_pct = vol_position_metrics.get("atm_iv_pct")
if current_iv_pct is None or pd.isna(current_iv_pct):
    current_iv_pct = vol_position_metrics.get("current_monthly_iv_pct")
chain_df = pd.DataFrame()

if market_metrics_history.empty and summary["rows"] > 0:
    chain_df = _cached_option_chain_daily(
        symbol,
        trade_date,
        include_short_cycle,
        use_test_tables,
        underlying_price,
    )
    summary = summarize_option_chain(chain_df)
    iv_history = _cached_iv_history(symbol, 252, use_test_tables)
    current_iv_pct = calculate_atm_iv_pct(chain_df, underlying_price=underlying_price) or _atm_iv_pct(
        chain_df,
        underlying_price,
    )
    vol_position_metrics = calculate_volatility_positioning_metrics(
        stock_df=stock_df,
        chain_df=chain_df,
        iv_history=iv_history,
        trade_date=trade_date,
        current_iv_pct=current_iv_pct,
        iv_rank=None,
        market_metrics_history=market_metrics_history,
    )

_render_kpi_strip(
    [
        (
            str(card.get("label") or ""),
            str(card.get("value") or "--"),
            str(card.get("detail") or ""),
            str(card.get("color") or "#0f172a"),
            str(card.get("hint") or ""),
        )
        for card in _cached_market_climate_strip()
    ]
)

if summary["rows"] == 0:
    if latest_option_trade_date and latest_option_trade_date != trade_date:
        latest_label = dt.datetime.strptime(latest_option_trade_date, "%Y%m%d").strftime("%Y/%m/%d")
        st.info(f"{symbol} 期权数据最近可用日期是 {latest_label}；当前选择的 {selected_date:%Y/%m/%d} 尚未入库。")
    _empty_command_hint(symbol, trade_date)
elif summary["provider_iv_rows"] == 0 or summary["open_interest_rows"] == 0:
    st.markdown(
        (
            '<div class="us-lab-note">'
            "当前数据缺少官方 IV 或 OI，页面会保留价格、computed IV 和空状态，升级/补数后可直接验收。"
            "</div>"
        ),
        unsafe_allow_html=True,
    )

st.markdown('<div class="us-lab-tab-divider"></div>', unsafe_allow_html=True)

if active_view == "总览":
    main_col, rail_col = st.columns([2.55, 1.05], gap="small")
    with main_col:
        _render_lightweight_chart(stock_df, iv_history, symbol, trade_date, current_iv_pct, height=650)
    with rail_col:
        _render_rail(vol_position_metrics, trade_date)

elif active_view == "波动率曲面":
    surface_df = _cached_option_surface_snapshot(
        symbol,
        trade_date,
        include_short_cycle,
        use_test_tables,
        underlying_price,
        10.0,
        135,
    )
    available_dates = _cached_available_option_trade_dates(symbol, use_test_tables, 8)
    previous_trade_date = next((value for value in available_dates if str(value or "") < str(trade_date)), None)
    previous_surface_df = pd.DataFrame()
    if previous_trade_date:
        previous_price = selected_underlying_price(stock_df, previous_trade_date)
        previous_surface_df = _cached_option_surface_snapshot(
            symbol,
            previous_trade_date,
            include_short_cycle,
            use_test_tables,
            previous_price,
            10.0,
            135,
        )
    cone_history = _cached_volatility_cone_history(symbol, trade_date, 252, use_test_tables)
    today_cone_line = build_volatility_cone_line(surface_df)
    previous_cone_line = build_volatility_cone_line(previous_surface_df)
    today_otm_curve = build_otm_volatility_curve(surface_df)
    previous_otm_curve = build_otm_volatility_curve(previous_surface_df)
    c1, c2 = st.columns([1, 1], gap="small")
    with c1:
        st.markdown(
            '<div class="us-lab-panel"><div class="us-lab-panel-title"><strong>波动率锥</strong><span>近一年历史分位 + 今日/昨日</span></div>',
            unsafe_allow_html=True,
        )
        st.plotly_chart(
            _build_volatility_cone_figure(cone_history, today_cone_line, previous_cone_line, "vol_cone"),
            width="stretch",
            config={"displaylogo": False},
            key=f"vol_cone_{symbol}_{trade_date}_{previous_trade_date or 'none'}_{table_label}",
        )
        st.markdown("</div>", unsafe_allow_html=True)
    with c2:
        st.markdown(
            '<div class="us-lab-panel"><div class="us-lab-panel-title"><strong>波动率曲线</strong><span>OTM Put + OTM Call</span></div>',
            unsafe_allow_html=True,
        )
        st.plotly_chart(
            _build_otm_volatility_curve_figure(today_otm_curve, previous_otm_curve, "otm_vol_curve"),
            width="stretch",
            config={"displaylogo": False},
            key=f"otm_vol_curve_{symbol}_{trade_date}_{previous_trade_date or 'none'}_{table_label}",
        )
        st.markdown("</div>", unsafe_allow_html=True)

elif active_view == "持仓防线":
    defense_history = _cached_oi_defense_history(symbol, trade_date, 20, use_test_tables)
    if defense_history.empty:
        st.info("暂无足够持仓防线数据。请确认近 20 个交易日期权日线中已有 OI。")
    else:
        latest_defense = defense_history.iloc[-1]
        call_strike = _clean_float(latest_defense.get("call_strike"))
        put_strike = _clean_float(latest_defense.get("put_strike"))
        close_price = _clean_float(latest_defense.get("underlying_close"))
        call_distance = _clean_float(latest_defense.get("call_distance_pct"))
        put_distance = _clean_float(latest_defense.get("put_distance_pct"))
        _render_kpi_strip(
            [
                (
                    "Call 压力位",
                    _fmt_strike(call_strike),
                    f"OI {_fmt_int(latest_defense.get('call_oi'))} · 距现价 {_fmt_signed(call_distance, 2, '%')}",
                    "#dc2626",
                ),
                (
                    "Put 支撑位",
                    _fmt_strike(put_strike),
                    f"OI {_fmt_int(latest_defense.get('put_oi'))} · 距现价 {_fmt_signed(put_distance, 2, '%')}",
                    "#16a34a",
                ),
                (
                    "现价位置",
                    _fmt_number(close_price, 2),
                    f"压力/支撑 {_fmt_strike(call_strike)} / {_fmt_strike(put_strike)}",
                    "#0f172a",
                ),
                (
                    "Put/Call OI",
                    _fmt_number(latest_defense.get("put_call_oi"), 2),
                    "近价 0-90D 聚合",
                    "#ea580c",
                ),
            ]
        )
        st.plotly_chart(
            _build_oi_defense_figure(defense_history, symbol),
            width="stretch",
            config={"displaylogo": False},
            key=f"oi_defense_{symbol}_{trade_date}_{table_label}",
        )
        with st.expander("查看近 20 日防线明细", expanded=False):
            st.dataframe(
                _style_oi_defense_table(_oi_defense_display(defense_history)),
                width="stretch",
                hide_index=True,
                key=f"oi_defense_detail_{symbol}_{trade_date}_{table_label}",
            )
