import html
import os

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

from sidebar_navigation import show_navigation
from tools.kronos_poc.client import predict_eod_interval
from tools.kronos_poc.config import DEFAULT_LOOKBACK, PILOT_ASSETS
from ui_components import inject_sidebar_toggle_style


st.set_page_config(page_title="时序预测实验", page_icon="📈", layout="wide")

with st.sidebar:
    show_navigation()

inject_sidebar_toggle_style(mode="high_contrast")


def _inject_finance_theme() -> None:
    st.markdown(
        """
<style>
    .stApp {
        background-color: #070b12;
        background-image:
            linear-gradient(180deg, rgba(12, 22, 39, 0.96) 0%, rgba(7, 11, 18, 1) 58%, rgba(5, 8, 13, 1) 100%),
            repeating-linear-gradient(90deg, rgba(255,255,255,0.018) 0, rgba(255,255,255,0.018) 1px, transparent 1px, transparent 80px);
        color: #e7edf8;
    }
    header[data-testid="stHeader"] {
        background-color: transparent !important;
        box-shadow: none !important;
        border-bottom: 0 !important;
    }
    [data-testid="stDecoration"] {
        display: none !important;
    }
    [data-testid="stMainBlockContainer"] {
        max-width: 82rem;
        padding-top: 0.35rem;
        padding-bottom: 0.9rem;
    }
    .block-container {
        padding-top: 0.35rem !important;
    }
    h1, h2, h3 {
        font-family: "Noto Serif SC", "Songti SC", "STSong", serif;
        color: #f1f5ff !important;
        letter-spacing: 0.4px;
    }
    h1 {
        font-size: 2.25rem !important;
        margin-bottom: 0.15rem !important;
        color: #f8fbff !important;
        text-shadow: 0 0 18px rgba(77, 245, 198, 0.16);
    }
    h2, h3 {
        font-size: 1.35rem !important;
    }
    p, label, .stCaption {
        color: #d3def2 !important;
    }
    .ta-hero-shell {
        border: 1px solid rgba(120, 149, 204, 0.32);
        border-radius: 16px;
        padding: 18px 18px 16px;
        background:
            radial-gradient(900px 300px at 78% -30%, rgba(60, 200, 255, 0.2), transparent 58%),
            radial-gradient(700px 260px at 8% 0%, rgba(77, 245, 198, 0.13), transparent 60%),
            linear-gradient(120deg, rgba(12, 26, 54, 0.92), rgba(10, 22, 46, 0.78));
        box-shadow: 0 16px 40px rgba(0, 0, 0, 0.32), inset 0 1px 0 rgba(255, 255, 255, 0.04);
        margin-bottom: 14px;
        position: relative;
        overflow: hidden;
        animation: taHeroOpen 900ms cubic-bezier(.22,.9,.28,1) both;
    }
    .ta-hero-shell::after {
        content: "";
        position: absolute;
        top: 0;
        left: -38%;
        width: 32%;
        height: 100%;
        background: linear-gradient(90deg, transparent, rgba(108, 171, 255, 0.18), transparent);
        transform: skewX(-16deg);
        animation: taScanSweep 6.2s ease-in-out infinite;
    }
    .ta-hero-title {
        color: #f7fbff;
        font-family: "Noto Serif SC", "Songti SC", "STSong", serif;
        font-size: clamp(30px, 4vw, 46px);
        line-height: 1.06;
        font-weight: 800;
        letter-spacing: 0.02em;
        margin: 0;
        text-shadow: 0 0 18px rgba(77, 245, 198, 0.14);
    }
    .ta-hero-sub {
        color: #9fb0cd;
        font-size: 16px;
        margin-top: 6px;
    }
    .ta-hero-note {
        margin-top: 10px;
        color: #c8d6ef;
        font-size: 14px;
        line-height: 1.5;
        animation: taFadeUp 760ms ease both;
        animation-delay: 220ms;
    }
    .ta-panel-title {
        color: #f2f6ff;
        font-size: 1.18rem;
        font-weight: 800;
        margin: 0 0 0.8rem;
    }
    .ta-title-row {
        display: inline-flex;
        align-items: center;
        gap: 0.48rem;
        margin: 0 0 0.8rem;
    }
    .ta-info-icon {
        position: relative;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 19px;
        height: 19px;
        border: 1px solid rgba(77, 245, 198, 0.46);
        border-radius: 50%;
        color: #dffcf7;
        background: rgba(8, 18, 30, 0.92);
        font-size: 12px;
        font-weight: 800;
        cursor: help;
        box-shadow: 0 0 14px rgba(77, 245, 198, 0.12);
    }
    .ta-info-tooltip {
        visibility: hidden;
        opacity: 0;
        position: absolute;
        z-index: 999;
        left: 50%;
        top: 26px;
        transform: translateX(-50%) translateY(4px);
        width: min(360px, 82vw);
        padding: 12px 13px;
        border: 1px solid rgba(120, 149, 204, 0.36);
        border-radius: 12px;
        background: rgba(7, 13, 24, 0.97);
        color: #dce8ff;
        font-size: 13px;
        line-height: 1.58;
        font-weight: 500;
        text-align: left;
        box-shadow: 0 18px 38px rgba(0,0,0,0.38), 0 0 22px rgba(77,245,198,0.12);
        transition: opacity 140ms ease, transform 140ms ease, visibility 140ms ease;
    }
    .ta-info-icon:hover .ta-info-tooltip {
        visibility: visible;
        opacity: 1;
        transform: translateX(-50%) translateY(0);
    }
    .ta-info-tooltip strong {
        color: #f6d46c;
    }
    .ta-control-band {
        margin: 0.5rem 0 1rem;
        padding: 0.65rem 0 0.25rem;
        border-top: 1px solid rgba(130, 153, 192, 0.12);
        border-bottom: 1px solid rgba(130, 153, 192, 0.12);
    }
    .ta-empty-state {
        min-height: 620px;
        display: flex;
        align-items: center;
        justify-content: center;
        border: 1px solid rgba(130, 153, 192, 0.13);
        background: rgba(7, 13, 24, 0.42);
        color: #93a4c1;
        font-size: 1rem;
    }
    .ta-result-bar {
        display: flex;
        align-items: flex-end;
        justify-content: space-between;
        gap: 1rem;
        margin-bottom: 0.45rem;
    }
    .ta-result-symbol {
        color: #f6f8ff;
        font-size: 1.75rem;
        font-weight: 850;
        line-height: 1.1;
    }
    .ta-result-meta {
        color: #9fb0cc;
        font-size: 0.86rem;
        margin-top: 0.35rem;
    }
    .ta-cache-pill {
        color: #d8b451;
        border: 1px solid rgba(216, 180, 81, 0.28);
        background: rgba(216, 180, 81, 0.08);
        border-radius: 999px;
        padding: 0.28rem 0.62rem;
        font-size: 0.78rem;
        white-space: nowrap;
    }
    .ta-interval-grid {
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 0.9rem;
        margin-top: 0.9rem;
    }
    .ta-interval-card {
        border: 1px solid rgba(245, 201, 90, 0.22);
        border-top: 2px solid rgba(245, 201, 90, 0.72);
        background:
            linear-gradient(180deg, rgba(245, 201, 90, 0.16), rgba(8, 14, 25, 0.28)),
            repeating-linear-gradient(90deg, rgba(77,245,198,0.035) 0, rgba(77,245,198,0.035) 1px, transparent 1px, transparent 36px);
        padding: 0.95rem 1rem 0.82rem;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.05), 0 12px 26px rgba(0,0,0,0.24);
    }
    .ta-interval-step {
        color: #f3d06a;
        font-size: 0.82rem;
        letter-spacing: 0.02em;
    }
    .ta-interval-mid {
        color: #ffe27a;
        font-size: 1.75rem;
        font-weight: 850;
        margin-top: 0.24rem;
    }
    .ta-interval-range {
        color: #b8c5dc;
        font-size: 0.86rem;
        margin-top: 0.22rem;
    }
    .ta-bottom-risk {
        margin-top: 0.85rem;
    }
    @media (max-width: 760px) {
        .ta-result-bar {
            align-items: flex-start;
            flex-direction: column;
        }
        .ta-interval-grid {
            grid-template-columns: 1fr;
        }
    }
    [data-testid="stSelectbox"] label p {
        color: #dce8ff !important;
    }
    div[data-baseweb="select"] > div {
        background: rgba(15, 29, 56, 0.85) !important;
        border: 1px solid rgba(116, 144, 195, 0.35) !important;
    }
    div[data-baseweb="select"] span,
    div[data-baseweb="select"] input,
    div[data-baseweb="select"] div {
        color: #eef4ff !important;
        opacity: 1 !important;
    }
    div[data-baseweb="select"] svg {
        fill: #b6c8ec !important;
    }
    [role="listbox"] [role="option"] {
        color: #e9f1ff !important;
        background: rgba(14, 28, 56, 0.96) !important;
    }
    [role="listbox"] [role="option"][aria-selected="true"] {
        background: rgba(58, 92, 154, 0.55) !important;
    }
    .stButton > button {
        position: relative !important;
        overflow: hidden !important;
        background:
            linear-gradient(180deg, rgba(18, 37, 55, 0.98), rgba(8, 18, 30, 0.98)) !important;
        color: #dffcf7 !important;
        border: 1px solid rgba(77, 245, 198, 0.52) !important;
        border-radius: 12px !important;
        font-weight: 700 !important;
        letter-spacing: 0.03em !important;
        box-shadow:
            inset 0 1px 0 rgba(255, 255, 255, 0.06),
            0 0 0 1px rgba(77, 245, 198, 0.08),
            0 0 18px rgba(77, 245, 198, 0.12),
            0 10px 24px rgba(0, 0, 0, 0.32) !important;
        text-shadow: 0 0 10px rgba(77, 245, 198, 0.24) !important;
        transition: transform 160ms ease, box-shadow 160ms ease, filter 160ms ease !important;
    }
    .stButton > button:hover {
        transform: translateY(-1px) !important;
        filter: brightness(1.08) !important;
        box-shadow:
            inset 0 1px 0 rgba(255, 255, 255, 0.1),
            0 0 0 1px rgba(77, 245, 198, 0.18),
            0 0 24px rgba(77, 245, 198, 0.2),
            0 12px 28px rgba(0, 0, 0, 0.38) !important;
    }
    .stButton > button:active {
        transform: translateY(0) scale(0.99) !important;
    }
    [data-testid="stMetric"] {
        background: linear-gradient(160deg, rgba(20, 35, 65, 0.95) 0%, rgba(13, 26, 52, 0.9) 100%);
        border: 1px solid rgba(114, 146, 205, 0.32);
        border-radius: 14px;
        padding: 0.75rem 0.9rem;
        box-shadow: 0 6px 24px rgba(2, 8, 20, 0.35);
    }
    [data-testid="stMetricLabel"] p {
        color: #aebfdf !important;
    }
    [data-testid="stMetricValue"] {
        color: #f3f7ff !important;
        font-weight: 750 !important;
    }
    [data-testid="stMetricDelta"] {
        color: #b5c8ea !important;
    }
    [data-testid="stCheckbox"] p {
        color: #d9e6ff !important;
    }
    [data-testid="stExpander"] {
        background: rgba(9, 20, 41, 0.82);
        border: 1px solid rgba(96, 130, 196, 0.24);
        border-radius: 12px;
    }
    [data-testid="stDataFrame"] {
        border: 1px solid rgba(114, 146, 205, 0.25);
        border-radius: 12px;
    }
    [data-testid="stAlert"] {
        font-size: 0.92rem !important;
        line-height: 1.35 !important;
    }
    [data-testid="stAlert"] p,
    [data-testid="stAlert"] span,
    [data-testid="stAlert"] div {
        color: #eaf2ff !important;
        font-size: 0.92rem !important;
        line-height: 1.35 !important;
    }
    @keyframes taHeroOpen {
        0% {
            opacity: 0;
            transform: translateY(14px) scale(0.994);
            filter: blur(6px);
        }
        100% {
            opacity: 1;
            transform: translateY(0) scale(1);
            filter: blur(0);
        }
    }
    @keyframes taScanSweep {
        0% { left: -38%; }
        58% { left: 118%; }
        100% { left: 118%; }
    }
    @keyframes taFadeUp {
        0% {
            opacity: 0;
            transform: translateY(10px);
        }
        100% {
            opacity: 1;
            transform: translateY(0);
        }
    }
</style>
""",
        unsafe_allow_html=True,
    )


def _format_price(value: object) -> str:
    try:
        return f"{float(value):,.2f}"
    except (TypeError, ValueError):
        return "-"


def _request_timeout_seconds() -> float:
    try:
        return max(10.0, float(os.getenv("KRONOS_POC_REQUEST_TIMEOUT", "180")))
    except ValueError:
        return 180.0


def _build_chart(resp: dict) -> go.Figure:
    hist = (((resp or {}).get("debug") or {}).get("history_plot") or {})
    hist_dates_raw = hist.get("trade_dates", [])
    hist_opens_raw = hist.get("open_prices", [])
    hist_highs_raw = hist.get("high_prices", [])
    hist_lows_raw = hist.get("low_prices", [])
    hist_closes_raw = hist.get("close_prices", [])
    preds = resp.get("predictions", [])

    hist_dates = pd.to_datetime(pd.Series(hist_dates_raw, dtype="object"), format="%Y%m%d", errors="coerce")
    hist_opens = pd.to_numeric(pd.Series(hist_opens_raw, dtype="object"), errors="coerce")
    hist_highs = pd.to_numeric(pd.Series(hist_highs_raw, dtype="object"), errors="coerce")
    hist_lows = pd.to_numeric(pd.Series(hist_lows_raw, dtype="object"), errors="coerce")
    hist_closes = pd.to_numeric(pd.Series(hist_closes_raw, dtype="object"), errors="coerce")
    has_ohlc = (
        len(hist_opens) == len(hist_dates)
        and len(hist_highs) == len(hist_dates)
        and len(hist_lows) == len(hist_dates)
        and len(hist_closes) == len(hist_dates)
    )
    valid_hist = hist_dates.notna() & hist_closes.notna()
    if has_ohlc:
        valid_hist = valid_hist & hist_opens.notna() & hist_highs.notna() & hist_lows.notna()
    hist_dates = hist_dates[valid_hist]
    hist_opens = hist_opens[valid_hist] if has_ohlc else pd.Series(dtype="float64")
    hist_highs = hist_highs[valid_hist] if has_ohlc else pd.Series(dtype="float64")
    hist_lows = hist_lows[valid_hist] if has_ohlc else pd.Series(dtype="float64")
    hist_closes = hist_closes[valid_hist]
    hist_x = list(range(len(hist_dates)))

    fig = go.Figure()

    if not hist_dates.empty:
        if has_ohlc and not hist_opens.empty:
            fig.add_trace(
                go.Candlestick(
                    x=hist_x,
                    open=hist_opens,
                    high=hist_highs,
                    low=hist_lows,
                    close=hist_closes,
                    name="K线",
                    increasing=dict(line=dict(color="#ff4d5e", width=1.15), fillcolor="rgba(255,77,94,0.52)"),
                    decreasing=dict(line=dict(color="#20c982", width=1.15), fillcolor="rgba(32,201,130,0.42)"),
                    hovertemplate=(
                        "%{customdata}<br>"
                        "开: %{open:.2f}<br>"
                        "高: %{high:.2f}<br>"
                        "低: %{low:.2f}<br>"
                        "收: %{close:.2f}<extra></extra>"
                    ),
                    customdata=hist_dates.dt.strftime("%Y-%m-%d"),
                )
            )
        else:
            fig.add_trace(
                go.Scatter(
                    x=hist_x,
                    y=hist_closes,
                    mode="lines",
                    name="历史",
                    line=dict(color="#7aa5ff", width=2.25),
                    customdata=hist_dates.dt.strftime("%Y-%m-%d"),
                    hovertemplate="%{customdata}<br>历史收盘: %{y:.2f}<extra></extra>",
                )
            )
        fig.add_trace(
            go.Scatter(
                x=[hist_x[-1]],
                y=[hist_closes.iloc[-1]],
                mode="markers",
                name="最新",
                marker=dict(size=7, color="#dce8ff", line=dict(color="#516ea8", width=1)),
                customdata=[hist_dates.iloc[-1].strftime("%Y-%m-%d")],
                hovertemplate="%{customdata}<br>最新收盘: %{y:.2f}<extra></extra>",
            )
        )

    if preds:
        future_raw = [row.get("target_trade_date") for row in preds]
        future_dates = pd.to_datetime(pd.Series(future_raw, dtype="object"), format="%Y%m%d", errors="coerce")
        if future_dates.isna().any() and not hist_dates.empty:
            fallback_dates = pd.bdate_range(start=hist_dates.iloc[-1] + pd.Timedelta(days=1), periods=len(preds), freq="B")
            future_dates = future_dates.fillna(pd.Series(fallback_dates))
        future_x = list(range(len(hist_x), len(hist_x) + len(preds)))
        future_labels = future_dates.dt.strftime("%Y-%m-%d").tolist()
        p10 = [float(row.get("p10_close")) for row in preds]
        p50 = [float(row.get("p50_close")) for row in preds]
        p90 = [float(row.get("p90_close")) for row in preds]
        core_low = [
            float(row.get("core_low_close") if row.get("core_low_close") is not None else (float(row.get("p10_close")) + float(row.get("p50_close"))) / 2)
            for row in preds
        ]
        core_high = [
            float(row.get("core_high_close") if row.get("core_high_close") is not None else (float(row.get("p50_close")) + float(row.get("p90_close"))) / 2)
            for row in preds
        ]

        if future_x:
            forecast_x0 = future_x[0] - 0.65
            forecast_x1 = future_x[-1] + 0.85
            fig.add_vrect(
                x0=forecast_x0,
                x1=forecast_x1,
                fillcolor="rgba(245, 201, 90, 0.12)",
                line=dict(color="rgba(245, 201, 90, 0.32)", width=1),
                layer="below",
            )
            fig.add_annotation(
                x=forecast_x1,
                y=max(p90),
                text="未来 3 日预测",
                showarrow=False,
                xanchor="right",
                yanchor="bottom",
                font=dict(color="#f7d36c", size=12),
                bgcolor="rgba(7,12,22,0.72)",
                bordercolor="rgba(245,201,90,0.32)",
                borderpad=4,
            )
        fig.add_trace(
            go.Scatter(
                x=future_x,
                y=p90,
                mode="lines",
                name="P90",
                showlegend=False,
                line=dict(color="rgba(255,215,102,0.42)", width=1.5, dash="dot"),
                customdata=future_labels,
                hovertemplate="%{customdata}<br>P90 高位价格百分位: %{y:.2f}<extra></extra>",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=future_x,
                y=p10,
                mode="lines",
                name="尾部风险",
                fill="tonexty",
                fillcolor="rgba(255,215,102,0.13)",
                line=dict(color="rgba(255,215,102,0.42)", width=1.5, dash="dot"),
                customdata=future_labels,
                hovertemplate="%{customdata}<br>P10 低位价格百分位: %{y:.2f}<extra></extra>",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=future_x,
                y=core_high,
                mode="lines",
                name="主情景上沿",
                showlegend=False,
                line=dict(color="rgba(255,226,122,0.92)", width=2.4),
                customdata=future_labels,
                hovertemplate="%{customdata}<br>主情景上沿: %{y:.2f}<extra></extra>",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=future_x,
                y=core_low,
                mode="lines",
                name="主情景区间",
                fill="tonexty",
                fillcolor="rgba(255,226,122,0.42)",
                line=dict(color="rgba(255,226,122,0.92)", width=2.4),
                customdata=future_labels,
                hovertemplate="%{customdata}<br>主情景下沿: %{y:.2f}<extra></extra>",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=future_x,
                y=p50,
                mode="lines+markers+text",
                name="中位",
                line=dict(color="#ffffff", width=3.6),
                marker=dict(size=10, color="#fff2a8", line=dict(color="#151006", width=1.2)),
                text=[f"D+{row.get('step')}" for row in preds],
                textposition="top center",
                textfont=dict(color="#ffe27a", size=11),
                customdata=future_labels,
                hovertemplate="%{customdata}<br>P50 中位价格: %{y:.2f}<extra></extra>",
            )
        )

    all_dates = hist_dates.dt.strftime("%m-%d").tolist()
    if preds:
        all_dates.extend(future_dates.dt.strftime("%m-%d").tolist())
    tick_step = max(1, len(all_dates) // 8)
    tickvals = list(range(0, len(all_dates), tick_step))
    if all_dates and (len(all_dates) - 1) not in tickvals:
        tickvals.append(len(all_dates) - 1)
    ticktext = [all_dates[i] for i in tickvals]
    latest_focus_bars = 9
    total_points = len(all_dates)
    forecast_points = len(preds) if preds else 0
    focus_start = max(0, len(hist_x) - latest_focus_bars)
    focus_end = max(total_points - 1, len(hist_x) + forecast_points - 1) + 0.65
    initial_x_range = [focus_start - 0.65, focus_end]
    visible_values: list[float] = []
    if has_ohlc and not hist_highs.empty:
        visible_hist_mask = [(x >= focus_start) for x in hist_x]
        visible_values.extend(pd.to_numeric(hist_highs[visible_hist_mask], errors="coerce").dropna().tolist())
        visible_values.extend(pd.to_numeric(hist_lows[visible_hist_mask], errors="coerce").dropna().tolist())
    else:
        visible_values.extend(pd.to_numeric(hist_closes.tail(latest_focus_bars), errors="coerce").dropna().tolist())
    if preds:
        visible_values.extend(p10)
        visible_values.extend(p90)
        visible_values.extend(core_low)
        visible_values.extend(core_high)
    visible_values = [float(v) for v in visible_values if pd.notna(v)]
    if visible_values:
        y_min = min(visible_values)
        y_max = max(visible_values)
        y_mid = (y_min + y_max) / 2
        y_span = y_max - y_min
        # Use price-scale-aware padding. A fixed 1.0 pad makes low-price ETFs look flat.
        min_pad = max(abs(y_mid) * 0.006, 0.01)
        y_pad = max(y_span * 0.18, min_pad)
        initial_y_range = [y_min - y_pad, y_max + y_pad]
    else:
        initial_y_range = None

    fig.update_layout(
        height=590,
        margin=dict(l=8, r=8, t=16, b=8),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(5,11,20,0.62)",
        font=dict(color="#d8e2f4", family="Avenir Next, PingFang SC, Helvetica Neue, sans-serif"),
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1.0,
            font=dict(color="#d5e0f4", size=12),
            bgcolor="rgba(0, 0, 0, 0)",
        ),
        xaxis=dict(
            title=None,
            showgrid=False,
            linecolor="rgba(128,157,215,0.18)",
            type="linear",
            tickmode="array",
            tickvals=tickvals,
            ticktext=ticktext,
            tickfont=dict(color="#8190ac", size=11),
            range=initial_x_range,
            rangeslider=dict(visible=False),
        ),
        yaxis=dict(
            title=None,
            showgrid=True,
            gridcolor="rgba(77,245,198,0.075)",
            zeroline=False,
            tickfont=dict(color="#8190ac", size=11),
            range=initial_y_range,
        ),
        hovermode="x unified",
        dragmode="pan",
    )
    return fig


def _present_warnings(raw_warnings: list[str]) -> None:
    user_warn = []
    user_error = []

    for w in raw_warnings:
        txt = str(w)
        if "降级为统计学兜底" in txt:
            user_error.append("当前结果已降级为兜底模型，建议稍后重试或检查 Kronos 服务状态。")
            continue
        if "未匹配到 ForecastingPipeline" in txt:
            continue
        if "分位数由" in txt:
            continue
        if "实验性功能" in txt:
            continue
        user_warn.append(txt)

    for msg in dict.fromkeys(user_error):
        st.error(msg)
    for msg in dict.fromkeys(user_warn):
        st.warning(msg)


def _render_empty_state() -> None:
    st.markdown(
        '<div class="ta-empty-state">选择标的后生成未来 3 日区间</div>',
        unsafe_allow_html=True,
    )


def _render_result_summary(resp: dict) -> None:
    symbol = html.escape(str(resp.get("symbol") or "-"))
    trade_date = html.escape(str(resp.get("latest_trade_date") or "-"))
    cache_text = "今日已计算" if resp.get("cache_hit") else "本次新计算"
    st.markdown(
        f"""
<div class="ta-result-bar">
  <div>
    <div class="ta-result-symbol">{symbol}</div>
    <div class="ta-result-meta">数据截止 {trade_date}</div>
  </div>
  <div class="ta-cache-pill">{cache_text}</div>
</div>
""",
        unsafe_allow_html=True,
    )


def _render_interval_cards(preds: list[dict]) -> None:
    cards = []
    for row in preds[:3]:
        step = html.escape(str(row.get("step") or "-"))
        p50 = _format_price(row.get("p50_close"))
        if row.get("core_low_close") is not None and row.get("core_high_close") is not None:
            range_label = f"主情景 {_format_price(row.get('core_low_close'))} - {_format_price(row.get('core_high_close'))}"
        else:
            range_label = f"{_format_price(row.get('p10_close'))} - {_format_price(row.get('p90_close'))}"
        cards.append(
            f"""
<div class="ta-interval-card">
  <div class="ta-interval-step">D+{step}</div>
  <div class="ta-interval-mid">{p50}</div>
  <div class="ta-interval-range">{html.escape(range_label)}</div>
</div>
"""
        )
    st.markdown(f'<div class="ta-interval-grid">{"".join(cards)}</div>', unsafe_allow_html=True)


_inject_finance_theme()

st.markdown(
    """
<div class="ta-hero-shell">
  <h1 class="ta-hero-title">价格区间预测</h1>
  <div class="ta-hero-sub">未来 1-3 个交易日收盘价区间</div>
  <div class="ta-hero-note">基于日线 OHLCV 的实验性数值视角，仅作辅助参考，不构成交易建议。</div>
</div>
""",
    unsafe_allow_html=True,
)

asset_labels = [a.label for a in PILOT_ASSETS]
lookback_options = [60, 90, 120, 180, 240]
lookback_labels = {days: f"应用 {days} 日数据" for days in lookback_options}
default_index = lookback_options.index(DEFAULT_LOOKBACK) if DEFAULT_LOOKBACK in lookback_options else 2

st.markdown('<div class="ta-control-band">', unsafe_allow_html=True)
ctrl_asset, ctrl_window, ctrl_refresh, ctrl_action = st.columns([2.1, 1.35, 1.0, 1.35], vertical_alignment="bottom")
with ctrl_asset:
    selected_asset = st.selectbox("试点标的", asset_labels, index=0, label_visibility="collapsed")
with ctrl_window:
    lookback_window = st.selectbox(
        "回看天数",
        lookback_options,
        format_func=lambda days: lookback_labels.get(int(days), f"应用 {days} 日数据"),
        index=default_index,
        help="模型读取最近 N 个交易日作为输入。N 越大更平稳，N 越小更敏感。",
        label_visibility="collapsed",
    )
with ctrl_refresh:
    force_refresh = st.checkbox("重新计算", value=False)
with ctrl_action:
    run_btn = st.button("生成价格区间", type="primary", use_container_width=True)
st.markdown("</div>", unsafe_allow_html=True)

st.markdown(
    """
<div class="ta-title-row">
  <div class="ta-panel-title" style="margin:0;">预测区间</div>
  <div class="ta-info-icon">?
    <div class="ta-info-tooltip">
      <strong>它怎么来：</strong>Kronos 会读取最近日线 OHLCV，把 K 线序列转成模型可理解的市场 token，再生成未来 1-3 日的多种可能收盘路径。P10/P50/P90 是这些预测价格的百分位：P10 偏低、P50 中位、P90 偏高。<br>
      <strong>怎么看：</strong>亮金区域是预测样本最集中的主情景区间；淡金外带是 P10-P90 尾部风险范围；白线是 P50 中位价格。区间越宽，代表模型越不确定。它是辅助视角，不是确定目标价。
    </div>
  </div>
</div>
""",
    unsafe_allow_html=True,
)
if not run_btn:
    _render_empty_state()
else:
    with st.spinner("正在请求本地预测引擎..."):
        try:
            resp = predict_eod_interval(
                symbol=selected_asset,
                lookback_window=int(lookback_window),
                horizon=3,
                quantiles=[0.1, 0.5, 0.9],
                force_refresh=bool(force_refresh),
                timeout=_request_timeout_seconds(),
            )
        except requests.exceptions.Timeout:
            st.error("本次预测计算时间较长，页面等待已超时。请稍后重试，或取消“重新计算”使用今日缓存结果。")
            resp = None
        except requests.exceptions.RequestException:
            st.error("预测服务暂不可用，请先启动本地服务或稍后重试。")
            resp = None

    if resp:
        if not resp.get("ok"):
            st.error(f"{resp.get('error_code', 'ERROR')}: {resp.get('message', '未知错误')}")
            if resp.get("latest_trade_date"):
                st.caption(f"数据截止交易日：{resp['latest_trade_date']}")
        else:
            _render_result_summary(resp)
            _present_warnings(resp.get("warnings") or [])
            st.plotly_chart(
                _build_chart(resp),
                use_container_width=True,
                config={
                    "scrollZoom": True,
                    "displayModeBar": True,
                    "modeBarButtonsToRemove": ["lasso2d", "select2d"],
                    "modeBarButtonsToAdd": ["pan2d", "zoom2d", "resetScale2d"],
                },
            )
            _render_interval_cards(resp.get("predictions", []))

st.markdown('<div class="ta-bottom-risk">', unsafe_allow_html=True)
with st.expander("边界与风险", expanded=False):
    st.markdown(
        """
- 仅基于日线 `OHLCV` 数据，不覆盖盘中预测。
- 商品主连换月、突发事件行情可能影响稳定性。
- 页面展示的是概率区间，不构成交易建议。
"""
    )
st.markdown("</div>", unsafe_allow_html=True)
