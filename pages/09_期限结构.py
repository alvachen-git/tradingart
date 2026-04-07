import logging
import os
import sys
import time
import html
from typing import Dict, List

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(
    page_title="爱波塔-期限结构",
    page_icon="favicon.ico",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Ensure root imports work from pages/*
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
sys.path.insert(0, root_dir)

from sidebar_navigation import show_navigation  # noqa: E402
import data_engine as de  # noqa: E402
from term_structure_service import (  # noqa: E402
    ANCHOR_LABEL_LATEST,
    ANCHOR_LABEL_MID,
    ANCHOR_LABEL_START,
    build_term_structure_payload,
    build_index_basis_term_structure_payload,
    build_index_basis_longterm_payload,
)

PAGE_NAME = "期限结构"
_PAGE_T0 = time.perf_counter()
_PERF_LOGGER = logging.getLogger(__name__)


def _perf_page_log(
    *,
    page: str,
    render_ms: float = 0.0,
    db_ms: float = 0.0,
    cache_hit: int = -1,
    stage: str = "main",
) -> None:
    msg = (
        f"PERF_PAGE page={page} stage={stage} "
        f"render_ms={render_ms:.1f} db_ms={db_ms:.1f} cache_hit={cache_hit}"
    )
    print(msg)
    _PERF_LOGGER.info(msg)


def _perf_user_id() -> str:
    return str(
        st.session_state.get("username")
        or st.session_state.get("user")
        or st.session_state.get("current_user")
        or "anonymous"
    )


with st.sidebar:
    show_navigation()

css_path = os.path.join(root_dir, "style.css")
if os.path.exists(css_path):
    with open(css_path, encoding="utf-8") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Rajdhani:wght@500;600;700&family=IBM+Plex+Mono:wght@400;500;600&display=swap');

    :root {
        --bg-0: #060d1f;
        --bg-1: #0b1730;
        --line: rgba(120, 149, 204, 0.32);
        --text: #ecf3ff;
        --muted: #9fb0cd;
    }

    .stApp {
        background:
            radial-gradient(1200px 600px at 72% -10%, rgba(51, 108, 201, 0.32), transparent 62%),
            radial-gradient(900px 500px at 10% 0%, rgba(24, 154, 127, 0.18), transparent 58%),
            linear-gradient(150deg, var(--bg-0), var(--bg-1));
        color: var(--text);
        font-family: "Rajdhani", "Noto Sans SC", sans-serif;
    }
    [data-testid="stMainBlockContainer"] {
        max-width: 95rem !important;
        padding-top: 0.8rem;
        padding-bottom: 1.6rem;
    }
    [data-testid="stAppViewContainer"] .main,
    [data-testid="stAppViewContainer"] .main > div,
    [data-testid="stMain"] {
        background: transparent !important;
    }
    [data-testid="stHeader"] {
        background: transparent !important;
    }
    [data-testid="stDecoration"] {
        display: none;
    }
    [data-testid="stMarkdownContainer"],
    [data-testid="stMarkdownContainer"] p,
    [data-testid="stCaptionContainer"] {
        color: var(--muted) !important;
    }
    h1, h2, h3, h4, h5 {
        color: var(--text) !important;
    }

    [data-testid="stAlert"] {
        background: linear-gradient(140deg, rgba(12, 28, 58, 0.88), rgba(9, 22, 47, 0.72)) !important;
        border: 1px solid var(--line) !important;
        color: #93c5fd !important;
    }
    [data-testid="stAlert"] p {
        color: #93c5fd !important;
    }

    .ts-hero {
        border: 1px solid var(--line);
        border-radius: 16px;
        padding: 18px 20px;
        background:
            radial-gradient(900px 320px at 85% -20%, rgba(51, 108, 201, 0.25), transparent 70%),
            linear-gradient(120deg, rgba(12, 26, 54, 0.92), rgba(10, 22, 46, 0.78));
        box-shadow: 0 12px 28px rgba(2, 6, 23, 0.38), inset 0 0 0 1px rgba(125, 211, 252, 0.04);
        margin-bottom: 14px;
    }
    .ts-kicker {
        color: #7dd3fc;
        letter-spacing: .08em;
        font-size: 12px;
        margin-bottom: 6px;
    }
    .ts-title {
        color: #e2e8f0;
        font-size: 34px;
        font-weight: 700;
        margin-bottom: 8px;
        line-height: 1.05;
    }
    .ts-sub {
        color: #94a3b8;
        font-size: 14px;
    }
    .ts-card {
        border: 1px solid var(--line);
        border-radius: 14px;
        padding: 14px 16px;
        background: rgba(9, 22, 45, 0.72);
        box-shadow: inset 0 0 0 1px rgba(125, 211, 252, 0.03);
    }
    .ts-card .label {
        color: #7dd3fc;
        font-size: 12px;
        letter-spacing: .04em;
        margin-bottom: 8px;
    }
    .ts-card .value {
        color: #e2e8f0;
        font-size: 42px;
        font-weight: 700;
        line-height: 1;
    }
    .ts-metric {
        border: 1px solid var(--line);
        border-radius: 12px;
        background: rgba(10, 21, 43, 0.72);
        padding: 12px 14px;
        box-shadow: inset 0 0 0 1px rgba(125, 211, 252, 0.03);
        min-height: 94px;
    }
    .ts-metric .k {
        color: #93c5fd;
        font-size: 12px;
        margin-bottom: 8px;
    }
    .ts-metric .v {
        color: #f8fafc;
        font-size: 24px;
        font-weight: 700;
        line-height: 1.1;
    }
    .ts-footnote {
        color: #94a3b8;
        font-size: 13px;
    }
    .ts-table-wrap {
        margin-top: 10px;
        border: 1px solid var(--line);
        border-radius: 14px;
        overflow: hidden;
        background:
            radial-gradient(900px 260px at 88% -18%, rgba(56, 118, 214, 0.22), transparent 68%),
            linear-gradient(145deg, rgba(8, 20, 43, 0.92), rgba(6, 16, 36, 0.84));
        box-shadow: 0 10px 24px rgba(2, 6, 23, 0.35), inset 0 0 0 1px rgba(147, 197, 253, 0.05);
    }
    .ts-table-scroll {
        width: 100%;
        overflow-x: auto;
    }
    .ts-table {
        width: 100%;
        min-width: 760px;
        border-collapse: collapse;
        font-size: 14px;
        color: #dbeafe;
    }
    .ts-table thead th {
        text-align: center;
        padding: 12px 14px;
        font-weight: 600;
        color: #7dd3fc;
        background: rgba(16, 35, 69, 0.92);
        border-bottom: 1px solid rgba(125, 211, 252, 0.22);
        letter-spacing: 0.03em;
        white-space: nowrap;
    }
    .ts-table tbody td {
        text-align: center;
        padding: 12px 14px;
        border-bottom: 1px solid rgba(96, 165, 250, 0.14);
        white-space: nowrap;
    }
    .ts-table tbody tr:nth-child(odd) td {
        background: rgba(9, 23, 48, 0.62);
    }
    .ts-table tbody tr:nth-child(even) td {
        background: rgba(8, 19, 40, 0.48);
    }
    .ts-table tbody tr:hover td {
        background: rgba(14, 41, 82, 0.75);
        box-shadow: inset 0 0 0 1px rgba(125, 211, 252, 0.15);
    }
    .ts-table .col-contract {
        text-align: center;
        color: #f8fafc;
        font-weight: 700;
        font-family: "IBM Plex Mono", "Consolas", monospace;
    }
    .ts-table .col-price {
        text-align: center;
        font-family: "IBM Plex Mono", "Consolas", monospace;
        color: #bfdbfe;
    }
    .ts-table-empty {
        padding: 14px;
        color: #93c5fd;
        font-size: 13px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

COMMODITIES = {
    "IH": "上证50",
    "IF": "沪深300",
    "IC": "中证500",
    "IM": "中证1000",
    "TS": "2年期国债",
    "T": "10年期国债",
    "TL": "30年期国债",
    "LC": "碳酸锂",
    "SI": "工业硅",
    "PS": "多晶硅",
    "PT": "铂金",
    "PD": "钯金",
    "AU": "黄金",
    "AG": "白银",
    "CU": "沪铜",
    "AL": "沪铝",
    "ZN": "沪锌",
    "NI": "沪镍",
    "SN": "沪锡",
    "PB": "沪铅",
    "RU": "橡胶",
    "BR": "BR橡胶",
    "I": "铁矿石",
    "JM": "焦煤",
    "J": "焦炭",
    "RB": "螺纹钢",
    "HC": "热卷",
    "SP": "纸浆",
    "LG": "原木",
    "AO": "氧化铝",
    "SH": "烧碱",
    "FG": "玻璃",
    "SA": "纯碱",
    "M": "豆粕",
    "A": "豆一",
    "B": "豆二",
    "C": "玉米",
    "LH": "生猪",
    "JD": "鸡蛋",
    "CJ": "红枣",
    "P": "棕榈油",
    "Y": "豆油",
    "OI": "菜油",
    "L": "塑料",
    "PK": "花生",
    "RM": "菜粕",
    "MA": "甲醇",
    "TA": "PTA",
    "PX": "对二甲苯",
    "PR": "瓶片",
    "PP": "聚丙烯",
    "V": "PVC",
    "EB": "苯乙烯",
    "EG": "乙二醇",
    "SS": "不锈钢",
    "AD": "铝合金",
    "BU": "沥青",
    "FU": "燃料油",
    "EC": "集运欧线",
    "UR": "尿素",
    "SR": "白糖",
    "CF": "棉花",
    "AP": "苹果",
}

WINDOW_OPTIONS = {
    "3d": "3交易日",
    "1w": "1周",
    "2w": "2周",
    "1m": "1月",
}

SERIES_ORDER = [ANCHOR_LABEL_START, ANCHOR_LABEL_MID, ANCHOR_LABEL_LATEST]
SERIES_COLORS = {
    ANCHOR_LABEL_START: "#38bdf8",   # cyan
    ANCHOR_LABEL_MID: "#f59e0b",     # amber
    ANCHOR_LABEL_LATEST: "#fb7185",  # rose
}
STOCK_INDEX_FUTURES = {"IF", "IH", "IC", "IM"}


@st.cache_data(ttl=120, show_spinner=False)
def _cached_payload(
    user_id: str,
    page: str,
    product_code: str,
    window_key: str,
    contract_slots: int,
):
    return build_term_structure_payload(
        engine=de.engine,
        product_code=product_code,
        window_key=window_key,
        contract_slots=contract_slots,
    )


@st.cache_data(ttl=120, show_spinner=False)
def _cached_basis_anchor_payload(
    user_id: str,
    page: str,
    product_code: str,
    window_key: str,
    contract_slots: int,
):
    return build_index_basis_term_structure_payload(
        engine=de.engine,
        product_code=product_code,
        window_key=window_key,
        contract_slots=contract_slots,
    )


@st.cache_data(ttl=120, show_spinner=False)
def _cached_basis_longterm_payload(
    user_id: str,
    page: str,
    product_code: str,
    lookback_years: int,
):
    return build_index_basis_longterm_payload(
        engine=de.engine,
        product_code=product_code,
        lookback_years=lookback_years,
    )


def _series_map(series_list: List[Dict]) -> Dict[str, Dict]:
    return {str(s.get("label")): s for s in (series_list or [])}


def _section_title(text_value: str) -> None:
    st.markdown(
        (
            '<div style="margin: 10px 0 6px 0; font-size: 36px; font-weight: 700; '
            'color: #dbeafe; letter-spacing: 0.01em; text-shadow: 0 2px 10px rgba(2,6,23,.45);">'
            f"{html.escape(text_value)}"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


with st.sidebar:
    option_list = [f"{k} - {v}" for k, v in COMMODITIES.items()]
    selected_option = st.selectbox("选择标的", option_list, index=0, key="term_structure_symbol")
    product_code = selected_option.split(" - ")[0].upper()
    window_label = st.selectbox(
        "观察窗口",
        [WINDOW_OPTIONS[k] for k in WINDOW_OPTIONS.keys()],
        index=0,
        key="term_structure_window",
    )
    window_key = next((k for k, v in WINDOW_OPTIONS.items() if v == window_label), "3d")

st.markdown(
    """
    <div class="ts-hero">
      <div class="ts-title">期限结构</div>
      <div class="ts-sub">观察期货不同月份的价格差异</div>
    </div>
    """,
    unsafe_allow_html=True,
)

_db_t0 = time.perf_counter()
payload = _cached_payload(
    _perf_user_id(),
    PAGE_NAME,
    product_code,
    window_key,
    7,
)
_perf_page_log(
    page=PAGE_NAME,
    db_ms=(time.perf_counter() - _db_t0) * 1000,
    cache_hit=1,
    stage="load_term_structure",
)

error = payload.get("error")
if error:
    st.warning(f"当前数据不足，无法生成期限结构（{error}）。")
    st.stop()

contracts: List[str] = payload.get("contracts", [])
series_list: List[Dict] = payload.get("series", [])
summary: Dict = payload.get("summary", {})
meta: Dict = payload.get("meta", {})
anchors: List[Dict] = payload.get("anchors", [])
series_by_label = _series_map(series_list)

if not contracts or not series_list:
    st.warning("当前品种暂无可展示的期限结构数据。")
    st.stop()
fig = go.Figure()
visible_series_count = 0
invisible_labels: list[str] = []
for label in SERIES_ORDER:
    s = series_by_label.get(label)
    if not s:
        continue
    name = f"{s.get('display_date', s.get('trade_date', ''))} · {label}"
    points = s.get("points", [])
    y_values = [p.get("close_price") for p in points]
    if all(v is None for v in y_values):
        invisible_labels.append(label)
        continue
    visible_series_count += 1
    fig.add_trace(
        go.Scatter(
            x=[str(x) for x in contracts],
            y=y_values,
            mode="lines+markers",
            name=name,
            line=dict(color=SERIES_COLORS.get(label, "#4b5563"), width=3.5, shape="linear"),
            marker=dict(
                size=8,
                symbol="circle",
                color="#f8fafc",
                line=dict(width=1.8, color=SERIES_COLORS.get(label, "#4b5563")),
            ),
            connectgaps=False,
        )
    )

if visible_series_count < 3 and invisible_labels:
    st.warning(f"以下锚点日期暂无有效数据，未绘制曲线：{', '.join(invisible_labels)}")

fig.update_layout(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="#070f1f",
    height=500,
    margin=dict(l=20, r=20, t=18, b=28),
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=-0.30,
        xanchor="center",
        x=0.5,
        font=dict(size=14, color="#dbeafe"),
    ),
    font=dict(color="#cbd5e1"),
    annotations=[
        dict(
            x=0.01,
            y=0.98,
            xref="paper",
            yref="paper",
            showarrow=False,
            align="left",
            text=f"{product_code} · {COMMODITIES.get(product_code, product_code)}",
            font=dict(size=15, color="#e2e8f0"),
        )
    ],
)
fig.update_xaxes(
    title="\u5408\u7ea6\u6708\u4efd (YYMM)",
    showgrid=False,
    showline=True,
    linecolor="rgba(56, 189, 248, 0.35)",
    tickfont=dict(color="#bfdbfe", size=13),
    title_font=dict(color="#93c5fd", size=20),
    type="category",
    categoryorder="array",
    categoryarray=[str(x) for x in contracts],
)
fig.update_yaxes(
    title="\u6536\u76d8\u4ef7",
    showgrid=True,
    gridcolor="rgba(148, 163, 184, 0.22)",
    zeroline=True,
    zerolinecolor="rgba(56, 189, 248, 0.26)",
    tickfont=dict(color="#bfdbfe", size=13),
    title_font=dict(color="#93c5fd", size=20),
)
st.plotly_chart(fig, use_container_width=True)

if product_code in STOCK_INDEX_FUTURES:
    _section_title("\u80a1\u6307\u5347\u8d34\u6c34\u671f\u9650\u7ed3\u6784")
    _db_t1 = time.perf_counter()
    basis_payload = _cached_basis_anchor_payload(
        _perf_user_id(),
        PAGE_NAME,
        product_code,
        window_key,
        7,
    )
    _perf_page_log(
        page=PAGE_NAME,
        db_ms=(time.perf_counter() - _db_t1) * 1000,
        cache_hit=1,
        stage="load_basis_anchor",
    )
    basis_error = basis_payload.get("error")
    basis_contracts: List[str] = basis_payload.get("contracts", [])
    basis_series_list: List[Dict] = basis_payload.get("series", [])
    basis_series_by_label = _series_map(basis_series_list)
    if basis_error:
        st.warning(f"\u80a1\u6307\u5347\u8d34\u6c34\u671f\u9650\u7ed3\u6784\u6682\u4e0d\u53ef\u7528\uff08{basis_error}\uff09\u3002")
    elif not basis_contracts or not basis_series_list:
        st.warning("\u80a1\u6307\u5347\u8d34\u6c34\u671f\u9650\u7ed3\u6784\u6682\u65e0\u53ef\u5c55\u793a\u6570\u636e\u3002")
    else:
        fig_basis = go.Figure()
        basis_visible_series_count = 0
        basis_invisible_labels: list[str] = []
        for label in SERIES_ORDER:
            s = basis_series_by_label.get(label)
            if not s:
                continue
            name = f"{s.get('display_date', s.get('trade_date', ''))} | {label}"
            points = s.get("points", [])
            y_values = [p.get("basis") for p in points]
            if all(v is None for v in y_values):
                basis_invisible_labels.append(label)
                continue
            basis_visible_series_count += 1
            fig_basis.add_trace(
                go.Scatter(
                    x=[str(x) for x in basis_contracts],
                    y=y_values,
                    mode="lines+markers",
                    name=name,
                    line=dict(color=SERIES_COLORS.get(label, "#4b5563"), width=3.5, shape="linear"),
                    marker=dict(
                        size=8,
                        symbol="circle",
                        color="#f8fafc",
                        line=dict(width=1.8, color=SERIES_COLORS.get(label, "#4b5563")),
                    ),
                    customdata=[[p.get("futures_close"), p.get("spot_close")] for p in points],
                    hovertemplate=(
                        "\u5408\u7ea6 %{x}<br>"
                        "\u5347\u8d34\u6c34 %{y:.2f}<br>"
                        "\u671f\u8d27 %{customdata[0]}<br>"
                        "\u73b0\u8d27 %{customdata[1]}<extra></extra>"
                    ),
                    connectgaps=False,
                )
            )
        if basis_visible_series_count < 3 and basis_invisible_labels:
            st.warning(
                "\u4ee5\u4e0b\u951a\u70b9\u65e5\u671f\u6682\u65e0\u6709\u6548\u5347\u8d34\u6c34\u6570\u636e\uff0c\u672a\u7ed8\u5236\u66f2\u7ebf\uff1a"
                + ", ".join(basis_invisible_labels)
            )
        fig_basis.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="#070f1f",
            height=500,
            margin=dict(l=20, r=20, t=18, b=28),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.30,
                xanchor="center",
                x=0.5,
                font=dict(size=14, color="#dbeafe"),
            ),
            font=dict(color="#cbd5e1"),
            annotations=[
                dict(
                    x=0.01,
                    y=0.98,
                    xref="paper",
                    yref="paper",
                    showarrow=False,
                    align="left",
                    text=f"{product_code} | {COMMODITIES.get(product_code, product_code)} | \u5347\u8d34\u6c34",
                    font=dict(size=15, color="#e2e8f0"),
                )
            ],
        )
        fig_basis.update_xaxes(
            title="\u5408\u7ea6\u6708\u4efd (YYMM)",
            showgrid=False,
            showline=True,
            linecolor="rgba(56, 189, 248, 0.35)",
            tickfont=dict(color="#bfdbfe", size=13),
            title_font=dict(color="#93c5fd", size=20),
            type="category",
            categoryorder="array",
            categoryarray=[str(x) for x in basis_contracts],
        )
        fig_basis.update_yaxes(
            title="\u5347\u8d34\u6c34 (\u671f\u8d27-\u73b0\u8d27)",
            showgrid=True,
            gridcolor="rgba(148, 163, 184, 0.22)",
            zeroline=True,
            zerolinecolor="rgba(56, 189, 248, 0.26)",
            tickfont=dict(color="#bfdbfe", size=13),
            title_font=dict(color="#93c5fd", size=20),
        )
        st.plotly_chart(fig_basis, use_container_width=True)
    _section_title("\u80a1\u6307\u8fd1\u6708\u5347\u8d34\u6c34\uff08\u6700\u8fd11\u5e74\uff09")
    _db_t2 = time.perf_counter()
    long_payload = _cached_basis_longterm_payload(
        _perf_user_id(),
        PAGE_NAME,
        product_code,
        1,
    )
    _perf_page_log(
        page=PAGE_NAME,
        db_ms=(time.perf_counter() - _db_t2) * 1000,
        cache_hit=1,
        stage="load_basis_longterm",
    )
    long_error = long_payload.get("error")
    long_points: List[Dict] = long_payload.get("points", [])
    if long_error and not long_points:
        st.warning(f"\u80a1\u6307\u8fd1\u6708\u5347\u8d34\u6c34\u957f\u671f\u56fe\u6682\u4e0d\u53ef\u7528\uff08{long_error}\uff09\u3002")
    elif not long_points:
        st.warning("\u80a1\u6307\u8fd1\u6708\u5347\u8d34\u6c34\u957f\u671f\u56fe\u6682\u65e0\u53ef\u5c55\u793a\u6570\u636e\u3002")
    else:
        fig_long = go.Figure()
        fig_long.add_trace(
            go.Scatter(
                x=[p.get("display_date", p.get("trade_date")) for p in long_points],
                y=[p.get("basis") for p in long_points],
                mode="lines+markers",
                name=f"{product_code} \u8fd1\u6708\u5347\u8d34\u6c34",
                line=dict(color="#22d3ee", width=2.6, shape="linear"),
                marker=dict(size=5, color="#f8fafc", line=dict(width=1.2, color="#22d3ee")),
                customdata=[
                    [p.get("contract"), p.get("futures_close"), p.get("spot_close")]
                    for p in long_points
                ],
                hovertemplate=(
                    "\u65e5\u671f %{x}<br>"
                    "\u5347\u8d34\u6c34 %{y:.2f}<br>"
                    "\u8fd1\u6708\u5408\u7ea6 %{customdata[0]}<br>"
                    "\u671f\u8d27 %{customdata[1]}<br>"
                    "\u73b0\u8d27 %{customdata[2]}<extra></extra>"
                ),
                connectgaps=False,
            )
        )
        fig_long.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="#070f1f",
            height=460,
            margin=dict(l=20, r=20, t=18, b=28),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.30,
                xanchor="center",
                x=0.5,
                font=dict(size=14, color="#dbeafe"),
            ),
            font=dict(color="#cbd5e1"),
            annotations=[
                dict(
                    x=0.01,
                    y=0.98,
                    xref="paper",
                    yref="paper",
                    showarrow=False,
                    align="left",
                    text=f"{product_code} | \u8fd1\u6708\u5347\u8d34\u6c34 | \u6700\u8fd11\u5e74",
                    font=dict(size=15, color="#e2e8f0"),
                )
            ],
        )
        fig_long.update_xaxes(
            title="\u4ea4\u6613\u65e5",
            showgrid=False,
            showline=True,
            linecolor="rgba(56, 189, 248, 0.35)",
            tickfont=dict(color="#bfdbfe", size=12),
            title_font=dict(color="#93c5fd", size=18),
            type="category",
        )
        fig_long.update_yaxes(
            title="\u5347\u8d34\u6c34 (\u671f\u8d27-\u73b0\u8d27)",
            showgrid=True,
            gridcolor="rgba(148, 163, 184, 0.22)",
            zeroline=True,
            zerolinecolor="rgba(56, 189, 248, 0.26)",
            tickfont=dict(color="#bfdbfe", size=13),
            title_font=dict(color="#93c5fd", size=18),
        )
        st.plotly_chart(fig_long, use_container_width=True)
        if long_error:
            st.info(f"\u957f\u671f\u56fe\u5b58\u5728\u90e8\u5206\u65e5\u671f\u7f3a\u5931\uff08{long_error}\uff09\uff0c\u5df2\u5c55\u793a\u53ef\u7528\u6570\u636e\u3002")
structure_type_map = {
    "Contango": "正向市场",
    "Backwardation": "反向市场",
    "Flat": "平坦",
    "InsufficientData": "数据不足",
}
structure_type = structure_type_map.get(summary.get("structure_type"), str(summary.get("structure_type") or "--"))
spread_abs = summary.get("spread_abs")
spread_pct = summary.get("spread_pct")
slope = summary.get("slope_per_step")

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(
        f"""
        <div class="ts-metric">
          <div class="k">结构类型</div>
          <div class="v">{structure_type}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
with c2:
    v = "--" if spread_abs is None else f"{spread_abs:,.2f}"
    st.markdown(
        f"""
        <div class="ts-metric">
          <div class="k">近远月价差</div>
          <div class="v">{v}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
with c3:
    v = "--" if spread_pct is None else f"{spread_pct * 100:.2f}%"
    st.markdown(
        f"""
        <div class="ts-metric">
          <div class="k">近远月价差%</div>
          <div class="v">{v}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
with c4:
    v = "--" if slope is None else f"{slope:,.2f}"
    st.markdown(
        f"""
        <div class="ts-metric">
          <div class="k">每档斜率</div>
          <div class="v">{v}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

table_rows = []
for month in contracts:
    row = {"合约月份": month}
    for label in SERIES_ORDER:
        s = series_by_label.get(label, {})
        p = next((x for x in s.get("points", []) if x.get("contract") == month), None)
        col = f"{label}({s.get('display_date', s.get('trade_date', ''))})"
        row[col] = None if not p else p.get("close_price")
    table_rows.append(row)

df_table = pd.DataFrame(table_rows)
display_df = df_table.copy()
if not display_df.empty:
    price_cols = [c for c in display_df.columns if c != "合约月份"]
    for col in price_cols:
        display_df[col] = display_df[col].map(
            lambda x: "--" if pd.isna(x) else f"{float(x):,.2f}"
        )

    header_html = "".join(f"<th>{html.escape(str(col))}</th>" for col in display_df.columns)
    rows_html = "".join(
        "<tr>"
        + "".join(
            f'<td class="col-contract">{html.escape(str(val))}</td>'
            if idx == 0
            else f'<td class="col-price">{html.escape(str(val))}</td>'
            for idx, val in enumerate(row)
        )
        + "</tr>"
        for row in display_df.itertuples(index=False, name=None)
    )
    st.markdown(
        f"""
        <div class="ts-table-wrap">
          <div class="ts-table-scroll">
            <table class="ts-table">
              <thead><tr>{header_html}</tr></thead>
              <tbody>{rows_html}</tbody>
            </table>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
else:
    st.markdown('<div class="ts-table-empty">暂无可展示数据</div>', unsafe_allow_html=True)

_perf_page_log(
    page=PAGE_NAME,
    render_ms=(time.perf_counter() - _PAGE_T0) * 1000,
    cache_hit=-1,
    stage="page_done",
)

