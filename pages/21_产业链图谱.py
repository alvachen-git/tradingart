import os
import sys
from html import escape

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from industry_chain_tools import get_chain_snapshot, get_recent_screener_dates, load_chain_templates, scale_flow_width
from sidebar_navigation import show_navigation
from ui_components import inject_quant_ops_header_style, render_quant_ops_header

st.set_page_config(
    page_title="产业链图谱",
    page_icon="favicon.ico",
    layout="wide",
    initial_sidebar_state="expanded",
)

with st.sidebar:
    show_navigation()


def _inject_chain_page_style() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Rajdhani:wght@500;600;700&family=IBM+Plex+Mono:wght@400;500;600&display=swap');

        :root {
            --chain-bg-0: #060d1e;
            --chain-bg-1: #0c1933;
            --chain-card: rgba(12, 25, 52, 0.76);
            --chain-card-strong: rgba(11, 26, 58, 0.90);
            --chain-line: rgba(120, 149, 204, 0.32);
            --chain-text: #eaf1ff;
            --chain-muted: #9fb0cd;
            --chain-accent: #60a5fa;
        }

        .stApp {
            background:
                radial-gradient(1200px 600px at 72% -10%, rgba(51, 108, 201, 0.30), transparent 62%),
                radial-gradient(900px 500px at 10% 0%, rgba(24, 154, 127, 0.16), transparent 58%),
                linear-gradient(150deg, var(--chain-bg-0), var(--chain-bg-1));
            color: var(--chain-text);
            font-family: "Rajdhani", "Noto Sans SC", sans-serif;
        }
        [data-testid="stMainBlockContainer"] {
            max-width: 95rem !important;
            padding-top: 0.7rem;
            padding-bottom: 1.8rem;
        }
        [data-testid="stHeader"] {
            background: transparent !important;
        }
        [data-testid="stDecoration"] {
            display: none !important;
        }
        h1, h2, h3, h4, p, label, .stCaption {
            color: var(--chain-text) !important;
        }
        [data-testid="stMarkdownContainer"] p {
            color: var(--chain-muted);
        }

        div[data-baseweb="select"] > div {
            background: var(--chain-card-strong) !important;
            border: 1px solid var(--chain-line) !important;
            border-radius: 12px !important;
            box-shadow: 0 10px 24px rgba(2, 8, 24, 0.32) !important;
        }
        div[data-baseweb="select"] * {
            color: #e6efff !important;
        }
        [data-testid="stSelectbox"] label p {
            color: #b7c8e6 !important;
            font-weight: 600 !important;
            letter-spacing: 0.03em !important;
        }

        .chain-meta-right {
            text-align: right;
            color: #9fb0cd;
            font-family: "IBM Plex Mono", "Noto Sans SC", monospace;
            font-size: 12px;
            letter-spacing: 0.02em;
            margin-bottom: 2px;
        }
        .chain-stage-title {
            margin: 16px 0 8px 0;
            padding: 8px 12px;
            border: 1px solid var(--chain-line);
            border-radius: 12px;
            background: linear-gradient(110deg, rgba(11, 30, 63, 0.86), rgba(8, 24, 50, 0.60));
            color: #eaf1ff !important;
            font-size: 24px;
            font-weight: 700;
            letter-spacing: 0.03em;
            box-shadow: inset 0 1px 0 rgba(255,255,255,0.05);
        }

        [data-testid="stDataFrame"] {
            border: 1px solid var(--chain-line) !important;
            border-radius: 14px !important;
            overflow: hidden !important;
            box-shadow: 0 16px 34px rgba(2, 8, 24, 0.26) !important;
            background: rgba(8, 22, 46, 0.80) !important;
        }
        [data-testid="stDataFrame"] [role="columnheader"] {
            background: rgba(15, 33, 67, 0.96) !important;
            color: #b6c8e9 !important;
            border-bottom: 1px solid var(--chain-line) !important;
            font-family: "IBM Plex Mono", "Noto Sans SC", monospace !important;
            font-weight: 600 !important;
        }
        [data-testid="stDataFrame"] [role="gridcell"] {
            background: rgba(9, 19, 39, 0.78) !important;
            color: #dce8ff !important;
            border-bottom: 1px solid rgba(120, 149, 204, 0.12) !important;
        }
        [data-testid="stDataFrame"] [role="row"]:hover [role="gridcell"] {
            background: rgba(52, 108, 196, 0.15) !important;
        }

        .chain-table-wrap {
            border: 1px solid var(--chain-line);
            border-radius: 14px;
            overflow: auto;
            max-height: 460px;
            background: rgba(9, 19, 39, 0.80);
            box-shadow: 0 16px 34px rgba(2, 8, 24, 0.26), inset 0 1px 0 rgba(255,255,255,0.04);
        }
        .chain-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 14px;
            color: #dce8ff;
            min-width: 1140px;
        }
        .chain-table thead th {
            position: sticky;
            top: 0;
            z-index: 2;
            background: linear-gradient(180deg, rgba(19, 42, 82, 0.98), rgba(13, 32, 66, 0.96));
            color: #b6c8e9;
            text-align: left;
            font-family: "IBM Plex Mono", "Noto Sans SC", monospace;
            font-weight: 600;
            letter-spacing: 0.03em;
            border-bottom: 1px solid var(--chain-line);
            border-right: 1px solid rgba(120, 149, 204, 0.12);
            padding: 9px 10px;
            white-space: nowrap;
        }
        .chain-table thead th:last-child {
            border-right: 0;
        }
        .chain-table tbody td {
            border-bottom: 1px solid rgba(120, 149, 204, 0.10);
            border-right: 1px solid rgba(120, 149, 204, 0.08);
            padding: 8px 10px;
            line-height: 1.24;
            vertical-align: top;
            background: rgba(7, 18, 37, 0.64);
        }
        .chain-table tbody td:last-child {
            border-right: 0;
        }
        .chain-table tbody tr:nth-child(even) td {
            background: rgba(10, 25, 50, 0.70);
        }
        .chain-table tbody tr:hover td {
            background: rgba(37, 99, 235, 0.20);
        }
        .chain-table .mono {
            font-family: "IBM Plex Mono", "Noto Sans SC", monospace;
            letter-spacing: 0.01em;
            white-space: nowrap;
        }
        .chain-table .company-col {
            white-space: nowrap;
            max-width: 150px;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        .chain-table .num {
            text-align: right;
            white-space: nowrap;
            font-family: "IBM Plex Mono", "Noto Sans SC", monospace;
        }
        .chain-table .pattern {
            max-width: 420px;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        .chain-table .tag-cell {
            min-width: 220px;
            white-space: nowrap;
        }
        .chain-table .tag-chip {
            display: inline-block;
            margin: 2px 6px 2px 0;
            padding: 2px 8px;
            border-radius: 999px;
            border: 1px solid rgba(96, 165, 250, 0.46);
            background: rgba(44, 110, 214, 0.18);
            color: #dbeafe;
            font-size: 12px;
            line-height: 1.2;
            font-weight: 600;
            letter-spacing: 0.01em;
        }
        .chain-table .tag-chip-empty {
            border-color: rgba(148, 163, 184, 0.42);
            background: rgba(71, 85, 105, 0.24);
            color: #cbd5e1;
        }
        .chain-table .tag-detail-hint {
            display: inline-block;
            margin-left: 6px;
            color: #93c5fd;
            font-size: 11px;
            opacity: 0.82;
        }
        .chain-table .pos {
            color: #ef4444;
            font-weight: 700;
        }
        .chain-table .neg {
            color: #22c55e;
            font-weight: 700;
        }
        .chain-table .flat {
            color: #c8d6ef;
            font-weight: 600;
        }
        .chain-table .sig-in {
            color: #fda4af;
            font-weight: 700;
        }
        .chain-table .sig-out {
            color: #86efac;
            font-weight: 700;
        }
        .chain-table .sig-mid {
            color: #fbbf24;
            font-weight: 700;
        }

        [data-testid="stExpander"] details {
            border: 1px solid var(--chain-line) !important;
            border-radius: 12px !important;
            background: rgba(10, 23, 48, 0.56) !important;
        }
        [data-testid="stExpander"] summary {
            color: #d6e4ff !important;
            font-weight: 600;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

def _pick_scale_mode(scale_mode_label: str) -> str:
    mapping = {"对数": "log", "线性": "linear", "分档": "bucket"}
    return mapping.get(scale_mode_label, "log")


def _build_flow_bubble_chart(snapshot: dict, flow_window: str, scale_mode: str) -> go.Figure:
    stages = snapshot.get("stages", [])
    meta = snapshot.get("meta", {})
    if not stages:
        fig = go.Figure()
        fig.update_layout(height=420)
        return fig

    stage_names = [str(s.get("name") or "") for s in stages]
    stage_x = list(range(len(stage_names)))
    net_1d = [float(s.get("net_flow_1d") or 0.0) for s in stages]
    net_5d = [float(s.get("net_flow_5d") or 0.0) for s in stages]
    ext_in = [float(s.get("flow_in_external") or 0.0) for s in stages]
    ext_out = [float(s.get("flow_out_external") or 0.0) for s in stages]
    company_cnt = [float(s.get("company_count") or 0.0) for s in stages]

    history_dates = [str(x) for x in (meta.get("fund_history_dates") or []) if str(x).strip()]
    period_defs = []
    if flow_window == "5D":
        if not history_dates and meta.get("fund_trade_date"):
            history_dates = [str(meta.get("fund_trade_date"))]
        history_dates = history_dates[:3]
        for idx, d in enumerate(history_dates):
            vals = []
            for stage in stages:
                if idx == 0:
                    vals.append(float(stage.get("net_flow_5d") or 0.0))
                else:
                    hist_map = {
                        str(x.get("trade_date") or ""): float(x.get("net_flow_5d") or 0.0)
                        for x in (stage.get("net_flow_5d_history") or [])
                    }
                    vals.append(float(hist_map.get(d, 0.0)))
            period_defs.append(
                {
                    "label": "当日主力资金流" if idx == 0 else f"{idx}天前主力资金流",
                    "trade_date": d,
                    "values": vals,
                    "alpha": [0.90, 0.62, 0.46][min(idx, 2)],
                    "size_scale": [1.0, 0.86, 0.74][min(idx, 2)],
                    "symbol": ["circle", "circle-open", "diamond-open"][min(idx, 2)],
                    "hollow": idx > 0,
                    "draw_order": 2 - min(idx, 2),
                    "show_text": idx == 0,
                }
            )
    else:
        period_defs.append(
            {
                "label": "当日主力资金流",
                "trade_date": str(meta.get("fund_trade_date") or ""),
                "values": net_1d,
                "alpha": 0.88,
                "size_scale": 1.0,
                "symbol": "circle",
                "hollow": False,
                "draw_order": 2,
                "show_text": True,
            }
        )

    abs_all = []
    for p in period_defs:
        abs_all.extend(abs(float(v)) for v in p["values"])

    mode = _pick_scale_mode(scale_mode)
    transformed = [scale_flow_width(v, mode=mode) for v in abs_all]
    max_t = max(transformed) if transformed else 0.0

    def _bubble_size(v_abs: float, size_scale: float) -> float:
        if max_t <= 0:
            return 16.0 * size_scale
        t = scale_flow_width(v_abs, mode=mode)
        return (14.0 + 50.0 * (t / max_t)) * size_scale

    fig = go.Figure()
    render_defs = sorted(period_defs, key=lambda x: int(x.get("draw_order", 0)))
    for p in render_defs:
        values = [float(v) for v in p["values"]]
        sizes = [_bubble_size(abs(v), float(p["size_scale"])) for v in values]
        colors = []
        line_colors = []
        is_hollow = bool(p.get("hollow", False))
        for v in values:
            if v > 0:
                if is_hollow:
                    colors.append("rgba(251,113,133,0.98)")
                    line_colors.append("rgba(251,113,133,0.98)")
                else:
                    colors.append(f"rgba(220,38,38,{float(p['alpha']):.3f})")
                    line_colors.append("rgba(253,164,175,0.92)")
            elif v < 0:
                if is_hollow:
                    colors.append("rgba(74,222,128,0.98)")
                    line_colors.append("rgba(74,222,128,0.98)")
                else:
                    colors.append(f"rgba(22,163,74,{float(p['alpha']):.3f})")
                    line_colors.append("rgba(134,239,172,0.92)")
            else:
                if is_hollow:
                    colors.append("rgba(203,213,225,0.92)")
                    line_colors.append("rgba(203,213,225,0.92)")
                else:
                    colors.append(f"rgba(107,114,128,{max(0.22, float(p['alpha']) * 0.8):.3f})")
                    line_colors.append("rgba(203,213,225,0.9)")

        text_vals = [f"{v / 10000.0:+.2f}亿" for v in values] if p["show_text"] else [""] * len(values)
        mode_text = "markers+text" if p["show_text"] else "markers"
        x_vals = stage_x
        customdata = [
            [
                str(p["trade_date"]),
                net_1d[i],
                net_5d[i],
                values[i],
                ext_in[i],
                ext_out[i],
                company_cnt[i],
                abs(values[i]),
                stage_names[i],
            ]
            for i in range(len(stages))
        ]
        fig.add_trace(
            go.Scatter(
                x=x_vals,
                y=values,
                mode=mode_text,
                text=text_vals,
                textposition="top center",
                textfont=dict(size=12, color="#dbeafe"),
                name=f"{p['label']} ({p['trade_date']})",
                marker=dict(
                    size=sizes,
                    symbol=str(p["symbol"]),
                    color=colors,
                    line=dict(color=line_colors, width=1.8),
                ),
                customdata=customdata,
                hovertemplate=(
                    "环节: %{customdata[8]}<br>"
                    "口径日期: %{customdata[0]}<br>"
                    "该期净流(万元): %{customdata[3]:,.2f}<br>"
                    "当日1D净流(万元): %{customdata[1]:,.2f}<br>"
                    "当日5D净流(万元): %{customdata[2]:,.2f}<br>"
                    "链外流入(万元): %{customdata[4]:,.2f}<br>"
                    "链外流出(万元): %{customdata[5]:,.2f}<br>"
                    "环节公司数: %{customdata[6]:,.0f}<br>"
                    "气泡金额基数(|净流|,万元): %{customdata[7]:,.2f}<extra></extra>"
                ),
            )
        )

    fig.update_layout(
        height=460,
        margin=dict(l=8, r=8, t=56, b=8),
        font=dict(color="#dbeafe", family='"Rajdhani", "Noto Sans SC", sans-serif'),
        xaxis=dict(
            title="产业链环节",
            tickfont=dict(size=13, color="#dbeafe"),
            tickmode="array",
            tickvals=stage_x,
            ticktext=stage_names,
            range=[-0.6, max(stage_x) + 0.6] if stage_x else None,
            gridcolor="rgba(120,149,204,0.20)",
            linecolor="rgba(120,149,204,0.34)",
        ),
        yaxis=dict(
            title=f"{flow_window}主力净流（万元）",
            tickfont=dict(size=12, color="#dbeafe"),
            zeroline=True,
            zerolinewidth=1.4,
            zerolinecolor="rgba(176,190,212,0.80)",
            gridcolor="rgba(120,149,204,0.20)",
            linecolor="rgba(120,149,204,0.34)",
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.03,
            x=0.0,
            font=dict(size=14, color="#eef4ff"),
            bgcolor="rgba(6, 15, 33, 0.72)",
            bordercolor="rgba(120,149,204,0.42)",
            borderwidth=1,
        ),
        plot_bgcolor="rgba(9, 21, 44, 0.75)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    fig.add_hline(y=0, line_color="rgba(176,190,212,0.80)", line_width=1.2)
    return fig


@st.cache_data(ttl=1800)
def _get_snapshot_cached(
    sector_name: str, limit_per_stage: int, screener_trade_date: str, flow_window: str, cache_version: str = "v2"
) -> dict:
    _ = cache_version
    force_date = screener_trade_date if screener_trade_date and screener_trade_date != "AUTO" else None
    return get_chain_snapshot(
        sector_name=sector_name,
        limit_per_stage=limit_per_stage,
        force_screener_trade_date=force_date,
        flow_window=flow_window,
    )


def _render_stage(stage: dict):
    st.markdown(
        f"<div class='chain-stage-title'>{stage.get('name', '-')}</div>",
        unsafe_allow_html=True,
    )
    companies = stage.get("companies") or []
    if not companies:
        st.info("暂无公司数据")
        return

    rows = []
    for c in companies:
        tech_label = c.get("pattern") or c.get("ma_trend") or "待更新"
        raw_tags = c.get("domain_tags") or []
        if not raw_tags and c.get("domain_tags_text"):
            raw_tags = [
                x.strip()
                for x in str(c.get("domain_tags_text") or "").replace("/", "|").split("|")
                if x.strip()
            ]
        slim_tags = [str(x).strip() for x in raw_tags if str(x).strip()][:3]
        rows.append(
            {
                "代码": c.get("ts_code", ""),
                "公司": c.get("name", ""),
                "总市值(亿元)": float(c.get("market_cap") or 0.0) / 10000.0,
                "擅长业务领域标签": slim_tags,
                "技术形态": tech_label,
                "形态分": int(c.get("score") or 0),
                "主力净流(1D, 万)": float(c.get("main_net_amount_1d") or 0.0),
                "主力净流(5D, 万)": float(c.get("main_net_amount_5d") or 0.0),
                "资金信号": c.get("fund_signal") or "待更新",
            }
        )

    def _tone_class_num(v: float) -> str:
        if v > 0:
            return "pos"
        if v < 0:
            return "neg"
        return "flat"

    def _tone_signal(v: str) -> str:
        s = str(v or "")
        if s == "持续流入":
            return "sig-in"
        if s == "反抽修复":
            return "sig-mid"
        if s == "短线分歧":
            return "sig-mid"
        if s == "持续流出":
            return "sig-out"
        return "flat"

    table_rows = []
    for r in rows:
        code = escape(str(r["代码"]))
        name = escape(str(r["公司"]))
        tags = r["擅长业务领域标签"] or []
        if tags:
            tag_html = "".join(
                [f"<span class='tag-chip'>{escape(str(tag))}</span>" for tag in tags]
            )
        else:
            tag_html = "<span class='tag-chip tag-chip-empty'>待补全</span>"
        pattern_raw = str(r["技术形态"])
        pattern = escape(pattern_raw)
        score = int(r["形态分"])
        mv = float(r["总市值(亿元)"])
        f1 = float(r["主力净流(1D, 万)"])
        f5 = float(r["主力净流(5D, 万)"])
        signal = escape(str(r["资金信号"]))

        table_rows.append(
            (
                "<tr>"
                f"<td class='mono'>{code}</td>"
                f"<td class='company-col' title='{name}'>{name}</td>"
                f"<td class='num mono'>{mv:,.2f}</td>"
                f"<td class='tag-cell'>{tag_html}</td>"
                f"<td class='pattern' title='{pattern}'>{pattern}</td>"
                f"<td class='num mono'>{score}</td>"
                f"<td class='num mono {_tone_class_num(f1)}'>{f1:,.2f}</td>"
                f"<td class='num mono {_tone_class_num(f5)}'>{f5:,.2f}</td>"
                f"<td class='{_tone_signal(signal)}'>{signal}</td>"
                "</tr>"
            )
        )

    table_html = (
        "<div class='chain-table-wrap'>"
        "<table class='chain-table'>"
        "<thead><tr>"
        "<th>代码</th>"
        "<th>公司</th>"
        "<th>总市值(亿元)</th>"
        "<th>擅长业务领域</th>"
        "<th>技术形态</th>"
        "<th>形态分</th>"
        "<th>主力净流(1D, 万)</th>"
        "<th>主力净流(5D, 万)</th>"
        "<th>资金信号</th>"
        "</tr></thead>"
        f"<tbody>{''.join(table_rows)}</tbody>"
        "</table>"
        "</div>"
    )
    st.markdown(table_html, unsafe_allow_html=True)

    with st.expander("查看业务详情", expanded=False):
        for c in companies:
            detail_main = (c.get("main_business") or "").strip()
            detail_scope = (c.get("business_scope") or "").strip()
            tech_highlights = c.get("tech_highlights") or []
            customer_profile = (c.get("customer_profile") or "").strip()
            moat_note = (c.get("moat_note") or "").strip()
            boundary_risk = (c.get("boundary_risk") or "").strip()
            insight_text = (c.get("domain_insight_text") or "").strip()
            st.markdown(f"**{c.get('name', '')} ({c.get('ts_code', '')})**")
            st.caption(f"业务标签: {c.get('domain_tags_text') or '待补全'}")
            if insight_text:
                st.write(f"精华说明: {insight_text}")
            if tech_highlights:
                st.write(f"技术要点: {'；'.join([str(x) for x in tech_highlights if str(x).strip()])}")
            if customer_profile:
                st.write(f"客户属性: {customer_profile}")
            if moat_note:
                st.write(f"护城河: {moat_note}")
            if boundary_risk:
                st.write(f"边界风险: {boundary_risk}")
            if detail_main:
                st.write(f"主营: {detail_main[:300]}")
            if detail_scope:
                st.write(f"经营范围: {detail_scope[:500]}")
            st.markdown("---")


def main():
    _inject_chain_page_style()
    inject_quant_ops_header_style()

    templates = load_chain_templates()
    sectors = sorted(templates.keys())
    screener_dates = get_recent_screener_dates(limit=10)
    date_options = ["AUTO"] + screener_dates

    render_quant_ops_header(
        title="产业链图谱",
        subtitle="",
        note="展示口径：A股环节聚合主力净流（万元），颜色区分流入流出，气泡大小映射资金绝对值。",
    )

    col1, col2, col3, col4 = st.columns([1.2, 1, 1, 1])
    with col1:
        selected_sector = st.selectbox(
            "板块",
            options=sectors,
            index=sectors.index("半导体") if "半导体" in sectors else 0,
        )
    with col2:
        limit_per_stage = st.selectbox("每环节公司数", options=[5, 10, 15, 20], index=1)
    with col3:
        default_date_idx = 1 if "20260320" in date_options else 0
        screener_trade_date = st.selectbox(
            "技术形态日期",
            options=date_options,
            index=default_date_idx,
            help="AUTO=自动选完整交易日；也可手工指定日期。",
        )
    with col4:
        scale_mode = st.selectbox("缩放", options=["对数", "线性", "分档"], index=1)

    flow_window = "5D"

    with st.spinner("正在构建产业链图谱..."):
        snapshot = _get_snapshot_cached(
            selected_sector,
            int(limit_per_stage),
            screener_trade_date,
            flow_window,
            "v2",
        )

    meta = snapshot.get("meta", {})
    right_meta_col1, right_meta_col2 = st.columns([4, 2])
    with right_meta_col2:
        st.markdown(
            f"<div class='chain-meta-right'>"
            f"资金数据更新日期: {meta.get('fund_trade_date') or '-'}"
            f"</div>",
            unsafe_allow_html=True,
        )

    for w in meta.get("warnings", []):
        st.warning(w)

    st.plotly_chart(
        _build_flow_bubble_chart(snapshot, flow_window=flow_window, scale_mode=scale_mode),
        use_container_width=True,
    )

    stages = snapshot.get("stages", [])
    stage_options = ["全部子环节"] + [str(s.get("name") or "") for s in stages]
    stage_filter = st.selectbox("子环节筛选", options=stage_options, index=0)

    for stage in stages:
        if stage_filter != "全部子环节" and stage_filter != str(stage.get("name") or ""):
            continue
        _render_stage(stage)


if __name__ == "__main__":
    main()
