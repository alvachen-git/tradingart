import os
import sys
import html

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

import data_engine as de
from ui_components import inject_sidebar_toggle_style


def _fmt_cell(v):
    if pd.isna(v):
        return "-"
    if isinstance(v, float):
        return f"{v:,.3f}".rstrip("0").rstrip(".")
    if isinstance(v, int):
        return f"{v:,}"
    return html.escape(str(v))


def _render_tech_table(df: pd.DataFrame, max_height: int = 320) -> str:
    if df is None or df.empty:
        return '<div class="iv-empty-box">暂无数据</div>'

    cols = list(df.columns)
    header = "".join(f"<th>{html.escape(str(c))}</th>" for c in cols)
    rows_html = []
    for _, row in df.iterrows():
        tds = []
        for c in cols:
            v = row[c]
            cell_cls = "num" if pd.api.types.is_number(v) else "txt"
            tds.append(f'<td class="{cell_cls}">{_fmt_cell(v)}</td>')
        rows_html.append(f"<tr>{''.join(tds)}</tr>")

    body = "".join(rows_html)
    return (
        f'<div class="iv-table-shell"><div class="iv-table-scroll" style="max-height:{int(max_height)}px;">'
        f'<table class="iv-table"><thead><tr>{header}</tr></thead><tbody>{body}</tbody></table>'
        "</div></div>"
    )

st.set_page_config(
    page_title="爱波塔·跨资产IV温度指数",
    page_icon="favicon.ico",
    layout="wide",
    initial_sidebar_state="expanded",
)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sidebar_navigation import show_navigation  # noqa: E402

with st.sidebar:
    show_navigation()

inject_sidebar_toggle_style(mode="high_contrast")

if os.path.exists("style.css"):
    with open("style.css", encoding="utf-8") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

st.markdown(
    """
<style>
    @import url('https://fonts.googleapis.com/css2?family=Rajdhani:wght@500;600;700&family=IBM+Plex+Mono:wght@400;500;600&display=swap');
    :root {
        --iv-bg-0: #060d1f;
        --iv-bg-1: #0b1730;
        --iv-card: rgba(15, 31, 62, 0.82);
        --iv-card-2: rgba(11, 24, 49, 0.86);
        --iv-line: rgba(105, 155, 245, 0.35);
        --iv-text: #ecf3ff;
        --iv-muted: #9fb0cd;
        --iv-cyan: #3cc8ff;
        --iv-amber: #f59e0b;
    }
    .stApp {
        background:
            radial-gradient(1200px 620px at 78% -10%, rgba(52, 114, 214, 0.28), transparent 60%),
            radial-gradient(900px 520px at 8% 0%, rgba(22, 164, 145, 0.16), transparent 56%),
            linear-gradient(150deg, var(--iv-bg-0), var(--iv-bg-1));
        color: var(--iv-text);
        font-family: "Rajdhani", "Noto Sans SC", sans-serif;
    }
    [data-testid="stMainBlockContainer"] {
        max-width: 96rem !important;
        padding-top: 0.7rem;
        padding-bottom: 1.4rem;
    }
    [data-testid="stHeader"] { background: transparent !important; }
    [data-testid="stDecoration"] { display: none; }
    h1, h2, h3, h4, p, label, .stCaption { color: var(--iv-text) !important; }
    .iv-hero {
        border: 1px solid var(--iv-line);
        border-radius: 16px;
        padding: 18px 20px;
        background: linear-gradient(120deg, rgba(13, 30, 59, 0.92), rgba(10, 22, 46, 0.82));
        box-shadow: 0 16px 36px rgba(0, 0, 0, 0.32), inset 0 1px 0 rgba(255, 255, 255, 0.05);
        position: relative;
        overflow: hidden;
        margin-bottom: 12px;
    }
    .iv-hero::after {
        content: "";
        position: absolute;
        top: 0;
        left: -34%;
        width: 28%;
        height: 100%;
        background: linear-gradient(90deg, transparent, rgba(112, 176, 255, 0.2), transparent);
        transform: skewX(-15deg);
        animation: ivScan 6.4s ease-in-out infinite;
    }
    @keyframes ivScan {
        0%, 100% { left: -34%; opacity: 0; }
        10% { opacity: 1; }
        50% { left: 108%; opacity: 1; }
        60% { opacity: 0; }
    }
    .iv-eyebrow {
        color: var(--iv-cyan);
        letter-spacing: 0.12em;
        font-size: 12px;
        margin-bottom: 6px;
    }
    .iv-title {
        font-size: clamp(30px, 4vw, 44px);
        font-weight: 700;
        line-height: 1.08;
    }
    .iv-sub {
        margin-top: 6px;
        color: var(--iv-muted);
        font-size: 15px;
    }
    .iv-thermo-card {
        border: 1px solid var(--iv-line);
        border-radius: 14px;
        background: linear-gradient(130deg, var(--iv-card), var(--iv-card-2));
        padding: 14px 16px 12px 16px;
        margin-top: 8px;
        margin-bottom: 2px;
    }
    .iv-thermo-label {
        font-size: 15px;
        color: #b6c9e8;
        margin-bottom: 2px;
    }
    .iv-thermo-value {
        font-family: "IBM Plex Mono", monospace;
        font-size: 34px;
        color: #f8fbff;
        font-weight: 600;
        margin-bottom: 8px;
    }
    .iv-thermo-track {
        position: relative;
        width: 100%;
        height: 26px;
        border-radius: 14px;
        background: linear-gradient(90deg, #22c55e 0%, #facc15 50%, #f97316 75%, #ef4444 100%);
        box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.08), 0 6px 20px rgba(0, 0, 0, 0.22);
    }
    .iv-thermo-pointer {
        position: absolute;
        top: -7px;
        width: 18px;
        height: 40px;
        display: flex;
        align-items: center;
        justify-content: center;
    }
    .iv-thermo-pointer::before {
        content: "";
        width: 4px;
        height: 34px;
        border-radius: 2px;
        background: #e2edff;
        box-shadow: 0 0 10px rgba(154, 212, 255, 0.8);
    }
    .iv-thermo-scale {
        display: flex;
        justify-content: space-between;
        color: var(--iv-muted);
        font-size: 12px;
        margin-top: 6px;
    }
    [data-testid="stPlotlyChart"] > div {
        border: 1px solid var(--iv-line);
        border-radius: 14px;
        background: linear-gradient(130deg, rgba(12, 28, 58, 0.78), rgba(9, 21, 44, 0.76));
        padding: 6px 6px 2px 6px;
    }
    [data-testid="stDataFrame"] {
        border: 1px solid var(--iv-line);
        border-radius: 14px;
        overflow: hidden;
        background: linear-gradient(130deg, rgba(12, 28, 58, 0.82), rgba(9, 21, 44, 0.80));
        box-shadow: 0 10px 28px rgba(0, 0, 0, 0.28), inset 0 1px 0 rgba(255, 255, 255, 0.04);
    }
    [data-testid="stDataFrame"] [data-testid="stDataFrameResizable"] {
        background: transparent !important;
    }
    [data-testid="stDataFrame"] thead tr th {
        background: linear-gradient(90deg, rgba(24, 49, 93, 0.95), rgba(15, 37, 74, 0.95)) !important;
        color: #ecf4ff !important;
        border-bottom: 1px solid rgba(120, 170, 255, 0.35) !important;
        font-weight: 700 !important;
        font-size: 14px !important;
    }
    [data-testid="stDataFrame"] tbody tr td {
        color: #dbeafe !important;
        background: rgba(10, 24, 50, 0.55) !important;
        border-bottom: 1px solid rgba(98, 138, 201, 0.18) !important;
        font-size: 14px !important;
    }
    [data-testid="stDataFrame"] tbody tr:nth-child(even) td {
        background: rgba(12, 29, 58, 0.72) !important;
    }
    [data-testid="stDataFrame"] tbody tr:hover td {
        background: rgba(34, 73, 138, 0.36) !important;
        box-shadow: inset 0 0 0 1px rgba(86, 159, 255, 0.18);
    }
    [data-testid="stDataFrame"] div[role="gridcell"] {
        color: #dbeafe !important;
        font-family: "IBM Plex Mono", "Rajdhani", sans-serif !important;
    }
    [data-testid="stDataFrame"] div[role="columnheader"] {
        color: #ecf4ff !important;
        font-family: "Rajdhani", "Noto Sans SC", sans-serif !important;
        font-weight: 700 !important;
    }
    [data-testid="stDataFrame"] [data-testid="stDataFrameToolbar"] {
        background: rgba(10, 24, 50, 0.78) !important;
        border-bottom: 1px solid rgba(120, 170, 255, 0.2) !important;
    }
    [data-testid="stDataFrame"] svg {
        color: #9ec7ff !important;
        fill: #9ec7ff !important;
    }
    .iv-table-shell {
        border: 1px solid rgba(112, 164, 246, 0.45);
        border-radius: 14px;
        background: linear-gradient(130deg, rgba(12, 28, 58, 0.86), rgba(9, 21, 44, 0.84));
        box-shadow: 0 12px 26px rgba(0, 0, 0, 0.24), inset 0 1px 0 rgba(255, 255, 255, 0.05);
        overflow: hidden;
    }
    .iv-table-scroll {
        overflow: auto;
    }
    .iv-table {
        width: 100%;
        border-collapse: collapse;
        min-width: 560px;
        font-family: "IBM Plex Mono", "Rajdhani", "Noto Sans SC", sans-serif;
    }
    .iv-table thead th {
        position: sticky;
        top: 0;
        z-index: 2;
        background: linear-gradient(90deg, rgba(29, 59, 110, 0.98), rgba(19, 45, 89, 0.98));
        color: #f2f7ff;
        font-size: 14px;
        font-weight: 700;
        text-align: left;
        padding: 10px 12px;
        border-bottom: 1px solid rgba(141, 185, 255, 0.45);
    }
    .iv-table tbody td {
        color: #dbeafe;
        font-size: 13px;
        padding: 9px 12px;
        border-bottom: 1px solid rgba(98, 138, 201, 0.2);
    }
    .iv-table tbody tr:nth-child(odd) td {
        background: rgba(11, 27, 54, 0.52);
    }
    .iv-table tbody tr:nth-child(even) td {
        background: rgba(14, 32, 63, 0.68);
    }
    .iv-table tbody tr:hover td {
        background: rgba(38, 84, 158, 0.35);
        box-shadow: inset 0 0 0 1px rgba(112, 176, 255, 0.18);
    }
    .iv-table td.num {
        text-align: right;
        color: #f4f8ff;
        font-variant-numeric: tabular-nums;
    }
    .iv-empty-box {
        border: 1px dashed rgba(124, 164, 232, 0.42);
        border-radius: 12px;
        padding: 12px;
        color: #b8cae8;
        background: rgba(11, 25, 51, 0.62);
    }
    .js-plotly-plot .plotly .legend text,
    .js-plotly-plot .plotly .gtitle,
    .js-plotly-plot .plotly .xtitle,
    .js-plotly-plot .plotly .ytitle,
    .js-plotly-plot .plotly .xtick text,
    .js-plotly-plot .plotly .ytick text {
        fill: #eef5ff !important;
    }
</style>
""",
    unsafe_allow_html=True,
)

st.markdown(
    """
<div class="iv-hero">
  <div class="iv-eyebrow">AIBOTA VOL-LAB</div>
  <div class="iv-title">跨资产IV温度指数</div>
  <div class="iv-sub">13个核心资产 IV Rank 加权温度（0-100），用于感知全市场隐含波动率冷热，不代表价格涨跌方向。</div>
</div>
""",
    unsafe_allow_html=True,
)

min_coverage_pct = float(getattr(de, "CROSS_ASSET_IV_MIN_COVERAGE_PCT", 60.0))

snapshot = de.get_cross_asset_iv_index(auto_compute=False)
if not snapshot.get("trade_date"):
    st.warning("暂无可用的跨资产IV温度指数数据，请等待定时任务完成日更。")
    st.stop()

trade_date = str(snapshot.get("trade_date"))
snapshot = de.get_cross_asset_iv_index(end_date=trade_date, auto_compute=False)

index_raw = snapshot.get("index_raw")
coverage_pct = float(snapshot.get("coverage_pct") or 0.0)
available_weight = float(snapshot.get("available_weight") or 0.0)
trade_date = str(snapshot.get("trade_date"))

history_df = de.get_cross_asset_iv_index_history(
    start_date="20250115",
    end_date=trade_date,
    min_coverage_pct=min_coverage_pct,
)

if not history_df.empty:
    history_df = history_df.copy()
    history_df["trade_date_dt"] = pd.to_datetime(history_df["trade_date"], format="%Y%m%d", errors="coerce")
    history_df = history_df.dropna(subset=["trade_date_dt"]).sort_values("trade_date_dt")
    history_df["index_ewma20"] = history_df["index_raw"].ewm(span=20, adjust=False).mean()

if index_raw is not None:
    temp_value = max(0.0, min(100.0, float(index_raw)))
    st.markdown(
        f"""
        <div class="iv-thermo-card">
            <div class="iv-thermo-label">当前温度</div>
            <div class="iv-thermo-value">{temp_value:.2f}</div>
            <div class="iv-thermo-track">
                <div class="iv-thermo-pointer" style="left: calc({temp_value:.2f}% - 9px);"></div>
            </div>
            <div class="iv-thermo-scale">
                <span>0 低</span><span>20</span><span>60</span><span>85</span><span>100 极高</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
else:
    st.info("当前温度暂无可用数据。")

st.caption(f"最新交易日：{trade_date}")
if coverage_pct < min_coverage_pct:
    st.warning(f"当前覆盖率为 {coverage_pct:.1f}%（阈值 {min_coverage_pct:.1f}%），样本不足，指数仅供参考。")

if not history_df.empty:
    if len(history_df) <= 1:
        st.info("当前只有1个满足覆盖率门槛的交易日，待后续日更后将显示完整曲线。")

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=history_df["trade_date_dt"],
            y=history_df["index_raw"],
            mode="lines+markers",
            name="指数原始值",
            line=dict(color="#f59e0b", width=2),
            marker=dict(size=6),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=history_df["trade_date_dt"],
            y=history_df["index_ewma20"],
            mode="lines+markers",
            name="20日EWMA",
            line=dict(color="#38bdf8", width=2),
            marker=dict(size=6),
        )
    )
    fig.update_layout(
        height=420,
        margin=dict(l=20, r=20, t=72, b=20),
        xaxis_title="交易日",
        yaxis_title="温度值(0-100)",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.05,
            x=0,
            font=dict(size=16, color="#f8fbff"),
            bgcolor="rgba(8, 21, 45, 0.45)",
            bordercolor="rgba(132, 179, 255, 0.35)",
            borderwidth=1,
        ),
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#dbeafe"),
        xaxis=dict(
            title_font=dict(size=18, color="#f8fbff"),
            tickfont=dict(size=14, color="#dbeafe"),
            gridcolor="rgba(150, 180, 230, 0.18)",
        ),
        yaxis=dict(
            range=[0, 100],
            title_font=dict(size=18, color="#f8fbff"),
            tickfont=dict(size=14, color="#dbeafe"),
            gridcolor="rgba(150, 180, 230, 0.22)",
            zerolinecolor="rgba(180, 210, 255, 0.35)",
        ),
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info(
        f"暂无满足覆盖率阈值（{min_coverage_pct:.1f}%）的历史序列数据。"
        " 若需查看低覆盖阶段，请先补齐商品IV历史后再回填指数。"
    )

components_df = de.get_cross_asset_iv_components(trade_date=trade_date, auto_compute=False)
if components_df.empty and snapshot.get("components"):
    components_df = pd.DataFrame(snapshot["components"])

st.subheader("组件贡献分解")
if components_df.empty:
    st.info("暂无组件明细。")
    st.stop()

components_df = components_df.copy()
for col in ["iv", "iv_rank", "weight", "weighted_contribution", "valid_flag"]:
    if col in components_df.columns:
        components_df[col] = pd.to_numeric(components_df[col], errors="coerce")
components_df["valid_flag"] = components_df["valid_flag"].fillna(0).astype(int)

effective_weight = available_weight if available_weight > 0 else float(
    components_df.loc[components_df["valid_flag"] == 1, "weight"].sum()
)
if effective_weight <= 0:
    effective_weight = 1.0

components_df["neutral_contribution"] = components_df.apply(
    lambda x: (float(x["weight"]) * 50.0 / effective_weight) if int(x["valid_flag"]) == 1 else 0.0,
    axis=1,
)
components_df["driver_score"] = components_df["weighted_contribution"] - components_df["neutral_contribution"]

top_up = components_df[components_df["driver_score"] > 0].sort_values("driver_score", ascending=False).head(5)
top_down = components_df[components_df["driver_score"] < 0].sort_values("driver_score", ascending=True).head(5)

up_col, down_col = st.columns(2)
with up_col:
    st.markdown("**波动率上行贡献**")
    if top_up.empty:
        st.caption("暂无明显上行驱动。")
    else:
        top_up_view = (
            top_up[["asset_name", "asset_code", "iv_rank", "weighted_contribution", "driver_score"]]
            .rename(
                columns={
                    "asset_name": "资产",
                    "asset_code": "代码",
                    "iv_rank": "IV Rank",
                    "weighted_contribution": "贡献点数",
                    "driver_score": "偏离中性",
                }
            )
            .round(2)
        )
        st.markdown(_render_tech_table(top_up_view, max_height=280), unsafe_allow_html=True)

with down_col:
    st.markdown("**波动率下行贡献**")
    if top_down.empty:
        st.caption("暂无明显下行驱动。")
    else:
        top_down_view = (
            top_down[["asset_name", "asset_code", "iv_rank", "weighted_contribution", "driver_score"]]
            .rename(
                columns={
                    "asset_name": "资产",
                    "asset_code": "代码",
                    "iv_rank": "IV Rank",
                    "weighted_contribution": "贡献点数",
                    "driver_score": "偏离中性",
                }
            )
            .round(2)
        )
        st.markdown(_render_tech_table(top_down_view, max_height=280), unsafe_allow_html=True)

display_df = components_df.sort_values(["valid_flag", "weighted_contribution"], ascending=[False, False]).copy()
display_df["valid_flag"] = display_df["valid_flag"].map({1: "有效", 0: "缺失"})
full_view = (
    display_df[
        [
            "asset_name",
            "asset_code",
            "iv",
            "iv_rank",
            "weighted_contribution",
            "driver_score",
            "valid_flag",
        ]
    ]
    .rename(
        columns={
            "asset_name": "资产",
            "asset_code": "代码",
            "iv": "IV",
            "iv_rank": "IV Rank",
            "weighted_contribution": "贡献点数",
            "driver_score": "偏离中性",
            "valid_flag": "有效性",
        }
    )
    .round(3)
)
st.markdown(_render_tech_table(full_view, max_height=560), unsafe_allow_html=True)

st.info(
    "说明：贡献点数为该资产对当日温度值的直接贡献（加总=指数原始值）；"
    "偏离中性用于辅助识别相对50分中枢的上/下行驱动。"
)

