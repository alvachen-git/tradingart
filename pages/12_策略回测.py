import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text

from backtest_engine import engine, run_etf_roll_backtest, get_etf_underlyings, get_etf_expiries, get_etf_strikes_for_expiry, get_etf_first_trade_date, get_etf_strikes_for_range


st.set_page_config(page_title="策略回测", layout="wide")


# 🔥 添加统一的侧边栏导航
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sidebar_navigation import show_navigation
with st.sidebar:
    show_navigation()

st.title("策略回测（MVP）")
st.caption("日线回测：ETF 近月滚动。双卖/深虚值看跌策略（固定合约布局，滚动换月）。")

st.markdown(
    """
<style>
    .stApp {
        background-color: #0b1121 !important;
        background-image: radial-gradient(circle at 50% 0%, #1e293b 0%, #0b1121 70%);
        color: #e6e6e6 !important;
    }
    header[data-testid="stHeader"] {
        background-color: transparent !important;
    }
    [data-testid="stDecoration"] { display: none; }
    [data-testid="stSidebar"] {
        background: #0f172a !important;
        border-right: 1px solid rgba(148, 163, 184, 0.15) !important;
    }
    [data-testid="stSidebar"] * { color: #cbd5e1 !important; }
    [data-testid="stSidebarNav"] span {
        color: #cbd5e1 !important;
        font-weight: 600;
    }
    [data-testid="stSidebarNav"] a:hover {
        background: rgba(59, 130, 246, 0.12) !important;
        border-radius: 8px;
    }
    .ta-hero {
        padding: 18px 20px;
        border: 1px solid rgba(59, 130, 246, 0.2);
        border-radius: 16px;
        background: linear-gradient(135deg, rgba(15, 23, 42, 0.9), rgba(30, 41, 59, 0.9));
        box-shadow: 0 8px 30px rgba(15, 23, 42, 0.35);
        margin-bottom: 18px;
    }
    .ta-section-title {
        font-size: 16px;
        font-weight: 700;
        letter-spacing: 0.02em;
        color: #f8fafc;
        margin-bottom: 8px;
    }
    .ta-card {
        padding: 14px 16px;
        border: 1px solid rgba(100, 116, 139, 0.2);
        border-radius: 14px;
        background: rgba(15, 23, 42, 0.85);
        box-shadow: 0 6px 18px rgba(2, 6, 23, 0.35);
    }
    .ta-metric {
        font-size: 13.5px;
        color: #94a3b8;
        margin-bottom: 4px;
    }
    .ta-value {
        font-size: 24px;
        font-weight: 700;
        color: #e2e8f0;
    }
    .ta-subtle {
        color: #cbd5f5;
        font-size: 13.5px;
    }
    .ta-overview {
        font-size: 15px;
        line-height: 1.6;
        color: #e2e8f0;
    }
    .ta-overview strong {
        color: #f8fafc;
    }
    .ta-divider {
        height: 1px;
        background: linear-gradient(90deg, rgba(59,130,246,0.0), rgba(59,130,246,0.6), rgba(59,130,246,0.0));
        margin: 8px 0 12px;
    }
    .ta-panel {
        padding: 12px 16px;
        border-radius: 14px;
        border: 1px solid rgba(59, 130, 246, 0.25);
        background: rgba(2, 6, 23, 0.75);
        box-shadow: 0 10px 24px rgba(2, 6, 23, 0.45);
        margin-top: 8px;
    }
    .ta-table-wrap {
        padding: 12px 14px;
        border-radius: 14px;
        border: 1px solid rgba(148, 163, 184, 0.2);
        background: rgba(15, 23, 42, 0.75);
        box-shadow: 0 8px 20px rgba(2, 6, 23, 0.4);
        margin-top: 8px;
    }
    .ta-note {
        font-size: 13px;
        color: #94a3b8;
        margin-top: 6px;
    }
    .ta-table-wrap [data-testid="stDataFrame"] > div {
        border-radius: 10px;
    }
    .ta-table-wrap [role="row"]:hover {
        background: rgba(59, 130, 246, 0.12) !important;
    }
    .ta-fade-in {
        animation: taFadeIn 0.6s ease-out;
    }
    @keyframes taFadeIn {
        from { opacity: 0; transform: translateY(6px); }
        to { opacity: 1; transform: translateY(0); }
    }
    div.stButton > button {
        background: #1f2937 !important;
        color: #e5e7eb !important;
        border: 1px solid rgba(59, 130, 246, 0.5) !important;
        border-radius: 10px !important;
        padding: 0.5rem 1rem !important;
        transition: all 0.2s ease-in-out !important;
        font-weight: 600 !important;
    }
    div.stButton > button:hover {
        background: #111827 !important;
        border-color: #60a5fa !important;
        transform: translateY(-1px);
    }
    .stDataFrame, .stTable {
        background-color: rgba(15, 23, 42, 0.75) !important;
        border-radius: 12px !important;
    }
    label, .stSelectbox label, .stNumberInput label, .stDateInput label {
        color: #e2e8f0 !important;
        font-size: 14.5px !important;
        font-weight: 600 !important;
    }
    [data-testid="stCaptionContainer"] {
        color: #cbd5f5 !important;
    }
    [data-testid="stMainBlockContainer"] {
        padding-top: 1rem;
        padding-bottom: 1rem;
    }
    .ta-hero {
        padding: 12px 16px;
        margin-bottom: 10px;
    }
    .ta-section-title {
        margin-bottom: 6px;
    }
    .stMarkdown { margin-bottom: 0.4rem; }
    .block-container { padding-top: 1rem; }
    @media (min-width: 1200px) {
        div.stButton > button { width: auto !important; }
    }
</style>
""",
    unsafe_allow_html=True,
)


def _latest_trade_date(table: str) -> str:
    if engine is None:
        return datetime.now().strftime("%Y%m%d")
    sql = text(f"SELECT MAX(trade_date) AS md FROM {table}")
    with engine.connect() as conn:
        row = conn.execute(sql).fetchone()
        if row and row[0]:
            return str(row[0])
    return datetime.now().strftime("%Y%m%d")


etf_latest = _latest_trade_date("option_daily")
com_latest = _latest_trade_date("commodity_opt_daily")

default_end = datetime.strptime(etf_latest, "%Y%m%d")
default_start = default_end - timedelta(days=180)

st.markdown('<div class="ta-hero">', unsafe_allow_html=True)
st.markdown('<div class="ta-section-title">回测参数</div>', unsafe_allow_html=True)
col1, col2, col3 = st.columns([1, 1, 1])
with col1:
    start_date = st.date_input("开始日期", value=default_start)
with col2:
    end_date = st.date_input("结束日期", value=default_end)
with col3:
    extra_margin_rate = st.number_input("额外保证金比例", min_value=0.0, max_value=0.5, value=0.0, step=0.01, format="%.2f")
st.markdown("</div>", unsafe_allow_html=True)

start_str = start_date.strftime("%Y%m%d")
end_str = end_date.strftime("%Y%m%d")


st.markdown('<div class="ta-section-title">单标的回测</div>', unsafe_allow_html=True)

etf_list = get_etf_underlyings()
if not etf_list:
    st.warning("未检测到 ETF 期权数据（option_basic 为空或无 underlying）")
else:
    row1 = st.columns([2.2, 2.2, 1.2])
    with row1[0]:
        symbol = st.selectbox("标的 ETF", etf_list, index=0)
    with row1[1]:
        strategy = st.selectbox(
            "策略",
            [
                "持有标的ETF",
                "单买认购",
                "单买认沽",
                "单卖认购",
                "单卖认沽",
                "牛市价差",
                "熊市价差",
                "双买",
                "双卖",
                "日历价差",
            ],
            index=0,
        )
    with row1[2]:
        lots = st.number_input("手数", min_value=1, max_value=100, value=1, step=1)

    calendar_type = "卖近买远(认购)"
    if strategy == "持有标的ETF":
        strike_mode = None
        st.info("持有标的ETF：不需要选择行权价与保证金，每手=10000股。")
    elif strategy == "日历价差":
        strike_mode = "ATM"
        calendar_type = st.selectbox(
            "行权价选择",
            ["卖近买远(认购)", "卖近买远(认沽)", "买近卖远(认购)", "买近卖远(认沽)"],
            index=0,
        )
    else:
        strike_mode = st.selectbox("行权价选择", ["平值(ATM)", "虚值5%", "虚值10%", "手动选择"], index=0)

    # 手动合约选择：从起始日的近月合约里取可选行权价
    manual_params = {}
    calendar_type = "C"
    trade_date = get_etf_first_trade_date(symbol, start_str, end_str) or start_str
    expiries = get_etf_expiries(symbol, trade_date)
    expiry = expiries[0] if expiries else None
    strikes = get_etf_strikes_for_range(symbol, start_str, end_str)
    if not strikes["C"] and not strikes["P"] and expiry:
        strikes = get_etf_strikes_for_expiry(symbol, trade_date, expiry)

    if strike_mode == "手动选择":
        if strategy in {"单买认购", "单卖认购"}:
            if strikes["C"]:
                manual_params["single_strike"] = st.selectbox("认购行权价", strikes["C"])
        if strategy in {"单买认沽", "单卖认沽"}:
            if strikes["P"]:
                manual_params["single_strike"] = st.selectbox("认沽行权价", strikes["P"])
        if strategy in {"双买", "双卖"}:
            if strikes["C"]:
                manual_params["call_strike"] = st.selectbox("认购行权价", strikes["C"])
            if strikes["P"]:
                manual_params["put_strike"] = st.selectbox("认沽行权价", strikes["P"])
        if strategy in {"牛市价差"}:
            if strikes["C"]:
                manual_params["low_strike"] = st.selectbox("低行权价(买)", strikes["C"])
                manual_params["high_strike"] = st.selectbox("高行权价(卖)", strikes["C"])
        if strategy in {"熊市价差"}:
            if strikes["P"]:
                manual_params["high_strike"] = st.selectbox("高行权价(买)", strikes["P"])
                manual_params["low_strike"] = st.selectbox("低行权价(卖)", strikes["P"])
        if strategy in {"日历价差"}:
            cp_key = "C" if "认购" in calendar_type else "P"
            if strikes[cp_key]:
                manual_params["calendar_strike"] = st.selectbox("行权价", strikes[cp_key])

    run_btn = st.button("运行回测", use_container_width=True)
    if run_btn:
        strat_key_map = {
            "持有标的ETF": "hold_underlying",
            "单买认购": "single_call",
            "单买认沽": "single_put",
            "单卖认购": "single_sell_call",
            "单卖认沽": "single_sell_put",
            "牛市价差": "bull_spread",
            "熊市价差": "bear_spread",
            "双买": "double_buy",
            "双卖": "double_sell",
            "日历价差": "calendar_spread",
        }
        strat_key = strat_key_map.get(strategy)
        with st.spinner("正在计算回测，请稍候..."):
            progress = st.progress(0)
            progress.progress(25)
            if strategy == "日历价差":
                mapped_strike_mode = "ATM"
            else:
                mapped_strike_mode = (
                    "ATM"
                    if strike_mode == "平值(ATM)"
                    else "OTM5"
                    if strike_mode == "虚值5%"
                    else "OTM10"
                    if strike_mode == "虚值10%"
                    else "MANUAL"
                )

            result = run_etf_roll_backtest(
                underlying=symbol,
                strategy=strat_key,
                start_date=start_str,
                end_date=end_str,
                fee_per_lot=2.0,
                margin_rate=0.15 + extra_margin_rate,
                strike_mode=mapped_strike_mode,
                manual_params=manual_params,
                lots=lots,
                calendar_type=calendar_type,
            )
            progress.progress(100)
            progress.empty()
            if "error" in result:
                st.error(result["error"])
            else:
                summary = result["summary"]
                if result["equity"]["pnl"].isna().any():
                    missing_dates = result.get("missing_dates", [])
                    no_contract_dates = result.get("no_contract_dates", [])
                    if missing_dates or no_contract_dates:
                        st.warning(
                            "图中出现断点："
                            f"缺少合约报价 {len(missing_dates)} 天；"
                            f"无可用合约 {len(no_contract_dates)} 天。"
                        )
                    else:
                        st.warning("部分交易日缺少合约报价或无可用合约，图中将出现断点（已跳过这些日期）。")
                st.markdown('<div class="ta-fade-in">', unsafe_allow_html=True)
                top1, top2, top3, top4 = st.columns(4)
                top1.markdown(
                    f"""<div class='ta-card'>
                    <div class='ta-metric'>总盈亏(元)</div>
                    <div class='ta-value'>{summary['total_pnl']:.2f}</div>
                    <div class='ta-subtle'>区间 {summary['start_date']} ~ {summary['end_date']}</div>
                    </div>""",
                    unsafe_allow_html=True,
                )
                ann_pct = "N/A"
                if summary.get("annualized_return_pct") is not None:
                    ann_pct = f"{summary.get('annualized_return_pct', 0.0):.2%}"
                top2.markdown(
                    f"""<div class='ta-card'>
                    <div class='ta-metric'>年化收益率</div>
                    <div class='ta-value'>{ann_pct}</div>
                    <div class='ta-subtle'>年化盈亏 {summary['annualized_pnl']:.2f}</div>
                    </div>""",
                    unsafe_allow_html=True,
                )
                top3.markdown(
                    f"""<div class='ta-card'>
                    <div class='ta-metric'>最大回撤(元)</div>
                    <div class='ta-value'>{summary['max_drawdown']:.2f}</div>
                    <div class='ta-subtle'>最大回撤率 {summary.get('max_drawdown_pct', 0.0):.2%}</div>
                    </div>""",
                    unsafe_allow_html=True,
                )
                top4.markdown(
                    f"""<div class='ta-card'>
                    <div class='ta-metric'>交易次数</div>
                    <div class='ta-value'>{summary['trades']}</div>
                    <div class='ta-subtle'>平均保证金 {summary.get('avg_margin', 0.0):.2f}</div>
                    </div>""",
                    unsafe_allow_html=True,
                )
                st.markdown('<div class="ta-divider"></div>', unsafe_allow_html=True)
                st.markdown(
                    f"""<div class='ta-card'>
                    <div class='ta-metric'>策略概览</div>
                    <div class='ta-overview'><strong>标的</strong> {summary['symbol']} ｜ <strong>策略</strong> {strategy} ｜ 
                    <strong>已实现</strong> {summary.get('realized_pnl', 0.0):.2f} ｜ <strong>未实现</strong> {summary.get('unrealized_pnl', 0.0):.2f} ｜ 
                    <strong>年化盈亏</strong> {summary['annualized_pnl']:.2f} ｜ <strong>胜率</strong> {summary['win_rate']:.2%} ｜ <strong>累计权利金</strong> {summary.get('premium_paid_total', 0.0):.2f} ｜ 
                    <strong>平均单笔</strong> {summary['avg_return']:.2f}</div>
                    </div>""",
                    unsafe_allow_html=True,
                )
                st.markdown('</div>', unsafe_allow_html=True)
                st.markdown('<div class="ta-panel ta-fade-in">', unsafe_allow_html=True)
                st.markdown('<div class="ta-section-title">收益曲线</div>', unsafe_allow_html=True)
                equity_df = result["equity"].dropna(subset=["pnl"]).copy()
                equity_df["date"] = equity_df["date"].astype(str)
                equity_df["date_dt"] = pd.to_datetime(equity_df["date"], format="%Y%m%d", errors="coerce")
                equity_df = equity_df.dropna(subset=["date_dt"])
                fig = go.Figure()
                fig.add_trace(
                    go.Scatter(
                        x=equity_df["date_dt"],
                        y=equity_df["pnl"],
                        mode="lines",
                        line=dict(color="#60a5fa", width=2.6),
                        fill="tozeroy",
                        fillcolor="rgba(59, 130, 246, 0.15)",
                        hovertemplate="日期 %{x|%Y-%m-%d}<br>盈亏 %{y:.2f}<extra></extra>",
                        name="盈亏",
                    )
                )
                fig.update_layout(
                    height=360,
                    margin=dict(l=10, r=10, t=10, b=10),
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(15,23,42,0.35)",
                    font=dict(color="#e2e8f0"),
                    xaxis=dict(
                        type="date",
                        showgrid=True,
                        gridcolor="rgba(148,163,184,0.15)",
                        tickfont=dict(color="#cbd5f5"),
                        tickformat="%Y-%m-%d",
                    ),
                    yaxis=dict(
                        showgrid=True,
                        gridcolor="rgba(148,163,184,0.15)",
                        tickfont=dict(color="#cbd5f5"),
                        zerolinecolor="rgba(148,163,184,0.3)",
                    ),
                )
                st.plotly_chart(fig, use_container_width=True)
                st.markdown('<div class="ta-note">图表基于交易日收盘价计算，断点表示缺行情或无可用合约。</div>', unsafe_allow_html=True)
                st.markdown('</div>', unsafe_allow_html=True)
                if not result["trades"].empty:
                    st.markdown('<div class="ta-table-wrap ta-fade-in">', unsafe_allow_html=True)
                    st.markdown('<div class="ta-section-title">交易明细</div>', unsafe_allow_html=True)
                    trades_df = result["trades"].copy()
                    if "margin" not in trades_df.columns:
                        trades_df["margin"] = 0.0
                    trades_df = trades_df.rename(
                        columns={
                            "entry_date": "开仓日",
                            "exit_date": "平仓日",
                            "contracts": "合约",
                            "gross_pnl": "平仓盈亏",
                            "fees": "手续费",
                            "net_pnl": "净盈亏",
                            "margin": "保证金",
                            "underlying_price": "当日标的价格",
                            "long_entry": "买腿开仓价",
                            "short_entry": "卖腿开仓价",
                            "net_debit": "净借记",
                            "spread_width": "价差宽度",
                            "max_loss": "最大亏损",
                        }
                    )
                    st.dataframe(trades_df, use_container_width=True)
                    st.markdown('</div>', unsafe_allow_html=True)
                if "open_positions" in result and not result["open_positions"].empty:
                    st.markdown('<div class="ta-table-wrap ta-fade-in">', unsafe_allow_html=True)
                    st.markdown('<div class="ta-section-title">未平仓持仓</div>', unsafe_allow_html=True)
                    open_df = result["open_positions"].copy()
                    if "margin" not in open_df.columns:
                        open_df["margin"] = 0.0
                    open_df = open_df.rename(
                        columns={
                            "ts_code": "合约代码",
                            "name": "合约",
                            "entry_date": "开仓日",
                            "entry_price": "开仓价",
                            "last_price": "最新价",
                            "unrealized_pnl": "未实现盈亏",
                            "margin": "保证金",
                            "underlying_price": "标的现价",
                        }
                    )
                    st.dataframe(open_df, use_container_width=True)
                    st.markdown('</div>', unsafe_allow_html=True)
                if strategy == "日历价差" and "calendar_diag" in result and not result["calendar_diag"].empty:
                    st.markdown('<div class="ta-table-wrap ta-fade-in">', unsafe_allow_html=True)
                    st.markdown('<div class="ta-section-title">日历价差诊断</div>', unsafe_allow_html=True)
                    diag_df = result["calendar_diag"].copy()
                    diag_df = diag_df.rename(
                        columns={
                            "date": "交易日",
                            "near_expiry": "近月到期日",
                            "far_expiry": "远月到期日",
                            "near_cnt": "近月合约数",
                            "far_cnt": "远月合约数",
                            "cp": "类型",
                        }
                    )
                    st.dataframe(diag_df, use_container_width=True)
                    st.markdown('</div>', unsafe_allow_html=True)
                if "pick_diag" in result and not result["pick_diag"].empty:
                    st.markdown('<div class="ta-table-wrap ta-fade-in">', unsafe_allow_html=True)
                    st.markdown('<div class="ta-section-title">合约选择诊断</div>', unsafe_allow_html=True)
                    pick_df = result["pick_diag"].copy()
                    base_cols = [
                        "date",
                        "reason",
                        "strategy",
                        "S",
                        "target_call",
                        "target_put",
                        "picked_call",
                        "picked_put",
                        "expiry",
                    ]
                    if "expiries" in pick_df.columns:
                        base_cols.append("expiries")
                    pick_df = pick_df[base_cols]
                    pick_df["call_otm_pct"] = (pick_df["picked_call"] / pick_df["S"] - 1.0) * 100
                    pick_df["put_otm_pct"] = (1.0 - pick_df["picked_put"] / pick_df["S"]) * 100
                    pick_df = pick_df.rename(
                        columns={
                            "date": "交易日",
                            "reason": "类型",
                            "strategy": "策略",
                            "S": "标的价格",
                            "target_call": "目标认购行权价",
                            "target_put": "目标认沽行权价",
                            "picked_call": "实际认购行权价",
                            "picked_put": "实际认沽行权价",
                            "expiry": "到期日",
                            "expiries": "可选到期日",
                            "call_otm_pct": "认购虚值%",
                            "put_otm_pct": "认沽虚值%",
                        }
                    )
                    st.dataframe(pick_df, use_container_width=True)
                    st.markdown('</div>', unsafe_allow_html=True)
