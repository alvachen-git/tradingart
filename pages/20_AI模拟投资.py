import html
import os
import sys

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ai_simulation_service import (
    OFFICIAL_PORTFOLIO_ID,
    get_daily_review,
    get_latest_snapshot,
    get_nav_series,
    get_positions,
    get_review_dates,
    get_trades,
)
from ui_components import inject_sidebar_toggle_style


def _safe_float(value) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _fmt_trade_date(raw: str) -> str:
    digits = "".join(ch for ch in str(raw or "") if ch.isdigit())
    if len(digits) == 8:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:]}"
    return str(raw or "-")


def _fmt_money(value: float) -> str:
    v = _safe_float(value)
    abs_v = abs(v)
    if abs_v >= 1e8:
        return f"{v / 1e8:.2f}亿"
    if abs_v >= 1e4:
        return f"{v / 1e4:.2f}万"
    return f"{v:,.2f}"


def _trim_float_text(value: float, digits: int) -> str:
    s = f"{value:.{digits}f}".rstrip("0").rstrip(".")
    if s in {"", "-0"}:
        return "0"
    return s


def _fmt_total_fund(value: float) -> str:
    v = _safe_float(value)
    abs_v = abs(v)
    if abs_v >= 1e8:
        return f"{_trim_float_text(v / 1e8, 2)}亿"
    if abs_v >= 1e4:
        return f"{_trim_float_text(v / 1e4, 1)}万"
    return _trim_float_text(v, 2)


def _fmt_pct(value: float) -> str:
    return f"{_safe_float(value):+.2%}"


def _tone(value: float) -> str:
    v = _safe_float(value)
    if v > 0:
        return "pos"
    if v < 0:
        return "neg"
    return "flat"


def _kpi_card(
    label: str,
    value: str,
    sub: str = "",
    tone: str = "flat",
    raw: float | None = None,
    kind: str = "text",
) -> str:
    sub_html = f'<div class="kpi-sub">{html.escape(sub)}</div>' if sub else '<div class="kpi-sub">&nbsp;</div>'
    data_attr = ""
    if raw is not None:
        data_attr = f' data-raw="{_safe_float(raw):.10f}" data-kind="{html.escape(kind)}"'
    return (
        f'<div class="kpi-card {tone}">'
        f'<div class="kpi-label">{html.escape(label)}</div>'
        f'<div class="kpi-value"{data_attr}>{html.escape(value)}</div>'
        f"{sub_html}"
        "</div>"
    )


def _render_holdings_table(pos_df: pd.DataFrame) -> str:
    if pos_df.empty:
        return '<div class="empty-box">该交易日暂无持仓</div>'

    df = pos_df.copy()
    df = df.sort_values("market_value", ascending=False)
    rows = []
    for _, row in df.iterrows():
        pnl = _safe_float(row.get("unrealized_pnl"))
        pnl_cls = "pos" if pnl > 0 else ("neg" if pnl < 0 else "flat")
        rows.append(
            "<tr>"
            f"<td class='mono'>{html.escape(str(row.get('symbol') or '-'))}</td>"
            f"<td>{html.escape(str(row.get('name') or '-'))}</td>"
            f"<td class='mono right'>{int(_safe_float(row.get('quantity'))):,}</td>"
            f"<td class='mono right'>{_safe_float(row.get('avg_cost')):.2f}</td>"
            f"<td class='mono right'>{_safe_float(row.get('close_price')):.2f}</td>"
            f"<td class='mono right'>{_safe_float(row.get('market_value')):,.0f}</td>"
            f"<td class='mono right {pnl_cls}'>{pnl:,.0f}</td>"
            f"<td class='mono right'>{_safe_float(row.get('weight')):.2%}</td>"
            "</tr>"
        )
    body = "".join(rows)
    return (
        '<div class="table-wrap">'
        '<table class="desk-table">'
        "<thead><tr>"
        "<th>代码</th><th>名称</th><th class='right'>数量</th><th class='right'>成本</th>"
        "<th class='right'>现价</th><th class='right'>市值</th><th class='right'>浮盈亏</th><th class='right'>权重</th>"
        "</tr></thead>"
        f"<tbody>{body}</tbody>"
        "</table>"
        "</div>"
    )


def _render_trades_table(trades_df: pd.DataFrame) -> str:
    if trades_df.empty:
        return '<div class="empty-box">最近2周暂无交易记录</div>'

    side_map = {"buy": "买入", "sell": "卖出"}
    df = trades_df.copy()
    df["trade_date_dt"] = pd.to_datetime(
        df["trade_date"].astype(str).str.replace(r"[^0-9]", "", regex=True).str[:8],
        format="%Y%m%d",
        errors="coerce",
    )
    valid_dt = df["trade_date_dt"].dropna()
    if not valid_dt.empty:
        latest_dt = valid_dt.max()
        cutoff_dt = latest_dt - pd.Timedelta(days=13)
        df = df[df["trade_date_dt"] >= cutoff_dt]
    if df.empty:
        return '<div class="empty-box">最近2周暂无交易记录</div>'

    df["created_at_dt"] = pd.to_datetime(df.get("created_at"), errors="coerce")
    df = df.sort_values(["trade_date_dt", "created_at_dt"], ascending=[False, False], kind="stable")

    rows = []
    for _, row in df.iterrows():
        side = str(row.get("side") or "").lower()
        side_text = side_map.get(side, side.upper() or "-")
        side_cls = "pos" if side == "buy" else ("neg" if side == "sell" else "flat")
        realized_pnl = _safe_float(row.get("realized_pnl"))
        if side == "sell":
            pnl_text = f"{realized_pnl:+,.0f}"
            pnl_cls = "pos" if realized_pnl > 0 else ("neg" if realized_pnl < 0 else "flat")
        else:
            pnl_text = "--"
            pnl_cls = "flat"
        rows.append(
            "<tr>"
            f"<td class='mono'>{_fmt_trade_date(str(row.get('trade_date') or '-'))}</td>"
            f"<td class='mono'>{html.escape(str(row.get('symbol') or '-'))}</td>"
            f"<td class='{side_cls}'>{side_text}</td>"
            f"<td class='mono right'>{int(_safe_float(row.get('quantity'))):,}</td>"
            f"<td class='mono right'>{_safe_float(row.get('price')):.2f}</td>"
            f"<td class='mono right'>{_safe_float(row.get('amount')):,.0f}</td>"
            f"<td class='mono right {pnl_cls}'>{pnl_text}</td>"
            "</tr>"
        )
    body = "".join(rows)
    return (
        '<div class="table-wrap trades">'
        '<table class="desk-table">'
        "<thead><tr><th>交易日</th><th>代码</th><th>方向</th><th class='right'>数量</th><th class='right'>价格</th><th class='right'>金额</th><th class='right'>单笔盈亏</th></tr></thead>"
        f"<tbody>{body}</tbody>"
        "</table>"
        "</div>"
    )


def _render_watchlist_cards(watchlist: list[dict]) -> str:
    if not watchlist:
        return '<div class="empty-box">暂无次日观察标的</div>'

    cards = []
    for item in watchlist[:8]:
        symbol = html.escape(str(item.get("symbol") or "-"))
        name = html.escape(str(item.get("name") or "未命名"))
        score = _safe_float(item.get("score"))
        tone = "watch-mid"
        if score >= 85:
            tone = "watch-hot"
        elif score <= 60:
            tone = "watch-cold"
        cards.append(
            f'<div class="watch-card {tone}">'
            f'<div class="watch-head"><span class="watch-symbol">{symbol}</span><span class="watch-score">{score:.1f}</span></div>'
            f'<div class="watch-name">{name}</div>'
            f'<div class="watch-foot">次日重点跟踪</div>'
            "</div>"
        )
    return f'<div class="watch-grid">{"".join(cards)}</div>'


st.set_page_config(page_title="爱波塔-AI炒股", page_icon="favicon.ico", layout="wide", initial_sidebar_state="expanded")

from sidebar_navigation import show_navigation

with st.sidebar:
    show_navigation()

inject_sidebar_toggle_style(mode="high_contrast")

st.markdown(
    """
<style>
    @import url('https://fonts.googleapis.com/css2?family=Rajdhani:wght@500;600;700&family=IBM+Plex+Mono:wght@400;500;600&display=swap');

    :root {
        --bg-0: #060d1f;
        --bg-1: #0b1730;
        --card: #111f3e;
        --card-soft: rgba(17, 31, 62, 0.72);
        --line: rgba(120, 149, 204, 0.32);
        --text: #ecf3ff;
        --muted: #9fb0cd;
        --green: #2ecb88;
        --red: #ff6b7a;
        --amber: #f3b34a;
        --cyan: #3cc8ff;
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
    [data-testid="stHeader"] {
        background: transparent !important;
    }
    [data-testid="stDecoration"] {
        display: none;
    }
    h1, h2, h3, h4, p, label, .stCaption {
        color: var(--text) !important;
    }

    .hero-shell {
        border: 1px solid var(--line);
        border-radius: 16px;
        padding: 18px 18px 16px 18px;
        background: linear-gradient(120deg, rgba(12, 26, 54, 0.92), rgba(10, 22, 46, 0.78));
        box-shadow: 0 16px 40px rgba(0, 0, 0, 0.32), inset 0 1px 0 rgba(255, 255, 255, 0.04);
        margin-bottom: 14px;
        position: relative;
        overflow: hidden;
        animation: heroOpen 900ms cubic-bezier(.22,.9,.28,1) both;
    }
    .hero-shell::after {
        content: "";
        position: absolute;
        top: 0;
        left: -38%;
        width: 32%;
        height: 100%;
        background: linear-gradient(90deg, transparent, rgba(108, 171, 255, 0.18), transparent);
        transform: skewX(-16deg);
        animation: scanSweep 6.2s ease-in-out infinite;
    }
    .hero-top {
        display: flex;
        align-items: baseline;
        justify-content: flex-start;
        flex-wrap: wrap;
        gap: 10px;
    }
    .hero-title {
        font-size: clamp(28px, 3.8vw, 44px);
        line-height: 1.06;
        font-weight: 700;
        letter-spacing: 0.02em;
        margin: 0;
    }
    .hero-sub {
        color: var(--muted);
        font-size: 16px;
        margin-top: 6px;
    }
    .hero-note {
        margin-top: 10px;
        color: #c8d6ef;
        font-size: 14px;
        line-height: 1.5;
        animation: fadeUp 760ms ease both;
        animation-delay: 220ms;
    }

    .kpi-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
        gap: 10px;
        margin-top: 12px;
    }
    .kpi-card {
        border: 1px solid var(--line);
        border-radius: 12px;
        background: var(--card-soft);
        padding: 10px 12px;
        min-height: 84px;
        display: flex;
        flex-direction: column;
        justify-content: center;
        opacity: 0;
        transform: translateY(14px);
        animation: kpiIn 560ms cubic-bezier(.2,.85,.28,1) forwards;
    }
    .kpi-card:nth-child(1) { animation-delay: 90ms; }
    .kpi-card:nth-child(2) { animation-delay: 130ms; }
    .kpi-card:nth-child(3) { animation-delay: 170ms; }
    .kpi-card:nth-child(4) { animation-delay: 210ms; }
    .kpi-card:nth-child(5) { animation-delay: 250ms; }
    .kpi-card:nth-child(6) { animation-delay: 290ms; }
    .kpi-card:nth-child(7) { animation-delay: 330ms; }
    .kpi-card:nth-child(8) { animation-delay: 370ms; }
    .kpi-card:nth-child(9) { animation-delay: 410ms; }
    .kpi-card:nth-child(10) { animation-delay: 450ms; }
    .kpi-card:nth-child(11) { animation-delay: 490ms; }
    .kpi-card:nth-child(12) { animation-delay: 530ms; }
    .kpi-card:nth-child(13) { animation-delay: 570ms; }
    .kpi-card:nth-child(14) { animation-delay: 610ms; }
    .kpi-card:nth-child(15) { animation-delay: 650ms; }
    .kpi-card:nth-child(16) { animation-delay: 690ms; }
    .kpi-card:nth-child(17) { animation-delay: 730ms; }
    .kpi-card:nth-child(18) { animation-delay: 770ms; }
    .kpi-card:nth-child(19) { animation-delay: 810ms; }
    .kpi-card:nth-child(20) { animation-delay: 850ms; }
    }
    .kpi-label {
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: var(--muted);
        line-height: 1.1;
        margin-bottom: 6px;
    }
    .kpi-value {
        font-family: "IBM Plex Mono", monospace;
        font-size: clamp(22px, 2.3vw, 33px);
        letter-spacing: 0.01em;
        line-height: 1.08;
        color: #eff6ff;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        transform: translateY(10px);
        filter: blur(4px);
        animation: numberIn 620ms ease forwards;
        animation-delay: 260ms;
    }
    .kpi-sub {
        margin-top: 4px;
        font-family: "IBM Plex Mono", monospace;
        font-size: 12px;
        color: #8fa7ce;
    }
    .kpi-card.pos .kpi-value {
        color: var(--green);
    }
    .kpi-card.neg .kpi-value {
        color: var(--red);
    }
    .kpi-card.flat .kpi-value {
        color: #eff6ff;
    }

    .panel-title {
        margin: 6px 0 8px 0;
        font-size: clamp(22px, 2.1vw, 34px);
        letter-spacing: 0.03em;
        font-weight: 700;
        animation: fadeUp 560ms ease both;
        animation-delay: 180ms;
    }
    .panel-sub {
        color: var(--muted);
        margin-bottom: 8px;
        font-size: 14px;
    }
    .section-divider {
        margin: 12px 0 14px 0;
        border: 0;
        height: 1px;
        background: linear-gradient(90deg, transparent, rgba(124, 157, 212, 0.5), transparent);
    }
    .panel-box {
        border: 1px solid var(--line);
        border-radius: 14px;
        padding: 12px;
        background: rgba(10, 21, 43, 0.72);
        animation: fadeUp 580ms ease both;
        animation-delay: 220ms;
    }
    .mini-kpi {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 8px;
        margin-top: 8px;
    }
    .mini-kpi .item {
        border: 1px solid rgba(120, 149, 204, 0.24);
        border-radius: 10px;
        background: rgba(12, 28, 58, 0.6);
        padding: 8px;
    }
    .mini-kpi .label {
        color: var(--muted);
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.07em;
    }
    .mini-kpi .val {
        color: #f2f7ff;
        font-family: "IBM Plex Mono", monospace;
        font-size: 16px;
        margin-top: 2px;
    }

    .table-wrap {
        border: 1px solid var(--line);
        border-radius: 14px;
        overflow: auto;
        max-height: 430px;
        background: rgba(9, 19, 39, 0.74);
        animation: fadeUp 620ms ease both;
        animation-delay: 240ms;
    }
    .table-wrap.trades {
        max-height: 380px;
    }
    .desk-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 14px;
        color: #dce8ff;
    }
    .desk-table thead th {
        position: sticky;
        top: 0;
        background: rgba(15, 33, 67, 0.96);
        color: #b6c8e9;
        text-align: left;
        font-weight: 600;
        letter-spacing: 0.03em;
        border-bottom: 1px solid var(--line);
        padding: 10px 12px;
        z-index: 1;
    }
    .desk-table tbody td {
        border-bottom: 1px solid rgba(120, 149, 204, 0.14);
        padding: 10px 12px;
        line-height: 1.25;
    }
    .desk-table tbody tr:hover {
        background: rgba(52, 108, 196, 0.15);
    }
    .desk-table .mono {
        font-family: "IBM Plex Mono", monospace;
        letter-spacing: 0.01em;
    }
    .desk-table .right {
        text-align: right;
    }
    .pos {
        color: var(--green);
    }
    .neg {
        color: var(--red);
    }
    .flat {
        color: #dce8ff;
    }
    .empty-box {
        border: 1px dashed var(--line);
        color: var(--muted);
        border-radius: 12px;
        padding: 16px;
        text-align: center;
        background: rgba(10, 23, 48, 0.5);
    }
    .watch-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(145px, 1fr));
        gap: 10px;
        margin-top: 10px;
    }
    .watch-card {
        border: 1px solid rgba(121, 153, 214, 0.28);
        border-radius: 12px;
        background: linear-gradient(140deg, rgba(11, 28, 58, 0.88), rgba(9, 22, 47, 0.72));
        padding: 10px;
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.04);
    }
    .watch-head {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 6px;
    }
    .watch-symbol {
        font-family: "IBM Plex Mono", monospace;
        font-size: 13px;
        letter-spacing: 0.03em;
        color: #dce8ff;
    }
    .watch-score {
        font-family: "IBM Plex Mono", monospace;
        font-size: 12px;
        padding: 2px 7px;
        border-radius: 999px;
        border: 1px solid rgba(96, 132, 196, 0.38);
        color: #e7f0ff;
    }
    .watch-name {
        font-size: 16px;
        line-height: 1.2;
        color: #f4f8ff;
        min-height: 38px;
    }
    .watch-foot {
        margin-top: 8px;
        color: #8ea3c8;
        font-size: 12px;
    }
    .watch-hot .watch-score {
        border-color: rgba(46, 203, 136, 0.65);
        color: #c8ffe5;
        background: rgba(22, 111, 79, 0.30);
    }
    .watch-cold .watch-score {
        border-color: rgba(255, 107, 122, 0.55);
        color: #ffd0d5;
        background: rgba(120, 36, 47, 0.28);
    }
    .diary-box {
        border: 1px solid var(--line);
        border-radius: 14px;
        padding: 12px 12px 2px 12px;
        background: rgba(9, 22, 45, 0.72);
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.03);
    }

    @keyframes heroOpen {
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
    @keyframes scanSweep {
        0% { left: -38%; }
        58% { left: 118%; }
        100% { left: 118%; }
    }
    @keyframes kpiIn {
        0% {
            opacity: 0;
            transform: translateY(14px);
        }
        100% {
            opacity: 1;
            transform: translateY(0);
        }
    }
    @keyframes numberIn {
        0% {
            transform: translateY(10px);
            filter: blur(4px);
        }
        100% {
            transform: translateY(0);
            filter: blur(0);
        }
    }
    @keyframes fadeUp {
        0% {
            opacity: 0;
            transform: translateY(10px);
        }
        100% {
            opacity: 1;
            transform: translateY(0);
        }
    }

    @media (max-width: 980px) {
        .hero-title {
            font-size: 30px;
        }
        .kpi-grid {
            grid-template-columns: repeat(auto-fit, minmax(145px, 1fr));
        }
        .kpi-value {
            font-size: 24px;
        }
    }
</style>
""",
    unsafe_allow_html=True,
)

snapshot = get_latest_snapshot(OFFICIAL_PORTFOLIO_ID)

if not snapshot.get("has_data"):
    st.info("暂无模拟投资数据。请等待每日任务执行后查看。")
    st.stop()

snapshot_trade_date = str(snapshot.get("trade_date") or "")
snapshot_trade_date_view = _fmt_trade_date(snapshot_trade_date)

pos_df = get_positions(OFFICIAL_PORTFOLIO_ID, as_of_date=snapshot_trade_date, strict_as_of=True)
trades_df = get_trades(OFFICIAL_PORTFOLIO_ID, days=20)
review_dates = get_review_dates(OFFICIAL_PORTFOLIO_ID, limit=260)
if not review_dates and snapshot_trade_date:
    review_dates = [snapshot_trade_date]
selected_review_date = review_dates[0] if review_dates else snapshot_trade_date
review = get_daily_review(OFFICIAL_PORTFOLIO_ID, trade_date=selected_review_date)

if not pos_df.empty and "trade_date" in pos_df.columns:
    pos_trade_date = str(pos_df["trade_date"].astype(str).iloc[0])
    if pos_trade_date != snapshot_trade_date:
        st.warning(f"数据日期存在错位：净值={snapshot_trade_date}，持仓={pos_trade_date}")

st.markdown(
    (
        '<div class="hero-shell">'
        '<div class="hero-top">'
        '<div>'
        '<h1 class="hero-title">爱波塔-量化操盘室</h1>'
        '<div class="hero-sub">千问模型搭配交易汇训练｜A股 + ETF｜只做多｜每日20:30更新</div>'
        "</div>"
        "</div>"
        '<div class="hero-note">执行口径：AI自主推理选股，无人工干预，成交价格统一用当日收盘价，不计手续费与滑点。</div>'
        "</div>"
    ),
    unsafe_allow_html=True,
)

kpi_html = "".join(
    [
        _kpi_card(
            "总资金",
            _fmt_total_fund(snapshot.get("nav")),
            f"¥{_safe_float(snapshot.get('nav')):,.2f}",
            "flat",
            raw=_safe_float(snapshot.get("nav")),
            kind="money_compact",
        ),
        _kpi_card(
            "当日收益",
            _fmt_pct(snapshot.get("daily_return")),
            "日频",
            _tone(snapshot.get("daily_return")),
            raw=_safe_float(snapshot.get("daily_return")),
            kind="pct_signed",
        ),
        _kpi_card(
            "累计收益",
            _fmt_pct(snapshot.get("cum_return")),
            "上线以来",
            _tone(snapshot.get("cum_return")),
            raw=_safe_float(snapshot.get("cum_return")),
            kind="pct_signed",
        ),
        _kpi_card(
            "最大回撤",
            f"{_safe_float(snapshot.get('max_drawdown')):.2%}",
            "峰值到谷底",
            "neg",
            raw=_safe_float(snapshot.get("max_drawdown")),
            kind="pct_abs",
        ),
        _kpi_card(
            "换手率",
            f"{_safe_float(snapshot.get('turnover')):.2%}",
            "当日",
            "flat",
            raw=_safe_float(snapshot.get("turnover")),
            kind="pct_abs",
        ),
        _kpi_card(
            "剩余现金",
            _fmt_money(snapshot.get("cash")),
            f"¥{_safe_float(snapshot.get('cash')):,.2f}",
            "flat",
            raw=_safe_float(snapshot.get("cash")),
            kind="money_short",
        ),
        _kpi_card(
            "持仓市值",
            _fmt_money(snapshot.get("position_value")),
            f"¥{_safe_float(snapshot.get('position_value')):,.2f}",
            "flat",
            raw=_safe_float(snapshot.get("position_value")),
            kind="money_short",
        ),
        _kpi_card(
            "Alpha vs 沪深300",
            _fmt_pct(snapshot.get("alpha_vs_hs300")),
            "基准超额",
            _tone(snapshot.get("alpha_vs_hs300")),
            raw=_safe_float(snapshot.get("alpha_vs_hs300")),
            kind="pct_signed",
        ),
        _kpi_card(
            "Alpha vs 中证1000",
            _fmt_pct(snapshot.get("alpha_vs_zz1000")),
            "基准超额",
            _tone(snapshot.get("alpha_vs_zz1000")),
            raw=_safe_float(snapshot.get("alpha_vs_zz1000")),
            kind="pct_signed",
        ),
    ]
)
st.markdown(f'<div class="kpi-grid">{kpi_html}</div>', unsafe_allow_html=True)
components.html(
    """
<script>
(() => {
  const root = window.parent && window.parent.document ? window.parent.document : document;
  const values = root.querySelectorAll(".kpi-value[data-raw]:not([data-animated='1'])");
  const duration = 1100;
  const easeOut = (t) => 1 - Math.pow(1 - t, 3);
  const trimNum = (num, digits) => {
    let s = Number(num).toFixed(digits);
    s = s.replace(/\.?0+$/, "");
    return s === "-0" ? "0" : s;
  };
  const fmt = (v, kind) => {
    if (kind === "money_compact") {
      const abs = Math.abs(v);
      if (abs >= 1e8) return `${trimNum(v / 1e8, 2)}亿`;
      if (abs >= 1e4) return `${trimNum(v / 1e4, 1)}万`;
      return trimNum(v, 2);
    }
    if (kind === "money_short") {
      const abs = Math.abs(v);
      if (abs >= 1e8) return `${(v / 1e8).toFixed(2)}亿`;
      if (abs >= 1e4) return `${(v / 1e4).toFixed(2)}万`;
      return v.toLocaleString("zh-CN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    }
    if (kind === "pct_signed") {
      const p = v * 100;
      return `${p >= 0 ? "+" : ""}${p.toFixed(2)}%`;
    }
    if (kind === "pct_abs") {
      return `${(v * 100).toFixed(2)}%`;
    }
    return `${v}`;
  };
  const run = (el) => {
    const target = parseFloat(el.dataset.raw || "");
    if (Number.isNaN(target)) return;
    el.dataset.animated = "1";
    const kind = el.dataset.kind || "text";
    const start = target >= 0 ? 0 : 0;
    const t0 = performance.now();
    const tick = (now) => {
      const p = Math.min(1, (now - t0) / duration);
      const cur = start + (target - start) * easeOut(p);
      el.textContent = fmt(cur, kind);
      if (p < 1) requestAnimationFrame(tick);
      else el.textContent = fmt(target, kind);
    };
    requestAnimationFrame(tick);
  };
  values.forEach(run);
})();
</script>
""",
    height=0,
)
st.markdown('<hr class="section-divider" />', unsafe_allow_html=True)

st.markdown('<div class="panel-title">复盘日记档案</div>', unsafe_allow_html=True)
if review_dates:
    selected_review_date = st.selectbox(
        "翻阅历史日记",
        options=review_dates,
        index=0,
        format_func=_fmt_trade_date,
        key="ai_sim_review_date_selector",
    )
    review = get_daily_review(OFFICIAL_PORTFOLIO_ID, trade_date=selected_review_date)
else:
    review = get_daily_review(OFFICIAL_PORTFOLIO_ID, trade_date=snapshot_trade_date)

st.markdown('<div class="diary-box">', unsafe_allow_html=True)
st.markdown(
    f'<div class="panel-sub">复盘日期：{_fmt_trade_date(selected_review_date or review.get("trade_date") or snapshot_trade_date)}</div>',
    unsafe_allow_html=True,
)
st.markdown(review.get("summary_md", "暂无复盘"))
st.markdown(review.get("buys_md", ""))
st.markdown(review.get("sells_md", ""))
st.markdown(review.get("risk_md", ""))
st.markdown("</div>", unsafe_allow_html=True)

st.markdown('<hr class="section-divider" />', unsafe_allow_html=True)

left, right = st.columns([2.2, 1.0], gap="large")

with left:
    st.markdown('<div class="panel-title">净值与双基准曲线</div>', unsafe_allow_html=True)
    st.markdown('<div class="panel-sub">观察组合净值与沪深300/中证1000归一化走势</div>', unsafe_allow_html=True)
    days = st.selectbox("查看区间", options=[30, 60, 120, 250, 9999], index=2, format_func=lambda x: "全部" if x == 9999 else f"近{x}日")
    nav_df = get_nav_series(OFFICIAL_PORTFOLIO_ID, days)

    if nav_df.empty:
        st.info("暂无净值曲线数据")
    else:
        nav_df = nav_df.copy()
        nav_df["trade_date_dt"] = pd.to_datetime(nav_df["trade_date"].astype(str), format="%Y%m%d", errors="coerce")
        nav_df = nav_df.dropna(subset=["trade_date_dt"])

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=nav_df["trade_date_dt"],
                y=nav_df["nav"] / float(snapshot.get("initial_capital", 1_000_000)),
                mode="lines",
                name="组合净值",
                line=dict(color="#f3b34a", width=2.8),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=nav_df["trade_date_dt"],
                y=nav_df["bench_hs300"],
                mode="lines",
                name="沪深300",
                line=dict(color="#3cc8ff", width=2.1),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=nav_df["trade_date_dt"],
                y=nav_df["bench_zz1000"],
                mode="lines",
                name="中证1000",
                line=dict(color="#2ecb88", width=2.1),
            )
        )
        fig.update_layout(
            height=420,
            margin=dict(l=18, r=18, t=14, b=18),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(11, 24, 49, 0.85)",
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0, font=dict(color="#e7f1ff", size=15)),
            xaxis_title="日期",
            yaxis_title="归一化净值",
            font=dict(color="#e6efff", family="Rajdhani", size=15),
        )
        fig.update_xaxes(
            gridcolor="rgba(120,149,204,0.20)",
            linecolor="rgba(120,149,204,0.30)",
            title_font=dict(color="#eaf2ff", size=20),
            tickfont=dict(color="#b7c8e9", size=13),
        )
        fig.update_yaxes(
            gridcolor="rgba(120,149,204,0.20)",
            linecolor="rgba(120,149,204,0.30)",
            title_font=dict(color="#eaf2ff", size=20),
            tickfont=dict(color="#b7c8e9", size=13),
        )
        st.plotly_chart(fig, use_container_width=True)

with right:
    st.markdown('<div class="panel-title">仓位雷达</div>', unsafe_allow_html=True)
    st.markdown('<div class="panel-sub">按权重观察当前主要暴露</div>', unsafe_allow_html=True)
    st.markdown('<div class="panel-box">', unsafe_allow_html=True)

    if pos_df.empty:
        st.markdown('<div class="empty-box">暂无持仓权重分布</div>', unsafe_allow_html=True)
    else:
        pview = pos_df.copy()
        pview["weight"] = pd.to_numeric(pview["weight"], errors="coerce").fillna(0.0)
        pview = pview.sort_values("weight", ascending=True).tail(8)
        wf = go.Figure(
            go.Bar(
                x=(pview["weight"] * 100.0),
                y=pview["symbol"],
                orientation="h",
                marker=dict(
                    color=pview["weight"] * 100.0,
                    colorscale=[[0, "#19516f"], [0.6, "#2f90d1"], [1.0, "#2ecb88"]],
                    line=dict(width=0),
                ),
            )
        )
        wf.update_layout(
            height=250,
            margin=dict(l=8, r=10, t=4, b=6),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(9, 20, 41, 0.50)",
            xaxis_title="权重(%)",
            yaxis_title="",
            showlegend=False,
            font=dict(color="#dce8ff", family="Rajdhani", size=13),
        )
        wf.update_xaxes(gridcolor="rgba(120,149,204,0.16)", linecolor="rgba(120,149,204,0.24)")
        wf.update_yaxes(gridcolor="rgba(120,149,204,0.0)", linecolor="rgba(120,149,204,0.0)")
        st.plotly_chart(wf, use_container_width=True)

    nav = _safe_float(snapshot.get("nav"))
    cash = _safe_float(snapshot.get("cash"))
    pos_val = _safe_float(snapshot.get("position_value"))
    cash_ratio = cash / nav if nav > 0 else 0.0
    pos_ratio = pos_val / nav if nav > 0 else 0.0
    mini_html = (
        '<div class="mini-kpi">'
        f'<div class="item"><div class="label">持仓数量</div><div class="val">{len(pos_df)}</div></div>'
        f'<div class="item"><div class="label">总资金</div><div class="val">{_fmt_money(nav)}</div></div>'
        f'<div class="item"><div class="label">现金占比</div><div class="val">{cash_ratio:.2%}</div></div>'
        f'<div class="item"><div class="label">仓位占比</div><div class="val">{pos_ratio:.2%}</div></div>'
        "</div>"
    )
    st.markdown(mini_html, unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

st.markdown('<hr class="section-divider" />', unsafe_allow_html=True)

st.markdown('<div class="panel-title">持仓明细</div>', unsafe_allow_html=True)
st.markdown('<div class="panel-sub">同交易日口径，按市值从高到低</div>', unsafe_allow_html=True)
st.markdown(_render_holdings_table(pos_df), unsafe_allow_html=True)

st.markdown('<hr class="section-divider" />', unsafe_allow_html=True)

b1, b2 = st.columns([1.35, 1.0], gap="large")
with b1:
    st.markdown('<div class="panel-title">最近交易流水</div>', unsafe_allow_html=True)
    st.markdown(_render_trades_table(trades_df), unsafe_allow_html=True)

with b2:
    st.markdown('<div class="panel-title">次日观察列表</div>', unsafe_allow_html=True)
    st.markdown(
        f'<div class="panel-sub">基于 { _fmt_trade_date(selected_review_date or review.get("trade_date") or snapshot_trade_date) } 的候选观察</div>',
        unsafe_allow_html=True,
    )
    watchlist = review.get("next_watchlist") or []
    st.markdown(_render_watchlist_cards(watchlist), unsafe_allow_html=True)
