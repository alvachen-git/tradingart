import time
from datetime import date, timedelta

import extra_streamlit_components as stx
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import text

import auth_utils as auth
import kline_game as kg
from sidebar_navigation import show_navigation


st.set_page_config(page_title="K线交易复盘", page_icon="📋", layout="wide", initial_sidebar_state="collapsed")

with st.sidebar:
    show_navigation()


if "is_logged_in" not in st.session_state:
    st.session_state["is_logged_in"] = False
if "user_id" not in st.session_state:
    st.session_state["user_id"] = None
if "token" not in st.session_state:
    st.session_state["token"] = None
if "review_cookie_retry_once" not in st.session_state:
    st.session_state["review_cookie_retry_once"] = False

cookie_manager = stx.CookieManager(key="kline_review_cookie_manager")
cookies = cookie_manager.get_all() or {}


def _restore_login_with_cookie_state(cookies_dict: dict):
    cookies_dict = cookies_dict or {}
    try:
        restored = auth.restore_login_from_cookies(cookies_dict)
    except Exception:
        return False, "error"

    if restored:
        return True, "ok"

    c_user = str(cookies_dict.get("username") or "").strip()
    c_token = str(cookies_dict.get("token") or "").strip()
    if not c_user and not c_token:
        return False, "empty"
    if (c_user and not c_token) or (c_token and not c_user):
        return False, "partial"
    return False, "invalid"


def _render_radar(radar: dict):
    dims = list((radar or {}).get("dimensions") or [])
    vals = list((radar or {}).get("values") or [])
    if not dims or not vals or len(dims) != len(vals):
        st.info("能力雷达数据暂不可用")
        return
    dims_closed = dims + [dims[0]]
    vals_closed = vals + [vals[0]]
    fig = go.Figure()
    fig.add_trace(
        go.Scatterpolar(
            r=vals_closed,
            theta=dims_closed,
            fill="toself",
            name="能力评分",
            line=dict(color="#0ea5e9", width=2),
            fillcolor="rgba(14,165,233,0.25)",
        )
    )
    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        showlegend=False,
        height=380,
        margin=dict(l=10, r=10, t=10, b=10),
    )
    st.plotly_chart(fig, use_container_width=True)


def _join_reason_labels(labels) -> str:
    parts = [str(x).strip() for x in (labels or []) if str(x).strip()]
    return "；".join(parts)


ACTION_LABEL_MAP = {
    "open_long": "买入开多",
    "add_long": "加仓做多",
    "buy": "买入开多",
    "close_long": "平仓多头",
    "close_long_partial": "部分平多",
    "close_long_all": "全部平多",
    "sell_long": "卖出平多",
    "close": "平仓",
    "open_short": "卖出开空",
    "add_short": "加仓做空",
    "sell_short": "卖出开空",
    "close_short": "平仓空头",
    "close_short_partial": "部分平空",
    "close_short_all": "全部平空",
    "buy_to_cover": "买入平空",
}

BIAS_LABEL_MAP = {
    "long_bias": "多头偏向",
    "short_bias": "空头偏向",
    "neutral": "中性",
}

ALIGNMENT_LABEL_MAP = {
    "aligned": "符合体系（顺势）",
    "counter": "偏离体系（逆势）",
    "observe": "待观察",
    "risk_control_good": "风险控制良好",
    "risk_control_neutral": "风险控制中性",
    "risk_control_bad": "风险控制不足",
    "premature_take_profit": "趋势中提前止盈",
}

TAG_LABEL_MAP = {
    "counter_trend_entry": "逆势开仓/加仓",
    "add_to_loser": "亏损中加仓",
    "premature_take_profit": "趋势中提前止盈",
    "overtrading": "过度交易",
}


def _zh_action(action: str) -> str:
    a = str(action or "").strip().lower()
    return ACTION_LABEL_MAP.get(a, a or "未知动作")


def _zh_bias(bias: str) -> str:
    b = str(bias or "").strip()
    return BIAS_LABEL_MAP.get(b, b or "中性")


def _zh_alignment(alignment: str) -> str:
    a = str(alignment or "").strip()
    return ALIGNMENT_LABEL_MAP.get(a, a or "待观察")


def _zh_tags(tags) -> str:
    out = []
    for t in (tags or []):
        s = str(t or "").strip()
        if not s:
            continue
        out.append(TAG_LABEL_MAP.get(s, s))
    return "、".join(out)


if not st.session_state.get("is_logged_in"):
    restored, restore_state = _restore_login_with_cookie_state(cookies)
    if restored:
        st.session_state["review_cookie_retry_once"] = False
    elif restore_state in ("empty", "partial", "error") and not st.session_state.get("review_cookie_retry_once", False):
        st.session_state["review_cookie_retry_once"] = True
        time.sleep(0.15)
        st.rerun()

if not st.session_state.get("is_logged_in"):
    st.warning("🔒 请先在首页登录后查看交易复盘")
    st.page_link("Home.py", label="🏠 返回首页登录", use_container_width=True)
    st.stop()

viewer_id = str(st.session_state.get("user_id") or "").strip()
is_coach = kg.is_review_coach(viewer_id)


@st.cache_data(ttl=30)
def _list_users_for_coach() -> list:
    try:
        with kg.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT user_id, MAX(id) AS latest_id
                    FROM kline_game_records
                    WHERE status = 'finished'
                      AND end_reason = 'completed'
                    GROUP BY user_id
                    ORDER BY latest_id DESC
                    LIMIT 300
                    """
                )
            ).fetchall()
        return [str(r[0]) for r in rows if r and r[0]]
    except Exception:
        return []


st.title("📋 K线交易复盘")
st.caption("查看历史对局、逐笔交易判定和 AI 检讨建议。")


default_target = viewer_id
if is_coach:
    user_options = _list_users_for_coach()
    if viewer_id not in user_options:
        user_options = [viewer_id] + user_options
    default_idx = user_options.index(viewer_id) if viewer_id in user_options else 0
    selected_user = st.selectbox("复盘用户", options=user_options, index=default_idx)
else:
    selected_user = viewer_id

col_f1, col_f2, col_f3, col_f4, col_f5 = st.columns([1.1, 1.1, 1, 1, 1.4])
with col_f1:
    default_from = date.today() - timedelta(days=30)
    date_from = st.date_input("开始日期", value=default_from)
with col_f2:
    date_to = st.date_input("结束日期", value=date.today())
with col_f3:
    symbol_type = st.selectbox("品种类型", options=["全部", "stock", "index", "future"], index=0)
with col_f4:
    profit_side = st.selectbox("盈亏方向", options=["全部", "profit", "loss", "flat"], index=0)
with col_f5:
    score_min, score_max = st.slider("体系分范围", min_value=0, max_value=100, value=(0, 100), step=1)

list_payload = kg.list_review_games(
    viewer_id=viewer_id,
    target_user=selected_user,
    date_from=str(date_from),
    date_to=str(date_to),
    limit=200,
    offset=0,
    symbol_type=None if symbol_type == "全部" else symbol_type,
    profit_side=None if profit_side == "全部" else profit_side,
    score_min=float(score_min),
    score_max=float(score_max),
)

if not list_payload.get("ok"):
    st.error(f"加载复盘列表失败：{list_payload.get('message', '未知错误')}")
    st.stop()

items = list_payload.get("items") or []
st.caption(f"共 {int(list_payload.get('total') or 0)} 局，当前显示 {len(items)} 局")

if not items:
    st.info("暂无符合筛选条件的复盘记录")
    st.stop()

rows_for_df = []
for it in items:
    rows_for_df.append(
        {
            "game_id": int(it.get("game_id") or 0),
            "用户": str(it.get("user_id") or ""),
            "结束时间": str(it.get("game_end_time") or ""),
            "品种": str(it.get("symbol_name") or it.get("symbol") or ""),
            "类型": str(it.get("symbol_type") or ""),
            "杠杆": int(it.get("leverage") or 1),
            "盈亏": float(it.get("profit") or 0),
            "交易次数": int(it.get("trade_count") or 0),
            "体系分": float(it.get("overall_score") or 0),
            "主要问题": "、".join(it.get("main_mistakes") or []),
        }
    )

st.dataframe(pd.DataFrame(rows_for_df), use_container_width=True, hide_index=True)

focus_game_id = int(st.session_state.get("kline_review_focus_game_id") or 0)
game_ids = [int(x.get("game_id") or 0) for x in items if int(x.get("game_id") or 0) > 0]
if focus_game_id not in game_ids:
    focus_game_id = game_ids[0]

selected_game_id = st.selectbox("选择对局", options=game_ids, index=game_ids.index(focus_game_id))

if st.session_state.get("kline_review_focus_game_id"):
    st.session_state["kline_review_focus_game_id"] = 0

detail = kg.get_review_detail(viewer_id=viewer_id, game_id=int(selected_game_id), target_user=selected_user)
if not detail.get("ok"):
    st.error(f"加载复盘详情失败：{detail.get('message', '未知错误')}")
    st.stop()

game = detail.get("game") or {}
report = detail.get("report") or {}
metrics = report.get("metrics") or {}
trades = detail.get("trades") or []
evals = detail.get("evaluations") or []
chart = detail.get("chart") or {}
global_review = detail.get("global_review") or {}

st.markdown("---")
st.subheader("单局总览")

m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("体系总分", f"{float(report.get('overall_score') or 0):.1f}")
m2.metric("方向分", f"{float(report.get('direction_score') or 0):.1f}")
m3.metric("风险分", f"{float(report.get('risk_score') or 0):.1f}")
m4.metric("执行分", f"{float(report.get('execution_score') or 0):.1f}")
m5.metric("交易次数", f"{int(game.get('trade_count') or 0)}")

c1, c2 = st.columns([1.1, 1])
with c1:
    st.write(f"品种：`{game.get('symbol_name') or game.get('symbol')}` ({game.get('symbol_type')})")
    st.write(f"盈亏：`{float(game.get('profit') or 0):,.0f}`")
    st.write(f"收益率：`{float(game.get('profit_rate') or 0) * 100:.2f}%`")
    st.write(f"最大回撤：`{float(game.get('max_drawdown') or 0) * 100:.2f}%`")
with c2:
    mistakes = report.get("mistakes") or []
    st.write("主要问题：")
    if mistakes:
        for m in mistakes[:4]:
            st.write(f"- {m.get('title') or m.get('tag')} ({int(m.get('count') or 0)}次)")
    else:
        st.write("- 暂无")

st.markdown("---")
st.subheader("K线复盘图")

bars = chart.get("bars") or []
if bars:
    dfb = pd.DataFrame(bars)
    fig = go.Figure(
        data=[
            go.Candlestick(
                x=dfb["date"],
                open=dfb["open"],
                high=dfb["high"],
                low=dfb["low"],
                close=dfb["close"],
                increasing_line_color="#ef4444",
                decreasing_line_color="#22c55e",
                name="K线",
            )
        ]
    )

    markers = chart.get("trade_markers") or []
    if markers:
        dfm = pd.DataFrame(markers)
        if not dfm.empty:
            def _action_group(action: str) -> str:
                a = str(action or "").strip().lower()
                if a in {"open_long", "add_long", "buy"}:
                    return "buy_long"
                if a in {"close_long", "close_long_partial", "close_long_all", "sell_long", "close"}:
                    return "close_long"
                if a in {"open_short", "add_short", "sell_short"}:
                    return "sell_short"
                if a in {"close_short", "close_short_partial", "close_short_all", "buy_to_cover"}:
                    return "close_short"
                return "other"

            style_map = {
                "buy_long": {"name": "↑ 买入做多", "symbol": "triangle-up", "color": "#ef4444"},
                "close_long": {"name": "↓ 平仓多头", "symbol": "triangle-down-open", "color": "#2563eb"},
                "sell_short": {"name": "↓ 卖出做空", "symbol": "triangle-down", "color": "#16a34a"},
                "close_short": {"name": "↑ 平仓空头", "symbol": "triangle-up-open", "color": "#9333ea"},
                "other": {"name": "其他动作", "symbol": "diamond", "color": "#64748b"},
            }

            dfm["action_group"] = dfm["action"].apply(_action_group)
            # 箭头与K线做垂直区隔，避免与蜡烛实体重叠
            dfm["date_str"] = dfm["date"].astype(str)
            ref = dfb.copy()
            ref["date_str"] = ref["date"].astype(str)
            ref = ref[["date_str", "high", "low"]].drop_duplicates(subset=["date_str"], keep="last")
            ref = ref.set_index("date_str")
            dfm = dfm.join(ref, on="date_str", rsuffix="_k")

            candle_range = (dfb["high"] - dfb["low"]).abs()
            median_range = float(candle_range.median()) if not candle_range.empty else 0.0
            base_gap = median_range * 0.22
            if base_gap <= 0:
                base_gap = max(float(dfm["price"].abs().median() if "price" in dfm else 0) * 0.006, 0.02)

            dfm["ref_high"] = dfm["high"].fillna(dfm["price"])
            dfm["ref_low"] = dfm["low"].fillna(dfm["price"])
            dfm["is_up_arrow"] = dfm["action_group"].isin(["buy_long", "close_short"])
            dfm["stack_idx"] = dfm.groupby(["date_str", "is_up_arrow"]).cumcount()
            step_factor = 0.65
            dfm["plot_price"] = dfm["price"]
            up_mask = dfm["is_up_arrow"]
            down_mask = ~up_mask
            dfm.loc[up_mask, "plot_price"] = dfm.loc[up_mask, "ref_low"] - base_gap * (
                1 + dfm.loc[up_mask, "stack_idx"] * step_factor
            )
            dfm.loc[down_mask, "plot_price"] = dfm.loc[down_mask, "ref_high"] + base_gap * (
                1 + dfm.loc[down_mask, "stack_idx"] * step_factor
            )

            for group_key, conf in style_map.items():
                one = dfm[dfm["action_group"] == group_key]
                if one.empty:
                    continue
                fig.add_trace(
                    go.Scatter(
                        x=one["date"],
                        y=one["plot_price"],
                        mode="markers",
                        marker=dict(
                            size=14,
                            color=conf["color"],
                            symbol=conf["symbol"],
                            line=dict(width=1, color=conf["color"]),
                        ),
                        text=[
                            f"#{int(s)} {_zh_action(a)} {int(l)}手 @ {float(p):.2f}"
                            for s, a, l, p in zip(one["trade_seq"], one["action"], one["lots"], one["price"])
                        ],
                        hovertemplate="%{text}<extra></extra>",
                        name=conf["name"],
                    )
                )

    err_markers = chart.get("error_markers") or []
    if err_markers:
        dfe = pd.DataFrame(err_markers)
        if not dfe.empty:
            fig.add_trace(
                go.Scatter(
                    x=dfe["date"],
                    y=dfe["price"],
                    mode="markers",
                    marker=dict(size=12, color="#f59e0b", symbol="x"),
                    text=[
                        (
                            f"#{int(s)} 标签: {'、'.join(tags)}"
                            + (
                                f"<br>方向规则: {_join_reason_labels(reasons)}"
                                if _join_reason_labels(reasons) else ""
                            )
                            + f"<br>方向加减分: {float(dp):.1f}"
                        )
                        for s, tags, reasons, dp in zip(
                            dfe["trade_seq"],
                            dfe["tags"],
                            dfe.get("direction_reason_labels", ""),
                            dfe.get("direction_points", 0.0),
                        )
                    ],
                    hovertemplate="%{text}<extra></extra>",
                    name="错误点",
                )
            )

    fig.update_layout(height=480, xaxis_rangeslider_visible=False, margin=dict(l=10, r=10, t=10, b=10))
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("该局无可用K线图数据")

st.markdown("---")
st.subheader("逐笔交易与判定")

if trades:
    eval_map = {int(e.get("trade_seq") or 0): e for e in evals}
    rows = []
    for t in trades:
        seq = int(t.get("trade_seq") or 0)
        e = eval_map.get(seq, {})
        rows.append(
            {
                "trade_seq": seq,
                "序号": seq,
                "日期": str(t.get("bar_date") or ""),
                "动作": _zh_action(str(t.get("action") or "")),
                "价格": float(t.get("price") or 0),
                "手数": int(t.get("lots") or 0),
                "方向偏向": _zh_bias(str(e.get("market_bias") or "neutral")),
                "判定": _zh_alignment(str(e.get("alignment") or "observe")),
                "多空偏向分(0-100)": float(e.get("rule_score") or 0),
                "方向加减分": float(e.get("direction_points") or 0),
                "方向规则原因": _join_reason_labels(e.get("direction_reason_labels") or e.get("direction_reasons") or []),
                "证据形态": "、".join(e.get("evidence_patterns") or []),
                "问题标签": _zh_tags(e.get("violation_tags") or []),
            }
        )
    df_rows = pd.DataFrame(rows)
    if "trade_seq" in df_rows.columns:
        df_rows = df_rows.drop(columns=["trade_seq"])
    st.dataframe(df_rows, use_container_width=True, hide_index=True)
    st.caption("多空偏向分满分100分；50附近为中性，越接近100越偏多，越接近0越偏空。")
else:
    st.info("该局没有交易明细")

st.markdown("---")
st.subheader("全部交易总分析（最近2000笔）")
if not global_review.get("ok"):
    st.info("总分析暂不可用")
else:
    g_report = global_review.get("report") or {}
    gm = g_report.get("metrics") or {}
    gh = g_report.get("habit_summary") or {}
    gr = g_report.get("radar") or {}
    gai = g_report.get("ai_report") or {}
    g_status = str(g_report.get("ai_status") or "rule_only")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("覆盖交易笔数", f"{int(g_report.get('source_trade_count') or 0)}")
    c2.metric("覆盖对局数", f"{int(g_report.get('source_game_count') or 0)}")
    c3.metric("平均体系分", f"{float(gm.get('avg_overall_score') or 0):.1f}")
    c4.metric("转折应对率", f"{float(gm.get('reversal_response_rate') or 0):.1f}%")

    force_global_ai = bool(g_status == "ai")
    if st.button("AI详细分析", use_container_width=True, type="primary"):
        with st.spinner("正在生成用户级 AI 详细报告..."):
            out = kg.generate_user_global_review_ai(
                viewer_id=viewer_id,
                target_user=selected_user,
                max_trades=2000,
                force=force_global_ai,
            )
        if out.get("ok"):
            if force_global_ai:
                st.success("AI详细分析已刷新")
            else:
                st.success("AI详细分析已生成")
            st.rerun()
        else:
            st.error(f"生成失败：{out.get('message', '未知错误')}")

    st.write("高频问题习惯：")
    top_habits = gh.get("top_habits") or []
    if top_habits:
        for h in top_habits[:6]:
            st.write(f"- {h.get('tag')}：{int(h.get('count') or 0)} 次")
    else:
        st.write("- 暂无高频问题标签")

    st.markdown("#### 能力五角图")
    _render_radar(gr)

    st.markdown("#### AI详细报告")
    if gai:
        st.write(f"画像总结：{gai.get('profile_summary') or '暂无'}")
        st.write("核心习惯：")
        for item in gai.get("core_habits") or []:
            st.write(f"- {item.get('tag')} ({int(item.get('count') or 0)} 次)")
        st.write("维度诊断：")
        for item in gai.get("dimension_diagnosis") or []:
            st.write(f"- {item.get('dimension')}：{item.get('score')} - {item.get('note') or ''}")
        st.write("7天改进计划：")
        for x in gai.get("improvement_plan_7d") or []:
            st.write(f"- {x}")
        st.write("30天改进计划：")
        for x in gai.get("improvement_plan_30d") or []:
            st.write(f"- {x}")
    else:
        st.info("暂无AI详细报告，可点击按钮生成")
