"""
K线肉鸽卡牌 MVP（独立页）
- 不改原 K线训练页面
- 独立存档与独立经验系统（kline_card_*）
"""

import os
import sys
import time
from typing import Dict, List, Optional

import extra_streamlit_components as stx
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import auth_utils as auth
import kline_card_rules as rules
import kline_card_storage as storage


st.set_page_config(page_title="K线卡牌MVP", page_icon="🃏", layout="wide", initial_sidebar_state="collapsed")



# 🔥 添加统一的侧边栏导航
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sidebar_navigation import show_navigation
with st.sidebar:
    show_navigation()

st.markdown(
    """
    <style>
    .stApp { background: #0b1121 !important; color: #e2e8f0 !important; }
    [data-testid="stHeader"] { background: transparent !important; box-shadow: none !important; border-bottom: 0 !important; }
    .block-container { max-width: 1180px !important; padding-top: 1rem !important; }
    .card-box {
        background: linear-gradient(155deg, rgba(15,23,42,0.98), rgba(30,41,59,0.78));
        border: 1px solid #334155;
        border-radius: 12px;
        padding: 12px;
    }
    .kpi {
        background: rgba(15,23,42,0.82);
        border: 1px solid #334155;
        border-radius: 10px;
        padding: 8px 10px;
    }
    .kpi b { color: #f8fafc; }
    .hand-card {
        border-radius: 12px;
        border: 1px solid #334155;
        padding: 10px;
        min-height: 154px;
        background: linear-gradient(155deg, rgba(15,23,42,0.98), rgba(30,41,59,0.85));
    }
    .hand-tier {
        font-size: 12px;
        border-radius: 999px;
        padding: 2px 8px;
        display: inline-block;
        margin-bottom: 8px;
    }
    .tier-1 { background: rgba(59,130,246,0.22); color: #93c5fd; }
    .tier-2 { background: rgba(16,185,129,0.22); color: #6ee7b7; }
    .tier-3 { background: rgba(245,158,11,0.25); color: #fcd34d; }
    .hand-name { font-size: 16px; font-weight: 700; color: #f8fafc; }
    .hand-meta { font-size: 12px; color: #94a3b8; margin-top: 4px; }
    .hand-desc { font-size: 13px; color: #cbd5e1; margin-top: 10px; line-height: 1.35; }
    .queue-tip {
        font-size: 12px;
        color: #93c5fd;
        background: rgba(30, 64, 175, 0.2);
        border: 1px solid rgba(59,130,246,0.28);
        border-radius: 8px;
        padding: 8px 10px;
    }
    .settle-fx-wrap {
        border: 1px solid #334155;
        border-radius: 12px;
        padding: 10px 12px;
        margin: 10px 0;
        background: linear-gradient(155deg, rgba(15,23,42,0.98), rgba(30,41,59,0.82));
        animation: fx-enter 0.5s ease;
    }
    .settle-fx-grid {
        display: grid;
        grid-template-columns: repeat(3, minmax(90px, 1fr));
        gap: 10px;
    }
    .settle-item {
        border-radius: 10px;
        padding: 8px 10px;
        border: 1px solid #334155;
        background: rgba(2,6,23,0.45);
    }
    .settle-title { font-size: 12px; color: #94a3b8; }
    .settle-value { font-size: 20px; font-weight: 800; }
    .up { color: #f43f5e; animation: pulse-up 0.9s ease; }
    .down { color: #22c55e; animation: pulse-down 0.9s ease; }
    .flat { color: #e2e8f0; }
    @keyframes pulse-up {
        0% { transform: translateY(6px); opacity: 0.25; }
        100% { transform: translateY(0); opacity: 1; }
    }
    @keyframes pulse-down {
        0% { transform: translateY(6px); opacity: 0.25; }
        100% { transform: translateY(0); opacity: 1; }
    }
    @keyframes fx-enter {
        0% { transform: translateY(8px); opacity: 0; }
        100% { transform: translateY(0); opacity: 1; }
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def _init_session():
    if "is_logged_in" not in st.session_state:
        st.session_state["is_logged_in"] = False
        st.session_state["user_id"] = None
        st.session_state["token"] = None
    if "card_cookie_retry_once" not in st.session_state:
        st.session_state["card_cookie_retry_once"] = False
    if "card_run_id" not in st.session_state:
        st.session_state["card_run_id"] = None
    if "card_last_turn_result" not in st.session_state:
        st.session_state["card_last_turn_result"] = {}
    if "card_boot_message" not in st.session_state:
        st.session_state["card_boot_message"] = ""
    if "card_queue" not in st.session_state:
        st.session_state["card_queue"] = {}
    if "card_fx_nonce" not in st.session_state:
        st.session_state["card_fx_nonce"] = 0


def _restore_login():
    cookie_manager = stx.CookieManager(key="card_mvp_cookie_manager")
    cookies = cookie_manager.get_all() or {}
    if not st.session_state.get("is_logged_in") and not st.session_state.get("just_logged_out", False):
        restored = auth.restore_login_from_cookies(cookies)
        if not restored and not cookies and not st.session_state.get("card_cookie_retry_once", False):
            st.session_state["card_cookie_retry_once"] = True
            time.sleep(0.15)
            st.rerun()
        elif not restored and (cookies.get("username") or cookies.get("token")):
            try:
                cookie_manager.delete("username", key="card_del_user")
                cookie_manager.delete("token", key="card_del_token")
            except Exception:
                pass
    if st.session_state.get("just_logged_out", False):
        st.session_state["just_logged_out"] = False


def _card_tag_text(tag: str) -> str:
    mapping = {"bull": "偏多", "bear": "偏空", "neutral": "通用"}
    return mapping.get(str(tag), "通用")


def _card_tier_text(tier: int) -> str:
    mapping = {1: "普通", 2: "稀有", 3: "史诗"}
    return mapping.get(int(tier or 1), "普通")


def _prepare_run_with_feedback(user_id: str, resume_run_id: Optional[int] = None, resume_stage: Optional[int] = None):
    if resume_run_id:
        rid = int(resume_run_id)
        stage_no = int(resume_stage or 1)
        with st.status("正在恢复对局...", expanded=True) as status:
            status.write("步骤1/1：预热当前关卡候选数据")
            prep = storage.start_stage(rid, stage_no, None)
            if not prep.get("ok"):
                status.update(label=f"恢复失败：{prep.get('message', '未知错误')}", state="error")
                return {"ok": False, "message": prep.get("message", "恢复失败")}
            status.update(label="恢复完成", state="complete")
        return {"ok": True, "run_id": rid}

    with st.status("正在创建新对局...", expanded=True) as status:
        status.write("步骤1/2：创建独立游戏存档")
        run_id = storage.create_run(user_id)
        if not run_id:
            status.update(label="创建失败", state="error")
            return {"ok": False, "message": "创建对局失败"}
        status.write("步骤2/2：抽取第一关候选K线（最多3个）")
        prep = storage.start_stage(run_id, 1, None)
        if not prep.get("ok"):
            status.update(label=f"开局失败：{prep.get('message', '未知错误')}", state="error")
            return {"ok": False, "message": prep.get("message", "开局准备失败")}
        status.update(label="开局准备完成，请选择标的", state="complete")
    return {"ok": True, "run_id": int(run_id)}


def _render_kline_chart(bars: List[Dict[str, object]], title: str):
    if not bars:
        st.info("暂无可展示K线")
        return
    df = pd.DataFrame(bars)
    if df.empty:
        st.info("暂无可展示K线")
        return

    candle_colors = ["#ef4444" if c >= o else "#22c55e" for o, c in zip(df["open"], df["close"])]
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.78, 0.22],
    )
    fig.add_trace(
        go.Candlestick(
            x=df["date"],
            open=df["open"],
            high=df["high"],
            low=df["low"],
            close=df["close"],
            increasing_line_color="#ef4444",
            decreasing_line_color="#22c55e",
            increasing_fillcolor="#ef4444",
            decreasing_fillcolor="#22c55e",
            showlegend=False,
            name="K线",
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Bar(
            x=df["date"],
            y=df["volume"],
            marker_color=candle_colors,
            opacity=0.9,
            showlegend=False,
            name="成交量",
        ),
        row=2,
        col=1,
    )
    fig.update_layout(
        title=title,
        template="plotly_dark",
        height=420,
        margin=dict(l=20, r=20, t=40, b=20),
        paper_bgcolor="#0f172a",
        plot_bgcolor="#0f172a",
    )
    fig.update_xaxes(rangeslider_visible=False, showticklabels=False, row=1, col=1)
    fig.update_xaxes(showticklabels=False, row=2, col=1)
    fig.update_yaxes(title_text="价格", row=1, col=1)
    fig.update_yaxes(title_text="量", row=2, col=1)
    st.plotly_chart(fig, use_container_width=True, config={"displaylogo": False})


def _render_settle_fx(last_res: Dict[str, object]):
    if not last_res:
        return
    turn_score = int(last_res.get("turn_score", 0) or 0)
    conf_delta = int(last_res.get("confidence_delta", 0) or 0)
    stage_delta = int(last_res.get("stage_score_delta", turn_score) or 0)

    def _cls(v: int) -> str:
        if v > 0:
            return "up"
        if v < 0:
            return "down"
        return "flat"

    def _fmt(v: int) -> str:
        return f"+{v}" if v > 0 else str(v)

    st.markdown(
        f"""
        <div class="settle-fx-wrap">
            <div class="settle-fx-grid">
                <div class="settle-item">
                    <div class="settle-title">回合得分</div>
                    <div class="settle-value {_cls(turn_score)}">{_fmt(turn_score)}</div>
                </div>
                <div class="settle-item">
                    <div class="settle-title">信心变化</div>
                    <div class="settle-value {_cls(conf_delta)}">{_fmt(conf_delta)}</div>
                </div>
                <div class="settle-item">
                    <div class="settle-title">关卡分变化</div>
                    <div class="settle-value {_cls(stage_delta)}">{_fmt(stage_delta)}</div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_meta_panel(user_id: str):
    meta = storage.get_card_meta(user_id)
    if not meta.get("ok"):
        st.error("独立经验系统初始化失败")
        return meta

    st.markdown("### 局外成长（独立经验）")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(f'<div class="kpi">等级<br><b>Lv.{meta["level"]}</b></div>', unsafe_allow_html=True)
    with c2:
        st.markdown(f'<div class="kpi">经验<br><b>{meta["exp"]}</b></div>', unsafe_allow_html=True)
    with c3:
        st.markdown(f'<div class="kpi">可用点数<br><b>{meta["skill_points"]}</b></div>', unsafe_allow_html=True)
    with c4:
        st.markdown(f'<div class="kpi">通关次数<br><b>{meta["games_cleared"]}</b></div>', unsafe_allow_html=True)

    upgrades = dict(meta.get("upgrades") or {})
    with st.expander("加点面板", expanded=False):
        for code, cfg in rules.META_UPGRADES.items():
            current = int(upgrades.get(code, 0) or 0)
            max_lv = int(cfg.get("max_level", 0) or 0)
            col_a, col_b = st.columns([4, 1])
            with col_a:
                st.write(f"**{cfg.get('name', code)}**  Lv.{current}/{max_lv}")
            with col_b:
                can_up = meta.get("skill_points", 0) > 0 and current < max_lv
                if st.button("升级", key=f"meta_up_{code}", disabled=not can_up):
                    out = storage.apply_card_meta_upgrade(user_id, code)
                    if out.get("ok"):
                        st.success(f"已升级：{cfg.get('name', code)}")
                    else:
                        st.warning(out.get("message", "升级失败"))
                    st.rerun()
    return meta


def _render_stage_choice(run_id: int, stage_no: int, candidates: List[Dict[str, object]]):
    st.markdown(f"### 第 {stage_no} 关：请选择挑战标的（3选1）")
    cols = st.columns(3)
    for idx, c in enumerate(candidates):
        with cols[idx % 3]:
            symbol = c.get("symbol", "N/A")
            symbol_name = c.get("symbol_name", symbol)
            symbol_type = c.get("symbol_type", "unknown")
            bars = c.get("bars", [])
            st.markdown('<div class="card-box">', unsafe_allow_html=True)
            st.markdown(f"**{symbol_name}**")
            st.caption(f"{symbol} | {symbol_type}")
            if bars:
                first_close = float(bars[0].get("close", 0))
                last_close = float(bars[min(len(bars) - 1, 19)].get("close", 0))
                ret = (last_close / first_close - 1.0) * 100 if first_close > 0 else 0
                st.write(f"前20根涨跌幅: `{ret:+.2f}%`")
            if st.button(f"选择 {symbol}", key=f"pick_{run_id}_{stage_no}_{symbol}", use_container_width=True):
                with st.status(f"正在载入 {symbol} 关卡...", expanded=False) as status:
                    out = storage.start_stage(run_id, stage_no, symbol)
                    if out.get("ok"):
                        status.update(label="载入完成", state="complete")
                    else:
                        status.update(label=f"载入失败：{out.get('message', '未知错误')}", state="error")
                if out.get("ok"):
                    st.rerun()
                st.error(out.get("message", "开关失败"))
            st.markdown("</div>", unsafe_allow_html=True)


def _render_upgrade_selection(run_id: int, run: Dict[str, object]):
    options = list(run.get("pending_upgrades") or [])
    st.markdown("### 关卡已通过：选择 1 个强化")
    if not options:
        st.warning("当前无可选强化，系统将自动进入下一关。")
        out = storage.finish_stage(run_id)
        if out.get("ok"):
            st.rerun()
        return

    cols = st.columns(2)
    for idx, opt in enumerate(options):
        code = str(opt.get("code", ""))
        with cols[idx % 2]:
            st.markdown('<div class="card-box">', unsafe_allow_html=True)
            st.write(f"**{opt.get('name', code)}**")
            if st.button("选择强化", key=f"upgrade_{run_id}_{code}", use_container_width=True):
                out = storage.apply_stage_upgrade(run_id, code)
                if out.get("ok"):
                    st.success("强化已生效，进入下一关。")
                    st.rerun()
                st.error(out.get("message", "强化失败"))
            st.markdown("</div>", unsafe_allow_html=True)


def _render_run_panel(run_id: int):
    state = storage.get_run_state(run_id)
    if not state.get("ok"):
        st.error(state.get("message", "读取局状态失败"))
        st.session_state["card_run_id"] = None
        st.stop()

    run = dict(state.get("run") or {})
    stage = dict(state.get("stage") or {})
    run_status = str(run.get("status", ""))
    stage_no = int(run.get("current_stage", 1))

    if run_status == "await_stage_start":
        with st.status("正在准备本关...", expanded=False) as status:
            prep = storage.start_stage(run_id, stage_no, None)
            if prep.get("ok"):
                status.update(label="准备完成", state="complete")
            else:
                status.update(label=f"准备失败：{prep.get('message', '未知错误')}", state="error")
        if prep.get("ok") and prep.get("need_choice"):
            _render_stage_choice(run_id, stage_no, prep.get("candidates") or [])
            return
        if prep.get("ok"):
            st.info("关卡已准备，进入战斗。")
            st.rerun()
        st.error(prep.get("message", "关卡准备失败"))
        return

    if run_status == "stage_cleared":
        _render_upgrade_selection(run_id, run)
        return

    if run_status in {"failed", "cleared"}:
        done = storage.finish_run(run_id)
        st.markdown("### 本局结算")
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("结果", "通关" if run_status == "cleared" else "失败")
        with c2:
            st.metric("总分", int(run.get("total_score", 0)))
        with c3:
            st.metric("获得经验", int(done.get("reward_exp", 0)))

        if st.button("🎮 再开新局", type="primary", use_container_width=True):
            new_run = storage.create_run(st.session_state.get("user_id"))
            st.session_state["card_run_id"] = new_run or None
            st.rerun()
        return

    stage_status = str(stage.get("status", ""))
    if stage_status == "choose_symbol":
        _render_stage_choice(run_id, stage_no, stage.get("candidate_pool") or [])
        return

    if stage_status in {"cleared", "failed"}:
        out = storage.finish_stage(run_id)
        if out.get("ok"):
            st.rerun()
        st.error(out.get("message", "阶段结算失败"))
        return

    if stage_status != "playing":
        st.warning("关卡状态异常，尝试重建关卡。")
        out = storage.start_stage(run_id, stage_no, None)
        if out.get("ok"):
            st.rerun()
        st.error(out.get("message", "无法恢复关卡"))
        return

    bars = list(stage.get("bars") or [])
    visible_end = int(stage.get("visible_end", 20))
    visible_bars = bars[:max(20, visible_end)]
    event_state = dict(stage.get("event_state") or {})

    st.markdown(f"### 第 {stage_no} 关战斗中：{stage.get('symbol_name', stage.get('symbol', '未知标的'))}")
    k1, k2, k3, k4, k5 = st.columns(5)
    with k1:
        st.metric("回合", f"{int(stage.get('current_turn', 1))}/20")
    with k2:
        st.metric("信心", int(run.get("confidence", 0)))
    with k3:
        st.metric("关卡分", int(stage.get("stage_score", 0)))
    with k4:
        st.metric("目标分", int(stage.get("target_score", 0)))
    with k5:
        st.metric("总分", int(run.get("total_score", 0)))

    if event_state:
        st.info(
            f"随机事件：{event_state.get('name', '未知')} | 触发回合: {event_state.get('trigger_turn', 10)} | 已触发: {'是' if event_state.get('applied') else '否'}"
        )

    _render_kline_chart(visible_bars, "已公开K线")

    hand = list(run.get("hand") or [])
    queue_bucket = st.session_state.get("card_queue") or {}
    queue_key = str(run_id)
    queued_cards = list(queue_bucket.get(queue_key) or [])
    # 保证等待区不会超过当前手牌可用数量
    hand_count: Dict[str, int] = {}
    for h in hand:
        hand_count[h] = hand_count.get(h, 0) + 1
    normalized_queue: List[str] = []
    used_count: Dict[str, int] = {}
    for q in queued_cards:
        qid = str(q)
        used = used_count.get(qid, 0)
        if used < hand_count.get(qid, 0):
            normalized_queue.append(qid)
            used_count[qid] = used + 1
    if normalized_queue != queued_cards:
        queue_bucket[queue_key] = normalized_queue
        st.session_state["card_queue"] = queue_bucket
    queued_cards = normalized_queue

    if hand:
        st.markdown("### 手牌区")
        st.markdown('<div class="queue-tip">点击“加入等待区”后可在下方调整顺序，最后点击“执行等待区并结束回合”。</div>', unsafe_allow_html=True)
        cols = st.columns(4)
        for idx, card_id in enumerate(hand):
            card = rules.CARD_LIBRARY.get(card_id, {})
            tier = int(card.get("tier", 1) or 1)
            tag = _card_tag_text(str(card.get("tag", "neutral")))
            with cols[idx % 4]:
                st.markdown(
                    f"""
                    <div class="hand-card">
                        <span class="hand-tier tier-{tier}">{_card_tier_text(tier)}</span>
                        <div class="hand-name">{card.get("name", card_id)}</div>
                        <div class="hand-meta">定位：{tag} | 基础值：{card.get("base", "-")}</div>
                        <div class="hand-desc">{card.get("desc", "暂无说明")}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
                queued_n = queued_cards.count(card_id)
                can_enqueue = queued_n < hand.count(card_id)
                if st.button("加入等待区", key=f"pick_card_{run_id}_{idx}_{card_id}", use_container_width=True, disabled=not can_enqueue):
                    queue_bucket = st.session_state.get("card_queue") or {}
                    q = list(queue_bucket.get(queue_key) or [])
                    q.append(card_id)
                    queue_bucket[queue_key] = q
                    st.session_state["card_queue"] = queue_bucket
                    st.rerun()
    else:
        st.caption("当前手牌为空，仅可 PASS。")

    st.markdown("### 等待区（执行顺序）")
    if queued_cards:
        st.caption("回合执行会按从上到下顺序结算，后出牌会有递减权重。")
        for i, cid in enumerate(queued_cards):
            c_name = rules.CARD_LIBRARY.get(cid, {}).get("name", cid)
            q1, q2, q3, q4 = st.columns([5, 1, 1, 1])
            with q1:
                st.markdown(f"`{i + 1}.` {c_name} (`{cid}`)")
            with q2:
                if st.button("↑", key=f"q_up_{run_id}_{i}", disabled=(i == 0)):
                    q = list(queued_cards)
                    q[i - 1], q[i] = q[i], q[i - 1]
                    queue_bucket = st.session_state.get("card_queue") or {}
                    queue_bucket[queue_key] = q
                    st.session_state["card_queue"] = queue_bucket
                    st.rerun()
            with q3:
                if st.button("↓", key=f"q_dn_{run_id}_{i}", disabled=(i >= len(queued_cards) - 1)):
                    q = list(queued_cards)
                    q[i + 1], q[i] = q[i], q[i + 1]
                    queue_bucket = st.session_state.get("card_queue") or {}
                    queue_bucket[queue_key] = q
                    st.session_state["card_queue"] = queue_bucket
                    st.rerun()
            with q4:
                if st.button("移除", key=f"q_rm_{run_id}_{i}"):
                    q = list(queued_cards)
                    q.pop(i)
                    queue_bucket = st.session_state.get("card_queue") or {}
                    queue_bucket[queue_key] = q
                    st.session_state["card_queue"] = queue_bucket
                    st.rerun()
    else:
        st.caption("等待区为空。你可以从上方手牌“加入等待区”，再点击执行。")

    c_action_1, c_action_2, c_action_3 = st.columns(3)
    with c_action_1:
        play_disabled = not bool(queued_cards)
        if st.button("🃏 执行等待区并结束回合", type="primary", use_container_width=True, disabled=play_disabled, key=f"play_queue_{run_id}"):
            out = storage.play_turn(run_id, {"type": "combo", "cards": queued_cards})
            if out.get("ok"):
                st.session_state["card_last_turn_result"] = out
                st.session_state["card_fx_nonce"] = time.time()
                queue_bucket = st.session_state.get("card_queue") or {}
                queue_bucket[queue_key] = []
                st.session_state["card_queue"] = queue_bucket
                if out.get("stage_complete"):
                    storage.finish_stage(run_id)
                    if out.get("run_status") in {"failed", "cleared"}:
                        storage.finish_run(run_id)
                st.rerun()
            st.error(out.get("message", "回合结算失败"))
    with c_action_2:
        if st.button("PASS（跳过本回合）", use_container_width=True, key=f"pass_turn_{run_id}"):
            out = storage.play_turn(run_id, {"type": "pass"})
            if out.get("ok"):
                st.session_state["card_last_turn_result"] = out
                st.session_state["card_fx_nonce"] = time.time()
                queue_bucket = st.session_state.get("card_queue") or {}
                queue_bucket[queue_key] = []
                st.session_state["card_queue"] = queue_bucket
                if out.get("stage_complete"):
                    storage.finish_stage(run_id)
                    if out.get("run_status") in {"failed", "cleared"}:
                        storage.finish_run(run_id)
                st.rerun()
            st.error(out.get("message", "回合结算失败"))
    with c_action_3:
        if st.button("清空等待区", use_container_width=True, key=f"clear_queue_{run_id}", disabled=not bool(queued_cards)):
            queue_bucket = st.session_state.get("card_queue") or {}
            queue_bucket[queue_key] = []
            st.session_state["card_queue"] = queue_bucket
            st.rerun()

    last_res = st.session_state.get("card_last_turn_result") or stage.get("last_result") or {}
    if last_res:
        _render_settle_fx(last_res)
        played_card = str(last_res.get("played_card") or last_res.get("card_id") or "")
        played_cards = list(last_res.get("played_cards") or [])
        if played_cards:
            played_text = " -> ".join([rules.CARD_LIBRARY.get(cid, {}).get("name", cid) for cid in played_cards])
        else:
            played_text = ""
        played_card_name = rules.CARD_LIBRARY.get(played_card, {}).get("name", played_card) if played_card else "PASS"
        action_type = str(last_res.get("action_type", ""))
        if action_type == "pass":
            action_text = "PASS"
        elif action_type == "combo":
            action_text = f"连携：{played_text or played_card_name}"
        else:
            action_text = f"出牌：{played_card_name}"
        st.markdown("#### 最近一回合")
        st.write(
            f"{action_text} | 得分 `{int(last_res.get('turn_score', 0))}` | 达标线 `{int(last_res.get('threshold', 0))}` | "
            f"罚分 `{int(last_res.get('penalty', 0))}` | 信心 `{int(last_res.get('confidence', last_res.get('confidence_after', 0)))}`"
        )
        card_results = list(last_res.get("card_results") or [])
        if card_results:
            detail_parts = []
            for item in card_results:
                cid = str(item.get("card_id", ""))
                nm = rules.CARD_LIBRARY.get(cid, {}).get("name", cid)
                detail_parts.append(f"{item.get('order', 0)}.{nm}:{int(item.get('weighted_score', 0))}")
            st.caption("顺序结算：" + " | ".join(detail_parts))
        if last_res.get("event_message"):
            st.caption(f"事件触发：{last_res.get('event_message')}")


_init_session()
_restore_login()

if not st.session_state.get("is_logged_in"):
    st.warning("请先在首页登录后进入 K线卡牌MVP。")
    st.stop()

storage.init_card_game_schema()
user_id = st.session_state.get("user_id")

st.markdown("## 🃏 K线肉鸽卡牌 MVP")
st.caption("真实历史K线 + 随机事件 + 关卡挑战。经验值与原系统完全独立。")

_render_meta_panel(user_id)

resume = storage.get_resume_run(user_id)
if st.session_state.get("card_run_id") is None and resume:
    st.session_state["card_run_id"] = int(resume.get("run_id"))

c1, c2, c3 = st.columns([1, 1, 1])
with c1:
    if st.button("新开一局", use_container_width=True):
        out = _prepare_run_with_feedback(user_id)
        if out.get("ok"):
            st.session_state["card_run_id"] = out.get("run_id")
            st.session_state["card_last_turn_result"] = {}
            st.toast("新对局已创建，请选择第一关标的。")
            st.rerun()
        st.error(out.get("message", "开局失败"))
with c2:
    if resume and st.button("继续未完成局", use_container_width=True):
        resume_status = str(resume.get("status", ""))
        if resume_status == "await_stage_start":
            out = _prepare_run_with_feedback(
                user_id=user_id,
                resume_run_id=int(resume.get("run_id")),
                resume_stage=int(resume.get("current_stage", 1)),
            )
        else:
            out = {"ok": True, "run_id": int(resume.get("run_id"))}
        if out.get("ok"):
            st.session_state["card_run_id"] = int(resume.get("run_id"))
            st.toast("已恢复到未完成对局。")
            st.rerun()
        st.error(out.get("message", "恢复失败"))
with c3:
    if st.button("清空当前局视图", use_container_width=True):
        st.session_state["card_run_id"] = None
        st.session_state["card_last_turn_result"] = {}
        st.rerun()

run_id = st.session_state.get("card_run_id")
if not run_id:
    st.markdown(
        """
        ### MVP 规则速览
        - 每局 5 关，每关 20 回合，第 5 关为魔王关（强制随机标的）
        - 首回合 3 张牌，之后每回合抽 1 张，手牌上限 10
        - 每回合看已公开K线后出牌，系统用未来 5 根真实K线结算
        - 双失败条件：信心归零 或 关卡总分不达标
        - 关卡间自动存档，可中途退出后继续
        """
    )
    st.stop()

_render_run_panel(int(run_id))
