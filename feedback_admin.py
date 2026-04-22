import os
import sys
from datetime import date, datetime, time, timedelta

import extra_streamlit_components as stx
import pandas as pd
import plotly.express as px
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import auth_utils as auth
import data_engine as de
from chat_feedback_service import (
    default_feedback_sample_optimization_type,
    ensure_chat_feedback_tables,
    list_chat_feedback_events,
    list_chat_feedback_failure_candidates,
    list_chat_feedback_samples,
    update_chat_feedback_sample,
    upsert_chat_feedback_sample,
)


st.set_page_config(
    page_title="爱波塔 - 反馈后台",
    page_icon="🧭",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown(
    """
    <style>
    .stApp {
        background:
            radial-gradient(circle at top left, rgba(29, 78, 216, 0.18), transparent 28%),
            linear-gradient(180deg, #081120 0%, #0b1324 100%);
        color: #e5eefc;
    }
    [data-testid="stHeader"] { background: transparent !important; }
    [data-testid="stDecoration"] { display: none !important; }
    [data-testid="stSidebar"] { display: none !important; }
    .block-container { padding-top: 1.1rem !important; padding-bottom: 2rem !important; }
    .hero {
        background: linear-gradient(135deg, rgba(15, 23, 42, 0.96), rgba(30, 64, 175, 0.72));
        border: 1px solid rgba(96, 165, 250, 0.28);
        border-radius: 18px;
        padding: 18px 20px;
        margin-bottom: 14px;
        box-shadow: 0 14px 32px rgba(2, 6, 23, 0.28);
    }
    .hero-title {
        font-size: 30px;
        font-weight: 800;
        color: #f8fafc;
        margin-bottom: 8px;
    }
    .hero-sub {
        color: #cbd5e1;
        font-size: 14px;
        line-height: 1.7;
    }
    .kpi-card {
        background: rgba(15, 23, 42, 0.92);
        border: 1px solid rgba(148, 163, 184, 0.18);
        border-radius: 14px;
        padding: 14px 16px;
        min-height: 102px;
        box-shadow: inset 0 0 0 1px rgba(59, 130, 246, 0.06);
    }
    .kpi-label {
        color: #93c5fd;
        font-size: 13px;
        margin-bottom: 8px;
    }
    .kpi-value {
        color: #f8fafc;
        font-size: 28px;
        font-weight: 800;
        line-height: 1.1;
    }
    .kpi-sub {
        color: #94a3b8;
        font-size: 12px;
        margin-top: 8px;
        line-height: 1.5;
    }
    .panel-title {
        color: #f8fafc;
        font-size: 20px;
        font-weight: 800;
        margin-top: 4px;
        margin-bottom: 8px;
    }
    .panel-sub {
        color: #94a3b8;
        font-size: 13px;
        margin-bottom: 10px;
    }
    .tag {
        display: inline-block;
        padding: 4px 10px;
        border-radius: 999px;
        border: 1px solid rgba(96, 165, 250, 0.28);
        background: rgba(30, 64, 175, 0.2);
        color: #dbeafe;
        font-size: 12px;
        font-weight: 700;
        margin-right: 8px;
    }
    .stExpander summary,
    .stExpander summary p,
    .stExpander label,
    div[data-testid="stExpander"] summary,
    div[data-testid="stExpander"] summary p {
        color: #e2e8f0 !important;
    }
    .stSelectbox label,
    .stTextInput label,
    .stDateInput label,
    .stCheckbox label,
    .stMultiSelect label,
    .stNumberInput label,
    .stRadio label,
    .stTextArea label,
    div[data-testid="stWidgetLabel"] label,
    div[data-testid="stWidgetLabel"] p {
        color: #cbd5e1 !important;
        opacity: 1 !important;
        font-weight: 600 !important;
    }
    .stCheckbox label p,
    .stSelectbox label p,
    .stTextInput label p,
    .stDateInput label p {
        color: #cbd5e1 !important;
    }
    div[data-baseweb="select"] span,
    div[data-baseweb="select"] input,
    div[data-baseweb="input"] input,
    .stDateInput input,
    .stTextInput input {
        color: #0f172a !important;
    }
    div[data-testid="stExpanderDetails"] {
        border-top: 1px solid rgba(148, 163, 184, 0.12);
    }
    </style>
    """,
    unsafe_allow_html=True,
)


DEFAULT_ADMIN_USERS = {"mike0919"}
ENV_ADMIN_USERS = {
    item.strip()
    for item in str(os.getenv("AI_FEEDBACK_ADMIN_USERS", "")).split(",")
    if item.strip()
}
ADMIN_USERS = DEFAULT_ADMIN_USERS | ENV_ADMIN_USERS

REASON_LABELS = {
    "not_personalized": "不够贴合用户情况",
    "too_generic": "太泛，不够具体",
    "wrong_fact": "事实或数据不对",
    "not_actionable": "没有给出可执行建议",
}

REASON_ACTIONS = {
    "not_personalized": "强化用户画像、持仓信息和风险偏好注入，优先检查个性化提示词链路。",
    "too_generic": "补强回答模板，强制输出更具体的步骤、条件、仓位和风险提示。",
    "wrong_fact": "优先检查数据源、RAG 检索和事实校验逻辑，必要时增加来源约束。",
    "not_actionable": "要求回答必须包含下一步动作、触发条件和止损/观察线。",
}

OWNER_LANES = {
    "not_personalized": "个性化",
    "too_generic": "Prompt",
    "wrong_fact": "数据/RAG",
    "not_actionable": "行动建议",
}

PRIORITY_WEIGHTS = {
    "not_personalized": 1.2,
    "too_generic": 1.0,
    "wrong_fact": 1.5,
    "not_actionable": 1.35,
}

SAMPLE_STATUS_LABELS = {
    "new": "待处理",
    "reviewed": "已审核",
    "accepted": "已采纳",
    "rejected": "不采纳",
    "fixed": "已修复",
}

OPTIMIZATION_TYPE_LABELS = {
    "prompt": "Prompt",
    "rag": "RAG",
    "rule": "规则",
    "fine_tune": "微调",
}


def _reason_label(code: str) -> str:
    normalized = str(code or "").strip().lower()
    if not normalized:
        return "未填写"
    return REASON_LABELS.get(normalized, normalized)


def _sample_status_label(code: str) -> str:
    normalized = str(code or "").strip().lower()
    if not normalized:
        return "未设置"
    return SAMPLE_STATUS_LABELS.get(normalized, normalized)


def _optimization_type_label(code: str) -> str:
    normalized = str(code or "").strip().lower()
    if not normalized:
        return "未设置"
    return OPTIMIZATION_TYPE_LABELS.get(normalized, normalized)


def _render_login_hint() -> None:
    st.info("请先在主站完成登录，再打开这个独立反馈后台。")
    st.code("streamlit run Home.py", language="powershell")
    st.stop()


def _restore_login_with_cookie_state(cookies: dict):
    cookies = cookies or {}
    try:
        restored = auth.restore_login_from_cookies(cookies)
    except Exception as exc:
        print(f"[feedback-admin] restore_login_from_cookies failed: {exc}")
        return False, "error"

    if restored:
        return True, "ok"

    c_user = str(cookies.get("username") or "").strip()
    c_token = str(cookies.get("token") or "").strip()
    if not c_user and not c_token:
        return False, "empty"
    if (c_user and not c_token) or (c_token and not c_user):
        return False, "partial"
    return False, "invalid"


def _auto_restore_login_if_needed() -> None:
    if st.session_state.get("is_logged_in", False):
        return
    if st.session_state.get("just_logged_out", False):
        return

    cookie_manager = stx.CookieManager(key="feedback_admin_cookie_manager")
    cookies = cookie_manager.get_all() or {}
    restored, state = _restore_login_with_cookie_state(cookies)
    if restored:
        st.rerun()

    if state in {"empty", "partial", "error"} and not st.session_state.get("feedback_admin_cookie_retry_once", False):
        st.session_state["feedback_admin_cookie_retry_once"] = True
        st.rerun()


def _current_user() -> str:
    return str(st.session_state.get("user_id") or "").strip()


def _ensure_feedback_admin() -> None:
    if not st.session_state.get("is_logged_in", False):
        _render_login_hint()

    current_user = _current_user()
    if not current_user:
        _render_login_hint()

    if current_user not in ADMIN_USERS:
        st.error(f"当前账号 `{current_user}` 没有查看反馈后台的权限。")
        st.caption(f"当前默认管理员包含：{', '.join(sorted(ADMIN_USERS))}")
        st.stop()


def _combine_date_bounds(date_range):
    today = date.today()
    default_start = today - timedelta(days=13)

    if isinstance(date_range, (list, tuple)):
        if len(date_range) >= 2:
            start_date = date_range[0] or default_start
            end_date = date_range[1] or today
        elif len(date_range) == 1:
            start_date = date_range[0] or default_start
            end_date = start_date
        else:
            start_date, end_date = default_start, today
    else:
        start_date = date_range or default_start
        end_date = date_range or today

    start_at = datetime.combine(start_date, time.min).isoformat()
    end_at = datetime.combine(end_date, time.max).isoformat()
    return start_date, end_date, start_at, end_at


def _load_feedback_event_df(
    *,
    limit: int,
    feedback_type: str,
    user_id: str,
    intent_domain: str,
    reason_code: str,
    keyword: str,
    start_at: str,
    end_at: str,
) -> pd.DataFrame:
    events = list_chat_feedback_events(
        de.engine,
        limit=limit,
        feedback_type=feedback_type,
        user_id=user_id,
        intent_domain=intent_domain,
        reason_code=reason_code,
        keyword=keyword,
        start_at=start_at,
        end_at=end_at,
    )
    df = pd.DataFrame(events)
    if df.empty:
        return df
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["reason_label"] = df["reason_code"].map(_reason_label)
    df["feedback_text"] = df["feedback_text"].fillna("")
    df["prompt_text"] = df["prompt_text"].fillna("")
    df["response_text"] = df["response_text"].fillna("")
    df["intent_domain"] = df["intent_domain"].fillna("general")
    return df


def _load_failure_candidates_df(
    *,
    limit: int,
    intent_domain: str,
    reason_code: str,
    keyword: str,
    start_at: str,
    end_at: str,
    min_occurrence: int,
) -> pd.DataFrame:
    rows = list_chat_feedback_failure_candidates(
        de.engine,
        limit=limit,
        intent_domain=intent_domain,
        reason_code=reason_code,
        keyword=keyword,
        start_at=start_at,
        end_at=end_at,
        min_occurrence=min_occurrence,
    )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["reason_label"] = df["reason_code"].map(_reason_label)
    df["latest_feedback_at"] = pd.to_datetime(df["latest_feedback_at"], errors="coerce")
    return df


def _load_sample_df(
    *,
    limit: int,
    sample_status: str,
    optimization_type: str,
    intent_domain: str,
    reason_code: str,
    keyword: str,
) -> pd.DataFrame:
    rows = list_chat_feedback_samples(
        de.engine,
        limit=limit,
        sample_status=sample_status,
        optimization_type=optimization_type,
        intent_domain=intent_domain,
        reason_code=reason_code,
        keyword=keyword,
    )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["reason_label"] = df["reason_code"].map(_reason_label)
    df["sample_status_label"] = df["sample_status"].map(_sample_status_label)
    df["optimization_type_label"] = df["optimization_type"].map(_optimization_type_label)
    df["last_seen_at"] = pd.to_datetime(df["last_seen_at"], errors="coerce")
    df["reviewed_at"] = pd.to_datetime(df["reviewed_at"], errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")
    df["review_notes"] = df["review_notes"].fillna("")
    df["latest_feedback_text"] = df["latest_feedback_text"].fillna("")
    return df


def _render_kpi_card(label: str, value: str, sub: str = "") -> None:
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value">{value}</div>
            <div class="kpi-sub">{sub}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _format_pct(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return "0.0%"
    return f"{(numerator / denominator * 100):.1f}%"


def _build_domain_health_df(events_df: pd.DataFrame) -> pd.DataFrame:
    if events_df.empty:
        return pd.DataFrame()

    grouped = (
        events_df.groupby(["intent_domain", "feedback_type"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
        .rename_axis(None, axis=1)
    )
    if "up" not in grouped.columns:
        grouped["up"] = 0
    if "down" not in grouped.columns:
        grouped["down"] = 0
    grouped["total"] = grouped["up"] + grouped["down"]
    grouped["satisfaction_rate"] = grouped.apply(
        lambda row: round((row["up"] / row["total"] * 100) if row["total"] else 0.0, 1), axis=1
    )
    grouped["down_rate"] = grouped.apply(
        lambda row: round((row["down"] / row["total"] * 100) if row["total"] else 0.0, 1), axis=1
    )
    return grouped.sort_values(["down", "down_rate", "total"], ascending=[False, False, False])


def _build_priority_queue(failure_df: pd.DataFrame) -> pd.DataFrame:
    if failure_df.empty:
        return pd.DataFrame()

    today = pd.Timestamp.now().normalize()
    queue_df = failure_df.copy()
    queue_df["days_since_last"] = (today - queue_df["latest_feedback_at"].dt.normalize()).dt.days.fillna(999)
    queue_df["reason_weight"] = queue_df["reason_code"].map(lambda code: PRIORITY_WEIGHTS.get(str(code or ""), 1.0))
    queue_df["recency_weight"] = queue_df["days_since_last"].map(
        lambda days: 1.35 if days <= 3 else (1.2 if days <= 7 else (1.1 if days <= 14 else 1.0))
    )
    queue_df["priority_score"] = (
        queue_df["occurrence_count"].astype(float) * queue_df["reason_weight"] * queue_df["recency_weight"]
    ).round(2)
    queue_df["owner_lane"] = queue_df["reason_code"].map(lambda code: OWNER_LANES.get(str(code or ""), "通用优化"))
    queue_df["suggested_action"] = queue_df["reason_code"].map(
        lambda code: REASON_ACTIONS.get(str(code or ""), "回看原始反馈，确认是提示词、知识源还是动作建议问题。")
    )
    queue_df["follow_up_hint"] = queue_df["days_since_last"].map(
        lambda days: "最近 7 天仍在发生" if days <= 7 else ("近两周持续出现" if days <= 14 else "可排入常规修复")
    )
    queue_df = queue_df.sort_values(["priority_score", "occurrence_count", "latest_feedback_at"], ascending=[False, False, False])
    return queue_df


if "is_logged_in" not in st.session_state:
    st.session_state["is_logged_in"] = False
if "user_id" not in st.session_state:
    st.session_state["user_id"] = None
if "token" not in st.session_state:
    st.session_state["token"] = None
if "just_logged_out" not in st.session_state:
    st.session_state["just_logged_out"] = False
if "feedback_admin_cookie_retry_once" not in st.session_state:
    st.session_state["feedback_admin_cookie_retry_once"] = False

_auto_restore_login_if_needed()
_ensure_feedback_admin()
ensure_chat_feedback_tables(de.engine)

st.caption("主站登录入口：`streamlit run Home.py`")

st.markdown(
    """
    <div class="hero">
        <div class="hero-title">反馈后台</div>
        <div class="hero-sub">
            这里把 AI 回答后的真实用户反馈整理成运营视图。我们可以先看哪里被频繁点踩、近期满意度有没有变化，
            再把高频问题整理成可执行的优化优先队列。
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

with st.expander("筛选条件", expanded=True):
    line1 = st.columns([1.1, 1.1, 1.1, 1.2, 1])
    default_start = date.today() - timedelta(days=13)
    with line1[0]:
        event_limit = st.selectbox("读取反馈条数", options=[200, 500, 1000, 2000], index=1)
    with line1[1]:
        selected_range = st.date_input("时间范围", value=(default_start, date.today()), format="YYYY-MM-DD")
    with line1[2]:
        feedback_type_option = st.selectbox("反馈类型", options=["全部", "仅点赞", "仅点踩"], index=0)
    with line1[3]:
        reason_option = st.selectbox(
            "差评原因",
            options=["全部", "不够贴合用户情况", "太泛，不够具体", "事实或数据不对", "没有给出可执行建议"],
            index=0,
        )
    with line1[4]:
        min_occurrence = st.selectbox("失败问题最少出现次数", options=[1, 2, 3, 5], index=1)

    line2 = st.columns([1.1, 1.1, 1.2, 1.4, 0.8])
    with line2[0]:
        user_filter = st.text_input("用户 ID", placeholder="例如 mike0919")
    with line2[1]:
        domain_filter = st.text_input("领域关键词", placeholder="例如 stock_portfolio")
    with line2[2]:
        keyword_filter = st.text_input("关键词", placeholder="搜索问题、回答、补充说明")
    with line2[3]:
        only_with_note = st.checkbox("只看用户写了补充说明的反馈", value=False)
    with line2[4]:
        refresh_clicked = st.button("刷新", type="primary", use_container_width=True)

if refresh_clicked:
    st.rerun()

_, _, start_at, end_at = _combine_date_bounds(selected_range)
feedback_type = ""
if feedback_type_option == "仅点赞":
    feedback_type = "up"
elif feedback_type_option == "仅点踩":
    feedback_type = "down"

reason_code = ""
for code, label in REASON_LABELS.items():
    if reason_option == label:
        reason_code = code
        break

events_df = _load_feedback_event_df(
    limit=int(event_limit),
    feedback_type=feedback_type,
    user_id=user_filter,
    intent_domain=domain_filter,
    reason_code=reason_code,
    keyword=keyword_filter,
    start_at=start_at,
    end_at=end_at,
)
if only_with_note and not events_df.empty:
    events_df = events_df[events_df["feedback_text"].astype(str).str.strip() != ""].copy()

failure_df = _load_failure_candidates_df(
    limit=100,
    intent_domain=domain_filter,
    reason_code=reason_code,
    keyword=keyword_filter,
    start_at=start_at,
    end_at=end_at,
    min_occurrence=int(min_occurrence),
)
if only_with_note and not failure_df.empty:
    failure_df = failure_df[failure_df["latest_feedback_text"].astype(str).str.strip() != ""].copy()

sample_df = _load_sample_df(
    limit=200,
    sample_status="",
    optimization_type="",
    intent_domain=domain_filter,
    reason_code=reason_code,
    keyword=keyword_filter,
)

if events_df.empty:
    st.info("当前筛选范围内还没有反馈数据。可以先在前台问答里点几次“有帮助 / 没帮助”，再回来查看。")
    st.stop()

total_feedback = int(len(events_df))
up_feedback = int((events_df["feedback_type"] == "up").sum())
down_feedback = int((events_df["feedback_type"] == "down").sum())
distinct_users = int(events_df["user_id"].nunique())
repeat_issue_count = int((failure_df["occurrence_count"] >= 2).sum()) if not failure_df.empty else 0
sample_pool_count = int(len(sample_df)) if not sample_df.empty else 0

recent_end = pd.Timestamp.now()
recent_start = recent_end - pd.Timedelta(days=7)
previous_start = recent_start - pd.Timedelta(days=7)
recent_df = events_df[events_df["created_at"] >= recent_start].copy()
previous_df = events_df[(events_df["created_at"] >= previous_start) & (events_df["created_at"] < recent_start)].copy()
recent_satisfaction = _format_pct(int((recent_df["feedback_type"] == "up").sum()), len(recent_df)) if not recent_df.empty else "0.0%"
previous_satisfaction = _format_pct(int((previous_df["feedback_type"] == "up").sum()), len(previous_df)) if not previous_df.empty else "0.0%"

kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
with kpi1:
    _render_kpi_card("反馈总数", str(total_feedback), "当前筛选范围内")
with kpi2:
    _render_kpi_card("满意率", _format_pct(up_feedback, total_feedback), f"最近 7 天 {recent_satisfaction}")
with kpi3:
    _render_kpi_card("点踩数", str(down_feedback), f"占比 {_format_pct(down_feedback, total_feedback)}")
with kpi4:
    _render_kpi_card("反馈用户数", str(distinct_users), "按 user_id 去重")
with kpi5:
    _render_kpi_card("重复失败主题", str(repeat_issue_count), f"样本池当前 {sample_pool_count} 条")

chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.markdown('<div class="panel-title">差评原因分布</div>', unsafe_allow_html=True)
    st.markdown('<div class="panel-sub">只统计点踩反馈，帮助我们快速判断主要缺陷集中在哪一类。</div>', unsafe_allow_html=True)
    down_df = events_df[events_df["feedback_type"] == "down"].copy()
    if down_df.empty:
        st.info("当前筛选范围内没有点踩反馈。")
    else:
        reason_count = (
            down_df.groupby("reason_label", dropna=False)
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
        )
        fig_reason = px.bar(
            reason_count,
            x="count",
            y="reason_label",
            orientation="h",
            color="count",
            color_continuous_scale="Blues",
            text="count",
        )
        fig_reason.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(15,23,42,0.72)",
            font_color="#e5eefc",
            coloraxis_showscale=False,
            margin=dict(l=10, r=10, t=10, b=10),
            xaxis_title="数量",
            yaxis_title="",
        )
        st.plotly_chart(fig_reason, use_container_width=True, config={"displayModeBar": False})

with chart_col2:
    st.markdown('<div class="panel-title">每日满意率趋势</div>', unsafe_allow_html=True)
    st.markdown('<div class="panel-sub">看最近每天的满意率和反馈量，判断模型体验是否出现波动。</div>', unsafe_allow_html=True)
    trend_df = events_df.copy()
    trend_df["day"] = trend_df["created_at"].dt.date.astype(str)
    trend_group = (
        trend_df.groupby(["day", "feedback_type"]).size().unstack(fill_value=0).reset_index().rename_axis(None, axis=1)
    )
    if "up" not in trend_group.columns:
        trend_group["up"] = 0
    if "down" not in trend_group.columns:
        trend_group["down"] = 0
    trend_group["total"] = trend_group["up"] + trend_group["down"]
    trend_group["satisfaction_rate"] = trend_group.apply(
        lambda row: round((row["up"] / row["total"] * 100) if row["total"] else 0.0, 1), axis=1
    )
    fig_trend = px.line(
        trend_group,
        x="day",
        y="satisfaction_rate",
        markers=True,
    )
    fig_trend.update_traces(line_color="#38bdf8")
    fig_trend.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(15,23,42,0.72)",
        font_color="#e5eefc",
        margin=dict(l=10, r=10, t=10, b=10),
        xaxis_title="日期",
        yaxis_title="满意率 (%)",
        yaxis_range=[0, 100],
    )
    st.plotly_chart(fig_trend, use_container_width=True, config={"displayModeBar": False})

priority_df = _build_priority_queue(failure_df)
domain_health_df = _build_domain_health_df(events_df)

tabs = st.tabs(["优化优先队列", "高频失败问题", "样本池", "原始反馈明细", "按领域健康度"])

with tabs[0]:
    st.markdown('<div class="panel-title">优化优先队列</div>', unsafe_allow_html=True)
    st.markdown('<div class="panel-sub">把高频差评问题转换成可执行动作，方便团队安排 Prompt、个性化或数据侧的修复顺序。</div>', unsafe_allow_html=True)
    if priority_df.empty:
        st.info("当前筛选范围内还没有可生成优先队列的高频失败问题。")
    else:
        display_df = priority_df[
            [
                "priority_score",
                "owner_lane",
                "prompt_text",
                "reason_label",
                "intent_domain",
                "occurrence_count",
                "latest_feedback_at",
                "follow_up_hint",
                "suggested_action",
            ]
        ].copy()
        display_df = display_df.rename(
            columns={
                "priority_score": "优先级分数",
                "owner_lane": "归属方向",
                "prompt_text": "典型问题",
                "reason_label": "主要差评原因",
                "intent_domain": "领域",
                "occurrence_count": "重复次数",
                "latest_feedback_at": "最近反馈时间",
                "follow_up_hint": "排期提示",
                "suggested_action": "建议动作",
            }
        )
        st.dataframe(display_df, use_container_width=True, height=460)

with tabs[1]:
    st.markdown('<div class="panel-title">高频失败问题</div>', unsafe_allow_html=True)
    st.markdown('<div class="panel-sub">适合直接定位哪些问题最常被点踩，以及用户最近是怎么补充说明的。</div>', unsafe_allow_html=True)
    if failure_df.empty:
        st.info("当前没有可展示的高频失败问题。")
    else:
        display_df = failure_df[
            [
                "prompt_text",
                "reason_label",
                "intent_domain",
                "occurrence_count",
                "latest_feedback_at",
                "latest_feedback_text",
                "sample_response_text",
            ]
        ].copy()
        display_df = display_df.rename(
            columns={
                "prompt_text": "问题",
                "reason_label": "差评原因",
                "intent_domain": "领域",
                "occurrence_count": "出现次数",
                "latest_feedback_at": "最近反馈时间",
                "latest_feedback_text": "最近补充说明",
                "sample_response_text": "示例回答",
            }
        )
        st.dataframe(display_df, use_container_width=True, height=440)

        candidate_choices = [
            (
                row["sample_answer_id"],
                f'{str(row["prompt_text"])[:36]} | {row["reason_label"]} | {int(row["occurrence_count"])} 次'
            )
            for _, row in failure_df.iterrows()
        ]
        candidate_label_map = {key: label for key, label in candidate_choices}
        st.markdown('<div class="panel-title">加入样本池</div>', unsafe_allow_html=True)
        st.markdown('<div class="panel-sub">从高频失败问题里挑一条加入样本池，后续就可以持续审核、采纳和标记已修复。</div>', unsafe_allow_html=True)
        with st.form("feedback_sample_create_form"):
            selected_candidate_key = st.selectbox(
                "选择失败问题",
                options=[item[0] for item in candidate_choices],
                format_func=lambda key: candidate_label_map.get(key, key),
            )
            selected_candidate_row = failure_df[failure_df["sample_answer_id"] == selected_candidate_key].iloc[0]
            create_col1, create_col2 = st.columns(2)
            with create_col1:
                sample_status_create = st.selectbox(
                    "入池状态",
                    options=list(SAMPLE_STATUS_LABELS.keys()),
                    index=0,
                    format_func=_sample_status_label,
                )
            with create_col2:
                default_type = default_feedback_sample_optimization_type(str(selected_candidate_row["reason_code"]))
                default_index = list(OPTIMIZATION_TYPE_LABELS.keys()).index(default_type)
                optimization_type_create = st.selectbox(
                    "优化类型",
                    options=list(OPTIMIZATION_TYPE_LABELS.keys()),
                    index=default_index,
                    format_func=_optimization_type_label,
                )
            review_notes_create = st.text_area(
                "入池备注",
                value="",
                placeholder="例如：优先检查个性化提示词，补充仓位比例和触发条件。",
                height=100,
            )
            submit_create = st.form_submit_button("加入样本池", type="primary", use_container_width=True)

        if submit_create:
            result = upsert_chat_feedback_sample(
                de.engine,
                prompt_text=str(selected_candidate_row["prompt_text"] or "").strip(),
                reason_code=str(selected_candidate_row["reason_code"] or "").strip(),
                intent_domain=str(selected_candidate_row["intent_domain"] or "general").strip(),
                occurrence_count=int(selected_candidate_row["occurrence_count"] or 1),
                latest_feedback_at=str(selected_candidate_row["latest_feedback_at"] or ""),
                latest_feedback_text=str(selected_candidate_row["latest_feedback_text"] or "").strip(),
                sample_answer_id=str(selected_candidate_row["sample_answer_id"] or "").strip(),
                sample_trace_id=str(selected_candidate_row["sample_trace_id"] or "").strip(),
                sample_response_text=str(selected_candidate_row["sample_response_text"] or "").strip(),
                created_by=_current_user(),
                sample_status=sample_status_create,
                optimization_type=optimization_type_create,
                review_notes=review_notes_create,
            )
            if result.get("ok"):
                st.success("样本已加入样本池。")
                st.rerun()
            else:
                st.error("加入样本池失败，请稍后重试。")

with tabs[2]:
    st.markdown('<div class="panel-title">样本池</div>', unsafe_allow_html=True)
    st.markdown('<div class="panel-sub">这里是已经入池的候选训练/优化样本。我们可以持续审核、标记采纳状态，并分配到 Prompt、RAG、规则或微调方向。</div>', unsafe_allow_html=True)
    sample_filter_col1, sample_filter_col2 = st.columns([1.2, 1.2])
    with sample_filter_col1:
        sample_status_filter = st.selectbox(
            "样本状态过滤",
            options=[""] + list(SAMPLE_STATUS_LABELS.keys()),
            index=0,
            format_func=lambda value: "全部" if not value else _sample_status_label(value),
            key="sample_status_filter",
        )
    with sample_filter_col2:
        sample_opt_filter = st.selectbox(
            "优化类型过滤",
            options=[""] + list(OPTIMIZATION_TYPE_LABELS.keys()),
            index=0,
            format_func=lambda value: "全部" if not value else _optimization_type_label(value),
            key="sample_opt_filter",
        )
    sample_view_df = _load_sample_df(
        limit=200,
        sample_status=sample_status_filter,
        optimization_type=sample_opt_filter,
        intent_domain=domain_filter,
        reason_code=reason_code,
        keyword=keyword_filter,
    )
    if sample_view_df.empty:
        st.info("样本池里还没有符合当前筛选条件的样本。先在“高频失败问题”里挑几条入池。")
    else:
        sample_display_df = sample_view_df[
            [
                "sample_status_label",
                "optimization_type_label",
                "prompt_text",
                "reason_label",
                "intent_domain",
                "occurrence_count",
                "last_seen_at",
                "review_notes",
            ]
        ].copy()
        sample_display_df = sample_display_df.rename(
            columns={
                "sample_status_label": "样本状态",
                "optimization_type_label": "优化类型",
                "prompt_text": "问题",
                "reason_label": "差评原因",
                "intent_domain": "领域",
                "occurrence_count": "重复次数",
                "last_seen_at": "最近出现时间",
                "review_notes": "审核备注",
            }
        )
        st.dataframe(sample_display_df, use_container_width=True, height=360)

        sample_choices = [
            (
                row["sample_key"],
                f'{_sample_status_label(row["sample_status"])} | {str(row["prompt_text"])[:34]} | {int(row["occurrence_count"])} 次'
            )
            for _, row in sample_view_df.iterrows()
        ]
        sample_label_map = {key: label for key, label in sample_choices}
        with st.form("feedback_sample_update_form"):
            selected_sample_key = st.selectbox(
                "选择样本",
                options=[item[0] for item in sample_choices],
                format_func=lambda key: sample_label_map.get(key, key),
            )
            selected_sample_row = sample_view_df[sample_view_df["sample_key"] == selected_sample_key].iloc[0]
            update_col1, update_col2 = st.columns(2)
            with update_col1:
                current_status = str(selected_sample_row["sample_status"] or "new")
                if current_status not in SAMPLE_STATUS_LABELS:
                    current_status = "new"
                status_index = list(SAMPLE_STATUS_LABELS.keys()).index(current_status)
                sample_status_update = st.selectbox(
                    "更新样本状态",
                    options=list(SAMPLE_STATUS_LABELS.keys()),
                    index=status_index,
                    format_func=_sample_status_label,
                )
            with update_col2:
                current_opt_type = str(selected_sample_row["optimization_type"] or "")
                if current_opt_type not in OPTIMIZATION_TYPE_LABELS:
                    current_opt_type = default_feedback_sample_optimization_type(str(selected_sample_row["reason_code"]))
                opt_index = list(OPTIMIZATION_TYPE_LABELS.keys()).index(current_opt_type)
                optimization_type_update = st.selectbox(
                    "更新优化类型",
                    options=list(OPTIMIZATION_TYPE_LABELS.keys()),
                    index=opt_index,
                    format_func=_optimization_type_label,
                )
            review_notes_update = st.text_area(
                "审核备注",
                value=str(selected_sample_row["review_notes"] or ""),
                height=120,
            )
            submit_update = st.form_submit_button("保存样本状态", type="primary", use_container_width=True)

        if submit_update:
            result = update_chat_feedback_sample(
                de.engine,
                sample_key=selected_sample_key,
                sample_status=sample_status_update,
                optimization_type=optimization_type_update,
                review_notes=review_notes_update,
                reviewed_by=_current_user(),
            )
            if result.get("ok"):
                st.success("样本状态已更新。")
                st.rerun()
            else:
                st.error("样本更新失败，请稍后重试。")

with tabs[3]:
    st.markdown('<div class="panel-title">原始反馈明细</div>', unsafe_allow_html=True)
    st.markdown('<div class="panel-sub">适合回看具体用户、具体问题、具体补充说明，排查真实使用场景。</div>', unsafe_allow_html=True)
    detail_df = events_df[
        [
            "created_at",
            "user_id",
            "feedback_type",
            "reason_label",
            "intent_domain",
            "prompt_text",
            "feedback_text",
            "answer_id",
        ]
    ].copy()
    detail_df = detail_df.rename(
        columns={
            "created_at": "时间",
            "user_id": "用户",
            "feedback_type": "类型",
            "reason_label": "差评原因",
            "intent_domain": "领域",
            "prompt_text": "问题",
            "feedback_text": "补充说明",
            "answer_id": "answer_id",
        }
    )
    st.dataframe(detail_df, use_container_width=True, height=520)

with tabs[4]:
    st.markdown('<div class="panel-title">按领域健康度</div>', unsafe_allow_html=True)
    st.markdown('<div class="panel-sub">看哪些领域的满意率偏低、点踩偏高，帮助我们安排下一轮优化重点。</div>', unsafe_allow_html=True)
    if domain_health_df.empty:
        st.info("当前筛选范围内没有可用的领域数据。")
    else:
        health_chart_df = domain_health_df.copy().head(12)
        fig_domain = px.bar(
            health_chart_df,
            x="intent_domain",
            y="down_rate",
            color="down",
            text="down",
            color_continuous_scale="Oranges",
        )
        fig_domain.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(15,23,42,0.72)",
            font_color="#e5eefc",
            coloraxis_showscale=False,
            margin=dict(l=10, r=10, t=10, b=10),
            xaxis_title="领域",
            yaxis_title="点踩率 (%)",
        )
        st.plotly_chart(fig_domain, use_container_width=True, config={"displayModeBar": False})

        domain_display_df = domain_health_df.rename(
            columns={
                "intent_domain": "领域",
                "up": "点赞数",
                "down": "点踩数",
                "total": "反馈总数",
                "satisfaction_rate": "满意率 (%)",
                "down_rate": "点踩率 (%)",
            }
        )
        st.dataframe(domain_display_df, use_container_width=True, height=360)
        st.markdown(
            '<span class="tag">建议</span> 先看点踩数和点踩率都高的领域，再回到“优化优先队列”确认应该优先修 Prompt、个性化还是数据链路。',
            unsafe_allow_html=True,
        )
