import os
import sys
import time
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st
import extra_streamlit_components as stx

# 添加父目录到路径，便于导入 data_engine
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import data_engine as de
import auth_utils as auth
from task_manager import TaskManager


st.set_page_config(
    page_title="爱波塔-持仓体检",
    page_icon="favicon.ico",
    layout="wide",
    initial_sidebar_state="expanded",
)



# 🔥 添加统一的侧边栏导航
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sidebar_navigation import show_navigation
with st.sidebar:
    show_navigation()

st.markdown(
    """
<style>
    .stApp {
        background-color: #0b1121 !important;
        background-image: radial-gradient(circle at 40% 0%, #1e293b 0%, #0b1121 72%);
        color: #e2e8f0;
    }
    header[data-testid="stHeader"] { background-color: transparent !important; }
    [data-testid="stDecoration"] { display: none; }
    .block-container { padding-top: 1.4rem !important; }
    [data-testid="stSidebar"] {
        background-color: #0f172a !important;
        border-right: 1px solid #1e293b;
    }
    [data-testid="stSidebarNav"] * {
        color: #cbd5e1 !important;
        opacity: 1 !important;
    }
    [data-testid="stSidebarNav"] a:hover {
        color: #ffffff !important;
    }
    .kpi-card {
        background: linear-gradient(135deg, rgba(30,41,59,.92), rgba(15,23,42,.9));
        border: 1px solid #334155;
        border-radius: 12px;
        padding: 14px 16px;
        min-height: 90px;
    }
    .kpi-label { color: #94a3b8; font-size: 13px; }
    .kpi-value { color: #f8fafc; font-size: 24px; font-weight: 700; margin-top: 6px; }
    .tip-box {
        border: 1px solid #3b82f6;
        background: rgba(15, 23, 42, 0.86);
        border-radius: 12px;
        padding: 16px 18px;
        box-shadow: 0 0 0 1px rgba(59,130,246,0.14) inset;
    }
    .processing-box {
        display: flex;
        align-items: center;
        gap: 12px;
        border: 1px solid #1d4ed8;
        border-radius: 12px;
        padding: 12px 14px;
        background: rgba(30, 64, 175, 0.18);
        margin: 8px 0 14px 0;
    }
    .spinner-dot {
        width: 12px;
        height: 12px;
        border-radius: 50%;
        background: #38bdf8;
        box-shadow: 0 0 0 0 rgba(56, 189, 248, 0.8);
        animation: pulse-ring 1.6s infinite;
        flex-shrink: 0;
    }
    .processing-text {
        color: #cbd5e1;
        font-size: 14px;
        line-height: 1.6;
    }
    .empty-state-box {
        border: 1px solid #3b82f6;
        border-radius: 12px;
        background: rgba(30, 64, 175, 0.26);
        padding: 12px 14px;
        color: #f8fafc;
        font-size: 15px;
        font-weight: 600;
        line-height: 1.6;
        box-shadow: 0 0 0 1px rgba(59, 130, 246, 0.2) inset;
    }
    .empty-state-box .hint {
        color: #bfdbfe;
        font-weight: 500;
    }
    .alert-box {
        border-radius: 12px;
        padding: 12px 14px;
        margin: 6px 0 10px 0;
        border: 1px solid transparent;
        font-size: 16px;
        line-height: 1.7;
        font-weight: 600;
    }
    .alert-box.error {
        background: rgba(127, 29, 29, 0.35);
        border-color: rgba(248, 113, 113, 0.75);
        color: #fee2e2;
    }
    .alert-box.info {
        background: rgba(30, 64, 175, 0.32);
        border-color: rgba(96, 165, 250, 0.72);
        color: #dbeafe;
    }
    /* 高对比按钮样式，避免深色背景下文字发灰看不清 */
    div.stButton > button {
        background: #1d4ed8 !important;
        color: #ffffff !important;
        border: 1px solid #93c5fd !important;
        border-radius: 10px !important;
        padding: 0.45rem 1rem !important;
        font-weight: 700 !important;
        font-size: 16px !important;
        line-height: 1.2 !important;
        box-shadow: 0 6px 16px rgba(15, 23, 42, 0.45) !important;
    }
    div.stButton > button:hover {
        background: #2563eb !important;
        color: #ffffff !important;
        border-color: #bfdbfe !important;
    }
    div.stButton > button:disabled {
        background: #334155 !important;
        color: #f1f5f9 !important;
        border-color: #64748b !important;
        opacity: 0.95 !important;
    }
    @keyframes pulse-ring {
        0% { box-shadow: 0 0 0 0 rgba(56, 189, 248, 0.75); }
        70% { box-shadow: 0 0 0 10px rgba(56, 189, 248, 0); }
        100% { box-shadow: 0 0 0 0 rgba(56, 189, 248, 0); }
    }
</style>
""",
    unsafe_allow_html=True,
)

if "portfolio_cookie_retry_once" not in st.session_state:
    st.session_state.portfolio_cookie_retry_once = False


def _restore_login_with_cookie_state(cookies: dict):
    cookies = cookies or {}
    try:
        restored = auth.restore_login_from_cookies(cookies)
    except Exception:
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


def _auto_restore_login_if_needed():
    should_auto_login = (
        not st.session_state.get("is_logged_in", False)
        and not st.session_state.get("just_logged_out", False)
    )
    if not should_auto_login:
        return

    cm = stx.CookieManager(key="portfolio_page_cookie_manager")
    cookies = cm.get_all() or {}
    restored, state = _restore_login_with_cookie_state(cookies)
    if restored:
        st.session_state.portfolio_cookie_retry_once = False
        st.rerun()

    # 首次加载 cookie 组件偶发拿不到值，允许重试一次。
    if state in {"empty", "partial", "error"} and not st.session_state.get("portfolio_cookie_retry_once", False):
        st.session_state.portfolio_cookie_retry_once = True
        time.sleep(0.15)
        st.rerun()


_auto_restore_login_if_needed()

if not st.session_state.get("is_logged_in", False):
    st.warning("🔒 请先登录后查看持仓体检。")
    st.stop()

user_id = st.session_state.get("user_id", "")
task_manager = TaskManager()
pending_meta = task_manager.get_user_pending_portfolio_task(user_id)
pending_status = None
if pending_meta:
    start_time = float(pending_meta.get("start_time") or 0)
    if start_time and (time.time() - start_time > 1800):
        task_manager.clear_user_pending_portfolio_task(user_id)
        pending_meta = None
        st.warning("上次持仓体检任务已超时，请返回首页重新上传截图。")

if pending_meta and pending_meta.get("task_id"):
    try:
        pending_status = task_manager.get_task_status(pending_meta["task_id"])
        if pending_status.get("status") in {"success", "error"}:
            task_manager.clear_user_pending_portfolio_task(user_id)
            pending_meta = None
            pending_status = None
    except Exception:
        pending_status = {"status": "pending", "progress": "正在处理..."}

snapshot = de.get_user_portfolio_snapshot(user_id)
positions_df = de.get_user_portfolio_positions(user_id)


def _to_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


snapshot_recognized_count = _to_int((snapshot or {}).get("recognized_count"), 0)
snapshot_positions_mismatch = bool(snapshot) and snapshot_recognized_count > 0 and positions_df.empty
if snapshot_positions_mismatch:
    # 二次兜底：直接走服务层读取，避免页面层偶发查询异常造成“有图无表”。
    try:
        from portfolio_analysis_service import get_user_portfolio_positions_df as _svc_get_positions_df

        retry_df = _svc_get_positions_df(user_id)
        if retry_df is not None and not retry_df.empty:
            positions_df = retry_df
            snapshot_positions_mismatch = False
    except Exception as e:
        print(f"持仓体检二次拉取明细失败: {e}")

if snapshot_positions_mismatch:
    st.markdown(
        """
<div class="alert-box error">
  检测到持仓快照与明细不一致（总览有数据，但明细为空）。为避免误导，已暂停渲染旧图表。
</div>
""",
        unsafe_allow_html=True,
    )
    st.markdown(
        """
<div class="alert-box info">
  请返回首页重新上传持仓截图。若该问题持续，可先点击下方按钮清理旧快照。
</div>
""",
        unsafe_allow_html=True,
    )
    if st.button("清理当前旧快照"):
        if de.clear_user_portfolio_snapshot(user_id):
            st.success("已清理旧快照，请重新上传持仓截图。")
            time.sleep(0.5)
            st.rerun()
        else:
            st.warning("快照清理失败，请稍后重试。")
    st.stop()

if not positions_df.empty and "symbol" in positions_df.columns:
    momentum_map = de.get_portfolio_momentum_scores(positions_df["symbol"].tolist(), window_days=10)
    positions_df["momentum_score_10d"] = positions_df["symbol"].map(momentum_map)

    def _grade_from_momentum(score):
        if pd.isna(score):
            return None
        try:
            v = float(score)
        except (TypeError, ValueError):
            return None
        if v >= 90:
            return "增持"
        if v >= 50:
            return "持有"
        return "减仓"

    if "technical_grade" not in positions_df.columns:
        positions_df["technical_grade"] = "持有"
    derived_grade = positions_df["momentum_score_10d"].apply(_grade_from_momentum)
    positions_df["technical_grade"] = derived_grade.combine_first(positions_df["technical_grade"])

st.markdown("## 持仓体检")
st.caption("自动识别持仓截图后生成：行业占比、技术分级、组合指数相关度。")

if pending_meta and (pending_status or {}).get("status") in {"pending", "processing"}:
    progress_text = (pending_status or {}).get("progress") or "正在计算行业占比与相关度..."
    count_text = int(pending_meta.get("positions_count") or 0)
    st.markdown(
        f"""
<div class="processing-box">
  <div class="spinner-dot"></div>
  <div class="processing-text">
    <div>持仓体检进行中：{progress_text}</div>
    <div>已识别 {count_text} 只股票，结果生成后将自动刷新。</div>
  </div>
</div>
""",
        unsafe_allow_html=True,
    )

if (not snapshot) and positions_df.empty:
    if pending_meta and (pending_status or {}).get("status") in {"pending", "processing"}:
        st.info("正在计算持仓数据，请稍候...")
        time.sleep(1.2)
        st.rerun()
    st.markdown(
        """
<div class="empty-state-box">
  暂无持仓体检数据。
  <span class="hint">去首页上传持仓截图后会自动生成。</span>
</div>
""",
        unsafe_allow_html=True,
    )
    st.stop()

summary_text = str(snapshot.get("summary_text") or "暂无总结")


def _format_market_value(value: float) -> str:
    try:
        v = float(value or 0.0)
    except (TypeError, ValueError):
        return "0"
    if abs(v) >= 1e8:
        return f"{v / 1e8:.2f} 亿"
    if abs(v) >= 1e4:
        return f"{v / 1e4:.2f} 万"
    return f"{v:.2f}"


def _to_beijing_time_str(raw_value) -> str:
    if raw_value is None:
        return "暂无"
    text_value = str(raw_value).strip()
    if not text_value:
        return "暂无"

    fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    ]
    dt = None
    for fmt in fmts:
        try:
            dt = datetime.strptime(text_value, fmt)
            break
        except ValueError:
            continue
    if dt is None:
        return text_value

    # 持仓快照入库时间当前按 UTC 写入，这里统一转换为北京时间显示。
    dt_bj = dt + timedelta(hours=8)
    return dt_bj.strftime("%Y-%m-%d %H:%M")


hkd_cny_rate = de.get_latest_hkd_cny_rate(default_rate=0.92)
mv_series = pd.Series(dtype=float)
if not positions_df.empty and "market_value" in positions_df.columns:
    mv_df = positions_df.copy()
    mv_df["market_value"] = pd.to_numeric(mv_df["market_value"], errors="coerce").fillna(0.0)
    market_series = (
        mv_df["market"].astype(str).str.upper()
        if "market" in mv_df.columns
        else pd.Series([""] * len(mv_df), index=mv_df.index)
    )
    symbol_series = (
        mv_df["symbol"].astype(str).str.upper()
        if "symbol" in mv_df.columns
        else pd.Series([""] * len(mv_df), index=mv_df.index)
    )
    is_hk = (market_series == "HK") | symbol_series.str.endswith(".HK")
    mv_df["market_value_cny"] = mv_df["market_value"]
    mv_df.loc[is_hk, "market_value_cny"] = mv_df.loc[is_hk, "market_value"] * float(hkd_cny_rate)
    mv_series = mv_df["market_value_cny"]
total_market_value = float(mv_series.sum()) if not mv_series.empty else 0.0
total_market_value_text = _format_market_value(total_market_value)

max_profit_text = "暂无（缺成本价）"
if not positions_df.empty:
    calc_df = positions_df.copy()
    for col in ("quantity", "price", "cost_price", "market_value"):
        if col in calc_df.columns:
            calc_df[col] = pd.to_numeric(calc_df[col], errors="coerce")
    if "quantity" in calc_df.columns and "cost_price" in calc_df.columns:
        current_value = calc_df["market_value"] if "market_value" in calc_df.columns else pd.Series(dtype=float)
        if current_value.empty:
            current_value = calc_df["quantity"] * calc_df.get("price", 0)
        else:
            fallback_value = calc_df["quantity"] * calc_df.get("price", 0)
            current_value = current_value.where(current_value.notna(), fallback_value)

        cost_value = calc_df["quantity"] * calc_df["cost_price"]
        pnl = current_value - cost_value
        valid_mask = cost_value.notna() & current_value.notna()
        pnl = pnl.where(valid_mask)
        if pnl.notna().any():
            best_idx = pnl.idxmax()
            best_pnl = float(pnl.loc[best_idx])
            if best_pnl > 0:
                best_name = str(calc_df.loc[best_idx].get("name") or calc_df.loc[best_idx].get("symbol") or "")
                best_symbol = str(calc_df.loc[best_idx].get("symbol") or "")
                max_profit_text = f"{best_name}({best_symbol}) +{best_pnl:.2f}"
            else:
                max_profit_text = "暂无盈利持仓"

updated_at = _to_beijing_time_str(snapshot.get("updated_at"))

col1, col2, col3 = st.columns(3)
with col1:
    st.markdown(
        f"""
<div class="kpi-card">
  <div class="kpi-label">总市值</div>
  <div class="kpi-value">{total_market_value_text}</div>
</div>
""",
        unsafe_allow_html=True,
    )
with col2:
    st.markdown(
        f"""
<div class="kpi-card">
  <div class="kpi-label">最大获利股</div>
  <div class="kpi-value" style="font-size:18px;">{max_profit_text}</div>
</div>
""",
        unsafe_allow_html=True,
    )
with col3:
    st.markdown(
        f"""
<div class="kpi-card">
  <div class="kpi-label">最近更新时间</div>
  <div class="kpi-value" style="font-size:18px;">{updated_at}</div>
</div>
""",
        unsafe_allow_html=True,
    )

st.markdown("<br>", unsafe_allow_html=True)

col_a, col_b, col_c = st.columns([1.1, 1.0, 1.0])

with col_a:
    st.markdown("### 行业占比")
    industry_alloc = snapshot.get("industry_allocation") or []
    industry_df = pd.DataFrame(industry_alloc)
    if not industry_df.empty and "weight_pct" in industry_df.columns:
        fig_pie = px.pie(
            industry_df,
            names="industry",
            values="weight_pct",
            hole=0.45,
            color_discrete_sequence=px.colors.sequential.Blues_r,
        )
        fig_pie.update_traces(
            textposition="inside",
            textinfo="percent",
            insidetextfont=dict(color="#f8fafc", size=14),
            hovertemplate="%{label}: %{value:.2f}%<extra></extra>",
        )
        fig_pie.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#cbd5e1"),
            margin=dict(l=20, r=20, t=20, b=20),
            height=320,
            uniformtext_minsize=12,
            uniformtext_mode="hide",
            legend=dict(
                font=dict(color="#e2e8f0", size=15),
                bgcolor="rgba(15,23,42,0.35)",
            ),
        )
        st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.info("暂无行业占比数据")

with col_b:
    st.markdown("### 组合指数相关度")
    corr_map = snapshot.get("portfolio_corr") or {}
    corr_rows = []
    for k, v in corr_map.items():
        try:
            corr_rows.append({"index_name": str(k), "corr": float(v)})
        except (TypeError, ValueError):
            continue
    corr_df = pd.DataFrame(corr_rows)
    if not corr_df.empty and "corr" in corr_df.columns:
        corr_df = corr_df.sort_values("corr", ascending=False)
        fig_corr = px.bar(
            corr_df.head(8),
            x="corr",
            y="index_name",
            orientation="h",
            color="corr",
            color_continuous_scale=["#1e3a8a", "#334155", "#dc2626"],
            range_color=[-1, 1],
            text="corr",
        )
        fig_corr.update_traces(texttemplate="%{text:.2f}", textposition="outside")
        fig_corr.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="#0f172a",
            font=dict(color="#e2e8f0"),
            margin=dict(l=20, r=20, t=20, b=20),
            coloraxis_showscale=False,
            height=320,
            yaxis_title="",
            xaxis=dict(
                title=dict(text="相关系数", font=dict(color="#e2e8f0")),
                tickfont=dict(color="#e2e8f0"),
            ),
            yaxis=dict(tickfont=dict(color="#e2e8f0")),
        )
        fig_corr.update_traces(textfont=dict(color="#f8fafc"))
        st.plotly_chart(fig_corr, use_container_width=True)
    else:
        st.info("暂无组合相关度数据")

with col_c:
    st.markdown("### 技术分级分布")
    if not positions_df.empty and "technical_grade" in positions_df.columns:
        grade_df = (
            positions_df["technical_grade"]
            .fillna("持有")
            .value_counts()
            .rename_axis("grade")
            .reset_index(name="count")
        )
        fig_grade = px.bar(
            grade_df,
            x="grade",
            y="count",
            color="grade",
            color_discrete_map={"增持": "#16a34a", "持有": "#3b82f6", "减仓": "#ef4444"},
            text="count",
        )
        fig_grade.update_traces(textposition="outside")
        fig_grade.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="#0f172a",
            font=dict(color="#cbd5e1"),
            margin=dict(l=20, r=20, t=20, b=20),
            height=320,
            xaxis_title="",
            yaxis_title="数量",
            showlegend=False,
        )
        st.plotly_chart(fig_grade, use_container_width=True)
    else:
        st.info("暂无技术分级数据")

st.markdown("<br>", unsafe_allow_html=True)
st.markdown(
    f"""
<div class="tip-box">
  <div style="font-size:22px; color:#f8fafc; margin-bottom:8px; font-weight:700;">结果状态</div>
  <div style="font-size:18px; color:#dbeafe; line-height:1.9;">
    {summary_text}
  </div>
</div>
""",
    unsafe_allow_html=True,
)

st.markdown("---")
st.markdown("### 持仓明细")

if positions_df.empty:
    st.info("暂无持仓明细")
else:
    if "index_corr" not in positions_df.columns and "index_corr_json" in positions_df.columns:
        def _parse_corr(raw):
            if not raw:
                return {}
            try:
                import json
                return json.loads(raw)
            except Exception:
                return {}
        positions_df["index_corr"] = positions_df["index_corr_json"].apply(_parse_corr)

    def _top_corr_name(v):
        if not isinstance(v, dict) or not v:
            return "无"
        pairs = []
        for k, item in v.items():
            try:
                pairs.append((str(k), float(item)))
            except (TypeError, ValueError):
                continue
        if not pairs:
            return "无"
        return max(pairs, key=lambda kv: kv[1])[0]

    def _top_corr_value(v):
        if not isinstance(v, dict) or not v:
            return None
        vals = []
        for item in v.values():
            try:
                vals.append(float(item))
            except (TypeError, ValueError):
                continue
        if not vals:
            return None
        return round(max(vals), 4)

    show_df = positions_df.copy()
    if "momentum_score_10d" not in show_df.columns:
        show_df["momentum_score_10d"] = None
    if "updated_at" in show_df.columns:
        show_df["updated_at"] = show_df["updated_at"].apply(_to_beijing_time_str)
    show_df["top_index"] = show_df["index_corr"].apply(_top_corr_name) if "index_corr" in show_df.columns else "无"
    show_df["top_corr"] = show_df["index_corr"].apply(_top_corr_value) if "index_corr" in show_df.columns else None
    show_df.rename(
        columns={
            "symbol": "代码",
            "name": "名称",
            "market": "市场",
            "quantity": "数量",
            "market_value": "市值",
            "industry": "行业",
            "technical_grade": "技术评级",
            "momentum_score_10d": "动能分数(近10日均分)",
            "top_index": "最强相关指数",
            "top_corr": "相关系数",
            "updated_at": "更新时间",
        },
        inplace=True,
    )
    keep_cols = [
        "代码",
        "名称",
        "市场",
        "数量",
        "市值",
        "行业",
        "技术评级",
        "最强相关指数",
        "相关系数",
        "动能分数(近10日均分)",
        "更新时间",
    ]
    keep_cols = [c for c in keep_cols if c in show_df.columns]
    view_df = show_df[keep_cols].copy()
    if "动能分数(近10日均分)" in view_df.columns:
        view_df["动能分数(近10日均分)"] = pd.to_numeric(
            view_df["动能分数(近10日均分)"], errors="coerce"
        )
    st.dataframe(
        view_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "动能分数(近10日均分)": st.column_config.NumberColumn(
                "动能分数(近10日均分)",
                format="%.1f",
            )
        },
    )

if pending_meta and (pending_status or {}).get("status") in {"pending", "processing"}:
    time.sleep(1.2)
    st.rerun()
