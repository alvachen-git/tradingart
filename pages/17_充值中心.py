from datetime import datetime

import streamlit as st

import payment_service as pay_svc
import subscription_service as sub_svc
from sidebar_navigation import show_navigation
from ui_components import inject_sidebar_toggle_style


st.set_page_config(page_title="爱波塔-充值中心", page_icon="💳", layout="wide")


if not st.session_state.get("is_logged_in", False):
    st.warning("🔒 请先登录后使用充值中心")
    st.page_link("Home.py", label="返回首页登录")
    st.stop()

user_id = st.session_state.get("user_id")
if not user_id:
    st.warning("🔒 登录信息已失效，请重新登录")
    st.page_link("Home.py", label="返回首页登录")
    st.stop()

with st.sidebar:
    show_navigation()
inject_sidebar_toggle_style(mode="high_contrast")

st.markdown(
    """
    <style>
    :root {
        --bg-0: #060d1f;
        --bg-1: #0b1730;
        --card: rgba(13, 28, 58, 0.86);
        --card-soft: rgba(12, 24, 49, 0.75);
        --line: rgba(120, 149, 204, 0.32);
        --text: #ecf3ff;
        --muted: #9fb0cd;
        --cyan: #44d6ff;
        --blue: #2d7fff;
        --blue-2: #1b5dff;
        --green: #2ecb88;
        --amber: #f3b34a;
        --danger: #ff6b7a;
    }

    .stApp {
        background:
            radial-gradient(1200px 620px at 80% -15%, rgba(59, 130, 246, 0.24), transparent 62%),
            radial-gradient(900px 520px at 8% -4%, rgba(14, 165, 233, 0.14), transparent 56%),
            linear-gradient(150deg, var(--bg-0), var(--bg-1));
        color: var(--text);
    }

    [data-testid="stHeader"] {
        background: transparent !important;
    }

    [data-testid="stDecoration"] {
        display: none;
    }

    [data-testid="stMainBlockContainer"] {
        max-width: 78rem !important;
        padding-top: 0.9rem;
        padding-bottom: 2rem;
    }

    h1, h2, h3, h4, h5, h6, p, label {
        color: var(--text) !important;
    }

    .mall-title {
        font-size: clamp(30px, 4vw, 44px);
        line-height: 1.05;
        font-weight: 800;
        margin: 4px 0 6px;
        letter-spacing: 0.02em;
    }

    .mall-subtitle {
        color: var(--muted);
        font-size: 15px;
        margin-bottom: 10px;
    }

    .wallet-shell {
        border: 1px solid var(--line);
        border-radius: 16px;
        padding: 16px;
        background: linear-gradient(130deg, rgba(11, 26, 53, 0.95), rgba(10, 22, 44, 0.85));
        box-shadow: 0 16px 40px rgba(0, 0, 0, 0.28), inset 0 1px 0 rgba(255, 255, 255, 0.04);
        margin-bottom: 14px;
        position: relative;
        overflow: hidden;
    }

    .wallet-shell::after {
        content: "";
        position: absolute;
        top: 0;
        left: -32%;
        width: 25%;
        height: 100%;
        background: linear-gradient(90deg, transparent, rgba(108, 171, 255, 0.14), transparent);
        transform: skewX(-18deg);
        animation: walletSweep 6s ease-in-out infinite;
        pointer-events: none;
    }
    .wallet-panel-wrap {
        position: relative;
        margin-bottom: 14px;
    }
    .st-key-wallet_panel {
        border: 1px solid var(--line);
        border-radius: 16px;
        padding: 16px;
        background: linear-gradient(130deg, rgba(11, 26, 53, 0.95), rgba(10, 22, 44, 0.85));
        box-shadow: 0 16px 40px rgba(0, 0, 0, 0.28), inset 0 1px 0 rgba(255, 255, 255, 0.04);
        margin-bottom: 14px;
        position: relative;
        overflow: hidden;
    }
    .st-key-wallet_panel::after {
        content: "";
        position: absolute;
        top: 0;
        left: -32%;
        width: 25%;
        height: 100%;
        background: linear-gradient(90deg, transparent, rgba(108, 171, 255, 0.14), transparent);
        transform: skewX(-18deg);
        animation: walletSweep 6s ease-in-out infinite;
        pointer-events: none;
    }

    @keyframes walletSweep {
        0%, 100% { transform: translateX(0) skewX(-18deg); }
        50% { transform: translateX(520%) skewX(-18deg); }
    }

    .wallet-head {
        display: flex;
        align-items: center;
        justify-content: flex-start;
        gap: 10px;
        margin-bottom: 10px;
        flex-wrap: wrap;
    }

    .wallet-head .label {
        color: #9ab6dd;
        font-size: 13px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }

    .wallet-balance {
        font-size: clamp(34px, 5vw, 50px);
        font-weight: 900;
        line-height: 1;
        color: #f5f8ff;
        text-shadow: 0 4px 20px rgba(45, 127, 255, 0.24);
    }
    .wallet-balance.refresh-pop {
        animation: balancePulse 680ms cubic-bezier(.22,.9,.25,1);
    }
    .wallet-balance-row {
        display: flex;
        align-items: center;
        gap: 10px;
    }
    .wallet-refresh-wrap {
        display: flex;
        align-items: center;
    }
    .wallet-refresh-note {
        color: #8fe2ff;
        font-size: 12px;
        margin-top: 4px;
        min-height: 18px;
    }

    .wallet-cny {
        color: #86c8ff;
        font-size: 15px;
        margin-top: 4px;
    }
    .wallet-cny.refresh-pop {
        animation: cnyFlash 620ms ease;
    }

    .wallet-metrics {
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 10px;
        margin-top: 10px;
    }
    .wallet-metrics.refresh-pop .metric-item {
        animation: metricPulse 700ms ease;
    }

    .metric-item {
        border: 1px solid rgba(120, 149, 204, 0.22);
        border-radius: 12px;
        background: var(--card-soft);
        padding: 10px 12px;
    }

    .metric-k {
        color: var(--muted);
        font-size: 12px;
        letter-spacing: 0.05em;
        text-transform: uppercase;
    }

    .metric-v {
        color: #f4f8ff;
        font-size: 22px;
        font-weight: 800;
        margin-top: 4px;
    }
    @keyframes balancePulse {
        0% { transform: scale(1); filter: drop-shadow(0 0 0 rgba(68, 214, 255, 0)); }
        40% { transform: scale(1.06); filter: drop-shadow(0 0 10px rgba(68, 214, 255, 0.45)); }
        100% { transform: scale(1); filter: drop-shadow(0 0 0 rgba(68, 214, 255, 0)); }
    }
    @keyframes cnyFlash {
        0% { opacity: .72; }
        50% { opacity: 1; color: #b9e9ff; }
        100% { opacity: 1; }
    }
    @keyframes metricPulse {
        0% { box-shadow: 0 0 0 rgba(68, 214, 255, 0); border-color: rgba(120,149,204,0.22); }
        45% { box-shadow: 0 0 18px rgba(68, 214, 255, 0.16); border-color: rgba(115, 201, 255, 0.52); }
        100% { box-shadow: 0 0 0 rgba(68, 214, 255, 0); border-color: rgba(120,149,204,0.22); }
    }

    .section-title {
        margin: 8px 0 12px;
        font-size: 22px;
        font-weight: 800;
        letter-spacing: 0.02em;
    }

    .section-sub {
        color: var(--muted);
        margin-top: -6px;
        margin-bottom: 12px;
        font-size: 14px;
    }

    .shop-card {
        border: 1px solid var(--line);
        border-radius: 14px;
        background: linear-gradient(145deg, rgba(14, 29, 61, 0.92), rgba(11, 22, 45, 0.84));
        padding: 14px;
        min-height: 168px;
        position: relative;
        overflow: hidden;
    }

    .shop-card::before {
        content: "";
        position: absolute;
        inset: 0;
        background: radial-gradient(120% 80% at 100% 0%, rgba(68, 214, 255, 0.12), transparent 46%);
        pointer-events: none;
    }

    .shop-badge {
        position: absolute;
        top: 10px;
        right: 10px;
        font-size: 11px;
        line-height: 1;
        padding: 6px 8px;
        border-radius: 999px;
        border: 1px solid rgba(255, 255, 255, 0.14);
        color: #f9fcff;
        background: rgba(59, 130, 246, 0.2);
        font-weight: 700;
        letter-spacing: 0.04em;
    }

    .shop-badge.reco-hot {
        background: linear-gradient(145deg, rgba(255, 80, 100, 0.92), rgba(182, 30, 45, 0.92));
        border-color: rgba(255, 154, 166, 0.9);
        color: #fff5f7;
        box-shadow: 0 8px 16px rgba(180, 25, 48, 0.28);
    }

    .shop-name {
        color: #eaf1ff;
        font-size: 21px;
        font-weight: 800;
        margin-bottom: 10px;
    }

    .shop-price {
        color: #f7b64e;
        font-size: 30px;
        font-weight: 900;
        line-height: 1.05;
        font-family: "IBM Plex Mono", "Consolas", monospace;
    }

    .shop-points {
        color: #9ec6ff;
        font-size: 16px;
        margin-top: 2px;
    }

    .shop-desc {
        color: #9eb1cf;
        font-size: 13px;
        margin-top: 10px;
    }

    .order-box {
        border: 1px solid rgba(129, 165, 224, 0.3);
        border-radius: 14px;
        background: linear-gradient(130deg, rgba(15, 33, 66, 0.86), rgba(10, 22, 44, 0.74));
        padding: 14px;
        margin-top: 10px;
    }

    .order-title {
        color: #dbe9ff;
        font-size: 14px;
        margin-bottom: 6px;
    }

    .order-main {
        color: #f1f6ff;
        font-size: 21px;
        font-weight: 800;
        line-height: 1.2;
    }

    .order-sub {
        color: #9fb6d9;
        font-size: 13px;
        margin-top: 4px;
    }

    .pay-method-title {
        font-size: 14px;
        font-weight: 700;
        color: #b7cbec;
        margin: 12px 0 10px;
    }

    .pay-method-card {
        display: flex;
        align-items: center;
        gap: 12px;
        border: 1px solid rgba(98, 146, 227, 0.34);
        border-radius: 12px;
        background: rgba(14, 30, 63, 0.78);
        padding: 12px 14px;
        margin-bottom: 10px;
    }

    .pay-logo {
        width: 34px;
        height: 34px;
        border-radius: 9px;
        background: #1677ff;
        color: #fff;
        font-size: 20px;
        font-weight: 900;
        display: flex;
        align-items: center;
        justify-content: center;
        line-height: 1;
    }

    .pay-meta {
        flex: 1;
        min-width: 0;
    }

    .pay-name {
        color: #edf4ff;
        font-size: 15px;
        font-weight: 700;
    }

    .pay-desc {
        color: #8ea9d1;
        font-size: 12px;
        margin-top: 2px;
    }

    .pay-check {
        color: #3ec6ff;
        font-size: 18px;
        font-weight: 800;
    }

    .alipay-pay-btn {
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 8px;
        width: 100%;
        border-radius: 12px;
        border: 1px solid rgba(67, 168, 255, 0.8);
        background: linear-gradient(180deg, #2791ff 0%, #1b5dff 100%);
        color: #ffffff !important;
        text-decoration: none !important;
        font-size: 16px;
        font-weight: 800;
        padding: 11px 14px;
        box-shadow: 0 12px 22px rgba(27, 93, 255, 0.3);
        transition: all .16s ease;
    }

    .alipay-pay-btn:hover {
        filter: brightness(1.06);
        transform: translateY(-1px);
    }

    .alipay-pay-btn:active {
        transform: translateY(0);
    }

    .alipay-pay-btn .btn-icon {
        width: 24px;
        height: 24px;
        border-radius: 7px;
        background: rgba(255,255,255,.18);
        display: inline-flex;
        align-items: center;
        justify-content: center;
        font-size: 15px;
        font-weight: 800;
        line-height: 1;
    }

    .product-head {
        border: 1px solid rgba(116, 154, 220, 0.28);
        border-radius: 12px;
        background: rgba(12, 24, 50, 0.72);
        padding: 12px;
        display: flex;
        align-items: flex-start;
        justify-content: space-between;
        gap: 12px;
    }
    .product-main {
        flex: 1;
        min-width: 0;
    }
    .product-side {
        min-width: 160px;
        display: flex;
        flex-direction: column;
        align-items: flex-end;
        gap: 6px;
    }

    .product-name {
        font-size: 17px;
        color: #e6f0ff;
        font-weight: 800;
        margin-bottom: 4px;
        display: flex;
        align-items: center;
        gap: 8px;
    }

    .product-tech-icon {
        width: 22px;
        height: 22px;
        border-radius: 7px;
        border: 1px solid rgba(245, 197, 102, 0.45);
        background: linear-gradient(145deg, rgba(35, 29, 18, 0.95), rgba(18, 16, 12, 0.92));
        color: #f6c973;
        font-size: 12px;
        font-weight: 900;
        line-height: 1;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        box-shadow: 0 0 0 1px rgba(255, 207, 115, 0.08) inset, 0 6px 14px rgba(0, 0, 0, 0.25);
    }

    .product-meta {
        color: #9eb4d8;
        font-size: 13px;
        line-height: 1.35;
    }
    .product-desc {
        color: #bfd0ea;
        font-size: 13px;
        line-height: 1.5;
        margin-top: 6px;
    }
    .sub-chip {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        padding: 4px 10px;
        border-radius: 999px;
        border: 1px solid rgba(113, 166, 245, 0.45);
        background: rgba(31, 65, 122, 0.55);
        color: #ddecff;
        font-size: 12px;
        font-weight: 700;
        letter-spacing: 0.03em;
        white-space: nowrap;
    }
    .sub-chip.active {
        border-color: rgba(77, 207, 176, 0.52);
        background: rgba(19, 95, 79, 0.42);
        color: #a9f0dc;
    }
    .sub-chip.expired {
        border-color: rgba(243, 179, 74, 0.52);
        background: rgba(111, 70, 16, 0.34);
        color: #ffd998;
    }
    .sub-chip.none {
        border-color: rgba(129, 152, 192, 0.38);
        background: rgba(57, 70, 95, 0.28);
        color: #c2d0e8;
    }
    .sub-exp {
        color: #9eb4d8;
        font-size: 12px;
        line-height: 1.35;
        text-align: right;
    }

    .section-divider {
        margin: 16px 0 14px;
        border: 0;
        height: 1px;
        background: linear-gradient(90deg, transparent, rgba(131, 164, 219, 0.6), transparent);
    }

    div.stButton > button {
        border: 1px solid rgba(99, 157, 239, 0.44) !important;
        border-radius: 12px !important;
        background: linear-gradient(145deg, rgba(30, 63, 124, 0.95), rgba(18, 38, 75, 0.92)) !important;
        color: #ecf3ff !important;
        font-weight: 700 !important;
        letter-spacing: 0.02em;
        min-height: 46px;
        box-shadow: 0 8px 20px rgba(4, 10, 20, 0.28) !important;
    }

    div.stButton > button:hover {
        border-color: rgba(95, 183, 255, 0.75) !important;
        background: linear-gradient(145deg, rgba(44, 91, 181, 0.95), rgba(24, 53, 106, 0.92)) !important;
        transform: translateY(-1px);
    }

    div.stButton > button[kind="primary"] {
        border-color: rgba(67, 168, 255, 0.9) !important;
        background: linear-gradient(180deg, #2690ff 0%, #1c5eff 100%) !important;
        box-shadow: 0 12px 22px rgba(28, 94, 255, 0.26) !important;
    }
    .st-key-wallet_panel .st-key-wallet_refresh_icon button {
        width: 36px !important;
        min-width: 36px !important;
        max-width: 36px !important;
        height: 36px !important;
        min-height: 36px !important;
        padding: 0 !important;
        border-radius: 10px !important;
        border: 1px solid rgba(95, 183, 255, 0.75) !important;
        background: linear-gradient(180deg, rgba(38, 144, 255, 0.95), rgba(23, 87, 184, 0.95)) !important;
        box-shadow: 0 8px 16px rgba(13, 38, 80, 0.35) !important;
        font-size: 16px !important;
        line-height: 1 !important;
        margin: 0 !important;
    }
    .st-key-wallet_panel .st-key-wallet_refresh_icon button:hover {
        filter: brightness(1.08);
        transform: rotate(25deg);
    }

    [data-baseweb="tab-list"] {
        gap: 6px;
        margin-top: 2px;
        margin-bottom: 12px;
    }

    [data-baseweb="tab"] {
        border-radius: 10px !important;
        padding: 8px 14px !important;
        border: 1px solid rgba(120,149,204,0.26) !important;
        background: rgba(12, 24, 50, 0.54) !important;
        color: #bfd3f4 !important;
        font-weight: 700;
    }

    [aria-selected="true"][data-baseweb="tab"] {
        color: #f5f9ff !important;
        border-color: rgba(73, 169, 255, 0.76) !important;
        background: linear-gradient(145deg, rgba(40, 83, 165, 0.62), rgba(22, 49, 95, 0.72)) !important;
    }

    @media (max-width: 768px) {
        [data-testid="stMainBlockContainer"] {
            padding-left: 0.7rem;
            padding-right: 0.7rem;
        }

        .wallet-metrics {
            grid-template-columns: 1fr;
        }
        .product-head {
            flex-direction: column;
        }
        .product-side {
            min-width: 0;
            align-items: flex-start;
        }
        .sub-exp {
            text-align: left;
        }

        .shop-price {
            font-size: 28px;
        }
    }
    </style>
    """,
    unsafe_allow_html=True,
)

if "points_last_order" not in st.session_state:
    st.session_state.points_last_order = None
if "points_pending_purchase" not in st.session_state:
    st.session_state.points_pending_purchase = None

points_info = pay_svc.get_user_points(user_id)
balance = int(points_info.get("balance") or 0)
total_earned = int(points_info.get("total_earned") or 0)
total_spent = int(points_info.get("total_spent") or 0)
updated_at = points_info.get("updated_at")

updated_text = (
    updated_at.strftime("%Y-%m-%d %H:%M:%S")
    if hasattr(updated_at, "strftime")
    else str(updated_at or "-")
)

st.markdown('<div class="mall-title">充值中心</div>', unsafe_allow_html=True)
st.markdown('<div class="mall-subtitle">点数可用于购买晚报与情报套餐，支付成功后权限自动生效。</div>', unsafe_allow_html=True)

with st.container(key="wallet_panel"):
    wallet_info_col, wallet_spacer_col = st.columns([5.8, 6.2], vertical_alignment="top")
    with wallet_info_col:
        bal_col, refresh_col = st.columns([5.3, 0.7], vertical_alignment="center")
        with refresh_col:
            st.markdown('<div class="wallet-refresh-wrap">', unsafe_allow_html=True)
            refreshed = st.button("↻", key="wallet_refresh_icon", help="刷新余额")
            st.markdown("</div>", unsafe_allow_html=True)
        anim_cls = " refresh-pop" if refreshed else ""
        refresh_text = "余额已刷新" if refreshed else "&nbsp;"
        with bal_col:
            st.markdown(
                f"""
                <div class="wallet-head">
                    <div>
                        <div class="label">POINTS WALLET</div>
                        <div class="wallet-balance-row">
                            <div class="wallet-balance{anim_cls}">{balance} 点</div>
                        </div>
                        <div class="wallet-cny{anim_cls}">约等于 ￥{balance / 10:.1f}</div>
                        <div class="wallet-refresh-note">{refresh_text}</div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
    with wallet_spacer_col:
        st.markdown("", unsafe_allow_html=True)

    st.markdown(
        f"""
        <div class="wallet-metrics{anim_cls}">
            <div class="metric-item">
                <div class="metric-k">累计充值</div>
                <div class="metric-v">{total_earned}</div>
            </div>
            <div class="metric-item">
                <div class="metric-k">累计消费</div>
                <div class="metric-v">{total_spent}</div>
            </div>
            <div class="metric-item">
                <div class="metric-k">最近更新时间</div>
                <div class="metric-v" style="font-size:16px;">{updated_text}</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
st.markdown('<div class="section-title">商城中心</div>', unsafe_allow_html=True)

shop_topup_tab, shop_paid_tab = st.tabs(["充值商城", "付费产品"])

with shop_topup_tab:
    st.markdown('<div class="section-sub">选择充值套餐并跳转支付宝完成付款</div>', unsafe_allow_html=True)

    if not pay_svc.is_points_payment_enabled():
        st.warning(
            "支付功能当前未开启（POINTS_PAYMENT_ENABLED=false）。"
            "请先完成企业支付宝配置后再开放给用户。"
        )

    cols = st.columns(2)
    for idx, pkg in enumerate(pay_svc.POINTS_PACKAGES):
        with cols[idx % 2]:
            bonus = int(pkg["points"] - int(float(pkg["rmb"]) * 10))
            bonus_text = f"赠送 {bonus} 点" if bonus > 0 else "标准兑换比例"
            pkg_name = str(pkg.get("name") or "")

            badge_html = ""
            if "超值" in pkg_name:
                badge_html = '<span class="shop-badge reco-hot">推荐</span>'

            card_html = (
                f'<div class="shop-card">{badge_html}'
                f'<div class="shop-name">{pkg_name}</div>'
                f'<div class="shop-price">￥{pkg["rmb"]}</div>'
                f'<div class="shop-points">{pkg["points"]} 点</div>'
                f'<div class="shop-desc">{bonus_text}</div>'
                "</div>"
            )
            st.markdown(card_html, unsafe_allow_html=True)

            if st.button(
                f"立即充值 · {pkg_name}",
                key=f"topup_{idx}",
                use_container_width=True,
            ):
                result = pay_svc.create_topup_order(user_id, pkg_name)
                if not result:
                    st.error("创建充值订单失败：支付未开启或支付宝配置未完成。")
                else:
                    st.session_state.points_last_order = result
                    st.success("订单创建成功，请在下方选择支付。")

    order = st.session_state.get("points_last_order")
    if order:
        st.markdown(
            f"""
            <div class="order-box">
                <div class="order-title">当前待支付订单</div>
                <div class="order-main">订单号 {order['order_id']}</div>
                <div class="order-sub">充值 {order['points']} 点 ｜ 金额 ￥{order['rmb']:.2f}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown(
            """
            <div class="pay-method-title">选择支付方式</div>
            <div class="pay-method-card">
                <div class="pay-logo">支</div>
                <div class="pay-meta">
                    <div class="pay-name">支付宝支付</div>
                    <div class="pay-desc">推荐 · 实时到账</div>
                </div>
                <div class="pay-check">✓</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown(
            f"<a class='alipay-pay-btn' href='{order['pay_url']}' target='_blank' rel='noopener noreferrer'><span class='btn-icon'>💳</span>去支付宝收银台</a>",
            unsafe_allow_html=True,
        )
        st.caption("支付完成后请点击余额旁刷新图标查看到账结果。")

with shop_paid_tab:
    st.markdown('<div class="section-sub">使用点数直接购买晚报与情报套餐，按月开通</div>', unsafe_allow_html=True)

    products = pay_svc.get_paid_products()
    user_subs = sub_svc.get_user_subscriptions(user_id)
    user_sub_map = {int(x["channel_id"]): x for x in user_subs}
    points_balance = int(pay_svc.get_user_points(user_id).get("balance") or 0)
    product_desc_map = {
        "daily_report": "根据新闻热点、宏观数据、技术面分析，给出商品和股票的操作建议。",
        "expiry_option_radar": "针对快到期期权，综合技术面和波动率，给出适合的策略。",
        "broker_position_report": "分析正指标机构 + 反指标散户的期货持仓，给出期货操盘建议。",
        "fund_flow_report": "分析每天股票市场的资金流动，跟踪成交量异常的潜力股。",
    }

    if not products:
        st.info("暂无可点数购买商品。")
    else:
        common_icon_html = '<span class="product-tech-icon">◈</span>'
        for product in products:
            ptype = product.get("product_type")
            code = str(product.get("code") or "")
            name = str(product.get("name") or "")
            ppm = int(product.get("points_monthly") or 0)

            col_a, col_b, col_c = st.columns([3.8, 1.2, 1.3])

            if ptype == "channel":
                channel_id = int(product["id"])
                sub_info = user_sub_map.get(channel_id)
                status_label = "未订阅"
                status_class = "none"
                expiry_text = "尚未开通"
                if sub_info and sub_info.get("is_active"):
                    exp = sub_info.get("expire_at")
                    exp_txt = exp.strftime("%Y-%m-%d") if hasattr(exp, "strftime") else str(exp)
                    status_label = "已订阅"
                    status_class = "active"
                    expiry_text = f"到期：{exp_txt}"
                elif sub_info:
                    exp = sub_info.get("expire_at")
                    exp_txt = exp.strftime("%Y-%m-%d") if hasattr(exp, "strftime") else str(exp or "")
                    status_label = "已过期"
                    status_class = "expired"
                    expiry_text = f"过期：{exp_txt}" if exp_txt else "已过期，请续费"

                with col_a:
                    desc_text = product_desc_map.get(code, "")
                    desc_html = f'<div class="product-desc">{desc_text}</div>' if desc_text else ""
                    card_html = (
                        '<div class="product-head">'
                        '<div class="product-main">'
                        f'<div class="product-name">{common_icon_html} {name}</div>'
                        f'<div class="product-meta">价格：{ppm} 点 / 月</div>'
                        f"{desc_html}"
                        '</div>'
                        '<div class="product-side">'
                        f'<div class="sub-chip {status_class}">{status_label}</div>'
                        f'<div class="sub-exp">{expiry_text}</div>'
                        '</div>'
                        '</div>'
                    )
                    st.markdown(
                        card_html,
                        unsafe_allow_html=True,
                    )

                with col_b:
                    months = st.selectbox(
                        f"{name} 月数",
                        options=product.get("months_options", [1]),
                        index=0,
                        key=f"months_{code}",
                        label_visibility="collapsed",
                    )
                    total_cost = ppm * int(months)
                    st.caption(f"合计 {total_cost} 点")

                with col_c:
                    disabled = total_cost <= 0
                    if st.button("用点数购买", key=f"buy_{code}", use_container_width=True, disabled=disabled):
                        st.session_state.points_pending_purchase = {
                            "product_type": "channel",
                            "channel_id": channel_id,
                            "channel_name": name,
                            "months": int(months),
                            "total_cost": total_cost,
                        }
            else:
                include_names = product.get("includes_names", [])
                include_codes = product.get("includes", [])
                include_rows = [
                    x
                    for x in products
                    if x.get("product_type") == "channel" and x.get("code") in include_codes
                ]
                active_cnt = 0
                for row in include_rows:
                    sub_info = user_sub_map.get(int(row["id"]))
                    if sub_info and sub_info.get("is_active"):
                        active_cnt += 1

                months = int(product.get("months_options", [1])[0])
                total_cost = ppm * months
                include_text = "、".join(include_names) if include_names else "暂无"
                covered_all = len(include_codes) > 0 and active_cnt == len(include_codes)
                package_status_label = "已全覆盖" if covered_all else "套餐权益"
                package_status_cls = "active" if covered_all else "none"
                package_status_text = f"已覆盖 {active_cnt}/{len(include_codes)} 个频道"

                with col_a:
                    desc_text = product_desc_map.get(code, "")
                    desc_html = f'<div class="product-desc">{desc_text}</div>' if desc_text else ""
                    card_html = (
                        '<div class="product-head">'
                        '<div class="product-main">'
                        f'<div class="product-name">{common_icon_html} {name}</div>'
                        f'<div class="product-meta">包含：{include_text}</div>'
                        f'<div class="product-meta">价格：{ppm} 点 / 月</div>'
                        f"{desc_html}"
                        '</div>'
                        '<div class="product-side">'
                        f'<div class="sub-chip {package_status_cls}">{package_status_label}</div>'
                        f'<div class="sub-exp">{package_status_text}</div>'
                        '</div>'
                        '</div>'
                    )
                    st.markdown(
                        card_html,
                        unsafe_allow_html=True,
                    )

                with col_b:
                    months = st.selectbox(
                        f"{name} 月数",
                        options=product.get("months_options", [1]),
                        index=0,
                        key=f"months_{code}",
                        label_visibility="collapsed",
                    )
                    total_cost = ppm * int(months)
                    st.caption(f"合计 {total_cost} 点")

                with col_c:
                    disabled = total_cost <= 0
                    if st.button("用点数购买", key=f"buy_{code}", use_container_width=True, disabled=disabled):
                        st.session_state.points_pending_purchase = {
                            "product_type": "package",
                            "package_code": code,
                            "channel_name": name,
                            "months": int(months),
                            "total_cost": total_cost,
                        }

            st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

    pending = st.session_state.get("points_pending_purchase")
    if pending:
        enough = points_balance >= int(pending["total_cost"])
        st.warning(
            f"确认购买：{pending['channel_name']} {pending['months']} 个月\n"
            f"需 {pending['total_cost']} 点（当前余额 {points_balance} 点）"
        )

        c1, c2 = st.columns(2)
        with c1:
            if st.button(
                "确认扣点并开通",
                key="confirm_points_purchase",
                type="primary",
                use_container_width=True,
            ):
                if not enough:
                    st.error("余额不足，请先充值。")
                else:
                    if pending.get("product_type") == "channel":
                        biz_id = (
                            f"web_sub:{user_id}:{pending['channel_id']}:{pending['months']}:"
                            f"{datetime.now().strftime('%Y%m%d%H%M%S')}"
                        )
                        ok, msg = pay_svc.purchase_subscription_with_points(
                            user_id,
                            int(pending["channel_id"]),
                            months=int(pending["months"]),
                            biz_id=biz_id,
                        )
                    else:
                        biz_id = (
                            f"web_pkg:{user_id}:{pending['package_code']}:{pending['months']}:"
                            f"{datetime.now().strftime('%Y%m%d%H%M%S')}"
                        )
                        ok, msg = pay_svc.purchase_intel_package_with_points(
                            user_id,
                            months=int(pending["months"]),
                            biz_id=biz_id,
                        )

                    if ok:
                        st.success(msg)
                    else:
                        st.error(msg)

                    st.session_state.points_pending_purchase = None
                    st.rerun()

        with c2:
            if st.button("取消", key="cancel_points_purchase", use_container_width=True):
                st.session_state.points_pending_purchase = None
                st.rerun()

    st.subheader("最近流水")
    history = pay_svc.get_points_history(user_id, limit=30)
    if not history:
        st.caption("暂无流水记录")
    else:
        rows = []
        type_map = {
            "topup": "充值",
            "spend": "消费",
            "refund": "退款",
            "admin_grant": "赠送",
        }

        for item in history:
            created_at = item.get("created_at")
            ts = created_at.strftime("%m-%d %H:%M") if hasattr(created_at, "strftime") else str(created_at)
            amount = int(item.get("amount") or 0)
            rows.append(
                {
                    "时间": ts,
                    "类型": type_map.get(item.get("type"), item.get("type")),
                    "变动点数": f"{amount:+d}",
                    "余额": int(item.get("balance_after") or 0),
                    "说明": item.get("description") or "",
                }
            )

        st.dataframe(rows, use_container_width=True, hide_index=True)
