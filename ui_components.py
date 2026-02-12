import streamlit as st


def inject_sidebar_toggle_style(mode: str = "high_contrast") -> None:
    """
    注入全站统一的侧边栏折叠/展开按钮样式。
    默认高对比模式，兼容不同 Streamlit 版本的 testid。
    """
    if mode != "high_contrast":
        raise ValueError(f"Unsupported sidebar toggle style mode: {mode}")

    st.markdown(
        """
        <style>
        header[data-testid="stHeader"] {
            background-color: transparent !important;
        }

        button[data-testid="stExpandSidebarButton"],
        [data-testid="stSidebarHeader"] button[data-testid="stBaseButton-headerNoPadding"],
        button[data-testid="collapsedControl"],
        button[data-testid="stSidebarCollapsedControl"],
        [data-testid="stSidebarCollapsedControl"],
        [data-testid="stSidebarCollapsedControl"] > button {
            pointer-events: auto !important;
        }

        button[data-testid="stExpandSidebarButton"],
        [data-testid="stSidebarHeader"] button[data-testid="stBaseButton-headerNoPadding"],
        button[data-testid="collapsedControl"],
        button[data-testid="stSidebarCollapsedControl"],
        [data-testid="stSidebarCollapsedControl"],
        [data-testid="stSidebarCollapsedControl"] > button {
            visibility: visible !important;
            display: flex !important;
            align-items: center !important;
            justify-content: center !important;
            width: 40px !important;
            height: 40px !important;
            min-width: 40px !important;
            min-height: 40px !important;
            background-color: #2563eb !important;
            border: 2px solid rgba(255, 255, 255, 0.85) !important;
            border-radius: 12px !important;
            box-shadow: 0 6px 16px rgba(2, 6, 23, 0.55) !important;
            opacity: 1 !important;
            transition: transform 0.18s ease, background-color 0.18s ease, box-shadow 0.18s ease !important;
        }

        button[data-testid="stExpandSidebarButton"] {
            position: fixed !important;
            top: 15px !important;
            left: 15px !important;
            z-index: 999997 !important;
        }

        button[data-testid="collapsedControl"]:hover,
        button[data-testid="stSidebarCollapsedControl"]:hover,
        button[data-testid="stExpandSidebarButton"]:hover,
        [data-testid="stSidebarHeader"] button[data-testid="stBaseButton-headerNoPadding"]:hover,
        [data-testid="stSidebarCollapsedControl"]:hover,
        [data-testid="stSidebarCollapsedControl"] > button:hover {
            background-color: #1d4ed8 !important;
            border-color: #ffffff !important;
            transform: scale(1.08) !important;
            box-shadow: 0 0 0 3px rgba(147, 197, 253, 0.35), 0 12px 28px rgba(2, 6, 23, 0.65) !important;
        }

        button[data-testid="collapsedControl"]:active,
        button[data-testid="stSidebarCollapsedControl"]:active,
        button[data-testid="stExpandSidebarButton"]:active,
        [data-testid="stSidebarHeader"] button[data-testid="stBaseButton-headerNoPadding"]:active,
        [data-testid="stSidebarCollapsedControl"]:active,
        [data-testid="stSidebarCollapsedControl"] > button:active {
            transform: scale(1.02) !important;
        }

        button[data-testid="collapsedControl"]:focus-visible,
        button[data-testid="stSidebarCollapsedControl"]:focus-visible,
        button[data-testid="stExpandSidebarButton"]:focus-visible,
        [data-testid="stSidebarHeader"] button[data-testid="stBaseButton-headerNoPadding"]:focus-visible,
        [data-testid="stSidebarCollapsedControl"]:focus-visible,
        [data-testid="stSidebarCollapsedControl"] > button:focus-visible {
            outline: 2px solid #dbeafe !important;
            outline-offset: 2px !important;
            box-shadow: 0 0 0 4px rgba(96, 165, 250, 0.55), 0 10px 24px rgba(2, 6, 23, 0.6) !important;
        }

        button[data-testid="stExpandSidebarButton"] span,
        button[data-testid="stExpandSidebarButton"] p,
        button[data-testid="stExpandSidebarButton"] svg,
        button[data-testid="stExpandSidebarButton"] i,
        [data-testid="stSidebarHeader"] button[data-testid="stBaseButton-headerNoPadding"] span,
        [data-testid="stSidebarHeader"] button[data-testid="stBaseButton-headerNoPadding"] p,
        [data-testid="stSidebarHeader"] button[data-testid="stBaseButton-headerNoPadding"] svg,
        [data-testid="stSidebarHeader"] button[data-testid="stBaseButton-headerNoPadding"] i,
        button[data-testid="collapsedControl"] span,
        button[data-testid="stSidebarCollapsedControl"] span,
        [data-testid="stSidebarCollapsedControl"] span,
        button[data-testid="collapsedControl"] p,
        button[data-testid="stSidebarCollapsedControl"] p,
        [data-testid="stSidebarCollapsedControl"] p,
        button[data-testid="collapsedControl"] svg,
        button[data-testid="stSidebarCollapsedControl"] svg,
        [data-testid="stSidebarCollapsedControl"] svg,
        button[data-testid="collapsedControl"] i,
        button[data-testid="stSidebarCollapsedControl"] i,
        [data-testid="stSidebarCollapsedControl"] i,
        [data-testid="stSidebarCollapsedControl"] > button span,
        [data-testid="stSidebarCollapsedControl"] > button p,
        [data-testid="stSidebarCollapsedControl"] > button svg,
        [data-testid="stSidebarCollapsedControl"] > button i,
        button[data-testid="stExpandSidebarButton"] *,
        [data-testid="stSidebarHeader"] button[data-testid="stBaseButton-headerNoPadding"] *,
        button[data-testid="collapsedControl"] * ,
        button[data-testid="stSidebarCollapsedControl"] *,
        [data-testid="stSidebarCollapsedControl"] > button *,
        [data-testid="stSidebarCollapsedControl"] * {
            fill: #ffffff !important;
            color: #ffffff !important;
            stroke: #ffffff !important;
            width: 20px !important;
            height: 20px !important;
            opacity: 1 !important;
            font-weight: 800 !important;
            text-shadow: 0 1px 2px rgba(0, 0, 0, 0.45) !important;
        }

        @media (max-width: 768px) {
            button[data-testid="stExpandSidebarButton"],
            [data-testid="stSidebarHeader"] button[data-testid="stBaseButton-headerNoPadding"],
            button[data-testid="collapsedControl"],
            button[data-testid="stSidebarCollapsedControl"],
            [data-testid="stSidebarCollapsedControl"],
            [data-testid="stSidebarCollapsedControl"] > button {
                width: 36px !important;
                height: 36px !important;
                min-width: 36px !important;
                min-height: 36px !important;
            }

            button[data-testid="stExpandSidebarButton"] {
                top: 12px !important;
                left: 12px !important;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def card_html(symbol, name, direction, net_vol, main_player):
    """
    生成一个 QuantLab 风格的玻璃卡片
    """
    # 根据做多/做空决定颜色
    if direction == "做多":
        color_class = "neon-green"
        bg_class = "rgba(16, 185, 129, 0.1)"
        border_class = "rgba(16, 185, 129, 0.2)"
        arrow = "▲"
        label = "BULLISH"
        bar_color = "#10b981"
    else:
        color_class = "neon-red"
        bg_class = "rgba(239, 68, 68, 0.1)"
        border_class = "rgba(239, 68, 68, 0.2)"
        arrow = "▼"
        label = "BEARISH"
        bar_color = "#ef4444"

    html = f"""
    <div class="glass-card">
        <div style="position:absolute; top:0; left:0; width:100%; height:4px; background:{bar_color};"></div>

        <div style="display:flex; justify-content:space-between; align-items:start; margin-top:10px;">
            <div>
                <h3 style="margin:0; font-size:1.5rem; color:white; font-weight:bold;">
                    {symbol} <span style="font-size:0.8rem; color:#94a3b8; font-weight:normal;">{name}</span>
                </h3>
                <p style="margin:5px 0 0 0; font-size:0.75rem; color:#64748b;">主力: {main_player}</p>
            </div>
            <div style="padding:2px 8px; background:{bg_class}; border:1px solid {border_class}; border-radius:4px; font-size:0.7rem; color:{bar_color}; font-weight:bold;">
                {label}
            </div>
        </div>

        <div style="margin-top:15px; display:flex; align-items:baseline;">
            <span class="{color_class}" style="font-size:2.2rem; font-weight:bold; font-family:'JetBrains Mono'">{direction}</span>
            <span style="margin-left:10px; font-size:1.5rem; color:{bar_color};">{arrow}</span>
        </div>

        <div style="margin-top:15px; padding-top:15px; border-top:1px solid rgba(255,255,255,0.05); display:flex; justify-content:space-between; font-size:0.85rem;">
            <span style="color:#64748b;">净单量</span>
            <span class="mono-font" style="color:white;">{net_vol}</span>
        </div>
    </div>
    """
    return html
