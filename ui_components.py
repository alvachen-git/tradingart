import streamlit as st


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