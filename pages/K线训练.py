import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import json
from datetime import datetime, timedelta
import sys
import os
import time

# 添加父目录到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import kline_game as kg
import auth_utils as auth
import extra_streamlit_components as stx

# ==========================================
# 页面配置
# ==========================================
st.set_page_config(
    page_title="K线训练场",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==========================================
# CSS样式（与Home.py保持一致）
# ==========================================
st.markdown("""
<style>
    /* 强制全局背景为深空蓝黑 */
    .stApp {
        background-color: #0b1121 !important;
        background-image: radial-gradient(circle at 50% 0%, #1e293b 0%, #0b1121 70%);
        color: white !important;
    }

    /* 拓宽主内容区域 */
    .block-container {
        padding: 1rem 2rem !important;
        max-width: 100% !important;
    }

    /* 不要隐藏整个 Header，只把背景变透明 */
    header[data-testid="stHeader"] {
        background-color: transparent !important;
    }

    /* 隐藏顶部的彩虹装饰线条 */
    [data-testid="stDecoration"] {
        display: none;
    }

    /* 侧边栏样式 */
    [data-testid="stSidebar"] {
        background-color: #0f172a !important;
    }
    [data-testid="stSidebar"] p, 
    [data-testid="stSidebar"] span, 
    [data-testid="stSidebar"] div {
        color: #cbd5e1 !important;
    }

    /* 隐藏其他默认元素 */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)


# ==========================================
# Cookie管理器初始化（与Home.py相同）
# ==========================================
def get_cookie_manager():
    return stx.CookieManager(key="kline_game_cookie_manager")


cookie_manager = get_cookie_manager()

# 初始化登录状态
if 'is_logged_in' not in st.session_state:
    st.session_state['is_logged_in'] = False
    st.session_state['user_id'] = None

# ==========================================
# 从Cookie恢复登录状态
# ==========================================
if not st.session_state.get('is_logged_in') and not st.session_state.get('just_logged_out', False):
    cookies = cookie_manager.get_all() or {}
    c_user = cookies.get("username")
    c_token = cookies.get("token")

    if c_user and c_token and str(c_user).strip() != "":
        try:
            if auth.check_token(str(c_user), c_token):
                st.session_state['is_logged_in'] = True
                st.session_state['user_id'] = str(c_user)
        except:
            pass

# 重置登出标记
if st.session_state.get('just_logged_out', False):
    st.session_state['just_logged_out'] = False

# ==========================================
# 侧边栏
# ==========================================
with st.sidebar:
    st.markdown("### 🎮 K线训练场")
    st.markdown("---")

    if st.session_state.get('is_logged_in'):
        st.success(f"👤 {st.session_state.get('user_id')}")
    else:
        st.info("请先在首页登录")

# ==========================================
# 游戏结算通过API服务器完成
# 需要先启动: python trade_api.py
# ==========================================

# ==========================================
# 处理游戏结果 URL 参数（必须在登录检查之前！）
# ==========================================
# 获取URL参数
game_done = st.query_params.get('game_done', '')

if game_done == '1':
    # 读取所有参数
    profit = int(st.query_params.get('profit', '0') or '0')
    profit_rate = float(st.query_params.get('rate', '0') or '0')
    trade_count = int(st.query_params.get('trades', '0') or '0')
    max_drawdown = float(st.query_params.get('drawdown', '0') or '0')
    had_30_loss = st.query_params.get('had_loss', '') == '1'

    # 从URL参数获取游戏信息
    game_id_str = st.query_params.get('game_id', '') or ''
    game_id = int(game_id_str) if game_id_str.isdigit() else None
    symbol = st.query_params.get('symbol', '') or ''
    symbol_name = st.query_params.get('symbol_name', '') or '未知'
    symbol_type = st.query_params.get('symbol_type', '') or 'stock'
    capital_str = st.query_params.get('capital', '') or '0'
    capital_before = int(float(capital_str)) if capital_str != '0' else 1000000
    capital_after = capital_before + profit

    # 立即清除URL参数
    st.query_params.clear()

    # 从游戏记录中获取user_id并恢复登录状态
    result_user_id = None
    new_achievements = []

    try:
        if game_id:
            game_info = kg.get_game_info(game_id)
            if game_info:
                result_user_id = game_info.get('user_id')

                # 恢复登录状态（关键修复！）
                if result_user_id and not st.session_state.get('is_logged_in'):
                    st.session_state.is_logged_in = True
                    st.session_state.user_id = result_user_id

                # 保存结果到数据库
                success = kg.end_game(
                    game_id, result_user_id, 'finished', 'completed',
                    profit, profit_rate, int(capital_after),
                    trade_count, max_drawdown
                )
                if success:
                    new_achievements = kg.check_achievements(
                        result_user_id, profit, profit_rate, trade_count,
                        max_drawdown, had_30_loss, capital_after
                    )
    except Exception as e:
        st.warning(f"保存游戏结果时出错: {e}")

    # 显示结果页面（无论保存是否成功都显示）
    st.markdown("<h2 style='text-align: center; color: #e5e7eb;'>游戏结束</h2>", unsafe_allow_html=True)

    # 揭晓品种
    symbol_type_text = '股票' if symbol_type == 'stock' else '期货'

    st.markdown(f"""
    <div style="text-align: center; margin: 20px 0;">
        <div style="background: #111827; border: 1px solid #1f2937; border-radius: 8px; padding: 24px; display: inline-block;">
            <div style="font-size: 14px; color: #6b7280; margin-bottom: 8px;">揭晓品种</div>
            <div style="font-size: 32px; font-weight: 600; color: #e5e7eb;">{symbol_name}</div>
            <div style="font-size: 14px; color: #6b7280; margin-top: 4px;">{symbol_type_text} · {symbol}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    profit_color = '#ef4444' if profit > 0 else '#22c55e' if profit < 0 else '#e5e7eb'

    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(f"""
        <div style="background: #111827; border: 1px solid #1f2937; border-radius: 8px; padding: 24px; text-align: center;">
            <div style="font-size: 14px; color: #6b7280; margin-bottom: 8px;">本局盈亏</div>
            <div style="font-size: 32px; font-weight: 600; color: {profit_color};">{profit:+,.0f}</div>
            <div style="font-size: 14px; color: #6b7280; margin-top: 4px;">{profit_rate * 100:+.1f}%</div>
        </div>
        """, unsafe_allow_html=True)
    with col2:
        st.markdown(f"""
        <div style="background: #111827; border: 1px solid #1f2937; border-radius: 8px; padding: 24px; text-align: center;">
            <div style="font-size: 14px; color: #6b7280; margin-bottom: 8px;">账户余额</div>
            <div style="font-size: 32px; font-weight: 600; color: #e5e7eb;">{capital_after:,.0f}</div>
        </div>
        """, unsafe_allow_html=True)
    with col3:
        st.markdown(f"""
        <div style="background: #111827; border: 1px solid #1f2937; border-radius: 8px; padding: 24px; text-align: center;">
            <div style="font-size: 14px; color: #6b7280; margin-bottom: 8px;">交易次数</div>
            <div style="font-size: 32px; font-weight: 600; color: #e5e7eb;">{trade_count}</div>
        </div>
        """, unsafe_allow_html=True)

    # 显示新解锁的成就
    if new_achievements:
        achievement_html = '<div style="text-align: center; background: rgba(34, 197, 94, 0.1); border: 1px solid rgba(34, 197, 94, 0.3); border-radius: 8px; padding: 20px; margin: 24px 0;">'
        achievement_html += '<div style="font-size: 18px; color: #86efac; margin-bottom: 12px;">🎉 解锁新成就！</div>'
        for ach in new_achievements:
            achievement_html += f'<span style="display:inline-block;background:linear-gradient(135deg,#3b82f6,#8b5cf6);color:white;padding:6px 16px;border-radius:4px;font-size:14px;margin:4px;">{ach["name"]} (+{ach["exp"]}经验)</span>'
        achievement_html += '</div>'
        st.markdown(achievement_html, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        if st.button("再来一局", use_container_width=True, type="primary"):
            st.session_state.kline_game_phase = 'setup'
            st.session_state.kline_game_id = None
            st.rerun()

    # 必须停止，不要继续执行到登录检查
    st.stop()

# ==========================================
# 检查登录状态
# ==========================================
if 'is_logged_in' not in st.session_state or not st.session_state.get('is_logged_in'):
    st.warning("请先登录后再进行游戏")
    st.stop()

user_id = st.session_state.get('user_id')
if not user_id:
    st.error("无法获取用户信息")
    st.stop()

# ==========================================
# 初始化 Session State
# ==========================================
if 'kline_game_phase' not in st.session_state:
    st.session_state.kline_game_phase = 'setup'

if 'kline_game_id' not in st.session_state:
    st.session_state.kline_game_id = None

# ==========================================
# 检查未完成的游戏（防作弊机制）
# ==========================================
# 检测数据库中是否有状态为'playing'的游戏
unfinished = kg.check_unfinished_game(user_id)

# 如果有未完成游戏，且当前不是那个游戏（说明玩家离开过）
if unfinished:
    current_game_id = st.session_state.get('kline_game_id')

    # 判断是否需要惩罚：
    # 1. session中没有游戏ID（刷新了页面）
    # 2. session中的游戏ID与数据库中不一致
    # 3. phase不是playing（已经离开了游戏界面）
    need_penalty = (
            current_game_id is None or
            current_game_id != unfinished['id'] or
            st.session_state.kline_game_phase != 'playing'
    )

    if need_penalty:
        # 执行惩罚：固定扣5万
        penalty_result = kg.settle_abandoned_game(user_id, unfinished['id'], penalty=50000)

        # 清除session中的游戏数据
        st.session_state.kline_game_phase = 'setup'
        st.session_state.kline_game_id = None
        if 'game_config' in st.session_state:
            del st.session_state.game_config
        if 'kline_data' in st.session_state:
            del st.session_state.kline_data
        if 'game_result' in st.session_state:
            del st.session_state.game_result

        # 显示惩罚信息
        if penalty_result:
            st.error(f"""
            ⚠️ **检测到上局游戏未正常结算**

            品种：{unfinished.get('symbol_name', '未知')}  
            惩罚：**-50,000 元**  

            提示：游戏进行中请勿离开页面，否则将受到惩罚！
            """)
        else:
            st.warning("检测到未完成的游戏，已自动结算")

# ==========================================
# 获取用户信息
# ==========================================
user_capital = kg.get_user_capital(user_id)
user_stats = kg.get_user_stats(user_id)

# ==========================================
# 阶段1：游戏设置
# ==========================================
if st.session_state.kline_game_phase == 'setup':

    st.markdown(f"""
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 24px;">
        <div>
            <h1 style="margin: 0; font-size: 26px; font-weight: 600; color: #e5e7eb;">K线训练场</h1>
            <p style="margin: 4px 0 0; font-size: 14px; color: #6b7280;">随机历史K线 · 模拟交易训练</p>
        </div>
        <div style="text-align: right;">
            <div style="font-size: 14px; color: #6b7280;">账户资金</div>
            <div style="font-size: 24px; font-weight: 600; color: #e5e7eb;">{user_capital:,.0f}</div>
        </div>
    </div>
    <hr style="border-color: #1f2937; margin-bottom: 24px;">
    """, unsafe_allow_html=True)

    col1, col2 = st.columns([2, 1])

    with col1:
        st.markdown("""
        <div style="background: #111827; border: 1px solid #1f2937; border-radius: 4px; padding: 16px; margin-bottom: 16px;">
            <div style="font-size: 14px; color: #6b7280; margin-bottom: 8px;">游戏规则</div>
            <div style="font-size: 14px; color: #9ca3af; line-height: 1.8;">
                · 随机抽取一段历史K线（股票或期货），品种隐藏<br>
                · 自动播放 100 根K线，不可暂停<br>
                · 每次下单固定 1,000 元，可多次加仓/减仓<br>
                · 可选择 1倍 或 10倍 杠杆（开仓后锁定）<br>
                · 10倍杠杆时，持仓不超过资金的50%，浮亏超70%强制平仓<br>
                · 走完100根K线后自动结算，揭晓品种
            </div>
        </div>
        """, unsafe_allow_html=True)

        speed = st.radio("播放速度", [1, 5], format_func=lambda x: "快速 (1秒/根)" if x == 1 else "慢速 (5秒/根)",
                         horizontal=True)
        leverage = st.radio("杠杆倍数", [1, 10], format_func=lambda x: f"{x}倍" + (" (持仓上限50%)" if x == 10 else ""),
                            horizontal=True)

        if leverage == 10:
            st.warning("10倍杠杆风险提示：浮亏超过账户资金70%将强制平仓")

        if st.button("开始游戏", type="primary", use_container_width=True):
            if user_capital < 1000:
                st.error("账户资金不足，至少需要 1,000 元")
            else:
                with st.spinner("正在抽取K线数据..."):
                    symbol, symbol_name, symbol_type, kline_df = kg.get_random_kline_data(100, 60)  # 100播放 + 60历史

                if kline_df is None:
                    st.error("获取K线数据失败，请重试")
                else:
                    game_id = kg.create_game(
                        user_id=user_id, speed=speed, leverage=leverage,
                        symbol=symbol, symbol_name=symbol_name, symbol_type=symbol_type,
                        data_start_date=kline_df.index[0].date(),
                        data_end_date=kline_df.index[-1].date(),
                        capital_before=user_capital
                    )

                    if game_id:
                        kline_data = []
                        for idx, row in kline_df.iterrows():
                            kline_data.append({
                                'time': idx.strftime('%Y-%m-%d'),
                                'open': float(row['open_price']),
                                'high': float(row['high_price']),
                                'low': float(row['low_price']),
                                'close': float(row['close_price']),
                                'volume': float(row['vol'])
                            })

                        st.session_state.kline_game_phase = 'playing'
                        st.session_state.kline_game_id = game_id
                        st.session_state.kline_data = kline_data
                        st.session_state.game_config = {
                            'speed': speed,
                            'leverage': leverage,
                            'capital': user_capital,
                            'symbol': symbol,
                            'symbol_name': symbol_name,
                            'symbol_type': symbol_type
                        }
                        st.rerun()

    with col2:
        st.markdown("""<div style="background: #111827; border: 1px solid #1f2937; border-radius: 4px; padding: 16px;">
            <div style="font-size: 14px; color: #6b7280; margin-bottom: 8px;">我的战绩</div>""", unsafe_allow_html=True)

        if user_stats:
            total_games = user_stats.get('total_games', 0)
            win_games = user_stats.get('win_games', 0)
            win_rate = (win_games / total_games * 100) if total_games > 0 else 0
            total_profit = user_stats.get('total_profit', 0)
            profit_color = '#ef4444' if total_profit > 0 else '#22c55e' if total_profit < 0 else '#e5e7eb'

            st.markdown(f"""<div style="font-size: 14px; color: #9ca3af; line-height: 2;">
                总局数：<span style="color: #e5e7eb;">{total_games}</span><br>
                胜率：<span style="color: #e5e7eb;">{win_rate:.1f}%</span><br>
                累计盈亏：<span style="color: {profit_color};">{total_profit:+,.0f}</span><br>
                最长连胜：<span style="color: #e5e7eb;">{user_stats.get('max_streak', 0)}</span>
            </div>""", unsafe_allow_html=True)
        else:
            st.markdown('<div style="color: #6b7280;">暂无游戏记录</div>', unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

        achievements = kg.get_user_achievements(user_id)
        if achievements:
            st.markdown("""<div style="background: #111827; border: 1px solid #1f2937; border-radius: 4px; padding: 16px; margin-top: 16px;">
                <div style="font-size: 14px; color: #6b7280; margin-bottom: 8px;">已获成就</div>""",
                        unsafe_allow_html=True)
            badges = "".join([
                                 f'<span style="display:inline-block;background:linear-gradient(135deg,#3b82f6,#8b5cf6);color:white;padding:2px 8px;border-radius:4px;font-size:12px;margin:2px;">{a["achievement_name"]}</span>'
                                 for a in achievements[:6]])
            st.markdown(f"{badges}</div>", unsafe_allow_html=True)

# ==========================================
# 阶段2：游戏进行中 (JavaScript)
# ==========================================
elif st.session_state.kline_game_phase == 'playing':

    game_id = st.session_state.kline_game_id

    # 二次进入检测（防止玩家离开后再回来重玩同一局）
    # 原理：首次进入时设置标记，如果再次进入发现标记已存在，说明离开过
    game_entered_key = f"game_{game_id}_entered"

    if st.session_state.get(game_entered_key):
        # 已经进入过这个游戏了，现在又进来了 = 用户离开过页面
        # 检查数据库中游戏是否还在进行
        unfinished = kg.check_unfinished_game(user_id)
        if unfinished and unfinished['id'] == game_id:
            # 执行惩罚
            penalty_result = kg.settle_abandoned_game(user_id, game_id, penalty=50000)

            # 清除游戏状态
            st.session_state.kline_game_phase = 'setup'
            st.session_state.kline_game_id = None
            if 'game_config' in st.session_state:
                del st.session_state.game_config
            if 'kline_data' in st.session_state:
                del st.session_state.kline_data
            # 清除进入标记
            del st.session_state[game_entered_key]

            st.error(f"""
            ⚠️ **检测到游戏中途离开**

            您在游戏进行中离开了页面，游戏已自动结算。

            惩罚：**-50,000 元**

            提示：游戏进行中请勿切换页面或刷新，否则将受到惩罚！
            """)
            st.stop()
    else:
        # 首次进入，设置标记
        st.session_state[game_entered_key] = True

    config = st.session_state.game_config
    kline_data = st.session_state.kline_data

    game_html = f'''
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <script src="https://unpkg.com/lightweight-charts@4.1.0/dist/lightweight-charts.standalone.production.js"></script>
    <script>
        // 结算API
        async function settleGame(profit, profitRate, tradeCount, maxDrawdown, had30Loss) {{
            try {{
                const response = await fetch('http://127.0.0.1:5000/api/settle', {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify({{
                        game_id: {game_id},
                        user_id: '{user_id}',
                        profit: profit,
                        profit_rate: profitRate,
                        trade_count: tradeCount,
                        max_drawdown: maxDrawdown,
                        had_30_loss: had30Loss,
                        capital: {config['capital']}
                    }})
                }});
                return await response.json();
            }} catch(e) {{
                console.error('结算API调用失败:', e);
                return null;
            }}
        }}
    </script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; background: #0b1120; color: #e5e7eb; }}
        .container {{ padding: 12px; }}
        .header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; }}
        .title {{ font-size: 18px; font-weight: 600; }}
        .capital {{ text-align: right; }}
        .capital-label {{ font-size: 11px; color: #6b7280; }}
        .capital-value {{ font-size: 18px; font-weight: 600; }}
        .progress-container {{ margin-bottom: 8px; }}
        .progress-info {{ display: flex; justify-content: space-between; font-size: 12px; color: #6b7280; margin-bottom: 4px; }}
        .progress-bar {{ background: #1f2937; height: 6px; border-radius: 3px; overflow: hidden; }}
        .progress-fill {{ background: #3b82f6; height: 100%; transition: width 0.3s; }}
        #kline-container {{ width: 100%; height: 480px; background: #111827; border: 1px solid #1f2937; border-radius: 4px 4px 0 0; }}
        #volume-container {{ width: 100%; height: 50px; background: #111827; border: 1px solid #1f2937; border-top: none; border-radius: 0 0 4px 4px; margin-bottom: 10px; }}
        .info-bar {{ display: flex; align-items: center; gap: 20px; padding: 8px 12px; background: #111827; border: 1px solid #1f2937; border-radius: 4px; margin-bottom: 10px; }}
        .info-label {{ font-size: 11px; color: #6b7280; }}
        .info-value {{ font-size: 16px; font-weight: 600; }}
        .info-value.positive {{ color: #ef4444; }}
        .info-value.negative {{ color: #22c55e; }}

        .trade-panel {{ display: flex; gap: 20px; align-items: center; flex-wrap: wrap; }}
        .trade-buttons {{ display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }}
        .trade-btn {{ padding: 12px 20px; border: 1px solid #374151; border-radius: 4px; font-size: 14px; font-weight: 500; cursor: pointer; transition: all 0.2s; min-width: 100px; background: #1f2937; color: #e5e7eb; }}
        .trade-btn:hover:not(:disabled) {{ background: #374151; }}
        .trade-btn:disabled {{ opacity: 0.4; cursor: not-allowed; }}
        .btn-long {{ border-color: #dc2626; color: #fca5a5; }}
        .btn-long:hover:not(:disabled) {{ background: #7f1d1d; }}
        .btn-short {{ border-color: #7c3aed; color: #c4b5fd; }}
        .btn-short:hover:not(:disabled) {{ background: #4c1d95; }}
        .btn-close-long {{ border-color: #16a34a; color: #86efac; }}
        .btn-close-long:hover:not(:disabled) {{ background: #14532d; }}
        .btn-close-short {{ border-color: #d97706; color: #fcd34d; }}
        .btn-close-short:hover:not(:disabled) {{ background: #78350f; }}

        .lot-selector {{ display: flex; align-items: center; gap: 12px; }}
        .lot-label {{ font-size: 13px; color: #9ca3af; }}
        .lot-value {{ font-size: 24px; font-weight: 600; color: #e5e7eb; min-width: 30px; text-align: center; }}
        .lot-controls {{ display: flex; gap: 4px; }}
        .lot-btn {{ width: 28px; height: 28px; border: 1px solid #374151; background: #1f2937; color: #e5e7eb; border-radius: 4px; font-size: 16px; cursor: pointer; }}
        .lot-btn:hover {{ background: #374151; }}

        .side-info {{ display: flex; align-items: center; gap: 12px; margin-left: auto; }}
        .leverage-badge {{ padding: 6px 10px; background: #1f2937; border: 1px solid #374151; border-radius: 4px; font-size: 12px; color: #9ca3af; }}
        .end-game-btn {{ padding: 8px 16px; background: #374151; border: 1px solid #4b5563; color: #e5e7eb; border-radius: 4px; font-size: 12px; cursor: pointer; }}
        .end-game-btn:hover {{ background: #4b5563; }}

        .trade-log {{ margin-top: 10px; }}
        .trade-log-header {{ display: flex; justify-content: space-between; align-items: center; padding: 8px 12px; background: #1f2937; border: 1px solid #374151; border-radius: 4px; cursor: pointer; }}
        .trade-log-header:hover {{ background: #374151; }}
        .trade-log-title {{ font-size: 13px; color: #9ca3af; }}
        .trade-log-toggle {{ font-size: 12px; color: #6b7280; }}
        .trade-log-content {{ display: none; padding: 8px 12px; background: #111827; border: 1px solid #1f2937; border-top: none; border-radius: 0 0 4px 4px; max-height: 100px; overflow-y: auto; }}
        .trade-item {{ font-size: 12px; color: #9ca3af; padding: 4px 0; border-bottom: 1px solid #1f2937; }}
        .trade-item:last-child {{ border-bottom: none; }}
        .game-over {{ position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.85); display: flex; align-items: center; justify-content: center; z-index: 1000; }}
        .game-over-content {{ background: #111827; border: 1px solid #1f2937; border-radius: 8px; padding: 32px; text-align: center; min-width: 400px; }}
        .game-over h2 {{ font-size: 24px; margin-bottom: 8px; }}
        .reveal {{ font-size: 28px; font-weight: 600; margin: 20px 0; color: #e5e7eb; }}
        .reveal-sub {{ font-size: 14px; color: #6b7280; }}
        .result-row {{ display: flex; justify-content: space-around; margin: 24px 0; }}
        .result-item {{ text-align: center; }}
        .result-label {{ font-size: 13px; color: #6b7280; margin-bottom: 4px; }}
        .result-value {{ font-size: 24px; font-weight: 600; }}
        .btn-restart {{ margin-top: 16px; padding: 12px 32px; background: #3b82f6; color: white; border: none; border-radius: 4px; font-size: 15px; cursor: pointer; }}
        .btn-restart:hover {{ background: #2563eb; }}
        .force-close {{ background: rgba(239,68,68,0.1); border: 1px solid rgba(239,68,68,0.3); color: #fca5a5; padding: 12px; border-radius: 4px; margin-bottom: 16px; text-align: center; font-size: 16px; display: none; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div><div class="title">K线训练场</div></div>
            <div class="capital"><div class="capital-label">账户资金</div><div class="capital-value" id="capital">{config['capital']:,.0f}</div></div>
        </div>
        <div class="progress-container">
            <div class="progress-info"><span id="progress-text">第 0 / 100 根</span><span>{config['speed']}秒/根</span></div>
            <div class="progress-bar"><div class="progress-fill" id="progress-fill" style="width: 1%;"></div></div>
        </div>
        <div class="force-close" id="force-close-alert">浮亏超过70%，强制平仓！</div>
        <div id="kline-container"></div>
        <div id="volume-container"></div>
        <div class="info-bar">
            <div class="info-item"><div class="info-label">当前价格</div><div class="info-value" id="current-price">--</div></div>
            <div class="info-item"><div class="info-label">持仓</div><div class="info-value" id="position">空仓</div></div>
            <div class="info-item"><div class="info-label">浮动盈亏</div><div class="info-value" id="floating-pnl">0</div></div>
            <div class="info-item"><div class="info-label">本局盈亏</div><div class="info-value" id="total-pnl">0</div></div>
        </div>

        <div class="trade-panel">
            <div class="trade-buttons">
                <button class="trade-btn btn-long" id="btn-buy" onclick="doBuy()">买入做多</button>
                <button class="trade-btn btn-short" id="btn-short" onclick="doShort()">卖出做空</button>
                <button class="trade-btn btn-close-long" id="btn-sell" onclick="doSell()" disabled>平仓多头</button>
                <button class="trade-btn btn-close-short" id="btn-cover" onclick="doCover()" disabled>平仓空头</button>
            </div>
            <div class="lot-selector">
                <span class="lot-label">手数</span>
                <button class="lot-btn" onclick="changeLot(-1)">-</button>
                <span class="lot-value" id="lot-value">1</span>
                <button class="lot-btn" onclick="changeLot(1)">+</button>
            </div>
            <div class="side-info">
                <div class="leverage-badge">{config['leverage']}x · 每手1000元</div>
                <button class="end-game-btn" onclick="manualEnd()">结束游戏</button>
            </div>
        </div>

        <div class="trade-log">
            <div class="trade-log-header" onclick="toggleTradeLog()">
                <span class="trade-log-title">交易记录 (<span id="trade-count">0</span>笔)</span>
                <span class="trade-log-toggle" id="trade-log-toggle">展开 ▼</span>
            </div>
            <div class="trade-log-content" id="trade-log-content"></div>
        </div>
    </div>
    <div class="game-over" id="game-over" style="display:none;">
        <div class="game-over-content">
            <h2 id="game-over-title">游戏结束</h2>
            <div class="reveal" id="reveal-name"></div>
            <div class="reveal-sub" id="reveal-info"></div>
            <div class="result-row">
                <div class="result-item"><div class="result-label">本局盈亏</div><div class="result-value" id="final-profit">0</div></div>
                <div class="result-item"><div class="result-label">收益率</div><div class="result-value" id="final-rate">0%</div></div>
                <div class="result-item"><div class="result-label">交易次数</div><div class="result-value" id="final-trades">0</div></div>
            </div>
            <div id="settle-status" style="font-size: 14px; color: #f59e0b; margin-bottom: 16px; line-height: 1.6;">正在结算...</div>
            <button id="settle-btn" class="btn-restart" disabled>请稍候</button>
        </div>
    </div>
    <script>
        const CONFIG = {{ speed: {config['speed']} * 1000, leverage: {config['leverage']}, capital: {config['capital']}, symbol: "{config['symbol']}", symbolName: "{config['symbol_name']}", symbolType: "{config['symbol_type']}", gameId: {game_id}, historyBars: 60, playBars: 100 }};
        const KLINE_DATA = {json.dumps(kline_data)};
        const TOTAL_BARS = KLINE_DATA.length;  // 160
        const HISTORY_BARS = CONFIG.historyBars;  // 60
        const PLAY_BARS = CONFIG.playBars;  // 100

        let state = {{ currentBar: HISTORY_BARS - 1, positionDirection: 'none', positionAmount: 0, positionAvgPrice: 0, realizedProfit: 0, maxProfit: 0, maxDrawdown: 0, had30Loss: false, tradeCount: 0, trades: [], isGameOver: false, currentPrice: 0, playedBars: 0, lotSize: 1 }};
        const maxPosition = CONFIG.leverage === 10 ? CONFIG.capital * 0.5 : CONFIG.capital;

        // 音效系统
        const audioCtx = new (window.AudioContext || window.webkitAudioContext)();

        function playOpenSound() {{
            // 开仓音效：短促的"叮"声
            const osc = audioCtx.createOscillator();
            const gain = audioCtx.createGain();
            osc.connect(gain);
            gain.connect(audioCtx.destination);
            osc.frequency.value = 880;  // A5音
            osc.type = 'sine';
            gain.gain.setValueAtTime(0.3, audioCtx.currentTime);
            gain.gain.exponentialRampToValueAtTime(0.01, audioCtx.currentTime + 0.15);
            osc.start(audioCtx.currentTime);
            osc.stop(audioCtx.currentTime + 0.15);
        }}

        function playCloseSound() {{
            // 平仓音效：金币落下的声音（多个短音）
            const notes = [1318, 1568, 2093];  // E6, G6, C7
            notes.forEach((freq, i) => {{
                setTimeout(() => {{
                    const osc = audioCtx.createOscillator();
                    const gain = audioCtx.createGain();
                    osc.connect(gain);
                    gain.connect(audioCtx.destination);
                    osc.frequency.value = freq;
                    osc.type = 'sine';
                    gain.gain.setValueAtTime(0.25, audioCtx.currentTime);
                    gain.gain.exponentialRampToValueAtTime(0.01, audioCtx.currentTime + 0.2);
                    osc.start(audioCtx.currentTime);
                    osc.stop(audioCtx.currentTime + 0.2);
                }}, i * 80);
            }});
        }}

        // 手数控制
        function changeLot(delta) {{
            state.lotSize = Math.max(1, Math.min(10, state.lotSize + delta));
            document.getElementById('lot-value').textContent = state.lotSize;
        }}

        // 交易记录折叠
        function toggleTradeLog() {{
            const content = document.getElementById('trade-log-content');
            const toggle = document.getElementById('trade-log-toggle');
            if (content.style.display === 'block') {{
                content.style.display = 'none';
                toggle.textContent = '展开 ▼';
            }} else {{
                content.style.display = 'block';
                toggle.textContent = '收起 ▲';
            }}
        }}

        // 手动结束游戏
        function manualEnd() {{
            if (state.isGameOver) return;
            // 平掉所有持仓
            if (state.positionDirection !== 'none') {{
                const price = state.currentPrice;
                let pnlPct = state.positionDirection === 'long' ? (price - state.positionAvgPrice) / state.positionAvgPrice : (state.positionAvgPrice - price) / state.positionAvgPrice;
                state.realizedProfit += state.positionAmount * CONFIG.leverage * pnlPct;
            }}
            showGameOver('manual');
        }}

        // K线图
        const klineContainer = document.getElementById('kline-container');
        const klineChart = LightweightCharts.createChart(klineContainer, {{
            width: klineContainer.clientWidth, height: 480,
            layout: {{ background: {{ type: 'solid', color: '#111827' }}, textColor: '#9ca3af' }},
            grid: {{ vertLines: {{ color: '#1f2937' }}, horzLines: {{ color: '#1f2937' }} }},
            rightPriceScale: {{ borderColor: '#1f2937' }},
            timeScale: {{ borderColor: '#1f2937', visible: false }}
        }});
        const candlestickSeries = klineChart.addCandlestickSeries({{ upColor: '#ef4444', downColor: '#22c55e', borderUpColor: '#ef4444', borderDownColor: '#22c55e', wickUpColor: '#ef4444', wickDownColor: '#22c55e' }});

        // 成交量图
        const volumeContainer = document.getElementById('volume-container');
        const volumeChart = LightweightCharts.createChart(volumeContainer, {{
            width: volumeContainer.clientWidth, height: 50,
            layout: {{ background: {{ type: 'solid', color: '#111827' }}, textColor: '#9ca3af' }},
            grid: {{ vertLines: {{ color: '#1f2937' }}, horzLines: {{ color: '#1f2937' }} }},
            rightPriceScale: {{ borderColor: '#1f2937' }},
            timeScale: {{ borderColor: '#1f2937', visible: false }}
        }});
        const volumeSeries = volumeChart.addHistogramSeries({{ color: '#3b82f6', priceFormat: {{ type: 'volume' }} }});

        // 同步两个图表的时间轴
        klineChart.timeScale().subscribeVisibleLogicalRangeChange((range) => {{
            if (range) volumeChart.timeScale().setVisibleLogicalRange(range);
        }});
        volumeChart.timeScale().subscribeVisibleLogicalRangeChange((range) => {{
            if (range) klineChart.timeScale().setVisibleLogicalRange(range);
        }});

        function formatNum(n, d=0) {{ return n.toLocaleString('zh-CN', {{minimumFractionDigits: d, maximumFractionDigits: d}}); }}

        function updateDisplay() {{
            const price = state.currentPrice;
            document.getElementById('current-price').textContent = formatNum(price, 2);
            let posText = '空仓';
            if (state.positionDirection === 'long' && state.positionAmount > 0) {{
                posText = `多 ${{formatNum(state.positionAmount)}}元 @${{formatNum(state.positionAvgPrice, 2)}}`;
            }} else if (state.positionDirection === 'short' && state.positionAmount > 0) {{
                posText = `空 ${{formatNum(state.positionAmount)}}元 @${{formatNum(state.positionAvgPrice, 2)}}`;
            }}
            document.getElementById('position').textContent = posText;

            let floatingPnl = 0;
            if (state.positionDirection !== 'none' && state.positionAmount > 0 && state.positionAvgPrice > 0) {{
                let pnlPct = state.positionDirection === 'long' ? (price - state.positionAvgPrice) / state.positionAvgPrice : (state.positionAvgPrice - price) / state.positionAvgPrice;
                floatingPnl = state.positionAmount * CONFIG.leverage * pnlPct;
                if (isNaN(floatingPnl)) floatingPnl = 0;
            }}
            const floatingEl = document.getElementById('floating-pnl');
            floatingEl.textContent = (isNaN(floatingPnl) ? '0' : ((floatingPnl >= 0 ? '+' : '') + formatNum(floatingPnl)));
            floatingEl.className = 'info-value ' + (floatingPnl > 0 ? 'positive' : floatingPnl < 0 ? 'negative' : '');

            const totalPnl = state.realizedProfit + floatingPnl;
            const totalEl = document.getElementById('total-pnl');
            totalEl.textContent = (isNaN(totalPnl) ? '0' : ((totalPnl >= 0 ? '+' : '') + formatNum(totalPnl)));
            totalEl.className = 'info-value ' + (totalPnl > 0 ? 'positive' : totalPnl < 0 ? 'negative' : '');

            if (totalPnl > state.maxProfit) state.maxProfit = totalPnl;
            if (state.maxProfit > 0) {{ const dd = (state.maxProfit - totalPnl) / CONFIG.capital; if (dd > state.maxDrawdown) state.maxDrawdown = dd; }}
            if (totalPnl < -CONFIG.capital * 0.3) state.had30Loss = true;

            const tradeAmount = state.lotSize * 1000;
            const canAdd = state.positionAmount + tradeAmount <= maxPosition;
            document.getElementById('btn-buy').disabled = !canAdd || state.positionDirection === 'short';
            document.getElementById('btn-sell').disabled = state.positionDirection !== 'long' || state.positionAmount < tradeAmount;
            document.getElementById('btn-short').disabled = !canAdd || state.positionDirection === 'long';
            document.getElementById('btn-cover').disabled = state.positionDirection !== 'short' || state.positionAmount < tradeAmount;

            if (CONFIG.leverage === 10 && floatingPnl < -CONFIG.capital * 0.7) forceClose();
            return totalPnl;
        }}

        // 存储所有交易标记
        let tradeMarkers = [];

        function addTradeLog(action, price, amount, profit=null) {{
            state.trades.push({{bar: state.currentBar, action, price, amount, profit}});
            state.tradeCount++;
            document.getElementById('trade-count').textContent = state.tradeCount;
            const logEl = document.getElementById('trade-log-content');
            const actionText = {{buy:'买多', sell:'平多', short:'卖空', cover:'平空'}}[action];
            const qty = Math.round(amount / price * 100) / 100;
            const profitText = profit !== null ? ` (${{profit >= 0 ? '+' : ''}}${{formatNum(profit)}})` : '';
            const item = document.createElement('div');
            item.className = 'trade-item';
            item.textContent = `#${{state.playedBars}} ${{actionText}} ${{formatNum(qty, 2)}}股 @${{formatNum(price, 2)}}${{profitText}}`;
            logEl.insertBefore(item, logEl.firstChild);

            // 添加K线图标记
            const markerConfig = {{
                buy: {{ position: 'belowBar', color: '#ef4444', shape: 'arrowUp', text: 'B' }},
                sell: {{ position: 'aboveBar', color: '#22c55e', shape: 'arrowDown', text: 'S' }},
                short: {{ position: 'aboveBar', color: '#8b5cf6', shape: 'arrowDown', text: '空' }},
                cover: {{ position: 'belowBar', color: '#f59e0b', shape: 'arrowUp', text: '平' }}
            }};
            const mc = markerConfig[action];
            tradeMarkers.push({{
                time: state.currentBar,
                position: mc.position,
                color: mc.color,
                shape: mc.shape,
                text: mc.text
            }});
            // 更新标记
            candlestickSeries.setMarkers(tradeMarkers);
        }}

        function doBuy() {{
            if (state.isGameOver) return;
            const price = state.currentPrice;
            const amount = state.lotSize * 1000;
            if (state.positionDirection === 'none') {{ 
                state.positionDirection = 'long'; 
                state.positionAmount = amount; 
                state.positionAvgPrice = price; 
            }} else if (state.positionDirection === 'long') {{ 
                const totalCost = state.positionAmount * state.positionAvgPrice + amount * price; 
                state.positionAmount += amount; 
                state.positionAvgPrice = totalCost / state.positionAmount; 
            }}
            playOpenSound();
            addTradeLog('buy', price, amount); 
            updateDisplay();
        }}

        function doSell() {{
            if (state.isGameOver || state.positionDirection !== 'long') return;
            const price = state.currentPrice;
            const amount = Math.min(state.lotSize * 1000, state.positionAmount);
            const pnlPct = (price - state.positionAvgPrice) / state.positionAvgPrice;
            const realized = amount * CONFIG.leverage * pnlPct;
            state.realizedProfit += realized;
            state.positionAmount -= amount;
            if (state.positionAmount <= 0) {{ 
                state.positionDirection = 'none'; 
                state.positionAmount = 0; 
                state.positionAvgPrice = 0; 
            }}
            playCloseSound();
            addTradeLog('sell', price, amount, realized);
            updateDisplay();
        }}

        function doShort() {{
            if (state.isGameOver) return;
            const price = state.currentPrice;
            const amount = state.lotSize * 1000;
            if (state.positionDirection === 'none') {{ 
                state.positionDirection = 'short'; 
                state.positionAmount = amount; 
                state.positionAvgPrice = price; 
            }} else if (state.positionDirection === 'short') {{ 
                const totalCost = state.positionAmount * state.positionAvgPrice + amount * price; 
                state.positionAmount += amount; 
                state.positionAvgPrice = totalCost / state.positionAmount; 
            }}
            playOpenSound();
            addTradeLog('short', price, amount); 
            updateDisplay();
        }}

        function doCover() {{
            if (state.isGameOver || state.positionDirection !== 'short') return;
            const price = state.currentPrice;
            const amount = Math.min(state.lotSize * 1000, state.positionAmount);
            const pnlPct = (state.positionAvgPrice - price) / state.positionAvgPrice;
            const realized = amount * CONFIG.leverage * pnlPct;
            state.realizedProfit += realized;
            state.positionAmount -= amount;
            if (state.positionAmount <= 0) {{ 
                state.positionDirection = 'none'; 
                state.positionAmount = 0; 
                state.positionAvgPrice = 0; 
            }}
            playCloseSound();
            addTradeLog('cover', price, amount, realized);
            updateDisplay();
        }}

        function forceClose() {{
            state.isGameOver = true;
            document.getElementById('force-close-alert').style.display = 'block';
            if (state.positionDirection !== 'none') {{
                const price = state.currentPrice;
                let pnlPct = state.positionDirection === 'long' ? (price - state.positionAvgPrice) / state.positionAvgPrice : (state.positionAvgPrice - price) / state.positionAvgPrice;
                state.realizedProfit += state.positionAmount * CONFIG.leverage * pnlPct;
                state.positionDirection = 'none'; state.positionAmount = 0;
            }}
            setTimeout(() => showGameOver('强制平仓'), 1500);
        }}

        function showGameOver(reason) {{
            state.isGameOver = true;
            const totalPnl = state.realizedProfit;
            const profitRate = totalPnl / CONFIG.capital;
            document.getElementById('game-over-title').textContent = reason === '强制平仓' ? '强制平仓' : '游戏结束';
            document.getElementById('reveal-name').textContent = CONFIG.symbolName;
            document.getElementById('reveal-info').textContent = (CONFIG.symbolType === 'stock' ? '股票' : '期货') + ' · ' + CONFIG.symbol;
            const profitEl = document.getElementById('final-profit');
            profitEl.textContent = (totalPnl >= 0 ? '+' : '') + formatNum(totalPnl);
            profitEl.className = 'result-value ' + (totalPnl > 0 ? 'positive' : totalPnl < 0 ? 'negative' : '');
            const rateEl = document.getElementById('final-rate');
            const profitRatePct = profitRate * 100;
            rateEl.textContent = (profitRatePct >= 0 ? '+' : '') + profitRatePct.toFixed(1) + '%';
            rateEl.className = 'result-value ' + (profitRatePct > 0 ? 'positive' : profitRatePct < 0 ? 'negative' : '');
            document.getElementById('final-trades').textContent = state.tradeCount;
            document.getElementById('game-over').style.display = 'flex';

            // 调用API结算
            document.getElementById('settle-status').textContent = '正在结算...';
            document.getElementById('settle-status').style.color = '#f59e0b';
            document.getElementById('settle-btn').disabled = true;

            settleGame(totalPnl, profitRate, state.tradeCount, state.maxDrawdown, state.had30Loss)
                .then(result => {{
                    if (result && result.success) {{
                        document.getElementById('settle-status').innerHTML = '✓ 结算成功！<br><span style="font-size:12px;color:#6b7280;">请刷新页面查看最新资金</span>';
                        document.getElementById('settle-status').style.color = '#22c55e';
                        document.getElementById('settle-btn').textContent = '刷新页面';
                        document.getElementById('settle-btn').disabled = false;
                        document.getElementById('settle-btn').onclick = function() {{
                            try {{ window.top.location.reload(); }} catch(e) {{ alert('请按 F5 刷新页面'); }}
                        }};
                    }} else {{
                        // API失败，使用URL参数方式
                        document.getElementById('settle-status').textContent = '请点击按钮完成结算';
                        document.getElementById('settle-status').style.color = '#f59e0b';
                        document.getElementById('settle-btn').textContent = '手动结算';
                        document.getElementById('settle-btn').disabled = false;
                        document.getElementById('settle-btn').onclick = function() {{ doSettle(); }};
                    }}
                }});
        }}

        // 备用：URL参数结算
        function doSettle() {{
            const params = new URLSearchParams({{
                game_done: '1',
                profit: Math.round(state.realizedProfit).toString(),
                rate: (state.realizedProfit / CONFIG.capital).toFixed(4),
                trades: state.tradeCount.toString(),
                drawdown: state.maxDrawdown.toFixed(4),
                had_loss: state.had30Loss ? '1' : '0',
                game_id: CONFIG.gameId.toString(),
                symbol: CONFIG.symbol,
                symbol_name: CONFIG.symbolName,
                symbol_type: CONFIG.symbolType,
                capital: CONFIG.capital.toString()
            }});
            let baseUrl = '/K线训练';
            try {{ baseUrl = window.top.location.href.split('?')[0]; }} catch(e) {{}}
            const url = baseUrl + '?' + params.toString();

            try {{
                window.top.location.href = url;
            }} catch(e) {{
                window.open(url, '_blank');
                document.getElementById('settle-status').textContent = '已在新标签页打开';
            }}
        }}

        function advanceBar() {{
            if (state.isGameOver) return;

            state.currentBar++;
            state.playedBars++;

            // 播放了100根后游戏结束
            if (state.playedBars >= PLAY_BARS || state.currentBar >= TOTAL_BARS) {{
                // 游戏结束，自动平仓
                if (state.positionDirection !== 'none') {{
                    const price = state.currentPrice;
                    let pnlPct = state.positionDirection === 'long' ? (price - state.positionAvgPrice) / state.positionAvgPrice : (state.positionAvgPrice - price) / state.positionAvgPrice;
                    state.realizedProfit += state.positionAmount * CONFIG.leverage * pnlPct;
                }}
                showGameOver('completed');
                return;
            }}

            // 更新图表
            const visibleData = KLINE_DATA.slice(0, state.currentBar + 1).map((d, i) => ({{ time: i, open: d.open, high: d.high, low: d.low, close: d.close }}));
            const volumeData = KLINE_DATA.slice(0, state.currentBar + 1).map((d, i) => ({{ time: i, value: d.volume || 0, color: d.close >= d.open ? 'rgba(239,68,68,0.5)' : 'rgba(34,197,94,0.5)' }}));
            candlestickSeries.setData(visibleData);
            volumeSeries.setData(volumeData);
            // 重新设置交易标记（setData后标记会被清除）
            if (tradeMarkers.length > 0) {{
                candlestickSeries.setMarkers(tradeMarkers);
            }}
            // 滚动到最右边显示最新K线
            klineChart.timeScale().scrollToPosition(0, false);
            volumeChart.timeScale().scrollToPosition(0, false);

            state.currentPrice = KLINE_DATA[state.currentBar].close || 0;
            document.getElementById('progress-text').textContent = `第 ${{state.playedBars}} / ${{PLAY_BARS}} 根`;
            document.getElementById('progress-fill').style.width = (state.playedBars / PLAY_BARS * 100) + '%';
            updateDisplay();
            setTimeout(advanceBar, CONFIG.speed);
        }}

        // 初始化：显示60根历史K线
        function initChart() {{
            const initialData = KLINE_DATA.slice(0, HISTORY_BARS).map((d, i) => ({{ time: i, open: d.open, high: d.high, low: d.low, close: d.close }}));
            const initialVolume = KLINE_DATA.slice(0, HISTORY_BARS).map((d, i) => ({{ time: i, value: d.volume || 0, color: d.close >= d.open ? 'rgba(239,68,68,0.5)' : 'rgba(34,197,94,0.5)' }}));
            candlestickSeries.setData(initialData);
            volumeSeries.setData(initialVolume);
            // 让图表自适应内容
            klineChart.timeScale().fitContent();
            volumeChart.timeScale().fitContent();
            state.currentPrice = KLINE_DATA[HISTORY_BARS - 1].close || 0;
            document.getElementById('progress-text').textContent = `第 0 / ${{PLAY_BARS}} 根`;
            document.getElementById('progress-fill').style.width = '0%';
            updateDisplay();
            // 2秒后开始自动播放
            setTimeout(advanceBar, 2000);
        }}

        initChart();

        window.addEventListener('resize', () => {{ 
            klineChart.applyOptions({{ width: klineContainer.clientWidth }}); 
            volumeChart.applyOptions({{ width: volumeContainer.clientWidth }}); 
        }});
    </script>
</body>
</html>
'''

    # 使用components.html渲染游戏
    components.html(game_html, height=820, scrolling=True)

    # 提示信息
    st.markdown("""
    <div style="text-align: center; padding: 8px; color: #6b7280; font-size: 12px;">
        游戏结束后会自动结算，按提示刷新页面即可
    </div>
    """, unsafe_allow_html=True)

# ==========================================
# 阶段3：结果页面
# ==========================================
elif st.session_state.kline_game_phase == 'result':

    user_capital = kg.get_user_capital(user_id)
    result = st.session_state.get('game_result') or {}

    profit = result.get('profit', 0) or 0
    profit_color = '#ef4444' if profit > 0 else '#22c55e' if profit < 0 else '#e5e7eb'
    symbol_type_text = '股票' if result.get('symbol_type') == 'stock' else '期货'
    profit_rate = (result.get('profit_rate', 0) or 0) * 100

    st.markdown("<h2 style='text-align: center; color: #e5e7eb;'>游戏结束</h2>", unsafe_allow_html=True)

    # 揭晓品种
    st.markdown(f"""
    <div style="text-align: center; margin: 30px 0;">
        <div style="background: #111827; border: 1px solid #1f2937; border-radius: 4px; padding: 20px; display: inline-block;">
            <div style="font-size: 14px; color: #6b7280; margin-bottom: 4px;">揭晓品种</div>
            <div style="font-size: 28px; font-weight: 600; color: #e5e7eb;">{result.get('symbol_name', '???')}</div>
            <div style="font-size: 13px; color: #6b7280;">{symbol_type_text} · {result.get('symbol', '')}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # 结果统计
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(f"""
        <div style="background: #111827; border: 1px solid #1f2937; border-radius: 4px; padding: 20px; text-align: center;">
            <div style="font-size: 13px; color: #6b7280; margin-bottom: 4px;">本局盈亏</div>
            <div style="font-size: 28px; font-weight: 600; color: {profit_color};">{profit:+,.0f}</div>
            <div style="font-size: 13px; color: #6b7280;">{profit_rate:+.1f}%</div>
        </div>
        """, unsafe_allow_html=True)
    with col2:
        st.markdown(f"""
        <div style="background: #111827; border: 1px solid #1f2937; border-radius: 4px; padding: 20px; text-align: center;">
            <div style="font-size: 13px; color: #6b7280; margin-bottom: 4px;">账户余额</div>
            <div style="font-size: 28px; font-weight: 600; color: #e5e7eb;">{user_capital:,.0f}</div>
        </div>
        """, unsafe_allow_html=True)
    with col3:
        st.markdown(f"""
        <div style="background: #111827; border: 1px solid #1f2937; border-radius: 4px; padding: 20px; text-align: center;">
            <div style="font-size: 13px; color: #6b7280; margin-bottom: 4px;">交易次数</div>
            <div style="font-size: 28px; font-weight: 600; color: #e5e7eb;">{result.get('trade_count', 0)}</div>
        </div>
        """, unsafe_allow_html=True)

    # 显示新解锁的成就
    new_achievements = result.get('new_achievements', [])
    if new_achievements:
        st.markdown("<br>", unsafe_allow_html=True)
        achievement_html = '<div style="text-align: center; background: rgba(34, 197, 94, 0.1); border: 1px solid rgba(34, 197, 94, 0.3); border-radius: 4px; padding: 16px;">'
        achievement_html += '<div style="font-size: 16px; color: #86efac; margin-bottom: 8px;">解锁新成就！</div>'
        for ach in new_achievements:
            achievement_html += f'<span style="display:inline-block;background:linear-gradient(135deg,#3b82f6,#8b5cf6);color:white;padding:4px 12px;border-radius:4px;font-size:13px;margin:4px;">{ach["name"]} (+{ach["exp"]}经验)</span>'
        achievement_html += '</div>'
        st.markdown(achievement_html, unsafe_allow_html=True)

    st.markdown(
        f"<div style='text-align: center; color: #6b7280; font-size: 14px; margin: 20px 0;'>获得 {kg.BASE_EXP_PER_GAME} 基础经验值</div>",
        unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        if st.button("再来一局", type="primary", use_container_width=True):
            st.session_state.kline_game_phase = 'setup'
            st.session_state.kline_game_id = None
            st.session_state.game_result = None
            st.rerun()