import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import statsmodels.api as sm_api
import tushare as ts
import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# 1. 环境初始化
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
try:
    import symbol_map as sym_map
except:
    sym_map = None

load_dotenv(override=True)
ts.set_token(os.getenv("TUSHARE_TOKEN"))
try:
    pro = ts.pro_api()
except:
    st.error("请配置 TUSHARE_TOKEN")
    st.stop()


# 初始化数据库连接 (懒加载)
def get_db_engine():
    try:
        db_url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        return create_engine(db_url)
    except:
        return None


st.set_page_config(
    page_title="爱波塔-对冲分析",
    page_icon="favicon.ico",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 2. 样式升级：全中文沉浸式暗黑风 (字体优化版)
st.markdown("""
<style>
    /* 全局背景：深邃午夜蓝 */
    .stApp { 
        background-color: #0f172a; 
        color: #e2e8f0;
        font-family: 'PingFang SC', 'Microsoft YaHei', sans-serif;
    }

    /* 隐藏顶部白条 */
    header[data-testid="stHeader"] {
        background-color: #0f172a !important;
    }

    /* 侧边栏 */
    section[data-testid="stSidebar"] { 
        background-color: #020617; 
        border-right: 1px solid #1e293b;
    }
    section[data-testid="stSidebar"] * { color: #94a3b8 !important; }

    /* 输入框样式 */
    div[data-testid="stTextInput"] input, 
    div[data-testid="stNumberInput"] input, 
    div[data-testid="stSelectbox"] > div > div {
        background-color: #1e293b; 
        color: #f8fafc;
        border: 1px solid #334155;
        border-radius: 4px;
        height: 42px;
    }
    div[data-testid="stTextInput"] label,
    div[data-testid="stNumberInput"] label,
    div[data-testid="stSelectbox"] label {
        color: #cbd5e1 !important; 
        font-size: 14px;
        font-weight: 500;
    }

    /* 按钮样式 */
    div.stButton > button {
        background: linear-gradient(180deg, #3b82f6, #2563eb);
        color: white;
        border: none;
        border-radius: 4px;
        height: 42px;
        font-weight: 600;
        letter-spacing: 1px;
        box-shadow: 0 4px 6px -1px rgba(59, 130, 246, 0.5);
        transition: all 0.2s;
    }
    div.stButton > button:hover {
        background: linear-gradient(180deg, #60a5fa, #3b82f6);
        box-shadow: 0 0 15px rgba(59, 130, 246, 0.7);
    }

    /* --- 指标卡片 (Metrics) 核心优化 --- */
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
        padding: 18px;
        border-radius: 8px;
        border: 1px solid #475569;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.5), 0 0 20px rgba(59, 130, 246, 0.1);
        height: 100%; /* 强制等高 */
    }

    /* 【超强选择器】指标标题：覆盖所有可能的元素 */
    div[data-testid="stMetricLabel"],
    div[data-testid="stMetricLabel"] *,
    div[data-testid="stMetric"] label,
    div[data-testid="stMetric"] label *,
    div[data-testid="stMetric"] [class*="label"],
    div[data-testid="stMetric"] [class*="label"] *,
    div[data-testid="stMetric"] > div > div:first-child,
    div[data-testid="stMetric"] > div > div:first-child * { 
        color: #60a5fa !important; 
        font-size: 18px !important; 
        font-weight: 800 !important;
        text-shadow: 
            0 0 15px rgba(96, 165, 250, 1),
            0 0 25px rgba(96, 165, 250, 0.8),
            0 0 35px rgba(96, 165, 250, 0.6),
            0 3px 6px rgba(0, 0, 0, 0.9),
            0 0 3px rgba(255, 255, 255, 1) !important;
        letter-spacing: 1px !important;
        text-transform: none !important;
        filter: brightness(1.4) contrast(1.5) !important;
        background: transparent !important;
        -webkit-text-stroke: 0.3px rgba(255, 255, 255, 0.3) !important;
    }

    /* 指标数值：纯白发光效果 */
    div[data-testid="stMetricValue"],
    div[data-testid="stMetricValue"] *,
    div[data-testid="stMetric"] [data-testid="stMetricValue"],
    div[data-testid="stMetric"] [data-testid="stMetricValue"] * { 
        color: #ffffff !important; 
        font-family: 'Roboto Mono', monospace !important; 
        font-size: 34px !important; 
        font-weight: 700 !important;
        text-shadow: 
            0 0 10px rgba(255, 255, 255, 0.8),
            0 0 20px rgba(255, 255, 255, 0.6),
            0 3px 6px rgba(0, 0, 0, 0.9) !important;
        filter: brightness(1.4) !important;
        background: transparent !important;
    }

    /* 标题与分割线 */
    h1 { color: #f8fafc; letter-spacing: 1px; }
    hr { border-color: #334155; }

    /* 自定义容器样式 */
    .finance-card {
        background-color: #1e293b;
        border: 1px solid #334155;
        border-radius: 6px;
        padding: 20px;
        margin-bottom: 20px;
    }
    .card-header {
        color: #60a5fa;
        font-size: 15px;
        font-weight: 700;
        letter-spacing: 1px;
        margin-bottom: 15px;
        border-bottom: 1px solid #334155;
        padding-bottom: 8px;
    }
    .highlight-val { color: #fbbf24; font-weight: bold; font-family: 'Roboto Mono'; }
</style>
""", unsafe_allow_html=True)


# 3. 辅助逻辑
def ensure_suffix(code):
    if '.' in code: return code
    if code in ['IM', 'IC', 'IF', 'IH']: return code
    if code.startswith(('60', '68', '900', '588', '510')): return f"{code}.SH"
    if code.startswith(('00', '30', '159', '399')): return f"{code}.SZ"
    return code


def get_benchmark_data_enhanced(benchmark_code, s_date, e_date, futures_code=None):
    try:
        df_bench = pro.index_daily(ts_code=benchmark_code, start_date=s_date, end_date=e_date)
        if not df_bench.empty:
            return df_bench
    except:
        pass

    if futures_code:
        engine = get_db_engine()
        if engine:
            s_date_dash = f"{s_date[:4]}-{s_date[4:6]}-{s_date[6:]}"
            e_date_dash = f"{e_date[:4]}-{e_date[4:6]}-{e_date[6:]}"

            variants = [futures_code, f"{futures_code}.CFE", f"{futures_code}L"]

            for code_variant in variants:
                try:
                    sql = text(f"""
                        SELECT * FROM futures_price 
                        WHERE ts_code = :fcode 
                        AND (trade_date BETWEEN :sdate AND :edate 
                             OR trade_date BETWEEN :sdate_dash AND :edate_dash)
                        ORDER BY trade_date ASC
                    """)

                    with engine.connect() as conn:
                        result = conn.execute(sql, {
                            'fcode': code_variant,
                            'sdate': s_date, 'edate': e_date,
                            'sdate_dash': s_date_dash, 'edate_dash': e_date_dash
                        })
                        df_db = pd.DataFrame(result.fetchall(), columns=result.keys())

                    if not df_db.empty:
                        cols = [c.lower() for c in df_db.columns]
                        if 'close' not in cols and 'close_price' in df_db.columns:
                            df_db.rename(columns={'close_price': 'close'}, inplace=True)
                        if 'settle' in df_db.columns and 'close' not in df_db.columns:
                            df_db.rename(columns={'settle': 'close'}, inplace=True)

                        df_db['ts_code'] = benchmark_code

                        if 'trade_date' in df_db.columns and 'close' in df_db.columns:
                            df_db['trade_date'] = pd.to_datetime(df_db['trade_date'])
                            return df_db[['trade_date', 'ts_code', 'close']]

                except Exception as e:
                    continue

    return pd.DataFrame()


@st.cache_data(ttl=3600)
def calculate_beta_metrics(stock_code, benchmark_idx_code, benchmark_etf_code, futures_code, days):
    try:
        stock_code = ensure_suffix(stock_code)

        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=int(days * 2 + 60))
        s_date, e_date = start_dt.strftime('%Y%m%d'), end_dt.strftime('%Y%m%d')

        try:
            df_stock = ts.pro_bar(ts_code=stock_code, adj='qfq', start_date=s_date, end_date=e_date)
        except:
            df_stock = None

        if df_stock is None or df_stock.empty:
            df_stock = pro.daily(ts_code=stock_code, start_date=s_date, end_date=e_date)
            is_adjusted = False
        else:
            is_adjusted = True

        df_bench_idx = get_benchmark_data_enhanced(benchmark_idx_code, s_date, e_date, futures_code)

        has_etf_data = False
        if benchmark_etf_code:
            df_bench_etf = pro.fund_daily(ts_code=benchmark_etf_code, start_date=s_date, end_date=e_date)
            if not df_bench_etf.empty:
                has_etf_data = True
        else:
            df_bench_etf = pd.DataFrame()

        stock_name = stock_code
        try:
            info = pro.stock_basic(ts_code=stock_code)
            if not info.empty: stock_name = info.iloc[0]['name']
        except:
            pass

        if df_bench_idx.empty:
            return None, f"基准数据缺失: {benchmark_idx_code}。"
        if df_stock is None or df_stock.empty:
            return None, f"个股数据缺失: {stock_code}"

        df_stock['trade_date'] = pd.to_datetime(df_stock['trade_date']).dt.normalize()
        df_bench_idx['trade_date'] = pd.to_datetime(df_bench_idx['trade_date']).dt.normalize()

        df = pd.merge(df_stock[['trade_date', 'close']], df_bench_idx[['trade_date', 'close']],
                      on='trade_date', suffixes=('_s', '_b_idx'), how='inner')

        if has_etf_data:
            df_bench_etf['trade_date'] = pd.to_datetime(df_bench_etf['trade_date']).dt.normalize()
            df = pd.merge(df, df_bench_etf[['trade_date', 'close']], on='trade_date', how='left')
            df.rename(columns={'close': 'close_etf'}, inplace=True)
        else:
            df['close_etf'] = np.nan

        df = df.sort_values('trade_date').tail(days)

        if len(df) < 5:
            return None, f"有效重叠数据不足 (仅 {len(df)} 天)。"

        df['stock_ret'] = df['close_s'].astype(float).pct_change()
        df['bench_ret'] = df['close_b_idx'].astype(float).pct_change()
        df.dropna(inplace=True)

        if len(df) < 5: return None, "计算收益率后数据不足"

        X = sm_api.add_constant(df['bench_ret'])
        model = sm_api.OLS(df['stock_ret'], X).fit()

        var_99 = np.percentile(df['bench_ret'], 1)

        return {
            'beta': model.params['bench_ret'],
            'alpha_annual': model.params['const'] * 250 * 100,
            'r2': model.rsquared,
            'data': df,
            'name': stock_name,
            'etf_price': df['close_etf'].iloc[-1] if has_etf_data else 0,
            'idx_price': df['close_b_idx'].iloc[-1],
            'var_99': var_99,
            'is_adjusted': is_adjusted,
            'has_etf_data': has_etf_data
        }, None
    except Exception as e:
        return None, str(e)


# 4. 页面主体
st.title("Beta对冲分析")

# --- 参数栏 ---
with st.container():
    c1, c2, c3, c4, c5 = st.columns([1.5, 1.5, 1, 1, 1], vertical_alignment="bottom")

    with c1:
        s_input = st.text_input("持仓资产 / 代码", value="300750", help="请输入股票代码")
        target = s_input
        if sym_map:
            p, _ = sym_map.resolve_symbol(s_input)
            if p and '.' in p: target = p

    with c2:
        # 中文映射
        bench_map = {
            "000300.SH": ("沪深300ETF)", "510300.SH", 300, True, "IF"),
            "000016.SH": ("上证50ETF)", "510050.SH", 300, True, "IH"),
            "000905.SH": ("中证500ETF)", "510500.SH", 200, True, "IC"),
            "000688.SH": ("科创50ETF)", "588000.SH", 10000, True, None),
            "399006.SZ": ("创业板ETF)", "159915.SZ", 10000, True, None)
        }

        bench_idx = st.selectbox(
            "对冲基准 (Benchmark)",
            list(bench_map.keys()),
            format_func=lambda x: bench_map[x][0]
        )

        bench_info = bench_map[bench_idx]
        bench_etf_code = bench_info[1]
        fut_multiplier = bench_info[2]
        has_options = bench_info[3]
        futures_code = bench_info[4]

    with c3:
        win = st.number_input("回测窗口 (天)", 30, 250, 60, step=10)
    with c4:
        pos = st.number_input("持仓市值 (万元)", 10, 500000, 100, step=10)
    with c5:
        run = st.button("开始分析", use_container_width=True)

# --- 结果 ---
if run or target:
    st.write("")
    res, err = calculate_beta_metrics(target, bench_idx, bench_etf_code, futures_code, win)

    if err:
        st.error(f"❌ 计算失败: {err}")
    elif res:
        beta = res['beta']
        r2 = res['r2']
        alpha = res['alpha_annual']
        etf_price = res['etf_price']
        idx_price = res['idx_price']
        var_99 = res['var_99']

        # A. 核心指标区 (使用自定义 HTML 以确保样式生效)
        c_m1, c_m2, c_m3 = st.columns(3)
        with c_m1:
            st.markdown(f"""
            <div style="
                background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
                padding: 18px;
                border-radius: 8px;
                border: 1px solid #475569;
                box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.5), 0 0 20px rgba(59, 130, 246, 0.1);
                min-height: 120px;
            ">
                <div style="
                    color: #60a5fa;
                    font-size: 18px;
                    font-weight: 800;
                    margin-bottom: 12px;
                    text-shadow: 
                        0 0 15px rgba(96, 165, 250, 1),
                        0 0 25px rgba(96, 165, 250, 0.8),
                        0 3px 6px rgba(0, 0, 0, 0.9);
                    letter-spacing: 1px;
                    filter: brightness(1.4) contrast(1.5);
                ">
                    Beta 系数 (敏感度)
                </div>
                <div style="
                    color: #ffffff;
                    font-size: 34px;
                    font-weight: 700;
                    font-family: 'Roboto Mono', monospace;
                    text-shadow: 
                        0 0 10px rgba(255, 255, 255, 0.8),
                        0 0 20px rgba(255, 255, 255, 0.6),
                        0 3px 6px rgba(0, 0, 0, 0.9);
                    filter: brightness(1.4);
                ">
                    {beta:.2f}
                </div>
            </div>
            """, unsafe_allow_html=True)
            with st.expander("ℹ️ 说明", expanded=False):
                st.caption(
                    "【数值含义说明】\n\n"
                    "• Beta > 1.0 (进攻型): 波动比大盘大。例如 Beta=1.5，大盘涨 1%，组合通常涨 1.5%。\n"
                    "• Beta = 1.0 (平衡型): 与大盘同涨同跌。\n"
                    "• Beta < 1.0 (防守型): 波动比大盘小。例如 Beta=0.8，大盘跌 1%，组合通常只跌 0.8%。\n"
                    "• Beta < 0   (反向型): 走势与大盘相反。\n\n"
                    "此系数直接决定了您需要做空多少手期货来对冲风险。"
                )
        with c_m2:
            st.markdown(f"""
            <div style="
                background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
                padding: 18px;
                border-radius: 8px;
                border: 1px solid #475569;
                box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.5), 0 0 20px rgba(59, 130, 246, 0.1);
                min-height: 120px;
            ">
                <div style="
                    color: #60a5fa;
                    font-size: 18px;
                    font-weight: 800;
                    margin-bottom: 12px;
                    text-shadow: 
                        0 0 15px rgba(96, 165, 250, 1),
                        0 0 25px rgba(96, 165, 250, 0.8),
                        0 3px 6px rgba(0, 0, 0, 0.9);
                    letter-spacing: 1px;
                    filter: brightness(1.4) contrast(1.5);
                ">
                    R² 拟合优度
                </div>
                <div style="
                    color: #ffffff;
                    font-size: 34px;
                    font-weight: 700;
                    font-family: 'Roboto Mono', monospace;
                    text-shadow: 
                        0 0 10px rgba(255, 255, 255, 0.8),
                        0 0 20px rgba(255, 255, 255, 0.6),
                        0 3px 6px rgba(0, 0, 0, 0.9);
                    filter: brightness(1.4);
                ">
                    {r2:.1%}
                </div>
            </div>
            """, unsafe_allow_html=True)
            with st.expander("ℹ️ 说明", expanded=False):
                st.caption(
                    "代表对冲策略的【可靠程度】。\n\n"
                    "• > 60%: 强相关，用该指数对冲非常有效。\n"
                    "• 30% - 60%: 中等相关，对冲有一点效果，但会有残差风险。\n"
                    "• < 30%: 弱相关，个股走势非常独立，不建议使用该指数进行对冲。"
                )
        with c_m3:
            st.markdown(f"""
            <div style="
                background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
                padding: 18px;
                border-radius: 8px;
                border: 1px solid #475569;
                box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.5), 0 0 20px rgba(59, 130, 246, 0.1);
                min-height: 120px;
            ">
                <div style="
                    color: #60a5fa;
                    font-size: 18px;
                    font-weight: 800;
                    margin-bottom: 12px;
                    text-shadow: 
                        0 0 15px rgba(96, 165, 250, 1),
                        0 0 25px rgba(96, 165, 250, 0.8),
                        0 3px 6px rgba(0, 0, 0, 0.9);
                    letter-spacing: 1px;
                    filter: brightness(1.4) contrast(1.5);
                ">
                    年化 Alpha (超额)
                </div>
                <div style="
                    color: #ffffff;
                    font-size: 34px;
                    font-weight: 700;
                    font-family: 'Roboto Mono', monospace;
                    text-shadow: 
                        0 0 10px rgba(255, 255, 255, 0.8),
                        0 0 20px rgba(255, 255, 255, 0.6),
                        0 3px 6px rgba(0, 0, 0, 0.9);
                    filter: brightness(1.4);
                ">
                    {alpha:.1f}%
                </div>
            </div>
            """, unsafe_allow_html=True)
            with st.expander("ℹ️ 说明", expanded=False):
                st.caption(
                    "【超额收益能力】\n\n"
                    "这是剔除了大盘涨跌影响后，纯粹由选股能力带来的年化回报。\n"
                    "• 正数 (+): 跑赢大盘\n"
                    "• 负数 (-): 跑输大盘"
                )

        st.write("")

        # B. 对冲逻辑区
        col_main, col_chart = st.columns([1.4, 1.6])

        with col_main:
            st.markdown('<div>', unsafe_allow_html=True)

            hedge_val = pos * beta
            st.markdown(f'<div class="card-header">智能对冲策略 (Hedging Strategy)</div>', unsafe_allow_html=True)
            st.markdown(f"""
            <div style="display:flex; justify-content:space-between; margin-bottom:15px; color:#94a3b8; font-size:14px;">
                <span>目标对冲敞口 (Target Exposure)</span>
                <span class="highlight-val">¥ {hedge_val:.1f} 万</span>
            </div>
            """, unsafe_allow_html=True)

            # 1. 只有期货的情况
            if not has_options:
                st.info(f"📉 **仅支持期货对冲 ({bench_info[4]})**")

                # 期货张数
                qty_fut = (hedge_val * 10000) / (idx_price * fut_multiplier)

                st.write(f"建议做空手数:")
                st.markdown(
                    f"<span style='font-size:24px; color:#ef4444; font-weight:bold;'>{qty_fut:.1f}</span> <span style='color:#64748b'>手</span>",
                    unsafe_allow_html=True)
                st.caption(f"基于指数点位 {idx_price:.0f} × 合约乘数 {fut_multiplier}")

            else:
                # 2. 支持期权的情况
                tab1, tab2, tab3 = st.tabs(["🔒 完全对冲", "🦢 黑天鹅防御", "💰 备兑增厚"])

                if etf_price > 0:
                    contract_unit = 10000
                    contract_face = etf_price * contract_unit / 10000  # 单张市值(万)
                    qty_atm = hedge_val / contract_face

                    with tab1:  # 完全对冲
                        st.write("买入平值认沽 (ATM Put) 锁定下行风险。")
                        st.markdown(
                            f"<span style='font-size:24px; color:#3b82f6; font-weight:bold;'>{qty_atm:.0f}</span> <span style='color:#64748b'>张</span>",
                            unsafe_allow_html=True)
                        st.caption(f"行权价: {etf_price:.3f} (平值)")

                    with tab2:  # 黑天鹅
                        var_percent = abs(var_99)
                        strike_var = etf_price * (1 - var_percent)
                        otm_mult = 3.0  # 默认3倍
                        qty_otm = qty_atm * otm_mult

                        st.write(f"买入深虚值认沽 (防范 99% VaR 极端暴跌)。")
                        st.markdown(
                            f"<span style='font-size:24px; color:#ef4444; font-weight:bold;'>{qty_otm:.0f}</span> <span style='color:#64748b'>张</span>",
                            unsafe_allow_html=True)
                        st.caption(f"行权价: {strike_var:.3f} (-{var_percent:.1%}) | 建议倍数: {otm_mult}x")

                    with tab3:  # 备兑
                        strike_call = etf_price * 1.05
                        st.write("卖出虚值认购 (OTM Call) 收取权利金。")
                        st.markdown(
                            f"<span style='font-size:24px; color:#a855f7; font-weight:bold;'>{qty_atm:.0f}</span> <span style='color:#64748b'>张</span>",
                            unsafe_allow_html=True)
                        st.caption(f"行权价: {strike_call:.3f} (+5%)")
                else:
                    st.error("无法获取 ETF 价格，暂不支持期权计算")

            st.markdown('</div>', unsafe_allow_html=True)

        # C. 拟合图
        with col_chart:
            st.markdown('<div>', unsafe_allow_html=True)
            st.markdown('<div class="card-header">回归分析 (Regression Analysis)</div>', unsafe_allow_html=True)

            if not res['data'].empty:
                # 使用 Plotly 深色模板
                fig = px.scatter(
                    res['data'], x='bench_ret', y='stock_ret',
                    labels={'bench_ret': "基准涨跌幅 %", 'stock_ret': "持仓涨跌幅 %"},
                    template="plotly_dark", opacity=0.7
                )

                # 调整点的颜色
                fig.update_traces(marker=dict(size=8, color='#38bdf8', line=dict(width=1, color='#0f172a')))

                x_range = np.linspace(res['data']['bench_ret'].min(), res['data']['bench_ret'].max(), 100)
                y_pred = (res['alpha_annual'] / (250 * 100)) + beta * x_range

                fig.add_traces(go.Scatter(
                    x=x_range, y=y_pred, mode='lines',
                    name=f'Beta={beta:.2f}',
                    line=dict(color='#fbbf24', width=2)
                ))

                if not np.isnan(var_99):
                    fig.add_vline(x=var_99, line_width=1, line_dash="dash", line_color="#ef4444")
                    fig.add_annotation(x=var_99, y=res['data']['stock_ret'].max(), text="VaR 99%", showarrow=False,
                                       font=dict(color="#ef4444"))

                fig.update_layout(
                    margin=dict(l=10, r=10, t=10, b=10),
                    height=320,
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    xaxis=dict(showgrid=True, gridcolor='#334155'),
                    yaxis=dict(showgrid=True, gridcolor='#334155')
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("暂无数据绘图")
            st.markdown('</div>', unsafe_allow_html=True)