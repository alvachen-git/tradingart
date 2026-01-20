import pandas as pd
import re
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from langchain_core.tools import tool
import symbol_map
from sqlalchemy import create_engine, text
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import uuid
import hashlib
import json

# 初始化
load_dotenv(override=True)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CHART_DIR = os.path.join(BASE_DIR, "static", "charts")
os.makedirs(CHART_DIR, exist_ok=True)


def get_db_engine():
    db_url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    return create_engine(db_url, pool_recycle=3600, pool_pre_ping=True)


engine = get_db_engine()

# 数据库表映射
TABLE_MAP = {
    'stock': 'stock_price',
    'index': 'index_price',
    'future': 'futures_price',
}


def _calculate_start_date(period: str):
    """计算起始日期"""
    today = datetime.now()
    period = str(period).lower().strip()
    if not period or period == 'default': return (today - timedelta(days=180)).strftime('%Y%m%d')
    if period == 'ytd': return datetime(today.year, 1, 1).strftime('%Y%m%d')
    if len(period) == 8 and period.isdigit(): return period
    try:
        unit = period[-1]
        val = int(period[:-1])
        if unit == 'd':
            delta = timedelta(days=val)
        elif unit == 'w':
            delta = timedelta(weeks=val)
        elif unit == 'm':
            delta = timedelta(days=val * 30)
        elif unit == 'y':
            delta = timedelta(days=val * 365)
        else:
            delta = timedelta(days=180)
        return (today - delta).strftime('%Y%m%d')
    except:
        return (today - timedelta(days=180)).strftime('%Y%m%d')


def _resolve_symbol_smart(query):
    """
    智能解析：优先拦截股指期货别名，以及常见的指数别名。
    """
    # 1. 清洗输入
    q = query.upper().strip()

    # 2. 定义拦截名单 (Map)
    # 键: 用户可能的输入, 值: (代码, 类型)
    alias_map = {
        # === 股指期货 (原有) ===
        "IF": ("IF", "future"),
        "IC": ("IC", "future"),
        "IH": ("IH", "future"),
        "IM": ("IM", "future"),
        "A50": ("A50", "future"),
        "沪深300股指期货": ("IF", "future"),
        "沪深300期货": ("IF", "future"),
        "中证500股指期货": ("IC", "future"),
        "中证500期货": ("IC", "future"),
        "上证50股指期货": ("IH", "future"),
        "上证50期货": ("IH", "future"),
        "中证1000股指期货": ("IM", "future"),
        "中证1000期货": ("IM", "future"),

        # === 🔥【新增】指数别名 (手动补全) ===
        "沪深300指数": ("000300.SH", "index"),
        "沪深300": ("000300.SH", "index"),
        "300指数": ("000300.SH", "index"),

        "中证500指数": ("000905.SH", "index"),
        "中证500": ("000905.SH", "index"),
        "500指数": ("000905.SH", "index"),

        "上证50指数": ("000016.SH", "index"),
        "上证50": ("000016.SH", "index"),

        "中证1000指数": ("000852.SH", "index"),
        "中证1000": ("000852.SH", "index"),
        "1000指数": ("000852.SH", "index"),
        # === 科技类 ===
        "半导体": ("512480.SH", "stock"),
        "芯片": ("512760.SH", "stock"),  # 或者 159995
        "计算机": ("512720.SH", "stock"),
        "人工智能": ("515250.SH", "stock"),
        "AI": ("515250.SH", "stock"),
        "5G": ("515050.SH", "stock"),
        "通信": ("515050.SH", "stock"),
        "游戏": ("159869.SZ", "stock"),
        "传媒": ("159869.SZ", "stock"),  # 游戏常代表传媒
        "云计算": ("516510.SH", "stock"),
        "大数据": ("515400.SH", "stock"),

        # === 新能源类 ===
        "新能源": ("516160.SH", "stock"),
        "新能车": ("515030.SH", "stock"),
        "电动车": ("515030.SH", "stock"),
        "光伏": ("515790.SH", "stock"),
        "电池": ("159755.SZ", "stock"),
        "锂电池": ("159755.SZ", "stock"),

        # === 消费医药类 ===
        "白酒": ("512690.SH", "stock"),
        "酒": ("512690.SH", "stock"),
        "医药": ("512010.SH", "stock"),
        "医疗": ("512170.SH", "stock"),
        "创新药": ("515080.SH", "stock"),
        "家电": ("159996.SZ", "stock"),
        "养殖": ("159865.SZ", "stock"),
        "猪肉": ("159865.SZ", "stock"),

        # === 周期资源类 ===
        "煤炭": ("515220.SH", "stock"),
        "有色": ("512400.SH", "stock"),
        "钢铁": ("515210.SH", "stock"),
        "化工": ("516220.SH", "stock"),
        "稀土": ("516780.SH", "stock"),

        # === 金融地产类 ===
        "证券": ("512880.SH", "stock"),
        "券商": ("512880.SH", "stock"),
        "银行": ("512800.SH", "stock"),
        "地产": ("512200.SH", "stock"),
        "房地产": ("512200.SH", "stock"),

        # === 跨境/其他 ===
        "红利ETF": ("510880.SH", "stock"),
        "纳指": ("513100.SH", "stock"),
        "纳斯达克ETF": ("513100.SH", "stock"),
        "恒生科技ETF": ("513180.SH", "stock"),
        "港股科技ETF": ("513180.SH", "stock"),
        "中概互联ETF": ("513050.SH", "stock"),
    }

    # 3. 精准匹配
    if q in alias_map:
        return alias_map[q]

    # 4. 模糊匹配 (只针对长中文名)
    # 这里的逻辑是为了防止 "中证500" 匹配到 "中证500期货"
    # 所以我们要小心：如果用户输入 "沪深300"，map里有精准匹配，会在第3步直接返回指数。
    # 如果用户输入 "沪深300期货"，map里也有精准匹配，第3步返回期货。
    # 只有当用户输入 weird 的名字（比如"主力沪深300期货"）才会走到这里。

    for key, val in alias_map.items():
        if len(key) > 3 and key in q:
            return val

    # 5. 放行
    return symbol_map.resolve_symbol(query)

def save_chart_as_json(fig, name):
    """保存图表并返回文件名（检票员模式）"""
    safe_name = hashlib.md5(name.encode()).hexdigest()[:8]
    filename = f"chart_{safe_name}_{uuid.uuid4().hex[:6]}.json"
    filepath = os.path.join(CHART_DIR, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(fig.to_json())

    return filename


# ==========================================
#  🔥 [新增] 数据摘要生成器
# ==========================================
def _generate_data_summary(df, type_name="数据"):
    """
    生成一段给 AI 看的数据分析小抄，包含最新价、极值、涨跌幅等
    """
    try:
        if df.empty: return "数据为空"

        # 1. 基础时间范围
        start_dt = df['trade_date'].min()
        end_dt = df['trade_date'].max()

        summary = f"【{type_name}统计 ({start_dt} - {end_dt})】\n"

        # 2. 根据列名生成不同摘要
        if 'close_price' in df.columns:
            last_close = df['close_price'].iloc[-1]
            first_close = df['close_price'].iloc[0]
            high = df['close_price'].max()
            low = df['close_price'].min()
            change = (last_close - first_close) / first_close * 100

            summary += f"- 最新价: {last_close:.2f}\n"
            summary += f"- 区间涨跌幅: {change:.2f}%\n"
            summary += f"- 最高: {high:.2f}, 最低: {low:.2f}\n"

        if 'oi' in df.columns:  # 持仓量
            last_oi = df['oi'].iloc[-1]
            max_oi = df['oi'].max()
            min_oi = df['oi'].min()
            summary += f"- 最新持仓: {last_oi} 手\n"
            summary += f"- 持仓峰值: {max_oi}, 谷值: {min_oi}\n"

        if 'val' in df.columns:  # 价差/比价
            last_val = df['val'].iloc[-1]
            max_val = df['val'].max()
            min_val = df['val'].min()
            avg_val = df['val'].mean()
            summary += f"- 最新值: {last_val:.2f}\n"
            summary += f"- 均值: {avg_val:.2f}\n"
            summary += f"- 波动范围: {min_val:.2f} ~ {max_val:.2f}\n"

        if 'pe_ttm' in df.columns:
            last_pe = df['pe_ttm'].iloc[-1]
            max_pe = df['pe_ttm'].max()
            min_pe = df['pe_ttm'].min()
            median_pe = df['pe_ttm'].median()

            # 计算当前分位状态
            status = "适中"
            if last_pe < min_pe + (max_pe - min_pe) * 0.2:
                status = "历史低位 (便宜)"
            elif last_pe > min_pe + (max_pe - min_pe) * 0.8:
                status = "历史高位 (贵)"

            summary += f"- 最新PE: {last_pe:.2f} ({status})\n"
            summary += f"- 历史中位数: {median_pe:.2f}\n"
            summary += f"- 极值区间: {min_pe:.2f} ~ {max_pe:.2f}\n"

        return summary
    except Exception as e:
        return f"摘要生成失败: {e}"

# ==========================================
#  📈 图表绘制核心逻辑
# ==========================================

def _fetch_data(ts_code, start_date, asset_type):
    """通用数据获取函数"""
    if asset_type == 'future':
        # 期货特殊处理：尝试匹配主连或具体合约
        base_code = ts_code.upper()
        # 注意：这里增加了 'oi' 字段的查询 (假设数据库列名为 oi 或 open_interest，请根据实际情况调整)
        # 如果您的数据库列名是 'amount' 或其他，请在这里修改
        sql = text(f"""
            SELECT trade_date, open_price, close_price, high_price, low_price, vol, oi 
            FROM futures_price 
            WHERE (ts_code = :c1 OR ts_code = :c2) AND trade_date >= :s_date 
            ORDER BY trade_date ASC
        """)
        df = pd.read_sql(sql, engine, params={"c1": base_code, "c2": f"{base_code}0", "s_date": start_date})
    else:
        # 股票/指数
        table = TABLE_MAP.get(asset_type, 'stock_price')
        sql = text(f"""
            SELECT trade_date, open_price, close_price, high_price, low_price, vol 
            FROM {table} 
            WHERE ts_code = :code AND trade_date >= :s_date 
            ORDER BY trade_date ASC
        """)
        df = pd.read_sql(sql, engine, params={"code": ts_code, "s_date": start_date})

    return df


# ==========================================
#  [新增] 获取期货商持仓数据
# ==========================================
def _fetch_broker_holding(ts_code, broker_name, start_date, holding_type='total'):
    """
    查询指定期货商在某品种上的持仓变化
    holding_type: 'total'(总持仓), 'net'(净持仓), 'long'(多单), 'short'(空单)
    """
    base_code = ts_code.upper()

    # 🔥 根据类型选择查询的字段
    if holding_type == 'net':
        # 净持仓
        select_clause = "net_vol as oi"
    elif holding_type == 'long':
        # 多单
        select_clause = "long_vol as oi"
    elif holding_type == 'short':
        # 空单
        select_clause = "short_vol as oi"
    else:
        # 默认：总持仓 (多 + 空)
        select_clause = "(long_vol + short_vol) as oi"

    sql = text(f"""
        SELECT trade_date, {select_clause}
        FROM futures_holding 
        WHERE (ts_code = :c1 OR ts_code = :c2) 
          AND broker = :broker
          AND trade_date >= :s_date 
        ORDER BY trade_date ASC
    """)

    df = pd.read_sql(sql, engine, params={
        "c1": base_code,
        "c2": f"{base_code}0",
        "broker": broker_name,
        "s_date": start_date
    })

    return df


def _plot_kline(ts_code, name, period, asset_type):
    """画 K 线图"""
    try:
        start_date = _calculate_start_date(period)
        df = _fetch_data(ts_code, start_date, asset_type)

        if df.empty: return f"❌ 未找到 {name} 的数据"

        # K线 + 成交量
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03, row_heights=[0.7, 0.3])
        fig.add_trace(go.Candlestick(
            x=df['trade_date'],
            open=df['open_price'],
            high=df['high_price'],
            low=df['low_price'],
            close=df['close_price'],
            name='K线',
            increasing_line_color='#ef4444',  # 🔴 涨：红色
            decreasing_line_color='#22c55e'  # 🟢 跌：绿色
        ), row=1, col=1)
        colors = ['#ef4444' if c >= o else '#22c55e' for o, c in zip(df['open_price'], df['close_price'])]
        fig.add_trace(go.Bar(x=df['trade_date'], y=df['vol'], name='成交量', marker_color=colors), row=2, col=1)

        fig.update_layout(title=f"{name} K线走势", template="plotly_dark", height=500, xaxis_rangeslider_visible=False,
                          showlegend=False)

        filename = save_chart_as_json(fig, f"kline_{name}")

        # 🔥 [插入] 生成数据摘要
        data_summary = _generate_data_summary(df, f"{name} K线数据")
        return f"{data_summary}\n\nIMAGE_CREATED:{filename}"
    except Exception as e:
        return f"❌ K线绘制失败: {e}"


def _plot_oi_line(ts_code, name, period, asset_type, broker=None, holding_type='total'):
    """
    画持仓量折线图 (支持 总持仓/净持仓/多单/空单)
    """
    try:
        if asset_type != 'future': return "❌ 仅支持期货"
        start_date = _calculate_start_date(period)

        # 定义图表基础信息
        chart_title = ""
        line_name = ""
        color = '#3b82f6'  # 默认蓝

        # 🔥 分支逻辑
        if broker:
            # 1. 查期货商数据
            df = _fetch_broker_holding(ts_code, broker, start_date, holding_type)

            # 根据类型生成标题
            type_map = {
                'net': '净持仓',
                'long': '多单持仓',
                'short': '空单持仓',
                'total': '总持仓(多+空)'
            }
            type_str = type_map.get(holding_type, '总持仓')

            chart_title = f"{broker} 在 {name} 的{type_str}变化"
            line_name = f"{broker}-{type_str}"

            # 不同类型给不同颜色，方便区分
            if holding_type == 'net':
                color = '#8b5cf6'  # 紫色
            elif holding_type == 'long':
                color = '#ef4444'  # 红色
            elif holding_type == 'short':
                color = '#22c55e'  # 绿色
            else:
                color = '#f59e0b'  # 橙色

        else:
            # 2. 查品种总数据 (全市场总持仓没有多空之分，只有总量)
            df = _fetch_data(ts_code, start_date, asset_type)
            chart_title = f"{name} 总持仓量(OI)变化"
            line_name = "全市场总持仓"
            color = '#3b82f6'

        if df.empty or 'oi' not in df.columns: return f"❌ 无数据"

        # 画图
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df['trade_date'], y=df['oi'],
            mode='lines', name=line_name,
            line=dict(color=color, width=2),
            fill='tozeroy'
        ))

        # 如果是净持仓，加一条 0 轴线
        if holding_type == 'net':
            fig.add_hline(y=0, line_dash="dash", line_color="gray")

        fig.update_layout(title=chart_title, template="plotly_dark", height=450)

        # 保存文件
        safe_suffix = f"_{broker}_{holding_type}" if broker else ""
        filename = save_chart_as_json(fig, f"oi_{name}{safe_suffix}")

        # 生成摘要
        data_summary = _generate_data_summary(df, chart_title)

        return f"{data_summary}\n\nIMAGE_CREATED:{filename}"
    except Exception as e:
        return f"❌ 持仓图失败: {e}"


# ==========================================
#  [新增] 获取估值数据
# ==========================================
def _fetch_valuation(ts_code, start_date, asset_type='stock'):
    """从数据库查询 PE/PB 数据"""
    try:
        # 1. 确定查哪张表
        table_name = 'stock_valuation'
        if asset_type == 'index':
            table_name = 'index_valuation'

        # 2. 确定 PE 字段名
        # 股票表里叫 pe_ttm，指数表接口返回 pe 和 pe_ttm
        # 我们统一取 pe_ttm，如果指数没有 ttm 就取 pe
        pe_col = "pe_ttm"

        sql = text(f"""
                SELECT trade_date, {pe_col} as pe_ttm, pb 
                FROM {table_name}
                WHERE ts_code = :code AND trade_date >= :s_date 
                ORDER BY trade_date ASC
            """)

        df = pd.read_sql(sql, engine, params={"code": ts_code, "s_date": start_date})

        # 容错：如果指数表里 pe_ttm 全是 0 或空，尝试用 pe 字段补救 (部分指数可能没有 TTM)
        if asset_type == 'index' and (df.empty or df['pe_ttm'].sum() == 0):
            sql_backup = text(f"""
                    SELECT trade_date, pe as pe_ttm, pb 
                    FROM index_valuation
                    WHERE ts_code = :code AND trade_date >= :s_date 
                    ORDER BY trade_date ASC
                """)
            df = pd.read_sql(sql_backup, engine, params={"code": ts_code, "s_date": start_date})

        return df
    except Exception as e:
        print(f"Valuation fetch error: {e}")
        return pd.DataFrame()


# ==========================================
#  [新增] 绘制 PE 走势图
# ==========================================
def _plot_pe_line(ts_code, name, period, asset_type='stock'):
    """画市盈率(PE-TTM)走势图"""
    try:
        start_date = _calculate_start_date(period)

        # 🔥 传入 asset_type
        df = _fetch_valuation(ts_code, start_date, asset_type)

        if df.empty:
            table = 'index_valuation' if asset_type == 'index' else 'stock_valuation'
            return f"❌ 未找到 {name} ({ts_code}) 的估值数据。\n请检查 `{table}` 表是否有数据。"

        # 过滤掉亏损数据(PE < 0) 或 极端值，避免图形被压缩（可选）
        # 这里只保留 PE > 0 的数据来画图，或者保留原样看亏损
        # df = df[df['pe_ttm'] > 0]

        current_pe = df['pe_ttm'].iloc[-1]
        median_pe = df['pe_ttm'].median()

        # 绘图
        fig = go.Figure()

        # 1. PE 曲线
        fig.add_trace(go.Scatter(
            x=df['trade_date'], y=df['pe_ttm'],
            mode='lines', name='PE-TTM',
            line=dict(color='#f59e0b', width=2),  # 橙色
            fill='tozeroy',  # 填充底部背景，更有分量感
            fillcolor='rgba(245, 158, 11, 0.1)'
        ))

        # 2. 中位数参考线 (虚线)
        fig.add_hline(
            y=median_pe,
            line_dash="dash", line_color="gray", annotation_text=f"中位数: {median_pe:.2f}",
            annotation_position="top right"
        )

        fig.update_layout(
            title=f"{name} 市盈率(PE-TTM) 历史走势",
            template="plotly_dark",
            height=450,
            yaxis_title="市盈率 (倍)"
        )

        filename = save_chart_as_json(fig, f"pe_{name}")

        # 生成摘要
        summary = _generate_data_summary(df, f"{name} 估值数据")
        return f"{summary}\n\nIMAGE_CREATED:{filename}"

    except Exception as e:
        return f"❌ PE图绘制失败: {e}"


def _plot_spread_chart(query, period, mode='diff'):
    """🔥 新增：画价差/比价图"""
    try:
        # 1. 解析两个品种
        codes = query.replace("，", ",").split(",")
        if len(codes) != 2:
            return "❌ 价差分析需要提供两个品种，例如：'RB2410,RB2501'"

        code_a = codes[0].strip()
        code_b = codes[1].strip()

        # 2. 获取数据
        res_a = _resolve_symbol_smart(code_a)
        res_b = _resolve_symbol_smart(code_b)

        if not res_a or not res_b: return f"❌ 无法识别代码: {code_a} 或 {code_b}"

        start_date = _calculate_start_date(period)
        df_a = _fetch_data(res_a[0], start_date, res_a[1])
        df_b = _fetch_data(res_b[0], start_date, res_b[1])

        if df_a.empty or df_b.empty: return "❌ 数据获取失败"

        # 3. 数据对齐 (Inner Join)
        # 只保留两个品种都有交易的日期
        merged = pd.merge(df_a[['trade_date', 'close_price']], df_b[['trade_date', 'close_price']],
                          on='trade_date', suffixes=('_a', '_b'), how='inner')

        if merged.empty: return "❌ 两个品种没有重叠的交易日期"

        # 4. 计算价差或比值
        fig = go.Figure()

        if mode == 'diff':
            merged['val'] = merged['close_price_a'] - merged['close_price_b']
            title = f"价差分析: {code_a} - {code_b}"
            y_label = "价差 (Spread)"
            color = '#f59e0b'  # 橙色
        else:
            merged['val'] = merged['close_price_a'] / merged['close_price_b']
            title = f"比价分析: {code_a} / {code_b}"
            y_label = "比值 (Ratio)"
            color = '#8b5cf6'  # 紫色

        # 画线
        fig.add_trace(go.Scatter(
            x=merged['trade_date'], y=merged['val'],
            mode='lines', name=y_label,
            line=dict(color=color, width=2)
        ))

        # 增加零轴线 (如果是价差)
        if mode == 'diff':
            fig.add_hline(y=0, line_dash="dash", line_color="gray")

        fig.update_layout(title=title, template="plotly_dark", height=450, yaxis_title=y_label)

        filename = save_chart_as_json(fig, f"spread_{code_a}_{code_b}")
        # 🔥 [插入] 生成摘要 (注意这里传入的是 merged 这个 DataFrame)
        data_summary = _generate_data_summary(merged, title)
        return f"{data_summary}\n\nIMAGE_CREATED:{filename}"

    except Exception as e:
        return f"❌ 价差图绘制失败: {e}"


def _plot_stock_comparison(stock_names_str, period):
    """画涨跌幅对比 (柱状图)"""
    try:
        names = stock_names_str.replace("，", ",").split(",")
        data_list = []
        start_date = _calculate_start_date(period)
        for name in names:
            name = name.strip()
            if not name: continue
            res = _resolve_symbol_smart(name)
            if not res or not res[0]: continue
            df = _fetch_data(res[0], start_date, res[1])  # 复用 _fetch_data
            if len(df) > 0:
                pct = (df.iloc[-1]['close_price'] - df.iloc[0]['close_price']) / df.iloc[0]['close_price'] * 100
                data_list.append({"name": name, "pct": pct})

        if not data_list: return "❌ 无有效数据"
        df_res = pd.DataFrame(data_list).sort_values("pct", ascending=False)
        colors = ['#ef4444' if x >= 0 else '#22c55e' for x in df_res['pct']]
        fig = go.Figure(
            go.Bar(x=df_res['name'], y=df_res['pct'], text=[f"{x:.2f}%" for x in df_res['pct']], textposition='auto',
                   marker_color=colors))
        fig.update_layout(title=f"涨跌幅对比", template="plotly_dark", height=400)

        filename = save_chart_as_json(fig, "comparison")
        # 🔥 [插入] 生成简单摘要 (这里逻辑比较特殊，手动拼一个摘要)
        summary = "【涨跌幅排名】\n"
        # 假设您的结果 DataFrame 叫 df_res
        for idx, row in df_res.iterrows():
            summary += f"{row['name']}: {row['pct']:.2f}%\n"
        return f"{summary}\n\nIMAGE_CREATED:{filename}"
    except Exception as e:
        return f"❌ 绘制失败: {e}"


def plot_correlation_scatter(symbol_a, symbol_b, period='1y'):
    """
    绘制两个品种的相关性散点图 (Scatter Plot)
    """
    try:
        # 1. 解析代码
        res_a = symbol_map._resolve_symbol_smart(symbol_a)
        res_b = symbol_map._resolve_symbol_smart(symbol_b)

        if not res_a[0] or not res_b[0]:
            return f"❌ 无法识别品种: {symbol_a} 或 {symbol_b}"

        code_a, type_a = res_a
        code_b, type_b = res_b

        # 2. 获取数据
        start_date = _calculate_start_date(period)

        # 定义内部获取数据的 helper (复用 get_price_data 的逻辑)
        def _fetch(code, asset_type):
            table = TABLE_MAP.get(asset_type, 'stock_price')
            col_code = 'ts_code'
            query = f"SELECT trade_date, close FROM {table} WHERE {col_code}='{code}' AND trade_date>='{start_date}' ORDER BY trade_date"
            return pd.read_sql(query, engine)

        df_a = _fetch(code_a, type_a)
        df_b = _fetch(code_b, type_b)

        if df_a.empty or df_b.empty:
            return "❌ 数据不足，无法分析相关性"

        # 3. 数据对齐 (Merge on Date)
        # 必须确保只保留两天都有交易的日期
        merged_df = pd.merge(df_a, df_b, on='trade_date', suffixes=('_A', '_B'))

        if len(merged_df) < 10:
            return "❌ 有效重叠数据太少，无法计算相关性"

        # 4. 计算相关系数 (Pearson Correlation)
        corr_value = merged_df['close_A'].corr(merged_df['close_B'])

        # 5. 绘图 (散点图 + 趋势线)
        import plotly.express as px

        # 使用 Plotly Express 快速画带有趋势线的散点图
        fig = px.scatter(
            merged_df,
            x='close_A',
            y='close_B',
            trendline="ols",  # 增加线性回归线
            title=f"【{symbol_a} vs {symbol_b}】价格相关性分析 (近{period})",
            hover_data=['trade_date'],
            labels={'close_A': f"{symbol_a} 价格", 'close_B': f"{symbol_b} 价格"}
        )

        # 增加相关系数的标注
        fig.add_annotation(
            xref="paper", yref="paper",
            x=0.05, y=0.95,
            text=f"相关系数 (Correlation): <b>{corr_value:.4f}</b>",
            showarrow=False,
            font=dict(size=16, color="red"),
            bgcolor="rgba(255,255,255,0.8)"
        )

        # 优化样式
        fig.update_layout(
            template="plotly_dark",
            height=600,
            xaxis_title=f"{symbol_a} ({code_a})",
            yaxis_title=f"{symbol_b} ({code_b})"
        )

        # 6. 保存并返回
        filename = f"chart_corr_{uuid.uuid4().hex[:8]}.json"
        with open(os.path.join(CHART_DIR, filename), "w", encoding="utf-8") as f:
            f.write(fig.to_json())

        return f"![Correlation Chart]({filename})\n\nIMAGE_CREATED:{filename}\n\n📊 **分析结果**：\n- **{symbol_a}** 与 **{symbol_b}** 的相关系数为 **{corr_value:.4f}**。\n- 系数越接近 1 代表正相关性越强（同涨同跌）；接近 -1 代表负相关；接近 0 代表无相关。"

    except Exception as e:
        import traceback
        return f"❌ 相关性图表生成失败: {str(e)}"


# ==========================================
#  🔥 新增：饼图/环形图绘制函数
# ==========================================
def _plot_pie_chart(query, title_prefix="资产分布"):
    """
    解析格式: "类别A:100, 类别B:200"
    """
    try:
        # 1. 解析字符串为数据
        # 支持中文冒号和英文冒号，支持逗号分隔
        items = re.split(r'[,，、]', query)
        labels = []
        values = []

        data_for_summary = []

        for item in items:
            if ":" in item or "：" in item:
                # 分割名称和数值
                parts = re.split(r'[:：]', item)
                if len(parts) >= 2:
                    name = parts[0].strip()
                    try:
                        # 提取数字 (支持 30% 或 3000)
                        val_str = parts[1].strip().replace('%', '')
                        val = float(val_str)
                        labels.append(name)
                        values.append(val)
                        data_for_summary.append({"label": name, "value": val})
                    except:
                        continue

        if not labels:
            return "❌ 无法解析数据，请使用格式：'股票:50, 债券:30, 现金:20'"

        # 2. 绘图 (环形图)
        fig = go.Figure(data=[go.Pie(
            labels=labels,
            values=values,
            hole=.4,  # 变成环形图
            textinfo='label+percent',  # 显示标签和百分比
            insidetextorientation='radial'
        )])

        fig.update_layout(
            title=f"{title_prefix}",
            template="plotly_dark",
            height=450
        )

        filename = save_chart_as_json(fig, f"pie_{uuid.uuid4().hex[:6]}")
        summary = _generate_data_summary(data_for_summary, title_prefix)

        return f"{summary}\n\nIMAGE_CREATED:{filename}"

    except Exception as e:
        return f"❌ 饼图绘制失败: {e}"

# ==========================================
#  🛠️ 对外暴露工具 (API 升级)
# ==========================================
@tool
def draw_chart_tool(query: str, chart_type: str = "kline", time_period: str = "6m"):
    """
    【画图工具】支持多种专业图表绘制。

    参数:
    - query:
       * K线/总持仓: '豆粕'
       * 期货商持仓: '豆粕,永安期货' (默认总持仓)
       * 饼图: '股票:60, 债券:40' 或 '腾讯:50, 阿里:30, 茅台:20'
       * 指定持仓类型: '豆粕,永安期货,净持仓' (支持: 净持仓, 多单, 空单)
       * 价差: 'M2505,M2509'
       * 相关性：黄金和白银
    - chart_type: 图表类型
       * 'kline': K线图 (包含成交量)
       * 'line_oi': 持仓量(Open Interest)折线图 (仅期货)
       * 'line_pe': 市盈率PE走势图 (仅股票/指数)
       * 'line_pe': 市盈率PE走势图 (仅股票/指数)
       * 'spread_diff': 价差图 (A - B)
       * 'spread_ratio': 比价图 (A / B)
       * 'pie' (饼图/占比图)
       * 'bar_compare': 多股涨跌幅对比
    - time_period: 时间范围 ('1m', '3m', '6m', '1y', 'ytd')
    """
    # 1. 价差分析
    query = query.replace("：", ":").replace("，", ",").strip()

    if chart_type == 'pie':
        # query 格式预期: "A:10, B:20"
        return _plot_pie_chart(query, title_prefix="占比分析")

    if 'spread' in chart_type:
        return _plot_spread_chart(query, time_period, mode='diff' if 'diff' in chart_type else 'ratio')

    if "相关" in query or "correlation" in query.lower():
        # 尝试提取两个品种，支持逗号、和、与、vs
        # 移除关键词
        clean_q = re.sub(r'(相关性|的|分析|图|画|之间|与|和|vs)', ' ', query)
        parts = re.split(r'[,\s]+', clean_q)
        parts = [p.strip() for p in parts if p.strip()]

        if len(parts) >= 2:
            # 调用刚才新写的散点图函数
            return plot_correlation_scatter(parts[0], parts[1])
        else:
            return "❌ 相关性分析需要两个品种，例如：'黄金 白银 相关性'"

    # 2. 涨跌对比
    if chart_type == 'bar_compare':
        return _plot_stock_comparison(query, time_period)

    # 3. 解析 query
    broker_name = None
    target_name = query
    holding_type = 'total'  # 默认查总持仓

    # 🔥 解析 "品种,期货商,类型"
    if chart_type == 'line_oi' and ("," in query or "，" in query):
        parts = query.replace("，", ",").split(",")

        # 第一部分：品种
        if len(parts) >= 1: target_name = parts[0].strip()

        # 第二部分：期货商
        if len(parts) >= 2: broker_name = parts[1].strip()

        # 第三部分：类型 (净持仓/多单/空单)
        if len(parts) >= 3:
            raw_type = parts[2].strip()
            if "净" in raw_type:
                holding_type = 'net'
            elif "多" in raw_type:
                holding_type = 'long'
            elif "空" in raw_type:
                holding_type = 'short'

    # 4. 解析代码
    res = _resolve_symbol_smart(target_name)
    if not res or not res[0]: return f"❌ 无法识别品种: {target_name}"

    ts_code = res[0]
    asset_type = res[1]

    # 5. 分发任务
    if chart_type == 'kline':
        return _plot_kline(ts_code, target_name, time_period, asset_type)

    elif chart_type == 'line_oi':
        # 🔥 传入解析好的参数
        return _plot_oi_line(ts_code, target_name, time_period, asset_type, broker=broker_name,
                             holding_type=holding_type)
    elif chart_type == 'line_pe':
        if asset_type == 'future': return "❌ 期货没有市盈率数据"
        return _plot_pe_line(ts_code, target_name, time_period, asset_type)

    return f"❌ 不支持: {chart_type}"