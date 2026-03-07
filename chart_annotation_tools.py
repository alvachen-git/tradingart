"""K线形态注释图工具（富标注版）

每根关键K线真实着色（非叠加背景）+ 气泡框标注含具体价格数值。

Tool A: draw_pattern_annotation_chart - 形态标注图
Tool B: draw_forecast_chart           - 关键价位预测图
"""

import os
import re
import pandas as pd
import plotly.graph_objects as go
from langchain_core.tools import tool
from dotenv import load_dotenv
from sqlalchemy import create_engine
import symbol_map
from symbol_match import strict_futures_prefix_pattern
from kline_algo import calculate_kline_signals
from plot_tools import save_chart_as_json

load_dotenv(override=True)


# --- DB setup ---
def _get_engine():
    db_url = (f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
              f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}")
    return create_engine(db_url, pool_recycle=3600, pool_pre_ping=True)

_engine = _get_engine()


# --- 颜色常量 ---
BG_COLOR       = "#0f172a"
UP_COLOR       = "#ef4444"   # 普通阳线红
DOWN_COLOR     = "#22c55e"   # 普通阴线绿
MA5_COLOR      = "#60a5fa"
MA20_COLOR     = "#f59e0b"

# 角色颜色
ROLE_COLOR = {
    "swing_low": "#fbbf24",   # 金：摆动低点
    "break":     "#f97316",   # 橙：破底
    "flip":      "#818cf8",   # 紫：翻
    "bullish":   "#4ade80",   # 亮绿：多头信号K线
    "bearish":   "#f87171",   # 亮红：空头信号K线
    "star":      "#fbbf24",   # 金：星体
    "neutral":   "#94a3b8",   # 灰：十字星等中性
    "highlight": "#a78bfa",   # 淡紫：通用高亮
}

ROLE_LABEL = {
    "swing_low": "摆动低点",
    "break":     "破底",
    "flip":      "翻",
    "bullish":   "多头信号",
    "bearish":   "空头信号",
    "star":      "星体",
    "neutral":   "中性K线",
    "highlight": "形态K线",
}


# ==========================================
#  内部辅助：数据获取
# ==========================================

def _fetch_kline(query: str, trade_date: str = None):
    """返回 (df, symbol, asset_type)，df 按 trade_date 升序，最多60根"""
    result = symbol_map.resolve_symbol(query)
    if not result or isinstance(result, str):
        return None, None, None

    symbol, asset_type = result
    date_condition = ""
    if trade_date:
        clean_date = trade_date.replace("-", "").replace("/", "")
        date_condition = f"AND trade_date <= '{clean_date}'"

    df = pd.DataFrame()

    if asset_type == 'stock':
        sql = f"""SELECT trade_date, open_price, high_price, low_price, close_price
                  FROM stock_price WHERE ts_code='{symbol}' {date_condition}
                  ORDER BY trade_date DESC LIMIT 60"""
        df = pd.read_sql(sql, _engine)
        if df.empty:
            sql2 = f"""SELECT trade_date, open_price, high_price, low_price, close_price
                       FROM index_price WHERE ts_code='{symbol}' {date_condition}
                       ORDER BY trade_date DESC LIMIT 60"""
            df = pd.read_sql(sql2, _engine)
            if not df.empty:
                asset_type = 'index'

    elif asset_type == 'future':
        has_month = bool(re.search(r'\d{2,4}', symbol))
        if has_month:
            sql = f"""SELECT trade_date, open_price, high_price, low_price, close_price
                      FROM futures_price
                      WHERE UPPER(ts_code) LIKE '{symbol.upper()}%%' AND ts_code NOT LIKE '%%TAS%%'
                      {date_condition} ORDER BY trade_date DESC LIMIT 60"""
            df = pd.read_sql(sql, _engine)
        else:
            clean_sym = ''.join(c for c in symbol if not c.isdigit())
            pattern = strict_futures_prefix_pattern(clean_sym)
            sql_main = f"""SELECT ts_code FROM futures_price
                           WHERE UPPER(ts_code) REGEXP '{pattern}'
                             AND ts_code NOT LIKE '%%TAS%%'
                             AND ts_code REGEXP '[0-9]{{4}}$'
                           ORDER BY trade_date DESC, oi DESC LIMIT 1"""
            df_main = pd.read_sql(sql_main, _engine)
            if df_main.empty:
                return None, symbol, asset_type
            main_contract = df_main.iloc[0]['ts_code']
            sql = f"""SELECT trade_date, open_price, high_price, low_price, close_price
                      FROM futures_price WHERE ts_code='{main_contract}' {date_condition}
                      ORDER BY trade_date DESC LIMIT 60"""
            df = pd.read_sql(sql, _engine)

    elif asset_type == 'index':
        sql = f"""SELECT trade_date, open_price, high_price, low_price, close_price
                  FROM index_price WHERE ts_code='{symbol}' {date_condition}
                  ORDER BY trade_date DESC LIMIT 60"""
        df = pd.read_sql(sql, _engine)

    if df.empty:
        return None, symbol, asset_type

    df = df.sort_values('trade_date').reset_index(drop=True)
    for col in ['open_price', 'high_price', 'low_price', 'close_price']:
        df[col] = pd.to_numeric(df[col])
    # 将日期格式 "20251203" 统一转为 "2025-12-03"（Plotly 日期轴要求 ISO 格式）
    df['trade_date'] = df['trade_date'].astype(str).str.replace(
        r'^(\d{4})(\d{2})(\d{2})$', r'\1-\2-\3', regex=True
    )
    return df, symbol, asset_type


# ==========================================
#  内部辅助：图表构建
# ==========================================

def _build_colored_kline(df: pd.DataFrame, role_map: dict, title: str) -> go.Figure:
    """
    K线着色方案：
    - 普通K线：ONE go.Candlestick trace（标准红/绿，最可靠）
    - 特殊角色K线：fig.add_shape 手动画实体矩形+影线（彻底避免多trace category轴渲染问题）
    role_map: {df_integer_index: role_name}
    """
    df = df.copy()
    df['MA5']  = df['close_price'].rolling(5).mean()
    df['MA20'] = df['close_price'].rolling(20).mean()
    n = len(df)
    all_dates = df['trade_date'].astype(str).tolist()
    special_indices = set(role_map.keys())

    fig = go.Figure()

    # 1. 所有普通K线用一个 Candlestick trace（特殊bar位置用 float('nan') 留空）
    o = [float(df.iloc[i]['open_price'])  if i not in special_indices else float('nan') for i in range(n)]
    h = [float(df.iloc[i]['high_price'])  if i not in special_indices else float('nan') for i in range(n)]
    l = [float(df.iloc[i]['low_price'])   if i not in special_indices else float('nan') for i in range(n)]
    c = [float(df.iloc[i]['close_price']) if i not in special_indices else float('nan') for i in range(n)]

    fig.add_trace(go.Candlestick(
        x=all_dates, open=o, high=h, low=l, close=c,
        increasing_line_color=UP_COLOR,   decreasing_line_color=DOWN_COLOR,
        increasing_fillcolor=UP_COLOR,    decreasing_fillcolor=DOWN_COLOR,
        name='K线', showlegend=False,
    ))

    # 2. 特殊角色K线：用 add_shape 手动画（日期轴下 x 用日期字符串 ±偏移量）
    seen_roles = set()
    for idx in sorted(special_indices):
        role  = role_map[idx]
        color = ROLE_COLOR.get(role, '#818cf8')
        row   = df.iloc[idx]
        open_ = float(row['open_price'])
        close_= float(row['close_price'])
        high_ = float(row['high_price'])
        low_  = float(row['low_price'])
        body_lo = min(open_, close_)
        body_hi = max(open_, close_)
        date_str = str(row['trade_date'])  # "YYYY-MM-DD"
        ts = pd.Timestamp(date_str)
        # ±10小时：约等于 K线实体宽度的 40%（与 Plotly 默认蜡烛宽度匹配）
        x0_str = (ts - pd.Timedelta(hours=10)).isoformat()
        x1_str = (ts + pd.Timedelta(hours=10)).isoformat()

        # 实体（doji 时画横线，否则画矩形）
        if abs(body_hi - body_lo) < 1e-9:
            fig.add_shape(type='line',
                          x0=x0_str, x1=x1_str,
                          y0=body_lo, y1=body_lo,
                          line=dict(color=color, width=2),
                          xref='x', yref='y')
        else:
            fig.add_shape(type='rect',
                          x0=x0_str, x1=x1_str,
                          y0=body_lo, y1=body_hi,
                          fillcolor=color, line_color=color,
                          xref='x', yref='y', layer='above')

        # 上影线
        if high_ > body_hi:
            fig.add_shape(type='line',
                          x0=date_str, x1=date_str, y0=body_hi, y1=high_,
                          line=dict(color=color, width=1.5),
                          xref='x', yref='y')
        # 下影线
        if body_lo > low_:
            fig.add_shape(type='line',
                          x0=date_str, x1=date_str, y0=low_, y1=body_lo,
                          line=dict(color=color, width=1.5),
                          xref='x', yref='y')

        # 图例（每种角色只加一次 dummy trace）
        if role not in seen_roles:
            seen_roles.add(role)
            fig.add_trace(go.Scatter(
                x=[None], y=[None], mode='markers',
                marker=dict(color=color, size=10, symbol='square'),
                name=ROLE_LABEL.get(role, role),
                showlegend=True,
            ))

    # 3. MA5 / MA20
    fig.add_trace(go.Scatter(
        x=all_dates, y=df['MA5'],
        mode='lines', line=dict(color=MA5_COLOR, width=1.5), name='MA5'
    ))
    fig.add_trace(go.Scatter(
        x=all_dates, y=df['MA20'],
        mode='lines', line=dict(color=MA20_COLOR, width=1.5), name='MA20'
    ))

    # 4. Layout
    fig.update_layout(
        title=dict(text=title, font=dict(color='white', family='Microsoft YaHei', size=15)),
        paper_bgcolor=BG_COLOR,
        plot_bgcolor=BG_COLOR,
        xaxis=dict(
            type='date',
            showgrid=False,
            tickfont=dict(color='#94a3b8', size=9),
            tickangle=-45,
            nticks=12,
            rangeslider=dict(visible=False),
            rangebreaks=[dict(bounds=["sat", "mon"])],  # 跳过周末
        ),
        yaxis=dict(showgrid=True, gridcolor='#1e293b', tickfont=dict(color='#94a3b8')),
        font=dict(family='Microsoft YaHei', color='white'),
        margin=dict(l=50, r=60, t=60, b=70),
        legend=dict(
            font=dict(color='white'),
            bgcolor='rgba(15,23,42,0.85)',
            bordercolor='#334155',
            borderwidth=1,
            x=1.01, y=1, xanchor='left',
        ),
    )
    return fig


def _add_callout(fig: go.Figure, x: str, y: float, text: str, color: str,
                 ay: int = -50, ax: int = 30, yanchor: str = 'bottom') -> None:
    """统一气泡框标注：带彩色边框 + 深色背景 + 箭头"""
    fig.add_annotation(
        x=x, y=y,
        text=text.replace('\n', '<br>'),
        font=dict(color=color, size=11, family='Microsoft YaHei'),
        showarrow=True,
        arrowcolor=color,
        arrowwidth=1.5,
        arrowhead=2,
        ay=ay, ax=ax,
        yanchor=yanchor,
        bgcolor='rgba(15,23,42,0.88)',
        bordercolor=color,
        borderwidth=1.5,
        borderpad=6,
        align='left',
    )


def _apply_shapes(fig: go.Figure, shape_specs: list) -> None:
    """批量将辅助线/矩形应用到图表"""
    for spec in shape_specs:
        stype = spec.pop('type')
        if stype == 'hline':
            fig.add_hline(**spec)
        elif stype == 'hrect':
            fig.add_hrect(**spec)
        # 用完后不需要还原 spec，本函数调用一次


# ==========================================
#  各形态标注函数
#  统一返回: (role_map, callout_specs, shape_specs)
# ==========================================

def _annotate_pdt(df: pd.DataFrame):
    """破底翻：摆动低点(金) / 破底(橙) / 翻(紫) + 支撑水平线"""
    n = len(df)
    dates = df['trade_date'].astype(str).tolist()
    role_map, callout_specs, shape_specs = {}, [], []

    # 重新定位摆动低点
    _side = 2
    win_start = max(0, n - 32)
    _win = df.iloc[win_start:-2]['low_price'].values
    sup, sup_idx = None, None
    for _i in range(_side, len(_win) - _side):
        _v = _win[_i]
        if (all(_win[_i - j] > _v for j in range(1, _side + 1)) and
                all(_win[_i + j] > _v for j in range(1, _side + 1))):
            sup = _v
            sup_idx = win_start + _i

    if sup is None:
        return {}, [], []

    curr  = df.iloc[-1]
    prev  = df.iloc[-2]
    pprev = df.iloc[-3] if n >= 3 else None

    # 摆动低点（金色）
    role_map[sup_idx] = 'swing_low'

    # 支撑水平线
    shape_specs.append({
        'type': 'hline',
        'y': sup,
        'line_dash': 'dot',
        'line_color': ROLE_COLOR['swing_low'],
        'line_width': 1.5,
        'annotation_text': f"支撑 {sup:.2f}",
        'annotation_font_color': ROLE_COLOR['swing_low'],
        'annotation_position': 'top right',
    })

    # 破底K线（橙色）
    three_bar = (pprev is not None
                 and pprev['close_price'] < sup
                 and prev['close_price'] < sup
                 and curr['close_price'] > sup)
    if three_bar:
        role_map[n - 3] = 'break'
        role_map[n - 2] = 'break'
        break_pct = (sup - prev['close_price']) / sup * 100
        callout_specs.append({
            'x': dates[-2], 'y': prev['low_price'],
            'text': f"昨收 {prev['close_price']:.2f}\n< 支撑 {sup:.2f}\n跌破 {break_pct:.1f}%",
            'color': ROLE_COLOR['break'], 'ay': 55, 'ax': -30, 'yanchor': 'top',
        })
    else:
        role_map[n - 2] = 'break'
        break_pct = (sup - prev['close_price']) / sup * 100
        callout_specs.append({
            'x': dates[-2], 'y': prev['low_price'],
            'text': f"昨收 {prev['close_price']:.2f}\n< 支撑 {sup:.2f}\n跌破 {break_pct:.1f}%",
            'color': ROLE_COLOR['break'], 'ay': 55, 'ax': -30, 'yanchor': 'top',
        })

    # 翻K线（紫色）
    role_map[n - 1] = 'flip'
    callout_specs.append({
        'x': dates[-1], 'y': curr['high_price'],
        'text': f"今收 {curr['close_price']:.2f}\n> 支撑 {sup:.2f}\n破底翻确认↑",
        'color': ROLE_COLOR['flip'], 'ay': -55, 'ax': 30, 'yanchor': 'bottom',
    })

    return role_map, callout_specs, shape_specs


def _annotate_engulfing(df: pd.DataFrame, bullish: bool):
    """多头/空头吞噬"""
    n = len(df)
    dates = df['trade_date'].astype(str).tolist()
    prev, curr = df.iloc[-2], df.iloc[-1]

    if bullish:
        role_map = {n - 2: 'bearish', n - 1: 'bullish'}
        callout_specs = [{
            'x': dates[-1], 'y': curr['high_price'],
            'text': (f"今开 {curr['open_price']:.2f} < 昨收 {prev['close_price']:.2f}\n"
                     f"今收 {curr['close_price']:.2f} > 昨高 {prev['high_price']:.2f}\n"
                     f"多头吞噬确认↑"),
            'color': ROLE_COLOR['bullish'], 'ay': -55, 'ax': 30, 'yanchor': 'bottom',
        }]
    else:
        role_map = {n - 2: 'bullish', n - 1: 'bearish'}
        callout_specs = [{
            'x': dates[-1], 'y': curr['low_price'],
            'text': (f"今开 {curr['open_price']:.2f} > 昨收 {prev['close_price']:.2f}\n"
                     f"今收 {curr['close_price']:.2f} < 昨低 {prev['low_price']:.2f}\n"
                     f"空头吞噬确认↓"),
            'color': ROLE_COLOR['bearish'], 'ay': 55, 'ax': -30, 'yanchor': 'top',
        }]

    return role_map, callout_specs, []


def _annotate_morning_star(df: pd.DataFrame, is_morning: bool):
    """晨星 / 夜星"""
    n = len(df)
    if n < 3:
        return {}, [], []
    dates = df['trade_date'].astype(str).tolist()
    b3, b2, b1 = df.iloc[-3], df.iloc[-2], df.iloc[-1]

    rng = b2['high_price'] - b2['low_price']
    star_body_pct = (abs(b2['close_price'] - b2['open_price']) / rng * 100) if rng > 0 else 0

    if is_morning:
        role_map = {n - 3: 'bearish', n - 2: 'star', n - 1: 'bullish'}
        callout_specs = [
            {
                'x': dates[-2], 'y': b2['low_price'],
                'text': f"★ 星体\n实体比 {star_body_pct:.0f}%",
                'color': ROLE_COLOR['star'], 'ay': 40, 'ax': 0, 'yanchor': 'top',
            },
            {
                'x': dates[-1], 'y': b1['high_price'],
                'text': f"晨星确认↑\n今收 {b1['close_price']:.2f}",
                'color': ROLE_COLOR['bullish'], 'ay': -45, 'ax': 30, 'yanchor': 'bottom',
            },
        ]
    else:
        role_map = {n - 3: 'bullish', n - 2: 'star', n - 1: 'bearish'}
        callout_specs = [
            {
                'x': dates[-2], 'y': b2['high_price'],
                'text': f"★ 星体\n实体比 {star_body_pct:.0f}%",
                'color': ROLE_COLOR['star'], 'ay': -40, 'ax': 0, 'yanchor': 'bottom',
            },
            {
                'x': dates[-1], 'y': b1['low_price'],
                'text': f"夜星确认↓\n今收 {b1['close_price']:.2f}",
                'color': ROLE_COLOR['bearish'], 'ay': 45, 'ax': -30, 'yanchor': 'top',
            },
        ]

    return role_map, callout_specs, []


def _annotate_box_breakout(df: pd.DataFrame, box_high: float, box_low: float,
                            is_breakout: bool):
    """箱体突破 / 跌破"""
    n = len(df)
    dates = df['trade_date'].astype(str).tolist()
    curr = df.iloc[-1]

    if is_breakout:
        role_map = {n - 1: 'bullish'}
        pct = (curr['close_price'] - box_high) / box_high * 100
        callout_specs = [{
            'x': dates[-1], 'y': curr['high_price'],
            'text': f"收 {curr['close_price']:.2f}\n突破箱顶 {box_high:.2f}  +{pct:.1f}%",
            'color': ROLE_COLOR['bullish'], 'ay': -55, 'ax': 30, 'yanchor': 'bottom',
        }]
        shape_specs = [
            {'type': 'hline', 'y': box_high, 'line_dash': 'dot',
             'line_color': '#60a5fa', 'line_width': 1,
             'annotation_text': f"箱顶 {box_high:.2f}",
             'annotation_font_color': '#60a5fa', 'annotation_position': 'top left'},
            {'type': 'hline', 'y': box_low, 'line_dash': 'dot',
             'line_color': '#60a5fa', 'line_width': 1,
             'annotation_text': f"箱底 {box_low:.2f}",
             'annotation_font_color': '#60a5fa', 'annotation_position': 'bottom left'},
        ]
    else:
        role_map = {n - 1: 'bearish'}
        pct = (box_low - curr['close_price']) / box_low * 100
        callout_specs = [{
            'x': dates[-1], 'y': curr['low_price'],
            'text': f"收 {curr['close_price']:.2f}\n跌破箱底 {box_low:.2f}  -{pct:.1f}%",
            'color': ROLE_COLOR['bearish'], 'ay': 55, 'ax': -30, 'yanchor': 'top',
        }]
        shape_specs = [
            {'type': 'hline', 'y': box_high, 'line_dash': 'dot',
             'line_color': '#60a5fa', 'line_width': 1,
             'annotation_text': f"箱顶 {box_high:.2f}",
             'annotation_font_color': '#60a5fa', 'annotation_position': 'top left'},
            {'type': 'hline', 'y': box_low, 'line_dash': 'dot',
             'line_color': '#60a5fa', 'line_width': 1,
             'annotation_text': f"箱底 {box_low:.2f}",
             'annotation_font_color': '#60a5fa', 'annotation_position': 'bottom left'},
        ]

    return role_map, callout_specs, shape_specs


def _annotate_generic(df: pd.DataFrame, pattern_name: str):
    """通用：高亮最后一根 + 价格涨跌幅气泡"""
    n = len(df)
    dates = df['trade_date'].astype(str).tolist()
    curr, prev = df.iloc[-1], df.iloc[-2]
    chg = (curr['close_price'] - prev['close_price']) / prev['close_price'] * 100

    role_map = {n - 1: 'highlight'}
    callout_specs = [{
        'x': dates[-1], 'y': curr['high_price'],
        'text': f"{pattern_name}\n收 {curr['close_price']:.2f}  {chg:+.1f}%",
        'color': ROLE_COLOR['highlight'], 'ay': -45, 'ax': 20, 'yanchor': 'bottom',
    }]
    return role_map, callout_specs, []


# ==========================================
#  Tool A：形态标注图
# ==========================================

@tool
def draw_pattern_annotation_chart(query: str, trade_date: str = None) -> str:
    """
    AI识别到K线形态后，生成带真实着色 + 价格气泡标注的K线图。
    适用：用户说"画出今日形态"、"图解一下这个信号"，或你识别到明显形态时主动调用。
    无形态时不生成图，直接返回文字说明。

    Args:
        query: 品种名称，如"豆粕"、"螺纹钢"、"沪深300"
        trade_date: 截止日期（可选），格式"YYYY-MM-DD"或"YYYYMMDD"

    Returns:
        形态摘要 + IMAGE_CREATED:{filename}（若有形态）
    """
    df, symbol, asset_type = _fetch_kline(query, trade_date)
    if df is None or df.empty:
        return f"未找到 {query} 的K线数据，无法生成形态图。"
    if len(df) < 10:
        return f"{query} 数据不足（{len(df)}根），无法识别形态。"

    signals = calculate_kline_signals(df)
    patterns = signals.get('patterns', [])
    if not patterns:
        last_price = df['close_price'].iloc[-1]
        return f"今日暂无明显形态信号，不生成图表。（{query} 当前价：{last_price:.2f}）"

    primary = patterns[0]

    # 路由到对应标注函数
    if "破底翻" in primary:
        role_map, callouts, shapes = _annotate_pdt(df)
        if not role_map:  # 摆动低点未找到时降级
            role_map, callouts, shapes = _annotate_generic(df, primary)

    elif "多头吞噬" in primary:
        role_map, callouts, shapes = _annotate_engulfing(df, bullish=True)

    elif "空头吞噬" in primary:
        role_map, callouts, shapes = _annotate_engulfing(df, bullish=False)

    elif "晨星" in primary:
        role_map, callouts, shapes = _annotate_morning_star(df, is_morning=True)

    elif "夜星" in primary:
        role_map, callouts, shapes = _annotate_morning_star(df, is_morning=False)

    elif "突破" in primary or "跌破" in primary or "破位" in primary:
        m = re.search(r'(\d+)', primary)
        period = min(int(m.group(1)) if m else 20, len(df) - 2)
        box = df.iloc[-(period + 1):-1]
        box_high, box_low = box['high_price'].max(), box['low_price'].min()
        is_up = "跌破" not in primary and "破位" not in primary
        role_map, callouts, shapes = _annotate_box_breakout(df, box_high, box_low, is_up)

    else:
        role_map, callouts, shapes = _annotate_generic(df, primary)

    # 构建图表
    title = f"{query} · {primary}"
    fig = _build_colored_kline(df, role_map, title)

    # 应用辅助线
    _apply_shapes(fig, shapes)

    # 应用气泡标注
    for c in callouts:
        _add_callout(fig, **c)

    # 多形态：左下角列出所有信号
    if len(patterns) > 1:
        fig.add_annotation(
            xref='paper', yref='paper',
            x=0.01, y=0.01,
            text="全部信号：" + " | ".join(patterns),
            font=dict(color='#94a3b8', size=10, family='Microsoft YaHei'),
            showarrow=False,
            bgcolor='rgba(15,23,42,0.75)',
            xanchor='left', yanchor='bottom',
        )

    filename = save_chart_as_json(fig, f"pattern_{query}_{primary}")
    last_price = df['close_price'].iloc[-1]
    return (
        f"**{query}** 检测到形态：**{primary}**\n"
        f"当前价格：{last_price:.2f}\n"
        f"全部信号：{', '.join(patterns)}\n\n"
        f"IMAGE_CREATED:{filename}"
    )


# ==========================================
#  Tool B：关键价位预测图
# ==========================================

@tool
def draw_forecast_chart(
    query: str,
    support: float = None,
    resistance: float = None,
    target: float = None,
    note: str = "",
    trade_date: str = None,
) -> str:
    """
    AI完成技术分析后，将支撑/压力/目标价画在K线图上，配气泡标注（行情预测可视化）。
    适用：用户说"螺纹钢支撑3700压力3900"、"帮我画出关键价位"，或你做完分析后主动可视化结论。

    Args:
        query:      品种名称，如"螺纹钢"、"黄金"
        support:    支撑价位（绿色虚线，可选）
        resistance: 压力/阻力价位（红色虚线，可选）
        target:     目标价位（紫色点线，可选）
        note:       分析摘要，显示在图右上角（可选）
        trade_date: 截止日期（可选）

    Returns:
        摘要文字 + IMAGE_CREATED:{filename}
    """
    df, symbol, asset_type = _fetch_kline(query, trade_date)
    if df is None or df.empty:
        return f"未找到 {query} 的K线数据，无法生成预测图。"

    if all(x is None for x in [support, resistance, target]):
        return "请至少提供支撑、压力或目标价中的一个价位。"

    last_price = df['close_price'].iloc[-1]
    title = f"{query} · 关键价位  当前 {last_price:.2f}"

    # 空 role_map → 全部正常红绿
    fig = _build_colored_kline(df, {}, title)
    dates = df['trade_date'].astype(str).tolist()

    # 支撑线（绿）
    if support is not None:
        fig.add_hline(
            y=support, line_dash="dash", line_color="#22c55e", line_width=2,
        )
        diff_pct = (last_price - support) / support * 100
        _add_callout(fig,
                     x=dates[-1], y=support,
                     text=f"支撑 {support}\n距今 {diff_pct:+.1f}%",
                     color="#22c55e", ay=40, ax=40, yanchor='top')

    # 压力线（红）
    if resistance is not None:
        fig.add_hline(
            y=resistance, line_dash="dash", line_color="#ef4444", line_width=2,
        )
        diff_pct = (resistance - last_price) / last_price * 100
        _add_callout(fig,
                     x=dates[-1], y=resistance,
                     text=f"压力 {resistance}\n距今 +{diff_pct:.1f}%",
                     color="#ef4444", ay=-40, ax=40, yanchor='bottom')

    # 目标价（紫）
    if target is not None:
        fig.add_hline(
            y=target, line_dash="dot", line_color="#818cf8", line_width=2,
        )
        diff_pct = (target - last_price) / last_price * 100
        _add_callout(fig,
                     x=dates[-1], y=target,
                     text=f"目标 {target}\n空间 {diff_pct:+.1f}%",
                     color="#818cf8", ay=-40, ax=-40, yanchor='bottom')

    # 分析摘要（右上角）
    if note:
        fig.add_annotation(
            xref='paper', yref='paper',
            x=0.99, y=0.99,
            text=note.replace('\n', '<br>'),
            font=dict(color='#94a3b8', size=11, family='Microsoft YaHei'),
            showarrow=False, align='right',
            bgcolor='rgba(15,23,42,0.85)',
            bordercolor='#334155', borderwidth=1,
            xanchor='right', yanchor='top',
        )

    filename = save_chart_as_json(fig, f"forecast_{query}")
    lines_info = " | ".join(filter(None, [
        f"支撑：{support}" if support is not None else "",
        f"压力：{resistance}" if resistance is not None else "",
        f"目标：{target}" if target is not None else "",
    ]))
    return (
        f"**{query}** 关键价位图已生成\n"
        f"当前价格：{last_price:.2f} | {lines_info}\n\n"
        f"IMAGE_CREATED:{filename}"
    )
