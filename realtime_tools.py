import akshare as ak
import pandas as pd
import streamlit as st
from langchain_core.tools import tool
import datetime
import re


# ==========================================
#  1. 辅助：替身生成器
# ==========================================
def _get_active_proxies(symbol: str):
    """
    生成替身列表。如果查不到 IH2512，就查 IH2502, IH2503...
    """
    # 提取品种字母，如 "IH", "rb"
    alpha = "".join(filter(str.isalpha, symbol))

    # 真实世界当前活跃的月份后缀 (根据当前实际时间调整)
    # 假设现在是 2025年初，活跃合约通常是 2502-2512
    active_suffixes = [
        "2601", "2602", "2605", "2609"
    ]

    proxies = [f"{alpha}{suffix}" for suffix in active_suffixes]
    return proxies


# ==========================================
#  2. 核心：获取分时数据 (带自动回退)
# ==========================================
@st.cache_data(ttl=300)
def fetch_minute_trend(symbol: str):
    """
    获取分时走势数据 (AkShare版 + 替身机制)。
    """
    # 清洗代码: nf_rb2505 -> rb2505
    target = symbol.replace('nf_', '').strip()

    # 定义内部尝试函数
    def _try_get(code):
        try:
            # period="5" 获取5分钟K线模拟分时
            df = ak.futures_zh_minute_sina(symbol=code, period="5")
            if df is not None and not df.empty:
                return df
        except Exception:
            pass  # 忽略所有报错 (包括 Length mismatch)
        return None

    # --- 第1次尝试: 查目标合约 ---
    print(f"DEBUG: AkShare 尝试查询 -> {target}")
    df = _try_get(target)

    # --- 第2次尝试: 如果失败，启动替身计划 ---
    if df is None:
        print(f"DEBUG: {target} 无数据/报错，启动替身搜索...")
        proxies = _get_active_proxies(target)

        for p in proxies:
            print(f"   -> 试探替身: {p}")
            df = _try_get(p)
            if df is not None:
                print(f"✅ 成功找到替身: {p}")
                break

            # 针对金融期货，尝试加 CFF_RE_ 前缀 (AkShare有时需要)
            # 比如 CFF_RE_IH2503
            if target.upper().startswith(('IH', 'IF', 'IC', 'IM')):
                p_cff = f"CFF_RE_{p.upper()}"
                df = _try_get(p_cff)
                if df is not None:
                    print(f"✅ 成功找到替身(CFF): {p_cff}")
                    break

    # 如果还是空，返回空表
    if df is None or df.empty:
        return pd.DataFrame()

    # --- 数据清洗 ---
    try:
        # AkShare 返回列: datetime, open, high, low, close, volume, hold
        df = df[['datetime', 'close']].copy()
        df.columns = ['date', 'close']

        df['close'] = df['close'].astype(float)
        df['date'] = df['date'].astype(str)

        # 取最近 100 条
        return df.tail(100)
    except Exception as e:
        print(f"数据清洗失败: {e}")
        return pd.DataFrame()


# ==========================================
#  3. 获取实时报价 (快照)
# ==========================================
@tool
def get_future_snapshot(symbol: str):
    """
    获取期货实时报价。
    """
    try:
        # 使用 AkShare 的新浪接口 (支持列表查询，比较快)
        # 需要处理前缀: rb2505 -> nf_rb2505, IH2503 -> CFF_RE_IH2503
        clean = symbol.replace('nf_', '').replace('CFF_RE_', '')

        # 简单判断前缀
        if clean.upper().startswith(('IH', 'IF', 'IC', 'IM', 'T', 'TF', 'TS')):
            sina_sym = f"CFF_RE_{clean}"
        else:
            sina_sym = f"nf_{clean}"

        # 注意: AkShare 没有直接查单个 sina 快照的简单函数，
        # 这里为了稳健，我们还是用 requests 查新浪原生接口 (最快)
        # 或者用 ak.futures_zh_spot() 查全市场 (较慢)

        # 这里混用一下原生 request，因为它作为 Tool 使用频率高，要求速度
        import requests
        url = f"http://hq.sinajs.cn/list={sina_sym}"
        resp = requests.get(url, timeout=2)

        if '="' in resp.text:
            data = resp.text.split('="')[1]
            parts = data.split(',')
            if len(parts) > 5:
                # 金融期货价格在 index 3, 商品在 index 8
                price = parts[3] if "CFF_RE_" in sina_sym else parts[8]
                return f"【实时报价】{parts[0]} ({clean}) 现价: {price}"

        return "暂无报价"

    except Exception as e:
        return f"行情获取失败: {e}"


# ==========================================
#  4. AI 工具包
# ==========================================
@tool
def get_kline_analysis_tool(symbol: str):
    """AI 查看趋势专用"""
    df = fetch_minute_trend(symbol)
    if df.empty: return "暂无数据"

    start = df.iloc[0]['close']
    end = df.iloc[-1]['close']
    trend = "上涨" if end > start else "下跌"
    return f"根据实时数据，{symbol} 近期从 {start} 到 {end}，整体趋势{trend}。"