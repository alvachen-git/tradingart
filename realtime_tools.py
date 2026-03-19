import akshare as ak
import pandas as pd
import streamlit as st
from langchain_core.tools import tool
import symbol_map
import requests
import datetime
import re

_SINA_HEADERS = {
    "Referer": "http://finance.sina.com.cn/",
    "User-Agent": "Mozilla/5.0",
}


def _build_sina_session() -> requests.Session:
    """
    所有新浪请求统一使用独立 Session：
    - 显式 trust_env=False，避免继承系统代理导致 SOCKS 依赖错误。
    - 固定请求头，降低 403/空响应概率。
    """
    session = requests.Session()
    session.trust_env = False
    session.headers.update(_SINA_HEADERS)
    return session


# ==========================================
#  1. 辅助：替身生成器
# ==========================================
def _get_active_proxies(symbol: str):
    """
    🔥 [修复版 - 纯标准库] 动态生成替身列表
    """
    import datetime
    import calendar

    alpha = "".join(filter(str.isalpha, symbol))

    now = datetime.datetime.now()
    active_suffixes = []

    year = now.year
    month = now.month

    for i in range(6):  # 生成6个月
        # 计算未来月份
        future_month = month + i
        future_year = year

        # 处理跨年
        while future_month > 12:
            future_month -= 12
            future_year += 1

        # 格式化 YYMM
        suffix = f"{future_year % 100:02d}{future_month:02d}"
        active_suffixes.append(suffix)

    proxies = [f"{alpha}{suffix}" for suffix in active_suffixes]

    print(f"[DEBUG] {symbol} 的替身列表: {proxies}")

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
        url = f"http://hq.sinajs.cn/list={sina_sym}"
        session = _build_sina_session()
        resp = session.get(url, timeout=2)

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


# realtime_tools.py (追加到文件末尾)

def get_realtime_prices_batch(symbol_list: list):
    """
    🔥 [新增] 批量获取实时价格 (高性能版)
    输入: ['白银', 'rb2505', 'IF2506']
    输出: {
        '白银': {'price': 7100.0, 'name': '白银2606', 'code': 'AG2606'},
        'rb2505': {'price': 3600.0, 'name': '螺纹2505', 'code': 'RB2505'},
        ...
    }
    """
    # 1. 预处理：为每个输入生成“候选合约列表”
    # 比如输入 "白银"，生成 ["nf_ag2606", "nf_ag2608", "nf_ag2612"...]
    task_map = {}  # { '白银': ['nf_ag2606', 'nf_ag2608'] }
    all_sina_codes = set()

    for symbol in set(symbol_list):  # 去重处理
        if not symbol: continue

        # A. 解析代码
        target_code = symbol
        if not any(char.isdigit() for char in symbol):
            res = symbol_map.resolve_symbol(symbol)
            if res and res[0]:
                target_code = res[0]
            else:
                target_code = symbol.upper()

        # B. 生成候选列表 (复用之前的替身逻辑)
        clean_code = target_code.replace('nf_', '').replace('CFF_RE_', '')
        codes_to_try = []

        # 如果自带数字 (如 AG2606)，优先查它
        if any(char.isdigit() for char in clean_code):
            codes_to_try.append(clean_code)

        # 无论有没有数字，都生成一批活跃替身 (防止主力换月导致旧代码失效)
        proxies = _get_active_proxies(clean_code)
        for p in proxies:
            if p not in codes_to_try: codes_to_try.append(p)

        # C. 转换为新浪格式
        sina_codes = []
        for c in codes_to_try:
            if c.upper().startswith(('IH', 'IF', 'IC', 'IM', 'T', 'TF', 'TS')):
                sina_codes.append(f"CFF_RE_{c.upper()}")
            else:
                sina_codes.append(f"nf_{c.lower()}")

        task_map[symbol] = sina_codes
        all_sina_codes.update(sina_codes)

    # 2. 批量请求 (分批处理，防止 URL 过长)
    # 新浪接口通常支持一次查几十个，我们设定每批 50 个
    batch_size = 50
    all_codes_list = list(all_sina_codes)
    price_cache = {}  # { 'nf_ag2606': {'price': 7100, 'name': '白银'} }
    session = _build_sina_session()

    for i in range(0, len(all_codes_list), batch_size):
        chunk = all_codes_list[i:i + batch_size]
        url = f"http://hq.sinajs.cn/list={','.join(chunk)}"

        try:
            resp = session.get(url, timeout=2)
            # 解析返回数据: var hq_str_nf_ag2606="白银2606,5800...";
            lines = resp.text.split(';')
            for line in lines:
                if '="' not in line: continue

                # 提取 code: var hq_str_nf_ag2606 -> nf_ag2606
                code_part = line.split('=')[0]
                sina_code = code_part.split('hq_str_')[-1]

                # 提取数据
                content = line.split('="')[1].strip('"')
                if len(content) < 5: continue  # 空数据

                parts = content.split(',')
                name = parts[0]
                price = 0.0

                # 金融期货 vs 商品期货
                if "CFF_RE_" in sina_code:
                    price = float(parts[3]) if len(parts) > 3 else 0.0
                else:
                    price = float(parts[8]) if len(parts) > 8 else 0.0
                    if price == 0 and len(parts) > 6: price = float(parts[6])  # 兜底买一价

                if price > 0:
                    price_cache[sina_code] = {'price': price, 'name': name}

        except Exception as e:
            print(f"批量请求失败: {e}")

    # 3. 匹配回原始输入
    final_results = {}
    for symbol, candidates in task_map.items():
        found = False
        # 遍历该品种的所有候选合约，找到第一个有数据的
        for c in candidates:
            if c in price_cache:
                data = price_cache[c]
                # 构造返回结构
                real_code = c.replace('nf_', '').replace('CFF_RE_', '').upper()
                final_results[symbol] = {
                    'price': data['price'],
                    'name': data['name'],
                    'code': real_code
                }
                found = True
                break  # 找到了主力就不找了

        if not found:
            final_results[symbol] = None

    return final_results
