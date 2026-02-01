import pandas as pd
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from langchain_core.tools import tool
# 引入 Pydantic 進行嚴格的參數控制
from pydantic import BaseModel, Field
import symbol_map
import traceback
from datetime import datetime
import streamlit as st

# 1. 初始化環境
load_dotenv(override=True)
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")


def get_db_engine():
    if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]): return None
    db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(
        db_url,
        pool_pre_ping=True,      # 每次查询前检查连接是否有效
        pool_recycle=7200,       # 1小时回收连接
        pool_size=5,             # 连接池大小
        max_overflow=10          # 最大溢出连接数
    )


engine = get_db_engine()


# --- 2. 定義 AI 調用工具時的參數結構 ---
class PriceStatsInput(BaseModel):
    query_list: str = Field(description="品種名稱，例如 '黃金' 或 '寧德時代,茅台'")
    start_date: str = Field(description="開始日期，格式必須是 YYYYMMDD，例如 '20231001'")
    end_date: str = Field(description="結束日期，格式必須是 YYYYMMDD，例如 '20231031'")


# --- 3. 輔助函數：清洗日期格式 ---
def clean_date_str(date_str: str) -> str:
    """防止 AI 傳入 '2023-01-01' 導致 SQL 查詢失敗，統一轉為 '20230101'"""
    if not date_str: return ""
    return date_str.replace("-", "").replace("/", "").replace(".", "").strip()


# --- 4. 核心工具：歷史價格統計 ---
@tool(args_schema=PriceStatsInput)
def get_price_statistics(query_list: str, start_date: str, end_date: str):
    """
    【区间行情統計工具】
    用於統計某段時間內的：最高价、最低价、區区间漲跌幅。
    适用于回答："上月最高价多少"、"上周誰漲得好"等问题。
    """
    if engine is None: return "❌ 數據庫連接失敗，請檢查 .env 配置"

    # A. 參數預處理
    s_date = clean_date_str(start_date)
    e_date = clean_date_str(end_date)
    queries = [q.strip() for q in query_list.split(',') if q.strip()]
    results = []

    for q in queries:
        # B. 解析代碼
        symbol_code, asset_type = symbol_map.resolve_symbol(q)
        if not symbol_code:
            results.append(f"⚠️ 未找到品種: {q}")
            continue

        try:
            target_code = symbol_code.upper()
            df = pd.DataFrame()

            # C. 構建查詢 (增加 pct_chg 字段)
            if asset_type == 'stock':
                is_hk = target_code.endswith('.HK')

                if is_hk:
                    # 🔥【港股】使用精确匹配，避免01810.HK和81810.HK混淆
                    sql = text("""
                               SELECT trade_date,
                                      close_price as close, high_price as high, 
                               low_price as low, open_price as open, pct_chg
                               FROM stock_price
                               WHERE ts_code = :code
                                 AND trade_date >= :s_date
                                 AND trade_date <= :e_date
                               ORDER BY trade_date ASC
                               """)
                    df = pd.read_sql(sql, engine, params={
                        "code": target_code,
                        "s_date": s_date,
                        "e_date": e_date
                    })
                else:
                    # 【A股】保持原逻辑
                    codes_to_try = [target_code]
                    if "." in target_code:
                        codes_to_try.append(target_code.split('.')[0])
                    else:
                        codes_to_try.extend([f"{target_code}.SZ", f"{target_code}.SH"])
                    code_str = "','".join(set(codes_to_try))

                    sql = text(f"""
                        SELECT trade_date, close_price as close, high_price as high, 
                               low_price as low, open_price as open, pct_chg 
                        FROM stock_price 
                        WHERE ts_code IN ('{code_str}')
                          AND trade_date >= :s_date 
                          AND trade_date <= :e_date
                        ORDER BY trade_date ASC
                    """)
                    df = pd.read_sql(sql, engine, params={"s_date": s_date, "e_date": e_date})

            # --- 分支 2: 指数 (新增) ---
            elif asset_type == 'index':
                # 指数表结构现在包含了 high_price, low_price
                sql = text("""
                    SELECT trade_date, 
                           close_price as close, 
                           high_price as high, 
                           low_price as low, 
                           pct_chg 
                    FROM index_price 
                    WHERE ts_code = :code
                      AND trade_date >= :s_date 
                      AND trade_date <= :e_date
                    ORDER BY trade_date ASC
                """)
                df = pd.read_sql(sql, engine, params={"code": target_code, "s_date": s_date, "e_date": e_date})

            else:
                # 【修改点 2】增加 pct_chg
                sql = text("""
                           SELECT trade_date, close_price as close, high_price as high, low_price as low, pct_chg
                           FROM futures_price
                           WHERE ts_code = :code
                             AND trade_date >= :s_date
                             AND trade_date <= :e_date
                           ORDER BY trade_date ASC
                           """)
                df = pd.read_sql(sql, engine, params={"code": target_code, "s_date": s_date, "e_date": e_date})

            # D. 數據計算
            if df.empty:
                results.append(f"⚠️ {q} ({target_code}): 在區間 {s_date}-{e_date} 無交易數據")
                continue

            start_price = df.iloc[0]['close']
            end_price = df.iloc[-1]['close']

            # 【核心修复逻辑】
            # 如果只查了一天 (start_date == end_date)，或者结果只有一行
            # 直接使用数据库里的 pct_chg (这是官方算好的，包含昨收信息)
            if len(df) == 1:
                period_chg = df.iloc[0]['pct_chg']
                if period_chg is None: period_chg = 0.0  # 防止空值
            else:
                # 如果是多天区间，计算累计涨幅
                if start_price == 0:
                    period_chg = 0.0
                else:
                    period_chg = (end_price - start_price) / start_price * 100

            max_price = df['high'].max()
            min_price = df['low'].min()
            max_date_row = df.loc[df['high'].idxmax()]
            max_date_str = str(max_date_row['trade_date'])

            # 优化输出文案
            results.append(f"""
📊 **{q} 行情统计 ({s_date} - {e_date})**
- **期间涨跌**: {period_chg:+.2f}% {'🔥' if period_chg > 0 else '💧'}
- **收盘价**: {end_price}
- **最高价**: {max_price} (出现在 {max_date_str})
- **最低价**: {min_price}
            """)

        except Exception as e:
            print(f"計算 {q} 時出錯: {traceback.format_exc()}")
            results.append(f"❌ 計算 {q} 時發生錯誤: {e}")

    return "\n".join(results)


# --- 5. 另一個工具：獲取最新快照 (保持簡單可用) ---
@tool
def get_market_snapshot(query: str):
    """
    【最新价格查询】
    查询数据库中已收录的**最新日线记录**
    输入：品种名称（如 "豆粕"、"茅台"）。
    """
    if engine is None: return "數據庫未連接"
    symbol_code, asset_type = symbol_map.resolve_symbol(query)
    if not symbol_code: return f"未找到 {query}"

    try:
        target_code = symbol_code.upper()
        if asset_type == 'stock':
            # 模糊匹配查詢最新一條
            sql = text("""
                       SELECT ts_code, name, trade_date, close_price, pct_chg
                       FROM stock_price
                       WHERE ts_code = :code
                       ORDER BY trade_date DESC LIMIT 1
                       """)
            df = pd.read_sql(sql, engine, params={"code": target_code})

        elif asset_type == 'index':
            # 新增指数查询
            sql = text(f"SELECT * FROM index_price WHERE ts_code = :code ORDER BY trade_date DESC LIMIT 1")
            df = pd.read_sql(sql, engine, params={"code": target_code})

        else:
            sql = text("SELECT * FROM futures_price WHERE ts_code LIKE :code AND ts_code NOT LIKE '%TAS%' ORDER BY trade_date DESC LIMIT 1")
            df = pd.read_sql(sql, engine, params={"code": f"{target_code}%"})

        if df.empty: return f"暫無 {query} 最新數據"

        row = df.iloc[0]
        price = row.get('close') or row.get('close_price')
        date = row['trade_date']
        name = row.get('name', query)
        ts_code = row.get('ts_code', query)
        return f"📍 **{name}({ts_code}) 行情**\n日期: {date}\n价格: {price}\n(如需更多历史数据请询问具体时间段)"

    except Exception as e:
        return f"查询错误: {e}"


# ==========================================
#   🔥 期权价格查询工具（支持ETF期权+商品期权）
#   - 商品期权未指定月份时，自动查找持仓量最大的主力月
#   - 兼容所有交易所格式：
#     - 大商所: M2505-C-3200.DCE (有 - 分隔)
#     - 上期所: ZN2602C26500.SHF (无 - 分隔)
#     - 郑商所: CF505-C-12000.ZCE
# ==========================================
@tool
def tool_query_specific_option(query: str):
    """
    【期权价格查询专用】
    当需要具体的期权合约价格时使用。
    支持ETF期权和商品期权。
    例如："50ETF 1月 3.1 认购价格"、"豆粕2505 3200看涨"、"M2505 3200 认沽"
    """
    if engine is None: return "❌ 数据库未连接"

    import re
    from datetime import datetime
    from symbol_map import COMMON_ALIASES  # 导入现有映射

    # ==========================================
    #  1. ETF期权映射
    # ==========================================
    ETF_CODE_MAP = {
        '50ETF': '510050.SH', '上证50': '510050.SH',
        '300ETF': '510300.SH', '沪深300': '510300.SH',
        '500ETF': '510500.SH', '中证500': '510500.SH',
        '创业板': '159915.SZ', '创业板ETF': '159915.SZ',
        '科创50': '588000.SH', '科创板': '588000.SH', '科创50ETF': '588000.SH'
    }

    # ==========================================
    #  2. 判断是ETF期权还是商品期权
    # ==========================================
    query_upper = query.upper()
    target_code = None
    target_name = ""
    is_etf_option = False
    is_commodity_option = False

    # 先检查ETF
    for name, code in ETF_CODE_MAP.items():
        if name.upper() in query_upper:
            target_code = code
            target_name = name
            is_etf_option = True
            break

    # 再检查商品（使用 COMMON_ALIASES）
    if not target_code:
        for name, code in COMMON_ALIASES.items():
            # 排除股票/指数（带.SH/.SZ的）
            if '.' in str(code):
                continue
            if name.upper() in query_upper:
                target_code = str(code).lower()  # 确保小写
                target_name = name
                is_commodity_option = True
                break

    # 如果中文没匹配到，尝试匹配英文代码 (如 M2505, MA2505)
    if not target_code:
        match = re.match(r'^([A-Za-z]+)\d*', query_upper)
        if match:
            code_input = match.group(1).lower()
            # 检查是否在 COMMON_ALIASES 的 values 中
            if code_input in [str(v).lower() for v in COMMON_ALIASES.values() if '.' not in str(v)]:
                target_code = code_input
                target_name = code_input.upper()
                is_commodity_option = True

    if not target_code:
        return "⚠️ 未识别到期权品种。支持：50ETF/300ETF/500ETF/创业板ETF/科创50ETF，以及豆粕/白糖/铜/黄金等商品期权。"

    # ==========================================
    #  3. 解析方向 (认购/认沽)
    # ==========================================
    cp_type = None
    if any(x in query for x in ['认购', '看涨', 'CALL', 'Call', 'call']):
        cp_type = 'C'
        cp_name = "认购"
    elif any(x in query for x in ['认沽', '看跌', 'PUT', 'Put', 'put']):
        cp_type = 'P'
        cp_name = "认沽"

    if not cp_type:
        return f"⚠️ 请指明是【认购】还是【认沽】？(例如：{target_name} 3200 认购)"

    # ==========================================
    #  4. (順序已調整) 先解析月份
    #  🔥 修改理由：必須先知道哪個數字是月份(如2603)，
    #  等下解析行權價時才能避開它，防止把2603誤判為價格。
    # ==========================================
    # 嘗試匹配 "X月" 格式
    match_month = re.search(r'(\d{1,2})月', query)
    # 嘗試匹配合約月份格式 (如 2505, 2503)
    match_contract = re.search(r'(2[0-9]\d{2})', query)

    search_month_str = None

    if match_month:
        m = int(match_month.group(1))
        curr_year = datetime.now().year % 100
        search_month_str = f"{curr_year}{m:02d}"  # 如 "2601"
    elif match_contract:
        search_month_str = match_contract.group(1)  # 如 "2505"

    # ==========================================
    #  5. (順序已調整) 後解析行權價
    # ==========================================
    numbers = re.findall(r"\d+\.?\d*", query)
    strike_price = None

    for num in numbers:
        val = float(num)

        # 🔥【新增過濾邏輯】
        # 如果這個數字剛好等於我們識別到的月份 (例如 2603)，直接跳過！不要把它當成價格！
        if search_month_str and num == search_month_str:
            continue

        if is_etf_option:
            # ETF期權行權價一般 < 20
            if val < 20 and val > 0.5:
                strike_price = val
                break
        else:
            # 商品期權行權價一般 > 100 (排除一些明顯太小的數字)
            if val > 100:
                strike_price = val
                break

    # 兜底檢查
    if strike_price is None:
        example = f"{target_name} 3.2 認購" if is_etf_option else f"{target_name} 3200 認購"
        return f"⚠️ 未識別到行權價。(例如：{example})"

    try:
        # ==========================================
        #  6. 查询数据库
        # ==========================================
        if is_etf_option:
            # --- ETF期权查询 ---
            sql_find_contract = f"""
                SELECT ts_code, exercise_price, delist_date 
                FROM option_basic 
                WHERE underlying = '{target_code}' 
                  AND call_put = '{cp_type}'
                  AND delist_date >= '{datetime.now().strftime('%Y%m%d')}'
                ORDER BY delist_date ASC
            """
            df_basic = pd.read_sql(sql_find_contract, engine)

            if df_basic.empty:
                return f"❌ 数据库中未找到 {target_name} 的ETF期权合约信息。"

            # 筛选月份 (delist_date 格式: 20250122)
            df_basic['month_str'] = df_basic['delist_date'].astype(str).str[2:6]  # 取 "2501"

            if search_month_str:
                target_contracts = df_basic[df_basic['month_str'] == search_month_str]
            else:
                # ETF期权默认取最近月份
                search_month_str = df_basic['month_str'].iloc[0]
                target_contracts = df_basic[df_basic['month_str'] == search_month_str]

            if target_contracts.empty:
                # 显示可用月份
                available_months = df_basic['month_str'].unique()[:5]
                return f"⚠️ 未找到 {search_month_str} 月份的合约。可用月份: {', '.join(available_months)}"

            # 筛选行权价
            target_contracts = target_contracts.copy()
            target_contracts['diff'] = abs(target_contracts['exercise_price'] - strike_price)
            best_match = target_contracts.sort_values('diff').iloc[0]

            if best_match['diff'] > 0.2:
                available_strikes = sorted(target_contracts['exercise_price'].unique())
                return f"⚠️ 未找到行权价 {strike_price} 的合约。可用行权价: {available_strikes[:10]}"

            final_ts_code = best_match['ts_code']
            matched_strike = best_match['exercise_price']

            # 查价格
            sql_price = f"""
                SELECT trade_date, close, vol, oi 
                FROM option_daily 
                WHERE ts_code = '{final_ts_code}' 
                ORDER BY trade_date DESC LIMIT 2
            """
            df_price = pd.read_sql(sql_price, engine)

        else:
            # 🔥【修复 1】解决 M (豆粕) 和 MA (甲醇) 混淆
            # 原理：使用 REGEXP 强制代码后必须跟数字。
            # ^M[0-9] 可以匹配 M2505，但不能匹配 MA2505
            # 同时兼容大小写 (M 和 m)

            # 构造正则模式：以 target_code 开头，紧接着是数字
            # 例如 target_code='M' -> '^(M|m)[0-9]'
            regex_pattern = f"^({target_code}|{target_code.lower()})[0-9]"

            sql_find_contract = f"""
                            SELECT ts_code, exercise_price, maturity_date 
                            FROM commodity_option_basic 
                            WHERE ts_code REGEXP '{regex_pattern}'
                              AND call_put = '{cp_type}'
                              AND maturity_date >= '{datetime.now().strftime('%Y%m%d')}'
                            ORDER BY maturity_date ASC
                        """

            try:
                df_basic = pd.read_sql(sql_find_contract, engine)
            except Exception as e:
                # 如果数据库不支持 REGEXP (极少见)，回退到旧逻辑并做 Python 过滤
                print(f"REGEXP 查询失败，回退到 LIKE: {e}")
                sql_fallback = f"""
                                SELECT ts_code, exercise_price, maturity_date 
                                FROM commodity_option_basic 
                                WHERE (ts_code LIKE '{target_code}%%' OR ts_code LIKE '{target_code.lower()}%%')
                                  AND call_put = '{cp_type}'
                                  AND maturity_date >= '{datetime.now().strftime('%Y%m%d')}'
                            """
                df_basic = pd.read_sql(sql_fallback, engine)
                # Python 层过滤 MA
                df_basic = df_basic[df_basic['ts_code'].str.match(f"^{target_code}\d", case=False)]

            if df_basic.empty:
                # 🔥 增强版: 先检查是否真的没有该品种的任何期权数据
                check_sql = f"""
                                SELECT COUNT(*) as cnt,
                                       GROUP_CONCAT(DISTINCT SUBSTRING(ts_code, 1, 6) SEPARATOR ', ') as available_months
                                FROM commodity_option_basic 
                                WHERE ts_code REGEXP '{regex_pattern}'
                                  AND maturity_date >= '{datetime.now().strftime('%Y%m%d')}'
                                LIMIT 1
                            """

                try:
                    df_check = pd.read_sql(check_sql, engine)
                    total_count = df_check.iloc[0]['cnt']

                    if total_count > 0:
                        # 品种有期权,但查不到指定月份
                        available = df_check.iloc[0]['available_months']
                        return f"""
            ⚠️ 【{target_name}】有期权，但未找到符合条件的合约

            可能原因:
            1. 指定的月份不存在或已过期
            2. 该月份尚未上市

            可用月份示例: {available}

            💡 建议: 请确认月份后重试，或不指定月份让系统自动选择主力合约
                                    """
                    else:
                        # 品种真的没有期权数据
                        return f"""
            ❌ 数据库中暂无 {target_name} ({target_code}) 的期权数据

            可能原因:
            1. 数据尚未同步到数据库
            2. 该品种确实暂无场内期权

            💡 建议: 
            - 如果确认该品种有期权,请联系管理员检查数据同步
            - 或尝试查询其他品种
                                    """
                except Exception as e:
                    # 降级到原提示
                    return f"❌ 查询 {target_name} 期权时出错: {e}"

            # 🔥【修复 2】提取月份 (兼容郑商所 3位年份 和 其他 4位年份)
            # 这里的正则提取逻辑：找到代码中第一串连续的数字
            # ZN2602 -> 2602, CF603 -> 603
            df_basic['month_str'] = df_basic['ts_code'].str.extract(r'([0-9]{3,4})')[0]

            # 统一补全为 4 位年份 (603 -> 2603)
            def _fix_year(m):
                if pd.isna(m): return None
                # 如果是 3 位数 (郑商所)，补个 2
                if len(m) == 3: return f"2{m}"
                return m

            # 创建一个标准化月份列用于对比
            df_basic['std_month'] = df_basic['month_str'].apply(_fix_year)

            if search_month_str:
                # 用户指定了月份 (如 2603)
                # 直接用标准化的 std_month 对比
                target_contracts = df_basic[df_basic['std_month'] == search_month_str]

                if target_contracts.empty:
                    # 如果找不到，展示前几个可用的月份
                    available = df_basic['std_month'].dropna().unique()[:5]
                    return f"⚠️ 未找到 {search_month_str} 月份的合约。可用月份: {', '.join(available)}"
            else:
                # 未指定月份，自动找持仓量最大的主力月
                sql_oi = f"""
                    SELECT ts_code, oi
                    FROM commodity_opt_daily
                    WHERE ts_code LIKE '{target_code.upper()}%%'
                      AND trade_date = (SELECT MAX(trade_date) FROM commodity_opt_daily WHERE ts_code LIKE '{target_code.upper()}%%')
                """
                df_oi = pd.read_sql(sql_oi, engine)

                if not df_oi.empty:
                    # 在 Python 中用正则提取月份（兼容所有格式）
                    df_oi['month_str'] = df_oi['ts_code'].str.extract(r'[A-Za-z]+(\d{4})')[0]
                    # 按月份汇总持仓量，找最大的
                    month_oi = df_oi.groupby('month_str')['oi'].sum().sort_values(ascending=False)
                    if not month_oi.empty:
                        search_month_str = month_oi.index[0]
                        target_contracts = df_basic[df_basic['month_str'] == search_month_str]
                    else:
                        # 兜底：取最近到期的月份
                        search_month_str = df_basic['month_str'].dropna().iloc[0] if not df_basic[
                            'month_str'].dropna().empty else None
                        if search_month_str:
                            target_contracts = df_basic[df_basic['month_str'] == search_month_str]
                        else:
                            return f"❌ 无法确定 {target_name} 的主力月份。"
                else:
                    # 兜底：取最近到期的月份
                    search_month_str = df_basic['month_str'].dropna().iloc[0] if not df_basic[
                        'month_str'].dropna().empty else None
                    if search_month_str:
                        target_contracts = df_basic[df_basic['month_str'] == search_month_str]
                    else:
                        return f"❌ 无法确定 {target_name} 的主力月份。"

            if target_contracts.empty:
                available_months = df_basic['month_str'].dropna().unique()[:5]
                return f"⚠️ 未找到 {search_month_str} 月份的合约。可用月份: {', '.join(available_months)}"

            # 筛选行权价
            target_contracts = target_contracts.copy()
            target_contracts['diff'] = abs(target_contracts['exercise_price'] - strike_price)
            best_match = target_contracts.sort_values('diff').iloc[0]

            # 商品期权行权价间距较大，允许更大误差
            if best_match['diff'] > 100:
                available_strikes = sorted(target_contracts['exercise_price'].unique())
                return f"⚠️ 未找到行权价 {int(strike_price)} 的合约。可用行权价: {[int(s) for s in available_strikes[:10]]}"

            final_ts_code = best_match['ts_code']
            matched_strike = best_match['exercise_price']

            # 查价格
            sql_price = f"""
                SELECT trade_date, close, vol, oi 
                FROM commodity_opt_daily 
                WHERE ts_code = '{final_ts_code}' 
                ORDER BY trade_date DESC LIMIT 2
            """
            df_price = pd.read_sql(sql_price, engine)

        # ==========================================
        #  7. 返回结果
        # ==========================================
        if df_price.empty:
            return f"✅ 找到合约 **{final_ts_code}**，但暂无最新交易数据。"

        curr_row = df_price.iloc[0]

        # 计算涨跌
        if len(df_price) >= 2:
            prev_close = df_price.iloc[1]['close']
            change_val = curr_row['close'] - prev_close
            change_text = f"{change_val:+.2f}"
        else:
            change_text = "新上市"

        # 格式化行权价和价格显示
        if is_etf_option:
            strike_display = f"{matched_strike:.3f}"
            price_display = f"{curr_row['close']:.4f}"
        else:
            strike_display = f"{int(matched_strike)}"
            price_display = f"{curr_row['close']:.1f}"

        # 处理持仓量显示
        oi_display = int(curr_row['oi']) if pd.notna(curr_row['oi']) else 'N/A'
        vol_display = int(curr_row['vol']) if pd.notna(curr_row['vol']) else 'N/A'

        # 主力月提示
        month_note = "（主力月）" if not match_month and not match_contract else ""

        return f"""
-------------------------
📝 **合约**: {target_name} {cp_name} @ {strike_display}
📅 **月份**: {search_month_str} {month_note}
🔖 **代码**: {final_ts_code}
-------------------------
💰 **最新价**: **{price_display}**
📅 **数据日期**: {curr_row['trade_date']}
📊 **涨跌**: **{change_text}** (较昨收)
📈 **成交量**: {vol_display}
📈 **持仓量**: {oi_display}
        """

    except Exception as e:
        import traceback
        return f"查询过程发生错误: {e}\n{traceback.format_exc()}"


# ==========================================
#   🔥 新增：查询历史某一天的价格
# ==========================================
@tool
def get_historical_price(query: str, trade_date: str):
    """
    【历史价格查询】
    查询股票(A股/港股)、期货、指数在某一天的价格。
    适用于回答："阿里在12月2日价格多少"、"茅台上周五收盘价"、"黄金在2025年11月1日的价格"。

    参数:
    - query: 品种名称，如 "阿里"、"茅台"、"黄金"、"小米"
    - trade_date: 查询日期，格式 YYYYMMDD，如 "20251202"
    """
    if engine is None:
        return "❌ 数据库未连接"

    # 清洗日期
    clean_date = trade_date.replace("-", "").replace("/", "").replace(".", "").strip()

    # 解析品种
    symbol_code, asset_type = symbol_map.resolve_symbol(query)
    if not symbol_code:
        return f"⚠️ 未找到品种: {query}"

    try:
        target_code = symbol_code.upper()
        df = pd.DataFrame()

        if asset_type == 'stock':
            is_hk = target_code.endswith('.HK')

            if is_hk:
                # 港股精确匹配
                sql = text("""
                           SELECT ts_code,
                                  name,
                                  trade_date,
                                  open_price,
                                  high_price,
                                  low_price,
                                  close_price,
                                  pct_chg
                           FROM stock_price
                           WHERE ts_code = :code
                             AND trade_date = :date
                           """)
                df = pd.read_sql(sql, engine, params={"code": target_code, "date": clean_date})
            else:
                # A股
                codes_to_try = [target_code]
                if "." in target_code:
                    codes_to_try.append(target_code.split('.')[0])
                else:
                    codes_to_try.extend([f"{target_code}.SZ", f"{target_code}.SH"])
                code_str = "','".join(set(codes_to_try))

                sql = text(f"""
                    SELECT ts_code, name, trade_date, open_price, high_price, 
                           low_price, close_price, pct_chg
                    FROM stock_price 
                    WHERE ts_code IN ('{code_str}') AND trade_date = :date
                """)
                df = pd.read_sql(sql, engine, params={"date": clean_date})

        elif asset_type == 'index':
            sql = text("""
                       SELECT ts_code,
                              trade_date,
                              open_price,
                              high_price,
                              low_price,
                              close_price,
                              pct_chg
                       FROM index_price
                       WHERE ts_code = :code
                         AND trade_date = :date
                       """)
            df = pd.read_sql(sql, engine, params={"code": target_code, "date": clean_date})

        else:  # future
            sql = text("""
                       SELECT ts_code,
                              trade_date,
                              open_price,
                              high_price,
                              low_price,
                              close_price,
                              pct_chg
                       FROM futures_price
                       WHERE ts_code = :code
                         AND trade_date = :date
                       """)
            df = pd.read_sql(sql, engine, params={"code": target_code, "date": clean_date})

        if df.empty:
            return f"⚠️ {query} ({target_code}) 在 {clean_date} 无交易数据（可能是休市日）"

        row = df.iloc[0]
        ts_code = row.get('ts_code', target_code)
        name = row.get('name', query)
        open_p = row.get('open_price')
        high_p = row.get('high_price')
        low_p = row.get('low_price')
        close_p = row.get('close_price')
        pct_chg = row.get('pct_chg', 0)

        # 港股显示货币单位
        currency = "港元" if '.HK' in str(ts_code) else "元"

        # 涨跌幅
        if pct_chg and pct_chg != 0:
            chg_str = f"{float(pct_chg):+.2f}%"
            chg_emoji = "🔴" if float(pct_chg) < 0 else "🟢"
        else:
            chg_str = "0.00%"
            chg_emoji = "⚪"

        return f"""📅 **{name} ({ts_code}) {clean_date} 行情**

💰 **收盘价**: {close_p} {currency}
📈 **开盘价**: {open_p} {currency}
🔺 **最高价**: {high_p} {currency}
🔻 **最低价**: {low_p} {currency}
{chg_emoji} **涨跌幅**: {chg_str}
"""

    except Exception as e:
        print(f"查询错误: {traceback.format_exc()}")
        return f"❌ 查询出错: {e}"


# ==========================================
#  AI 工具
# ==========================================
from langchain.tools import tool
import pandas as pd
from sqlalchemy import text


@tool
def get_trending_hotspots(category: str = "all"):
    """
    【热点发现】
    获取当前正在上升的热点话题。

    参数:
    - category: "all"=全部, "finance"=仅金融相关

    当用户问"最近什么热门"、"有什么新热点"、"市场在关注什么"时使用。
    """
    if engine is None:
        return "❌ 数据库未连接"

    try:
        if category == "finance":
            sql = text("""
                       SELECT keyword, source, alert_type, description, created_at
                       FROM trend_alert_v2
                       WHERE is_finance_related = 1
                         AND created_at >= DATE_SUB(NOW(), INTERVAL 3 DAY)
                       ORDER BY created_at DESC LIMIT 20
                       """)
        else:
            sql = text("""
                       SELECT keyword, source, alert_type, description, created_at
                       FROM trend_alert_v2
                       WHERE created_at >= DATE_SUB(NOW(), INTERVAL 3 DAY)
                       ORDER BY created_at DESC LIMIT 30
                       """)

        df = pd.read_sql(sql, engine)

        if df.empty:
            return "📭 近3天暂无新发现的热点。"

        result = "🔥 **近期热点动态**\n\n"

        # 按类型分组
        new_items = df[df['alert_type'] == 'new']
        rising_items = df[df['alert_type'] == 'rising']
        persistent_items = df[df['alert_type'] == 'persistent']

        if not new_items.empty:
            result += "**🆕 新上榜热点:**\n"
            for _, row in new_items.head(10).iterrows():
                source_emoji = '🔍' if row['source'] == 'google' else '🎵'
                result += f"{source_emoji} **{row['keyword']}** - {row['description']}\n"
            result += "\n"

        if not rising_items.empty:
            result += "**📈 快速上升:**\n"
            for _, row in rising_items.head(5).iterrows():
                source_emoji = '🔍' if row['source'] == 'google' else '🎵'
                result += f"{source_emoji} **{row['keyword']}** - {row['description']}\n"
            result += "\n"

        if not persistent_items.empty:
            result += "**🔄 持续热点:**\n"
            for _, row in persistent_items.head(5).iterrows():
                source_emoji = '🔍' if row['source'] == 'google' else '🎵'
                result += f"{source_emoji} **{row['keyword']}** - {row['description']}\n"

        return result

    except Exception as e:
        return f"查询出错: {e}"


@tool
def get_today_hotlist(source: str = "all"):
    """
    【今日热榜】
    获取今天的完整热搜榜单。

    参数:
    - source: "all"=全部, "google"=Google热搜, "douyin"=抖音热榜

    当用户问"今天热搜"、"抖音热榜"、"Google热搜"时使用。
    """
    if engine is None:
        return "❌ 数据库未连接"

    try:
        if source == "all":
            sql = text("""
                       SELECT keyword, source, ranking, hot_score
                       FROM trend_hotlist
                       WHERE trend_date = CURDATE()
                       ORDER BY source, ranking LIMIT 50
                       """)
        else:
            sql = text("""
                       SELECT keyword, source, ranking, hot_score
                       FROM trend_hotlist
                       WHERE trend_date = CURDATE()
                         AND source = :source
                       ORDER BY ranking LIMIT 10
                       """)

        params = {"source": source} if source != "all" else {}
        df = pd.read_sql(sql, engine, params=params)

        if df.empty:
            return "📭 今日暂无热榜数据。"

        result = f"📊 **今日热榜** ({datetime.now().strftime('%Y-%m-%d')})\n\n"

        for src in df['source'].unique():
            src_df = df[df['source'] == src].head(20)
            src_name = 'Google 热搜' if src == 'google' else '抖音热榜'
            src_emoji = '🔍' if src == 'google' else '🎵'

            result += f"**{src_emoji} {src_name} TOP20:**\n"
            for _, row in src_df.iterrows():
                score_text = f" ({row['hot_score']:,})" if row['hot_score'] > 0 else ""
                result += f"{row['ranking']}. {row['keyword']}{score_text}\n"
            result += "\n"

        return result

    except Exception as e:
        return f"查询出错: {e}"


@tool
def analyze_keyword_trend(keyword: str):
    """
    【关键词趋势分析】
    分析某个关键词的搜索趋势变化（近30天）。

    当用户问"XX热度怎么样"、"XX趋势"、"XX关注度"时使用。
    """
    if engine is None:
        return "❌ 数据库未连接"

    try:
        # 1. 查询趋势历史
        sql_trend = text("""
                         SELECT trend_date, source, trend_value
                         FROM trend_history
                         WHERE keyword LIKE :keyword
                           AND trend_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                         ORDER BY trend_date
                         """)

        df_trend = pd.read_sql(sql_trend, engine, params={"keyword": f"%{keyword}%"})

        # 2. 查询热榜出现情况
        sql_hotlist = text("""
                           SELECT trend_date, source, ranking
                           FROM trend_hotlist
                           WHERE keyword LIKE :keyword
                             AND trend_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                           ORDER BY trend_date
                           """)

        df_hotlist = pd.read_sql(sql_hotlist, engine, params={"keyword": f"%{keyword}%"})

        if df_trend.empty and df_hotlist.empty:
            return f"📭 未找到 **{keyword}** 的趋势数据。"

        result = f"📊 **{keyword}** 趋势分析\n\n"

        # 热榜出现情况
        if not df_hotlist.empty:
            days_on_list = df_hotlist['trend_date'].nunique()
            avg_rank = df_hotlist['ranking'].mean()
            best_rank = df_hotlist['ranking'].min()

            result += f"**热榜表现 (近30天):**\n"
            result += f"  • 上榜天数: {days_on_list} 天\n"
            result += f"  • 平均排名: #{avg_rank:.0f}\n"
            result += f"  • 最高排名: #{best_rank}\n\n"

        # 趋势指数
        if not df_trend.empty:
            for src in df_trend['source'].unique():
                src_df = df_trend[df_trend['source'] == src].sort_values('trend_date')

                if len(src_df) >= 7:
                    recent = src_df.tail(7)['trend_value'].mean()
                    earlier = src_df.head(7)['trend_value'].mean()
                    change = ((recent - earlier) / earlier * 100) if earlier > 0 else 0

                    trend_emoji = "📈" if change > 20 else ("📉" if change < -20 else "➡️")
                    src_name = 'Google' if src == 'google' else '抖音'

                    result += f"**{src_name}趋势指数:** {trend_emoji} {change:+.1f}%\n"
                    result += f"  当前: {recent:.0f} | 之前: {earlier:.0f}\n\n"

        return result

    except Exception as e:
        return f"查询出错: {e}"


@tool
def get_finance_related_trends():
    """
    【金融相关热点】
    获取近期与金融市场可能相关的热点话题。

    当用户问"有什么消息影响行情"、"最近有什么利好利空"时使用。
    系统会自动判断热点是否与金融相关。
    """
    if engine is None:
        return "❌ 数据库未连接"

    try:
        sql = text("""
                   SELECT keyword, source, alert_type, description, created_at
                   FROM trend_alert_v2
                   WHERE is_finance_related = 1
                     AND created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
                   ORDER BY created_at DESC LIMIT 20
                   """)

        df = pd.read_sql(sql, engine)

        if df.empty:
            return "✅ 近7天暂无发现与金融市场明显相关的新热点。"

        result = "💹 **金融相关热点** (近7天)\n\n"
        result += "以下热点可能与金融市场相关：\n\n"

        for _, row in df.iterrows():
            type_emoji = {'new': '🆕', 'rising': '📈', 'persistent': '🔄'}.get(row['alert_type'], '📌')
            source_emoji = '🔍' if row['source'] == 'google' else '🎵'

            result += f"{type_emoji}{source_emoji} **{row['keyword']}**\n"
            result += f"   {row['description']}\n\n"

        result += "\n💡 **提示**: 这些热点可能影响相关商品或股票，建议结合具体品种分析。"

        return result

    except Exception as e:
        return f"查询出错: {e}"


@tool
def search_hotlist_history(keyword: str, days: int = 7):
    """
    【热榜历史搜索】
    搜索某个关键词在热榜中的历史出现情况。

    当用户想知道某个话题是否上过热搜、什么时候上的时使用。
    """
    if engine is None:
        return "❌ 数据库未连接"

    try:
        sql = text("""
                   SELECT trend_date, source, ranking, hot_score
                   FROM trend_hotlist
                   WHERE keyword LIKE :keyword
                     AND trend_date >= DATE_SUB(CURDATE(), INTERVAL :days DAY)
                   ORDER BY trend_date DESC
                   """)

        df = pd.read_sql(sql, engine, params={"keyword": f"%{keyword}%", "days": days})

        if df.empty:
            return f"📭 近{days}天热榜中未找到 **{keyword}** 相关内容。"

        result = f"🔍 **{keyword}** 热榜历史 (近{days}天)\n\n"

        for _, row in df.iterrows():
            source_name = 'Google' if row['source'] == 'google' else '抖音'
            score_text = f" (热度:{row['hot_score']:,})" if row['hot_score'] > 0 else ""
            result += f"📅 {row['trend_date']} | {source_name} #{row['ranking']}{score_text}\n"

        return result

    except Exception as e:
        return f"查询出错: {e}"