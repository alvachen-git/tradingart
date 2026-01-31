import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import streamlit as st
import traceback

# 1. 初始化配置
load_dotenv(override=True)

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")


@st.cache_resource
def get_db_engine():
    if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]): return None
    db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(db_url, pool_recycle=3600, pool_pre_ping=True)


engine = get_db_engine()


# ==========================================
#   功能 1: ETF 期权主力持仓防线分析 (反向侦探版)
# ==========================================
@st.cache_data(ttl=1200)
def get_etf_option_analysis(etf_code="510050", days=100):
    if engine is None: return None

    # 1. 准备代码格式
    base_code = etf_code.split('.')[0]  # 588000
    if "." not in etf_code:
        suffix = ".SZ" if etf_code.startswith("15") or etf_code.startswith("16") else ".SH"
        full_code = base_code + suffix
    else:
        full_code = etf_code

    # 🔥【新增】关联指数映射 (极大概率 underlying 存的是指数代码)
    # 科创50ETF(588000) -> 对应指数 000688.SH
    # 50ETF(510050) -> 对应指数 000016.SH
    # 300ETF(510300) -> 对应指数 000300.SH
    INDEX_MAP = {
        '588000': '000688.SH',
        '510050': '000016.SH',
        '510300': '000300.SH',
        '510500': '000905.SH',
        '159915': '399006.SZ'
    }
    related_index = INDEX_MAP.get(base_code, '')

    # 构造关键词
    cn_keyword = '科创' if '588000' in base_code else ''

    print(f"[*] [分析开始] 目标: {full_code} (关联指数: {related_index})")

    try:
        # 构造 SQL 查询条件 (三管齐下：查ETF代码 OR 查指数代码 OR 查名称)
        conditions = []
        conditions.append(f"b.underlying = '{full_code}'")  # 查 588000.SH
        conditions.append(f"b.underlying = '{base_code}'")  # 查 588000
        if related_index:
            conditions.append(f"b.underlying = '{related_index}'")  # 查 000688.SH (指数)
        if cn_keyword:
            conditions.append(f"b.name LIKE '%%{cn_keyword}%%'")  # 查名称

        where_clause = " OR ".join(conditions)

        # 2. 尝试标准查询
        date_check_sql = f"""
            SELECT DISTINCT d.trade_date 
            FROM option_daily d
            JOIN option_basic b ON d.ts_code = b.ts_code
            WHERE ({where_clause})
            ORDER BY d.trade_date DESC 
            LIMIT {days}
        """
        dates_df = pd.read_sql(date_check_sql, engine)

        # ==========================================
        # 🕵️‍♂️ 触发反向侦探模式
        # ==========================================
        if dates_df.empty:
            st.warning(f"⚠️ 常规匹配失败，正在进行【数据库反向采样】以寻找真相...")

            # 1. 既然 option_daily 有数据，我们直接抓几条最新的期权合约代码
            # 假设科创50期权代码通常以 1000xxxx 开头 (上交所期权)
            sample_sql = """
                         SELECT ts_code, trade_date \
                         FROM option_daily
                         ORDER BY trade_date DESC LIMIT 5 \
                         """
            sample_df = pd.read_sql(sample_sql, engine)

            if sample_df.empty:
                st.error("❌ 致命错误：option_daily 表是空的！请检查数据同步脚本。")
                return None

            sample_codes = sample_df['ts_code'].tolist()
            sample_codes_str = "'" + "','".join(sample_codes) + "'"

            # 2. 去 option_basic 查这几个代码的户口信息
            inspector_sql = f"""
                SELECT ts_code, name, underlying, exercise_price 
                FROM option_basic 
                WHERE ts_code IN ({sample_codes_str})
            """
            inspector_df = pd.read_sql(inspector_sql, engine)

            # 3. 展示结果给用户
            st.markdown("### 🔍 数据库实况侦查报告")
            st.write("我们在交易表中找到了以下最新合约，它们在基础表中的信息如下：")
            st.dataframe(inspector_df)

            st.error(f"""
            **诊断结论**：
            请看上表中的 `underlying` 列和 `name` 列。
            - 你的数据库里，科创50期权的 underlying 填的是什么？(可能是 '{related_index}' 或其他)
            - name 是怎么写的？

            **当前代码尝试搜索的条件是**：{where_clause}
            **原因**：你要找的 {full_code} 既不在 underlying 里，名字里也没匹配上。
            """)
            return None

        min_date = dates_df['trade_date'].min()

        # 3. 获取正式数据 (逻辑不变)
        sql = f"""
            SELECT 
                d.trade_date as date,
                b.call_put,
                b.exercise_price as strike,
                d.oi,
                d.close as price,
                d.ts_code as code
            FROM option_daily d
            JOIN option_basic b ON d.ts_code = b.ts_code
            WHERE ({where_clause})
              AND d.trade_date >= '{min_date}'
              AND d.oi > 0
        """
        df_raw = pd.read_sql(sql, engine)
        if df_raw.empty: return None

        df_raw['type'] = df_raw['call_put'].map({'C': '认购', 'P': '认沽'})

        # 4. 合并标的价格
        try:
            # 优先查ETF价格
            kline_sql = f"SELECT trade_date as date, close_price as underlying_price FROM stock_price WHERE ts_code='{full_code}' AND trade_date >= '{min_date}'"
            df_stock = pd.read_sql(kline_sql, engine)

            # 如果没查到，查指数价格 (科创50ETF通常跟随 000688.SH)
            if df_stock.empty:
                idx_code = related_index if related_index else '000688.SH'
                kline_sql = f"SELECT trade_date as date, close_price as underlying_price FROM index_price WHERE ts_code='{idx_code}' AND trade_date >= '{min_date}'"
                df_stock = pd.read_sql(kline_sql, engine)

            if not df_stock.empty:
                df_raw = pd.merge(df_raw, df_stock, on='date', how='left')
            else:
                df_raw['underlying_price'] = np.nan
        except:
            df_raw['underlying_price'] = np.nan

        # 5. 分组处理
        daily_candidates_map = {}
        grouped = df_raw.groupby(['date', 'type'])

        for (date, otype), group in grouped:
            if group.empty: continue
            group = group.sort_values('oi', ascending=False)
            u_price = group['underlying_price'].iloc[0] if 'underlying_price' in group.columns else np.nan
            candidates_list = []

            if pd.notna(u_price) and u_price > 0:
                filtered = group[abs(group['strike'] - u_price) / u_price <= 0.1]
                top3 = filtered.head(3) if not filtered.empty else group.head(3)
            else:
                top3 = group.head(3)

            for _, row in top3.iterrows():
                candidates_list.append({
                    'strike': row['strike'], 'oi': row['oi'], 'price': row['price'], 'code': row['code']
                })
            if date not in daily_candidates_map: daily_candidates_map[date] = {}
            daily_candidates_map[date][otype] = candidates_list

        # 6. 输出
        final_results = []
        sorted_dates = sorted(daily_candidates_map.keys())
        for otype_raw in ['认购', '认沽']:
            type_label = f"{otype_raw} ({'压力' if otype_raw == '认购' else '支撑'})"
            last_strike = None
            for date in sorted_dates:
                day_data = daily_candidates_map[date]
                if otype_raw not in day_data: continue
                candidates = day_data[otype_raw]
                if not candidates: continue
                selected = candidates[0]
                if last_strike is not None:
                    if abs(selected['strike'] - last_strike) / last_strike > 0.05 and len(candidates) > 1:
                        if abs(candidates[1]['strike'] - last_strike) / last_strike <= 0.05: selected = candidates[1]
                last_strike = selected['strike']
                final_results.append({
                    'date': date, 'type': type_label, 'strike': selected['strike'], 'oi': selected['oi'],
                    'price': selected['price'], 'code': selected['code']
                })

        return pd.DataFrame(final_results)

    except Exception as e:
        st.error(f"系统异常: {e}")
        traceback.print_exc()
        return None


# ==========================================
#   功能 2: IV Rank 计算
# ==========================================
@st.cache_data(ttl=600)
def get_iv_rank_data(etf_code, window=252):
    if engine is None: return None
    if "." not in etf_code:
        etf_code += ".SZ" if etf_code.startswith("15") else ".SH"
    try:
        sql = f"SELECT trade_date, iv FROM etf_iv_history WHERE etf_code='{etf_code}' ORDER BY trade_date DESC LIMIT {window}"
        df = pd.read_sql(sql, engine)
        if df.empty or len(df) < 10: return None
        current = df.iloc[0]['iv'];
        high = df['iv'].max();
        low = df['iv'].min()
        rank = (current - low) / (high - low) * 100 if high != low else 0
        pct = (len(df[df['iv'] < current]) / len(df)) * 100
        return {"current_iv": current, "iv_rank": rank, "iv_percentile": pct, "max_iv": high, "min_iv": low,
                "date": df.iloc[0]['trade_date']}
    except:
        return None


# ==========================================
#   功能 3: 获取绘图数据
# ==========================================
@st.cache_data(ttl=600)
def get_kline_and_iv_data(etf_code, limit=100):
    if engine is None: return pd.DataFrame(), pd.DataFrame()
    if "." not in etf_code: etf_code += ".SZ" if etf_code.startswith("15") else ".SH"
    try:
        iv_sql = f"SELECT * FROM etf_iv_history WHERE etf_code='{etf_code}' ORDER BY trade_date"
        df_iv = pd.read_sql(iv_sql, engine)
        kline_sql = f"SELECT trade_date, open_price as open, high_price as high, low_price as low, close_price as close FROM stock_price WHERE ts_code='{etf_code}' ORDER BY trade_date DESC LIMIT {limit}"
        df_k = pd.read_sql(kline_sql, engine)
        if df_k.empty:
            kline_sql = f"SELECT trade_date, open_price as open, high_price as high, low_price as low, close_price as close FROM index_price WHERE ts_code='{etf_code}' ORDER BY trade_date DESC LIMIT {limit}"
            df_k = pd.read_sql(kline_sql, engine)
        return df_k.sort_values('trade_date'), df_iv
    except:
        return pd.DataFrame(), pd.DataFrame()