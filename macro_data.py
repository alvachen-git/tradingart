import tushare as ts
import streamlit as st
import pandas as pd
import akshare as ak
import requests
import re
import datetime
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
# Tushare 鍒濆鍖?(纭繚宸查厤缃?Token)
ts_token = os.getenv("TUSHARE_TOKEN")
pro = ts.pro_api()

# 鏁版嵁搴撹繛鎺?(浣犵殑浜戠 MySQL)
# 1. 鍒濆鍖?
load_dotenv(override=True)

# --- 銆愬畨鍏ㄤ慨姝ｃ€戜粠鐜鍙橀噺璇诲彇鏁版嵁搴撻厤缃?---
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# 妫€鏌ラ厤缃槸鍚﹁鍙栨垚鍔?(闃叉 .env 娌￠厤濂芥姤閿?
if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
    raise ValueError("数据库配置缺失，请检查 .env 中的 DB_HOST / DB_USER / DB_PASSWORD / DB_NAME")

# 銆愪慨鏀圭偣銆戝姞涓婅繖涓楗板櫒
@st.cache_resource
def get_db_engine():
    db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    # 澧炲姞杩炴帴姹犻厤缃紝闃叉杩炴帴鏂紑
    return create_engine(db_url, pool_recycle=3600, pool_pre_ping=True)

engine = get_db_engine()


def get_china_us_spread():
    # 鉂?鏆傛椂娉ㄩ噴鎺?try锛岃鎶ラ敊鐩存帴鐖嗗嚭鏉ワ紒
    # try:
    print("馃攳 姝ｅ湪鎵ц SQL 鏌ヨ涓編鍥藉€烘暟鎹?..")

    # 1. 绠€鍗曠殑 SQL
    sql = "SELECT * FROM macro_bond_yields"

    # 2. 璇诲彇
    df = pd.read_sql(sql, engine)

    print(f"   -> 查询结果: {len(df)} 条数据")

    if df.empty:
        return pd.DataFrame()

    # 3. 绠€鍗曠殑鏁版嵁澶勭悊
    # 纭繚鍒楀悕鏄皬鍐欑殑 (闃叉鏁版嵁搴撴槸澶у啓瀵艰嚧 KeyError)
    df.columns = df.columns.str.lower()

    # 杞崲鏃ユ湡
    df['date'] = pd.to_datetime(df['trade_date'].astype(str))

    # 璁＄畻鍒╁樊
    if 'cn_10y' in df.columns and 'us_10y' in df.columns:
        df['spread'] = (df['cn_10y'] - df['us_10y']) * 100
        return df
    else:
        print(f"鉂?鍒楀悕瀵逛笉涓婏紒褰撳墠鍒楀悕: {df.columns.tolist()}")
        return pd.DataFrame()


# except Exception as e:
#     print(f"鉂?涓ラ噸鎶ラ敊: {e}")
#     return pd.DataFrame()


def get_gold_silver_ratio():
    """
    銆愭牳蹇冧慨鏀广€戜粠鏁版嵁搴撹鍙栭粍閲?au)鍜岀櫧閾?ag)涓诲姏杩炵画浠锋牸锛岃绠楅噾閾舵瘮
    """
    try:
        # 1. 淇敼 SQL锛氱洿鎺ユ煡鎵?'au' 鍜?'ag' (瀵瑰簲鏂扮殑涓诲姏杩炵画浠ｇ爜)
        sql = """
              SELECT trade_date as date, ts_code as symbol, close_price as close
              FROM futures_price
              WHERE ts_code IN ('au', 'ag') 
                AND trade_date >= '20230101'
              ORDER BY trade_date ASC
              """

        df = pd.read_sql(sql, engine)

        if df.empty:
            print("未查询到 au/ag 数据，请检查 futures_price 表。")
            return pd.DataFrame()

        # 2. 鏁版嵁閫忚 (Long -> Wide)
        df_pivot = df.pivot(index='date', columns='symbol', values='close').dropna()

        # 3. 璁＄畻閲戦摱姣?(娉ㄦ剰鍒楀悕鐜板湪鏄?au 鍜?ag)
        # 鍏煎澶у皬鍐?(鏁版嵁搴撳彲鑳芥槸 au 鎴?AU)
        # 鍏堢粺涓€杞皬鍐欏垪鍚嶆柟渚垮鐞?
        df_pivot.columns = df_pivot.columns.str.lower()

        if 'au' in df_pivot.columns and 'ag' in df_pivot.columns:
            # 榛勯噾(鍏?鍏? / (鐧介摱(鍏?鍗冨厠) / 1000) -> 缁熶竴鍗曚綅鍚庣浉姣?
            # 鐧介摱鎶ヤ环閫氬父鏄?鍏?鍗冨厠锛岄粍閲戞槸 鍏?鍏?
            # 閲戦摱姣?= (榛勯噾浠锋牸) / (鐧介摱浠锋牸 / 1000)
            df_pivot['ratio'] = df_pivot['au'] / (df_pivot['ag'] / 1000)
            return df_pivot.reset_index()
        else:
            print(f"鉂?鏁版嵁缂哄け锛屽綋鍓嶅垪: {df_pivot.columns}")
            return pd.DataFrame()

    except Exception as e:
        print(f"鉂?閲戦摱姣旇绠楀け璐? {e}")
        return pd.DataFrame()


def get_cpi_ppi_data():
    """
    浠庢暟鎹簱璇诲彇 CPI/PPI 鏁版嵁锛屽苟璁＄畻鍓垁宸?
    """
    try:
        # 1. 璇诲簱
        sql = "SELECT * FROM macro_cpi_ppi ORDER BY date ASC"
        df = pd.read_sql(sql, engine)

        if df.empty:
            return pd.DataFrame()

        # 2. 鏍煎紡杞崲
        df['date'] = pd.to_datetime(df['date'])

        # 3. 璁＄畻銆愬壀鍒€宸€?PPI - CPI)
        # 閫昏緫锛氬壀鍒€宸墿澶э紝閫氬父鎰忓懗鐫€涓婃父宸ヤ笟鍒╂鼎鎸ゅ帇涓嬫父娑堣垂锛涘壀鍒€宸缉灏忔垨璐熷€硷紝鎰忓懗鐫€鍒╂鼎鍚戜笅娓歌浆绉汇€?
        df['scissor'] = df['ppi_yoy'] - df['cpi_yoy']
        # 杩欐牱鍙栧埌鐨?.iloc[-1] 灏变竴瀹氭槸鏈€杩戜竴娆°€愬凡鍙戝竷銆戠殑鏁版嵁
        df.dropna(subset=['cpi_yoy', 'ppi_yoy'], inplace=True)

        return df

    except Exception as e:
        print(f"璇诲彇 CPI/PPI 澶辫触: {e}")
        return pd.DataFrame()

def get_dashboard_metrics():
    """
    获取宏观仪表盘核心指标（最新值 + 与上一期变化）。
    统一口径：美元指数仅使用 macro_daily 表中的 DXY。
    """
    metrics = {}

    try:
        # 1. 中美国债利差（来自 macro_bond_yields）
        sql_bond = "SELECT * FROM macro_bond_yields ORDER BY trade_date DESC LIMIT 2"
        df_bond = pd.read_sql(sql_bond, engine)

        if len(df_bond) >= 2:
            latest = df_bond.iloc[0]
            prev = df_bond.iloc[1]
            spread_now = (latest['cn_10y'] - latest['us_10y']) * 100
            spread_prev = (prev['cn_10y'] - prev['us_10y']) * 100
            metrics['spread'] = {
                'value': f"{spread_now:.0f} BP",
                'delta': f"{spread_now - spread_prev:.0f} BP"
            }

        # 2. 金银比
        df_gs = get_gold_silver_ratio()
        if not df_gs.empty and len(df_gs) >= 2:
            latest = df_gs.iloc[-1]
            prev = df_gs.iloc[-2]
            metrics['gs_ratio'] = {
                'value': f"{latest['ratio']:.1f}",
                'delta': f"{latest['ratio'] - prev['ratio']:.1f}"
            }

        # 3. PPI同比
        sql_ppi = "SELECT * FROM macro_cpi_ppi ORDER BY date DESC LIMIT 2"
        df_ppi = pd.read_sql(sql_ppi, engine)
        if len(df_ppi) >= 2:
            latest = df_ppi.iloc[0]
            prev = df_ppi.iloc[1]
            metrics['ppi'] = {
                'value': f"{latest['ppi_yoy']}%",
                'delta': f"{latest['ppi_yoy'] - prev['ppi_yoy']:.1f}%"
            }

        # 4. 美元指数 DXY（统一从 macro_daily 读取）
        sql_dxy = """
            SELECT trade_date, close_value, change_value, change_pct
            FROM macro_daily
            WHERE indicator_code = 'DXY'
            ORDER BY trade_date DESC
            LIMIT 2
        """
        df_dxy = pd.read_sql(sql_dxy, engine)

        if len(df_dxy) >= 1:
            latest = df_dxy.iloc[0]
            prev = df_dxy.iloc[1] if len(df_dxy) >= 2 else None

            close_val = pd.to_numeric(latest.get('close_value'), errors='coerce')
            change_val = pd.to_numeric(latest.get('change_value'), errors='coerce')

            # 若 change_value 缺失，使用前一日收盘差值兜底。
            if pd.isna(change_val) and prev is not None:
                prev_close = pd.to_numeric(prev.get('close_value'), errors='coerce')
                if pd.notna(prev_close) and pd.notna(close_val):
                    change_val = close_val - prev_close

            if pd.isna(change_val):
                change_val = 0.0

            if pd.notna(close_val):
                metrics['dxy'] = {
                    'value': f"{float(close_val):.2f}",
                    'delta': f"{float(change_val):+.2f}"
                }
            else:
                metrics['dxy'] = {'value': '-', 'delta': '0.00'}
        else:
            metrics['dxy'] = {'value': '-', 'delta': '0.00'}

    except Exception as e:
        print(f"仪表盘数据获取失败: {e}")

    return metrics
