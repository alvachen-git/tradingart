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
# Tushare 初始化 (确保已配置 Token)
ts_token = os.getenv("TUSHARE_TOKEN")
pro = ts.pro_api()

# 数据库连接 (你的云端 MySQL)
# 1. 初始化
load_dotenv(override=True)

# --- 【安全修正】从环境变量读取数据库配置 ---
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# 检查配置是否读取成功 (防止 .env 没配好报错)
if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
    raise ValueError("❌ 数据库配置缺失！请检查 .env 文件中是否包含 DB_HOST, DB_USER, DB_PASSWORD 等信息。")

# 【修改点】加上这个装饰器
@st.cache_resource
def get_db_engine():
    db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    # 增加连接池配置，防止连接断开
    return create_engine(db_url, pool_recycle=3600, pool_pre_ping=True)

engine = get_db_engine()


def get_china_us_spread():
    # ❌ 暂时注释掉 try，让报错直接爆出来！
    # try:
    print("🔍 正在执行 SQL 查询中美国债数据...")

    # 1. 简单的 SQL
    sql = "SELECT * FROM macro_bond_yields"

    # 2. 读取
    df = pd.read_sql(sql, engine)

    print(f"   -> 查询结果: {len(df)} 条数据")

    if df.empty:
        return pd.DataFrame()

    # 3. 简单的数据处理
    # 确保列名是小写的 (防止数据库是大写导致 KeyError)
    df.columns = df.columns.str.lower()

    # 转换日期
    df['date'] = pd.to_datetime(df['trade_date'].astype(str))

    # 计算利差
    if 'cn_10y' in df.columns and 'us_10y' in df.columns:
        df['spread'] = (df['cn_10y'] - df['us_10y']) * 100
        return df
    else:
        print(f"❌ 列名对不上！当前列名: {df.columns.tolist()}")
        return pd.DataFrame()


# except Exception as e:
#     print(f"❌ 严重报错: {e}")
#     return pd.DataFrame()


def get_gold_silver_ratio():
    """
        【真实版】从数据库读取黄金和白银价格，计算金银比
        """
    try:
        # 1. 编写 SQL 查询
        # 🟢 修正：直接查找 'au0' 和 'ag0'
        # 🟢 修正：使用 correct column names (trade_date, close_price)
        sql = """
              SELECT trade_date as date, ts_code as symbol, close_price as close
              FROM futures_price
              WHERE ts_code IN ('au0' \
                  , 'ag0')
                AND trade_date >= '20240101' -- 建议日期稍微往前一点，保证有数据
              ORDER BY trade_date ASC \
              """

        # 2. 读取数据
        df = pd.read_sql(sql, engine)

        if df.empty:
            print("❌ 依然查不到数据，请检查 futures_price 表是否有 au0/ag0 的数据。")
            return pd.DataFrame()

        # 3. 数据透视 (Pivot)
        # 把长表变成宽表
        df_pivot = df.pivot(index='date', columns='symbol', values='close').dropna()

        # 4. 计算金银比
        # 你的数据库代码是 au0 和 ag0，直接用这两个名字取列
        if 'au0' in df_pivot.columns and 'ag0' in df_pivot.columns:
            # 黄金(元/克) / (白银(元/千克) / 1000)
            df_pivot['ratio'] = df_pivot['au0'] / (df_pivot['ag0'] / 1000)

            # 整理返回
            return df_pivot.reset_index()
        else:
            print("❌ 数据透视后缺少 au0 或 ag0 列")
            return pd.DataFrame()

    except Exception as e:
        print(f"❌ 计算失败: {e}")
        return pd.DataFrame()


def get_cpi_ppi_data():
    """
    从数据库读取 CPI/PPI 数据，并计算剪刀差
    """
    try:
        # 1. 读库
        sql = "SELECT * FROM macro_cpi_ppi ORDER BY date ASC"
        df = pd.read_sql(sql, engine)

        if df.empty:
            return pd.DataFrame()

        # 2. 格式转换
        df['date'] = pd.to_datetime(df['date'])

        # 3. 计算【剪刀差】(PPI - CPI)
        # 逻辑：剪刀差扩大，通常意味着上游工业利润挤压下游消费；剪刀差缩小或负值，意味着利润向下游转移。
        df['scissor'] = df['ppi_yoy'] - df['cpi_yoy']
        # 这样取到的 .iloc[-1] 就一定是最近一次【已发布】的数据
        df.dropna(subset=['cpi_yoy', 'ppi_yoy'], inplace=True)

        return df

    except Exception as e:
        print(f"读取 CPI/PPI 失败: {e}")
        return pd.DataFrame()

def get_dashboard_metrics():
        """
        获取宏观仪表盘所需的 4 个核心指标 (最新值 + 较上期变化)
        返回一个字典，例如: {'spread': {'val': -221, 'delta': 5}, ...}
        """
        metrics = {}

        try:
            # 1. 中美国债利差 (来自 macro_bond_yields 表)
            sql_bond = "SELECT * FROM macro_bond_yields ORDER BY trade_date DESC LIMIT 2"
            df_bond = pd.read_sql(sql_bond, engine)

            if len(df_bond) >= 2:
                latest = df_bond.iloc[0]
                prev = df_bond.iloc[1]

                # 计算利差 (CN - US) * 100
                spread_now = (latest['cn_10y'] - latest['us_10y']) * 100
                spread_prev = (prev['cn_10y'] - prev['us_10y']) * 100

                metrics['spread'] = {
                    'value': f"{spread_now:.0f} BP",
                    'delta': f"{spread_now - spread_prev:.0f} BP"
                }

            # 2. 金银比 (调用现有的 get_gold_silver_ratio 函数)
            # 注意：这里我们复用逻辑，但只取最后两行，避免重复写 SQL
            df_gs = get_gold_silver_ratio()
            if not df_gs.empty and len(df_gs) >= 2:
                latest = df_gs.iloc[-1]
                prev = df_gs.iloc[-2]

                metrics['gs_ratio'] = {
                    'value': f"{latest['ratio']:.1f}",
                    'delta': f"{latest['ratio'] - prev['ratio']:.1f}"
                }

            # 3. PPI 同比 (来自 macro_cpi_ppi 表)
            sql_ppi = "SELECT * FROM macro_cpi_ppi ORDER BY date DESC LIMIT 2"
            df_ppi = pd.read_sql(sql_ppi, engine)

            if len(df_ppi) >= 2:
                latest = df_ppi.iloc[0]
                prev = df_ppi.iloc[1]

                metrics['ppi'] = {
                    'value': f"{latest['ppi_yoy']}%",
                    'delta': f"{latest['ppi_yoy'] - prev['ppi_yoy']:.1f}%"
                }

                # 🟢 D. 美元指数 (DXY) - 新浪底层接口直连
                # ----------------------------------------------------
                try:
                    # DINIW 是美元指数的标准代码
                    url = "http://hq.sinajs.cn/list=DINIW"
                    headers = {"Referer": "http://finance.sina.com.cn/"}

                    # 设定短超时，防止卡顿页面
                    resp = requests.get(url, headers=headers, timeout=2)

                    # 返回格式: var hq_str_DINIW="美元指数,106.32,0.05,..."
                    content = resp.text

                    if '="' in content:
                        # 提取引号中的内容
                        data_str = content.split('="')[1].split('";')[0]
                        data_parts = data_str.split(',')

                        # 新浪 DINIW 格式通常是: [名称, 最新价, 涨跌额, ...]
                        # 即使格式微调，最新价通常在 index 1
                        price = float(data_parts[1])
                        change = float(data_parts[2])

                        metrics['dxy'] = {
                            'value': f"{price:.2f}",
                            'delta': f"{change:.2f}"
                        }
                    else:
                        metrics['dxy'] = {'value': 'Error', 'delta': '0'}

                except Exception as e:
                    print(f"新浪接口请求失败: {e}")
                    # 失败时显示横杠，或者你可以填 99.01 兜底
                    metrics['dxy'] = {'value': '-', 'delta': '0'}
                # ----------------------------------------------------

        except Exception as e:
            print(f"仪表盘数据获取失败: {e}")

        return metrics