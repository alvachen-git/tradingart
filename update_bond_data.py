import streamlit as st
import akshare as ak
import pandas as pd
from sqlalchemy import create_engine, text
import datetime
import os
from dotenv import load_dotenv

# 1. 初始化
load_dotenv(override=True)

# Tushare 初始化 (确保已配置 Token)


# --- 【安全修正】从环境变量读取数据库配置 ---
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

@st.cache_resource
def get_db_engine():
    db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    # 增加连接池配置，防止连接断开
    return create_engine(db_url, pool_recycle=3600, pool_pre_ping=True)

engine = get_db_engine()


def update_bond_yields():
    print("🌏 正在通过 AKShare 获取中美国债利差 (精准剔除版)...")

    try:
        # 1. 获取全量数据
        df = ak.bond_zh_us_rate()

        # 2. 精准寻找列名 (增加排除逻辑)
        col_date = None
        col_cn = None
        col_us = None

        for col in df.columns:
            # 找日期
            if "日期" in col:
                col_date = col

            # 找中国10年 (排除 "-2年", "利差")
            elif "中国" in col and "10年" in col:
                if "-" not in col and "利差" not in col:
                    col_cn = col

            # 找美国10年 (排除 "-2年", "利差")
            elif "美国" in col and "10年" in col:
                if "-" not in col and "利差" not in col:
                    col_us = col

        # 3. 检查是否找到
        if not (col_date and col_cn and col_us):
            print(f"❌ 错误：未找到纯净的 10 年期收益率列。")
            print(f"   当前所有列名: {df.columns.tolist()}")
            return

        print(f"   -> 锁定列名: 日期=[{col_date}], 中国=[{col_cn}], 美国=[{col_us}]")

        # 4. 提取并重命名
        df_final = df[[col_date, col_cn, col_us]].copy()
        df_final.rename(columns={
            col_date: 'trade_date',
            col_cn: 'cn_10y',
            col_us: 'us_10y'
        }, inplace=True)

        # 5. 数据清洗
        df_final['trade_date'] = pd.to_datetime(df_final['trade_date']).dt.strftime('%Y%m%d')
        start_date = (pd.Timestamp.now() - pd.Timedelta(days=365 * 3)).strftime('%Y%m%d')
        df_final = df_final[df_final['trade_date'] >= start_date]

        # 6. 存入数据库
        with engine.connect() as conn:
            conn.execute(text("""
                              CREATE TABLE IF NOT EXISTS macro_bond_yields
                              (
                                  trade_date
                                  VARCHAR
                              (
                                  8
                              ) PRIMARY KEY,
                                  cn_10y FLOAT,
                                  us_10y FLOAT
                                  );
                              """))

        df_final.to_sql('macro_bond_yields', engine, if_exists='replace', index=False)

        print(f"✅ 更新成功！最新数据 ({df_final.iloc[-1]['trade_date']}):")
        print(f"   🇨🇳 中国 10Y: {df_final.iloc[-1]['cn_10y']}%")
        print(f"   🇺🇸 美国 10Y: {df_final.iloc[-1]['us_10y']}%")

    except Exception as e:
        print(f"❌ 更新失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    update_bond_yields()