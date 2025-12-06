import akshare as ak
import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# 1. 初始化
load_dotenv(override=True)

# --- 【安全修正】从环境变量读取数据库配置 ---
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")


def get_db_engine():
    db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    # 增加连接池配置，防止连接断开
    return create_engine(db_url, pool_recycle=3600, pool_pre_ping=True)

engine = get_db_engine()

def find_col(df, keywords):
    """辅助函数：在 DataFrame 中模糊查找列名"""
    for col in df.columns:
        for kw in keywords:
            if kw in col:
                return col
    return None


def update_cpi_ppi_final():
    print("🌏 正在更新中国 CPI / PPI 数据 (V8.0 靶向治疗版)...")

    try:
        # ==========================================
        # 1. 获取 CPI
        # ==========================================
        print("   -> 下载 CPI 数据...")
        df_cpi = ak.macro_china_cpi_monthly()

        # 🎯 靶向提取：根据诊断结果，取索引 1(日期) 和 2(今值)
        # 原始列: [商品, 日期, 今值, 预测值, 前值]
        df_cpi = df_cpi.iloc[:, [1, 2]].copy()
        df_cpi.columns = ['date', 'cpi_yoy']

        # 清洗
        df_cpi['date'] = pd.to_datetime(df_cpi['date'], errors='coerce')
        df_cpi.dropna(subset=['date'], inplace=True)
        # 对齐到当月1号
        df_cpi['date'] = df_cpi['date'].apply(lambda x: x.replace(day=1))

        # ==========================================
        # 2. 获取 PPI
        # ==========================================
        print("   -> 下载 PPI 数据...")
        try:
            # 优先尝试 yearly (虽然诊断显示它返回的也是这种格式)
            df_ppi = ak.macro_china_ppi_yearly()
        except:
            df_ppi = ak.macro_china_ppi()

        # 🎯 靶向提取：同样取索引 1(日期) 和 2(今值)
        df_ppi = df_ppi.iloc[:, [1, 2]].copy()
        df_ppi.columns = ['date', 'ppi_yoy']

        # 清洗
        df_ppi['date'] = pd.to_datetime(df_ppi['date'], errors='coerce')
        df_ppi.dropna(subset=['date'], inplace=True)
        # 对齐到当月1号
        df_ppi['date'] = df_ppi['date'].apply(lambda x: x.replace(day=1))

        # ==========================================
        # 3. 合并
        # ==========================================
        print("   -> 合并数据...")
        df_merge = pd.merge(df_cpi, df_ppi, on='date', how='inner')

        # 只要最近 15 年
        df_merge.sort_values('date', inplace=True)
        df_merge = df_merge[df_merge['date'] >= '2010-01-01']

        df_merge['date'] = df_merge['date'].dt.strftime('%Y-%m-%d')

        if df_merge.empty:
            print("❌ 依然为空！请检查日期范围。")
            return

        # ==========================================
        # 4. 存库
        # ==========================================
        with engine.connect() as conn:
            conn.execute(text("""
                              CREATE TABLE IF NOT EXISTS macro_cpi_ppi
                              (
                                  date
                                  VARCHAR
                              (
                                  10
                              ) PRIMARY KEY,
                                  cpi_yoy FLOAT,
                                  ppi_yoy FLOAT
                                  );
                              """))

        df_merge.to_sql('macro_cpi_ppi', engine, if_exists='replace', index=False)

        print(f"✅ 完美成功！最新数据 ({df_merge.iloc[-1]['date']}):")
        print(f"   CPI: {df_merge.iloc[-1]['cpi_yoy']}%")
        print(f"   PPI: {df_merge.iloc[-1]['ppi_yoy']}%")

    except Exception as e:
        print(f"❌ 程序崩溃: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    update_cpi_ppi_final()