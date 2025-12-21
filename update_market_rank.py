import pandas as pd
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime, timedelta
import data_engine as de  # 復用您現有的計算邏輯

# 1. 初始化
load_dotenv(override=True)
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)


def calculate_and_save_rank(days=200, top_n=10):
    print(f"[*] 開始計算全市場近 {days} 天風雲榜...")

    # 1. 定義要掃描的品種 (全市場)
    # 注意：這裡應該包含您數據庫裡有的所有品種
    target_symbols = [
        'lc','si','ps','rb','hc','i','m','c','p','y','oi','jm','jd','ta','ma','eb','eg','cf','sr','ap',
        'fg','sa','sp','rm','sh','ao','IF','IM','IC','T','au','ag','cu','al','zn','ru','sn','sc','bu'
    ]

    all_scores = []
    cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')

    # 2. 循環計算每個品種
    for symbol in target_symbols:
        try:
            # 這裡復用 data_engine 裡的單品種計算邏輯
            # 注意：calculate_broker_rankings 內部可能用了緩存，但腳本運行時無所謂
            df = de.calculate_broker_rankings(symbol)

            if not df.empty:
                # 過濾日期
                df_recent = df[df['trade_date'] >= cutoff_date]
                if not df_recent.empty:
                    all_scores.append(df_recent[['broker', 'score']])
            print(f"  > {symbol} 計算完成")
        except Exception as e:
            print(f"  ! {symbol} 出錯: {e}")

    if not all_scores:
        print("[-] 無有效數據")
        return

    if not all_scores:
        print("[-] 无有效数据")
        return

        # 3. 汇总计算
    big_df = pd.concat(all_scores)

    # --- 【关键修改】在这里清洗机构名称 ---
    # 必须在 groupby 之前清洗，这样 "永安" 和 "永安(代客)" 才能合并成一个
    big_df['broker'] = big_df['broker'].str.replace(r'[（\(]代客[）\)]', '', regex=True).str.strip()

    # 分组求和 (清洗后，同名机构的分数会自动加在一起)
    rank_df = big_df.groupby('broker')['score'].sum().reset_index()

    # 4. 提取前 N 名
    # 盈利榜
    winners = rank_df.sort_values('score', ascending=False).head(top_n).copy()
    winners['rank_type'] = 'WIN'

    # 虧損榜
    losers = rank_df.sort_values('score', ascending=True).head(top_n).copy()
    losers['rank_type'] = 'LOSE'

    final_df = pd.concat([winners, losers])

    # 添加日期標記 (今天的日期，表示這是截至今天的排名)
    today_str = datetime.now().strftime('%Y%m%d')
    final_df['trade_date'] = today_str

    # 5. 入庫 (先刪後寫)
    try:
        with engine.connect() as conn:
            conn.execute(text(f"DELETE FROM market_rank_daily WHERE trade_date='{today_str}'"))
            conn.commit()

        final_df.to_sql('market_rank_daily', engine, if_exists='append', index=False, dtype={
            # 映射不需要特別指定，pandas 會自動匹配
        })
        print(f"[√] 排行榜更新成功！存入 {len(final_df)} 條記錄 (日期: {today_str})")

    except Exception as e:
        print(f"[X] 入庫失敗: {e}")


if __name__ == "__main__":
    calculate_and_save_rank()