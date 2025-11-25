import pandas as pd
import akshare as ak
from sqlalchemy import create_engine, types, text
from datetime import datetime
import time

# --- 1. 配置数据库连接 ---
DB_USER = 'root'
DB_PASSWORD = 'alva13557941'
DB_HOST = '39.102.215.198'
DB_PORT = '3306'
DB_NAME = 'finance_data'

db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)


def save_futures_price(symbol="lc0", name="碳酸锂", start_date=None, end_date=None):
    """
    获取期货主连日线行情，并存入数据库 (含涨跌幅计算)
    """
    print(f"[*] 正在从新浪财经获取 {name} ({symbol}) 的全量历史行情...")

    try:
        # 1. 调用 AkShare 接口 (新浪源)
        df = ak.futures_zh_daily_sina(symbol=symbol)

        if df.empty:
            print(f"[!] 未获取到 {name} 的数据，请检查代码。")
            return

        # 2. 重命名列
        # 新浪返回: date, open, high, low, close, volume, hold, settle
        df = df.rename(columns={
            'date': 'trade_date',
            'open': 'open_price',
            'high': 'high_price',
            'low': 'low_price',
            'close': 'close_price',
            'volume': 'vol',
            'hold': 'oi',
            'settle': 'settle_price'  # 虽然这里加了映射，但如果是旧表结构可能会被忽略
        })

        # 3. 添加元数据
        df['ts_code'] = symbol
        df['name'] = name

        # 格式化日期 (为了后续排序和过滤)
        df['trade_date'] = pd.to_datetime(df['trade_date'])

        # --- 【关键修改】计算涨跌幅 ---
        # 必须先按日期排序，否则计算会错
        df = df.sort_values('trade_date')

        # 计算公式：(今收 - 昨收) / 昨收
        # pct_change() 是 Pandas 自带的函数，自动计算这一行和上一行的变化率
        df['pct_chg'] = df['close_price'].pct_change()

        # 第一天的数据因为没有“昨天”，结果会是 NaN (空)，我们填 0
        df['pct_chg'] = df['pct_chg'].fillna(0)

        # 格式化回字符串
        df['trade_date'] = df['trade_date'].dt.strftime('%Y%m%d')

        # 4. 时间过滤
        if start_date:
            df = df[df['trade_date'] >= start_date]
        if end_date:
            df = df[df['trade_date'] <= end_date]

        print(f"    [+] 准备写入 {len(df)} 条数据...")

        # --- 5. 筛选列 ---
        # 确保只写入我们需要的列 (防止数据库报错)
        # 注意：我们这里不写入 settle_price，除非您确定数据库表里加了这个字段
        # 这里只加上 pct_chg
        cols_to_save = ['trade_date', 'ts_code', 'name', 'open_price', 'high_price', 'low_price', 'close_price', 'vol',
                        'oi', 'pct_chg']

        # 如果数据源里有这些列才保留
        df_save = df[cols_to_save].copy()

        # 6. 存入数据库
        df_save.to_sql(
            'futures_price',
            engine,
            if_exists='append',
            index=False,
            dtype={
                'trade_date': types.VARCHAR(8),
                'ts_code': types.VARCHAR(10),
                'name': types.VARCHAR(50),
                'open_price': types.Float(),
                'high_price': types.Float(),
                'low_price': types.Float(),
                'close_price': types.Float(),
                'vol': types.BigInteger(),
                'oi': types.BigInteger(),
                'pct_chg': types.Float()  # <--- 新增字段类型
            }
        )
        print(f"[√] {name} 数据已追加写入！")

    except Exception as e:
        print(f"[X] 发生错误: {e}")


if __name__ == "__main__":
    # --- 【关键步骤】重建表结构 ---
    # 因为我们要增加新的一列 'pct_chg'，必须把旧的表删掉重新建
    # 否则直接 append 会报错 (因为列数对不上)
    try:
        with engine.connect() as conn:
            # 删除旧表
            conn.execute(text("DROP TABLE IF EXISTS futures_price"))
            print("[-] 旧表已删除，准备重新构建全量数据...")

            # 重新建表 (包含 pct_chg 字段)
            # 虽然 to_sql 会自动建表，但手动建表可以更好地控制字段类型和注释
            create_sql = """
                         CREATE TABLE futures_price \
                         ( \
                             trade_date  VARCHAR(8), \
                             ts_code     VARCHAR(10), \
                             name        VARCHAR(50), \
                             open_price  FLOAT, \
                             high_price  FLOAT, \
                             low_price   FLOAT, \
                             close_price FLOAT, \
                             vol         BIGINT, \
                             oi          BIGINT, \
                             pct_chg     FLOAT COMMENT '涨跌幅', \
                             PRIMARY KEY (trade_date, ts_code)
                         ) \
                         """
            conn.execute(text(create_sql))
            print("[+] 新表结构创建成功 (含 pct_chg 列)")

    except Exception as e:
        print(f"初始化表结构失败: {e}")

    # --- 批量写入 ---
    # 设定过滤时间 (例如只取2023年以后的)
    FILTER_START = '20230102'

    # 1. 股指
    save_futures_price(symbol="ih0", name="上证50", start_date=FILTER_START)
    save_futures_price(symbol="if0", name="沪深300", start_date=FILTER_START)
    save_futures_price(symbol="ic0", name="中证500", start_date=FILTER_START)
    save_futures_price(symbol="im0", name="中证1000", start_date=FILTER_START)
    save_futures_price(symbol="t0", name="10年期国债", start_date=FILTER_START)
    save_futures_price(symbol="tl0", name="30年期国债", start_date=FILTER_START)
    save_futures_price(symbol="ts0", name="2年期国债", start_date=FILTER_START)

    # 2. 广期所
    save_futures_price(symbol="lc0", name="碳酸锂", start_date=FILTER_START)
    save_futures_price(symbol="si0", name="工业硅", start_date=FILTER_START)
    save_futures_price(symbol="ps0", name="多晶硅", start_date=FILTER_START)

    # 3. 上期所
    save_futures_price(symbol="cu0", name="沪铜", start_date=FILTER_START)
    save_futures_price(symbol="al0", name="沪铝", start_date=FILTER_START)
    save_futures_price(symbol="zn0", name="沪锌", start_date=FILTER_START)
    save_futures_price(symbol="au0", name="沪金", start_date=FILTER_START)
    save_futures_price(symbol="ag0", name="沪银", start_date=FILTER_START)
    save_futures_price(symbol="ni0", name="沪镍", start_date=FILTER_START)
    save_futures_price(symbol="ao0", name="氧化铝", start_date=FILTER_START)
    save_futures_price(symbol="ru0", name="橡胶", start_date=FILTER_START)
    save_futures_price(symbol="sp0", name="纸浆", start_date=FILTER_START)

    # 4. 大商所
    save_futures_price(symbol="m0", name="豆粕", start_date=FILTER_START)
    save_futures_price(symbol="lh0", name="生猪", start_date=FILTER_START)
    save_futures_price(symbol="i0", name="铁矿石", start_date=FILTER_START)
    save_futures_price(symbol="p0", name="棕榈油", start_date=FILTER_START)
    save_futures_price(symbol="y0", name="豆油", start_date=FILTER_START)
    save_futures_price(symbol="c0", name="玉米", start_date=FILTER_START)
    save_futures_price(symbol="jm0", name="焦煤", start_date=FILTER_START)
    save_futures_price(symbol="jd0", name="鸡蛋", start_date=FILTER_START)
    save_futures_price(symbol="v0", name="PVC", start_date=FILTER_START)
    save_futures_price(symbol="l0", name="塑料", start_date=FILTER_START)
    save_futures_price(symbol="eb0", name="苯乙烯", start_date=FILTER_START)
    save_futures_price(symbol="eg0", name="乙二醇", start_date=FILTER_START)

    # 5. 郑商所
    save_futures_price(symbol="fg0", name="玻璃", start_date=FILTER_START)
    save_futures_price(symbol="sa0", name="纯碱", start_date=FILTER_START)
    save_futures_price(symbol="sr0", name="白糖", start_date=FILTER_START)
    save_futures_price(symbol="cf0", name="棉花", start_date=FILTER_START)
    save_futures_price(symbol="ma0", name="甲醇", start_date=FILTER_START)
    save_futures_price(symbol="ta0", name="PTA", start_date=FILTER_START)
    save_futures_price(symbol="ap0", name="苹果", start_date=FILTER_START)
    save_futures_price(symbol="ur0", name="尿素", start_date=FILTER_START)
    save_futures_price(symbol="sh0", name="烧碱", start_date=FILTER_START)
    save_futures_price(symbol="rm0", name="菜粕", start_date=FILTER_START)
    save_futures_price(symbol="oi0", name="菜油", start_date=FILTER_START)
