import tushare as ts
import requests
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import time
from datetime import datetime
import sys

# --- 1. 初始化配置 ---
load_dotenv(override=True)

if not os.getenv("DB_USER"):
    print("❌ [Error] 环境变量未加载，请检查 .env 文件路径")
    sys.exit(1)

# 数据库连接
db_url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_url, pool_recycle=3600)

# Tushare 初始化
ts.set_token(os.getenv("TUSHARE_TOKEN"))
pro = ts.pro_api(timeout=120)

# 交易所配置
EXCHANGE_LIST = ['SHFE', 'DCE', 'CZCE', 'CFFEX', 'INE', 'GFEX']


# --- 核心工具：将 Tushare 代码转换为新浪实时代码 ---
def map_code_to_sina(ts_code, exchange):
    symbol = ts_code.split('.')[0]
    return f"nf_{symbol}"


def get_sina_futures_custom(sina_codes):
    """
    [已修复] 基于 2026-01-26 实测数据校准：
    - 中金所 (IM/IF/IC/IH) 使用无前缀格式 (索引偏移 -2)
    - 商品期货 (RB/M/SR) 使用标准格式
    """
    if not sina_codes: return pd.DataFrame()

    url = f"http://hq.sinajs.cn/list={','.join(sina_codes)}"
    headers = {'Referer': 'http://finance.sina.com.cn/'}

    try:
        r = requests.get(url, headers=headers, timeout=5)
        text = r.text
    except Exception as e:
        print(f"      [!] 网络请求失败: {e}")
        return pd.DataFrame()

    data_list = []
    lines = text.split('\n')

    for line in lines:
        if not line.strip(): continue
        try:
            # 1. 解析代码
            eq_idx = line.find('=')
            if eq_idx == -1: continue

            code_part = line[:eq_idx]
            if code_part.startswith("var hq_str_"):
                sina_code = code_part.replace("var hq_str_", "")
            else:
                parts = code_part.split('_')
                sina_code = parts[-2] + '_' + parts[-1]

            # 2. 解析数据
            val_part = line[eq_idx + 1:].strip().strip('";')
            if not val_part: continue
            vals = val_part.split(',')

            # 3. 核心分流逻辑 (基于你的截图证据)
            # 中金所代码特征: nf_IM, nf_IF, nf_IC, nf_IH, nf_T, nf_TF, nf_TS
            is_cffex = any(
                x in sina_code.upper() for x in ['NF_IF', 'NF_IC', 'NF_IH', 'NF_IM', 'NF_TF', 'NF_T', 'NF_TS'])

            if is_cffex:
                # --- 中金所 (IM2603) 格式 ---
                # 截图证实: 0:开盘, 1:最高, 2:最低, 3:最新价, 4:成交量, 5:成交额, 6:持仓量
                if len(vals) < 7: continue
                try:
                    open_p = float(vals[0])
                    high_p = float(vals[1])
                    low_p = float(vals[2])
                    current_p = float(vals[3])
                    vol = float(vals[4])
                    oi = float(vals[6])

                    # 中金所接口经常不返回昨收，暂用开盘价兜底，防止涨跌幅计算报错
                    pre_close = open_p
                except:
                    continue

            else:
                # --- 商品期货 (RB2605) 格式 ---
                # 截图证实: 0:名, 1:时, 2:开, 3:高, 4:低 ... 8:现价 ... 13:持仓 ... 14:成交量
                if len(vals) < 15: continue
                try:
                    open_p = float(vals[2])
                    high_p = float(vals[3])
                    low_p = float(vals[4])
                    current_p = float(vals[8])
                    pre_close = float(vals[5])
                    oi = float(vals[13])
                    vol = float(vals[14])
                except:
                    continue

            # 4. 数据有效性检查
            if open_p == 0 and current_p == 0: continue

            # 双重保险：如果最低价依然大于最高价，强制修正 (防止万一接口抽风)
            if low_p > high_p:
                low_p = current_p
                high_p = current_p

            data_item = {
                'sina_code': sina_code,
                'price': current_p,
                'open': open_p,
                'high': high_p,
                'low': low_p,
                'pre_close': pre_close,
                'volume': vol,
                'amount': 0,
                'position': oi
            }
            data_list.append(data_item)

        except Exception:
            continue

    return pd.DataFrame(data_list)


# --- 核心工具：获取实时快照并清洗 ---
def fetch_realtime_snapshot(exchange):
    print(f"   [*] 正在获取 {exchange} 的活跃合约列表...")

    df_list = pd.DataFrame()

    # 1. 获取列表 (优先 Tushare, 失败走本地)
    try:
        df_list = pro.fut_basic(exchange=exchange, fut_type='1', status='L', fields='ts_code,symbol')
    except:
        pass

    if df_list.empty:
        # 本地兜底逻辑 (代码省略，保持您原有的即可)
        # ... 如果您需要这部分代码请告诉我，否则假设您保留了原有的本地兜底 ...
        print(f"      ⚠️ 无法获取合约列表，跳过 {exchange}")
        return pd.DataFrame()

    # 2. 生成新浪代码
    # 定义映射函数 (内联或调用外部均可)
    def _map_sina(code):
        sym = code.split('.')[0]
        # 强制所有交易所（包括中金所）都用 nf_ 接口
        return f"nf_{sym}"

    df_list['sina_code'] = df_list['ts_code'].apply(_map_sina)
    sina_codes = df_list['sina_code'].tolist()

    print(f"      -> 锁定 {len(sina_codes)} 个合约，开始请求新浪实时接口...")

    # 3. 分批抓取 (改用自定义函数!)
    all_realtime_data = []
    chunk_size = 50  # 新浪URL长度限制，一次50个比较稳

    for i in range(0, len(sina_codes), chunk_size):
        try:
            batch = sina_codes[i: i + chunk_size]
            # 🔥【关键修改】这里不用 ts.get_realtime_quotes 了，用我们手写的
            df_rt = get_sina_futures_custom(batch)

            if not df_rt.empty:
                all_realtime_data.append(df_rt)

            # 稍微快一点，因为我们自己写的解析更快，但还是留点间隔
            time.sleep(0.05)
        except Exception as e:
            print(f"Batch error: {e}")

    if not all_realtime_data:
        return pd.DataFrame()

    # 4. 合并数据
    df_snapshot = pd.concat(all_realtime_data, ignore_index=True)

    # 5. 还原合并 (inner join 确保只保留我们要的)
    df_merged = pd.merge(df_snapshot, df_list, left_on='sina_code', right_on='sina_code', how='inner')

    # 6. 字段清洗与重命名
    output = pd.DataFrame()
    output['ts_code'] = df_merged['ts_code']
    output['trade_date'] = datetime.now().strftime('%Y%m%d')

    output['open_price'] = pd.to_numeric(df_merged['open'])
    output['high_price'] = pd.to_numeric(df_merged['high'])
    output['low_price'] = pd.to_numeric(df_merged['low'])
    output['close_price'] = pd.to_numeric(df_merged['price'])

    pre_close = pd.to_numeric(df_merged['pre_close'])
    output['vol'] = pd.to_numeric(df_merged['volume'])
    output['oi'] = pd.to_numeric(df_merged['position'])

    # 临时用最新价填充结算价
    output['settle_price'] = output['close_price']

    # 计算涨跌幅
    # 防止分母为0
    output['pct_chg'] = 0.0
    mask = pre_close > 0
    output.loc[mask, 'pct_chg'] = (output.loc[mask, 'close_price'] - pre_close[mask]) / pre_close[mask] * 100

    # ========================================================
    # 🔥🔥🔥 【核心新增】 生成主力合约逻辑 (参考 update_future_price_daily)
    # ========================================================

    # A. 提取品种符号 (如 RB2505.SHF -> RB)
    # 这里的正则 ^([a-zA-Z]+) 提取开头的字母部分
    output['symbol'] = output['ts_code'].str.extract(r'^([a-zA-Z]+)')

    # B. 找到每个品种持仓量(OI)最大的合约索引
    # dropna() 是防止解析失败，groupby('symbol')['oi'].idxmax() 找最大持仓那一行
    idx_max = output.dropna(subset=['symbol']).groupby('symbol')['oi'].idxmax()

    # C. 提取主力合约行
    df_dom = output.loc[idx_max].copy()

    # D. 将 ts_code 改为主力代码 (如 RB2505.SHF 改为 RB)
    # 这一步非常关键，数据库里通常用纯字母代码表示主力连续
    df_dom['ts_code'] = df_dom['symbol']

    # E. 合并：原始合约 + 主力合约
    output_final = pd.concat([output, df_dom], ignore_index=True)

    # F. 去重 (保留最后出现的，防止万一有重复)
    output_final = output_final.drop_duplicates(subset=['ts_code'], keep='last')

    # G. 清理临时列
    if 'symbol' in output_final.columns:
        output_final = output_final.drop(columns=['symbol'])

    # 返回处理好的最终数据
    return output_final


# --- 测试模式开关 ---
TEST_MODE = False # ⚠️ 设置为 True 可以只打印不入库

if __name__ == "__main__":
    today = datetime.now().strftime('%Y%m%d')
    print(f"🚀 [Midday Update] 启动... (TEST_MODE={TEST_MODE})")

    if not TEST_MODE:
        # 正式模式：先删除今日数据
        try:
            with engine.connect() as conn:
                conn.execute(text(f"DELETE FROM futures_price WHERE trade_date = '{today}'"))
                conn.commit()
            print("🧹 [1/3] 今日旧数据已清理")
        except Exception as e:
            print(f"⚠️ 清理失败: {e}")

    # 循环抓取
    for ex in EXCHANGE_LIST:
        try:
            df = fetch_realtime_snapshot(ex)
            if df.empty: continue

            if TEST_MODE:
                print(f"🔍 [测试] {ex} 抓取到 {len(df)} 条数据，前3行预览:")
                print(df.head(3).to_markdown(index=False))  # 打印预览
                print("-" * 30)
            else:
                # 正式入库
                df.to_sql('futures_price', engine, if_exists='append', index=False, chunksize=2000)
                print(f"✅ {ex}: 入库 {len(df)} 条")

        except Exception as e:
            print(f"❌ {ex} 异常: {e}")

    print("🏁 完成")