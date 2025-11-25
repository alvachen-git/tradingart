import akshare as ak
import pandas as pd
from datetime import datetime


def get_simple_option_iv(symbol="lc"):
    """
    直接从新浪获取期权 T 型报价 (包含交易所计算好的隐含波动率)
    symbol: lc (碳酸锂), si (工业硅), m (豆粕) ...
    """
    print(f"[*] 正在获取 {symbol} 的实时期权 T 型报价...")

    try:
        # 1. 调用 AkShare 接口 (新浪源)
        # 这个接口返回的是当前所有合约的 T 型报价，直接包含 IV
        df = ak.option_sina_commodity_spot(symbol=symbol)

        if df.empty:
            print("[-] 未获取到数据，请检查品种代码或当前时间。")
            return None

        # 2. 数据清洗 (新浪返回的列名很长，我们简化一下)
        # 原始列名示例: '看涨合约-买量', '看涨合约-隐含波动率', '行权价', '看跌合约-隐含波动率'...

        # 我们只保留核心列：行权价、价格、隐含波动率
        # 这里的 '隐含波动率' 是新浪算好的，直接用！

        # 提取看涨 (Call)
        df_call = df[['看涨合约-代码', '看涨合约-最新价', '行权价', '看涨合约-隐含波动率']].copy()
        df_call.columns = ['合约代码', '最新价', '行权价', 'IV']
        df_call['类型'] = 'Call'

        # 提取看跌 (Put)
        df_put = df[['看跌合约-代码', '看跌合约-最新价', '行权价', '看跌合约-隐含波动率']].copy()
        df_put.columns = ['合约代码', '最新价', '行权价', 'IV']
        df_put['类型'] = 'Put'

        # 合并
        df_final = pd.concat([df_call, df_put], ignore_index=True)

        # 3. 简单过滤
        # 去掉价格为0的（没成交的）
        df_final = df_final[df_final['最新价'] > 0]

        # 排序
        df_final = df_final.sort_values(['类型', '行权价'])

        return df_final

    except Exception as e:
        print(f"[!] 发生错误: {e}")
        return None


if __name__ == "__main__":
    # 获取碳酸锂数据
    df = get_simple_option_iv("lc")

    if df is not None:
        print("\n=== 碳酸锂期权隐含波动率 (IV) 数据 ===")
        print(df.head(20).to_markdown(index=False))

        # 如果您想保存到 Excel 看看
        # df.to_excel("lc_option_iv.xlsx", index=False)
        # print("\n[√] 数据已保存为 lc_option_iv.xlsx")