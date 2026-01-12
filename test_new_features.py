#!/usr/bin/env python3
"""
测试新功能脚本
测试内容：
1. K线工具的前几日K线识别功能
2. PE图绘制功能
3. 指数估值判断功能
"""

import sys
import os
from datetime import datetime

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_kline_analysis():
    """测试K线分析功能（含前几日K线识别）"""
    print("\n" + "="*60)
    print("测试1: K线分析功能（含前几日K线识别）")
    print("="*60)

    try:
        from kline_tools import analyze_kline_pattern

        # 测试案例1：分析白银K线
        print("\n测试案例1: 分析白银K线")
        result = analyze_kline_pattern.invoke({"query": "白银", "trade_date": None})
        print(result)

        # 测试案例2：分析茅台K线
        print("\n测试案例2: 分析茅台K线")
        result = analyze_kline_pattern.invoke({"query": "茅台", "trade_date": None})
        print(result)

        # 测试案例3：分析50ETF
        print("\n测试案例3: 分析50ETF")
        result = analyze_kline_pattern.invoke({"query": "50ETF", "trade_date": None})
        print(result)

        print("\n✅ K线分析功能测试通过")
        return True

    except Exception as e:
        print(f"\n❌ K线分析功能测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_pe_chart():
    """测试PE图绘制功能"""
    print("\n" + "="*60)
    print("测试2: PE图绘制功能")
    print("="*60)

    try:
        from plot_tools import draw_chart_tool

        # 测试案例1：绘制沪深300指数PE图
        print("\n测试案例1: 绘制沪深300指数PE图")
        result = draw_chart_tool.invoke({
            "query": "沪深300",
            "chart_type": "line_pe",
            "time_period": "1y"
        })
        print(result)

        # 测试案例2：绘制茅台PE图
        print("\n测试案例2: 绘制茅台PE图")
        result = draw_chart_tool.invoke({
            "query": "茅台",
            "chart_type": "line_pe",
            "time_period": "1y"
        })
        print(result)

        print("\n✅ PE图绘制功能测试通过")
        return True

    except Exception as e:
        print(f"\n❌ PE图绘制功能测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_valuation_data():
    """测试估值数据获取功能"""
    print("\n" + "="*60)
    print("测试3: 估值数据获取功能")
    print("="*60)

    try:
        from data_engine import get_stock_valuation

        # 测试案例1：获取茅台估值
        print("\n测试案例1: 获取茅台估值")
        result = get_stock_valuation.invoke({"query": "茅台"})
        print(result)

        # 测试案例2：获取沪深300估值
        print("\n测试案例2: 获取沪深300估值")
        result = get_stock_valuation.invoke({"query": "沪深300"})
        print(result)

        print("\n✅ 估值数据获取功能测试通过")
        return True

    except Exception as e:
        print(f"\n❌ 估值数据获取功能测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_legacy_features():
    """测试之前的行情问答功能"""
    print("\n" + "="*60)
    print("测试4: 之前的行情问答功能（随机抽查）")
    print("="*60)

    try:
        # 测试市场快照功能
        print("\n测试案例1: 市场快照功能")
        from market_tools import get_market_snapshot
        result = get_market_snapshot.invoke({"query": "白银"})
        print(result)

        # 测试价格统计功能
        print("\n测试案例2: 价格统计功能")
        from market_tools import get_price_statistics
        result = get_price_statistics.invoke({
            "query": "白银",
            "start_date": "20250101",
            "end_date": "20250112"
        })
        print(result)

        # 测试期权IV查询
        print("\n测试案例3: 期权IV查询")
        from data_engine import get_commodity_iv_info
        result = get_commodity_iv_info.invoke({"query": "白银"})
        print(result)

        print("\n✅ 之前的行情问答功能测试通过")
        return True

    except Exception as e:
        print(f"\n❌ 之前的行情问答功能测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """主测试函数"""
    print("\n" + "="*60)
    print(f"开始测试新功能 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

    results = {
        "K线分析功能": test_kline_analysis(),
        "PE图绘制功能": test_pe_chart(),
        "估值数据获取": test_valuation_data(),
        "之前功能": test_legacy_features()
    }

    # 汇总结果
    print("\n" + "="*60)
    print("测试结果汇总")
    print("="*60)

    for name, passed in results.items():
        status = "✅ 通过" if passed else "❌ 失败"
        print(f"{name}: {status}")

    all_passed = all(results.values())

    print("\n" + "="*60)
    if all_passed:
        print("🎉 所有测试通过！新功能运行正常！")
    else:
        print("⚠️ 有部分测试失败，请检查上述输出")
    print("="*60)

    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
