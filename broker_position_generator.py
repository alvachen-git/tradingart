"""
期货商持仓晚报生成器 v6.0
=====================================
更新内容：
- 从数据库动态获取品种价格（不再写死）
- 今日核心信号：优先展示正反指标分歧 + 技术面验证
- 机构5日累计布局：用资金金额（亿元）
- 反指标做多/做空区分正确
"""

import pandas as pd
import os
import re
import time
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from langchain_community.chat_models import ChatTongyi
from langchain_core.messages import HumanMessage
from langchain_core.tools import tool
from langgraph.prebuilt import create_react_agent

# ==========================================
# 1. 引入工具包
# ==========================================
from data_engine import (
    search_broker_holdings_on_date,
    tool_analyze_broker_positions,
    tool_analyze_position_change,
    get_latest_data_date
)
from kline_tools import analyze_kline_pattern
from plot_tools import draw_chart_tool
from news_tools import get_financial_news
from search_tools import search_web
import subscription_service as sub_svc

# 初始化环境
load_dotenv(override=True)

# 数据库连接
db_url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_url)

# 初始化 LLM
llm = ChatTongyi(model="qwen-plus", api_key=os.getenv("DASHSCOPE_API_KEY"))

# ==========================================
# 2. 期货商分类配置
# ==========================================
BROKER_CONFIG = {
    "正指标_机构": ["海通期货", "东证期货", "国泰君安"],
    "正指标_外资": ["乾坤期货", "摩根大通"],
    "反指标": ["中信建投", "东方财富", "方正中期"]
}

BROKER_DB_NAMES = {
    "海通期货": "海通期货（代客）",
    "东证期货": "东证期货（代客）",
    "国泰君安": "国泰君安（代客）",
    "乾坤期货": "乾坤期货",
    "摩根大通": "摩根大通",
    "中信建投": "中信建投（代客）",
    "东方财富": "东方财富",
    "方正中期": "方正中期（代客）"
}

# ==========================================
# 3. 合约乘数配置（只存乘数，价格从数据库获取）
# ==========================================
CONTRACT_MULTIPLIER = {
    # 金属
    "CU": 5,  # 铜，5吨/手
    "AL": 5,  # 铝，5吨/手
    "ZN": 5,  # 锌，5吨/手
    "PB": 5,  # 铅，5吨/手
    "NI": 1,  # 镍，1吨/手
    "SN": 1,  # 锡，1吨/手
    "AU": 1000,  # 黄金，1000克/手
    "AG": 15,  # 白银，15千克/手

    # 黑色
    "RB": 10,  # 螺纹钢，10吨/手
    "HC": 10,  # 热卷，10吨/手
    "I": 100,  # 铁矿石，100吨/手
    "J": 100,  # 焦炭，100吨/手
    "JM": 60,  # 焦煤，60吨/手
    "SF": 5,  # 硅铁，5吨/手
    "SM": 5,  # 锰硅，5吨/手
    "SS": 5,  # 不锈钢，5吨/手

    # 能化
    "SC": 1000,  # 原油，1000桶/手
    "FU": 10,  # 燃料油，10吨/手
    "LU": 10,  # 低硫燃油，10吨/手
    "BU": 10,  # 沥青，10吨/手
    "TA": 5,  # PTA，5吨/手
    "EG": 10,  # 乙二醇，10吨/手
    "MA": 10,  # 甲醇，10吨/手
    "PP": 5,  # 聚丙烯，5吨/手
    "L": 5,  # 塑料，5吨/手
    "V": 5,  # PVC，5吨/手
    "EB": 5,  # 苯乙烯，5吨/手
    "PG": 20,  # LPG，20吨/手
    "SA": 20,  # 纯碱，20吨/手
    "FG": 20,  # 玻璃，20吨/手
    "UR": 20,  # 尿素，20吨/手

    # 农产品
    "M": 10,  # 豆粕，10吨/手
    "Y": 10,  # 豆油，10吨/手
    "A": 10,  # 豆一，10吨/手
    "C": 10,  # 玉米，10吨/手
    "CS": 10,  # 淀粉，10吨/手
    "P": 10,  # 棕榈油，10吨/手
    "OI": 10,  # 菜油，10吨/手
    "RM": 10,  # 菜粕，10吨/手
    "CF": 5,  # 棉花，5吨/手
    "SR": 10,  # 白糖，10吨/手
    "AP": 10,  # 苹果，10吨/手
    "CJ": 5,  # 红枣，5吨/手
    "JD": 5,  # 鸡蛋，5吨/手
    "LH": 16,  # 生猪，16吨/手
    "PK": 5,  # 花生，5吨/手

    # 股指
    "IF": 300,  # 沪深300，300元/点
    "IC": 200,  # 中证500，200元/点
    "IM": 200,  # 中证1000，200元/点
    "IH": 300,  # 上证50，300元/点

    # 新能源
    "LC": 1,  # 碳酸锂，1吨/手
    "SI": 5,  # 工业硅，5吨/手
    "AO": 20,  # 氧化铝，20吨/手

    # 航运
    "EC": 50,  # 欧线集运，50点/手
}


def get_db_broker_name(broker_name: str) -> str:
    return BROKER_DB_NAMES.get(broker_name, broker_name)


def _normalize_trade_date(value) -> str:
    """将数据库日期值规范成 YYYYMMDD。"""
    digits = re.sub(r"\D", "", str(value or ""))
    return digits[:8] if len(digits) >= 8 else ""


def get_recent_trading_days(n_days: int = 5):
    """从数据库获取最近 n 个交易日（含当日）。"""
    try:
        sql = f"SELECT DISTINCT trade_date FROM futures_holding ORDER BY trade_date DESC LIMIT {n_days + 2}"
        df = pd.read_sql(sql, engine)
        if len(df) >= 2:
            dates = [_normalize_trade_date(d) for d in df["trade_date"].tolist()]
            dates = [d for d in dates if len(d) == 8]
            if len(dates) >= 2:
                today = dates[0]
                yesterday = dates[1]
                # 口径统一：n_days=5 => 取 [today ... 往前第4个交易日]，累计恰好5个交易日
                start_idx = min(max(n_days - 1, 0), len(dates) - 1)
                start_date = dates[start_idx]
                return today, yesterday, start_date
    except Exception as e:
        print(f"获取交易日失败: {e}")

    today = datetime.now()
    return (
        today.strftime("%Y%m%d"),
        (today - timedelta(days=1)).strftime("%Y%m%d"),
        (today - timedelta(days=7)).strftime("%Y%m%d")
    )


def should_skip_non_trading_publish() -> bool:
    """
    交易日门禁：
    当数据库最新交易日不是今天时，跳过发布。
    """
    try:
        latest_db_date = _normalize_trade_date(get_latest_data_date())
        today = datetime.now().strftime("%Y%m%d")
        if latest_db_date != today:
            print(f"⏭️ 非交易日或当日数据未就绪，跳过发布。today={today}, latest_db={latest_db_date or 'N/A'}")
            return True
        return False
    except Exception as e:
        print(f"⚠️ 交易日门禁检查失败，按保守策略跳过发布: {e}")
        return True


# ==========================================
# 4. 新增工具：获取品种价格和计算资金
# ==========================================
@tool
def get_futures_price_and_value(product_code: str, lots: int):
    """
    【期货持仓价值计算器】
    根据品种代码和手数，查询最新价格并计算持仓资金价值。

    参数:
    - product_code: 品种代码，如 'CU', 'RB', 'M', 'SA'
    - lots: 持仓手数（正数表示多头，负数表示空头）

    返回: 品种价格、合约价值、持仓资金（亿元）
    """
    if engine is None:
        return "数据库连接失败"

    code = product_code.upper().strip()

    try:
        # 1. 查询该品种主力合约的最新价格
        # 使用正则匹配品种代码开头的合约，按持仓量排序取主力
        sql = f"""
            SELECT ts_code, close_price, trade_date
            FROM futures_price
            WHERE UPPER(ts_code) LIKE '{code}%'
              AND ts_code NOT LIKE '%TAS%'
              AND ts_code REGEXP '[0-9]{{4}}$'
            ORDER BY trade_date DESC, oi DESC
            LIMIT 1
        """
        df = pd.read_sql(sql, engine)

        if df.empty:
            return f"未找到品种 {code} 的价格数据"

        price = float(df.iloc[0]['close_price'])
        contract = df.iloc[0]['ts_code']
        trade_date = str(df.iloc[0]['trade_date'])

        # 2. 获取合约乘数
        multiplier = CONTRACT_MULTIPLIER.get(code, 10)  # 默认10

        # 3. 计算资金价值
        # 资金(亿元) = |手数| × 乘数 × 价格 / 1亿
        value_yuan = abs(lots) * multiplier * price
        value_yi = value_yuan / 100000000

        # 4. 单手价值
        single_lot_value = multiplier * price

        return f"""
📊 **{code} 持仓价值计算**
- 主力合约: {contract}
- 最新价格: {price:,.2f} (日期: {trade_date})
- 合约乘数: {multiplier}
- 单手价值: {single_lot_value:,.0f} 元
- 持仓手数: {lots:+,} 手
- **持仓资金: {value_yi:.2f} 亿元**
"""

    except Exception as e:
        return f"查询价格出错: {e}"


@tool
def batch_calculate_position_value(positions: str):
    """
    【批量计算持仓资金】
    批量计算多个品种的持仓资金价值。

    参数:
    - positions: 品种和手数列表，格式为 "品种1:手数1,品种2:手数2"
                例如: "M:115000,SA:-96000,RB:50000"

    返回: 各品种的资金价值汇总
    """
    if engine is None:
        return "数据库连接失败"

    results = []
    total_long = 0  # 多头总资金
    total_short = 0  # 空头总资金

    try:
        items = positions.split(',')
        for item in items:
            parts = item.strip().split(':')
            if len(parts) != 2:
                continue

            code = parts[0].strip().upper()
            lots = int(parts[1].strip())

            # 查询价格
            sql = f"""
                SELECT close_price
                FROM futures_price
                WHERE UPPER(ts_code) LIKE '{code}%'
                  AND ts_code NOT LIKE '%TAS%'
                  AND ts_code REGEXP '[0-9]{{4}}$'
                ORDER BY trade_date DESC, oi DESC
                LIMIT 1
            """
            df = pd.read_sql(sql, engine)

            if df.empty:
                results.append(f"- {code}: 未找到价格")
                continue

            price = float(df.iloc[0]['close_price'])
            multiplier = CONTRACT_MULTIPLIER.get(code, 10)
            value_yi = abs(lots) * multiplier * price / 100000000

            direction = "多" if lots > 0 else "空"
            results.append(f"- {code}: {lots:+,}手 × {multiplier} × {price:,.0f} = **{value_yi:.2f}亿** ({direction})")

            if lots > 0:
                total_long += value_yi
            else:
                total_short += value_yi

        summary = f"""
📊 **批量持仓资金计算**

{chr(10).join(results)}

---
**汇总**:
- 多头总资金: {total_long:.2f} 亿
- 空头总资金: {total_short:.2f} 亿
- 净资金规模: {total_long - total_short:+.2f} 亿
"""
        return summary

    except Exception as e:
        return f"批量计算出错: {e}"


# ==========================================
# 5. AI 记者 - 数据采集
# ==========================================
def collect_broker_position_data():
    print("🕵️‍♂️ [持仓记者] 出发采集期货商持仓数据...")

    tools = [
        search_broker_holdings_on_date,
        tool_analyze_broker_positions,
        tool_analyze_position_change,
        analyze_kline_pattern,
        draw_chart_tool,
        get_financial_news,
        search_web,
        # 新增：价格和资金计算工具
        get_futures_price_and_value,
        batch_calculate_position_value,
    ]

    today_str = datetime.now().strftime("%Y年%m月%d日")

    # 获取实际交易日
    today_date, yesterday, five_days_ago = get_recent_trading_days(5)
    print(f"📅 交易日：今日={today_date}, 昨日={yesterday}, 5日前={five_days_ago}")

    institution_brokers_db = [get_db_broker_name(b) for b in BROKER_CONFIG["正指标_机构"]]
    foreign_brokers_db = [get_db_broker_name(b) for b in BROKER_CONFIG["正指标_外资"]]
    contra_brokers_db = [get_db_broker_name(b) for b in BROKER_CONFIG["反指标"]]

    system_prompt = f"""
你是一位**期货商持仓分析专家**，为《爱波塔-期货商持仓晚报》采集数据。
当前日期：{today_str}

【期货商分类】
- 正指标（机构）：{', '.join(institution_brokers_db)}
- 正指标（外资）：{', '.join(foreign_brokers_db)}
- 反指标：{', '.join(contra_brokers_db)}

【重要提示 - 品种名称处理】
同一品种可能有不同的合约代码，在汇总时必须合并为同一品种！
- 纯碱sa → 纯碱
- 白糖sr → 白糖
- 玻璃fg → 玻璃
以此类推，按品种大类合并统计。

【采集流程】

## 第一步：机构当日持仓变化 ⭐必做
查询3家机构**当日**持仓变化：

调用 `tool_analyze_broker_positions`，参数：
- start_date: {yesterday}
- end_date: {today_date}
- sort_by: "net"

依次查询：
1. 海通期货（代客）
2. 东证期货（代客）
3. 国泰君安（代客）

**输出要求**：
- 将3家数据按品种合并
- 计算**当日净多头增仓TOP5**（品种、合计手数、各家明细）
- 计算**当日净空头增仓TOP5**（品种、合计手数、各家明细）

## 第二步：机构5日累计持仓变化 + 资金计算 ⭐必做
查询3家机构**近5个交易日**累计变化：

调用 `tool_analyze_broker_positions`，参数：
- start_date: {five_days_ago}
- end_date: {today_date}
- sort_by: "net"

**重要！计算资金价值**：
汇总完5日累计持仓后，调用 `batch_calculate_position_value` 计算资金：
- 格式: "M:115000,SA:-96000,RB:50000,..."
- 这个工具会从数据库查询最新价格，计算准确的资金金额

**输出要求**：
- 计算**5日累计净多头增仓TOP5品种**（手数 + 资金亿元）
- 计算**5日累计净空头增仓TOP5品种**（手数 + 资金亿元）

## 第三步：外资当日持仓变化 ⭐必做
查询2家外资的当日持仓变化：
- start_date: {yesterday}
- end_date: {today_date}

1. 乾坤期货
2. 摩根大通

## 第四步：反指标当日持仓变化 ⭐必做
查询3家反指标的当日持仓变化：
- start_date: {yesterday}
- end_date: {today_date}

1. 中信建投（代客）
2. 东方财富
3. 方正中期（代客）

**重要**：只汇总品种和合计方向，**不记录具体期货商名称**。
**注意区分**：
- 净持仓 > 0 的品种 → 反指标做多
- 净持仓 < 0 的品种 → 反指标做空

## 第五步：正反指标分歧分析 ⭐核心任务
对比机构和反指标的持仓方向，找出分歧品种：
- 【经典分歧A】机构做多 + 反指标做空 → 跟随机构做多
- 【经典分歧B】机构做空 + 反指标做多 → 跟随机构做空
- 【罕见共识】机构和反指标同方向 → 需要警惕

## 第六步：分歧品种技术面验证 ⭐必做
对正反分歧最明显的前2-3个品种，调用 `analyze_kline_pattern` 验证技术面。

【输出格式总结】：
1. 品种名称要统一（合并不同合约）
2. 5日累计要有资金金额（从数据库查询计算）
3. 必须有正反分歧分析
4. 必须有技术面验证结果
5. 反指标只汇总品种和方向，不显示期货商名称
6. 反指标数据要区分做多（净持仓>0）和做空（净持仓<0）
"""

    reporter_agent = create_react_agent(llm, tools, prompt=system_prompt)

    try:
        trigger_msg = f"""开始期货商持仓扫描：

交易日参数：
- 当日变化：{yesterday} vs {today_date}
- 5日累计：{five_days_ago} vs {today_date}

请按顺序完成：
1. 机构当日持仓变化（合并同品种）
2. 机构5日累计持仓变化（用 batch_calculate_position_value 计算资金）
3. 外资当日持仓变化
4. 反指标当日持仓变化（只汇总品种，区分做多和做空）
5. 正反指标分歧分析
6. 分歧品种技术面验证
"""

        result = reporter_agent.invoke(
            {"messages": [HumanMessage(content=trigger_msg)]},
            {"recursion_limit": 150}  # 增加步骤限制，因为多了资金计算
        )

        collected_content = result["messages"][-1].content
        print("✅ [持仓记者] 采集完成。")
        return collected_content

    except Exception as e:
        print(f"❌ [持仓记者] 采集出错: {e}")
        import traceback
        traceback.print_exc()
        return "AI 采集失败。"


# ==========================================
# 6. AI 主编 - 撰写报告
# ==========================================
def draft_broker_position_report(raw_material):
    print("✏️ [持仓主编] 正在撰写晚报...")

    today = datetime.now().strftime("%Y年%m月%d日")
    weekday = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][datetime.now().weekday()]

    prompt = f"""
你是【爱波塔持仓研究中心】主编，正在撰写《爱波塔-持仓数据流晚报》。

【记者提交的素材】：
{raw_material}

【期货商分类】：
- **机构**：海通期货、东证期货、国泰君安（正指标，跟随）
- **外资**：乾坤期货、摩根大通（正指标，跟随）
- **反指标**：散户聚集地（报告中**不显示具体名称**，反向参考）

【报告结构】（共6个板块）：
1. 今日核心信号 ← 聚焦正反分歧+技术验证
2. 机构当日动向
3. 机构5日累计布局 ← 用资金金额
4. 外资风向标
5. 反指标信号
6. AI毒舌点评

【设计规范】：
- 主色：#6366f1（紫色）
- 多头：#ef4444（红色）
- 空头：#22c55e（绿色）
- 辅助：#94a3b8（灰色）
- 风格：极简高端

【关键要求】：

### 1. 今日核心信号（最重要！）
必须包含：
- **正反分歧品种**：机构和反指标方向相反的品种
- **技术面验证结果**：是否形成"资金+技术"共振

### 2. 机构5日累计布局
- 必须显示**资金金额（亿元）**
- 格式：品种名 +XX万手 (约X.X亿)

### 3. 反指标信号区分
- **反指标做多区块**：只放净持仓 > 0 的品种（数字为正，红色）
- **反指标做空区块**：只放净持仓 < 0 的品种（数字为负，绿色）

### 4. AI毒舌点评
- 风格：幽默、毒舌、有梗
- 必须聚焦正反指标对比

【HTML模板】：

```html
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <style>
    * {{ box-sizing: border-box; }}
    body {{ margin:0; padding:0; background:#0f172a; font-family:'PingFang SC','Microsoft YaHei',system-ui,sans-serif; color:#e2e8f0; }}
    .container {{ max-width:680px; margin:0 auto; padding:32px 20px; }}
    .card {{ background:rgba(30,41,59,0.8); border:1px solid rgba(99,102,241,0.2); border-radius:12px; padding:20px; margin-bottom:24px; }}
    .section-title {{ color:#6366f1; font-size:17px; font-weight:600; margin:0 0 16px 0; display:flex; align-items:center; gap:8px; }}
    .section-title::before {{ content:''; width:3px; height:18px; background:#6366f1; border-radius:2px; }}
    .sub-text {{ color:#94a3b8; font-size:12px; }}
    .data-table {{ width:100%; border-collapse:collapse; margin-top:12px; }}
    .data-table th {{ text-align:left; padding:10px 8px; color:#94a3b8; font-size:12px; font-weight:500; border-bottom:1px solid rgba(255,255,255,0.06); }}
    .data-table td {{ padding:12px 8px; font-size:13px; border-bottom:1px solid rgba(255,255,255,0.03); }}
    .text-red {{ color:#ef4444; }}
    .text-green {{ color:#22c55e; }}
    .text-purple {{ color:#6366f1; }}
    .text-gray {{ color:#94a3b8; }}
    .tag {{ display:inline-block; padding:2px 8px; border-radius:4px; font-size:11px; margin-right:6px; }}
    .tag-long {{ background:rgba(239,68,68,0.15); color:#ef4444; }}
    .tag-short {{ background:rgba(34,197,94,0.15); color:#22c55e; }}
    .tag-warn {{ background:rgba(251,191,36,0.15); color:#fbbf24; }}
    .highlight-box {{ background:rgba(99,102,241,0.1); border-left:3px solid #6366f1; padding:16px; border-radius:0 8px 8px 0; margin:16px 0; }}
    .detail-text {{ font-size:11px; color:#94a3b8; margin-top:4px; }}
    .signal-item {{ margin-bottom:16px; padding:12px; background:rgba(99,102,241,0.05); border-radius:8px; }}
  </style>
</head>
<body>
<div class="container">

  <!-- 标题 -->
  <div style="text-align:center; margin-bottom:32px;">
    <h1 style="color:#6366f1; font-size:26px; margin:0 0 8px 0; font-weight:700;">爱波塔-持仓数据流晚报</h1>
    <p class="sub-text">{today} {weekday} · 追踪聪明钱动向</p>
  </div>

  <!-- 今日核心信号 -->
  <div class="card" style="border-color:rgba(99,102,241,0.4);">
    <h2 class="section-title">今日核心信号</h2>
    <div style="line-height:1.9; font-size:14px;">
      <!-- 核心信号：正反分歧 + 技术验证 -->
    </div>
  </div>

  <!-- 机构当日动向 -->
  <div class="card">
    <h2 class="section-title">机构当日动向</h2>
    <p class="sub-text" style="margin:-8px 0 16px 0;">海通 · 东证 · 国泰君安 ｜ 当日净持仓变化</p>

    <p style="font-size:14px; font-weight:600; margin:16px 0 8px 0;"><span class="text-red">●</span> 当日净多头增仓 TOP5</p>
    <table class="data-table">
      <tr><th>品种</th><th style="text-align:right;">合计</th><th style="text-align:right;">明细</th></tr>
    </table>

    <p style="font-size:14px; font-weight:600; margin:24px 0 8px 0;"><span class="text-green">●</span> 当日净空头增仓 TOP5</p>
    <table class="data-table">
      <tr><th>品种</th><th style="text-align:right;">合计</th><th style="text-align:right;">明细</th></tr>
    </table>
  </div>

  <!-- 机构5日累计布局 -->
  <div class="card">
    <h2 class="section-title">机构5日累计布局</h2>
    <p class="sub-text" style="margin:-8px 0 16px 0;">海通 · 东证 · 国泰君安 ｜ 近5个交易日累计 · 按资金规模排序</p>

    <div style="display:grid; grid-template-columns:1fr 1fr; gap:16px;">
      <div>
        <p style="font-size:13px; font-weight:600; margin:0 0 8px 0;"><span class="text-red">●</span> 累计做多</p>
        <div style="font-size:13px; line-height:1.8;">
          <!-- 格式：1. 豆粕 <span class="text-red">+11.5万手</span> <span class="text-gray">(约3.7亿)</span> -->
        </div>
      </div>
      <div>
        <p style="font-size:13px; font-weight:600; margin:0 0 8px 0;"><span class="text-green">●</span> 累计做空</p>
        <div style="font-size:13px; line-height:1.8;">
        </div>
      </div>
    </div>
  </div>

  <!-- 外资风向标 -->
  <div class="card">
    <h2 class="section-title">外资风向标</h2>
    <p class="sub-text" style="margin:-8px 0 16px 0;">乾坤期货 · 摩根大通 ｜ 当日净持仓变化</p>
    <div style="line-height:1.9; font-size:13px;">
    </div>
  </div>

  <!-- 反指标信号 -->
  <div class="card">
    <h2 class="section-title">反指标信号</h2>
    <p class="sub-text" style="margin:-8px 0 16px 0;">散户聚集地 ｜ 当日净持仓变化 · 反向参考</p>

    <!-- 反指标做多区块：只放净持仓 > 0 的品种 -->
    <p style="font-size:14px; font-weight:600; margin:16px 0 8px 0;"><span class="tag tag-short">反着看</span> 反指标大幅做多</p>
    <table class="data-table">
      <tr><th>品种</th><th style="text-align:right;">合计净多</th><th>潜在信号</th></tr>
      <!-- ⚠️ 只放净持仓 > 0 的品种，数字为正（红色） -->
    </table>

    <!-- 反指标做空区块：只放净持仓 < 0 的品种 -->
    <p style="font-size:14px; font-weight:600; margin:24px 0 8px 0;"><span class="tag tag-long">反着看</span> 反指标大幅做空</p>
    <table class="data-table">
      <tr><th>品种</th><th style="text-align:right;">合计净空</th><th>潜在信号</th></tr>
      <!-- ⚠️ 只放净持仓 < 0 的品种，数字为负（绿色） -->
    </table>
  </div>

  <!-- AI毒舌点评 -->
  <div class="card" style="border-color:rgba(99,102,241,0.4);">
    <h2 class="section-title">AI毒舌点评</h2>
    <div class="highlight-box" style="font-size:14px; line-height:2.0;">
      <!-- 2-3段毒舌点评，聚焦正反对比 -->
    </div>
  </div>

  <!-- 底部 -->
  <div style="text-align:center; padding:24px 0 0 0; border-top:1px solid rgba(255,255,255,0.06);">
    <p class="sub-text">⚠️ 本报告仅供参考，不构成投资建议</p>
    <p class="sub-text" style="margin-top:8px;">爱波塔 · 期货商持仓研究中心</p>
  </div>

</div>
</body>
</html>
```

【检查清单】：
- [ ] 今日核心信号：有正反分歧 + 技术验证
- [ ] 5日累计：有资金金额（亿元）
- [ ] 品种去重（无重复）
- [ ] 反指标不显示期货商名称
- [ ] **反指标做多区块：只放净持仓>0的品种（数字为正，红色）**
- [ ] **反指标做空区块：只放净持仓<0的品种（数字为负，绿色）**
- [ ] AI毒舌点评：幽默+正反对比

【输出】：只返回HTML代码。
"""

    res = llm.invoke([HumanMessage(content=prompt)])
    html = res.content.replace("```html", "").replace("```", "").strip()
    return html


# ==========================================
# 7. 发布
# ==========================================
def extract_summary_from_html(html_content: str) -> str:
    import re
    match = re.search(r'今日核心信号.*?<p[^>]*>(.*?)</p>', html_content, re.DOTALL)
    if match:
        summary = re.sub(r'<[^>]+>', '', match.group(1))
        return summary[:200].strip()
    return "今日期货商持仓动向分析"


def publish_broker_position_report(html_content: str):
    print("📤 [发布] 正在发布...")

    today_str = datetime.now().strftime("%m月%d日")
    weekday = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][datetime.now().weekday()]

    title = f"{today_str} {weekday} 期货商持仓晚报"
    summary = extract_summary_from_html(html_content)

    try:
        success, result = sub_svc.publish_content(
            channel_code="broker_position_report",
            title=title,
            content=html_content,
            summary=summary if summary else f"{today_str}期货商持仓动向分析"
        )
        if success:
            print(f"✅ [发布] 成功，ID: {result}")
            return True, result
        else:
            print(f"❌ [发布] 失败: {result}")
            return False, result
    except Exception as e:
        print(f"❌ [发布] 异常: {e}")
        return False, str(e)


# ==========================================
# 8. 主流程
# ==========================================
def main():
    start_t = time.time()
    print("=" * 60)
    print("🏛️ 期货商持仓晚报生成器 v6.0")
    print("=" * 60)

    if should_skip_non_trading_publish():
        print("✅ 任务已安全跳过（非交易日门禁）")
        return

    print("\n【第一步】数据采集...")
    material = collect_broker_position_data()

    if len(material) < 100:
        print("❌ 素材过少")
        return

    with open("broker_material_debug.txt", "w", encoding="utf-8") as f:
        f.write(material)
    print("📝 素材已保存: broker_material_debug.txt")

    print("\n【第二步】撰写报告...")
    report_html = draft_broker_position_report(material)

    preview_path = "preview_broker_position_report.html"
    with open(preview_path, "w", encoding="utf-8") as f:
        f.write(report_html)
    print(f"✅ 预览: {preview_path}")

    if len(report_html) < 300:
        print("❌ 报告过短")
        return

    print("\n【第三步】发布...")
    pub_success, pub_result = publish_broker_position_report(report_html)

    print(f"\n{'=' * 60}")
    print(f"📊 结果汇总")
    print(f"{'=' * 60}")
    print(f"采集: ✅ | 撰写: ✅ | 发布: {'✅' if pub_success else '❌'}")
    print(f"预览: {preview_path}")
    print(f"耗时: {time.time() - start_t:.1f}s")
    print("=" * 60)


if __name__ == "__main__":
    main()
