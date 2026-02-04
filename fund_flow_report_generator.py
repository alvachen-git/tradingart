import pandas as pd
import os
import time
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from langchain_community.chat_models import ChatTongyi
from langchain_core.messages import HumanMessage
from langgraph.prebuilt import create_react_agent

# ==========================================
# 1. 引入工具包
# ==========================================
from fund_flow_tools import tool_get_retail_money_flow
from stock_volume_tools import search_volume_anomalies, query_stock_volume
from market_tools import get_finance_related_trends, get_today_hotlist
from news_tools import get_financial_news
from kline_tools import analyze_kline_pattern
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
# 2. AI 记者 - 数据采集
# ==========================================
def collect_fund_flow_data():
    """
    资金流晚报专用采集
    """
    print("🕵️‍♂️ [资金流记者] 出发采集市场资金数据...")

    tools = [
        # 资金流核心工具
        tool_get_retail_money_flow,
        search_volume_anomalies,
        query_stock_volume,

        # 热点追踪
        get_finance_related_trends,
        get_today_hotlist,
        get_financial_news,
        search_web,

        # 技术分析
        analyze_kline_pattern,
    ]

    today_str = datetime.now().strftime("%Y年%m月%d日")

    system_prompt = f"""
你是一位**资金流向专家**，为《每日资金流晚报》采集数据。
当前日期：{today_str}

【核心任务】：追踪今日股市资金流向，发现异常信号和潜力机会。

【采集流程 - 必须按顺序执行】：

## 第一步：市场总体资金温度 ⭐必做
- 调用 `tool_get_retail_money_flow`
- 重点记录：
  * 沪深总成交额（与昨日对比）
  * **主力净流入TOP5板块**（板块名、流入金额、涨幅）
  * **主力净流出TOP5板块**（板块名、流出金额、跌幅）
- 判断市场整体情绪：放量活跃 / 缩量观望

## 第二步：热点事件关联分析 ⭐必做
- 调用 `get_finance_related_trends` 看今天有什么热点
- 调用 `get_today_hotlist` 看社媒热搜
- 找出今日热点事件，并分析：
  * 哪些板块受益于这些热点？
  * 热点是否引发了明显的资金流入？
- 如果有重大热点，调用 `search_web` 挖掘更多细节

## 第三步：资金异动个股筛选 ⭐必做
- 调用 `search_volume_anomalies(days=1, min_score=60, limit=30)`
- 筛选出今日资金异动股
- 分类整理：
  * **爆量股**（评分>80，单日量比>3倍）
  * **持续放量股**（评分60-80，10日均量比>1.5）
  * **异常缩量股**（量比<0.5，可能见顶）

## 第四步：重点个股深度分析 ⭐必做
- 对资金流入TOP10个股，逐一深度分析：
  1. 调用 `query_stock_volume(股票名, days=5)` 看量能细节
  2. 调用 `analyze_kline_pattern(股票代码)` 看技术形态
  3. 调用 `search_web` 搜索该股票今天的新闻
- 重点判断：
  * 放量是配合突破还是高位放量？
  * K线形态是否健康？
  * 有没有利好消息支撑？

## 第五步：风险股票预警
- 从第三步的异常缩量股中，找出：
  * 高位滞涨+缩量的股票
  * 逆市下跌+资金流出的股票
- 这些是明日需要规避的风险品种

【输出要求】：
1. 将所有采集的数据整理成详细的**素材笔记**
2. 不要写成新闻稿，只需罗列事实和数据
3. 对于每个板块、每只股票，都要有明确的数据支撑
4. 重点标注：哪些是机会，哪些是风险

【特别注意】：
- 确保覆盖市场温度、板块轮动、个股异动、热点事件四个维度
- 数据要全面，但要精炼，突出重点
"""

    reporter_agent = create_react_agent(llm, tools, prompt=system_prompt)

    try:
        trigger_msg = """开始今天的资金流扫描任务，请确保：
1. 完成市场总体资金流向分析（板块TOP5流入流出）
2. 识别今日热点事件及对应受益板块
3. 筛选资金异动个股（爆量、持续放量、异常缩量）
4. 对重点个股进行深度分析（量能+K线+新闻）
5. 给出风险警示清单
"""

        result = reporter_agent.invoke(
            {"messages": [HumanMessage(content=trigger_msg)]},
            {"recursion_limit": 120}
        )

        collected_content = result["messages"][-1].content
        print("✅ [资金流记者] 采集完成，素材已提交。")
        return collected_content

    except Exception as e:
        print(f"❌ [资金流记者] 采集出错: {e}")
        return "AI 采集失败，请检查日志。"


# ==========================================
# 3. AI 主编 - 撰写报告
# ==========================================
def draft_fund_flow_report(raw_material):
    """
    撰写资金流晚报 HTML
    """
    print("✏️ [资金流主编] 正在撰写晚报...")

    today = datetime.now().strftime("%Y年%m月%d日")
    weekday = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][datetime.now().weekday()]

    prompt = f"""
你是【爱波塔资金流研究中心】主编，正在撰写《每日资金流晚报》。

【记者提交的素材】：
{raw_material}

【写作要求】：

## 1. 格式：纯 HTML 代码（无 Markdown）

## 2. 设计理念
- 主色调：蓝色 #3b82f6（象征资金流动）
- 辅助色：绿色 #22c55e（流入）、红色 #ef4444（流出）
- 风格：玻璃拟态 + 深色背景
- 适配：邮件端兼容 + 网页端增强

## 3. 完整 HTML 模板：

```html
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <style>
    @media screen and (max-width: 640px) {{
      .main-container {{ padding: 20px 16px !important; }}
      .section-title {{ font-size: 18px !important; }}
      .two-col-table td {{ display: block !important; width: 100% !important; }}
    }}

    @media screen {{
      .glass-card {{
        backdrop-filter: blur(12px) !important;
        background: rgba(30, 41, 59, 0.8) !important;
        border: 1px solid rgba(255,255,255,0.08) !important;
      }}
    }}
  </style>
</head>
<body style="margin:0; padding:0; background:#0f172a; font-family:system-ui,-apple-system,sans-serif;">

  <!-- 主容器 -->
  <div class="main-container" style="max-width:700px; margin:0 auto; padding:30px 20px; background:linear-gradient(180deg, #1e293b 0%, #0f172a 100%);">

    <!-- 标题 -->
    <div style="text-align:center; margin-bottom:40px;">
      <h1 style="color:#3b82f6; font-size:28px; margin:0 0 8px 0; font-weight:700; letter-spacing:1px;">
        💰 每日资金流晚报
      </h1>
      <p style="color:#64748b; font-size:14px; margin:0;">
        {today} {weekday} | 追踪资金流向 · 发现市场机会
      </p>
    </div>

    <!-- 📊 市场温度计 -->
    <div style="margin-bottom:28px;">
      <h2 style="color:#3b82f6; font-size:18px; margin:0 0 14px 0; font-weight:600; display:flex; align-items:center; gap:8px;">
        <span style="width:3px; height:20px; background:#3b82f6; border-radius:2px;"></span>
        📊 市场温度计
      </h2>
      <div class="glass-card" style="background:rgba(30,41,59,0.6); padding:20px; border-radius:14px; border:1px solid rgba(255,255,255,0.06);">
        <!-- 根据素材填写：
        - 沪深总成交额（与昨日对比）
        - 涨跌家数比
        - 市场情绪判断（放量活跃/缩量观望）

        使用进度条可视化：
        <div style="display:flex; align-items:center; gap:10px; margin:10px 0;">
          <span style="color:#94a3b8; min-width:80px; font-size:13px;">沪深成交</span>
          <div style="flex:1; background:rgba(255,255,255,0.08); border-radius:4px; height:6px; overflow:hidden;">
            <div style="width:75%; height:100%; background:linear-gradient(90deg,#22c55e,#3b82f6); border-radius:4px;"></div>
          </div>
          <span style="color:#22c55e; min-width:80px; font-size:12px; text-align:right;">1.2万亿 ↑15%</span>
        </div>
        -->
      </div>
    </div>

    <!-- 💰 资金流向地图 -->
    <div style="margin-bottom:28px;">
      <h2 style="color:#3b82f6; font-size:18px; margin:0 0 14px 0; font-weight:600; display:flex; align-items:center; gap:8px;">
        <span style="width:3px; height:20px; background:#3b82f6; border-radius:2px;"></span>
        💰 资金流向地图
      </h2>

      <!-- 流入板块 -->
      <div style="margin-bottom:16px;">
        <div style="color:#22c55e; font-size:14px; font-weight:600; margin-bottom:10px;">
          🟢 主力净流入板块
        </div>
        <div class="glass-card" style="background:rgba(34,197,94,0.08); padding:16px; border-radius:12px; border:1px solid rgba(34,197,94,0.15);">
          <table style="width:100%; border-collapse:collapse;">
            <tr style="color:#94a3b8; font-size:12px; border-bottom:1px solid rgba(255,255,255,0.05);">
              <th style="text-align:left; padding:8px 0; font-weight:500;">板块</th>
              <th style="text-align:right; padding:8px 0; font-weight:500;">流入金额</th>
              <th style="text-align:right; padding:8px 0; font-weight:500;">涨幅</th>
            </tr>
            <!-- 根据素材填充TOP5板块数据 -->
            <!-- 示例：
            <tr style="color:#e2e8f0; font-size:13px;">
              <td style="padding:10px 0;">🏥 医药生物</td>
              <td style="text-align:right; color:#22c55e;">+15.8亿</td>
              <td style="text-align:right; color:#22c55e;">+3.2%</td>
            </tr>
            -->
          </table>
        </div>
      </div>

      <!-- 流出板块 -->
      <div>
        <div style="color:#ef4444; font-size:14px; font-weight:600; margin-bottom:10px;">
          🔴 主力净流出板块
        </div>
        <div class="glass-card" style="background:rgba(239,68,68,0.08); padding:16px; border-radius:12px; border:1px solid rgba(239,68,68,0.15);">
          <table style="width:100%; border-collapse:collapse;">
            <tr style="color:#94a3b8; font-size:12px; border-bottom:1px solid rgba(255,255,255,0.05);">
              <th style="text-align:left; padding:8px 0; font-weight:500;">板块</th>
              <th style="text-align:right; padding:8px 0; font-weight:500;">流出金额</th>
              <th style="text-align:right; padding:8px 0; font-weight:500;">跌幅</th>
            </tr>
            <!-- 根据素材填充TOP5板块数据 -->
          </table>
        </div>
      </div>
    </div>

    <!-- 🔥 热点事件溯源 -->
    <div style="margin-bottom:28px;">
      <h2 style="color:#3b82f6; font-size:18px; margin:0 0 14px 0; font-weight:600; display:flex; align-items:center; gap:8px;">
        <span style="width:3px; height:20px; background:#3b82f6; border-radius:2px;"></span>
        🔥 热点事件溯源
      </h2>
      <div class="glass-card" style="background:rgba(30,41,59,0.6); padding:20px; border-radius:14px; border:1px solid rgba(255,255,255,0.06);">
        <p style="color:#e2e8f0; font-size:14px; line-height:1.9; margin:0;">
          <!-- 根据素材填写：
          1. 今日热点事件（政策/新闻/突发）
          2. 受益板块分析
          3. 资金流向逻辑

          格式：
          <strong style="color:#3b82f6;">【事件】</strong> 事件描述...<br><br>
          <strong style="color:#22c55e;">【受益板块】</strong> 板块名称，原因...<br><br>
          <strong style="color:#fbbf24;">【资金反应】</strong> 主力净流入XX亿...
          -->
        </p>
      </div>
    </div>

    <!-- 🎯 狙击清单 -->
    <div style="margin-bottom:28px;">
      <h2 style="color:#3b82f6; font-size:18px; margin:0 0 14px 0; font-weight:600; display:flex; align-items:center; gap:8px;">
        <span style="width:3px; height:20px; background:#3b82f6; border-radius:2px;"></span>
        🎯 狙击清单
      </h2>

      <!-- 潜力股池 -->
      <div style="margin-bottom:20px;">
        <div style="color:#22c55e; font-size:14px; font-weight:600; margin-bottom:10px;">
          🚀 潜力股池（买入候选）
        </div>
        <div class="glass-card" style="background:rgba(34,197,94,0.06); padding:18px; border-radius:12px; border:1px solid rgba(34,197,94,0.12);">
          <p style="color:#e2e8f0; font-size:13px; line-height:1.9; margin:0;">
            <!-- 列出5只潜力股：
            <span style="color:#22c55e;">▸</span> <strong style="color:#e2e8f0;">股票名称（代码）</strong><br>
            &nbsp;&nbsp;• 评分：XX分 | 量能：持续放量<br>
            &nbsp;&nbsp;• 技术形态：突破平台/金叉等<br>
            &nbsp;&nbsp;• 逻辑：热点板块+资金流入+基本面支撑<br><br>
            -->
          </p>
        </div>
      </div>

      <!-- 风险预警 -->
      <div>
        <div style="color:#ef4444; font-size:14px; font-weight:600; margin-bottom:10px;">
          ⚠️ 风险预警（规避品种）
        </div>
        <div class="glass-card" style="background:rgba(239,68,68,0.06); padding:18px; border-radius:12px; border:1px solid rgba(239,68,68,0.12);">
          <p style="color:#e2e8f0; font-size:13px; line-height:1.9; margin:0;">
            <!-- 列出3只风险股：
            <span style="color:#ef4444;">▸</span> <strong style="color:#e2e8f0;">股票名称（代码）</strong><br>
            &nbsp;&nbsp;• 量能：高位缩量/爆量后萎缩<br>
            &nbsp;&nbsp;• 技术形态：滞涨/破位<br>
            &nbsp;&nbsp;• 风险：逆市下跌+资金流出<br><br>
            -->
          </p>
        </div>
      </div>
    </div>

    <!-- 💡 明日作战计划 -->
    <div style="margin-bottom:28px;">
      <div class="glass-card" style="background:rgba(59,130,246,0.08); padding:22px; border-radius:14px; border:1px solid rgba(59,130,246,0.25);">
        <h2 style="color:#3b82f6; font-size:18px; margin:0 0 14px 0; font-weight:600;">
          💡 明日作战计划
        </h2>
        <p style="color:#e2e8f0; font-size:14px; line-height:1.9; margin:0;">
          <!-- 根据素材给出明日建议：
          <strong style="color:#3b82f6;">【延续板块】</strong> 今日强势能否延续...<br><br>
          <strong style="color:#22c55e;">【轮动方向】</strong> 资金可能流向...<br><br>
          <strong style="color:#ef4444;">【风险提示】</strong> 需要规避的板块/个股...
          -->
        </p>
      </div>
    </div>

    <!-- 底部 -->
    <div style="text-align:center; padding:20px 0; border-top:1px solid rgba(255,255,255,0.06);">
      <p style="color:#94a3b8; font-size:14px; font-style:italic; margin:0; font-weight:500;">
        💬 "今日毒舌点评：[根据素材写一句犀利总结]"
      </p>
      <p style="color:#cbd5e1; font-size:12px; margin-top:14px; line-height:1.6;">
        ⚠️ 本报告仅供参考，不构成投资建议。股市有风险，投资需谨慎。
      </p>
      <p style="color:#94a3b8; font-size:13px; margin-top:8px;">
        爱波塔 · 资金流研究中心 | <span style="color:#3b82f6;">www.aiprota.com</span>
      </p>
    </div>

  </div>
</body>
</html>
```

## 4. 关键设计要求

### 配色规范：
- 主标题：#3b82f6（蓝色）
- 流入：#22c55e（绿色）
- 流出：#ef4444（红色）
- 警示：#fbbf24（金色）
- 背景：深色系 #0f172a / #1e293b

### 表格设计：
- 简洁三列布局
- 流入用绿色背景卡片
- 流出用红色背景卡片
- 移动端自动换行

### 内容要求：
1. **数据必须来自素材**，不要编造
2. 每个板块都要有具体数据支撑
3. 狙击清单中每只股票要有完整逻辑（量能+技术+基本面）
4. 明日计划要基于今日资金流向推导
5. 毒舌点评要犀利有趣，但不煽动情绪

### 文风：
- 幽默但专业
- 有态度但不夸张
- 给建议但加免责
- 通俗易懂，避免术语堆砌

【输出】：
只返回完整的HTML代码，不要有任何 ```html 标记或多余说明。
"""

    res = llm.invoke([HumanMessage(content=prompt)])
    html = res.content.replace("```html", "").replace("```", "").strip()
    return html


# ==========================================
# 4. 发布到订阅中心
# ==========================================
def extract_summary_from_html(html_content: str) -> str:
    """从HTML中提取摘要"""
    import re
    # 尝试提取市场温度计内容作为摘要
    match = re.search(r'市场温度计.*?<div[^>]*>(.*?)</div>', html_content, re.DOTALL)
    if match:
        summary = re.sub(r'<[^>]+>', '', match.group(1))
        return summary[:200].strip()
    return "今日市场资金流向分析"


def publish_fund_flow_report(html_content: str):
    """
    发布到订阅中心数据库
    """
    print("📤 [发布] 正在发布资金流晚报...")

    today_str = datetime.now().strftime("%m月%d日")
    weekday = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][datetime.now().weekday()]

    title = f"{today_str} {weekday} 资金流晚报"
    summary = extract_summary_from_html(html_content)

    try:
        success, result = sub_svc.publish_content(
            channel_code="fund_flow_report",  # 资金流晚报专属频道
            title=title,
            content=html_content,
            summary=summary if summary else f"{today_str}市场资金流向分析"
        )

        if success:
            print(f"✅ [发布] 成功发布到数据库，内容ID: {result}")
            return True, result
        else:
            print(f"❌ [发布] 发布失败: {result}")
            return False, result
    except Exception as e:
        print(f"❌ [发布] 发布异常: {e}")
        return False, str(e)


# ==========================================
# 5. 主流程
# ==========================================
def main():
    """
    主流程：采集 → 撰写 → 发布
    """
    start_t = time.time()
    print("=" * 60)
    print("🚀 资金流晚报生成器启动")
    print("=" * 60)

    # Step 1: AI 记者采集数据
    print("\n【第一步】数据采集中...")
    material = collect_fund_flow_data()

    if len(material) < 100:
        print("❌ 采集素材失败，素材内容过少")
        return

    # Step 2: AI 主编撰写报告
    print("\n【第二步】撰写报告中...")
    report_html = draft_fund_flow_report(material)

    # Step 3: 保存预览
    preview_path = "preview_fund_flow_report.html"
    with open(preview_path, "w", encoding="utf-8") as f:
        f.write(report_html)
    print(f"✅ 预览文件已保存: {preview_path}")

    if len(report_html) < 300:
        print("❌ 报告内容过少，取消发布")
        return

    # Step 4: 发布到订阅中心
    print("\n【第三步】发布到订阅中心...")
    pub_success, pub_result = publish_fund_flow_report(report_html)

    # 汇总
    print(f"\n{'=' * 60}")
    print(f"📊 生成结果汇总")
    print(f"{'=' * 60}")
    print(f"数据采集: ✅ 成功")
    print(f"报告撰写: ✅ 成功")
    print(f"数据库发布: {'✅ 成功' if pub_success else '❌ 失败'}")
    print(f"预览文件: {preview_path}")
    print(f"⏱️ 总耗时: {time.time() - start_t:.1f} 秒")
    print("=" * 60)


if __name__ == "__main__":
    main()