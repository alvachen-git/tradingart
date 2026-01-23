import pandas as pd
import os
import time
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from langchain_community.chat_models import ChatTongyi
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.prebuilt import create_react_agent

# ==========================================
# 1. 引入全套工具 (Toolbox)
# ==========================================
from news_tools import get_financial_news
from fund_flow_tools import tool_get_retail_money_flow
from futures_fund_flow_tools import get_futures_fund_flow
from volume_oi_tools import get_option_volume_abnormal, analyze_etf_option_sentiment
from screener_tool import search_top_stocks
from kline_tools import analyze_kline_pattern
from data_engine import get_commodity_iv_info, search_broker_holdings_on_date
from email_utils2 import send_email
from search_tools import search_web
from polymarket_tool import tool_get_polymarket_sentiment
from market_tools import get_today_hotlist, get_finance_related_trends

# 1. 初始化环境
load_dotenv(override=True)

# 数据库连接
db_url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_url)

# 初始化 LLM
llm = ChatTongyi(model="qwen-plus", api_key=os.getenv("DASHSCOPE_API_KEY"))


# ==========================================
# 2. 定义【AI 首席记者】(The Reporter)
# ==========================================
def collect_data_via_agent():
    """
    🔥 派出 AI 记者去采集素材
    """
    print("🕵️‍♂️ [AI记者] 正在出发采集全市场情报 (ReAct 模式)...")

    tools = [
        # 舆情类
        get_finance_related_trends,
        get_today_hotlist,
        tool_get_polymarket_sentiment,
        get_financial_news,
        search_web,

        # 资金类
        tool_get_retail_money_flow,
        get_futures_fund_flow,
        search_broker_holdings_on_date,

        # 期权/技术类
        get_commodity_iv_info,
        analyze_etf_option_sentiment,
        get_option_volume_abnormal,
        analyze_kline_pattern,

        # 选股类
        search_top_stocks
    ]

    today_str = datetime.now().strftime("%Y年%m月%d日")

    system_prompt = f"""
    你是一位**顶级财经记者**，正在为今天的《晚间深度复盘日报》采集素材。
    当前日期：{today_str}。

    【你的任务目标】：
    利用手中的工具，主动发现今日市场的**核心噱头**和**异常数据**。

    【采集策略 (思维链)】：

    ## 第一步：先找热点
    - 调用 `get_financial_news` 看当天财经新闻
    - 用 `get_finance_related_trends` 或 `get_today_hotlist` 看今天大家在讨论什么
    - 发现热点后，可以针对热点去调用 search_web 挖掘细节

    ## 第二步：宏观预测
    - 针对今天的热点事件（如美联储、地缘），调用 `tool_get_polymarket_sentiment` 看市场押注概率

    ## 第三步：资金流向
    - 调用 `tool_get_retail_money_flow` 看当天股票板块资金

    ## 第四步：期货商持仓分析 
    - 调用 `search_broker_holdings_on_date` 记录以下期货商的前3大多头净持仓和前3大空头净持仓
    - 海通期货
    - 东证期货
    - 国泰君安


    ## 第五步：⚠️【必做】商品期货深度分析
    **这是强制任务，必须完成！**

    请对以下 10 个核心商品逐一调用 `analyze_kline_pattern` 做技术分析：
    1. **黄金** 
    2. **白银**  
    3. **原油** 
    4. **铜** 
    5. **碳酸锂** 
    6. **铁矿石** 
    7. **豆粕** 
    8. **橡胶** 
    9. **棉花**
    10.**PTA**

    对每个品种，记录：
    - 当前趋势（多/空/震荡）
    - K线形态（如大阳线、十字星、吞噬等）
    - 关键支撑/压力位
    - 你的短期判断

    ## 第六步：ETF期权分析 (Options)
    记录以下 ETF 的期权IV等级和K线分析：
    - 510300
    - 510500
    - 159915
    - 588000
    - 510050
    调用get_commodity_iv_info计算IV等级，不是单纯IV
    调用analyze_kline_pattern做最近几天技术面分析

    ## 第七步：选股与技术 (Picks)
    - 调用 `search_top_stocks` 选出 5 个今日出现突破的强势股
    - 再选出 5 个出现破位或下降三法的危险弱势股

    【输出要求】：
    请将你采集到的所有有价值的信息，整理成一篇**详细的素材笔记**返回。

    **特别注意**：商品期货分析部分必须包含完整的 10 个品种分析结果！
    如果某个品种查询失败，请注明并继续下一个。

    不要写成最终新闻稿，只要罗列事实、数据和你的发现即可，供主编后续使用。
    """

    reporter_agent = create_react_agent(llm, tools, prompt=system_prompt)

    try:
        trigger_msg = """开始今天的市场扫描任务，请确保：
        1. 覆盖宏观、资金、期权和选股四个维度
        2. ⚠️ 必须完成 10 个商品期货的技术分析（黄金、白银、原油、铜、铁矿石、碳酸锂、豆粕、橡胶、棉花、PTA）
        3. 每个商品都要给出趋势判断和关键点位
        """

        result = reporter_agent.invoke(
            {"messages": [HumanMessage(content=trigger_msg)]},
            {"recursion_limit": 160}
        )

        collected_content = result["messages"][-1].content
        print("✅ [AI记者] 采集完成，素材已提交。")
        return collected_content

    except Exception as e:
        print(f"❌ [AI记者] 采集过程出错: {e}")
        return "AI 采集失败，请检查日志。"


def draft_report(raw_material):
    """
    让 AI 主编基于记者提供的素材写稿
    🔥 v2.0 升级版：玻璃拟态 + 响应式 + 商品图标 + IV进度条
    """
    print("✏️ [AI主编] 正在撰写晚报...")

    today = datetime.now().strftime("%Y年%m月%d日")
    weekday = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][datetime.now().weekday()]

    prompt = f"""
    你是【爱波塔首席投研】的主编。你的记者刚刚提交了今天的市场调研素材。
    请根据这些素材，写一份**《每日深度复盘》**。

    【记者提交的素材】：
    {raw_material}

    【写作要求】：

    ## 1. 格式：纯 HTML 代码（无 Markdown）

    ## 2. ⚠️ 核心设计理念
    - 配色统一简洁：所有板块标题统一用金色 #fbbf24
    - 邮件端：Table布局兜底 + 深色背景降级
    - 网页端：CSS增强（玻璃拟态、响应式）
    - 手机端：自动变为单列布局

    ## 3. 商品图标映射（必须使用）
    | 商品 | 图标 |
    |------|------|
    | 黄金 | 🪙 |
    | 白银 | 🥈 |
    | 原油 | 🛢️ |
    | 铜 | 🔶 |
    | 碳酸锂 | 🔋 |
    | 铁矿石 | �ite |
    | 豆粕 | 🌱 |
    | 橡胶 | 🌴 |
    | 棉花 | 🌸 |
    | PTA | 🧪 |

    ## 4. ⚠️ 趋势标签颜色（中国市场：红涨绿跌）
    - **看多/偏多**：背景 #dc2626 (红色)，白字
    - **看空/偏空**：背景 #16a34a (绿色)，白字  
    - **震荡/中性**：背景 #d97706 (橙黄)，白字

    ## 5. IV等级进度条（数据可视化）
    ```html
    <div style="display:flex; align-items:center; gap:10px; margin:6px 0;">
      <span style="color:#94a3b8; min-width:85px; font-size:13px;">沪深300</span>
      <div style="flex:1; background:rgba(255,255,255,0.08); border-radius:4px; height:6px; overflow:hidden;">
        <div style="width:45%; height:100%; background:linear-gradient(90deg,#22c55e,#eab308,#ef4444); border-radius:4px;"></div>
      </div>
      <span style="color:#22c55e; min-width:55px; font-size:12px; text-align:right;">45% 中</span>
    </div>
    ```

    ## 6. 完整 HTML 模板：

    ```html
    <!DOCTYPE html>
    <html>
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <style>
        @media screen and (max-width: 640px) {{
          .two-col-table {{ width: 100% !important; }}
          .two-col-table td {{ 
            display: block !important; 
            width: 100% !important; 
            padding: 6px 0 !important;
          }}
          .main-container {{ padding: 20px 16px !important; }}
          .section-title {{ font-size: 18px !important; }}
          .card-content {{ padding: 16px !important; }}
        }}

        @media screen {{
          .glass-card {{
            backdrop-filter: blur(12px) !important;
            -webkit-backdrop-filter: blur(12px) !important;
            background: rgba(30, 41, 59, 0.8) !important;
            border: 1px solid rgba(255,255,255,0.08) !important;
            box-shadow: 0 8px 32px rgba(0,0,0,0.3) !important;
          }}
          .glass-header {{
            backdrop-filter: blur(16px) !important;
            -webkit-backdrop-filter: blur(16px) !important;
            background: rgba(15, 23, 42, 0.9) !important;
          }}
        }}
      </style>
    </head>
    <body style="margin:0; padding:0; background:#0f172a; font-family:'PingFang SC','Microsoft YaHei',sans-serif;">

    <div class="main-container" style="max-width:700px; margin:0 auto; padding:30px 24px; background:linear-gradient(180deg,#0f172a 0%,#1e293b 100%);">

      <!-- 头部 -->
      <div class="glass-header" style="text-align:center; padding:32px 24px; border-radius:20px; background:rgba(15,23,42,0.9); border:1px solid rgba(255,255,255,0.08); margin-bottom:28px;">
        <div style="font-size:13px; color:#64748b; letter-spacing:2px; margin-bottom:8px;">AIPROTA DAILY REPORT</div>
        <h1 style="color:#fbbf24; font-size:26px; margin:0; font-weight:700; letter-spacing:2px;">📊 爱波塔复盘晚报</h1>
        <p style="color:#64748b; font-size:14px; margin-top:12px;">{today} {weekday} | 深度复盘</p>
      </div>

      <!-- 🚀 市场头条 -->
      <div style="margin-bottom:24px;">
        <h2 class="section-title" style="color:#fbbf24; font-size:18px; margin:0 0 14px 0; font-weight:600; display:flex; align-items:center; gap:8px;">
          <span style="width:3px; height:20px; background:#fbbf24; border-radius:2px;"></span>
          🚀 市场头条
        </h2>
        <div class="glass-card card-content" style="background:rgba(30,41,59,0.6); padding:18px; border-radius:14px; border:1px solid rgba(255,255,255,0.06);">
          <p style="color:#e2e8f0; font-size:14px; margin:0; line-height:1.9;">
            <!-- 根据素材填写 -->
          </p>
        </div>
      </div>

      <!-- 💰 资金暗流 -->
      <div style="margin-bottom:24px;">
        <h2 class="section-title" style="color:#fbbf24; font-size:18px; margin:0 0 14px 0; font-weight:600; display:flex; align-items:center; gap:8px;">
          <span style="width:3px; height:20px; background:#fbbf24; border-radius:2px;"></span>
          💰 资金暗流
        </h2>
        <table class="two-col-table" width="100%" cellpadding="0" cellspacing="0" border="0">
          <tr>
            <td width="50%" style="padding:0 6px 12px 0;" valign="top">
              <div class="glass-card card-content" style="background:rgba(30,41,59,0.6); padding:18px; border-radius:14px; border:1px solid rgba(255,255,255,0.06); height:100%;">
                <h4 style="color:#94a3b8; margin:0 0 10px 0; font-size:14px; font-weight:600;">📈 股票板块</h4>
                <p style="color:#e2e8f0; font-size:13px; margin:0; line-height:1.8;">
                  <!-- 根据素材填写 -->
                </p>
              </div>
            </td>
            <td width="50%" style="padding:0 0 12px 6px;" valign="top">
              <div class="glass-card card-content" style="background:rgba(30,41,59,0.6); padding:18px; border-radius:14px; border:1px solid rgba(255,255,255,0.06); height:100%;">
                <h4 style="color:#94a3b8; margin:0 0 10px 0; font-size:14px; font-weight:600;">📊 期货商持仓</h4>
                <p style="color:#e2e8f0; font-size:13px; margin:0; line-height:1.8;">
                  <!-- 根据素材填写 -->
                </p>
              </div>
            </td>
          </tr>
        </table>
      </div>

      <!-- 🏆 商品期货全景 -->
      <div style="margin-bottom:24px;">
        <h2 class="section-title" style="color:#fbbf24; font-size:18px; margin:0 0 14px 0; font-weight:600; display:flex; align-items:center; gap:8px;">
          <span style="width:3px; height:20px; background:#fbbf24; border-radius:2px;"></span>
          🏆 商品期货全景
        </h2>

        <!-- 
        商品卡片模板：
        <td width="50%" style="padding:0 6px 10px 0;" valign="top">
          <div class="glass-card" style="background:rgba(30,41,59,0.6); padding:14px 16px; border-radius:12px; border:1px solid rgba(255,255,255,0.06);">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">
              <span style="color:#e2e8f0; font-weight:600; font-size:15px;">🪙 黄金</span>
              <span style="background:#dc2626; color:white; padding:3px 12px; border-radius:12px; font-size:12px; font-weight:500;">看多</span>
            </div>
            <p style="color:#94a3b8; font-size:12px; margin:0; line-height:1.6;">
              形态：xxx<br>支撑：xxx | 压力：xxx
            </p>
          </div>
        </td>

        趋势标签：看多=#dc2626, 看空=#16a34a, 震荡=#d97706
        -->

        <table class="two-col-table" width="100%" cellpadding="0" cellspacing="0" border="0">
          <!-- 5行2列，共10个商品 -->
        </table>
      </div>

      <!-- ⚖️ 期权波动率 -->
      <div style="margin-bottom:24px;">
        <h2 class="section-title" style="color:#fbbf24; font-size:18px; margin:0 0 14px 0; font-weight:600; display:flex; align-items:center; gap:8px;">
          <span style="width:3px; height:20px; background:#fbbf24; border-radius:2px;"></span>
          ⚖️ 期权波动率
        </h2>
        <div class="glass-card card-content" style="background:rgba(30,41,59,0.6); padding:18px; border-radius:14px; border:1px solid rgba(255,255,255,0.06);">
          <!-- IV进度条 + 分析文字 -->
        </div>
      </div>

      <!-- 🐂 每日牛股 -->
      <div style="margin-bottom:24px;">
        <h2 class="section-title" style="color:#fbbf24; font-size:18px; margin:0 0 14px 0; font-weight:600; display:flex; align-items:center; gap:8px;">
          <span style="width:3px; height:20px; background:#fbbf24; border-radius:2px;"></span>
          🐂 每日牛股
        </h2>
        <div class="glass-card card-content" style="background:rgba(30,41,59,0.6); padding:18px; border-radius:14px; border:1px solid rgba(255,255,255,0.06);">
          <p style="color:#e2e8f0; font-size:13px; margin:0; line-height:1.9;">
            <!-- 用 <span style="color:#dc2626;">▸</span> 作为列表符号 -->
          </p>
        </div>
      </div>

      <!-- 🐻 风险警示 -->
      <div style="margin-bottom:24px;">
        <h2 class="section-title" style="color:#fbbf24; font-size:18px; margin:0 0 14px 0; font-weight:600; display:flex; align-items:center; gap:8px;">
          <span style="width:3px; height:20px; background:#fbbf24; border-radius:2px;"></span>
          🐻 风险警示
        </h2>
        <div class="glass-card card-content" style="background:rgba(30,41,59,0.6); padding:18px; border-radius:14px; border:1px solid rgba(255,255,255,0.06);">
          <p style="color:#e2e8f0; font-size:13px; margin:0; line-height:1.9;">
            <!-- 用 <span style="color:#16a34a;">▸</span> 作为列表符号 -->
          </p>
        </div>
      </div>

      <!-- 💡 明日策略 -->
      <div style="margin-bottom:24px;">
        <div class="glass-card" style="background:rgba(251,191,36,0.08); padding:20px; border-radius:14px; border:1px solid rgba(251,191,36,0.25);">
          <h2 style="color:#fbbf24; font-size:18px; margin:0 0 14px 0; font-weight:600;">
            💡 明日策略
          </h2>
          <p style="color:#e2e8f0; font-size:14px; line-height:1.9; margin:0;">
            <!-- 用 <strong style="color:#fbbf24;">【类别】</strong> 分类 -->
          </p>
        </div>
      </div>

      <!-- 底部 -->
      <div style="text-align:center; padding:20px 0; border-top:1px solid rgba(255,255,255,0.06);">
        <p style="color:#64748b; font-size:13px; font-style:italic; margin:0;">
          💬 "毒舌点评"
        </p>
        <p style="color:#475569; font-size:12px; margin-top:14px;">
          爱波塔 · 最懂期权的AI | www.aiprota.com
        </p>
      </div>

    </div>
    </body>
    </html>
    ```

    ## 7. 关键样式要求

    | 元素 | 字号 | 颜色 |
    |------|------|------|
    | 大标题 | 26px | #fbbf24 (金色) |
    | **所有板块标题** | 18px | **#fbbf24 (统一金色)** |
    | 卡片小标题 | 14px | #94a3b8 (灰色) |
    | 商品名称 | 15px | #e2e8f0 (亮白) |
    | 正文内容 | 13-14px | #e2e8f0 (亮白) |
    | 次要文字 | 12-13px | #94a3b8 (灰色) |

    ## 8. ⚠️ 商品期货趋势判断规则
    根据素材判断：
    - "多头趋势"、"突破"、"站上均线"、"金叉" → 标签"看多"，背景 #dc2626
    - "空头趋势"、"破位"、"跌破支撑"、"死叉" → 标签"看空"，背景 #16a34a
    - "震荡"、"盘整"、"观望" → 标签"震荡"，背景 #d97706

    ## 9. 内容要求
    - 商品期货全景：必须包含 10 个商品卡片（5行2列）
    - 期权IV：用进度条可视化
    - 数据必须来自素材，不要编造
    - 底部写一句幽默毒舌点评
    """

    res = llm.invoke([HumanMessage(content=prompt)])
    html = res.content.replace("```html", "").replace("```", "").strip()
    return html


def blast_emails(html_content):
    """发送邮件"""
    print("📧 准备群发...")
    try:
        with engine.connect() as conn:
            sql = text(
                "SELECT username, email FROM users WHERE is_subscribed = 1 AND email IS NOT NULL AND email != ''")
            df = pd.read_sql(sql, conn)

        if df.empty:
            print("📭 无订阅用户。")
            return

        today_str = datetime.now().strftime("%m月%d日")
        subject = f"【爱波塔】{today_str} | 复盘晚报"

        success_cnt = 0
        for _, row in df.iterrows():
            try:
                # 预先检查邮箱格式
                email_addr = row['email']
                if not email_addr or "@" not in str(email_addr):
                    print(f" -> 跳过无效邮箱: {row['username']}")
                    continue

                if send_email(email_addr, subject, html_content):
                    success_cnt += 1
                    print(f" -> 发送成功: {row['username']}")
                else:
                    print(f" -> 发送失败: {row['username']}")

                time.sleep(1.5)  # 加上延时

            except Exception as inner_e:
                # 确保单个人出错不会卡死整个循环
                print(f" -> 处理用户 {row['username']} 时发生未知错误: {inner_e}")
                continue
    except Exception as e:
        print(f"❌ 群发错误: {e}")


if __name__ == "__main__":
    start_t = time.time()

    # 1. AI 记者出动
    material = collect_data_via_agent()

    # 2. AI 主编撰稿
    if len(material) > 100:
        report_html = draft_report(material)

        # 保存到本地预览
        with open("preview_report.html", "w", encoding="utf-8") as f:
            f.write(report_html)
        print("📄 预览文件已保存: preview_report.html")

        # 3. 发送
        if len(report_html) > 300:
            blast_emails(report_html)
        else:
            print("❌ 报告内容过少，取消发送")
    else:
        print("❌ 采集素材失败")

    print(f"⏱️ 总耗时: {time.time() - start_t:.1f} 秒")