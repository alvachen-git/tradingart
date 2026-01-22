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
    - 调用 `search_broker_holdings_on_date` 记录以下期货商的前2大多头净持仓和前2大空头净持仓
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
    8. **棕榈油** 
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
        2. ⚠️ 必须完成 10 个商品期货的技术分析（黄金、白银、原油、铜、铁矿石、碳酸锂、豆粕、棕榈油、棉花、PTA）
        3. 每个商品都要给出趋势判断和关键点位
        """

        result = reporter_agent.invoke(
            {"messages": [HumanMessage(content=trigger_msg)]},
            {"recursion_limit": 150}
        )

        collected_content = result["messages"][-1].content
        print("✅ [AI记者] 采集完成，素材已提交。")
        return collected_content

    except Exception as e:
        print(f"❌ [AI记者] 采集过程出错: {e}")
        return "AI 采集失败，请检查日志。"


def draft_report(raw_material):
    """让 AI 主编基于记者提供的素材写稿 - 精美排版版"""
    print("✍️ [AI主编] 正在撰写晚报...")

    today = datetime.now().strftime("%Y年%m月%d日")
    weekday = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][datetime.now().weekday()]

    # 🔥 优化：更大的字体 + 更柔和的背景
    prompt = f"""
    你是【爱波塔首席投研】的主编。你的记者刚刚提交了今天的市场调研素材。
    请根据这些素材，写一份**《每日深度复盘》**。

    【记者提交的素材】：
    {raw_material}

    【写作要求】：

    ## 1. 格式：纯 HTML 代码（无 Markdown）

    ## 2. ⚠️ 邮件兼容性要求（极重要！）
    部分邮箱（如139邮箱、QQ邮箱）会剥离CSS背景色，所以：
    - **正文文字必须用深色 #1a202c**，确保白底下也能看清
    - **次要文字用 #4a5568**
    - **标题用深色版主题色**（见下方色板）
    - 每个div必须同时加 style="background:..." 和 bgcolor="..." 双重保障
    - 用 table 布局替代 CSS grid，兼容性更好

    ## 3. 邮件兼容色板（必须严格遵守）
    | 用途 | 颜色 |
    |------|------|
    | 正文文字 | #1a202c (深黑) |
    | 次要文字 | #4a5568 (深灰) |
    | 金色标题 | #b7791f |
    | 蓝色标题 | #2b6cb0 |
    | 绿色标题 | #276749 |
    | 紫色标题 | #6b46c1 |
    | 红色标题 | #c53030 |
    | 灰色标题 | #4a5568 |

    ## 4. HTML 结构模板（邮件兼容版）：

    ```html
    <div style="max-width: 700px; margin: 0 auto; font-family: 'PingFang SC', 'Microsoft YaHei', sans-serif; background: #2d3748; padding: 35px; border-radius: 20px; line-height: 1.8;" bgcolor="#2d3748">

      <!-- 头部 -->
      <div style="text-align: center; padding-bottom: 30px; border-bottom: 2px solid #4a5568;">
        <h1 style="color: #f6c744; font-size: 32px; margin: 0; letter-spacing: 3px; font-weight: 700;">📊 爱波塔复盘晚报</h1>
        <p style="color: #4a5568; font-size: 16px; margin-top: 12px;">{today} {weekday} | 深度复盘</p>
      </div>

      <!-- 🚀 市场头条 -->
      <div style="margin-top: 30px;">
        <h2 style="color: #2b6cb0; font-size: 22px; margin-bottom: 18px; font-weight: 600; border-left: 4px solid #3182ce; padding-left: 12px;">
          🚀 市场头条
        </h2>
        <div style="background: #ebf8ff; border: 1px solid #3182ce; padding: 20px; border-radius: 12px;" bgcolor="#ebf8ff">
          <p style="color: #1a202c; font-size: 16px; margin: 0; line-height: 1.9;">
            <!-- 头条内容 -->
          </p>
        </div>
      </div>

      <!-- 💰 资金暗流 -->
      <div style="margin-top: 30px;">
        <h2 style="color: #276749; font-size: 22px; margin-bottom: 18px; font-weight: 600; border-left: 4px solid #38a169; padding-left: 12px;">
          💰 资金暗流
        </h2>
        <table width="100%" cellpadding="0" cellspacing="8" border="0">
          <tr>
            <td width="50%" valign="top">
              <div style="background: #f0fff4; padding: 20px; border-radius: 12px; border: 1px solid #38a169;" bgcolor="#f0fff4">
                <h4 style="color: #276749; margin: 0 0 12px 0; font-size: 17px; font-weight: 600;">📈 股票板块</h4>
                <p style="color: #1a202c; font-size: 15px; margin: 0; line-height: 1.8;">
                  <!-- 内容 -->
                </p>
              </div>
            </td>
            <td width="50%" valign="top">
              <div style="background: #f0fff4; padding: 20px; border-radius: 12px; border: 1px solid #38a169;" bgcolor="#f0fff4">
                <h4 style="color: #276749; margin: 0 0 12px 0; font-size: 17px; font-weight: 600;">📊 期货商持仓</h4>
                <p style="color: #1a202c; font-size: 15px; margin: 0; line-height: 1.8;">
                  <!-- 内容 -->
                </p>
              </div>
            </td>
          </tr>
        </table>
      </div>

      <!-- 🏆 商品期货全景 -->
      <div style="margin-top: 30px;">
        <h2 style="color: #b7791f; font-size: 22px; margin-bottom: 18px; font-weight: 600; border-left: 4px solid #d69e2e; padding-left: 12px;">
          🏆 商品期货全景
        </h2>
        <table width="100%" cellpadding="0" cellspacing="8" border="0">
          <tr>
            <!-- 黄金 -->
            <td width="50%" valign="top">
              <div style="background: #fffff0; padding: 16px; border-radius: 12px; border: 1px solid #d69e2e;" bgcolor="#fffff0">
                <div style="margin-bottom: 10px;">
                  <span style="color: #b7791f; font-weight: 700; font-size: 17px;">🥇 黄金</span>
                  <span style="background: #38a169; color: white; padding: 4px 12px; border-radius: 6px; font-size: 13px; font-weight: 600; margin-left: 10px;">看多</span>
                </div>
                <p style="color: #1a202c; font-size: 15px; margin: 0; line-height: 1.7;">
                  形态：xxx<br>支撑：xxx | 压力：xxx
                </p>
              </div>
            </td>
            <!-- 白银 -->
            <td width="50%" valign="top">
              <div style="background: #fffff0; padding: 16px; border-radius: 12px; border: 1px solid #d69e2e;" bgcolor="#fffff0">
                <div style="margin-bottom: 10px;">
                  <span style="color: #4a5568; font-weight: 700; font-size: 17px;">🥈 白银</span>
                  <span style="background: #d69e2e; color: white; padding: 4px 12px; border-radius: 6px; font-size: 13px; font-weight: 600; margin-left: 10px;">震荡</span>
                </div>
                <p style="color: #1a202c; font-size: 15px; margin: 0; line-height: 1.7;">
                  形态：xxx<br>支撑：xxx | 压力：xxx
                </p>
              </div>
            </td>
          </tr>
          <!-- 第二行：原油、铜 -->
          <!-- 第三行：碳酸锂、铁矿石 -->
          <!-- 第四行：豆粕、棕榈油 -->
          <!-- 第五行：棉花、PTA -->
          <!-- 共10个商品，用5行2列的table布局 -->
        </table>
      </div>

      <!-- ⚖️ 期权波动率 -->
      <div style="margin-top: 30px;">
        <h2 style="color: #6b46c1; font-size: 22px; margin-bottom: 18px; font-weight: 600; border-left: 4px solid #805ad5; padding-left: 12px;">
          ⚖️ 期权波动率
        </h2>
        <div style="background: #faf5ff; padding: 20px; border-radius: 12px; border: 1px solid #805ad5;" bgcolor="#faf5ff">
          <p style="color: #1a202c; font-size: 15px; margin: 0; line-height: 1.8;">
            <!-- ETF期权数据 -->
          </p>
        </div>
      </div>

      <!-- 🐂 每日牛股 -->
      <div style="margin-top: 30px;">
        <h2 style="color: #c53030; font-size: 22px; margin-bottom: 18px; font-weight: 600; border-left: 4px solid #e53e3e; padding-left: 12px;">
          🐂 每日牛股
        </h2>
        <div style="background: #fff5f5; padding: 20px; border-radius: 12px; border: 1px solid #e53e3e;" bgcolor="#fff5f5">
          <p style="color: #1a202c; font-size: 15px; margin: 0; line-height: 1.8;">
            <!-- 牛股列表 -->
          </p>
        </div>
      </div>

      <!-- 🐻 风险警示 -->
      <div style="margin-top: 30px;">
        <h2 style="color: #4a5568; font-size: 22px; margin-bottom: 18px; font-weight: 600; border-left: 4px solid #718096; padding-left: 12px;">
          🐻 风险警示
        </h2>
        <div style="background: #f7fafc; padding: 20px; border-radius: 12px; border: 1px solid #718096;" bgcolor="#f7fafc">
          <p style="color: #1a202c; font-size: 15px; margin: 0; line-height: 1.8;">
            <!-- 熊股列表 -->
          </p>
        </div>
      </div>

      <!-- 💡 明日策略 -->
      <div style="margin-top: 35px; background: #fffff0; padding: 25px; border-radius: 16px; border: 3px solid #d69e2e;" bgcolor="#fffff0">
        <h2 style="color: #b7791f; font-size: 22px; margin: 0 0 18px 0; font-weight: 600;">
          💡 明日策略
        </h2>
        <p style="color: #1a202c; font-size: 16px; line-height: 2; margin: 0;">
          <!-- 操作建议 -->
        </p>
      </div>

      <!-- 底部毒舌 -->
      <div style="margin-top: 40px; padding-top: 25px; border-top: 2px solid rgba(255,255,255,0.08); text-align: center;">
        <p style="color: #718096; font-size: 15px; font-style: italic; margin: 0; line-height: 1.6;">
          💬 "这里写一句幽默毒舌的点评"
        </p>
        <p style="color: #4a5568; font-size: 13px; margin-top: 20px;">
          爱波塔 · 最懂期权的AI | www.aiprota.com
        </p>
      </div>

    </div>
    ```

    ## 4. 关键样式要求（务必遵守）

    | 元素 | 字号 | 颜色 |
    |------|------|------|
    | 大标题 | 32px | #f6c744 (金色) |
    | 板块标题 | 22px | 深色版主题色(见色板) |
    | 卡片小标题 | 17px | 深色版主题色 |
    | **正文内容** | 15-16px | **#1a202c (深黑)** |
    | 次要文字 | 14px | #4a5568 (深灰) |
    | 行高 | 1.7-2.0 | - |

    ## 5. 趋势标签颜色
    - 看多：背景 #38a169，白字
    - 看空：背景 #e53e3e，白字  
    - 震荡：背景 #d69e2e，白字

    ## 6. 内容要求
    - **商品期货全景**：必须包含 10 个商品的分析卡片，用table布局(5行2列)
    - 每个商品显示：趋势标签、形态、支撑/压力位
    - 数据必须来自素材，不要编造
    -  明日策略的内容要具体，但用幽默的文风表达
    - 文字要精炼但信息量足

    ## 7. 毒舌彩蛋
    - 底部写一句幽默毒舌点评（嘲讽追高、踏空、满仓抄底等）
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
            if send_email(row['email'], subject, html_content):
                success_cnt += 1
                print(f" -> 发送成功: {row['username']}")
            else:
                print(f" -> 发送失败: {row['username']}")
        print(f"✅ 推送完成: {success_cnt}/{len(df)}")
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