# tasks.py
import os
import sys
from celery_config import celery_app
from dotenv import load_dotenv

load_dotenv(override=True)

# 清理代理
for key in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"]:
    if key in os.environ:
        del os.environ[key]

from langchain_community.chat_models import ChatTongyi
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
from agent_core import build_trading_graph
import re


@celery_app.task(bind=True, name='tasks.process_ai_query')
def process_ai_query(self, user_id, prompt, image_context="", risk_preference="稳健型", history_messages=None):
    """后台处理 AI 查询"""
    try:
        self.update_state(state='PROCESSING', meta={'progress': '正在初始化 AI 模型...'})

        # 初始化 LLM
        fast_llm = ChatTongyi(model="qwen-turbo", streaming=False, temperature=0.1)
        mid_llm = ChatTongyi(model="qwen-plus", streaming=False, temperature=0.2)
        smart_llm = ChatTongyi(model="qwen-max", streaming=False, temperature=0.4)

        self.update_state(state='PROCESSING', meta={'progress': '正在构建分析团队...'})

        app = build_trading_graph(fast_llm, mid_llm, smart_llm)

        final_prompt = image_context + prompt if image_context else prompt
        input_messages = []

        if history_messages:
            for msg in history_messages:
                if msg.get("role") == "user":
                    input_messages.append(HumanMessage(content=msg["content"]))
                elif msg.get("role") in ["assistant", "ai"]:
                    content = msg["content"][:500] + "..." if len(msg["content"]) > 500 else msg["content"]
                    input_messages.append(AIMessage(content=content))

        input_messages.append(HumanMessage(content=final_prompt))

        inputs = {
            "user_query": final_prompt,
            "messages": input_messages,
            "risk_preference": risk_preference
        }

        self.update_state(state='PROCESSING', meta={'progress': '团队正在协作分析...'})

        final_state = app.invoke(inputs, {"recursion_limit": 30})

        self.update_state(state='PROCESSING', meta={'progress': '正在整理报告...'})

        messages = final_state.get("messages", [])
        input_message_count = len(input_messages)
        new_messages = messages[input_message_count:]

        # 🔥 [修复] 初始化时包含所有可能的键，避免 KeyError
        report_card = {
            "analyst": "",
            "monitor": "",
            "strategist": "",
            "researcher": "",
            "news": "",           # 🔥 [新增] 添加 news 键
            "generalist": "",
            "screener": "",
            "roaster": "",
            "macro_analyst": "",
            "chatter": "",        # 🔥 [修复] 添加 chatter 键
            "finalizer": ""
        }

        seen_contents = set()
        for msg in new_messages:
            content = getattr(msg, 'content', str(msg))
            content_hash = hash(content[:100])
            if content_hash in seen_contents:
                continue
            seen_contents.add(content_hash)

            # 技术分析师
            if "【技术分析】" in content or "技术分析" in content[:50]:
                report_card["analyst"] = content

            # 数据监控员
            elif "【数据监控】" in content or "资金面监控" in content:
                report_card["monitor"] = content

            # 王牌分析师
            elif "【王牌分析】" in content or "【深度分析】" in content:
                report_card["generalist"] = content

            # 最终决策者
            elif "【最终决策】" in content or "最终建议" in content:
                report_card["finalizer"] = content

            # 情报研究员（支持多种标题）
            elif any(keyword in content for keyword in [
                "【情报与舆情】",
                "【市场情报】",
                "【舆情分析】",
                "【新闻分析】"
            ]):
                report_card["researcher"] = content
                report_card["news"] = content

            # 🔥 [关键修复] 选股策略师 - 支持多种标题变体
            elif any(keyword in content for keyword in [
                "【选股策略】",
                "【股票推荐】",
                "【K线趋势股推荐】",  # 🔥 新增
                "【精选股票】",  # 🔥 新增
                "【推荐股票】",  # 🔥 新增
                "【个股推荐】",  # 🔥 新增
                "【标的推荐】"  # 🔥 新增
            ]):
                report_card["screener"] = content

            # 期权策略师
            elif "【期权策略】" in content or "期权建议" in content:
                report_card["strategist"] = content

            # 宏观分析师
            elif "【宏观分析】" in content or "宏观经济" in content:
                report_card["macro_analyst"] = content

            # 闲聊/知识问答
            elif "【闲聊】" in content or "【知识问答】" in content:
                report_card["chatter"] = content

            # 吐槽模式
            elif "【吐槽】" in content:
                report_card["roaster"] = content

        # 这是兜底逻辑，防止因为标题变化导致内容丢失
        if not any(report_card.values()):
            print(f"⚠️ 警告：所有标记都未匹配，尝试智能识别...")
            for msg in new_messages:
                content = getattr(msg, 'content', str(msg))
                # 如果内容很长且包含分析关键词，默认作为王牌分析
                if len(content) > 200 and any(kw in content for kw in ["分析", "建议", "推荐", "策略"]):
                    print(f"✅ 智能识别为王牌分析：{content[:100]}...")
                    report_card["generalist"] = content
                    break

        final_response = ""

        # 🔥 [修复] 全部使用 .get() 安全访问，避免 KeyError
        chatter_txt = report_card.get("chatter", "")
        generalist_txt = report_card.get("generalist", "")
        finalizer_txt = report_card.get("finalizer", "")
        roaster_txt = report_card.get("roaster", "")

        # 场景 0: 吐槽模式
        if roaster_txt:
            final_response = roaster_txt
        # 场景 1: 闲聊/知识问答
        elif chatter_txt and "已制定计划" not in chatter_txt:
            final_response = chatter_txt
        # 场景 2: 王牌分析师独立回答
        elif generalist_txt and not any([
            report_card.get("analyst", ""),
            report_card.get("strategist", ""),
            report_card.get("monitor", ""),
            report_card.get("researcher", "")
        ]):
            final_response = generalist_txt
        # 场景 3: 情报研究员
        elif report_card.get("researcher", ""):
            final_response = report_card["researcher"]
        # 场景 4: 选股策略
        elif report_card.get("screener", ""):
            final_response = report_card["screener"]
        # 场景 5: 综合报告
        else:
            is_integrated = finalizer_txt and "PASS" not in finalizer_txt

            if is_integrated:
                final_response = finalizer_txt
            else:
                # 拼接各模块报告
                if report_card.get("macro_analyst", ""):
                    final_response += f"{report_card['macro_analyst']}\n\n"
                if report_card.get("analyst", ""):
                    final_response += f"{report_card['analyst']}\n\n"
                if report_card.get("monitor", "") and report_card["monitor"] != "无数据":
                    final_response += f"### 💸 资金面监控\n{report_card['monitor']}\n\n"
                if report_card.get("strategist", ""):
                    final_response += f"### ⚖️ 衍生品策略建议\n{report_card['strategist']}\n\n"
                if report_card.get("screener", ""):
                    final_response += f"{report_card['screener']}\n\n"
                if report_card.get("news", ""):
                    final_response += f"### 📰 相关情报\n{report_card['news']}\n"

                # 如果 finalizer 有修正意见，追加在最后
                if finalizer_txt and "PASS" not in finalizer_txt:
                    final_response += f"\n\n---\n{finalizer_txt}"

        # 🔥 [新增] 最后的兜底检查
        if not final_response or len(final_response.strip()) < 10:
            print(f"❌ 严重警告：final_response 为空或太短")
            print(f"report_card 内容：{report_card}")
            print(f"所有新消息：")
            for i, msg in enumerate(new_messages):
                content = getattr(msg, 'content', str(msg))
                print(f"  消息 {i}: {content[:200]}...")

            # 兜底方案：返回所有新消息的拼接
            final_response = "\n\n".join([
                getattr(msg, 'content', str(msg))
                for msg in new_messages
                if len(getattr(msg, 'content', str(msg))) > 50
            ])

            if not final_response:
                final_response = "抱歉，AI 分析过程出现异常，请重试或联系客服。"

        # 提取图表路径
        chart_img = final_state.get("chart_img", "")
        if not chart_img and final_response:  # 🔥 [修复] 检查 final_response 非空
            chart_match = re.search(r'IMAGE_CREATED:(chart_[a-zA-Z0-9_]+\.json)', final_response)
            if chart_match:
                chart_img = chart_match.group(1)

        # 🔥 [修复] 清理前检查 final_response 非空
        if final_response:
            final_response = re.sub(r'!\[.*?\]\(.*?\)', '', final_response)
            final_response = re.sub(r'IMAGE_CREATED:chart_[a-zA-Z0-9_]+\.json', '', final_response)

            # 🔥 [新增] 移除工具名称的反引号
            final_response = re.sub(r'`([a-z_]+)`', r'\1', final_response)

            final_response = final_response.strip()

        return {
            "status": "success",
            "response": final_response or "抱歉，暂时没有获取到有效分析结果",
            "chart": chart_img,
            "error": None
        }

    except Exception as e:
        import traceback
        error_msg = f"任务执行失败: {str(e)}\n{traceback.format_exc()}"
        print(f"❌ {error_msg}")

        return {
            "status": "error",
            "response": "分析过程中出现错误，请稍后重试",  # 🔥 [修复] 返回友好提示而非 None
            "chart": None,
            "error": error_msg
        }