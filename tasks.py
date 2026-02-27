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

from llm_compat import ChatTongyiCompat as ChatTongyi
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
from agent_core import build_trading_graph
from knowledge_tools import search_knowledge_structured
from tools.oss_utils import generate_signed_get_url
import re

ATTACHMENT_MIN_SCORE = 0.3
ATTACHMENT_RELATIVE_RATIO = 0.85
INLINE_IMAGE_TOKEN_PATTERN = re.compile(r"\[\[KNOWLEDGE_IMAGE_(\d+)\]\]")


def _normalize_knowledge_query(text: str) -> str:
    if not text:
        return ""
    cleaned = re.sub(r"【用户上传图信息】：[\s\S]*?----------------", " ", text)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:400]


def _filter_image_hits_for_attachments(image_hits, top_k: int):
    scored = []
    for hit in image_hits or []:
        try:
            score_val = float(hit.get("score", 0.0))
        except Exception:
            continue
        if score_val < ATTACHMENT_MIN_SCORE:
            continue
        scored.append((score_val, hit))

    if not scored:
        return []

    scored.sort(key=lambda x: x[0], reverse=True)
    top_score = scored[0][0]
    relative_floor = top_score * ATTACHMENT_RELATIVE_RATIO

    filtered = [hit for score, hit in scored if score >= relative_floor]
    return filtered[:top_k]


def _extract_attachment_keywords(item: dict):
    text = f"{item.get('title', '')} {item.get('source', '')}"
    zh_words = re.findall(r"[\u4e00-\u9fff]{2,}", text)
    en_words = [w.lower() for w in re.findall(r"[A-Za-z0-9]{3,}", text)]
    stopwords = {
        "knowledge", "images", "image", "png", "jpg", "jpeg",
        "docs", "doc", "future", "app", "source", "oss", "http", "https",
    }
    keywords = []
    for w in zh_words + en_words:
        if w.lower() in stopwords:
            continue
        if w not in keywords:
            keywords.append(w)
    return keywords[:8]


def _inject_inline_attachment_tokens(response_text: str, attachments):
    if not response_text or not attachments:
        return response_text, attachments
    if INLINE_IMAGE_TOKEN_PATTERN.search(response_text):
        return response_text, attachments

    paragraphs = [p.strip() for p in re.split(r"\n\s*\n", response_text) if p.strip()]
    if not paragraphs:
        return response_text, attachments

    placement = {}
    for idx, item in enumerate(attachments, start=1):
        token = f"[[KNOWLEDGE_IMAGE_{idx}]]"
        item["placeholder"] = token
        keywords = _extract_attachment_keywords(item)

        best_para = -1
        best_score = 0
        if keywords:
            for p_idx, para in enumerate(paragraphs):
                score = sum(1 for kw in keywords if kw in para or kw.lower() in para.lower())
                if score > best_score:
                    best_score = score
                    best_para = p_idx

        if best_para < 0:
            best_para = min(len(paragraphs) - 1, max(0, idx - 1))
        placement.setdefault(best_para, []).append(token)

    rendered_parts = []
    for p_idx, para in enumerate(paragraphs):
        rendered_parts.append(para)
        tokens = placement.get(p_idx, [])
        if tokens:
            rendered_parts.append("\n".join(tokens))

    return "\n\n".join(rendered_parts).strip(), attachments


def _build_image_attachments(query: str, top_k: int = 3):
    attachments = []
    normalized_query = _normalize_knowledge_query(query)
    if not normalized_query:
        return attachments

    try:
        data = search_knowledge_structured(
            query=normalized_query,
            limit=8,
            image_limit=top_k,
            min_score=ATTACHMENT_MIN_SCORE,
        )
        image_hits = _filter_image_hits_for_attachments(data.get("image_hits", []), top_k=top_k)
        if not image_hits:
            return attachments

        for hit in image_hits:
            oss_key = str(hit.get("oss_key", "")).strip()
            if not oss_key:
                continue
            signed = generate_signed_get_url(oss_key)
            if not signed:
                continue

            score_val = float(hit.get("score", 0.0))
            attachments.append(
                {
                    "type": "knowledge_image",
                    "image_id": str(hit.get("image_id", "")),
                    "title": str(hit.get("title", "图片知识")),
                    "source": str(hit.get("source", "未知来源")),
                    "score": round(score_val, 4),
                    "url": signed["url"],
                    "expires_at": signed["expires_at"],
                }
            )
    except Exception as e:
        print(f"[Attachment] 图片附件构建失败: {e}")

    return attachments


@celery_app.task(bind=True, name='tasks.process_ai_query')
def process_ai_query(
    self,
    user_id,
    prompt,
    image_context="",
    risk_preference="稳健型",
    history_messages=None,
    context_payload=None,
):
    """后台处理 AI 查询"""
    try:
        self.update_state(state='PROCESSING', meta={'progress': '正在初始化 AI 模型...'})

        # 初始化 LLM
        fast_llm = ChatTongyi(model="qwen-turbo", streaming=False, temperature=0.1)
        mid_llm = ChatTongyi(model="qwen3.5-plus", streaming=False, temperature=0.2)
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

        context_payload = context_payload or {}

        inputs = {
            "user_query": final_prompt,
            "messages": input_messages,
            "risk_preference": risk_preference,
            "is_followup": bool(context_payload.get("is_followup", False)),
            "recent_context": str(context_payload.get("recent_context", "")),
            "memory_context": str(context_payload.get("memory_context", "")),
            "conversation_id": str(context_payload.get("conversation_id", f"{user_id}-default")),
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

        # 场景 1: 闲聊/知识问答（独立回答，没有走流水线）
        elif chatter_txt and "已制定计划" not in chatter_txt:
            final_response = chatter_txt

        # 场景 2: 选股策略（独立回答）
        elif report_card.get("screener", "") and not finalizer_txt:
            final_response = report_card["screener"]

        # 🔥 [关键修复] 场景 3: 综合报告 - finalizer 优先！
        elif finalizer_txt and "PASS" not in finalizer_txt:
            final_response = finalizer_txt

        # 场景 4: 王牌分析师独立回答
        elif generalist_txt:
            final_response = generalist_txt

        # 场景 5: 只有情报研究员回答（没有其他分析师）
        elif report_card.get("researcher", "") and not any([
            report_card.get("analyst", ""),
            report_card.get("strategist", ""),
            report_card.get("monitor", "")
        ]):
            final_response = report_card["researcher"]

        # 场景 6: 兜底 - 拼接各模块报告
        else:
            if report_card.get("macro_analyst", ""):
                final_response += f"{report_card['macro_analyst']}\n\n"
            if report_card.get("analyst", ""):
                final_response += f"{report_card['analyst']}\n\n"
            if report_card.get("monitor", "") and report_card["monitor"] != "无数据":
                final_response += f"### 💸 资金面监控\n{report_card['monitor']}\n\n"
            if report_card.get("researcher", ""):
                final_response += f"### 📰 情报与舆情\n{report_card['researcher']}\n\n"
            if report_card.get("strategist", ""):
                final_response += f"### ⚖️ 衍生品策略建议\n{report_card['strategist']}\n\n"
            if report_card.get("screener", ""):
                final_response += f"{report_card['screener']}\n\n"

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

        attachments = _build_image_attachments(prompt, top_k=3)
        final_response, attachments = _inject_inline_attachment_tokens(final_response, attachments)

        return {
            "status": "success",
            "response": final_response or "抱歉，暂时没有获取到有效分析结果",
            "chart": chart_img,
            "attachments": attachments,
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
            "attachments": [],
            "error": error_msg
        }
