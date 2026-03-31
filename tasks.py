# tasks.py
import os
import sys
from celery_config import celery_app
from dotenv import load_dotenv

load_dotenv(override=True)

# 清理代理
for key in [
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
]:
    if key in os.environ:
        del os.environ[key]

from llm_compat import ChatTongyiCompat as ChatTongyi
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
from agent_core import build_trading_graph
from knowledge_tools import search_knowledge_structured
from tools.oss_utils import generate_signed_get_url
from tools.url_content_tools import build_link_context, extract_first_url
from portfolio_analysis_service import process_portfolio_snapshot as run_portfolio_snapshot
import data_engine as de
import re

ATTACHMENT_MIN_SCORE = 0.38
ATTACHMENT_RELATIVE_RATIO = 0.85
INLINE_IMAGE_TOKEN_PATTERN = re.compile(r"\[\[KNOWLEDGE_IMAGE_(\d+)\]\]")
URL_PREPROCESS_ENABLED = str(os.getenv("URL_PREPROCESS_ENABLED", "true")).strip().lower() in ("1", "true", "yes", "on")

BULL_DIRECTION_TERMS = [
    "牛市", "牛市价差", "看涨", "bull", "bull spread", "bull call spread", "bull put spread",
]
BEAR_DIRECTION_TERMS = [
    "熊市", "熊市价差", "看跌", "bear", "bear spread", "bear call spread", "bear put spread",
]


def _build_link_failure_notice(link_ctx: dict) -> str:
    reason = str(link_ctx.get("error_message") or "未知原因").strip()
    url = str(link_ctx.get("url") or "").strip()
    if url:
        return (
            f"⚠️ 链接抓取失败（{reason}）。\n"
            f"链接：{url}\n"
            "为避免误判，本轮已停止自动推断。请粘贴正文或关键段落，我再继续精确分析。"
        )
    return (
        f"⚠️ 链接抓取失败（{reason}）。"
        "为避免误判，本轮已停止自动推断。请粘贴正文或关键段落，我再继续精确分析。"
    )


@celery_app.task(bind=True, name="tasks.process_portfolio_snapshot")
def process_portfolio_snapshot_task(
    self,
    user_id,
    positions,
    screenshot_hash="",
    source_text="",
):
    """后台处理持仓截图结构化分析与覆盖更新。"""
    try:
        self.update_state(state="PROCESSING", meta={"progress": "正在标准化持仓数据..."})
        if not isinstance(positions, list):
            return {
                "status": "error",
                "error": "positions 参数必须为数组",
                "response": "持仓数据格式错误，请重试上传截图。",
            }

        self.update_state(state="PROCESSING", meta={"progress": "正在计算行业占比与相关度..."})
        result = run_portfolio_snapshot(
            user_id=str(user_id),
            raw_positions=positions,
            screenshot_hash=str(screenshot_hash or ""),
            lookback_days=120,
        )

        if result.get("status") != "success":
            err = result.get("error", "未知错误")
            return {
                "status": "error",
                "error": err,
                "response": f"持仓分析失败：{err}",
            }

        self.update_state(state="PROCESSING", meta={"progress": "正在整理持仓分析报告..."})
        return {
            "status": "success",
            "response": result.get("summary_text", ""),
            "result": result,
            "retrieval_summary": result.get("retrieval_summary", ""),
            "source_text": source_text or "",
            "error": None,
        }
    except Exception as e:
        import traceback

        return {
            "status": "error",
            "response": "持仓分析过程中出现错误，请稍后重试。",
            "error": f"{e}\n{traceback.format_exc()}",
        }


def _normalize_knowledge_query(text: str) -> str:
    if not text:
        return ""
    cleaned = re.sub(r"【用户上传图信息】：[\s\S]*?----------------", " ", text)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:400]


def _detect_direction_labels(text: str):
    text_norm = str(text or "").lower()
    labels = set()
    if any(term in text_norm for term in BULL_DIRECTION_TERMS):
        labels.add("bull")
    if any(term in text_norm for term in BEAR_DIRECTION_TERMS):
        labels.add("bear")
    return labels


def _is_direction_conflict(intent_labels, hit_labels) -> bool:
    if not intent_labels or not hit_labels:
        return False
    if "bull" in intent_labels and "bear" in hit_labels and "bull" not in hit_labels:
        return True
    if "bear" in intent_labels and "bull" in hit_labels and "bear" not in hit_labels:
        return True
    return False


def _build_hit_direction_text(hit: dict) -> str:
    parts = [
        str(hit.get("title", "")),
        str(hit.get("source", "")),
        str(hit.get("summary_text", "")),
        str(hit.get("ocr_text", "")),
        str(hit.get("content", "")),
    ]
    tags = hit.get("tags")
    if isinstance(tags, list):
        parts.extend(str(t) for t in tags)
    elif tags:
        parts.append(str(tags))
    return " ".join(parts)


def _normalize_symbol_token(token: str) -> str:
    return re.sub(r"[^A-Z0-9\u4e00-\u9fff]", "", str(token or "").upper())


def _build_symbol_aliases(symbol_hint: str = "", symbol_name_hint: str = "", query_text: str = ""):
    aliases = set()
    raw_tokens = []

    for src in [symbol_hint, symbol_name_hint]:
        if src:
            raw_tokens.extend(re.split(r"[,\s，、/]+", str(src)))

    if query_text:
        raw_tokens.extend(
            re.findall(r"(?<![A-Za-z0-9])[A-Za-z]{1,4}\d{0,4}(?![A-Za-z0-9])", query_text)
        )
        raw_tokens.extend(re.findall(r"(?<!\d)\d{6}(?!\d)", query_text))

    for token in raw_tokens:
        norm = _normalize_symbol_token(token)
        if not norm:
            continue
        aliases.add(norm)

        alpha_prefix = re.match(r"^[A-Z]{1,6}", norm)
        if alpha_prefix:
            aliases.add(alpha_prefix.group(0))

        if norm.isdigit() and len(norm) == 6:
            aliases.add(f"{norm}ETF")
            if norm.startswith(("510", "159", "588")):
                try:
                    aliases.add(f"{int(norm[-3:])}ETF")
                except ValueError:
                    pass
        elif norm.endswith("ETF"):
            digits_match = re.search(r"(\d{3,6})ETF$", norm)
            if digits_match:
                aliases.add(digits_match.group(1))

    product_map = getattr(de, "PRODUCT_MAP", {}) or {}
    for code in list(aliases):
        if re.fullmatch(r"[A-Z]{1,4}", code):
            name = str(product_map.get(code, "")).strip()
            name_norm = _normalize_symbol_token(name)
            if name_norm:
                aliases.add(name_norm)

    blacklist = {"ETF", "OPTION", "OPTIONS", "期权", "技术分析", "技术面", "行情"}
    return {item for item in aliases if len(item) >= 2 and item not in blacklist}


def _is_symbol_consistent(hit: dict, symbol_aliases) -> bool:
    if not symbol_aliases:
        return True
    hit_text = _normalize_symbol_token(_build_hit_direction_text(hit))
    if not hit_text:
        return False
    return any(alias in hit_text for alias in symbol_aliases)


def _filter_image_hits_for_attachments(image_hits, top_k: int, intent_labels=None, symbol_aliases=None):
    scored = []
    for hit in image_hits or []:
        try:
            score_val = float(hit.get("score", 0.0))
        except Exception:
            continue
        if score_val < ATTACHMENT_MIN_SCORE:
            continue

        hit_labels = _detect_direction_labels(_build_hit_direction_text(hit))
        if _is_direction_conflict(intent_labels or set(), hit_labels):
            continue
        if symbol_aliases and not _is_symbol_consistent(hit, symbol_aliases):
            continue
        scored.append((score_val, hit))

    if not scored:
        return []

    scored.sort(key=lambda x: x[0], reverse=True)
    top_score = scored[0][0]
    dynamic_cap = _dynamic_attachment_cap(top_score, hard_cap=top_k)
    if dynamic_cap <= 0:
        return []

    relative_floor = top_score * ATTACHMENT_RELATIVE_RATIO

    filtered = [hit for score, hit in scored if score >= relative_floor]
    return filtered[:dynamic_cap]


def _dynamic_attachment_cap(top_score: float, hard_cap: int = 3) -> int:
    if top_score < ATTACHMENT_MIN_SCORE:
        cap = 0
    elif top_score < 0.5:
        cap = 1
    elif top_score < 0.7:
        cap = 2
    else:
        cap = 3
    return max(0, min(cap, hard_cap))


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


def _build_image_attachments(query: str, top_k: int = 3, symbol_hint: str = "", symbol_name_hint: str = ""):
    attachments = []
    normalized_query = _normalize_knowledge_query(query)
    if not normalized_query:
        return attachments
    intent_labels = _detect_direction_labels(normalized_query)
    symbol_aliases = _build_symbol_aliases(
        symbol_hint=symbol_hint,
        symbol_name_hint=symbol_name_hint,
        query_text=normalized_query,
    )

    try:
        data = search_knowledge_structured(
            query=normalized_query,
            limit=8,
            image_limit=top_k,
            min_score=ATTACHMENT_MIN_SCORE,
        )
        image_hits = _filter_image_hits_for_attachments(
            data.get("image_hits", []),
            top_k=top_k,
            intent_labels=intent_labels,
            symbol_aliases=symbol_aliases,
        )
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


@celery_app.task(bind=True, name='tasks.update_user_profile')
def update_user_profile_task(self, user_id, user_input):
    """
    后台更新用户画像任务
    分析用户输入，提取风险偏好、情绪、关注品种等特征
    """
    try:
        # 🔥 双重防护：任务级别的去重检查
        import hashlib
        import redis
        redis_client = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)

        msg_fingerprint = hashlib.md5(f"{user_id}:{user_input}".encode()).hexdigest()
        exec_lock_key = f"profile_exec_lock:{msg_fingerprint}"

        # 检查是否正在执行相同任务
        if redis_client.exists(exec_lock_key):
            print(f"⏭️ 跳过重复执行：相同消息正在处理中 ({user_id})")
            return {
                "status": "skipped",
                "message": "重复任务已跳过"
            }

        # 设置执行锁（120秒过期，防止任务执行时间过长）
        redis_client.setex(exec_lock_key, 120, "1")

        self.update_state(state="PROCESSING", meta={"progress": "正在分析用户行为特征..."})

        # 调用 data_engine 的用户画像更新函数
        de.update_user_memory_async(user_id, user_input)

        # 清除执行锁
        redis_client.delete(exec_lock_key)

        return {
            "status": "success",
            "message": f"用户 {user_id} 画像更新成功"
        }

    except Exception as e:
        print(f"❌ 用户画像更新任务失败: {e}")
        # 失败时也清除锁
        try:
            redis_client.delete(exec_lock_key)
        except:
            pass
        return {
            "status": "error",
            "error": str(e),
            "message": "用户画像更新失败"
        }


@celery_app.task(bind=True, name='tasks.process_ai_query')
def process_ai_query(
    self,
    user_id,
    prompt,
    image_context="",
    risk_preference="稳健型",
    history_messages=None,
    context_payload=None,
    has_portfolio=False,
):
    """后台处理 AI 查询"""
    try:
        prompt_for_graph = prompt
        link_failure_notice = ""
        url_fetch_blocked = False

        if URL_PREPROCESS_ENABLED:
            extracted_url = extract_first_url(prompt)
            if extracted_url:
                self.update_state(state='PROCESSING', meta={'progress': '正在读取链接正文...'})
                try:
                    link_ctx = build_link_context(prompt)
                except Exception as e:
                    link_ctx = {
                        "ok": False,
                        "url": extracted_url,
                        "title": "",
                        "snippet": "",
                        "error_code": "preprocess_exception",
                        "error_message": f"链接预处理异常: {e}",
                    }

                if link_ctx.get("ok"):
                    link_ref_block = (
                        f"【链接参考内容】\n"
                        f"来源: {link_ctx.get('url', '')}\n"
                        f"标题: {link_ctx.get('title', '未提取到标题')}\n"
                        f"摘要:\n{link_ctx.get('snippet', '')}\n"
                        "请优先基于以上链接内容回答；若信息不足，再补充常识并明确说明不确定项。"
                    )
                    prompt_for_graph = f"{prompt}\n\n{link_ref_block}".strip()
                    print(f"[URL_PREPROCESS] 链接正文注入成功: {link_ctx.get('url', '')}")
                elif link_ctx.get("error_code") != "no_url":
                    link_failure_notice = _build_link_failure_notice(link_ctx)
                    url_fetch_blocked = True
                    print(
                        f"[URL_PREPROCESS] 链接正文注入失败: "
                        f"{link_ctx.get('error_code')} | {link_ctx.get('error_message')}"
                    )

        if url_fetch_blocked:
            self.update_state(state='PROCESSING', meta={'progress': '链接抓取失败，等待用户补充正文...'})
            return {
                "status": "success",
                "response": link_failure_notice,
                "chart": None,
                "attachments": [],
                "error": None,
            }

        self.update_state(state='PROCESSING', meta={'progress': '正在初始化 AI 模型...'})

        # 初始化 LLM
        fast_llm = ChatTongyi(model="qwen-turbo", streaming=False, temperature=0.1)
        mid_llm = ChatTongyi(model="qwen3.5-plus", streaming=False, temperature=0.2)
        smart_llm = ChatTongyi(model="qwen-max", streaming=False, temperature=0.4)

        self.update_state(state='PROCESSING', meta={'progress': '正在构建分析团队...'})

        app = build_trading_graph(fast_llm, mid_llm, smart_llm)

        final_prompt = image_context + prompt_for_graph if image_context else prompt_for_graph
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
            "user_id": user_id,
            "has_portfolio": has_portfolio,
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
            "portfolio_analyst": "",  # 🔥 [新增] 添加 portfolio_analyst 键
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

        if link_failure_notice:
            final_response = (
                f"{link_failure_notice}\n\n{final_response}".strip()
                if final_response
                else link_failure_notice
            )

        attachments = _build_image_attachments(
            prompt,
            top_k=3,
            symbol_hint=str(final_state.get("symbol", "") or ""),
            symbol_name_hint=str(final_state.get("symbol_name", "") or ""),
        )
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
