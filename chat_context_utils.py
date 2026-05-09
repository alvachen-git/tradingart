import re
from datetime import datetime, timedelta
from typing import Any, Iterable


FOLLOWUP_KEYWORDS = (
    "刚刚", "刚才", "上一个", "上一条", "上次", "前面",
    "继续", "接着", "承接", "基于刚才", "刚聊到", "上一轮",
    "详细说明", "详细说", "展开说", "展开讲", "再展开", "再详细", "那为什么", "为什么呢", "补充一下",
    "查一下", "帮我查", "具体原因", "到底为什么", "详细原因", "那具体呢", "再具体一点", "具体说说",
)

FOLLOWUP_CONNECTOR_HINTS = ("那", "这", "它", "他", "她", "这个", "这波", "这家公司", "这件事")
FOLLOWUP_ACTION_HINTS = ("查", "看", "找", "解释", "展开", "说明", "讲", "说")
FOLLOWUP_DETAIL_HINTS = ("具体", "详细", "原因", "为什么", "怎么回事", "到底", "再", "继续")
FOLLOWUP_LOOKUP_HINTS = ("查一下", "帮我查", "具体原因", "到底为什么", "详细原因", "为什么会这样", "查查")
FOLLOWUP_NUMERIC_HINTS = (
    "详细数值", "具体数值", "具体数据", "具体是多少", "数值", "数据", "相关系数", "相关度",
    "权重", "占比", "比例", "区间", "指标", "统计", "历史统计", "窗口", "近20天", "近60天",
    "列出来", "具体点位",
)
FOLLOWUP_FACT_HINTS = (
    "来源", "名单", "公告", "时间", "日期", "年份", "关键节点", "成分股", "大类", "出处", "口径",
)
FOLLOWUP_ANALYZE_REASON_HINTS = ("为什么会这样", "背后原因", "这意味着什么", "原因是什么", "为什么")
FOLLOWUP_ANALYZE_IMPACT_HINTS = ("影响大吗", "值不值得", "那该怎么做", "怎么看", "哪个更强", "适合做吗")
FOLLOWUP_EXPLAIN_HINTS = ("再展开", "再详细", "举个例子", "展开讲", "详细说明", "具体讲讲", "继续说说")
FOLLOWUP_REQUEST_PREFIXES = ("我要", "给我", "我想", "想看", "对，", "对,", "那", "再", "继续")
CORRECTION_DIRECT_HINTS = (
    "你错了", "错了", "搞错了", "你搞错了", "你理解错了", "理解错了", "你说错了", "说错了",
    "你这个不对", "这个不对", "不对", "不是这样的", "不是这个意思", "不是我说的那个",
    "不是中微公司", "不是这个", "有这家公司", "有这个公司", "你再想想", "你仔细想下",
    "你仔细想想", "你仔细思考下", "你重新想想", "重新核实", "重新查一下",
)
CORRECTION_NEGATION_PREFIXES = ("不是", "不对", "错了")
CORRECTION_CHALLENGE_HINTS = ("你再想想", "你仔细想", "你仔细思考", "重新核实", "重新想想")

FOCUS_ENTITY_SUFFIXES = (
    "股份", "集团", "科技", "技术", "控股", "电子", "电气", "机械", "汽车", "能源",
    "药业", "银行", "证券", "制造", "动力", "材料", "智能", "软件", "通信", "航空",
    "医药", "生物", "实业", "新材",
)
FOCUS_ENTITY_PATTERN = re.compile(
    r"[一-龥]{2,10}(?:%s)" % "|".join(FOCUS_ENTITY_SUFFIXES)
)
FOCUS_ENTITY_BAD_SUBSTRINGS = ("的", "业务", "或")
FOCUS_PRONOUN_HINTS = ("他", "她", "它", "他的", "她的", "它的", "这家公司", "这个公司", "这个票", "这只票")
COMMON_MARKET_ENTITIES = (
    "英特尔", "特斯拉", "英伟达", "苹果", "微软", "亚马逊", "谷歌", "Meta", "meta",
    "英伟达", "纳指", "纳斯达克", "标普", "道指", "黄金", "白银", "原油", "比特币", "以太坊",
)
US_TICKER_PATTERN = re.compile(r"\b(AAPL|TSLA|NVDA|MSFT|AMZN|GOOG|META|AVGO|AMD|INTC|TSM)\b", re.IGNORECASE)

FOCUS_ASPECT_KEYWORDS = (
    "机器人业务", "汽车业务", "机器人", "汽车", "工业自动化", "工业软件",
    "协作机器人", "服务机器人", "工业机器人", "业务线", "这块业务", "这个业务",
)

COMPANY_NEWS_TOPIC_KEYWORDS = (
    "最近有什么好消息", "最近有没有好消息", "最近有什么动态", "最近动态", "最近进展",
    "最近催化", "最近有没有催化", "最近消息", "最新消息", "近期动态", "近期进展",
    "近期催化", "最近怎么样", "业务最近怎么样", "业务最近如何",
)
PRICE_MOVE_REASON_KEYWORDS = (
    "为什么涨这么多", "为什么跌这么多", "为什么大涨", "为什么大跌", "为什么拉升", "为什么跳水",
    "今晚为什么涨", "今天为什么涨", "今晚为什么跌", "今天为什么跌", "异动原因", "上涨原因", "下跌原因",
)
CONCEPT_EXPLAIN_KEYWORDS = (
    "什么是", "解释", "解释一下", "科普一下", "说说", "讲讲", "区别", "原理", "举例",
    "通俗说", "什么意思", "是什么", "怎么理解",
)
MARKET_ANALYSIS_KEYWORDS = (
    "怎么看", "怎么做", "怎么办", "分析", "走势", "盘面", "行情", "对股价影响", "影响大吗",
    "值不值得买", "能不能买", "能买吗", "会不会涨", "适合做吗", "适合吗",
)
CONVERSATION_MEMORY_TIME_HINTS = (
    "昨天", "前天", "今天", "上次", "之前", "以前", "最近", "前面", "前几天", "上一轮", "上一个", "上一条",
)
CONVERSATION_MEMORY_RECENT_ONLY_HINTS = ("刚刚", "刚才", "刚聊", "刚说")
CONVERSATION_MEMORY_CONTENT_HINTS = ("聊", "聊天", "对话", "说", "问", "回答", "讨论")
CONVERSATION_MEMORY_ACTION_HINTS = ("记得", "记住", "还记得", "回忆", "想起", "讲过", "说过", "问过", "聊过")


def is_conversation_memory_query(prompt_text: str) -> bool:
    text = str(prompt_text or "").strip()
    if not text:
        return False
    if "你记得我什么" in text or "你记住了我什么" in text or "你记得我持仓" in text:
        return False
    if "风险偏好" in text or "画像" in text:
        return False

    has_content = any(keyword in text for keyword in CONVERSATION_MEMORY_CONTENT_HINTS)
    has_action = any(keyword in text for keyword in CONVERSATION_MEMORY_ACTION_HINTS)
    has_time = any(keyword in text for keyword in CONVERSATION_MEMORY_TIME_HINTS + CONVERSATION_MEMORY_RECENT_ONLY_HINTS)
    asks_content = any(word in text for word in ("什么", "啥", "哪", "内容", "主题", "东西"))
    return (has_action and has_content) or (has_time and has_content and asks_content)


def infer_conversation_memory_window(prompt_text: str, *, now: datetime | None = None) -> dict[str, Any]:
    text = str(prompt_text or "").strip()
    if not is_conversation_memory_query(text):
        return {"is_query": False, "source": "", "label": "", "since": "", "until": "", "limit": 0}

    now_dt = now or datetime.now()
    source = "long"
    label = "最近"
    since = ""
    until = ""
    limit = 6

    if "昨天" in text:
        day = now_dt.date() - timedelta(days=1)
        label = "昨天"
        since = f"{day.isoformat()} 00:00:00"
        until = f"{day.isoformat()} 23:59:59"
        limit = 8
    elif "前天" in text:
        day = now_dt.date() - timedelta(days=2)
        label = "前天"
        since = f"{day.isoformat()} 00:00:00"
        until = f"{day.isoformat()} 23:59:59"
        limit = 8
    elif "今天" in text:
        day = now_dt.date()
        label = "今天"
        since = f"{day.isoformat()} 00:00:00"
        until = f"{day.isoformat()} 23:59:59"
        limit = 8
    elif any(keyword in text for keyword in CONVERSATION_MEMORY_RECENT_ONLY_HINTS) and not any(
        keyword in text for keyword in ("上次", "之前", "以前", "最近", "前几天")
    ):
        source = "recent"
        label = "刚才"
        limit = 0
    elif "上次" in text or "上一轮" in text or "上一个" in text or "上一条" in text:
        label = "上次"
        limit = 5
    elif "之前" in text or "以前" in text or "前面" in text or "前几天" in text:
        label = "之前"
        limit = 8

    return {"is_query": True, "source": source, "label": label, "since": since, "until": until, "limit": limit}


def infer_followup_goal(prompt_text: str, *, recent_context: str = "", recent_focus_topic: str = "") -> str:
    text = str(prompt_text or "").strip().lower()
    if not text:
        return ""
    recent_text = str(recent_context or "").strip().lower()
    topic = str(recent_focus_topic or "").strip()

    if any(keyword in text for keyword in FOLLOWUP_ANALYZE_IMPACT_HINTS):
        return "analyze_impact"
    if any(keyword in text for keyword in FOLLOWUP_ANALYZE_REASON_HINTS):
        return "analyze_reason"
    if any(keyword in text for keyword in FOLLOWUP_NUMERIC_HINTS):
        return "fetch_numeric"
    if any(keyword in text for keyword in FOLLOWUP_FACT_HINTS):
        return "fetch_facts"
    if any(keyword in text for keyword in FOLLOWUP_LOOKUP_HINTS):
        return "fetch_facts"
    if any(keyword in text for keyword in FOLLOWUP_EXPLAIN_HINTS):
        return "explain_more"

    if "查" in text or "找" in text:
        return "fetch_facts"
    if "多少" in text and ("相关" in text or "权重" in text or "占比" in text or "比例" in text):
        return "fetch_numeric"
    if "具体" in text and ("数据" in text or "数值" in text or "指标" in text):
        return "fetch_numeric"
    if "详细" in text and ("年份" in text or "时间" in text or "节点" in text):
        return "fetch_facts"

    if topic in ("异动原因", "盘面分析"):
        if "为什么" in text or "原因" in text:
            return "analyze_reason"
        if "影响" in text or "怎么做" in text or "怎么看" in text:
            return "analyze_impact"
    if topic in ("概念解释", "公司近期动态") and ("详细" in text or "展开" in text or "举例" in text):
        return "explain_more"
    if recent_text and ("相关度" in recent_text or "相关系数" in recent_text) and ("多少" in text or "数值" in text):
        return "fetch_numeric"

    return ""


def infer_correction_intent(prompt_text: str, *, recent_context: str = "", recent_focus_topic: str = "") -> bool:
    text = str(prompt_text or "").strip().lower()
    if not text:
        return False
    if text.endswith("不是吗") or text.endswith("不对吗") or text.endswith("对吗"):
        return False
    if any(keyword in text for keyword in CORRECTION_DIRECT_HINTS):
        return True
    if any(text.startswith(prefix) for prefix in CORRECTION_NEGATION_PREFIXES):
        if "吗" not in text[-3:]:
            return True
    if any(keyword in text for keyword in ("搞错", "理解错", "说错", "答错", "认错")):
        return True
    has_recent_state = bool(str(recent_context or "").strip() or str(recent_focus_topic or "").strip())
    if has_recent_state and any(keyword in text for keyword in CORRECTION_CHALLENGE_HINTS):
        return True
    if has_recent_state and "有这家公司" in text:
        return True
    return False


def extract_similarity_tokens(text: str) -> set[str]:
    if not text:
        return set()
    normalized = re.sub(r"[^0-9a-zA-Z\u4e00-\u9fff]+", " ", str(text).lower())
    tokens: set[str] = set()
    for word in normalized.split():
        if len(word) >= 2:
            tokens.add(word)
        if re.search(r"[\u4e00-\u9fff]", word) and len(word) >= 2:
            for i in range(len(word) - 1):
                tokens.add(word[i : i + 2])
    return tokens


def is_semantically_related(prompt_text: str, recent_turns: Iterable[dict], threshold: float = 0.18) -> bool:
    current_tokens = extract_similarity_tokens(prompt_text)
    if not current_tokens:
        return False

    best_score = 0.0
    for turn in recent_turns:
        turn_tokens = extract_similarity_tokens(str(turn.get("content", "")))
        if not turn_tokens:
            continue
        union = current_tokens | turn_tokens
        if not union:
            continue
        score = len(current_tokens & turn_tokens) / len(union)
        best_score = max(best_score, score)
    return best_score >= threshold


def infer_followup_intent(prompt_text: str) -> bool:
    text = str(prompt_text or "").strip().lower()
    if not text:
        return False
    followup_goal = infer_followup_goal(text)
    if infer_correction_intent(text):
        return True
    if any(keyword in text for keyword in FOLLOWUP_KEYWORDS):
        return True
    has_connector = any(hint in text for hint in FOLLOWUP_CONNECTOR_HINTS)
    has_action = any(hint in text for hint in FOLLOWUP_ACTION_HINTS)
    has_detail = any(hint in text for hint in FOLLOWUP_DETAIL_HINTS)
    has_request_prefix = text.startswith(FOLLOWUP_REQUEST_PREFIXES)
    if has_connector and (has_action or has_detail):
        return True
    if len(text) <= 16 and has_action and has_detail:
        return True
    if followup_goal and (has_request_prefix or len(text) <= 18):
        return True
    return False


def infer_lookup_followup_intent(prompt_text: str) -> bool:
    text = str(prompt_text or "").strip().lower()
    if not text:
        return False
    if any(keyword in text for keyword in FOLLOWUP_LOOKUP_HINTS):
        return True
    has_lookup_action = any(hint in text for hint in ("查", "找", "看看", "确认"))
    has_reason = any(hint in text for hint in ("原因", "为什么", "怎么回事"))
    has_connector = any(hint in text for hint in FOLLOWUP_CONNECTOR_HINTS)
    return (has_lookup_action and has_reason) or (has_connector and has_lookup_action and has_reason)


def should_preserve_recent_context(
    prompt_text: str,
    *,
    is_followup: bool,
    semantic_related: bool,
    is_same_domain: bool,
    recent_turns: Iterable[dict],
    recent_focus_entity: str = "",
    recent_focus_topic: str = "",
) -> bool:
    text = str(prompt_text or "").strip()
    if not text:
        return False
    followup_goal = infer_followup_goal(text, recent_focus_topic=recent_focus_topic)
    if is_followup or infer_lookup_followup_intent(text) or infer_correction_intent(
        text,
        recent_context=" ".join(str(turn.get("content", "")) for turn in recent_turns),
        recent_focus_topic=recent_focus_topic,
    ) or bool(followup_goal):
        return True
    if semantic_related and is_same_domain:
        return True
    has_pronoun = any(hint in text for hint in FOCUS_PRONOUN_HINTS)
    has_recent_state = bool(str(recent_focus_entity or "").strip() or str(recent_focus_topic or "").strip())
    if len(text) <= 18 and (has_pronoun or has_recent_state or infer_followup_intent(text)):
        return True
    return False


def extract_focus_entity(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    code_match = re.search(r"(?<!\d)\d{6}(?!\d)", raw)
    if code_match:
        return code_match.group(0)
    us_ticker_match = US_TICKER_PATTERN.search(raw)
    if us_ticker_match:
        return us_ticker_match.group(1).upper()
    for entity in COMMON_MARKET_ENTITIES:
        if entity.lower() in raw.lower():
            return entity
    company_match = FOCUS_ENTITY_PATTERN.search(raw)
    if not company_match:
        return ""
    candidate = company_match.group(0)
    if any(bad in candidate for bad in FOCUS_ENTITY_BAD_SUBSTRINGS):
        return ""
    return candidate


def extract_focus_aspect(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    hits = []
    for keyword in FOCUS_ASPECT_KEYWORDS:
        if keyword in raw and keyword not in hits:
            hits.append(keyword)
    return "、".join(hits[:2])


def infer_focus_topic(text: str) -> tuple[str, str]:
    raw = str(text or "").strip().lower()
    if not raw:
        return "", ""
    if any(keyword in raw for keyword in COMPANY_NEWS_TOPIC_KEYWORDS):
        return "公司近期动态", "company_news"
    if any(keyword in raw for keyword in PRICE_MOVE_REASON_KEYWORDS):
        return "异动原因", "price_move_reason"
    if ("为什么" in raw or "原因" in raw) and any(
        keyword in raw for keyword in ("涨", "跌", "拉升", "跳水", "异动", "大涨", "大跌")
    ):
        return "异动原因", "price_move_reason"
    if any(keyword in raw for keyword in CONCEPT_EXPLAIN_KEYWORDS):
        return "概念解释", "concept_explain"
    if any(keyword in raw for keyword in MARKET_ANALYSIS_KEYWORDS):
        return "盘面分析", "market_analysis"
    return "", ""


def summarize_anchor_text(text: str, max_chars: int = 120) -> str:
    raw = re.sub(r"\s+", " ", str(text or "")).strip()
    return raw[:max_chars]


def build_topic_anchors(messages: Iterable[dict], max_anchors: int = 3) -> list[dict[str, Any]]:
    anchors: list[dict[str, Any]] = []
    pending_user: dict[str, Any] | None = None

    for idx, raw_msg in enumerate(messages):
        if not isinstance(raw_msg, dict):
            continue
        if bool(raw_msg.get("is_task_placeholder")):
            continue

        role = str(raw_msg.get("role", "")).strip().lower()
        content = str(raw_msg.get("content", "")).strip()
        if not content:
            continue

        if role == "user":
            pending_user = {
                "index": idx,
                "content": content,
                "linked_task_id": str(raw_msg.get("linked_task_id") or "").strip(),
            }
            continue

        if role not in {"assistant", "ai"} or not pending_user:
            continue

        user_query = str(pending_user.get("content") or "").strip()
        assistant_text = content
        combined_text = f"{user_query}\n{assistant_text}"
        focus_entity = extract_focus_entity(user_query) or extract_focus_entity(assistant_text)
        focus_aspect = extract_focus_aspect(user_query) or extract_focus_aspect(assistant_text)
        focus_topic, focus_mode_hint = infer_focus_topic(user_query)
        if not focus_topic:
            focus_topic, focus_mode_hint = infer_focus_topic(assistant_text)

        anchor_id = str(pending_user.get("linked_task_id") or "").strip() or f"anchor_{idx}"
        anchors.append(
            {
                "anchor_id": anchor_id,
                "user_query": user_query,
                "assistant_summary": summarize_anchor_text(assistant_text, max_chars=180),
                "context_text": f"用户: {user_query[:260]}\nAI: {assistant_text[:260]}",
                "focus_entity": focus_entity,
                "focus_topic": focus_topic,
                "focus_aspect": focus_aspect,
                "focus_mode_hint": focus_mode_hint,
                "source_user_index": int(pending_user.get("index") or 0),
            }
        )
        pending_user = None

    if max_anchors <= 0:
        return []
    return anchors[-max_anchors:]


def _anchor_topic_signature(anchor: dict[str, Any]) -> str:
    entity = str(anchor.get("focus_entity") or "").strip()
    topic = str(anchor.get("focus_topic") or "").strip()
    aspect = str(anchor.get("focus_aspect") or "").strip()
    user_query = summarize_anchor_text(anchor.get("user_query") or "", max_chars=40)
    return "|".join(part for part in (entity, topic, aspect, user_query) if part)


def _anchor_goal_score(anchor: dict[str, Any], followup_goal: str) -> float:
    goal = str(followup_goal or "").strip().lower()
    if not goal:
        return 0.0
    text = f"{anchor.get('user_query', '')}\n{anchor.get('assistant_summary', '')}".lower()
    if goal == "fetch_numeric":
        return 0.22 if any(keyword in text for keyword in ("相关", "数值", "数据", "区间", "权重", "占比", "比例", "系数", "统计")) else 0.0
    if goal == "fetch_facts":
        return 0.18 if any(keyword in text for keyword in ("年份", "时间", "日期", "节点", "来源", "公告", "成分")) else 0.0
    if goal == "explain_more":
        return 0.15 if any(keyword in text for keyword in ("什么是", "解释", "原理", "概念", "举例")) else 0.0
    if goal in {"analyze_reason", "analyze_impact"}:
        return 0.18 if any(keyword in text for keyword in ("为什么", "原因", "怎么看", "策略", "影响", "走势")) else 0.0
    return 0.0


def select_target_anchor(
    prompt_text: str,
    anchors: Iterable[dict[str, Any]],
    *,
    followup_goal: str = "",
    is_followup: bool = False,
) -> dict[str, Any]:
    anchor_list = list(anchors or [])
    if not anchor_list:
        return {
            "target_anchor": None,
            "candidate_anchors": [],
            "recent_topic_anchor": None,
            "anchor_confidence": 0.0,
            "followup_anchor_ambiguous": False,
            "followup_anchor_clarify": "",
        }

    prompt_raw = str(prompt_text or "").strip()
    prompt_text_lower = prompt_raw.lower()
    explicit_entity = extract_focus_entity(prompt_raw)
    explicit_aspect = extract_focus_aspect(prompt_raw)
    prompt_tokens = extract_similarity_tokens(prompt_raw)
    low_info_prompt = len(prompt_raw) <= 12 and not explicit_entity and not explicit_aspect

    scored: list[tuple[float, dict[str, Any]]] = []
    for reverse_idx, anchor in enumerate(reversed(anchor_list)):
        recency_score = max(0.25, 0.90 - reverse_idx * 0.20)
        score = recency_score

        anchor_entity = str(anchor.get("focus_entity") or "").strip()
        anchor_aspect = str(anchor.get("focus_aspect") or "").strip()
        anchor_blob = f"{anchor.get('user_query', '')}\n{anchor.get('assistant_summary', '')}"
        anchor_tokens = extract_similarity_tokens(anchor_blob)

        if explicit_entity:
            if explicit_entity == anchor_entity or explicit_entity.lower() in anchor_blob.lower():
                score += 0.75
            else:
                score -= 0.18

        if explicit_aspect:
            if explicit_aspect and explicit_aspect in anchor_aspect:
                score += 0.45
            else:
                score -= 0.10

        if prompt_tokens and anchor_tokens:
            union = prompt_tokens | anchor_tokens
            if union:
                overlap = len(prompt_tokens & anchor_tokens) / len(union)
                score += min(0.35, overlap * 1.2)

        score += _anchor_goal_score(anchor, followup_goal)

        if is_followup and not explicit_entity and not explicit_aspect and reverse_idx == 0:
            score += 0.10

        scored.append((score, anchor))

    scored.sort(key=lambda item: item[0], reverse=True)
    top_score, top_anchor = scored[0]
    candidate_anchors = anchor_list[-3:]
    recent_topic_anchor = anchor_list[-1]

    ambiguous = False
    clarify = ""
    if low_info_prompt and len(scored) >= 2 and followup_goal in {"fetch_numeric", "fetch_facts", "explain_more"}:
        second_score, second_anchor = scored[1]
        topic_changed = _anchor_topic_signature(top_anchor) != _anchor_topic_signature(second_anchor)
        ambiguous_gap = 0.18 if followup_goal == "fetch_numeric" else 0.32
        if topic_changed and (top_score - second_score) < ambiguous_gap:
            ambiguous = True
            latest = recent_topic_anchor
            previous = candidate_anchors[-2] if len(candidate_anchors) >= 2 else second_anchor
            clarify = (
                f"你是要“{summarize_anchor_text(latest.get('user_query', ''), 18)}”这题的详细信息，"
                f"还是前面“{summarize_anchor_text(previous.get('user_query', ''), 18)}”那题？"
            )

    return {
        "target_anchor": top_anchor,
        "candidate_anchors": candidate_anchors,
        "recent_topic_anchor": recent_topic_anchor,
        "anchor_confidence": round(float(top_score), 3),
        "followup_anchor_ambiguous": ambiguous,
        "followup_anchor_clarify": clarify,
    }
