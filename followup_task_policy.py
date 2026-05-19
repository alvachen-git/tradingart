from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, List


CHAT_MODE_SIMPLE = "simple_chat"
CHAT_MODE_KNOWLEDGE = "knowledge_chat"
CHAT_MODE_ANALYSIS = "analysis_chat"

OVERRIDE_FORCE = "force"
OVERRIDE_SUGGEST = "suggest"
OVERRIDE_CONTEXT_ONLY = "context_only"

INTENT_CONTINUE_EXPLANATION = "continue_explanation"
INTENT_EXECUTE_SUGGESTION = "execute_suggestion"
INTENT_FETCH_DETAIL_OR_DATA = "fetch_detail_or_data"
INTENT_ANALYZE_REASON_OR_IMPACT = "analyze_reason_or_impact"
INTENT_MODIFY_CONSTRAINT = "modify_constraint"
INTENT_CHOOSE_OPTION = "choose_option"
INTENT_CORRECTION_OR_CHALLENGE = "correction_or_challenge"
INTENT_NEW_TASK = "new_task"
INTENT_AMBIGUOUS = "ambiguous"


DATA_HINTS = (
    "价格",
    "现价",
    "多少",
    "数值",
    "具体数值",
    "iv",
    "波动率",
    "保证金",
    "合约乘数",
    "一手",
    "持仓",
    "基差",
    "库存",
    "仓单",
    "交割",
    "成交量",
)
STOCK_SCREEN_ACTION_HINTS = ("筛选", "选股", "股票名单", "候选股", "股票池")
STOCK_SCREEN_SUBJECT_HINTS = (
    "股票",
    "个股",
    "候选股",
    "股票池",
    "股票名单",
    "综合评分",
    "高股息",
    "防御板块",
    "防御性板块",
)
OPTION_STRATEGY_HINTS = ("期权", "认购", "认沽", "价差", "买方", "卖方", "策略", "保护", "对冲")
KNOWLEDGE_HINTS = ("什么是", "解释", "概念", "原理", "举例", "知识")
ANALYSIS_HINTS = ("行情", "走势", "趋势", "影响", "原因", "技术面", "基本面", "买点", "操作")
NEWS_HINTS = ("新闻", "消息", "催化", "利好", "利空", "动态")
MODIFY_HINTS = (
    "稳健一点",
    "激进一点",
    "保守一点",
    "换成短线",
    "换成长线",
    "不要期权",
    "只看低风险",
    "只看高股息",
    "只看防御",
    "低风险",
)
CHOOSE_HINTS = ("第一种", "第二种", "第三种", "就选这个", "选这个", "哪个更适合", "哪种更适合")


@dataclass(frozen=True)
class FollowupTaskPolicy:
    followup_intent: str = INTENT_NEW_TASK
    recommended_chat_mode: str = ""
    recommended_plan: tuple[str, ...] = ()
    override_level: str = ""
    context_note: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "followup_intent": self.followup_intent,
            "recommended_chat_mode": self.recommended_chat_mode,
            "recommended_plan": list(self.recommended_plan),
            "override_level": self.override_level,
            "context_note": self.context_note,
        }


def _contains_any(text: str, keywords: Iterable[str]) -> bool:
    lower = str(text or "").lower()
    return any(str(keyword).lower() in lower for keyword in keywords)


def _anchor_text(anchor: dict[str, Any] | None) -> str:
    if not isinstance(anchor, dict):
        return ""
    actions = " ".join(str(item) for item in (anchor.get("suggested_actions") or []))
    return " ".join(
        str(anchor.get(key) or "")
        for key in ("user_query", "assistant_summary", "context_text", "focus_topic", "focus_aspect")
    ) + " " + actions


def _compact_plan(plan: Iterable[str]) -> tuple[str, ...]:
    out: List[str] = []
    for item in plan:
        step = str(item or "").strip()
        if step and step not in out:
            out.append(step)
    return tuple(out)


def _looks_like_stock_screen_context(text: str) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return False
    has_action = _contains_any(raw, STOCK_SCREEN_ACTION_HINTS)
    has_subject = _contains_any(raw, STOCK_SCREEN_SUBJECT_HINTS)
    return bool(has_action and has_subject)


def _infer_execute_plan(context_text: str) -> tuple[str, ...]:
    if not str(context_text or "").strip():
        return ()
    if _looks_like_stock_screen_context(context_text):
        return ("screener",)
    if _contains_any(context_text, DATA_HINTS):
        return ("monitor",)
    if _contains_any(context_text, OPTION_STRATEGY_HINTS):
        return ("analyst", "strategist")
    if _contains_any(context_text, KNOWLEDGE_HINTS):
        return ("chatter",)
    return ()


def _infer_context_plan(context_text: str) -> tuple[str, ...]:
    if _looks_like_stock_screen_context(context_text):
        return ("screener",)
    if _contains_any(context_text, DATA_HINTS):
        return ("monitor",)
    if _contains_any(context_text, OPTION_STRATEGY_HINTS):
        return ("analyst", "strategist")
    if _contains_any(context_text, NEWS_HINTS):
        return ("researcher",)
    if _contains_any(context_text, ANALYSIS_HINTS):
        return ("analyst",)
    if _contains_any(context_text, KNOWLEDGE_HINTS):
        return ("chatter",)
    return ("generalist",)


def classify_followup_task_policy(
    prompt_text: str,
    *,
    is_followup: bool = False,
    followup_goal: str = "",
    recent_context: str = "",
    target_anchor: dict[str, Any] | None = None,
    focus_topic: str = "",
    focus_mode_hint: str = "",
    correction_intent: bool = False,
) -> FollowupTaskPolicy:
    text = str(prompt_text or "").strip()
    goal = str(followup_goal or "").strip().lower()
    context_text = " ".join(
        part
        for part in (
            str(recent_context or ""),
            _anchor_text(target_anchor),
            str(focus_topic or ""),
            str(focus_mode_hint or ""),
        )
        if part
    )
    has_context = bool(context_text.strip())

    if not text:
        return FollowupTaskPolicy(INTENT_AMBIGUOUS, CHAT_MODE_SIMPLE, ("chatter",), OVERRIDE_SUGGEST, "当前追问为空，需要澄清。")

    if not is_followup and not goal and not correction_intent:
        return FollowupTaskPolicy(INTENT_NEW_TASK, "", (), OVERRIDE_CONTEXT_ONLY, "")

    if correction_intent:
        if _contains_any(text, ("不是这个", "不是这家", "不是这个公司", "不是这个标的")) or _contains_any(
            context_text, KNOWLEDGE_HINTS
        ):
            return FollowupTaskPolicy(
                INTENT_CORRECTION_OR_CHALLENGE,
                CHAT_MODE_KNOWLEDGE,
                ("chatter",),
                OVERRIDE_FORCE,
                "用户在纠正上一轮事实或实体，应先核对事实，不要沿用错误对象。",
            )
        return FollowupTaskPolicy(
            INTENT_CORRECTION_OR_CHALLENGE,
            CHAT_MODE_ANALYSIS,
            _infer_context_plan(context_text),
            OVERRIDE_SUGGEST,
            "用户在挑战上一轮判断，应承接上下文并重新分析。",
        )

    if goal == "execute_suggested_action":
        plan = _infer_execute_plan(context_text)
        if not has_context or not plan:
            return FollowupTaskPolicy(
                INTENT_EXECUTE_SUGGESTION,
                CHAT_MODE_SIMPLE,
                ("chatter",),
                OVERRIDE_CONTEXT_ONLY,
                "用户用了执行类短语，但没有可承接的金融建议上下文，按普通聊天处理。",
            )
        return FollowupTaskPolicy(
            INTENT_EXECUTE_SUGGESTION,
            CHAT_MODE_KNOWLEDGE if plan == ("chatter",) else CHAT_MODE_ANALYSIS,
            plan,
            OVERRIDE_FORCE,
            "用户当前请求是在执行上一轮建议，不要反问已明确的执行对象。",
        )

    if _contains_any(text, MODIFY_HINTS):
        plan = _infer_context_plan(context_text)
        return FollowupTaskPolicy(
            INTENT_MODIFY_CONSTRAINT,
            CHAT_MODE_KNOWLEDGE if plan == ("chatter",) else CHAT_MODE_ANALYSIS,
            plan,
            OVERRIDE_SUGGEST,
            "用户在修改上一轮条件，应沿用上一轮任务类型并注入新约束。",
        )

    if _contains_any(text, CHOOSE_HINTS):
        plan = _infer_context_plan(context_text)
        return FollowupTaskPolicy(
            INTENT_CHOOSE_OPTION,
            CHAT_MODE_KNOWLEDGE if plan == ("chatter",) else CHAT_MODE_ANALYSIS,
            plan,
            OVERRIDE_SUGGEST,
            "用户在选择上一轮方案，应按所选方案继续落地。",
        )

    if goal in {"fetch_numeric", "fetch_facts"} or _contains_any(text, DATA_HINTS):
        if _contains_any(text + " " + context_text, DATA_HINTS):
            return FollowupTaskPolicy(
                INTENT_FETCH_DETAIL_OR_DATA,
                CHAT_MODE_ANALYSIS,
                ("monitor",),
                OVERRIDE_FORCE,
                "用户在追问具体数据，应优先调用数据监控节点。",
            )
        return FollowupTaskPolicy(
            INTENT_FETCH_DETAIL_OR_DATA,
            CHAT_MODE_KNOWLEDGE,
            ("chatter",),
            OVERRIDE_SUGGEST,
            "用户在追问事实细节或来源，应补充解释和出处。",
        )

    if goal in {"analyze_reason", "analyze_impact"}:
        if _contains_any(context_text, NEWS_HINTS):
            plan = ("researcher",)
        elif _contains_any(context_text, OPTION_STRATEGY_HINTS) or _contains_any(text, ("怎么办", "怎么做", "操作")):
            plan = ("analyst", "strategist")
        else:
            plan = ("analyst",)
        return FollowupTaskPolicy(
            INTENT_ANALYZE_REASON_OR_IMPACT,
            CHAT_MODE_ANALYSIS,
            plan,
            OVERRIDE_SUGGEST,
            "用户在追问原因、影响或行动建议，应承接上一轮结论继续分析。",
        )

    if goal == "explain_more":
        if _contains_any(context_text, KNOWLEDGE_HINTS) and not _contains_any(context_text, ANALYSIS_HINTS):
            return FollowupTaskPolicy(
                INTENT_CONTINUE_EXPLANATION,
                CHAT_MODE_KNOWLEDGE,
                ("chatter",),
                OVERRIDE_SUGGEST,
                "用户要求继续解释上一轮知识点。",
            )
        return FollowupTaskPolicy(
            INTENT_CONTINUE_EXPLANATION,
            CHAT_MODE_ANALYSIS if has_context else CHAT_MODE_SIMPLE,
            ("generalist",) if has_context else ("chatter",),
            OVERRIDE_CONTEXT_ONLY if has_context else OVERRIDE_SUGGEST,
            "用户要求继续展开，应优先承接上一轮上下文。",
        )

    if is_followup and not has_context:
        return FollowupTaskPolicy(INTENT_AMBIGUOUS, CHAT_MODE_SIMPLE, ("chatter",), OVERRIDE_SUGGEST, "缺少可承接上下文，需要澄清。")

    if is_followup:
        plan = _infer_context_plan(context_text)
        return FollowupTaskPolicy(
            INTENT_AMBIGUOUS,
            CHAT_MODE_KNOWLEDGE if plan == ("chatter",) else CHAT_MODE_ANALYSIS,
            _compact_plan(plan),
            OVERRIDE_CONTEXT_ONLY,
            "当前是低信息追问，应承接上一轮上下文，复杂综合时才交给 generalist。",
        )

    return FollowupTaskPolicy(INTENT_NEW_TASK, "", (), OVERRIDE_CONTEXT_ONLY, "")


def build_followup_route_context(policy: FollowupTaskPolicy | dict[str, Any] | None) -> str:
    if policy is None:
        return ""
    payload = policy.to_dict() if isinstance(policy, FollowupTaskPolicy) else dict(policy)
    intent = str(payload.get("followup_intent") or "").strip()
    note = str(payload.get("context_note") or "").strip()
    plan = payload.get("recommended_plan") or []
    mode = str(payload.get("recommended_chat_mode") or "").strip()
    if not intent or intent == INTENT_NEW_TASK:
        return ""
    lines = ["【追问派工策略】", f"- 追问意图：{intent}"]
    if mode:
        lines.append(f"- 推荐大路由：{mode}")
    if plan:
        lines.append(f"- 推荐专家：{', '.join(str(item) for item in plan)}")
    if note:
        lines.append(f"- 承接说明：{note}")
    return "\n".join(lines)


def apply_followup_supervisor_policy(
    plan: Iterable[str],
    *,
    is_followup: bool,
    has_context: bool,
    followup_task_policy: dict[str, Any] | None = None,
    is_execute_suggested_stock_selection: bool = False,
) -> list[str]:
    current_plan = [str(item).strip() for item in (plan or []) if str(item).strip()]
    if not is_followup:
        return current_plan

    policy = followup_task_policy if isinstance(followup_task_policy, dict) else {}
    policy_plan = [
        str(item).strip()
        for item in (policy.get("recommended_plan") or [])
        if str(item).strip()
    ]
    policy_override = str(policy.get("override_level") or "").strip()

    if policy_override == OVERRIDE_FORCE and policy_plan:
        return policy_plan
    if is_execute_suggested_stock_selection:
        return ["screener"]
    if not has_context:
        return ["chatter"]
    if policy_override == OVERRIDE_SUGGEST and policy_plan and (
        not current_plan or current_plan[0] in {"generalist", "chatter"}
    ):
        return policy_plan
    if not current_plan:
        return policy_plan or ["generalist"]
    return current_plan
