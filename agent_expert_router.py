from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple


EXPERT_NAMES: Tuple[str, ...] = (
    "analyst",
    "researcher",
    "monitor",
    "strategist",
    "chatter",
    "generalist",
    "screener",
    "macro_analyst",
    "roaster",
    "portfolio_analyst",
)


@dataclass(frozen=True)
class ExpertSpec:
    name: str
    capabilities: Tuple[str, ...]
    parallelizable: bool
    dependencies: Tuple[str, ...] = ()
    default_model_tier: str = "mid"
    cost_level: int = 2


@dataclass(frozen=True)
class ExpertScore:
    name: str
    score: float
    evidence: Tuple[str, ...] = ()

    def as_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "score": round(float(self.score), 3),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True)
class RouteDecision:
    plan: List[str]
    symbol: str = ""
    expert_scores: List[ExpertScore] = field(default_factory=list)
    confidence: float = 0.0
    route_mode: str = "single"
    reason: str = ""
    selected_expert_count: int = 0
    route_tags: Tuple[str, ...] = ()

    def as_dict(self) -> Dict[str, Any]:
        return {
            "plan": list(self.plan),
            "symbol": self.symbol,
            "expert_scores": [score.as_dict() for score in self.expert_scores],
            "confidence": round(float(self.confidence), 3),
            "route_mode": self.route_mode,
            "reason": self.reason,
            "selected_expert_count": int(self.selected_expert_count),
            "route_tags": list(self.route_tags),
        }


EXPERT_REGISTRY: Dict[str, ExpertSpec] = {
    "analyst": ExpertSpec(
        name="analyst",
        capabilities=("technical_analysis", "trend", "single_asset_analysis"),
        parallelizable=True,
    ),
    "researcher": ExpertSpec(
        name="researcher",
        capabilities=("news", "event", "policy", "market_context"),
        parallelizable=True,
    ),
    "monitor": ExpertSpec(
        name="monitor",
        capabilities=("market_data", "option_data", "margin", "position_data"),
        parallelizable=True,
    ),
    "macro_analyst": ExpertSpec(
        name="macro_analyst",
        capabilities=("macro", "rates", "inflation", "usd", "cross_asset"),
        parallelizable=True,
    ),
    "strategist": ExpertSpec(
        name="strategist",
        capabilities=("option_strategy", "trade_plan", "risk_reward"),
        parallelizable=False,
        dependencies=("analyst", "monitor", "researcher", "macro_analyst"),
    ),
    "generalist": ExpertSpec(
        name="generalist",
        capabilities=("comparison", "chart", "complex_synthesis", "backtest"),
        parallelizable=False,
        default_model_tier="smart",
        cost_level=3,
    ),
    "screener": ExpertSpec(
        name="screener",
        capabilities=("stock_selection", "screening", "volume_scan"),
        parallelizable=False,
    ),
    "portfolio_analyst": ExpertSpec(
        name="portfolio_analyst",
        capabilities=("portfolio", "personalized_risk", "position_adjustment"),
        parallelizable=False,
    ),
    "chatter": ExpertSpec(
        name="chatter",
        capabilities=("clarification", "knowledge", "simple_chat"),
        parallelizable=False,
        default_model_tier="fast",
        cost_level=1,
    ),
    "roaster": ExpertSpec(
        name="roaster",
        capabilities=("adversarial_review", "roast_mode"),
        parallelizable=False,
    ),
}


HARD_SINGLE_EXPERT_TAGS = {
    "market_data",
    "pure_option_data",
    "stock_selection",
    "technical_concept",
    "chart",
    "portfolio",
}


def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, float(value)))


def _dedupe_plan(plan: Sequence[str] | None) -> List[str]:
    out: List[str] = []
    for raw_step in plan or []:
        step = str(raw_step or "").strip()
        if step in EXPERT_REGISTRY and step not in out:
            out.append(step)
    return out


def _normalize_planner_scores(scores: Mapping[str, Any] | None) -> Dict[str, float]:
    normalized: Dict[str, float] = {}
    for name, raw_score in (scores or {}).items():
        if name not in EXPERT_REGISTRY:
            continue
        try:
            normalized[name] = _clamp(float(raw_score))
        except (TypeError, ValueError):
            continue
    return normalized


def _infer_route_mode(plan: Sequence[str], route_tags: Iterable[str]) -> str:
    normalized_tags = set(route_tags)
    if not plan:
        return "clarify"
    if plan == ["chatter"] and (
        "low_confidence" in normalized_tags or "needs_clarification" in normalized_tags
    ):
        return "clarify"
    if len(plan) == 1:
        return "single"
    if "strategist" in plan:
        return "serial_pipeline"
    if all(EXPERT_REGISTRY[step].parallelizable for step in plan):
        return "top_k"
    return "mixed"


def _infer_confidence(
    plan: Sequence[str],
    *,
    route_mode: str,
    route_tags: Iterable[str],
    planner_confidence: float | None = None,
) -> float:
    tags = set(route_tags)
    if not plan:
        inferred = 0.35
    elif route_mode == "clarify":
        inferred = 0.42
    elif len(plan) == 1 and tags.intersection(HARD_SINGLE_EXPERT_TAGS):
        inferred = 0.93
    elif route_mode == "single":
        inferred = 0.84
    elif route_mode == "serial_pipeline":
        inferred = 0.80
    elif route_mode == "top_k":
        inferred = 0.76
    else:
        inferred = 0.68

    if planner_confidence is None or planner_confidence <= 0:
        return inferred
    return _clamp((inferred * 0.65) + (_clamp(planner_confidence) * 0.35))


def _score_selected_experts(
    plan: Sequence[str],
    *,
    confidence: float,
    route_mode: str,
    planner_scores: Mapping[str, float],
    route_tags: Sequence[str],
) -> List[ExpertScore]:
    scores: Dict[str, Tuple[float, List[str]]] = {
        name: (score, ["planner_score"])
        for name, score in planner_scores.items()
        if score > 0
    }

    for index, name in enumerate(plan):
        evidence = ["selected_by_plan"]
        if name == "strategist" and route_mode == "serial_pipeline":
            evidence.append("depends_on_prior_experts")
        if name == "monitor" and any(tag in route_tags for tag in ("market_data", "pure_option_data")):
            evidence.append("data_query_guardrail")
        if name == "chatter" and route_mode == "clarify":
            evidence.append("low_confidence_or_missing_subject")
        if name == "generalist" and "chart" in route_tags:
            evidence.append("chart_guardrail")
        if name == "portfolio_analyst" and "portfolio" in route_tags:
            evidence.append("portfolio_context")

        base_score = confidence
        if len(plan) > 1:
            base_score = max(0.58, confidence - (index * 0.03))
        if name == "strategist" and route_mode == "serial_pipeline":
            base_score = min(base_score, 0.72)

        current_score, current_evidence = scores.get(name, (0.0, []))
        scores[name] = (
            max(current_score, _clamp(base_score)),
            list(dict.fromkeys(current_evidence + evidence)),
        )

    return [
        ExpertScore(name=name, score=score, evidence=tuple(evidence))
        for name, (score, evidence) in sorted(
            scores.items(),
            key=lambda item: item[1][0],
            reverse=True,
        )
    ]


def build_route_decision(
    *,
    query: str,
    plan: Sequence[str] | None,
    symbol: str = "",
    planner_expert_scores: Mapping[str, Any] | None = None,
    planner_confidence: float | None = None,
    planner_reason: str = "",
    route_tags: Sequence[str] | None = None,
) -> RouteDecision:
    del query  # Reserved for later lexical scoring without changing the API.
    normalized_plan = _dedupe_plan(plan)
    normalized_symbol = str(symbol or "").strip()
    normalized_tags = tuple(dict.fromkeys(str(tag) for tag in (route_tags or []) if str(tag).strip()))

    if not normalized_plan:
        normalized_plan = ["chatter"]
        normalized_tags = tuple(dict.fromkeys((*normalized_tags, "low_confidence")))

    route_mode = _infer_route_mode(normalized_plan, normalized_tags)
    confidence = _infer_confidence(
        normalized_plan,
        route_mode=route_mode,
        route_tags=normalized_tags,
        planner_confidence=planner_confidence,
    )
    planner_scores = _normalize_planner_scores(planner_expert_scores)
    expert_scores = _score_selected_experts(
        normalized_plan,
        confidence=confidence,
        route_mode=route_mode,
        planner_scores=planner_scores,
        route_tags=normalized_tags,
    )

    if planner_reason:
        reason = str(planner_reason).strip()
    elif route_mode == "clarify":
        reason = "route confidence is low or the request needs clarification"
    elif route_mode == "top_k":
        reason = "multiple parallel experts are useful for this request"
    elif route_mode == "serial_pipeline":
        reason = "selected experts include a dependent strategy step"
    else:
        reason = "single expert route is sufficient"

    return RouteDecision(
        plan=list(normalized_plan),
        symbol=normalized_symbol,
        expert_scores=expert_scores,
        confidence=confidence,
        route_mode=route_mode,
        reason=reason,
        selected_expert_count=len(normalized_plan),
        route_tags=normalized_tags,
    )
