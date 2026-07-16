from __future__ import annotations

import math
import os
import re
import time
import time
from dataclasses import dataclass, field
from typing import Any, Literal

import pandas as pd
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.tools import tool
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text

from kline_algo import calculate_kline_signals


load_dotenv(override=True)

DEFAULT_DRAWDOWN_PCT = 20.0
DEFAULT_IV_PERCENTILE = 70.0
DEFAULT_IV_MIN_SAMPLES = 60
DEFAULT_VOLUME_RATIO = 1.5
DEFAULT_ABS_RETURN_PCT = 10.0
DEFAULT_RETURN_WINDOW = 20
DEFAULT_DRAWDOWN_WINDOW = 60
RECENT_PATTERN_DAYS = 5
DEFAULT_REVERSAL_RULE = "止跌转折＝多头吞噬/底部平台突破/假跌破/破底翻之一，或已确认的晨星/锤子/V型反转"

# Split factors in a trusted adjusted-close series are normally exact, but a
# cash dividend on the same session can make the observed step slightly noisy.
COMMON_SPLIT_RATIOS = (1.5, 2.0, 3.0, 4.0, 5.0, 7.0, 8.0, 10.0, 15.0, 20.0, 25.0, 50.0, 100.0)
SPLIT_FACTOR_TOLERANCE = 0.06
UNRESOLVED_PRICE_BREAK_MIN_RATIO = 1.8
UNRESOLVED_PRICE_BREAK_TOLERANCE = 0.12

SUPPORTED_PATTERN_ALIASES = {
    "多头吞噬": ("多头吞噬",),
    "看涨吞噬": ("多头吞噬",),
    "阳包阴": ("多头吞噬",),
    "底部突破": ("平台突破", "小区间突破"),
    "平台突破": ("平台突破", "小区间突破"),
    "假跌破": ("假跌破",),
    "空头陷阱": ("假跌破",),
    "诱空": ("假跌破",),
    "破底翻": ("破底翻",),
    "晨星": ("晨星",),
    "早晨之星": ("晨星",),
    "锤子": ("锤子线",),
    "锤子线": ("锤子线",),
    "金针探底": ("锤子线",),
    "V型反转": ("V型反转雏形",),
    "V形反转": ("V型反转雏形",),
}
STRONG_PATTERN_TOKENS = ("多头吞噬", "平台突破", "小区间突破", "假跌破", "破底翻")
SECONDARY_PATTERN_TOKENS = ("晨星", "锤子线", "V型反转雏形")


class USStockFilter(BaseModel):
    metric: str = Field(description="筛选指标，例如 max_drawdown_60d_pct、iv_percentile、volume_ratio_20d")
    operator: Literal["gte", "lte", "between", "abs_gte", "eq", "contains_any"] = "gte"
    value: Any = None
    value2: Any = None
    label: str = ""


class ScreenExpression(BaseModel):
    kind: Literal["condition", "all", "any", "not"] = Field(
        default="condition",
        description="condition为叶子条件；all/any/not为逻辑节点",
    )
    metric: str = Field(default="", description="仅condition填写注册表指标名，逻辑节点必须留空")
    operator: Literal["gte", "lte", "between", "abs_gte", "eq", "contains_any"] = "eq"
    value: Any = None
    value2: Any = None
    window: int | None = None
    unit: str = ""
    source_text: str = ""
    label: str = ""
    clauses: list["ScreenExpression"] = Field(
        default_factory=list,
        description="仅all/any/not填写子表达式；condition必须为空",
    )


if hasattr(ScreenExpression, "model_rebuild"):
    ScreenExpression.model_rebuild()


class ScreenPlan(BaseModel):
    market: Literal["US"] = "US"
    universe: Literal["auto", "all_local", "option_covered"] = "auto"
    where: ScreenExpression = Field(description="完整筛选表达式，必须保留用户原始AND/OR分组")
    sort_by: str = ""
    sort_order: Literal["asc", "desc"] = "desc"
    limit: int = Field(default=10, ge=1, le=30)
    defaults_used: list[str] = Field(default_factory=list)
    unsupported_clauses: list[str] = Field(default_factory=list)
    ambiguous_clauses: list[str] = Field(default_factory=list)
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)


class ScreenPlanCompileResult(BaseModel):
    """Outcome of the model-only ScreenPlan compilation step."""

    plan: ScreenPlan | None = None
    status: Literal[
        "success",
        "no_tool_call",
        "parse_error",
        "semantic_invalid",
        "timeout",
        "provider_error",
    ]
    elapsed_ms: int = 0
    model: str = ""
    error: str = ""
    has_tool_call: bool = False


class USStockScreenInput(BaseModel):
    query: str = Field(default="", description="客户的自然语言美股筛选条件")
    filters: list[USStockFilter] = Field(default_factory=list, description="可选结构化筛选条件；明确条件优先于自然语言默认值")
    logic: Literal["and", "or"] = Field(default="and", description="结构化筛选条件之间的逻辑")
    universe: Literal["auto", "all_local", "option_covered"] = Field(default="auto", description="本地美股池或具备期权指标的交集池")
    sort_by: str = Field(default="", description="排序字段，例如 iv_percentile、volume_ratio_20d、return_20d_pct")
    sort_order: Literal["asc", "desc"] = "desc"
    limit: int = Field(default=10, ge=1, le=30)
    plan: ScreenPlan | None = Field(default=None, description="经语义编译并通过约束的结构化筛选计划")


@dataclass(frozen=True)
class MetricSpec:
    key: str
    aliases: tuple[str, ...]
    operators: tuple[str, ...]
    unit: str
    min_value: float | None = None
    max_value: float | None = None
    min_window: int | None = None
    max_window: int | None = None
    default_window: int | None = None
    requires_iv: bool = False
    required_columns: tuple[str, ...] = ()
    min_history: int = 1
    default_rule: str = ""
    feature_template: str = ""


METRIC_REGISTRY: dict[str, MetricSpec] = {
    "latest_price": MetricSpec("latest_price", ("价格", "股价", "现价"), ("gte", "lte", "between"), "USD", 0.0, 10_000_000.0, required_columns=("close",), feature_template="latest_price"),
    "return_pct": MetricSpec("return_pct", ("涨跌幅", "涨幅", "跌幅"), ("gte", "lte", "between", "abs_gte"), "percent", -10_000.0, 10_000.0, 1, 252, 20, required_columns=("close",), min_history=2, default_rule="近期涨跌幅较大＝20日绝对涨跌幅至少10%", feature_template="return_{window}d_pct"),
    "max_drawdown": MetricSpec("max_drawdown", ("最大回撤", "回撤", "前期跌幅"), ("lte",), "percent", -100.0, 0.0, 2, 252, 60, required_columns=("close",), min_history=3, default_rule="前期跌幅大＝60日最大回撤至少20%", feature_template="max_drawdown_{window}d_pct"),
    "volume": MetricSpec("volume", ("成交量", "交易量"), ("gte", "lte", "between"), "shares", 0.0, None, required_columns=("volume",), feature_template="volume"),
    "volume_ratio": MetricSpec("volume_ratio", ("量比", "成交量倍数", "均量倍数", "放量"), ("gte", "lte", "between"), "ratio", 0.0, 1000.0, 1, 252, 20, required_columns=("volume",), min_history=2, default_rule="放量＝20日量比至少1.5", feature_template="volume_ratio_{window}d"),
    "rsi": MetricSpec("rsi", ("RSI", "相对强弱"), ("gte", "lte", "between"), "index", 0.0, 100.0, 2, 252, 14, required_columns=("close",), min_history=3, feature_template="rsi{window}"),
    "pattern": MetricSpec("pattern", tuple(SUPPORTED_PATTERN_ALIASES), ("eq", "contains_any"), "pattern", required_columns=("open", "high", "low", "close", "volume"), min_history=90, feature_template="strict_patterns"),
    "reversal_confirmed": MetricSpec("reversal_confirmed", ("止跌转折", "止跌反转", "底部转折"), ("eq",), "boolean", required_columns=("open", "high", "low", "close", "volume"), min_history=90, default_rule=DEFAULT_REVERSAL_RULE, feature_template="reversal_confirmed"),
    "atm_iv": MetricSpec("atm_iv", ("ATM IV", "期权波动率", "隐含波动率"), ("gte", "lte", "between"), "percent", 0.0, 1000.0, requires_iv=True, required_columns=("atm_iv_pct",), feature_template="atm_iv_pct"),
    "iv_rank": MetricSpec("iv_rank", ("IV Rank", "IV排名"), ("gte", "lte", "between"), "percent", 0.0, 100.0, requires_iv=True, required_columns=("atm_iv_pct",), min_history=60, feature_template="iv_rank"),
    "iv_percentile": MetricSpec("iv_percentile", ("IV Percentile", "IV分位", "隐波分位"), ("gte", "lte", "between"), "percent", 0.0, 100.0, requires_iv=True, required_columns=("atm_iv_pct",), min_history=60, default_rule="IV偏高＝252日IV Percentile至少70且至少60个样本", feature_template="iv_percentile"),
}

METRIC_ALIAS_INDEX = {
    alias.lower(): key
    for key, spec in METRIC_REGISTRY.items()
    for alias in (key, *spec.aliases)
}

FUZZY_DEFAULT_CONDITIONS: dict[str, tuple[str, float, int | None]] = {
    "return_pct": ("abs_gte", DEFAULT_ABS_RETURN_PCT, DEFAULT_RETURN_WINDOW),
    "max_drawdown": ("lte", -DEFAULT_DRAWDOWN_PCT, DEFAULT_DRAWDOWN_WINDOW),
    "volume_ratio": ("gte", DEFAULT_VOLUME_RATIO, 20),
    "iv_percentile": ("gte", DEFAULT_IV_PERCENTILE, None),
}


@dataclass
class ParsedScreenRequest:
    filters: list[USStockFilter] = field(default_factory=list)
    logic: str = "and"
    universe: str = "auto"
    sort_by: str = ""
    sort_order: str = "desc"
    limit: int = 10
    defaults: list[str] = field(default_factory=list)
    unsupported: list[str] = field(default_factory=list)
    ambiguous: list[str] = field(default_factory=list)
    where: ScreenExpression | None = None
    parser_mode: str = "rules"
    confidence: float = 1.0

    @property
    def requires_iv(self) -> bool:
        return any(str(item.metric).startswith(("iv_", "atm_iv", "option_")) for item in self.filters)


@dataclass(frozen=True)
class FeatureRequirements:
    """Price features that must be calculated for one screening request."""

    return_windows: frozenset[int] = frozenset()
    drawdown_windows: frozenset[int] = frozenset()
    volume_ratio_windows: frozenset[int] = frozenset()
    rsi_windows: frozenset[int] = frozenset()
    include_patterns: bool = False
    minimum_history: int = 90

    @classmethod
    def legacy_full(cls) -> "FeatureRequirements":
        """Keep direct, filter-less feature-builder calls backward compatible."""

        return cls(
            return_windows=frozenset({1, 5, 20, 60, 120}),
            drawdown_windows=frozenset({1, 5, 20, 60, 120}),
            volume_ratio_windows=frozenset({1, 5, 20, 60, 120}),
            rsi_windows=frozenset({14}),
            include_patterns=True,
            minimum_history=90,
        )


def _clean_float(value: Any) -> float | None:
    try:
        number = float(value)
    except Exception:
        return None
    return number if math.isfinite(number) else None


def _number(value: str) -> float:
    return float(str(value).replace(",", ""))


def _extract_window(text_value: str, default: int) -> int:
    match = re.search(r"(?:近|最近|过去|前)\s*(\d{1,3})\s*(?:个)?(?:交易日|日|天)", text_value)
    if not match:
        return default
    return max(1, min(252, int(match.group(1))))


def _add_filter(parsed: ParsedScreenRequest, condition: USStockFilter) -> None:
    for idx, existing in enumerate(parsed.filters):
        if existing.metric == condition.metric:
            parsed.filters[idx] = condition
            return
    parsed.filters.append(condition)


def _explicit_threshold(text_value: str, anchors: tuple[str, ...]) -> float | None:
    anchor_expr = "|".join(re.escape(item) for item in anchors)
    match = re.search(
        rf"(?:{anchor_expr}).{{0,14}}?(?:不低于|不少于|至少|大于|高于|超过|>=|＞)\s*([0-9]+(?:\.[0-9]+)?)\s*%?",
        text_value,
        re.IGNORECASE,
    )
    return _number(match.group(1)) if match else None


def _model_validate(model_class, value):
    if isinstance(value, model_class):
        return value
    if hasattr(model_class, "model_validate"):
        return model_class.model_validate(value)
    return model_class.parse_obj(value)


def _walk_screen_conditions(expression: ScreenExpression | None) -> list[ScreenExpression]:
    if expression is None:
        return []
    if expression.kind == "condition":
        return [expression]
    conditions: list[ScreenExpression] = []
    for clause in expression.clauses:
        conditions.extend(_walk_screen_conditions(clause))
    return conditions


def _canonical_metric(metric: str) -> str | None:
    return METRIC_ALIAS_INDEX.get(str(metric or "").strip().lower())


def _internal_metric(metric: str, window: int | None) -> str:
    if metric == "return_pct":
        return f"return_{window or DEFAULT_RETURN_WINDOW}d_pct"
    if metric == "max_drawdown":
        return f"max_drawdown_{window or DEFAULT_DRAWDOWN_WINDOW}d_pct"
    if metric == "volume_ratio":
        return f"volume_ratio_{window or 20}d"
    if metric == "rsi":
        return f"rsi{window or 14}"
    if metric == "pattern":
        return "patterns_any"
    if metric == "atm_iv":
        return "atm_iv_pct"
    return metric


def _condition_label(metric: str, window: int | None) -> str:
    labels = {
        "latest_price": "最新价",
        "return_pct": f"近{window or DEFAULT_RETURN_WINDOW}日涨跌幅",
        "max_drawdown": f"近{window or DEFAULT_DRAWDOWN_WINDOW}日最大回撤",
        "volume": "成交量",
        "volume_ratio": f"{window or 20}日量比",
        "rsi": f"RSI{window or 14}",
        "pattern": "K线形态",
        "reversal_confirmed": "已确认止跌转折",
        "atm_iv": "ATM IV",
        "iv_rank": "IV Rank",
        "iv_percentile": "IV Percentile",
    }
    return labels.get(metric, metric)


def _normalize_pattern_values(value: Any) -> list[str]:
    values = value if isinstance(value, list) else [value]
    normalized: list[str] = []
    for raw_value in values:
        text_value = str(raw_value or "").strip()
        matched = False
        for alias, tokens in SUPPORTED_PATTERN_ALIASES.items():
            if alias in text_value or text_value in alias:
                normalized.extend(tokens)
                matched = True
        if not matched and any(token in text_value for token in STRONG_PATTERN_TOKENS + SECONDARY_PATTERN_TOKENS):
            normalized.append(text_value)
    return list(dict.fromkeys(normalized))


def _number_matches_source(source_text: str, value: Any) -> bool:
    target = _clean_float(value)
    if target is None:
        return True
    numbers = [_clean_float(item) for item in re.findall(r"-?[0-9]+(?:\.[0-9]+)?", source_text or "")]
    numbers = [item for item in numbers if item is not None]
    if not numbers:
        return True
    return any(math.isclose(abs(float(item)), abs(target), rel_tol=1e-9, abs_tol=1e-9) for item in numbers)


def _condition_matches_registered_default(expression: ScreenExpression, canonical: str) -> bool:
    expected = FUZZY_DEFAULT_CONDITIONS.get(canonical)
    if expected is None:
        return False
    expected_operator, expected_value, expected_window = expected
    actual_value = _clean_float(expression.value)
    actual_window = expression.window
    if actual_window is None:
        actual_window = METRIC_REGISTRY[canonical].default_window
    return bool(
        expression.operator == expected_operator
        and actual_value is not None
        and math.isclose(actual_value, float(expected_value), rel_tol=1e-9, abs_tol=1e-9)
        and actual_window == expected_window
    )


def _attach_registered_defaults(plan: ScreenPlan) -> ScreenPlan:
    """Add only public defaults whose model conditions exactly match the registry."""

    defaults = list(plan.defaults_used)
    for item in _walk_screen_conditions(plan.where):
        canonical = _canonical_metric(item.metric)
        if canonical == "reversal_confirmed":
            normalized_value = str(item.value).strip().lower()
            is_confirmed = item.value is True or normalized_value in {"true", "1", "yes", "是", "已确认"}
            default_rule = METRIC_REGISTRY[canonical].default_rule
            if is_confirmed and default_rule and default_rule not in defaults:
                defaults.append(default_rule)
            continue
        if canonical in {"pattern", None}:
            continue
        spec = METRIC_REGISTRY[canonical]
        source_has_number = bool(re.search(r"[0-9]+(?:\.[0-9]+)?", item.source_text or ""))
        if (
            not source_has_number
            and spec.default_rule
            and _condition_matches_registered_default(item, canonical)
            and spec.default_rule not in defaults
        ):
            defaults.append(spec.default_rule)
    if defaults == list(plan.defaults_used):
        return plan
    if hasattr(plan, "model_copy"):
        return plan.model_copy(update={"defaults_used": defaults})
    plan.defaults_used = defaults
    return plan


def _validate_screen_expression(
    expression: ScreenExpression,
    *,
    filters: list[USStockFilter],
    errors: list[str],
) -> ScreenExpression:
    if expression.kind in {"all", "any", "not"}:
        if not expression.clauses:
            errors.append(f"{expression.kind.upper()}表达式没有子条件")
            return expression
        if expression.kind == "not" and len(expression.clauses) != 1:
            errors.append("NOT表达式必须且只能包含一个子条件")
        return ScreenExpression(
            kind=expression.kind,
            clauses=[_validate_screen_expression(item, filters=filters, errors=errors) for item in expression.clauses],
        )

    canonical = _canonical_metric(expression.metric)
    if canonical is None:
        errors.append(f"不支持的筛选指标：{expression.metric or '空指标'}")
        return expression
    spec = METRIC_REGISTRY[canonical]
    if expression.operator not in spec.operators:
        errors.append(f"{canonical}不支持运算符{expression.operator}")
    accepted_units = {
        spec.unit.lower(),
        {"percent": "%", "ratio": "倍", "USD": "美元", "shares": "股", "index": "指数", "boolean": "布尔", "pattern": "形态"}.get(spec.unit, spec.unit).lower(),
    }
    if expression.unit and str(expression.unit).strip().lower() not in accepted_units:
        errors.append(f"{canonical}单位{expression.unit}无效，应为{spec.unit}")
    window = expression.window if expression.window is not None else spec.default_window
    if window is not None:
        if spec.min_window is None or spec.max_window is None or not spec.min_window <= int(window) <= spec.max_window:
            errors.append(f"{canonical}窗口{window}超出支持范围")
        window = int(window)
        source_numbers = [_clean_float(item) for item in re.findall(r"[0-9]+(?:\.[0-9]+)?", expression.source_text or "")]
        if expression.window is not None and source_numbers and not any(
            item is not None and math.isclose(float(item), float(window), rel_tol=1e-9, abs_tol=1e-9)
            for item in source_numbers
        ):
            errors.append(f"{canonical}窗口{window}与用户原文不一致")
    if not str(expression.source_text or "").strip():
        errors.append(f"{_condition_label(canonical, window)}缺少对应的用户原文")

    value = expression.value
    value2 = expression.value2
    operator = expression.operator
    if canonical == "pattern":
        patterns = _normalize_pattern_values(value)
        if not patterns:
            errors.append(f"无法识别K线形态：{value}")
        value = patterns
        operator = "contains_any"
    elif canonical == "reversal_confirmed":
        if isinstance(value, str):
            normalized_bool = value.strip().lower()
            if normalized_bool in {"true", "1", "yes", "是", "已确认"}:
                value = True
            elif normalized_bool in {"false", "0", "no", "否", "未确认"}:
                value = False
            else:
                errors.append(f"止跌转折布尔值无效：{value}")
        else:
            value = bool(value)
    else:
        numeric = _clean_float(value)
        if numeric is None:
            errors.append(f"{_condition_label(canonical, window)}缺少有效数值")
        else:
            if spec.min_value is not None and numeric < spec.min_value:
                errors.append(f"{canonical}阈值{numeric}低于允许范围")
            if spec.max_value is not None and numeric > spec.max_value:
                errors.append(f"{canonical}阈值{numeric}高于允许范围")
            if not _number_matches_source(expression.source_text, numeric):
                errors.append(f"{_condition_label(canonical, window)}阈值与用户原文不一致")
            value = numeric
        if operator == "between":
            numeric2 = _clean_float(value2)
            if numeric2 is None or not _number_matches_source(expression.source_text, numeric2):
                errors.append(f"{_condition_label(canonical, window)}区间上限无效或与原文不一致")
            value2 = numeric2

    internal = _internal_metric(canonical, window)
    condition = USStockFilter(
        metric=internal,
        operator=operator,
        value=value,
        value2=value2,
        label=_condition_label(canonical, window),
    )
    filters.append(condition)
    return ScreenExpression(
        kind="condition",
        metric=internal,
        operator=operator,
        value=value,
        value2=value2,
        window=window,
        unit=spec.unit,
        source_text=expression.source_text,
        label=_condition_label(canonical, window),
    )


QUERY_DIMENSION_COVERAGE = {
    "价格条件": (("股价", "价格", "现价"), {"latest_price"}),
    "涨跌幅条件": (("涨跌幅", "涨幅", "跌幅"), {"return_pct", "max_drawdown"}),
    "回撤条件": (("回撤", "前期跌幅", "前期大跌", "跌幅大"), {"max_drawdown"}),
    "成交量条件": (("成交量", "交易量", "量比", "放量", "均量"), {"volume", "volume_ratio"}),
    "K线形态条件": (("止跌", "反转", "吞噬", "突破", "假跌破", "破底翻", "晨星", "锤子", "金针"), {"pattern", "reversal_confirmed"}),
    "RSI条件": (("rsi", "相对强弱"), {"rsi"}),
    "期权波动率条件": (("期权波动率", "隐含波动率", "iv", "隐波"), {"atm_iv", "iv_rank", "iv_percentile"}),
}


def validate_screen_plan(plan: ScreenPlan | dict[str, Any], query: str = "") -> ParsedScreenRequest:
    validated = _model_validate(ScreenPlan, plan)
    filters: list[USStockFilter] = []
    errors: list[str] = []
    normalized_where = _validate_screen_expression(validated.where, filters=filters, errors=errors)
    original_metrics = {
        _canonical_metric(item.metric)
        for item in _walk_screen_conditions(validated.where)
        if _canonical_metric(item.metric)
    }
    for item in _walk_screen_conditions(validated.where):
        canonical = _canonical_metric(item.metric)
        if canonical in {"pattern", "reversal_confirmed"} or canonical is None:
            continue
        source_has_number = bool(re.search(r"[0-9]+(?:\.[0-9]+)?", item.source_text or ""))
        spec = METRIC_REGISTRY[canonical]
        if not source_has_number and spec.default_rule and spec.default_rule not in validated.defaults_used:
            errors.append(f"{_condition_label(canonical, item.window)}使用了模糊条件，但未声明公开默认口径")
        elif not source_has_number and spec.default_rule and not _condition_matches_registered_default(item, canonical):
            errors.append(f"{_condition_label(canonical, item.window)}与公开默认口径不一致")
        elif not source_has_number and not spec.default_rule:
            errors.append(f"{_condition_label(canonical, item.window)}缺少明确阈值且没有公开默认规则")
    lower_query = str(query or "").lower()
    if re.search(r"(?:\bsql\b|\bselect\b|\binsert\b|\bupdate\b|\bdelete\b|\bdrop\b|\bpython\b|__import__)", lower_query):
        errors.append("筛选条件禁止包含SQL、Python或可执行代码")
    for label, (tokens, expected_metrics) in QUERY_DIMENSION_COVERAGE.items():
        if any(token in lower_query for token in tokens) and not (original_metrics & expected_metrics):
            errors.append(f"{label}未被结构化解析")
    if validated.confidence < 0.65:
        errors.append(f"语义解析置信度过低：{validated.confidence:.2f}")
    if validated.sort_by and _canonical_metric(validated.sort_by) is None:
        errors.append(f"不支持的排序指标：{validated.sort_by}")
    errors.extend(str(item) for item in validated.ambiguous_clauses if str(item).strip())
    unsupported = [str(item) for item in validated.unsupported_clauses if str(item).strip()]
    if unsupported:
        errors.extend(f"不支持条件：{item}" for item in unsupported)

    parsed = ParsedScreenRequest(
        filters=filters,
        universe=validated.universe,
        sort_by=_internal_metric(_canonical_metric(validated.sort_by) or validated.sort_by, None) if validated.sort_by else "",
        sort_order=validated.sort_order,
        limit=validated.limit,
        defaults=list(validated.defaults_used),
        unsupported=unsupported,
        ambiguous=list(dict.fromkeys(errors)),
        where=normalized_where,
        parser_mode="llm",
        confidence=validated.confidence,
    )
    if parsed.requires_iv and parsed.universe == "auto":
        parsed.universe = "option_covered"
    elif parsed.universe == "auto":
        parsed.universe = "all_local"
    return parsed


def compile_screen_plan_with_llm(query: str, llm, *, limit: int = 10) -> ScreenPlanCompileResult:
    started_at = time.perf_counter()
    model_name = str(
        getattr(llm, "model_name", "")
        or getattr(llm, "model", "")
        or getattr(llm, "name", "")
        or type(llm).__name__
    )

    def outcome(
        status: str,
        *,
        plan: ScreenPlan | None = None,
        error: str = "",
        has_tool_call: bool = False,
    ) -> ScreenPlanCompileResult:
        return ScreenPlanCompileResult(
            plan=plan,
            status=status,
            elapsed_ms=max(0, int((time.perf_counter() - started_at) * 1000)),
            model=model_name,
            error=str(error or ""),
            has_tool_call=bool(has_tool_call),
        )

    if llm is None or not hasattr(llm, "bind_tools"):
        return outcome("provider_error", error="当前模型不支持工具调用")
    registry_text = "\n".join(
        f"- {key}: aliases={','.join(spec.aliases)}; operators={','.join(spec.operators)}; "
        f"window={spec.min_window or '-'}..{spec.max_window or '-'}; unit={spec.unit}"
        for key, spec in METRIC_REGISTRY.items()
    )
    system_prompt = f"""
你是美股筛选条件编译器，只负责把用户语言转换为ScreenPlan，不负责推荐股票。
只能使用以下指标注册表：
{registry_text}

强约束：
1. where必须忠实保留且/并且/同时对应all，或对应any，否定对应not；禁止遗漏任何条件。
2. condition.metric只能使用注册表标准名称；禁止SQL、Python、自定义公式和股票代码结果。
3. 显式窗口和阈值原样保留；source_text必须复制该条件对应的用户原文。
4. 模糊词只允许使用公开默认值：前期跌幅大=max_drawdown 60日 <= -20%；放量=volume_ratio 20日 >=1.5；IV偏高=iv_percentile >=70且至少60个样本；近期涨跌幅较大=return_pct 20日绝对值>=10%。
5. 不支持内容写入unsupported_clauses；逻辑或阈值无法确定时写入ambiguous_clauses，不要猜测。
6. sort和limit只表示排序与展示数量，不属于where。市场固定US。

规范示例：
- “近60日最大回撤至少25%”＝condition(metric=max_drawdown, operator=lte, value=-25, window=60, unit=percent)。
- “假跌破或破底翻，且20日量比至少1.5”＝all(any(pattern=假跌破, pattern=破底翻), volume_ratio window=20 gte 1.5)。
- “前期跌幅大、止跌转折、IV偏高”＝all(max_drawdown window=60 lte -20, reversal_confirmed eq true, iv_percentile window=null gte 70)；
  defaults_used必须列出前期跌幅、止跌转折定义和IV偏高这三条公开默认口径。
- IV Percentile的至少60个样本是引擎数据资格，不是condition.window；iv_percentile、iv_rank和atm_iv的window必须为null。
- all/any节点只能放clauses，不得同时填写metric；condition节点必须填写metric且不得包含clauses。
""".strip()
    try:
        # ChatTongyi.with_structured_output() silently returns None when the
        # provider emits text instead of a tool call. Bind and inspect the raw
        # message ourselves so None is never sent to Pydantic.
        compiler = llm.bind_tools(
            [ScreenPlan],
            tool_choice={"type": "function", "function": {"name": "ScreenPlan"}},
        )
        raw_message = compiler.invoke(
            [SystemMessage(content=system_prompt), HumanMessage(content=str(query or ""))]
        )
        tool_calls = list(getattr(raw_message, "tool_calls", None) or [])
        if not tool_calls:
            return outcome("no_tool_call", error="模型未返回ScreenPlan工具调用")
        selected_call = next(
            (
                item
                for item in tool_calls
                if str(item.get("name", "") if isinstance(item, dict) else getattr(item, "name", ""))
                == "ScreenPlan"
            ),
            tool_calls[0],
        )
        raw_args = (
            selected_call.get("args")
            if isinstance(selected_call, dict)
            else getattr(selected_call, "args", None)
        )
        if raw_args is None:
            return outcome("parse_error", error="ScreenPlan工具调用缺少参数", has_tool_call=True)
        try:
            plan = _model_validate(ScreenPlan, raw_args)
        except Exception as exc:
            return outcome("parse_error", error=str(exc), has_tool_call=True)
        update = {"limit": max(1, min(30, int(limit or plan.limit)))}
        if hasattr(plan, "model_copy"):
            plan = plan.model_copy(update=update)
        else:
            plan.limit = update["limit"]
        plan = _attach_registered_defaults(plan)
        try:
            parsed = validate_screen_plan(plan, query=query)
        except Exception as exc:
            return outcome("semantic_invalid", error=str(exc), has_tool_call=True)
        semantic_issues = list(dict.fromkeys([*parsed.unsupported, *parsed.ambiguous]))
        if not parsed.filters:
            semantic_issues.append("语义计划没有可执行筛选条件")
        if semantic_issues:
            return outcome(
                "semantic_invalid",
                error="；".join(str(item) for item in semantic_issues if str(item).strip()),
                has_tool_call=True,
            )
        return outcome("success", plan=plan, has_tool_call=True)
    except Exception as exc:
        error_text = f"{type(exc).__name__}: {exc}"
        status = "timeout" if "timeout" in error_text.lower() or "timed out" in error_text.lower() else "provider_error"
        return outcome(status, error=error_text)


def compare_semantic_plan_to_rules(query: str, plan: ScreenPlan | dict[str, Any], *, limit: int = 10) -> dict[str, Any]:
    semantic = validate_screen_plan(plan, query=query)
    rules = parse_us_stock_screen_query(query, limit=limit)
    rules.ambiguous.extend(_fallback_coverage_errors(query, rules))
    semantic_text = _expression_text(semantic.where) or "；".join(_condition_text(item) for item in semantic.filters)
    rules_text = "；".join(_condition_text(item) for item in rules.filters)
    return {
        "same": semantic_text == rules_text and not semantic.ambiguous and not rules.ambiguous,
        "semantic_conditions": semantic_text,
        "rule_conditions": rules_text,
        "semantic_issues": semantic.ambiguous,
        "rule_issues": rules.ambiguous,
    }


def parse_us_stock_screen_query(
    query: str,
    *,
    filters: list[USStockFilter] | None = None,
    logic: str = "and",
    universe: str = "auto",
    sort_by: str = "",
    sort_order: str = "desc",
    limit: int = 10,
) -> ParsedScreenRequest:
    raw = str(query or "").strip()
    lower = raw.lower()
    parsed = ParsedScreenRequest(
        filters=list(filters or []),
        logic="or" if str(logic).lower() == "or" else "and",
        universe=universe if universe in {"auto", "all_local", "option_covered"} else "auto",
        sort_by=str(sort_by or "").strip(),
        sort_order="asc" if str(sort_order).lower() == "asc" else "desc",
        limit=max(1, min(30, int(limit or 10))),
    )

    if filters:
        if parsed.requires_iv and parsed.universe == "auto":
            parsed.universe = "option_covered"
        elif parsed.universe == "auto":
            parsed.universe = "all_local"
        return parsed

    # 价格区间和上下限。
    between = re.search(
        r"(?:股价|价格|现价).{0,8}?([0-9]+(?:\.[0-9]+)?)\s*(?:到|至|[-~—])\s*([0-9]+(?:\.[0-9]+)?)\s*(?:美元|美金|刀)?",
        raw,
    )
    if between:
        _add_filter(parsed, USStockFilter(metric="latest_price", operator="between", value=_number(between.group(1)), value2=_number(between.group(2)), label="价格区间"))
    else:
        low_price = re.search(r"(?:股价|价格|现价).{0,8}?(?:不低于|至少|大于|高于|超过|>=|＞)\s*\$?([0-9]+(?:\.[0-9]+)?)", raw)
        high_price = re.search(r"(?:股价|价格|现价).{0,8}?(?:不超过|至多|小于|低于|<=|＜)\s*\$?([0-9]+(?:\.[0-9]+)?)", raw)
        if low_price:
            _add_filter(parsed, USStockFilter(metric="latest_price", operator="gte", value=_number(low_price.group(1)), label="最低股价"))
        if high_price:
            _add_filter(parsed, USStockFilter(metric="latest_price", operator="lte", value=_number(high_price.group(1)), label="最高股价"))

    # 前期跌幅/回撤。数值明确时覆盖20%的默认值。
    if "回撤" in raw or any(token in raw for token in ("前期跌幅", "前期大跌", "跌幅大")):
        window = _extract_window(raw, DEFAULT_DRAWDOWN_WINDOW)
        threshold = _explicit_threshold(raw, ("跌幅", "回撤"))
        if threshold is None:
            threshold = DEFAULT_DRAWDOWN_PCT
            parsed.defaults.append(f"前期跌幅大＝近{window}日最大回撤至少{threshold:.0f}%")
        _add_filter(parsed, USStockFilter(metric=f"max_drawdown_{window}d_pct", operator="lte", value=-abs(threshold), label=f"近{window}日最大回撤"))

    # 明确的N日涨跌幅，以及模糊的近期涨跌幅较大。
    ret_match = re.search(
        r"(?:近|最近|过去)?\s*(\d{1,3})\s*(?:个)?(?:交易日|日|天).{0,10}?(涨幅|跌幅|涨跌幅).{0,10}?(?:不低于|不少于|至少|大于|超过|高于|>=|＞)?\s*([0-9]+(?:\.[0-9]+)?)\s*%",
        raw,
    )
    if ret_match:
        window = max(1, min(252, int(ret_match.group(1))))
        direction = ret_match.group(2)
        threshold = _number(ret_match.group(3))
        if direction == "涨幅":
            condition = USStockFilter(metric=f"return_{window}d_pct", operator="gte", value=threshold, label=f"近{window}日涨幅")
        elif direction == "跌幅":
            condition = USStockFilter(metric=f"return_{window}d_pct", operator="lte", value=-threshold, label=f"近{window}日跌幅")
        else:
            condition = USStockFilter(metric=f"return_{window}d_pct", operator="abs_gte", value=threshold, label=f"近{window}日涨跌幅绝对值")
        _add_filter(parsed, condition)
    elif any(token in raw for token in ("近期涨跌幅较大", "近期涨跌幅大", "涨跌幅度大")):
        _add_filter(parsed, USStockFilter(metric=f"return_{DEFAULT_RETURN_WINDOW}d_pct", operator="abs_gte", value=DEFAULT_ABS_RETURN_PCT, label=f"近{DEFAULT_RETURN_WINDOW}日涨跌幅绝对值"))
        parsed.defaults.append(f"近期涨跌幅较大＝近{DEFAULT_RETURN_WINDOW}日绝对涨跌幅至少{DEFAULT_ABS_RETURN_PCT:.0f}%")

    # K线转折形态。默认止跌转折只接受强信号或已确认的次级信号。
    requested_patterns: list[str] = []
    for alias, tokens in SUPPORTED_PATTERN_ALIASES.items():
        if alias in raw:
            requested_patterns.extend(tokens)
    has_reversal_phrase = any(token in raw for token in ("止跌转折", "止跌反转", "止跌信号", "底部转折", "出现转折"))
    if has_reversal_phrase:
        _add_filter(parsed, USStockFilter(metric="reversal_confirmed", operator="eq", value=True, label="已确认止跌转折"))
        parsed.defaults.append(DEFAULT_REVERSAL_RULE)
    elif requested_patterns:
        unique_patterns = list(dict.fromkeys(requested_patterns))
        _add_filter(parsed, USStockFilter(metric="patterns_any", operator="contains_any", value=unique_patterns, label="K线形态"))

    # 成交量/量比。
    relative_volume_match = re.search(
        r"(?:成交量|交易量).{0,10}?(?:超过|达到|不低于|至少|为|是)?\s*(\d{1,3})\s*(?:个)?(?:交易日|日|天)(?:平均量|均量).{0,6}?(?:的)?\s*([0-9]+(?:\.[0-9]+)?)\s*倍(?:以上)?",
        raw,
    )
    volume_match = re.search(r"(?:量比|成交量倍数|交易量倍数).{0,10}?(?:不低于|至少|大于|高于|超过|>=|＞)\s*([0-9]+(?:\.[0-9]+)?)", raw)
    if relative_volume_match:
        window = max(1, min(252, int(relative_volume_match.group(1))))
        _add_filter(
            parsed,
            USStockFilter(
                metric=f"volume_ratio_{window}d",
                operator="gte",
                value=_number(relative_volume_match.group(2)),
                label=f"{window}日量比",
            ),
        )
    elif volume_match:
        _add_filter(parsed, USStockFilter(metric="volume_ratio_20d", operator="gte", value=_number(volume_match.group(1)), label="20日量比"))
    elif any(token in raw for token in ("放量", "成交量放大", "交易量放大", "成交量明显增加")):
        _add_filter(parsed, USStockFilter(metric="volume_ratio_20d", operator="gte", value=DEFAULT_VOLUME_RATIO, label="20日量比"))
        parsed.defaults.append(f"放量＝成交量至少为此前20日均量的{DEFAULT_VOLUME_RATIO:.1f}倍")

    rsi_match = re.search(
        r"(?:rsi|相对强弱(?:指标)?)\s*(\d{1,3})?.{0,8}?(不高于|不超过|低于|小于|<=|＜|不低于|至少|高于|大于|>=|＞)\s*([0-9]+(?:\.[0-9]+)?)",
        lower,
        re.IGNORECASE,
    )
    if rsi_match:
        window = max(2, min(252, int(rsi_match.group(1) or 14)))
        operator = "lte" if rsi_match.group(2) in {"不高于", "不超过", "低于", "小于", "<=", "＜"} else "gte"
        _add_filter(parsed, USStockFilter(metric=f"rsi{window}", operator=operator, value=_number(rsi_match.group(3)), label=f"RSI{window}"))

    # 期权波动率：优先使用客户点名的 Rank/Percentile/绝对ATM IV。
    iv_rank = _explicit_threshold(lower, ("iv rank", "ivrank"))
    iv_percentile = _explicit_threshold(lower, ("iv percentile", "iv分位", "波动率分位", "隐波分位"))
    atm_iv = _explicit_threshold(lower, ("atm iv", "期权波动率", "隐含波动率"))
    if iv_rank is not None:
        _add_filter(parsed, USStockFilter(metric="iv_rank", operator="gte", value=iv_rank, label="IV Rank"))
    elif iv_percentile is not None:
        _add_filter(parsed, USStockFilter(metric="iv_percentile", operator="gte", value=iv_percentile, label="IV Percentile"))
    elif atm_iv is not None:
        _add_filter(parsed, USStockFilter(metric="atm_iv_pct", operator="gte", value=atm_iv, label="ATM IV"))
    elif any(token in lower for token in ("iv偏高", "iv 偏高", "波动率偏高", "隐含波动率偏高", "期权波动率还偏高", "期权波动率偏高")):
        _add_filter(parsed, USStockFilter(metric="iv_percentile", operator="gte", value=DEFAULT_IV_PERCENTILE, label="IV Percentile"))
        parsed.defaults.append(f"IV偏高＝252日IV Percentile不低于{DEFAULT_IV_PERCENTILE:.0f}，至少{DEFAULT_IV_MIN_SAMPLES}个样本")

    if not parsed.sort_by:
        if re.search(r"(?:按|根据).{0,8}(?:iv|波动率).{0,8}(?:从高到低|降序|排名|排行)", lower):
            parsed.sort_by = "iv_percentile"
        elif re.search(r"(?:按|根据).{0,8}(?:成交量|量比|交易量).{0,8}(?:从高到低|降序|排名|排行)", raw):
            parsed.sort_by = "volume_ratio_20d"
        elif "跌幅最大" in raw or "回撤最大" in raw:
            parsed.sort_by = f"max_drawdown_{_extract_window(raw, DEFAULT_DRAWDOWN_WINDOW)}d_pct"
            parsed.sort_order = "asc"

    unsupported_map = {
        "基本面": ("基本面", "营收", "利润", "净利润"),
        "估值": ("市盈率", "市净率", "估值", "pe", "pb"),
        "市值": ("市值", "market cap"),
        "行业/板块": ("行业", "板块"),
    }
    for label, tokens in unsupported_map.items():
        if any(token in lower for token in tokens):
            parsed.unsupported.append(label)

    if parsed.requires_iv and parsed.universe == "auto":
        parsed.universe = "option_covered"
    elif parsed.universe == "auto":
        parsed.universe = "all_local"
    return parsed


def _fallback_coverage_errors(query: str, parsed: ParsedScreenRequest) -> list[str]:
    metric_names = {str(item.metric) for item in parsed.filters}
    canonical_metrics: set[str] = set()
    for metric in metric_names:
        if metric.startswith("return_"):
            canonical_metrics.add("return_pct")
        elif metric.startswith("max_drawdown_"):
            canonical_metrics.add("max_drawdown")
        elif metric.startswith("volume_ratio_"):
            canonical_metrics.add("volume_ratio")
        elif metric.startswith("rsi"):
            canonical_metrics.add("rsi")
        elif metric == "patterns_any":
            canonical_metrics.add("pattern")
        elif metric == "atm_iv_pct":
            canonical_metrics.add("atm_iv")
        else:
            canonical_metrics.add(metric)
    errors: list[str] = []
    lower_query = str(query or "").lower()
    for label, (tokens, expected_metrics) in QUERY_DIMENSION_COVERAGE.items():
        if any(token in lower_query for token in tokens) and not (canonical_metrics & expected_metrics):
            errors.append(f"{label}未被降级解析器识别")
    return errors


def is_us_multifactor_screen_query(query: str) -> bool:
    text_value = str(query or "").strip()
    lower = text_value.lower()
    if not text_value:
        return False
    has_us = any(token in text_value for token in ("美股", "纳斯达克", "纽交所")) or "us stock" in lower
    us_pool_action = bool(re.search(
        r"(?:从|在)?美股(?:股票)?(?:池)?(?:里|中|内)?(?:帮我)?(?:找|筛选|选|挑)",
        text_value,
    ))
    has_screen = us_pool_action or any(
        token in text_value
        for token in ("筛选", "帮我找", "帮我选", "找几只", "选几只", "候选股", "股票池")
    )
    if not (has_us and has_screen):
        return False
    if any(token in text_value for token in ("做空", "看跌", "空头", "破位")) and not any(
        token in text_value for token in ("止跌", "反转", "多头吞噬", "假跌破", "破底翻")
    ):
        return False
    dimensions = (
        "涨幅", "跌幅", "回撤", "价格", "股价", "成交量", "交易量", "量比", "放量",
        "期权波动率", "隐含波动率", "iv", "止跌", "反转", "吞噬", "突破", "假跌破",
        "破底翻", "晨星", "锤子", "金针探底", "V型反转", "V形反转",
        "rsi", "相对强弱",
    )
    return any(token in lower for token in dimensions)


def _nearest_split_multiplier(
    value: Any,
    *,
    tolerance: float = SPLIT_FACTOR_TOLERANCE,
) -> float | None:
    """Return a signed split multiplier only when the ratio is near a common split."""
    number = _clean_float(value)
    if number is None or number <= 0:
        return None
    candidates = (*COMMON_SPLIT_RATIOS, *(1.0 / item for item in COMMON_SPLIT_RATIOS))
    for candidate in candidates:
        if abs(number - candidate) / candidate <= tolerance:
            return float(candidate)
    return None


def _normalize_stock_history(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    required = {"date", "symbol", "open", "high", "low", "close", "volume"}
    if df is None or df.empty:
        return pd.DataFrame(), "stock_prices没有可用美股日线"
    missing = required - set(df.columns)
    if missing:
        return pd.DataFrame(), f"stock_prices缺少字段：{', '.join(sorted(missing))}"
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
    out["symbol"] = out["symbol"].astype(str).str.upper().str.strip()
    for col in ("open", "high", "low", "close", "volume", "adjClose", "adjVolume"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.dropna(subset=["date", "symbol", "open", "high", "low", "close"]).sort_values(["symbol", "date"])
    out = out.drop_duplicates(["symbol", "date"], keep="last")
    out["_unresolved_price_break"] = False
    out["_trusted_split_adjustment"] = False
    split_adjusted_symbols: set[str] = set()
    price_anomaly_symbols: set[str] = set()

    normalized_groups: list[pd.DataFrame] = []
    for symbol, group in out.groupby("symbol", sort=False):
        group = group.sort_values("date").copy()
        raw_close = pd.to_numeric(group["close"], errors="coerce")
        if "adjClose" in group.columns:
            price_factor = pd.to_numeric(group["adjClose"], errors="coerce") / raw_close.replace(0, pd.NA)
        else:
            price_factor = pd.Series(1.0, index=group.index, dtype=float)
        price_factor = price_factor.where(price_factor.gt(0), 1.0).fillna(1.0).astype(float)

        # Price features use the supplied adjusted-close basis.  The same
        # factor is applied to OHLC so candlestick geometry remains coherent.
        for col in ("open", "high", "low", "close"):
            group[col] = pd.to_numeric(group[col], errors="coerce") * price_factor

        # Prefer an explicit adjusted-volume series.  When the table only has
        # raw volume, derive volume adjustment solely from confirmed integer
        # jumps in the trusted price factor.  We deliberately do not apply the
        # full price factor because that also contains cash-dividend effects.
        explicit_adj_volume = (
            pd.to_numeric(group["adjVolume"], errors="coerce")
            if "adjVolume" in group.columns
            else pd.Series(index=group.index, dtype=float)
        )
        has_explicit_adj_volume = bool(explicit_adj_volume.notna().any())
        if has_explicit_adj_volume:
            valid_volume = explicit_adj_volume.notna() & explicit_adj_volume.ge(0)
            group.loc[valid_volume, "volume"] = explicit_adj_volume.loc[valid_volume]
        else:
            factor_steps = price_factor / price_factor.shift(1).replace(0, pd.NA)
            volume_multiplier = pd.Series(1.0, index=group.index, dtype=float)
            matched_event = False
            for position in range(1, len(group)):
                multiplier = _nearest_split_multiplier(factor_steps.iloc[position])
                if multiplier is None:
                    continue
                matched_event = True
                volume_multiplier.iloc[:position] *= multiplier
            if matched_event:
                group["volume"] = pd.to_numeric(group["volume"], errors="coerce") * volume_multiplier
                group["_trusted_split_adjustment"] = True
                split_adjusted_symbols.add(str(symbol))

        adjusted_close = pd.to_numeric(group["close"], errors="coerce")
        close_steps = adjusted_close / adjusted_close.shift(1).replace(0, pd.NA)
        unresolved = pd.Series(False, index=group.index)
        for position in range(1, len(group)):
            step = _clean_float(close_steps.iloc[position])
            if step is None or step <= 0:
                continue
            scale = max(step, 1.0 / step)
            if scale < UNRESOLVED_PRICE_BREAK_MIN_RATIO:
                continue
            if _nearest_split_multiplier(step, tolerance=UNRESOLVED_PRICE_BREAK_TOLERANCE) is not None:
                unresolved.iloc[position] = True
        if bool(unresolved.any()):
            group["_unresolved_price_break"] = unresolved
            price_anomaly_symbols.add(str(symbol))
        normalized_groups.append(group)

    out = pd.concat(normalized_groups, ignore_index=True) if normalized_groups else out.iloc[0:0].copy()
    out.attrs["quality_stats"] = {
        "split_adjusted_symbols": sorted(split_adjusted_symbols),
        "price_anomaly_symbols": sorted(price_anomaly_symbols),
    }
    return out, ""


def _max_drawdown_pct(series: pd.Series) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if len(values) < 2:
        return None
    running_peak = values.cummax().replace(0, pd.NA)
    drawdowns = values / running_peak - 1.0
    value = drawdowns.min()
    return None if pd.isna(value) else float(value * 100.0)


def _rsi(series: pd.Series, window: int = 14) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna()
    window = max(2, min(252, int(window)))
    if len(values) < window + 1:
        return None
    delta = values.diff()
    gains = delta.clip(lower=0).tail(window).mean()
    losses = (-delta.clip(upper=0)).tail(window).mean()
    if losses == 0:
        return 100.0 if gains > 0 else 50.0
    rs = gains / losses
    return float(100.0 - 100.0 / (1.0 + rs))


def _rsi14(series: pd.Series) -> float | None:
    return _rsi(series, 14)


def _v_reversal_pattern(group: pd.DataFrame) -> bool:
    closes = pd.to_numeric(group["close"], errors="coerce").dropna()
    if len(closes) < 5:
        return False
    changes = closes.pct_change().tail(4).tolist()
    return bool(changes[0] < -0.01 and changes[1] < -0.01 and changes[2] > 0 and changes[3] > 0.01)


def _latest_swing_low(group: pd.DataFrame, end_index: int) -> float | None:
    values = pd.to_numeric(group.iloc[max(0, end_index - 32): max(0, end_index - 2)]["low"], errors="coerce").dropna().tolist()
    for idx in range(len(values) - 3, 1, -1):
        if idx + 2 >= len(values):
            continue
        value = values[idx]
        if values[idx - 1] > value and values[idx - 2] > value and values[idx + 1] > value and values[idx + 2] > value:
            return float(value)
    return None


def _pattern_key_level(group: pd.DataFrame, end_index: int, pattern: str) -> float | None:
    signal = group.iloc[end_index - 1]
    if "平台突破" in pattern:
        match = re.search(r"(\d+)日", pattern)
        period = int(match.group(1)) if match else 20
        prior = group.iloc[max(0, end_index - period - 1): end_index - 1]
        return _clean_float(prior["high"].max()) if not prior.empty else None
    if "假跌破" in pattern or "破底翻" in pattern:
        swing_low = _latest_swing_low(group, end_index)
        if swing_low is not None:
            return swing_low
        prior = group.iloc[max(0, end_index - 22): max(0, end_index - 2)]
        return _clean_float(prior["low"].min()) if not prior.empty else None
    if "多头吞噬" in pattern and end_index >= 2:
        return _clean_float(group.iloc[end_index - 2]["high"])
    return _clean_float(signal.get("high"))


def _pattern_state(group: pd.DataFrame) -> dict[str, Any]:
    recent_patterns: list[str] = []
    strict_patterns: list[str] = []
    waiting_patterns: list[str] = []
    details: list[str] = []
    key_levels: list[float] = []
    strict_key_levels: list[float] = []
    strict_signal_dates: list[str] = []
    confirmation_bases: list[str] = []
    waiting_bases: list[str] = []
    count = len(group)
    if count < 30:
        return {
            "recent_patterns": "",
            "strict_patterns": "",
            "waiting_patterns": "",
            "reversal_confirmed": False,
            "key_level": None,
            "pattern_details": "",
            "signal_date": "",
            "confirmation_basis": "",
            "waiting_basis": "",
        }

    start = max(30, count - RECENT_PATTERN_DAYS + 1)
    for end_index in range(start, count + 1):
        sample = group.iloc[:end_index].copy()
        algo_input = sample.rename(columns={"open": "open_price", "high": "high_price", "low": "low_price", "close": "close_price"})
        try:
            result = calculate_kline_signals(algo_input[["date", "open_price", "high_price", "low_price", "close_price"]].copy())
            patterns = [str(item) for item in (result.get("patterns") or [])]
        except Exception:
            patterns = []
        if _v_reversal_pattern(sample):
            patterns.append("V型反转雏形")

        signal = group.iloc[end_index - 1]
        later = group.iloc[end_index:]
        prior_volume = pd.to_numeric(group.iloc[max(0, end_index - 21): end_index - 1]["volume"], errors="coerce").dropna()
        volume_ratio = None
        if not prior_volume.empty and float(prior_volume.mean()) > 0:
            volume_ratio = float(signal["volume"]) / float(prior_volume.mean())

        for pattern in patterns:
            if not any(token in pattern for token in STRONG_PATTERN_TOKENS + SECONDARY_PATTERN_TOKENS):
                continue
            if pattern not in recent_patterns:
                recent_patterns.append(pattern)
            level = _pattern_key_level(group, end_index, pattern)
            signal_date = pd.to_datetime(signal["date"]).strftime("%Y-%m-%d")
            latest_close = _clean_float(group.iloc[-1]["close"])
            invalidated = bool(
                latest_close is not None
                and (
                    latest_close < float(signal["low"])
                    or (
                        level is not None
                        and any(token in pattern for token in STRONG_PATTERN_TOKENS)
                        and latest_close < float(level)
                    )
                )
            )
            accepted_strict = False
            if any(token in pattern for token in STRONG_PATTERN_TOKENS) and not invalidated:
                if pattern not in strict_patterns:
                    strict_patterns.append(pattern)
                accepted_strict = True
                basis = f"{pattern}强信号成立"
                if latest_close is not None and level is not None:
                    basis += f"；最新收盘{latest_close:.2f}未跌破关键位{level:.2f}"
                confirmation_bases.append(basis)
            elif any(token in pattern for token in SECONDARY_PATTERN_TOKENS):
                later_closes = pd.to_numeric(later["close"], errors="coerce") if not later.empty else pd.Series(dtype=float)
                price_matches = later_closes[later_closes > float(signal["high"])]
                price_confirmed = not price_matches.empty
                volume_confirmed = volume_ratio is not None and volume_ratio >= DEFAULT_VOLUME_RATIO
                if (price_confirmed or volume_confirmed) and not invalidated:
                    if pattern not in strict_patterns:
                        strict_patterns.append(pattern)
                    accepted_strict = True
                    if price_confirmed:
                        confirmation_bases.append(
                            f"{pattern}后续收盘{float(price_matches.iloc[0]):.2f}突破信号高点{float(signal['high']):.2f}，确认成立"
                        )
                    if volume_confirmed:
                        confirmation_bases.append(
                            f"{pattern}信号日量比{float(volume_ratio):.2f}≥{DEFAULT_VOLUME_RATIO:.1f}，放量确认"
                        )
                elif pattern not in waiting_patterns:
                    waiting_patterns.append(pattern)
                    waiting_bases.append(
                        f"{pattern}尚需后续收盘突破{float(signal['high']):.2f}或信号日量比达到{DEFAULT_VOLUME_RATIO:.1f}"
                    )
            if accepted_strict:
                strict_signal_dates.append(signal_date)
            if level is not None:
                key_levels.append(float(level))
                if accepted_strict:
                    strict_key_levels.append(float(level))
                details.append(f"{signal_date} {pattern}，关键位{level:.2f}")
            else:
                details.append(f"{signal_date} {pattern}")

    return {
        "recent_patterns": "；".join(recent_patterns),
        "strict_patterns": "；".join(strict_patterns),
        "waiting_patterns": "；".join(waiting_patterns),
        "reversal_confirmed": bool(strict_patterns),
        "key_level": strict_key_levels[-1] if strict_key_levels else (key_levels[-1] if key_levels else None),
        "pattern_details": "；".join(dict.fromkeys(details)),
        "signal_date": "；".join(dict.fromkeys(strict_signal_dates)),
        "confirmation_basis": "；".join(dict.fromkeys(confirmation_bases)),
        "waiting_basis": "；".join(dict.fromkeys(waiting_bases)),
    }


def derive_feature_requirements(
    filters: list[USStockFilter] | None = None,
    *,
    sort_by: str = "",
) -> FeatureRequirements:
    """Derive the smallest deterministic feature set needed by a request."""

    return_windows: set[int] = set()
    drawdown_windows: set[int] = set()
    volume_ratio_windows: set[int] = set()
    rsi_windows: set[int] = set()
    include_patterns = False
    metrics = [str(item.metric or "").strip() for item in (filters or [])]
    if sort_by:
        metrics.append(str(sort_by).strip())

    for metric in metrics:
        match = re.fullmatch(r"return_(\d{1,3})d_pct", metric)
        if match:
            return_windows.add(max(1, min(252, int(match.group(1)))))
            continue
        if metric == "return_pct":
            return_windows.add(DEFAULT_RETURN_WINDOW)
            continue
        match = re.fullmatch(r"max_drawdown_(\d{1,3})d_pct", metric)
        if match:
            drawdown_windows.add(max(2, min(252, int(match.group(1)))))
            continue
        if metric == "max_drawdown":
            drawdown_windows.add(DEFAULT_DRAWDOWN_WINDOW)
            continue
        match = re.fullmatch(r"volume_ratio_(\d{1,3})d", metric)
        if match:
            volume_ratio_windows.add(max(1, min(252, int(match.group(1)))))
            continue
        if metric == "volume_ratio":
            volume_ratio_windows.add(20)
            continue
        match = re.fullmatch(r"rsi(\d{1,3})", metric)
        if match:
            rsi_windows.add(max(2, min(252, int(match.group(1)))))
            continue
        if metric == "rsi":
            rsi_windows.add(14)
            continue
        if metric in {
            "pattern", "patterns_any", "strict_patterns", "recent_patterns",
            "waiting_patterns", "reversal_confirmed",
        }:
            include_patterns = True

    requested_windows = (
        return_windows | drawdown_windows | volume_ratio_windows | rsi_windows
    )
    minimum_history = max(90, max(requested_windows, default=0) + 1)
    return FeatureRequirements(
        return_windows=frozenset(return_windows),
        drawdown_windows=frozenset(drawdown_windows),
        volume_ratio_windows=frozenset(volume_ratio_windows),
        rsi_windows=frozenset(rsi_windows),
        include_patterns=include_patterns,
        minimum_history=minimum_history,
    )


def build_us_stock_feature_frame(
    stock_df: pd.DataFrame,
    option_metrics_history: pd.DataFrame | None = None,
    *,
    trade_date: Any = None,
    filters: list[USStockFilter] | None = None,
    sort_by: str = "",
    requirements: FeatureRequirements | None = None,
) -> tuple[pd.DataFrame, str]:
    normalize_started = time.perf_counter()
    work, warning = _normalize_stock_history(stock_df)
    if warning:
        return pd.DataFrame(), warning
    normalize_ms = (time.perf_counter() - normalize_started) * 1000.0
    latest = pd.to_datetime(trade_date, errors="coerce").normalize() if trade_date is not None else work["date"].max()
    if pd.isna(latest):
        return pd.DataFrame(), "无法识别美股筛选交易日"
    work = work[work["date"] <= latest]
    if requirements is None:
        requirements = (
            FeatureRequirements.legacy_full()
            if filters is None and not sort_by
            else derive_feature_requirements(filters, sort_by=sort_by)
        )
    min_bars = max(90, int(requirements.minimum_history))
    rows: list[dict[str, Any]] = []
    normalization_quality = dict(work.attrs.get("quality_stats") or {})
    history_eligible_symbols: list[str] = []
    excluded_anomaly_symbols: list[str] = []
    basic_started = time.perf_counter()
    pattern_seconds = 0.0

    for symbol, group in work.groupby("symbol"):
        retained_bars = max(min_bars + 1, 270 if requirements.include_patterns else min_bars + 1)
        group = group.sort_values("date").tail(retained_bars).reset_index(drop=True)
        if len(group) < min_bars or group.iloc[-1]["date"] != latest:
            continue
        history_eligible_symbols.append(str(symbol))
        relevant_history = group.tail(min_bars + 1)
        if bool(relevant_history.get("_unresolved_price_break", pd.Series(False, index=relevant_history.index)).fillna(False).any()):
            excluded_anomaly_symbols.append(str(symbol))
            continue
        latest_row = group.iloc[-1]
        close = float(latest_row["close"])
        row: dict[str, Any] = {
            "symbol": symbol,
            "trade_date": latest,
            "latest_price": close,
            "volume": _clean_float(latest_row.get("volume")),
            "avg_dollar_volume_20d": float((group.iloc[-20:]["close"] * group.iloc[-20:]["volume"]).mean()),
        }
        for window in requirements.return_windows:
            if len(group) > window and float(group.iloc[-window - 1]["close"]) > 0:
                row[f"return_{window}d_pct"] = (close / float(group.iloc[-window - 1]["close"]) - 1.0) * 100.0
            else:
                row[f"return_{window}d_pct"] = None
        for window in requirements.drawdown_windows:
            row[f"max_drawdown_{window}d_pct"] = _max_drawdown_pct(group.tail(window + 1)["close"])
        for window in requirements.volume_ratio_windows:
            prior_volume = pd.to_numeric(group.iloc[-window - 1:-1]["volume"], errors="coerce").dropna()
            row[f"volume_ratio_{window}d"] = (
                float(latest_row["volume"]) / float(prior_volume.mean())
                if not prior_volume.empty and float(prior_volume.mean()) > 0
                else None
            )
        for window in requirements.rsi_windows:
            row[f"rsi{window}"] = _rsi(group["close"], window)
        if requirements.include_patterns:
            pattern_started = time.perf_counter()
            row.update(_pattern_state(group))
            pattern_seconds += time.perf_counter() - pattern_started
        rows.append(row)

    features = pd.DataFrame(rows)
    basic_ms = max(0.0, (time.perf_counter() - basic_started - pattern_seconds) * 1000.0)
    pattern_ms = pattern_seconds * 1000.0
    quality_stats = {
        **normalization_quality,
        "price_history_rows": len(history_eligible_symbols),
        "price_anomaly_symbols": sorted(set(excluded_anomaly_symbols)),
        "price_anomaly_rows": len(set(excluded_anomaly_symbols)),
        "split_adjusted_symbols": sorted(
            set(normalization_quality.get("split_adjusted_symbols") or []) & set(history_eligible_symbols)
        ),
    }
    features.attrs["quality_stats"] = quality_stats
    features.attrs["stage_timings"] = {
        "normalize_ms": normalize_ms,
        "feature_basic_ms": basic_ms,
        "feature_pattern_ms": pattern_ms,
        "option_feature_ms": 0.0,
    }
    if features.empty:
        return features, "没有具备足够历史长度且更新到筛选日的美股"

    option_started = time.perf_counter()
    if option_metrics_history is not None and not option_metrics_history.empty:
        option = option_metrics_history.copy()
        option["trade_date"] = pd.to_datetime(option["trade_date"].astype(str), errors="coerce").dt.normalize()
        option["underlying"] = option["underlying"].astype(str).str.upper().str.strip()
        option["atm_iv_pct"] = pd.to_numeric(option.get("atm_iv_pct"), errors="coerce")
        option = option[option["trade_date"] <= latest].sort_values(["underlying", "trade_date"])
        option_rows: list[dict[str, Any]] = []
        for underlying, history in option.groupby("underlying"):
            current_rows = history[history["trade_date"] == latest]
            if current_rows.empty:
                continue
            current = current_rows.iloc[-1]
            current_iv = _clean_float(current.get("atm_iv_pct"))
            series = pd.to_numeric(history["atm_iv_pct"], errors="coerce").dropna().tail(252)
            iv_rank = None
            iv_percentile = None
            if current_iv is not None and len(series) >= DEFAULT_IV_MIN_SAMPLES:
                min_iv = float(series.min())
                max_iv = float(series.max())
                iv_rank = None if math.isclose(min_iv, max_iv) else (current_iv - min_iv) / (max_iv - min_iv) * 100.0
                iv_percentile = float((series <= current_iv).sum() / len(series) * 100.0)
            option_rows.append(
                {
                    "symbol": underlying,
                    "atm_iv_pct": current_iv,
                    "iv_rank": iv_rank,
                    "iv_percentile": iv_percentile,
                    "iv_history_samples": int(len(series)),
                    "iv_change_1d": _clean_float(current.get("iv_change_1d")),
                    "iv_rv20_spread": _clean_float(current.get("iv_rv20_spread")),
                    "option_total_volume": _clean_float(current.get("total_volume")),
                }
            )
        if option_rows:
            features = features.merge(pd.DataFrame(option_rows), how="left", on="symbol")
            features.attrs["quality_stats"] = quality_stats
    option_ms = (time.perf_counter() - option_started) * 1000.0
    features.attrs["quality_stats"] = quality_stats
    features.attrs["stage_timings"] = {
        "normalize_ms": normalize_ms,
        "feature_basic_ms": basic_ms,
        "feature_pattern_ms": pattern_ms,
        "option_feature_ms": option_ms,
    }
    return features, ""


def _matches_pattern(value: Any, requested: Any) -> bool:
    text_value = str(value or "")
    candidates = requested if isinstance(requested, list) else [requested]
    return any(str(token) in text_value for token in candidates if str(token))


def _condition_result(row: pd.Series, condition: USStockFilter) -> tuple[bool, bool]:
    metric = str(condition.metric or "")
    if metric == "patterns_any":
        value = row.get("strict_patterns") or row.get("recent_patterns")
        return _matches_pattern(value, condition.value), False
    value = row.get(metric)
    if value is None or (not isinstance(value, bool) and pd.isna(value)):
        return False, True
    operator = str(condition.operator or "gte")
    if operator == "eq":
        return bool(value) == bool(condition.value), False
    numeric = _clean_float(value)
    target = _clean_float(condition.value)
    if numeric is None or target is None:
        return False, True
    if operator == "gte":
        return numeric >= target, False
    if operator == "lte":
        return numeric <= target, False
    if operator == "abs_gte":
        return abs(numeric) >= abs(target), False
    if operator == "between":
        target2 = _clean_float(condition.value2)
        return (target2 is not None and min(target, target2) <= numeric <= max(target, target2)), target2 is None
    return False, True


def _condition_text(condition: USStockFilter) -> str:
    label = condition.label or condition.metric
    if condition.operator == "eq":
        return f"{label}＝{'是' if bool(condition.value) else '否'}"
    if condition.operator == "contains_any":
        values = condition.value if isinstance(condition.value, list) else [condition.value]
        return f"{label}包含任一：{' / '.join(map(str, values))}"
    symbols = {"gte": "≥", "lte": "≤", "abs_gte": "绝对值≥", "between": "介于"}
    if condition.operator == "between":
        return f"{label}{symbols['between']}{condition.value}～{condition.value2}"
    suffix = "%" if any(token in condition.metric for token in ("pct", "percentile", "iv_rank")) else ""
    return f"{label}{symbols.get(condition.operator, condition.operator)}{condition.value}{suffix}"


def _expression_text(expression: ScreenExpression | None) -> str:
    if expression is None:
        return ""
    if expression.kind == "condition":
        return _condition_text(
            USStockFilter(
                metric=expression.metric,
                operator=expression.operator,
                value=expression.value,
                value2=expression.value2,
                label=expression.label or expression.metric,
            )
        )
    parts = [_expression_text(item) for item in expression.clauses]
    parts = [item for item in parts if item]
    if expression.kind == "not":
        return f"非（{parts[0]}）" if parts else ""
    connector = " 且 " if expression.kind == "all" else " 或 "
    return f"（{connector.join(parts)}）"


def _condition_relevance(row: pd.Series, condition: USStockFilter) -> float:
    """Score only the margin by which the user's requested condition is met."""

    passed, missing = _condition_result(row, condition)
    if missing or not passed:
        return 0.0
    if condition.metric in {"patterns_any", "reversal_confirmed"} or condition.operator in {"eq", "contains_any"}:
        return 1.0

    numeric = _clean_float(row.get(condition.metric))
    target = _clean_float(condition.value)
    if numeric is None or target is None:
        return 0.0
    denominator = max(abs(target), 1.0)
    if condition.operator == "gte":
        margin = (numeric - target) / denominator
    elif condition.operator == "lte":
        margin = (target - numeric) / denominator
    elif condition.operator == "abs_gte":
        margin = (abs(numeric) - abs(target)) / denominator
    elif condition.operator == "between":
        target2 = _clean_float(condition.value2)
        if target2 is None:
            return 0.0
        low, high = sorted((target, target2))
        denominator = max(abs(low), abs(high), 1.0)
        margin = min(numeric - low, high - numeric) / denominator
    else:
        return 1.0
    return float(max(0.0, min(3.0, margin)))


def _expression_relevance(row: pd.Series, expression: ScreenExpression) -> float:
    if expression.kind == "condition":
        return _condition_relevance(
            row,
            USStockFilter(
                metric=expression.metric,
                operator=expression.operator,
                value=expression.value,
                value2=expression.value2,
                label=expression.label or expression.metric,
            ),
        )
    child_scores = [_expression_relevance(row, child) for child in expression.clauses]
    if not child_scores:
        return 0.0
    if expression.kind == "all":
        return float(sum(child_scores) / len(child_scores))
    if expression.kind == "any":
        return float(max(child_scores))
    if expression.kind == "not":
        passed, _, _, _ = _expression_result(row, expression)
        return 1.0 if passed else 0.0
    return 0.0


def _screen_relevance_score(row: pd.Series, parsed: ParsedScreenRequest) -> float:
    if parsed.where is not None:
        return _expression_relevance(row, parsed.where)
    scores = [_condition_relevance(row, condition) for condition in parsed.filters]
    if not scores:
        return 0.0
    return float(max(scores) if parsed.logic == "or" else sum(scores) / len(scores))


def _expression_result(row: pd.Series, expression: ScreenExpression) -> tuple[bool, int, list[str], list[str]]:
    if expression.kind == "condition":
        condition = USStockFilter(
            metric=expression.metric,
            operator=expression.operator,
            value=expression.value,
            value2=expression.value2,
            label=expression.label or expression.metric,
        )
        passed, missing = _condition_result(row, condition)
        label = condition.label or condition.metric
        return passed, int(missing), ([] if passed else [label]), ([label] if passed else [])

    child_results = [_expression_result(row, child) for child in expression.clauses]
    if expression.kind == "all":
        passed = all(item[0] for item in child_results)
        failures = [label for item in child_results for label in item[2]]
        matches = [label for item in child_results for label in item[3]]
        return passed, sum(item[1] for item in child_results), failures, matches
    if expression.kind == "any":
        passed_children = [item for item in child_results if item[0]]
        if passed_children:
            matches = [label for item in passed_children for label in item[3]]
            return True, 0, [], matches
        labels = [label for item in child_results for label in item[2]]
        group_label = "任一满足（" + " / ".join(dict.fromkeys(labels)) + "）"
        return False, min((item[1] for item in child_results), default=0), [group_label], []
    if expression.kind == "not":
        child = child_results[0] if child_results else (False, 0, ["空NOT条件"], [])
        return (not child[0]), child[1], ([] if not child[0] else ["不满足否定条件"]), ([] if child[0] else ["否定条件"])
    return False, 1, ["未知逻辑表达式"], []


def evaluate_us_stock_screen(features: pd.DataFrame, parsed: ParsedScreenRequest) -> dict[str, Any]:
    quality_stats = dict(getattr(features, "attrs", {}).get("quality_stats") or {}) if features is not None else {}
    if features is None or features.empty:
        return {
            "strict": pd.DataFrame(), "waiting": pd.DataFrame(), "near": pd.DataFrame(),
            "missing_rows": 0, "feature_rows": 0, "strict_total": 0, "displayed_count": 0,
            **quality_stats,
        }
    work = features.copy()
    feature_rows = len(work)
    samples = pd.to_numeric(work.get("iv_history_samples", pd.Series(index=work.index, dtype=float)), errors="coerce")
    atm_iv = pd.to_numeric(work.get("atm_iv_pct", pd.Series(index=work.index, dtype=float)), errors="coerce")
    no_iv_record_rows = int(samples.isna().sum())
    current_iv_missing_rows = int((samples.notna() & atm_iv.isna()).sum())
    insufficient_iv_rows = int((samples.notna() & (samples < DEFAULT_IV_MIN_SAMPLES)).sum())
    insufficient_valid_iv_rows = int((samples.notna() & atm_iv.notna() & (samples < DEFAULT_IV_MIN_SAMPLES)).sum())
    requested_iv_columns = [
        item.metric for item in parsed.filters
        if str(item.metric).startswith(("iv_", "atm_iv", "option_"))
    ]
    if requested_iv_columns:
        iv_value_available = pd.Series(False, index=work.index)
        for column in requested_iv_columns:
            if column in work.columns:
                iv_value_available |= pd.to_numeric(work[column], errors="coerce").notna()
    else:
        iv_value_available = atm_iv.notna()
    iv_eligible_mask = samples.ge(DEFAULT_IV_MIN_SAMPLES) & iv_value_available
    iv_eligible_rows = int(iv_eligible_mask.sum())
    if parsed.universe == "option_covered":
        before = len(work)
        work = work[iv_eligible_mask]
        missing_rows = before - len(work)
    else:
        missing_rows = 0

    pass_flags: list[bool] = []
    fail_counts: list[int] = []
    missing_counts: list[int] = []
    fail_labels: list[str] = []
    matched_labels: list[str] = []
    for _, row in work.iterrows():
        if parsed.where is not None:
            row_pass, missing_count, failures, matches = _expression_result(row, parsed.where)
        else:
            results = [_condition_result(row, condition) for condition in parsed.filters]
            passes = [item[0] for item in results]
            missing = [item[1] for item in results]
            row_pass = any(passes) if parsed.logic == "or" else all(passes) if passes else False
            failures = [parsed.filters[idx].label or parsed.filters[idx].metric for idx, passed in enumerate(passes) if not passed]
            matches = [parsed.filters[idx].label or parsed.filters[idx].metric for idx, passed in enumerate(passes) if passed]
            missing_count = sum(1 for item in missing if item)
        pass_flags.append(row_pass)
        fail_counts.append(len(failures))
        missing_counts.append(missing_count)
        fail_labels.append("；".join(failures))
        matched_labels.append("；".join(dict.fromkeys(matches)))

    work["_pass"] = pass_flags
    work["_fail_count"] = fail_counts
    work["_missing_count"] = missing_counts
    work["未满足条件"] = fail_labels
    work["命中条件"] = matched_labels
    work["_screen_score"] = [
        _screen_relevance_score(row, parsed) for _, row in work.iterrows()
    ]
    strict_all = work.loc[work["_pass"].astype(bool)].copy()
    strict_total = len(strict_all)
    if parsed.sort_by and parsed.sort_by in strict_all.columns:
        strict_all = strict_all.sort_values(parsed.sort_by, ascending=parsed.sort_order == "asc", na_position="last")
    else:
        strict_all = strict_all.sort_values("_screen_score", ascending=False)
    strict = strict_all.head(parsed.limit)

    non_reversal = [item for item in parsed.filters if item.metric not in {"reversal_confirmed", "patterns_any"}]
    waiting_mask = []
    for _, row in work.iterrows():
        passes_other = all(_condition_result(row, item)[0] for item in non_reversal) if non_reversal else True
        waiting_mask.append(bool(passes_other and str(row.get("waiting_patterns") or "")))
    waiting = work[pd.Series(waiting_mask, index=work.index) & ~work["_pass"]].sort_values("_screen_score", ascending=False).head(3)
    near = pd.DataFrame()
    if strict_total == 0:
        near = work[(work["_fail_count"] == 1) & (work["_missing_count"] == 0)].sort_values("_screen_score", ascending=False).head(3)
    return {
        "strict": strict,
        "waiting": waiting,
        "near": near,
        "missing_rows": missing_rows,
        "feature_rows": feature_rows,
        "evaluated_rows": len(work),
        "strict_total": strict_total,
        "displayed_count": len(strict),
        "no_iv_record_rows": no_iv_record_rows,
        "current_iv_missing_rows": current_iv_missing_rows,
        "insufficient_iv_rows": insufficient_iv_rows,
        "insufficient_valid_iv_rows": insufficient_valid_iv_rows,
        "iv_eligible_rows": iv_eligible_rows,
        **quality_stats,
    }


def _display_metric_label(metric: str) -> str:
    labels = {
        "latest_price": "最新价",
        "volume": "成交量",
        "atm_iv_pct": "ATM IV",
        "iv_rank": "IV Rank",
        "iv_percentile": "IV Percentile",
    }
    if metric in labels:
        return labels[metric]
    match = re.fullmatch(r"return_(\d+)d_pct", metric)
    if match:
        return f"{match.group(1)}日涨跌幅"
    match = re.fullmatch(r"max_drawdown_(\d+)d_pct", metric)
    if match:
        return f"{match.group(1)}日最大回撤"
    match = re.fullmatch(r"volume_ratio_(\d+)d", metric)
    if match:
        return f"{match.group(1)}日量比"
    match = re.fullmatch(r"rsi(\d+)", metric)
    if match:
        return f"RSI{match.group(1)}"
    return metric


def _display_metric_value(metric: str, value: Any) -> str:
    if value is None or (not isinstance(value, (list, tuple, dict)) and pd.isna(value)):
        return "N/A"
    number = _clean_float(value)
    if number is None:
        return str(value)
    if metric in {"atm_iv_pct", "iv_rank", "iv_percentile"} or metric.endswith("_pct"):
        return f"{number:.1f}%"
    if re.fullmatch(r"volume_ratio_\d+d", metric):
        return f"{number:.2f}x"
    if metric == "volume":
        return f"{number:,.0f}"
    if re.fullmatch(r"rsi\d+", metric):
        return f"{number:.1f}"
    return f"{number:.2f}"


def _requested_display_metrics(parsed: ParsedScreenRequest | None, df: pd.DataFrame) -> list[str]:
    hidden_metrics = {"patterns_any", "reversal_confirmed", "strict_patterns", "waiting_patterns"}
    metrics: list[str] = []
    for condition in parsed.filters if parsed else []:
        metric = condition.metric
        if metric in hidden_metrics or metric == "latest_price" or metric not in df.columns:
            continue
        metrics.append(metric)
    return list(dict.fromkeys(metrics))


def _display_candidates(
    df: pd.DataFrame,
    *,
    parsed: ParsedScreenRequest | None = None,
    include_failure: bool = False,
) -> str:
    if df is None or df.empty:
        return ""
    metrics = _requested_display_metrics(parsed, df)
    show_pattern = any(
        condition.metric in {"patterns_any", "reversal_confirmed"}
        for condition in (parsed.filters if parsed else [])
    )
    explicit_pattern_filter = any(
        condition.metric == "patterns_any" for condition in (parsed.filters if parsed else [])
    )
    cards: list[str] = []
    for index, (_, row) in enumerate(df.iterrows(), start=1):
        symbol = str(row.get("symbol") or "").strip().upper()
        if symbol and not symbol.endswith(".US"):
            symbol += ".US"
        latest = _display_metric_value("latest_price", row.get("latest_price"))
        card = [f"{index}. **{symbol or 'N/A'}** · 现价 {latest}"]

        metric_parts = [
            f"{_display_metric_label(metric)} {_display_metric_value(metric, row.get(metric))}"
            for metric in metrics
        ]
        if metric_parts:
            card.append(f"   - 指标：{'｜'.join(metric_parts)}")

        if show_pattern:
            pattern = str(row.get("strict_patterns") or row.get("recent_patterns") or "").strip()
            if pattern:
                pattern_label = "命中形态" if explicit_pattern_filter else "已确认形态"
                card.append(f"   - {pattern_label}：{pattern}")
            signal_parts: list[str] = []
            signal_date = str(row.get("signal_date") or "").strip()
            if signal_date:
                signal_parts.append(f"信号日 {signal_date}")
            key_level = row.get("key_level")
            if key_level is not None and not pd.isna(key_level):
                signal_parts.append(f"关键位 {_display_metric_value('latest_price', key_level)}")
            if signal_parts:
                card.append(f"   - {'｜'.join(signal_parts)}")

        if include_failure:
            failure = str(row.get("未满足条件") or "").strip()
            if failure:
                card.append(f"   - 未满足：{failure}")
        cards.append("\n".join(card))
    return "\n\n".join(cards)


def format_us_stock_screen_result(
    parsed: ParsedScreenRequest,
    evaluated: dict[str, Any],
    *,
    trade_date: Any,
    source_universe_count: int,
) -> str:
    date_text = pd.to_datetime(trade_date).strftime("%Y-%m-%d")
    conditions = _expression_text(parsed.where) or "；".join(_condition_text(item) for item in parsed.filters) or "未识别到可执行条件"
    lines = [
        "【美股多维筛选】",
        f"- 数据日期：{date_text}（美股日线 EOD）",
        "- 数据覆盖："
        f"原始{source_universe_count}只 → 价格历史充足{evaluated.get('price_history_rows', evaluated.get('feature_rows', 0))}只 → "
        f"实际参与{evaluated.get('evaluated_rows', 0)}只",
        f"- 严格条件：{conditions}",
    ]
    anomaly_symbols = list(evaluated.get("price_anomaly_symbols") or [])
    split_adjusted_symbols = list(evaluated.get("split_adjusted_symbols") or [])
    if anomaly_symbols or split_adjusted_symbols:
        quality_parts = [f"可信拆股因子修正{len(split_adjusted_symbols)}只"]
        if anomaly_symbols:
            symbol_text = "、".join(f"{item}.US" for item in anomaly_symbols[:5])
            if len(anomaly_symbols) > 5:
                symbol_text += "等"
            quality_parts.append(f"未确认价格断层排除{len(anomaly_symbols)}只（{symbol_text}）")
        lines.append(f"- 复权质量：{'；'.join(quality_parts)}")
    if parsed.requires_iv:
        lines.extend([
            f"- IV覆盖：可计算{evaluated.get('iv_eligible_rows', 0)}只，排除{evaluated.get('missing_rows', 0)}只",
            f"  - 无同期记录：{evaluated.get('no_iv_record_rows', 0)}只",
            f"  - 当日IV缺失：{evaluated.get('current_iv_missing_rows', 0)}只",
            f"  - 有当日IV但有效历史不足{DEFAULT_IV_MIN_SAMPLES}日：{evaluated.get('insufficient_valid_iv_rows', 0)}只",
        ])
    if parsed.defaults:
        lines.append("- 本轮采用默认口径：")
        lines.extend(f"  - {item}" for item in dict.fromkeys(parsed.defaults))
    if parsed.unsupported:
        lines.append(f"- 未执行条件：{'、'.join(dict.fromkeys(parsed.unsupported))}（当前本地美股筛选暂不支持，未静默忽略）")

    strict = evaluated.get("strict", pd.DataFrame())
    near = evaluated.get("near", pd.DataFrame())
    if strict is not None and not strict.empty:
        strict_total = int(evaluated.get("strict_total", len(strict)))
        displayed = int(evaluated.get("displayed_count", len(strict)))
        conclusion = f"\n结论：严格命中{strict_total}只候选"
        if displayed < strict_total:
            conclusion += f"，展示前{displayed}只"
        lines.extend([conclusion + "。", "", _display_candidates(strict, parsed=parsed)])
    else:
        lines.append("\n结论：严格条件交集为零，没有自动降低阈值。")
        if near is not None and not near.empty:
            lines.extend(["\n仅差一项的近似候选（最多3只）：", "", _display_candidates(near, parsed=parsed, include_failure=True)])
    lines.append("\n- 提醒：结果是确定性条件筛选，不是直接买入指令；盘中跳空、财报和期权流动性仍需另行确认。")
    return "\n".join(lines)


def _db_engine():
    values = [os.getenv("DB_USER"), os.getenv("DB_PASSWORD"), os.getenv("DB_HOST"), os.getenv("DB_NAME")]
    if not all(values):
        return None
    port = os.getenv("DB_PORT") or "3306"
    url = f"mysql+pymysql://{values[0]}:{values[1]}@{values[2]}:{port}/{values[3]}"
    return create_engine(url, pool_pre_ping=True, pool_recycle=3600)


def _load_screen_frames(engine, parsed: ParsedScreenRequest) -> tuple[pd.DataFrame, pd.DataFrame, pd.Timestamp | None, str]:
    try:
        with engine.connect() as conn:
            columns = {str(row[0]) for row in conn.execute(text("SHOW COLUMNS FROM stock_prices")).fetchall()}
            stock_latest = conn.execute(text("SELECT MAX(date) FROM stock_prices")).scalar()
        if not stock_latest:
            return pd.DataFrame(), pd.DataFrame(), None, "stock_prices没有最新交易日"
        select_adj = ", adjClose" if "adjClose" in columns else ""
        start_date = pd.to_datetime(stock_latest) - pd.Timedelta(days=550)
        stock_sql = text(
            f"""
            SELECT date, UPPER(symbol) AS symbol, open, high, low, close, volume{select_adj}
            FROM stock_prices
            WHERE date >= :start_date AND date <= :end_date
            ORDER BY symbol, date
            """
        )
        with engine.connect() as conn:
            stock_df = pd.read_sql(stock_sql, conn, params={"start_date": start_date.date(), "end_date": stock_latest})

        option_df = pd.DataFrame()
        effective_date = pd.to_datetime(stock_latest).normalize()
        if parsed.requires_iv or parsed.universe == "option_covered":
            option_start = (effective_date - pd.Timedelta(days=550)).strftime("%Y%m%d")
            option_sql = text(
                """
                SELECT trade_date, UPPER(underlying) AS underlying, atm_iv_pct, iv_change_1d,
                       iv_rv20_spread, total_volume
                FROM us_option_market_metrics_daily
                WHERE trade_date >= :start_date
                ORDER BY underlying, trade_date
                """
            )
            with engine.connect() as conn:
                option_df = pd.read_sql(option_sql, conn, params={"start_date": option_start})
            if option_df.empty:
                return stock_df, option_df, None, "期权日度指标表没有可用数据"
            stock_dates = set(pd.to_datetime(stock_df["date"], errors="coerce").dropna().dt.normalize())
            option_dates = set(pd.to_datetime(option_df["trade_date"].astype(str), errors="coerce").dropna().dt.normalize())
            common_dates = stock_dates & option_dates
            if not common_dates:
                return stock_df, option_df, None, "美股价格与期权指标没有共同交易日"
            effective_date = max(common_dates)
            stock_df = stock_df[pd.to_datetime(stock_df["date"], errors="coerce").dt.normalize() <= effective_date]
            option_df = option_df[pd.to_datetime(option_df["trade_date"].astype(str), errors="coerce").dt.normalize() <= effective_date]
        return stock_df, option_df, effective_date, ""
    except Exception as exc:
        return pd.DataFrame(), pd.DataFrame(), None, f"加载美股筛选数据失败：{exc}"


def run_us_stock_screen(
    *,
    query: str = "",
    filters: list[USStockFilter] | None = None,
    logic: str = "and",
    universe: str = "auto",
    sort_by: str = "",
    sort_order: str = "desc",
    limit: int = 10,
    plan: ScreenPlan | dict[str, Any] | None = None,
    engine=None,
) -> str:
    screen_started = time.perf_counter()
    if plan is not None:
        try:
            parsed = validate_screen_plan(plan, query=query)
        except Exception as exc:
            return f"【美股多维筛选】\n结论：结构化筛选计划校验失败。\n- 原因：{exc}"
    else:
        parsed = parse_us_stock_screen_query(
            query,
            filters=filters,
            logic=logic,
            universe=universe,
            sort_by=sort_by,
            sort_order=sort_order,
            limit=limit,
        )
        parsed.parser_mode = "rules"
        parsed.ambiguous.extend(_fallback_coverage_errors(query, parsed))
    if parsed.unsupported or parsed.ambiguous:
        recognized = _expression_text(parsed.where) or "；".join(_condition_text(item) for item in parsed.filters) or "无"
        unresolved = list(dict.fromkeys([*parsed.unsupported, *parsed.ambiguous]))
        return (
            "【美股多维筛选】\n"
            "结论：当前问题无法完整、无歧义地执行，因此没有返回部分筛选结果。\n"
            f"- 已识别条件：{recognized}\n"
            f"- 未执行或需澄清：{'；'.join(unresolved)}\n"
            "- 建议：请明确指标、窗口、比较方向和阈值；系统不会静默忽略任何必要条件。"
        )
    if not parsed.filters:
        unsupported = f"；未支持：{'、'.join(parsed.unsupported)}" if parsed.unsupported else ""
        return (
            "【美股多维筛选】\n结论：未识别到可执行筛选条件。\n"
            "- 当前支持：价格/1-252日涨跌幅/最大回撤、K线止跌形态、成交量与量比、ATM IV、IV Rank、IV Percentile。"
            f"{unsupported}"
        )
    engine = engine or _db_engine()
    if engine is None:
        return "【美股多维筛选】\n结论：数据不足\n- 原因：数据库连接配置不完整。"
    load_started = time.perf_counter()
    stock_df, option_df, trade_date, warning = _load_screen_frames(engine, parsed)
    db_load_ms = (time.perf_counter() - load_started) * 1000.0
    if warning or trade_date is None:
        return f"【美股多维筛选】\n结论：数据不足\n- 原因：{warning}"
    requirements = derive_feature_requirements(parsed.filters, sort_by=parsed.sort_by)
    features, feature_warning = build_us_stock_feature_frame(
        stock_df,
        option_df,
        trade_date=trade_date,
        filters=parsed.filters,
        sort_by=parsed.sort_by,
        requirements=requirements,
    )
    if feature_warning:
        return f"【美股多维筛选】\n结论：数据不足\n- 原因：{feature_warning}"
    source_count = int(stock_df[pd.to_datetime(stock_df["date"], errors="coerce").dt.normalize() == trade_date]["symbol"].nunique())
    evaluate_started = time.perf_counter()
    evaluated = evaluate_us_stock_screen(features, parsed)
    evaluate_ms = (time.perf_counter() - evaluate_started) * 1000.0
    feature_timings = dict(features.attrs.get("stage_timings") or {})
    print(
        "[USStockScreen] stages "
        f"parser_mode={parsed.parser_mode} db_load_ms={db_load_ms:.0f} "
        f"stock_rows={len(stock_df)} symbols={source_count} "
        f"feature_basic_ms={feature_timings.get('feature_basic_ms', 0.0):.0f} "
        f"feature_pattern_ms={feature_timings.get('feature_pattern_ms', 0.0):.0f} "
        f"option_feature_ms={feature_timings.get('option_feature_ms', 0.0):.0f} "
        f"evaluate_ms={evaluate_ms:.0f} "
        f"screen_total_ms={(time.perf_counter() - screen_started) * 1000.0:.0f}"
    )
    return format_us_stock_screen_result(parsed, evaluated, trade_date=trade_date, source_universe_count=source_count)


@tool(args_schema=USStockScreenInput)
def screen_us_stocks(
    query: str = "",
    filters: list[USStockFilter] | None = None,
    logic: str = "and",
    universe: str = "auto",
    sort_by: str = "",
    sort_order: str = "desc",
    limit: int = 10,
    plan: ScreenPlan | None = None,
) -> str:
    """按价格、涨跌幅、K线转折、成交量和期权波动率确定性筛选本地美股池。"""
    return run_us_stock_screen(
        query=query,
        filters=filters,
        logic=logic,
        universe=universe,
        sort_by=sort_by,
        sort_order=sort_order,
        limit=limit,
        plan=plan,
    )
