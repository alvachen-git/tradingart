from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class OptionStrategyPolicy:
    risk_key: str
    risk_label: str
    trend_regime: str
    iv_tier: str
    dte_tier: str
    hard_rules: List[str]
    preferred_strategies: List[str]
    avoid_strategies: List[str]
    required_checks: List[str]

    def as_prompt_context(self) -> str:
        lines = [
            "【个性化期权策略规则】",
            f"- 风险层级：{self.risk_label}",
            f"- 行情状态：{_TREND_LABELS.get(self.trend_regime, self.trend_regime)}",
            f"- IV分层：{_IV_LABELS.get(self.iv_tier, self.iv_tier)}",
            f"- DTE分层：{_DTE_LABELS.get(self.dte_tier, self.dte_tier)}",
        ]
        if self.required_checks:
            lines.append("- 按需检查：" + "；".join(self.required_checks))
        if self.hard_rules:
            lines.append("- 硬规则：" + "；".join(self.hard_rules))
        if self.preferred_strategies:
            lines.append("- 优先考虑：" + "；".join(self.preferred_strategies))
        if self.avoid_strategies:
            lines.append("- 不适合/禁止：" + "；".join(self.avoid_strategies))
        lines.append("- 输出时必须说明首选策略、为什么适合、不适合策略、触发/失效条件。")
        return "\n".join(lines)


_TREND_LABELS = {
    "range": "震荡/无明显趋势",
    "breakout": "突破/加速",
    "trend_slow": "有趋势但速度不足",
    "trend": "趋势行情",
    "unknown": "未确认",
}

_IV_LABELS = {
    "ultra_low": "极低（IV Rank <20）",
    "low": "偏低（20<=IV Rank<30）",
    "neutral": "中性（30<=IV Rank<=70）",
    "high": "偏高（IV Rank>70）",
    "unknown": "未知",
}

_DTE_LABELS = {
    "expiry": "末日（0-3天）",
    "ultra_short": "超短（4-7天）",
    "near": "近期（8-20天）",
    "regular": "常规（21-45天）",
    "far": "远期（>45天）",
    "unknown": "未知",
}


def normalize_option_risk_preference(risk_preference: Any, profile_context: str = "", user_query: str = "") -> str:
    # 优先级：当前问题明确表达 > 结构化画像 > 旧 profile 字段。
    for part in [user_query, profile_context, risk_preference]:
        risk = _risk_from_text(str(part or ""))
        if risk:
            return risk
    return "balanced"


def classify_iv_rank(iv_rank: Optional[float]) -> str:
    if iv_rank is None:
        return "unknown"
    value = float(iv_rank)
    if value < 20:
        return "ultra_low"
    if value < 30:
        return "low"
    if value > 70:
        return "high"
    return "neutral"


def classify_dte(days_to_expiry: Optional[int]) -> str:
    if days_to_expiry is None:
        return "unknown"
    days = int(days_to_expiry)
    if days <= 3:
        return "expiry"
    if days <= 7:
        return "ultra_short"
    if days <= 20:
        return "near"
    if days <= 45:
        return "regular"
    return "far"


def classify_trend_regime(user_query: str = "", trend_signal: str = "", technical_summary: str = "") -> str:
    text = f"{user_query} {trend_signal} {technical_summary}".lower()
    if any(k in text for k in ["震荡", "横盘", "区间", "无明显趋势", "没有明显趋势", "没有突破", "没突破", "neutral", "range"]):
        return "range"
    if any(k in text for k in ["突破", "放量上破", "跌破", "加速", "breakout"]):
        return "breakout"
    if any(k in text for k in ["趋势有但", "有趋势但没速度", "趋势没速度", "慢涨", "慢跌"]):
        return "trend_slow"
    if any(k in text for k in ["看涨", "看跌", "多头", "空头", "上涨", "下跌", "bull", "bear"]):
        return "trend"
    return "unknown"


def build_option_strategy_policy(
    *,
    risk_preference: Any = "",
    profile_context: str = "",
    user_query: str = "",
    trend_signal: str = "",
    technical_summary: str = "",
    iv_rank: Optional[float] = None,
    days_to_expiry: Optional[int] = None,
) -> OptionStrategyPolicy:
    risk_key = normalize_option_risk_preference(
        risk_preference,
        profile_context=profile_context,
        user_query=user_query,
    )
    risk_label = {
        "conservative": "偏保守",
        "balanced": "稳健/平衡",
        "aggressive": "偏积极/激进",
    }[risk_key]
    trend_regime = classify_trend_regime(user_query, trend_signal, technical_summary)
    iv_tier = classify_iv_rank(iv_rank)
    if iv_tier == "unknown":
        iv_tier = _infer_iv_tier_from_text(user_query, technical_summary)
    dte_tier = classify_dte(days_to_expiry)
    if dte_tier == "unknown":
        dte_tier = _infer_dte_tier_from_text(user_query, technical_summary)
    hard_rules: List[str] = ["当前问题明确表达优先于长期画像；交易安全规则优先于用户偏好"]
    preferred: List[str] = []
    avoid: List[str] = []
    checks = ["具体策略建议前按需确认 IV Rank、距离到期日、标的现价；不做普通问答预检索"]

    if risk_key == "conservative":
        hard_rules.append("偏保守默认禁止买末日期权，除非用户本轮明确接受高风险")
        hard_rules.append("偏保守卖期权必须偏虚值，优先风险有限结构")
        avoid.extend(["裸卖平值/实值期权", "单腿追买高IV期权"])
        preferred.extend(["牛市/熊市价差", "保护性组合", "备兑或有保护的卖方"])
    elif risk_key == "aggressive":
        preferred.extend(["趋势突破买期权", "短DTE顺势策略", "轻仓高赔率虚值买方"])
        hard_rules.append("偏积极/激进也必须保留仓位、止损和胜率风险提示")
    else:
        preferred.extend(["价差策略", "顺势卖虚值期权", "买平值或轻度实值期权"])

    if trend_regime == "range":
        if risk_key == "conservative" and iv_tier == "ultra_low":
            avoid.append("震荡但IV Rank低于20时，不建议偏保守客户做双卖")
        else:
            preferred.append("震荡行情可考虑双卖")
            if risk_key == "conservative":
                hard_rules.append("震荡双卖只允许偏虚值")
            elif risk_key == "aggressive":
                hard_rules.append("震荡双卖可考虑浅虚值，但必须控制保证金和突破止损")
    elif trend_regime == "breakout":
        if risk_key == "aggressive" and dte_tier == "far":
            preferred.append("突破且DTE较远时，激进客户可用方向性买方或高凸性买方作为卫星仓")
            hard_rules.append("高凸性买方必须提示失效快、胜率低、仓位要轻")
            hard_rules.append("用户明确询问方向性买方策略时，必须正面回答可以/不可以；若可以，不得只用价差策略替代回答")
        elif risk_key == "conservative":
            preferred.append("突破行情偏保守优先用价差跟随")

    if dte_tier in {"expiry", "ultra_short"}:
        if risk_key == "conservative":
            avoid.append("买末日/超短期期权")
        else:
            hard_rules.append("DTE少于7天只有趋势明确时才允许激进策略")
    if dte_tier == "far":
        if iv_tier == "high":
            preferred.append("DTE大于45天且IV高时，优先顺势卖方或价差")
        elif iv_tier in {"low", "ultra_low"}:
            preferred.append("DTE大于45天且IV低时，可考虑买虚值或低权利金方向策略")

    if iv_tier == "high":
        preferred.append("IV偏高时优先价差、比率价差、备兑/保护组合")
        avoid.append("默认单腿追高买权")
        if risk_key == "aggressive" and trend_regime == "breakout" and dte_tier == "far":
            preferred.append("若突破有效且用户偏积极，IV偏高时可用价差作主仓、方向性买方作小仓位卫星")
            hard_rules.append("IV偏高不能把激进突破远期的方向性买方完全否定，只能降低仓位、改为卫星仓或改用价差表达")
    if trend_regime == "trend_slow":
        preferred.extend(["趋势有但速度不足时，优先期货、实值期权或价差"])
        avoid.append("趋势没速度时重仓买远虚值")

    return OptionStrategyPolicy(
        risk_key=risk_key,
        risk_label=risk_label,
        trend_regime=trend_regime,
        iv_tier=iv_tier,
        dte_tier=dte_tier,
        hard_rules=_dedupe(hard_rules),
        preferred_strategies=_dedupe(preferred),
        avoid_strategies=_dedupe(avoid),
        required_checks=_dedupe(checks),
    )


def build_option_strategy_policy_context(**kwargs: Any) -> str:
    return build_option_strategy_policy(**kwargs).as_prompt_context()


def _dedupe(items: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for item in items:
        text = str(item).strip()
        if text and text not in seen:
            out.append(text)
            seen.add(text)
    return out


def _risk_from_text(text: str) -> Optional[str]:
    raw = str(text or "")
    if not raw.strip():
        return None
    cleaned = re.sub(
        r"(不是|并非|不算|别按|不要按|不再按|不是很)\s*(偏?保守|保守型|conservative)",
        "",
        raw,
        flags=re.IGNORECASE,
    )
    lowered = cleaned.lower()
    if any(k in lowered for k in ["aggress", "seek_profit"]):
        return "aggressive"
    if any(k in cleaned for k in ["偏激进", "激进", "偏积极", "积极", "高风险"]):
        return "aggressive"
    if any(k in lowered for k in ["conserv", "risk_averse"]):
        return "conservative"
    if any(k in cleaned for k in ["偏保守", "保守", "低风险"]):
        return "conservative"
    return None


def _infer_iv_tier_from_text(*parts: str) -> str:
    text = " ".join(str(x or "") for x in parts)
    match = re.search(r"(?:iv|IV|波动率|iv\s*rank|IV\s*Rank|rank|Rank)[^\d]{0,8}(\d+(?:\.\d+)?)", text)
    if match:
        return classify_iv_rank(float(match.group(1)))
    if any(k in text for k in ["IV很高", "iv很高", "波动率很高", "波动率太高", "IV太高", "iv太高", "IV偏高", "iv偏高"]):
        return "high"
    if any(k in text for k in ["IV很低", "iv很低", "波动率很低", "IV极低", "iv极低", "波动率极低"]):
        return "ultra_low"
    if any(k in text for k in ["IV偏低", "iv偏低", "波动率偏低"]):
        return "low"
    return "unknown"


def _infer_dte_tier_from_text(*parts: str) -> str:
    text = " ".join(str(x or "") for x in parts)
    match = re.search(r"(?:剩余|还有|还剩|距离到期|到期)[^\d]{0,6}(\d+)\s*天", text)
    if match:
        return classify_dte(int(match.group(1)))
    if any(k in text for k in ["末日", "当天到期", "今天到期", "0天到期"]):
        return "expiry"
    if any(k in text for k in ["不到7天", "少于7天", "小于7天", "一周内", "超短期"]):
        return "ultra_short"
    if any(k in text for k in ["到期还远", "DTE较远", "dte较远", "远月", "远期期权"]):
        return "far"
    return "unknown"
