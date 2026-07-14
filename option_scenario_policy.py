from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Literal


ScenarioKind = Literal["none", "technical", "volatility", "combined"]
ScenarioMarketBias = Literal["bullish", "bearish", "unknown"]
ScenarioIvMove = Literal["up", "down", "unknown"]


@dataclass(frozen=True)
class OptionHypotheticalScenario:
    kind: ScenarioKind = "none"
    assumed_market_bias: ScenarioMarketBias = "unknown"
    assumed_iv_move: ScenarioIvMove = "unknown"
    condition_text: str = ""

    @property
    def active(self) -> bool:
        return self.kind != "none"


_CONDITION_PATTERN = re.compile(r"(?:如果|假如|假设|倘若|一旦|若)")
_OPTION_DOMAIN_PATTERN = re.compile(
    r"(?:期权|认购|认沽|购权|沽权|权利金|行权价|到期月|"
    r"牛市价差|熊市价差|信用价差|借记价差|铁鹰|跨式|宽跨|"
    r"\bcall\b|\bput\b|(?<![A-Za-z])iv(?![A-Za-z])|隐含波动率)",
    re.IGNORECASE,
)
_ACTION_INTENT_PATTERN = re.compile(
    r"(?:怎么操作|如何操作|应该怎么|该怎么|怎么做|如何做|"
    r"什么策略|哪种策略|策略怎么|如何开仓|怎么开仓|"
    r"如何调整|怎么调整|如何应对|怎么应对|"
    r"应该买|应该卖|该买|该卖|是否适合|能不能做|可以买|可以卖|"
    r"买[^，。！？?]{0,12}还是[^，。！？?]{0,12}卖|"
    r"卖[^，。！？?]{0,12}还是[^，。！？?]{0,12}买)"
)
_ACTION_BOUNDARY_PATTERN = re.compile(
    r"(?:(?:用)?期权(?:应该|该|怎么|如何)|应该怎么操作|该怎么操作|"
    r"怎么操作|如何操作|应该怎么做|该怎么做|怎么做|如何做|"
    r"该用什么策略|应该用什么策略|什么策略|如何应对|怎么应对)"
)

_BULLISH_TECHNICAL_PATTERNS = (
    re.compile(r"破坏空头结构"),
    re.compile(r"空头结构[^，。！？?]{0,8}(?:被)?(?:破坏|解除|失效)"),
    re.compile(r"(?:有效|确认|重新|成功)?(?:站回|站上|站稳)"),
    re.compile(r"(?:向上突破|上破|收复|(?:有效|确认|放量)突破|突破(?=\s*\d)|突破(?:压力|阻力|前高|均线|平台))"),
    re.compile(r"(?:转为|形成|确认|恢复)(?:了)?多头(?:结构|趋势)?"),
    re.compile(r"(?:反转向上|确认转强)"),
)
_BEARISH_TECHNICAL_PATTERNS = (
    re.compile(r"破坏多头结构"),
    re.compile(r"多头结构[^，。！？?]{0,8}(?:被)?(?:破坏|解除|失效)"),
    re.compile(r"(?:有效|确认|继续)?(?:跌破|下破|失守)"),
    re.compile(r"(?:向下突破|跌破(?:支撑|均线|平台|前低))"),
    re.compile(r"(?:转为|形成|确认|恢复)(?:了)?空头(?:结构|趋势)?"),
    re.compile(r"(?:反转向下|确认转弱)"),
)
_IV_TERM = r"(?:(?<![A-Za-z])iv(?![A-Za-z])|隐含波动率|波动率)"
_IV_UP_PATTERNS = (
    re.compile(rf"{_IV_TERM}[^，。！？?]{{0,20}}(?:升到|升至|上升|走高|抬升|升高|飙升|继续升|突破)", re.IGNORECASE),
    re.compile(r"升波"),
)
_IV_DOWN_PATTERNS = (
    re.compile(rf"{_IV_TERM}[^，。！？?]{{0,20}}(?:降到|降至|下降|回落|走低|降低|下行|继续降)", re.IGNORECASE),
    re.compile(r"降波"),
)


def detect_option_hypothetical_scenario(user_query: str) -> OptionHypotheticalScenario:
    query = str(user_query or "").strip()
    if not query:
        return OptionHypotheticalScenario()
    if not _CONDITION_PATTERN.search(query):
        return OptionHypotheticalScenario()
    if not _OPTION_DOMAIN_PATTERN.search(query):
        return OptionHypotheticalScenario()
    if not _ACTION_INTENT_PATTERN.search(query):
        return OptionHypotheticalScenario()

    condition_text = _extract_condition_text(query)
    if not condition_text:
        return OptionHypotheticalScenario()

    direction_text = _strip_negated_direction_triggers(condition_text)
    bullish = _matches_any(direction_text, _BULLISH_TECHNICAL_PATTERNS)
    bearish = _matches_any(direction_text, _BEARISH_TECHNICAL_PATTERNS)
    if bullish == bearish:
        market_bias: ScenarioMarketBias = "unknown"
    else:
        market_bias = "bullish" if bullish else "bearish"

    iv_text = _strip_negated_iv_triggers(condition_text)
    iv_up = _matches_any(iv_text, _IV_UP_PATTERNS)
    iv_down = _matches_any(iv_text, _IV_DOWN_PATTERNS)
    if iv_up == iv_down:
        iv_move: ScenarioIvMove = "unknown"
    else:
        iv_move = "up" if iv_up else "down"

    has_technical_scenario = bullish or bearish
    has_volatility_scenario = iv_up or iv_down
    if has_technical_scenario and has_volatility_scenario:
        kind: ScenarioKind = "combined"
    elif has_technical_scenario:
        kind = "technical"
    elif has_volatility_scenario:
        kind = "volatility"
    else:
        return OptionHypotheticalScenario()

    return OptionHypotheticalScenario(
        kind=kind,
        assumed_market_bias=market_bias,
        assumed_iv_move=iv_move,
        condition_text=condition_text,
    )


def build_strategist_scenario_context(scenario: OptionHypotheticalScenario) -> str:
    if not scenario.active:
        return ""
    bias_label = {
        "bullish": "看涨",
        "bearish": "看跌",
        "unknown": "未指定/存在冲突，不强行定向",
    }[scenario.assumed_market_bias]
    iv_label = {
        "up": "上升",
        "down": "下降",
        "unknown": "未指定/存在冲突",
    }[scenario.assumed_iv_move]
    return f"""

        【条件场景推演（仅本题生效，优先于上方对当前策略方向的限制）】
        - 用户设定的条件：{scenario.condition_text}
        - 场景类型：{scenario.kind}
        - 假设成立后的价格方向：{bias_label}
        - 假设成立后的IV变化：{iv_label}
        - 必须先用1至2句说明“当前基线”，只陈述【技术面参考】和工具给出的当前事实。
        - 随后必须明确写“若上述条件成立”，再回答该条件成立后的首选策略、适用原因、触发确认和失效条件；不得把假设写成已经发生的事实。
        - 若假设价格方向为看涨或看跌，场景策略方向跟随该假设方向；不得用当前技术方向否定与条件一致的场景策略。
        - 若只假设IV变化而没有新的价格方向，价格方向继续沿用当前技术面，只调整买方/卖方、价差和波动率暴露选择，不得擅自翻转多空。
        - 用户给出的价格或IV数字只是条件阈值，不是当前行情；当前现价、IV、DTE和具体合约仍必须来自工具，缺数据时只给筛选框架。
        - 不硬编码牛市价差或熊市价差，仍需结合风险偏好、IV和DTE选择风险有限且方向一致的结构。
        """


def build_finalizer_scenario_context(scenario: OptionHypotheticalScenario) -> str:
    if not scenario.active:
        return ""
    bias_label = {
        "bullish": "看涨",
        "bearish": "看跌",
        "unknown": "未指定/存在冲突",
    }[scenario.assumed_market_bias]
    iv_label = {
        "up": "上升",
        "down": "下降",
        "unknown": "未指定/存在冲突",
    }[scenario.assumed_iv_move]
    return f"""

                【条件场景整合（仅本题生效）】
                - 用户设定的条件：{scenario.condition_text}
                - 假设成立后的价格方向：{bias_label}；IV变化：{iv_label}。
                - 保持上方既有 CIO 报告标题、章节和排版，不新增另一套报告模板。
                - 在【核心结论】中先区分“当前基线”，再写“若条件成立后的结论”；不得把条件阈值包装成当前行情，也不得声称条件已经触发。
                - 在【交易策略部署】中正面回答条件成立后的策略。技术面当前方向与假设后方向不同时，两者都要保留，不得用当前方向覆盖 strategist 的条件策略。
                - 若只有IV假设而没有新的价格方向，不得擅自翻转多空，只调整波动率结构选择。
                - 具体价格、IV、DTE、行权价和权利金仍以团队报告池中的确定性数据为准；缺失时不得补造。
                """


def _extract_condition_text(query: str) -> str:
    condition_match = _CONDITION_PATTERN.search(query)
    if not condition_match:
        return ""
    tail = query[condition_match.start():]
    boundary_match = _ACTION_BOUNDARY_PATTERN.search(tail, condition_match.end() - condition_match.start())
    if boundary_match:
        tail = tail[:boundary_match.start()]
    return tail.strip(" \t\r\n，,。；;：:！？!?")


def _strip_negated_direction_triggers(text: str) -> str:
    cleaned = re.sub(
        r"(?:空头|多头)结构[^，。！？?]{0,5}(?:没有|并未|尚未|未)(?:被)?(?:破坏|解除|失效)",
        "",
        text,
    )
    return re.sub(
        r"(?:未能|没有|并未|尚未|未|不能|无法|不)(?:有效)?"
        r"(?:站回|站上|站稳|突破|上破|收复|跌破|下破|失守)",
        "",
        cleaned,
    )


def _strip_negated_iv_triggers(text: str) -> str:
    return re.sub(
        rf"{_IV_TERM}[^，。！？?]{{0,8}}(?:不再|没有|并未|尚未|未|不会|无法)"
        r"(?:继续)?(?:上升|下降|回落|走高|走低|抬升|降低)",
        "",
        text,
        flags=re.IGNORECASE,
    )


def _matches_any(text: str, patterns: tuple[re.Pattern[str], ...]) -> bool:
    return any(pattern.search(text) for pattern in patterns)
