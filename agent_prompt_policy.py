from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, List, Sequence, Tuple

from option_strategy_policy import normalize_option_risk_preference


OPTION_STRATEGY_ACTION_KEYWORDS: Tuple[str, ...] = (
    "策略",
    "适合",
    "能不能",
    "可不可以",
    "该不该",
    "怎么做",
    "买",
    "卖",
    "双卖",
    "价差",
    "买方",
    "卖方",
    "认购",
    "认沽",
    "虚值",
    "实值",
)

ETF_UNDERLYING_ALIASES: Tuple[str, ...] = (
    "50ETF",
    "上证50ETF",
    "300ETF",
    "沪深300ETF",
    "500ETF",
    "中证500ETF",
    "创业板ETF",
    "创业板",
    "创业板指",
    "创业板指数",
    "科创50ETF",
    "科创板",
    "科创50",
    "深100ETF",
)

INDEX_UNDERLYING_ALIASES: Tuple[str, ...] = (
    "沪深300",
    "中证500",
    "中证1000",
    "上证50",
    "沪深300股指",
    "中证500股指",
    "中证1000股指",
    "上证50股指",
)

COMMODITY_UNDERLYING_ALIASES: Tuple[str, ...] = (
    "黄金",
    "白银",
    "沪金",
    "沪银",
    "铜",
    "沪铜",
    "氧化铝",
    "铝",
    "沪铝",
    "锌",
    "沪锌",
    "铅",
    "镍",
    "锡",
    "碳酸锂",
    "工业硅",
    "多晶硅",
    "原油",
    "豆粕",
    "豆油",
    "棕榈油",
    "菜油",
    "菜粕",
    "豆一",
    "豆二",
    "玉米",
    "淀粉",
    "棉花",
    "白糖",
    "苹果",
    "鸡蛋",
    "生猪",
    "花生",
    "红枣",
    "棉纱",
    "橡胶",
    "20号胶",
    "螺纹",
    "螺纹钢",
    "热卷",
    "铁矿",
    "铁矿石",
    "焦炭",
    "焦煤",
    "不锈钢",
    "锰硅",
    "硅铁",
    "纸浆",
    "燃料油",
    "燃油",
    "液化气",
    "PTA",
    "甲醇",
    "沥青",
    "烧碱",
    "聚丙烯",
    "塑料",
    "PVC",
    "苯乙烯",
    "乙二醇",
    "纯苯",
    "尿素",
    "纯碱",
    "玻璃",
    "对二甲苯",
    "PX",
    "BR橡胶",
)

OPTION_UNDERLYING_HINTS: Tuple[str, ...] = tuple(
    sorted(
        set(ETF_UNDERLYING_ALIASES + INDEX_UNDERLYING_ALIASES + COMMODITY_UNDERLYING_ALIASES),
        key=len,
        reverse=True,
    )
)

FUTURES_OPTION_CODE_ALIASES: Tuple[str, ...] = (
    "IO",
    "MO",
    "HO",
    "IF",
    "IH",
    "IM",
    "IC",
    "AU",
    "AG",
    "CU",
    "AL",
    "ZN",
    "PB",
    "NI",
    "SN",
    "AO",
    "LC",
    "SI",
    "RB",
    "HC",
    "I",
    "J",
    "JM",
    "M",
    "Y",
    "P",
    "OI",
    "RM",
    "A",
    "B",
    "C",
    "CF",
    "SR",
    "AP",
    "JD",
    "LH",
    "PK",
    "SC",
    "FU",
    "PG",
    "TA",
    "MA",
    "PP",
    "L",
    "V",
    "EB",
    "EG",
    "UR",
    "SA",
    "FG",
    "RU",
    "PX",
    "BR",
)

GENERIC_OPTION_QUESTION_HINTS: Tuple[str, ...] = (
    "想象题",
    "假设",
    "不涉及标的",
    "跟标的不相干",
    "和标的不相干",
    "单纯",
    "概念",
    "一般来说",
    "原则上",
    "理论上",
)

CHAT_MODE_KNOWLEDGE = "knowledge_chat"
CHAT_MODE_ANALYSIS = "analysis_chat"

TASK_TYPE_NORMAL = "normal"
TASK_TYPE_STOCK_SELECTION = "stock_selection"
TASK_TYPE_SINGLE_STOCK_ANALYSIS = "single_stock_analysis"
TASK_TYPE_TECHNICAL_CONCEPT = "technical_concept"
TASK_TYPE_OPTION_STRATEGY_WITH_SUBJECT = "option_strategy_with_subject"
TASK_TYPE_OPTION_STRATEGY_NEEDS_SUBJECT = "option_strategy_needs_subject"

STOCK_SELECTION_ACTION_KEYWORDS: Tuple[str, ...] = (
    "帮我找",
    "帮我选",
    "帮我筛",
    "推荐",
    "筛选",
    "找几只",
    "选几只",
    "找一下",
    "哪些",
    "有什么",
    "股票池",
    "候选股",
    "选股",
)

STOCK_SELECTION_SUBJECT_KEYWORDS: Tuple[str, ...] = (
    "股票",
    "个股",
    "概念股",
    "龙头股",
    "强势股",
    "弱势股",
    "标的",
)

STOCK_SELECTION_PATTERN_KEYWORDS: Tuple[str, ...] = (
    "放量突破",
    "成交量异常",
    "成交量放大",
    "量能异动",
    "放量",
    "缩量",
    "换手率异常",
    "技术形态",
    "形态比较强",
    "形态强",
    "红三兵",
    "多头吞噬",
    "均线多头",
)

STOCK_SELECTION_CONCEPT_KEYWORDS: Tuple[str, ...] = (
    "AI概念股",
    "ai概念股",
    "半导体",
    "芯片",
    "低空经济",
    "新能源",
    "机器人概念股",
)

EXPLAIN_PREFIXES: Tuple[str, ...] = (
    "什么是",
    "解释",
    "解释一下",
    "科普",
    "科普一下",
    "怎么理解",
    "什么意思",
    "是什么",
    "如何判断",
    "怎么判断",
    "怎样判断",
    "如何识别",
    "怎么识别",
    "原理",
)

TECHNICAL_CONCEPT_KEYWORDS: Tuple[str, ...] = (
    "k线",
    "K线",
    "均线",
    "图表",
    "技术面",
    "技术分析",
    "真假突破",
    "假突破",
    "假跌破",
    "支撑位",
    "阻力位",
    "成交量",
    "回踩",
    "趋势线",
    "多头陷阱",
    "空头陷阱",
    "止损",
    "止盈",
    "仓位管理",
    "突破四原则",
    "放量突破",
)

COMPANY_ENTITY_SUFFIXES: Tuple[str, ...] = (
    "股份",
    "集团",
    "科技",
    "技术",
    "控股",
    "电子",
    "电气",
    "机械",
    "汽车",
    "能源",
    "药业",
    "银行",
    "证券",
    "制造",
    "动力",
    "材料",
    "智能",
    "软件",
    "通信",
    "航空",
    "医药",
    "生物",
    "实业",
    "新材",
)

COMPANY_ENTITY_PATTERN = re.compile(
    r"[一-龥]{2,10}(?:%s)" % "|".join(COMPANY_ENTITY_SUFFIXES)
)

SINGLE_STOCK_ANALYSIS_KEYWORDS: Tuple[str, ...] = (
    "基本面",
    "财报",
    "公告",
    "近期动态",
    "最近动态",
    "公司动态",
    "消息面",
    "新闻",
    "资讯",
    "业绩",
    "利好",
    "利空",
    "催化",
    "技术面",
    "技术分析",
    "K线",
    "k线",
    "均线",
    "走势",
    "趋势",
    "支撑",
    "压力",
    "阻力",
    "突破",
    "破位",
    "形态",
    "分析",
)

RESEARCH_NEED_KEYWORDS: Tuple[str, ...] = (
    "基本面",
    "财报",
    "公告",
    "近期动态",
    "最近动态",
    "公司动态",
    "消息面",
    "新闻",
    "资讯",
    "业绩",
    "利好",
    "利空",
    "催化",
)

TECHNICAL_NEED_KEYWORDS: Tuple[str, ...] = (
    "技术面",
    "技术分析",
    "K线",
    "k线",
    "均线",
    "走势",
    "趋势",
    "支撑",
    "压力",
    "阻力",
    "突破",
    "破位",
    "形态",
)


@dataclass(frozen=True)
class AnalysisTaskPolicy:
    task_type: str
    recommended_chat_mode: str
    recommended_plan: Tuple[str, ...]
    clear_symbol: bool
    hard_override: bool
    reason: str


@dataclass(frozen=True)
class SubjectPolicy:
    mode: str
    is_option_strategy: bool
    has_explicit_subject: bool
    is_concept_question: bool
    needs_clarification: bool
    reason: str

    def as_prompt_context(self) -> str:
        lines = ["【标的上下文策略】"]
        if not self.is_option_strategy:
            lines.append("- 本轮没有特殊期权策略标的约束，按常规路由处理。")
            return "\n".join(lines)
        if self.has_explicit_subject:
            lines.append("- 本轮具备明确交易对象，可在行情、数据和策略链路中围绕该对象展开。")
        elif self.is_concept_question:
            lines.append("- 本轮是概念/原则型问题，按一般机制解释，不使用默认行情对象补全问题。")
        elif self.needs_clarification:
            lines.append("- 本轮缺少可执行建议所需交易对象，先澄清对象，再进入行情或策略判断。")
        lines.append(f"- 识别依据：{self.reason}")
        return "\n".join(lines)


@dataclass(frozen=True)
class ProfilePolicy:
    risk_key: str
    risk_label: str
    source: str

    def as_prompt_context(self) -> str:
        return (
            "【画像优先级】\n"
            f"- 本轮有效风险偏好：{self.risk_label}（来源：{self.source}）。\n"
            "- 当前问题中的明确表达优先于结构化画像；结构化画像优先于旧字段；交易安全规则优先于用户偏好。\n"
            "- 年龄、性别等身份信息只用于必要的表达个性化，不作为交易策略依据。"
        )


def _contains_any(text: str, keywords: Sequence[str]) -> bool:
    return any(keyword in text for keyword in keywords)


def _looks_like_stock_selection(query: str) -> bool:
    text = str(query or "").strip()
    if not text:
        return False
    lowered = text.lower()
    if _contains_any(text, EXPLAIN_PREFIXES):
        return False
    has_action = _contains_any(text, STOCK_SELECTION_ACTION_KEYWORDS)
    has_subject = _contains_any(text, STOCK_SELECTION_SUBJECT_KEYWORDS)
    has_pattern = _contains_any(text, STOCK_SELECTION_PATTERN_KEYWORDS)
    has_concept = any(keyword in text or keyword in lowered for keyword in STOCK_SELECTION_CONCEPT_KEYWORDS)
    if has_action and (has_subject or has_pattern or has_concept):
        return True
    if has_subject and has_pattern:
        return True
    return "概念股有哪些" in text or "相关股票" in text or "相关个股" in text


def _looks_like_technical_concept(query: str) -> bool:
    text = str(query or "").strip()
    if not text:
        return False
    if not _contains_any(text, EXPLAIN_PREFIXES):
        return False
    return _contains_any(text, TECHNICAL_CONCEPT_KEYWORDS)


def _looks_like_single_stock_analysis(query: str, *, symbol_hint: str = "", focus_entity: str = "") -> bool:
    text = str(query or "").strip()
    if not text:
        return False
    has_subject = bool(str(symbol_hint or "").strip()) or bool(str(focus_entity or "").strip())
    has_subject = has_subject or bool(re.search(r"(?<!\d)\d{6}(?!\d)", text))
    has_subject = has_subject or bool(COMPANY_ENTITY_PATTERN.search(text))
    if not has_subject:
        return False
    return _contains_any(text, SINGLE_STOCK_ANALYSIS_KEYWORDS)


def _single_stock_recommended_plan(query: str) -> Tuple[str, ...]:
    text = str(query or "")
    has_research = _contains_any(text, RESEARCH_NEED_KEYWORDS)
    has_technical = _contains_any(text, TECHNICAL_NEED_KEYWORDS)
    if has_research and has_technical:
        return ("analyst", "researcher")
    if has_research:
        return ("researcher",)
    if has_technical:
        return ("analyst",)
    return ()


def classify_analysis_task_type(
    query: str,
    symbol_hint: str = "",
    focus_entity: str = "",
    is_followup: bool = False,
    recent_context: str = "",
) -> AnalysisTaskPolicy:
    text = str(query or "").strip()
    if _looks_like_stock_selection(text):
        return AnalysisTaskPolicy(
            task_type=TASK_TYPE_STOCK_SELECTION,
            recommended_chat_mode=CHAT_MODE_ANALYSIS,
            recommended_plan=("screener",),
            clear_symbol=True,
            hard_override=True,
            reason="用户要求找/选/筛/推荐一批股票",
        )
    if _looks_like_technical_concept(text):
        return AnalysisTaskPolicy(
            task_type=TASK_TYPE_TECHNICAL_CONCEPT,
            recommended_chat_mode=CHAT_MODE_KNOWLEDGE,
            recommended_plan=(),
            clear_symbol=False,
            hard_override=True,
            reason="用户在解释或判断技术概念",
        )

    subject_policy = build_subject_policy(text, symbol_hint=symbol_hint)
    if subject_policy.is_option_strategy and subject_policy.has_explicit_subject:
        return AnalysisTaskPolicy(
            task_type=TASK_TYPE_OPTION_STRATEGY_WITH_SUBJECT,
            recommended_chat_mode=CHAT_MODE_ANALYSIS,
            recommended_plan=("analyst", "strategist"),
            clear_symbol=False,
            hard_override=False,
            reason=subject_policy.reason,
        )
    if subject_policy.is_option_strategy and not subject_policy.has_explicit_subject:
        return AnalysisTaskPolicy(
            task_type=TASK_TYPE_OPTION_STRATEGY_NEEDS_SUBJECT,
            recommended_chat_mode=CHAT_MODE_ANALYSIS,
            recommended_plan=("chatter",),
            clear_symbol=True,
            hard_override=True,
            reason=subject_policy.reason,
        )

    if _looks_like_single_stock_analysis(text, symbol_hint=symbol_hint, focus_entity=focus_entity):
        return AnalysisTaskPolicy(
            task_type=TASK_TYPE_SINGLE_STOCK_ANALYSIS,
            recommended_chat_mode=CHAT_MODE_ANALYSIS,
            recommended_plan=_single_stock_recommended_plan(text),
            clear_symbol=False,
            hard_override=False,
            reason="用户要求分析明确单一股票或公司",
        )

    return AnalysisTaskPolicy(
        task_type=TASK_TYPE_NORMAL,
        recommended_chat_mode="",
        recommended_plan=(),
        clear_symbol=False,
        hard_override=False,
        reason="未命中高置信任务类型，交给原有路由",
    )


def is_option_strategy_question(query: str) -> bool:
    text = str(query or "")
    if not text:
        return False
    has_option = "期权" in text or any(k in text for k in ["认购", "认沽", "价差", "双卖", "买方", "卖方"])
    return bool(has_option and any(k in text for k in OPTION_STRATEGY_ACTION_KEYWORDS))


def has_explicit_option_underlying(query: str, symbol_hint: str = "") -> bool:
    if str(symbol_hint or "").strip():
        return True
    text = str(query or "").upper()
    if re.search(r"\b\d{6}\b", text):
        return True
    if re.search(r"\b[A-Z]{1,3}\d{3,4}\b", text):
        return True
    raw = str(query or "")
    if any(hint.upper() in text or hint in raw for hint in OPTION_UNDERLYING_HINTS):
        return True
    return _has_explicit_futures_option_code(text)


def _has_explicit_futures_option_code(upper_text: str) -> bool:
    for code in FUTURES_OPTION_CODE_ALIASES:
        pattern = rf"(?<![A-Z0-9]){re.escape(code)}(?:\d{{3,4}}|期权|合约|认购|认沽)(?![A-Z0-9])"
        if re.search(pattern, upper_text):
            return True
    return False


def is_generic_option_strategy_question(query: str) -> bool:
    text = str(query or "")
    return bool(is_option_strategy_question(text) and any(hint in text for hint in GENERIC_OPTION_QUESTION_HINTS))


def build_subject_policy(query: str, symbol_hint: str = "") -> SubjectPolicy:
    option_strategy = is_option_strategy_question(query)
    has_subject = has_explicit_option_underlying(query, symbol_hint=symbol_hint)
    concept = is_generic_option_strategy_question(query)
    if not option_strategy:
        return SubjectPolicy(
            mode="normal",
            is_option_strategy=False,
            has_explicit_subject=has_subject,
            is_concept_question=False,
            needs_clarification=False,
            reason="非期权策略类问题",
        )
    if has_subject:
        return SubjectPolicy(
            mode="actionable_with_subject",
            is_option_strategy=True,
            has_explicit_subject=True,
            is_concept_question=concept,
            needs_clarification=False,
            reason="问题包含明确期权品种、标的代码或品种名",
        )
    if concept:
        return SubjectPolicy(
            mode="concept_without_subject",
            is_option_strategy=True,
            has_explicit_subject=False,
            is_concept_question=True,
            needs_clarification=False,
            reason="用户表达为概念、原则或假设场景",
        )
    return SubjectPolicy(
        mode="needs_subject",
        is_option_strategy=True,
        has_explicit_subject=False,
        is_concept_question=False,
        needs_clarification=True,
        reason="问题要求策略判断，但没有给出可执行建议所需交易对象",
    )


def enforce_unspecified_option_strategy_routing(
    query: str,
    plan: Sequence[str],
    symbol: str = "",
) -> tuple[List[str], str]:
    # 这里故意不采信 planner 生成的 symbol；它可能正是模型自行补出的默认对象。
    policy = build_subject_policy(query, symbol_hint="")
    if not policy.is_option_strategy or policy.has_explicit_subject:
        return list(plan), symbol
    return ["chatter"], ""


def build_profile_policy(
    *,
    risk_preference: Any = "",
    profile_context: str = "",
    user_query: str = "",
) -> ProfilePolicy:
    user_risk = normalize_option_risk_preference("", user_query=user_query)
    profile_risk = normalize_option_risk_preference("", profile_context=profile_context)
    fallback_risk = normalize_option_risk_preference(risk_preference)
    if user_risk != "balanced":
        key, source = user_risk, "当前问题明确表达"
    elif profile_risk != "balanced":
        key, source = profile_risk, "结构化画像"
    else:
        key, source = fallback_risk, "旧风险字段" if str(risk_preference or "").strip() else "默认稳健"
    label = {
        "conservative": "偏保守",
        "balanced": "稳健/平衡",
        "aggressive": "偏积极/激进",
    }[key]
    return ProfilePolicy(risk_key=key, risk_label=label, source=source)


def build_data_policy_context(*, symbol: str = "", mode: str = "analysis") -> str:
    subject = str(symbol or "").strip() or "未锁定"
    return (
        "【统一数据与边界规则】\n"
        f"- 标的锁定：本轮分析对象为 {subject}；若上游未锁定对象，只能做原则性回答或澄清，不补默认对象。\n"
        "- 数据采信：行情、价格、涨跌幅、支撑压力、均线值、IV、DTE、保证金、财报数字、机构目标价等可核验数字只引用工具或团队报告池；工具失败时明确说明数据源未查到并降级为条件式建议。\n"
        "- 技术指标：技术面默认只允许 K 线和均线；不得自行输出 RSI、MACD、KDJ、BOLL、布林、量能突破等非授权指标。\n"
        "- 基本面与资讯：基本面、财报、公告、近期动态必须来自研究员工具链或团队报告池；没有资料时说暂无可用资料，不用模型记忆补事实。\n"
        "- 交易安全：画像只辅助个性化，不能关闭必要风险提示；策略建议必须保留仓位、失效条件和反向风险。\n"
        "- 输出风格：先回答用户的直接问题，再说明依据来自趋势、IV/DTE、风险偏好或数据缺口中的哪些项。"
    )
