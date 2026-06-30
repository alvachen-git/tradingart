import streamlit as st
import pandas as pd
import data_engine as de
import html
import re
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode, ColumnsAutoSizeMode
import time
from market_monitor_grid import (
    AG_GRID_LOCALE_ZH_CN,
    GRID_NUMBER_COMPARATOR,
    GRID_NUMBER_FILTER_PARAMS,
    format_contract_expiry_suffix,
    make_grid_number_filter_value_getter,
)
from ui_components import inject_sidebar_toggle_style

# ============================================================
# 页面配置
# ============================================================

st.set_page_config(
    page_title="爱波塔-市场扫描器",
    page_icon="favicon.ico",
    layout="wide",
    initial_sidebar_state="expanded"
)



# 🔥 添加统一的侧边栏导航
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sidebar_navigation import show_navigation
with st.sidebar:
    show_navigation()

# ============================================================
# 🎯 配置区域 - 在这里自定义品种名称和分类
# ============================================================

# 品种名称映射（合约代码前缀 -> 显示名称）
# 根据你的实际合约代码修改
PRODUCT_NAMES = {
    # 股指
    "IO": "沪深300",
    "MO": "中证1000",
    "HO": "上证50",
    "IF": "沪深300股指",
    "IH": "上证50股指",
    "IM": "中证1000股指",
    "IC": "中证500股指",
    "TS": "2年国债",
    "TF": "5年国债",
    "T": "10年国债",
    "TL": "30年国债",
    "EC": "集运欧线",

    # 农产品
    "C": "玉米",
    "CS": "淀粉",
    "A": "豆一",
    "B": "豆二",
    "M": "豆粕",
    "Y": "豆油",
    "P": "棕榈油",
    "OI": "菜油",
    "RM": "菜粕",
    "SR": "白糖",
    "CF": "棉花",
    "AP": "苹果",
    "CJ": "红枣",
    "PK": "花生",
    "JD": "鸡蛋",
    "LH": "生猪",

    # 工业品
    "RB": "螺纹钢",
    "HC": "热卷",
    "I": "铁矿石",
    "J": "焦炭",
    "JM": "焦煤",
    "SM": "锰硅",
    "SF": "硅铁",
    "SS": "不锈钢",
    "WR": "线材",
    "FG": "玻璃",
    "SA": "纯碱",
    "SP": "纸浆",

    # 化工
    "L": "塑料",
    "PP": "聚丙烯",
    "V": "PVC",
    "EB": "苯乙烯",
    "EG": "乙二醇",
    "PF": "短纤",
    "TA": "PTA",
    "MA": "甲醇",
    "UR": "尿素",
    "RU": "橡胶",
    "NR": "20号胶",
    "BR": "丁苯橡胶",
    "FU": "燃油",
    "LU": "低硫燃油",
    "BU": "沥青",
    "SC": "原油",
    "PG": "LPG",
    "PX": "PX",
    "BZ": "纯苯",
    "PL": "丙烯",
    "PR": "瓶片",

    # 有色金属
    "CU": "铜",
    "AL": "铝",
    "ZN": "锌",
    "PB": "铅",
    "NI": "镍",
    "SN": "锡",
    "BC": "国际铜",
    "AO": "氧化铝",

    # 贵金属
    "AU": "黄金",
    "AG": "白银",
    "PT": "铂金",
    "PD": "钯金",

    # 新能源
    "SI": "工业硅",
    "LC": "碳酸锂",
    "PS": "多晶硅",


    # ETF期权
    "510050": "50ETF",
    "510300": "300ETF沪",
    "510500": "500ETF",
    "159901": "深100ETF",
    "159915": "创业板ETF",
    "159919": "300ETF深",
    "588000": "科创50ETF",
    "588080": "科创板ETF",
}

PRODUCT_DESCRIPTIONS = {
    "IO": "沪深300指数期权，反映大盘蓝筹波动率预期。",
    "MO": "中证1000指数期权，偏中小盘风格波动率观察。",
    "HO": "上证50指数期权，跟踪大盘权重股风险偏好。",
    "IF": "沪深300股指期货，代表核心宽基指数风险敞口。",
    "IH": "上证50股指期货，偏金融和大盘权重风格。",
    "IM": "中证1000股指期货，偏小盘成长与量化资金敏感。",
    "IC": "中证500股指期货，偏中盘风格风险敞口。",
    "TS": "2年期国债期货，偏短端利率品种，关注资金面、央行操作和短债收益率。",
    "TF": "5年期国债期货，反映中短端利率预期，常用于观察债市久期和套保需求。",
    "T": "10年期国债期货，债市核心长端利率品种，关注宏观增长、通胀和货币政策预期。",
    "TL": "30年期国债期货，超长端利率品种，对久期交易、保险配置和长期利率预期更敏感。",
    "EC": "集运指数（欧线）期货，跟踪中国出口至欧洲航线集装箱运价，受外贸需求、舱位供给和航线扰动影响。",
    "C": "玉米是饲料和深加工原料，关注库存、进口和养殖需求。",
    "CS": "淀粉由玉米加工而来，反映深加工利润和下游消费。",
    "A": "豆一代表国产大豆，关注国产供应和压榨需求。",
    "B": "豆二代表进口大豆，受美豆、巴西豆和到港节奏影响。",
    "M": "豆粕是大豆压榨后的饲料原料，常用于观察养殖饲料成本。",
    "Y": "豆油是植物油品种，受大豆压榨、油脂消费和外盘影响。",
    "P": "棕榈油是主要植物油，受马印产量、库存和进口利润影响。",
    "OI": "菜油是植物油品种，关注菜籽供应和油脂替代需求。",
    "RM": "菜粕是水产和畜禽饲料原料，受菜籽压榨和养殖需求影响。",
    "SR": "白糖是软商品，关注压榨季、进口和消费旺季。",
    "CF": "棉花是纺织原料，受种植、库存和下游订单影响。",
    "AP": "苹果是生鲜农产品，关注产区天气、入库和消费。",
    "CJ": "红枣是特色农产品，受产区供给和季节性消费影响。",
    "PK": "花生是油料和食品原料，关注产量、油厂收购和消费。",
    "JD": "鸡蛋反映养殖补栏和消费季节性。",
    "LH": "生猪连接养殖利润和猪肉消费，是农产品周期核心品种。",
    "RB": "螺纹钢是建筑用钢材，反映地产和基建施工需求。",
    "HC": "热卷是制造业板材用钢，关注汽车、家电和机械需求。",
    "I": "铁矿石是钢铁原料，受钢厂生产、港口库存和海外发运影响。",
    "J": "焦炭是钢铁冶炼燃料，关注钢厂利润和焦化产能。",
    "JM": "焦煤是焦炭上游原料，受煤矿供应和进口煤影响。",
    "SM": "锰硅是钢铁合金原料，受钢厂需求和电力成本影响。",
    "SF": "硅铁是合金和金属镁原料，关注电价、产区开工和钢需。",
    "SS": "不锈钢反映镍、铬成本和制造业需求。",
    "WR": "线材是建筑和工业用钢，关注终端施工需求。",
    "FG": "玻璃用于地产和光伏，关注库存、产线和终端需求。",
    "SA": "纯碱是玻璃和化工原料，受光伏玻璃和浮法玻璃需求影响。",
    "SP": "纸浆是造纸原料，关注进口供应和成品纸需求。",
    "L": "塑料是聚乙烯品种，受原油、煤化工成本和包装需求影响。",
    "PP": "聚丙烯用于塑编、注塑和膜料，关注化工开工和下游订单。",
    "V": "PVC是建材和塑料原料，受地产需求和电石成本影响。",
    "EB": "苯乙烯用于塑料和橡胶制品，受纯苯、乙烯和下游利润影响。",
    "EG": "乙二醇是聚酯原料，关注煤化工、油化工供应和聚酯需求。",
    "PF": "短纤是纺织原料，连接聚酯成本和纱线需求。",
    "TA": "PTA是聚酯产业链原料，上接PX和原油，下接纺织消费。",
    "MA": "甲醇是煤化工基础品种，关注煤价、烯烃需求和港口库存。",
    "UR": "尿素是氮肥品种，受农业需求、煤价和出口节奏影响。",
    "RU": "橡胶用于轮胎和工业制品，关注产区天气和汽车需求。",
    "NR": "20号胶偏轮胎原料，受东南亚供应和轮胎开工影响。",
    "BR": "丁苯橡胶是合成橡胶，关注丁二烯成本和轮胎需求。",
    "FU": "燃油与船燃和工业燃料相关，受原油、炼厂和航运需求影响。",
    "LU": "低硫燃油是船用燃料，关注原油、裂解价差和航运需求。",
    "BU": "沥青用于道路建设，受原油成本和基建施工影响。",
    "SC": "原油是能源定价核心品种，受供需、库存和地缘风险影响。",
    "PG": "LPG是液化石油气，关注进口到港、化工需求和民用消费。",
    "PX": "PX是PTA上游原料，受芳烃利润和聚酯需求影响。",
    "BZ": "纯苯是化工基础原料，影响苯乙烯、己内酰胺等链条。",
    "PL": "丙烯是化工原料，连接聚丙烯和下游化工需求。",
    "PR": "瓶片用于饮料包装和聚酯出口，受聚酯利润和消费旺季影响。",
    "CU": "铜是宏观敏感有色金属，反映电力、地产和制造业需求。",
    "AL": "铝受电解产能、电力成本和加工需求影响。",
    "ZN": "锌用于镀锌和基建制造，关注矿端供应和消费。",
    "PB": "铅主要用于蓄电池，关注再生铅供应和电池需求。",
    "NI": "镍连接不锈钢和新能源电池，受印尼供应影响较大。",
    "SN": "锡用于焊料和电子，关注半导体周期和矿端供应。",
    "BC": "国际铜对接境外铜价，便于观察内外盘价差。",
    "AO": "氧化铝是电解铝原料，关注矿石、冶炼和电解铝利润。",
    "AU": "黄金是贵金属避险资产，受利率、美元和风险偏好影响。",
    "AG": "白银兼具贵金属和工业属性，波动通常高于黄金。",
    "PT": "铂金用于汽车催化和工业，受贵金属替代和供给影响。",
    "PD": "钯金用于汽车催化，关注汽车产销和贵金属替代。",
    "SI": "工业硅用于有机硅、多晶硅和铝合金，关注新能源链条需求。",
    "LC": "碳酸锂是电池材料核心品种，受新能源车和库存周期影响。",
    "PS": "多晶硅是光伏上游材料，关注硅料供给和组件需求。",
    "510050": "50ETF期权跟踪上证50，观察大盘权重波动率。",
    "510300": "沪深300ETF期权，适合观察宽基指数波动率。",
    "510500": "中证500ETF期权，反映中盘风格波动率。",
    "159901": "深100ETF期权，跟踪深市核心资产波动率。",
    "159915": "创业板ETF期权，反映成长风格风险偏好。",
    "159919": "深市300ETF期权，观察沪深300相关波动率。",
    "588000": "科创50ETF期权，反映科创板成长资产波动率。",
    "588080": "科创板ETF期权，关注科技成长风格波动率。",
}

# 品种分类映射（合约代码前缀 -> 分类）
PRODUCT_CATEGORY = {
    # 股指
    "IO": "股指", "MO": "股指", "HO": "股指",
    "IF": "股指", "IH": "股指", "IM": "股指","IC": "股指",
    "TS": "国债", "TF": "国债", "T": "国债", "TL": "国债",
    "EC": "航运",
    "510050": "股指", "510300": "股指", "510500": "股指",
    "159901": "股指", "159915": "股指", "159919": "股指",
    "588000": "股指", "588080": "股指",

    # 农产品
    "C": "农产", "CS": "农产", "A": "农产", "B": "农产",
    "M": "农产", "Y": "农产", "P": "农产", "OI": "农产",
    "RM": "农产", "SR": "农产", "CF": "农产", "AP": "农产",
    "CJ": "农产", "PK": "农产", "JD": "农产", "LH": "农产",

    # 工业品
    "RB": "工业", "HC": "工业", "I": "工业", "J": "工业","RU": "工业",
    "JM": "工业", "SM": "工业", "SF": "工业", "SS": "工业","BR": "工业",
    "WR": "工业", "FG": "工业", "SA": "工业", "SP": "工业","AO": "工业",

    # 化工
    "L": "化工", "PP": "化工", "V": "化工", "EB": "化工",
    "EG": "化工", "PF": "化工", "TA": "化工", "MA": "化工",
    "UR": "化工", "PR": "化工", "FU": "化工","PG": "化工",
    "LU": "化工", "BU": "化工", "SC": "化工","SH": "化工",
    "BR": "化工", "PX": "化工", "BZ": "化工", "PL": "化工",

    # 有色金属
    "CU": "有色", "AL": "有色", "ZN": "有色", "PB": "有色",
    "NI": "有色", "SN": "有色",

    # 贵金属
    "AU": "贵金属", "AG": "贵金属", "PT": "贵金属","PD": "贵金属",

    # 新能源
    "SI": "新能源", "LC": "新能源", "PS": "新能源",
}

# 分类列表（按显示顺序）
CATEGORIES = ["全部", "股指", "国债", "农产", "工业", "化工", "有色", "贵金属", "新能源", "航运"]
FOCUS_IV_TREND_CACHE_VERSION = "v2"
FOCUS_HOLDING_TREND_CACHE_VERSION = "v1"
if "rank_focus_open" not in st.session_state:
    st.session_state.rank_focus_open = False
rank_sort_param = st.query_params.get("rank_sort", "")
if isinstance(rank_sort_param, list):
    rank_sort_param = rank_sort_param[0] if rank_sort_param else ""
rank_sort_expiry_active = rank_sort_param == "expiry"


# ============================================================
# 辅助函数
# ============================================================

def extract_product_code(contract_name):
    """从合约名称中提取品种代码"""
    # ETF期权（数字开头）
    if contract_name and contract_name[0].isdigit():
        return contract_name[:6]
    # 商品/股指期权（字母开头）
    code = ""
    for char in contract_name:
        if char.isalpha():
            code += char.upper()
        else:
            break
    return code


def get_product_name(contract_name):
    """获取品种显示名称"""
    code = extract_product_code(contract_name)
    return PRODUCT_NAMES.get(code, code)


def get_product_category(contract_name):
    """获取品种分类"""
    code = extract_product_code(contract_name)
    return PRODUCT_CATEGORY.get(code, "其他")


def get_product_description(contract_name):
    """获取右侧详情面板的简短品种说明。"""
    code = extract_product_code(contract_name)
    category = get_product_category(contract_name)
    return PRODUCT_DESCRIPTIONS.get(
        code,
        f"该品种属于{category}板块，适合结合波动率、价格和资金变化做快速扫描。",
    )


def contract_alpha_sort_key(contract_name):
    """按合约代码字母和月份排序，供右侧聚焦下拉使用。"""
    raw = str(contract_name or "").strip().upper()
    code_part = raw.split(" ", 1)[0]
    match = re.match(r"^([A-Z]+)(\d{3,4})", code_part)
    if match:
        return (match.group(1), int(match.group(2)), code_part)
    return (code_part, 0, raw)


def to_float(value):
    """把表格中的百分比、千分位数字、N/A 安全转换成浮点数。"""
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.replace(",", "").replace("%", "").strip()
        if cleaned in {"", "-", "N/A", "nan", "None", "快到期"}:
            return None
    else:
        cleaned = value
    try:
        number = float(cleaned)
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return number


def count_by_rule(df, column, rule):
    if column not in df.columns:
        return 0
    return int(df[column].apply(lambda value: (to_float(value) is not None) and rule(to_float(value))).sum())


SCAN_FILTERS = [
    {"key": "全部", "label": "全部扫描", "hint": "当前结果"},
    {"key": "高IV", "label": "高IV Rank", "hint": ">= 80"},
    {"key": "低IV", "label": "低IV Rank", "hint": "<= 20"},
    {"key": "IV升幅", "label": "IV日升幅", "hint": ">= 0.30"},
    {"key": "价格异动", "label": "价格异动", "hint": "|日涨跌| >= 1%"},
    {"key": "机构流入", "label": "机构流入", "hint": "日变动 >= 1,000"},
    {"key": "散户流出", "label": "散户流出", "hint": "日变动 <= -1,000"},
    {"key": "缺IV", "label": "缺IV数据", "hint": "无有效IV"},
]

RAIL_TAG_ABBR = {
    "高波": "高",
    "低波": "低",
    "IV升": "IV+",
    "价异动": "价",
    "机构流入": "机+",
    "散户流出": "散-",
    "缺IV": "缺",
    "快到期": "期",
    "观察": "观",
}


def build_scan_metrics(df):
    missing_iv = 0
    if {"当前IV", "IV Rank"}.issubset(df.columns):
        missing_iv = int(
            df.apply(
                lambda row: to_float(row.get("当前IV")) is None or to_float(row.get("IV Rank")) is None,
                axis=1,
            ).sum()
        )

    return {
        "全部": len(df),
        "高IV": count_by_rule(df, "IV Rank", lambda value: value >= 80),
        "低IV": count_by_rule(df, "IV Rank", lambda value: value <= 20),
        "IV升幅": count_by_rule(df, "IV变动(日)", lambda value: value >= 0.30),
        "价格异动": count_by_rule(df, "涨跌%(日)", lambda value: abs(value) >= 1),
        "机构流入": count_by_rule(df, "机构变动(日)", lambda value: value >= 1000),
        "散户流出": count_by_rule(df, "散户变动(日)", lambda value: value <= -1000),
        "缺IV": missing_iv,
    }


def apply_text_search(df, search_term):
    if not search_term:
        return df
    mask = df['合约'].str.contains(search_term, case=False, na=False) | df['品种'].str.contains(search_term, case=False, na=False)
    return df[mask]


def build_scan_tags(row):
    tags = []
    iv_rank = to_float(row.get("IV Rank"))
    current_iv = to_float(row.get("当前IV"))
    iv_change = to_float(row.get("IV变动(日)"))
    price_change = to_float(row.get("涨跌%(日)"))
    inst_change = to_float(row.get("机构变动(日)"))
    retail_change = to_float(row.get("散户变动(日)"))

    if str(row.get("IV Rank", "")).strip() == "快到期":
        tags.append("快到期")
    elif current_iv is None or iv_rank is None:
        tags.append("缺IV")
    elif iv_rank >= 80:
        tags.append("高波")
    elif iv_rank <= 20:
        tags.append("低波")

    if iv_change is not None and iv_change >= 0.30:
        tags.append("IV升")
    if price_change is not None and abs(price_change) >= 1:
        tags.append("价异动")
    if inst_change is not None and inst_change >= 1000:
        tags.append("机构流入")
    if retail_change is not None and retail_change <= -1000:
        tags.append("散户流出")

    return tags[:3] or ["观察"]


def add_scan_signal_column(df):
    df_with_signal = df.copy()
    if "扫描信号" in df_with_signal.columns:
        return df_with_signal
    insert_at = 1 if "合约" in df_with_signal.columns else 0
    signals = df_with_signal.apply(lambda row: "|".join(build_scan_tags(row)), axis=1)
    df_with_signal.insert(insert_at, "扫描信号", signals)
    return df_with_signal


def format_percent(value):
    number = to_float(value)
    return "-" if number is None else f"{number:.2f}%"


def format_signed_number(value, decimals=2, thousands=False):
    number = to_float(value)
    if number is None:
        return "-"
    sign = "+" if number > 0 else ""
    if thousands:
        return f"{sign}{int(number):,}"
    return f"{sign}{number:.{decimals}f}"


def format_compact_number(value):
    number = to_float(value)
    if number is None:
        return "-"
    sign = "+" if number > 0 else ""
    abs_value = abs(number)
    if abs_value >= 10000:
        return f"{sign}{number / 10000:.1f}万"
    if abs_value >= 1000:
        return f"{sign}{number / 1000:.0f}k"
    return f"{sign}{number:.0f}"


def format_rail_iv_rank(value):
    if str(value).strip() == "快到期":
        return "期"
    number = to_float(value)
    if number is None:
        return "N/A"
    return f"{number:.0f}"


def format_grid_iv_rank(value):
    if str(value).strip() == "快到期":
        return "快到期"
    number = to_float(value)
    if number is None:
        return "N/A"
    return f"{number:.0f}"


def tone_class(value):
    number = to_float(value)
    if number is None or number == 0:
        return "flat"
    return "up" if number > 0 else "down"


def apply_grid_column_groups(grid_options):
    column_defs = grid_options.get("columnDefs", [])
    by_field = {
        column.get("field"): column
        for column in column_defs
        if column.get("field")
    }
    used_fields = set()

    def children(fields):
        grouped_children = []
        for field in fields:
            if field in by_field:
                grouped_children.append(by_field[field])
                used_fields.add(field)
        return grouped_children

    groups = []
    for group_name, fields in [
        ("合约", ["合约", "到期", "扫描信号"]),
        ("波动率", ["当前IV", "IV Rank", "IV变动(日)", "IV变动(5日)"]),
        ("价格", ["涨跌%(日)", "涨跌%(5日)"]),
        ("持仓资金", ["散户变动(日)", "散户变动(5日)", "机构变动(日)", "机构变动(5日)"]),
    ]:
        grouped_children = children(fields)
        if grouped_children:
            groups.append({
                "headerName": group_name,
                "marryChildren": True,
                "children": grouped_children,
            })

    leftovers = [column for column in column_defs if column.get("field") not in used_fields]
    grid_options["columnDefs"] = groups + leftovers
    grid_options["groupHeaderHeight"] = 32
    grid_options["headerHeight"] = 34
    return grid_options


def focus_metric_html(label, value, tone="flat"):
    return (
        f'<div class="focus-kpi">'
        f'<div class="focus-kpi-label">{html.escape(label)}</div>'
        f'<div class="focus-kpi-value {tone}">{html.escape(str(value))}</div>'
        f'</div>'
    )


def _format_trend_date(date_value):
    raw = str(date_value or "").replace("-", "").replace("/", "").strip()
    if len(raw) == 8 and raw.isdigit():
        return f"{raw[4:6]}-{raw[6:]}"
    return raw or "-"


def _format_trend_value(value):
    number = to_float(value)
    if number is None:
        return "-"
    return f"{number:.2f}%"


def _format_position_axis(value):
    number = to_float(value)
    if number is None:
        return "-"
    abs_value = abs(number)
    if abs_value >= 10000:
        return f"{number / 10000:.1f}万"
    if abs_value >= 1000:
        return f"{number / 1000:.0f}k"
    return f"{number:.0f}"


def _format_position_value(value):
    number = to_float(value)
    if number is None:
        return "-"
    prefix = "+" if number > 0 else ""
    return f"{prefix}{number:,.0f}"


def render_iv_trend_html(iv_history):
    if iv_history is None or len(iv_history) < 2:
        return '<div class="iv-trend-empty">暂无近5日IV趋势</div>'

    if isinstance(iv_history, pd.DataFrame):
        records = iv_history.to_dict(orient="records")
    else:
        records = list(iv_history)

    points_data = []
    for record in records:
        value = to_float(record.get("iv") if isinstance(record, dict) else None)
        if value is None:
            continue
        date_label = _format_trend_date(record.get("trade_date", "") if isinstance(record, dict) else "")
        source = str(record.get("source", "") if isinstance(record, dict) else "")
        points_data.append({"date": date_label, "iv": value, "source": source})

    if len(points_data) < 2:
        return '<div class="iv-trend-empty">暂无近5日IV趋势</div>'

    values = [item["iv"] for item in points_data]
    min_v = min(values)
    max_v = max(values)
    if abs(max_v - min_v) < 0.01:
        min_v -= 1
        max_v += 1

    width, height = 320, 154
    pad_left, pad_right, pad_top, pad_bottom = 40, 18, 20, 30
    plot_w = width - pad_left - pad_right
    plot_h = height - pad_top - pad_bottom
    span = max_v - min_v

    svg_points = []
    circles = []
    for idx, item in enumerate(points_data):
        x = pad_left + (plot_w * idx / max(1, len(points_data) - 1))
        y = pad_top + ((max_v - item["iv"]) / span * plot_h)
        svg_points.append(f"{x:.1f},{y:.1f}")
        circles.append(
            f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3.5" fill="#60a5fa" '
            f'stroke="#dbeafe" stroke-width="1.4" />'
        )

    grid_values = [max_v, (max_v + min_v) / 2, min_v]
    grid_lines = []
    for value in grid_values:
        y = pad_top + ((max_v - value) / span * plot_h)
        grid_lines.append(
            f'<line x1="{pad_left}" y1="{y:.1f}" x2="{width - pad_right}" y2="{y:.1f}" '
            f'stroke="rgba(148,163,184,0.18)" stroke-width="1" />'
            f'<text x="{pad_left - 8}" y="{y + 4:.1f}" text-anchor="end" '
            f'fill="#94a3b8" font-size="10">{value:.0f}%</text>'
        )

    latest = points_data[-1]
    first = points_data[0]
    source = latest.get("source", "")
    source_label = "合约IV" if source == "contract" else "连续品种IV" if source == "product" else "IV历史"
    latest_x, latest_y = [float(part) for part in svg_points[-1].split(",")]
    latest_label_x = latest_x - 8 if latest_x > width - 78 else latest_x + 8
    latest_anchor = "end" if latest_x > width - 78 else "start"
    area_points = f"{pad_left},{height - pad_bottom} {' '.join(svg_points)} {width - pad_right},{height - pad_bottom}"

    svg_html = (
        f'<svg class="iv-trend-svg" viewBox="0 0 {width} {height}" role="img" '
        f'aria-label="近5日IV趋势">'
        f'<polygon points="{area_points}" fill="rgba(59,130,246,0.12)" />'
        f'{"".join(grid_lines)}'
        f'<polyline points="{" ".join(svg_points)}" fill="none" '
        f'stroke="#3b82f6" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round" />'
        f'{"".join(circles)}'
        f'<text x="{pad_left}" y="{height - 8}" text-anchor="middle" fill="#94a3b8" font-size="11">{html.escape(first["date"])}</text>'
        f'<text x="{width - pad_right}" y="{height - 8}" text-anchor="middle" fill="#94a3b8" font-size="11">{html.escape(latest["date"])}</text>'
        f'<text x="{latest_label_x:.1f}" y="{latest_y - 7:.1f}" text-anchor="{latest_anchor}" '
        f'fill="#e2e8f0" font-size="12" font-weight="700">{latest["iv"]:.2f}%</text>'
        f'</svg>'
    )
    return (
        f'<div class="iv-trend-head">'
        f'<span>{html.escape(source_label)}</span>'
        f'<strong>{html.escape(_format_trend_value(latest["iv"]))}</strong>'
        f'</div>'
        f'{svg_html}'
    )


def render_holding_trend_html(holding_history):
    if holding_history is None or len(holding_history) < 2:
        return '<div class="iv-trend-empty">暂无近5日持仓趋势</div>'

    if isinstance(holding_history, pd.DataFrame):
        records = holding_history.to_dict(orient="records")
    else:
        records = list(holding_history)

    points_data = []
    for record in records:
        if not isinstance(record, dict):
            continue
        dumb_net = to_float(record.get("dumb_net"))
        smart_net = to_float(record.get("smart_net"))
        if dumb_net is None and smart_net is None:
            continue
        points_data.append(
            {
                "date": _format_trend_date(record.get("trade_date", "")),
                "dumb_net": dumb_net or 0,
                "smart_net": smart_net or 0,
            }
        )

    if len(points_data) < 2:
        return '<div class="iv-trend-empty">暂无近5日持仓趋势</div>'

    values = [item["dumb_net"] for item in points_data] + [item["smart_net"] for item in points_data]
    min_v = min(values)
    max_v = max(values)
    if abs(max_v - min_v) < 1:
        min_v -= 1
        max_v += 1

    width, height = 320, 164
    pad_left, pad_right, pad_top, pad_bottom = 48, 18, 22, 34
    plot_w = width - pad_left - pad_right
    plot_h = height - pad_top - pad_bottom
    span = max_v - min_v

    def _line_points(field):
        points = []
        circles = []
        for idx, item in enumerate(points_data):
            x = pad_left + (plot_w * idx / max(1, len(points_data) - 1))
            y = pad_top + ((max_v - item[field]) / span * plot_h)
            points.append(f"{x:.1f},{y:.1f}")
            circles.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3" />')
        return points, circles

    dumb_points, dumb_circles = _line_points("dumb_net")
    smart_points, smart_circles = _line_points("smart_net")

    grid_values = [max_v, (max_v + min_v) / 2, min_v]
    grid_lines = []
    for value in grid_values:
        y = pad_top + ((max_v - value) / span * plot_h)
        grid_lines.append(
            f'<line x1="{pad_left}" y1="{y:.1f}" x2="{width - pad_right}" y2="{y:.1f}" '
            f'stroke="rgba(148,163,184,0.18)" stroke-width="1" />'
            f'<text x="{pad_left - 8}" y="{y + 4:.1f}" text-anchor="end" '
            f'fill="#94a3b8" font-size="10">{html.escape(_format_position_axis(value))}</text>'
        )

    latest = points_data[-1]
    first = points_data[0]

    svg_html = (
        f'<svg class="iv-trend-svg holding-trend-svg" viewBox="0 0 {width} {height}" role="img" '
        f'aria-label="近5日持仓趋势">'
        f'{"".join(grid_lines)}'
        f'<polyline points="{" ".join(dumb_points)}" fill="none" '
        f'stroke="#60a5fa" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" />'
        f'<g fill="#60a5fa" stroke="#dbeafe" stroke-width="1.2">{"".join(dumb_circles)}</g>'
        f'<polyline points="{" ".join(smart_points)}" fill="none" '
        f'stroke="#22c55e" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" />'
        f'<g fill="#22c55e" stroke="#dcfce7" stroke-width="1.2">{"".join(smart_circles)}</g>'
        f'<text x="{pad_left}" y="{height - 10}" text-anchor="middle" fill="#94a3b8" font-size="11">{html.escape(first["date"])}</text>'
        f'<text x="{width - pad_right}" y="{height - 10}" text-anchor="middle" fill="#94a3b8" font-size="11">{html.escape(latest["date"])}</text>'
        f'</svg>'
    )

    return (
        f'<div class="holding-trend-head">'
        f'<span><i class="dumb-line"></i>散户 <strong>{html.escape(_format_position_value(latest["dumb_net"]))}</strong></span>'
        f'<span><i class="smart-line"></i>机构 <strong>{html.escape(_format_position_value(latest["smart_net"]))}</strong></span>'
        f'</div>'
        f'{svg_html}'
    )


def render_focus_panel(row, iv_history=None, holding_history=None):
    description = html.escape(get_product_description(str(row.get("合约", ""))))
    tags = "".join(
        f"<span class='focus-tag'>{html.escape(tag)}</span>"
        for tag in build_scan_tags(row)
    )
    iv_rank_value = to_float(row.get("IV Rank"))
    iv_rank_display = "-" if iv_rank_value is None else f"{iv_rank_value:.0f}"

    overview_metrics = [
        focus_metric_html("当前IV", format_percent(row.get("当前IV"))),
        focus_metric_html("IV Rank", iv_rank_display),
        focus_metric_html("IV变动(日)", format_signed_number(row.get("IV变动(日)")), tone_class(row.get("IV变动(日)"))),
        focus_metric_html("IV变动(5日)", format_signed_number(row.get("IV变动(5日)")), tone_class(row.get("IV变动(5日)"))),
    ]
    price_metrics = [
        focus_metric_html("涨跌%(日)", format_signed_number(row.get("涨跌%(日)")), tone_class(row.get("涨跌%(日)"))),
        focus_metric_html("涨跌%(5日)", format_signed_number(row.get("涨跌%(5日)")), tone_class(row.get("涨跌%(5日)"))),
    ]
    holding_metrics = [
        focus_metric_html("散户(日)", format_signed_number(row.get("散户变动(日)"), decimals=0, thousands=True), tone_class(row.get("散户变动(日)"))),
        focus_metric_html("散户(5日)", format_signed_number(row.get("散户变动(5日)"), decimals=0, thousands=True), tone_class(row.get("散户变动(5日)"))),
        focus_metric_html("机构(日)", format_signed_number(row.get("机构变动(日)"), decimals=0, thousands=True), tone_class(row.get("机构变动(日)"))),
        focus_metric_html("机构(5日)", format_signed_number(row.get("机构变动(5日)"), decimals=0, thousands=True), tone_class(row.get("机构变动(5日)"))),
    ]
    iv_trend_html = render_iv_trend_html(iv_history)
    holding_trend_html = render_holding_trend_html(holding_history)

    panel_html = (
        f'<div class="focus-panel">'
        f'<div class="focus-intro-label">品种说明</div>'
        f'<div class="focus-intro-text">{description}</div>'
        f'<div class="focus-tags">{tags}</div>'
        f'<div class="focus-block">'
        f'<div class="focus-block-title">波动率概览</div>'
        f'<div class="focus-grid">{"".join(overview_metrics)}</div>'
        f'</div>'
        f'<div class="focus-block">'
        f'<div class="focus-block-title">IV趋势（近5日）</div>'
        f'{iv_trend_html}'
        f'</div>'
        f'<div class="focus-block">'
        f'<div class="focus-block-title">价格表现</div>'
        f'<div class="focus-grid">{"".join(price_metrics)}</div>'
        f'</div>'
        f'<div class="focus-block">'
        f'<div class="focus-block-title">持仓资金</div>'
        f'<div class="focus-grid">{"".join(holding_metrics)}</div>'
        f'</div>'
        f'<div class="focus-block">'
        f'<div class="focus-block-title">持仓趋势（近5日）</div>'
        f'{holding_trend_html}'
        f'</div>'
        f'</div>'
    )
    st.markdown(panel_html, unsafe_allow_html=True)


# ============================================================
# 全局CSS样式
# ============================================================

st.markdown("""
<style>
    /* === 基础重置 === */
    .block-container {
        padding: 1.15rem 1.65rem 1.25rem !important;
        max-width: 100% !important;
    }
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}

    /* === 深色主题 === */
    .stApp {
        background: radial-gradient(circle at 50% -18%, rgba(37, 99, 235, 0.13), transparent 36%),
                    linear-gradient(135deg, #08111f 0%, #0b1424 50%, #091526 100%);
        color: #e2e8f0;
    }

    /* === 侧边栏 === */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f172a 0%, #1e293b 100%);
        border-right: 1px solid #334155;
    }
    section[data-testid="stSidebar"] p, 
    section[data-testid="stSidebar"] span, 
    section[data-testid="stSidebar"] div {
        color: #94a3b8 !important;
    }

    /* === 标题 === */
    h1, h2, h3 {
        color: #f8fafc !important; 
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
        font-weight: 600;
    }

    /* === 输入框 === */
    div[data-testid="stTextInput"] input {
        background-color: rgba(30, 41, 59, 0.8) !important;
        border: 1px solid #475569 !important;
        color: #e2e8f0 !important;
        border-radius: 10px;
        padding: 12px 16px;
    }
    div[data-testid="stTextInput"] input:focus {
        border-color: #3b82f6 !important;
        box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.2) !important;
    }

    /* === 按钮 === */
    div.stButton > button {
        background: linear-gradient(135deg, #1d4ed8, #2563eb) !important;
        color: #ffffff !important;
        border: none !important;
        border-radius: 9px;
        padding: 9px 22px;
        font-weight: 600;
        box-shadow: 0 8px 18px rgba(37, 99, 235, 0.24);
        transition: all 0.2s ease;
    }
    div.stButton > button:hover {
        box-shadow: 0 6px 20px rgba(59, 130, 246, 0.5);
        transform: translateY(-1px);
    }

    /* === Tab样式 === */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: rgba(30, 41, 59, 0.5);
        padding: 6px;
        border-radius: 12px;
        border: 1px solid rgba(71, 85, 105, 0.3);
    }
    .stTabs [data-baseweb="tab"] {
        background-color: transparent;
        border-radius: 8px;
        color: #94a3b8;
        font-weight: 500;
        padding: 10px 20px;
    }
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #3b82f6, #2563eb) !important;
        color: #ffffff !important;
        box-shadow: 0 4px 12px rgba(59, 130, 246, 0.3);
    }
    .stTabs [data-baseweb="tab-panel"] {
        padding-top: 1rem;
    }

    /* === AgGrid表格 === */
    .stAgGrid {
        background-color: transparent !important;
    }
    .ag-root-wrapper {
        border: 1px solid rgba(71, 85, 105, 0.3) !important;
        border-radius: 16px !important;
        overflow: hidden;
    }

    /* === Expander === */
    .streamlit-expanderHeader {
        background-color: rgba(30, 41, 59, 0.5) !important;
        border-radius: 10px;
    }
    .data-help-grid {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 8px 14px;
        margin-top: 4px;
    }
    .data-help-grid > div {
        display: flex;
        gap: 8px;
        align-items: baseline;
        color: #cbd5e1;
        font-size: 12px;
        line-height: 1.45;
    }
    .data-help-grid b,
    .data-help-advice b {
        color: #f8fafc;
        font-size: 12px;
        white-space: nowrap;
    }
    .data-help-grid span,
    .data-help-advice span {
        color: #94a3b8;
    }
    .data-help-advice {
        margin-top: 12px;
        padding-top: 10px;
        border-top: 1px solid rgba(71, 85, 105, 0.35);
        color: #94a3b8;
        font-size: 12px;
        line-height: 1.55;
    }

    /* === Metric卡片 === */
    [data-testid="stMetric"] {
        background: linear-gradient(135deg, rgba(30, 41, 59, 0.8), rgba(15, 23, 42, 0.9));
        border: 1px solid rgba(71, 85, 105, 0.3);
        border-radius: 16px;
        padding: 20px;
    }
    [data-testid="stMetricLabel"] {
        color: #94a3b8 !important;
    }
    [data-testid="stMetricValue"] {
        color: #f1f5f9 !important;
        font-weight: 700 !important;
    }

    /* === 扫描工作台 === */
    .scan-toolbar,
    .scan-title,
    .scan-subtitle {
        display: none;
    }
    .scan-card {
        min-height: 54px;
        padding: 10px 14px;
        background: rgba(15, 27, 47, 0.66);
        border: 1px solid rgba(71, 85, 105, 0.34);
        border-radius: 10px;
        box-shadow: none;
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 10px;
    }
    .scan-card-label {
        color: #cbd5e1;
        font-size: 13px;
        font-weight: 700;
        white-space: nowrap;
    }
    .scan-card-rule {
        color: #64748b;
        font-size: 10px;
        white-space: nowrap;
    }
    .scan-card-value {
        color: #f8fafc;
        font-size: 18px;
        line-height: 1;
        font-weight: 800;
        letter-spacing: 0;
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    .scan-card-high { border-color: rgba(248, 113, 113, 0.25); }
    .scan-card-low { border-color: rgba(74, 222, 128, 0.25); }
    .scan-card-focus { border-color: rgba(96, 165, 250, 0.28); }
    .scan-card-high .scan-card-label,
    .scan-card-high .scan-card-value { color: #fca5a5; }
    .scan-card-low .scan-card-label,
    .scan-card-low .scan-card-value { color: #86efac; }
    .scan-card-focus .scan-card-label,
    .scan-card-focus .scan-card-value { color: #bfdbfe; }
    .st-key-category_filter [data-testid="stWidgetLabel"] {
        display: none;
    }
    .st-key-category_filter [data-baseweb="select"] > div {
        min-height: 54px;
        padding: 10px 14px;
        background: rgba(15, 27, 47, 0.76) !important;
        border: 1px solid rgba(71, 85, 105, 0.38) !important;
        border-radius: 10px !important;
        box-shadow: none;
        color: #f8fafc !important;
    }
    .st-key-category_filter [data-baseweb="select"] > div div,
    .st-key-category_filter [data-baseweb="select"] > div span {
        color: #f8fafc !important;
        font-size: 14px !important;
        font-weight: 800 !important;
        letter-spacing: 0;
        opacity: 1 !important;
    }
    .st-key-category_filter [data-baseweb="select"] svg {
        color: #94a3b8 !important;
        fill: #94a3b8 !important;
    }
    .scan-table-gap {
        height: 14px;
    }
    .focus-panel {
        background: rgba(15, 23, 42, 0.78);
        border: 1px solid rgba(71, 85, 105, 0.42);
        border-radius: 14px;
        padding: 14px;
        min-height: 600px;
    }
    .focus-rail-card {
        min-height: 640px;
        padding: 16px 8px;
        background: linear-gradient(180deg, rgba(30, 41, 59, 0.72), rgba(15, 23, 42, 0.86));
        border: 1px solid rgba(71, 85, 105, 0.34);
        border-radius: 12px;
        display: flex;
        flex-direction: column;
        align-items: center;
        gap: 12px;
        color: #cbd5e1;
        box-shadow: 0 18px 42px rgba(2, 6, 23, 0.18);
    }
    .focus-rail-contract {
        margin-top: 2px;
        padding-bottom: 10px;
        border-bottom: 1px solid rgba(71, 85, 105, 0.24);
        text-align: center;
        display: flex;
        flex-direction: column;
        gap: 4px;
        width: 100%;
    }
    .focus-rail-contract strong {
        color: #f8fafc;
        font-size: 12px;
        font-weight: 800;
        line-height: 1.2;
        word-break: break-all;
    }
    .focus-rail-contract em {
        color: #94a3b8;
        font-style: normal;
        font-size: 11px;
        line-height: 1.2;
    }
    .focus-rail-metric-list {
        width: 100%;
        display: flex;
        flex-direction: column;
        gap: 7px;
    }
    .focus-rail-metric {
        min-width: 0;
        padding: 7px 8px;
        border-radius: 8px;
        background: rgba(15, 23, 42, 0.44);
        border: 1px solid rgba(71, 85, 105, 0.20);
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 6px;
    }
    .focus-rail-metric span {
        flex: 0 0 auto;
        color: #64748b;
        font-size: 10px;
        font-weight: 800;
        line-height: 1;
    }
    .focus-rail-metric strong {
        min-width: 0;
        flex: 1 1 auto;
        color: #e2e8f0;
        font-size: 12px;
        line-height: 1;
        letter-spacing: 0;
        text-align: right;
        white-space: nowrap;
    }
    .focus-rail-metric strong.up { color: #f87171; }
    .focus-rail-metric strong.down { color: #4ade80; }
    .focus-rail-metric strong.flat { color: #94a3b8; }
    .focus-rail-tags {
        display: flex;
        flex-wrap: wrap;
        gap: 6px;
        justify-content: center;
        width: 100%;
    }
    .focus-rail-tags span {
        min-width: 28px;
        padding: 3px 6px;
        border-radius: 999px;
        color: #bfdbfe;
        background: rgba(37, 99, 235, 0.16);
        border: 1px solid rgba(96, 165, 250, 0.22);
        font-size: 10px;
        font-weight: 700;
        line-height: 1.2;
        text-align: center;
    }
    .st-key-rank_focus_open_btn div.stButton > button,
    .st-key-rank_focus_close_btn div.stButton > button,
    .st-key-rank_focus_open_btn button,
    .st-key-rank_focus_close_btn button {
        background: rgba(15, 27, 47, 0.76) !important;
        border: 1px solid rgba(71, 85, 105, 0.34) !important;
        box-shadow: none !important;
        border-radius: 10px !important;
        color: #cbd5e1 !important;
        min-height: 42px;
        padding: 8px 10px !important;
        font-size: 13px;
        font-weight: 800;
    }
    .st-key-rank_focus_open_btn div.stButton > button:hover,
    .st-key-rank_focus_close_btn div.stButton > button:hover,
    .st-key-rank_focus_open_btn button:hover,
    .st-key-rank_focus_close_btn button:hover {
        border-color: rgba(96, 165, 250, 0.45) !important;
        color: #f8fafc !important;
        transform: none;
    }
    .focus-intro-label {
        color: #94a3b8;
        font-size: 11px;
        font-weight: 700;
        margin-bottom: 6px;
    }
    .focus-intro-text {
        color: #e2e8f0;
        font-size: 13px;
        line-height: 1.55;
        margin-bottom: 12px;
    }
    .focus-tags {
        display: flex;
        flex-wrap: wrap;
        gap: 6px;
        margin-bottom: 14px;
    }
    .focus-tag {
        padding: 4px 8px;
        border-radius: 999px;
        background: rgba(59, 130, 246, 0.16);
        border: 1px solid rgba(96, 165, 250, 0.24);
        color: #bfdbfe;
        font-size: 11px;
        font-weight: 700;
    }
    .focus-block {
        border-top: 1px solid rgba(71, 85, 105, 0.32);
        padding-top: 12px;
        margin-top: 12px;
    }
    .focus-block-title {
        color: #cbd5e1;
        font-size: 12px;
        font-weight: 700;
        margin-bottom: 10px;
    }
    .focus-grid {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 8px;
    }
    .focus-kpi {
        background: rgba(30, 41, 59, 0.58);
        border: 1px solid rgba(71, 85, 105, 0.24);
        border-radius: 10px;
        padding: 9px 10px;
    }
    .focus-kpi-label {
        color: #94a3b8;
        font-size: 11px;
        margin-bottom: 5px;
    }
    .focus-kpi-value {
        color: #e2e8f0;
        font-size: 15px;
        font-weight: 800;
        letter-spacing: 0;
    }
    .focus-kpi-value.up { color: #f87171; }
    .focus-kpi-value.down { color: #4ade80; }
    .focus-kpi-value.flat { color: #94a3b8; }
    .iv-trend-head {
        display: flex;
        align-items: center;
        justify-content: space-between;
        color: #94a3b8;
        font-size: 11px;
        margin-bottom: 4px;
    }
    .iv-trend-head strong {
        color: #e2e8f0;
        font-size: 13px;
        letter-spacing: 0;
    }
    .iv-trend-svg {
        display: block;
        width: 100%;
        height: 154px;
        background: rgba(15, 23, 42, 0.34);
        border: 1px solid rgba(71, 85, 105, 0.22);
        border-radius: 10px;
    }
    .iv-trend-empty {
        height: 126px;
        display: flex;
        align-items: center;
        justify-content: center;
        color: #64748b;
        border: 1px dashed rgba(148, 163, 184, 0.24);
        border-radius: 10px;
        background: rgba(15, 23, 42, 0.28);
        font-size: 12px;
    }
    .holding-trend-svg {
        height: 164px;
    }
    .holding-trend-head {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 10px;
        color: #94a3b8;
        font-size: 11px;
        margin-bottom: 6px;
    }
    .holding-trend-head span {
        display: inline-flex;
        align-items: center;
        gap: 5px;
        white-space: nowrap;
    }
    .holding-trend-head strong {
        color: #e2e8f0;
        font-size: 12px;
        letter-spacing: 0;
    }
    .holding-trend-head i {
        width: 14px;
        height: 3px;
        border-radius: 999px;
        display: inline-block;
    }
    .holding-trend-head .dumb-line {
        background: #60a5fa;
    }
    .holding-trend-head .smart-line {
        background: #22c55e;
    }
    .focus-note {
        color: #94a3b8;
        font-size: 12px;
        line-height: 1.6;
        margin-top: 10px;
    }
    .empty-hint {
        padding: 24px;
        border: 1px dashed rgba(148, 163, 184, 0.32);
        border-radius: 12px;
        color: #94a3b8;
        text-align: center;
        background: rgba(15, 23, 42, 0.46);
    }

    /* === 分隔线 === */
    hr {
        border-color: rgba(71, 85, 105, 0.3) !important;
        margin: 1.5rem 0 !important;
    }
</style>
""", unsafe_allow_html=True)
inject_sidebar_toggle_style(mode="high_contrast")

# ============================================================
# 页面标题区域
# ============================================================

col_title, col_info_top, col_refresh = st.columns([3.0, 2.6, 1.2], gap="large")


def _format_latest_date(date_text):
    raw = str(date_text or "").replace("-", "").replace("/", "").strip()
    if len(raw) == 8 and raw.isdigit():
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    return raw or "未知"


def get_latest_date_from_market_data(df):
    if df is None or df.empty or '_数据日期' not in df.columns:
        return ""
    latest_values = df['_数据日期'].dropna().astype(str)
    if latest_values.empty:
        return ""
    return latest_values.iloc[0]


@st.cache_data(ttl=300)
def load_latest_data_date_fallback():
    """Fallback for old cached monitor data that was created before _数据日期 existed."""
    try:
        return de.get_latest_data_date()
    except Exception:
        return ""


with col_title:
    st.markdown("""
    <div style="display: flex; align-items: center; gap: 16px; margin-bottom: 8px;">
        <div style="
            width: 52px; height: 52px;
            background: linear-gradient(135deg, #3b82f6, #8b5cf6);
            border-radius: 14px;
            display: flex; align-items: center; justify-content: center;
            font-size: 28px;
            box-shadow: 0 0 30px rgba(59, 130, 246, 0.4);
        ">📊</div>
        <div>
            <h1 style="
                margin: 0; font-size: 32px; font-weight: 700;
                background: linear-gradient(90deg, #f8fafc, #94a3b8);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                letter-spacing: -0.5px;
            ">市场扫描器</h1>
            <p style="margin: 4px 0 0; font-size: 14px; color: #64748b;">
                追踪波动率和持仓数据动向
            </p>
        </div>
    </div>
    """, unsafe_allow_html=True)

with col_info_top:
    with st.expander("数据说明", expanded=False):
        st.markdown("""
<div class="data-help-grid">
  <div><b>IV</b><span>隐含波动率</span></div>
  <div><b>IV Rank</b><span>近一年百分位排名</span></div>
  <div><b>散户变动</b><span>散户集中席位净持仓变化，偏反向观察</span></div>
  <div><b>机构变动</b><span>机构集中席位净持仓变化，偏顺向观察</span></div>
</div>
<div class="data-help-advice">
  <b>扫描建议</b>
  <span>IV Rank &gt; 80 关注卖方机会；IV Rank &lt; 20 关注买方机会；机构流入 + 散户流出作为资金共振线索继续核对。</span>
</div>
        """, unsafe_allow_html=True)

refresh_requested = False
with col_refresh:
    refresh_requested = st.button("🔄 刷新数据", key="refresh_btn", use_container_width=True)
    latest_date_placeholder = st.empty()
    latest_date_placeholder.markdown(
        f"""
        <div style="margin-top:6px; text-align:right; font-size:12px; color:#94a3b8;">
            最新数据日期：<span style="color:#e2e8f0; font-weight:600;">读取中</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ============================================================
# 数据加载
# ============================================================

@st.cache_data(ttl=1800)  # 缓存30分钟
def load_data():
    """加载并处理数据"""
    df = de.get_comprehensive_market_data()
    if not df.empty:
        # 添加品种名称和分类
        df['品种'] = df['合约'].apply(get_product_name)
        df['分类'] = df['合约'].apply(get_product_category)
    return df


@st.cache_data(ttl=1800)
def load_focus_iv_trend(contract_name, data_date="", cache_version=FOCUS_IV_TREND_CACHE_VERSION):
    """加载右侧详情面板近5日 IV 趋势。"""
    try:
        return de.get_market_monitor_iv_trend(contract_name, points=5)
    except Exception:
        return pd.DataFrame(columns=["trade_date", "iv", "source"])


@st.cache_data(ttl=1800)
def load_focus_holding_trend(contract_name, data_date="", cache_version=FOCUS_HOLDING_TREND_CACHE_VERSION):
    """加载右侧详情面板近5日散户/机构持仓趋势。"""
    try:
        return de.get_market_monitor_holding_trend(contract_name, points=5, data_date=data_date)
    except Exception:
        return pd.DataFrame(columns=["trade_date", "dumb_net", "smart_net"])


@st.fragment
def render_focus_panel_selector(df_focus, category_key, latest_date_display):
    """局部刷新右侧详情面板，避免下拉切换时重建整张排行榜。"""
    focus_contracts = sorted(
        df_focus["合约"].astype(str).tolist(),
        key=contract_alpha_sort_key,
    )
    if not focus_contracts:
        return

    selected_contract = st.selectbox(
        "聚焦合约",
        focus_contracts,
        key=f"focus_contract_{category_key}",
        label_visibility="collapsed",
    )
    focus_row = df_focus[df_focus["合约"].astype(str) == selected_contract].iloc[0]
    focus_iv_trend = load_focus_iv_trend(
        selected_contract,
        latest_date_display,
        FOCUS_IV_TREND_CACHE_VERSION,
    )
    focus_holding_trend = load_focus_holding_trend(
        selected_contract,
        latest_date_display,
        FOCUS_HOLDING_TREND_CACHE_VERSION,
    )
    render_focus_panel(focus_row, focus_iv_trend, focus_holding_trend)


def get_focus_summary_row(df_focus, category_key):
    """读取当前聚焦合约，供收合态 rail 展示摘要。"""
    focus_contracts = sorted(
        df_focus["合约"].astype(str).tolist(),
        key=contract_alpha_sort_key,
    )
    if not focus_contracts:
        return None, ""

    state_key = f"focus_contract_{category_key}"
    selected_contract = st.session_state.get(state_key)
    if selected_contract not in focus_contracts:
        selected_contract = focus_contracts[0]
        st.session_state[state_key] = selected_contract

    focus_row = df_focus[df_focus["合约"].astype(str) == selected_contract].iloc[0]
    return focus_row, selected_contract


def render_focus_rail(row):
    """渲染收合态的轻量合约详情入口。"""
    contract_name = str(row.get("合约", ""))
    code_part = contract_name.split(" ", 1)[0]
    product_name = get_product_name(contract_name)
    rail_metrics = [
        ("IV", format_percent(row.get("当前IV")), ""),
        ("R", format_rail_iv_rank(row.get("IV Rank")), ""),
        ("Δ", format_signed_number(row.get("IV变动(日)")), tone_class(row.get("IV变动(日)"))),
        ("机", format_compact_number(row.get("机构变动(日)")), tone_class(row.get("机构变动(日)"))),
    ]
    metric_html = "".join(
        f"""
        <div class="focus-rail-metric">
            <span>{html.escape(label)}</span>
            <strong class="{html.escape(tone)}">{html.escape(value)}</strong>
        </div>
        """
        for label, value, tone in rail_metrics
    )
    tags = "".join(
        f"<span title='{html.escape(tag, quote=True)}'>{html.escape(RAIL_TAG_ABBR.get(tag, tag))}</span>"
        for tag in build_scan_tags(row)[:3]
    )
    st.markdown(
        f"""
        <div class="focus-rail-card">
            <div class="focus-rail-contract">
                <strong>{html.escape(code_part)}</strong>
                <em>{html.escape(product_name)}</em>
            </div>
            <div class="focus-rail-metric-list">{metric_html}</div>
            <div class="focus-rail-tags">{tags}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


if refresh_requested:
    # 仅清理本页 load_data 缓存，避免全局缓存被清空后引发整站重算。
    load_data.clear()
    load_focus_iv_trend.clear()
    load_focus_holding_trend.clear()
    load_latest_data_date_fallback.clear()
    if hasattr(de, "clear_comprehensive_market_data_snapshot"):
        de.clear_comprehensive_market_data_snapshot()


# 数据加载：保留缓存，移除固定 sleep，避免命中缓存时仍然“看起来很慢”
start_time = time.time()
if refresh_requested:
    with st.spinner("🔄 正在刷新市场扫描器数据..."):
        df_monitor = load_data()
else:
    df_monitor = load_data()
load_time = time.time() - start_time
latest_date_value = get_latest_date_from_market_data(df_monitor)
if not latest_date_value:
    latest_date_value = load_latest_data_date_fallback()
latest_date_display = _format_latest_date(latest_date_value)
latest_date_placeholder.markdown(
    f"""
    <div style="margin-top:6px; text-align:right; font-size:12px; color:#94a3b8;">
        最新数据日期：<span style="color:#e2e8f0; font-weight:600;">{latest_date_display}</span>
    </div>
    """,
    unsafe_allow_html=True,
)

# ============================================================
# 主内容区域
# ============================================================

if not df_monitor.empty:

    st.markdown("<div style='height: 14px'></div>", unsafe_allow_html=True)

    scan_metrics = build_scan_metrics(df_monitor)
    search_term = ""
    df_searched = apply_text_search(df_monitor, search_term).copy()

    category_counts = {}
    for category_name in CATEGORIES:
        if category_name == "全部":
            category_counts[category_name] = len(df_searched)
        else:
            category_counts[category_name] = len(df_searched[df_searched['分类'] == category_name])
    category_label_lookup = {
        category: (
            f"全部扫描 · {category_counts.get(category, 0):,}"
            if category == "全部"
            else f"{category} · {category_counts.get(category, 0):,}"
        )
        for category in CATEGORIES
    }

    scan_card_styles = {
        "高IV": "scan-card-high",
        "低IV": "scan-card-low",
        "缺IV": "scan-card-focus",
    }
    scan_chip_labels = {
        "高IV": "高IV",
        "低IV": "低IV",
        "IV升幅": "IV日升",
        "价格异动": "价格异动",
        "机构流入": "机构流入",
        "散户流出": "散户流出",
        "缺IV": "缺IV",
    }
    scan_cols = st.columns([1.35, 0.9, 0.9, 1.0, 1.1, 1.15, 1.15, 0.95], gap="small")
    with scan_cols[0]:
        selected_category = st.selectbox(
            "扫描范围",
            options=CATEGORIES,
            index=0,
            format_func=lambda category: category_label_lookup.get(category, category),
            key="category_filter",
            label_visibility="collapsed",
        )
    for scan_col, item in zip(scan_cols[1:], SCAN_FILTERS[1:]):
        css_class = scan_card_styles.get(item["key"], "scan-card")
        if css_class != "scan-card":
            css_class = f"scan-card {css_class}"
        value = scan_metrics.get(item["key"], 0)
        with scan_col:
            st.markdown(
                f"""
                    <div class="{css_class}" title="{html.escape(item["hint"])}">
                    <div>
                        <div class="scan-card-label">{html.escape(scan_chip_labels.get(item["key"], item["label"]))}</div>
                        <div class="scan-card-rule">{html.escape(item["hint"])}</div>
                    </div>
                    <div class="scan-card-value">{value:,}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    st.markdown("<div class='scan-table-gap'></div>", unsafe_allow_html=True)

    df_scanned = df_searched.copy()

    # === 分类内容 ===
    category_container = st.container()

    for category in [selected_category]:
        with category_container:
            # 筛选数据
            if category == "全部":
                df_filtered = df_scanned.copy()
            else:
                df_filtered = df_scanned[df_scanned['分类'] == category].copy()

            if df_filtered.empty:
                st.markdown(
                    f"<div class='empty-hint'>暂无符合「{html.escape(category_label_lookup.get(category, category))}」的合约</div>",
                    unsafe_allow_html=True,
                )
                continue

            df_filtered = add_scan_signal_column(df_filtered)

            # 移除品种和分类列（右侧聚焦面板使用，不在表格中重复显示）
            display_columns = [col for col in df_filtered.columns if col not in ['品种', '分类', '_数据日期', '期权到期日']]
            df_display = df_filtered[display_columns].copy()
            if "到期剩余天数" in df_display.columns:
                df_display["到期"] = df_display["到期剩余天数"].apply(format_contract_expiry_suffix)
            if "合约" in df_display.columns:
                leading_columns = ["合约"]
                if "到期" in df_display.columns:
                    leading_columns.append("到期")
                if "扫描信号" in df_display.columns:
                    leading_columns.append("扫描信号")
                df_display = df_display[
                    leading_columns
                    + [
                        col for col in df_display.columns
                        if col not in leading_columns
                    ]
                ]
            if rank_sort_expiry_active and "到期剩余天数" in df_display.columns:
                df_display["_到期排序"] = df_display["到期剩余天数"].apply(
                    lambda value: to_float(value) if to_float(value) is not None else 999999
                )
                df_display = (
                    df_display.sort_values(
                        by=["_到期排序", "合约"],
                        ascending=[True, True],
                        kind="mergesort",
                    )
                    .drop(columns=["_到期排序"])
                    .reset_index(drop=True)
                )
            if "IV Rank" in df_display.columns:
                df_display["IV Rank"] = df_display["IV Rank"].apply(format_grid_iv_rank)

            # === AgGrid配置 ===
            gb = GridOptionsBuilder.from_dataframe(df_display)

            # 基础配置
            gb.configure_default_column(
                resizable=True,
                filter=True,
                sortable=True,
                cellStyle={
                    'display': 'flex',
                    'alignItems': 'center',
                    'justifyContent': 'flex-end',
                    'paddingLeft': '10px',
                    'paddingRight': '14px'
                },
                minWidth=96
            )

            contract_renderer = JsCode("""
            class ContractRenderer {
                init(params) {
                    const contractName = params.value || '';
                    this.eGui = document.createElement('div');
                    this.eGui.style.display = 'flex';
                    this.eGui.style.alignItems = 'center';
                    this.eGui.style.width = '100%';
                    this.eGui.style.minWidth = '0';

                    const name = document.createElement('span');
                    name.textContent = contractName;
                    name.style.minWidth = '0';
                    name.style.overflow = 'hidden';
                    name.style.textOverflow = 'ellipsis';
                    name.style.whiteSpace = 'nowrap';
                    this.eGui.appendChild(name);
                }
                getGui() { return this.eGui; }
            }
            """)

            expiry_renderer = JsCode("""
            class ExpiryRenderer {
                init(params) {
                    const label = params.value || '';
                    const rawDays = params.data ? params.data['到期剩余天数'] : null;
                    const parsedDays = rawDays !== null && rawDays !== undefined && String(rawDays).trim() !== ''
                        ? Number(rawDays)
                        : NaN;

                    this.eGui = document.createElement('div');
                    this.eGui.style.display = 'flex';
                    this.eGui.style.alignItems = 'center';
                    this.eGui.style.justifyContent = 'center';
                    this.eGui.style.width = '100%';

                    if (!label) {
                        return;
                    }

                    const badge = document.createElement('span');
                    badge.style.display = 'inline-flex';
                    badge.style.alignItems = 'center';
                    badge.style.justifyContent = 'center';
                    badge.style.height = '20px';
                    badge.style.minWidth = '54px';
                    badge.style.padding = '0 8px';
                    badge.style.borderRadius = '999px';
                    badge.style.fontSize = '11px';
                    badge.style.fontWeight = '800';
                    badge.style.lineHeight = '20px';
                    badge.style.letterSpacing = '0';
                    badge.textContent = label;

                    if (label === '已到期' || (Number.isFinite(parsedDays) && parsedDays < 0)) {
                        badge.style.color = '#cbd5e1';
                        badge.style.background = 'rgba(100, 116, 139, 0.22)';
                        badge.style.border = '1px solid rgba(148, 163, 184, 0.22)';
                    } else if (label.includes('⚠') || (Number.isFinite(parsedDays) && parsedDays < 3)) {
                        badge.style.color = '#fde68a';
                        badge.style.background = 'rgba(217, 119, 6, 0.22)';
                        badge.style.border = '1px solid rgba(251, 191, 36, 0.32)';
                    } else {
                        badge.style.color = '#bfdbfe';
                        badge.style.background = 'rgba(37, 99, 235, 0.18)';
                        badge.style.border = '1px solid rgba(96, 165, 250, 0.24)';
                    }

                    this.eGui.appendChild(badge);
                }
                getGui() { return this.eGui; }
            }
            """)

            expiry_comparator = JsCode("""
            function(valueA, valueB, nodeA, nodeB) {
                const getVal = (node) => {
                    const raw = node && node.data ? node.data['到期剩余天数'] : null;
                    if (raw === null || raw === undefined || String(raw).trim() === '') return 999999;
                    const parsed = Number(raw);
                    return Number.isFinite(parsed) ? parsed : 999999;
                };
                return getVal(nodeA) - getVal(nodeB);
            }
            """)

            # 合约列
            gb.configure_column("合约",
                                pinned='left',
                                width=230,
                                filter=False,
                                cellRenderer=contract_renderer,
                                cellStyle={
                                    'fontWeight': '600',
                                    'color': '#f1f5f9',
                                    'justifyContent': 'flex-start',
                                    'paddingLeft': '14px',
                                    'paddingRight': '10px'
                                }
                                )

            if "到期" in df_display.columns:
                expiry_column_options = {
                    "pinned": "left",
                    "width": 82,
                    "filter": False,
                    "sortable": True,
                    "comparator": expiry_comparator,
                    "cellRenderer": expiry_renderer,
                    "cellStyle": {
                        'justifyContent': 'center',
                        'paddingLeft': '6px',
                        'paddingRight': '6px'
                    },
                }
                if rank_sort_expiry_active:
                    expiry_column_options["sort"] = "asc"
                gb.configure_column("到期", **expiry_column_options)

            if "到期剩余天数" in df_display.columns:
                gb.configure_column("到期剩余天数",
                                    hide=True,
                                    sortable=True,
                                    filter=False
                                    )

            signal_renderer = JsCode("""
            class SignalRenderer {
                init(params) {
                    const raw = params.value || '观察';
                    const colorMap = {
                        '高波': ['rgba(220, 38, 38, 0.25)', '#fecaca', 'rgba(248, 113, 113, 0.28)'],
                        '低波': ['rgba(22, 163, 74, 0.24)', '#bbf7d0', 'rgba(74, 222, 128, 0.28)'],
                        '缺IV': ['rgba(100, 116, 139, 0.26)', '#cbd5e1', 'rgba(148, 163, 184, 0.24)'],
                        '快到期': ['rgba(217, 119, 6, 0.22)', '#fde68a', 'rgba(251, 191, 36, 0.28)'],
                        'IV升': ['rgba(37, 99, 235, 0.24)', '#bfdbfe', 'rgba(96, 165, 250, 0.28)'],
                        '价异动': ['rgba(126, 34, 206, 0.22)', '#ddd6fe', 'rgba(168, 85, 247, 0.28)'],
                        '机构流入': ['rgba(15, 118, 110, 0.24)', '#99f6e4', 'rgba(45, 212, 191, 0.28)'],
                        '散户流出': ['rgba(30, 64, 175, 0.24)', '#bfdbfe', 'rgba(96, 165, 250, 0.28)'],
                        '观察': ['rgba(51, 65, 85, 0.26)', '#cbd5e1', 'rgba(148, 163, 184, 0.2)'],
                    };
                    const tags = String(raw).split('|').filter(Boolean);
                    this.eGui = document.createElement('div');
                    this.eGui.style.display = 'flex';
                    this.eGui.style.gap = '4px';
                    this.eGui.style.alignItems = 'center';
                    this.eGui.style.overflow = 'hidden';
                    this.eGui.style.width = '100%';
                    this.eGui.innerHTML = tags.map(tag => {
                        const colors = colorMap[tag] || colorMap['观察'];
                        return `<span style="
                            padding: 2px 6px;
                            border-radius: 999px;
                            background: ${colors[0]};
                            border: 1px solid ${colors[2]};
                            color: ${colors[1]};
                            font-size: 11px;
                            font-weight: 700;
                            white-space: nowrap;
                        ">${tag}</span>`;
                    }).join('');
                }
                getGui() { return this.eGui; }
            }
            """)

            gb.configure_column("扫描信号",
                                pinned='left',
                                width=156,
                                filter=False,
                                sortable=False,
                                cellRenderer=signal_renderer,
                                cellStyle={
                                    'justifyContent': 'flex-start',
                                    'paddingLeft': '10px',
                                    'paddingRight': '10px'
                                }
                                )

            # 涨跌配色 - 纯文字颜色，无背景
            change_style_js = JsCode("""
            function(params) {
                let val = parseFloat(params.value);
                let baseStyle = {
                    'backgroundColor': 'transparent',
                    'textAlign': 'right',
                    'paddingLeft': '10px',
                    'paddingRight': '14px'
                };
                if (isNaN(val)) return {...baseStyle, 'color': '#64748b'};
                if (val > 0) return {...baseStyle, 'color': '#f87171', 'fontWeight': '600'};
                if (val < 0) return {...baseStyle, 'color': '#4ade80', 'fontWeight': '600'};
                return {...baseStyle, 'color': '#94a3b8'};
            }
            """)

            # IV Rank 渲染器 - 优化清晰度
            iv_rank_renderer = JsCode("""
            class IVRankRenderer {
                init(params) {
                    const val = params.value;
                    this.eGui = document.createElement('div');
                    this.eGui.style.width = '100%';
                    this.eGui.style.height = '26px';
                    this.eGui.style.position = 'relative';
                    this.eGui.style.display = 'flex';
                    this.eGui.style.alignItems = 'center';
                    this.eGui.style.justifyContent = 'center';

                    if (val === '快到期') {
                        this.eGui.innerHTML = '<span style="color: #fbbf24; font-weight: 700; font-size: 12px;">快到期</span>';
                        return;
                    }

                    if (val === null || val === undefined || val === '' || val === 'N/A') {
                        this.eGui.innerHTML = `
                            <div style="
                                width: 100%; height: 22px;
                                background: rgba(100, 116, 139, 0.24);
                                border: 1px solid rgba(148, 163, 184, 0.22);
                                border-radius: 4px;
                                display: flex;
                                align-items: center;
                                justify-content: center;
                                color: #cbd5e1;
                                font-size: 12px;
                                font-weight: 700;
                            ">N/A</div>
                        `;
                        return;
                    }

                    const numVal = parseFloat(val);
                    if (Number.isNaN(numVal)) {
                        this.eGui.innerHTML = '<span style="color: #94a3b8; font-weight: 700; font-size: 12px;">N/A</span>';
                        return;
                    }
                    let barColor, textColor;

                    if (numVal > 80) {
                        barColor = '#dc2626';
                        textColor = '#fecaca';
                    } else if (numVal < 20) {
                        barColor = '#16a34a';
                        textColor = '#bbf7d0';
                    } else {
                        barColor = '#2563eb';
                        textColor = '#bfdbfe';
                    }

                    this.eGui.innerHTML = `
                        <div style="
                            position: absolute;
                            left: 0; top: 2px;
                            width: 100%; height: 22px;
                            background: #1e293b;
                            border-radius: 4px;
                        "></div>
                        <div style="
                            position: absolute;
                            left: 0; top: 2px;
                            width: ${Math.max(0, Math.min(100, numVal))}%; height: 22px;
                            background: ${barColor};
                            border-radius: 4px;
                            opacity: 0.85;
                        "></div>
                        <span style="
                            position: relative;
                            z-index: 1;
                            font-size: 12px;
                            font-weight: 700;
                            color: ${textColor};
                        ">${numVal.toFixed(0)}</span>
                    `;
                }
                getGui() { return this.eGui; }
            }
            """)

            # IV Rank 排序器
            iv_rank_comparator = JsCode("""
            function(valueA, valueB) {
                const getVal = (v) => {
                    if (v === '快到期') return -100;
                    const parsed = parseFloat(v);
                    return Number.isNaN(parsed) ? -100 : parsed;
                };
                return getVal(valueA) - getVal(valueB);
            }
            """)

            gb.configure_column("IV Rank",
                                width=120,
                                cellRenderer=iv_rank_renderer,
                                comparator=iv_rank_comparator,
                                filter="agNumberColumnFilter",
                                filterParams=GRID_NUMBER_FILTER_PARAMS,
                                filterValueGetter=make_grid_number_filter_value_getter("IV Rank")
                                )

            # 当前IV
            gb.configure_column("当前IV",
                                width=100,
                                valueFormatter=JsCode("""
                                function(params) {
                                    const raw = params.value;
                                    if (raw === null || raw === undefined || raw === '' || raw === '-' || raw === 'N/A') return '-';
                                    const val = Number(String(raw).replace('%', '').replace(',', ''));
                                    if (!Number.isFinite(val) || val <= 0) return '-';
                                    return val.toFixed(2) + '%';
                                }
                                """),
                                comparator=GRID_NUMBER_COMPARATOR,
                                filter="agNumberColumnFilter",
                                filterParams=GRID_NUMBER_FILTER_PARAMS,
                                filterValueGetter=make_grid_number_filter_value_getter("当前IV"),
                                cellStyle={
                                    'color': '#e2e8f0',
                                    'fontWeight': '600',
                                    'textAlign': 'right',
                                    'justifyContent': 'flex-end',
                                    'paddingLeft': '10px',
                                    'paddingRight': '14px'
                                }
                                )

            # 数值格式化（保留2位小数）
            number_formatter = JsCode("""
            function(params) {
                if (!params.value && params.value !== 0) return '-';
                let val = parseFloat(params.value);
                if (isNaN(val)) return '-';
                return val.toFixed(2);
            }
            """)

            # 持仓变动格式化
            hold_formatter = JsCode("""
            function(params) {
                if (!params.value && params.value !== 0) return '-';
                let val = parseInt(params.value);
                if (isNaN(val)) return '-';
                return (val > 0 ? '+' : '') + val.toLocaleString();
            }
            """)
            # IV变动、涨跌%等数值列配置
            numeric_columns = ['IV变动(日)', 'IV变动(5日)', '涨跌%(日)', '涨跌%(5日)']
            for col in numeric_columns:
                if col in df_display.columns:
                    gb.configure_column(col,
                                        cellStyle=change_style_js,
                                        valueFormatter=number_formatter,
                                        comparator=GRID_NUMBER_COMPARATOR,
                                        filter="agNumberColumnFilter",
                                        filterParams=GRID_NUMBER_FILTER_PARAMS,
                                        filterValueGetter=make_grid_number_filter_value_getter(col),
                                        width=110
                                        )

            for col in ['散户变动(日)', '散户变动(5日)', '机构变动(日)', '机构变动(5日)']:
                if col in df_display.columns:
                    gb.configure_column(col,
                                        cellStyle=change_style_js,
                                        valueFormatter=hold_formatter,
                                        comparator=GRID_NUMBER_COMPARATOR,
                                        filter="agNumberColumnFilter",
                                        filterParams=GRID_NUMBER_FILTER_PARAMS,
                                        filterValueGetter=make_grid_number_filter_value_getter(col),
                                        width=120
                                        )

            gridOptions = gb.build()
            gridOptions["localeText"] = AG_GRID_LOCALE_ZH_CN
            gridOptions = apply_grid_column_groups(gridOptions)
            gridOptions["rowHeight"] = 34
            visible_row_count = min(len(df_display), 25)
            grid_height = max(640, min(900, 82 + visible_row_count * 34))

            focus_open = bool(st.session_state.get("rank_focus_open", False))
            grid_ratio = [4.7, 1.25] if focus_open else [14.0, 0.72]
            grid_col, focus_col = st.columns(grid_ratio, gap="small")

            with grid_col:
                # 渲染表格
                AgGrid(
                    df_display,
                    gridOptions=gridOptions,
                    height=grid_height,
                    width='100%',
                    columns_auto_size_mode=ColumnsAutoSizeMode.FIT_ALL_COLUMNS_TO_VIEW,
                    allow_unsafe_jscode=True,
                    enable_enterprise_modules=False,
                    key=f"market_monitor_grid_{category}",
                    update_mode="NO_UPDATE",
                    update_on=[],
                    custom_css={
                    ".ag-root-wrapper": {
                        "background-color": "#0b1627 !important",
                        "border": "1px solid rgba(71, 85, 105, 0.30) !important",
                        "border-radius": "12px !important",
                        "box-shadow": "0 18px 44px rgba(2, 6, 23, 0.20)"
                    },
                    ".ag-header": {
                        "background-color": "#172235 !important",
                        "color": "#94a3b8 !important",
                        "border-bottom": "1px solid rgba(71, 85, 105, 0.32) !important",
                        "font-size": "12px",
                        "font-weight": "650"
                    },
                    ".ag-header-group-cell": {
                        "background-color": "#101b2d !important",
                        "border-right": "1px solid rgba(148, 163, 184, 0.13) !important",
                        "color": "#cbd5e1 !important",
                        "font-size": "12px",
                        "font-weight": "800",
                        "padding-left": "14px !important"
                    },
                    ".ag-header-group-cell-label": {
                        "justify-content": "flex-start !important"
                    },
                    ".ag-header-cell": {
                        "border-right": "1px solid rgba(148, 163, 184, 0.11) !important",
                        "box-shadow": "none",
                        "padding-left": "14px !important",
                        "padding-right": "10px !important",
                        "text-align": "left !important"
                    },
                    ".ag-header-cell-comp-wrapper": {
                        "justify-content": "flex-start !important",
                        "align-items": "center !important",
                        "width": "100% !important",
                        "min-width": "0"
                    },
                    ".ag-cell-label-container": {
                        "justify-content": "flex-start !important",
                        "align-items": "center !important",
                        "text-align": "left !important",
                        "width": "100% !important"
                    },
                    ".ag-right-aligned-header .ag-cell-label-container": {
                        "flex-direction": "row !important",
                        "justify-content": "flex-start !important",
                        "text-align": "left !important"
                    },
                    ".ag-header-cell-label": {
                        "flex": "1 1 auto !important",
                        "width": "100% !important",
                        "justify-content": "flex-start !important",
                        "text-align": "left !important",
                        "gap": "6px",
                        "min-width": "0"
                    },
                    ".ag-sort-order": {
                        "display": "none !important"
                    },
                    ".ag-sort-indicator-icon.ag-sort-order": {
                        "display": "none !important",
                        "width": "0 !important",
                        "overflow": "hidden !important"
                    },
                    ".ag-header-cell-text": {
                        "color": "#94a3b8 !important",
                        "text-align": "left !important",
                        "overflow": "hidden",
                        "text-overflow": "ellipsis",
                        "white-space": "nowrap"
                    },
                    ".ag-header-icon": {
                        "margin-left": "2px !important",
                        "opacity": "0.9"
                    },
                    ".ag-header-cell-menu-button, .ag-header-cell-filter-button": {
                        "margin-left": "6px !important",
                        "margin-right": "0 !important"
                    },
                    ".ag-right-aligned-header .ag-header-cell-label": {
                        "flex-direction": "row !important",
                        "justify-content": "flex-start !important"
                    },
                    ".ag-header-cell-filtered": {
                        "background": "rgba(239, 68, 68, 0.12) !important"
                    },
                    ".ag-header-cell-filtered .ag-header-cell-text": {
                        "color": "#f8fafc !important",
                        "font-weight": "700 !important"
                    },
                    ".ag-header-cell-filtered .ag-header-cell-text::after": {
                        "content": "' 筛选中'",
                        "margin-left": "6px",
                        "padding": "1px 5px",
                        "border-radius": "4px",
                        "background": "rgba(239, 68, 68, 0.2)",
                        "color": "#fca5a5",
                        "font-size": "10px",
                        "font-weight": "700"
                    },
                    ".ag-row": {
                        "background-color": "#0d1728 !important",
                        "color": "#e2e8f0 !important",
                        "border-bottom": "1px solid rgba(71, 85, 105, 0.16) !important"
                    },
                    ".ag-row-odd": {
                        "background-color": "#0d1728 !important"
                    },
                    ".ag-row-even": {
                        "background-color": "#101b2d !important"
                    },
                    ".ag-row-hover": {
                        "background-color": "#17243a !important"
                    },
                    ".ag-cell": {
                        "background-color": "transparent !important",
                        "border-right": "1px solid rgba(148, 163, 184, 0.055) !important",
                        "font-size": "12.5px",
                        "letter-spacing": "0"
                    },
                    ".ag-body-viewport": {
                        "background-color": "#0d1728 !important"
                    },
                    "::-webkit-scrollbar": {
                        "width": "8px",
                        "height": "8px",
                        "background": "#0f172a"
                    },
                    "::-webkit-scrollbar-thumb": {
                        "background": "#475569",
                        "border-radius": "4px"
                    },
                    "::-webkit-scrollbar-thumb:hover": {
                        "background": "#64748b"
                    }
                    }
                )

            with focus_col:
                focus_summary_row, _ = get_focus_summary_row(df_filtered, category)
                if focus_open:
                    if st.button("收起详情", key="rank_focus_close_btn", use_container_width=True):
                        st.session_state.rank_focus_open = False
                        st.rerun()
                    render_focus_panel_selector(df_filtered, category, latest_date_display)
                elif focus_summary_row is not None:
                    if st.button("详情 ›", key="rank_focus_open_btn", use_container_width=True):
                        st.session_state.rank_focus_open = True
                        st.rerun()
                    render_focus_rail(focus_summary_row)


    # 底部图例
    st.markdown("""
    <div style="
        margin-top: 16px;
        padding: 14px 20px;
        background: rgba(30, 41, 59, 0.3);
        border-radius: 12px;
        border: 1px solid rgba(71, 85, 105, 0.2);
        display: flex;
        justify-content: space-between;
        align-items: center;
        font-size: 12px;
        color: #64748b;
    ">
        <div style="display: flex; gap: 24px;">
            <span>
                <span style="display: inline-block; width: 12px; height: 12px; background: #ef4444; border-radius: 2px; margin-right: 6px;"></span>
                IV Rank > 80 (卖方机会)
            </span>
            <span>
                <span style="display: inline-block; width: 12px; height: 12px; background: #22c55e; border-radius: 2px; margin-right: 6px;"></span>
                IV Rank < 20 (买方机会)
            </span>
            <span>
                <span style="display: inline-block; width: 12px; height: 12px; background: #3b82f6; border-radius: 2px; margin-right: 6px;"></span>
                IV Rank 20-80 (中性区间)
            </span>
        </div>
        <div>
            数据来源：Tushare + 各交易所官网 · 每日盘后更新 · 缓存30分钟
        </div>
    </div>
    """, unsafe_allow_html=True)

else:
    # 无数据提示
    st.markdown("""
    <div style="
        text-align: center;
        padding: 60px 20px;
        background: rgba(30, 41, 59, 0.3);
        border-radius: 16px;
        border: 1px solid rgba(71, 85, 105, 0.3);
        margin-top: 40px;
    ">
        <div style="font-size: 64px; margin-bottom: 16px;">📭</div>
        <h3 style="color: #f1f5f9; margin-bottom: 8px;">暂无数据</h3>
        <p style="color: #64748b;">请检查数据库连接或稍后重试</p>
    </div>
    """, unsafe_allow_html=True)

    if st.button("🔍 查看详细错误"):
        st.code("""
可能的原因：
1. 数据库未连接
2. 今日尚无交易数据  
3. 数据表为空

请检查：
- .env 配置是否正确
- 数据库是否运行
- 运行数据更新脚本
        """)
