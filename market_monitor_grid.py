import json
import math

from st_aggrid import JsCode


AG_GRID_LOCALE_ZH_CN = {
    "selectAll": "全选",
    "selectAllSearchResults": "全选搜索结果",
    "searchOoo": "搜索...",
    "blanks": "空白",
    "noMatches": "无匹配项",
    "filterOoo": "筛选...",
    "equals": "等于",
    "notEqual": "不等于",
    "blank": "为空",
    "notBlank": "非空",
    "empty": "请选择",
    "lessThan": "小于",
    "lessThanOrEqual": "小于等于",
    "greaterThan": "大于",
    "greaterThanOrEqual": "大于等于",
    "inRange": "介于",
    "inRangeStart": "起始值",
    "inRangeEnd": "结束值",
    "contains": "包含",
    "notContains": "不包含",
    "startsWith": "开头是",
    "endsWith": "结尾是",
    "andCondition": "且",
    "orCondition": "或",
    "applyFilter": "应用",
    "resetFilter": "重置",
    "clearFilter": "清除",
    "cancelFilter": "取消",
    "columns": "列",
    "filters": "筛选",
    "noRowsToShow": "暂无数据",
    "loadingOoo": "加载中...",
}


GRID_NUMBER_FILTER_PARAMS = {
    "filterOptions": [
        "greaterThan",
        "greaterThanOrEqual",
        "lessThan",
        "lessThanOrEqual",
        "equals",
        "notEqual",
        "inRange",
        "blank",
        "notBlank",
    ],
    "defaultOption": "greaterThan",
    "debounceMs": 150,
    "maxNumConditions": 1,
    "numAlwaysVisibleConditions": 1,
    "includeBlanksInEquals": False,
    "includeBlanksInLessThan": False,
    "includeBlanksInGreaterThan": False,
    "numberParser": JsCode(
        """
        function(text) {
            if (text === null || text === undefined || text === '') return null;
            const normalized = String(text).replace(/[%+,\\s]/g, '');
            const value = Number(normalized);
            return Number.isFinite(value) ? value : null;
        }
        """
    ),
}


GRID_NUMBER_COMPARATOR = JsCode(
    """
    function(valueA, valueB) {
        const normalize = (value) => {
            if (value === null || value === undefined || value === '' || value === '快到期') return null;
            if (typeof value === 'number') return Number.isFinite(value) ? value : null;
            const parsed = Number(String(value).replace(/[%+,\\s]/g, ''));
            return Number.isFinite(parsed) ? parsed : null;
        };
        const a = normalize(valueA);
        const b = normalize(valueB);
        if (a === null && b === null) return 0;
        if (a === null) return -1;
        if (b === null) return 1;
        return a - b;
    }
    """
)


def make_grid_number_filter_value_getter(field):
    field_name = json.dumps(field, ensure_ascii=False)
    return JsCode(
        f"""
        function(params) {{
            const value = params.data && params.data[{field_name}];
            if (value === null || value === undefined || value === '' || value === '快到期') return null;
            if (typeof value === 'number') return Number.isFinite(value) ? value : null;
            const parsed = Number(String(value).replace(/[%+,\\s]/g, ''));
            return Number.isFinite(parsed) ? parsed : null;
        }}
        """
    )


def _to_optional_float(value):
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
    if math.isnan(number):
        return None
    return number


def format_contract_expiry_suffix(days_left):
    number = _to_optional_float(days_left)
    if number is None:
        return ""
    days = int(number)
    if days < 0:
        return "已到期"
    prefix = "⚠ " if days < 3 else ""
    return f"{prefix}D-{days}"


def format_contract_for_grid(row):
    contract_name = str(row.get("合约") or "")
    suffix = format_contract_expiry_suffix(row.get("到期剩余天数"))
    return f"{contract_name} {suffix}" if suffix else contract_name
