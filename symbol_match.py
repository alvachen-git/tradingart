import re


def strict_futures_prefix_pattern(prefix: str) -> str:
    """
    Strict prefix match for futures codes to avoid collisions (e.g., A vs AU).
    Supports A0 / A2405 / A-2405 / A2405.DCE (case-insensitive).
    """
    if not prefix:
        return r"^$"
    p = re.sub(r"[^A-Z0-9]", "", str(prefix).upper())
    return rf"^{p}(0|[-]?[0-9])"


def sql_prefix_condition(code: str, column: str = "ts_code") -> str:
    """
    Build SQL condition for strict prefix match.
    Single-letter uses REGEXP, multi-letter uses LIKE.
    Note: LIKE percent signs are doubled to avoid DB-API % interpolation errors.
    """
    if not code:
        return "1=0"
    clean = re.sub(r"[^A-Z0-9]", "", str(code).upper())
    if len(clean) == 1:
        return f"UPPER({column}) REGEXP '{strict_futures_prefix_pattern(clean)}'"
    return f"UPPER({column}) LIKE '{clean}%%'"
