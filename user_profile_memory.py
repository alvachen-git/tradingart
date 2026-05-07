import re
from datetime import datetime
from typing import Any, Callable, Dict, List

from sqlalchemy import text


PROFILE_MEMORY_ACTIVE = "active"
PROFILE_MEMORY_SUPERSEDED = "superseded"
PROFILE_MEMORY_PENDING = "pending"

KEY_RISK_PREFERENCE = "risk_preference"
KEY_PREFERRED_PRODUCTS = "preferred_products"
KEY_ANSWER_STYLE = "answer_style"
KEY_AGE = "age"
KEY_GENDER = "gender"
KEY_HOBBIES = "hobbies"
KEY_FEARS = "fears"
KEY_DISLIKES = "dislikes"

PROFILE_MEMORY_LABELS = {
    KEY_RISK_PREFERENCE: "风险偏好",
    KEY_PREFERRED_PRODUCTS: "常看品种",
    KEY_ANSWER_STYLE: "回答偏好",
    KEY_AGE: "年龄",
    KEY_GENDER: "性别",
    KEY_HOBBIES: "爱好",
    KEY_FEARS: "害怕/担心",
    KEY_DISLIKES: "厌恶/不喜欢",
}

PROFILE_MEMORY_DISPLAY_ORDER = (
    KEY_RISK_PREFERENCE,
    KEY_PREFERRED_PRODUCTS,
    KEY_ANSWER_STYLE,
    KEY_AGE,
    KEY_GENDER,
    KEY_HOBBIES,
    KEY_FEARS,
    KEY_DISLIKES,
)

TRADING_PROFILE_KEYS = (KEY_RISK_PREFERENCE, KEY_PREFERRED_PRODUCTS, KEY_ANSWER_STYLE)
PERSONAL_PROFILE_KEYS = (KEY_AGE, KEY_GENDER, KEY_HOBBIES, KEY_FEARS, KEY_DISLIKES)
INFERRED_PROFILE_KEYS = {KEY_HOBBIES, KEY_FEARS, KEY_DISLIKES}

GUEST_USER_IDS = {"", "访客", "guest", "anonymous"}

_SCHEMA_READY_ENGINE_IDS: set[str] = set()


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _clean_text(value: Any, max_len: int = 500) -> str:
    return str(value or "").strip()[:max_len]


def _is_guest_user(user_id: str) -> bool:
    return str(user_id or "").strip().lower() in GUEST_USER_IDS


def ensure_profile_memory_table(engine) -> bool:
    if engine is None:
        return False
    dialect = str(getattr(engine.dialect, "name", "") or "").lower()
    db_name = str(getattr(getattr(engine, "url", None), "database", "") or "")
    cacheable_schema = not (dialect == "sqlite" and db_name in {"", ":memory:"})
    engine_id = f"{id(engine)}:profile_memory_v1"
    if cacheable_schema and engine_id in _SCHEMA_READY_ENGINE_IDS:
        return True

    if dialect == "sqlite":
        ddl = """
            CREATE TABLE IF NOT EXISTS user_profile_memory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id VARCHAR(255) NOT NULL,
                memory_key VARCHAR(64) NOT NULL,
                memory_value TEXT NOT NULL,
                confidence FLOAT NOT NULL DEFAULT 0.7,
                source_text TEXT NULL,
                status VARCHAR(32) NOT NULL DEFAULT 'active',
                occurrence_count INTEGER NOT NULL DEFAULT 1,
                created_at VARCHAR(40) NOT NULL,
                updated_at VARCHAR(40) NOT NULL
            )
        """
    else:
        ddl = """
            CREATE TABLE IF NOT EXISTS user_profile_memory (
                id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL,
                memory_key VARCHAR(64) NOT NULL,
                memory_value TEXT NOT NULL,
                confidence FLOAT NOT NULL DEFAULT 0.7,
                source_text TEXT NULL,
                status VARCHAR(32) NOT NULL DEFAULT 'active',
                occurrence_count INT NOT NULL DEFAULT 1,
                created_at VARCHAR(40) NOT NULL,
                updated_at VARCHAR(40) NOT NULL,
                INDEX idx_user_profile_memory_active (user_id, memory_key, status)
            )
        """
    try:
        with engine.begin() as conn:
            conn.execute(text(ddl))
        if cacheable_schema:
            _SCHEMA_READY_ENGINE_IDS.add(engine_id)
        return True
    except Exception as exc:
        print(f"[profile-memory] ensure table failed err={exc}")
        return False


def _fetch_active_by_key(conn, user_id: str, memory_key: str) -> Dict[str, Any]:
    row = conn.execute(
        text(
            """
            SELECT id, user_id, memory_key, memory_value, confidence, source_text,
                   status, occurrence_count, created_at, updated_at
            FROM user_profile_memory
            WHERE user_id=:user_id AND memory_key=:memory_key AND status=:status
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            """
        ),
        {"user_id": user_id, "memory_key": memory_key, "status": PROFILE_MEMORY_ACTIVE},
    ).mappings().fetchone()
    return dict(row or {})


def get_active_profile_memories(engine, user_id: str) -> List[Dict[str, Any]]:
    uid = _clean_text(user_id, 255)
    if _is_guest_user(uid) or not ensure_profile_memory_table(engine):
        return []
    try:
        with engine.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT id, user_id, memory_key, memory_value, confidence, source_text,
                           status, occurrence_count, created_at, updated_at
                    FROM user_profile_memory
                    WHERE user_id=:user_id AND status=:status
                    ORDER BY memory_key ASC, updated_at DESC, id DESC
                    """
                ),
                {"user_id": uid, "status": PROFILE_MEMORY_ACTIVE},
            ).mappings().fetchall()
        latest: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            item = dict(row)
            latest.setdefault(str(item.get("memory_key") or ""), item)
        return [v for k, v in latest.items() if k]
    except Exception as exc:
        print(f"[profile-memory] read active failed user={uid} err={exc}")
        return []


def upsert_profile_memory(
    engine,
    *,
    user_id: str,
    memory_key: str,
    memory_value: str,
    confidence: float = 0.9,
    source_text: str = "",
) -> bool:
    uid = _clean_text(user_id, 255)
    key = _clean_text(memory_key, 64)
    value = _clean_text(memory_value, 500)
    if _is_guest_user(uid) or key not in PROFILE_MEMORY_LABELS or not value:
        return False
    if not ensure_profile_memory_table(engine):
        return False

    now = _now_iso()
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE user_profile_memory
                    SET status=:superseded, updated_at=:updated_at
                    WHERE user_id=:user_id AND memory_key=:memory_key AND status=:active
                    """
                ),
                {
                    "user_id": uid,
                    "memory_key": key,
                    "active": PROFILE_MEMORY_ACTIVE,
                    "superseded": PROFILE_MEMORY_SUPERSEDED,
                    "updated_at": now,
                },
            )
            conn.execute(
                text(
                    """
                    INSERT INTO user_profile_memory
                        (user_id, memory_key, memory_value, confidence, source_text,
                         status, occurrence_count, created_at, updated_at)
                    VALUES
                        (:user_id, :memory_key, :memory_value, :confidence, :source_text,
                         :status, 1, :created_at, :updated_at)
                    """
                ),
                {
                    "user_id": uid,
                    "memory_key": key,
                    "memory_value": value,
                    "confidence": max(0.0, min(float(confidence or 0.9), 1.0)),
                    "source_text": _clean_text(source_text, 1000),
                    "status": PROFILE_MEMORY_ACTIVE,
                    "created_at": now,
                    "updated_at": now,
                },
            )
        return True
    except Exception as exc:
        print(f"[profile-memory] upsert failed user={uid} key={key} err={exc}")
        return False


def _record_pending_conflict(
    engine,
    *,
    user_id: str,
    memory_key: str,
    memory_value: str,
    source_text: str,
) -> int:
    uid = _clean_text(user_id, 255)
    key = _clean_text(memory_key, 64)
    value = _clean_text(memory_value, 500)
    if not ensure_profile_memory_table(engine):
        return 0
    now = _now_iso()
    try:
        with engine.begin() as conn:
            existing = conn.execute(
                text(
                    """
                    SELECT id, occurrence_count
                    FROM user_profile_memory
                    WHERE user_id=:user_id AND memory_key=:memory_key
                      AND memory_value=:memory_value AND status=:status
                    ORDER BY updated_at DESC, id DESC
                    LIMIT 1
                    """
                ),
                {
                    "user_id": uid,
                    "memory_key": key,
                    "memory_value": value,
                    "status": PROFILE_MEMORY_PENDING,
                },
            ).mappings().fetchone()
            if existing:
                count = int(existing.get("occurrence_count") or 0) + 1
                conn.execute(
                    text(
                        """
                        UPDATE user_profile_memory
                        SET occurrence_count=:count, source_text=:source_text, updated_at=:updated_at
                        WHERE id=:id
                        """
                    ),
                    {
                        "id": existing.get("id"),
                        "count": count,
                        "source_text": _clean_text(source_text, 1000),
                        "updated_at": now,
                    },
                )
                return count
            conn.execute(
                text(
                    """
                    INSERT INTO user_profile_memory
                        (user_id, memory_key, memory_value, confidence, source_text,
                         status, occurrence_count, created_at, updated_at)
                    VALUES
                        (:user_id, :memory_key, :memory_value, 0.55, :source_text,
                         :status, 1, :created_at, :updated_at)
                    """
                ),
                {
                    "user_id": uid,
                    "memory_key": key,
                    "memory_value": value,
                    "source_text": _clean_text(source_text, 1000),
                    "status": PROFILE_MEMORY_PENDING,
                    "created_at": now,
                    "updated_at": now,
                },
            )
            return 1
    except Exception as exc:
        print(f"[profile-memory] pending conflict failed user={uid} key={key} err={exc}")
        return 0


def _extract_risk_value(prompt_text: str) -> str:
    text_norm = str(prompt_text or "").lower()
    if any(k in text_norm for k in ("不喜欢太激进", "不想太激进", "不要太激进", "别太激进")):
        return "偏保守"
    if any(k in text_norm for k in ("激进", "高波动", "高风险", "进攻", "能接受波动", "能承受波动")):
        return "偏激进"
    if any(k in text_norm for k in ("保守", "稳健", "怕亏", "少亏", "低风险", "不想亏太多")):
        return "偏保守"
    return ""


def _extract_products_value(prompt_text: str) -> str:
    raw = str(prompt_text or "")
    products = []
    product_keywords = (
        "ETF期权", "etf期权", "商品期权", "股指期权", "黄金", "白银", "原油",
        "创业板期权", "科创50期权", "期货", "股票", "美股",
    )
    for keyword in product_keywords:
        if keyword in raw and keyword.upper() not in products:
            products.append("ETF期权" if keyword == "etf期权" else keyword)
    return "、".join(products[:4])


def _extract_answer_style_value(prompt_text: str) -> str:
    raw = str(prompt_text or "")
    if any(k in raw for k in ("别给我讲太基础", "不要太基础", "少讲基础", "不用科普")):
        return "少讲基础，优先给结论和执行条件"
    if any(k in raw for k in ("先给结论", "直接给结论", "结论先行")):
        return "先给结论，再给关键依据"
    if any(k in raw for k in ("别提示风险", "不要提示风险", "不用提示风险")):
        return "风险提示保持简洁，但必要风险边界仍需保留"
    return ""


def _extract_age_value(prompt_text: str) -> str:
    raw = str(prompt_text or "")
    age_match = re.search(r"(?:我是|我今年|记住我是|记住我今年)\s*(\d{1,3})\s*岁", raw)
    if age_match:
        age = int(age_match.group(1))
        if 1 <= age <= 120:
            return f"{age}岁"
    birth_year_match = re.search(r"(?:我)?\s*((?:19|20)\d{2})\s*年\s*(?:生|出生)", raw)
    if birth_year_match:
        year = int(birth_year_match.group(1))
        current_year = datetime.now().year
        if 1900 <= year <= current_year:
            return f"{year}年生"
    return ""


def _extract_gender_value(prompt_text: str) -> str:
    raw = str(prompt_text or "")
    if any(k in raw for k in ("我是女性", "我是女生", "我是女的", "记住我是女性", "记住我是女生")):
        return "女性"
    if any(k in raw for k in ("我是男性", "我是男生", "我是男的", "记住我是男性", "记住我是男生")):
        return "男性"
    if re.search(r"(^|[，,。；;\s])女(?:性|生|的)?($|[，,。；;\s])", raw):
        return "女性"
    if re.search(r"(^|[，,。；;\s])男(?:性|生|的)?($|[，,。；;\s])", raw):
        return "男性"
    return ""


def _clean_profile_fragment(value: str, max_len: int = 80) -> str:
    cleaned = re.sub(r"[。；;\n\r]+", " ", str(value or "")).strip()
    cleaned = re.sub(r"^(和|、|，|,)+", "", cleaned).strip()
    cleaned = re.split(r"[，,]", cleaned, maxsplit=1)[0].strip()
    cleaned = re.sub(r"(你|你们)?(要)?记住.*$", "", cleaned).strip()
    return cleaned[:max_len]


def _extract_hobbies_value(prompt_text: str) -> str:
    raw = str(prompt_text or "")
    patterns = (
        r"(?:我喜欢|我爱好|我平时喜欢|我平时更爱|记住我喜欢)([^。；;\n]{1,80})",
        r"(?:我的爱好是|爱好是)([^。；;\n]{1,80})",
        r"(?:^|[，,。；;\s])喜欢([^。；;\n]{1,80})",
    )
    for pattern in patterns:
        match = re.search(pattern, raw)
        if match:
            value = _clean_profile_fragment(match.group(1))
            if value.startswith(("的", "什么", "哪")):
                continue
            return value
    return ""


def _extract_fears_value(prompt_text: str) -> str:
    raw = str(prompt_text or "")
    patterns = (
        r"(?:我害怕|我怕|我担心|我最怕)([^。；;\n]{1,80})",
        r"一看到([^。；;\n]{1,40})就(?:慌|焦虑|难受)",
    )
    for pattern in patterns:
        match = re.search(pattern, raw)
        if match:
            value = _clean_profile_fragment(match.group(1))
            if pattern.startswith("一看到") and value:
                return f"看到{value}容易慌"
            return value
    return ""


def _extract_dislikes_value(prompt_text: str) -> str:
    raw = str(prompt_text or "")
    patterns = (
        r"(?:我讨厌|我厌恶|我不喜欢)([^。；;\n]{1,80})",
        r"(?:别再|不要再)([^。；;\n]{1,80})",
    )
    for pattern in patterns:
        match = re.search(pattern, raw)
        if match:
            value = _clean_profile_fragment(match.group(1))
            if value and "提示风险" not in value:
                return value
    return ""


def extract_profile_signals(prompt_text: str) -> Dict[str, str]:
    signals: Dict[str, str] = {}
    risk_value = _extract_risk_value(prompt_text)
    product_value = _extract_products_value(prompt_text)
    style_value = _extract_answer_style_value(prompt_text)
    age_value = _extract_age_value(prompt_text)
    gender_value = _extract_gender_value(prompt_text)
    hobbies_value = _extract_hobbies_value(prompt_text)
    fears_value = _extract_fears_value(prompt_text)
    dislikes_value = _extract_dislikes_value(prompt_text)
    if risk_value:
        signals[KEY_RISK_PREFERENCE] = risk_value
    if product_value:
        signals[KEY_PREFERRED_PRODUCTS] = product_value
    if style_value:
        signals[KEY_ANSWER_STYLE] = style_value
    if age_value:
        signals[KEY_AGE] = age_value
    if gender_value:
        signals[KEY_GENDER] = gender_value
    if hobbies_value:
        signals[KEY_HOBBIES] = hobbies_value
    if fears_value:
        signals[KEY_FEARS] = fears_value
    if dislikes_value:
        signals[KEY_DISLIKES] = dislikes_value
    return signals


def _has_memory_command(prompt_text: str) -> bool:
    raw = str(prompt_text or "")
    return any(k in raw for k in ("记住", "以后", "把我", "设为", "改成", "别再", "不要再", "我是", "我今年"))


def _has_profile_update_command(prompt_text: str) -> bool:
    raw = str(prompt_text or "")
    return any(
        keyword in raw
        for keyword in (
            "改成",
            "设为",
            "设置为",
            "调整为",
            "把我",
            "记住我主要",
            "记住，我主要",
            "记住我常",
            "记住我关注",
            "记住我是",
            "记住我今年",
        )
    )


def _has_no_long_term_command(prompt_text: str) -> bool:
    raw = str(prompt_text or "")
    return any(k in raw for k in ("不用记", "不要记", "别记", "只是今天", "仅限今天", "临时"))


def _looks_stable_profile_statement(prompt_text: str) -> bool:
    raw = str(prompt_text or "")
    if any(k in raw for k in ("这次", "今天", "这轮", "临时", "试一下", "试试")):
        return False
    return any(
        k in raw
        for k in (
            "我比较",
            "我一般",
            "我通常",
            "我主要",
            "我常",
            "我不喜欢",
            "我的风格",
            "我喜欢",
            "我爱好",
            "我害怕",
            "我担心",
            "我怕",
            "我讨厌",
            "我厌恶",
            "一看到",
            "我平时更爱",
        )
    )


def _looks_explicit_personal_profile_statement(prompt_text: str) -> bool:
    raw = str(prompt_text or "")
    return any(
        k in raw
        for k in (
            "我是",
            "我今年",
            "我喜欢",
            "我爱好",
            "我的爱好是",
            "我害怕",
            "我担心",
            "我怕",
            "我讨厌",
            "我厌恶",
            "我不喜欢",
        )
    )


def _is_portfolio_status_query(prompt_text: str) -> bool:
    raw = str(prompt_text or "").strip()
    if not raw:
        return False
    if any(k in raw for k in ("分析", "判断", "建议", "风险大吗", "调仓", "加仓", "减仓", "怎么做", "怎么办")):
        return False
    return any(
        keyword in raw
        for keyword in (
            "你记得我持仓吗",
            "你记得我的持仓吗",
            "你有我的持仓吗",
            "我上传过持仓吗",
            "你知道我现在持仓吗",
            "你知道我的持仓吗",
            "你记住我持仓了吗",
        )
    )


def _is_profile_query(prompt_text: str) -> bool:
    raw = str(prompt_text or "").strip()
    if not raw:
        return False
    return any(
        keyword in raw
        for keyword in (
            "我的风险偏好是什么",
            "我是什么风险偏好",
            "我的风险偏好",
            "你记住了我什么",
            "你记得我什么",
            "你记住我的什么",
            "我主要做什么",
            "我主要看什么",
            "我的回答偏好",
            "你觉得我是怎样的人",
            "你觉得我是怎么样的人",
            "你觉得我是个什么样的人",
            "我是怎样的人",
            "我是怎么样的人",
        )
    )


def _is_memory_challenge(prompt_text: str) -> bool:
    raw = str(prompt_text or "").strip()
    if not raw:
        return False
    return any(
        keyword in raw
        for keyword in (
            "我什么时候",
            "你为什么说我",
            "我哪里说过",
            "我说过吗",
            "这不是我的意思",
            "不是我的意思",
            "我没这么说",
            "我没有这么说",
            "我没做过",
            "我没有做过",
            "别这么说",
            "不要这么说",
        )
    )


def extract_profile_memory_directive(prompt_text: str) -> Dict[str, Any]:
    raw = str(prompt_text or "").strip()
    if not raw:
        return {"action": "none", "updates": {}}
    signals = extract_profile_signals(raw)
    if _is_memory_challenge(raw):
        return {"action": "challenge", "updates": {}}
    if _is_portfolio_status_query(raw):
        return {"action": "portfolio_status_query", "updates": {}}
    if _has_no_long_term_command(raw):
        return {
            "action": "temporary_only",
            "updates": signals,
            "confirmation": "好，这次按你当前说法来处理，不写入长期画像。",
        }
    if _is_profile_query(raw) and not _has_profile_update_command(raw):
        return {"action": "query", "updates": {}}
    if not _has_memory_command(raw):
        return {"action": "none", "updates": signals}

    updates = dict(signals)
    risk_match = re.search(r"风险偏好.{0,8}(?:改成|设为|设置为|调整为)([^，。；;、\n]{2,12})", raw)
    if risk_match:
        raw_value = risk_match.group(1).strip()
        if "激进" in raw_value or "高波动" in raw_value:
            updates[KEY_RISK_PREFERENCE] = "偏激进"
        elif "保守" in raw_value or "稳健" in raw_value:
            updates[KEY_RISK_PREFERENCE] = "偏保守"
        else:
            updates[KEY_RISK_PREFERENCE] = raw_value

    if "主要做" in raw or "常看" in raw or "关注" in raw:
        product_value = _extract_products_value(raw)
        if product_value:
            updates[KEY_PREFERRED_PRODUCTS] = product_value

    if not updates:
        return {"action": "none", "updates": {}}
    return {
        "action": "update",
        "updates": updates,
        "confirmation": _build_update_confirmation(updates),
    }


def _build_update_confirmation(updates: Dict[str, str]) -> str:
    if set(updates.keys()) == {KEY_RISK_PREFERENCE}:
        value = str(updates.get(KEY_RISK_PREFERENCE) or "").strip()
        if value:
            return f"好，我记住了。以后涉及策略、仓位和风险提示时，我会按{value}的口径来回答。"
    if set(updates.keys()) == {KEY_PREFERRED_PRODUCTS}:
        value = str(updates.get(KEY_PREFERRED_PRODUCTS) or "").strip()
        if value:
            return f"好，我记住了。以后聊期权策略时，我会优先按{value}的口径来展开。"
    if set(updates.keys()) == {KEY_ANSWER_STYLE}:
        value = str(updates.get(KEY_ANSWER_STYLE) or "").strip()
        if value:
            return f"好，我记住了。以后回答时我会按这个偏好处理：{value}。"
    if set(updates.keys()) == {KEY_AGE}:
        value = str(updates.get(KEY_AGE) or "").strip()
        if value:
            return f"好，我记住了。你的年龄画像是：{value}。"
    if set(updates.keys()) == {KEY_GENDER}:
        value = str(updates.get(KEY_GENDER) or "").strip()
        if value:
            return f"好，我记住了。你的性别画像是：{value}。"
    if set(updates.keys()) == {KEY_HOBBIES}:
        value = str(updates.get(KEY_HOBBIES) or "").strip()
        if value:
            return f"好，我记住了。以后聊天时我会留意你的爱好：{value}。"
    if set(updates.keys()) == {KEY_FEARS}:
        value = str(updates.get(KEY_FEARS) or "").strip()
        if value:
            return f"好，我记住了。之后涉及风险和决策压力时，我会留意你担心：{value}。"
    if set(updates.keys()) == {KEY_DISLIKES}:
        value = str(updates.get(KEY_DISLIKES) or "").strip()
        if value:
            return f"好，我记住了。以后我会尽量避开你不喜欢的表达或内容：{value}。"
    parts = []
    for key, value in updates.items():
        label = PROFILE_MEMORY_LABELS.get(key, key)
        parts.append(f"{label}：{value}")
    if not parts:
        return "好，我记住了。"
    return "好，我记住了。之后我会按这个画像辅助回答：" + "；".join(parts) + "。"


def _active_memory_map(memories: List[Dict[str, Any]]) -> Dict[str, str]:
    return {
        str(item.get("memory_key") or ""): str(item.get("memory_value") or "").strip()
        for item in memories
        if str(item.get("memory_key") or "") in PROFILE_MEMORY_LABELS and str(item.get("memory_value") or "").strip()
    }


def _build_profile_query_answer(prompt_text: str, memories: List[Dict[str, Any]]) -> str:
    raw = str(prompt_text or "")
    active = _active_memory_map(memories)
    if not active:
        return "目前我还没有记录到你的明确交易画像。你可以直接说“把我的风险偏好改成偏保守”或“记住我主要做 ETF期权”。"

    if "风险偏好" in raw:
        value = active.get(KEY_RISK_PREFERENCE)
        if value:
            return f"你当前记录的风险偏好是：{value}。我会用它来辅助策略、仓位和风险提示，但当前问题里的明确要求会优先。"
        return "目前我还没有记录到明确的风险偏好。你可以告诉我“偏保守”“偏激进”或你的具体风险边界。"

    if "主要做" in raw or "主要看" in raw:
        value = active.get(KEY_PREFERRED_PRODUCTS)
        if value:
            return f"你当前记录的常看品种是：{value}。"
        return "目前我还没有记录到你主要做什么品种。你可以说“记住我主要做 ETF期权”。"

    if "回答偏好" in raw:
        value = active.get(KEY_ANSWER_STYLE)
        if value:
            return f"你当前记录的回答偏好是：{value}。"
        return "目前我还没有记录到明确的回答偏好。"

    if any(k in raw for k in ("你觉得我是怎样的人", "你觉得我是怎么样的人", "你觉得我是个什么样的人", "我是怎样的人", "我是怎么样的人")):
        explicit_lines = []
        inferred_lines = []
        for item in memories:
            key = str(item.get("memory_key") or "")
            value = str(item.get("memory_value") or "").strip()
            if key not in PROFILE_MEMORY_LABELS or not value:
                continue
            line = f"- {PROFILE_MEMORY_LABELS[key]}：{value}"
            confidence = float(item.get("confidence") or 0.0)
            if confidence < 0.7:
                inferred_lines.append(line)
            else:
                explicit_lines.append(line)
        if not explicit_lines and not inferred_lines:
            return "目前记录还少，我还不足以判断你是什么样的人。你可以直接告诉我你的偏好、爱好或不喜欢的回答方式。"
        lines = ["我只能基于结构化画像来回答，不会编造你的历史操作。"]
        if explicit_lines:
            lines.append("明确记录：")
            lines.extend(explicit_lines)
        if inferred_lines:
            lines.append("推断印象（可能不准，你可以随时纠正）：")
            lines.extend(inferred_lines)
        return "\n".join(lines)

    lines = ["我当前记住的结构化画像是："]
    for key in PROFILE_MEMORY_DISPLAY_ORDER:
        value = active.get(key)
        if value:
            lines.append(f"- {PROFILE_MEMORY_LABELS[key]}：{value}")
    return "\n".join(lines)


def _build_portfolio_status_answer(snapshot: Dict[str, Any] | None) -> str:
    data = snapshot or {}
    try:
        recognized_count = int(data.get("recognized_count") or 0)
    except Exception:
        recognized_count = 0
    if recognized_count <= 0:
        return "目前我没有查到你的结构化持仓记录。你可以上传持仓图，我会先识别保存；需要分析或判断时你再说。"
    updated_at = str(data.get("updated_at") or "").strip()
    summary = str(data.get("summary_text") or "").strip()
    time_part = f"，更新时间是 {updated_at}" if updated_at else ""
    summary_part = f" 概况：{summary}" if summary else ""
    return (
        f"我记得你有一份结构化持仓记录，最近一次识别到 {recognized_count} 只{time_part}。"
        f"{summary_part} 你要我分析或判断时再展开。"
    )


def _delete_matching_active_memories(engine, *, user_id: str, prompt_text: str, memories: List[Dict[str, Any]]) -> int:
    raw = str(prompt_text or "")
    if not any(keyword in raw for keyword in ("删掉", "删除", "清掉", "别再提", "不要再提")):
        return 0
    matched_ids = []
    for item in memories:
        memory_id = item.get("id")
        key = str(item.get("memory_key") or "")
        value = str(item.get("memory_value") or "").strip()
        label = PROFILE_MEMORY_LABELS.get(key, key)
        if memory_id and ((value and value in raw) or (label and label in raw)):
            matched_ids.append(memory_id)
    if not matched_ids:
        return 0
    try:
        with engine.begin() as conn:
            for memory_id in matched_ids:
                conn.execute(
                    text(
                        """
                        UPDATE user_profile_memory
                        SET status=:status, updated_at=:updated_at
                        WHERE id=:id AND user_id=:user_id AND status=:active
                        """
                    ),
                    {
                        "id": memory_id,
                        "user_id": user_id,
                        "active": PROFILE_MEMORY_ACTIVE,
                        "status": PROFILE_MEMORY_SUPERSEDED,
                        "updated_at": _now_iso(),
                    },
                )
        return len(matched_ids)
    except Exception as exc:
        print(f"[profile-memory] delete matching failed user={user_id} err={exc}")
        return 0


def _build_challenge_answer(
    engine,
    *,
    user_id: str,
    prompt_text: str,
    memories: List[Dict[str, Any]],
) -> str:
    deleted_count = _delete_matching_active_memories(
        engine,
        user_id=user_id,
        prompt_text=prompt_text,
        memories=memories,
    )
    if deleted_count:
        return "你说得对，我不应该把未确认内容当成你的历史操作。我已经把匹配到的结构化画像记录停用了，后续不会按那条来回答。"

    active = _active_memory_map(memories)
    if not active:
        return "你说得对，我不应该把未确认内容当成你的历史操作。当前结构化画像里没有这条记录，后续我不会把它当作已确认事实。"

    lines = [
        "你说得对，我不应该把未确认内容当成你的历史操作。",
        "我当前结构化画像里只记录了这些内容：",
    ]
    for key in PROFILE_MEMORY_DISPLAY_ORDER:
        value = active.get(key)
        if value:
            lines.append(f"- {PROFILE_MEMORY_LABELS[key]}：{value}")
    lines.append("你提到的那条操作记录不在结构化画像里，后续我不会把它当作已确认事实。")
    return "\n".join(lines)


def _format_profile_lines(memories: List[Dict[str, Any]], keys: tuple[str, ...]) -> List[str]:
    lines: List[str] = []
    latest_by_key = {str(item.get("memory_key") or ""): item for item in memories}
    for key in keys:
        item = latest_by_key.get(key) or {}
        key = str(item.get("memory_key") or "")
        value = str(item.get("memory_value") or "").strip()
        if key in PROFILE_MEMORY_LABELS and value:
            label = PROFILE_MEMORY_LABELS[key]
            try:
                confidence = float(item.get("confidence") or 0.0)
            except Exception:
                confidence = 0.0
            if confidence and confidence < 0.7:
                label = f"{label}（推断）"
            lines.append(f"- {label}：{value}")
    return lines


def format_profile_context(
    memories: List[Dict[str, Any]],
    *,
    temporary_overrides: Dict[str, str] | None = None,
    conflict_notes: List[str] | None = None,
) -> str:
    lines: List[str] = []
    trading_lines = _format_profile_lines(memories, TRADING_PROFILE_KEYS)
    personal_lines = _format_profile_lines(memories, PERSONAL_PROFILE_KEYS)
    if trading_lines:
        lines.append("【交易画像】")
        lines.extend(trading_lines)
    if personal_lines:
        lines.append("【个人画像】")
        lines.extend(personal_lines)
    overrides = temporary_overrides or {}
    if overrides:
        lines.append("【本轮当前表达优先】")
        for key, value in overrides.items():
            label = PROFILE_MEMORY_LABELS.get(key, key)
            lines.append(f"- {label}：{value}")
    for note in conflict_notes or []:
        if note:
            lines.append(f"- 冲突处理：{note}")
    if not lines:
        return ""
    lines.append("- 安全规则：画像只辅助个性化；必要风险提示不能因用户偏好而关闭。")
    return "\n".join(lines)


def build_profile_memory_context(
    engine,
    *,
    user_id: str,
    prompt_text: str,
    portfolio_snapshot_loader: Callable[[str], Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    uid = _clean_text(user_id, 255)
    if _is_guest_user(uid):
        return {
            "profile_context": "",
            "memory_action": "guest_skip",
            "confirmation": "",
            "should_short_circuit": False,
            "temporary_overrides": {},
        }
    if not ensure_profile_memory_table(engine):
        return {
            "profile_context": "",
            "memory_action": "unavailable",
            "confirmation": "",
            "should_short_circuit": False,
            "temporary_overrides": {},
        }

    directive = extract_profile_memory_directive(prompt_text)
    action = str(directive.get("action") or "none")
    updates = {
        key: value
        for key, value in (directive.get("updates") or {}).items()
        if key in PROFILE_MEMORY_LABELS and str(value or "").strip()
    }

    if action == "update" and updates:
        for key, value in updates.items():
            upsert_profile_memory(
                engine,
                user_id=uid,
                memory_key=key,
                memory_value=value,
                confidence=0.95,
                source_text=prompt_text,
            )
        memories = get_active_profile_memories(engine, uid)
        return {
            "profile_context": format_profile_context(memories),
            "memory_action": "updated",
            "confirmation": str(directive.get("confirmation") or _build_update_confirmation(updates)),
            "should_short_circuit": True,
            "temporary_overrides": {},
        }

    memories = get_active_profile_memories(engine, uid)
    active_by_key = {str(item.get("memory_key") or ""): item for item in memories}
    temporary_overrides: Dict[str, str] = {}
    conflict_notes: List[str] = []

    if action == "query":
        return {
            "profile_context": format_profile_context(memories),
            "memory_action": "query",
            "confirmation": _build_profile_query_answer(prompt_text, memories),
            "should_short_circuit": True,
            "temporary_overrides": {},
        }

    if action == "portfolio_status_query":
        snapshot: Dict[str, Any] = {}
        if portfolio_snapshot_loader:
            try:
                snapshot = portfolio_snapshot_loader(uid) or {}
            except Exception as exc:
                print(f"[profile-memory] portfolio status read failed user={uid} err={exc}")
                snapshot = {}
        return {
            "profile_context": format_profile_context(memories),
            "memory_action": "portfolio_status_query",
            "confirmation": _build_portfolio_status_answer(snapshot),
            "should_short_circuit": True,
            "temporary_overrides": {},
        }

    if action == "challenge":
        return {
            "profile_context": format_profile_context(memories),
            "memory_action": "challenge",
            "confirmation": _build_challenge_answer(
                engine,
                user_id=uid,
                prompt_text=prompt_text,
                memories=memories,
            ),
            "should_short_circuit": True,
            "temporary_overrides": {},
        }

    if action == "temporary_only":
        temporary_overrides.update(updates)
        return {
            "profile_context": format_profile_context(
                memories,
                temporary_overrides=temporary_overrides,
                conflict_notes=["用户明确表示不用长期记忆，本轮只临时覆盖。"] if updates else [],
            ),
            "memory_action": "temporary_only",
            "confirmation": str(directive.get("confirmation") or ""),
            "should_short_circuit": False,
            "temporary_overrides": temporary_overrides,
        }

    for key, value in updates.items():
        active_value = str((active_by_key.get(key) or {}).get("memory_value") or "").strip()
        if active_value and active_value != value:
            temporary_overrides[key] = value
            count = _record_pending_conflict(
                engine,
                user_id=uid,
                memory_key=key,
                memory_value=value,
                source_text=prompt_text,
            )
            if count >= 2:
                upsert_profile_memory(
                    engine,
                    user_id=uid,
                    memory_key=key,
                    memory_value=value,
                    confidence=0.82,
                    source_text=prompt_text,
                )
                memories = get_active_profile_memories(engine, uid)
                active_by_key[key] = {"memory_value": value}
                conflict_notes.append(f"{PROFILE_MEMORY_LABELS[key]}已因重复表达更新为“{value}”。")
            else:
                conflict_notes.append(
                    f"历史画像为“{active_value}”，本轮按用户当前表达“{value}”临时回答，不自动覆盖长期画像。"
                )
        elif not active_value and (_has_memory_command(prompt_text) or _looks_stable_profile_statement(prompt_text)):
            if key in (KEY_AGE, KEY_GENDER) or _looks_explicit_personal_profile_statement(prompt_text):
                confidence = 0.9
            elif key in INFERRED_PROFILE_KEYS and not _has_memory_command(prompt_text):
                confidence = 0.55
            else:
                confidence = 0.75
            upsert_profile_memory(
                engine,
                user_id=uid,
                memory_key=key,
                memory_value=value,
                confidence=confidence,
                source_text=prompt_text,
            )
            memories = get_active_profile_memories(engine, uid)

    return {
        "profile_context": format_profile_context(
            memories,
            temporary_overrides=temporary_overrides,
            conflict_notes=conflict_notes,
        ),
        "memory_action": "context",
        "confirmation": "",
        "should_short_circuit": False,
        "temporary_overrides": temporary_overrides,
    }
