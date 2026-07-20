from __future__ import annotations

import datetime as dt
from collections.abc import Iterable


def _as_date(value: str | dt.date | dt.datetime) -> dt.date | None:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    raw = str(value or "").strip().replace("/", "-")
    if not raw:
        return None
    if len(raw) == 8 and raw.isdigit():
        raw = f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
    try:
        return dt.date.fromisoformat(raw[:10])
    except ValueError:
        return None


def select_target_monthly_expiration(
    expirations: Iterable[str | dt.date | dt.datetime],
    trade_date: str | dt.date | dt.datetime,
    *,
    requested_expiration: str | dt.date | dt.datetime | None = None,
) -> str | None:
    """Choose one stable monthly expiry for official Delta/Gamma storage.

    Candidates are limited to 7-90 DTE. The primary window is 20-60 DTE,
    then the closest expiry to 30 DTE is selected with deterministic ties.
    """
    trade_day = _as_date(trade_date)
    if trade_day is None:
        return None

    candidates: list[tuple[dt.date, int]] = []
    seen: set[dt.date] = set()
    for value in expirations:
        expiration = _as_date(value)
        if expiration is None or expiration in seen:
            continue
        seen.add(expiration)
        dte = (expiration - trade_day).days
        if 7 <= dte <= 90:
            candidates.append((expiration, dte))
    if not candidates:
        return None

    if requested_expiration is not None:
        requested = _as_date(requested_expiration)
        if requested is None:
            return None
        return requested.isoformat() if any(item[0] == requested for item in candidates) else None

    primary = [item for item in candidates if 20 <= item[1] <= 60]
    pool = primary or candidates
    expiration, _ = min(pool, key=lambda item: (abs(item[1] - 30), item[1], item[0]))
    return expiration.isoformat()
