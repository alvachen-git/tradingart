from __future__ import annotations


FRESHNESS_THRESHOLD_BY_FREQ = {
    "D": 7,
    "W": 21,
    "M": 45,
    "Q": 120,
}

# FRED dates monthly observations at the first day of the reference month.
# These limits include the normal publication lag plus an ingestion buffer.
OBSERVATION_AGE_THRESHOLD_BY_CODE = {
    "CPIAUCSL": 80,
    "PCEPILFE": 100,
}


def freshness_threshold_days(frequency: str, indicator_code: str = "") -> int:
    code = str(indicator_code or "").strip().upper()
    if code in OBSERVATION_AGE_THRESHOLD_BY_CODE:
        return OBSERVATION_AGE_THRESHOLD_BY_CODE[code]
    return FRESHNESS_THRESHOLD_BY_FREQ.get(str(frequency or "D").upper(), 45)


def freshness_basis(indicator_code: str = "") -> str:
    code = str(indicator_code or "").strip().upper()
    if code in OBSERVATION_AGE_THRESHOLD_BY_CODE:
        return "release_lag_adjusted"
    return "frequency_threshold"
