import os
import re
import sys
from datetime import datetime

import pandas as pd
import tushare as ts
from dotenv import dotenv_values
from sqlalchemy import create_engine, text, types


EXCHANGES = ["DCE", "CZCE", "SHFE", "GFEX", "INE", "CFFEX"]
STAGING_TABLE = "commodity_option_basic_staging"
MAIN_TABLE = "commodity_option_basic"
TEMP_SWAP_TABLE = "commodity_option_basic_old"


def _load_config():
    cfg = {}
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_path):
        cfg = dotenv_values(env_path)

    def gv(key: str, default: str = "") -> str:
        v = os.getenv(key, cfg.get(key, default))
        if v is None:
            return ""
        return str(v).strip().strip('"').strip("'")

    required = ["DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_NAME", "TUSHARE_TOKEN"]
    missing = [k for k in required if not gv(k)]
    if missing:
        raise RuntimeError(f"缺少必要配置: {missing}")

    return {
        "db_user": gv("DB_USER"),
        "db_password": gv("DB_PASSWORD"),
        "db_host": gv("DB_HOST"),
        "db_port": gv("DB_PORT"),
        "db_name": gv("DB_NAME"),
        "ts_token": gv("TUSHARE_TOKEN"),
        "min_total_rows": int(gv("BASIC_MIN_TOTAL_ROWS", "3000")),
        "min_rows_ratio": float(gv("BASIC_MIN_ROWS_RATIO", "0.60")),
        "min_strike_ratio": float(gv("BASIC_MIN_STRIKE_RATIO", "0.80")),
        "baseline_min_rows": int(gv("BASIC_BASELINE_MIN_ROWS", "20")),
        "max_missing_abs": int(gv("BASIC_DAILY_MISSING_ABS", "10")),
        "max_missing_ratio": float(gv("BASIC_DAILY_MISSING_RATIO", "0.02")),
    }


def _build_engine(cfg: dict):
    db_url = (
        f"mysql+pymysql://{cfg['db_user']}:{cfg['db_password']}"
        f"@{cfg['db_host']}:{cfg['db_port']}/{cfg['db_name']}"
    )
    return create_engine(db_url, pool_pre_ping=True, pool_recycle=7200)


def _validate_token(pro):
    today = datetime.now().strftime("%Y%m%d")
    df = pro.trade_cal(exchange="SHFE", start_date=today, end_date=today, is_open="1")
    if df is None:
        raise RuntimeError("Tushare token 校验失败：trade_cal 返回 None")


def _normalize_basic_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    cols = ["ts_code", "name", "exercise_price", "maturity_date", "call_put"]
    for c in cols:
        if c not in df.columns:
            raise RuntimeError(f"opt_basic 缺少字段: {c}")

    out = df[cols].copy()
    out["ts_code"] = out["ts_code"].astype(str).str.strip().str.upper()
    out["name"] = out["name"].astype(str).str.strip()
    out["call_put"] = out["call_put"].astype(str).str.strip().str.upper().str[0]
    out = out[out["call_put"].isin(["C", "P"])]

    out["exercise_price"] = pd.to_numeric(out["exercise_price"], errors="coerce")
    out["maturity_date"] = (
        out["maturity_date"].astype(str).str.replace(r"\D", "", regex=True).str[:8]
    )

    out = out.dropna(subset=["ts_code", "exercise_price"])
    out = out[out["maturity_date"].str.match(r"^\d{8}$", na=False)]

    # 统一提取标的代码
    out["underlying"] = out["ts_code"].str.extract(r"^([A-Z]+)")[0].str.lower()
    out = out.drop_duplicates(subset=["ts_code"], keep="first")
    return out


def _fetch_all_basic(pro):
    rows = []
    for ex in EXCHANGES:
        print(f"[*] 拉取 {ex} opt_basic ...")
        df = pro.opt_basic(
            exchange=ex,
            fields="ts_code,name,exercise_price,maturity_date,call_put",
        )
        norm = _normalize_basic_df(df)
        if norm.empty:
            print(f"    [-] {ex} 无有效数据")
            continue
        rows.append(norm)
        print(f"    [√] {ex} 有效 {len(norm)} 条")

    if not rows:
        raise RuntimeError("所有交易所 opt_basic 拉取后均为空")

    merged = pd.concat(rows, ignore_index=True)
    merged = merged.drop_duplicates(subset=["ts_code"], keep="first")
    return merged


def _prepare_staging(engine):
    with engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE IF NOT EXISTS {STAGING_TABLE} LIKE {MAIN_TABLE}"))
        conn.execute(text(f"TRUNCATE TABLE {STAGING_TABLE}"))


def _insert_staging(engine, df: pd.DataFrame):
    df.to_sql(
        STAGING_TABLE,
        engine,
        if_exists="append",
        index=False,
        dtype={"exercise_price": types.Float()},
    )


def _fetch_underlying_stats(engine, table_name: str) -> pd.DataFrame:
    sql = text(
        f"""
        SELECT
            LOWER(underlying) AS underlying,
            COUNT(*) AS row_cnt,
            MAX(CASE WHEN call_put='C' THEN exercise_price END) AS call_max_strike
        FROM {table_name}
        WHERE maturity_date >= DATE_FORMAT(CURDATE(), '%Y%m%d')
        GROUP BY LOWER(underlying)
        """
    )
    return pd.read_sql(sql, engine)


def _quality_gates(engine, cfg: dict):
    # Gate 1: 总行数下限
    q_total = text(f"SELECT COUNT(*) AS cnt FROM {STAGING_TABLE}")
    total = int(pd.read_sql(q_total, engine).iloc[0]["cnt"])
    if total < cfg["min_total_rows"]:
        raise RuntimeError(f"质检失败: staging 总行数过低 {total} < {cfg['min_total_rows']}")

    # Gate 2: 相对旧表的完整性（按 underlying）
    old_stats = _fetch_underlying_stats(engine, MAIN_TABLE)
    new_stats = _fetch_underlying_stats(engine, STAGING_TABLE)

    if old_stats.empty:
        print("[i] 旧表为空，跳过按品种对比门禁")
    else:
        old_map = old_stats.set_index("underlying").to_dict("index")
        new_map = new_stats.set_index("underlying").to_dict("index")

        failures = []
        for underlying, old_v in old_map.items():
            old_rows = int(old_v["row_cnt"] or 0)
            if old_rows < cfg["baseline_min_rows"]:
                continue

            if underlying not in new_map:
                failures.append(f"{underlying}: 新表缺失")
                continue

            new_rows = int(new_map[underlying]["row_cnt"] or 0)
            if new_rows < int(old_rows * cfg["min_rows_ratio"]):
                failures.append(f"{underlying}: 行数骤降 old={old_rows}, new={new_rows}")

            old_max = old_v.get("call_max_strike")
            new_max = new_map[underlying].get("call_max_strike")
            if pd.notna(old_max) and pd.notna(new_max):
                if float(new_max) < float(old_max) * cfg["min_strike_ratio"]:
                    failures.append(
                        f"{underlying}: C行权价上限异常 old={old_max}, new={new_max}"
                    )

        if failures:
            raise RuntimeError("质检失败: " + " | ".join(failures[:20]))

    # Gate 3: 最新交易日 daily -> basic 覆盖率
    q_latest = text("SELECT MAX(trade_date) AS d FROM commodity_opt_daily")
    latest = pd.read_sql(q_latest, engine).iloc[0]["d"]
    if pd.isna(latest):
        raise RuntimeError("质检失败: commodity_opt_daily 无任何数据")

    q_cov = text(
        f"""
        SELECT
            COUNT(DISTINCT UPPER(d.ts_code)) AS total_cnt,
            COUNT(DISTINCT CASE WHEN s.ts_code IS NULL THEN UPPER(d.ts_code) END) AS miss_cnt
        FROM commodity_opt_daily d
        LEFT JOIN {STAGING_TABLE} s
          ON UPPER(d.ts_code)=UPPER(s.ts_code)
        WHERE d.trade_date = :d
        """
    )
    cov = pd.read_sql(q_cov, engine, params={"d": str(latest)}).iloc[0]
    total_cnt = int(cov["total_cnt"] or 0)
    miss_cnt = int(cov["miss_cnt"] or 0)
    miss_ratio = (miss_cnt / total_cnt) if total_cnt else 1.0

    if miss_cnt > cfg["max_missing_abs"] and miss_ratio > cfg["max_missing_ratio"]:
        raise RuntimeError(
            f"质检失败: latest={latest} daily->basic 缺口过大 miss={miss_cnt}, total={total_cnt}, ratio={miss_ratio:.2%}"
        )

    print(
        f"[√] 质检通过: total={total}, latest={latest}, miss={miss_cnt}/{total_cnt} ({miss_ratio:.2%})"
    )


def _atomic_swap(engine):
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {TEMP_SWAP_TABLE}"))
        conn.execute(
            text(
                f"""
                RENAME TABLE
                  {MAIN_TABLE} TO {TEMP_SWAP_TABLE},
                  {STAGING_TABLE} TO {MAIN_TABLE},
                  {TEMP_SWAP_TABLE} TO {STAGING_TABLE}
                """
            )
        )


def main():
    try:
        cfg = _load_config()
        engine = _build_engine(cfg)

        ts.set_token(cfg["ts_token"])
        pro = ts.pro_api()
        _validate_token(pro)

        merged = _fetch_all_basic(pro)
        print(f"[i] 合并后去重: {len(merged)} 条")

        _prepare_staging(engine)
        _insert_staging(engine, merged)
        _quality_gates(engine, cfg)
        _atomic_swap(engine)

        print("✅ commodity_option_basic 安全更新完成（staging->main 已原子替换）")
    except Exception as e:
        print(f"❌ commodity_option_basic 安全更新失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
