import argparse
import os
import re
import sys
import time
from datetime import datetime

import pandas as pd
import requests
import tushare as ts
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(override=True)

DEFAULT_SYMBOLS = [
    "510050.SH",
    "510300.SH",
    "510500.SH",
    "588000.SH",
    "159915.SZ",
]

SYMBOL_NAME_MAP = {
    "510050.SH": "上证50ETF",
    "510300.SH": "沪深300ETF",
    "510500.SH": "中证500ETF",
    "588000.SH": "科创50ETF",
    "159915.SZ": "创业板ETF",
}

_TS_CODE_RE = re.compile(r"^\d{6}\.(SH|SZ)$", re.IGNORECASE)


def _parse_args():
    parser = argparse.ArgumentParser(
        description="中午更新 ETF 期权标的实时价到 stock_price"
    )
    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="仅打印解析结果，不写库",
    )
    parser.add_argument(
        "--trade-date",
        default=datetime.now().strftime("%Y%m%d"),
        help="写入交易日，格式 YYYYMMDD，默认当天",
    )
    parser.add_argument(
        "--symbols",
        default=",".join(DEFAULT_SYMBOLS),
        help="TS 代码列表，逗号分隔，例如 510050.SH,159915.SZ",
    )
    return parser.parse_args()


def _build_engine():
    required = ["DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_NAME"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f".env 缺少数据库配置: {missing}")

    db_url = (
        f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    return create_engine(db_url, pool_recycle=3600)


def _build_tushare_client():
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        raise RuntimeError(".env 缺少 TUSHARE_TOKEN")
    ts.set_token(token)
    return ts.pro_api(timeout=120)


def _normalize_symbols(raw_symbols):
    items = [x.strip().upper() for x in str(raw_symbols).split(",") if x.strip()]
    if not items:
        raise ValueError("symbols 为空")

    invalid = [s for s in items if not _TS_CODE_RE.match(s)]
    if invalid:
        raise ValueError(f"非法 symbols: {invalid}")

    unique = []
    seen = set()
    for s in items:
        if s not in seen:
            unique.append(s)
            seen.add(s)
    return unique


def _to_sina_code(ts_code):
    code, market = ts_code.split(".")
    market = market.upper()
    if market == "SH":
        return f"sh{code}"
    if market == "SZ":
        return f"sz{code}"
    return ""


def _is_trading_day(pro, trade_date):
    try:
        cal = pro.trade_cal(
            exchange="SSE",
            start_date=trade_date,
            end_date=trade_date,
            fields="cal_date,is_open",
        )
        if cal is None or cal.empty:
            print(f"[-] trade_cal empty for {trade_date}, skip for safety.")
            return False
        return int(cal.iloc[0].get("is_open", 0)) == 1
    except Exception as exc:
        print(f"[-] trade_cal check failed: {exc}. skip for safety.")
        return False


def _parse_sina_line(line):
    m = re.match(r'var hq_str_(\w+)="(.*)";', line.strip())
    if not m:
        return None, None
    sina_code = m.group(1)
    values = m.group(2).split(",")
    if len(values) < 10:
        return sina_code, None

    try:
        name = (values[0] or "").strip()
        open_price = float(values[1] or 0)
        pre_close = float(values[2] or 0)
        close_price = float(values[3] or 0)
        high_price = float(values[4] or 0)
        low_price = float(values[5] or 0)
        vol = float(values[8] or 0)
        amount = float(values[9] or 0)
    except Exception:
        return sina_code, None

    if close_price <= 0 and pre_close > 0:
        close_price = pre_close
    if open_price <= 0 and close_price > 0:
        open_price = close_price
    if high_price <= 0 and close_price > 0:
        high_price = close_price
    if low_price <= 0 and close_price > 0:
        low_price = close_price

    pct_chg = 0.0
    if pre_close > 0:
        pct_chg = (close_price - pre_close) / pre_close * 100

    return sina_code, {
        "name": name,
        "open_price": open_price,
        "high_price": high_price,
        "low_price": low_price,
        "close_price": close_price,
        "vol": vol,
        "amount": amount,
        "pct_chg": pct_chg,
    }


def _fetch_sina_quotes(sina_codes, retries=3, timeout=(2, 3)):
    if not sina_codes:
        return {}

    session = requests.Session()
    session.trust_env = False
    session.headers.update(
        {
            "Referer": "https://finance.sina.com.cn",
            "User-Agent": "Mozilla/5.0",
        }
    )

    url = "https://hq.sinajs.cn/list=" + ",".join(sina_codes)
    last_exc = None
    try:
        for i in range(retries):
            try:
                resp = session.get(url, timeout=timeout)
                text_raw = resp.content.decode("gbk", errors="replace")
                parsed = {}
                for line in text_raw.splitlines():
                    sina_code, data = _parse_sina_line(line)
                    if sina_code and data:
                        parsed[sina_code] = data
                return parsed
            except Exception as exc:
                last_exc = exc
                print(f"[warn] 新浪请求失败，重试 {i + 1}/{retries}: {exc}")
                time.sleep(0.5)
    finally:
        session.close()

    if last_exc:
        raise RuntimeError(f"新浪行情请求失败: {last_exc}")
    return {}


def _build_rows(symbols, trade_date, quote_map):
    rows = []
    for ts_code in symbols:
        sina_code = _to_sina_code(ts_code)
        quote = quote_map.get(sina_code)
        if not quote:
            print(f"[warn] 未拿到行情: {ts_code} ({sina_code})")
            continue
        row = {
            "trade_date": trade_date,
            "ts_code": ts_code,
            "name": quote.get("name") or SYMBOL_NAME_MAP.get(ts_code, ts_code),
            "open_price": float(quote.get("open_price", 0.0) or 0.0),
            "high_price": float(quote.get("high_price", 0.0) or 0.0),
            "low_price": float(quote.get("low_price", 0.0) or 0.0),
            "close_price": float(quote.get("close_price", 0.0) or 0.0),
            "vol": float(quote.get("vol", 0.0) or 0.0),
            "amount": float(quote.get("amount", 0.0) or 0.0),
            "pct_chg": float(quote.get("pct_chg", 0.0) or 0.0),
        }
        rows.append(row)
    return rows


def _save_rows(engine, rows, trade_date):
    symbols = [r["ts_code"] for r in rows]
    # SQLAlchemy text+IN 绑定在不同版本兼容性不稳定，这里显式构造占位符。
    in_placeholders = ", ".join([f":s{i}" for i in range(len(symbols))])
    sql = text(
        f"""
        DELETE FROM stock_price
        WHERE trade_date = :trade_date
          AND ts_code IN ({in_placeholders})
        """
    )
    params = {"trade_date": trade_date}
    params.update({f"s{i}": s for i, s in enumerate(symbols)})

    with engine.connect() as conn:
        conn.execute(sql, params)
        conn.commit()

    df = pd.DataFrame(rows)
    df.to_sql("stock_price", engine, if_exists="append", index=False, chunksize=2000)


def run():
    args = _parse_args()
    symbols = _normalize_symbols(args.symbols)
    trade_date = str(args.trade_date).strip()

    if not re.match(r"^\d{8}$", trade_date):
        raise ValueError("--trade-date 必须是 YYYYMMDD")

    print(
        f"[Noon ETF Underlying] start trade_date={trade_date}, "
        f"symbols={symbols}, test_mode={args.test_mode}"
    )

    pro = _build_tushare_client()

    # 中午任务默认只处理当前交易日。传历史日期时不做交易日门禁，方便排障演练。
    today = datetime.now().strftime("%Y%m%d")
    if trade_date == today:
        if datetime.now().weekday() >= 5:
            print(f"[-] weekend ({trade_date}), skip.")
            return 0
        if not _is_trading_day(pro, trade_date):
            print(f"[-] non-trading day ({trade_date}), skip.")
            return 0

    sina_codes = [_to_sina_code(s) for s in symbols]
    quote_map = _fetch_sina_quotes(sina_codes, retries=3, timeout=(2, 3))
    rows = _build_rows(symbols, trade_date, quote_map)

    if not rows:
        raise RuntimeError("无可写入的 ETF 实时行情行")

    df = pd.DataFrame(rows)
    print(f"[*] 准备写入 rows={len(df)}")
    print(df.to_markdown(index=False))

    if args.test_mode:
        print("[TEST] test-mode enabled, skip DB write.")
        return 0

    engine = _build_engine()
    _save_rows(engine, rows, trade_date)
    print(f"[ok] 写入完成 trade_date={trade_date}, rows={len(rows)}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(run())
    except Exception as exc:
        print(f"[error] {exc}")
        sys.exit(1)
