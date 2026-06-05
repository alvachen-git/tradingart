import tushare as ts
import pandas as pd
import akshare as ak
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import time
from datetime import datetime, timedelta

# 1. 初始化
load_dotenv(override=True)
db_url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_url)
TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN")
pro = ts.pro_api(TUSHARE_TOKEN) if TUSHARE_TOKEN else None
DEFAULT_INDEX_UPDATE_ALERT_EMAIL = "alvachenart@163.com"

_PROXY_ENV_KEYS = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
)
_NO_PROXY_ENV_KEYS = ("NO_PROXY", "no_proxy")


def _clear_proxy_env_for_market_fetch():
    """Avoid letting AkShare inherit broken local/server proxy settings."""
    cleared = []
    for key in _PROXY_ENV_KEYS:
        if os.environ.pop(key, None):
            cleared.append(key)
    for key in _NO_PROXY_ENV_KEYS:
        if os.environ.get(key) != "*":
            os.environ[key] = "*"
            cleared.append(key)
    return cleared


def _hk_spot_code_candidates(ts_code):
    code = str(ts_code or "").strip().upper()
    if not code:
        return []
    candidates = [code]
    if code.startswith("HK") and len(code) > 2:
        candidates.append(code[2:])
    else:
        candidates.append(f"HK{code}")
    return list(dict.fromkeys(candidates))


def _match_hk_spot_index_row(df_spot, ts_code):
    if df_spot is None or df_spot.empty or "代码" not in df_spot.columns:
        return None
    candidates = set(_hk_spot_code_candidates(ts_code))
    codes = df_spot["代码"].astype(str).str.strip().str.upper()
    matched = df_spot[codes.isin(candidates)]
    if matched.empty:
        return None
    return matched.iloc[0]


def _split_alert_recipients(raw_value):
    if not raw_value:
        return []
    return [part.strip() for part in str(raw_value).replace(";", ",").split(",") if part.strip()]


def _index_update_alert_recipients(alert_email=None):
    configured = alert_email or os.getenv("INDEX_UPDATE_ALERT_EMAIL") or os.getenv("INDEX_UPDATE_ALERT_EMAILS")
    recipients = _split_alert_recipients(configured)
    return recipients or [DEFAULT_INDEX_UPDATE_ALERT_EMAIL]


def _should_send_hk_update_alert(start_date, end_date, now=None):
    now = now or datetime.now()
    today = now.strftime("%Y%m%d")
    if str(start_date) != str(end_date) or str(end_date) != today:
        return False
    # 港股 16:00 收盘，16:20 后仍未写到今天才告警。
    return (now.hour, now.minute) >= (16, 20)


def _build_hk_update_alert(subject_date, failures, generated_at=None):
    generated_at = generated_at or datetime.now()
    rows = []
    for item in failures:
        source_errors = item.get("source_errors") or []
        source_error_html = "<br>".join(source_errors[-6:]) if source_errors else "无"
        rows.append(
            "<tr>"
            f"<td>{item.get('ts_code', '')}</td>"
            f"<td>{item.get('name', '')}</td>"
            f"<td>{item.get('reason', '')}</td>"
            f"<td>{item.get('latest_date') or '-'}</td>"
            f"<td>{item.get('source') or '-'}</td>"
            f"<td>{source_error_html}</td>"
            "</tr>"
        )
    table_rows = "\n".join(rows)
    subject = f"【TradingArt告警】港股指数更新失败 {subject_date}"
    html = f"""
    <div style="font-family: Arial, sans-serif; line-height: 1.6;">
      <h2>TradingArt 港股指数更新失败</h2>
      <p>目标日期：<strong>{subject_date}</strong></p>
      <p>生成时间：{generated_at.strftime('%Y-%m-%d %H:%M:%S')}</p>
      <table border="1" cellspacing="0" cellpadding="6" style="border-collapse: collapse;">
        <thead>
          <tr>
            <th>代码</th>
            <th>名称</th>
            <th>失败原因</th>
            <th>源端最新日期</th>
            <th>选中来源</th>
            <th>源端错误摘要</th>
          </tr>
        </thead>
        <tbody>
          {table_rows}
        </tbody>
      </table>
      <p>请检查服务器定时任务日志、AkShare/Tushare 数据源、代理环境和 index_price 最新日期。</p>
    </div>
    """
    return subject, html


def _send_hk_update_failure_alert(failures, start_date, end_date, alert_email=None, email_sender=None):
    if not failures:
        return True
    recipients = _index_update_alert_recipients(alert_email)
    subject, html = _build_hk_update_alert(end_date, failures)
    if email_sender is None:
        from email_utils2 import send_email as email_sender
    ok = True
    for recipient in recipients:
        try:
            sent = bool(email_sender(recipient, subject, html))
        except Exception as exc:
            sent = False
            print(f"❌ 港股指数更新告警邮件异常: {recipient} | {exc}")
        print(f"{'✅' if sent else '❌'} 港股指数更新告警邮件: {recipient}")
        ok = ok and sent
    return ok


def init_index_table():
    """初始化指数价格表"""
    with engine.connect() as conn:
        sql = """
              CREATE TABLE IF NOT EXISTS index_price \
              ( \
                  trade_date \
                  VARCHAR \
              ( \
                  20 \
              ),
                  ts_code VARCHAR \
              ( \
                  20 \
              ),
                  open_price FLOAT,
                  high_price FLOAT,
                  low_price FLOAT,
                  close_price FLOAT,
                  pct_chg FLOAT,
                  vol FLOAT,
                  amount FLOAT,
                  PRIMARY KEY \
              ( \
                  trade_date, \
                  ts_code \
              )
                  ) DEFAULT CHARSET=utf8mb4;
              """
        conn.execute(text(sql))


def fetch_and_save_indices(start_date, end_date):
    # 指数列表
    indices = {
        '000001.SH': '上证指数',
        '399001.SZ': '深证成指',
        '000300.SH': '沪深300',
        '000905.SH': '中证500',
        '000852.SH': '中证1000',
        '000688.SH': '科创50',
        '399006.SZ': '创业板指',
        '000016.SH': '上证50',
        '399005.SZ': '中小100',
        '932000.CSI': '中证2000',
    }

    print(f"🚀 开始拉取指数数据: {start_date} 至 {end_date} ...")

    for code, name in indices.items():
        try:
            # 1. 清理旧数据 (幂等性)
            with engine.connect() as conn:
                del_sql = text(
                    f"DELETE FROM index_price WHERE ts_code='{code}' AND trade_date >= '{start_date}' AND trade_date <= '{end_date}'")
                conn.execute(del_sql)
                conn.commit()

            # 2. 调用接口
            df = pro.index_daily(ts_code=code, start_date=start_date, end_date=end_date)

            if not df.empty:
                rename_map = {
                    'close': 'close_price',
                    'open': 'open_price',
                    'high': 'high_price',
                    'low': 'low_price',
                }
                df = df.rename(columns=rename_map)

                cols_to_save = ['trade_date', 'ts_code', 'open_price', 'high_price',
                                'low_price', 'close_price', 'pct_chg', 'vol', 'amount']
                final_cols = [c for c in cols_to_save if c in df.columns]

                # 3. 入库
                df[final_cols].to_sql('index_price', engine, if_exists='append', index=False)
                print(f"   [√] {name} 更新成功")
            else:
                print(f"   [-] {name} 无数据 (可能是非交易日或收盘前)")

            time.sleep(0.3)

        except Exception as e:
            print(f"   [x] {name} 异常: {e}")


def fetch_and_save_hk_indices(start_date, end_date, send_alert=True, alert_email=None, email_sender=None):
    """
    抓取港股核心指数 (使用 AkShare)
    并自动计算涨跌幅
    """
    # 强制转为字符串，防止报错
    start_date = str(start_date)
    end_date = str(end_date)

    # 映射表: AkShare代码 -> 中文名
    # HSI: 恒生指数, HSTECH: 恒生科技指数
    hk_indices = {
        'HSI': '恒生指数',
        'HSTECH': '恒生科技指数'
    }

    cleared_proxy_keys = _clear_proxy_env_for_market_fetch()
    if cleared_proxy_keys:
        print(f"   [i] 港股指数抓取已清理代理环境变量: {','.join(cleared_proxy_keys)}")

    print(f"🚀 [港股] 开始拉取指数数据: {start_date} 至 {end_date} ...")
    failures = []

    for symbol, name in hk_indices.items():
        source_errors = []
        try:
            # 1. 拉取数据（多源容错）
            source_used = None
            df = pd.DataFrame()
            latest_from_source = None
            source_results = []

            def fetch_tushare_hk_index(ts_code):
                if pro is None:
                    raise ValueError("TUSHARE_TOKEN 缺失，无法使用 tushare 兜底")
                # 取较长历史区间，便于后续统一计算 pct_chg
                df_ts = pro.query("index_global", ts_code=ts_code, start_date="20000101", end_date=end_date)
                if df_ts is None or df_ts.empty:
                    return pd.DataFrame()
                rename_map_ts = {
                    "trade_date": "date",
                    "vol": "volume",
                }
                df_ts = df_ts.rename(columns=rename_map_ts)
                return df_ts

            def fetch_hk_spot_today(ts_code):
                """当日实时兜底：仅在 end_date=今天且收盘后尝试抓取快照。"""
                today_str = datetime.now().strftime('%Y%m%d')
                if end_date != today_str:
                    return pd.DataFrame()
                # 港股 16:00 收盘，保守留到 16:20 后再使用快照写入日线
                if datetime.now().hour < 16:
                    return pd.DataFrame()
                df_spot = ak.stock_hk_index_spot_sina()
                if df_spot is None or df_spot.empty:
                    return pd.DataFrame()
                row = _match_hk_spot_index_row(df_spot, ts_code)
                if row is None:
                    return pd.DataFrame()
                return pd.DataFrame([{
                    'date': today_str,
                    'open': row.get('今开', 0),
                    'high': row.get('最高', 0),
                    'low': row.get('最低', 0),
                    'close': row.get('最新价', 0),
                    'pct_chg': row.get('涨跌幅', 0),
                    'volume': 0,
                }])

            source_candidates = [
                ("tushare", lambda: fetch_tushare_hk_index(symbol)),
                ("sina", lambda: ak.stock_hk_index_daily_sina(symbol=symbol)),
                ("em", lambda: ak.stock_hk_index_daily_em(symbol=symbol)),
                ("spot_today", lambda: fetch_hk_spot_today(symbol)),
            ]
            for source_name, fetcher in source_candidates:
                try:
                    retries_map = {"sina": 2, "em": 4, "tushare": 3, "spot_today": 2}
                    max_retries = retries_map.get(source_name, 2)
                    raw_df = None
                    for attempt in range(1, max_retries + 1):
                        try:
                            raw_df = fetcher()
                            if attempt > 1:
                                print(f"   [i] {name} {source_name} 第 {attempt} 次重试成功")
                            break
                        except Exception as retry_err:
                            if attempt < max_retries:
                                wait_s = min(8, attempt * 1.5)
                                print(
                                    f"   [!] {name} {source_name} 第 {attempt}/{max_retries} 次失败: {retry_err}，{wait_s:.1f}s 后重试"
                                )
                                time.sleep(wait_s)
                            else:
                                source_errors.append(f"{source_name}: {retry_err}")
                                print(
                                    f"   [x] {name} {source_name} 第 {attempt}/{max_retries} 次失败: {retry_err}"
                                )

                    if raw_df is None:
                        continue

                    if raw_df is None or raw_df.empty:
                        source_errors.append(f"{source_name}: empty")
                        print(f"   [-] {name} {source_name} 返回空数据")
                        continue

                    one_df = raw_df.copy()
                    # em 接口字段是 latest，这里统一成 close
                    if source_name == "em" and "latest" in one_df.columns and "close" not in one_df.columns:
                        one_df["close"] = one_df["latest"]

                    if 'date' not in one_df.columns or 'close' not in one_df.columns:
                        source_errors.append(f"{source_name}: missing columns {one_df.columns.tolist()}")
                        print(f"   [!] {name} {source_name} 缺少关键列: {one_df.columns.tolist()}")
                        continue

                    # 统一日期格式
                    one_df['trade_date'] = pd.to_datetime(one_df['date'], errors='coerce').dt.strftime('%Y%m%d')
                    one_df = one_df.dropna(subset=['trade_date']).copy()
                    if one_df.empty:
                        source_errors.append(f"{source_name}: invalid dates")
                        print(f"   [-] {name} {source_name} 日期解析后为空")
                        continue

                    latest_from_source = one_df['trade_date'].max()
                    print(f"   [i] {name} {source_name} 最新日期: {latest_from_source}")
                    source_results.append((latest_from_source, source_name, one_df))
                except Exception as source_err:
                    source_errors.append(f"{source_name}: {source_err}")
                    print(f"   [x] {name} {source_name} 异常: {source_err}")
                    continue

            if not source_results:
                failures.append({
                    "ts_code": symbol,
                    "name": name,
                    "reason": "所有数据源均不可用",
                    "latest_date": None,
                    "source": None,
                    "source_errors": source_errors,
                })
                print(f"   [-] {name} 所有数据源均不可用")
                continue

            # 选择“最新日期”更大的来源，避免主源停更但仍命中 start_date 的情况
            source_results.sort(key=lambda x: x[0], reverse=True)
            latest_from_source, source_used, df = source_results[0]
            if latest_from_source < end_date:
                source_errors.append(f"all_sources_latest={latest_from_source} < target={end_date}")
                print(
                    f"   [!] {name} 所有源最新仅到 {latest_from_source}，目标结束日期为 {end_date}"
                )

            # 2. 数据清洗与计算
            for col in ['open', 'high', 'low', 'close', 'volume', 'pct_chg']:
                if col not in df.columns:
                    df[col] = 0

            # 统一数值类型
            for col in ['open', 'high', 'low', 'close', 'volume', 'pct_chg']:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            # 计算涨跌幅（优先基于 close 连续计算；若首行无前值则保留源端 pct_chg）
            # 逻辑：(今收 - 昨收) / 昨收 * 100
            df = df.sort_values('trade_date').reset_index(drop=True)
            source_pct = df['pct_chg'].copy()
            df['pct_chg'] = df['close'].pct_change() * 100
            df['pct_chg'] = df['pct_chg'].where(df['pct_chg'].notna(), source_pct)
            df['pct_chg'] = df['pct_chg'].fillna(0).round(4)  # 保留4位小数

            # 3. 过滤日期区间
            mask = (df['trade_date'] >= start_date) & (df['trade_date'] <= end_date)
            df_filtered = df.loc[mask].copy()

            if df_filtered.empty:
                failures.append({
                    "ts_code": symbol,
                    "name": name,
                    "reason": "目标区间无可写入数据",
                    "latest_date": latest_from_source,
                    "source": source_used,
                    "source_errors": source_errors,
                })
                print(
                    f"   [-] {name} 区间内无数据 (source={source_used}, latest={latest_from_source}, target={start_date}~{end_date})"
                )
                continue

            # 字段重命名
            rename_map = {
                'open': 'open_price',
                'high': 'high_price',
                'low': 'low_price',
                'close': 'close_price',
                'volume': 'vol'
            }
            df_filtered = df_filtered.rename(columns=rename_map)

            # 补充字段
            df_filtered['ts_code'] = symbol
            df_filtered['amount'] = 0

            # 4. 入库（仅替换“本次已获取到的日期”，避免删空区间）
            cols_to_save = ['trade_date', 'ts_code', 'open_price', 'high_price',
                            'low_price', 'close_price', 'pct_chg', 'vol', 'amount']

            for c in cols_to_save:
                if c not in df_filtered.columns:
                    df_filtered[c] = 0

            replace_dates = sorted(df_filtered['trade_date'].dropna().astype(str).unique().tolist())
            if str(end_date) not in replace_dates:
                failures.append({
                    "ts_code": symbol,
                    "name": name,
                    "reason": "未写入目标日期",
                    "latest_date": latest_from_source,
                    "source": source_used,
                    "source_errors": source_errors,
                })
            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    del_sql = text("DELETE FROM index_price WHERE ts_code=:ts_code AND trade_date=:trade_date")
                    for one_day in replace_dates:
                        conn.execute(del_sql, {"ts_code": symbol, "trade_date": one_day})
                    df_filtered[cols_to_save].to_sql('index_price', conn, if_exists='append', index=False)
                    trans.commit()
                except Exception:
                    trans.rollback()
                    raise
            print(f"   [√] {name} 更新成功 ({len(df_filtered)} 条，source={source_used})")

            time.sleep(1)

        except Exception as e:
            failures.append({
                "ts_code": symbol,
                "name": name,
                "reason": f"更新异常: {e}",
                "latest_date": None,
                "source": None,
                "source_errors": source_errors,
            })
            print(f"   [x] {name} 异常: {e}")

    if send_alert and failures and _should_send_hk_update_alert(start_date, end_date):
        _send_hk_update_failure_alert(failures, start_date, end_date, alert_email=alert_email, email_sender=email_sender)

    return failures


if __name__ == "__main__":
    init_index_table()

    # 自动获取今天
    today = datetime.now().strftime('%Y%m%d')

    # 如果想补全历史数据，可以把下面这行解开注释并修改日期：
    # fetch_and_save_hk_indices("20240101", today)

    print(f"⚡️ 正在执行每日更新模式: {today}")

    # 更新 A 股
    fetch_and_save_indices(today, today)

    # 更新 港股
    fetch_and_save_hk_indices(today, today)

    print("\n=== ✅ 所有指数数据更新结束 ===")
