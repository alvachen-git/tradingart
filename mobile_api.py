"""
mobile_api.py — 爱波塔手机端专用 FastAPI 后端

启动命令:
    uvicorn mobile_api:app --host 0.0.0.0 --port 8001

注意：不要加 --workers N，本服务含后台线程（实时价格刷新）和全局缓存，
      多进程会导致每个 worker 各跑一份线程且状态不共享，Windows 下 Ctrl+C 也无法正常退出。

端点汇总:
  GET    /api/health                    健康检查
  POST   /api/auth/login                密码登录
  POST   /api/auth/login/email          邮箱验证码登录
  POST   /api/auth/logout               登出当前设备
  GET    /api/auth/verify               验证 Token

  POST   /api/chat/submit               提交 AI 问答任务
  GET    /api/chat/status/{task_id}     轮询 AI 任务状态

  GET    /api/intel/reports             获取情报站晚报列表（支持分页/频道筛选）
  GET    /api/intel/report/{id}         获取单篇晚报完整内容
  POST   /api/intel/subscribe           订阅白名单频道
  POST   /api/alipay/notify             支付宝异步回调通知（验签+到账）

  GET    /api/pay/wallet                点数钱包信息
  GET    /api/pay/packages              充值套餐列表
  GET    /api/pay/products              付费产品列表
  POST   /api/pay/purchase              使用点数购买权限
  GET    /api/pay/config                充值中心配置（外部链接）

  GET    /api/market/snapshot           综合行情快照

  POST   /api/portfolio/upload          上传持仓截图 → 识别 → 提交分析
  GET    /api/portfolio/status/{id}     轮询持仓分析进度
  GET    /api/portfolio/result          获取最新持仓体检结果

  GET    /api/user/profile              获取用户资料与订阅状态
"""

import hashlib
import io
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional, List, Any

import redis
from fastapi import FastAPI, HTTPException, Depends, UploadFile, File, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel

# 确保同目录模块可以 import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import auth_utils as auth
from task_manager import TaskManager
import subscription_service as sub_svc
import payment_service as pay_svc
import data_engine as de
from vision_tools import analyze_portfolio_image
from mobile_trading_day import enrich_prices_payload_with_trading_day

# ── Redis ─────────────────────────────────────────────────────
_REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
_redis = redis.from_url(_REDIS_URL, decode_responses=True)

# ════════════════════════════════════════════════════════════
#  实时行情后台刷新 — 直连新浪行情接口（绕过 akshare 封装层）
# ════════════════════════════════════════════════════════════

_PRICES_KEY = "mobile:futures:prices"
_PRICES_TTL = 30  # seconds
_PRICES_REFRESH_INTERVAL = 10  # seconds
_PRICES_REFRESH_LOCK_KEY = "mobile:futures:prices:refresh:lock"
_PRICES_REFRESH_LOCK_TTL = 9  # seconds, must be < refresh interval
_MARKET_CHART_CACHE_PREFIX = "mobile:market:chart"
_MARKET_CHART_CACHE_TTL = int(str(os.getenv("MOBILE_MARKET_CHART_CACHE_TTL", "120")).strip() or 120)
_INSTANCE_ID = f"{os.getenv('HOSTNAME', 'local')}:{os.getpid()}"

# 品种代码 → 交易所（决定新浪行情代码前缀和月份格式）
_PRODUCT_EXCHANGE: dict[str, str] = {
    # SHFE 上海期货交易所 → nf_{CODE}
    "cu":"SHFE","al":"SHFE","zn":"SHFE","pb":"SHFE","ni":"SHFE","sn":"SHFE",
    "au":"SHFE","ag":"SHFE","rb":"SHFE","hc":"SHFE","ss":"SHFE","bu":"SHFE",
    "ru":"SHFE","fu":"SHFE","lu":"SHFE","sp":"SHFE","bc":"SHFE","ao":"SHFE",
    "sc":"SHFE","sh":"SHFE","ad":"SHFE","br":"SHFE","ec":"SHFE","pt":"SHFE",
    # 原油(INE)、烧碱、铝合金、BR橡胶、集运欧线、铂金
    # DCE 大连商品交易所 → nf_{CODE}
    "m":"DCE","y":"DCE","a":"DCE","b":"DCE","c":"DCE","cs":"DCE","jd":"DCE",
    "l":"DCE","pp":"DCE","v":"DCE","eb":"DCE","eg":"DCE","j":"DCE","jm":"DCE",
    "i":"DCE","rr":"DCE","lh":"DCE","pg":"DCE","p":"DCE","lg":"DCE",
    # CZCE 郑商所 → nf_{CODE}（实测与SHFE/DCE格式相同）
    "sr":"CZCE","cf":"CZCE","ta":"CZCE","ma":"CZCE","rm":"CZCE","oi":"CZCE",
    "zc":"CZCE","fg":"CZCE","sa":"CZCE","ur":"CZCE","ap":"CZCE","cj":"CZCE",
    "lc":"CZCE","bz":"CZCE","pr":"CZCE","si":"CZCE","ps":"CZCE","nr":"CZCE",
    "sf":"CZCE","sm":"CZCE","wt":"CZCE","pm":"CZCE","pf":"CZCE","cy":"CZCE",
    "pl":"CZCE","op":"CZCE","fb":"CZCE","pk":"CZCE","px":"CZCE",   # 短纤、棉纱、丙烯、双胶纸、纤维板、花生、PX
    # CFFEX 中金所 → nf_{CODE}（字段布局与商品期货不同，见 _fetch_sina_prices）
    "if":"CFFEX","ic":"CFFEX","ih":"CFFEX","im":"CFFEX",
    "ts":"CFFEX","tf":"CFFEX","t":"CFFEX","tl":"CFFEX",   # 补充30年国债
}

# 期权相关品种代码集合（只抓这些品种的合约）
_OPTION_PRODUCTS: set[str] = set(_PRODUCT_EXCHANGE.keys())


def _is_trading_hours() -> bool:
    """判断当前是否在期货交易时段（上海时间）。"""
    import pytz
    now = datetime.now(pytz.timezone("Asia/Shanghai"))
    t = now.hour * 60 + now.minute
    return (
        (9 * 60 <= t <= 11 * 60 + 30) or
        (13 * 60 + 30 <= t <= 15 * 60) or
        (21 * 60 <= t <= 23 * 60 + 59) or
        (0 <= t <= 2 * 60 + 30)
    )


def _safe_float(v, default: float = 0.0) -> float:
    try:
        f = float(v)
        return f if f == f else default
    except (TypeError, ValueError):
        return default


def _market_chart_cache_key(product: str, contract: str) -> str:
    p = str(product or "").strip().lower()
    c = str(contract or "").strip().upper() or "_"
    return f"{_MARKET_CHART_CACHE_PREFIX}:{p}:{c}"


def _is_missing_value(v) -> bool:
    if v is None:
        return True
    if isinstance(v, str) and v.strip() in {"", "-", "—", "--", "nan", "NaN", "None"}:
        return True
    try:
        return bool(v != v)  # NaN
    except Exception:
        return False


def _compute_iv_chg_fallback_map(df):
    """
    从最近数日 IV 历史中计算“最近两个交易日”的日变动回退值。
    入参 df 列: code, td, iv
    返回: {code_lower: iv_chg_1d}
    """
    if df is None or getattr(df, "empty", True):
        return {}

    import pandas as pd

    req_cols = {"code", "td", "iv"}
    if not req_cols.issubset(set(df.columns)):
        return {}

    work = df.copy()
    work["code"] = work["code"].astype(str).str.lower()
    work["td"] = work["td"].astype(str)
    work["iv"] = pd.to_numeric(work["iv"], errors="coerce")
    work = work.dropna(subset=["iv"])
    if work.empty:
        return {}

    work = work.sort_values(["code", "td"], ascending=[True, False])
    out = {}
    for code, grp in work.groupby("code"):
        vals = []
        seen_td = set()
        for _, r in grp.iterrows():
            td = str(r["td"])
            if td in seen_td:
                continue
            seen_td.add(td)
            vals.append(float(r["iv"]))
            if len(vals) >= 2:
                break
        if len(vals) >= 2 and vals[1] > 0:
            out[code] = round(vals[0] - vals[1], 2)
    return out


def _to_sina_code(contract: str) -> str:
    """将大写合约代码转换为新浪行情请求码。
    所有交易所（SHFE/DCE/CZCE/CFFEX）统一格式：nf_{CODE}
    RB2604 → nf_RB2604
    SR2605 → nf_SR2605
    """
    m = re.match(r'^([A-Za-z]+)(\d+)$', contract)
    if not m:
        return ""
    prod = m.group(1).lower()
    if prod not in _PRODUCT_EXCHANGE:
        return ""
    return f"nf_{contract.upper()}"


def _get_active_contracts() -> list[str]:
    """从 futures_price 表获取最近 5 天内有数据的活跃合约代码（大写）。"""
    try:
        import pandas as pd
        df = pd.read_sql(
            """
            SELECT DISTINCT UPPER(SUBSTRING_INDEX(ts_code, '.', 1)) AS code
            FROM futures_price
            WHERE trade_date >= DATE_SUB(CURDATE(), INTERVAL 5 DAY)
              AND ts_code NOT LIKE '%%TAS%%'
            """,
            de.engine,
        )
        result = []
        for code in df["code"].tolist():
            m = re.match(r'^([A-Z]+)\d+$', code)
            if m and m.group(1).lower() in _OPTION_PRODUCTS:
                result.append(code)
        return result
    except Exception:
        return []


def _fetch_sina_prices(contracts: list[str]) -> dict:
    """直接调用新浪行情接口，绕过 akshare。
    返回 {大写合约代码: {open,high,low,price,pct,volume}}
    """
    import requests as _req
    # 建立 sina_code → contract_code 映射
    sina_map: dict[str, str] = {}
    for c in contracts:
        sc = _to_sina_code(c)
        if sc:
            sina_map[sc] = c

    if not sina_map:
        return {}

    result: dict = {}
    batch_size = 80
    keys = list(sina_map.keys())
    # 不继承系统代理环境，避免 ALL_PROXY/HTTP_PROXY 指向 socks 时触发依赖错误。
    session = _req.Session()
    session.trust_env = False

    try:
        for i in range(0, len(keys), batch_size):
            batch = keys[i:i + batch_size]
            try:
                url = "https://hq.sinajs.cn/list=" + ",".join(batch)
                headers = {
                    "Referer": "https://finance.sina.com.cn",
                    "User-Agent": "Mozilla/5.0",
                }
                resp = session.get(url, headers=headers, timeout=10)
                text = resp.content.decode("gbk", errors="replace")
                for line in text.split("\n"):
                    line = line.strip()
                    mat = re.match(r'var hq_str_(.+?)="(.*)";', line)
                    if not mat:
                        continue
                    sc = mat.group(1)
                    contract = sina_map.get(sc, "")
                    if not contract:
                        continue
                    fields = mat.group(2).split(",")
                    try:
                        # 判断交易所，CFFEX 字段布局与商品期货完全不同
                        m_prod = re.match(r'^([A-Z]+)\d+$', contract)
                        exchange = _PRODUCT_EXCHANGE.get(m_prod.group(1).lower(), "") if m_prod else ""

                        if exchange == "CFFEX":
                            # CFFEX 金融期货字段布局（股指/国债）：
                            # [0]=开 [1]=高 [2]=低 [3]=最新价 [4]=成交量
                            # [9]=涨停价(≠昨结算!) [10]=跌停价 [13]=昨结算价
                            if len(fields) < 14:
                                continue
                            cur  = _safe_float(fields[3])
                            prev = _safe_float(fields[13])  # 昨结算价，非fields[9](涨停价)
                            if cur <= 0:
                                continue
                            result[contract] = {
                                "open":   _safe_float(fields[0]),
                                "high":   _safe_float(fields[1]),
                                "low":    _safe_float(fields[2]),
                                "price":  cur,
                                "pct":    round((cur - prev) / prev * 100, 2) if prev > 0 else 0.0,
                                "volume": int(_safe_float(fields[4])),
                                "updated_at": "",
                            }
                        else:
                            # 商品期货字段布局（SHFE/DCE/CZCE）：
                            # [0]=合约名 [1]=昨持仓量 [2]=开 [3]=高 [4]=低
                            # [5]=最新价(盘后=0) [6]=买价 [8]=成交价 [10]=昨结算 [14]=成交量
                            if len(fields) < 11:
                                continue
                            prev = _safe_float(fields[10])
                            cur  = _safe_float(fields[5])
                            if cur <= 0:
                                cur = _safe_float(fields[8])
                            if cur <= 0:
                                cur = _safe_float(fields[6])
                            if cur <= 0:
                                continue
                            result[contract] = {
                                "open":   _safe_float(fields[2]),
                                "high":   _safe_float(fields[3]),
                                "low":    _safe_float(fields[4]),
                                "price":  cur,
                                "pct":    round((cur - prev) / prev * 100, 2) if prev > 0 else 0.0,
                                "volume": int(_safe_float(fields[14])) if len(fields) > 14 else 0,
                                "updated_at": "",
                            }
                    except Exception:
                        continue
            except Exception as e:
                print(f"[sina_batch] ERROR: {e}", flush=True)
                continue
    finally:
        session.close()

    return result


# 活跃合约列表缓存（每小时从 DB 刷新一次）
_contract_cache: list[str] = []
_contract_cache_ts: float = 0.0

# last snapshot fallback when Redis read fails
_last_prices_payload: dict = {"items": [], "is_trading": False, "refreshed_at": "", "contracts": {}}
_last_prices_lock = threading.Lock()


def _save_last_prices_payload(payload: dict) -> None:
    global _last_prices_payload
    with _last_prices_lock:
        # JSON round-trip copy to avoid shared mutable references across threads
        _last_prices_payload = json.loads(json.dumps(payload, ensure_ascii=False))


def _load_last_prices_payload() -> dict:
    with _last_prices_lock:
        return json.loads(json.dumps(_last_prices_payload, ensure_ascii=False))


def _try_acquire_prices_refresh_lock() -> bool:
    """Singleflight lock across multi-worker or multi-instance deployment."""
    try:
        ok = _redis.set(_PRICES_REFRESH_LOCK_KEY, _INSTANCE_ID, nx=True, ex=_PRICES_REFRESH_LOCK_TTL)
        return bool(ok)
    except Exception:
        # If Redis is unavailable, fallback to local refresh so service still works
        return True


def _prices_refresh_loop():
    """Background thread: refresh once every 10 seconds and write shared snapshot to Redis."""
    global _contract_cache, _contract_cache_ts
    while True:
        try:
            # singleflight: only one instance fetches upstream data per refresh window
            if not _try_acquire_prices_refresh_lock():
                time.sleep(_PRICES_REFRESH_INTERVAL)
                continue

            is_trading = _is_trading_hours()
            # 收盘后保留最后一笔实时快照，避免前端回退到午盘 DB 价。
            last_payload = _load_last_prices_payload()
            contracts: dict = last_payload.get("contracts", {}) or {}
            items: list = last_payload.get("items", []) or []

            if is_trading:
                # refresh active contracts from DB hourly
                if time.time() - _contract_cache_ts > 3600 or not _contract_cache:
                    _contract_cache = _get_active_contracts()
                    _contract_cache_ts = time.time()

                if _contract_cache:
                    contracts = _fetch_sina_prices(_contract_cache)

                    # choose major contract per product by max volume
                    prod_best: dict = {}
                    for code, data in contracts.items():
                        m = re.match(r"^([A-Z]+)\d+$", code)
                        if not m:
                            continue
                        prod = m.group(1).lower()
                        if prod not in prod_best or data["volume"] > prod_best[prod]["volume"]:
                            prod_best[prod] = {
                                "code": prod,
                                "name": code,
                                "price": data["price"],
                                "pct": data["pct"],
                                "volume": data["volume"],
                                "updated_at": "",
                            }
                    items = sorted(prod_best.values(), key=lambda x: x["code"])

            payload_obj = {
                "items": items,
                "is_trading": is_trading,
                "refreshed_at": datetime.now().strftime("%H:%M:%S"),
                "contracts": contracts,
            }
            payload_obj = enrich_prices_payload_with_trading_day(payload_obj)
            payload_raw = json.dumps(payload_obj, ensure_ascii=False)

            try:
                _redis.setex(_PRICES_KEY, _PRICES_TTL, payload_raw)
            except Exception as e:
                print(f"[prices_loop] REDIS_WRITE_FAIL: {e}", flush=True)

            _save_last_prices_payload(payload_obj)

        except Exception as e:
            print(f"[prices_loop] CRASH: {e}", flush=True)

        time.sleep(_PRICES_REFRESH_INTERVAL)


# ════════════════════════════════════════════════════════════
#  工具函数
# ════════════════════════════════════════════════════════════

def _html_to_plain(html: str, max_len: int = 120) -> str:
    """剥除 HTML 标签，返回纯文字摘要，用于列表卡片预览。"""
    if not html:
        return ""
    text = re.sub(r"<style[\s\S]*?</style>", "", html, flags=re.IGNORECASE)
    text = re.sub(r"<script[\s\S]*?</script>", "", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("&nbsp;", " ").replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_len] + ("…" if len(text) > max_len else "")


def _fmt_expire_at(expire_at: Any) -> str:
    if hasattr(expire_at, "strftime"):
        return expire_at.strftime("%Y-%m-%d")
    text = str(expire_at or "").strip()
    return text[:10] if len(text) >= 10 else text


# ════════════════════════════════════════════════════════════
#  App & CORS
# ════════════════════════════════════════════════════════════

app = FastAPI(
    title="爱波塔 Mobile API",
    version="1.0.0",
    description="uni-app 手机端专用后端接口",
)


@app.on_event("startup")
def _start_prices_thread():
    t = threading.Thread(target=_prices_refresh_loop, daemon=True)
    t.start()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # 上线后改为小程序/域名白名单
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ════════════════════════════════════════════════════════════
#  Bearer Token 认证
#
#  客户端存储的 token 格式:  "username:raw_uuid_token"
#  示例: "alice:550e8400-e29b-41d4-a716-446655440000"
# ════════════════════════════════════════════════════════════

_bearer = HTTPBearer()


def _unpack_token(token_str: str):
    """拆分 token，返回 (username, raw_token)，格式非法时抛出 401。"""
    if not token_str or ":" not in token_str:
        raise HTTPException(status_code=401, detail="Token 格式错误")
    username, raw_token = token_str.split(":", 1)
    if not username or not raw_token:
        raise HTTPException(status_code=401, detail="Token 格式错误")
    return username, raw_token


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer),
) -> str:
    """验证 Bearer Token，返回 username；失败则抛出 401。"""
    username, raw_token = _unpack_token(credentials.credentials)
    if not auth.check_token(username, raw_token):
        raise HTTPException(status_code=401, detail="Token 无效或已过期，请重新登录")
    return username


def _pack_token(username: str, raw_token: str) -> str:
    return f"{username}:{raw_token}"


# ════════════════════════════════════════════════════════════
#  Request Schemas
# ════════════════════════════════════════════════════════════

class LoginRequest(BaseModel):
    account: str        # 用户名 或 邮箱
    password: str


class EmailLoginRequest(BaseModel):
    email: str
    code: str


class SendCodeRequest(BaseModel):
    email: str


class ChatSubmitRequest(BaseModel):
    prompt: str
    history: List[dict] = []    # [{role: "user"/"assistant", content: "..."}]


class SubscribeRequest(BaseModel):
    channel_code: str


class PayPurchaseRequest(BaseModel):
    product_type: str   # channel | package
    code: str
    months: int = 1


def _build_mobile_chat_prompt(user_prompt: str) -> str:
    """为移动端问答补充稳定的输出格式约束，提升手机阅读可读性。"""
    raw = str(user_prompt or "").strip()
    if "【移动端输出格式要求】" in raw:
        return raw
    style_hint = (
        "【移动端输出格式要求】\n"
        "1) 禁止使用 Markdown 表格（不要输出 |---| 等表格语法）。\n"
        "2) 可使用简短小标题和项目符号，每段不超过2句。\n"
        "3) 关键结论置顶（先结论，再依据与建议）。\n"
        "4) 语言尽量简洁，适配手机端阅读。\n"
        "5) 结尾保留“仅供参考，不构成投资建议”。"
    )
    if not raw:
        return style_hint
    return f"{style_hint}\n\n【用户问题】\n{raw}"


# ════════════════════════════════════════════════════════════
#  Health Check
# ════════════════════════════════════════════════════════════

@app.get("/api/health", tags=["系统"])
def health():
    return {"status": "ok", "service": "爱波塔 Mobile API v1.0"}


# ════════════════════════════════════════════════════════════
#  AUTH
# ════════════════════════════════════════════════════════════

@app.post("/api/auth/send-code", tags=["认证"])
def send_code(body: SendCodeRequest):
    """发送邮箱登录验证码（60 秒内限发一次）。"""
    from email_utils import send_login_code
    try:
        result = send_login_code(body.email)
        success, msg = (result[0], result[1]) if isinstance(result, tuple) else (bool(result), "验证码已发送")
        if not success:
            raise HTTPException(status_code=400, detail=msg)
        return {"message": msg}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"发送失败: {e}")


@app.post("/api/auth/login", tags=["认证"])
def login(body: LoginRequest):
    """用户名或邮箱 + 密码登录，返回 token 和 username。"""
    success, msg, raw_token, username = auth.login_user(body.account, body.password)
    if not success:
        raise HTTPException(status_code=401, detail=msg)
    return {
        "token": _pack_token(username, raw_token),
        "username": username,
        "message": msg,
    }


@app.post("/api/auth/login/email", tags=["认证"])
def login_email(body: EmailLoginRequest):
    """邮箱验证码登录，返回 token 和 username。"""
    success, msg, raw_token, username = auth.login_with_email_code(body.email, body.code)
    if not success:
        raise HTTPException(status_code=401, detail=msg)
    return {
        "token": _pack_token(username, raw_token),
        "username": username,
        "message": msg,
    }


@app.post("/api/auth/logout", tags=["认证"])
def logout(credentials: HTTPAuthorizationCredentials = Depends(_bearer)):
    """登出当前设备，其他设备不受影响。"""
    username, raw_token = _unpack_token(credentials.credentials)
    auth.logout_user(username, raw_token)
    return {"message": "已登出"}


@app.get("/api/auth/verify", tags=["认证"])
def verify_token(username: str = Depends(get_current_user)):
    """验证 Token 是否仍然有效。"""
    return {"valid": True, "username": username}


# ════════════════════════════════════════════════════════════
#  CHAT — AI 问答
# ════════════════════════════════════════════════════════════

@app.post("/api/chat/submit", tags=["AI问答"])
def chat_submit(
    body: ChatSubmitRequest,
    username: str = Depends(get_current_user),
):
    """
    提交 AI 分析问答任务。
    返回 task_id，客户端每隔 2-3 秒轮询 /api/chat/status/{task_id}。
    """
    profile = de.get_user_profile(username) or {}
    risk = profile.get("risk_preference", "稳健型")

    normalized_prompt = _build_mobile_chat_prompt(body.prompt)

    task_id = TaskManager.create_task(
        user_id=username,
        prompt=normalized_prompt,
        risk_preference=risk,
        history_messages=body.history,
    )
    return {"task_id": task_id, "message": "任务已提交，正在分析..."}


@app.get("/api/chat/status/{task_id}", tags=["AI问答"])
def chat_status(task_id: str, username: str = Depends(get_current_user)):
    """
    轮询 AI 任务状态。
    status 值: pending | processing | success | error
    """
    status = TaskManager.get_task_status(task_id)
    if status.get("status") in ("success", "error"):
        TaskManager.clear_user_pending_task(username)
    return status


# ════════════════════════════════════════════════════════════
#  INTEL — 情报站晚报
# ════════════════════════════════════════════════════════════

# 可以自助订阅的频道 code 白名单（默认空，环境变量灰度开启）
# 示例: FREE_SELF_SUBSCRIBE_CHANNEL_CODES=daily_report,expiry_option_radar
_FREE_CHANNEL_CODES = {
    item.strip().lower()
    for item in str(os.getenv("FREE_SELF_SUBSCRIBE_CHANNEL_CODES", "")).split(",")
    if item.strip()
}
_FORCE_PAID_CHANNEL_CODES = {
    "daily_report",
    "expiry_option_radar",
    "broker_position_report",
    "fund_flow_report",
}
_EFFECTIVE_FREE_CHANNEL_CODES = _FREE_CHANNEL_CODES - _FORCE_PAID_CHANNEL_CODES
_INTEL_SELF_SUBSCRIBE_API_ENABLED = (
    str(os.getenv("INTEL_SELF_SUBSCRIBE_API_ENABLED", "false")).strip().lower()
    in {"1", "true", "on", "yes"}
)
_ALLOW_SELF_SUB_IN_PROD = (
    str(os.getenv("INTEL_SELF_SUBSCRIBE_ALLOW_IN_PROD", "false")).strip().lower()
    in {"1", "true", "on", "yes"}
)
_INTEL_CHANNEL_CODE_ALIASES = {
    # 历史小程序包曾使用该 code，兼容到后端真实频道码
    "expiry_option_report": "expiry_option_radar",
}


def _normalize_intel_channel_code(code: Optional[str]) -> Optional[str]:
    raw = str(code or "").strip().lower()
    if not raw:
        return None
    return _INTEL_CHANNEL_CODE_ALIASES.get(raw, raw)


def _extract_published_at(payload: dict) -> str:
    # 兼容 subscription_service 的 publish_time 与旧字段 published_at
    ts = payload.get("publish_time")
    if ts is None:
        ts = payload.get("published_at", "")
    return str(ts or "")


def _is_production_env() -> bool:
    env_val = (
        str(os.getenv("APP_ENV", "")).strip()
        or str(os.getenv("ENV", "")).strip()
        or str(os.getenv("DEPLOY_ENV", "")).strip()
    ).lower()
    return env_val in {"prod", "production", "online"}


@app.get("/api/intel/reports", tags=["情报站"])
def intel_reports(
    channel_code: Optional[str] = None,
    page: int = 1,
    page_size: int = 20,
    username: str = Depends(get_current_user),
):
    """
    获取情报站晚报列表。
    - channel_code: 可选频道筛选（如 "fund_flow_report"）
    - page / page_size: 分页参数
    """
    page = max(1, page)
    page_size = min(50, max(1, page_size))
    offset = (page - 1) * page_size
    channel_code = _normalize_intel_channel_code(channel_code)

    # 多拉一些再切片，避免多次 DB 查询
    raw = sub_svc.get_channel_contents(
        channel_code=channel_code,
        days=10,
        limit=offset + page_size,
    ) or []
    paged = raw[offset: offset + page_size]

    items = []
    for c in paged:
        content_text = c.get("content") or ""
        summary = _html_to_plain(content_text, max_len=120)
        items.append({
            "id": c.get("id"),
            "title": c.get("title", ""),
            "channel_name": c.get("channel_name", ""),
            "channel_code": c.get("channel_code", ""),
            "summary": summary,
            "published_at": _extract_published_at(c),
        })

    return {
        "items": items,
        "page": page,
        "page_size": page_size,
        "has_more": len(raw) > offset + page_size,
    }


@app.get("/api/intel/report/{report_id}", tags=["情报站"])
def intel_report_detail(
    report_id: int,
    username: str = Depends(get_current_user),
):
    """获取单篇晚报的完整内容。"""
    content = sub_svc.get_content_by_id(report_id)
    if not content:
        raise HTTPException(status_code=404, detail="内容不存在或已下架")
    if content.get("is_premium"):
        access = sub_svc.check_subscription_access(username, int(content.get("channel_id") or 0))
        if not access.get("has_access"):
            raise HTTPException(status_code=403, detail="该内容需要订阅后查看")
    # 统一返回字段，避免客户端被 publish_time/published_at 命名差异影响
    content["published_at"] = _extract_published_at(content)
    return content


@app.post("/api/intel/subscribe", tags=["情报站"])
def intel_subscribe(
    body: SubscribeRequest,
    username: str = Depends(get_current_user),
):
    """订阅白名单情报频道（默认关闭，通过环境变量灰度开启）。"""
    if not _INTEL_SELF_SUBSCRIBE_API_ENABLED:
        raise HTTPException(status_code=403, detail="当前环境未开启自助订阅接口")
    if _is_production_env() and not _ALLOW_SELF_SUB_IN_PROD:
        raise HTTPException(status_code=403, detail="生产环境默认关闭该接口")

    channel_code = _normalize_intel_channel_code(body.channel_code)
    if not channel_code:
        raise HTTPException(status_code=400, detail="channel_code 不能为空")
    channel = sub_svc.get_channel_by_code(channel_code)
    if not channel:
        raise HTTPException(status_code=404, detail="频道不存在")

    if channel_code.lower() not in _EFFECTIVE_FREE_CHANNEL_CODES:
        raise HTTPException(status_code=403, detail="该频道需要人工开通，请联系客服")

    result = sub_svc.add_subscription(
        username,
        channel["id"],
        days=3650,
        source_type="self_subscribe_whitelist",
        source_ref=f"api:intel_subscribe:{channel_code.lower()}",
        source_note="mobile_api_whitelist_free_subscribe",
        operator="user_self_service",
    )
    # add_subscription 返回 (success, message) 或单个 bool
    if isinstance(result, tuple):
        success, msg = result[0], result[1] if len(result) > 1 else "操作完成"
    else:
        success, msg = bool(result), "订阅成功" if result else "订阅失败"

    if not success:
        raise HTTPException(status_code=400, detail=msg)
    return {"message": msg}


@app.post("/api/alipay/notify", tags=["支付"])
async def alipay_notify(request: Request):
    """
    支付宝异步回调端点：
    - 不走 JWT 鉴权
    - 回调成功必须返回纯文本 success
    """
    try:
        form_data = await request.form()
        payload = {str(k): str(v) for k, v in form_data.items()}
    except Exception as exc:
        print(f"[mobile_api][alipay_notify] parse_form_failed err={exc}")
        return PlainTextResponse("failure", status_code=200)

    ok, reason = pay_svc.process_alipay_notify(payload)
    if ok:
        return PlainTextResponse("success", status_code=200)
    print(f"[mobile_api][alipay_notify] failed reason={reason}")
    return PlainTextResponse("failure", status_code=200)


# ════════════════════════════════════════════════════════════
#  PAYMENT — 充值与点数购买
# ════════════════════════════════════════════════════════════

@app.get("/api/pay/wallet", tags=["支付"])
def pay_wallet(username: str = Depends(get_current_user)):
    """获取点数钱包信息。"""
    try:
        points_info = pay_svc.get_user_points(username) or {}
        updated_at = points_info.get("updated_at")
        if hasattr(updated_at, "strftime"):
            updated_text = updated_at.strftime("%Y-%m-%d %H:%M:%S")
        else:
            updated_text = str(updated_at or "")
        return {
            "balance": int(points_info.get("balance") or 0),
            "total_earned": int(points_info.get("total_earned") or 0),
            "total_spent": int(points_info.get("total_spent") or 0),
            "updated_at": updated_text,
            "payment_enabled": bool(pay_svc.is_points_payment_enabled()),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"钱包信息获取失败: {e}")


@app.get("/api/pay/packages", tags=["支付"])
def pay_packages(username: str = Depends(get_current_user)):
    """获取充值套餐列表。"""
    _ = username
    try:
        items = []
        for pkg in getattr(pay_svc, "POINTS_PACKAGES", []):
            rmb = float(pkg.get("rmb") or 0)
            points = int(pkg.get("points") or 0)
            bonus = max(points - int(round(rmb * 10)), 0)
            items.append({
                "name": str(pkg.get("name") or ""),
                "rmb": round(rmb, 2),
                "points": points,
                "bonus_points": bonus,
            })
        return {"items": items}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"充值套餐获取失败: {e}")


@app.get("/api/pay/products", tags=["支付"])
def pay_products(username: str = Depends(get_current_user)):
    """获取可点数购买产品（频道 + 套餐）。"""
    _ = username
    try:
        products = pay_svc.get_paid_products() or []

        # 与桌面端一致：兜底补齐 trade_signal 产品
        has_trade_signal = any(
            str(item.get("code") or "") == "trade_signal"
            and str(item.get("product_type") or "") == "channel"
            for item in products
        )
        if not has_trade_signal:
            trade_signal = sub_svc.get_channel_by_code("trade_signal")
            if trade_signal and trade_signal.get("id"):
                products.append({
                    "product_type": "channel",
                    "code": "trade_signal",
                    "id": int(trade_signal["id"]),
                    "name": trade_signal.get("name") or "交易信号",
                    "icon": trade_signal.get("icon") or "⚡",
                    "points_monthly": 800,
                    "months_options": [1, 3, 6, 12],
                })

        channel_order = {
            "daily_report": 0,
            "expiry_option_radar": 1,
            "broker_position_report": 2,
            "fund_flow_report": 3,
            "trade_signal": 4,
        }

        def _sort_key(item: dict):
            ptype = str(item.get("product_type") or "")
            code = str(item.get("code") or "")
            return (
                0 if ptype == "channel" else 1,
                channel_order.get(code, 999),
                code,
            )

        norm_items = []
        for item in sorted(products, key=_sort_key):
            norm_items.append({
                "product_type": str(item.get("product_type") or ""),
                "code": str(item.get("code") or ""),
                "id": int(item.get("id")) if item.get("id") is not None else None,
                "name": str(item.get("name") or ""),
                "icon": str(item.get("icon") or ""),
                "points_monthly": int(item.get("points_monthly") or 0),
                "months_options": [int(x) for x in (item.get("months_options") or [1, 3, 6, 12])],
                "includes": [str(x) for x in (item.get("includes") or [])],
                "includes_names": [str(x) for x in (item.get("includes_names") or [])],
            })

        return {"items": norm_items}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"付费产品获取失败: {e}")


@app.post("/api/pay/purchase", tags=["支付"])
def pay_purchase(
    body: PayPurchaseRequest,
    username: str = Depends(get_current_user),
):
    """使用点数购买频道或套餐权限。"""
    try:
        months = int(body.months or 0)
    except Exception:
        months = 0
    if months <= 0 or months > 36:
        raise HTTPException(status_code=400, detail="months 必须在 1~36 之间")

    product_type = str(body.product_type or "").strip().lower()
    code = str(body.code or "").strip().lower()
    if not code:
        raise HTTPException(status_code=400, detail="code 不能为空")

    if product_type == "channel":
        channel = sub_svc.get_channel_by_code(code)
        if not channel or not channel.get("id"):
            raise HTTPException(status_code=404, detail="频道不存在或已下架")
        ok, msg = pay_svc.purchase_subscription_with_points(
            username,
            int(channel["id"]),
            months=months,
        )
    elif product_type == "package":
        pkg_code = str(getattr(pay_svc, "INTEL_PACKAGE_PRODUCT", {}).get("code", "intel_package")).strip().lower()
        if code != pkg_code:
            raise HTTPException(status_code=404, detail="套餐不存在")
        ok, msg = pay_svc.purchase_intel_package_with_points(
            username,
            months=months,
        )
    else:
        raise HTTPException(status_code=400, detail="product_type 仅支持 channel 或 package")

    if ok:
        message = "购买成功" if str(msg) == "already_processed" else str(msg or "购买成功")
        return {"ok": True, "message": message}
    raise HTTPException(status_code=400, detail=str(msg or "购买失败"))


@app.get("/api/pay/config", tags=["支付"])
def pay_config(username: str = Depends(get_current_user)):
    """获取充值中心配置。"""
    _ = username
    recharge_url = str(os.getenv("MOBILE_RECHARGE_URL", "https://www.aiprota.com")).strip() or "https://www.aiprota.com"
    service_wechat = str(os.getenv("MOBILE_SERVICE_WECHAT", "trader-sec")).strip() or "trader-sec"
    service_phone = str(os.getenv("MOBILE_SERVICE_PHONE", "17521591756")).strip() or "17521591756"
    return {
        "recharge_url": recharge_url,
        "service_wechat": service_wechat,
        "service_phone": service_phone,
    }


# ════════════════════════════════════════════════════════════
#  MARKET — 综合行情快照
# ════════════════════════════════════════════════════════════

@app.get("/api/market/snapshot", tags=["行情"])
def market_snapshot(username: str = Depends(get_current_user)):
    """获取综合行情快照（原始接口，兼容旧版）"""
    try:
        data = de.get_comprehensive_market_data()
        if isinstance(data, dict) and "error" in data:
            raise HTTPException(status_code=500, detail=data["error"])
        return {"data": data}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"行情获取失败: {e}")


@app.get("/api/market/options", tags=["行情"])
def market_options(username: str = Depends(get_current_user)):
    """
    商品期权行情：合约名称、当前IV、IV Rank、当日涨跌%、当日IV变动。
    建议客户端缓存 60 秒。
    """
    try:
        df = de.get_comprehensive_market_data()
        if df is None or (hasattr(df, 'empty') and df.empty):
            return {"items": [], "updated_at": ""}

        import pandas as pd
        records = []
        for _, row in df.iterrows():
            iv_rank = row.get("IV Rank", 0)
            try:
                iv_rank_num = float(iv_rank) if iv_rank not in ("快到期", None, "") else -1
            except Exception:
                iv_rank_num = -1

            # 提取品种代码（合约格式如 "m2605 (豆粕)"，取括号前的字母部分）
            name_str = str(row.get("合约", ""))
            prod_match = re.match(r"([a-zA-Z]+)", name_str)
            product_code = prod_match.group(1).lower() if prod_match else ""

            # 当前IV：已经是百分比形式（如 18.53 = 18.53%），直接使用
            raw_iv = float(row.get("当前IV", 0) or 0)
            # IV变动(日)：优先使用综合数据，缺失时走历史回退计算
            iv_chg_raw = row.get("IV变动(日)", None)
            iv_chg_missing = _is_missing_value(iv_chg_raw)
            raw_iv_chg = 0.0 if iv_chg_missing else float(iv_chg_raw or 0)

            records.append({
                "name":         name_str,
                "product_code": product_code,
                "iv":           round(raw_iv, 1),
                "iv_rank":      iv_rank_num,
                "iv_chg_1d":    round(raw_iv_chg, 2),
                "pct_1d":       round(float(row.get("涨跌%(日)", 0) or 0), 2),
                "pct_5d":       round(float(row.get("涨跌%(5日)", 0) or 0), 2),
                "retail_chg":   int(row.get("散户变动(日)", 0) or 0),
                "inst_chg":     int(row.get("机构变动(日)", 0) or 0),
                "cur_price":    0.0,  # 下面批量回填
                "_iv_chg_missing": iv_chg_missing,
            })

        # ── IV变动缺失回退：按最近两个收盘日计算（合约级）──────────
        if records:
            try:
                code_set = set()
                for r in records:
                    c = r["name"].split("(")[0].strip().lower()
                    if re.match(r"^[a-z]+[0-9]{3,4}$", c):
                        code_set.add(c)
                if code_set:
                    codes_sql = "','".join(sorted(code_set))
                    iv_recent_df = pd.read_sql(
                        f"""
                        SELECT
                            LOWER(SUBSTRING_INDEX(ts_code, '.', 1)) AS code,
                            REPLACE(trade_date,'-','')              AS td,
                            iv
                        FROM commodity_iv_history
                        WHERE LOWER(SUBSTRING_INDEX(ts_code, '.', 1)) IN ('{codes_sql}')
                          AND trade_date >= DATE_SUB(CURDATE(), INTERVAL 14 DAY)
                          AND iv IS NOT NULL
                        ORDER BY code ASC, td DESC
                        """,
                        de.engine
                    )
                    iv_fallback_map = _compute_iv_chg_fallback_map(iv_recent_df)
                    for r in records:
                        code = r["name"].split("(")[0].strip().lower()
                        if code in iv_fallback_map:
                            # 只要历史两日可算，优先采用回退值（覆盖 0 占位场景）
                            r["iv_chg_1d"] = iv_fallback_map[code]
            except Exception:
                pass

        # ── 批量查询最新收盘价 ────────────────────────────────
        if records:
            try:
                # name 格式如 "EB2604 (苯乙烯)"，提取合约代码小写
                codes = list({r["name"].split("(")[0].strip().lower() for r in records})
                codes_sql = "','".join(codes)
                price_df = pd.read_sql(
                    f"""
                    SELECT
                        LOWER(SUBSTRING_INDEX(ts_code, '.', 1)) AS code,
                        close_price
                    FROM futures_price
                    WHERE LOWER(SUBSTRING_INDEX(ts_code, '.', 1)) IN ('{codes_sql}')
                      AND ts_code NOT LIKE '%%TAS%%'
                      AND trade_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                    ORDER BY trade_date DESC
                    """,
                    de.engine
                )
                if not price_df.empty:
                    price_map = price_df.groupby("code")["close_price"].first().to_dict()
                    for r in records:
                        code = r["name"].split("(")[0].strip().lower()
                        r["cur_price"] = round(float(price_map.get(code, 0) or 0), 2)
            except Exception:
                pass  # 价格查询失败不影响其他字段

        for r in records:
            r.pop("_iv_chg_missing", None)

        # 按 IV Rank 降序排列，快到期(-1)排最后
        records.sort(key=lambda x: x["iv_rank"] if x["iv_rank"] >= 0 else -999, reverse=True)
        import datetime
        return {"items": records, "updated_at": datetime.datetime.now().strftime("%H:%M")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"期权行情获取失败: {e}")


@app.get("/api/market/contracts/{product}", tags=["行情"])
def market_contracts(product: str, username: str = Depends(get_current_user)):
    """
    指定品种的全部月份合约行情（供 App 品种 chip 点击后展开）。
    product: 品种代码小写，如 m / rb / cu
    返回格式与 /api/market/options 的 OptionItem 相同。
    """
    try:
        import pandas as pd
        import datetime as dt

        prod = product.strip().lower()
        pattern = f'^{prod}[0-9]'  # 匹配 m2605 / m2609 等

        # ── 最近两个交易日（按该品种自身日历）──
        dates_df = pd.read_sql(
            f"SELECT DISTINCT REPLACE(trade_date,'-','') as td FROM futures_price "
            f"WHERE ts_code REGEXP '{pattern}' AND ts_code NOT LIKE '%%TAS%%' "
            f"ORDER BY td DESC LIMIT 2",
            de.engine
        )
        if dates_df.empty:
            return {"items": []}
        latest_date = str(dates_df.iloc[0]['td'])
        prev_date   = str(dates_df.iloc[1]['td']) if len(dates_df) > 1 else ""

        # ── 最新日 + 前一日价格（一次取，用于计算涨跌）──
        price_sql = f"""
            SELECT ts_code, close_price, oi,
                   REPLACE(trade_date,'-','') as trade_date
            FROM futures_price
            WHERE REPLACE(trade_date,'-','') IN ('{latest_date}'{(",'" + prev_date + "'") if prev_date else ""})
              AND ts_code REGEXP '{pattern}'
              AND ts_code NOT LIKE '%%TAS%%'
              AND oi > 0
        """
        df_price_all = pd.read_sql(price_sql, de.engine)
        df_price = df_price_all[df_price_all['trade_date'] == latest_date].copy()
        df_price_prev = df_price_all[df_price_all['trade_date'] == prev_date].copy() if prev_date else pd.DataFrame()
        if df_price.empty:
            return {"items": []}
        df_price = df_price.sort_values('oi', ascending=False)

        # 前一日收盘价 map（用于计算涨跌）
        prev_close_map: dict = {}
        if not df_price_prev.empty:
            prev_close_map = dict(zip(df_price_prev['ts_code'], df_price_prev['close_price']))

        # ── IV：取 commodity_iv_history 最近两个日期（各自的日历）──
        iv_dates_df = pd.read_sql(
            f"SELECT DISTINCT REPLACE(trade_date,'-','') as td FROM commodity_iv_history "
            f"WHERE ts_code REGEXP '{pattern}' ORDER BY td DESC LIMIT 2",
            de.engine
        )
        iv_latest_date = str(iv_dates_df.iloc[0]['td']) if not iv_dates_df.empty else ""
        iv_prev_date   = str(iv_dates_df.iloc[1]['td']) if len(iv_dates_df) > 1 else ""

        iv_map: dict = {}
        iv_prev_map: dict = {}
        if iv_latest_date:
            iv_sql = f"""
                SELECT ts_code, iv, REPLACE(trade_date,'-','') as td
                FROM commodity_iv_history
                WHERE ts_code REGEXP '{pattern}'
                  AND REPLACE(trade_date,'-','') IN ('{iv_latest_date}'{(",'" + iv_prev_date + "'") if iv_prev_date else ""})
            """
            df_iv = pd.read_sql(iv_sql, de.engine)
            iv_map      = dict(zip(df_iv[df_iv['td'] == iv_latest_date]['ts_code'],
                                   df_iv[df_iv['td'] == iv_latest_date]['iv']))
            if iv_prev_date:
                iv_prev_map = dict(zip(df_iv[df_iv['td'] == iv_prev_date]['ts_code'],
                                       df_iv[df_iv['td'] == iv_prev_date]['iv']))

        # ── 1年 IV 历史（计算 IV Rank）──
        date_1y = (dt.datetime.now() - dt.timedelta(days=365)).strftime('%Y%m%d')
        iv_hist_sql = f"""
            SELECT ts_code, iv FROM commodity_iv_history
            WHERE REPLACE(trade_date,'-','') >= '{date_1y}'
              AND ts_code REGEXP '{pattern}'
              AND iv > 0
        """
        df_iv_hist = pd.read_sql(iv_hist_sql, de.engine)

        def iv_rank(ts_code: str, cur_iv: float) -> float:
            if cur_iv <= 0:
                return -1
            hist = df_iv_hist[df_iv_hist['ts_code'] == ts_code]['iv']
            if len(hist) < 20:
                return -1
            return round((hist < cur_iv).sum() / len(hist) * 100, 1)

        records = []
        for _, row in df_price.iterrows():
            ts = str(row['ts_code'])
            cur_close  = float(row.get('close_price', 0) or 0)
            prev_close = float(prev_close_map.get(ts, 0) or 0)
            pct_1d = round((cur_close - prev_close) / prev_close * 100, 2) if prev_close > 0 else 0.0

            cur_iv  = float(iv_map.get(ts, 0) or 0)
            prev_iv = float(iv_prev_map.get(ts, 0) or 0)
            iv_chg  = round(cur_iv - prev_iv, 2) if prev_iv > 0 else 0.0
            rank    = iv_rank(ts, cur_iv)

            records.append({
                "name":         ts.upper(),
                "product_code": prod,
                "iv":           round(cur_iv, 1),
                "iv_rank":      rank,
                "iv_chg_1d":    iv_chg,
                "pct_1d":       pct_1d,
                "pct_5d":       0.0,
                "retail_chg":   0,
                "inst_chg":     0,
            })

        records.sort(key=lambda x: x['oi'] if 'oi' in x else 0, reverse=True)
        return {"items": records}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"合约列表获取失败: {e}")


@app.get("/api/market/holding/{product}", tags=["行情"])
def market_holding(product: str, username: str = Depends(get_current_user)):
    """
    指定品种的期货商持仓分析：各期货商得分排名（近150天累计）。
    product: 品种代码，如 m / rb / cu / sc
    """
    try:
        symbol = product.lower()   # 与桌面端一致，传品种代码如 'm'、'rb'、'cu'
        df = de.calculate_broker_rankings(symbol=symbol, lookback_days=150)
        if df is None or (hasattr(df, 'empty') and df.empty):
            return {"product": product, "brokers": [], "trade_date": ""}

        # 聚合：每家期货商的累计得分 + 最新净持仓方向
        df["broker"] = df["broker"].str.replace(r"[（\(]代客[）\)]", "", regex=True).str.strip()
        agg = df.groupby("broker").agg(
            score=("score", "sum"),
            net_vol=("net_vol", "last"),
        ).reset_index()
        agg = agg.sort_values("score", ascending=False)

        latest_date = df["trade_date"].max() if not df.empty else ""

        brokers = []
        for _, row in agg.iterrows():
            s = float(row["score"])
            nv = float(row["net_vol"])
            if abs(s) < 1:
                continue
            brokers.append({
                "broker":   str(row["broker"]),
                "score":    round(s, 0),
                "net_vol":  int(nv),
                "direction": "多" if nv > 0 else "空" if nv < 0 else "平",
            })

        return {"product": product, "brokers": brokers, "trade_date": str(latest_date)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"持仓分析失败: {e}")


@app.get("/api/market/broker/{product}", tags=["行情"])
def market_broker_detail(
    product: str,
    broker: str = Query(..., description="期货商名称"),
    username: str = Depends(get_current_user)
):
    """
    指定品种 + 期货商的近150天逐日持仓与得分明细。
    product: 品种代码，如 m / rb / cu
    broker: 期货商名称，如 中信期货
    """
    try:
        symbol = product.lower()
        df = de.calculate_broker_rankings(symbol=symbol, lookback_days=150)
        if df is None or (hasattr(df, 'empty') and df.empty):
            return {"product": product, "broker": broker, "rows": [], "total_score": 0}

        df = df.copy()
        df["broker"] = df["broker"].str.replace(r"[（\(]代客[）\)]", "", regex=True).str.strip()
        df_broker = df[df["broker"] == broker].sort_values("trade_date").copy()

        if df_broker.empty:
            return {"product": product, "broker": broker, "rows": [], "total_score": 0}

        df_broker["cum_score"] = df_broker["score"].cumsum()
        total = float(df_broker["score"].sum())

        rows = []
        for _, row in df_broker.iterrows():
            dt = row["trade_date"]
            if hasattr(dt, 'strftime'):
                dt_str = dt.strftime('%Y-%m-%d')
            else:
                dt_str = str(dt)
                if len(dt_str) == 8 and '-' not in dt_str:
                    dt_str = f"{dt_str[:4]}-{dt_str[4:6]}-{dt_str[6:8]}"
            rows.append({
                "dt": dt_str,
                "net_vol": int(row["net_vol"]),
                "pct_chg": round(float(row.get("pct_chg", 0)), 2),  # DB已是百分比格式(2.0=2%)
                "score": round(float(row["score"]), 1),
                "cum_score": round(float(row["cum_score"]), 1),
            })

        return {
            "product": product,
            "broker": broker,
            "total_score": round(total, 0),
            "rows": rows,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"期货商明细获取失败: {e}")


@app.get("/api/market/chart/{product}", tags=["行情"])
def market_chart(
    product: str,
    contract: Optional[str] = Query(default=None, description="可选，指定具体合约，如 MA2609"),
    username: str = Depends(get_current_user),
):
    """
    指定品种近 60 天的价格 + IV 数据，用于前端绘制折线图。
    product: 如 m / rb / cu
    contract: 可选，传入时优先按该合约绘制；不传时沿用主力合约逻辑。
    """
    try:
        import datetime as _dt
        from sqlalchemy import text as _text

        eng = de.engine
        if eng is None:
            raise HTTPException(status_code=500, detail="数据库未连接")

        prod_upper = product.upper()
        cn_name = de.PRODUCT_MAP.get(prod_upper, product.upper())
        selected_contract = str(contract or "").strip().upper()
        selected_contract = selected_contract if re.match(r"^[A-Z]+[0-9]{3,4}$", selected_contract) else ""
        if selected_contract and not selected_contract.startswith(prod_upper):
            selected_contract = ""
        cache_key = _market_chart_cache_key(product, selected_contract)
        try:
            cached_raw = _redis.get(cache_key)
            if cached_raw:
                cached_payload = json.loads(cached_raw)
                if isinstance(cached_payload, dict):
                    return cached_payload
        except Exception as cache_err:
            print(f"[market_chart] cache_read_failed key={cache_key} err={cache_err}", flush=True)

        import pandas as _pd
        since = (_dt.datetime.now() - _dt.timedelta(days=400)).strftime("%Y%m%d")

        # ── 1. 决定图表目标合约 ts_code ─────────────────────────
        # 传入 contract 时优先使用指定合约；否则使用主力合约。
        pattern = f"^{product.upper()}[0-9]"
        pattern2 = f"^{product.lower()}[0-9]"

        if selected_contract:
            main_contract = selected_contract
        else:
            # 用品种代码前缀 REGEXP 匹配，取最新日期持仓量最大的合约（当前主力）
            main_sql = f"""
                SELECT ts_code FROM futures_price
                WHERE ts_code REGEXP '{pattern}'
                  AND ts_code NOT LIKE '%%TAS%%'
                  AND REPLACE(trade_date,'-','') = (
                      SELECT MAX(REPLACE(trade_date,'-','')) FROM futures_price
                      WHERE ts_code REGEXP '{pattern}'
                        AND ts_code NOT LIKE '%%TAS%%'
                  )
                ORDER BY oi DESC LIMIT 1
            """
            main_df = _pd.read_sql(main_sql, eng)
            if main_df.empty:
                main_sql2 = f"""
                    SELECT ts_code FROM futures_price
                    WHERE ts_code REGEXP '{pattern2}'
                      AND ts_code NOT LIKE '%%TAS%%'
                      AND REPLACE(trade_date,'-','') = (
                          SELECT MAX(REPLACE(trade_date,'-','')) FROM futures_price
                          WHERE ts_code REGEXP '{pattern2}'
                            AND ts_code NOT LIKE '%%TAS%%'
                      )
                    ORDER BY oi DESC LIMIT 1
                """
                main_df = _pd.read_sql(main_sql2, eng)

            if main_df.empty:
                empty_payload = {
                    "product": product.lower(),
                    "cn_name": cn_name,
                    "cur_price": None,
                    "cur_pct": None,
                    "cur_iv": None,
                    "ohlc": [],
                    "iv": [],
                }
                try:
                    _redis.setex(cache_key, _MARKET_CHART_CACHE_TTL, json.dumps(empty_payload, ensure_ascii=False))
                except Exception as cache_err:
                    print(f"[market_chart] cache_write_failed key={cache_key} err={cache_err}", flush=True)
                return empty_payload
            main_contract = str(main_df.iloc[0]["ts_code"]).upper()

        # ── 2. 拉取 OHLC 数据（近1年K线）────────────────────────
        ohlc_sql = f"""
            SELECT
                REPLACE(trade_date,'-','') as dt,
                open_price  as o,
                high_price  as h,
                low_price   as l,
                close_price as c,
                pct_chg,
                oi
            FROM futures_price
            WHERE UPPER(ts_code) = '{main_contract}'
              AND REPLACE(trade_date,'-','') >= '{since}'
            ORDER BY trade_date ASC
            LIMIT 300
        """
        ohlc_df = _pd.read_sql(ohlc_sql, eng)

        # ── 3. 拉取 IV 历史（与价格数据时间范围对齐）────────────
        if selected_contract:
            iv_sql = f"""
                SELECT REPLACE(trade_date,'-','') as dt, iv
                FROM commodity_iv_history
                WHERE UPPER(ts_code) = '{main_contract}'
                  AND REPLACE(trade_date,'-','') >= '{since}'
                ORDER BY trade_date ASC
            """
            iv_df = _pd.read_sql(iv_sql, eng)
        else:
            iv_sql = f"""
                SELECT REPLACE(trade_date,'-','') as dt, iv
                FROM commodity_iv_history
                WHERE ts_code REGEXP '{pattern}'
                  AND REPLACE(trade_date,'-','') >= '{since}'
                ORDER BY trade_date ASC
            """
            iv_df = _pd.read_sql(iv_sql, eng)
            if iv_df.empty:
                # 尝试小写
                iv_sql2 = f"""
                    SELECT REPLACE(trade_date,'-','') as dt, iv
                    FROM commodity_iv_history
                    WHERE ts_code REGEXP '{pattern2}'
                      AND REPLACE(trade_date,'-','') >= '{since}'
                    ORDER BY trade_date ASC
                """
                iv_df = _pd.read_sql(iv_sql2, eng)

        # 不指定合约时，IV按全品种日均；指定合约时按该合约原值。
        if not iv_df.empty and not selected_contract:
            iv_df = iv_df.groupby("dt")["iv"].mean().reset_index().sort_values("dt")

        # ── 4. 拉取反指标/正指标持仓数据 ─────────────────────────
        BROKERS_DUMB  = ['中信建投', '东方财富', '方正中期']
        BROKERS_SMART = ['海通期货', '东证期货', '国泰君安']
        hold_product  = ''.join(c for c in product if not c.isalpha() or True).lower()
        hold_product  = ''.join(c for c in product.lower() if c.isalpha())

        hold_df = _pd.DataFrame()
        try:
            hold_sql = f"""
                SELECT REPLACE(trade_date,'-','') as dt, broker,
                       long_vol, short_vol
                FROM futures_holding
                WHERE ts_code = '{hold_product}'
                  AND REPLACE(trade_date,'-','') >= '{since}'
                ORDER BY trade_date ASC
            """
            hold_df = _pd.read_sql(hold_sql, eng)
        except Exception:
            pass

        dumb_list = []
        smart_list = []
        total_oi_list = []
        dumb_chg_1d = None

        if not hold_df.empty:
            hold_df['net_vol'] = hold_df['long_vol'].fillna(0) - hold_df['short_vol'].fillna(0)
            # 清理代客后缀
            hold_df['broker_clean'] = hold_df['broker'].astype(str).str.replace(
                r'[（\(]代客[）\)]', '', regex=True).str.strip()

            def _broker_type(b):
                for d in BROKERS_DUMB:
                    if d in b: return 'dumb'
                for s in BROKERS_SMART:
                    if s in b: return 'smart'
                return 'other'

            hold_df['type'] = hold_df['broker_clean'].apply(_broker_type)

            dumb_agg  = hold_df[hold_df['type']=='dumb'].groupby('dt')['net_vol'].sum().reset_index().sort_values('dt')
            smart_agg = hold_df[hold_df['type']=='smart'].groupby('dt')['net_vol'].sum().reset_index().sort_values('dt')

            dumb_agg['chg']  = dumb_agg['net_vol'].diff(1).fillna(0)
            smart_agg['chg'] = smart_agg['net_vol'].diff(1).fillna(0)

            dumb_list  = [{"dt": str(r["dt"]), "net": int(r["net_vol"]), "chg": int(r["chg"])} for _, r in dumb_agg.iterrows()]
            smart_list = [{"dt": str(r["dt"]), "net": int(r["net_vol"]), "chg": int(r["chg"])} for _, r in smart_agg.iterrows()]
            dumb_chg_1d = dumb_list[-1]["chg"] if dumb_list else None

        # 总持仓量（主力合约 oi）
        if not ohlc_df.empty and 'oi' in ohlc_df.columns:
            total_oi_list = [
                {"dt": str(r["dt"]), "v": int(r["oi"]) if r["oi"] == r["oi"] else 0}
                for _, r in ohlc_df.iterrows()
            ]

        # ── 5. 整理输出 ───────────────────────────────────────
        ohlc_list = []
        for _, r in ohlc_df.iterrows():
            try:
                ohlc_list.append({
                    "dt": str(r["dt"]),
                    "o": round(float(r["o"]), 2),
                    "h": round(float(r["h"]), 2),
                    "l": round(float(r["l"]), 2),
                    "c": round(float(r["c"]), 2),
                    "pct": round(float(r["pct_chg"]), 2),
                })
            except Exception:
                pass

        # iv 已是百分比形式（如 18.53 = 18.53%），直接返回
        iv_list = [
            {"dt": str(r["dt"]), "v": round(float(r["iv"]), 2)}
            for _, r in iv_df.iterrows()
            if r["iv"] is not None
        ] if not iv_df.empty else []

        cur_price = ohlc_list[-1]["c"] if ohlc_list else None
        cur_pct   = round(float(ohlc_df.iloc[-1]["pct_chg"]), 2) if not ohlc_df.empty else None
        cur_iv    = iv_list[-1]["v"] if iv_list else None

        payload = {
            "product":       product.lower(),
            "cn_name":       cn_name,
            "main_contract": main_contract,
            "cur_price":     cur_price,
            "cur_pct":       cur_pct,
            "cur_iv":        cur_iv,
            "dumb_chg_1d":   dumb_chg_1d,
            "ohlc":          ohlc_list,
            "iv":            iv_list,
            "dumb":          dumb_list,
            "smart":         smart_list,
            "total_oi":      total_oi_list,
        }
        try:
            _redis.setex(cache_key, _MARKET_CHART_CACHE_TTL, json.dumps(payload, ensure_ascii=False))
        except Exception as cache_err:
            print(f"[market_chart] cache_write_failed key={cache_key} err={cache_err}", flush=True)
        return payload
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"图表数据获取失败: {e}")


@app.get("/api/market/prices", tags=["行情"])
def market_prices(username: str = Depends(get_current_user)):
    """
    Shared futures prices from server-side cache.
    All users read the same Redis snapshot; no repeated upstream fetch per request.
    """
    try:
        raw = _redis.get(_PRICES_KEY)
    except Exception as e:
        print(f"[market_prices] REDIS_READ_FAIL: {e}", flush=True)
        raw = None

    if raw:
        try:
            payload = json.loads(raw)
            if isinstance(payload, dict):
                _save_last_prices_payload(payload)
                return payload
        except Exception as e:
            print(f"[market_prices] JSON_DECODE_FAIL: {e}", flush=True)

    # Redis cold-start or temporary failure fallback
    local_fallback = _load_last_prices_payload()
    if local_fallback.get("items") or local_fallback.get("contracts"):
        return local_fallback

    return {"items": [], "is_trading": _is_trading_hours(), "refreshed_at": "", "contracts": {}}


# ════════════════════════════════════════════════════════════
#  PORTFOLIO — 持仓体检
# ════════════════════════════════════════════════════════════

class _BytesFileWrapper(io.BytesIO):
    """让 BytesIO 兼容 vision_tools.analyze_portfolio_image 的 seek/read 接口。"""
    pass


@app.post("/api/portfolio/upload", tags=["持仓体检"])
async def portfolio_upload(
    file: UploadFile = File(..., description="持仓截图，支持 jpg/png，不超过 10MB"),
    username: str = Depends(get_current_user),
):
    """
    上传持仓截图，流程:
    1. 视觉 AI 识别截图中的持仓股票
    2. 提交后台 Celery 分析任务
    3. 返回 task_id，客户端轮询 /api/portfolio/status/{task_id}
    """
    image_bytes = await file.read()

    if len(image_bytes) > 10 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="图片不能超过 10MB")

    if not image_bytes:
        raise HTTPException(status_code=400, detail="图片内容为空")

    # 生成截图指纹（去重用）
    screenshot_hash = hashlib.md5(image_bytes).hexdigest()

    # 视觉识别持仓
    file_wrapper = _BytesFileWrapper(image_bytes)
    vision_result = analyze_portfolio_image(file_wrapper)

    if not vision_result.get("ok"):
        raise HTTPException(
            status_code=422,
            detail=vision_result.get("error") or "无法识别持仓截图，请上传清晰的持仓页截图",
        )

    positions = vision_result.get("positions") or []
    if not positions:
        raise HTTPException(status_code=422, detail="未从截图中识别到任何持仓，请确认截图内容")

    # 提交后台分析任务
    task_id = TaskManager.create_portfolio_task(
        user_id=username,
        positions=positions,
        screenshot_hash=screenshot_hash,
        source_text=vision_result.get("raw_text", ""),
    )

    return {
        "task_id": task_id,
        "recognized_count": len(positions),
        "message": f"已识别 {len(positions)} 只持仓，正在生成体检报告...",
    }


@app.get("/api/portfolio/status/{task_id}", tags=["持仓体检"])
def portfolio_status(
    task_id: str,
    username: str = Depends(get_current_user),
):
    """
    轮询持仓分析任务进度。
    status 值: pending | processing | success | error
    """
    status = TaskManager.get_task_status(task_id)
    if status.get("status") in ("success", "error"):
        TaskManager.clear_user_pending_portfolio_task(username)
    return status


@app.get("/api/portfolio/result", tags=["持仓体检"])
def portfolio_result(username: str = Depends(get_current_user)):
    """
    获取用户最新的持仓体检结果（数据库中已完成的分析）。
    has_data=False 表示尚无数据，引导用户上传截图。
    """
    snapshot = de.get_user_portfolio_snapshot(username)
    if not snapshot:
        return {"has_data": False, "snapshot": None}
    return {"has_data": True, "snapshot": snapshot}


# ════════════════════════════════════════════════════════════
#  USER — 用户资料
# ════════════════════════════════════════════════════════════

@app.get("/api/user/profile", tags=["用户"])
def user_profile(username: str = Depends(get_current_user)):
    """获取用户基本资料、风险偏好与当前订阅频道。"""
    profile = de.get_user_profile(username) or {}
    info = auth.get_user_info(username) or {}
    subscriptions = sub_svc.get_user_subscriptions(username) or []

    return {
        "username": username,
        "email": auth.get_masked_email(username),
        "level": info.get("level", 1),
        "risk_preference": profile.get("risk_preference", "未知"),
        "focus_assets": profile.get("focus_assets", ""),
        "subscriptions": [
            {
                # 兼容 subscription_service 返回结构（code/name/expire_at）
                "channel_name": s.get("channel_name") or s.get("name") or "",
                "channel_code": (s.get("channel_code") or s.get("code") or "").strip().lower(),
                "expires_at": _fmt_expire_at(s.get("expires_at") or s.get("expire_at") or ""),
                "is_active": s.get("is_active", False),
            }
            for s in subscriptions
        ],
    }


# ════════════════════════════════════════════════════════════
#  KLINE TRAINING — K线训练
# ════════════════════════════════════════════════════════════

import kline_game as kg

def _parse_kline_bars(df) -> list:
    """从 DataFrame 解析 K 线 bars 列表（公共工具函数）"""
    import pandas as _pd

    def _sf(v):
        try:
            f = float(v)
            return f if _pd.notna(f) else None
        except Exception:
            return None

    bars = []
    for _, r in df.iterrows():
        o, h, l, c = _sf(r.get('open_price')), _sf(r.get('high_price')), _sf(r.get('low_price')), _sf(r.get('close_price'))
        if None in (o, h, l, c):
            continue
        v = _sf(r.get('vol')) or 0.0
        td = r.get('trade_date') if 'trade_date' in r else None
        if td is None and hasattr(r, 'name'):
            td = r.name
        import pandas as _pd2
        if isinstance(td, _pd2.Timestamp):
            td = td.strftime('%Y%m%d')
        elif td is not None:
            td = str(td)[:10].replace('-', '')
        bars.append({'dt': td or '', 'o': o, 'h': h, 'l': l, 'c': c, 'v': v})
    return bars


@app.get("/api/kline/data", tags=["K线训练"])
def kline_data(username: str = Depends(get_current_user)):
    """
    【第一步】仅获取随机K线数据，不创建游戏记录。
    玩家看到K线图开始运行后，再由前端调用 /api/kline/start 创建记录。
    这样加载期间离开不会被判定为中途放弃。
    """
    try:
        symbol, symbol_name, symbol_type, df = kg.get_random_kline_data(bars=100, history_bars=60)
        if df is None or len(df) < 100:
            raise HTTPException(status_code=500, detail="K线数据加载失败，请重试")

        bars = _parse_kline_bars(df)
        if len(bars) < 100:
            raise HTTPException(status_code=500, detail="K线数据质量不足，请重试")

        user_capital = kg.get_user_capital(username) or 100000
        return {
            "symbol": symbol,
            "symbol_name": symbol_name,
            "symbol_type": symbol_type,
            "capital": user_capital,
            "history_count": 60,
            "bars": bars,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"K线数据加载失败: {e}")


@app.post("/api/kline/start", tags=["K线训练"])
def kline_start(body: dict, username: str = Depends(get_current_user)):
    """
    【第二步】K线图开始运行时创建游戏记录（此时才算正式开始游戏）。
    body: {symbol, symbol_name, symbol_type, capital, leverage, speed}
    """
    try:
        symbol      = body.get("symbol", "")
        symbol_name = body.get("symbol_name", "")
        symbol_type = body.get("symbol_type", "futures")
        capital     = float(body.get("capital", 100000))
        leverage    = int(body.get("leverage", 1))
        speed       = int(body.get("speed", 1))
        game_id = kg.start_game(username, symbol, symbol_name, symbol_type, capital, leverage, speed)
        return {"game_id": game_id or 0}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建游戏记录失败: {e}")


# 保留旧端点兼容性（直接调用新流程）
@app.post("/api/kline/new", tags=["K线训练"])
def kline_new(username: str = Depends(get_current_user)):
    """旧端点，兼容旧版前端，内部等同于 /api/kline/data + /api/kline/start"""
    data = kline_data(username)
    start_body = {"symbol": data["symbol"], "symbol_name": data["symbol_name"],
                  "symbol_type": data["symbol_type"], "capital": data["capital"],
                  "leverage": 1, "speed": 1}
    start_res = kline_start(start_body, username)
    return {**data, "game_id": start_res["game_id"]}


@app.post("/api/kline/save", tags=["K线训练"])
def kline_save(body: dict, username: str = Depends(get_current_user)):
    """
    保存游戏结果（游戏结束时调用）。
    body: {game_id, profit, profit_rate, trade_count, max_drawdown}
    """
    try:
        game_id     = int(body.get('game_id') or 0)
        profit      = float(body.get('profit', 0))
        profit_rate = float(body.get('profit_rate', 0))
        trade_count = int(body.get('trade_count', 0))
        max_drawdown= float(body.get('max_drawdown', 0))
        capital     = float(body.get('capital', 100000))

        if game_id:
            kg.end_game(game_id, username, 'finished', 'completed',
                        profit, profit_rate, capital + profit, trade_count, max_drawdown)

        return {"ok": True, "profit": profit, "profit_rate": profit_rate}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"保存失败: {e}")


@app.get("/api/kline/check", tags=["K线训练"])
def kline_check(username: str = Depends(get_current_user)):
    """检查是否有未完成（中途离开）的游戏，有则返回游戏信息和惩罚金额。"""
    try:
        unfinished = kg.check_unfinished_game(username)
        if unfinished:
            return {
                "has_unfinished": True,
                "game_id": unfinished["id"],
                "symbol_name": unfinished.get("symbol_name", "???"),
                "capital": float(unfinished.get("capital_before", 100000)),
                "penalty": 20000,
            }
        return {"has_unfinished": False}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"检查失败: {e}")


@app.post("/api/kline/abandon", tags=["K线训练"])
def kline_abandon(body: dict, username: str = Depends(get_current_user)):
    """结算中途放弃的游戏，扣除固定惩罚 -20,000 元。"""
    try:
        game_id = int(body.get("game_id") or 0)
        if not game_id:
            raise HTTPException(status_code=400, detail="缺少 game_id")
        kg.settle_abandoned_game(username, game_id)
        return {"ok": True, "penalty": 20000}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"结算失败: {e}")


@app.get("/api/kline/entry", tags=["K线训练"])
def kline_entry(username: str = Depends(get_current_user)):
    """K线训练入口页：玩家当前资金 + 三榜排行榜（Top 10）。"""
    capital = kg.get_user_capital(username) or 100000
    boards  = kg.get_training_entry_leaderboards(limit=10, min_completed=2)
    return {"capital": capital, "leaderboard": boards}
