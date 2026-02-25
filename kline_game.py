"""
K线训练场 - 游戏逻辑模块

【修复说明】
本文件修复了以下问题：
1. 数据库字段 'loss_games' 不存在的问题
2. 添加了更详细的错误日志
3. 添加了数据库字段自动检查和修复功能

【使用前必须执行的SQL】
ALTER TABLE kline_game_stats ADD COLUMN loss_games INT DEFAULT 0 AFTER win_games;
"""
import pandas as pd
import numpy as np
from sqlalchemy import text
from datetime import datetime, timedelta
import random
import time
import re
import json
import gc
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import traceback  # 【新增】用于打印详细错误

# 导入数据库引擎
from data_engine import engine

# ==========================================
# 成就配置
# ==========================================
ACHIEVEMENTS = {
    # 新手
    "dual_leverage": {"name": "双杠杆试炼", "desc": "1倍和10倍杠杆都玩过", "exp": 300},
    "games_20": {"name": "勤学苦练", "desc": "累计完成20局", "exp": 500},
    # 盈利
    "profit_100k": {"name": "十万赢家", "desc": "单局盈利≥100,000", "exp": 1200},
    "profit_500k": {"name": "半百万赢家", "desc": "单局盈利≥500,000", "exp": 3000},
    "rate_50": {"name": "高胜倍率", "desc": "单局收益率≥50%", "exp": 1500},
    # 亏损
    "loss_100k": {"name": "风险教育I", "desc": "单局亏损≥100,000", "exp": 300},
    "loss_500k": {"name": "风险教育II", "desc": "单局亏损≥500,000", "exp": 800},
    # 稳健
    "no_drawdown_win": {"name": "零回撤大师", "desc": "单局无回撤且盈利", "exp": 2500},
    "streak_5": {"name": "五连胜", "desc": "连续5局盈利", "exp": 1500},
    "streak_10": {"name": "十连胜", "desc": "连续10局盈利", "exp": 4000},
    # 操作
    "trader_20": {"name": "高频操盘手", "desc": "单局交易≥20次", "exp": 600},
    "zen_1_trade_win": {"name": "一击制胜", "desc": "单局只交易1次且盈利", "exp": 900},
    # 风控
    "lev10_win": {"name": "十倍破局", "desc": "10倍杠杆盈利通关1次", "exp": 1800},
    # 里程碑（毛额）
    "gross_profit_100k": {"name": "盈利里程碑I", "desc": "累计总盈利（毛额）≥100,000", "exp": 1000},
    "gross_profit_1m": {"name": "盈利里程碑II", "desc": "累计总盈利（毛额）≥1,000,000", "exp": 3500},
    "gross_profit_10m": {"name": "盈利里程碑III", "desc": "累计总盈利（毛额）≥10,000,000", "exp": 12000},
    "gross_loss_500k": {"name": "风险承担者", "desc": "累计总亏损（毛额）≥500,000", "exp": 1000},
}

# 每局完成获得的基础经验
BASE_EXP_PER_GAME = 50

_TRADE_API_LOCK = threading.Lock()
_TRADE_API_STATE = {
    "thread": None,
    "server": None,
    "port": None,
    "same_domain_mounted": False,
    "same_domain_path": "/api/kline/trades/batch",
}


def _to_int(v, default=0):
    try:
        return int(v)
    except Exception:
        return default


def _to_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default


def _parse_trade_time(v):
    if isinstance(v, datetime):
        return v.replace(tzinfo=None)
    if not v:
        return datetime.now()
    try:
        s = str(v).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt.replace(tzinfo=None)
    except Exception:
        return datetime.now()


def _parse_trade_date(v):
    if not v:
        return None
    try:
        s = str(v)[:10]
        datetime.strptime(s, "%Y-%m-%d")
        return s
    except Exception:
        return None


def _ensure_trade_storage():
    """确保交易明细存储结构存在（表与必要字段）"""
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS kline_game_trades (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    game_id BIGINT NOT NULL,
                    user_id VARCHAR(128) NOT NULL,
                    trade_seq INT NOT NULL DEFAULT 0,
                    trade_time DATETIME NULL,
                    bar_index INT NULL,
                    bar_date DATE NULL,
                    action VARCHAR(32) NOT NULL,
                    price DECIMAL(18,6) DEFAULT 0,
                    lots INT DEFAULT 0,
                    amount DECIMAL(18,2) DEFAULT 0,
                    leverage INT DEFAULT 1,
                    position_before TEXT NULL,
                    position_after TEXT NULL,
                    realized_pnl_after DECIMAL(18,2) DEFAULT 0,
                    floating_pnl_after DECIMAL(18,2) DEFAULT 0,
                    symbol VARCHAR(32) NULL,
                    symbol_name VARCHAR(64) NULL,
                    symbol_type VARCHAR(16) NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uk_game_seq (game_id, trade_seq),
                    KEY idx_game (game_id),
                    KEY idx_user_time (user_id, trade_time)
                )
            """))

            # 兼容旧表：补字段
            desc_rows = conn.execute(text("DESCRIBE kline_game_trades")).fetchall()
            cols = [r[0] for r in desc_rows]
            col_types = {str(r[0]): str(r[1]).lower() for r in desc_rows}
            col_meta = {str(r[0]): r for r in desc_rows}
            add_cols = {
                "trade_seq": "INT NOT NULL DEFAULT 0",
                "bar_date": "DATE NULL",
                "lots": "INT DEFAULT 0",
                "leverage": "INT DEFAULT 1",
                "realized_pnl_after": "DECIMAL(18,2) DEFAULT 0",
                "floating_pnl_after": "DECIMAL(18,2) DEFAULT 0",
                "symbol": "VARCHAR(32) NULL",
                "symbol_name": "VARCHAR(64) NULL",
                "symbol_type": "VARCHAR(16) NULL",
                "created_at": "DATETIME DEFAULT CURRENT_TIMESTAMP",
                "updated_at": "DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
            }
            for col, ddl in add_cols.items():
                if col not in cols:
                    conn.execute(text(f"ALTER TABLE kline_game_trades ADD COLUMN {col} {ddl}"))

            # 兼容旧结构：trade_seq 若可为空或默认空值，改成 NOT NULL DEFAULT 0
            seq_meta = col_meta.get("trade_seq")
            if seq_meta is not None:
                is_nullable = str(seq_meta[2]).upper() == "YES"
                default_val = seq_meta[4]
                if is_nullable or default_val is None:
                    conn.execute(text("ALTER TABLE kline_game_trades MODIFY COLUMN trade_seq INT NOT NULL DEFAULT 0"))

            # 兼容旧结构：若仓位字段过短（如 VARCHAR），改为 LONGTEXT 避免 JSON 溢出
            pos_type_ok = {"text", "mediumtext", "longtext", "json"}
            for pos_col in ("position_before", "position_after"):
                ctype = (col_types.get(pos_col) or "").lower()
                if ctype and not any(t in ctype for t in pos_type_ok):
                    conn.execute(text(f"ALTER TABLE kline_game_trades MODIFY COLUMN {pos_col} LONGTEXT NULL"))

            idx = conn.execute(
                text("SHOW INDEX FROM kline_game_trades WHERE Key_name = 'uk_game_seq'")
            ).fetchall()
            if not idx:
                conn.execute(text("ALTER TABLE kline_game_trades ADD UNIQUE KEY uk_game_seq (game_id, trade_seq)"))

            # 给游戏记录补充“是否已落交易明细”标记，防重复
            rec_cols = [r[0] for r in conn.execute(text("DESCRIBE kline_game_records")).fetchall()]
            rec_add = {
                "trades_persisted": "TINYINT(1) DEFAULT 0",
                "trades_persisted_at": "DATETIME NULL",
                "trade_rows": "INT DEFAULT 0",
            }
            for col, ddl in rec_add.items():
                if col not in rec_cols:
                    conn.execute(text(f"ALTER TABLE kline_game_records ADD COLUMN {col} {ddl}"))
        return True
    except Exception as e:
        print(f"[TRADE_STORAGE] 初始化失败: {e}")
        traceback.print_exc()
        return False


def save_trade_batch(game_id, user_id, trades, symbol=None, symbol_name=None, symbol_type=None):
    """批量写入一局交易明细（幂等）"""
    if not game_id or not user_id:
        return {"ok": False, "message": "missing game_id/user_id"}
    if not isinstance(trades, list):
        return {"ok": False, "message": "trades must be a list"}
    if len(trades) > 2000:
        return {"ok": False, "message": "trades too many"}
    if not _ensure_trade_storage():
        return {"ok": False, "message": "trade storage not ready"}

    try:
        with engine.begin() as conn:
            game_row = conn.execute(
                text("""
                    SELECT id, user_id, COALESCE(trades_persisted, 0) AS trades_persisted
                    FROM kline_game_records
                    WHERE id = :gid
                    LIMIT 1
                """),
                {"gid": game_id},
            ).fetchone()

            if not game_row:
                return {"ok": False, "message": "game not found"}

            owner = str(game_row[1] or "")
            if owner != str(user_id):
                return {"ok": False, "message": "game ownership mismatch"}

            if int(game_row[2] or 0) == 1:
                existing = conn.execute(
                    text("SELECT COUNT(*) FROM kline_game_trades WHERE game_id = :gid"),
                    {"gid": game_id},
                ).scalar() or 0
                return {"ok": True, "saved": 0, "already_persisted": True, "total_rows": int(existing)}

            ins_sql = text("""
                INSERT INTO kline_game_trades
                (game_id, user_id, trade_seq, trade_time, bar_index, bar_date, action, price, lots, amount,
                 leverage, position_before, position_after, realized_pnl_after, floating_pnl_after,
                 symbol, symbol_name, symbol_type)
                VALUES
                (:gid, :uid, :seq, :trade_time, :bar_index, :bar_date, :action, :price, :lots, :amount,
                 :leverage, :pos_before, :pos_after, :realized_after, :floating_after,
                 :symbol, :symbol_name, :symbol_type)
                ON DUPLICATE KEY UPDATE id = id
            """)

            saved_rows = 0
            for i, t in enumerate(trades, start=1):
                if not isinstance(t, dict):
                    continue

                seq = _to_int(t.get("trade_seq"), i)
                if seq <= 0:
                    seq = i
                lots = max(0, _to_int(t.get("lots"), 0))
                if lots <= 0:
                    continue

                payload = {
                    "gid": game_id,
                    "uid": owner,
                    "seq": seq,
                    "trade_time": _parse_trade_time(t.get("trade_time")),
                    "bar_index": _to_int(t.get("bar_index"), 0),
                    "bar_date": _parse_trade_date(t.get("bar_date")),
                    "action": str(t.get("action") or "unknown")[:32],
                    "price": _to_float(t.get("price"), 0.0),
                    "lots": lots,
                    "amount": _to_float(t.get("amount"), 0.0),
                    "leverage": max(1, _to_int(t.get("leverage"), 1)),
                    "pos_before": json.dumps(t.get("position_before") or {}, ensure_ascii=False, separators=(",", ":"))[:20000],
                    "pos_after": json.dumps(t.get("position_after") or {}, ensure_ascii=False, separators=(",", ":"))[:20000],
                    "realized_after": _to_float(t.get("realized_pnl_after"), 0.0),
                    "floating_after": _to_float(t.get("floating_pnl_after"), 0.0),
                    "symbol": str(t.get("symbol") or symbol or "")[:32],
                    "symbol_name": str(t.get("symbol_name") or symbol_name or "")[:64],
                    "symbol_type": str(t.get("symbol_type") or symbol_type or "")[:16],
                }
                conn.execute(ins_sql, payload)
                saved_rows += 1

            total_rows = conn.execute(
                text("SELECT COUNT(*) FROM kline_game_trades WHERE game_id = :gid"),
                {"gid": game_id},
            ).scalar() or 0

            conn.execute(
                text("""
                    UPDATE kline_game_records
                    SET trades_persisted = 1,
                        trades_persisted_at = :ts,
                        trade_rows = :rows
                    WHERE id = :gid
                      AND user_id = :uid
                """),
                {"gid": game_id, "uid": owner, "ts": datetime.now(), "rows": int(total_rows)},
            )

            return {"ok": True, "saved": int(saved_rows), "total_rows": int(total_rows)}
    except Exception as e:
        print(f"[TRADE_BATCH] 保存失败: game_id={game_id}, user={user_id}, err={e}")
        traceback.print_exc()
        return {"ok": False, "message": str(e)}


def _ensure_feedback_storage():
    """确保游戏反馈表存在"""
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS kline_game_feedback (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    game_id BIGINT NOT NULL,
                    user_id VARCHAR(128) NOT NULL,
                    rating INT NULL,
                    content TEXT NOT NULL,
                    symbol VARCHAR(32) NULL,
                    symbol_name VARCHAR(64) NULL,
                    symbol_type VARCHAR(16) NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uk_game_user (game_id, user_id),
                    KEY idx_user_time (user_id, created_at)
                )
            """))
        return True
    except Exception as e:
        print(f"[FEEDBACK] 初始化失败: {e}")
        traceback.print_exc()
        return False


def save_game_feedback(game_id, user_id, content, rating=None, symbol=None, symbol_name=None, symbol_type=None):
    """保存一局游戏反馈（同一局重复提交则更新）"""
    gid = _to_int(game_id, 0)
    uid = str(user_id or "").strip()
    text_content = str(content or "").strip()
    score = _to_int(rating, 0)

    if gid <= 0 or not uid:
        return {"ok": False, "message": "missing game_id/user_id"}
    if not text_content:
        return {"ok": False, "message": "feedback content is empty"}
    if len(text_content) > 2000:
        text_content = text_content[:2000]
    if score <= 0:
        score = None
    elif score > 5:
        score = 5

    if not _ensure_feedback_storage():
        return {"ok": False, "message": "feedback storage not ready"}

    try:
        with engine.begin() as conn:
            game_row = conn.execute(
                text("""
                    SELECT id, user_id
                    FROM kline_game_records
                    WHERE id = :gid
                    LIMIT 1
                """),
                {"gid": gid},
            ).fetchone()
            if not game_row:
                return {"ok": False, "message": "game not found"}
            owner = str(game_row[1] or "")
            if owner != uid:
                return {"ok": False, "message": "game ownership mismatch"}

            conn.execute(
                text("""
                    INSERT INTO kline_game_feedback
                    (game_id, user_id, rating, content, symbol, symbol_name, symbol_type)
                    VALUES
                    (:gid, :uid, :rating, :content, :symbol, :symbol_name, :symbol_type)
                    ON DUPLICATE KEY UPDATE
                        rating = VALUES(rating),
                        content = VALUES(content),
                        symbol = VALUES(symbol),
                        symbol_name = VALUES(symbol_name),
                        symbol_type = VALUES(symbol_type),
                        updated_at = CURRENT_TIMESTAMP
                """),
                {
                    "gid": gid,
                    "uid": uid,
                    "rating": score,
                    "content": text_content,
                    "symbol": str(symbol or "")[:32],
                    "symbol_name": str(symbol_name or "")[:64],
                    "symbol_type": str(symbol_type or "")[:16],
                },
            )
        return {"ok": True}
    except Exception as e:
        print(f"[FEEDBACK] 保存失败: game_id={gid}, user={uid}, err={e}")
        traceback.print_exc()
        return {"ok": False, "message": str(e)}


def ensure_trade_batch_api_server(host="0.0.0.0", port=8765):
    """
    启动轻量交易批量写入 API（幂等）
    POST /api/kline/trades/batch
    """
    _ensure_trade_storage()
    with _TRADE_API_LOCK:
        thread = _TRADE_API_STATE.get("thread")
        if thread and thread.is_alive():
            return _TRADE_API_STATE.get("port")

        class TradeBatchHandler(BaseHTTPRequestHandler):
            def _reply(self, code, payload):
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
                self.send_header("Access-Control-Allow-Headers", "Content-Type")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_OPTIONS(self):
                self._reply(200, {"ok": True})

            def do_POST(self):
                if self.path.rstrip("/") != "/api/kline/trades/batch":
                    self._reply(404, {"ok": False, "message": "not found"})
                    return
                try:
                    length = _to_int(self.headers.get("Content-Length"), 0)
                    raw = self.rfile.read(length) if length > 0 else b"{}"
                    payload = json.loads(raw.decode("utf-8") or "{}")
                    result = save_trade_batch(
                        game_id=_to_int(payload.get("game_id"), 0),
                        user_id=str(payload.get("user_id") or ""),
                        trades=payload.get("trades") or [],
                        symbol=payload.get("symbol"),
                        symbol_name=payload.get("symbol_name"),
                        symbol_type=payload.get("symbol_type"),
                    )
                    self._reply(200 if result.get("ok") else 400, result)
                except Exception as e:
                    self._reply(500, {"ok": False, "message": str(e)})

            def log_message(self, format, *args):
                return

        try:
            httpd = ThreadingHTTPServer((host, int(port)), TradeBatchHandler)
        except Exception as e:
            print(f"[TRADE_API] 启动失败: {e}")
            if "Address already in use" in str(e):
                return int(port)
            return None

        thread = threading.Thread(
            target=httpd.serve_forever,
            daemon=True,
            name="kline-trade-batch-api",
        )
        thread.start()
        _TRADE_API_STATE.update({"thread": thread, "server": httpd, "port": int(port)})
        print(f"[TRADE_API] 已启动: 0.0.0.0:{int(port)}")
        return int(port)


def _get_same_domain_trade_path():
    try:
        from streamlit import config as st_config

        base = st_config.get_option("server.baseUrlPath") or ""
        base = "/" + str(base).strip("/") if str(base).strip("/") else ""
        return f"{base}/api/kline/trades/batch"
    except Exception:
        return "/api/kline/trades/batch"


def ensure_same_domain_trade_batch_route():
    """
    将交易明细批量写入接口挂到 Streamlit 同域路由：
    POST {baseUrlPath}/api/kline/trades/batch
    """
    if not _ensure_trade_storage():
        return None

    with _TRADE_API_LOCK:
        if _TRADE_API_STATE.get("same_domain_mounted"):
            return _TRADE_API_STATE.get("same_domain_path")

        try:
            import tornado.web
        except Exception as e:
            print(f"[TRADE_API] 同域路由不可用（tornado导入失败）: {e}")
            return None

        route_path = _get_same_domain_trade_path()
        route_regex = rf"{re.escape(route_path.rstrip('/'))}/?"

        class SameDomainTradeBatchHandler(tornado.web.RequestHandler):
            def check_xsrf_cookie(self):
                return

            def set_default_headers(self):
                self.set_header("Content-Type", "application/json; charset=utf-8")
                self.set_header("Access-Control-Allow-Origin", "*")
                self.set_header("Access-Control-Allow-Methods", "POST, OPTIONS")
                self.set_header("Access-Control-Allow-Headers", "Content-Type")

            def options(self):
                self.set_status(204)
                self.finish()

            def post(self):
                try:
                    raw = self.request.body or b"{}"
                    payload = json.loads(raw.decode("utf-8") or "{}")
                    result = save_trade_batch(
                        game_id=_to_int(payload.get("game_id"), 0),
                        user_id=str(payload.get("user_id") or ""),
                        trades=payload.get("trades") or [],
                        symbol=payload.get("symbol"),
                        symbol_name=payload.get("symbol_name"),
                        symbol_type=payload.get("symbol_type"),
                    )
                    self.set_status(200 if result.get("ok") else 400)
                    self.finish(json.dumps(result, ensure_ascii=False))
                except Exception as e:
                    self.set_status(500)
                    self.finish(json.dumps({"ok": False, "message": str(e)}, ensure_ascii=False))

        mounted = 0
        for obj in gc.get_objects():
            try:
                if not isinstance(obj, tornado.web.Application):
                    continue
                obj.add_handlers(r".*$", [(route_regex, SameDomainTradeBatchHandler)])
                mounted += 1
            except Exception:
                continue

        if mounted > 0:
            _TRADE_API_STATE["same_domain_mounted"] = True
            _TRADE_API_STATE["same_domain_path"] = route_path
            print(f"[TRADE_API] 同域路由已挂载: {route_path}")
            return route_path

        print("[TRADE_API] 同域路由挂载失败：未找到可注入的 Streamlit 应用实例")
        return None


def get_trade_batch_api_url(prefer_same_domain=True, fallback_port=8765):
    """
    返回交易明细批量写入 API 地址。
    默认优先同域路由；若失败则回退到本机端口服务。
    """
    if prefer_same_domain:
        same_domain = ensure_same_domain_trade_batch_route()
        if same_domain:
            return same_domain

    port = ensure_trade_batch_api_server(port=fallback_port)
    if port:
        return f"http://127.0.0.1:{int(port)}/api/kline/trades/batch"
    return ""


# ==========================================
# 【新增】数据库字段检查和修复函数
# ==========================================
def ensure_database_columns():
    """
    【新增函数】确保数据库表有所有必需的字段
    在程序启动时调用一次即可
    """
    try:
        with engine.connect() as conn:
            # 检查 kline_game_stats 表的字段
            result = conn.execute(text("DESCRIBE kline_game_stats"))
            existing_columns = [row[0] for row in result.fetchall()]
            print(f"[DB_CHECK] kline_game_stats 现有字段: {existing_columns}")

            # 必需的字段及其定义
            required_columns = {
                'loss_games': 'INT DEFAULT 0',
                'win_games': 'INT DEFAULT 0',
                'total_games': 'INT DEFAULT 0',
                'total_profit': 'DECIMAL(15,2) DEFAULT 0',
                'gross_profit': 'DECIMAL(18,2) DEFAULT 0',
                'gross_loss_abs': 'DECIMAL(18,2) DEFAULT 0',
                'best_profit': 'DECIMAL(15,2) DEFAULT 0',
                'worst_loss': 'DECIMAL(15,2) DEFAULT 0',
                'best_profit_rate': 'DECIMAL(10,4) DEFAULT 0',
                'worst_loss_rate': 'DECIMAL(10,4) DEFAULT 0',
                'current_streak': 'INT DEFAULT 0',
                'max_streak': 'INT DEFAULT 0',
                'total_trades': 'INT DEFAULT 0',
                'avg_trades_per_game': 'DECIMAL(10,2) DEFAULT 0',
                'best_max_drawdown': 'DECIMAL(10,4) DEFAULT 0',
                'worst_max_drawdown': 'DECIMAL(10,4) DEFAULT 0',
            }

            missing_columns = []
            for col, definition in required_columns.items():
                if col not in existing_columns:
                    missing_columns.append((col, definition))
                    print(f"[DB_CHECK] ❌ 缺失字段: {col}")
                else:
                    print(f"[DB_CHECK] ✓ 字段存在: {col}")

            return missing_columns

    except Exception as e:
        print(f"[DB_CHECK] 检查数据库字段失败: {e}")
        traceback.print_exc()
        return []


def fix_missing_columns(missing_columns):
    """
    【新增函数】自动修复缺失的数据库字段
    """
    if not missing_columns:
        print("[DB_FIX] 没有需要修复的字段")
        return True

    try:
        with engine.begin() as conn:
            for col, definition in missing_columns:
                try:
                    sql = f"ALTER TABLE kline_game_stats ADD COLUMN {col} {definition}"
                    conn.execute(text(sql))
                    print(f"[DB_FIX] ✓ 成功添加字段: {col}")
                except Exception as e:
                    if 'Duplicate column' in str(e):
                        print(f"[DB_FIX] 字段已存在: {col}")
                    else:
                        print(f"[DB_FIX] ❌ 添加字段失败 {col}: {e}")
                        return False
        return True
    except Exception as e:
        print(f"[DB_FIX] 修复数据库失败: {e}")
        traceback.print_exc()
        return False


# ==========================================
# 获取随机K线数据
# ==========================================
_SYMBOL_CACHE = {
    "stock": {"ts": 0, "min_bars": 0, "symbols": []},
    "index": {"ts": 0, "min_bars": 0, "symbols": []},
    "future": {"ts": 0, "min_bars": 0, "symbols": []},
}

_FUTURES_NAME_MAP = {
    # 金属
    "AU": "黄金", "AG": "白银", "CU": "铜", "AL": "铝", "ZN": "锌", "PB": "铅", "NI": "镍", "SN": "锡", "SS": "不锈钢",
    # 黑色
    "RB": "螺纹钢", "HC": "热卷", "I": "铁矿石", "J": "焦炭", "JM": "焦煤", "SF": "硅铁", "SM": "锰硅",
    # 能化
    "SC": "原油", "FU": "燃油", "BU": "沥青", "LU": "低硫燃料油", "PG": "液化石油气",
    "TA": "PTA", "MA": "甲醇", "EG": "乙二醇", "EB": "苯乙烯", "PP": "聚丙烯", "L": "线型低密度聚乙烯",
    "V": "PVC", "RU": "橡胶", "NR": "20号胶",
    # 农产品
    "M": "豆粕", "Y": "豆油", "P": "棕榈油", "A": "豆一", "B": "豆二", "OI": "菜油", "RM": "菜粕",
    "SR": "白糖", "CF": "棉花", "C": "玉米", "CS": "玉米淀粉", "JD": "鸡蛋", "AP": "苹果", "CJ": "红枣",
    # 金融/股指/国债
    "IF": "沪深300", "IH": "上证50", "IC": "中证500", "IM": "中证1000",
    "T": "10年国债", "TF": "5年国债", "TS": "2年国债",
}

_INDEX_NAME_MAP = {
    "000001.SH": "上证指数",
    "399001.SZ": "深证成指",
    "000300.SH": "沪深300",
    "000905.SH": "中证500",
    "000852.SH": "中证1000",
    "000688.SH": "科创50",
    "399006.SZ": "创业板指",
    "000016.SH": "上证50",
    "399005.SZ": "中小100",
    "932000.CSI": "中证2000",
}


def _future_root(symbol: str) -> str:
    s = (symbol or "").upper().split(".")[0]
    m = re.match(r"^([A-Z]+)", s)
    return m.group(1) if m else s


def _resolve_future_name(conn, symbol: str) -> str:
    # 1) 优先从库里取 name（如果有）
    try:
        row = conn.execute(
            text("""
                SELECT name
                FROM futures_price
                WHERE ts_code = :code
                  AND name IS NOT NULL
                  AND name <> ''
                LIMIT 1
            """),
            {"code": symbol},
        ).fetchone()
        if row and row[0]:
            return str(row[0])
    except Exception:
        pass

    # 2) 用代码前缀映射中文名（兼容带交易所后缀/数字等）
    root = _future_root(symbol)
    if root in _FUTURES_NAME_MAP:
        return _FUTURES_NAME_MAP[root]

    # 3) 再尝试按前缀在库里找一个 name
    try:
        row = conn.execute(
            text("""
                SELECT name
                FROM futures_price
                WHERE ts_code LIKE :prefix
                  AND name IS NOT NULL
                  AND name <> ''
                LIMIT 1
            """),
            {"prefix": f"{root}%"},
        ).fetchone()
        if row and row[0]:
            return str(row[0])
    except Exception:
        pass

    return symbol


def _resolve_index_name(symbol: str) -> str:
    return _INDEX_NAME_MAP.get(str(symbol or "").upper(), symbol)


def _get_cached_symbols(conn, market_type, min_bars, ttl_seconds=300):
    """
    获取可用品种列表（带缓存），减少每次游戏开始的聚合查询耗时
    """
    cache = _SYMBOL_CACHE.get(market_type, {})
    now = time.time()
    if cache and cache.get("symbols") and cache.get("min_bars") == min_bars and (now - cache.get("ts", 0) < ttl_seconds):
        return cache["symbols"]

    if market_type == "stock":
        sql = text("""
            SELECT ts_code
            FROM stock_price
            WHERE open_price IS NOT NULL
              AND close_price IS NOT NULL
            GROUP BY ts_code
            HAVING COUNT(*) >= :min_bars
        """)
    elif market_type == "index":
        # 参考项目内其他脚本（如 kline_tools.py）对 index_price 的查询方式
        sql = text("""
            SELECT ts_code
            FROM index_price
            WHERE open_price IS NOT NULL
              AND close_price IS NOT NULL
            GROUP BY ts_code
            HAVING COUNT(*) >= :min_bars
        """)
    else:
        sql = text("""
            SELECT ts_code
            FROM futures_price
            WHERE ts_code REGEXP '^[A-Za-z]+$'
              AND open_price IS NOT NULL
              AND close_price IS NOT NULL
            GROUP BY ts_code
            HAVING COUNT(*) >= :min_bars
        """)

    rows = conn.execute(sql, {"min_bars": min_bars}).fetchall()
    symbols = [r[0] for r in rows]
    _SYMBOL_CACHE[market_type] = {"ts": now, "min_bars": min_bars, "symbols": symbols}
    return symbols
def get_random_kline_data(bars=100, history_bars=60, _attempt=1, _max_attempts=12):
    """
    随机抽取一段历史K线数据
    返回: symbol, symbol_name, symbol_type, df (包含OHLCV)
    bars: 需要播放的K线数量
    history_bars: 初始显示的历史K线数量

    【修改】添加了更详细的日志
    """
    if _attempt > _max_attempts:
        print(f"[GET_KLINE] ❌ 超过最大尝试次数({_max_attempts})，终止抽样")
        return None, None, None, None

    total_bars = bars + history_bars  # 总共需要160根

    print(f"[GET_KLINE] 开始获取K线数据, 需要 {total_bars} 根, 尝试 {_attempt}/{_max_attempts}")

    try:
        with engine.connect() as conn:
            # 随机决定类型：股票40% / 指数10% / 期货50%
            selected_type = random.choices(
                ["stock", "index", "future"],
                weights=[40, 10, 50],
                k=1
            )[0]
            print(f"[GET_KLINE] 选择类型: {selected_type}")

            if selected_type == "stock":
                # 从股票中随机选一个有足够数据的（带缓存）
                symbols = _get_cached_symbols(conn, "stock", total_bars + 50)
                if not symbols:
                    print("[GET_KLINE] ❌ 未找到足够数据的股票")
                    return None, None, None, None

                symbol = random.choice(symbols)
                result = (symbol,)

                if not result:
                    print("[GET_KLINE] ❌ 未找到足够数据的股票")
                    return None, None, None, None

                symbol = result[0]
                symbol_type = 'stock'
                table_name = 'stock_price'
                print(f"[GET_KLINE] 选中股票: {symbol}")

                # 获取股票名称
                try:
                    name_sql = text("SELECT name FROM stock_price WHERE ts_code = :code AND name IS NOT NULL LIMIT 1")
                    name_result = conn.execute(name_sql, {"code": symbol}).fetchone()
                    symbol_name = name_result[0] if name_result else symbol
                except:
                    symbol_name = symbol
            elif selected_type == "index":
                symbols = _get_cached_symbols(conn, "index", total_bars + 50)
                if not symbols:
                    print("[GET_KLINE] ❌ 未找到足够数据的指数，改用股票")
                    return get_random_kline_data(bars, history_bars, _attempt + 1, _max_attempts)

                symbol = random.choice(symbols)
                symbol_type = 'index'
                table_name = 'index_price'
                symbol_name = _resolve_index_name(symbol)
                print(f"[GET_KLINE] 选中指数: {symbol} ({symbol_name})")
            else:
                # 从期货主力合约中随机选一个（不含数字的代码是主力，带缓存）
                symbols = _get_cached_symbols(conn, "future", total_bars + 50)
                if not symbols:
                    print("[GET_KLINE] ❌ 未找到足够数据的期货，改用股票")
                    # 如果期货没数据，改用股票
                    return get_random_kline_data(bars, history_bars, _attempt + 1, _max_attempts)

                symbol = random.choice(symbols)
                result = (symbol,)

                if not result:
                    print("[GET_KLINE] ❌ 未找到足够数据的期货，改用股票")
                    # 如果期货没数据，改用股票
                    return get_random_kline_data(bars, history_bars, _attempt + 1, _max_attempts)

                symbol = result[0]
                symbol_type = 'future'
                table_name = 'futures_price'
                print(f"[GET_KLINE] 选中期货: {symbol}")

                # 期货名优先取数据库 name，其次按代码映射中文名
                symbol_name = _resolve_future_name(conn, symbol)

            # 随机选择一个起始点
            count_sql = text(f"""
                SELECT COUNT(*) FROM {table_name} 
                WHERE ts_code = :code 
                  AND open_price IS NOT NULL 
                  AND close_price IS NOT NULL
            """)
            total_count = conn.execute(count_sql, {"code": symbol}).fetchone()[0]
            print(f"[GET_KLINE] {symbol} 总数据量: {total_count}")

            if total_count < total_bars + 50:
                print(f"[GET_KLINE] ❌ 数据不足: 需要 {total_bars + 50}, 实际 {total_count}")
                return None, None, None, None

            # 随机偏移，确保有足够数据
            max_offset = total_count - total_bars - 10
            offset = random.randint(0, max(0, max_offset))
            print(f"[GET_KLINE] 随机偏移: {offset}")

            # 获取K线数据（过滤NULL值）
            kline_sql = text(f"""
                SELECT trade_date, open_price, high_price, low_price, close_price, COALESCE(vol, 0) as vol
                FROM {table_name}
                WHERE ts_code = :code
                  AND open_price IS NOT NULL
                  AND close_price IS NOT NULL
                ORDER BY trade_date
                LIMIT :bars OFFSET :offset
            """)

            df = pd.read_sql(kline_sql, conn, params={
                "code": symbol,
                "bars": total_bars,
                "offset": offset
            })

            if len(df) < total_bars:
                print(f"[GET_KLINE] ❌ 获取数据不足: 需要 {total_bars}, 获取 {len(df)}")
                return None, None, None, None

            avg_vol = float(df["vol"].fillna(0).mean()) if "vol" in df.columns else 0.0
            if avg_vol < 500:
                print(f"[GET_KLINE] ❌ 成交量不足: avg(vol)={avg_vol:.2f} < 500, 标的 {symbol}")
                if _attempt < _max_attempts:
                    return get_random_kline_data(bars, history_bars, _attempt + 1, _max_attempts)
                print(f"[GET_KLINE] ❌ 已达到最大重试次数({_max_attempts})，放弃本次抽样")
                return None, None, None, None

            df['trade_date'] = pd.to_datetime(df['trade_date'])
            df.set_index('trade_date', inplace=True)

            print(f"[GET_KLINE] ✓ 成功获取 {len(df)} 根K线, 品种: {symbol_name}")

            return symbol, symbol_name, symbol_type, df

    except Exception as e:
        print(f"[GET_KLINE] ❌ 获取K线数据错误: {e}")
        traceback.print_exc()
        return None, None, None, None


# ==========================================
# 游戏核心逻辑
# ==========================================
def get_user_capital(user_id):
    """获取用户当前资金"""
    try:
        with engine.connect() as conn:
            sql = text("SELECT capital FROM users WHERE username = :uid")
            result = conn.execute(sql, {"uid": user_id}).fetchone()
            return result[0] if result else 0
    except:
        return 0


def update_user_capital(user_id, new_capital):
    """更新用户资金"""
    try:
        with engine.begin() as conn:
            sql = text("UPDATE users SET capital = :cap WHERE username = :uid")
            conn.execute(sql, {"cap": new_capital, "uid": user_id})
            return True
    except:
        return False


def add_user_experience(user_id, exp_amount):
    """增加用户经验值"""
    try:
        with engine.begin() as conn:
            sql = text("UPDATE users SET experience = experience + :exp WHERE username = :uid")
            conn.execute(sql, {"exp": exp_amount, "uid": user_id})
            return True
    except:
        return False


def get_game_info(game_id):
    """获取游戏信息"""
    try:
        with engine.connect() as conn:
            sql = text("""
                       SELECT id,
                              user_id,
                              symbol,
                              symbol_name,
                              symbol_type,
                              capital_before,
                              leverage,
                              status
                       FROM kline_game_records
                       WHERE id = :gid
                       """)
            result = conn.execute(sql, {"gid": game_id}).fetchone()
            if result:
                return {
                    'id': result[0],
                    'user_id': result[1],
                    'symbol': result[2],
                    'symbol_name': result[3],
                    'symbol_type': result[4],
                    'capital_before': result[5],
                    'leverage': result[6],
                    'status': result[7]
                }
            return None
    except Exception as e:
        print(f"获取游戏信息失败: {e}")
        return None


def create_game(user_id, speed, leverage, symbol, symbol_name, symbol_type,
                data_start_date, data_end_date, capital_before):
    """创建新游戏记录"""
    try:
        print(f"[CREATE_GAME] 创建游戏: user={user_id}, symbol={symbol}, capital={capital_before}")
        with engine.begin() as conn:
            sql = text("""
                       INSERT INTO kline_game_records
                       (user_id, game_start_time, speed, leverage, symbol, symbol_name,
                        symbol_type, data_start_date, data_end_date, capital_before, status)
                       VALUES (:uid, :start_time, :speed, :lev, :sym, :sym_name,
                               :sym_type, :start_date, :end_date, :cap, 'playing')
                       """)
            result = conn.execute(sql, {
                "uid": user_id,
                "start_time": datetime.now(),
                "speed": speed,
                "lev": leverage,
                "sym": symbol,
                "sym_name": symbol_name,
                "sym_type": symbol_type,
                "start_date": data_start_date,
                "end_date": data_end_date,
                "cap": capital_before
            })
            game_id = result.lastrowid
            print(f"[CREATE_GAME] ✓ 游戏创建成功, game_id={game_id}")
            return game_id
    except Exception as e:
        print(f"[CREATE_GAME] ❌ 创建游戏失败: {e}")
        traceback.print_exc()
        return None


def start_game(user_id, symbol, symbol_name, symbol_type, capital, leverage, speed=1):
    """
    开始新游戏 - 这是对create_game的简化包装

    【修改】添加了详细日志
    """
    try:
        print(f"[START_GAME] 开始游戏: user={user_id}, symbol={symbol}")

        # 设置默认值
        speed = speed or 1  # 由前端传入
        data_start_date = datetime.now().date()  # 当前日期作为开始日期
        data_end_date = data_start_date + timedelta(days=100)  # 100天后作为结束日期

        # 调用create_game函数
        game_id = create_game(
            user_id=user_id,
            speed=speed,
            leverage=leverage,
            symbol=symbol,
            symbol_name=symbol_name,
            symbol_type=symbol_type,
            data_start_date=data_start_date,
            data_end_date=data_end_date,
            capital_before=capital
        )

        if game_id:
            print(f"[START_GAME] ✓ 游戏开始成功, game_id={game_id}")
        else:
            print(f"[START_GAME] ❌ 游戏开始失败")

        return game_id
    except Exception as e:
        print(f"[START_GAME] ❌ 开始游戏失败: {e}")
        traceback.print_exc()
        return None


def update_game_progress(game_id, current_bar, position_direction, position_amount,
                         position_avg_price, last_price, profit, max_profit, max_drawdown):
    """更新游戏进度"""
    try:
        with engine.begin() as conn:
            sql = text("""
                       UPDATE kline_game_records
                       SET current_bar        = :bar,
                           position_direction = :dir,
                           position_amount    = :amt,
                           position_avg_price = :avg_price,
                           last_price         = :last_price,
                           profit             = :profit,
                           max_profit         = :max_profit,
                           max_drawdown       = :max_dd
                       WHERE id = :gid
                       """)
            conn.execute(sql, {
                "gid": game_id,
                "bar": current_bar,
                "dir": position_direction,
                "amt": position_amount,
                "avg_price": position_avg_price,
                "last_price": last_price,
                "profit": profit,
                "max_profit": max_profit,
                "max_dd": max_drawdown
            })
            return True
    except Exception as e:
        print(f"更新游戏进度失败: {e}")
        return False


def record_trade(game_id, user_id, bar_index, action, price, amount,
                 position_before, position_after):
    """记录交易操作"""
    try:
        if not _ensure_trade_storage():
            return False
        with engine.begin() as conn:
            next_seq = conn.execute(
                text("SELECT COALESCE(MAX(trade_seq), 0) + 1 FROM kline_game_trades WHERE game_id = :gid"),
                {"gid": game_id}
            ).scalar() or 1
            sql = text("""
                       INSERT INTO kline_game_trades
                       (game_id, user_id, trade_seq, trade_time, bar_index, action, price, amount,
                        position_before, position_after)
                       VALUES (:gid, :uid, :seq, :trade_time, :bar, :action, :price, :amt,
                               :pos_before, :pos_after)
                       ON DUPLICATE KEY UPDATE id = id
                       """)
            conn.execute(sql, {
                "gid": game_id,
                "uid": user_id,
                "seq": int(next_seq),
                "trade_time": datetime.now(),
                "bar": bar_index,
                "action": action,
                "price": price,
                "amt": amount,
                "pos_before": position_before,
                "pos_after": position_after
            })
            return True
    except Exception as e:
        print(f"记录交易失败: {e}")
        return False


def _recalculate_game_result_from_trades(game_id, user_id):
    """
    基于已落库交易明细重算结算结果（后端权威口径）
    返回:
      {
        "ok": bool,
        "profit": float,
        "profit_rate": float,
        "capital_before": float,
        "capital_after": float,
        "trade_count": int
      }
    """
    try:
        with engine.connect() as conn:
            game_row = conn.execute(text("""
                SELECT id, user_id, COALESCE(capital_before, 0) AS capital_before, COALESCE(leverage, 1) AS leverage
                FROM kline_game_records
                WHERE id = :gid
                  AND user_id = :uid
                LIMIT 1
            """), {"gid": game_id, "uid": user_id}).fetchone()
            if not game_row:
                return {"ok": False, "message": "game not found"}

            capital_before = float(game_row[2] or 0)
            leverage = max(1, int(float(game_row[3] or 1)))

            rows = conn.execute(text("""
                SELECT
                    id,
                    COALESCE(trade_seq, 0) AS trade_seq,
                    LOWER(COALESCE(action, '')) AS action,
                    COALESCE(price, 0) AS price,
                    COALESCE(lots, 0) AS lots,
                    COALESCE(amount, 0) AS amount
                FROM kline_game_trades
                WHERE game_id = :gid
                  AND user_id = :uid
                ORDER BY
                    CASE WHEN trade_seq IS NULL OR trade_seq = 0 THEN 2147483647 ELSE trade_seq END,
                    id ASC
            """), {"gid": game_id, "uid": user_id}).fetchall()

        lot_size = 1000.0
        realized = 0.0
        trade_count = 0
        pos_dir = None
        pos_lots = 0
        avg_price = 0.0

        open_long_actions = {"open_long", "add_long", "buy"}
        open_short_actions = {"open_short", "add_short", "sell_short"}
        close_long_actions = {"close_long", "close_long_partial", "close_long_all", "sell_long", "close"}
        close_short_actions = {"close_short", "close_short_partial", "close_short_all", "buy_to_cover"}

        for r in rows:
            action = str(r[2] or "").strip().lower()
            price = _to_float(r[3], 0.0)
            lots = _to_int(r[4], 0)
            if lots <= 0:
                amount = _to_float(r[5], 0.0)
                lots = int(round(amount / lot_size)) if amount > 0 else 0

            if not action or price <= 0 or lots <= 0:
                continue

            if action in open_long_actions:
                if pos_dir in (None, "long"):
                    if pos_dir is None:
                        pos_dir = "long"
                        pos_lots = lots
                        avg_price = price
                    else:
                        total_lots = pos_lots + lots
                        avg_price = ((avg_price * pos_lots) + (price * lots)) / total_lots
                        pos_lots = total_lots
                    trade_count += 1
                continue

            if action in open_short_actions:
                if pos_dir in (None, "short"):
                    if pos_dir is None:
                        pos_dir = "short"
                        pos_lots = lots
                        avg_price = price
                    else:
                        total_lots = pos_lots + lots
                        avg_price = ((avg_price * pos_lots) + (price * lots)) / total_lots
                        pos_lots = total_lots
                    trade_count += 1
                continue

            if action in close_long_actions and pos_dir == "long":
                close_lots = min(lots, pos_lots)
                if close_lots <= 0 or avg_price <= 0:
                    continue
                pnl = (price - avg_price) * close_lots * lot_size / avg_price * leverage
                realized += pnl
                pos_lots -= close_lots
                if pos_lots <= 0:
                    pos_dir = None
                    pos_lots = 0
                    avg_price = 0.0
                trade_count += 1
                continue

            if action in close_short_actions and pos_dir == "short":
                close_lots = min(lots, pos_lots)
                if close_lots <= 0 or avg_price <= 0:
                    continue
                pnl = (avg_price - price) * close_lots * lot_size / avg_price * leverage
                realized += pnl
                pos_lots -= close_lots
                if pos_lots <= 0:
                    pos_dir = None
                    pos_lots = 0
                    avg_price = 0.0
                trade_count += 1
                continue

        # 游戏结束前前端会自动全平；若数据异常仍有残仓，这里不再做二次估值，避免引入额外假设
        profit = float(round(realized, 2))
        profit_rate = (profit / capital_before) if capital_before > 0 else 0.0
        capital_after = capital_before + profit

        return {
            "ok": True,
            "profit": profit,
            "profit_rate": float(profit_rate),
            "capital_before": float(capital_before),
            "capital_after": float(capital_after),
            "trade_count": int(trade_count),
        }
    except Exception as e:
        print(f"[RECALC] ❌ 重算结算失败: game_id={game_id}, user={user_id}, err={e}")
        traceback.print_exc()
        return {"ok": False, "message": str(e)}


def end_game(game_id, user_id, status, end_reason, profit, profit_rate,
             capital_after, trade_count, max_drawdown):
    """结束游戏并结算"""
    try:
        print(f"[END_GAME] 结束游戏: game_id={game_id}, profit={profit}")

        # 正式结算口径：后端按交易明细重算，忽略前端上传盈亏
        if status == "finished" and end_reason == "completed":
            recalc = _recalculate_game_result_from_trades(game_id, user_id)
            if recalc.get("ok"):
                profit = float(recalc["profit"])
                profit_rate = float(recalc["profit_rate"])
                capital_after = float(recalc["capital_after"])
                trade_count = int(recalc["trade_count"])
                print(
                    f"[END_GAME] 使用后端重算结果: profit={profit}, "
                    f"profit_rate={profit_rate:.6f}, trades={trade_count}"
                )
            else:
                print(f"[END_GAME] ⚠ 后端重算失败，回退前端数值: {recalc.get('message')}")

        with engine.begin() as conn:
            # 结束游戏
            end_sql = text("""
                           UPDATE kline_game_records
                           SET game_end_time = :end_time,
                               status        = :status,
                               end_reason    = :end_reason,
                               profit        = :profit,
                               profit_rate   = :profit_rate,
                               capital_after = :capital_after,
                               trade_count   = :trade_count,
                               max_drawdown  = :max_drawdown
                           WHERE id = :gid
                             AND user_id = :uid
                           """)
            conn.execute(end_sql, {
                "gid": game_id,
                "uid": user_id,
                "end_time": datetime.now(),
                "status": status,
                "end_reason": end_reason,
                "profit": profit,
                "profit_rate": profit_rate,
                "capital_after": capital_after,
                "trade_count": trade_count,
                "max_drawdown": max_drawdown
            })

            # 更新用户资金
            update_user_capital(user_id, capital_after)

            # 更新用户统计
            update_user_stats(user_id, profit, profit_rate, status, trade_count, max_drawdown)

            # 增加基础经验
            add_user_experience(user_id, BASE_EXP_PER_GAME)

        print(f"[END_GAME] ✓ 游戏结束成功")
        return {
            "ok": True,
            "profit": float(profit),
            "profit_rate": float(profit_rate),
            "capital_after": float(capital_after),
            "trade_count": int(trade_count or 0),
        }
    except Exception as e:
        print(f"[END_GAME] ❌ 结束游戏失败: {e}")
        traceback.print_exc()
        return {"ok": False, "message": str(e)}


# ==========================================
# 【重点修改】update_user_stats 函数
# 这是导致 'loss_games' 字段不存在错误的函数
# ==========================================
def _ensure_stats_runtime_columns(conn):
    required = {
        "loss_games": "INT DEFAULT 0",
        "gross_profit": "DECIMAL(18,2) DEFAULT 0",
        "gross_loss_abs": "DECIMAL(18,2) DEFAULT 0",
    }
    for col, ddl in required.items():
        try:
            conn.execute(text(f"SELECT {col} FROM kline_game_stats LIMIT 1"))
        except Exception as e:
            if "Unknown column" in str(e):
                conn.execute(text(f"ALTER TABLE kline_game_stats ADD COLUMN {col} {ddl}"))
            else:
                raise


def _get_stats_columns(conn):
    rows = conn.execute(text("DESCRIBE kline_game_stats")).fetchall()
    return {r[0] for r in rows}


def update_user_stats(user_id, profit, profit_rate, status, trade_count, max_drawdown):
    """更新用户统计数据（兼容不同表结构，避免因字段差异失败）"""
    print(f"[UPDATE_STATS] 开始更新统计: user={user_id}, profit={profit}, status={status}")
    try:
        with engine.begin() as conn:
            try:
                _ensure_stats_runtime_columns(conn)
            except Exception:
                pass

            cols = _get_stats_columns(conn)
            row = conn.execute(text("SELECT * FROM kline_game_stats WHERE user_id = :uid"), {"uid": user_id}).fetchone()
            cur = dict(row._mapping) if row else {}

            is_win = profit > 0
            is_loss = profit < 0
            finished_inc = 1 if status == "finished" else 0
            abandoned_inc = 1 if status == "abandoned" else 0
            busted_inc = 1 if status == "busted" else 0
            win_inc = 1 if is_win else 0
            loss_inc = 1 if is_loss else 0
            gross_profit_add = max(0, float(profit))
            gross_loss_add = abs(min(0, float(profit)))

            if not row:
                fields = ["user_id"]
                values = [":uid"]
                params = {"uid": user_id}

                def add_field(col, val):
                    if col in cols:
                        fields.append(col)
                        values.append(f":{col}")
                        params[col] = val

                add_field("total_games", 1)
                add_field("finished_games", finished_inc)
                add_field("abandoned_games", abandoned_inc)
                add_field("busted_games", busted_inc)
                add_field("win_games", win_inc)
                add_field("loss_games", loss_inc)
                add_field("lose_games", loss_inc)
                add_field("total_profit", profit)
                add_field("gross_profit", gross_profit_add)
                add_field("gross_loss_abs", gross_loss_add)
                add_field("best_profit", max(0, profit))
                add_field("worst_loss", min(0, profit))
                add_field("best_profit_rate", max(0, profit_rate))
                add_field("worst_loss_rate", min(0, profit_rate))
                add_field("current_streak", 1 if is_win else 0)
                add_field("max_streak", 1 if is_win else 0)
                add_field("total_trades", trade_count)
                add_field("avg_trades_per_game", float(trade_count))
                add_field("best_max_drawdown", max_drawdown)
                add_field("worst_max_drawdown", max_drawdown)
                if "last_play_time" in cols:
                    fields.append("last_play_time")
                    values.append("NOW()")

                sql = f"INSERT INTO kline_game_stats ({', '.join(fields)}) VALUES ({', '.join(values)})"
                conn.execute(text(sql), params)
                print(f"[UPDATE_STATS] ✓ 新统计记录创建成功: user={user_id}")
                return True

            # existing row
            prev_streak = int(cur.get("current_streak", 0) or 0)
            prev_max_streak = int(cur.get("max_streak", 0) or 0)
            new_streak = prev_streak + 1 if is_win else 0
            new_max_streak = max(prev_max_streak, new_streak)
            next_total_games = int(cur.get("total_games", 0) or 0) + 1
            next_total_trades = int(cur.get("total_trades", 0) or 0) + int(trade_count or 0)
            next_avg_trades = (next_total_trades / next_total_games) if next_total_games > 0 else 0

            sets = []
            params = {"uid": user_id, "profit": profit, "rate": profit_rate, "max_dd": max_drawdown}

            def add_inc(col, param, val):
                if col in cols:
                    sets.append(f"{col} = COALESCE({col}, 0) + :{param}")
                    params[param] = val

            add_inc("total_games", "inc_games", 1)
            add_inc("finished_games", "inc_finished", finished_inc)
            add_inc("abandoned_games", "inc_abandoned", abandoned_inc)
            add_inc("busted_games", "inc_busted", busted_inc)
            add_inc("win_games", "inc_win", win_inc)
            add_inc("loss_games", "inc_loss", loss_inc)
            add_inc("lose_games", "inc_lose", loss_inc)
            add_inc("total_profit", "inc_profit", profit)
            add_inc("gross_profit", "inc_gross_profit", gross_profit_add)
            add_inc("gross_loss_abs", "inc_gross_loss", gross_loss_add)

            if "best_profit" in cols:
                sets.append("best_profit = GREATEST(COALESCE(best_profit, 0), :profit)")
            if "worst_loss" in cols:
                sets.append("worst_loss = LEAST(COALESCE(worst_loss, 0), :profit)")
            if "best_profit_rate" in cols:
                sets.append("best_profit_rate = GREATEST(COALESCE(best_profit_rate, 0), :rate)")
            if "worst_loss_rate" in cols:
                sets.append("worst_loss_rate = LEAST(COALESCE(worst_loss_rate, 0), :rate)")
            if "current_streak" in cols:
                sets.append("current_streak = :new_streak")
                params["new_streak"] = new_streak
            if "max_streak" in cols:
                sets.append("max_streak = :new_max_streak")
                params["new_max_streak"] = new_max_streak
            if "total_trades" in cols:
                sets.append("total_trades = :next_total_trades")
                params["next_total_trades"] = next_total_trades
            if "avg_trades_per_game" in cols:
                sets.append("avg_trades_per_game = :next_avg_trades")
                params["next_avg_trades"] = next_avg_trades
            if "best_max_drawdown" in cols:
                sets.append("best_max_drawdown = LEAST(COALESCE(best_max_drawdown, :max_dd), :max_dd)")
            if "worst_max_drawdown" in cols:
                sets.append("worst_max_drawdown = GREATEST(COALESCE(worst_max_drawdown, :max_dd), :max_dd)")
            if "last_play_time" in cols:
                sets.append("last_play_time = NOW()")

            if sets:
                sql = f"UPDATE kline_game_stats SET {', '.join(sets)} WHERE user_id = :uid"
                conn.execute(text(sql), params)
            print(f"[UPDATE_STATS] ✓ 统计更新成功: user={user_id}")
            return True

    except Exception as e:
        print(f"[UPDATE_STATS] ❌ 更新用户统计失败: {e}")
        traceback.print_exc()
        return False


def update_user_stats_fallback(user_id, profit, profit_rate, status, trade_count, max_drawdown):
    """
    【新增】备用的统计更新函数，不使用 loss_games 字段
    当 loss_games 字段不存在且无法自动添加时使用
    """
    print(f"[UPDATE_STATS_FALLBACK] 使用备用方法更新统计")

    try:
        with engine.begin() as conn:
            select_sql = text("SELECT * FROM kline_game_stats WHERE user_id = :uid")
            result = conn.execute(select_sql, {"uid": user_id}).fetchone()

            is_win = profit > 0

            if result is None:
                # 创建新记录（不包含 loss_games）
                insert_sql = text("""
                                  INSERT INTO kline_game_stats
                                  (user_id, total_games, win_games, total_profit,
                                   best_profit, worst_loss, best_profit_rate, worst_loss_rate,
                                   current_streak, max_streak, total_trades, avg_trades_per_game,
                                   best_max_drawdown, worst_max_drawdown)
                                  VALUES (:uid, 1, :win, :profit, :best_profit, :worst_loss,
                                          :best_rate, :worst_rate, :streak, :max_streak, :trades, :trades,
                                          :max_dd, :max_dd)
                                  """)
                conn.execute(insert_sql, {
                    "uid": user_id,
                    "win": 1 if is_win else 0,
                    "profit": profit,
                    "best_profit": max(0, profit),
                    "worst_loss": min(0, profit),
                    "best_rate": max(0, profit_rate),
                    "worst_rate": min(0, profit_rate),
                    "streak": 1 if is_win else 0,
                    "max_streak": 1 if is_win else 0,
                    "trades": trade_count,
                    "max_dd": max_drawdown
                })
            else:
                result_dict = dict(result._mapping)
                current_streak = result_dict.get('current_streak', 0)
                new_streak = (current_streak + 1) if is_win else 0
                max_streak_val = result_dict.get('max_streak', 0)
                new_max_streak = max(max_streak_val, new_streak) if is_win else max_streak_val

                total_trades = result_dict.get('total_trades', 0) + trade_count
                total_games = result_dict.get('total_games', 0) + 1
                avg_trades = total_trades / total_games if total_games > 0 else 0

                # 不更新 loss_games 字段
                update_sql = text("""
                                  UPDATE kline_game_stats
                                  SET total_games         = total_games + 1,
                                      win_games           = win_games + :win,
                                      total_profit        = total_profit + :profit,
                                      best_profit         = GREATEST(best_profit, :profit),
                                      worst_loss          = LEAST(worst_loss, :profit),
                                      best_profit_rate    = GREATEST(best_profit_rate, :rate),
                                      worst_loss_rate     = LEAST(worst_loss_rate, :rate),
                                      current_streak      = :current_streak,
                                      max_streak          = :max_streak,
                                      total_trades        = :total_trades,
                                      avg_trades_per_game = :avg_trades,
                                      best_max_drawdown   = LEAST(best_max_drawdown, :max_dd),
                                      worst_max_drawdown  = GREATEST(worst_max_drawdown, :max_dd)
                                  WHERE user_id = :uid
                                  """)
                conn.execute(update_sql, {
                    "uid": user_id,
                    "win": 1 if is_win else 0,
                    "profit": profit,
                    "rate": profit_rate,
                    "current_streak": new_streak,
                    "max_streak": new_max_streak,
                    "total_trades": total_trades,
                    "avg_trades": avg_trades,
                    "max_dd": max_drawdown
                })

        print(f"[UPDATE_STATS_FALLBACK] ✓ 备用方法更新成功")
        return True
    except Exception as e:
        print(f"[UPDATE_STATS_FALLBACK] ❌ 备用方法也失败了: {e}")
        traceback.print_exc()
        return False


def get_user_stats(user_id):
    """获取用户统计数据"""
    try:
        with engine.connect() as conn:
            sql = text("SELECT * FROM kline_game_stats WHERE user_id = :uid")
            result = conn.execute(sql, {"uid": user_id}).fetchone()
            if result:
                return dict(result._mapping)
            return None
    except:
        return None


def check_unfinished_game(user_id):
    """检查用户是否有未完成的游戏"""
    try:
        with engine.connect() as conn:
            sql = text("""
                       SELECT id, symbol, symbol_name, symbol_type, capital_before, leverage, game_start_time
                       FROM kline_game_records
                       WHERE user_id = :uid
                         AND status = 'playing'
                       ORDER BY game_start_time DESC LIMIT 1
                       """)
            result = conn.execute(sql, {"uid": user_id}).fetchone()
            if result:
                return dict(result._mapping)
            return None
    except Exception as e:
        print(f"检查未完成游戏失败: {e}")
        return None


def settle_abandoned_game(user_id, game_id, penalty=20000):
    """结算中途离开的游戏（固定扣除惩罚金额）"""
    print(f"[SETTLE_ABANDONED] 结算离开的游戏: user={user_id}, game_id={game_id}, penalty={penalty}")

    try:
        # 先获取游戏信息
        with engine.connect() as conn:
            sql = text("""
                       SELECT capital_before
                       FROM kline_game_records
                       WHERE id = :gid
                         AND user_id = :uid
                         AND status = 'playing'
                       """)
            game = conn.execute(sql, {"gid": game_id, "uid": user_id}).fetchone()

            if not game:
                print(f"[SETTLE_ABANDONED] ❌ 未找到游戏记录: game_id={game_id}, user_id={user_id}")
                return None

            capital_before = game[0]
            print(f"[SETTLE_ABANDONED] 游戏初始资金: {capital_before}")

        # 使用事务进行结算
        with engine.begin() as conn:
            # 固定扣除惩罚金额
            final_profit = -penalty
            capital_after = capital_before + final_profit
            profit_rate = final_profit / capital_before if capital_before > 0 else 0

            # 结束游戏
            end_sql = text("""
                           UPDATE kline_game_records
                           SET game_end_time = :end_time,
                               status        = 'abandoned',
                               end_reason    = 'user_left',
                               profit        = :profit,
                               profit_rate   = :profit_rate,
                               capital_after = :capital_after,
                               trade_count   = 0,
                               max_drawdown  = 0
                           WHERE id = :gid
                             AND user_id = :uid
                           """)
            conn.execute(end_sql, {
                "gid": game_id,
                "uid": user_id,
                "end_time": datetime.now(),
                "profit": int(final_profit),
                "profit_rate": profit_rate,
                "capital_after": int(capital_after)
            })

            # 更新用户资金
            capital_sql = text("UPDATE users SET capital = :cap WHERE username = :uid")
            conn.execute(capital_sql, {"cap": int(capital_after), "uid": user_id})

            # 更新用户统计
            update_user_stats(user_id, final_profit, profit_rate, 'abandoned', 0, 0)

        print(f"[SETTLE_ABANDONED] ✓ 结算完成: profit={final_profit}, capital_after={capital_after}")

        return {
            'penalty': penalty,
            'final_profit': final_profit,
            'capital_after': capital_after
        }
    except Exception as e:
        print(f"[SETTLE_ABANDONED] ❌ 结算离开游戏失败: {e}")
        traceback.print_exc()
        return None


# ==========================================
# 成就检测
# ==========================================
def _ensure_achievement_table(conn):
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS kline_game_achievements (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            user_id VARCHAR(128) NOT NULL,
            achievement_code VARCHAR(64) NOT NULL,
            achievement_name VARCHAR(128) NOT NULL,
            exp_reward INT DEFAULT 0,
            unlocked_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_user_achievement (user_id, achievement_code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """))


def check_achievements(user_id, game_profit, profit_rate, trade_count, max_drawdown, leverage=1):
    """检测并解锁成就（按当前版本规则）"""
    new_achievements = []

    try:
        try:
            with engine.begin() as conn:
                _ensure_achievement_table(conn)
        except Exception as e:
            print(f"[ACHIEVEMENT] 初始化成就表失败: {e}")

        with engine.connect() as conn:
            # 获取已解锁成就
            sql = text("SELECT achievement_code FROM kline_game_achievements WHERE user_id = :uid")
            unlocked = [r[0] for r in conn.execute(sql, {"uid": user_id}).fetchall()]

        def try_unlock(code):
            if code not in unlocked:
                unlock_achievement(user_id, code)
                new_achievements.append(ACHIEVEMENTS[code])

        with engine.connect() as conn:
            # 累计完成局数（只计 completed）
            completed_games = conn.execute(text("""
                SELECT COUNT(*)
                FROM kline_game_records
                WHERE user_id = :uid
                  AND status = 'finished'
                  AND end_reason = 'completed'
            """), {"uid": user_id}).fetchone()[0]

            # 双杠杆完成检查（1x + 10x），兼容数值/文本存储
            leverage_types = conn.execute(text("""
                SELECT COUNT(DISTINCT CAST(leverage AS SIGNED))
                FROM kline_game_records
                WHERE user_id = :uid
                  AND status = 'finished'
                  AND end_reason = 'completed'
                  AND CAST(leverage AS SIGNED) IN (1, 10)
            """), {"uid": user_id}).fetchone()[0]

            # 毛额累计（只计 completed）
            gp, gl = conn.execute(text("""
                SELECT
                  COALESCE(SUM(CASE WHEN profit > 0 THEN profit ELSE 0 END), 0) AS gp,
                  COALESCE(SUM(CASE WHEN profit < 0 THEN -profit ELSE 0 END), 0) AS gl
                FROM kline_game_records
                WHERE user_id = :uid
                  AND status = 'finished'
                  AND end_reason = 'completed'
            """), {"uid": user_id}).fetchone()
            gross_profit = float(gp or 0)
            gross_loss_abs = float(gl or 0)

            # 当前连胜（从最近 completed 往前连续盈利）
            rows = conn.execute(text("""
                SELECT profit
                FROM kline_game_records
                WHERE user_id = :uid
                  AND status = 'finished'
                  AND end_reason = 'completed'
                ORDER BY game_end_time DESC, id DESC
                LIMIT 300
            """), {"uid": user_id}).fetchall()
            current_streak = 0
            for r in rows:
                p = float(r[0] or 0)
                if p > 0:
                    current_streak += 1
                else:
                    break

            best_profit, worst_profit, best_rate, max_trades = conn.execute(text("""
                SELECT
                  COALESCE(MAX(profit), 0) AS best_profit,
                  COALESCE(MIN(profit), 0) AS worst_profit,
                  COALESCE(MAX(profit_rate), 0) AS best_rate,
                  COALESCE(MAX(trade_count), 0) AS max_trades
                FROM kline_game_records
                WHERE user_id = :uid
                  AND status = 'finished'
                  AND end_reason = 'completed'
            """), {"uid": user_id}).fetchone()

            has_one_trade_win = conn.execute(text("""
                SELECT COUNT(*)
                FROM kline_game_records
                WHERE user_id = :uid
                  AND status = 'finished'
                  AND end_reason = 'completed'
                  AND trade_count = 1
                  AND profit > 0
            """), {"uid": user_id}).fetchone()[0] > 0

            has_lev10_win = conn.execute(text("""
                SELECT COUNT(*)
                FROM kline_game_records
                WHERE user_id = :uid
                  AND status = 'finished'
                  AND end_reason = 'completed'
                  AND CAST(leverage AS SIGNED) >= 10
                  AND profit > 0
            """), {"uid": user_id}).fetchone()[0] > 0

            has_no_drawdown_win = conn.execute(text("""
                SELECT COUNT(*)
                FROM kline_game_records
                WHERE user_id = :uid
                  AND status = 'finished'
                  AND end_reason = 'completed'
                  AND profit > 0
                  AND COALESCE(max_drawdown, 0) <= 0.0001
            """), {"uid": user_id}).fetchone()[0] > 0

        print(f"[ACHIEVEMENT] user={user_id} completed={completed_games}, lev_types={leverage_types}, streak={current_streak}, gross_profit={gross_profit}, gross_loss={gross_loss_abs}")

        # 新手
        if leverage_types >= 2:
            try_unlock("dual_leverage")
        if completed_games >= 20:
            try_unlock("games_20")

        # 盈利
        if float(best_profit or 0) >= 100000:
            try_unlock("profit_100k")
        if float(best_profit or 0) >= 500000:
            try_unlock("profit_500k")
        if float(best_rate or 0) >= 0.5:
            try_unlock("rate_50")

        # 亏损
        if float(worst_profit or 0) <= -100000:
            try_unlock("loss_100k")
        if float(worst_profit or 0) <= -500000:
            try_unlock("loss_500k")

        # 稳健
        if has_no_drawdown_win:
            try_unlock("no_drawdown_win")
        if current_streak >= 5:
            try_unlock("streak_5")
        if current_streak >= 10:
            try_unlock("streak_10")

        # 操作
        if int(max_trades or 0) >= 20:
            try_unlock("trader_20")
        if has_one_trade_win:
            try_unlock("zen_1_trade_win")

        # 风控
        if has_lev10_win:
            try_unlock("lev10_win")

        # 里程碑（毛额）
        if gross_profit >= 100000:
            try_unlock("gross_profit_100k")
        if gross_profit >= 1000000:
            try_unlock("gross_profit_1m")
        if gross_profit >= 10000000:
            try_unlock("gross_profit_10m")
        if gross_loss_abs >= 500000:
            try_unlock("gross_loss_500k")

        return new_achievements

    except Exception as e:
        print(f"检测成就失败: {e}")
        return []


def unlock_achievement(user_id, achievement_code):
    """解锁成就"""
    try:
        achievement = ACHIEVEMENTS.get(achievement_code)
        if not achievement:
            return False

        with engine.begin() as conn:
            _ensure_achievement_table(conn)
            sql = text("""
                       INSERT
                       IGNORE INTO kline_game_achievements
                (user_id, achievement_code, achievement_name, exp_reward)
                VALUES (:uid, :code, :name, :exp)
                       """)
            conn.execute(sql, {
                "uid": user_id,
                "code": achievement_code,
                "name": achievement['name'],
                "exp": achievement['exp']
            })

        # 给予经验奖励
        add_user_experience(user_id, achievement['exp'])
        print(f"[ACHIEVEMENT] 解锁成功: user={user_id}, code={achievement_code}")
        return True
    except Exception as e:
        print(f"[ACHIEVEMENT] 解锁失败: user={user_id}, code={achievement_code}, err={e}")
        return False


def get_user_achievements(user_id):
    """获取用户已解锁的成就"""
    try:
        with engine.connect() as conn:
            sql = text("""
                       SELECT achievement_code, achievement_name, unlocked_at, exp_reward
                       FROM kline_game_achievements
                       WHERE user_id = :uid
                       ORDER BY unlocked_at DESC
                       """)
            results = conn.execute(sql, {"uid": user_id}).fetchall()
            return [dict(r._mapping) for r in results]
    except:
        return []


# ==========================================
# 排行榜
# ==========================================
def get_leaderboard(board_type='profit', limit=20):
    """获取排行榜"""
    try:
        with engine.connect() as conn:
            if board_type == 'profit':
                sql = text("""
                           SELECT user_id, total_profit, total_games, win_games
                           FROM kline_game_stats
                           WHERE total_games >= 5
                           ORDER BY total_profit DESC LIMIT :limit
                           """)
            elif board_type == 'winrate':
                sql = text("""
                           SELECT user_id,
                                  total_profit,
                                  total_games,
                                  win_games,
                                  ROUND(win_games * 100.0 / total_games, 1) as win_rate
                           FROM kline_game_stats
                           WHERE total_games >= 10
                           ORDER BY win_rate DESC LIMIT :limit
                           """)
            elif board_type == 'streak':
                sql = text("""
                           SELECT user_id, max_streak, total_games, total_profit
                           FROM kline_game_stats
                           WHERE total_games >= 5
                           ORDER BY max_streak DESC LIMIT :limit
                           """)
            else:
                return []

            results = conn.execute(sql, {"limit": limit}).fetchall()
            return [dict(r._mapping) for r in results]
    except:
        return []


def get_training_entry_leaderboards(limit=20, min_completed=2):
    """
    K线训练入口页排行榜（Top N）
    - 总资金榜：users.capital
    - 单局最大盈利榜：kline_game_records.profit 的每用户 MAX
    - 连胜榜：kline_game_stats.current_streak
    入榜门槛：完成局数 >= min_completed（只计 finished/completed）
    """
    try:
        with engine.connect() as conn:
            base_completed = """
                SELECT user_id, COUNT(*) AS completed_games
                FROM kline_game_records
                WHERE status = 'finished'
                  AND end_reason = 'completed'
                GROUP BY user_id
                HAVING COUNT(*) >= :min_completed
            """

            capital_sql = text(f"""
                SELECT u.username AS user_id, COALESCE(u.capital, 0) AS value
                FROM users u
                JOIN ({base_completed}) c ON c.user_id = u.username
                ORDER BY value DESC, user_id ASC
                LIMIT :limit
            """)
            max_profit_sql = text(f"""
                SELECT c.user_id AS user_id, COALESCE(mp.max_profit, 0) AS value
                FROM ({base_completed}) c
                LEFT JOIN (
                    SELECT user_id, MAX(COALESCE(profit, 0)) AS max_profit
                    FROM kline_game_records
                    WHERE status = 'finished'
                      AND end_reason = 'completed'
                    GROUP BY user_id
                ) mp ON mp.user_id = c.user_id
                ORDER BY value DESC, user_id ASC
                LIMIT :limit
            """)
            streak_sql = text(f"""
                SELECT c.user_id AS user_id, COALESCE(s.current_streak, 0) AS value
                FROM ({base_completed}) c
                LEFT JOIN kline_game_stats s ON s.user_id = c.user_id
                ORDER BY value DESC, user_id ASC
                LIMIT :limit
            """)

            params = {"min_completed": min_completed, "limit": limit}
            capital_rows = conn.execute(capital_sql, params).fetchall()
            max_profit_rows = conn.execute(max_profit_sql, params).fetchall()
            streak_rows = conn.execute(streak_sql, params).fetchall()

            def to_dicts(rows):
                return [dict(r._mapping) for r in rows]

            return {
                "capital": to_dicts(capital_rows),
                "max_profit": to_dicts(max_profit_rows),
                "streak": to_dicts(streak_rows),
            }
    except Exception as e:
        print(f"[LEADERBOARD] 获取入口排行榜失败: {e}")
        return {"capital": [], "max_profit": [], "streak": []}


# ==========================================
# 【新增】诊断函数 - 用于排查问题
# ==========================================
def diagnose_game_issue(user_id, symbol='BCL'):
    """
    【新增函数】诊断游戏问题
    在出现问题时调用此函数可以快速定位问题

    使用方法:
        from kline_game import diagnose_game_issue
        diagnose_game_issue('your_user_id', 'BCL')
    """
    print("=" * 60)
    print("K线游戏问题诊断")
    print("=" * 60)

    # 1. 检查数据库连接
    print("\n[1] 检查数据库连接...")
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("    ✓ 数据库连接正常")
    except Exception as e:
        print(f"    ✗ 数据库连接失败: {e}")
        return

    # 2. 检查表结构
    print("\n[2] 检查 kline_game_stats 表结构...")
    missing = ensure_database_columns()
    if missing:
        print(f"    ⚠ 发现缺失字段: {[m[0] for m in missing]}")
        print("    建议执行: fix_missing_columns(missing)")
    else:
        print("    ✓ 所有必需字段都存在")

    # 3. 检查K线数据
    print(f"\n[3] 检查K线数据...")
    try:
        with engine.connect() as conn:
            # 检查股票数据
            stock_sql = text("SELECT COUNT(DISTINCT ts_code), COUNT(*) FROM stock_price WHERE open_price IS NOT NULL")
            stock_result = conn.execute(stock_sql).fetchone()
            print(f"    股票: {stock_result[0]} 个品种, {stock_result[1]} 条数据")

            # 检查期货数据
            futures_sql = text(
                "SELECT COUNT(DISTINCT ts_code), COUNT(*) FROM futures_price WHERE open_price IS NOT NULL")
            futures_result = conn.execute(futures_sql).fetchone()
            print(f"    期货: {futures_result[0]} 个品种, {futures_result[1]} 条数据")
    except Exception as e:
        print(f"    ✗ 检查K线数据失败: {e}")

    # 4. 尝试获取随机K线
    print(f"\n[4] 尝试获取随机K线数据...")
    symbol, symbol_name, symbol_type, df = get_random_kline_data()
    if df is not None:
        print(f"    ✓ 成功获取: {symbol_name} ({symbol}), {len(df)} 条数据")
    else:
        print("    ✗ 获取K线数据失败")

    # 5. 检查用户会话
    print(f"\n[5] 检查用户 {user_id} 的会话...")
    try:
        with engine.connect() as conn:
            sessions_sql = text("""
                                SELECT id, status, symbol, capital_before, game_start_time
                                FROM kline_game_records
                                WHERE user_id = :uid
                                ORDER BY game_start_time DESC LIMIT 5
                                """)
            sessions = conn.execute(sessions_sql, {"uid": user_id}).fetchall()
            if sessions:
                for s in sessions:
                    print(f"    游戏 {s[0]}: {s[1]}, {s[2]}, 资金={s[3]}")
            else:
                print("    无历史会话")
    except Exception as e:
        print(f"    ✗ 查询会话失败: {e}")

    # 6. 检查用户统计
    print(f"\n[6] 检查用户 {user_id} 的统计...")
    stats = get_user_stats(user_id)
    if stats:
        print(f"    总局数: {stats.get('total_games', 0)}")
        print(f"    胜场: {stats.get('win_games', 0)}")
        print(f"    败场: {stats.get('loss_games', 'N/A')}")  # 可能不存在
        print(f"    总盈亏: {stats.get('total_profit', 0)}")
    else:
        print("    无统计记录")

    print("\n" + "=" * 60)
    print("诊断完成")
    print("=" * 60)


# ==========================================
# 【新增】程序启动时自动检查数据库
# ==========================================
def init_database_check():
    """
    【新增函数】初始化时检查数据库
    建议在程序启动时调用
    """
    print("[INIT] 检查数据库结构...")
    missing = ensure_database_columns()
    if missing:
        print(f"[INIT] 发现缺失字段，正在自动修复...")
        fix_missing_columns(missing)
    print("[INIT] 数据库检查完成")

# 如果需要在导入时自动检查，取消下面的注释
# init_database_check()
