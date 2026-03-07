"""
mobile_api.py — 爱波塔手机端专用 FastAPI 后端

启动命令:
    uvicorn mobile_api:app --host 0.0.0.0 --port 8001 --workers 2

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
  POST   /api/intel/subscribe           订阅免费频道

  GET    /api/market/snapshot           综合行情快照

  POST   /api/portfolio/upload          上传持仓截图 → 识别 → 提交分析
  GET    /api/portfolio/status/{id}     轮询持仓分析进度
  GET    /api/portfolio/result          获取最新持仓体检结果

  GET    /api/user/profile              获取用户资料与订阅状态
"""

import hashlib
import io
import os
import sys
from typing import Optional, List

from fastapi import FastAPI, HTTPException, Depends, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel

# 确保同目录模块可以 import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import auth_utils as auth
from task_manager import TaskManager
import subscription_service as sub_svc
import data_engine as de
from vision_tools import analyze_portfolio_image


# ════════════════════════════════════════════════════════════
#  App & CORS
# ════════════════════════════════════════════════════════════

app = FastAPI(
    title="爱波塔 Mobile API",
    version="1.0.0",
    description="uni-app 手机端专用后端接口",
)

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


class ChatSubmitRequest(BaseModel):
    prompt: str
    history: List[dict] = []    # [{role: "user"/"assistant", content: "..."}]


class SubscribeRequest(BaseModel):
    channel_code: str


# ════════════════════════════════════════════════════════════
#  Health Check
# ════════════════════════════════════════════════════════════

@app.get("/api/health", tags=["系统"])
def health():
    return {"status": "ok", "service": "爱波塔 Mobile API v1.0"}


# ════════════════════════════════════════════════════════════
#  AUTH
# ════════════════════════════════════════════════════════════

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

    task_id = TaskManager.create_task(
        user_id=username,
        prompt=body.prompt,
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

# 可以自助订阅的免费频道名称
_FREE_CHANNEL_NAMES = {"复盘晚报", "末日期权晚报", "期货商持仓分析晚报"}


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

    # 多拉一些再切片，避免多次 DB 查询
    raw = sub_svc.get_channel_contents(
        channel_code=channel_code,
        limit=offset + page_size,
    ) or []
    paged = raw[offset: offset + page_size]

    items = []
    for c in paged:
        content_text = c.get("content") or ""
        items.append({
            "id": c.get("id"),
            "title": c.get("title", ""),
            "channel_name": c.get("channel_name", ""),
            "channel_code": c.get("channel_code", ""),
            "summary": content_text[:150] + ("…" if len(content_text) > 150 else ""),
            "published_at": str(c.get("published_at", "")),
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
    return content


@app.post("/api/intel/subscribe", tags=["情报站"])
def intel_subscribe(
    body: SubscribeRequest,
    username: str = Depends(get_current_user),
):
    """订阅免费情报频道（复盘晚报 / 末日期权晚报 / 期货商持仓分析晚报）。"""
    channel = sub_svc.get_channel_by_code(body.channel_code)
    if not channel:
        raise HTTPException(status_code=404, detail="频道不存在")

    if channel.get("name") not in _FREE_CHANNEL_NAMES:
        raise HTTPException(status_code=403, detail="该频道需要人工开通，请联系客服")

    result = sub_svc.add_subscription(username, channel["id"], days=3650)
    # add_subscription 返回 (success, message) 或单个 bool
    if isinstance(result, tuple):
        success, msg = result[0], result[1] if len(result) > 1 else "操作完成"
    else:
        success, msg = bool(result), "订阅成功" if result else "订阅失败"

    if not success:
        raise HTTPException(status_code=400, detail=msg)
    return {"message": msg}


# ════════════════════════════════════════════════════════════
#  MARKET — 综合行情快照
# ════════════════════════════════════════════════════════════

@app.get("/api/market/snapshot", tags=["行情"])
def market_snapshot(username: str = Depends(get_current_user)):
    """
    获取综合行情快照，包含期货/ETF期权主要指标。
    数据较重，建议客户端缓存 60 秒。
    """
    try:
        data = de.get_comprehensive_market_data()
        if isinstance(data, dict) and "error" in data:
            raise HTTPException(status_code=500, detail=data["error"])
        return {"data": data}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"行情获取失败: {e}")


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
                "channel_name": s.get("channel_name"),
                "channel_code": s.get("channel_code"),
                "expires_at": str(s.get("expires_at", "")),
                "is_active": s.get("is_active", False),
            }
            for s in subscriptions
        ],
    }
