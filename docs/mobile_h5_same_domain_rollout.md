# 手机浏览器同域分流到 Mobile H5（首期方案）

## 1. 目标
- 手机浏览器访问同一网址时，按灰度切到 mobile H5。
- 桌面浏览器保持现有主站。
- 支持 `force_mobile=1` 与 `force_desktop=1` 强制参数。
- mobile H5 首期只覆盖高频核心页：登录、首页、情报、行情、我的。
- 未覆盖功能（如 K 线游戏、充值中心）在 H5 自动回退桌面站。

## 2. 本次代码改动

### 2.1 后端（`mobile_api.py`）
- 新增接口：`GET /api/auth/session/bootstrap`
  - 从同域 Cookie (`username`/`token`) 引导 H5 登录态。
  - 同域校验：校验 `Host` 与 `Origin/Referer` 一致性（若 `Origin/Referer` 存在）。
  - 限流：按 IP 每分钟默认 60 次（环境变量可调）。
  - 返回：
    - 成功：`{ logged_in: true, username, token, expire_at }`
    - 无 Cookie：`{ logged_in: false, reason: "missing_cookie" }`
    - token 无效：`{ logged_in: false, reason: "invalid_session" }`

### 2.2 前端（`mobile`）
- 新增 H5 启动引导：`mobile/src/utils/session_bootstrap.ts`
  - App 启动时调用 bootstrap 接口；
  - 成功后写入本地 token，再调用 `/api/auth/verify` 做闭环校验；
  - 支持 H5 运行时关键错误回退桌面站（动态模块加载失败等）。
- 新增首期范围兜底：`mobile/src/utils/desktop_fallback.ts`
  - H5 只放行首期页面；
  - 访问未覆盖页面自动回桌面站并携带原因参数。
- 页面鉴权优化：在核心页 `onShow` 前等待 bootstrap 完成，降低“先跳登录再恢复”的闪跳。
- H5 首期下线 K 线入口（底部导航只保留核心页）。

### 2.3 配置模板
- 新增 `ops/nginx_mobile_h5_same_domain.conf`：
  - UA + 灰度分流；
  - `force_mobile/force_desktop` 强制切流；
  - 同域 `/api/auth/session/bootstrap` 转发到 mobile_api；
  - mobile 静态资源缺失时回退桌面站。

## 3. 环境变量（mobile H5）

`mobile/.env.production`

```env
VITE_API_BASE=https://api.aiprota.com
VITE_ENABLE_AUTH_BOOTSTRAP=1
VITE_H5_BOOTSTRAP_PATH=/api/auth/session/bootstrap
VITE_DESKTOP_FALLBACK_URL=/?force_desktop=1
VITE_ENABLE_H5_RUNTIME_FALLBACK=1
```

`mobile/.env.development`

```env
VITE_API_BASE=http://localhost:8001
VITE_ENABLE_AUTH_BOOTSTRAP=0
VITE_H5_BOOTSTRAP_PATH=/api/auth/session/bootstrap
VITE_DESKTOP_FALLBACK_URL=/?force_desktop=1
VITE_ENABLE_H5_RUNTIME_FALLBACK=0
```

## 4. 发布步骤（建议）
1. 在 mobile 目录构建 H5：
   - `npm run type-check`
   - `npm run build:h5`
2. 将构建产物部署到 mobile H5 静态目录（与 Nginx 模板中的 `root` 对齐）。
3. 部署 `mobile_api.py`（包含 bootstrap 新接口）。
4. 应用同域 Nginx 分流配置，先设 5% 灰度。
5. 观察指标（登录成功率、首屏错误率、5xx）后提升到 30%、100%。

## 5. 验收清单
- 手机 UA 访问主域，命中 mobile H5。
- 桌面 UA 访问主域，命中原主站。
- 已登录主站用户访问 mobile H5，无需二次登录。
- Cookie 失效时不循环跳转，最终进入 mobile 登录页或桌面回退页。
- `force_mobile=1` 与 `force_desktop=1` 生效。
- 访问未覆盖页面时自动回桌面站（含 reason/feature 参数）。

