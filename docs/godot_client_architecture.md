# Godot 客户端架构草案 v0.1

目标：实现丝滑卡牌操作、清晰结算动效，并兼容未来 Steam 发布。

## 技术选择
- 引擎：Godot 4.x
- 脚本：GDScript（首版）
- 网络：`HTTPRequest` + JSON
- 目标平台：Windows（首发）-> macOS/Linux（第二阶段）

## 场景树建议

### 1. 启动与全局
- `Main.tscn`
- `Root`
- `AudioBus`
- `SceneLoader`
- `GlobalState`（autoload）
- `ApiClient`（autoload）

### 2. 登录与大厅
- `LoginScene.tscn`
- `CanvasLayer`
- `LoginPanel`
- `UsernameInput`
- `TokenInput`
- `LoginButton`
- `StatusLabel`

- `LobbyScene.tscn`
- `CanvasLayer`
- `MetaPanel`
- `RunButtons`
- `NewRunButton`
- `ResumeRunButton`
- `SettingsButton`

### 3. 战斗主场景
- `BattleScene.tscn`
- `BattleRoot`
- `TopHUD`
- `StageLabel`
- `TurnLabel`
- `ConfidenceBar`
- `StageScoreLabel`
- `TotalScoreLabel`
- `EventBanner`
- `ChartPanel`
- `KlineViewport`
- `VolumeViewport`
- `HandPanel`
- `HandGrid`
- `QueuePanel`
- `QueueList`
- `QueueExecuteButton`
- `QueueClearButton`
- `PassButton`
- `TurnResultFxLayer`
- `ScoreDeltaFx`
- `ConfidenceDeltaFx`
- `StageDeltaFx`
- `ComboBreakdownList`

### 4. 结算与升级
- `StageUpgradeScene.tscn`
- `OptionCardA`
- `OptionCardB`
- `ConfirmButton`

- `RunResultScene.tscn`
- `ResultTitle`
- `RewardExpLabel`
- `MetaDeltaPanel`
- `BackToLobbyButton`

## 模块划分

### 1. 网络层
- `ApiClient.gd`
- 统一封装 header 注入：`X-Username`、`X-Token`
- 统一错误处理：超时、4xx/5xx、重试策略
- 提供 typed wrapper：
- `get_meta()`
- `create_run()`
- `resume_run()`
- `start_stage()`
- `play_turn()`
- `finish_stage()`
- `finish_run()`

### 2. 状态层
- `RunState.gd`
- 保存当前 run/stage/hand/queue/fx 数据
- 避免 UI 节点直接拼业务逻辑

### 3. 表现层
- `KlineChartRenderer.gd`
- `CardView.gd`
- `QueueView.gd`
- `TurnResultFx.gd`
- 所有动效只读状态层输出

## 交互流程（单回合）
- 玩家在 `HandGrid` 拖拽或点击加入 `QueueList`
- 玩家在 `QueueList` 调整顺序
- 点击 `QueueExecuteButton`
- 客户端发送一次 `turn/play(type=combo)`
- 收到响应后播放结算动画：
- 回合得分变化
- 信心变化
- 关卡分变化
- 连携分项明细
- 动画结束后刷新 HUD 与图表

## 动效规范（首版）
- 总时长：600ms~900ms
- 分三段：
- 0~200ms：入场闪光 + 文字弹入
- 200~600ms：数值滚动到目标值
- 600~900ms：淡出并固定到 HUD
- 若 `confidence_delta < 0`，附加屏幕轻微震动（8px 内）

## Steam 预留设计
- 账号映射：客户端本地仅存 token，不存敏感明文
- 成就系统：后端返回成就触发事件，客户端只展示
- 云存档：以 `run_id + user_id + schema_version` 做键
- 构建流程：导出模板 + CI 打包 + SteamPipe 上传

## 里程碑
- M1（竖切）：1关完整战斗 + 等待区 + combo结算动效
- M2（完整）：5关、升级、失败/通关、恢复存档
- M3（发布）：Steamworks 接入、打包、发布检查

## 与现有仓库的集成
- 保留 Python 规则与数据层（`kline_card_*`）
- 新增 API 层（`game_api`）
- Godot 作为独立客户端工程（`godot_client`）
- Streamlit 仅用于后台运营与调试

