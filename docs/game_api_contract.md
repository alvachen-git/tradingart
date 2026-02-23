# K线卡牌游戏 API 契约 v0.2 (Card V2)

目标：给 Godot/Web 客户端提供统一后端接口，复用现有 `kline_card_storage` 规则与存档。

## 基础约定
- Base URL: `http://<host>:8787`
- 所有业务接口前缀：`/v1/card`
- 地图层接口前缀：`/v1/map`
- 鉴权方式：请求头携带
- `X-Username: <username>`
- `X-Token: <session_token>`
- 错误格式：HTTP 4xx + `{"detail": "<message>"}`

## 接口清单

### 1. 健康检查
- `GET /v1/card/health`
- 返回示例：
```json
{"ok": true, "service": "kline-card-api"}
```

### 2. 读取独立经验
- `POST /v1/card/meta/get`
- Body：空
- 返回关键字段：
- `level`
- `exp`
- `skill_points`
- `upgrades`

### 3. 升级独立天赋
- `POST /v1/card/meta/upgrade`
- 请求：
```json
{"upgrade_code":"confidence_core"}
```
- 返回：
```json
{"ok":true,"meta":{"level":2,"exp":620,"skill_points":1}}
```

### 4. 创建新局
- `POST /v1/card/run/create`
- 请求：
```json
{"seed":12345}
```
- 返回：
```json
{"ok":true,"run_id":1739950000123456}
```

### 5. 读取未完成局
- `POST /v1/card/run/resume`
- Body：空
- 返回：
```json
{"ok":true,"run":{...}}
```

### 6. 读取对局状态
- `POST /v1/card/run/state`
- 请求：
```json
{"run_id":1739950000123456}
```
- 返回：
```json
{"ok":true,"run":{...},"stage":{...}}
```

### 7. 开始关卡/选关
- `POST /v1/card/stage/start`
- 请求：
```json
{"run_id":1739950000123456,"stage_no":1,"symbol_choice":"000300.SH"}
```
- `symbol_choice` 可空。空时通常返回 `need_choice=true` 与 3 个候选。
- 返回关键字段：
- `need_choice`
- `candidates`
- `visible_bars`
- `hand`

### 8. 回合结算（支持 combo）
- `POST /v1/card/turn/play`
- `type=pass`：
```json
{"run_id":1739950000123456,"type":"pass"}
```
- `type=play`：
```json
{"run_id":1739950000123456,"type":"play","card_id":"trend_long_novice"}
```
- `type=combo`：
```json
{"run_id":1739950000123456,"type":"combo","cards":["trend_long_novice","breakout_long_novice"]}
```
- 返回关键字段：
- `turn_score`
- `confidence_before`
- `confidence`
- `confidence_delta`
- `stage_score`
- `stage_score_delta`
- `played_cards`
- `card_results`
- `rule_version`
- `mechanics.momentum_before/after`
- `mechanics.score_streak_before/after`
- `mechanics.confidence_events[]`
- `mechanics.tactic_chain[]`

错误语义：
- 当 `combo` 同时包含顺势多/空趋势牌时，返回 HTTP 400，`detail` 为“方向冲突：顺势做多与顺势做空不能同回合同时执行。”

### 9. 关卡结算
- `POST /v1/card/stage/finish`
- 请求：
```json
{"run_id":1739950000123456}
```

### 10. 关卡强化选择
- `POST /v1/card/stage/upgrade`
- 请求：
```json
{"run_id":1739950000123456,"upgrade_code":"risk_appetite"}
```
- Card V2 默认关闭强化系统；该接口会返回失败消息：`stage upgrade is disabled in v2`。

### 11. 对局结算（发放独立经验）
- `POST /v1/card/run/finish`
- 请求：
```json
{"run_id":1739950000123456}
```
- 返回关键字段：
- `status` (`failed`/`cleared`)
- `reward_exp`
- `meta`

## 客户端接入建议
- 每回合只调用一次 `/turn/play`，避免本地重复结算。
- 客户端动画层使用返回的 `*_delta` 字段驱动，不自行推导。
- 客户端可优先使用 `mechanics` 字段渲染动量、连得分与战术结算详情。
- 关卡/对局结束后，按顺序调用：
- `stage/finish`
- `run/finish`（仅整局结束时）

## 隔离约束（强制）
- 该 API 只写 `kline_card_*` 表。
- 不写 `users.experience`。
- 不写 `kline_game_records` / `kline_game_stats`。

---

# 地图层 API 契约 v0.1 (Map MVP)

目标：地图资源管理与卡牌战斗桥接，采用“地图主循环 + 战斗子流程”。

## 接口清单

### 1. 地图健康检查
- `GET /v1/map/health`
- 返回：
```json
{"ok": true, "service": "kline-map-api"}
```

### 2. 创建地图局
- `POST /v1/map/run/create`
- 请求：
```json
{"seed": 12345}
```
- 请求（扩展，向后兼容）：
```json
{
  "seed": 12345,
  "restart_existing_active": true,
  "new_game_setup": {
    "player_name": "阿晨",
    "traits": ["外向", "谦虚", "喜欢规则", "看重自由"],
    "style_answers": {
      "horizon_preference": "short",
      "risk_preference": "seek_profit",
      "priority_preference": "mindset"
    },
    "god_mode": false
  }
}
```
- 返回：
```json
{"ok": true, "map_run_id": 1770000000123456}
```
- 返回（有开局设定时可附带摘要）：
```json
{
  "ok": true,
  "map_run_id": 1770000000123456,
  "applied_setup_summary": {
    "player_name": "阿晨",
    "traits": ["外向", "谦虚", "喜欢规则", "看重自由"],
    "style_answers": {
      "horizon_preference": "short",
      "risk_preference": "seek_profit",
      "priority_preference": "mindset"
    },
    "god_mode": false,
    "initial_deck_size": 15
  }
}
```

### 3. 恢复地图局
- `POST /v1/map/run/resume`
- Body：空
- 返回：
```json
{"ok": true, "run": {...}}
```

### 4. 读取地图状态
- `POST /v1/map/run/state`
- 请求：
```json
{"map_run_id": 1770000000123456}
```

### 5. 地点移动
- `POST /v1/map/location/move`
- 请求：
```json
{"map_run_id": 1770000000123456, "to_location": "association"}
```
- 返回关键字段：
- `map_run`
- `resource_delta`
- `log_line`
- `locked`

### 6. 住宅休息并推进回合
- `POST /v1/map/turn/rest`
- 请求：
```json
{"map_run_id": 1770000000123456}
```

### 7. 住宅卡组读取/保存
- `POST /v1/map/home/deck/get`
- 请求：
```json
{"map_run_id": 1770000000123456}
```
- `POST /v1/map/home/deck/save`
- 请求：
```json
{"map_run_id": 1770000000123456, "deck_cards": ["trend_long_novice", "short_long_novice", "..."]}
```
- 规则：卡组长度必须 `10~15`。

### 8. 地图发起战斗
- `POST /v1/map/battle/start`
- 请求：
```json
{"map_run_id": 1770000000123456}
```
- 返回：
```json
{"ok": true, "battle_run_id": 1770000000999001, "map_run": {...}}
```

### 9. 战斗结果回写地图
- `POST /v1/map/battle/commit`
- 请求：
```json
{"map_run_id": 1770000000123456, "battle_run_id": 1770000000999001}
```
- 回写规则：
- 战斗胜利（`cleared`）：金钱 `+150`、名气 `+6`、经验 `+20`
- 战斗失败（`failed`）：金钱 `-120`、体力 `-20`、名气 `-4`

### 10. 手动结束地图局
- `POST /v1/map/run/finish`
- 请求：
```json
{"map_run_id": 1770000000123456}
```

## MapRun 关键字段
- `map_run_id`
- `status`（`playing`/`ended`）
- `player_name`
- `year_no`, `turn_index`, `month_no`, `month_half`, `date_label`
- `location`, `location_name`
- `stamina`, `money`, `management_aum`, `action_points`, `stress`, `confidence`, `fame`, `exp`
- `traits`, `style_answers`, `god_mode`
- `home_deck`, `deck_pending_apply`
- `linked_battle_run_id`, `battle_state`
- `ended_reason`, `result`
