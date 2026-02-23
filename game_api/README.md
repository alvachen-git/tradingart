# game_api

K线卡牌游戏后端 API（供 Godot/Web 客户端调用）。

## 启动

```bash
cd /Users/alvachen/aiproject/tradingart
python3 run_game_api.py
```

默认监听：`0.0.0.0:8787`

## 健康检查

```bash
curl http://127.0.0.1:8787/v1/card/health
```

## 一键烟测（需要账号 token）

```bash
cd /Users/alvachen/aiproject/tradingart
U=<username> T=<token> ./scripts/smoke_game_api.sh
```

## 鉴权

业务接口要求请求头：

- `X-Username`
- `X-Token`

详细契约见：

- `/Users/alvachen/aiproject/tradingart/docs/game_api_contract.md`
