# Godot Client（竖切联调版）

路径：`/Users/alvachen/aiproject/tradingart/godot_client`

## 当前能力
- 输入 API 地址、用户名、Token 并连接
- 创建/恢复对局
- 准备关卡、选择候选标的
- 手动维护等待区顺序并执行 combo 回合
- PASS 回合
- 关卡结束后选择强化
- 整局结束后领取经验
- 内置 K线 + 成交量图（不显示日期轴）
- 回合执行后展示分数/信心/关卡分变化动效标签

## 启动步骤
1. 先启动后端 API：
```bash
cd /Users/alvachen/aiproject/tradingart
python3 run_game_api.py
```
2. 启动客户端（会先自动 import 素材，再打开 Godot）：
```bash
cd /Users/alvachen/aiproject/tradingart/godot_client
./run_client.sh
```
3. 在客户端输入：
- API Base URL（默认 `http://127.0.0.1:8787`）
- Username
- Token

## 仅导入素材（不启动窗口）
```bash
cd /Users/alvachen/aiproject/tradingart/godot_client
./run_client.sh --import-only
```

## 一键稳定性回归（推荐）
```bash
cd /Users/alvachen/aiproject/tradingart
./scripts/check_kline_card_regression.sh
```

可选：带账号做 API smoke
```bash
cd /Users/alvachen/aiproject/tradingart
U=<username> T=<session_token> HOST=http://127.0.0.1:8787 ./scripts/check_kline_card_regression.sh
```

## 注意
- 这是联调竖切版，重点是流程打通与交互验证，不是最终美术版本。
- 当前图表与动效为首版实现，后续会继续升级视觉表现与手感。
