# K线卡牌稳定性回归清单（UI/交互）

目标：每次改动 `godot_client` 后，快速验证核心流程没有回归。

## 1) 自动门禁（一键）

```bash
cd /Users/alvachen/aiproject/tradingart
./scripts/check_kline_card_regression.sh
```

说明：
- 会自动执行素材 import、Godot headless 编译门禁、Python 单测。
- 若设置 `U`、`T`，还会自动跑 API smoke：

```bash
cd /Users/alvachen/aiproject/tradingart
U=<username> T=<session_token> HOST=http://127.0.0.1:8787 ./scripts/check_kline_card_regression.sh
```

## 2) 手工场景（必测）

1. 窗口缩放稳定性  
- 固定同一局，执行“放大->缩小”连续 10 次。  
- 验证手牌尺寸不累计变小，扇形位置不漂移。

2. 手牌入队/撤回稳定性  
- 连续执行“入队->撤回->入队”20 次。  
- 验证手牌不会突然整体靠右或错位。

3. 等待区重排  
- 通过拖拽和上移/下移改变顺序。  
- 验证执行顺序与等待区显示一致。

4. 执行与结算  
- 至少执行 3 回合（含 1 次 PASS）。  
- 验证回合结算、抽牌、信心/分数变化显示正常。

5. 强制弃牌流程  
- 触发“需弃牌”状态。  
- 验证弃牌门禁生效（不能直接执行/PASS），弃牌后可继续。

6. 分辨率覆盖  
- 至少验证 1920x1080 和一个较小窗口尺寸。  
- 验证手牌区、等待区、执行按钮均可见且可操作。

## 3) 通过标准

- 自动门禁全部通过。
- 手工 6 项全部通过。
- 操作日志无新的 Script Parse/Runtime 错误。

