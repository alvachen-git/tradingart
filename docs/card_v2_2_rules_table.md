# Card V2.2 Rules Table (Spec Freeze)

本文件是《卡牌设计第二版》+ 本轮澄清的可编码规格冻结版本，用于规则引擎与前端文案实现。

## 全局规则（冻结）

- 短线命中阈值按卡等级：新手=3 根同向，熟练/老手=2 根同向，大师=1 根同向（未来 5 根 K 线内统计，非必须连续）。
- 短线连续奖励：同方向短线牌连续 >=3 张，奖励该连续段内最大 `streak_conf_bonus`。
- 短线配对奖励：相邻且可重叠的短线多空配对，奖励取左侧牌 `pair_bonus`。
- 短线突破严重失误：
  - `short_long` 遇到空头突破（收盘价跌破前 15 根最低）触发。
  - `short_short` 遇到多头突破（收盘价突破前 15 根最高）触发。
  - 同回合存在方向匹配突破牌可免疫（不要求该突破牌命中）。
  - 触发时短线总计直接覆盖为 `-8`，短线基础分/配对分/连排信心全部失效。
- 突破判定口径：未来 5 根任一 `close` 相对最近 15 根历史 `high/low`，不用 wick 判定。
- 突破动量奖励：20->30，30->60。
- 套利：按“连续套利牌段”结算；连续同方向重复折叠去重计数（例：东西南=3，东东南=2，北北=0）；多段分别结算后求和。
- 套利失败：先看执行回合未来 5 根 K 线逐根 `abs(close/open-1)`，任一 >3% 则失败，任一 >5% 则严重失败（覆盖普通失败，失败惩罚 *2）。失败时按每张套利牌失败规则结算；否则按成功规则结算。
- 动态调整：下回合正常抽牌前，先将剩余手牌全部移入弃牌堆；同回合多张动态调整只生效一次。
- 自信下单：使用门槛 `confidence>=80`；结算到该牌时 `running_total>0` 则总分 *2，否则信心 -20。
- 快速止损：保护其后两张牌的“最终负分”（所有倍率后）。突破牌与买方期权（买看涨/买看跌）不可被保护，但会占位。
- 买方期权 Y/Z：四舍五入；暴击条件为连续 3 根同向，且第三根相对上一根收盘价涨跌幅 >3%。
- 卖方期权失败阈值：相对执行前（本回合最后一根可见 K 线）的收盘价比较。

## 卡牌字段说明

- `card_id`: 稳定 ID（代码使用）
- `type`: `short|trend|breakout|tactic|arbitrage|option`
- `direction`: `long|short|none`
- `effect`: 战术效果标记
- `hit_need_bars`: 短线命中阈值
- `arb_region`: 套利区域 `east|west|south|north`
- `option_style`: `buy|sell`
- `option_side`: `call|put`

## 旧卡修改（摘要）

### 短线牌（多/空对称，空熟练失败扣分按文档保留为 2）

| card_id | 命中阈值 | 成功 | 失败 | 配对 | 连排信心 |
|---|---:|---:|---:|---:|---:|
| short_long_novice | 3 | +1 | -2 | +1 | +5 |
| short_long_skilled | 2 | +2 | -1 | +3 | +5 |
| short_long_veteran | 2 | +3 | -1 | +4 | +5 |
| short_long_master | 1 | +4 | -1 | +6 | +10 |
| short_short_novice | 3 | +1 | -2 | +1 | +5 |
| short_short_skilled | 2 | +2 | -2 | +3 | +5 |
| short_short_veteran | 2 | +3 | -1 | +4 | +5 |
| short_short_master | 1 | +4 | -1 | +6 | +10 |

### 突破牌（判定改 close）

| card_id | 成功（无动量） | 成功（有动量） | 失败 |
|---|---:|---:|---:|
| breakout_long_novice | +20 | +30 | -20 |
| breakout_long_veteran | +30 | +60 | -20 |
| breakout_short_novice | +20 | +30 | -20 |
| breakout_short_veteran | +30 | +60 | -20 |

## 新卡牌（编码表）

### 战术牌

| card_id | 名称 | effect | 规则 |
|---|---|---|---|
| tactic_dynamic_adjust | 动态调整 | dynamic_adjust | 下回合抽牌前丢弃剩余手牌后，再进行正常抽牌与额外抽牌 |
| tactic_self_confidence | 自信下单 | self_confidence | `confidence>=80`；该牌结算时 `running_total>0` 则总分*2，否则信心-20 |
| tactic_fast_stop | 快速止损 | fast_stop | 后两张牌最终负分免除；突破/买方期权不可保护 |

### 套利牌

| card_id | 区域 | 档位 | 成功 | 失败 |
|---|---|---|---:|---:|
| arb_east_novice | 东 | 新手 | +2 | -2 |
| arb_east_veteran | 东 | 老手 | +3 | -1 |
| arb_west_novice | 西 | 新手 | +2 | -2 |
| arb_west_veteran | 西 | 老手 | +3 | -1 |
| arb_south_novice | 南 | 新手 | +2 | -2 |
| arb_south_veteran | 南 | 老手 | +3 | -1 |
| arb_north_novice | 北 | 新手 | +2 | -2 |
| arb_north_veteran | 北 | 老手 | +3 | -1 |

套利段倍率：去重链长 `L`，`L<2` 视为配对失败；`L=2` 倍率 `x1`；`L=3` 倍率 `x2`；`L>=4` 倍率 `x3`。

### 期权牌（买方/卖方）

#### 买方看涨（call, buy）

| card_id | 名称 | entry_cost | win_mult |
|---|---|---:|---:|
| option_buy_call_novice | 买看涨做多-新手 | 8 | 3 |
| option_buy_call_skilled | 买看涨做多-熟练 | 6 | 4 |
| option_buy_call_veteran | 买看涨做多-老手 | 4 | 4 |
| option_buy_call_master | 买看涨做多-大师 | 4 | 6 |

成功条件：未来 5 根任一 `high > first_future_open`，`Y = round((future_high/first_open - 1)*100)`，得分 `Y * win_mult`，并先扣 `entry_cost`。暴击再 *2。

#### 买方看跌（put, buy）

| card_id | 名称 | entry_cost | win_mult |
|---|---|---:|---:|
| option_buy_put_novice | 买看跌期权-新手 | 8 | 3 |
| option_buy_put_skilled | 买看跌期权-熟练 | 6 | 4 |
| option_buy_put_veteran | 买看跌期权-老手 | 4 | 4 |
| option_buy_put_master | 买看跌期权-大师 | 4 | 6 |

成功条件：未来 5 根任一 `low < first_future_open`，`Z = round((first_open/future_low - 1)*100)`，得分 `Z * win_mult`，并先扣 `entry_cost`。暴击再 *2。

#### 卖方期权（sell）

| card_id | 名称 | side | 成功 | 失败 | fail阈值 | severe阈值 | severe倍数 |
|---|---|---|---:|---:|---:|---:|---:|
| option_sell_call_novice | 卖看涨期权-新手 | call | +4 | -16 | 5% | 10% | x3 |
| option_sell_call_skilled | 卖看涨期权-熟练 | call | +3 | -12 | 5% | 10% | x3 |
| option_sell_call_veteran | 卖看涨期权-老手 | call | +3 | -8 | 4% | 10% | x2 |
| option_sell_call_master | 卖看涨期权-大师 | call | +2 | -4 | 3% | 10% | x2 |
| option_sell_put_novice | 卖看跌期权-新手 | put | +4 | -16 | 5% | 10% | x3 |
| option_sell_put_skilled | 卖看跌期权-熟练 | put | +3 | -12 | 5% | 10% | x3 |
| option_sell_put_veteran | 卖看跌期权-老手 | put | +3 | -8 | 5% | 10% | x3 |
| option_sell_put_master | 卖看跌期权-大师 | put | +2 | -4 | 5% | 10% | x3 |

## 冲突规则（新增）

- 保留现有：趋势多/空互斥；突破多/空互斥；突破与反向趋势互斥。
- 新增：`buy_call` 与 `sell_call` 互斥；`buy_put` 与 `sell_put` 互斥。
- 允许：`buy_call + buy_put`；`sell_call + sell_put`。
