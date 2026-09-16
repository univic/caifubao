# flip_wide 单股可执行化 + 收益率曲线（示例：sh600519 贵州茅台）

> Research-only。把多 regime 审计/候选验证（#177）的 **flip_wide**（8 分量构造层全翻转、
> 宽书横截面）转成一个**可回测/可执行**的单股择时策略，并输出收益率曲线。
> 曲线 PNG：`flip_wide_curves/sh600519_flip_wide.png`；逐日数据：
> `flip_wide_curves/sh600519_flip_wide.json`（含 trades、daily equity、benchmark daily）。

## flip_wide → 单股择时的语义映射

flip_wide 原语义是横截面：每天把 8 个技术分量按方向 −1 加权（signal_strength/momentum/
trend/breakout/industry/relative/real_relative/risk 全部取反），在全市场里**分数高 = 处于
低动量/低位/超跌**。横截面研究里它买翻转后 top 分位的宽书（800 只）。

单股可执行化：对单只股票，flip_wide 的翻转分位（flip_pct）变成**反转择时信号**：

| 条件（信号日 T 收盘） | 动作（T+1 开盘执行） | 直觉 |
|---|---|---|
| flip_pct ≥ entry（0.90） | 买入 | 该股处于全市场最低动量/超跌分位 → 均值回归买入 |
| flip_pct ≤ exit（0.30） | 卖出 | 已反弹回中性以上 → 兑现离场 |

评分语义不变（「选高买入」）：我们买的是翻转构造下的"高分"（= 原体系低动量股），这正是
#177 结论里 flip_wide 构造层翻转的 usage——不违背既定决策（分数语义不是"买低分"，是构造
把"高"定义成了低动量）。

## 执行口径（对齐生产回测内核）

- **T+1 无前视**：T 日收盘用当日 flip_pct 决策，T+1 开盘价 `open_hfq` 执行（已验证：买入
  记录中 flip_pct 为信号日 T-1 的值）。
- **摩擦**：佣金 0.025%（最低 5 元）、卖出印花税 0.1%、双边滑点 0.1%（exec_price 已含）。
- **停牌**：trade_status != 1 跳过下单，持仓顺延。
- **基准**：同窗口 buy & hold（首日收盘全额买入、持有至末日）。
- 初始资金 100 万、单票满仓（max_position_pct=1.0）。

## 示例结果（sh600519 贵州茅台，2024-01-01 ~ 2026-07-31，entry 0.90 / exit 0.30）

| 指标 | 值 |
|---|---|
| flip_wide 择时收益率 | **+15.33%** |
| buy & hold 收益率 | −11.78% |
| 超额 | **+27.11%** |
| 交易笔数 | 18（9 买 9 卖） |

解读：2024-2026 茅台整体下行（buy&hold −11.8%），flip_wide 反转择时在每次深跌分位
（flip_pct ≥ 0.90，如 2024-06/2024-09/2026-05 的 0.99/0.91/0.97）买入、反弹后（flip_pct ≤
0.30）卖出，把 −11.8% 的 buy&hold 转成 +15.3%，超额 +27.1%。这是单只示例，用于展示
flip_wide 作为**可执行策略**的形态与可视化，**不是**对茅台的个股推荐，也不是多股组合的
证据（组合级验证见 #177 flip_wide：全窗口 IR +0.71、decay 0.00）。

其他候选票扫描（同窗口，entry 0.90 / exit 0.30）：

| 股票 | 择时 | buy&hold | 超额 | 交易 |
|---|---|---|---|---|
| sh600519 茅台 | +15.33% | −11.78% | +27.11% | 18 |
| sz002726 | −39.26% | −73.01% | +33.74% | 14 |
| sh600161 | −36.02% | −49.14% | +13.12% | 12 |
| sz300294 | −38.01% | −51.44% | +13.43% | 8 |
| sh600377 | +17.97% | +30.00% | −12.04% | 12 |
| sz000651 格力 | +7.93% | +52.86% | −44.92% | 14 |

注意：反转择时在**趋势向上**的票（格力、宁沪高速）会跑输 buy&hold——它本质是均值回归
策略，在单边强趋势里天然吃亏；在下跌/震荡票上体现价值。这也说明单股反转择时**不能**替代
flip_wide 组合级用法，两者互补。

## 复现

```bash
PYTHONPATH=datahub datahub/.venv/bin/python scripts/h20_flipwide_single_stock.py \
  --stock sh600519 \
  --snapshot /tmp/h20-2019-2026-merged.parquet \
  --from 2024-01-01 --to 2026-07-31 \
  --entry-pct 0.90 --exit-pct 0.30 \
  --out-dir datahub/research/autoresearch/h20_excess_alpha/flip_wide_curves
```

## 与「策略层实现」的关系

这是架构文档（architecture-layers-strategy-design.md §4）「评分 → 组合/择时」可执行化的
**第一个最小实例**：flip_wide 从研究候选 yaml 变成可对任意单股运行并画曲线的回测脚本，
输出与生产 BacktestResult 同构（daily_values/trades/excess）。后续把它接入正式策略 runner
时，只需把 flip_pct 的实时计算接到生产评分（评分构造层版本化后），执行内核可复用。
