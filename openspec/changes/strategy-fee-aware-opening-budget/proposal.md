# Strategy NAV Fee-Aware Opening Buy Budget (roadmap 1.3a)

## Why

#202（causal paper timing，`87a0ca2`）之后 paper NAV 的开盘买入在整手缩量上仍有一处
预算语义缺陷（`nav.py` 买入分支）：

- 现逻辑先按 `budget / raw_open` 取整手数量，**未在数量里预留滑点与佣金**；
- 成交按含滑点执行价计值后，`value + commission > cash` 时**整笔订单被跳过**
  （`if spend > cash: continue`），即使**减少一手即可满足预算**；
- 同理目标预算（`total_before × weight`）只在取整时使用，未作为费用后的硬上限。

后果：本可缩量成交的订单整笔消失（低估已部署资金、高估闲置现金），与
`strategy-paper-causal-timing` 建立的开盘因果口径不一致。roadmap 1.3a 由此而来。

本 change 只修 `datahub/app/lib/strategy_engine/nav.py` 的 BUY sizing 语义与对应
测试/文档；不动评分、方向、券商执行、前瞻证据口径（#202 基线 `87a0ca2` 之上）。

## What Changes

- **费用预留的整手买入预算**：给定目标权重、开盘可用组合估值与现金，选择最大的
  整手数量 `qty = k × board_lot (k≥1)`，使
  `成交额（含滑点执行价） + commission ≤ min(现金, 目标预算)`；
  仅当**一手都放不下**（含费用）才跳过该订单——不再因初始整手未预留费用而整笔跳过。
- commission 按 `max(value × rate, minimum_commission)` 计入 spend（最低佣金边界
  与比例佣金边界同样满足预算约束）；缩量循环按整手步进（或等价二分），复杂度有界。
- 依序买入时每笔用**当前剩余现金**与固定目标预算取 min，保证多标的不透支。
- 零费用（slippage/commission=0）时行为与 #202 完全一致（回归：#202 的「未来收盘
  不改变当日开盘成交」测试保持通过）。

## Non-goals

- 不引入执行价预测/优化、不改变 SELL 路径、不改变选择/再平衡、不改变收盘估值口径。
- 不做真实下单、不授权 promote、不改评分/方向/前瞻证据计数。
