# Strategy Live Loop Minimal (roadmap 2.2-2.4, pre-broker)

## Why

外部评审要求在接券商之前先有一个**最小实盘闭环**：涨跌停/板块最小下单量、真实费用
（含过户费）、计划与实际对账、以及一键熔断。仓库侦察结论是：仓位/行业上限、费用与
滑点、整手尺寸**已经实现并被测试钉住**（`strategy-portfolio-risk-limits`、
`strategy-fee-aware-opening-budget`），因此本 change **只补齐三个真实缺口**，不重复
造轮子：

1. **涨跌停门禁缺失**：`nav.py` 只认 `trade_status`，`strategy_runner._load_quotes_for_codes`
   不取涨跌停信息，因此计划中的 BUY 可能在**涨停开盘**成交、SELL 可能在**跌停开盘**成交；
   而研究侧 `factor_lab.panel` / `scripts/factor_lab_account_replay.tradeable_open`
   早已有正确的 board-aware 判定。两边口径不一致，paper 账本会高估可执行性。
2. **板块最小下单量与过户费缺失**：`_fit_buy_quantity` 对所有标的统一用 100 股整手，
   科创板（sh688/sh689）的 **200 股起、1 股递增** 规则从未生效；成本模型里**没有过户费**
   （A 股双向收取，现行 0.001%/边），因此开盘 sizing 与 round-trip 记账都低估成本。
3. **无对账、无熔断**：没有「计划 vs 实际账户」的结构化差异，也没有默认关闭、可持久化、
   记录 who/when/why 的订单生成熔断开关。人工执行（A 模式）下，操作员拿到的目标组合
   既无法核对，也无法在异常时一键停发。

本 change 不接券商、不自动下单、不写 REST 端点、不加调度、不 promote、不改评分/方向/
前瞻证据口径。

## What Changes

- **涨跌停门禁落在生产 paper 路径**：新增共享判定 `strategy_engine/tradability.py`，
  复用 `factor_lab.panel.price_limit` 的 board 分类（主板 ±10%、创业板/科创板 ±20%、
  BSE ±30%、ST 主板 ±5%，0.1pp 容差），按 `open vs previous_close` 判定；涨停拒绝 BUY、
  跌停拒绝 SELL；**涨跌停信息不可得时按未知处理并对该侧 fail closed**（fail loud，绝不
  fail open）。被拒订单进入报告（`blocked` + `blocked_count`、CLI 输出
  `blocked_orders`），不是静默丢弃；持仓估值仍按最后可观测收盘标记（门禁只作用于成交）。
- **板块最小下单量与过户费**：`board_lot_rule(code)` 给出买入最小量与递增（科创板
  200 股起、1 股递增；创业板/主板 100 股整手）；**SELL 不受买入最小量约束**，零股也能
  平掉。成本模型新增双向 `transfer_fee_rate`（默认 0.001%/边，双向），并把卖方印花税
  由 0.001 修正为现行 0.0005（2023-08-28 起减半），使 `round_trip_cost()` 从
  0.0035 修正为 0.00302；两者进入开盘 sizing 与 SELL 现金流/round-trip 记账。费率常量
  **只在 `factor_lab.metrics` 定义一次**，`strategy_engine.config.PAPER_EXECUTION` 引用
  同一处（`holding_scan.ROUND_TRIP_COST` 也改为从该函数派生，不再各写一份）；
  新增可选 `execution` 配置块（费率/最低佣金/整手/初始净值）经校验后进入
  `config_hash`，缺省不写入默认配置以保持既有默认哈希可复现。
- **对账（只读）**：新增 `strategy_engine/reconcile.py:reconcile_plan_vs_account`，
  输出每只标的数量漂移、现金漂移、缺失/多余持仓与 `breaks`；超过容差即非零退出、
  结果可 JSON 序列化；附带薄 CLI（无 REST、无调度）。
- **熔断（kill switch）**：新增 `strategy_engine/halt.py`，文件持久化、**默认 OFF**、
  路径必须由 `CAIFUBAO_STRATEGY_HALT_FILE` 或显式参数提供（**无静默默认**）；
  `run_strategy` **第一件事**检查熔断，命中即 fail-loud 返回 HALTED（不产生任何订单/
  计划，不推进 120 会话计数，`evidence_kind` 语义不变）。提供 `halt engage|resume|status`
  CLI 与 `--halt-file`，flag 文件记录 who/when/why 与变更历史。
- **OpenSpec/文档**：本 change 的 spec 覆盖上述四项行为；操作文档补充熔断 runbook 与
  新增 CLI 子命令说明。

## Non-goals

- 不接券商通道/不自动下单/不做订单幂等（roadmap 2.2 另开切片）。
- 不加 REST 端点、不加调度、不写库中的新集合；对账只读。
- 不改评分/方向/选择/再平衡语义，不关闭或新开前瞻窗口，不给熔断伪造前瞻证据。
- 不把 `datahub/scripts/factor_lab_paper_run.py` 引入生产模块
  （`strategy-paper-research-ledger` 禁止该耦合）。

## Scope boundaries, divergences and residual risks

- **门禁只作用于成交/NAV 时点**：`run`/`export` 产出的是**计划**（signal date 的
  目标组合），此时执行日的行情与涨跌停尚不可知，因此计划阶段**不做**涨跌停门禁；
  涨跌停判定发生在 `nav` 复盘的执行日开盘。被拒订单只出现在 `nav` 的
  `blocked`/`blocked_orders` 输出里，**不写入** `StrategyPaperRun`（本切片不新增字段）。
- **与研究侧的刻意差异**：研究脚本 `scripts/factor_lab_account_replay.tradeable_open`
  在「涨跌停不可判定」时**fail open**（继续按可交易处理），生产 paper 路径在本 change
  中**fail closed**（拒绝并报告）。二者共享 board 分类，但判定策略不同。选择记录差异而
  非统一，是因为 `datahub/scripts/*` 由 `strategy-paper-research-ledger` 冻结、且历史
  研究结果的可复现性优先；该差异与披露由集成方（account-replay cost-model disclosure）
  处理，不在本 change 内改动研究脚本。
- **ST 位缺失是已知残留缺口**：`StockDailyQuote.isST` 在 2026+ 数据中可能缺失，
  `is_st` 仅 best-effort。缺位时 ST 名会被按主板 ±10% 判定，理论上一个 +5% 的 ST 涨停
  BUY 仍可能成交。爆炸半径受默认 `constraints.exclude_st=true` 限制（ST 名本就不入
  目标组合）；只有在显式关闭 `exclude_st` 且 isST 缺失时才会暴露。修复需要 2026+ 的
  point-in-time ST 数据源，属独立切片。
- **熔断不是 stickiness 保证**：flag 文件被删除即回到「未熔断」（只有**路径未配置**
  才 fail closed）。正常运维应保留该文件；把删除当作恢复是刻意的（否则丢失文件会让
  系统永久停摆）。
