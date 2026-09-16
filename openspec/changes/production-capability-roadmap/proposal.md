# Production Capability Roadmap（研究/MVP → 实盘能力，合规口径下仍称 demo/MVP）

## Why

MVP 阶段把「数据 → 因子 → 评分 → 回测/验证/校准」研究链路做到接近完整（
capability-inventory 覆盖 88%、数据管道 100%），已有 strategy-paper-runner change
描述 paper-first 策略闭环。该 change 的旧语义按 VERIFIED 评分驱动策略，现由因果时序
修正统一改为读取可用状态；当前第一切片只产生 replay 记录。
flip_wide 构造层翻转候选在研究证据上为正（walk-forward IR、06 月校准对比），但**从未
promote，也没有 ≥120 交易日的冻结后前瞻证据**——任何「可交易/可执行」结论都必须先过
后续独立的不可变前瞻证据门槛。

用户方向（2026-09-07）：**以具备实盘能力为后续工作的规划目标**，同时基于合规角度，
**产品对外定位与公开表述一律保持「研究/学习/演示 MVP，非投资建议」**。本 change 是把
该方向落到 OpenSpec：定义分阶段的能力路线图，并把「合规命名」「实盘门槛」「执行闸门」
三条不可违背的不变式固化为 spec requirements。这里的路线顺序不表示实盘能力已经可用或
必然提供；**本 change 不授权任何真实资金执行**。

## Current evidence status

- **P0 causal replay integrity — IN PROGRESS**：先完成 `paper_causal_v1` 的可用分数
  消费、实际 UTC `decision_at`、下一个交易日 `execution_date`、完整 config hash 隔离、
  开盘信息定量和 `evidence_kind=REPLAY` 标记。该切片的 output 不得进入 120 日计数。
- **NEXT — immutable forward capture/count**：P0 完成后另行设计并实现不可变前瞻捕获和
  120-session counter；它不属于当前 replay correction。历史 `2026-06-10` 与示例日期
  `2026-09-04` 都不是已认证的前瞻起点。

## What Changes

分阶段（每阶段可独立 PR、独立验证，任务编号见 tasks.md）：

- **阶段 0 — 证据与定版门槛**：先完成 P0 causal replay integrity（IN PROGRESS），再
  由 NEXT 独立切片启动不可变前瞻捕获和 120 日计数。当前 replay、历史 backfill、
  replacement、NAV recompute 和 job SUCCESS 都不构成前瞻证据；promote 检查清单只能在
  前瞻窗口真实完成后使用。生产环境部署验证、capability-inventory 与 openspec 收尾仍
  需单独核实。
- **阶段 1 — 决策与治理地基**：DecisionJournal（推荐→执行→成交→盈亏 全链）、持久化
  审计 + 限流、组合/风控规则引擎落到 paper 路径（单票/行业/总仓/ST/停牌/流动性约束，
  换手与再平衡预算）、每日「目标持仓 + 调仓清单」作为**人工可核对**的产出。
- **阶段 2 — 执行能力（默认关闭、人工确认优先）**：目标组合导出（csv/报告）+ 人工复核
  下单（最小实盘路径）；券商 API 适配器以 default-off 标志接入；对账（成交回报 vs 目标、
  资金/持仓对账）；kill-switch；告警体系（数据缺口/漂移/回撤/流水线失败）。
- **阶段 3 — 工程加固**：评分/验证/NAV 确定性可重放与审计、数据 SLA 与降级、行情正确性
  校验、交易日历权威源对齐。

合规不变式（requirements）：① 对外一律称 demo/MVP 并保留免责声明；② 任何真实执行
必须显式授权 + 默认关闭 + paper/audit-first + 可一键停止；③ 模型升格到「可交易/可执行」
状态必须有后续独立切片产生的 ≥120 个不可变前瞻纸面交易日，并对照研究 walk-forward
预期；replay、backfill、replacement、NAV recompute 和 job SUCCESS 均不能满足门槛，
否则只能产出研究/观察级输出。

## Non-goals

- 本 change 不含真实资金、券商直连的落地代码（只规划闸门与默认关闭的适配点）。
- 不改变评分语义（选高买入、方向只在构造层）、不回改已合入的评分/验证/策略行为。
- 不引入实时/日内行情、融资融券、财报基本面等新数据源（列为阶段 3+ 的 backlog，另行
  change）。
