# Immutable Forward Evidence Capture + 120-session Counter (roadmap NEXT.1)

## Why

#202（`paper_causal_v1`）建立了**因果**边界（decision_at/execution_date/REPLAY），但
所有产出仍 `evidence_kind=REPLAY`——即使当天运行最新评分日期也只算回溯。现状没有：

- **前瞻认证规则**：一条记录何时才算“前瞻证据”（决策先于执行、可复核、不可改写）；
- **不可变性**：replace/backfill/NAV recompute 可覆盖任何记录，无法防止“事后伪造前瞻”；
- **认证窗口与计数器**：120 日窗口从哪天算起、哪些记录计数、换配置后旧证据如何退役
  （roadmap 0.1/0.3 明确：06-10/09-04 与全部 REPLAY 不计数，promote 需 NEXT 的不可变
  前瞻 120 日窗口结束）。

本 change（roadmap **NEXT.1**）设计并落地该机制；它决定 120 日墙钟从哪天起算，是
promote 门槛（0.3）与“实战”证据链的入口。合规口径不变：对外 demo/MVP、非投资建议；
本 change 不授权真实下单、不授权 promote。

## What Changes

- **前瞻证据边界（evidence_kind=FORWARD）**：一条 run 记录只有在
  `decision_at < open(execution_date)`（决策先于其执行日开盘）、信号日期处于该
  config_hash 的**认证窗口**内、且不覆盖任何既有记录时才可标 FORWARD；其余一律
  REPLAY（缺省/历史）。
- **不可变性**：FORWARD 记录的目标组合/状态/配置一旦写入，replace/backfill/重跑
  MUST 拒绝改写（fail-closed）；NAV 属**派生数据**（可重算），永不改写计划证据。
- **认证窗口（append-only）**：按 (model_version, horizon, config_hash) 一个窗口，
  显式 operator 认证开启（记录 start_date/decision_at/配置指纹），不可回填；
  评分源或策略配置变化 → 旧窗口关闭、新窗口另开（证据不可跨配置）。
- **120-session counter**：只统计窗口内 distinct 的 FORWARD COMPLETED 信号日期；
  job SUCCESS、replay、replace、NAV recompute 一律不计；进度命令报告
  count/120、首末日期、缺口与连续段（供 0.3 promote 对照）。

## Non-goals

- 不引入每日调度/CronJob 接线（operator 手动日更先行，自动化属后续切片）。
- 不改评分/方向/选择语义、不做真实下单、不 promote、不改历史 REPLAY 记录。
- 组合其余约束（1.3）、真实执行（2.x）、数据 SLA（3.x）仍为独立切片。
