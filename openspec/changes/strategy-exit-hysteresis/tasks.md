# Strategy Exit Hysteresis Tasks

> 依据：2026-10-27 holding-scan 研究结论（出口带是被验证的降摩擦杠杆）。
> 范围：spec 契约 + 选择/调仓 + 计划装配 + 两个入口 + 测试；不动 NAV、评分、
> FORWARD 判定与已认证窗口。

## 1. Spec + 失败测试

- [ ] 1.1 spec-guardian 审阅本 change 的 requirements/scenarios
- [ ] 1.2 失败测试先行：无策略时行为/形状不变；出口带保留；最短持有保护；
      失格强制卖出；原因标签；`hold_count` 递增；spec hash 与 legacy 投影不变；
      非法字段拒绝
- [ ] 1.3 确认新增字段**不注入** normalized spec 与 legacy config（哈希安全）

## 2. 实现

- [x] 2.1 `strategy_spec` 允许并校验 `rebalance.exit_buffer` /
      `rebalance.min_hold_sessions`；`to_legacy_config` 丢弃 spec-only 字段
- [x] 2.2 `selection.plan_target_holdings` 实现出口带 + 最短持有 + 原因；
      `select_target_holdings` 保持旧签名/旧形状
- [x] 2.3 `runner.assemble_daily_plan` 输出 `hold_meta` / `exit_reasons`，
      `rebalance` 形状不变
- [x] 2.4 `StrategyPaperRun` 新增 `hold_meta` / `exit_reasons`；
      CLI 与 `strategy_forward_daily` 从 spec 透传策略

## 3. 验证

- [x] 3.1 focused + full datahub pytest 全绿（1684）
- [x] 3.2 Ruff check/format（CI 规则集）
- [ ] 3.3 `openspec validate --all --strict`
- [ ] 3.4 qa-reviewer 审阅；branch-conflict vs develop；Draft PR CI green
