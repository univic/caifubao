# Factor Lab Label Semantics Tasks

## 1. 实现（已随切片 1 落地）

- [x] 1.1 `panel.build_panel` 位置化标签：T+1 开盘进场 → 第 h 个交易日开盘出场，h=1/5/20/60
- [x] 1.2 `blocked_h{h}` 记录原因（`missing_price`/`suspended_entry`/`limit_up_entry`/
  `suspended_exit`/`limit_down_exit`/`no_exit_yet`），不可交易即丢弃、不滚动
- [x] 1.3 `panel.price_limit` board-aware 阈值 + 缺失 `change_rate` 视为不可交易
- [x] 1.4 单测：周末跨 h=1、T+1/h 偏移、停牌与涨跌停阻塞、board-aware 板别、缺失 change_rate
- [x] 1.5 `_load_panel` 保留 `blocked_h*` 原因（回归测试：parquet 往返后原因不被数值化抹掉）

## 2. 决策记录（本 change 的核心交付）

- [x] 2.1 在 `proposal.md` 记录 4 套标签/可交易性约定的差异与各自消费者
- [ ] 2.2 各消费者「不得共用标签」的说明补进对应代码注释（`autoresearch_h20_snapshot_runner`
  与 `backtest_service` 尚未注明与本 lab 的差异）
- [ ] 2.3 若未来要统一 4 套实现，另开迁移 change 并重跑受影响的研究结论

## 3. 验证

- [x] 3.1 `openspec validate --all --strict` 通过
- [x] 3.2 因子实验室对照验收：现有动量/趋势因子在本面板复现审计的负 IC（见
  `docs/autoresearch/runs/factor-lab/first-sweep-2024-2026.md`）
