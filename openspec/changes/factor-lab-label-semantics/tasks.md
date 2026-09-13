# Factor Lab Label Semantics Tasks

## 1. 标签实现（已随切片 1 落地）

- [x] 1.1 `panel.build_panel` 会话偏移标签：T+1 开盘进场 → 第 h 个交易日开盘出场，
  h=1/5/20/60；并用市场交易日历校验偏移（缺行 → `missing_session_between`，不悄悄
  拉长持有期）
- [x] 1.2 `blocked_h{h}` 记录原因（`no_next_session`/`missing_price`/`suspended_entry`/
  `missing_previous_close`/`limit_up_entry`/`no_exit_yet`/`suspended_exit`/
  `limit_down_exit`/`missing_session_between`），不可交易即丢弃、不滚动
- [x] 1.3 `panel.price_limit` board-aware 阈值（ST 只收紧主板；创业板/科创 ST 仍 20%）+
  用 `previous_close` 与**开盘价**判定涨跌停（无法解析前收盘 → 不可交易）
- [x] 1.4 镜像空头腿 `fwd_short_h{h}`/`blocked_short_h{h}`（`limit_down_entry`/
  `limit_up_exit`）
- [x] 1.5 单测：周末跨 h=1、T+1/h 偏移、停牌与涨跌停阻塞、board-aware 板别、
  开盘涨停/收盘涨停的区分、缺前收盘、缺行、镜像空头腿、`isST` 端到端 5% 档
- [x] 1.6 `_load_panel` 保留 `blocked_*` 原因并同时裁剪两条腿的 horizon
  （回归测试：parquet 往返后原因不被数值化抹掉）
- [x] 1.7 前瞻防护测试：篡改**中间行**（全部特征列）+ 80 个交易日噪声面板，断言该行之前
  全部因子值不变，且 17 个因子都非全 NaN

## 2. 指标口径（同一 change 内的修正）

- [x] 2.1 多空价差每腿各付一次往返：`top_minus_bottom = 毛价差 − 2 × round_trip_cost()`
  （旧实现从两腿各扣同一笔成本 → 恰好抵消，把毛价差当净价差报）
- [x] 2.2 `profit_concentration` 改为「单笔最大正收益 / 正收益合计」（对齐
  `autoresearch/profile.yaml`）：旧实现用最大分位桶计数占比，恒 ≈1/quantiles，永不触发
- [x] 2.3 走查衰减改为带符号（profile 的 `_walk_forward_decay` 也是带符号，但它只用
  validation 且在 0 处截断；本实现取 validation+test 均值、不截断）：样本外变号必须失败，
  而不是 decay=0 通过
- [x] 2.4 分位桶改为在**两条腿可解析观测的并集**上形成，各腿均值各自跳过对方缺失的标签
  （多头买不到但空头能卖的观测仍进入空头腿）
- [x] 2.5 `turnover_report` 按 horizon 节奏（每 h 个 session 比较一次）；IC 增加
  Newey-West t 统计量（lag = h−1）并用已知答案单测固定；`ic_report` 去掉未使用的
  `net` 参数（秩相关不受统一成本影响）

## 3. 决策记录（本 change 的核心交付）

- [x] 3.1 在 `proposal.md` 记录 4 套标签/可交易性约定的差异与各自消费者
- [ ] 3.2 各消费者「不得共用标签」的说明补进对应代码注释（`autoresearch_h20_snapshot_runner`
  与 `backtest_service` 尚未注明与本 lab 的差异）

## 4. 验证

- [x] 4.1 `openspec validate --all --strict` 通过
- [x] 4.2 因子实验室对照验收：现有动量/趋势因子在本面板复现审计的负 IC（见
  `docs/autoresearch/runs/factor-lab/first-sweep-2024-2026.md`）
- [x] 4.3 qa-reviewer 的两轮 P1/P2 修正后重跑 `pytest datahub/app/test/`、`ruff`、
  `openspec validate`，并用修正后的口径重出 §3/§4 结论

## 5. 明确不做（非本 change 的任务）

- 不统一现有 4 套标签实现：需要单独的迁移 change，且会改变已归档研究结论的口径
  （proposal `Non-goals`）。
- 不定义组合层/成交层语义（整手、最低佣金、市场冲击、排队）：那是
  `strategy-paper-*` 与 `backtest` 的范围；本 lab 的价差是统计价差，不等于可部署策略。
