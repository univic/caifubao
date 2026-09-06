# H20 人工实验总账（Manual Experiments Ledger）

> Research-only。官方 runner（autoresearch_runner，profile 口径 train 2024/validation
> 2025/test 2026H1）的记录见 `autoresearch/ledger.jsonl`。本文记录**官方 runner 之外**的
> 人工实验（#174-#186 期间脚本评估），每条标注评估协议与数据窗口——不同协议的结果必须
> 并存记录，避免把「官方单年窗口否决」误读为「扩展 walk-forward 也否决」或反之。
> 状态同步见 `autoresearch/state.yaml`。

## 协议说明

| 协议 | 数据 | 执行 | 结果载体 |
|---|---|---|---|
| 官方 runner | 2024-2026 快照，train 2024 / val 2025 / test 2026H1 | 评估器口径（净摩擦、decay gate） | ledger.jsonl |
| 扩展 walk-forward | 合并快照 2019-2026，train 2019-2023 / val 2024/2025 / test 2026H1 | 评估器口径（净摩擦、decay gate） | 本文 + 各分文档 |

## 人工实验记录

| 日期 | 实验 | 协议/窗口 | 结果 | 决策 | 文档 |
|---|---|---|---|---|---|
| 2026-09-04 | 多 regime 组件审计：8 分量 dailyIC | 2019-2026 八段 regime | 全 regime 显著负 IC（含牛市）| 恒反向、非 regime 依赖 | component-audit-2019-2026.md |
| 2026-09-04 | momentum 三合一冗余检验 | 同上 | IC/top/bot 完全相同（40% 权重同押注）| 剥离冗余 | 同上 |
| 2026-09-04 | 基本面因子（EP/BP/SP/DV/换手/市值）审计 | 2019-2026 per-regime + size 中性 | 估值正 IC 且 size 中性后不消失；turnover 恒负 | 估值作独立/regime 分量，不混入 flip_wide | fundamental-factor-audit-2019-2026.md |
| 2026-09-04 | flip_wide（构造层全翻转+宽书） | **扩展 walk-forward** 2019-2023→2024/2025→2026H1 | train +0.485 / val24 +0.975 / val25 +0.385 / test26H1 +0.426，decay 0.00 | 唯一全窗口正候选（研究证据）| flip-walkforward-2019-2026.md |
| 2026-09-04 | flip_wide 官方口径复核 | 官方 runner train 2024/val 2025 | **validation IR +0.385（正）**，但 train 2024 +0.975 更强 → decay 0.605 > 0.2 → discard | 官方窗口 IR 为正、仅因 decay 门被拦（非 IR 为负）| ledger.jsonl（已补录） |
| 2026-09-04 | 估值融合（w=0.2/0.3/0.5）| 扩展 walk-forward | 任何 w 破坏 2025 稳健性（+0.385→−0.742 @w=0.5）| 估值不加 | value-flip-blend-2019-2026.md |
| 2026-09-04 | 单股可执行化示例（sh600519）| 2024-2026，entry 0.90/exit 0.30 | +15.33% vs buy&hold −11.78% | 展示形态，非推荐 | flip_wide-single-stock-execution.md |
| 2026-09-06 | flip_wide 生产链 replay（task 3.3 实跑） | dev 全市场 ranked backfill 06-03~06-09（5 交易日），flip_wide_shadow_v1（注册 config_hash 8c8f3ee4）| 27,770 条（25,671 VERIFIED / 1,816 BLOCKED / 283 不足）；每日 ~5,134 VERIFIED，score 范围 -75~0（翻转语义正确），percentile 中位 ~0.50，推荐分布 BUY~258/WATCH~777/AVOID~1,023；compare vs score_v2_202605b（06-03）：basis=percentile，verdict=「Candidate clearly wins on both hit rate and return」；同 50 只子集 flip=baseline 收益（翻转改选择不改个股收益，数学正确）| 生产链 flip_wide 全市场 replay + T+1 verify + 校准对比全链路可用；选择有效性需 ≥120 天窗口判定（task 4.4）| task-3.3-flip-wide-shadow-validation.md |
| 2026-09-05 | 2025 异常年归因 | 2019-2026 + size 分桶 | 极端小盘轮动（H1 最强小盘溢价），非信号失效/伪影 | style-aware 评估 | attrib-2025-anomaly.md |

## 关键口径提示（避免误读）

1. **flip_wide 官方窗口 IR 为正、仅因 decay 门被拦**：官方口径（train 2024→val 2025）
   validation IR +0.385（正），因 train 2024 更强（+0.975）→ decay 0.605 > 0.2 被
   discard——不是 IR 为负。扩展 walk-forward（train 2019-23 含完整牛熊）全窗口正、
   decay 0.00。#183/#184 证明 2025 是 style 轮动年，单年窗口对该类候选天然苛刻——
   生产化评估应以扩展协议 + style-aware 为准（见 state.yaml）。
2. **baseline current_h20 官方口径亦被否决**（decay 6.14）——非 flip_wide 独有，是所有
   候选在 2024→2025 单年窗口的共同困境；扩展协议下 current_h20 同样 decay 2.11 失败，
   flip_wide 是唯一通过的。
3. 本 ledger 只记录**已写入文档/代码的事实**；评分方向版本化（#183）、模型注册表（#185）、
   决策版本约束（#186）是 flip_wide 生产化的研究侧前置，均未 promote。
