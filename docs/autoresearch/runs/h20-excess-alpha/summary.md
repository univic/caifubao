# H20 Excess Alpha Runs

> ⚠️ **本文为早期记录（2026-08，验证窗口 = 2025 单年、评估器默认 top5%/30只口径）。**
> 完整现状见 [`research-progress-2026-09-04.md`](./research-progress-2026-09-04.md)。
> 关键更正：本页「full_reversal 无效」结论受 **30 只小样本噪声 + 单年（2025）验证窗口**
> 限制——在多 regime 合并快照（2019-2026）+ 宽书（top-800）口径下，构造层全翻转候选
> `flip_wide` 全窗口 IR 为正（train +0.485 / val2024 +0.975 / val2025 +0.385 / test2026H1
> +0.426，decay 0.00），见 `flip-walkforward-2019-2026.md`。下表保留为当时运行记录。

Research-only evidence for the Caifubao learning and demonstration MVP. This is not investment advice or a production model promotion record.

| Configuration | Status | Score | IR | Net excess return | Max excess drawdown | Decision |
|---|---:|---:|---:|---:|---:|---|
| current_h20 (baseline) | completed | -999.0 | -0.859 | -0.131 | -0.949 | discard |
| full_reversal | completed | -999.0 | -0.918 | -0.114 | -0.956 | discard |
| exclude_d8_d9 | completed | -2.344 | -0.743 | -0.068 | -0.831 | keep |

> Validation split adjusted 2026-09: 2025H1-only (117 days, gated insufficient_period) →
> full 2025 (243 eligible days). Baseline now clears the sample gate but is discarded by
> `performance_decay` (train IR ≈ +0.17 → validation IR −0.86; walk_forward_decay 6.14 > 0.20).
>
> Controls (validation = full 2025, equal-weight benchmark):
> - `full_reversal` (all directions flipped) does NOT rescue the signal — validation IR −0.918,
>   still discarded (decay 2.31). Naive direction inversion is rejected.
> - `exclude_d8_d9` (buy the non-top-20% names) is the current best: IR −0.743 (least negative),
>   **walk_forward_decay 0.0** (consistently below benchmark in train and validation, no decay),
>   so it passes the hard gates and is kept. The marginal value is *avoiding* the high-score
>   D8/D9 names rather than buying low scores. Score is still negative (no positive-IR gate in
>   the frozen metric), so this is a relative/research result, not a promotion.
