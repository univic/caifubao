# H20 Excess Alpha Runs

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
