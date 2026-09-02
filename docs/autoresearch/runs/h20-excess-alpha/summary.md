# H20 Excess Alpha Runs

Research-only evidence for the Caifubao learning and demonstration MVP. This is not investment advice or a production model promotion record.

| Configuration | Status | Score | IR | Net excess return | Max excess drawdown | Decision |
|---|---:|---:|---:|---:|---:|---|
| current_h20 (baseline, val=2025 full yr) | completed | -999.0 | -0.859 | -0.131 | -0.949 | discard |

> Validation split adjusted 2026-09: 2025H1-only (117 days, gated insufficient_period) →
> full 2025 (243 eligible days). Baseline now clears the sample gate but is discarded by
> `performance_decay` (train IR ≈ +0.17 → validation IR −0.86; walk_forward_decay 6.14 > 0.20),
> consistent with the score inverting out of sample.
