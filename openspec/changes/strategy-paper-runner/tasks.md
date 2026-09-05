# Strategy Paper Runner Tasks

## 1. Strategy config + selection

- [x] 1.1 Versioned strategy config (score source model_version, selection
  top_percentile/top_n bounds/size, constraints, rebalance cadence; default =
  flip_wide shadow wide book). Validation rejects unknown keys at every level
  (typos fail loudly), bounds in [0,1], size > 0, explicit score source.
- [x] 1.2 Selection service: rank by score desc, filter by eligibility, apply
  the top_percentile band / top_n cap, equal-weight output; "buy high" only
  (no direction logic in the strategy layer). Runner slice maps VERIFIED
  predictions + stock flags onto the injected shapes.
- [x] 1.3 Rebalance-list derivation (diff previous vs target holdings).

## 2. Paper NAV simulation

- [x] 2.1 Paper NAV engine: next-open entry, commission/min/slippage/stamp
  duty, board lot, suspension roll-forward (sell kept, buy skipped, valuation
  held at last observed close); execution params aligned with the autoresearch
  profile.
- [x] 2.2 NAV/drawdown/daily-return curve + turnover per cycle; same-date
  equal-weight benchmark passthrough.

## 3. Runner + freshness + persistence

- [ ] 3.1 strategy_runner daily job + CLI; datahub_job_runs freshness record;
  skip (not empty) when no VERIFIED scores; assert the named model_version is
  registered and covers the configured horizon before backfill.
- [ ] 3.2 Persistence models for target portfolio / rebalance / paper NAV /
  equity curve (datahub model; hash the *validated* config, pin it).

## 4. Tests + gates

- [x] 4.1 Unit tests: config validation + hash order-insensitivity + mutation
  safety, selection bands/eligibility/caps, rebalance diff, NAV costs /
  turnover / suspension roll-forward / last-close valuation / benchmark
  passthrough / empty-schedule guard. Skip-on-no-data test lands with 3.1.
- [ ] 4.2 spec-guardian / qa-reviewer / contract-reviewer on the diff.
- [ ] 4.3 branch-conflict check against develop; CI green; merge.
- [ ] 4.4 (post-merge, operator) paper runbook: register flip_wide shadow if
  absent, backfill VERIFIED scores, pick an initial NAV coherent with the
  book size (per-name budget >= a few board lots), run paper for ≥120 trading
  days, record in the manual-experiment ledger.
