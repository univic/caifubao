# Strategy Paper Runner Tasks

## 1. Strategy config + selection

- [ ] 1.1 Versioned strategy config model (score source model_version, selection
  top_percentile bounds/size, constraints, rebalance cadence; default = flip_wide
  shadow wide book). Config validation (bounds in [0,1], size > 0, known keys).
- [ ] 1.2 Selection service: read VERIFIED predictions for the configured
  model_version + date, apply eligibility constraints, rank by score, select the
  wide book; "buy high" only (no direction logic in the strategy layer).
- [ ] 1.3 Daily target portfolio + rebalance-list derivation (diff vs previous
  portfolio).

## 2. Paper NAV simulation

- [ ] 2.1 Paper NAV engine: next-open entry, commission/min/slippage/stamp duty,
  board lot, suspension roll-forward; execution params aligned with the
  autoresearch profile.
- [ ] 2.2 Equity curve + per-date values; same-date equal-weight benchmark.

## 3. Runner + freshness + persistence

- [ ] 3.1 strategy_runner daily job + CLI; datahub_job_runs freshness record;
  skip (not empty) when no VERIFIED scores.
- [ ] 3.2 Persistence models for target portfolio / rebalance / paper NAV /
  equity curve (datahub model; config_hash pinned).

## 4. Tests + gates

- [ ] 4.1 Unit tests: selection bounds/size/eligibility, rebalance diff, NAV
  costs + suspension roll-forward, benchmark parity, skip-on-no-data.
- [ ] 4.2 spec-guardian / qa-reviewer / contract-reviewer on the diff.
- [ ] 4.3 branch-conflict check against develop; CI green; merge.
- [ ] 4.4 (post-merge, operator) paper runbook: register flip_wide shadow if
  absent, backfill VERIFIED scores, run paper for ≥120 trading days, record in
  the manual-experiment ledger.
