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

- [x] 3.1 strategy_runner daily job + CLI (run/report); datahub_job_runs
  freshness record; skip (not empty) when no VERIFIED scores; fails closed
  unless the score source is ACTIVE-registered and covers the horizon.
- [x] 3.2 StrategyPaperRun persistence model (date/model_version/horizon/
  config_hash of the *validated* config, target holdings, rebalance, status);
  `nav` CLI command recomputes the paper NAV curve over COMPLETED runs in a
  range (quote prices + equal-weight benchmark -> simulate_paper_nav) and
  writes each curve point back into the matching run's nav_snapshot.

## 4. Tests + gates

- [x] 4.1 Unit tests: config validation + hash order-insensitivity + mutation
  safety, selection bands/eligibility/caps, rebalance diff, NAV costs /
  turnover / suspension roll-forward / last-close valuation / benchmark
  passthrough / empty-schedule guard; runner registry fail-closed (unregistered
  / horizon-missing) + skip-on-no-data + parser wiring tests.
- [ ] 4.2 spec-guardian / qa-reviewer / contract-reviewer on the diff.
- [ ] 4.3 branch-conflict check against develop; CI green; merge.
- [ ] 4.4 (operator, DB access required) paper run per
  docs/autoresearch/runs/h20-excess-alpha/task-4.4-paper-run-120d.md: register
  flip_wide shadow if absent, backfill VERIFIED scores, pick an initial NAV
  coherent with the book size, run paper ≥120 trading days, record in the
  manual-experiment ledger.
