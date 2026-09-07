# Strategy Paper Runner Tasks

> **Causal-timing correction:** `strategy-paper-causal-timing` supersedes the
> pre-correction VERIFIED-only and forward-evidence interpretation in this
> change. Usable score states are `PENDING`, `TRACKING`, `VERIFIED` and
> `INSUFFICIENT_DATA`; `BLOCKED` and `FAILED` are excluded. The correction adds
> actual UTC `decision_at`, the next calendar-session `execution_date`, and
> `timing_version=paper_causal_v1`; NAV requires the exact full config, including
> `initial_nav`, to match its `config_hash`. All output from that first slice is
> `evidence_kind=REPLAY` and cannot count toward a 120-session forward gate.
> The checked entries below remain historical gate records for #190/#191/#192;
> they do not claim that the correction's implementation or tests are complete.

## 1. Strategy config + selection

- [x] 1.1 Versioned strategy config (score source model_version, selection
  top_percentile/top_n bounds/size, constraints, rebalance cadence; default =
  flip_wide shadow wide book). Validation rejects unknown keys at every level
  (typos fail loudly), bounds in [0,1], size > 0, explicit score source.
- [x] 1.2 Selection service: rank by score desc, filter by eligibility, apply
  the top_percentile band / top_n cap, equal-weight output; "buy high" only
  (no direction logic in the strategy layer). The runner consumes usable
  predictions (`PENDING`/`TRACKING`/`VERIFIED`/`INSUFFICIENT_DATA`) and maps
  them with stock flags onto the injected shapes; `BLOCKED`/`FAILED` rows are
  excluded.
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
  freshness record; skip (not empty) when no usable scores; fails closed
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
- [x] 4.2 spec-guardian / qa-reviewer / contract-reviewer on the diff — all
  GATE_OK across #190/#191/#192 (slice 1 P2s, slice 2 P1s, and the slice 3 P1
  date-key bug all fixed and re-reviewed).
- [x] 4.3 branch-conflict check against develop; CI green; merged (#190/#191/#192).
- [ ] 4.4 (operator, DB access required) paper run per
  docs/autoresearch/runs/h20-excess-alpha/task-4.4-paper-run-120d.md: register
  flip_wide shadow if absent, prepare usable scores, pass the exact full
  `paper_causal_v1` config (including `initial_nav`) to run and NAV, and record
  `decision_at`, next-session `execution_date`, and `evidence_kind=REPLAY`.
  This slice's replay output cannot count toward the 120-session forward gate;
  immutable forward capture/count is a separate later task.
