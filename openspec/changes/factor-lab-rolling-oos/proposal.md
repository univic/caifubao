## Why

`factor_lab_composite_backtest.py` reports a walk-forward composite, but the
external review recorded in `docs/operations/strategy-experiments-2026-08.md`
(2026-09-19) is still correct: the reported numbers are **not** a true
out-of-sample protocol. Weights at a rebalance use every IC realised before that
date, and portfolio evaluation reuses the whole sample. With 20/60/120-session
forward labels, the training ICs next to a cutoff have labels that overlap the
evaluation window, so information crosses the train/test boundary; and no single
document records every configuration that was attempted.

The frozen `riq_v1` candidate and its controls need a rolling train/test split
with purge and embargo before any result can be described as out-of-sample.

## What Changes

- Add `--mode oos` to `datahub/scripts/factor_lab_composite_backtest.py`: split
  the panel sessions into consecutive folds of `--train-sessions` (default 500)
  followed by `--test-sessions` (default 250), rolled forward by
  `test_sessions`; refuse loudly when the panel is shorter than one full fold.
- For each fold, freeze the composite ICIR weights at the causal cutoff
  `test_start_index - label_horizon - 1 - embargo` (purge = label horizon + 1,
  `--embargo` default 0, and a negative embargo is refused because it would move
  the cutoff later and re-admit the overlapping-label ICs), the same convention
  `walk_forward_composite` already uses, and evaluate the frozen book on the test
  window only, with the existing quarterly rebalance, equal-weight top-`--names`,
  2N hold-while-in-band buffer and T+1 open execution costing.
- Stitch the fold test-window returns into one out-of-sample curve and report
  total return, annualised return, max drawdown, annualised turnover and cash
  share, plus a per-fold table (including the effective IC history bounds
  `ic_first_date`/`ic_cutoff`, since `--train-sessions` shifts folds while
  `--lookback` caps the weights' history) and a `config` block naming every knob.
- Make the per-mode `--step` default explicit: `oos` alone defaults to 60 and the
  label-based modes to 20, so a mixed run must pass `--step` instead of silently
  giving the out-of-sample book the wrong cadence.
- Document `oos` (and the pre-existing `sprint`) in the module docstring and
  `docs/operations/factor-lab.md`, and record that "existing modes unchanged"
  covers the JSON document / `--output` payload, not stdout (the app log handler
  is now bound to stderr so stdout is the JSON payload alone).
- Add synthetic-panel tests in `datahub/app/test/test_factor_lab_oos.py` for the
  purge cutoff boundary (derived from the open-to-open label), fold tiling, the
  config/per-fold record, and the short-panel, negative-embargo and mixed-mode
  refusals.

## Impact

- Affected code: `datahub/scripts/factor_lab_composite_backtest.py` (new mode),
  `datahub/scripts/factor_lab_account_replay.py` (the daily replay loop is
  extracted into a reusable `replay_book` so the OOS mode shares it, and its
  `--start-date`/`--end-date` flags build the in-sample arm on a chosen window),
  the new test module, `docs/operations/factor-lab.md`, and the account-replay
  test. All research-only scripts that write nothing to MongoDB.
- Existing report modes keep their current **JSON document / `--output`
  payload**; the stdout stream now carries only that JSON (the pre-change build
  leaked environment and startup log lines onto stdout), and
  `factor_lab_account_replay` keeps its three variants' JSON content while
  echoing the requested `start_date`/`end_date`.
- Not affected: production scoring, signals, the strategy engine, backend APIs,
  OpenClaw contracts, auth, freshness metadata, data ownership.
- Data ownership: `factor_lab` stays in `datahub`, read-only, parquet in and
  JSON out.

## Non-goals

- No change to the production score model or to the paper-ledger forward
  evidence rules.
- No new data source, no network access, no scheduler.
- No claim that an OOS result is forward evidence or investment advice.
