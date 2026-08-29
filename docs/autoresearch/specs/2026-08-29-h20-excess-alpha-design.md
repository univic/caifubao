# H20 Excess Alpha Autoresearch Design

## Problem statement

Caifubao's `ranked_v1_h20` cross-sectional score is negatively associated
with subsequent 20-day returns in the currently verified 2026-06 to 2026-07
sample. The observed Spearman IC is -0.1206 and the highest score decile
underperforms the lowest score decile. This research target will determine
whether component direction, candidate weights, recommendation thresholds, and
market-regime filters can improve out-of-sample, transaction-cost-adjusted
excess returns relative to the contemporaneous tradable-universe equal-weight
benchmark.

This is research evidence for an A-share learning and demonstration MVP. It is
not investment advice, trading advice, or a claim of guaranteed profitability.

## Compatibility diagnosis

- Compatibility label: `v1-bootstrap-fit`
- Evidence: Caifubao is a multi-module Flask, MongoDB, datahub, and Vue
  application rather than a single-file training repository. Historical
  scoring replay is too expensive and stateful to repeat within every bounded
  experiment, but the candidate H20 scoring calculation and backtest can run
  as a local, single-process research command against one immutable snapshot.
- Thin adapter: planning may introduce a research-only command that (1) builds
  and verifies one immutable snapshot during bootstrap and (2) evaluates a
  candidate H20 YAML configuration against that snapshot, emitting one JSON
  report plus one plain numeric metric line. The adapter must reuse the
  repository's factor/scoring and transaction-cost semantics where applicable.
- Adapter boundary: the adapter must not change production model defaults,
  MongoDB records, public APIs, auth, OpenClaw contracts, frontend behavior,
  Kubernetes resources, scheduler behavior, or deployment configuration. It
  must not query mutable MongoDB data during an individual candidate run.

There is one experiment target: H20 cross-sectional excess-alpha research.
H5, H60, production promotion, and operations stability are separate targets
and are outside this cycle.

## Chosen approach

Use component-direction audit plus conditional recomposition. Candidate runs
may adjust approved H20 component directions, weights, percentile thresholds,
and a market-regime filter. Two simpler hypotheses remain immutable controls:
full percentile reversal and D8/D9 exclusion. The unchanged current score is
the baseline control.

This approach is preferred over unconditional percentile reversal because the
existing evidence establishes a high-score death zone in one bearish sample,
but does not establish that every low-score stock is a robust buy across market
regimes. It is preferred over exclusion-only research because component IC and
regime diagnostics can identify which duplicated trend/chasing exposures are
responsible for the inversion.

## Key decisions and trade-offs

- Primary objective: improve out-of-sample, transaction-cost-adjusted excess
  return relative to the same-date tradable-universe equal-weight benchmark.
- Research horizon: H20 only.
- Historical range: 2024-01-01 through 2026-07-31, full eligible A-share
  universe.
- Split: train 2024-01-01 through 2024-12-31; validation 2025-01-01 through
  2025-06-30; final test 2025-07-01 through 2026-07-31. Quarterly rolling
  walk-forward checks are also required.
- Execution: a score produced after market close can trade no earlier than the
  next trading day's open. A suspended or limit-up buy rolls forward to the
  first executable open. The H20 holding clock starts on that actual entry
  session; normal exit is first attempted at the twentieth subsequent trading
  session's open, and a suspended or limit-down sell rolls forward to the first
  executable open. Buy and sell commission is 0.025% with a CNY 5 minimum;
  sell stamp duty is 0.1%; adverse slippage is 0.1% per side.
- Main benchmark: equal-weight forward return of the same tradable cohort on
  each scoring date. CSI 300 and CSI 500 are diagnostic benchmarks only and
  cannot select candidates.
- Test isolation: keep/discard decisions use validation or quarterly
  walk-forward validation metrics only. The final test split is evaluated once
  after train and validation selection is frozen; its result is final research
  evidence and cannot feed back into candidate selection.
- Existing close-to-close 2026 decile results remain diagnostic evidence only;
  they cannot determine experiment keep/discard status.
- Production promotion is excluded. A winning candidate requires a later
  OpenSpec change, model version bump, full-market calibration comparison,
  focused tests, reviewers, and normal PR/CI gates.

## Experiment metric and gates

The adapter emits exactly one mechanical scalar named
`research_profitability_score`; higher is better. Its test-independent
candidate-selection value is computed on validation or walk-forward validation
data as:

```text
research_profitability_score =
    out_of_sample_information_ratio
    + 0.10 * annualized_net_excess_return
    - 2.00 * max(0, abs(excess_max_drawdown) - 0.10)
    - 0.02 * max(0, annual_turnover - 6.00)
    - 1.00 * max(0, profit_concentration - 0.25)
```

Returns and ratios use decimal units; `annual_turnover` is one-way annual
portfolio turnover expressed as a multiple of NAV. The fixed penalty equations
above are adapter semantics and cannot be edited by the experiment loop. The
information ratio is the dominant term.
The report must separately retain net excess return, information ratio,
maximum excess drawdown, turnover, concentration, sample size, and overfit
measurements.

Any of the following forces the scalar to `-999.0`: fewer than 5 completed
trades, fewer than 120 eligible trading days, a single profitable trade
contributing more than 40% of total profitable-trade P&L, or walk-forward
performance decay above 20%. Crashes and timeouts do not emit a competitive
metric and are recorded as crashes.

The exact stdout extraction shape is:

```text
research_profitability_score: <plain finite decimal number>
```

No percent sign, suffix, prose, or second metric may appear on that line.

## Frozen profile fields

- `runtime.manager: local-process`
- `runtime.env_prep_command: PYTHONPATH=datahub datahub/.venv/bin/python -m app.jobs.autoresearch_h20_runner prepare --profile autoresearch/profile.yaml`
- `runtime.entry_command: PYTHONPATH=datahub datahub/.venv/bin/python -m app.jobs.autoresearch_h20_runner run --profile autoresearch/profile.yaml --candidate datahub/research/autoresearch/h20_excess_alpha/candidate.yaml`
- `runtime.timeout_seconds: 600`
- `experiment.time_budget_seconds: 600`
- `experiment.metric_name: research_profitability_score`
- `experiment.metric_direction: maximize`
- `edit_scope.allowed_paths: [datahub/research/autoresearch/h20_excess_alpha/candidate.yaml]`
- `edit_scope.readonly_paths: [datahub/app/lib/factor_factory/, datahub/app/lib/signal_factory/, datahub/app/lib/scoring_engine/, datahub/app/model/, backend/, frontend/, k8s/, openspec/, docs/operations/, datahub/research/autoresearch/h20_excess_alpha/profile.yaml, datahub/research/autoresearch/h20_excess_alpha/snapshot-manifest.json]`
- `edit_scope.primary_edit_target: datahub/research/autoresearch/h20_excess_alpha/candidate.yaml`
- `baseline.must_run_first: true`
- `baseline.protocol: Build and validate one immutable 2024-01-01..2026-07-31 full-universe H20 research snapshot in dev; record its path, row counts by date, eligible universe counts, missingness, checksum, source model version, factor/signal versions, and generation command; then run the unchanged current H20 scoring configuration with the frozen T+1-open execution model, costs, benchmark, split, and quarterly walk-forward checks before any candidate experiment.`
- `baseline.baseline_description: Current unchanged H20 component directions, weights, percentile semantics, and thresholds evaluated against the tradable-universe equal-weight benchmark; full reversal and D8/D9 exclusion are fixed non-editable control configurations.`
- `git_policy.branch_prefix: codex/autoresearch-h20-`
- `git_policy.commit_before_run: true`
- `git_policy.keep_commit_strategy: keep-current-commit`
- `git_policy.discard_strategy: hard-reset-to-pre-run-commit`
- `git_policy.crash_strategy: keep-crash-commit-for-inspection`
- `logging.run_log_path: docs/autoresearch/runs/h20-excess-alpha/results.jsonl`
- `logging.summary_extract_command: PYTHONPATH=datahub datahub/.venv/bin/python -m app.jobs.autoresearch_h20_runner metric --report docs/autoresearch/runs/h20-excess-alpha/latest-report.json`
- `logging.results_columns: [run_id, git_ref, candidate_config_sha256, candidate_summary, snapshot_sha256, train_range, validation_range, test_range, information_ratio, annualized_net_excess_return, excess_max_drawdown, annual_turnover, profit_concentration, completed_trades, eligible_trading_days, walk_forward_decay, research_profitability_score, decision, reason, elapsed_seconds]`

The bootstrap snapshot may live outside Git. Its absolute local path and SHA-256
checksum must be recorded in `snapshot-manifest.json`. Snapshot contents,
MongoDB exports, credentials, tokens, and private infrastructure information
must never be committed.

## Open questions

None.
