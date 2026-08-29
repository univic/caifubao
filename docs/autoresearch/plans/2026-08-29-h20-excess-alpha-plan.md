# Caifubao Autoresearch Adaptation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (- [ ]) syntax for tracking.

**Goal:** Scaffold an auditable H20 loop that maximizes cost-adjusted out-of-sample excess return versus the same-date tradable-universe equal-weight benchmark without changing production scoring.

**Architecture:** A resource-bounded, research-only exporter reconstructs H20 component snapshots from dev MongoDB in scoring-date batches, without persisting predictions, and hands one source Parquet to bootstrap. Bootstrap validates and checksums that immutable full-market snapshot, then runs exactly one unchanged baseline. A research-only runner later evaluates JSON-compatible YAML candidates against the frozen snapshot; production factor, signal, scoring, API, deployment, and model-default files remain untouched.

**Tech Stack:** Python 3.12, pandas, NumPy, PyArrow, MongoEngine/PyMongo, pytest, ruff, JSON-compatible YAML, TSV, JSONL.

---

## File map

- Create autoresearch/profile.yaml as a symlink to the canonical profile.
- Modify autoresearch/state.yaml for pipeline state and real git refs.
- Create autoresearch/results.tsv and autoresearch/ledger.jsonl.
- Create datahub/research/autoresearch/h20_excess_alpha/profile.yaml for the frozen contract.
- Create baseline.yaml, full_reversal.yaml, exclude_d8_d9.yaml, and candidate.yaml beside the profile.
- Generate snapshot-manifest.json beside the profile; snapshot.parquet is local and ignored.
- Create datahub/app/lib/autoresearch/__init__.py and h20_excess_alpha.py for research-only evaluation.
- Create datahub/app/jobs/autoresearch_h20_runner.py for prepare, run, and metric.
- Create datahub/app/jobs/autoresearch_h20_snapshot_runner.py for read-only, bounded historical component reconstruction.
- Create datahub/app/test/test_autoresearch_h20.py.
- Create datahub/app/test/test_autoresearch_h20_snapshot.py.
- Create docs/autoresearch/runs/h20-excess-alpha/results.jsonl as a symlink to the ledger.
- Create docs/autoresearch/runs/h20-excess-alpha/summary.md.
- Ignore snapshot.parquet and latest-report.json in .gitignore.

### Task 1: Freeze profile and controls

**Files:**
- Create: datahub/research/autoresearch/h20_excess_alpha/profile.yaml
- Create: datahub/research/autoresearch/h20_excess_alpha/{baseline,full_reversal,exclude_d8_d9,candidate}.yaml
- Create: autoresearch/profile.yaml
- Verify: all configuration files parse with Python json

- [ ] **Step 1: Write the profile**

Write JSON-compatible YAML with these exact objects:

    runtime = {
      "manager":"local-process",
      "env_prep_command":"PYTHONPATH=datahub datahub/.venv/bin/python -m app.jobs.autoresearch_h20_runner prepare --profile autoresearch/profile.yaml",
      "entry_command":"PYTHONPATH=datahub datahub/.venv/bin/python -m app.jobs.autoresearch_h20_runner run --profile autoresearch/profile.yaml --candidate datahub/research/autoresearch/h20_excess_alpha/candidate.yaml",
      "timeout_seconds":600
    }
    experiment = {
      "time_budget_seconds":600,
      "metric_name":"research_profitability_score",
      "metric_direction":"maximize",
      "horizon":20,
      "data":["2024-01-01","2026-07-31"],
      "train":["2024-01-01","2024-12-31"],
      "validation":["2025-01-01","2025-06-30"],
      "test":["2025-07-01","2026-07-31"],
      "test_locked":true,
      "walk_forward_frequency":"quarterly"
    }
    execution = {
      "signal_time":"close",
      "entry_time":"next_trading_day_open",
      "commission_rate":0.00025,
      "minimum_commission_cny":5.0,
      "sell_stamp_duty_rate":0.001,
      "slippage_per_side":0.001,
      "blocked_order_policy":"roll_forward_until_executable"
    }
    benchmark = {
      "primary":"same_date_tradable_universe_equal_weight",
      "diagnostic":["CSI300","CSI500"]
    }
    metric = {
      "ir_weight":1.0,
      "annualized_net_excess_return_weight":0.10,
      "drawdown_free_allowance":0.10,
      "drawdown_penalty_weight":2.0,
      "turnover_free_allowance":6.0,
      "turnover_penalty_weight":0.02,
      "concentration_free_allowance":0.25,
      "concentration_penalty_weight":1.0,
      "minimum_completed_trades":5,
      "minimum_eligible_trading_days":120,
      "maximum_profit_concentration":0.40,
      "maximum_walk_forward_decay":0.20,
      "hard_failure_score":-999.0
    }
    edit_scope = {
      "allowed_paths":["datahub/research/autoresearch/h20_excess_alpha/candidate.yaml"],
      "primary_edit_target":"datahub/research/autoresearch/h20_excess_alpha/candidate.yaml",
      "readonly_paths":["datahub/app/lib/factor_factory/","datahub/app/lib/signal_factory/","datahub/app/lib/scoring_engine/","datahub/app/model/","backend/","frontend/","k8s/","openspec/","docs/operations/","datahub/research/autoresearch/h20_excess_alpha/profile.yaml","datahub/research/autoresearch/h20_excess_alpha/snapshot-manifest.json"]
    }
    baseline = {
      "must_run_first":true,
      "description":"Current unchanged H20 configuration; full reversal and D8/D9 exclusion are immutable controls",
      "protocol":"prepare and validate the immutable snapshot, capture git ref, then run current_h20 on validation before candidates"
    }
    git_policy = {
      "branch_prefix":"codex/autoresearch-h20-",
      "commit_before_run":true,
      "keep_commit_strategy":"keep-current-commit",
      "discard_strategy":"hard-reset-to-pre-run-commit",
      "crash_strategy":"keep-crash-commit-for-inspection"
    }
    logging = {
      "run_log_path":"docs/autoresearch/runs/h20-excess-alpha/results.jsonl",
      "summary_extract_command":"PYTHONPATH=datahub datahub/.venv/bin/python -m app.jobs.autoresearch_h20_runner metric --report docs/autoresearch/runs/h20-excess-alpha/latest-report.json",
      "results_columns":["run_id","git_ref","candidate_config_sha256","candidate_summary","snapshot_sha256","train_range","validation_range","test_range","information_ratio","annualized_net_excess_return","excess_max_drawdown","annual_turnover","profit_concentration","completed_trades","eligible_trading_days","walk_forward_decay","research_profitability_score","decision","reason","elapsed_seconds"]
    }
    artifacts = {
      "snapshot_path":"datahub/research/autoresearch/h20_excess_alpha/snapshot.parquet",
      "snapshot_manifest_path":"datahub/research/autoresearch/h20_excess_alpha/snapshot-manifest.json",
      "latest_report_path":"docs/autoresearch/runs/h20-excess-alpha/latest-report.json",
      "results_tsv_path":"autoresearch/results.tsv",
      "ledger_path":"autoresearch/ledger.jsonl"
    }

The file is one JSON object with those ten keys.

- [ ] **Step 2: Write controls**

Every control has component_directions, weights, selection, and regime_filter. Component order is signal_strength, momentum, trend_alignment, breakout_or_position, industry_momentum, relative_strength, real_relative_strength, risk_penalty. Baseline and initial candidate directions are [1,1,1,1,1,1,1,-1], weights are [15,15,30,5,5,15,10,15], selection is top_percentile [0.95,1.0] with portfolio_size 30, and regime mode is none.

full_reversal differs only by directions [-1,-1,-1,-1,-1,-1,-1,-1]. exclude_d8_d9 uses baseline directions and selection mode exclude_percentile [0.80,1.0], portfolio_size 30. Names are current_h20, full_reversal, and exclude_d8_d9. candidate initially equals baseline byte-for-byte.

- [ ] **Step 3: Link and verify**

Run:

    ln -s ../datahub/research/autoresearch/h20_excess_alpha/profile.yaml autoresearch/profile.yaml
    datahub/.venv/bin/python -c 'import json,pathlib; p=list(pathlib.Path("datahub/research/autoresearch/h20_excess_alpha").glob("*.yaml"))+[pathlib.Path("autoresearch/profile.yaml")]; [json.loads(x.read_text()) for x in p]; print("profile-and-controls: ok")'

Expected: exactly profile-and-controls: ok.

- [ ] **Step 4: Commit**

    git add autoresearch/profile.yaml datahub/research/autoresearch/h20_excess_alpha
    git commit -m "chore: add h20 autoresearch profile"

### Task 2: Scaffold state, results, ledger, and ignores

**Files:**
- Modify: autoresearch/state.yaml
- Create: autoresearch/results.tsv
- Create: autoresearch/ledger.jsonl
- Create: docs/autoresearch/runs/h20-excess-alpha/results.jsonl
- Create: docs/autoresearch/runs/h20-excess-alpha/summary.md
- Modify: .gitignore
- Verify: exact header, empty ledger, resolving symlink

- [ ] **Step 1: Write artifacts**

results.tsv has this exact 5-column tab-separated header required by the autoresearch bootstrap contract:

    commit metric_value memory_gb status description

Replace spaces between field names with literal tab characters. The 20 detailed research fields frozen in logging.results_columns belong to each JSONL ledger entry and full JSON report, not this compatibility TSV. ledger.jsonl starts empty. summary.md contains the title H20 Excess Alpha Runs, the research-only/non-investment-advice statement, and a table with Configuration, Status, Score, IR, Net excess return, Max excess drawdown, Decision.

Append exactly:

    datahub/research/autoresearch/h20_excess_alpha/snapshot.parquet
    docs/autoresearch/runs/h20-excess-alpha/latest-report.json

to .gitignore if absent.

- [ ] **Step 2: Link and verify**

    mkdir -p docs/autoresearch/runs/h20-excess-alpha
    ln -s ../../../../autoresearch/ledger.jsonl docs/autoresearch/runs/h20-excess-alpha/results.jsonl
    datahub/.venv/bin/python -c 'import pathlib; h=pathlib.Path("autoresearch/results.tsv").read_text().splitlines(); assert h==["commit\tmetric_value\tmemory_gb\tstatus\tdescription"]; assert pathlib.Path("autoresearch/ledger.jsonl").read_text()==""; assert pathlib.Path("docs/autoresearch/runs/h20-excess-alpha/results.jsonl").resolve()==pathlib.Path("autoresearch/ledger.jsonl").resolve(); print("autoresearch-scaffold: ok")'

Expected: exactly autoresearch-scaffold: ok.

- [ ] **Step 3: Commit**

    git add .gitignore autoresearch docs/autoresearch/runs/h20-excess-alpha
    git commit -m "chore: scaffold autoresearch run ledger"

### Task 3: Implement the research evaluator with tests first

**Files:**
- Create: datahub/app/lib/autoresearch/__init__.py
- Create: datahub/app/lib/autoresearch/h20_excess_alpha.py
- Create: datahub/app/test/test_autoresearch_h20.py
- Verify: focused pytest and ruff

- [ ] **Step 1: Write failing tests**

Use an in-memory DataFrame; never connect to MongoDB. Tests assert:

1. Candidate validation rejects weights not summing to 100, unknown components, directions outside {-1,0,1}, portfolio size below 1, and bounds outside [0,1].
2. Ranking is within scoring date, ties use average percentile, and risk_penalty direction -1 subtracts it.
3. Positions and all returns use actual_entry_open_hfq and
   actual_exit_open_hfq exclusively, never scoring-date close or a
   signal-anchored legacy label.
4. Blocked entry rolls forward to the first tradable open; no later tradable row means no trade.
5. Friction is max(value*0.00025,5) commission, value*0.001 sell duty, and adverse 0.1% slippage each side.
6. Benchmark freezes the same eligible date cohort before selection and excludes BSE, ST, listing age below 60, unresolved execution, and missing actual-price rows; eligibility_reason remains auditable.
7. IR equals mean excess divided by sample standard deviation times sqrt(252).
8. The frozen score equation is exact; each hard gate independently yields -999.0.
9. allow_test false rejects the final test split.
10. Full report and JSONL entry contain all 20 detailed research fields; the compatibility TSV contains exactly 5 fields; the metric line matches research_profitability_score: followed by one finite decimal.

Run:

    datahub/.venv/bin/python -m pytest datahub/app/test/test_autoresearch_h20.py -q

Expected before implementation: import failure for app.lib.autoresearch.h20_excess_alpha.

- [ ] **Step 2: Implement only this public API**

    load_json_yaml(path) -> dict
    validate_candidate(candidate) -> None
    validate_snapshot(frame, profile) -> dict
    rank_components(frame, candidate) -> DataFrame
    build_positions(scored, candidate, profile) -> DataFrame
    apply_friction(price, quantity, side, profile) -> tuple
    equal_weight_benchmark(frame) -> Series
    information_ratio(daily_excess) -> float
    profitability_score(metrics, profile) -> (float, list[str])
    evaluate_candidate(snapshot_path, candidate_path, profile_path, split="validation", allow_test=False) -> dict
    append_run_artifacts(report, profile) -> None
    metric_from_report(report_path) -> float

Rules: load JSON with stdlib json; read manifest-declared Parquet columns only; rank component values cross-sectionally per date; multiply percentiles by direction and weight; rank the combined score per date. Freeze eligible cohort before candidate selection. Rebalance every 20 trading days into at most 30 equal-weight names from CNY 1,000,000 using 100-share board lots. Regime none always permits entry; the only additional mode is market_breadth with minimum_fraction_above_ma60 and position_scale, using contemporaneous data only.

Compute daily strategy and benchmark NAV. IR uses daily strategy-minus-benchmark returns. Quarterly walk-forward decay is max(0,(train_ir-validation_ir)/max(abs(train_ir),1e-9)). Write reports atomically with os.replace. Append one detailed JSON object and one 5-field compatibility TSV row without rewriting history. Never write MongoDB, production predictions, snapshot data, controls, or profile.

- [ ] **Step 3: Verify**

    datahub/.venv/bin/python -m pytest datahub/app/test/test_autoresearch_h20.py -q
    datahub/.venv/bin/ruff check datahub/app/lib/autoresearch datahub/app/test/test_autoresearch_h20.py
    datahub/.venv/bin/ruff format --check datahub/app/lib/autoresearch datahub/app/test/test_autoresearch_h20.py

Expected: tests pass and both ruff commands exit 0.

- [ ] **Step 4: Commit**

    git add datahub/app/lib/autoresearch datahub/app/test/test_autoresearch_h20.py
    git commit -m "feat: add h20 excess alpha evaluator"

### Task 4: Implement prepare, run, and metric CLI

**Files:**
- Create: datahub/app/jobs/autoresearch_h20_runner.py
- Modify: datahub/app/test/test_autoresearch_h20.py
- Generate: datahub/research/autoresearch/h20_excess_alpha/snapshot-manifest.json
- Verify: synthetic lifecycle

- [ ] **Step 1: Add CLI tests**

Test main(argv) with temporary files. prepare rejects missing columns and leaves no manifest. prepare --source-parquet writes snapshot and manifest with absolute path, SHA-256, range, total and per-date counts, eligible counts, missingness, model/factor/signal versions, schema h20-excess-alpha-v1, and generation command. run writes report and appends one ledger and TSV row. metric prints one value matching ^-?[0-9]+\.?[0-9]*$. Test split requires --allow-test.

- [ ] **Step 2: Implement exact commands**

    prepare --profile PATH [--source-parquet PATH]
    run --profile PATH --candidate PATH [--split train|validation|test] [--allow-test]
    metric --report PATH

Real prepare accepts the source Parquet produced by Task 5 and normalizes it into:

    date, stock_code, is_bse, is_st, listing_days, trade_status,
    open_hfq, close_hfq, high_hfq, low_hfq,
    requested_entry_date, actual_entry_date, actual_entry_open_hfq,
    entry_blocked_sessions, requested_exit_date, actual_exit_date,
    actual_exit_open_hfq, exit_blocked_sessions, eligibility,
    eligibility_reason,
    signal_strength, momentum, trend_alignment, breakout_or_position,
    industry_momentum, relative_strength, real_relative_strength, risk_penalty,
    market_fraction_above_ma60, source_model_version, factor_version, signal_version

Components use only information available by scoring-date close. Entry/exit label columns never enter scoring. Write a temporary Parquet, validate, checksum, atomically rename, then write manifest. On failure remove only the new temporary file.

run prints exactly research_profitability_score: VALUE. metric prints only VALUE.

- [ ] **Step 3: Verify**

    datahub/.venv/bin/python -m pytest datahub/app/test/test_autoresearch_h20.py -q
    datahub/.venv/bin/ruff check datahub/app/jobs/autoresearch_h20_runner.py datahub/app/lib/autoresearch datahub/app/test/test_autoresearch_h20.py
    datahub/.venv/bin/ruff format --check datahub/app/jobs/autoresearch_h20_runner.py datahub/app/lib/autoresearch datahub/app/test/test_autoresearch_h20.py

Expected: all commands exit 0.

- [ ] **Step 4: Commit**

    git add datahub/app/jobs/autoresearch_h20_runner.py datahub/app/test/test_autoresearch_h20.py
    git commit -m "feat: add h20 autoresearch runner"

### Task 5: Export the historical H20 source snapshot

**Files:**
- Create: datahub/app/jobs/autoresearch_h20_snapshot_runner.py
- Create: datahub/app/test/test_autoresearch_h20_snapshot.py
- Generate: /private/tmp/caifubao-h20-source.parquet
- Verify: bounded read-only export, exact date coverage, deterministic checksum

- [ ] **Step 1: Add exporter tests**

Use injected in-memory quote, factor, signal, stock, and industry rows; never connect tests to MongoDB. Test the public functions build_trade_calendar(rows), build_date_batch(start, end, batch_days), reconstruct_component_rows(source, scoring_dates, horizon=20), validate_export(frame, expected_start, expected_end), and main(argv). Assert: scoring dates are completed trading dates only; every component uses records with date less than or equal to the scoring date; entry first attempts the next trading-day open and rolls past suspension/limit-up; exit is the twentieth trading session strictly after actual entry and rolls past suspension/limit-down; requested/actual dates and blocked-session counts are retained; unresolved orders are non-eligible rather than dropped and have a deterministic eligibility_reason; BSE, ST, listing age below 60, suspension, missing HFQ, and missing exit rows are retained with eligibility fields but excluded by eligible_mask; no legacy next_open_hfq/exit_open_hfq labels are written; batches contain at most 20 scoring dates; duplicate (date, stock_code) rows fail; an interrupted export leaves the destination untouched; and --dry-run performs queries and validation without writing.

- [ ] **Step 2: Implement the read-only exporter**

Implement this exact CLI:

    PYTHONPATH=datahub datahub/.venv/bin/python -m app.jobs.autoresearch_h20_snapshot_runner export --from-date 2024-01-01 --to-date 2026-07-31 --horizon 20 --batch-trading-days 20 --output /private/tmp/caifubao-h20-source.parquet

The runner initializes the existing datahub MongoEngine connection, reads IndividualStock, StockDailyQuote, StockFactorDaily, StockSignalDaily, industry aggregates, and trade calendars, and never calls save, update, delete, scoring_runner, or a production persistence method. For each scoring-date batch, reuse StockScoringService._build_components with the frozen H20 config and convert explanation components into the eight raw fields signal_strength, momentum, trend_alignment, breakout_or_position, industry_momentum, relative_strength, real_relative_strength, and risk_penalty. Read only information dated at or before the scoring date for component construction. Resolve execution labels using the existing A-share rule: suspended sessions are blocked; BUY is blocked when change_rate >= 9.9; SELL is blocked when change_rate <= -9.9. Start at the next trading session for entry, then count twenty trading sessions strictly after actual entry for requested exit; roll either order until executable. Never pass requested/actual execution labels into component construction.

Write the full schema listed in Task 4 plus is_bse, is_st, listing_days, trade_status, market_fraction_above_ma60, eligibility, and eligibility_reason. Use actual_entry_open_hfq and actual_exit_open_hfq as the only execution-price labels; do not emit legacy next_open_hfq or exit_open_hfq aliases. Derive eligibility_reason deterministically with the first applicable value from: bse, st, listing_age_below_60, scoring_session_not_tradable, missing_hfq, unresolved_entry, unresolved_exit, or eligible. Use projection and ordered date/code queries, process at most 20 scoring dates per batch, append batches to temporary Parquet row groups, validate the final range and uniqueness, then os.replace the requested output. On failure delete only the temporary file. Print exactly one JSON object containing output_path, row_count, eligible_count, date_min, date_max, and sha256; do not print database connection strings.

- [ ] **Step 3: Verify focused behavior**

Run:

    datahub/.venv/bin/python -m pytest datahub/app/test/test_autoresearch_h20_snapshot.py -q
    datahub/.venv/bin/ruff check datahub/app/jobs/autoresearch_h20_snapshot_runner.py datahub/app/test/test_autoresearch_h20_snapshot.py
    datahub/.venv/bin/ruff format --check datahub/app/jobs/autoresearch_h20_snapshot_runner.py datahub/app/test/test_autoresearch_h20_snapshot.py

Expected: tests pass and both ruff commands exit 0.

- [ ] **Step 4: Run resource-bounded real export**

Run the exact export command from Step 2. Expected: exit 0; JSON reports date_min 2024-01-01, date_max 2026-07-31, positive row_count and eligible_count; no credential-like keys; peak resident memory remains below 2 GiB as recorded by `/usr/bin/time -l` on macOS. If required raw component inputs do not cover the frozen range, stop with the first/last covered dates and per-collection missing counts; do not shorten the approved range.

- [ ] **Step 5: Commit**

    git add datahub/app/jobs/autoresearch_h20_snapshot_runner.py datahub/app/test/test_autoresearch_h20_snapshot.py
    git commit -m "feat: add bounded h20 snapshot exporter"

### Task 6: Bootstrap the immutable baseline

**Files:**
- Generate: snapshot.parquet and snapshot-manifest.json
- Modify: autoresearch/results.tsv, autoresearch/ledger.jsonl, summary.md, state.yaml
- Verify: baseline uses the frozen snapshot checksum

- [ ] **Step 1: Prepare real snapshot**

    datahub/.venv/bin/python -m app.jobs.autoresearch_h20_runner prepare --profile datahub/research/autoresearch/h20_excess_alpha/profile.yaml --source-parquet /private/tmp/caifubao-h20-source.parquet

Expected: range exactly 2024-01-01..2026-07-31; positive row and eligible counts; shasum -a 256 matches manifest; manifest has no credentials or connection strings.

- [ ] **Step 2: Record ref and run baseline first**

    git rev-parse HEAD
    datahub/.venv/bin/python -m app.jobs.autoresearch_h20_runner run --profile datahub/research/autoresearch/h20_excess_alpha/profile.yaml --candidate datahub/research/autoresearch/h20_excess_alpha/baseline.yaml --split validation

Expected: capture the 40-character SHA as baseline_ref; run ends within 600 seconds and emits one metric-prefixed line.

- [ ] **Step 3: Verify extraction**

    datahub/.venv/bin/python -m app.jobs.autoresearch_h20_runner metric --report docs/autoresearch/runs/h20-excess-alpha/latest-report.json

Expected: one line matching ^-?[0-9]+\.?[0-9]*$; reject percent, unit, empty, nan, inf, multiline, and prose.

- [ ] **Step 4: Update state and summary**

Populate summary from the baseline report only. Set current_stage bootstrap, stage_status completed, profile_status bootstrapped, baseline_status validated, baseline_ref/rollback_target to the captured baseline SHA, blocker_reason null, and next_allowed_skills to autoresearch-loop. Preserve best_ref exactly as required by the bootstrap state-ownership contract. Full reversal and D8/D9 exclusion remain immutable controls and become the first two bounded runs in autoresearch-loop.

- [ ] **Step 5: Commit**

    git add datahub/research/autoresearch/h20_excess_alpha/snapshot-manifest.json autoresearch/state.yaml autoresearch/results.tsv autoresearch/ledger.jsonl docs/autoresearch/runs/h20-excess-alpha/summary.md
    git commit -m "chore: record h20 autoresearch baseline"

### Task 7: Repository gates and loop readiness

**Files:**
- Modify: only mapped files when a check or reviewer requires a fix
- Verify: tests, lint, OpenSpec, reviews, conflict check, draft PR CI

- [ ] **Step 1: Validate**

    datahub/.venv/bin/python -m pytest datahub/app/test/test_autoresearch_h20.py -q
    datahub/.venv/bin/python -m pytest datahub/app/test/ -q
    datahub/.venv/bin/ruff check datahub/
    datahub/.venv/bin/ruff format --check datahub/
    openspec validate --all --strict

Expected: all exit 0. If full suite exceeds capacity, record the exact failure and rely on draft PR CI only after focused tests, ruff, and OpenSpec pass.

- [ ] **Step 2: Dry-run loop**

    git diff --name-only "$(git merge-base HEAD origin/develop)" HEAD
    datahub/.venv/bin/python -m app.jobs.autoresearch_h20_runner metric --report docs/autoresearch/runs/h20-excess-alpha/latest-report.json

Expected: mapped files only; the already-recorded baseline elapsed_seconds is at most 600; extraction returns one finite decimal. Do not execute another candidate or control during bootstrap readiness checks.

- [ ] **Step 3: Reviews**

Run spec-guardian because scoring/replay semantics are involved, then qa-reviewer after validation because this is non-trivial Python. contract-reviewer is not triggered because API, auth, freshness, and OpenClaw are excluded. Resolve every P1 and repeat affected validation/review.

- [ ] **Step 4: Conflict check**

    git fetch origin develop --quiet
    git merge-tree "$(git merge-base HEAD origin/develop)" origin/develop HEAD

Expected: no conflict markers.

- [ ] **Step 5: Draft PR and CI**

    gh pr create --draft --base develop --title "feat: bootstrap h20 excess alpha autoresearch" --body "Research-only H20 adapter, immutable snapshot manifest, controls, and loop scaffolding. No production default or API behavior changes."
    gh pr checks PR_NUMBER --watch
    gh pr ready PR_NUMBER

Use the number printed by create. Expected: keep draft until every required check passes, then mark ready.

## Self-review result

- Every frozen spec field maps to Tasks 1-7.
- File map, profile, state/results/ledger, adapter, extraction, synthetic and real readiness are explicit.
- H5, H60, production promotion, APIs, deployment, auth, frontend, and OpenClaw remain excluded. Control runs are explicitly deferred to autoresearch-loop.
- Extraction source is latest-report.json and output must match ^-?[0-9]+\.?[0-9]*$.
- Only runtime git SHA and PR number are unknown; exact producing commands and destinations are specified.
