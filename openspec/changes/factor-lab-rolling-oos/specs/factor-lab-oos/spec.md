# Factor-lab rolling out-of-sample specification

## ADDED Requirements

### Requirement: Rolling folds tile the panel without overlap

The `oos` mode SHALL split the panel's sessions into consecutive folds of
`train_sessions` training sessions followed by `test_sessions` test sessions,
each fold advanced by `test_sessions`. Test windows SHALL be consecutive and
non-overlapping and SHALL be anchored to panel sessions. The mode SHALL raise
instead of reporting when the panel holds fewer than one full train+test fold.

#### Scenario: Test windows are consecutive and disjoint

- GIVEN a panel long enough for several folds
- WHEN the `oos` mode plans its folds
- THEN fold `k+1`'s test window starts on the session after fold `k`'s last test
  session
- AND no session belongs to two test windows

#### Scenario: A short panel is refused

- GIVEN a panel shorter than `train_sessions + test_sessions`
- WHEN the `oos` mode runs
- THEN the CLI exits non-zero with an explicit error
- AND no out-of-sample metrics are reported

### Requirement: Fold weights are purged and embargoed

For each fold the composite weights SHALL be estimated only from IC dates `d`
that satisfy `d <= test_start_index - label_horizon - 1 - embargo`, where
`label_horizon` is the weight label's session horizon, the `+1` is the
open-to-open execution lag the existing walk-forward convention already applies,
and `--embargo` (default 0) adds extra spacing. `--embargo` SHALL be zero or a
positive session count; a negative value SHALL be refused before any weights are
frozen because it would move the cutoff later and re-admit overlapping-label
ICs. The mode SHALL report the first and last IC date used and the number of
purged IC dates per fold.

#### Scenario: No fold weight uses an IC at or after the cutoff

- GIVEN any fold
- WHEN its weights are estimated
- THEN every IC date used is at or before
  `test_start_index - label_horizon - 1 - embargo`
- AND the emitted per-fold `ic_cutoff` equals that session

#### Scenario: Embargo widens the purge

- GIVEN the same panel and fold with a larger `--embargo`
- WHEN the fold weights are estimated
- THEN the last IC date used moves earlier by the same number of sessions
- AND more IC dates are reported as purged

#### Scenario: A negative embargo is refused

- GIVEN an `oos` run with `--embargo -1`
- WHEN the CLI parses or runs it
- THEN it exits non-zero with an explicit error naming `--embargo`
- AND no fold weights are estimated with a later cutoff

### Requirement: Fold record states the effective IC history

The per-fold table SHALL distinguish the folder bounds from the weights actually
used: `train_start`/`train_end` SHALL describe the fold, while `ic_first_date`,
`ic_cutoff` and `ic_dates_used` SHALL describe the IC window the weights were
estimated from. The report SHALL carry a note that `--train-sessions` shifts the
folds while `--lookback` caps the IC history.

#### Scenario: Lookback caps the training history

- GIVEN a fold whose train window is longer than `--lookback`
- WHEN the fold row is read
- THEN `ic_dates_used` is at most `--lookback`
- AND `ic_first_date` is later than `train_start`

### Requirement: Mode steps are never ambiguously defaulted

The `oos` book SHALL default to a 60-session step and the label-based modes to a
20-session step. When `oos` is requested together with a label-based mode and no
`--step` is given, the CLI SHALL raise an explicit error instead of silently
running one of the two cadences.

#### Scenario: A mixed run without --step is refused

- GIVEN a run with `--mode oos --mode composite` and no `--step`
- WHEN the CLI runs
- THEN it exits non-zero with an error naming `--step`
- AND no report is produced with an ambiguous cadence

### Requirement: Out-of-sample evaluation is logged as one document

The `oos` mode SHALL evaluate the frozen book on each test window only and
stitch the test-window returns into one out-of-sample curve, reporting total
return, annualised return, max drawdown, annualised turnover and cash share. It
SHALL emit a `config` block recording panel, mode, step, names, components,
lookback, minimum IC dates, label, label lag, train/test sessions, embargo, AUM
and costs, together with a per-fold table carrying `train_start`, `train_end`,
`test_start`, `test_end`, `ic_first_date`, `ic_cutoff`, `purged_ic_dates` and
`test_total_return`. Stdout SHALL remain a single clean JSON payload; the
"existing modes unchanged" guarantee covers the JSON document / `--output`
payload rather than the stdout stream.

#### Scenario: Every attempted configuration is recorded

- GIVEN an `oos` run with non-default knobs
- WHEN its JSON is read
- THEN the `config` block repeats those knobs
- AND the per-fold table has one row per planned fold

#### Scenario: Evaluation does not reuse the training window

- GIVEN a fold
- WHEN its test return is measured
- THEN the account is flat at the fold's first test session
- AND selection inside the test window uses only data up to each decision close

### Requirement: Windowed in-sample arm reports its window

`factor_lab_account_replay.py` SHALL accept `--start-date`/`--end-date` to
restrict only the **evaluated** sessions while factors, ICs and walk-forward
weights still come from the full panel, so a windowed run is the in-sample arm
for a rolling out-of-sample run on the same window. It SHALL echo the requested
`start_date`/`end_date` in its top-level report so a windowed run is
distinguishable from a full run, and SHALL raise when the requested window holds
fewer than two panel sessions.

#### Scenario: A windowed run is identifiable

- GIVEN a run with `--start-date` (and optionally `--end-date`)
- WHEN its report JSON is read
- THEN the top-level `start_date`/`end_date` echo the request
- AND the three variants cover only the requested sessions

#### Scenario: An empty window is refused

- GIVEN a date window that contains fewer than two panel sessions
- WHEN the account replay runs
- THEN it exits non-zero with an explicit error
