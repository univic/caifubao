# Strategy Paper Runner

## ADDED Requirements

### Requirement: Versioned strategy config drives selection semantics

A strategy configuration MUST be versioned and reproducible, and MUST declare:
the score source `model_version` (default: a flip_wide-direction shadow version
such as `flip_wide_shadow_v1`, configurable to any registered version), the
selection rule (`top_percentile` wide book with lower/upper bounds and a
portfolio-size cap), portfolio constraints, and the rebalance schedule.
Selection semantics are always "buy high" — direction is fixed at the scoring
construction layer, never re-implemented in the strategy layer. The normalized
config MUST include `timing_version=paper_causal_v1`; its complete contents,
including `initial_nav`, MUST be hashed and reused when resolving holdings or
NAV.

#### Scenario: Default config selects the flip_wide wide book

- GIVEN the default strategy config (model_version = flip_wide shadow, selection
  = top_percentile lower 0.20 upper 1.00 portfolio_size 800, weekly rebalance,
  equal weight)
- WHEN the paper runner selects a daily target portfolio from usable scores
  (`PENDING`, `TRACKING`, `VERIFIED`, or `INSUFFICIENT_DATA`)
- THEN the portfolio holds the top-percentile wide book capped at the configured
  size
- AND every selected stock satisfies the eligibility constraints

#### Scenario: Score source must be explicit

- GIVEN a strategy config without a `score_model_version`
- WHEN the config is validated
- THEN validation raises
- AND no strategy run silently defaults to any score source (a typo can never
  run the wrong version)

#### Scenario: Unknown config keys are rejected at every level

- GIVEN a strategy config with an unknown key inside `selection`,
  `constraints`, or `rebalance`
- WHEN the config is validated
- THEN validation raises with the offending block named
- AND the typo is never absorbed into a default and hashed into a reproducible
  but wrong configuration

#### Scenario: Any registered model version can be the score source

- GIVEN a strategy config naming a different registered model version
- WHEN the paper runner runs
- THEN scores are read for that model_version only (no mixing across versions)

### Requirement: Paper runner records targets and rebalance, never trades

The daily strategy runner MUST read usable score predictions for its score
source. `PENDING`, `TRACKING`, `VERIFIED`, and `INSUFFICIENT_DATA` are usable;
`BLOCKED` and `FAILED` are excluded, and later outcome verification MUST NOT be
a ranking input. The runner MUST apply selection + constraints and persist the
target portfolio and rebalance list (what changed since the previous portfolio).
Each decision MUST persist the signal date, actual UTC `decision_at`, and the
strictly later next market-calendar-session `execution_date`. In paper mode it
MUST NOT place real orders or mutate any account/execution state. It MUST record
a strategy-freshness job run (same `datahub_job_runs` pattern as the other data
layers) so each output is traceable to date/model_version/config.

#### Scenario: Daily run persists target portfolio

- GIVEN a trading date with usable predictions for the configured model version
- WHEN the paper runner runs for that date
- THEN a target-portfolio record is persisted with date, model_version,
  config_hash, holdings, and selection summary

- AND the record contains actual UTC `decision_at`, the next calendar-session
  `execution_date`, and `evidence_kind=REPLAY`
- AND its normalized config pins `timing_version=paper_causal_v1`
- AND a freshness run record marks the layer fresh for that date

#### Scenario: Rebalance list reflects changes vs previous portfolio

- GIVEN a previous target portfolio from the prior rebalance date
- WHEN a later run produces a new target portfolio
- THEN the persisted rebalance list contains only added and removed stock codes

#### Scenario: No usable scores means the run is skipped, not empty

- GIVEN a date with no `PENDING`, `TRACKING`, `VERIFIED`, or
  `INSUFFICIENT_DATA` predictions for the configured model version
- WHEN the paper runner runs
- THEN the run is marked skipped with the reason recorded, and no target
  portfolio is written

- AND `BLOCKED` and `FAILED` predictions are never treated as usable scores

### Requirement: Paper NAV simulation uses realistic T+1 cost semantics

Paper NAV MUST simulate entry at the next market-calendar-session open after a
signal decision, per-side commission (with minimum), sell stamp duty, slippage,
board-lot rounding, and suspension roll-forward, using the same execution
parameters as the autoresearch profile (commission_rate 0.00025,
minimum_commission_cny 5.0, sell_stamp_duty_rate 0.001, slippage_per_side
0.001). Opening order sizing and turnover denominators MUST use only the
execution-day open or prior known marks; same-day closes are available only for
post-trade marking. Suspended or unknown statuses MUST NOT be converted to
tradable. NAV, drawdown, daily return, and turnover are computed per rebalance
cycle; the same-date tradable-universe equal-weight return is a diagnostic
baseline whose daily alignment remains subject to a separate validation slice.

#### Scenario: Suspended names hold last observed valuation

- GIVEN a held name that suspends after a trading day with a known close
- WHEN paper NAV is computed for the suspension day (no executable quote)
- THEN the name is valued at its last observed close, not its entry price
- AND no forced mark or valuation failure occurs
- AND an unknown or suspended status is not converted to tradable

#### Scenario: NAV marks to market with costs

- GIVEN a target portfolio and a subsequent price series
- WHEN paper NAV is computed
- THEN realized trades deduct commission/slippage/stamp duty
- AND the target decision executes on its `execution_date`, with suspended names
  rolling forward to the next executable open instead of failing the valuation
- AND per-cycle turnover (buy + sell notional / pre-cycle NAV) is reported

#### Scenario: Equity curve is comparable to equal-weight baseline

- GIVEN a paper NAV series over ≥2 rebalance cycles
- WHEN compared to the same-date equal-weight benchmark
- THEN the strategy equity curve and the benchmark curve are both recorded
  with per-date values

#### Scenario: NAV curve recomputes from persisted runs

- GIVEN a range with ≥1 COMPLETED StrategyPaperRun carrying target holdings
- WHEN the operator runs `strategy_runner nav --from <date> --to <date>`
- THEN the operator supplies the exact full strategy config used by those runs,
  including `initial_nav`, and the matching `config_hash` is selected
- AND the runs are sorted by signal date while execution dates form the
  rebalance schedule
- AND quote prices (open/close/trade_status) plus the diagnostic equal-weight
  benchmark are loaded through the last selected `execution_date`
- AND `simulate_paper_nav` runs with realistic T+1 costs
- AND each curve point (nav/daily_return/turnover/drawdown/benchmark_return) is
  attached to its originating signal record and written into that run's
  `nav_snapshot`
- AND only the same `timing_version=paper_causal_v1` track is used

#### Scenario: NAV recompute with no runs is reported, not crashed

- GIVEN a range with no COMPLETED runs for the configured model version
- WHEN the operator runs `strategy_runner nav --from <date> --to <date>`
- THEN the command returns a no-runs result with a reason
- AND nothing is written
- AND a range whose held codes have no quote rows also returns a reason
  instead of writing a flat cash nav_snapshot

### Requirement: Retrospective records cannot certify forward evidence

The first causal-timing slice MUST mark every newly created strategy record and
NAV recomputation as `evidence_kind=REPLAY`, including current-date runs,
historical backfills, replacements, and successful jobs. Replay output MUST NOT
count toward the 120-session forward gate. The forward capture and evidence
counter are a separate later change; `datahub_job_runs` `SUCCESS` is job health,
not forward evidence.

#### Scenario: Historical replay succeeds

- WHEN an operator backfills a historical signal date and computes its NAV
- THEN the records remain `evidence_kind=REPLAY`
- AND neither the run nor its `SUCCESS` job record advances the forward-day count

## Non-goals

- No real order execution, broker integration, account mutation, forward-capture
  counter, or model promotion. Any future execution or forward evidence gate
  requires a separate change, explicit authorization, and validation.
- No changes to scoring math, DEFAULT_MODEL_VERSION, or the decisions APIs.
- No valuation-factor blending (research #178 ruled it out for flip_wide).
- The backend manual Portfolio model is user bookkeeping, not the paper artifact.
