# Strategy Paper Runner

## ADDED Requirements

### Requirement: Versioned strategy config drives selection semantics

A strategy configuration MUST be versioned and reproducible, and MUST declare:
the score source `model_version` (default: a flip_wide-direction shadow version
such as `flip_wide_shadow_v1`, configurable to any registered version), the
selection rule (`top_percentile` wide book with lower/upper bounds and a
portfolio-size cap), portfolio constraints, and the rebalance schedule.
Selection semantics are always "buy high" — direction is fixed at the scoring
construction layer, never re-implemented in the strategy layer.

#### Scenario: Default config selects the flip_wide wide book

- GIVEN the default strategy config (model_version = flip_wide shadow, selection
  = top_percentile lower 0.20 upper 1.00 portfolio_size 800, weekly rebalance,
  equal weight)
- WHEN the paper runner selects a daily target portfolio from VERIFIED scores
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

The daily strategy runner MUST read VERIFIED score predictions for its score
source, apply selection + constraints, and persist the target portfolio and the
rebalance list (what changed since the previous portfolio). In paper mode it
MUST NOT place real orders or mutate any account/execution state. It MUST
record a strategy-freshness job run (same `datahub_job_runs` pattern as the
other data layers) so each output is traceable to date/model_version/config.

#### Scenario: Daily run persists target portfolio

- GIVEN a trading date with VERIFIED predictions for the configured model version
- WHEN the paper runner runs for that date
- THEN a target-portfolio record is persisted with date, model_version,
  config_hash, holdings, and selection summary
- AND a freshness run record marks the layer fresh for that date

#### Scenario: Rebalance list reflects changes vs previous portfolio

- GIVEN a previous target portfolio from the prior rebalance date
- WHEN a later run produces a new target portfolio
- THEN the persisted rebalance list contains only added and removed stock codes

#### Scenario: No VERIFIED scores means the run is skipped, not empty

- GIVEN a date with no VERIFIED predictions for the configured model version
- WHEN the paper runner runs
- THEN the run is marked skipped with the reason recorded, and no target
  portfolio is written

### Requirement: Paper NAV simulation uses realistic T+1 cost semantics

Paper NAV MUST simulate entry at next-trading-day open, per-side commission
(with minimum), sell stamp duty, slippage, board-lot rounding, and suspension
roll-forward, using the same execution parameters as the autoresearch profile
(commission_rate 0.00025, minimum_commission_cny 5.0, sell_stamp_duty_rate
0.001, slippage_per_side 0.001). NAV, drawdown, daily return, and turnover are
computed per rebalance cycle; the baseline is the same-date tradable-universe
equal-weight return.

#### Scenario: Suspended names hold last observed valuation

- GIVEN a held name that suspends after a trading day with a known close
- WHEN paper NAV is computed for the suspension day (no quote available)
- THEN the name is valued at its last observed close, not its entry price
- AND no forced mark or valuation failure occurs

#### Scenario: NAV marks to market with costs

- GIVEN a target portfolio and a subsequent price series
- WHEN paper NAV is computed
- THEN realized trades deduct commission/slippage/stamp duty
- AND suspended names roll forward to the next executable open instead of
  failing the valuation
- AND per-cycle turnover (buy + sell notional / pre-cycle NAV) is reported

#### Scenario: Equity curve is comparable to equal-weight baseline

- GIVEN a paper NAV series over ≥2 rebalance cycles
- WHEN compared to the same-date equal-weight benchmark
- THEN the strategy equity curve and the benchmark curve are both recorded
  with per-date values

## Non-goals

- No real order execution, broker integration, or account mutation (paper-only;
  a ≥120-trading-day T+1 track is required before any such change).
- No changes to scoring math, DEFAULT_MODEL_VERSION, or the decisions APIs.
- No valuation-factor blending (research #178 ruled it out for flip_wide).
- The backend manual Portfolio model is user bookkeeping, not the paper artifact.
