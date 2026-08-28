## ADDED Requirements

### Requirement: Multi-horizon Score Predictions

The scoring system SHALL generate horizon-specific stock predictions for `Score5`, `Score20`, and `Score60`.

#### Scenario: Daily scoring runs

- **GIVEN** quote, factor, signal, freshness, and trading-calendar inputs exist
- **WHEN** datahub scores active A-share stocks for an evaluation date
- **THEN** it SHALL write one prediction per stock, date, horizon, and model version
- **AND** each prediction SHALL include score, rank, percentile, recommendation, base price, target date, status, explanation, input snapshot, verification, model version, generated time, and updated time.

### Requirement: Horizon-specific Scoring Logic

The three score horizons SHALL use distinct horizon semantics and configurable weights.

#### Scenario: Score5 is calculated

- **GIVEN** short-term signal, momentum, trend, range-position, and risk inputs exist
- **WHEN** `Score5` is calculated
- **THEN** the score SHALL emphasize near-term bullish signals, 3-5 trading-day momentum, short breakout or range position, and short-term risk penalties.

#### Scenario: Score20 is calculated

- **GIVEN** swing-trading inputs exist
- **WHEN** `Score20` is calculated
- **THEN** the score SHALL emphasize MA20/MA60 trend structure, pullback or continuation quality, relative strength, and medium volatility risk.

#### Scenario: Score60 is calculated

- **GIVEN** medium-term trend inputs exist
- **WHEN** `Score60` is calculated
- **THEN** the score SHALL emphasize MA60/MA120 structure, stability, drawdown behavior, persistent relative strength, and future fundamental placeholders when reliable inputs exist.

### Requirement: Structured Score Explanation

Each score prediction SHALL store machine-readable explanation data.

#### Scenario: Frontend inspects a prediction

- **GIVEN** a score prediction exists
- **WHEN** the backend returns its explanation
- **THEN** the explanation SHALL include summary, horizon, score, additive components, penalties, thresholds, and model version
- **AND** each component SHALL include id, group, weight, contribution, direction, and evidence when available.

### Requirement: Input Availability Status

Scoring SHALL report missing or incomplete inputs explicitly.

#### Scenario: Required quote is missing

- **GIVEN** a stock has no quote on the evaluation date
- **WHEN** scoring runs for that stock
- **THEN** the prediction SHALL be `BLOCKED`
- **AND** the input snapshot SHALL identify the missing quote instead of silently producing a normal zero score.

### Requirement: Closed-loop Verification

Score predictions SHALL be verified against future quote outcomes for their horizon.

#### Scenario: Horizon window is complete

- **GIVEN** a prediction has enough future quote records for its horizon
- **WHEN** verification runs
- **THEN** the prediction SHALL become `VERIFIED`
- **AND** verification SHALL include target return, max return, min return, drawdown, quote count, hit target (dual-metric), hit stop loss, and verification time.

#### Scenario: Hit target is measured conservatively

- **GIVEN** a verified prediction with future quote data
- **WHEN** verification computes hit-target flags
- **THEN** it SHALL report `hit_target_close` (whether `return_at_target >= effective_threshold`, the conservative metric for strategy development)
- **AND** it SHALL report `hit_target_intra` (whether `max_return >= effective_threshold`, the aggressive metric for reference only)
- **AND** calibration reports and score-quality monitoring SHALL use `hit_target_close` as the primary success criterion.

#### Scenario: Horizon date passed with incomplete data

- **GIVEN** the target date has passed but future quotes are incomplete
- **WHEN** verification runs
- **THEN** the prediction SHALL become `INSUFFICIENT_DATA`.

### Requirement: Historical Replay and Calibration

The scoring system SHALL support historical replay and calibration without look-ahead bias.

#### Scenario: Historical scoring is replayed

- **GIVEN** historical quote, factor, and signal data exists
- **WHEN** a replay backfills predictions for a date range
- **THEN** scoring SHALL only read inputs available at or before each evaluation date
- **AND** SHALL NOT trigger quote, factor, or signal collection.

#### Scenario: Calibration report is generated

- **GIVEN** verified predictions exist for a horizon and model version
- **WHEN** a calibration report is generated
- **THEN** it SHALL summarize count, average score, target return, max return, drawdown, hit rate, stop-loss rate, score buckets, daily Top-N groups, component groups, false positives, and false negatives.

### Requirement: Score Experiment Records

The system SHALL store score experiments for comparing factor configurations and model versions.

#### Scenario: Researcher creates an experiment

- **GIVEN** a candidate model version, optional baseline version, date range, horizons, and factor config
- **WHEN** the experiment is created
- **THEN** the backend SHALL store the experiment with status, config, report snapshot, timestamps, and any error message.

#### Scenario: Experiment report is rebuilt

- **GIVEN** verified score predictions exist for an experiment
- **WHEN** the report is run
- **THEN** the backend SHALL aggregate horizon-level overall metrics, score buckets, Top-N groups, component metrics, false positives, false negatives, baseline metrics, and comparison deltas.

### Requirement: Datahub Experiment Replay

Datahub SHALL be able to replay a stored score experiment using its saved factor configuration.

#### Scenario: Operator runs a stored experiment

- **GIVEN** a `ScoreExperiment` exists
- **WHEN** an operator runs `python -m app.jobs.scoring_runner experiment --id <id>`
- **THEN** datahub SHALL load the experiment config
- **AND** apply horizon-specific scoring overrides
- **AND** backfill predictions under the experiment model version
- **AND** verify predictions and write reports back to the experiment when not in dry-run mode.

### Requirement: Automated Grid Experiment Generation

Datahub SHALL support automated batch creation of score experiments from
parameter grids to enable systematic exploration of factor weight and
threshold configurations.

#### Scenario: Operator generates experiments from weight and threshold grids

- **GIVEN** a weight grid (e.g., `"momentum": [20, 25]`) and a threshold grid
  (e.g., `"buy_threshold": [60, 70]`)
- **WHEN** the grid-search CLI command is invoked with a date range and
  configuration prefix
- **THEN** datahub SHALL create one `ScoreExperiment` per Cartesian product
  combination of weight values × threshold values per horizon
- **AND** each experiment SHALL receive a unique `model_version` derived from
  the naming prefix and a combination suffix
- **AND** the operator SHALL be able to preview generated experiments via
  `--dry-run` without writing to the database
- **AND** experiments SHALL be replayable through the existing
  `scoring_runner experiment --id <id>` command.

### Requirement: Score Read APIs

The backend SHALL expose score predictions through stable read APIs.

#### Scenario: Frontend requests score ranking

- **GIVEN** predictions exist for a date and horizon
- **WHEN** the frontend requests `/api/scores`
- **THEN** the backend SHALL return ranked score items with horizon, score, recommendation, status, verification, and model version.

#### Scenario: Frontend requests score explanation

- **GIVEN** a prediction exists for stock, date, horizon, and model version
- **WHEN** the frontend requests its explanation endpoint
- **THEN** the backend SHALL return structured explanation and input snapshot.

### Requirement: Score Research Frontend

The frontend SHALL provide a research page for score experiments.

#### Scenario: Researcher reviews experiment output

- **GIVEN** score experiment reports exist
- **WHEN** the researcher opens the score experiment page
- **THEN** the page SHALL show experiment creation controls, saved experiments, overall metrics, baseline deltas, score-bucket performance, Top-N performance, and component-level performance by horizon.

### Requirement: Market Comprehensive Score Summary

The market comprehensive API SHALL include multi-horizon score summaries.

#### Scenario: Market board requests ranked assets

- **GIVEN** score predictions exist for Score5, Score20, and Score60
- **WHEN** `/api/market/comprehensive` is requested with a selected horizon
- **THEN** the response SHALL use the selected horizon for display ranking
- **AND** include score summaries for all three horizons when available.

### Requirement: Score Distribution Calibration

The scoring system SHALL support distribution calibration to prevent
overly conservative or aggressive score ranges that produce too few
actionable signals. Calibration SHALL use full-market statistics, not
single-stock observations.

#### Scenario: Full-market distribution is used for calibration

- **GIVEN** score predictions exist for all active A-share stocks over
  a date range for a given horizon and model version
- **WHEN** a calibration analysis is requested
- **THEN** the system SHALL compute the full-market score distribution
  (min, P5, P25, median, P75, P95, max) and recommendation distribution
  (BUY/WATCH/NONE/AVOID counts) per evaluation date
- **AND** generate a recommendation: if BUY rate is consistently < 3%
  across the market, suggest lowering the buy threshold; if AVOID rate
  is consistently > 50%, suggest adjusting component weights
- **AND** calibration recommendations SHALL reference the full-market
  data, not a single stock's outcome.

#### Scenario: Thresholds support hybrid percentile + absolute scoring

- **GIVEN** calibration analysis indicates the raw score distribution
  is skewed
- **WHEN** recommendation thresholds are configured
- **THEN** the system SHALL support a hybrid mode where a stock is
  recommended BUY only when both conditions are met: (a) raw score >=
  a configurable absolute threshold, AND (b) the stock's percentile
  rank within its date/horizon cohort >= a configurable percentile
  threshold (e.g., >= 85th percentile)
- **AND** the hybrid thresholds SHALL be configurable per horizon and
  per model version.

#### Scenario: Threshold change requires new model version and full-market replay

- **GIVEN** a proposed change to buy_threshold or watch_threshold
- **WHEN** the change is implemented
- **THEN** the DEFAULT_MODEL_VERSION SHALL be incremented
- **AND** the new model SHALL be validated by backfilling scores for
  the full market (not a single stock) over a representative date range
- **AND** the calibration report for the new model version SHALL be
  compared against the previous model version's calibration report
- **AND** the threshold change SHALL NOT be accepted solely on the basis
  of single-stock backtest results.

#### Scenario: Score distribution metrics are reported

- **GIVEN** a calibration report is generated for a date range
- **WHEN** the report is assembled
- **THEN** it SHALL include score distribution statistics (min, P25, median,
  P75, max, recommendation counts) per horizon
- **AND** flag when BUY rate < 3% or AVOID rate > 50% as potentially
  miscalibrated
- **AND** report the suggested hybrid threshold values based on the
  observed distribution.

### Requirement: Signal Persistence Decay

The scoring system SHALL prevent abrupt score drops when a bullish
signal temporarily disappears by applying a persistence decay model.

#### Scenario: Bullish signal disappears for one day

- **GIVEN** a stock had `MA20_ABOVE_MA60` signal yesterday but not today
- **WHEN** the signal_strength component is calculated
- **THEN** the contribution SHALL decay gradually (e.g., exponential decay
  factor 0.7 per day since last signal) instead of instantly dropping to 0
- **AND** decay SHALL be bounded by a configurable `signal_decay_max_days`
  (default 5) beyond which the contribution reaches zero
- **AND** the decay behavior SHALL be configurable per horizon.

#### Scenario: Signal reappears before decay completes

- **GIVEN** signal persistence is active and a signal reappears within
  the decay window
- **WHEN** the signal_strength component is recalculated
- **THEN** the contribution SHALL reset to the full weighted value based
  on the current signal
- **AND** SHALL NOT double-count decay and live signal contributions.

### Requirement: Component Contribution and Factor Effectiveness

The scoring system SHALL support attribution of trading outcomes to specific
score components so that researchers can identify which factors drive
profitability and which are noise.

#### Scenario: Component contribution is computed for a backtest trade

- **GIVEN** a backtest trade was entered on a score-driven signal
- **WHEN** component contribution analysis is requested
- **THEN** the system SHALL retrieve the explanation at entry and exit
- **AND** compute the score contribution delta for each component between
  entry and exit
- **AND** rank components by absolute contribution change to identify
  which factor drove the exit signal.

#### Scenario: Component win rate is aggregated across trades

- **GIVEN** a set of trades all entered when a specific component
  (e.g., momentum) had the highest contribution among all components
- **WHEN** win rate by component is computed
- **THEN** the system SHALL compute separate win rate, average P&L, and
  average hold duration for trades dominated by each component
- **AND** flag components with win rate significantly below the average
  across all components as candidates for weight reduction or removal.

#### Scenario: Factor predictive power is quantified

- **GIVEN** a scoring component or external factor has been computed
  historically for all stocks
- **WHEN** factor evaluation is requested
- **THEN** the system SHALL compute the rank IC between factor values
  and forward horizon returns on each evaluation date
- **AND** return mean IC, ICIR, and the percentage of dates with positive IC
- **AND** SHALL NOT use future data when computing forward returns
  (look-ahead bias prevention).

#### Scenario: New factor can be evaluated before integration

- **GIVEN** a candidate factor (e.g., volume_ratio, RSI) has been
  computed and stored
- **WHEN** a researcher requests a factor evaluation before adding it
  to the scoring model
- **THEN** the system SHALL compute the factor's standalone IC, ICIR,
  and correlation with existing scoring components
- **AND** flag if the new factor is highly correlated (>0.7) with an
  existing component
- **AND** support a side-by-side comparison of the scoring model with
  and without the candidate factor.
