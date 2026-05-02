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
- **AND** verification SHALL include target return, max return, min return, drawdown, quote count, hit target, hit stop loss, and verification time.

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
