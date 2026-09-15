# Stock Scoring Replay Point-in-Time Guards

## MODIFIED Requirements

### Requirement: Historical Replay and Calibration

The scoring system SHALL support historical replay and calibration without look-ahead bias. Every input that carries a point-in-time anchor SHALL be attributed to an evaluation date only when the input's own anchor places it on or before that date. In particular, a stock's current industry classification SHALL be usable for evaluation date `D` only when its `assigned_at` is on or before `D` and no `industry_change_log` entry post-dates `D`; otherwise the `industry_momentum` component SHALL use its neutral fallback for that stock, and no past-dated `industry_daily_metrics` row SHALL be built from that classification.

#### Scenario: Historical scoring is replayed

- **GIVEN** historical quote, factor, and signal data exists
- **WHEN** a replay backfills predictions for a date range
- **THEN** scoring SHALL only read inputs available at or before each evaluation date
- **AND** SHALL NOT trigger quote, factor, or signal collection

#### Scenario: Replayed scoring does not use a later industry classification

- **GIVEN** a stock's current classification was assigned after evaluation date `D`, or its change history records a change after `D`
- **WHEN** replay scores `D`
- **THEN** `industry_momentum` SHALL use the neutral fallback for that stock
- **AND** the per-day industry prefetch SHALL omit that classification
- **AND** aggregation SHALL NOT create a `D`-dated industry metric from it

#### Scenario: Calibration report is generated

- **GIVEN** verified predictions exist for a horizon and model version
- **WHEN** a calibration report is generated
- **THEN** it SHALL summarize count, average score, target return, max return, drawdown, hit rate, stop-loss rate, score buckets, daily Top-N groups, component groups, false positives, and false negatives
