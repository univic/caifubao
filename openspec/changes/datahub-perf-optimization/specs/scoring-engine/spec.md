# Scoring Engine Performance Delta

## ADDED Requirements

### Requirement: Batch Score Persistence and Rank Assignment Preserve Results

The scoring engine SHALL fetch per-date market data in batched queries and
persist predictions and rank/percentile updates via bulk writes, producing
results identical to the per-stock path. Score computation for a given
(date, horizon, model_version) SHALL run at most once per environment per day.

#### Scenario: Batch persistence is field-identical

- **GIVEN** the same market data for a date and horizon
- **WHEN** predictions are produced by the batched path
- **THEN** every persisted prediction SHALL be field-identical to the
  per-stock path (score, recommendation, rank, percentile, explanation,
  verification, input_snapshot, model_version)

#### Scenario: Rank assignment via bulk write

- **GIVEN** all non-blocked predictions for a date/horizon/model_version
- **WHEN** ranks and percentiles are assigned
- **THEN** the rank ordering and percentile values SHALL equal the per-document
  save path
- **AND** the update SHALL be performed with bulk `$set` operations limited to
  rank/percentile fields

#### Scenario: Idempotent re-run does not rewrite unchanged predictions

- **GIVEN** predictions already persisted for a date/horizon/model_version
- **WHEN** scoring runs again without `--replace`
- **THEN** unchanged predictions SHALL NOT be recomputed or rewritten
- **AND** rank/percentile updates SHALL be skipped when values are unchanged

#### Scenario: Daily scoring runs at most once per environment

- **GIVEN** the daily pipeline phases (quote-stock job with factors, and the
  scoring cron job)
- **WHEN** scoring for the same (date, horizon, model_version) would execute
  twice in one day
- **THEN** the second execution SHALL skip recomputation and rank rewrites
  for already-scored predictions
- **AND** SHALL still complete job-run tracking with a SUCCESS record

#### Scenario: Verification processes only due predictions

- **GIVEN** predictions in PENDING/TRACKING states
- **WHEN** the verification service runs
- **THEN** it SHALL process only predictions whose target date has passed and
  that have new quotes since their last verification
- **AND** SHALL load only the fields needed for verification
- **AND** SHALL persist status/verification changes via bulk writes
