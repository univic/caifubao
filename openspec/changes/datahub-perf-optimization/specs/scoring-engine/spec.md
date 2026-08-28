# Scoring Engine Performance Delta

## ADDED Requirements

### Requirement: Batch Score Persistence and Rank Assignment Preserve Results

The scoring engine SHALL preserve the existing score, rank, percentile, and
recommendation results while avoiding repeated work for a complete cohort.
A cohort is identified by `(date, horizon, model_version, scoring_mode)` and
its expected stock-code set is frozen from `active_status=0` at run start.
Rank and recommendation finalization SHALL be restricted to that frozen code
set; persisted rows outside it SHALL remain untouched and SHALL NOT affect the
cohort.
The model version SHALL identify a single scoring configuration; rows whose
stored input snapshot does not match the requested raw/ranked mode SHALL NOT
qualify for a fast skip.

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
- **AND** only rows whose rank or percentile actually differs SHALL produce an
  update operation
- **AND** an empty delta SHALL NOT call `bulk_write`
- **AND** BLOCKED rows SHALL neither receive nor affect ranks
- **AND** equal scores SHALL use ascending `stock_code` as a deterministic
  secondary order while retaining sequential 1-based ranks

#### Scenario: Complete daily cohort is skipped before component reads

- **GIVEN** every frozen active stock code has exactly one prediction for the
  requested date, horizon, model version, and scoring mode
- **AND** every non-BLOCKED prediction has non-null rank and percentile values
  equal to the existing `-score` order and percentile formula
- **AND** every non-BLOCKED recommendation equals the current hybrid
  recommendation for its score and percentile
- **AND** every BLOCKED prediction has null rank/percentile and recommendation
  `NONE`
- **WHEN** scoring runs with `replace=False` and `dry_run=False`
- **THEN** that horizon SHALL return before per-stock quote, factor, signal, or
  component reads
- **AND** prediction, rank, and recommendation writes SHALL all be zero
- **AND** the result SHALL report the completed horizon as skipped
- **AND** raw-mode industry aggregation SHALL retain its existing retry path
- **AND** the job SHALL still complete successfully

#### Scenario: Partial cohort is repaired

- **GIVEN** one or more frozen active stock codes lack a prediction, rank,
  percentile, or finalized recommendation
- **WHEN** scoring runs with `replace=False`
- **THEN** missing predictions SHALL be produced without overwriting existing
  prediction business fields
- **AND** ranked mode SHALL still compute the complete cross-sectional raw
  cohort before producing a missing score
- **AND** ranked rows SHALL carry a fingerprint of the frozen stock-code set;
  a membership change or legacy row without that fingerprint SHALL require an
  explicit `replace` instead of mixing scores from different cross sections
- **AND** ranks and recommendations SHALL be finalized for the complete stored
  non-BLOCKED cohort
- **AND** the resulting score/rank/percentile/recommendation values SHALL equal
  a single complete run over the same frozen inputs
- **AND** legacy BLOCKED rank/percentile/recommendation values SHALL be repaired
  with changed-only updates before the cohort can qualify for a fast skip

#### Scenario: Horizons are gated independently

- **GIVEN** one requested horizon is complete and another is partial
- **WHEN** the daily scoring run executes
- **THEN** the complete horizon SHALL be skipped
- **AND** the partial horizon SHALL be repaired and finalized

#### Scenario: Explicit recomputation and preview bypass the gate

- **GIVEN** a complete stored cohort
- **WHEN** `replace=True` or `dry_run=True` is requested
- **THEN** the complete-cohort fast skip SHALL NOT change the existing
  recomputation or preview semantics

#### Scenario: Daily scoring runs at most once per environment

- **GIVEN** the daily pipeline phases (quote-stock job with factors, and the
  scoring cron job)
- **WHEN** scoring for the same (date, horizon, model_version) would execute
  twice in one day
- **THEN** the second execution SHALL skip recomputation and rank rewrites
  for already-scored predictions
- **AND** SHALL still complete job-run tracking with a SUCCESS record

#### Scenario: Finalization failures fail closed

- **GIVEN** a partial cohort requiring rank or recommendation finalization
- **WHEN** either bulk update fails
- **THEN** the horizon and job SHALL fail
- **AND** recommendation finalization SHALL NOT run after a rank failure
- **AND** a later run SHALL be able to repair any partially committed updates

#### Scenario: Verification processes only due predictions

- **GIVEN** predictions in PENDING/TRACKING states
- **WHEN** the verification service runs
- **THEN** it SHALL process only predictions whose target date has passed and
  that have new quotes since their last verification
- **AND** SHALL load only the fields needed for verification
- **AND** SHALL persist status/verification changes via bulk writes
