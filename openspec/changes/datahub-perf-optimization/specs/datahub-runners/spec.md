# Datahub Runners Performance Delta

## ADDED Requirements

### Requirement: Watermark-Based Incremental Prod-to-Dev Sync

The data sync engine SHALL default to incremental synchronization: for each
date-partitioned collection it SHALL copy only documents whose date is on or
after the recorded per-collection watermark, and SHALL advance the watermark
only after a successful run. Full-collection sync SHALL require an explicit
opt-in flag.

#### Scenario: Default run is incremental

- **GIVEN** a collection with a recorded watermark date W from a previous
  successful sync
- **WHEN** the data sync runner executes without an explicit date range
- **THEN** it SHALL read only source documents with `date >= W`
- **AND** SHALL upsert them idempotently by `_id`
- **AND** SHALL advance the watermark to the maximum synced date on success

#### Scenario: Full sync requires explicit opt-in

- **GIVEN** any collection state
- **WHEN** a full-collection sync is requested without an explicit date range
  and without the allow-full-sync flag
- **THEN** the runner SHALL refuse the run with a clear error
- **AND** SHALL NOT read or write the collection

#### Scenario: Dry run does not advance the watermark

- **GIVEN** a dry-run sync request
- **WHEN** the runner completes reading and (skipped) writing
- **THEN** the recorded watermark SHALL remain unchanged

#### Scenario: Cold start falls back to full sync

- **GIVEN** a collection with no recorded watermark
- **WHEN** the runner executes
- **THEN** it SHALL treat the run as a full sync
- **AND** the runbook-recommended path for the initial bulk load remains
  `mongodump`/`mongorestore`

#### Scenario: Dev-only signal collection rule is unchanged

- **GIVEN** a non-dev environment
- **WHEN** collections are resolved for sync
- **THEN** `stock_signal_daily` SHALL still be skipped outside dev
- **AND** small snapshot collections (`finance_market`, `stock_industry`)
  SHALL continue to sync in full

### Requirement: Stale MA Factor Runs Preserve the Selected Set

The factor runner SHALL route stale MA updates through the market batch path
without expanding or changing the code set selected by runner arguments.

#### Scenario: Stale MA run uses one market batch

- **GIVEN** stale MA mode and a selected code set produced after applying
  market, explicit `--code`, and `--limit` filters
- **WHEN** the factor runner executes
- **THEN** it SHALL call the MA market update path once with exactly that set
- **AND** pulled, written, skipped, failed, and failed-code results SHALL retain
  the existing runner meanings

#### Scenario: Dry-run and force behavior remain unchanged

- **GIVEN** MA factor runner arguments
- **WHEN** `--dry-run` is selected
- **THEN** it SHALL report the same selected set and perform zero writes
- **AND WHEN** force mode is selected
- **THEN** it SHALL retain the existing per-code full-recompute behavior

### Requirement: Stale Signal Runs Preserve the Selected Set

The signal runner SHALL route stale MA-signal updates through one market batch
without expanding the code set selected by market, explicit `--code`, or
`--limit` arguments. Dry-run SHALL report that same set without writes, while
force mode SHALL retain the authoritative per-code rebuild path.

#### Scenario: Stale signal run uses one market batch

- **GIVEN** stale signal mode and a selected code set produced after applying
  market, explicit `--code`, and `--limit` filters
- **WHEN** the signal runner executes
- **THEN** it SHALL call the signal market update path once with exactly that set
- **AND** dry-run SHALL perform zero writes for that same set
- **AND** force mode SHALL retain the authoritative per-code rebuild behavior
