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
force mode SHALL retain the authoritative per-code rebuild path. Signal
outcomes SHALL retain the existing runner meanings: `skipped` SHALL cover codes
whose configured signal names are all unevaluable, without failing the run,
while `failed` SHALL remain reserved for genuine errors. Per-signal-name
unevaluability inside a partially evaluable code SHALL be surfaced by the
factory (returned per code and counted by `skipped_signal_count`) without
failing the code or listing it as skipped.

#### Scenario: Stale signal run uses one market batch

- **GIVEN** stale signal mode and a selected code set produced after applying
  market, explicit `--code`, and `--limit` filters
- **WHEN** the signal runner executes
- **THEN** it SHALL call the signal market update path once with exactly that set
- **AND** dry-run SHALL perform zero writes for that same set
- **AND** force mode SHALL retain the authoritative per-code rebuild behavior

#### Scenario: Unevaluable short-history codes are reported as skipped

- **GIVEN** a force or stale signal run whose selected code set includes a code
  without enough factor history for every configured signal name
- **WHEN** the runner reports its result and job-run summary
- **THEN** `skipped_count` and `skipped_codes` SHALL report that code
- **AND** `failed_count` and `failed_codes` SHALL NOT report it
- **AND** a code whose history is insufficient for only some configured signal
  names SHALL remain GOOD and SHALL NOT appear in either `skipped_codes` or
  `failed_codes`, while its evaluable signal names are computed and its skipped
  signal names keep their existing freshness
- **AND** `skipped_signal_count` SHALL count the `(code, signal_name)` pairs
  skipped for insufficient factor history across both fully and partially
  skipped codes (a code skipped for an unsupported capability contributes zero)
- **AND** the job run SHALL finish with status SUCCESS
- **AND** the process SHALL exit non-zero only when `failed_count` is greater
  than zero

### Requirement: Historical Signal Rebuilds May Bypass the Daily Dependency Gate

The signal runner SHALL verify the current day's upstream quote+factor run
before starting, and SHALL only proceed without that verification when an
operator passes an explicit documented bypass flag. The runner SHALL accept the
bypass only for a non-scheduled trigger and only for a force rebuild, so a
scheduled invocation cannot enable it. The bypass SHALL be recorded in the
created job run and logged as a warning. A bypassed run does not establish the
day's signal freshness: because the scoring runner gates on the daily
`signal_daily` SUCCESS record, a bypassed run SHALL NOT satisfy that dependency
on its own, and the bypass SHALL NOT be treated as evidence that the current
day's signals were generated.

#### Scenario: Explicit operator bypass proceeds without the daily record

- **GIVEN** an operator-invoked force signal run that passes the bypass flag for
  a historical rebuild
- **WHEN** no successful upstream quote+factor run exists for the current
  scheduled day
- **THEN** the runner SHALL proceed with the requested signal work instead of
  recording a dependency SKIP
- **AND** it SHALL log a warning that the daily dependency check was bypassed
- **AND** the job-run record SHALL carry `dependency_check_bypassed` set to true
  in its extra metadata and in its summary on every outcome (success, failure,
  or dependency skip)

#### Scenario: Scheduled or incremental runs cannot bypass the gate

- **GIVEN** a signal run started by the daily CronJob
- **WHEN** the daily upstream quote+factor record is absent
- **THEN** the runner SHALL keep recording a dependency SKIP with no signal
  writes
- **AND** the job-run record SHALL carry `dependency_check_bypassed` set to
  false in its extra metadata and summary
- **AND** the runner SHALL reject the bypass flag outright when the trigger is
  scheduled (cron or startup) or when the requested mode is not force

#### Scenario: A bypassed run does not establish the daily signal dependency

- **GIVEN** an operator signal run that passed the bypass flag for a historical
  rebuild
- **WHEN** the scoring runner resolves today's `signal_daily` dependency
- **THEN** that bypassed record SHALL NOT satisfy the dependency, whether it
  finished SUCCESS, RUNNING, or FAILED
- **AND** scoring SHALL remain gated on a non-bypassed daily signal run that
  establishes the current day's signals

#### Scenario: The bypass changes nothing else about the run

- **GIVEN** a signal run that passes the bypass flag
- **WHEN** it executes the requested force rebuild
- **THEN** the bypass SHALL NOT change the selected code set, the mode
  semantics, or the meaning of the pulled, written, skipped, and failed
  counters
- **AND** it SHALL NOT alter the signal documents, `source_freshness`, or
  `generated_at` semantics defined by the existing signal requirements
