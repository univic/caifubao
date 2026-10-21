# Datahub runners

## MODIFIED Requirements

### Requirement: Observable Job Runs

Quote and factor refreshes SHALL record observable job status. In addition to
`RUNNING`, `SUCCESS`, `FAILED` and `SKIPPED`, the stock quote and index runners
SHALL be allowed to persist `DEGRADED`. A `DEGRADED` run SHALL remain visible as
not-clean to the health watcher and to `/api/datahub/status`; it SHALL NOT be
counted as a successful pipeline run merely because it continues downstream.

The archived failure and recovery scenarios remain in force:

#### Scenario: Quote refresh completes

- **GIVEN** a quote refresh job runs
- **WHEN** it completes or fails
- **THEN** the system SHALL record status, timing, phase statistics, and error details for freshness diagnosis.

#### Scenario: Quote source is unreachable

- **GIVEN** a quote refresh attempts to update one or more active stocks
- **WHEN** the selected history source returns no valid quote rows within the frozen target range
- **THEN** the runner SHALL fail the quote phase
- **AND** an idempotent replay that validates existing rows SHALL NOT fail merely because it inserts zero new documents
- **AND** SHALL NOT continue to factor, signal, or scoring phases as if the refresh succeeded.

#### Scenario: Spot source returns no stock universe

- **GIVEN** an empty database is being initialized
- **WHEN** the stock spot source returns an empty universe
- **THEN** the runner SHALL fail the quote phase instead of reporting a successful zero-stock bootstrap.

#### Scenario: A listed stock is temporarily suspended

- **GIVEN** a supported stock remains present in the spot universe with a zero close price
- **WHEN** its history legitimately has no row for the frozen `as_of_date`
- **THEN** the runner SHALL retain the stock as active
- **AND** SHALL preserve its stale freshness state for diagnosis
- **AND** SHALL NOT fail the whole market refresh solely because the stock is suspended.

#### Scenario: A bounded partial refresh is degraded, not failed

- **GIVEN** a stock quote run within the declared failure cap whose coverage meets the declared threshold
- **WHEN** the run finishes
- **THEN** the runner SHALL persist `DEGRADED` with the failed codes and coverage
- **AND** the run SHALL NOT be treated as `SUCCESS`
- **AND** an outage with hundreds of failures SHALL still persist `FAILED`.
