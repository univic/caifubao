# Signals Performance Delta

## ADDED Requirements

### Requirement: Incremental Signal Generation with Stable Generated-At

The MA signal factory SHALL compute signals incrementally from the
`DataAssetStatus.latest_data_date` evaluated-through anchor maintained for
each `(code, signal_name)`, and SHALL NOT modify existing signal documents
whose computed values are unchanged. A signal document records a historical
match, while the status anchor records how far evaluation completed even when
no signal matched. `generated_at` and `source_freshness` SHALL be immutable
generation-time snapshots written only on document insert.

#### Scenario: Cross signals computed only after the anchor

- **GIVEN** a stock whose MA10_CROSS_MA20 status was successfully evaluated
  through anchor date A
- **WHEN** the signal runner updates the stock
- **THEN** it SHALL read only factor/quote rows on or after the trading day
  before A (shift(1) lookback)
- **AND** SHALL upsert signal documents only for dates after A
- **AND** the resulting signal set SHALL equal a full-history recompute
  (dates, direction, strength, factor_snapshot)

#### Scenario: State signals evaluate dates after the anchor

- **GIVEN** a state-type signal (PRICE_ABOVE_MA60, MA20_ABOVE_MA60)
- **WHEN** the signal runner updates a stock whose factors are current
- **THEN** it SHALL evaluate and persist every available date after that
  signal's evaluated-through anchor (normally one trading day)
- **AND** SHALL NOT rewrite historical signal rows

#### Scenario: Re-run does not touch unchanged documents

- **GIVEN** signal documents that already exist with identical computed values
- **WHEN** the signal runner runs again for the same stock and date range
- **THEN** existing documents SHALL NOT be modified
- **AND** `generated_at` SHALL remain the original insert time
- **AND** new signal dates SHALL record the current run time in `generated_at`

#### Scenario: Missing anchor falls back to full compute

- **GIVEN** a stock with no evaluated-through status anchor for a signal name
- **WHEN** the signal runner updates the stock
- **THEN** it SHALL compute signals over the full available factor history
  once
- **AND** SHALL record the anchor for subsequent incremental runs

#### Scenario: Successful zero-match evaluation advances the anchor

- **GIVEN** a signal condition that does not match on the latest factor date
- **WHEN** evaluation and persistence complete successfully
- **THEN** no signal document SHALL be created for that date
- **AND** the signal status `latest_data_date` SHALL advance to that factor date
- **AND** its `data_count` SHALL equal the persisted match count

#### Scenario: Failure does not advance signal freshness

- **GIVEN** one or more selected `(code, signal_name)` evaluations
- **WHEN** calculation, signal persistence, or status persistence fails
- **THEN** the affected code SHALL be reported as failed by the runner
- **AND** signal-row failures SHALL NOT advance its evaluated-through date or
  `last_success_at`
- **AND** a partial status bulk failure SHALL fail the job; any status rows
  already committed remain recoverable because the next stale selection
  requires all signal-name anchors to equal the upstream target
- **AND** downstream callers SHALL NOT treat that code as successfully current

#### Scenario: Statuses are derived from committed signal rows

- **GIVEN** a batch of successfully persisted signal results
- **WHEN** signal statuses are refreshed
- **THEN** persisted match counts SHALL be aggregated from MongoDB
- **AND** status records SHALL be written with a bulk upsert
- **AND** status values and reasons SHALL match the single-code status semantics

#### Scenario: Historical correction rebuilds an authoritative range

- **GIVEN** upstream quote or factor values changed on or before an existing
  evaluated-through anchor
- **WHEN** force performs a full-history rebuild
- **THEN** the signal factory SHALL calculate the authoritative match set for
  the complete available factor history
- **AND** SHALL upsert the replacement set before deleting persisted signals
  that no longer match, so an upsert failure cannot first destroy the old set
- **AND** SHALL upsert matching rows by `(stock_code, date, signal_name)`
- **AND** SHALL advance the evaluated-through anchor only after the replacement
  set is fully persisted
