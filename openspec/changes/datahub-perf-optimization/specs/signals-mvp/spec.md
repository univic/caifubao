# Signals Performance Delta

## ADDED Requirements

### Requirement: Incremental Signal Generation with Stable Generated-At

The MA signal factory SHALL compute signals incrementally per stock anchor
date, and SHALL NOT modify existing signal documents whose computed values
are unchanged. `generated_at` SHALL be written only on document insert.

#### Scenario: Cross signals computed only after the anchor

- **GIVEN** a stock with existing MA10_CROSS_MA20 signals up to anchor date A
- **WHEN** the signal runner updates the stock
- **THEN** it SHALL read only factor/quote rows on or after the trading day
  before A (shift(1) lookback)
- **AND** SHALL upsert signal documents only for dates after A
- **AND** the resulting signal set SHALL equal a full-history recompute
  (dates, direction, strength, factor_snapshot)

#### Scenario: State signals evaluate the latest trading day

- **GIVEN** a state-type signal (PRICE_ABOVE_MA60, MA20_ABOVE_MA60)
- **WHEN** the signal runner updates a stock whose factors are current
- **THEN** it SHALL evaluate and persist only the latest trading day's state
- **AND** SHALL NOT rewrite historical signal rows

#### Scenario: Re-run does not touch unchanged documents

- **GIVEN** signal documents that already exist with identical computed values
- **WHEN** the signal runner runs again for the same stock and date range
- **THEN** existing documents SHALL NOT be modified
- **AND** `generated_at` SHALL remain the original insert time
- **AND** new signal dates SHALL record the current run time in `generated_at`

#### Scenario: Missing anchor falls back to full compute

- **GIVEN** a stock with no prior signal documents for a signal name
- **WHEN** the signal runner updates the stock
- **THEN** it SHALL compute signals over the full available factor history
  once
- **AND** SHALL record the anchor for subsequent incremental runs
