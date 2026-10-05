# Live Account Ledger Specification

## ADDED Requirements

### Requirement: Portfolio-scoped order intents

The system SHALL allow a caller to persist a BUY or SELL order intent for an existing
Portfolio with stock code, positive target quantity, optional target price, optional
source reference, and creation timestamp.

#### Scenario: Create a valid intent

- **WHEN** a caller submits a BUY or SELL intent with positive target quantity
- **THEN** the system persists it with status OPEN and returns its identifier

#### Scenario: Reject invalid intent

- **WHEN** side is unsupported or target quantity is not positive
- **THEN** the system returns 400 and writes no intent

### Requirement: Idempotent execution-fill ingestion

The system SHALL ingest execution fills using a Portfolio-scoped external fill
identifier and SHALL NOT apply the same fill to portfolio cash or positions more than
once.

#### Scenario: Apply a new fill

- **WHEN** a valid previously unseen fill is submitted
- **THEN** the system records the fill and applies exactly one corresponding Portfolio
  transaction

#### Scenario: Re-submit an existing fill

- **WHEN** the same Portfolio and external fill identifier are submitted again
- **THEN** the system returns the recorded fill as a duplicate and does not create
  another Portfolio transaction or mutate cash/positions again

### Requirement: Broker-neutral canonical CSV import

The system SHALL accept UTF-8 CSV execution data with columns
`external_fill_id,stock_code,side,quantity,price,fee,trade_time`.

#### Scenario: Import a mixed CSV batch

- **WHEN** a CSV contains valid, duplicate, and invalid rows
- **THEN** valid unseen rows are applied, duplicates are reported without reapplication,
  invalid rows are reported with row-level errors, and category counts are returned

### Requirement: Intent fill status

The system SHALL derive order-intent status from cumulative linked fill quantity.

#### Scenario: Partial fill

- **WHEN** cumulative linked fill quantity is greater than zero and lower than target
  quantity
- **THEN** the intent status becomes PARTIAL

#### Scenario: Full fill

- **WHEN** cumulative linked fill quantity is equal to or greater than target quantity
- **THEN** the intent status becomes FILLED

### Requirement: Persisted account reconciliation

The system SHALL compare an existing Portfolio ledger against caller-supplied account
cash and position quantities and persist a PASS or BREAK result.

#### Scenario: Matching account state

- **WHEN** cash and every position are within configured tolerances
- **THEN** reconciliation status is PASS and breaks is empty

#### Scenario: Account drift

- **WHEN** cash drift exceeds tolerance or any expected/actual position is missing,
  unexpected, or quantity-different beyond tolerance
- **THEN** reconciliation status is BREAK and each discrepancy is explicit

### Requirement: Manual-only execution boundary

The execution-ledger APIs SHALL NOT place, cancel, or modify orders at any external
broker and SHALL remain usable without broker credentials.

#### Scenario: Import does not trigger external execution

- **WHEN** an order intent or fill is created
- **THEN** only Caifubao persistence and existing Portfolio ledger state are changed
