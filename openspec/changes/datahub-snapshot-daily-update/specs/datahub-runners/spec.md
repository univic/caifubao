## ADDED Requirements

### Requirement: Snapshot-Driven Daily Quote Update

Datahub SHALL write the as-of quote row from the settled market snapshot for
stocks exactly one trading day behind the frozen as-of date (UPD), instead
of pulling per-stock history.

#### Scenario: Stock is one trading day behind and trading

- **GIVEN** a stock with quotes through the previous trading day and a
  settled market snapshot for the as-of date
- **WHEN** a stock quote runner processes the stock (UPD)
- **THEN** the runner SHALL write the as-of row from the snapshot row
- **AND** SHALL NOT call the history source

#### Scenario: Stock is more than one day behind, or suspended

- **GIVEN** a stock more than one day behind (INC/FULL) or flagged suspended
  (close = 0) in the snapshot
- **WHEN** a stock quote runner processes the stock
- **THEN** the runner SHALL fall back to the history source
- **AND** SHALL NOT write a snapshot row for a suspended stock
