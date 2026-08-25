## ADDED Requirements

### Requirement: Snapshot-Driven Daily Quote Update

Datahub SHALL write the as-of quote row from the settled market snapshot for
stocks exactly one trading day behind the frozen as-of date (UPD), instead
of pulling per-stock history, when the stock universe source is `tushare`
(whose `daily(trade_date)` bar is the settled as-of bar).

#### Scenario: Stock is one trading day behind and trading (tushare universe)

- **GIVEN** `DATAHUB_STOCK_UNIVERSE_SOURCE=tushare`, a stock with quotes
  through the previous trading day, and a settled market snapshot for the
  as-of date
- **WHEN** a stock quote runner processes the stock (UPD)
- **THEN** the runner SHALL write the as-of row from the snapshot row
- **AND** SHALL NOT call the history source
- **AND** SHALL refresh quote status for the as-of date and fail the phase
  when the status is not OK (zero-row gate and suspension-gap allowance
  remain in force)

#### Scenario: UPD index or spot universe stays on history

- **GIVEN** an index, or a stock universe sourced from the real-time spot
  (eastmoney/sina), with a stock one trading day behind
- **WHEN** a stock quote runner processes it (UPD)
- **THEN** the runner SHALL use the history source (the real-time spot is not
  the settled as-of bar)

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
