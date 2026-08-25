## ADDED Requirements

### Requirement: Tushare Stock Universe Source

Datahub SHALL support `tushare` as the stock universe/list source selected via
`DATAHUB_STOCK_UNIVERSE_SOURCE` (values `spot | tushare`; default `spot`),
replacing the eastmoney/sina spot list with `pro.stock_basic` + the daily
snapshot.

#### Scenario: Operator selects the tushare universe source

- **GIVEN** `DATAHUB_STOCK_UNIVERSE_SOURCE=tushare` and a valid `TUSHARE_TOKEN`
- **WHEN** a stock quote runner resolves the stock universe
- **THEN** the runner SHALL enumerate active A-shares from `pro.stock_basic`
- **AND** SHALL flag a stock as temporarily suspended (close = 0) when it is
  absent from the frozen-date daily snapshot or has `trade_status = 0`

#### Scenario: Spot remains the default

- **GIVEN** `DATAHUB_STOCK_UNIVERSE_SOURCE` is unset
- **WHEN** a stock quote runner resolves the stock universe
- **THEN** the runner SHALL use the existing eastmoney/sina spot path unchanged
