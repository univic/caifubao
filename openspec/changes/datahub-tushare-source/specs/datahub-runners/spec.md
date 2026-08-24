## ADDED Requirements

### Requirement: Tushare Stock History Source

Datahub SHALL support `tushare` as a stock history source selected via
`DATAHUB_STOCK_HISTORY_SOURCE` (values `akshare | baostock | tushare`; default
`akshare`), reading daily bars from the Tushare `pro.daily` interface.

#### Scenario: Operator selects the tushare history source

- **GIVEN** `DATAHUB_STOCK_HISTORY_SOURCE=tushare` and a valid `TUSHARE_TOKEN`
  in the runtime environment
- **WHEN** a stock quote runner pulls history for a stock
- **THEN** the runner SHALL return normalized quote rows for that stock
- **AND** rows SHALL be capped at the frozen `as_of_date`
- **AND** `volume` SHALL be in lots (手) and `trade_amount` in CNY yuan

#### Scenario: Tushare token is missing

- **GIVEN** `DATAHUB_STOCK_HISTORY_SOURCE=tushare` but `TUSHARE_TOKEN` is unset
- **WHEN** a stock quote runner pulls history
- **THEN** the runner SHALL fail with a clear error naming `TUSHARE_TOKEN`
