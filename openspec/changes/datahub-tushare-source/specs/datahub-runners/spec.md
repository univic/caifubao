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

#### Scenario: History response is empty

- **GIVEN** `DATAHUB_STOCK_HISTORY_SOURCE=tushare`
- **WHEN** the source returns no rows for a stock (e.g. listed after the frozen
  `as_of_date`, or suspended across the whole window)
- **THEN** the runner SHALL continue processing without crashing on the empty
  response (previously a `None` dereference aborted the whole run)
- **AND** the stock SHALL fail the quote phase per the existing zero-row
  contract, with the temporary-suspension gap allowance unchanged

#### Scenario: Full history for old listings

- **GIVEN** a stock listed before 2003 with more than 6000 trading days
- **WHEN** a stock quote runner pulls its full history up to the frozen
  `as_of_date`
- **THEN** the runner SHALL return rows covering the entire requested window
  (per-call row caps are handled by year-window pagination)

#### Scenario: Tushare rate limit is hit

- **GIVEN** `DATAHUB_STOCK_HISTORY_SOURCE=tushare`
- **WHEN** a pull is rejected with the tushare rate-limit message
  ("每分钟最多访问该接口")
- **THEN** the runner SHALL retry the request like other transient errors
