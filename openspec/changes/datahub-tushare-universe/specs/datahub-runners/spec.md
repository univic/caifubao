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
  absent from the frozen-date daily snapshot (tushare omits suspended stocks)

#### Scenario: Daily snapshot is empty

- **GIVEN** `DATAHUB_STOCK_UNIVERSE_SOURCE=tushare` and the frozen-date daily
  snapshot returns no rows (e.g. a non-trading day or source failure)
- **WHEN** a stock quote runner resolves the stock universe
- **THEN** the runner SHALL fail loudly instead of flagging the whole market
  as suspended and silently no-op'ing the quote phase

#### Scenario: Spot remains the default

- **GIVEN** `DATAHUB_STOCK_UNIVERSE_SOURCE` is unset
- **WHEN** a stock quote runner resolves the stock universe
- **THEN** the runner SHALL use the existing eastmoney/sina spot path unchanged
