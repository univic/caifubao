# FQ Adj Factor Fix Behavior

## ADDED Requirements

### Requirement: FQ factor uses the real tushare adj_factor

The datahub FQ factor pipeline MUST compute `fq_factor` from the tushare
`pro.adj_factor` series instead of the cumulative product of daily
`close/previous_close` ratios.

#### Scenario: No ex-dividend events

- GIVEN a stock whose quote history has no dividends
- AND the tushare adj_factor is constant across the window
- WHEN `build_fq_factor_frame` processes the quote rows
- THEN `fq_factor` equals the constant tushare adj_factor on every row
- AND `close_hfq / close` is constant across all rows

#### Scenario: Ex-dividend factor change

- GIVEN a stock with an ex-dividend date inside the quote window
- WHEN the tushare adj_factor changes on that date
- THEN `fq_factor` reflects that change exactly once
- AND `close_hfq == close * fq_factor` on every row
- AND open/high/low_hfq scale by the same ratio as close

#### Scenario: Missing factor rows

- GIVEN a trading day missing from the adj_factor response
- WHEN the factor frame is built
- THEN the most recent known adj_factor is carried forward for that day

#### Scenario: Factor source is temporarily unavailable

- GIVEN the adj_factor request fails with a retryable network, decoding, or
  rate-limit error
- WHEN the FQ pipeline fetches the factor series
- THEN each failed request is retried with bounded exponential backoff
- AND retry exhaustion fails that code without writing FQ fields

#### Scenario: Factor response has no usable data

- GIVEN a code has quote rows but any requested adj_factor window is empty or
  the aggregate response contains no finite, positive factor values
- WHEN the FQ pipeline validates the factor series
- THEN that code fails without writing FQ fields
- AND the missing data is not converted to `factor=1`

#### Scenario: Persistence is idempotent

- GIVEN an FQ factor update for a code
- WHEN rows are written
- THEN each `(code, date)` row is upserted with the corrected
  `fq_factor`/`close_hfq`/`open_hfq`/`high_hfq`/`low_hfq` values
