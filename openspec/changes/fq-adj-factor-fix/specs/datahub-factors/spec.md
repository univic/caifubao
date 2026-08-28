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

### Requirement: Daily market FQ updates use one full-market factor snapshot

For each target trade date in a normal stale market refresh, the datahub MUST
request `pro.adj_factor(trade_date=target)` at most once, excluding bounded
retries of that request. It MUST join the snapshot locally to persisted quote
rows and MUST write only the target-date FQ/HFQ fields. Force and backfill
operations MUST retain the per-code historical path.

#### Scenario: Complete daily snapshot

- GIVEN supported stocks have persisted daily quote rows for the target date
- AND the factor snapshot contains exactly one same-date, finite, positive
  factor for every such quote code
- WHEN the stale market FQ refresh runs
- THEN each quote is joined one-to-one by normalized stock code
- AND `fq_factor` equals the snapshot factor
- AND each OHLC HFQ value equals its raw OHLC value multiplied by that factor
- AND only target-date rows are written

#### Scenario: Daily snapshot validation fails

- GIVEN the factor snapshot is empty, has a mismatched date, has duplicate or
  unmappable required codes, or lacks a finite positive factor for any
  supported target-date quote
- WHEN the daily market FQ refresh validates its inputs
- THEN no FQ fields are written before the entire batch passes validation
- AND the phase fails without advancing FQ freshness
- AND factor=1, forward-fill, and backward-fill MUST NOT mask the failure

#### Scenario: Snapshot has extra codes

- GIVEN the factor snapshot contains codes without target-date quote rows
- WHEN the daily market FQ refresh joins the inputs
- THEN the extra factors are ignored
- AND no quote or FQ row is invented for those codes

#### Scenario: Suspended stock has no target-date quote

- GIVEN a supported active stock is suspended and has no quote row on the
  target trade date
- WHEN the daily market FQ refresh runs
- THEN that stock is excluded from the target-date factor coverage denominator
- AND its prior quote/FQ row is not joined to the target-date factor

#### Scenario: Persisting the daily snapshot fails

- GIVEN the complete input batch passed validation
- WHEN persistence raises an error
- THEN the phase fails
- AND freshness is derived from finally persisted data rather than advanced
  optimistically
- AND replaying the same target date remains idempotent
