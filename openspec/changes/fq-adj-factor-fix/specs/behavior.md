# FQ Adj Factor Fix Behavior

## Scenario: FQ factor uses real tushare adj_factor

- WHEN `build_fq_factor_frame` processes a stock whose quote history spans an
  ex-dividend date
- THEN `fq_factor` equals the tushare `adj_factor` on each trading day
- AND `fq_factor` is constant on non-dividend days
- AND `close_hfq == close × fq_factor`

## Scenario: No ex-dividend events

- WHEN a stock has no dividends in its entire history
- THEN `fq_factor` is constant across all rows
- AND `close_hfq` is proportional to `close` (`close_hfq / close` constant)

## Scenario: Dividend day factor change

- WHEN the tushare adj_factor changes on an ex-dividend date
- THEN the change is reflected exactly once in `fq_factor` (no drift from
  daily close ratios)
- AND open/high/low_hfq scale by the same factor as close

## Scenario: Missing adj_factor rows

- WHEN a trading day is missing from the adj_factor response
- THEN the most recent known adj_factor is carried forward (fallback anchor)

## Scenario: Persistence

- WHEN FQ update writes rows
- THEN each `(code, date)` row is upserted idempotently with the corrected
  `fq_factor` and `close_hfq`/`open_hfq`/`high_hfq`/`low_hfq`
