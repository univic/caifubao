## ADDED Requirements

### Requirement: ETF universe comes from tushare fund_basic

The datahub SHALL maintain an `exchange_traded_fund` master document
(`object_type=etf`) for every tushare `fund_basic(market='E')` row whose code
maps to an SH or SZ venue, and MUST reject codes without an exchange venue
(the `.OF` rows returned by fund endpoints are not exchange-traded funds).

#### Scenario: Listed and delisted funds

- GIVEN a `fund_basic` row with `status='L'` and a `510300.SH` ts_code
- WHEN the universe sync runs
- THEN an `exchange_traded_fund` document `sh510300` is upserted with
  `active_status=0`, the fund type, benchmark, management fee and list date
- AND a row with `status='D'` is stored with `active_status=2`
- AND the sync is idempotent for an existing code

#### Scenario: Off-exchange rows are excluded

- GIVEN a `fund_basic` or `fund_daily` row with ts_code `158008.OF`
- WHEN normalization runs
- THEN no universe or quote document is produced for it

### Requirement: One trade date's ETF quotes are persisted without invention

For a requested trade date the datahub SHALL fetch the full-market
`fund_daily(trade_date=...)` snapshot, normalize it to the canonical quote
schema, and upsert one `stock_daily_quote` row per returned SH/SZ fund keyed by
`(code, date)`. It MUST NOT synthesize a quote for a fund tushare omits
(absence means not traded) and MUST fail the run when the snapshot contains a
code that is not in the ETF universe.

#### Scenario: Snapshot is written

- GIVEN an ETF universe containing `sh510300`
- AND a `fund_daily` snapshot row for `510300.SH` with OHLC and `pre_close`
- WHEN the daily sync runs for that date
- THEN the quote row stores the raw OHLC, `previous_close=pre_close`,
  `trade_amount = amount × 1000` (千元 → 元), `volume=vol` and
  `trade_status=1`
- AND no `fq_factor`/`*_hfq` field is written by the quote phase

#### Scenario: Suspended fund is not invented

- GIVEN `sh510300` has no `fund_daily` row for the target date
- WHEN the daily sync runs
- THEN no quote row is created for that fund and date
- AND the run reports the funds that were actually written

#### Scenario: Unknown code fails the run

- GIVEN a `fund_daily` snapshot row whose code is absent from the universe
- WHEN the daily sync runs
- THEN the run fails before writing quotes for that date

### Requirement: ETF after-adjustment factors use fund_adj

The datahub SHALL derive ETF `fq_factor`, `close_hfq`, `open_hfq`, `high_hfq`
and `low_hfq` from tushare `pro.fund_adj` using the same arithmetic contract as
stock FQ (`*_hfq = raw × fq_factor`), and MUST NOT use the daily
`close/previous_close` ratio or a constant factor as a substitute.

#### Scenario: Constant factor

- GIVEN a fund whose `fund_adj` factor is constant across the window
- WHEN the ETF FQ phase runs
- THEN `fq_factor` equals that constant on every row
- AND `close_hfq / close` is constant

#### Scenario: Distribution changes the factor once

- GIVEN a fund with a distribution date inside the window
- WHEN the ETF FQ phase runs
- THEN the factor changes on that date exactly once and every HFQ field scales
  by the same ratio as close

#### Scenario: Daily FQ is all-or-nothing

- GIVEN the target date's `fund_adj` snapshot is missing a factor for a fund
  that has a quote that day
- WHEN the daily FQ phase runs
- THEN no FQ field is written before the whole batch validates
- AND the phase fails without advancing ETF FQ freshness
- AND no factor=1 fallback is used

### Requirement: Fund provider windows are bounded and never truncated

The datahub MUST request fund history in windows that stay inside the provider's
per-call row cap (verified 2,600 latest-first rows for `fund_adj`, 5,000 for
`fund_daily`) and MUST fail loudly when a capped response begins materially
after the requested window start instead of persisting the truncated series.

#### Scenario: Old fund history is complete

- GIVEN a fund listed before 2016 with more than 2,600 sessions of history
- WHEN its history is backfilled
- THEN `fund_adj` is requested in 10-year windows
- AND the earliest stored factor matches the fund's first session
- AND no year is silently missing

#### Scenario: Truncation is rejected

- GIVEN a `fund_adj` response hits the row cap and its earliest row is far
  after the requested start
- WHEN the interface validates the response
- THEN it raises a truncation error and the backfill fails for that fund
- AND no quote/FQ row is written from the truncated window

### Requirement: ETF freshness uses the shared status contract

ETF quote, FQ and backfill runs SHALL record `data_asset_status` rows with
`object_type=etf` and the shared status semantics, so a stale or missing ETF
asset is visible without reading logs.

#### Scenario: Successful daily sync is fresh

- GIVEN the quote and FQ phases both wrote the target date
- WHEN the run completes
- THEN the ETF quote status is OK with `latest_data_date` equal to the target
  date
- AND the ETF FQ status is OK and not behind the quote status

#### Scenario: Failed FQ does not advance freshness

- GIVEN the FQ phase failed validation
- WHEN the run ends
- THEN the ETF FQ status is not advanced to the target date

### Requirement: ETF data is not stock data

The ETF master document and its quotes MUST remain distinguishable from stocks:
ETF documents use `object_type=etf`, and stock-only workflows (stock universe,
scoring, signals, strategy selection) MUST NOT include ETF codes.

#### Scenario: Stock universe is unchanged

- GIVEN the ETF universe has been synced
- WHEN a stock universe query filters on `object_type=individual_stock`
- THEN no ETF code is returned
- AND stock scoring and strategy inputs are unchanged

### Requirement: ETF operations are explicit operator commands

The datahub SHALL expose ETF universe, daily, backfill and status operations
through a CLI module and the unified `scripts/caifubao` CLI, and the daily
operation SHALL be schedulable as a suspended-by-default research CronJob.

#### Scenario: Operator runs one day

- GIVEN the research namespace and a valid tushare token
- WHEN the operator runs `scripts/caifubao etf daily 2026-10-23`
- THEN the command runs the datahub runner in the pod and prints a JSON summary
- AND a failed phase exits non-zero with a FAILED job-run record
