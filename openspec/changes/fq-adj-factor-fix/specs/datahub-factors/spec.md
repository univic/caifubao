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

### Requirement: FQ/HFQ acceptance is verified against tushare adj_factor

Acceptance of the FQ/HFQ values MUST verify them against the tushare
`pro.adj_factor` series itself, and MUST NOT treat agreement with the legacy
stable environment as the source of truth for `fq_factor`, `close_hfq`,
`open_hfq`, `high_hfq`, `low_hfq`, or factors derived from `close_hfq`.

#### Scenario: Sampled codes match the source of truth

- GIVEN a supported stock with quote rows in the acceptance window
- WHEN the acceptance check fetches the tushare `adj_factor` series for that
  code and compares it with the stored FQ fields
- THEN `fq_factor` equals the source factor on every row where the source
  provides one
- AND the most recent known source factor is carried forward for rows the
  source omits
- AND `close_hfq == round(close * fq_factor, 4)`
- AND open/high/low_hq scale by the same ratio as close

#### Scenario: Factor source is unavailable during acceptance

- GIVEN the tushare request for a sampled code fails or returns no rows
- WHEN the acceptance check runs
- THEN the check FAILS
- AND it MUST NOT be skipped
- AND it MUST NOT fall back to legacy-stable parity for that field class

#### Scenario: Individual source rows are unusable

- GIVEN a source frame contains individual non-finite or non-positive factor
  rows
- WHEN the acceptance check builds the expected factor series for that code
- THEN those rows are skipped exactly as the writer skips them, so acceptance
  cannot fail on values the writer deliberately ignores
- AND the number of skipped rows is reported
- AND the check FAILS when no usable factor remains for that code

#### Scenario: Named acceptance date has no rows

- GIVEN an operator names a trade date for the acceptance check
- WHEN neither environment has a quote row for that date
- THEN the check FAILS instead of reporting a vacuous pass

#### Scenario: Acceptance covers the full recompute window

- GIVEN a market-wide FQ recompute over a date range
- WHEN acceptance runs the bounded discontinuity scan
  (`datahub/scripts/check_fq_factor_integrity.py --jump-scan --from-date
  <start> --to-date <end>`, backed by `scan_fq_factor_jumps`)
- THEN the scan window equals the recompute window rather than only the known
  incident date
- AND the scan output — per-date affected count, maximum change, and fraction
  of the scanned universe — is recorded in the acceptance record alongside the
  tushare source check
- AND the recorded per-date counts are compared against the recorded baseline
  rather than treated as a pass/fail threshold of their own

### Requirement: Legacy-stable FQ/HFQ equality is skipped only under a declared scope

A cross-environment parity check MUST classify every field it observes into an
explicit, declared field-scope class and MUST skip FQ-derived equality only
under such a declaration, with the exclusion and its reason recorded in the
report and the job-run record.

#### Scenario: Derived fields excluded by default

- GIVEN research's FQ/HFQ values were recomputed from the real source while the
  legacy stable environment is still frozen on the pre-fix values
- WHEN the parity check compares the two environments
- THEN FQ-derived fields are excluded from equality only because the declared
  scope classifies them as derived
- AND the report and job-run summary record the excluded class, its reason,
  and a version identifier of the declared scope
- AND prices, volumes, business keys, and other source-derived fields are
  still compared field by field

#### Scenario: Derived fields compared on request

- GIVEN the operator asks for derived fields to be compared
- WHEN the parity check runs
- THEN FQ-derived fields are compared field by field
- AND any mismatch FAILS the check

#### Scenario: Undeclared field fails closed

- GIVEN a field appears on either environment that the declared scope does not
  classify
- WHEN the parity check runs
- THEN the check FAILS and names the undeclared field
- AND no field may be silently ignored

#### Scenario: Field populated only by the newer writer

- GIVEN the current writer populates fields that the frozen legacy writer
  omitted
- WHEN the parity check runs
- THEN those fields are declared as research-populated with the producing
  writer, source, and code revision
- AND each of them MUST be present and non-null on the research side
- AND their absence on the legacy side is reported as informational rather than
  as a value mismatch

#### Scenario: Count differences are reported per class

- GIVEN the two environments differ in row coverage, for example index rows or
  unsupported-universe symbols
- WHEN the parity check compares counts
- THEN the comparison reports each declared class separately, with its own
  verdict, tolerance, and declared mode
- AND a class with zero rows on either side FAILS, unless that class is
  explicitly declared as excluded-by-design with a recorded reason (for example
  the unsupported-universe class, which the research environment excludes by
  the supported-universe rule)
- AND a declared minimum-coverage floor is applied to any class whose mode
  permits research coverage to exceed the legacy environment, so a coverage
  regression still FAILS
- AND a total count MUST NOT be used to mask a single-class regression
