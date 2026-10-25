## Why

Milestone A still cannot certify Research daily net asset values: raw ex-date
quotes without dividend/bonus-share events produce false drawdowns.
The Research ledger added entitlement/settlement primitives in #320, but the
NAV projection did not consume them. See #319.

## What Changes

An opt-in research-only corporate action input accepts explicit per-instrument
record/ex/pay dates, cash-per-share and split ratios. Projection requires
an explicit daily valuation calendar (including the record and ex-date),
applies entitlements before ex-date trading/marks and settles cash no earlier
than payment. It records immutable event identities and preserves the
independent ledger NAV parity assertions and rebuildability.

## Capabilities

### New Capabilities

- research-paper-nav: cash dividend accrual and split integration with
  realistic cash timing and explicit valuation sessions.

## Impact

Pure Research simulation only, plus unit tests. No new live DB writes,
Trading account, ingestion job, auto-schedule, or new strategy promotion.

## Non-Goals

This is NOT completed Milestone A accounting: authoritative PIT company-action
coverage, ETF cash distributions, tax/cash-in-lieu, delisting, daily NAV
materializer and actual Research runtime verification remain required.
