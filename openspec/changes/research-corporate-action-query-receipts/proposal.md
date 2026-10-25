## Why

Milestone A's corporate-action source gate (#324) cannot tell whether a
reported no-dividend day means the upstream vendor was queried or one of the
required symbols failed. Existing historical dividend backfill silently
counts failed or null provider responses as empty, and deduplicates ex-dates
without preserving query completeness receipts. A Research NAV based on
those assumptions could have an invented drawdown or return.

## What Changes

Add an explicitly REPLAY-only Tushare collector, one provider query per
security, with per-security successful-query receipts (including empty
results), a separate SHA-256 digest of receipts and event contents, strict
economic/date/revision validation and atomic immutable local JSON archiving.
Any failed, null, duplicate or malformed source query aborts publication.
The collector never asserts FORWARD provenance or data-vendor completeness.

## Capabilities

### New Capabilities

- research-corporate-query-receipts: reproducible offline per-stock
  retrospective action query evidence.

## Impact

New pure datahub helper, optional operator-only script, unit tests and
documentation. No CronJob, Research DB writer, live vendor query performed
as part of this PR, or Trading account changes.

## Non-goals

Tushare historical response timestamps and dividends do not establish PIT
vendor availability as of past sessions. This collector must not feed
FORWARD NAV or be promoted merely by renaming its manifest.
