## Why

Research NAV cannot be trusted merely because a corporate-action row list
exists: the operator may have queried only a subset of instruments, hit source
quota failures, used after-the-fact event revisions, or combined mismatched
parquet and manifest versions. Milestone A (#319) needs explicit input lineage
and missing-universe detection before simulated NAV.

## What Changes

A pure, default-not-connected corporate-action input gate checks a canonical
events SHA-256, per-stock source coverage inventory, source failures, record/
ex/pay/announcement dates and same-day freshness. Its normalized event output
is explicitly tagged UNVERIFIED_UPSTREAM; source inventory claims require
independent upstream verification before a real Research NAV job may use them.

## Capabilities

### New Capabilities

- research-corporate-action-input: fail-closed canonical source inventory and
  point-in-time action validation for future Research NAV projection.

## Impact

Only Research pure helper and tests, no database writes, cron scheduling,
broker integration or claim of certified account returns.

## Non-Goals

This does not create an authoritative Tushare/ETF corporate-action publisher,
prove the absence of distributions or enable any automated NAV writer. The
provenance and completeness of the manifest itself must be separately audited.
