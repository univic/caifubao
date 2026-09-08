# H20 snapshot evidence audit

## Why

The #208 handoff interprets the mean of overlapping H20 execution-label returns
as a daily return and compounds it as a portfolio loss. This does not establish
snapshot corruption. We need reproducible key/price/label comparisons and explicit
return units before rebuilding data or reusing historical research claims.

## What Changes

Add an offline, read-only snapshot audit command with explicit holding-period
units, temporal/price/key checks, execution-label self-joins and optional local
reference-quote comparison. Record the actual 2024–2026 audit results, including
the limits of a small dev sample. Correct the handoff interpretation without
certifying strategy effectiveness or declaring all market data clean.

## Non-goals

No candidate search, evaluator/metric changes, snapshot rebuilding, database
writes, forward-window certification, cleanup, deployment or model promotion.
Historical research results are preserved; audit results are not candidate runs
and are not appended to the official autoresearch ledger.
