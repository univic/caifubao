# Small-account strategy research tools

## Why

The factor lab can measure cross-sectional signals, but it cannot answer how a
small equal-weight book behaves after holding cadence, hysteresis, replacement
turnover, and friction.  The repository also lacks a bounded research surface
for ETF-universe reconstruction and replay-only strategy ledgers.  Without an
explicit contract these tools can accidentally overstate performance or be
mistaken for forward/live evidence.

## What Changes

- Add a holding-period × buffer scan over frozen factor-lab panels.
- Add research-only ETF and small-book reconstruction/simulation helpers.
- Define causal rebalance, explicit current-snapshot/PIT limitations, turnover,
  friction, and replay-ledger semantics.
- Record negative and control results without promoting a model or producing a
  user-facing recommendation.

## Non-goals

- No model promotion, public API, frontend recommendation, broker integration,
  live order, scheduled run, or account mutation.
- No claim that historical replay is forward evidence.
- No replacement of the production paper strategy or its 120-session gate.

## Module Impact

- `datahub/`: research-only factor/ETF/small-book helpers and CLI entry point.
- `docs/`: reproducible research reports and operator boundaries.
- `openspec/`: this change record.
