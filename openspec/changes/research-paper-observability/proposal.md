## Why

Milestone A writes Research strategy plans but users lack a continuous
Research-only read view. Existing /portfolio is a manual account and
/strategy-replays is historical REPLAY, not a certified paper account.

## What Changes

- An authenticated read-only /api/strategy/paper identity, overview and
  decision-details endpoint with exact model/horizon/hash filters.
- A /research-paper page that shows provisional NAV snapshots, pinned
  decisions/target allocations and distinct FORWARD versus REPLAY status.
- Never synthesize NAV or real fills, never auto-promote to certified evidence.

## Capabilities

### New Capabilities

- research-paper-observability: user-accessible research simulations.

## Impact

Backend read model, frontend API, page, navigation, tests and operations
documentation; no trading account writes, broker credentials or scheduled
Research deployment.

## Non-Goals

Milestone B here begins the observability surface, not full traceability
or certified NAV. Corporate-action source and data/runner operations remain
Milestone A blockers.
