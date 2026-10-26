## Why

A research portfolio can pass data readiness but fail its execution preflight
because a selected stock's minimum board lot exceeds the fixed per-name budget.
An explicit opt-in eligibility rule must exclude those names before selection
without changing capital, position limits, score order, or historical identity.

## What Changes

- Add optional boolean `constraints.exclude_infeasible_buys` to StrategySpec and
  its legacy research adapter; omission preserves historical normalization/hash.
- Filter candidates using signal-session closes and the existing board-lot,
  slippage, fee and minimum-commission feasibility arithmetic before selection.
- Retain final target-weight feasibility validation; report exclusion reasons.
- Existing specs and runs remain immutable; enabled strategies use a new hash.

## Impact

Shared StrategySpec, research config validation and daily-plan eligibility;
focused unit/contract tests. No broker, API endpoint, scoring construction,
readiness-state or timing-policy changes.
