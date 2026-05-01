# Backtest MVP

## Overview

The MVP backtest feature evaluates one stock against one simple strategy on daily data.

The scoring system's historical replay and calibration capability is intentionally separate from this trading backtest. Scoring replay answers "did high scores predict better future outcomes?" while trading backtest answers "would a concrete trading rule have produced an investable equity curve?"

## Rules

- Backtest is single-stock only.
- Backtest is daily-bar only.
- Backtest uses existing quote and MA factor data.
- Backtest should prefer `close_hfq` for pricing.
- Signal generation for the first version can happen inside the backend backtest service.
- The MVP trading backtest should use small project-owned code instead of introducing an external framework.

## Strategy Scope

- MA cross long-only.
- Optional close-above-MA60 trend strategy.
- No multi-stock portfolio, no strategy editor, no optimization grid.

## Engine Choice

For the MVP, implement a lightweight internal daily-bar backtest service.

Use:

- existing Mongo quote and factor data;
- simple deterministic signal generation;
- explicit next-day or close-price execution assumptions;
- small Python service code that is easy to inspect and test.

Do not introduce `backtrader`, `vectorbt`, `zipline`, `rqalpha`, or another full trading framework in the MVP. Those frameworks should be reconsidered only when the project needs portfolio-level trading simulation, including multi-stock rebalancing, execution constraints, fees, slippage, position sizing, turnover, benchmark comparison, and richer order semantics.

Recommended separation:

- `datahub` scoring replay/calibration: evaluates prediction quality.
- `backend` trading backtest MVP: evaluates a user-submitted simple strategy.

## Result Shape

- Store run status, parameters, metrics, equity curve, and trades.
- Return a simple payload the frontend can plot and inspect.

## Boundary

- `datahub` continues to produce data only.
- `backend` runs or serves backtest results.
- `frontend` submits the request and renders results.

## Acceptance Criteria

- A user can submit one backtest and receive metrics plus trade history.
- The result can be explained with daily quote and factor data already stored in Mongo.
- The implementation stays small enough for the MVP timeframe.
- The spec clearly distinguishes scoring replay/calibration from trading backtest simulation.
- No external backtest framework is required for MVP trading backtest.
