# Change: Make score-driven backtests execution-safe

## Why

Daily scores are produced after the market close, but the current backtest can
consume a score and trade at that same day's close. This introduces look-ahead
bias. Score-driven requests can also omit `model_version`, and unusable
`BLOCKED`/`FAILED` predictions are not excluded explicitly.

## What Changes

- Execute score-driven orders no earlier than the next actual trading day's
  adjusted open price.
- Require an explicit `model_version` for every score-driven backtest entry
  point and preserve it in the result configuration.
- Exclude `BLOCKED` and `FAILED` predictions without falling back to another
  model version.
- Mark new results with the score execution timing so they are distinguishable
  from historical close-executed results.
- Expose the required model-version input in the backtest UI.

## Capabilities

- Modified: `backtest-mvp`

## Impact

- Backend score-driven backtest simulation, API validation, CLI/async callers,
  and focused tests.
- Frontend backtest request form and TypeScript request contract.
- Existing persisted backtest results remain readable and are not rewritten.

## Non-goals

- No scoring algorithm or default model-version change.
- No historical scoring backfill or production data rewrite.
- The dev single-stock run is a linkage smoke test, not evidence of strategy
  effectiveness.
