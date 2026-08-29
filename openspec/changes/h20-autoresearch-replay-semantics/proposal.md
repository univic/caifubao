## Why

The approved H20 autoresearch design locks final-test data until candidate
selection is complete, while the archived profitability requirement says the
keep/discard metric comes from a test-period report. The H20 design also says
blocked orders roll forward, but the bootstrap snapshot did not freeze whether
the 20-day holding period starts on the signal date or the actual entry date.
Both ambiguities affect replay, look-ahead protection, and model-selection
semantics, so they must be resolved before historical snapshot export.

## What Changes

- H20 candidate keep/discard decisions use validation and quarterly
  walk-forward validation metrics only. The final test split remains unread
  until one candidate configuration is frozen, then is evaluated once.
- A signal produced after close first attempts entry at the next trading-day
  open. Suspended and limit-up sessions defer a buy to the first executable
  open.
- The H20 holding clock starts on the actual entry date. Exit is first
  attempted at the open of the twentieth subsequent trading session;
  suspended and limit-down sessions defer the sell to the first executable
  open.
- The immutable snapshot records requested and actual entry/exit dates,
  prices, and blocked-session counts so replay behavior is auditable.

## Non-goals

- No production scoring default, recommendation, factor, signal, API, auth,
  scheduler, frontend, OpenClaw, or deployment behavior changes.
- No automatic production promotion and no automated trading.
- No H5 or H60 research semantics.
