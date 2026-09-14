# Stock Timing Evaluator P0

## Why

The repository can backtest one stock, but a successful example is not evidence
that a timing rule generalises. The existing market scan ranks individual
results and discovers stocks from the current active universe, which encourages
cherry-picking and cannot represent a point-in-time research cohort. Score
execution also has two trust gaps: registered `scoring_mode` does not drive
engine dispatch, and close-observed stop-loss exits can be recorded at a price
that was no longer executable when the trigger became known.

P0 establishes a research-only timing evaluation boundary before any new signal
or production recommendation is attempted.

## What Changes

- Define a deterministic, in-memory single-stock timing decision record with
  position-aware `ENTER/HOLD/EXIT/NO_TRADE` actions, causal timestamps, expiry,
  score evidence, freshness, and explicit research-only provenance.
- Add a pooled evaluator over an explicit caller-supplied stock cohort. It
  consumes normalized execution-safe timing and same-stock buy-and-hold results,
  and reports coverage and aggregate distributions without selecting or naming
  a "best stock".
- Bind scoring dispatch to an ACTIVE registered model's `scoring_mode`; an
  explicit contradictory runtime mode fails closed instead of silently running
  a different construction.
- Make close-observed stop-loss exits causal: the close can form an exit order,
  but that order first attempts execution at the next tradable session's open.
- Correct the single-stock score-generation quote lookup to use the quote
  model's canonical `code` field.

## Non-goals

- No new factor, threshold optimization, per-stock tuning, alpha claim, model
  promotion, live order, broker adapter, account mutation, or investment advice.
- No automatic universe construction. Point-in-time cohort construction and
  delisted-name completeness remain the caller's responsibility and are exposed
  in report provenance.
- No public REST endpoint or frontend work. P0 is a deterministic service plus
  operator CLI; an API requires a later contract review.
- No persisted shared artifact or database write. The CLI report is transient
  research JSON, not a promotion record under the repository artifact contract.
- No daily scheduling or rebalance-cadence change. That work already has the
  independent `strategy-daily-schedule` change.

## Module Impact

- `backend/`: timing decision/pool evaluation service; causal backtest fix;
  single-stock score-generation query fix; focused tests.
- `datahub/`: registered scoring-mode dispatch; operator CLI; focused tests.
- `openspec/`: this change only.
