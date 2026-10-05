# Live Account Ledger P0 (manual execution bridge)

## Why

Caifubao already has research portfolios, target-composition export, a paper strategy
ledger, and read-only plan-vs-account reconciliation, but it still lacks the bridge
between a human-confirmed real-world execution and the portfolio ledger. That gap
prevents daily investing activity from being captured as auditable evidence.

This change adds the smallest manual-execution loop before any broker API exists:
an operator records an immutable order intent, imports broker execution fills, applies
each accepted fill exactly once to an existing Portfolio ledger, and stores a
reconciliation result against a broker-supplied account snapshot.

The feature remains research/demo infrastructure. It does not place orders, connect to
a broker, promote a strategy, or claim that any model is tradable.

## What Changes

- Extend Portfolio with an explicit `account_mode` boundary: existing/default portfolios
  remain `RESEARCH`; execution-ledger routes only accept `MANUAL_LIVE`. Add
  `book_type` values `RESEARCH/CORE/QUANT/DISCRETIONARY` so live capital can be
  separated without storing broker account numbers.
- Add backend execution-ledger models:
  - OrderIntent: the planned BUY/SELL quantity that a human may execute.
  - ExecutionFill: an imported broker/manual fill with an idempotency key.
  - AccountReconciliation: persisted PASS/BREAK comparison between Caifubao's
    Portfolio cash/positions and a manually supplied account snapshot.
- Add Portfolio-scoped REST APIs to create/list order intents, import fills in JSON or
  canonical CSV format, list fills, and run/list reconciliations.
- A fill is ledger-affecting only after validation and idempotency checks. Accepted
  fills reuse the existing Portfolio transaction semantics, so cash, position quantity,
  average cost, and realized P&L remain single-sourced.
- Filling an intent updates its derived status from OPEN to PARTIAL/FILLED according to
  the cumulative imported quantity.
- CSV import is deliberately broker-neutral. The canonical columns are:
  `external_fill_id,stock_code,side,quantity,price,fee,trade_time`.
- Reconciliation is fail-loud: cash drift, missing positions, unexpected positions, or
  quantity drift outside configured tolerances produce BREAK and explicit break items.

## Non-goals

- No broker API, QMT/PTrade integration, automatic order placement, cancellation, or
  broker polling.
- No automatic strategy-to-intent generation in this slice; callers create intents
  from existing target exports or manual decisions.
- No frontend UI in this slice.
- No changes to scoring, selection, forward-evidence, or model-promotion semantics.
- No automatic trust of imported files: invalid rows are rejected and reported.

## Success criteria

A backend user can: create a Portfolio-scoped intent; import the same fill twice
without double-applying cash/positions; import a canonical CSV; observe intent
PARTIAL/FILLED state; reconcile manually supplied broker cash/positions; and retrieve
the persisted audit records through REST APIs.
