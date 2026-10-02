# Contract decisions

- Input schema `etf-benchmark-v1`: `price_basis: raw`, `instrument` (`code`,
  `type: domestic_equity_etf`, `tick_size`), optional `initial_cash` (100000.00),
  required `allocation` in [0,1], `fees` (`commission_rate`, `minimum_commission`,
  `slippage_rate`), `decision_date`, `sessions`, `quotes`, and explicit
  `corporate_actions: []`. Quotes contain `date`, `open`, `close`, `trade_status`
  (0/1), `upper_limit`; missing/null values block the corresponding operation.
  Invalid supplied values fail validation. Stock fee/tax fields are not accepted.
- Decision date is the first supplied market session. Sessions and quote dates
  are unique and ordered; quotes outside the calendar fail. Every later session
  yields a ledger row, including missing quotes. Instrument identity and calendar
  are operator declarations, not exchange certification.
- Budget = initial cash * allocation, ROUND_DOWN cents. Commission = max(rate *
  notional, minimum), ROUND_HALF_UP cents. A private precision-40 Decimal context
  prevents caller rounding/precision from changing results. Allocation and fee
  rates accept at most 12 decimal places, keeping accepted arithmetic within
  that context; higher precision is rejected. Prices fit the tick,
  and adverse slippage is rounded UP to the tick. Quantity is the largest 100-unit
  multiple with notional + commission <= budget; no fill means no fee.
- Decision uses only previous-session close and existing account. Execution uses
  only same-session open/status/upper limit and pinned budget, never its close.
  Retry after a block uses that session's preceding close and original budget.
  Zero allocation is HOLD_CASH. After a fill, HOLD makes no further orders.
- Missing close never blocks an otherwise valid buy. New positions start marked
  at raw open with source OPEN; without a valid close they are STALE. Existing
  positions carry their last valid mark when close is missing/non-tradable, with
  actual mark date/source. Cash-only valuation status is CASH. Bought units are
  sellable from the next supplied market session, never on purchase day.
- Output uses cent-exact money strings, integer units, REPLAY evidence only,
  input/config hashes, at most one BUY, per-session cash/mark/freshness and
  final_account {positions,cash,planned_cash,as_of}, plus explicit-quantity
  target_holdings. The existing reconciler requires numeric cash: convert only
  cash/planned_cash at that interface, keeping the source ledger cent strings.
  This is a replay-derived
  account; live account persistence/idempotency are future slices.
- Normalised hashes exclude current time and paths. Trade IDs use config hash
  and execution date, not future prices. Identical frozen input yields identical
  result. Action-free interval is mandatory; any distribution/split is rejected.
- CLI runs locally without MongoDB or kubectl, using repo datahub on PYTHONPATH.
  Existing assert_not_halted is checked at entry and before atomic output.
  HALTED exits 2. Output cannot alias input or halt file through a symlink/hardlink.
  Atomic output failure preserves the old result.

# Task notes

Outcome: 100k opening -> fills -> ledger -> valuation -> reconciliation.
Module Impact: datahub strategy library, local unified CLI, docs and tests.
Spec Gate: required (replay, freshness and public docs).
Assumptions: operator supplies raw action-free ETF history and market sessions.
Write Scope: new benchmark module/tests/fixture/spec/runbook, CLI routing.
Validation Plan: hand calculation, causality, blocked fills, stale marks, halt,
CLI and relevant strategy regressions; Ruff and OpenSpec strict.
Reviewer Requests: pre-implementation spec-guardian conditionally passed; all
required clarifications incorporated here. Post-implementation spec, contract
and QA reviewers are required.
Branch Conflict Check: against origin/develop after reviewers complete.
