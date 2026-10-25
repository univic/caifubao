## Why

The Research paper NAV currently marks unadjusted prices and omits dividends.
Crediting a DIVIDEND event on the ex-date would repair the total-return curve
but would incorrectly make the money spendable before its payable date,
distorting fill sizes and exposing the future strategy-improvement loop to
biased returns. Issue #319 requires accurate, rebuildable cash and holdings.

## What Changes

- Extend the shared v1 event-kind vocabulary without changing existing event
  meanings or the research/trading authority boundary.
- Add Research-only \`DIVIDEND_ENTITLEMENT\` (book a NAV receivable at ex-date)
  and \`DIVIDEND_SETTLEMENT\` (convert to spendable cash no earlier than pay-date).
- Keep \`cash\` separate from \`receivables\`; account NAV includes both while
  order sizing may use only available cash. Settlement remains owed if the stock
  is sold after the record date.
- Validate date order, per-share amount, exact entitlement quantity, payment
  linkage and idempotency before mutating ledger state.
- Regression test parity/rebuild and fail-closed cases.

## Capabilities

### Modified Capabilities

- \`strategy-portfolio-ledger\`: Research-only settled-cash versus dividend-
  receivable accounting and event replay semantics.

## Impact

- \`shared/caifubao_contracts/strategy_ledger_events.py\`
- \`datahub/app/lib/strategy_engine/simulation_ledger.py\`
- \`datahub/app/test/test_strategy_dividend_receivables.py\`

## Non-Goals

This PR does not claim the \`strategy_runner nav\` calculation is already
correct. Before enabling daily NAV, a separately validated PIT corporate-action
source must supply entitlement/payment events, daily valuation sessions and
correct ex-date share processing. No live orders, Trading Paper or promotion.
