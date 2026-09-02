# Design: Score-driven backtest execution integrity

## Decisions

### Score availability and execution

A prediction dated trading day D becomes actionable only after D closes. The
simulation therefore shifts each usable score to the next date present in the
backtest's ordered trading-day calendar. A resulting order executes using that
day's `open_hfq`, with the existing directional slippage and commission model.
If adjusted open is unavailable, the order is skipped rather than using D's
close or another synthetic price.

Existing pending-order behavior is retained: an order blocked by suspension or
price-limit constraints remains pending and is retried on later trading days.
A score on the final backtest date cannot execute inside the requested range.

### Version and status isolation

All score-driven service calls require a non-empty `model_version`. Queries use
strict equality and exclude `BLOCKED` and `FAILED`. `PENDING`, `TRACKING`,
`VERIFIED`, and `INSUFFICIENT_DATA` remain eligible because verification state
does not invalidate the score as it existed at D close.

Every caller that can select a score-driven strategy must pass the version.
Public endpoints return a stable validation error when it is missing; direct
service and CLI calls fail closed as well.

### Result auditability

New score-driven results record both `model_version` and
`execution_timing=next_trading_day_open` in `score_config`. Existing results
without the marker keep their historical interpretation.

## Validation

- Unit tests prove D score cannot trade on D, next-trading-day adjusted-open
  pricing, end-of-range behavior, status filtering, and version isolation.
- API tests cover required-version errors and propagation through public entry
  points.
- OpenSpec strict validation, backend Ruff/pytest, and frontend lint/build.
- A dev single-stock smoke run inspects score date, trade date, and execution
  price; low sample flags are expected and explicitly reported.
