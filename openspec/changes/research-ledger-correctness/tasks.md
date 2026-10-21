# Research ledger correctness tasks

## 1. Ledger

- [x] 1.1 Add a research-only T+1 ledger: D-close decision, D+1-open fill, D+1-close mark.
- [x] 1.2 Enforce strict actual-position cap and no new entries while exits are blocked.
- [x] 1.3 Support a single continuous account with initial cash/positions.
- [x] 1.4 Apply corporate actions and explicit delisting exits; report write-offs with `lots × 100`.
- [x] 1.5 Report turnover on initial capital and average NAV separately; remove the 21-day zero inference.

## 2. Tests

- [x] 2.1 Synthetic tests: no same-day fill, cash conservation, T+1 mark.
- [x] 2.2 Synthetic tests: strict cap with blocked exits, single-account chaining.
- [x] 2.3 Synthetic tests: share multiplier/cash dividend, delisting write-off units, stale quote not zero.

## 3. Validation

- [x] 3.1 Ruff/format and full datahub tests pass.
- [x] 3.2 OpenSpec strict validation passes.
- [x] 3.3 Record the P0 result and the remaining data gaps; do not promote a strategy.

## 4. P0 data and integration

- [x] 4.1 Backfill implemented Tushare `dividend` events (shares and cash) for the panel codes.
- [x] 4.2 Build explicit delisting exits from the known delist dates and last available prices.
- [x] 4.3 Run one continuous 50k, strict-10-name monthly account with the new ledger and the decision-time delisting filter.
- [x] 4.4 Record the result and holdout/data limitations; do not promote a strategy.

## 5. Risk candidate

- [x] 5.1 Test one fixed 60% gross-exposure rule on the P0 ledger.
- [x] 5.2 Run a 2026-03-17..09-18 holdout with frozen last-fold weights.
- [x] 5.3 Record that the in-sample target passed but the holdout failed; do not promote.
