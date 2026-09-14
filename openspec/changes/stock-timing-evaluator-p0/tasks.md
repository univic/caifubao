# Stock Timing Evaluator P0 Tasks

## 1. Spec Gate

- [x] 1.1 Spec-guardian reviews action semantics, pooled evidence gates,
      scoring-mode precedence, and stop-loss timing before code changes.
- [x] 1.2 Record reviewer findings and resolve all P1/P2 issues.

## 2. Failing tests

- [x] 2.1 Decision artifact tests: four actions, missing evidence, timestamp
      causality, expiry, deterministic config hash, research-only label.
- [x] 2.2 Pool tests: explicit cohort only, duplicate/order determinism,
      same-stock BUY_HOLD comparison, errors retained, no best-stock field,
      50-stock/120-session/5-trade gates.
- [x] 2.3 Registered scoring mode dispatches ranked without environment help;
      contradictory explicit mode fails closed; unregistered legacy path stays
      compatible.
- [x] 2.4 Close-triggered stop loss cannot execute until the next tradable open,
      including one blocked-session retry.
- [x] 2.5 Single-stock score generation queries `StockDailyQuote.code`.

## 3. Implementation

- [x] 3.1 Add the pure decision constructor and pooled evaluator under backend
      services; add an operator-only `timing-pool` CLI command.
- [x] 3.2 Bind scoring dispatch to registered `scoring_mode` and persist the
      effective mode in score input snapshots.
- [x] 3.3 Correct causal stop-loss execution and the single-stock quote query.
- [x] 3.4 Update operator documentation with research-only boundaries and a
      reproducible explicit-cohort example.

## 4. Validation and gates

- [x] 4.1 Focused backend/datahub tests, then full affected suites.
- [x] 4.2 Ruff check/format and `openspec validate --all --strict`.
- [x] 4.3 Spec-guardian, contract-reviewer, and qa-reviewer after validation.
- [ ] 4.4 Branch conflict check, Draft PR, CI green, then ready for review.
