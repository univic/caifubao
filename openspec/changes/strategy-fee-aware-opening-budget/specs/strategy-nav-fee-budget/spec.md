# Strategy NAV Fee-Aware Opening Buy Budget

## ADDED Requirements

### Requirement: Opening BUY sizing MUST reserve slippage and commission

At each opening execution session the NAV engine MUST select the largest
board-lot quantity whose total spend — execution value at the slippage-adjusted
open price plus commission (`max(value × rate, minimum_commission)`) — does not
exceed `min(available cash, target budget)` where target budget is
`pre-session portfolio value × target weight`. An order MUST be skipped only
when even one board lot (with fees) cannot fit; it MUST NOT be skipped merely
because the fee-unadjusted initial lot count overshoots.

#### Scenario: 100% target still buys after reserving fees

- GIVEN a single target with weight 1.0 and an open price such that the
  fee-unadjusted max lots slightly exceed available cash once slippage and
  commission are applied
- WHEN the engine sizes the opening buy
- THEN a BUY trade is recorded at the largest lot count whose spend
  (value + commission) is within cash
- AND the spend does not exceed cash

#### Scenario: Minimum-commission boundary fits the budget

- GIVEN an order whose notional is small enough that
  `value × rate < minimum_commission`
- WHEN the engine sizes the opening buy
- THEN commission is charged at the minimum
- AND value + minimum commission still fits `min(cash, target budget)`
  (quantity reduced if needed, never the whole order skipped while one lot fits)

#### Scenario: Proportional-commission boundary fits the budget

- GIVEN an order whose notional is large enough that
  `value × rate >= minimum_commission`
- WHEN the engine sizes the opening buy
- THEN commission is proportional
- AND value + proportional commission fits `min(cash, target budget)`

#### Scenario: Cash below one fee-inclusive lot skips the order

- GIVEN available cash is less than the cost of one board lot including
  slippage and commission
- WHEN the engine sizes the opening buy
- THEN no BUY trade is recorded for that name this session

#### Scenario: Sequential buys never overdraw cash

- GIVEN a session with multiple new targets bought in code order
- WHEN the engine sizes each buy against the current remaining cash
- THEN every buy's spend is within its own `min(remaining cash, target budget)`
- AND the final cash is never negative

#### Scenario: Zero-cost execution is unchanged by fee reservation

- GIVEN execution with zero slippage, zero commission, zero stamp duty
- WHEN the engine sizes an opening buy
- THEN the selected quantity equals the plain budget/price board-lot quantity
  (identical to the pre-change behavior under zero costs)

### Requirement: Opening sizing MUST NOT read the session close

Opening buy quantity MUST depend only on information available at the opening
session (open price, pre-session value, cash, targets). Reading or mutating
the session's close or any later quote for sizing MUST NOT occur.

#### Scenario: Same-day close cannot change opening fills

- GIVEN two identical opening sessions that differ only in the same-day close
  price (or any later quote)
- WHEN the engine sizes each session's opening buys
- THEN the BUY fills are identical (regression preserved from
  strategy-paper-causal-timing)
