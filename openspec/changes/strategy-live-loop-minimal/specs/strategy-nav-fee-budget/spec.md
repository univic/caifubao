## MODIFIED Requirements

### Requirement: Opening BUY sizing MUST reserve slippage and commission

At each opening execution session the NAV engine MUST select the largest
board-legal quantity whose total spend — execution value at the
slippage-adjusted open price plus commission (`max(value × rate,
minimum_commission)`) plus the two-way transfer fee — does not exceed
`min(available cash, target budget)` where target budget is `pre-session
portfolio value × target weight`. Quantity MUST be board-legal (main board and
ChiNext in whole lots; STAR/科创板 at least 200 shares and then 1-share
increments). An order MUST be skipped only when even the board minimum (with
fees) cannot fit; it MUST NOT be skipped merely because the fee-unadjusted
initial quantity overshoots.

#### Scenario: 100% target still buys after reserving fees

- GIVEN a single target with weight 1.0 and an open price such that the
  fee-unadjusted max lots slightly exceed available cash once slippage,
  commission and the transfer fee are applied
- WHEN the engine sizes the opening buy
- THEN a BUY trade is recorded at the largest legal quantity whose spend
  (value + commission + transfer fee) is within cash
- AND the spend does not exceed cash

#### Scenario: Minimum-commission boundary fits the budget

- GIVEN an order whose notional is small enough that
  `value × rate < minimum_commission`
- WHEN the engine sizes the opening buy
- THEN commission is charged at the minimum
- AND value + minimum commission + transfer fee still fits
  `min(cash, target budget)` (quantity reduced if needed, never the whole order
  skipped while the board minimum fits)

#### Scenario: Proportional-commission boundary fits the budget

- GIVEN an order whose notional is large enough that
  `value × rate >= minimum_commission`
- WHEN the engine sizes the opening buy
- THEN commission is proportional
- AND value + proportional commission + transfer fee fits
  `min(cash, target budget)`

#### Scenario: Cash below one fee-inclusive lot skips the order

- GIVEN available cash is less than the cost of the board minimum including
  slippage, commission and the transfer fee
- WHEN the engine sizes the opening buy
- THEN no BUY trade is recorded for that name this session

#### Scenario: Sequential buys never overdraw cash

- GIVEN a session with multiple new targets bought in code order
- WHEN the engine sizes each buy against the current remaining cash
- THEN every buy's spend is within its own `min(remaining cash, target budget)`
- AND the final cash is never negative

#### Scenario: Zero-cost execution is unchanged by fee reservation

- GIVEN execution with zero slippage, zero commission, zero stamp duty AND a
  zero transfer fee
- WHEN the engine sizes an opening buy
- THEN the selected quantity equals the plain budget/price board-legal quantity
  (identical to the pre-change behavior under zero costs)

#### Scenario: The default transfer fee reduces the sized quantity

- GIVEN the default execution costs, which include the two-way transfer fee
- WHEN the engine sizes an opening buy
- THEN the selected quantity is reduced relative to a zero-fee run on the same
  inputs, so the fee-inclusive spend still respects the budget
