# Small-account execution plan

## ADDED Requirements

### Requirement: A target-weight plan SHALL produce board-legal, fee-aware orders

The planner SHALL produce a deterministic order list from an account snapshot
(`cash`, `positions`), target weights, an AUM and signal-date prices:

- sells for held names that are not in the target, before any buys;
- buys that respect the board minimum and increment (STAR/科创板 200 + 1;
  other boards 100). BSE/北交所 is outside the production universe for this
  slice: a BSE code SHALL be rejected with a reported reason, not sized with an
  unverified lot rule;
- commission with the configured minimum per side, sell stamp duty, and the
  two-way transfer fee;
- a target notional per name derived from the target weight and AUM, and never
  more than the available cash;
- a cash floor that is never violated;
- a skipped-order record for any name that cannot fit one board minimum or has
  no usable execution price.

AUM SHALL be `cash + sum(held_quantity * signal_date_price)`. If any held name
has no usable signal-date price, the plan SHALL fail closed with the name and
date; it SHALL NOT value that holding at zero. The plan MUST NOT emit a buy whose
total spend (slippage-adjusted value + commission + transfer fee) exceeds the
cash available after sells, and MUST fail loudly if the resulting cash would be
negative.

#### Scenario: A STAR name starts at 200 shares

- GIVEN a target STAR code with a budget that fits 300 shares
- WHEN the plan is built
- THEN the buy quantity is 300, not 200 or 400

#### Scenario: An unaffordable name is skipped, not partially filled

- GIVEN a target name whose one board minimum costs more than its target budget
- WHEN the plan is built
- THEN no buy order is emitted for that name
- AND the skip is reported with its reason

#### Scenario: Sells fund buys

- GIVEN a held name leaving the target and a buy entering it
- WHEN both are executable
- THEN the sell is planned before the buy
- AND the buy budget can use the post-sell cash

#### Scenario: An unvaluable holding fails closed

- GIVEN a held name with no usable signal-date price
- WHEN the planner builds the account
- THEN the plan fails with the name and date
- AND no order list is emitted

### Requirement: The plan SHALL use the shared execution and fee contract

The planner SHALL use the same board-aware limit verdict as the paper execution
path (`strategy_engine.tradability.tradeable_open`): a limit-up open refuses a
BUY, a limit-down open refuses a SELL, and an underivable verdict refuses the
side it would block. A refusal SHALL be reported per order, never silently
dropped. The plan is a pre-trade preview; the execution-time gate remains the
authority, and the plan SHALL state that.

The fee schedule SHALL be normative and derived from the existing execution
config: commission 0.025%/side with a ¥5 minimum, sell stamp duty 0.05%,
transfer fee 0.001%/side, slippage 0.1%/side, board lot rules as above. The
normalized strategy config hash (`strategy_config_hash`) SHALL be recorded; a
fee change SHALL change the hash.

#### Scenario: A limit-up target is not planned as a buy

- GIVEN a target whose execution session opens at the upper limit
- WHEN the plan is built with that session's prices
- THEN no buy order is emitted
- AND the refusal is reported with the limit-up reason

### Requirement: The plan SHALL be read-only and reproducible

The planner SHALL NOT write Mongo, place orders, mutate an account or contact a
broker. The same snapshot, targets, prices and config SHALL produce byte-identical
order output. The output SHALL include the input hash, the config hash, per-order
fees and the expected post-trade cash.

#### Scenario: Repeated plans are identical

- GIVEN the same plan inputs
- WHEN the planner runs twice
- THEN the two JSON outputs are byte-identical

### Requirement: A plan SHALL be reconcilable with an account snapshot

The output SHALL include target quantities per name and a per-name projected
position, so it can be compared with an account snapshot by the existing
reconciliation service without recomputation.

#### Scenario: Planned quantities match projected positions

- GIVEN an account with 100 shares of a held target and a plan that buys 200
- WHEN the plan is read
- THEN the projected position is 300 shares
- AND the target quantity is 300 shares
