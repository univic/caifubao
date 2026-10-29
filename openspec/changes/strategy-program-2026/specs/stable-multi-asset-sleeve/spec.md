# Stable Multi-Asset Sleeve

## ADDED Requirements

### Requirement: The stable sleeve MUST declare its risk profile

The stable sleeve SHALL be declared as a strategy declaration whose documented defaults
are an annualised volatility target of 10–12 percent, a maximum drawdown budget of 15–20
percent, an annual turnover budget of at most four, and a named benchmark that is either a
static allocation or a broad index blend. The declaration MUST record all four, and a
forward window MUST NOT be opened for the sleeve while any of them is absent.

#### Scenario: Missing drawdown budget blocks the window

- **GIVEN** a sleeve declaration without a drawdown budget
- **WHEN** certification of a forward window is attempted
- **THEN** it SHALL be refused

#### Scenario: Declared budget is enforced by the structural gate

- **GIVEN** a sleeve declaration whose implied annual turnover exceeds four
- **WHEN** the declaration is validated
- **THEN** validation SHALL fail

### Requirement: The sleeve MUST be declared as an allocation strategy

The stable sleeve SHALL declare the allocation strategy family. Its book semantics SHALL
be expressed as a fixed asset set, target weights, rebalance bands and a rebalance cadence
that is genuinely consumed, and it MUST NOT be required to declare rank-based exit
conditions that have no meaning for it. The rebalance cadence SHALL be consumed, and a
declared cadence that the implementation ignores MUST fail validation. Band retention SHALL
NOT override a per-asset stop: an asset that has breached its stop MUST be reduced or closed
even while it sits inside its band.

#### Scenario: No rank exit is required

- **GIVEN** an allocation-family sleeve declaration with a fixed asset set, target weights
  and bands
- **WHEN** the declaration is validated
- **THEN** it SHALL pass without any rank-based exit field

#### Scenario: Unconsumed cadence is rejected

- **GIVEN** an allocation declaration whose rebalance cadence no consumer reads
- **WHEN** the declaration is validated
- **THEN** validation SHALL fail

#### Scenario: Bands drive rebalancing

- **GIVEN** a declaration with rebalance bands and a holding inside its band that has not
  breached its per-asset stop
- **WHEN** a rebalance session is planned
- **THEN** the holding SHALL NOT be traded back to its exact target weight

#### Scenario: Band does not shield a stopped asset

- **GIVEN** a holding inside its rebalance band that has breached its per-asset stop
- **WHEN** the session is planned
- **THEN** the asset SHALL be reduced or closed with a stop reason
- **AND** the band SHALL NOT keep it at its target weight

#### Scenario: Per-asset stop-loss is mandatory and independent

- **GIVEN** the sleeve declaration
- **WHEN** it is validated
- **THEN** it SHALL declare a per-asset stop-loss
- **AND** a declaration that declares only portfolio-level de-risking SHALL be rejected

#### Scenario: Per-asset stop survives a de-risking session

- **GIVEN** a sleeve where one asset breaches its stop while the portfolio drawdown rule
  also triggers in the same session
- **WHEN** the session is planned
- **THEN** the breaching asset SHALL be reduced or closed with a stop reason
- **AND** the portfolio rule SHALL NOT suppress that action

### Requirement: The asset set MUST be committed before evaluation

The sleeve's asset set SHALL be fixed in the declaration before any evaluation, and the
backtest, the forward window and the live path MUST use the same set. Replacing an asset
after seeing its performance MUST create a new declaration and a new evidence identity,
and a result obtained by substituting assets post hoc MUST NOT be reported.

#### Scenario: Asset substitution creates a new identity

- **GIVEN** a sleeve declaration with a fixed asset set
- **WHEN** one asset is replaced
- **THEN** the declaration hash SHALL change
- **AND** existing evidence SHALL NOT carry over

#### Scenario: Post-hoc selection is not reportable

- **GIVEN** an evaluation that chose the asset set after observing returns
- **WHEN** results are reported
- **THEN** the report SHALL be rejected as an invalid backtest

### Requirement: The sleeve MUST NOT be built from narrow single-stock selection

The stable sleeve SHALL obtain diversification through pooled or broad-based instruments,
and MUST NOT depend on single-stock selection whose historical edge requires a number of
names that the sleeve's capital cannot afford at the board lot. A declaration that names
single-stock selection while its capital implies fewer names than the signal's documented
effective width MUST be rejected.

#### Scenario: Unaffordable width is rejected

- **GIVEN** a sleeve declaration using a single-stock signal whose documented edge exists
  only at a width the declared capital cannot reach at the board lot
- **WHEN** the declaration is validated
- **THEN** validation SHALL fail and state the unaffordable width

#### Scenario: Pooled instruments provide breadth cheaply

- **GIVEN** a sleeve built from broad-based pooled instruments
- **WHEN** the effective number of underlying names is reported
- **THEN** it SHALL exceed the number the declared capital could hold as single stocks

### Requirement: De-risking MUST be a portfolio-level declaration

Drawdown control for the sleeve SHALL be declared as a portfolio-level de-risking rule and
MUST NOT rely on per-name stops alone. The rule SHALL specify the trigger, the reduction
step, the maximum reduction and the recovery condition, and every threshold MUST be
recorded in the declaration so that changing it changes the identity.

#### Scenario: Trigger reduces exposure

- **GIVEN** a declaration with a drawdown trigger and a reduction step
- **WHEN** the sleeve's drawdown reaches the trigger
- **THEN** exposure SHALL be reduced by the declared step
- **AND** the reduction SHALL be recorded in the run

#### Scenario: Recovery condition is required

- **GIVEN** a de-risking rule with a trigger but no recovery condition
- **WHEN** the declaration is validated
- **THEN** validation SHALL fail

### Requirement: The sleeve MUST carry kill criteria

The sleeve SHALL declare the conditions under which it is retired, including a minimum
rolling excess-versus-benchmark measure over a stated horizon and a maximum tolerable
drawdown, and MUST evaluate them at the declared review cadence. A breach SHALL produce a
retirement recommendation rather than continued observation, and MUST be recorded.

#### Scenario: Breach recommends retirement

- **GIVEN** a sleeve whose rolling excess versus its benchmark falls below the declared
  minimum across the declared horizon
- **WHEN** the review runs
- **THEN** a retirement recommendation SHALL be recorded
- **AND** it SHALL NOT be replaced by a silent extension

#### Scenario: Review cadence is honoured

- **GIVEN** a declaration with a review cadence
- **WHEN** the sleeve has run fewer sessions than the cadence requires
- **THEN** the review SHALL report insufficient evidence rather than a pass
