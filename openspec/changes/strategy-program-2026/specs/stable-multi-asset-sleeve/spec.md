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
