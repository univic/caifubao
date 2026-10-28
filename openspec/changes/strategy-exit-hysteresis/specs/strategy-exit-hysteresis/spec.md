# Strategy Exit Hysteresis

## ADDED Requirements

### Requirement: Optional exit hysteresis declaration

The StrategySpec SHALL accept two optional `rebalance` fields: `exit_buffer`
(a finite number >= 1.0) and `min_hold_sessions` (an integer >= 0). Both SHALL be
rejected with a clear error when out of range or of the wrong type, and an unknown
`rebalance` key MUST still fail closed. Neither field SHALL be defaulted into the
normalized spec or into the legacy config projection, so a spec that omits them
keeps its exact normalized bytes, spec hash and legacy config hash.

#### Scenario: Omitted hysteresis keeps the certified identity

- **GIVEN** the certified research spec without either field
- **WHEN** it is validated, hashed and projected to a legacy config
- **THEN** the spec hash SHALL be unchanged
- **AND** the legacy `rebalance` block SHALL contain only `cadence_days`

#### Scenario: Out-of-range hysteresis is rejected

- **GIVEN** a spec declaring `exit_buffer` below 1.0, a non-integer
  `min_hold_sessions`, or an unknown `rebalance` key
- **WHEN** the spec is validated
- **THEN** validation SHALL fail with an error naming the offending field

### Requirement: Exit band and minimum holding period drive the target book

Given the previous completed book, selection SHALL keep a held name while its rank
is inside the exit band `ceil(portfolio_size × exit_buffer)`, SHALL keep a position
whose recorded `hold_count` is below `min_hold_sessions`, and SHALL sell a held name
that leaves the band once its minimum holding period has elapsed. A held name that
no longer has a usable prediction (suspended, ST, unknown limit, no score) SHALL be
sold regardless of the policy. Vacancies SHALL be filled from the remaining
candidates by score, and the final book SHALL still be capped at
`portfolio_size` and ordered by score then code.

#### Scenario: Exit band keeps a slipping name

- **GIVEN** a book of three names and a held name ranked fourth
- **WHEN** the plan is built with `exit_buffer = 2.0`
- **THEN** the held name SHALL stay in the target book
- **AND** its reason SHALL be `held_in_exit_band`

#### Scenario: Band exit without hysteresis sells the name

- **GIVEN** the same book with no policy (buffer 1.0)
- **WHEN** the plan is built
- **THEN** the name ranked fourth SHALL be sold
- **AND** its reason SHALL be `sold_out_of_exit_band`

#### Scenario: Minimum holding period protects a young position

- **GIVEN** a held name below the exit band with `hold_count = 0`
- **WHEN** the plan is built with `min_hold_sessions = 2`
- **THEN** the name SHALL stay
- **AND** its reason SHALL be `held_min_period`
- **AND** once `hold_count >= 2` the same rank SHALL be sold

#### Scenario: Ineligible held name is force-sold

- **GIVEN** a held name with no usable prediction on the decision date
- **WHEN** the plan is built with any exit buffer
- **THEN** the name SHALL be sold with reason `sold_ineligible`

### Requirement: Sell reasons and holding metadata are persisted

Every plan SHALL record a per-code reason map (`held_in_exit_band`,
`held_min_period`, `sold_out_of_exit_band`, `sold_ineligible`, `added_by_rank`) and
per-holding metadata `{hold_count, entry_reason}`. `hold_count` SHALL increment for a
held name and start at 1 for a new one. The reason map SHALL be persisted as
`exit_reasons` and the metadata as `hold_meta`, while `rebalance` SHALL keep exactly
the `added`/`removed`/`unchanged` shape it has today and target holdings SHALL keep
exactly `{stock_code, weight}`.

#### Scenario: Persisted shapes stay compatible

- **GIVEN** a completed run with or without the policy
- **WHEN** the run document is written
- **THEN** `target_holdings` entries SHALL contain only `stock_code` and `weight`
- **AND** `rebalance` SHALL contain only `added`, `removed` and `unchanged`
- **AND** reasons and hold counts SHALL live in `exit_reasons` and `hold_meta`
