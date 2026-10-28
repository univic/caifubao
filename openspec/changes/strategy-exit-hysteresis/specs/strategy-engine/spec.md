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

#### Scenario: Declared hysteresis survives the legacy projection round trip

- **GIVEN** a spec that declares `exit_buffer` and/or `min_hold_sessions`
- **WHEN** it is projected to a legacy config and migrated back
- **THEN** re-attaching the declared policy SHALL reproduce the original spec hash
- **AND** the legacy config SHALL NOT carry the fields, so
  `legacy_config_hash` stays that of the pre-hysteresis config

### Requirement: Exit band and minimum holding period drive the target book

Given the previous book (the latest COMPLETED run for the same strategy name,
model version, horizon and `config_hash`; changing the config hash starts a fresh
book with no carried hold counts), selection SHALL keep a held name while its rank
is inside the exit band `ceil(portfolio_size × exit_buffer)`, SHALL keep a position
whose recorded `hold_count` is below `min_hold_sessions`, and SHALL sell a held name
that leaves the band once its minimum holding period has elapsed. A held name with
no usable prediction for the decision date SHALL be sold regardless of the policy.
Vacancies SHALL be filled from the remaining candidates by score, and the final book
SHALL still be capped at `portfolio_size` and ordered by score then code.

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

- **GIVEN** a held name below the exit band with `hold_count = 1`
- **WHEN** the plan is built with `min_hold_sessions = 2`
- **THEN** the name SHALL stay
- **AND** its reason SHALL be `held_min_period`
- **AND** once `hold_count >= 2` the same rank SHALL be sold

#### Scenario: Ineligible held name is force-sold

- **GIVEN** a held name with no usable prediction on the decision date (suspended,
  ST, BSE, unknown limit verdict, no score, or outside the configured percentile
  band or eligibility set)
- **WHEN** the plan is built with any exit buffer
- **THEN** the name SHALL be sold with reason `sold_ineligible`

### Requirement: Sell reasons and holding metadata are persisted

Every completed (non-skipped) plan SHALL record a per-code reason map
(`held_in_exit_band`, `held_min_period`, `sold_out_of_exit_band`, `sold_ineligible`,
`added_by_rank`) and per-holding metadata `{hold_count, entry_reason}`.
`hold_count` SHALL increment for a held name and start at 1 for a new one.
`entry_reason` SHALL be the reason the position was opened and SHALL be preserved
for the whole holding period; the outcome of the current run SHALL live only in the
reason map. The reason map SHALL be persisted as `exit_reasons` and the metadata as
`hold_meta`, while `rebalance` SHALL keep exactly the `added`/`removed`/`unchanged`
shape it has today and target holdings SHALL keep exactly `{stock_code, weight}`.

#### Scenario: Persisted shapes stay compatible

- **GIVEN** a completed run with or without the policy
- **WHEN** the run document is written
- **THEN** `target_holdings` entries SHALL contain only `stock_code` and `weight`
- **AND** `rebalance` SHALL contain only `added`, `removed` and `unchanged`
- **AND** reasons and hold counts SHALL live in `exit_reasons` and `hold_meta`

#### Scenario: Opening reason is not overwritten by later runs

- **GIVEN** a position opened with reason `added_by_rank`
- **WHEN** it is kept for several sessions inside the exit band
- **THEN** `hold_meta.entry_reason` SHALL stay `added_by_rank`
- **AND** each run's `exit_reasons` SHALL report `held_in_exit_band`

### Requirement: FORWARD identity and replay parity under hysteresis

A spec that declares the policy SHALL have its own identity, distinct from the
pre-hysteresis spec; it MUST NOT continue, backfill or accumulate hold state for an
already certified window with a different spec hash. The research replay adapter
(`replay_strategy_spec_decision`) SHALL apply the same policy as the paper runner so
that a replay of the same spec and book can never full-re-sort a book the paper
keeps.

#### Scenario: Hysteresis spec cannot inherit an older window

- **GIVEN** an ACTIVE certified window whose spec hash predates the policy
- **WHEN** a run is attempted with a spec that declares `exit_buffer`
- **THEN** its identity SHALL NOT match that window
- **AND** no run SHALL be classified FORWARD for it without a new certification

#### Scenario: Replay matches paper under hysteresis

- **GIVEN** a spec with `exit_buffer` and a previous book
- **WHEN** the decision is replayed and planned
- **THEN** both SHALL return the same target holdings and hold metadata
