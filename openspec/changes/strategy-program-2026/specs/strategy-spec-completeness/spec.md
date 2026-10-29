# Strategy Spec Completeness

## ADDED Requirements

### Requirement: Every declared field MUST have a consumer

The strategy declaration SHALL be accompanied by a consumption manifest that maps every
allowed field path to the module that reads it and to a golden test that pins its effect.
`spec_version: "2"` MUST reject a declaration whose field path is absent from the
manifest, and CI MUST fail when the manifest and the validator's allowed-field set
diverge in either direction. A field that is validated and hashed but unread by any
consumer MUST be treated as a defect, not as a documented limitation.

#### Scenario: Unconsumed field is rejected

- **GIVEN** a v2 declaration containing a field that no manifest entry names
- **WHEN** the declaration is validated
- **THEN** validation SHALL fail with an error naming the offending field path

#### Scenario: Manifest and validator drift fails CI

- **GIVEN** a manifest entry for a field the validator no longer allows, or an allowed
  field with no manifest entry
- **WHEN** the manifest consistency check runs
- **THEN** it SHALL exit non-zero and list both sides of the divergence

#### Scenario: Consumed field is pinned by a golden test

- **GIVEN** a manifest entry that declares a consumer and a golden test
- **WHEN** the golden test is removed or no longer references the field
- **THEN** the manifest check SHALL fail

### Requirement: Declarations MUST carry data and model identity

A v2 declaration SHALL pin the data snapshot and the signal model configuration it was
evaluated against: a `data.snapshot_id`, a `data.calendar`, a `data.timezone`, explicit
missing-data policies, a `signal.score_basis`, and a `signal.model_config_hash`. The
declaration MUST reject an unresolvable or absent snapshot identifier, and MUST NOT
accept a model configuration hash that does not match the registered model version it
names.

#### Scenario: Missing snapshot pin is rejected

- **GIVEN** a v2 declaration with no `data.snapshot_id`
- **WHEN** the declaration is validated
- **THEN** validation SHALL fail

#### Scenario: Model hash must match the named version

- **GIVEN** a declaration naming `flip_wide_shadow_v1` with a `model_config_hash` that
  differs from the registered configuration hash
- **WHEN** the declaration is validated
- **THEN** validation SHALL fail without scoring or planning

#### Scenario: Universe is no longer a fixed equality assertion

- **GIVEN** a v1 declaration with `universe == {"market": "CN_A", "as_of": "PIT"}`
- **WHEN** it is read for historical replay
- **THEN** it SHALL remain readable and hashed byte-identically
- **AND** a v2 declaration SHALL be able to name a non-stock asset class

### Requirement: Exit semantics MUST be declared explicitly

A v2 declaration SHALL declare its exit policy in a dedicated block containing at least a
rank-based exit condition, an optional stop-loss fraction, an optional maximum holding
period in sessions, and the action taken when a held name becomes ineligible. Omission of
the block MUST be a validation error for `spec_version: "2"`. Downstream planning MUST
consume every declared exit condition, and no exit behaviour may depend on an implicit
consequence of ranking order.

#### Scenario: Omitted exit block is rejected

- **GIVEN** a v2 declaration with no exit block
- **WHEN** the declaration is validated
- **THEN** validation SHALL fail with an error naming the missing block

#### Scenario: Declared stop-loss is consumed

- **GIVEN** a declaration with `stop_loss_pct` set and a held name whose close is below
  the entry mark by more than that fraction
- **WHEN** the target book is planned
- **THEN** the name SHALL be sold with a stop-loss reason
- **AND** a declaration without the field SHALL NOT sell that name for that reason

#### Scenario: Declared maximum holding period is consumed

- **GIVEN** a declaration with `max_hold_sessions` set and a held name whose recorded
  hold count exceeds it while still ranking inside the entry band
- **WHEN** the target book is planned
- **THEN** the name SHALL be sold and the reason SHALL be recorded

### Requirement: Structural admission gate MUST reject doomed configurations

Validation SHALL apply a structural admission gate that rejects declarations that cannot
be evaluated honestly: a minimum number of holdings, a maximum annual turnover budget
derived from `cadence_days` and the exit policy, a drawdown budget, and a requirement
that any book below a declared concentration threshold declares a stop-loss. The gate
MUST be evaluated before a declaration may open a forward window, and its thresholds MUST
be part of the declaration so that changing them is a visible identity change.

#### Scenario: Too few holdings is rejected

- **GIVEN** a declaration whose `portfolio_size` is below the configured minimum
  holdings
- **WHEN** the declaration is validated for a new window
- **THEN** validation SHALL fail and state the configured minimum

#### Scenario: Concentrated book without a stop-loss is rejected

- **GIVEN** a declaration below the concentration threshold and without a stop-loss
- **WHEN** the declaration is validated for a new window
- **THEN** validation SHALL fail

#### Scenario: Turnover budget is enforced before planning

- **GIVEN** a declaration whose implied annual turnover exceeds its declared budget
- **WHEN** a planning run is attempted
- **THEN** the run SHALL fail closed rather than produce a target book

### Requirement: Versioned semantics MUST preserve certified identities

A semantic change SHALL require a new `spec_version` or a new field, and MUST NOT alter
the normalized bytes of an already certified declaration. A v1 declaration SHALL remain
readable, replayable and archivable, but MUST NOT be used to open a new forward window.
The certified research window `01c196b8…` and its legacy projection SHALL be pinned by a
regression test that fails if either hash changes.

#### Scenario: v1 window identity is pinned

- **GIVEN** the certified v1 declaration
- **WHEN** it is normalized, hashed and projected to a legacy config
- **THEN** `spec_hash` and `legacy_config_hash` SHALL equal their recorded values

#### Scenario: v1 cannot open a new window

- **GIVEN** an attempt to certify a new window with `spec_version: "1"`
- **WHEN** the certification runs
- **THEN** it SHALL refuse and direct the operator to a v2 declaration

#### Scenario: Semantic change without a version bump is rejected

- **GIVEN** a v2 declaration that relies on newly consumed semantics while declaring a
  field set identical to v1
- **WHEN** the declaration is validated
- **THEN** validation SHALL fail rather than silently reinterpret the v1 field set
