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

### Requirement: Declarations MUST declare data requirements, not a fixed snapshot

A declaration SHALL declare the data contract it requires — market and asset class,
calendar, timezone, point-in-time policy, missing-data policy and freshness/quality gates
— and MUST NOT pin one specific data snapshot as part of its identity. The snapshot that
was actually used SHALL be recorded by the evidence or run that consumed it, so that a
daily forward session can receive new data without changing the strategy identity while a
research verdict remains reproducible.

#### Scenario: Declaration is snapshot-independent

- **GIVEN** a certified declaration and two different daily snapshots
- **WHEN** each snapshot is used for a forward session
- **THEN** the declaration identity SHALL be unchanged
- **AND** each run SHALL record the snapshot it consumed

#### Scenario: Verdict remains reproducible

- **GIVEN** a research verdict
- **WHEN** it is read
- **THEN** it SHALL name both the declaration identity and the dataset identity it used
- **AND** a re-run with a different dataset SHALL NOT be presented as the same result

#### Scenario: Missing data contract is rejected

- **GIVEN** a v2 declaration with no timezone or no point-in-time policy
- **WHEN** the declaration is validated
- **THEN** validation SHALL fail

### Requirement: Execution semantics MUST bind to the declared version

Consumption of a declared field SHALL be selected by `spec_version`, and MUST NOT be
selected by the mere presence of the field. A `spec_version: "1"` declaration SHALL keep
executing with its historical semantics — in particular `rebalance.cadence_days` SHALL
remain validated-but-unconsumed and the daily path SHALL NOT begin consuming `risk.*` for
it — so that enabling new consumers cannot silently change a certified window's decision
cadence while its hashes stay identical. `spec_version: "2"` SHALL consume every field
the manifest declares.

#### Scenario: Legacy window keeps its cadence

- **GIVEN** the certified `01c196b8…` window whose declaration carries
  `rebalance.cadence_days = 5`
- **WHEN** daily runs continue after v2 consumers are implemented
- **THEN** those runs SHALL still re-plan every session rather than every fifth session

#### Scenario: Continuous multi-session regression, not hash equality

- **GIVEN** a v1 declaration and a recorded sequence of at least five consecutive sessions
- **WHEN** the implementation is changed to add v2 consumers
- **THEN** the target book, the order list and the ledger projection SHALL be identical
  for every session in the sequence
- **AND** hash equality alone SHALL NOT be accepted as the verification

#### Scenario: Version, not field presence, selects semantics

- **GIVEN** a v2 declaration that omits an optional field present in a v1 declaration
- **WHEN** the declaration is executed
- **THEN** v2 semantics SHALL apply
- **AND** a v1 declaration carrying the same field SHALL still use v1 semantics

### Requirement: Strategy family MUST determine the required book semantics

A declaration SHALL declare its strategy family, at least distinguishing rank-based
selection from asset allocation. Each family MUST require the semantics it actually uses
and MUST reject semantics that do not apply to it: a rank-selection declaration SHALL
declare its ranking exit conditions, while an allocation declaration SHALL declare target
weights, rebalance bands and a genuinely consumed rebalance cadence and MUST NOT be
required to invent a rank-based exit. Risk, execution, accounting, ledger and evidence
contracts SHALL be shared by all families.

#### Scenario: Allocation is not forced to declare a rank exit

- **GIVEN** an allocation declaration with a fixed asset set, target weights and bands
- **WHEN** the declaration is validated
- **THEN** it SHALL pass without declaring rank-based exit conditions

#### Scenario: Rank selection must declare its exits

- **GIVEN** a rank-selection declaration with no exit conditions
- **WHEN** the declaration is validated
- **THEN** validation SHALL fail

#### Scenario: Inapplicable semantics are rejected

- **GIVEN** an allocation declaration that declares a rank-based exit, or a rank-selection
  declaration that declares allocation bands
- **WHEN** the declaration is validated
- **THEN** validation SHALL fail rather than silently ignore the field

### Requirement: Cross-mode conformance MUST compare decision state chains

Cross-mode equivalence SHALL be established over a `DecisionState` input that includes
held positions with average cost, hold counts, entry reasons, portfolio high-water mark,
cash and outstanding orders. Equality of target books SHALL only be required when the
whole input tuple — declaration, snapshot, provider outputs, session and `DecisionState` —
is identical, and every run MUST record a `decision_state_hash` so that a divergence
between modes can be attributed to a specific fill rather than to the decision logic.

#### Scenario: Different state permits different targets

- **GIVEN** two runs with the same declaration, snapshot and session but different
  starting positions or cost bases
- **WHEN** the target book is planned
- **THEN** the books MAY differ
- **AND** no conformance failure SHALL be raised

#### Scenario: Divergence is attributed to fills

- **GIVEN** a backtest and a paper run that agree for several sessions then diverge
- **WHEN** the divergence is investigated
- **THEN** the recorded `decision_state_hash` and fill records SHALL identify the
  first diverging session
- **AND** the divergence SHALL NOT be reported as a decision-logic inconsistency

#### Scenario: Stop-loss depends on recorded cost basis

- **GIVEN** a declaration with a stop-loss and a held position whose average cost differs
  between two modes
- **WHEN** each mode plans the session
- **THEN** each mode SHALL apply the stop-loss from its own recorded cost basis

### Requirement: Structural admission gate MUST reject doomed configurations

Validation SHALL apply a structural admission gate that rejects declarations that cannot
be evaluated honestly: a minimum number of holdings, a maximum annual turnover budget
derived from the rebalance cadence and the exit policy, a drawdown budget, and a
requirement that any book below a declared concentration threshold declares a stop-loss.
The gate MUST be evaluated before a declaration may open a forward window, and its
thresholds MUST be part of the declaration so that changing them is a visible identity
change.

#### Scenario: Too few holdings is rejected

- **GIVEN** a declaration whose `portfolio_size` is below the configured minimum holdings
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
readable, replayable, archivable and executable under its own semantics, but MUST NOT be
used to open a new forward window. The certified research window `01c196b8…` and its
legacy projection SHALL be pinned by a regression test that fails if either hash changes.

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
