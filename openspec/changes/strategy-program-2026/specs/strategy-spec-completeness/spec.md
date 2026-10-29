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
and MUST reject semantics that do not apply to it, and role semantics such as risk limits,
execution assumptions, accounting policy, ledger events and evidence rules SHALL be shared
by all families rather than forked per family.

#### Scenario: Family is mandatory

- **GIVEN** a v2 declaration with no strategy family
- **WHEN** the declaration is validated
- **THEN** validation SHALL fail

#### Scenario: Inapplicable semantics are rejected

- **GIVEN** an allocation declaration that declares a rank-based exit, or a rank-selection
  declaration that declares allocation bands
- **WHEN** the declaration is validated
- **THEN** validation SHALL fail rather than silently ignore the field

#### Scenario: Shared role contracts are not forked

- **GIVEN** one rank-selection and one allocation declaration
- **WHEN** each is planned
- **THEN** both SHALL go through the same risk gate, execution model, accounting policy and
  ledger event schema

### Requirement: Rank-selection books MUST declare their full exit contract

A rank-selection declaration SHALL declare, and the planner MUST consume, all of: the exit
band expressed as the rank beyond which a held name is sold, a per-position stop-loss
fraction, a maximum holding period in sessions, and the action taken when a held name loses
eligibility. Each declared condition MUST produce a recorded sell reason, and no exit
behaviour may depend on an implicit consequence of ranking order. Mandatory exits — the
stop-loss, the maximum holding period and the loss of eligibility — SHALL take precedence
over every retention rule, so the exit band and the minimum holding period MUST NOT keep a
position that a mandatory exit condition has triggered.

#### Scenario: Omitted exit contract is rejected

- **GIVEN** a rank-selection declaration missing any of the exit band, the stop-loss, the
  maximum holding period or the ineligibility action
- **WHEN** the declaration is validated
- **THEN** validation SHALL fail naming the missing field

#### Scenario: Maximum holding period is enforced

- **GIVEN** a declaration with a maximum holding period and a held name whose recorded hold
  count exceeds it while still ranking inside the entry band
- **WHEN** the target book is planned
- **THEN** the name SHALL be sold and the reason SHALL be recorded

#### Scenario: Stop-loss uses the recorded cost basis

- **GIVEN** a declaration with a stop-loss and a held name whose close is below its recorded
  average cost by more than the declared fraction
- **WHEN** the target book is planned
- **THEN** the name SHALL be sold with a stop-loss reason
- **AND** a declaration without the field SHALL NOT sell that name for that reason

#### Scenario: Ineligible held name is sold regardless of the band

- **GIVEN** a held name with no usable score on the decision date, for example because it is
  suspended, suspended-then-ST, outside the eligibility set or missing a prediction
- **WHEN** the target book is planned with any exit band
- **THEN** the name SHALL be sold with an ineligibility reason
- **AND** the exit band SHALL NOT keep it

#### Scenario: Exit band keeps a slowing name

- **GIVEN** a held name ranked beyond the entry band but inside the declared exit band, and
  no mandatory exit condition — stop-loss, maximum holding period or loss of eligibility —
  has been triggered
- **WHEN** the target book is planned
- **THEN** the name SHALL stay and the reason SHALL record that it was held inside the band

#### Scenario: Mandatory exit overrides the band

- **GIVEN** a held name ranked inside the declared exit band — and, where a minimum holding
  period is declared, still inside it — whose recorded cost basis triggers the stop-loss or
  whose hold count exceeds the maximum holding period
- **WHEN** the target book is planned
- **THEN** the name SHALL be sold with the mandatory exit reason
- **AND** neither the exit band nor the minimum holding period SHALL keep it


#### Scenario: Contradictory holding windows are rejected

- **GIVEN** a declaration that declares a maximum holding period and whose minimum holding
  period exceeds it
- **WHEN** the declaration is validated
- **THEN** validation SHALL fail, because the mandatory exit would fire before the retention
  floor could ever protect a position and the declared minimum would be silently clamped to
  the maximum
- **AND** a declared maximum that is absent, or a minimum equal to the maximum, SHALL remain
  valid

### Requirement: Allocation books MUST declare weights, bands, cadence and a per-asset stop

An allocation declaration SHALL declare its fixed asset set, target weights, rebalance
bands, a rebalance cadence that a consumer genuinely reads, and a **single-asset
stop-loss** that applies to every held asset. It MUST NOT be required to declare a
rank-based exit, and the per-asset stop MUST NOT be satisfied by a portfolio-level
de-risking rule alone. The per-asset stop SHALL take precedence over the rebalance bands, so
a band MUST NOT keep an asset that has breached its stop.

#### Scenario: Allocation needs no rank exit

- **GIVEN** an allocation declaration with a fixed asset set, target weights, bands and a
  per-asset stop-loss
- **WHEN** the declaration is validated
- **THEN** it SHALL pass without any rank-based exit field

#### Scenario: Per-asset stop-loss is mandatory

- **GIVEN** an allocation declaration with portfolio de-risking but no per-asset stop-loss
- **WHEN** the declaration is validated
- **THEN** validation SHALL fail

#### Scenario: Per-asset stop is applied

- **GIVEN** an allocation declaration with a per-asset stop and an asset below it relative
  to its recorded cost basis
- **WHEN** a rebalance session is planned
- **THEN** that asset SHALL be reduced or closed with a recorded stop reason
- **AND** a portfolio de-risking rule triggering in the same session SHALL NOT cancel it

#### Scenario: Unconsumed cadence is rejected

- **GIVEN** an allocation declaration whose rebalance cadence no consumer reads
- **WHEN** the declaration is validated
- **THEN** validation SHALL fail

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

### Requirement: Entries MUST NOT exceed the cap and unexecuted exits MUST reserve slots

The planner MUST NOT emit a target book larger than the declared position cap. A held
position whose exit did not execute — because the name was suspended, moved to a limit, had
an unknown limit verdict, or its order remained unexecuted — MUST be carried into the next
`DecisionState` and MUST occupy a slot, so the number of newly opened positions is reduced
by the number of unsellable holdings. The plan chain SHALL be derived from the account
projection rather than from the previous target book, so an unexecuted exit cannot become
invisible to the next decision, and every blocked exit MUST be recorded as a rejection with
a reason code.

#### Scenario: A target book never exceeds the cap

- **GIVEN** any decision state, including one holding positions that could not be sold
- **WHEN** the target book is planned
- **THEN** its size SHALL NOT exceed the declared position cap

#### Scenario: An unexecuted exit reserves a slot

- **GIVEN** a cap of twenty and one held name whose exit was blocked in the previous session
- **WHEN** the next session is planned
- **THEN** that name SHALL appear in the `DecisionState` as a held position
- **AND** at most nineteen newly opened positions SHALL be planned

#### Scenario: The plan chain follows the account projection

- **GIVEN** a session whose exit was blocked
- **WHEN** the next session loads its previous holdings
- **THEN** they SHALL come from the account or ledger projection
- **AND** they SHALL NOT be read from the previous target book alone

#### Scenario: Blocked exits are recorded

- **GIVEN** an exit refused by suspension, a limit move or an unknown limit verdict
- **WHEN** the session is recorded
- **THEN** the refusal SHALL appear as a rejection carrying a reason code
- **AND** it SHALL be visible in the run output rather than silently skipped

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
