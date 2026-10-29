# Allocation Forward Window

## ADDED Requirements

### Requirement: Allocation windows MUST be keyed by strategy, not by a scoring model

A forward-evidence window for an allocation strategy SHALL be identified by a
`strategy_id` together with its `spec_hash`, and MUST NOT require a
`score_model_version`. An allocation declaration produces target weights from a
declared asset set and band; it has no scoring horizon, so keying its window by
`(model_version, horizon)` would either force a placeholder model version or make
the window unrepresentable. Allocation windows SHALL live in their own
append-only collection and MUST NOT be stored as scoring-model windows.

#### Scenario: An allocation window needs no model version

- **GIVEN** an allocation declaration with a fixed asset set and a family of
  `asset_allocation`
- **WHEN** a forward window is certified for it
- **THEN** the window SHALL record `strategy_id` and `spec_hash`
- **AND** it SHALL NOT require or record a scoring `model_version`

#### Scenario: The two window kinds are disjoint

- **GIVEN** an ACTIVE scoring-model window and an ACTIVE allocation window
- **WHEN** either is certified or closed
- **THEN** the other SHALL remain untouched
- **AND** no allocation window SHALL appear in scoring-window progress output

### Requirement: Certification MUST close only same-strategy predecessors

Certifying an allocation window SHALL close the ACTIVE predecessor for the same
`strategy_id` only, and MUST NOT close a window belonging to another strategy or
to a scoring-model pair. `start_date` SHALL be the certification session and MUST
NOT be backdated. Because the window is the only place the 120-session clock can
start, a certification that cannot produce plans on the same data MUST be refused
rather than opened.

#### Scenario: A different strategy keeps its window

- **GIVEN** an ACTIVE allocation window for `sleeve-stable`
- **WHEN** a window is certified for `sleeve-aggressive`
- **THEN** `sleeve-stable` SHALL remain ACTIVE

#### Scenario: Re-certifying the same strategy closes its predecessor

- **GIVEN** an ACTIVE window for `sleeve-stable` with a different `spec_hash`
- **WHEN** a new window is certified for `sleeve-stable`
- **THEN** the predecessor SHALL be closed and the new window SHALL start today

#### Scenario: Certification without an executable plan is refused

- **GIVEN** no allocation run path that can produce a plan for the declaration
- **WHEN** certification is attempted
- **THEN** it SHALL be refused with an explanation, because opening the clock
  without a way to fill it wastes the only irreversible resource

### Requirement: Only matching, decision-first runs count as forward evidence

A plan SHALL count toward an allocation window only when its `strategy_id` and
`spec_hash` match the ACTIVE window and its decision precedes the execution
session. Replays, backfilled plans, replaced plans, NAV recomputations and job
successes MUST NOT advance the count. Progress SHALL report observed sessions,
gaps and the remaining sessions to the requirement.

#### Scenario: A mismatched plan does not count

- **GIVEN** an ACTIVE window whose `spec_hash` differs from a plan's
- **WHEN** progress is reported
- **THEN** that plan SHALL NOT be counted

#### Scenario: A missing session is a permanent gap

- **GIVEN** an ACTIVE window covering a stretch of trading sessions
- **WHEN** a session has no qualifying plan
- **THEN** progress SHALL list it as a gap
- **AND** the gap SHALL NOT be fillable by a later backfill

### Requirement: Allocation evidence MUST carry its accounting and benchmark identity

An allocation window's evidence SHALL record the accounting policy and the
benchmark under which the sleeve is measured, because an excess number is
uninterpretable without them and two runs under different policies cannot be
compared. A window whose declaration omits either MUST NOT be certified.

#### Scenario: Missing accounting identity blocks certification

- **GIVEN** an allocation declaration without an accounting policy
- **WHEN** certification is attempted
- **THEN** it SHALL be refused

#### Scenario: Changing the benchmark starts a new window

- **GIVEN**
  a certified window with one benchmark and its plan history
- **WHEN** the declaration's benchmark changes
- **THEN** the spec hash SHALL change and the existing evidence SHALL NOT carry over

### Requirement: Allocation windows MUST NOT authorize trading

An allocation window SHALL remain research evidence. It MUST NOT enable
`trading-paper` or `prod`, place an order, or be read as a trading account. The
existing prohibition on real-money execution SHALL apply unchanged.

#### Scenario: A window does not grant execution

- **GIVEN** an ACTIVE allocation window with twelve forward sessions
- **WHEN** any execution path is consulted
- **THEN** it SHALL remain disabled regardless of the window's state
