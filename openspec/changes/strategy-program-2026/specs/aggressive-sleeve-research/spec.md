# Aggressive Sleeve Research

## ADDED Requirements

### Requirement: The aggressive sleeve MUST be capital-bounded and isolated

The aggressive sleeve SHALL have a declared maximum capital share, a separate accounting
identity, and a separate evidence identity, and its losses MUST NOT be able to consume the
stable sleeve's allocation. Sizing across sleeves SHALL be an explicit declaration rather
than an emergent result, and increasing the aggressive share MUST require a new
declaration and a new evidence window.

#### Scenario: Losses cannot cross sleeves

- **GIVEN** a stable sleeve and an aggressive sleeve with declared allocations
- **WHEN** the aggressive sleeve loses more than its declared share
- **THEN** the stable sleeve's allocation SHALL NOT be consumed
- **AND** the shortfall SHALL be reported

#### Scenario: Raising the aggressive share starts new evidence

- **GIVEN** an aggressive sleeve with a recorded capital share
- **WHEN** the share is increased
- **THEN** the declaration identity SHALL change
- **AND** prior forward sessions SHALL NOT count toward the new window

### Requirement: Trend claims MUST declare and honour their horizon

Any declaration whose hypothesis involves sustained directional appreciation SHALL declare
the evaluation horizon and MUST evaluate it at that horizon. Evidence obtained at a
different horizon MUST NOT be reused to support the claim, and a declaration MUST NOT
attribute a short-horizon cross-sectional reversal result to a long-horizon trend effect
or the reverse.

#### Scenario: Horizon mismatch is rejected

- **GIVEN** a declaration whose hypothesis is stated at one horizon but whose evidence was
  produced at another
- **WHEN** the declaration is validated for a window
- **THEN** validation SHALL fail

#### Scenario: Horizon is recorded with the evidence

- **GIVEN** a completed evaluation of a trend-style hypothesis
- **WHEN** its verdict is read
- **THEN** the evaluated horizon SHALL be recorded

### Requirement: New information MUST land through the data pipeline first

Any information set the sleeve relies on that is not already published SHALL be landed
through the data pipeline's fetch, normalize, validate, persist and publish stages, and
MUST carry point-in-time evidence for the date it is used. A signal derived from an
information set without a published snapshot MUST NOT be evaluated or used in a forward
window.

#### Scenario: Unpublished input blocks evaluation

- **GIVEN** a candidate that reads an information set with no published snapshot
- **WHEN** an evaluation is attempted
- **THEN** the evaluation SHALL fail closed

#### Scenario: Publication timestamp is recorded

- **GIVEN** a published information set
- **WHEN** it is consumed by a decision
- **THEN** the publication timestamp used for the point-in-time check SHALL be recorded

### Requirement: Large-model screening MUST be bounded and costed

Where a large model is used to screen candidates, the declaration SHALL bound the number
of evaluations per session and the number of candidates entering the model, and MUST
record the accumulated inference cost. The model MUST only rank or annotate candidates
produced by a cheaper first-stage filter, MUST NOT receive the full cross-section, and the
recorded cost SHALL be deducted from net performance.

#### Scenario: Full cross-section screening is refused

- **GIVEN** a declaration that sends the full cross-section to a large model
- **WHEN** the declaration is validated
- **THEN** validation SHALL fail and state the bounded-candidate requirement

#### Scenario: Cost is deducted and visible

- **GIVEN** a session whose large-model screening incurred cost
- **WHEN** net performance is reported
- **THEN** the inference cost SHALL be deducted
- **AND** it SHALL be visible separately from commissions and slippage

### Requirement: Aggressive candidates MUST pass the same evidence bar

An aggressive candidate SHALL pass the same forward-evidence window length as any other
candidate, and MUST additionally pass the capacity test at its declared target capital. A
candidate that has not passed both MUST NOT be described as profitable, validated or
promotable, and no return target MAY be stated as an expectation.

#### Scenario: Short evidence is insufficient

- **GIVEN** an aggressive candidate with fewer forward sessions than the required window
- **WHEN** promotion is considered
- **THEN** it SHALL be refused regardless of its backtest result

#### Scenario: No return target is promised

- **GIVEN** any artifact describing the aggressive sleeve
- **WHEN** it reports expectations
- **THEN** it SHALL NOT state a promised or targeted return multiple
