# Strategy Signal Providers

## ADDED Requirements

### Requirement: One provider contract MUST serve every signal kind

Every signal source SHALL implement a single provider contract that returns, for a
decision date and a candidate universe, one record per stock containing the stock code, a
score, a percentile on a declared basis, and an optional rationale. Rule-based,
machine-learning and large-model providers MUST return the same schema, and the
downstream selection, sizing, risk and execution stages MUST NOT branch on the provider
kind.

#### Scenario: Three provider kinds produce the same schema

- **GIVEN** a rule provider, a model provider and a large-model provider registered for
  the same date and universe
- **WHEN** each is queried
- **THEN** each SHALL return records that validate against the identical schema
- **AND** every record SHALL carry a percentile on the declared basis

#### Scenario: Selection does not inspect the provider kind

- **GIVEN** two declarations identical except for the provider identity
- **WHEN** both are planned against the same scores
- **THEN** the target book SHALL be identical

#### Scenario: Missing percentile is not defaulted

- **GIVEN** a provider record with no percentile
- **WHEN** the plan is built
- **THEN** the stock SHALL be treated as unusable for that date and reported, not
  silently assigned a default

### Requirement: Provider identity MUST be immutable and content-addressed

Each provider result set SHALL have an immutable identity composed of the provider kind,
the signal source identity, and a digest over the produced records. A rule or model
provider MUST pin its artifact or configuration digest, and a large-model provider MUST
additionally pin the prompt digest and the model identifier. Recomputing a result set with
a different digest under the same identity MUST fail closed instead of overwriting.

#### Scenario: Mismatched digest is refused

- **GIVEN** a stored result set and a recomputation that produces a different digest
- **WHEN** the recomputation is stored under the same identity
- **THEN** the write SHALL be refused

#### Scenario: Prompt or model change creates a new identity

- **GIVEN** a large-model provider whose prompt or model identifier changes
- **WHEN** its result set is produced
- **THEN** its identity SHALL change
- **AND** the previous identity SHALL NOT be reused

#### Scenario: Rolling artifact resolution is recorded per session

- **GIVEN** a model provider whose declaration allows periodic refitting
- **WHEN** a session is evaluated
- **THEN** the artifact digest actually resolved for that session SHALL be recorded with
  the result set
- **AND** the registration of artifacts SHALL be append-only so that replaying an earlier
  session resolves the same artifact

### Requirement: Non-deterministic providers MUST be replayable

A provider whose output cannot be reproduced exactly SHALL record every external request
and response once, including the prompt, the model identifier, the response payload and
the response digest, and every subsequent evaluation of the same decision date MUST read
the recording instead of issuing a new external call. A result set produced without a
recording MUST NOT be admitted as forward evidence.

#### Scenario: Second evaluation performs no external call

- **GIVEN** a large-model provider that has recorded the responses for one decision date
- **WHEN** the same decision date is evaluated again
- **THEN** no external request SHALL be issued
- **AND** the produced records SHALL equal the recorded ones

#### Scenario: Unrecorded output is not evidence

- **GIVEN** a result set produced without a recorded request/response pair
- **WHEN** a forward classification is attempted
- **THEN** the run SHALL NOT be classified FORWARD

### Requirement: Signal providers MUST NOT issue orders or sizes

A signal provider SHALL only produce scores, percentiles and optional annotations. It MUST
NOT produce an order side, a quantity, a target weight, a cash movement or any instruction
that could be executed, and the provider schema MUST reject such fields. Position sizing
SHALL remain a function of the declaration, and approval SHALL remain the responsibility of
the risk gate.

#### Scenario: Provider output carrying an order is rejected

- **GIVEN** a provider record that includes a side, a quantity or a target weight
- **WHEN** the record is validated
- **THEN** validation SHALL fail

#### Scenario: Model advice cannot bypass the risk gate

- **GIVEN** a large-model recommendation to hold a name the risk gate rejects
- **WHEN** the target book is planned
- **THEN** the risk gate decision SHALL prevail

### Requirement: Provider cost MUST be part of net performance

Any provider whose evaluation incurs a monetary cost SHALL record that cost per decision
date and per decision, and the cost SHALL be included in net performance and in the
friction accounting alongside commissions, taxes, transfer fees and slippage. A backtest
that omits provider cost MUST NOT be reported as a net result.

#### Scenario: Inference cost reduces net return

- **GIVEN** a declaration whose provider costs money per evaluation
- **WHEN** a backtest is run
- **THEN** reported net return SHALL include the accumulated provider cost
- **AND** the cost SHALL be separately visible in the result

#### Scenario: Provider cost is bounded by declaration

- **GIVEN** a declaration that bounds the number of provider evaluations per session
- **WHEN** the candidate pool exceeds that bound
- **THEN** evaluation SHALL stop at the bound and report the truncation
