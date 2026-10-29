# Strategy Research Pipeline

## ADDED Requirements

### Requirement: Evaluation MUST be pre-registered before it runs

Every research idea SHALL be registered before any evaluation, recording the hypothesis,
the data scope, the snapshot, the evaluation procedure, the success criteria, the
declared failure criteria the verdict will use, and a maximum number of trials. An
evaluation run that is not bound to a registration MUST NOT produce a research
conclusion, and the success criteria MUST NOT be modified after the first evaluation of
that registration.

#### Scenario: Unregistered evaluation produces no conclusion

- **GIVEN** an evaluation run with no registration identifier
- **WHEN** its results are recorded
- **THEN** no research conclusion SHALL be emitted
- **AND** the run SHALL be marked exploratory

#### Scenario: Criteria cannot be moved after the fact

- **GIVEN** a registration whose success criteria were recorded
- **WHEN** a later evaluation attempts to change them
- **THEN** the change SHALL be refused and recorded as an attempt

### Requirement: Every evaluated configuration MUST increment a trial counter

Each configuration evaluated against a registration SHALL increment that registration's
trial counter, and the confirmation-stage threshold SHALL be adjusted for the number of
trials recorded at the time of confirmation. A confirmation that reports a result without
applying the multiplicity adjustment MUST NOT be accepted as a research conclusion.

#### Scenario: Exploratory scan raises the bar for confirmation

- **GIVEN** a registration that has evaluated many configurations during exploration
- **WHEN** the confirmation stage evaluates the pre-registered configuration
- **THEN** the acceptance threshold SHALL be adjusted by the recorded trial count
- **AND** the adjusted threshold SHALL appear in the verdict

#### Scenario: Trial count is visible in the verdict

- **GIVEN** any verdict
- **WHEN** it is read
- **THEN** it SHALL report the number of trials consumed

### Requirement: Stages MUST terminate with a machine-readable verdict

The pipeline SHALL implement the ordered stages registration, feasibility, exploration,
confirmation, structural-and-capacity, forward and verdict. Each stage SHALL terminate
with a machine-readable outcome, and only confirmation and later stages MAY emit a
research conclusion while only the forward stage and later MAY emit a promotion
candidate. Exploration MUST NOT be able to emit either.

#### Scenario: Exploration cannot conclude

- **GIVEN** an idea that has only completed the exploration stage
- **WHEN** a verdict is requested
- **THEN** the verdict SHALL be labelled exploratory
- **AND** it SHALL NOT be usable for promotion

#### Scenario: Feasibility failure terminates early

- **GIVEN** a registration whose required data is absent or not point-in-time safe
- **WHEN** the feasibility stage runs
- **THEN** it SHALL terminate the idea with an explicit infeasibility verdict
- **AND** later stages SHALL NOT run

### Requirement: Structural and capacity gates MUST precede promotion candidacy

Before any verdict may be a promotion candidate, the candidate SHALL pass minimum
diversification, the declared turnover budget, the declared drawdown budget, the presence
of a declared stop-loss where the declaration requires one, and a capacity test that
re-evaluates the candidate at ten times the target capital. A capacity failure MUST be
reported as a finding and MUST NOT be silently absorbed.

#### Scenario: Capacity collapse blocks promotion

- **GIVEN** a candidate that passes confirmation at the target capital
- **WHEN** it is evaluated at ten times that capital
- **THEN** the capacity result SHALL be recorded in the verdict
- **AND** a collapse in the result SHALL prevent a promotion-candidate verdict

#### Scenario: Undeclared stop-loss blocks promotion

- **GIVEN** a concentrated candidate with no declared stop-loss
- **WHEN** the structural gate runs
- **THEN** the verdict SHALL be rejected

### Requirement: Verdicts MUST be immutable and traceable

Each verdict SHALL be an immutable artifact that records the registration identifier, the
declaration hash, the data snapshot, the code version, the trial count, the stage reached
and the outcome, and MUST be content-addressed so that a later edit is detected rather than
overwritten. A verdict SHALL be reproducible from the recorded inputs by anyone holding the
same snapshot and code version.

#### Scenario: Edited verdict is detected

- **GIVEN** a stored verdict
- **WHEN** an attempt is made to store different content under the same identity
- **THEN** the write SHALL be refused

#### Scenario: Verdict is reproducible

- **GIVEN** a verdict and its recorded snapshot and code version
- **WHEN** the pipeline is re-run with the same registration
- **THEN** the outcome SHALL reproduce, or the divergence SHALL be reported explicitly
