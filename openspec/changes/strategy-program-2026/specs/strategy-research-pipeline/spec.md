# Strategy Research Pipeline

## ADDED Requirements

### Requirement: Evaluation MUST be pre-registered before it runs

Every research idea SHALL be registered before any evaluation, recording the hypothesis,
the hypothesis family it belongs to, the data scope, the evaluation procedure, the success
criteria, the failure criteria the verdict will use, the confirmation slice, and a maximum
number of trials and confirmation runs. An evaluation run that is not bound to a
registration MUST NOT produce a research conclusion, and the success criteria and the
confirmation slice MUST NOT be modified after the first evaluation of that registration.

#### Scenario: Unregistered evaluation produces no conclusion

- **GIVEN** an evaluation run with no registration identifier
- **WHEN** its results are recorded
- **THEN** no research conclusion SHALL be emitted
- **AND** the run SHALL be marked exploratory

#### Scenario: Criteria cannot be moved after the fact

- **GIVEN** a registration whose success criteria were recorded
- **WHEN** a later evaluation attempts to change them
- **THEN** the change SHALL be refused and recorded as an attempt

#### Scenario: Confirmation slice is frozen at registration

- **GIVEN** a registration that recorded its confirmation dates and indices
- **WHEN** a later run attempts to widen or shift that slice
- **THEN** the change SHALL be refused

### Requirement: The confirmation slice MUST be isolated from exploration

The exploration stage SHALL NOT read the prices, labels or results of the confirmation
slice beyond what is needed to prove the data exists. The causal cutoff used to fit or
select anything for the confirmation run MUST be at most the confirmation start minus the
label horizon minus one session minus the declared embargo, and a negative embargo MUST be
rejected. A confirmation whose cutoff overlaps the confirmation window MUST be refused,
and the cutoff, the label horizon and the embargo MUST be recorded in the verdict.

#### Scenario: Overlapping labels are refused

- **GIVEN** a confirmation that fits or selects using data whose label horizon reaches into
  the confirmation window
- **WHEN** the confirmation runs
- **THEN** it SHALL fail closed rather than report a result

#### Scenario: Negative embargo is refused

- **GIVEN** a registration declaring a negative embargo
- **WHEN** the registration is validated
- **THEN** it SHALL be rejected

#### Scenario: Exploration cannot leak the confirmation outcome

- **GIVEN** a registration whose confirmation slice is recorded
- **WHEN** an exploratory evaluation is executed
- **THEN** it SHALL NOT read confirmation-period outcomes
- **AND** a run that does SHALL be marked contaminated and unable to conclude

### Requirement: Trials MUST be attributed to the hypothesis family

Each configuration evaluated SHALL increment the trial counter of its **hypothesis family**,
and a new registration belonging to an existing family MUST inherit that family's
cumulative trial count. The confirmation-stage threshold SHALL be adjusted for the
family-level count, the number of confirmation runs SHALL be limited by the declared
maximum for the family, and a registration that would exceed that maximum MUST be refused.
Re-registering the same hypothesis under a new identifier SHALL NOT create a fresh trial
budget.

#### Scenario: Re-registration does not reset the count

- **GIVEN** a hypothesis family with many recorded trials
- **WHEN** a new registration is created for the same family
- **THEN** its effective trial count SHALL include the family's earlier trials
- **AND** the adjusted threshold SHALL reflect that count

#### Scenario: Confirmation budget is exhausted

- **GIVEN** a family whose confirmation runs reached its declared maximum
- **WHEN** another confirmation is attempted
- **THEN** it SHALL be refused
- **AND** the refusal SHALL be recorded

#### Scenario: Trial count is visible in the verdict

- **GIVEN** any verdict
- **WHEN** it is read
- **THEN** it SHALL report both the family-level and the registration-level trial count

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
hypothesis family, the declaration hash, the dataset identity, the code version, the
family-level and registration-level trial counts, the confirmation counts, the causal
cutoff with its label horizon and embargo, the stage reached and the outcome, and MUST be
content-addressed so that a later edit is detected rather than overwritten. A verdict
SHALL be reproducible from the recorded inputs by anyone holding the same dataset and code
version.

#### Scenario: Edited verdict is detected

- **GIVEN** a stored verdict
- **WHEN** an attempt is made to store different content under the same identity
- **THEN** the write SHALL be refused

#### Scenario: Verdict is reproducible

- **GIVEN** a verdict and its recorded dataset and code version
- **WHEN** the pipeline is re-run with the same registration
- **THEN** the outcome SHALL reproduce, or the divergence SHALL be reported explicitly
