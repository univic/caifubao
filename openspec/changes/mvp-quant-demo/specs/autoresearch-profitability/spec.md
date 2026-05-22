## ADDED Requirements

### Requirement: Autoresearch Compatibility Profile

The system SHALL define a Caifubao-specific profile for using the Karpathy
autoresearch skill suite on scoring, factor, and strategy research.

#### Scenario: Autoresearch compatibility is diagnosed

- **GIVEN** Caifubao is a Flask, datahub, Vue, and MongoDB quantitative research
  MVP rather than a single-file training repository
- **WHEN** the project is evaluated for autoresearch usage
- **THEN** it SHALL be classified as `v1-bootstrap-fit`
- **AND** the required adapter boundary SHALL be documented before any
  experiment loop starts
- **AND** the adapter SHALL preserve existing datahub, backend, frontend, and
  OpenClaw module boundaries.

#### Scenario: Profile fields are frozen before experiments

- **GIVEN** an autoresearch experiment target is selected
- **WHEN** the target is prepared
- **THEN** the project SHALL freeze runtime command, timeout, metric name,
  metric direction, edit scope, readonly scope, baseline protocol, git policy,
  logging path, and results columns in an autoresearch spec or profile
- **AND** no field SHALL remain implicit or undefined.

### Requirement: Mechanical Profitability Research Metric

Autoresearch SHALL optimize only a single mechanical numeric metric extracted
from validation reports, not subjective assessment or manual judgment.

#### Scenario: Research profitability score is extracted

- **GIVEN** a completed score or strategy experiment report exists
- **WHEN** autoresearch evaluates whether to keep an experiment
- **THEN** it SHALL extract exactly one numeric `research_profitability_score`
  from the test-period report
- **AND** the score SHALL combine net excess return, information ratio, maximum
  drawdown penalty, turnover penalty, concentration penalty, sample-size
  penalty, and overfit penalty
- **AND** the extracted output SHALL match a plain numeric pattern with no
  percent sign, unit suffix, prose, or multi-line content.

#### Scenario: Profitability metric does not imply investment advice

- **GIVEN** an experiment improves `research_profitability_score`
- **WHEN** the result is displayed, exported, or reviewed
- **THEN** it SHALL be described as research evidence
- **AND** it SHALL NOT be described as guaranteed profit, investment advice,
  trading advice, or a financial service recommendation.

### Requirement: Autoresearch Edit Scope Guardrails

The autoresearch loop SHALL be limited to research-safe files and SHALL NOT
modify production contracts or deployment surfaces.

#### Scenario: Initial edit scope is enforced

- **GIVEN** autoresearch starts an experiment loop
- **WHEN** it proposes file edits
- **THEN** edits SHALL be limited to approved research configuration files,
  candidate scoring weights, threshold profiles, experiment profiles, and
  candidate factor drafts
- **AND** edits SHALL NOT touch authentication, OpenClaw endpoints, public API
  response contracts, Kubernetes manifests, secrets, or production default
  model version changes.

#### Scenario: Production promotion requires normal gates

- **GIVEN** autoresearch keeps an experiment as an improvement
- **WHEN** the result is considered for product behavior
- **THEN** promotion SHALL require OpenSpec review, model version bump when
  scoring semantics change, full-market calibration comparison, focused tests,
  and the normal reviewer gates
- **AND** the experiment result alone SHALL NOT update production defaults.

### Requirement: Baseline and Anti-overfitting Validation

Autoresearch experiments SHALL preserve the existing anti-overfitting and
market-wide validation requirements before any result can be kept.

#### Scenario: Baseline runs first

- **GIVEN** a new autoresearch target is approved
- **WHEN** bootstrap runs
- **THEN** it SHALL execute the baseline validation command before the first
  experiment
- **AND** record the baseline report path, metric value, git ref, and data
  date range.

#### Scenario: Kept experiments pass validation gates

- **GIVEN** an experiment produces a better mechanical metric
- **WHEN** autoresearch classifies it as `keep`
- **THEN** the experiment SHALL pass train/validation/test split checks,
  full-market or top-50 stock validation, walk-forward decay checks, minimum
  trade count checks, and concentration checks
- **AND** failed checks SHALL force the experiment to be discarded or flagged
  as overfit.

#### Scenario: Failed experiments remain auditable

- **GIVEN** an experiment crashes or fails validation
- **WHEN** the autoresearch loop records the result
- **THEN** it SHALL append the rejection or crash reason to the ledger
- **AND** preserve enough metadata to audit model-selection bias and repeated
  failed directions.
