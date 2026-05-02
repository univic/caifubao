## ADDED Requirements

### Requirement: Datahub Runner Entry Points

Datahub SHALL provide operational entry points for quote and factor refreshes.

#### Scenario: Operator refreshes data manually

- **GIVEN** the live datahub runtime configuration is available
- **WHEN** an operator launches a runner manually
- **THEN** the runner SHALL reuse the same environment shape as deployment
- **AND** support dry-run execution where applicable.

### Requirement: Observable Job Runs

Quote and factor refreshes SHALL record observable job status.

#### Scenario: Quote refresh completes

- **GIVEN** a quote refresh job runs
- **WHEN** it completes or fails
- **THEN** the system SHALL record status, timing, phase statistics, and error details for freshness diagnosis.

### Requirement: Ownership Boundary

Market data refresh SHALL remain owned by datahub.

#### Scenario: Frontend needs fresh data

- **GIVEN** the frontend displays market or data-quality information
- **WHEN** data is stale
- **THEN** the frontend SHALL NOT invoke Mongo or runner logic directly
- **AND** backend SHALL NOT own quote refresh execution.
