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

#### Scenario: Quote source is unreachable

- **GIVEN** a quote refresh attempts to update one or more active stocks
- **WHEN** the selected history source writes zero quote rows
- **THEN** the runner SHALL fail the quote phase
- **AND** SHALL NOT continue to factor, signal, or scoring phases as if the refresh succeeded.

### Requirement: Deployable Stock History Source

Stock history collection SHALL support an HTTPS-capable source for clusters
that cannot reach non-standard outbound TCP ports.

#### Scenario: Production uses HTTPS market data

- **GIVEN** the cluster can reach HTTPS market-data endpoints but cannot reach Baostock TCP port 10030
- **WHEN** the stock quote runner initializes or refreshes daily history
- **THEN** it SHALL use the configured HTTPS-capable source
- **AND** normalize source fields into the existing `StockDailyQuote` schema
- **AND** preserve Baostock as an explicit opt-in source rather than a required production dependency.

### Requirement: Ownership Boundary

Market data refresh SHALL remain owned by datahub.

#### Scenario: Frontend needs fresh data

- **GIVEN** the frontend displays market or data-quality information
- **WHEN** data is stale
- **THEN** the frontend SHALL NOT invoke Mongo or runner logic directly
- **AND** backend SHALL NOT own quote refresh execution.
