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
- **WHEN** the selected history source returns no valid quote rows within the frozen target range
- **THEN** the runner SHALL fail the quote phase
- **AND** an idempotent replay that validates existing rows SHALL NOT fail merely because it inserts zero new documents
- **AND** SHALL NOT continue to factor, signal, or scoring phases as if the refresh succeeded.

#### Scenario: Spot source returns no stock universe

- **GIVEN** an empty database is being initialized
- **WHEN** the stock spot source returns an empty universe
- **THEN** the runner SHALL fail the quote phase instead of reporting a successful zero-stock bootstrap.

#### Scenario: A listed stock is temporarily suspended

- **GIVEN** a supported stock remains present in the spot universe with a zero close price
- **WHEN** its history legitimately has no row for the frozen `as_of_date`
- **THEN** the runner SHALL retain the stock as active
- **AND** SHALL preserve its stale freshness state for diagnosis
- **AND** SHALL NOT fail the whole market refresh solely because the stock is suspended.

### Requirement: Stable Quote Run Snapshot

A quote run SHALL use one deterministic market-data snapshot.

#### Scenario: A quote run crosses midnight

- **GIVEN** a quote run starts before midnight and completes or retries after midnight
- **WHEN** its target trading day is resolved
- **THEN** the run SHALL freeze one `as_of_date` using `Asia/Shanghai`, the market close boundary, and the trading calendar
- **AND** that date SHALL remain unchanged for the logical run
- **AND** AkShare, Baostock, and any fallback SHALL receive the same inclusive end date
- **AND** records later than the frozen date SHALL be filtered or rejected.

### Requirement: Deployable Stock History Source

Stock history collection SHALL support an HTTPS-capable source for clusters
that cannot reach non-standard outbound TCP ports.

#### Scenario: Production uses HTTPS market data

- **GIVEN** the cluster can reach HTTPS market-data endpoints but cannot reach Baostock TCP port 10030
- **WHEN** the stock quote runner initializes or refreshes daily history
- **THEN** it SHALL use the configured HTTPS-capable source
- **AND** normalize source fields into the existing `StockDailyQuote` schema
- **AND** preserve Baostock as an explicit opt-in source rather than a required production dependency.

### Requirement: Idempotent Stock History Persistence

Stock history persistence SHALL be safely replayable after interruption.

#### Scenario: An interrupted bootstrap is retried

- **GIVEN** some `(code, date)` quote rows were persisted before the prior process stopped
- **WHEN** the same target range is replayed
- **THEN** existing identities SHALL be updated or validated without duplication
- **AND** missing identities SHALL be inserted
- **AND** quote freshness SHALL be recalculated from the final persisted rows
- **AND** rows outside the replay target range SHALL NOT be deleted.

### Requirement: Ownership Boundary

Market data refresh SHALL remain owned by datahub.

#### Scenario: Frontend needs fresh data

- **GIVEN** the frontend displays market or data-quality information
- **WHEN** data is stale
- **THEN** the frontend SHALL NOT invoke Mongo or runner logic directly
- **AND** backend SHALL NOT own quote refresh execution.
