## ADDED Requirements

### Requirement: Standard Signal Model

The signal layer SHALL store new daily signals in `StockSignalDaily`.

#### Scenario: Datahub generates a moving-average signal

- **GIVEN** current quote and MA factor data exists
- **WHEN** datahub detects a supported signal
- **THEN** it SHALL write a `StockSignalDaily` record
- **AND** include direction, signal type, strength, reason, factor snapshot, and source freshness.

### Requirement: MVP Signal Set

The MVP signal set SHALL be small, stable, and explainable.

#### Scenario: Supported signals are generated

- **GIVEN** MA factor data exists for a stock
- **WHEN** signal generation runs
- **THEN** it SHALL support `MA10_CROSS_MA20`
- **AND** leave room for `PRICE_ABOVE_MA60` and `MA20_ABOVE_MA60`.

### Requirement: Signal Read APIs

The backend SHALL provide read-only access to generated signals.

#### Scenario: User views signals

- **GIVEN** dated `StockSignalDaily` records exist
- **WHEN** the frontend requests `/api/signals`
- **THEN** the backend SHALL return a dated list with direction, strength, reason, price snapshot, factor snapshot, and freshness data.

#### Scenario: OpenClaw reads signals

- **GIVEN** OpenClaw has a valid read token
- **WHEN** it requests the OpenClaw signals endpoint
- **THEN** the backend SHALL return the same signal facts through the integration contract.
