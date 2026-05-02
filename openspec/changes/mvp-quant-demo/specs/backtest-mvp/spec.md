## ADDED Requirements

### Requirement: Lightweight Single-stock Backtest

The system SHALL provide an MVP backtest capability for one stock and one simple daily-bar strategy.

#### Scenario: User runs an MA-cross backtest

- **GIVEN** daily quote and moving-average factor data exists for one stock
- **WHEN** a user submits a backtest request for the MA-cross strategy
- **THEN** the backend SHALL evaluate the strategy with project-owned daily-bar logic
- **AND** return metrics, an equity curve, and trade history.

### Requirement: Backtest Boundary

The backtest MVP SHALL remain separate from score replay and calibration.

#### Scenario: Score replay is evaluated separately

- **GIVEN** historical score predictions exist
- **WHEN** the system evaluates whether scores predicted future outcomes
- **THEN** it SHALL use scoring replay/calibration services
- **AND** SHALL NOT treat that evaluation as a trading backtest.

### Requirement: Backtest Engine Scope

The MVP SHALL NOT introduce a full external backtest framework.

#### Scenario: Backtest dependencies are reviewed

- **GIVEN** the MVP only needs single-stock daily-bar simulation
- **WHEN** implementing the backtest service
- **THEN** the implementation SHALL use small project-owned code
- **AND** SHALL NOT add `backtrader`, `vectorbt`, `zipline`, `rqalpha`, or similar frameworks.
