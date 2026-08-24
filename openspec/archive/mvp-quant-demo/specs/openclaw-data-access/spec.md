## ADDED Requirements

### Requirement: OpenClaw Read-only API Contract

The backend SHALL expose stable read-only APIs for OpenClaw data access.

#### Scenario: OpenClaw fetches market data

- **GIVEN** OpenClaw has a valid service token
- **WHEN** it requests supported stocks, daily quotes, factors, signals, scores, or quality metadata
- **THEN** the backend SHALL return stable API payloads
- **AND** SHALL NOT expose raw Mongo collection structures.

#### Scenario: OpenClaw fetches score predictions

- **GIVEN** OpenClaw has a service token with `openclaw:score-read` scope
- **WHEN** it requests score predictions for a date, horizon, or stock code
- **THEN** the backend SHALL return score, rank, recommendation, verification status, and verification metrics
- **AND** SHALL include per-component explanation data and input-snapshot freshness
- **AND** SHALL NOT expose experiment management, backtest execution, or score-generation triggers.

### Requirement: OpenClaw Authentication

OpenClaw SHALL authenticate with a dedicated service token and read-only scope.

#### Scenario: Token scope is checked

- **GIVEN** a request includes `Authorization: Bearer <token>`
- **WHEN** the token is revoked, expired, disabled, or missing the required scope
- **THEN** the backend SHALL reject the request with a stable 401 or 403 response.

#### Scenario: Score-read scope is enforced

- **GIVEN** a request targets score-prediction endpoints
- **WHEN** the token has `openclaw:score-read` scope
- **THEN** the backend SHALL allow access to score and recommendation endpoints
- **AND** the token SHALL NOT be required to also have `openclaw:data-read`.

#### Scenario: Narrow scope is blocked on broad data endpoints

- **GIVEN** a request targets non-score endpoints (e.g. /stocks, /quotes/daily, /factors/daily)
- **WHEN** the token only has `openclaw:score-read` but not `openclaw:data-read`
- **THEN** the backend SHALL reject the request with 403

### Requirement: Data Freshness Metadata

OpenClaw API responses that depend on market data SHALL include freshness metadata.

#### Scenario: Downstream analysis checks readiness

- **GIVEN** OpenClaw receives data from caifubao
- **WHEN** quote or factor data is missing, stale, not applicable, or blocked by quote freshness
- **THEN** that state SHALL be explicit in the response
- **AND** OpenClaw SHALL be able to decide whether analysis should proceed.

### Requirement: Integration Non-goals

The OpenClaw integration SHALL NOT provide mutation or operational control endpoints in the MVP.

#### Scenario: OpenClaw attempts operational action

- **GIVEN** OpenClaw is a downstream data consumer
- **WHEN** it needs refreshed or backfilled data
- **THEN** caifubao maintainers SHALL trigger those operations
- **AND** OpenClaw SHALL NOT receive direct Mongo credentials, scheduler triggers, data mutation endpoints, or admin access.

#### Scenario: OpenClaw attempts backtest or experiment execution

- **GIVEN** OpenClaw is a read-only data consumer
- **WHEN** it requests backtest execution, score experiment creation, or scoring-run trigger
- **THEN** the backend SHALL reject the request with 403
- **AND** OpenClaw SHALL NOT be able to initiate compute tasks, grid searches, or parameter optimization.
