## ADDED Requirements

### Requirement: OpenClaw Read-only API Contract

The backend SHALL expose stable read-only APIs for OpenClaw data access.

#### Scenario: OpenClaw fetches market data

- **GIVEN** OpenClaw has a valid service token
- **WHEN** it requests supported stocks, daily quotes, factors, signals, or quality metadata
- **THEN** the backend SHALL return stable API payloads
- **AND** SHALL NOT expose raw Mongo collection structures.

### Requirement: OpenClaw Authentication

OpenClaw SHALL authenticate with a dedicated service token and read-only scope.

#### Scenario: Token scope is checked

- **GIVEN** a request includes `Authorization: Bearer <token>`
- **WHEN** the token is revoked, expired, disabled, or missing `openclaw:data-read`
- **THEN** the backend SHALL reject the request with a stable 401 or 403 response.

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
