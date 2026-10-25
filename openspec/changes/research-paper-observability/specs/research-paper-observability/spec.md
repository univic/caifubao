## ADDED Requirements

### Requirement: Research paper read models SHALL scope observations to one exact immutable identity

The authenticated Research API SHALL scope plan, decision and NAV snapshot
reads to a model version, horizon, config hash and hash kind, SHALL never
mutate Research or Trading state, and SHALL bound each response.

#### Scenario: Two spec hashes share a model and horizon

- **GIVEN** Research plans with different spec hashes or legacy hash kinds
- **WHEN** a user reads the paper overview for one pinned identity
- **THEN** the result contains only matching plans and NAV points

### Requirement: No replay, preliminary NAV or target weights SHALL masquerade as account evidence

REPLAY or mismatched FORWARD plans SHALL NOT count as forward progress.
NAV snapshots SHALL carry an UNVERIFIED accounting status until ledger
reconciliation is accepted. The UI SHALL NOT infer fills from target weights
or insert missing NAV marks.

#### Scenario: A FORWARD record has no matching ACTIVE window

- **GIVEN** a completed FORWARD record but no matching ACTIVE spec window
- **WHEN** the overview is rendered
- **THEN** forward progress is zero and the NAV remains provisional

#### Scenario: A plan has no NAV snapshot

- **GIVEN** one otherwise valid completed plan without NAV
- **WHEN** a user views the curve
- **THEN** no fabricated flat NAV point is created

### Requirement: Access SHALL require a normal authenticated user

Anonymous and service-token clients SHALL be refused before any database
read; only read endpoints are exposed.

#### Scenario: Missing user JWT

- **GIVEN** no authenticated user token
- **WHEN** an identity listing is requested
- **THEN** the API responds with an authentication failure
