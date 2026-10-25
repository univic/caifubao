## ADDED Requirements

### Requirement: Research NAV input SHALL require an exact source and instrument coverage manifest

The Research NAV adapter SHALL reject an event feed unless it has a known
schema, a matching canonical content hash, an explicit provider, an inventory
covering all requested instruments and dates, and no failed source queries.
It MUST label the source coverage assertion unverified until independent
upstream evidence is provided.

#### Scenario: A source query fails for one held stock

- **GIVEN** a source inventory with a failed fetch of one required code
- **WHEN** the feed is presented for Research NAV valuation
- **THEN** no validated action input is returned

#### Scenario: Feed body differs from the manifest fingerprint

- **GIVEN** a previously listed source SHA-256 and a subsequently mutated
  dividend amount
- **WHEN** the feed is verified
- **THEN** the source rejects the input instead of silently changing returns

#### Scenario: Zero distributions asserted for fully queried universe

- **GIVEN** an empty action feed with an explicit, current full-universe inventory
- **WHEN** an input verifier assesses completeness
- **THEN** it returns no actions but still labels upstream coverage unverified

### Requirement: FORWARD research inputs SHALL be temporally causal

The Research input gate SHALL reject retrospectively marked event inventories,
future-only announcements or stale feed dates, while retaining payable dates
that are known as of the observer's session.

#### Scenario: Replay inventory cannot be used as a certified forward input

- **GIVEN** a manifest labeled REPLAY for after-the-fact backfilling
- **WHEN** it is used for a FORWARD research NAV check
- **THEN** the adapter refuses the manifest
