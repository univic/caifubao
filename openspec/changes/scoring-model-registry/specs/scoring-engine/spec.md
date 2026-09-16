# Scoring Model-Version Registry

## ADDED Requirements

### Requirement: Registered model versions bind configuration

A registered model version MUST bind a per-horizon scoring override
(weights, thresholds, component directions) that is resolved over the
built-in SCORING_CONFIG base. Scoring runs that name a registered ACTIVE
version MUST apply that registered override. The registry pins the override
with a canonical config_hash, so a registered version's override is
reproducible; predictions store the model_version label for audit.

#### Scenario: Registered version config is loaded automatically

- GIVEN an ACTIVE registered model version with a per-horizon directions override
- WHEN a scoring service is constructed naming that version and no explicit
  scoring config
- THEN the service uses the registered per-horizon override

#### Scenario: Explicit scoring config takes precedence

- GIVEN a registered model version AND an explicit scoring_config passed to the
  scoring service
- WHEN the service is constructed
- THEN the explicit scoring_config is used

#### Scenario: Unregistered version falls back to built-in config

- GIVEN a model version with no registration
- WHEN a scoring service is constructed naming it
- THEN the service falls back to the built-in SCORING_CONFIG (unchanged
  behavior), including DEFAULT_MODEL_VERSION

#### Scenario: Retired version is ignored

- GIVEN a model version whose registration status is RETIRED
- WHEN a scoring service is constructed naming it
- THEN the service does NOT load the retired registration

### Requirement: Model versions are immutable

A registered model version MUST NOT be mutated in place: any configuration
change requires registering a new version name and retiring the previous one,
pinned by an order-insensitive canonical config_hash.

#### Scenario: Re-registration with same name requires force and replaces

- GIVEN an existing registration for model_version X
- WHEN registering X again without --force
- THEN registration raises an error (immutability)
- AND registering X with --force replaces the registration and updates the
  config_hash
