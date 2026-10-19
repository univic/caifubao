# Datahub pipeline degradation

## ADDED Requirements

### Requirement: A bounded number of quote validation failures SHALL degrade, not fail, the run

The stock quote job SHALL persist a distinct `DEGRADED` status (never
`SUCCESS`) when all of the following hold:

- at least one quote row validated;
- `failed_count <= max(5, ceil(0.002 * attempted_symbols))`;
- `coverage_rate = validated_symbols / attempted_symbols >= 0.99`;
- the history-source circuit breaker did not trip.

`attempted_symbols` SHALL be the number of symbols for which the run tried to
refresh a quote (`required_quote_attempt_count`: non-suspended UPD, INC, FULL
and NEW paths). It SHALL NOT be `pulled_total` and it SHALL NOT include symbols
that were deliberately `SKIP`ped because no update was needed. `validated_symbols`
SHALL be the number of attempted symbols whose final freshness/validation status
is `OK`. Both counters, `failed_count`, the failed codes and the coverage rate SHALL be
persisted in the run summary under `quote_validation`.

A run that exceeds the absolute/relative failure cap, validates zero rows, or
trips the consecutive-history circuit breaker SHALL persist `FAILED`. A run with
`2` failed symbols and `5,221` validated of `5,223` attempted SHALL persist
`DEGRADED` and keep the chain usable; a source outage with hundreds of failures
SHALL persist `FAILED`.

#### Scenario: Two failures in a 5,223-symbol refresh degrade

- GIVEN a stock quote run that attempts 5,223 symbols and validates 5,221
- WHEN the run finishes
- THEN the run status is `DEGRADED`
- AND `quote_validation.attempted_symbols` is 5,223
- AND `quote_validation.validated_symbols` is 5,221
- AND `quote_validation.failed_symbols` lists the two codes
- AND `quote_validation.coverage_rate` is at least 0.99

#### Scenario: A source outage still fails

- GIVEN a stock quote run in which hundreds of attempted symbols fail validation
- WHEN the run finishes
- THEN the run status is `FAILED`
- AND the run does not become usable downstream

#### Scenario: The 352 untouched symbols are not counted as failures

- GIVEN a run where 352 symbols are `SKIP`ped because they need no update
- WHEN coverage is computed
- THEN those symbols are excluded from the denominator
- AND coverage is computed over attempted symbols only

### Requirement: Downstream signal/scoring gates SHALL accept only usable degraded quote runs

The daily signal gate SHALL accept the upstream stock quote run when it is
`SUCCESS`, or when it is `DEGRADED` with
`quote_validation.coverage_rate >= 0.99` and
`update_ma_factor.written_count > 0`. The existing crash-recovery paths
(`RUNNING` or `FAILED` with the persisted quote and MA-factor phase evidence)
SHALL remain accepted exactly as today. The scoring gate's acceptance of a
partially-written signal run SHALL remain unchanged; `DEGRADED` is added to the
quote leg only.

Any other quote state SHALL skip the downstream job with
`reason=dependency_failed`. The gate decision and the observed
`coverage_rate`/factor count SHALL be recorded in the downstream job summary.

#### Scenario: A degraded run with factor evidence is accepted

- GIVEN today's stock quote run is `DEGRADED` with coverage 0.9996 and
  `update_ma_factor.written_count > 0`
- WHEN the signal gate evaluates the dependency
- THEN signals are allowed to run
- AND the job summary records the degraded acceptance

#### Scenario: A degraded run without factor evidence is rejected

- GIVEN a `DEGRADED` upstream run whose MA-factor phase wrote zero rows
- WHEN the signal gate evaluates the dependency
- THEN signals are skipped with `reason=dependency_failed`

#### Scenario: The crash-recovery path is unchanged

- GIVEN a `FAILED` upstream run whose quote and MA-factor phases have persisted
  evidence
- WHEN the signal gate evaluates the dependency
- THEN signals are still allowed to run as before

### Requirement: Non-core index history gaps SHALL skip, while core index failures SHALL fail

The index quote job SHALL classify indices into a declared core set
(`sh000001`, `sz399001`, `sz399006`, `sh000300`, `sh000905`). A core index that
cannot be refreshed or validated SHALL fail the run. A non-core index for which
the source has no history SHALL increment a `skipped_indices` counter and SHALL
NOT trip the consecutive-history-failure circuit breaker. A non-core skip that
is **not** in the declared persistent-absence allowlist SHALL persist `DEGRADED`;
a skip that is allowlisted is expected and SHALL leave the run clean. The skipped
codes SHALL be reported in `index_validation.skipped_indices`, and the allowlist
SHALL be configurable so a permanent structural absence cannot hold the run at
`DEGRADED` forever.

#### Scenario: Missing non-core index history does not fail the run

- GIVEN a non-core index with no history returned by the source
- WHEN the index run finishes with every core index fresh
- THEN the run status is `DEGRADED`
- AND `index_validation.skipped_indices` includes the non-core code

#### Scenario: An allowlisted structural absence leaves the run clean

- GIVEN a non-core index declared in the persistent-absence allowlist
- WHEN the index run finishes with every core index fresh
- THEN the run does not persist `DEGRADED` for that skip
- AND the code is still reported in `index_validation.skipped_indices`

#### Scenario: A core index failure fails the run

- GIVEN the CSI 300 index cannot be refreshed
- WHEN the index run finishes
- THEN the run status is `FAILED`

### Requirement: The existing strict defaults remain available

The operator SHALL be able to disable degradation so any validation failure
fails the run. The tolerance SHALL be documented with its absolute and relative
caps, and the environment/configuration switch SHALL be explicit.

#### Scenario: Strict mode fails on one symbol

- GIVEN the strict switch is enabled
- WHEN one symbol fails validation
- THEN the run status is `FAILED`
