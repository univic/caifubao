# Strategy Definition

## ADDED Requirements

### Requirement: A unified strategy MUST be a versioned, explicit declaration

Every strategy executed by the unified engine MUST be declared as one versioned
`StrategySpec` object rather than as an ad-hoc parameter dictionary. Legacy
strategies that already exist as code branches MUST be frozen read-only and are
exempt until they are replaced; no new strategy may be added as a code branch. A
spec MUST declare, at minimum: `spec_version`, `timing_version`,
`score_model_version`, `horizon`, `initial_nav`, the selection rule (entry and
exit semantics, including whether a held name is retained between entry and
exit), `weighting`, `cash_reserve_pct`, portfolio constraints, rebalance cadence,
the risk policy, execution assumptions, and the evidence policy. Every field that
can change a decision MUST be part of the hashed spec content.

#### Scenario: Two specs differing in one decision field hash differently

- GIVEN two specs identical except for one selection or constraint field
- WHEN each spec's canonical hash is computed
- THEN the two hashes differ
- AND the differing field is identifiable from the spec content alone

#### Scenario: A spec without a signal source is rejected

- GIVEN a spec object with no `score_model_version` or no horizon
- WHEN the spec is validated
- THEN validation fails with a specific error naming the missing field
- AND no run is started from the invalid spec

#### Scenario: A legacy code branch is not a new strategy

- GIVEN a legacy strategy that still executes through a code branch
- WHEN a new strategy is proposed
- THEN the new strategy is declared as a spec, not added as another branch
- AND the legacy branch is either frozen or replaced, never extended

### Requirement: Unknown, missing and infeasible fields MUST fail closed

Spec validation MUST reject unknown keys at every nesting level, MUST reject
missing required fields, and MUST reject combinations that cannot be executed
at the declared account size — at minimum, a position budget below one board
lot, a board minimum that cannot fit, or a cost regime where the minimum
commission makes every trade uneconomic. Validation MUST fail loudly with a
message naming the offending field or combination; it MUST NOT silently fall
back to a default.

#### Scenario: An unknown key is rejected rather than absorbed

- GIVEN a spec containing a misspelled selection key
- WHEN the spec is validated
- THEN validation fails and names the unknown key
- AND the spec is not silently merged with defaults

#### Scenario: An infeasible account size is rejected at definition time

- GIVEN a spec whose account size and portfolio size give a per-name budget
  below one board lot, or whose minimum commission dominates every trade
- WHEN the spec is validated
- THEN validation fails with an explanation of the infeasible combination
- AND the failure is reported before any run or export is produced

### Requirement: One spec MUST drive every execution mode

The same immutable StrategySpec and canonical `spec_hash` MUST be consumable by
BACKTEST, REPLAY, RESEARCH_SIMULATION, TRADING_PAPER and LIVE. Domain-local
runtimes MAY use different data adapters and fill adapters, but MUST preserve the
spec-declared selection, sizing, constraints, rebalance, risk policy and modeled
cost assumptions. Actual LIVE fill prices and fees are execution facts and MUST
be recorded as observed, not overwritten with simulation estimates. A mode-specific
semantic difference MUST be an explicit spec field with a documented default,
not an implicit branch.

#### Scenario: Paper and replay agree for the same spec and inputs

- GIVEN one spec and one frozen input snapshot
- WHEN the same decision date is evaluated through the paper path and the replay
  path
- THEN both produce the same target book and the same order intents
- AND any difference is reported as a defect, not as a mode convention

#### Scenario: A mode-specific policy is visible in the spec

- GIVEN a spec that opts into a replay-compatibility tradability policy
- WHEN the spec is rendered or exported
- THEN the policy appears as an explicit field and enters the spec hash
- AND the default spec does not select that policy

### Requirement: A spec used as evidence MUST be immutable

Once evidence is certified, any spec change MUST produce a new version and hash.
The engine MUST NOT mutate a spec in place, and a run whose supplied spec hash
does not match the active evidence window's hash MUST NOT be certified FORWARD.
Such a run MUST be recorded with the existing out-of-window classification
(`evidence_kind=REPLAY`) or fail; it MUST NOT advance the window's counter, and
the engine MUST NOT introduce a new hard-rejection policy for a mismatch that
the forward-evidence contract already classifies.

#### Scenario: A mismatched spec is not certified forward

- GIVEN an ACTIVE forward window with a pinned spec hash
- WHEN a run is started with a spec whose hash differs
- THEN the run is recorded outside the window's evidence as `REPLAY`
- AND the pinned evidence window and its counter are not advanced

#### Scenario: Legacy records stay readable

- GIVEN historical paper runs and replay artifacts written before this change
- WHEN they are read by the new engine
- THEN they remain readable and are never rewritten in place
- AND their original hash and evidence kind are preserved

### Requirement: Legacy configuration MUST migrate deterministically

Existing strategy configurations MUST migrate through a deterministic, tested
mapping. The migration MUST preserve the historical normalized configuration and
its existing `config_hash` exactly as `legacy_config_hash`, while computing a
separate canonical `spec_hash` from the complete versioned StrategySpec,
including explicit defaults and every decision-affecting field. The two hashes
MUST NOT be required to match because they identify different canonical
representations. For a legacy configuration whose behaviour is representable,
the migrated spec MUST reproduce the same target book, rebalance list and
declared execution decisions. Historical records and forward windows MUST keep
their original hash; a new spec hash MUST NOT inherit forward certification
unless the existing evidence contract explicitly certifies that exact hash.

#### Scenario: A legacy config preserves its historical identity and plan

- GIVEN a configuration accepted by the current engine
- WHEN it is migrated to a StrategySpec
- THEN its legacy normalized config and `config_hash` are preserved as
  `legacy_config_hash`
- AND the migrated spec produces the same target book, rebalance list and
  declared execution decisions
- AND its independent canonical `spec_hash` is stable for the same spec content

#### Scenario: A new spec does not silently inherit an old forward window

- GIVEN an ACTIVE forward window pinned to legacy config hash A
- WHEN the same configuration is represented by a StrategySpec with spec hash B
- THEN B is not certified FORWARD under window A
- AND the run follows the existing mismatch classification as REPLAY without
  advancing the old window counter
- AND a new window must be explicitly certified for B before B can count

#### Scenario: Migration refuses behaviour it cannot express

- GIVEN a legacy configuration with a field the target spec cannot represent
- WHEN migration is attempted
- THEN migration fails with a specific error
- AND no partially migrated spec is written


Existing strategy configurations MUST migrate to a spec through a deterministic,
tested mapping. For a configuration that expresses only behaviour the current
engine already supports, the migrated spec MUST reproduce the same target book,
the same rebalance list and the same canonical spec hash, and that hash MUST equal
the legacy `config_hash` so historical runs remain comparable. The mapping MUST
enumerate every inherited key, including `timing_version`, `weighting` and
`cash_reserve_pct`.

#### Scenario: A legacy config produces an identical plan

- GIVEN a configuration accepted by the current engine
- WHEN it is migrated to a spec and executed
- THEN the target book and rebalance list equal the legacy output
- AND the canonical spec hash equals the legacy `config_hash`

#### Scenario: Migration refuses behaviour it cannot express

- GIVEN a legacy configuration with a field the target spec cannot represent
- WHEN migration is attempted
- THEN migration fails with a specific error
- AND no partially migrated spec is written
