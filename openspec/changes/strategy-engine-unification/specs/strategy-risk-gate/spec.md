# Strategy Risk Gate

## ADDED Requirements

### Requirement: Risk MUST be a distinct, explainable stage between intent and execution

Order intents MUST pass through a risk gate that is separate from selection and
from execution. The gate MUST be a pure function of the spec's risk policy, the
intent list, the ledger state and the declared market snapshot, and it MUST
return either an approved intent list or explicit rejections. The gate MUST NOT
read or mutate persistence directly.

#### Scenario: A rejected intent is returned with a reason

- GIVEN an intent that violates a configured risk limit
- WHEN the risk gate evaluates the intent list
- THEN the intent is removed from the approved list
- AND it is reported with a machine-readable reason and the limit it breached
- AND the remaining intents are still evaluated

#### Scenario: The gate does not depend on wall-clock time

- GIVEN the same spec, ledger and market snapshot
- WHEN the gate runs twice
- THEN both runs return the identical approved list and rejection list

### Requirement: Turnover and rebalance budget MUST be enforceable

The spec MUST be able to declare a turnover or rebalance budget per session or
per period, and the gate MUST enforce it by reducing or refusing the lowest
priority intents before execution. When no budget is declared the behaviour MUST
remain unchanged from the current engine.

#### Scenario: A budget reduces the intent list deterministically

- GIVEN a turnover budget and a candidate list whose replacement cost exceeds it
- WHEN the gate evaluates the list
- THEN the approved list stays within the budget
- AND the refused intents are reported with the budget reason
- AND the ordering rule that decides which intents survive is deterministic

#### Scenario: An undeclared budget changes nothing

- GIVEN a spec with no turnover budget
- WHEN the gate evaluates an intent list
- THEN every otherwise-valid intent is approved
- AND the target book equals the selection output

### Requirement: Portfolio-level risk limits MUST gate orders

The gate MUST support portfolio-level controls that have no per-name
counterpart: exposure or concentration limits, a cash floor, a drawdown
circuit-breaker, and a market-regime condition. A breached circuit-breaker MUST
stop new risk rather than silently reduce it, and it MUST be visible in the run
record as a derived breaker state. It MUST NOT mutate the operator halt flag,
whose who/when/why semantics stay owned by the existing halt contract.

#### Scenario: A drawdown breaker blocks new entries

- GIVEN a declared drawdown threshold that has been breached
- WHEN the next session's intents are evaluated
- THEN opening intents are refused with the breaker reason
- AND risk-reducing intents (exits) remain approvable
- AND the run record states that the breaker was active

#### Scenario: Ops halt takes precedence over the gate

- GIVEN an engaged halt flag and a valid intent list
- WHEN a run starts
- THEN no plan and no order intents are produced
- AND the run is recorded as halted rather than as a normal no-op

### Requirement: Risk decisions MUST be replayable and auditable

Every rejection MUST be storable with the spec hash, the inputs that triggered it
and the limit version, so a replay can reproduce the same decision. Changing a
risk parameter MUST produce a new spec hash and MUST NOT retroactively alter past
records.

#### Scenario: A replay reproduces the same rejections

- GIVEN a stored run with recorded rejections
- WHEN the same spec, ledger and snapshot are replayed
- THEN the rejection set and reasons match the stored record

#### Scenario: A changed limit invalidates the old hash

- GIVEN a spec whose risk parameter is changed
- WHEN the new spec is validated and hashed
- THEN the hash differs from the previous one
- AND historical runs keep their original spec hash and decisions
