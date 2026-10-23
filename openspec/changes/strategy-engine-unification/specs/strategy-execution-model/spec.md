# Strategy Execution Model

## ADDED Requirements

### Requirement: One execution model MUST be authoritative

Every mode that produces fills (BACKTEST, REPLAY, PAPER, LIVE simulation) MUST
use one shared execution model for: board lots and board-specific minimums,
T+1 settlement, price-limit gating, suspension handling, and the fee model
(commission with its minimum, transfer fee on both sides, sell-side stamp duty,
slippage). A second implementation of any of these rules MUST NOT be introduced;
an existing one MUST be declared legacy or replaced, not kept as a silent
parallel.

#### Scenario: Two modes compute the same round-trip cost

- GIVEN a buy and a sell of the same quantity and prices in two execution modes
- WHEN each mode computes fees
- THEN the fee components are identical
- AND any difference comes from an explicitly different spec field

#### Scenario: A board minimum is enforced in every mode

- GIVEN a STAR-market code whose budget fits fewer than 200 shares
- WHEN a buy is sized in any execution mode
- THEN no order is produced
- AND the refusal reason is the same across modes

### Requirement: Unknown tradability verdicts MUST follow an explicit policy

When the price-limit verdict for a session cannot be derived, the engine MUST
apply a policy declared in the spec. The default MUST fail closed for the side a
limit would block. A replay-compatibility policy that treats an unknown verdict
as tradable MAY be selected explicitly; it MUST be visible in the spec hash and
MUST NOT be the default.

#### Scenario: The default refuses an unknown verdict

- GIVEN a spec with the default policy and a session whose limit verdict is
  underivable
- WHEN orders are executed
- THEN the side that a limit would block is refused and reported
- AND other orders are still processed

#### Scenario: The compatibility policy is explicit and hashed

- GIVEN a spec that selects the replay-compatibility policy
- WHEN the spec is hashed and a frozen replay is executed
- THEN the unknown verdict is treated as tradable
- AND the selected policy is part of the spec hash

### Requirement: Fills MUST be deterministic and idempotent

For a given decision, session, side and quantity, the execution model MUST
produce the same fill deterministically. Re-executing the same intent for the
same session MUST NOT produce a duplicate fill, and an order blocked in one
session MUST be retried according to the declared policy rather than silently
dropped or duplicated.

#### Scenario: Re-running a session does not duplicate a fill

- GIVEN a session already executed with one fill
- WHEN the same session is executed again
- THEN no second fill is written
- AND the ledger is unchanged

#### Scenario: A blocked exit rolls forward by policy

- GIVEN a sell refused because the session is limit-down
- WHEN the next session is evaluated
- THEN the exit intent is retried as declared
- AND each refusal remains visible in the record

### Requirement: A broker adapter MUST be default-off and semantics-preserving

A live broker adapter MUST only be enabled behind an explicit, default-off
configuration, and enabling it MUST NOT change selection, sizing, risk or cost
semantics defined by the spec. The absence of an adapter MUST fail loudly when
LIVE mode is requested rather than falling back to simulation silently.

#### Scenario: Requesting live mode without an adapter fails loudly

- GIVEN a spec configured for LIVE execution and no adapter installed
- WHEN the run starts
- THEN the run fails with an explicit error
- AND no simulated fill is produced or recorded as live

#### Scenario: An adapter does not alter the decision

- GIVEN the same spec evaluated in PAPER mode and through a broker adapter in
  dry-run
- WHEN the decision is produced
- THEN the target book and intents are identical
- AND only the fill source differs
