# Strategy Execution Model

## ADDED Requirements

### Requirement: Shared deterministic execution rules MUST be authoritative across domains

BACKTEST, REPLAY, RESEARCH_SIMULATION, TRADING_PAPER and LIVE MUST use one
versioned set of shared deterministic rules for board-specific minimum
quantities, T+1 eligibility, price-limit classification, suspension handling,
and modeled transaction costs. Domain-local simulators and trading adapters MAY
orchestrate these rules differently, but MUST NOT silently reimplement or
contradict them. Shared helpers MUST be deterministic and side-effect-free;
database access, job scheduling, operational order state and broker integration
MUST remain outside the shared contract library.

#### Scenario: Two domains compute the same modeled round-trip cost

- GIVEN the same cost profile, side, quantity and execution prices
- WHEN research simulation and trading pre-trade estimation compute costs
- THEN commission, minimum commission, transfer fee, stamp duty and modeled
  slippage are identical
- AND any difference is explicit in the versioned cost profile

#### Scenario: A board minimum is enforced consistently

- GIVEN a STAR-market code whose budget fits fewer than 200 shares
- WHEN a buy is sized in research simulation or trading paper
- THEN no order intent below the board minimum is produced
- AND the refusal reason is consistent

#### Scenario: Actual live fills remain facts

- GIVEN a LIVE order whose broker reports an actual fill price or fee that differs
  from the simulation estimate
- WHEN the fill is recorded
- THEN the ledger records the broker-reported execution facts
- AND the modeled estimate remains distinguishable from the actual fill and fee

### Requirement: Unknown tradability verdicts MUST follow an explicit policy

When the price-limit verdict for a session cannot be derived, the engine MUST
apply a policy declared in the spec. The default MUST fail closed for the side a
limit would block. A replay-compatibility policy that treats an unknown verdict
as tradable MAY be selected explicitly for research reproduction; it MUST be
visible in the spec hash and MUST NOT be the default. Trading paper/live MUST
NOT opt into fail-open behaviour unless a separate approved trading policy
explicitly authorizes it.

#### Scenario: The default refuses an unknown verdict

- GIVEN a spec with the default policy and a session whose limit verdict is
  underivable
- WHEN orders are evaluated
- THEN the side that a limit would block is refused and reported
- AND unrelated orders are still processed

#### Scenario: The compatibility policy is explicit and hashed

- GIVEN a research replay spec that selects the compatibility policy
- WHEN the spec is hashed and a frozen replay is executed
- THEN the unknown verdict is treated according to that explicit policy
- AND the policy is part of the spec hash
- AND the same policy cannot silently be applied to trading

### Requirement: Order and fill identities MUST make retries idempotent without losing partial fills

For a given intent and execution policy, rerunning the same simulated session
MUST NOT duplicate its order or fill events. The event model MUST distinguish
stable `intent_id`, `order_id`, execution-attempt identity and individual
`fill_id`. One order MAY produce multiple partial fills, each with a distinct
fill identity; those fills MUST aggregate deterministically. Rejected attempts
MUST remain visible and retries MUST follow the declared policy.

#### Scenario: Re-running a session does not duplicate a fill

- GIVEN a session whose fill events have stable identities
- WHEN the same session is executed again
- THEN the existing fills are recognized and no duplicate quantity or fee is applied

#### Scenario: Multiple partial fills remain distinct

- GIVEN one order that receives two partial fills in the same session
- WHEN both events are persisted
- THEN both events remain visible under the same order identity
- AND each fill has a distinct `fill_id`
- AND the total filled quantity equals the sum of the two fill events

#### Scenario: A blocked exit rolls forward by policy

- GIVEN a sell refused because the session is limit-down
- WHEN the next eligible session is evaluated
- THEN the exit intent is retried according to the declared policy
- AND each refusal remains visible without fabricating a fill

### Requirement: A broker adapter MUST be default-off and semantics-preserving

A live broker adapter MUST only be enabled behind an explicit, default-off
configuration, and enabling it MUST NOT change selection, sizing or declared
risk policy. The absence of an adapter MUST fail loudly when LIVE mode is
requested rather than falling back to simulation silently. Actual broker
execution facts MUST be recorded separately from modeled costs.

#### Scenario: Requesting live mode without an adapter fails loudly

- GIVEN a spec configured for LIVE execution and no adapter installed
- WHEN the run starts
- THEN the run fails with an explicit error
- AND no simulated fill is produced or recorded as live

#### Scenario: An adapter does not alter the decision

- GIVEN the same spec and market snapshot evaluated in research simulation and
  through a trading adapter in dry-run
- WHEN the decision is produced
- THEN the target book and order intents are identical
- AND only the execution source and resulting fill facts differ

### Requirement: Execution components MUST respect domain boundaries

The data domain MUST remain responsible for publishing versioned market facts;
it MUST NOT own stateful strategy execution or trading account state. The
existing `datahub/app/lib/execution/` path MAY remain temporarily as a
compatibility façade to shared cost functions, but MUST NOT become the
authoritative home for a full stateful ExecutionModel. Research simulation
orchestration belongs to the research domain. Trading order state, operational
risk enforcement, persistent trading ledger, reconciliation, halt operations
and broker adapters MUST belong to the trading domain. The backend API layer
MUST NOT become the execution engine.

#### Scenario: The compatibility façade remains thin

- GIVEN a consumer importing costs through `datahub/app/lib/execution/`
- WHEN shared cost or rule functions are updated
- THEN the façade only re-exports the shared implementation
- AND it contains no second copy of fee arithmetic or stateful order logic

#### Scenario: Research simulation does not own trading state

- GIVEN a research BACKTEST, REPLAY or RESEARCH_SIMULATION run
- WHEN its simulated execution completes
- THEN the result is stored as research evidence
- AND no trading order, account ledger or operational halt state is read or mutated

#### Scenario: Trading does not depend on research runtime

- GIVEN a TRADING_PAPER or LIVE run with a pinned promoted StrategySpec
- WHEN the research runtime is unavailable
- THEN the trading runtime can evaluate risk and process its own execution workflow
- AND it does not call the research service or access the research database
