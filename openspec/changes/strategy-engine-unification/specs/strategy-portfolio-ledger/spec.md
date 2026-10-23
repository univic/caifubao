# Strategy Portfolio Ledger

## ADDED Requirements

### Requirement: Position state MUST derive from ledger events, never from target weights

A ledger projection MUST derive position quantity and cost basis from ordered
ledger events, not from target weights. Events MAY include fills, an explicitly
audited opening balance, corporate actions, and approved adjustments; each
non-fill event MUST carry its source and reason. The ledger MUST track cash,
board-legal quantities, cost basis, realized and unrealized P&L. A target
portfolio is intent only and MUST NOT stand in for position state; NAV for a
simulation or account MUST be derived from that domain's ledger projection.

#### Scenario: A blocked buy creates no position

- GIVEN a target book containing a name whose buy is refused at execution
- WHEN the ledger is updated for that session
- THEN the name has no position and no shares
- AND the refusal is recorded with its reason and side
- AND cash is unchanged by the refused order

#### Scenario: A carried-over position keeps its cost basis

- GIVEN a held name that remains in the target book across sessions
- WHEN the ledger is marked for a later session
- THEN its quantity and cost basis are unchanged
- AND only unrealized P&L moves with the mark

#### Scenario: An opening balance is explicit and auditable

- GIVEN a trading account that already holds shares before the ledger begins
- WHEN the opening snapshot is imported
- THEN an attributed opening-balance event establishes the initial quantity and
  cost basis
- AND no fill is fabricated and no target book is used as the opening position

### Requirement: Research and trading ledgers MUST remain domain-local

The system MUST maintain separate state and persistence boundaries for research
simulation and trading accounts. A ResearchSimulationLedger MAY record simulated
fills and marks as immutable research evidence, but MUST NOT represent an
operational trading account or read/write the trading account store. The
TradingPortfolioLedger MUST be authoritative for TRADING_PAPER/LIVE positions,
cash, orders, fills, rejections and account reconciliation, and MUST be owned by
the trading runtime. Shared event schemas and accounting rules MUST NOT imply a
shared database, shared account state, or a runtime dependency from trading to
research.

#### Scenario: Research simulation cannot mutate a trading account

- GIVEN a BACKTEST, REPLAY or RESEARCH_SIMULATION run
- WHEN simulated fills and marks are produced
- THEN they are stored only as research simulation state or immutable research
  artifacts
- AND no trading order, account ledger or operational halt state is read or changed

#### Scenario: Trading remains independent of research availability

- GIVEN a TRADING_PAPER runtime with a promoted immutable StrategySpec artifact
- WHEN the research service or research database is unavailable
- THEN trading can continue using its pinned artifact and approved market-data
  inputs
- AND all order and account state remains in the trading domain

### Requirement: Fills and rejections MUST be persisted with reasons

Every fill and refused intent MUST be represented as an immutable event in its
own domain: a research artifact event for research simulation, or a persisted
ledger event for trading. Each event MUST carry at least `event_id`, account identity, symbol, strategy
identity when applicable, `spec_hash` when applicable, decision date, execution
date, side, quantity, price when filled, currency (CNY for the A-share ledger),
fee breakdown (commission, transfer fee, stamp duty and slippage), and a
machine-readable refusal reason when rejected. Events MUST identify the source
domain and link to the stable intent/order identity. Persistence MUST NOT relax
the existing tradability or halt contracts. A refused order MUST NOT be silently
dropped, and reports MUST reconstruct which intents became fills.

#### Scenario: A limit-locked sell is visible without fabricating a fill

- GIVEN a held name leaving the target whose session is limit-down
- WHEN the session is executed
- THEN no sell fill is written
- AND a rejection event is written with the position retained
- AND the report shows the refusal for that name and its reason code

#### Scenario: A fee breakdown is attributable per fill

- GIVEN a completed buy and a completed sell for one name
- WHEN the ledger or research artifact is queried for that name
- THEN both fill events expose their fee components
- AND the round-trip cost can be recomputed from those events alone

### Requirement: Ledger events MUST be rebuildable, deterministic and idempotent

Applying the same ordered event stream to an empty domain-local ledger MUST
reproduce the same positions, cash and P&L without reading target books or prior
derived state. Event identity MUST distinguish an intent, an order, an execution
attempt and each individual fill. Re-submitting the same event MUST be idempotent,
while multiple legitimate partial fills for one order MUST remain distinct.

#### Scenario: Re-running a session does not duplicate fills

- GIVEN a session already applied with identified fill events
- WHEN the same session is replayed
- THEN existing events are recognized by stable event identity
- AND no duplicate fill or fee is applied

#### Scenario: Partial fills remain distinct

- GIVEN one order intent that receives two partial fills in the same session
- WHEN both fill events are applied
- THEN both fills are retained with distinct fill identities
- AND their aggregate quantity is used for the position and cash projection

#### Scenario: Rebuild from events matches the original ledger

- GIVEN a ledger produced by an ordered sequence of fill and adjustment events
- WHEN the same events are replayed into an empty ledger
- THEN positions, cash and P&L match the original projection
- AND no target book is consulted during the rebuild

### Requirement: Accounting MUST preserve causal and capital invariants

The ledger MUST preserve the existing causal T+1 and cash-accounting contracts.
A close-of-session decision MUST NOT fill before the next eligible session open;
cash changes MUST equal traded notional plus or minus the exact recorded fees;
negative cash MUST be rejected. Actual holdings MUST respect the configured
position cap even when an exit is blocked. Corporate actions and delisting
exits MUST be explicit, and missing quotes or long suspensions MUST NOT silently
zero a position's value.

#### Scenario: First session remains unfilled

- GIVEN a target decided at the close of session D
- WHEN the ledger processes the close of D
- THEN no fill occurs in session D
- AND execution can occur no earlier than the next eligible session open

#### Scenario: A blocked exit does not permit an over-cap entry

- GIVEN the account is at its position cap and an exit is blocked
- WHEN the next target includes a new name
- THEN the new entry is blocked with a machine-readable reason
- AND the residual holding remains visible in the ledger

#### Scenario: Cash is conserved

- GIVEN a valid buy or sell fill with recorded notional and fee components
- WHEN the ledger applies the event
- THEN the cash delta equals the signed traded notional and exactly the recorded
  fees
- AND a fill that would make cash negative is rejected with an explicit reason

#### Scenario: Corporate actions and delisting are explicit

- GIVEN a held position with an ex-date share multiplier, cash distribution, or
  an explicit delisting event
- WHEN the ledger processes that event
- THEN the share multiplier and cash distribution are applied on the ex-date
- AND a delisting uses an explicit exit price or a reason-coded write-off whose
  value is quantity multiplied by the declared write-off price
- AND a missing quote alone never creates a delisting write-off

#### Scenario: A missing quote does not become a zero-value delisting

- GIVEN a held name with a stale or missing quote and no explicit delisting event
- WHEN the ledger marks the account
- THEN it carries the last known valuation and flags staleness
- AND it does not silently write the position off

### Requirement: Trading reconciliation MUST use the trading ledger of record

Trading account reconciliation MUST compare an external account snapshot with
the trading ledger projection and report per-name planned/actual quantity and
drift, cash drift, missing and unexpected holdings, structured breaks, JSON
serialization and fail-loud behaviour above tolerance. The planned side MUST
come from the trading ledger of record, not from a target book or a research
simulation ledger. This requirement does not authorize research to access the
trading database.

#### Scenario: Quantity drift beyond tolerance is a break

- GIVEN the trading ledger projects 1,000 shares and the account snapshot holds
  900 shares with zero quantity tolerance
- WHEN reconciliation runs
- THEN the result reports a 100-share drift as a break
- AND the command exits non-zero

#### Scenario: Missing and unexpected positions are explicit

- GIVEN a planned position absent from the account and an unplanned account
  holding
- WHEN reconciliation runs
- THEN both names are listed in the structured breaks
- AND the result is JSON-serializable for logging
