# Strategy Portfolio Ledger

## ADDED Requirements

### Requirement: A position MUST exist only as the result of a fill

The engine MUST keep a persisted ledger of actual account state: positions,
quantities, board lots, cost basis, cash, realized and unrealized P&L. A target
portfolio MUST be treated as intent only; it MUST NOT stand in for position
state, and NAV MUST be derived from the ledger rather than from target weights.

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

### Requirement: Fills and rejections MUST be persisted with reasons

Every executed fill and every refused order MUST be persisted as an immutable
ledger event carrying at least: the strategy id, spec hash, decision date,
execution date, side, quantity, execution price, fees (commission, transfer fee,
stamp duty, slippage), and — for refusals — a machine-readable reason code. An
order refused at the execution session MUST NOT be silently dropped, and the
daily report MUST be able to reconstruct which intents became fills.

#### Scenario: A limit-locked sell is visible in the ledger

- GIVEN a held name leaving the target book whose session is limit-down
- WHEN the session is executed
- THEN no sell fill is written
- AND a rejection event is written with the position retained
- AND the report shows the refusal for that name

#### Scenario: A fee breakdown is attributable per fill

- GIVEN a completed buy and a completed sell for one name
- WHEN the ledger is queried for that name
- THEN both fills expose their fee components
- AND the round-trip cost can be recomputed from the events alone

### Requirement: The ledger MUST be rebuildable and deterministic

Applying the same ordered fill events to an empty account MUST reproduce the same
positions, cash and P&L, without reading target books or prior derived state.
Marking a session twice MUST NOT double-count a fill or a fee.

#### Scenario: Rebuild from fills matches the live ledger

- GIVEN a ledger produced by a sequence of sessions
- WHEN the same fill events are replayed into an empty account
- THEN positions, cash and P&L match the original ledger
- AND no target book is consulted during the rebuild

#### Scenario: Re-applying an event is idempotent

- GIVEN a fill event already applied to the ledger
- WHEN the same event is applied again
- THEN the ledger state is unchanged
- AND no duplicate fee is charged

### Requirement: Reconciliation MUST compare the ledger with an account snapshot

Reconciliation MUST diff the ledger against an operator-supplied account snapshot
(positions and cash) and MUST report quantity drift, cash drift, missing planned
names, unexpected holdings and a structured breaks list. A breach above the
declared tolerance MUST fail loudly rather than produce a clean report.

#### Scenario: Quantity drift beyond tolerance is a break

- GIVEN a planned position of 1,000 shares and an account holding of 900 shares
- WHEN reconciliation runs with a zero quantity tolerance
- THEN the result reports a 100-share drift as a break
- AND the command exits non-zero

#### Scenario: A missing position is reported explicitly

- GIVEN a name present in the ledger and absent from the account snapshot
- WHEN reconciliation runs
- THEN the result lists the name under missing positions and in the breaks list
- AND the result is JSON-serialisable for logging
