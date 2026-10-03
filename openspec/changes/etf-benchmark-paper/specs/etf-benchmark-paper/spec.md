## ADDED Requirements

### Requirement: Frozen source adapter
The CLI SHALL optionally accept an etf-source-v1 bundle of configuration,
start_date, end_date, calendar, daily and execution arrays. Calendar SHALL cover
every natural day of the inclusive interval for the instrument exchange, with
unique dates and explicit is_open flags; both bounds SHALL be open and at least
two sessions SHALL exist. Row order SHALL not affect fills or valuations (the
full-source provenance hash may change). Mixed instruments,
duplicates, out-of-range rows and closed-session quotes SHALL be rejected.
Only raw open/close SHALL be used; zero prices SHALL map to unavailable. Daily
volume SHALL NOT determine opening tradability. Independent execution rows SHALL
declare opening trade_status/up_limit; missing entries SHALL remain unknown.
No price filling or limit inference SHALL occur. Source output SHALL include
source_provenance with adapter_version, full-bundle canonical SHA256 source_hash
and operator_opening_declaration status_basis. Native output SHALL be unchanged.
Source compatibility SHALL NOT certify authenticity or corporate-action absence.

#### Scenario: Calendar preserves missing quotes
- **WHEN** an open session has no daily row
- **THEN** it remains in the replay with unavailable prices

#### Scenario: Volume does not influence opening orders
- **WHEN** only daily volume changes
- **THEN** fills are unchanged and the provenance hash changes

### Requirement: Explicit offline ETF benchmark contract
The system SHALL provide a single domestic equity ETF buy-and-hold replay from
local JSON, independent of scores, MongoDB, broker connections and forward
certification. The input SHALL identify schema version, instrument, raw price
basis, an explicit ordered market-session calendar, decision date, commission
and slippage parameters, target allocation and an empty corporate-actions list.
Initial cash SHALL default to 100,000 CNY. Unknown fields, non-finite or invalid
numbers, duplicate/unsorted sessions or quotes and adjusted-price inputs SHALL
be rejected. Non-empty corporate actions SHALL be rejected until supported.

#### Scenario: Adjusted prices or unhandled distribution
- **WHEN** the input declares an adjusted price basis or any corporate action
- **THEN** the replay fails without emitting an account result

### Requirement: Causal, fee-aware initial purchase
The system SHALL derive each initial-buy decision only from the previous market
session's valid raw closing quote, the pinned allocation and existing account
state. Quantity SHALL be sized at the execution session's raw opening price
plus adverse slippage, rounded up to the instrument price tick, with commission
included in the allocation budget. Purchases SHALL be multiples of 100 units.
Commission SHALL use the explicit rate and minimum, rounded to cents; no stock
stamp duty or stock transfer fee SHALL be inherited. Cash SHALL never be negative.

#### Scenario: Opening budget reserves commission
- **WHEN** a 100,000 CNY account allocates 100%, the open is 4 CNY, commission
  is 0.025% with 5 CNY minimum, and slippage is zero
- **THEN** the purchase is 24,900 units, commission is 24.90 CNY and remaining
  cash is 375.10 CNY

#### Scenario: Future close cannot change an opening fill
- **WHEN** execution-session or subsequent closing prices change
- **THEN** the opening quantity, price and commission remain unchanged

### Requirement: Blocked execution and hold behaviour
The system SHALL refuse a purchase when the preceding close is missing or
non-tradable, or the execution open/status/upper limit is missing or invalid,
or the open is at its upper limit, or the slippage-adjusted price exceeds that
limit. It SHALL record the reason and retain cash; it MAY retry at a later session
using only that session's preceding quote. A completed initial purchase SHALL
remain held without further orders. A zero allocation SHALL remain cash.

#### Scenario: Missing execution session is not skipped silently
- **WHEN** the next market session has no quote
- **THEN** that session has an explicit blocked decision rather than a fill
  attributed to a later date

#### Scenario: Daily invocation does not imply daily turnover
- **WHEN** the initial purchase already exists in replay state
- **THEN** subsequent sessions emit HOLD with zero additional commission

### Requirement: Auditable cash and valuation
The replay SHALL use Decimal accounting, output cash and costs to cents, actual
integer units and daily raw-price NAV. Each valuation SHALL expose its mark date
and freshness; absent/non-tradable closes SHALL retain the last known mark and be
marked STALE rather than report a fresh close. Same-day purchased units SHALL
not be reported sellable until the next supplied market session. Outputs SHALL
include reproducible input/config hashes, an explicit REPLAY grade, the trade
ledger and final position/cash shapes usable by the existing reconciler after
explicit cent-string to numeric cash conversion at that legacy boundary. Replay
accounting SHALL use a private Decimal context independent of caller precision.
Allocation, commission rate and slippage rate SHALL accept at most 12 decimal
places and reject more precise inputs before calculation.

#### Scenario: Independent reconstruction
- **WHEN** initial cash and emitted fills are replayed independently
- **THEN** reconstructed cash and units match the final account exactly

### Requirement: Local CLI and halt boundary
The unified CLI SHALL run this command locally with the repository datahub on
PYTHONPATH and an explicitly chosen/local-venv Python interpreter, never kubectl.
It SHALL check the existing configured halt store before replay. A halt SHALL
produce no account artifact and exit 2 with HALTED. JSON output SHALL be written
atomically only after successful validation and SHALL not overwrite the input
or halt store. This replay SHALL not write database or forward-window records.

#### Scenario: Offline reproducibility
- **WHEN** the same frozen input and unchanged non-halted configuration run twice
- **THEN** the emitted JSON is identical and contains at most one initial buy
