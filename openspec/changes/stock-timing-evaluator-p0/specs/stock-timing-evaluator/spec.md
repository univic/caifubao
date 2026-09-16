# Stock Timing Evaluator P0

## ADDED Requirements

### Requirement: Timing decisions are causal research records

The system SHALL construct a deterministic, in-memory, position-aware timing
record with one action from `ENTER`, `HOLD`, `EXIT`, or `NO_TRADE`. It SHALL pin
the stock, signal date, injected timezone-aware decision time, immediate next
trading-calendar execution session, that session's expiry, position state,
score evidence, thresholds, data-as-of time, freshness, model version,
configuration hash, evidence kind, and reason tokens. Every record SHALL carry
`research_only=true` and `validation_status=UNVALIDATED`, and SHALL NOT be
represented as an order, persisted shared artifact, or investment
recommendation.

P0 SHALL use full-cohort `percentile` in `[0, 1]` as its only threshold basis. A
flat position enters at `percentile >= entry_percentile`; a held position exits
at `percentile <= exit_percentile`; all other fresh cases hold/no-trade.

#### Scenario: Flat position meets entry threshold

- GIVEN fresh usable percentile evidence for a flat stock position
- WHEN the fixed entry condition is met
- THEN the record action is `ENTER`
- AND its execution date is the immediate next session in the authoritative
  supplied trading calendar
- AND it remains research-only and unvalidated

#### Scenario: Held position meets exit threshold

- GIVEN fresh usable percentile evidence for a held stock position
- WHEN the fixed exit condition is met
- THEN the record action is `EXIT`
- AND the reason identifies the exit condition

#### Scenario: Fresh held position has no exit

- GIVEN fresh usable percentile evidence for a held stock position
- WHEN the exit condition is not met
- THEN the action is `HOLD`

#### Scenario: Fresh flat position has no entry

- GIVEN fresh usable percentile evidence for a flat stock position
- WHEN the entry condition is not met
- THEN the action is `NO_TRADE`

#### Scenario: Required evidence is missing, unusable, or stale

- GIVEN a missing percentile, a score status outside
  `PENDING/TRACKING/VERIFIED/INSUFFICIENT_DATA`, or freshness other than `FRESH`
- WHEN a timing record is requested
- THEN the action is `NO_TRADE`
- AND missing, unusable, or stale evidence is explicit
- AND no threshold action is inferred

#### Scenario: Calendar or decision clock cannot prove causality

- GIVEN no next calendar session, a non-trading signal date, a naive decision
  time, a missing/mismatched authoritative signal close, an expiry other than
  the immediate execution session close, a decision time before the signal
  session close, or a decision time at/after the next session open
- WHEN a timing record is constructed
- THEN construction fails
- AND no record is emitted

### Requirement: Timing evaluation is pooled over an explicit cohort

The evaluator SHALL accept an explicit stock-code cohort, cohort `as_of`, cohort
source identifier, caller-supplied delisted-completeness assertion, one fixed
strategy configuration, one model version, and one historical window. It SHALL
canonicalize the cohort to sorted unique codes and record a canonical cohort
hash. That hash SHALL be lowercase SHA-256 of UTF-8 canonical JSON using sorted
keys and compact separators over `codes`, RFC 3339 UTC `as_of`, and `source`.
The report SHALL pin the canonical cohort, model version, configuration hash,
historical window, and `delisted_completeness`, whose allowed values are
`VERIFIED`, `NOT_VERIFIED`, and `UNKNOWN`; it SHALL NOT infer completeness from
successful or currently surviving rows. It SHALL consume one normalized timing and
same-stock buy-and-hold result pair per code. Both sides SHALL use the same
initial cash, next-tradable-open entry, last-close valuation, board lots, and
friction, and any adapter SHALL run with `save_result=False`.

Each side SHALL name the requested stock and carry matching machine-readable
assumptions for initial cash, historical window, execution timing, valuation
timing, board lot, and friction. A missing or mismatched assumption SHALL fail
the row with a stable reason code. An index benchmark SHALL NOT substitute for
the same-stock buy-and-hold side. `completed_trades` SHALL count only filled
timing-side SELL records with positive quantity and execution price.
Every counted daily observation SHALL include a finite non-negative equity value,
and all daily observations and trade dates SHALL fall within the declared
historical window.

A row SHALL be `SUCCESS` only when both timing and buy-and-hold complete;
otherwise it SHALL be `FAILED` with a stable reason code and count against
coverage. `coverage` SHALL equal successful rows divided by requested unique
codes. `observed_sessions` SHALL equal aligned daily equity observations on both
sides. `completed_trades` SHALL count completed timing-side SELL trades. A
successful row SHALL be evidence-eligible only with at least 120 observed
sessions and at least five completed trades.

#### Scenario: Pool report compares every successful stock with buy-and-hold

- GIVEN an explicit cohort and a fixed timing configuration
- WHEN pool evaluation completes
- THEN every successful stock has timing return, same-stock buy-and-hold return,
  excess, drawdown, Sharpe, completed trades, observed sessions, and flags
- AND the summary reports requested/successful/failed/evidence-eligible counts,
  coverage, excess distribution, positive-excess share, and total trades
- AND the report contains no `best_stock`, recommendation, or order instruction
- AND it always carries `research_only=true` and
  `validation_status=UNVALIDATED`, whether gates pass or fail

#### Scenario: Input ordering and duplicates do not change the result

- GIVEN two cohort inputs containing the same stock codes in different orders
  and with duplicates
- WHEN they are evaluated with identical configuration, `as_of`, and source
- THEN their canonical cohort, cohort hash, rows, and pooled summary are identical

#### Scenario: Insufficient pooled evidence fails the gate

- GIVEN fewer than 50 requested unique codes, fewer than 50 evidence-eligible
  rows, or coverage below 90%
- WHEN the pool report is built
- THEN the corresponding gate is failed with stable reasons
- AND the report remains unvalidated and is not described as tradable

### Requirement: Registered scoring mode controls scoring dispatch

Scoring configuration precedence SHALL remain explicit config, ACTIVE registry
config, then built-in config. Independently, an ACTIVE model registry entry that
pins `scoring_mode` SHALL control raw or ranked construction. An explicit
runtime mode SHALL apply only when the registry leaves mode unspecified; a
conflict with a pinned mode SHALL fail before any write. Retired entries are
unregistered. Registry lookup failure SHALL fail closed for a non-default model
while preserving the built-in default model's legacy fallback. The effective
mode SHALL be stored in every prediction input snapshot.

Single-stock scoring SHALL remain raw-only and SHALL fail closed for an
effective ranked mode; ranked models require complete-cohort scoring.

#### Scenario: Ranked registered model needs no environment toggle

- GIVEN an ACTIVE registered model whose scoring mode is `ranked`
- WHEN the scoring service runs that model without a runtime mode override
- THEN it uses ranked cohort construction
- AND stored prediction snapshots identify `ranked`

#### Scenario: Runtime mode contradicts registry

- GIVEN an ACTIVE registered model pinned to `ranked`
- WHEN a caller explicitly requests `raw`
- THEN the run fails before scoring
- AND no mixed-mode prediction is written

#### Scenario: Unregistered model remains backward compatible

- GIVEN an unregistered model version
- WHEN no explicit mode is supplied
- THEN existing environment/default mode resolution is retained

#### Scenario: Ranked mode rejects the single-stock construction

- GIVEN a model whose effective mode is `ranked`
- WHEN its single-stock scoring path is requested
- THEN the request fails before scoring
- AND it does not silently compute a raw score under the ranked model version

### Requirement: Close-observed stop losses execute no earlier than next open

The simulator SHALL make close-observed stop-loss exits causal for
`SCORE_THRESHOLD`, `SCORE_MOMENTUM`, `MULTI_HORIZON_CONSENSUS`, and
`TOP_N_ROTATION`. A stop-loss condition observed from a session close SHALL form
a pending exit after that close. It SHALL first attempt execution at the next
actual tradable session's adjusted open with directional friction. It SHALL NOT
execute at the triggering close or that session's open. A blocked next session
SHALL retain the pending exit for a later tradable open. A score exit already
pending at an execution open SHALL execute first.

All four strategies SHALL use the same shared directional tradability predicate.
A session is executable only when adjusted open is finite and positive, trade
status is known and not suspended, and the requested direction is not blocked by
the applicable limit-up or limit-down rule. A missing quote, missing/invalid
adjusted open, missing status, or unknown status SHALL be blocked rather than
treated as tradable.

If no later session exists, the stop exit SHALL remain unexecuted; any
end-of-range valuation or liquidation SHALL NOT be recorded as that stop-loss
fill.

#### Scenario: Close breach exits at next open

- GIVEN a held stock whose day D close breaches the stop-loss threshold
- WHEN D1 is the next tradable session
- THEN the exit executes at D1 adjusted open with sell-side friction
- AND no sell is recorded on D

#### Scenario: First next session is blocked

- GIVEN a day D close breach and a blocked session D1
- WHEN D2 is the next tradable session
- THEN no sell is recorded on D1
- AND the pending exit executes at D2 adjusted open

#### Scenario: Range ends after the close breach

- GIVEN a stop-loss breach on the last available session
- WHEN no later trading session exists
- THEN no stop-loss fill is recorded
- AND the unexecuted pending exit is reported

### Requirement: Single-stock score generation uses the canonical quote key

The score-generation dependency check SHALL query a stock quote by the quote
model's canonical `code` field.

#### Scenario: Existing single-stock quote passes dependency check

- GIVEN a quote stored with `code=X` on the requested date
- WHEN score generation is requested for `stock_code=X`
- THEN the quote dependency check succeeds
- AND scoring is allowed to continue
