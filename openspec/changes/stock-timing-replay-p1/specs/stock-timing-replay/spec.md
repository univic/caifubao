# Stock Timing Replay Adapter P1

## ADDED Requirements

### Requirement: Replay cohort membership is frozen point-in-time evidence

The system SHALL accept only an explicit frozen cohort manifest with a stable
source, timezone-aware `as_of`, `membership_basis=point_in_time`,
`delisted_completeness=VERIFIED`, and an immutable provenance artifact URI and
lowercase SHA-256. Provenance SHALL report source member count, subsequently-
delisted count, suspended count, and explicit affirmative coverage of
subsequently delisted and suspended names. Every member SHALL provide a stock
code, listing date, optional delisting date, and evidence timestamp no later
than manifest `as_of`. Manifest `as_of` SHALL be earlier than the first replay
session open. A member SHALL be listed and not yet delisted at `as_of`; a member
delisted later SHALL remain included. Counts SHALL reconcile with the member
records. The provenance hash SHALL be included in the source identifier passed
to P0 and therefore in P0's cohort identity. The system SHALL NOT derive
membership from current active status or successful quote/score rows.

#### Scenario: Subsequently delisted member remains in the cohort

- GIVEN a member listed by `as_of` and delisted after `as_of`
- WHEN the frozen cohort is built
- THEN the member remains requested and any missing replay evidence counts as a
  failed row rather than removing the member

#### Scenario: Current-active or incomplete provenance is rejected

- GIVEN a manifest based on current-active discovery, without verified
  delisted coverage, without suspended-name coverage, with unreconciled counts,
  without an immutable provenance hash, or with evidence created after `as_of`
- WHEN replay is requested
- THEN the command fails before loading per-stock replay evidence

### Requirement: Replay pins one active ranked scoring construction

The system SHALL require an exact model version, model configuration hash,
horizon, entry percentile, and exit percentile. The resolved registry record
SHALL be ACTIVE, SHALL pin `scoring_mode=ranked`, and SHALL match the supplied
configuration hash. Missing, retired, raw, unpinned-mode, or hash-mismatched
models SHALL fail before any stock replay. Runtime defaults SHALL NOT replace a
missing pin.

#### Scenario: Registry pin matches the replay manifest

- GIVEN one ACTIVE ranked registry record whose version and configuration hash
  equal the manifest
- WHEN replay starts
- THEN the pinned version/horizon/configuration are recorded in the report

#### Scenario: Registry construction is ambiguous or mismatched

- GIVEN no registry row, a retired row, mode other than ranked, or a different
  configuration hash
- WHEN replay starts
- THEN replay fails before any per-stock runner call

### Requirement: Every ranked signal binds to a causal daily cohort

The manifest SHALL provide one immutable prediction-cohort provenance record
for every signal date used by replay. Each record SHALL include the signal
date, artifact URI and lowercase SHA-256, cohort fingerprint, member count, and
timezone-aware `data_as_of` no later than the authoritative signal-session
close. Every stored prediction SHALL have `prediction.date` equal to that
signal date and SHALL repeat exact `freshness=FRESH`, `scoring_mode=ranked`,
ranked status, cohort fingerprint, artifact hash, and `data_as_of` in its input
snapshot. Missing or
mismatched daily provenance, a future cutoff, or a prediction derived from a
current-active-only cohort SHALL be unusable and SHALL create no signal.
Canonical SHA-256 hashes of the complete daily prediction-cohort map and the
authoritative trading calendar SHALL be included in the fixed P0 configuration
and therefore in P0's report identity.

#### Scenario: Ranked prediction matches its frozen daily universe

- GIVEN a D prediction whose input snapshot matches D's manifest cohort
  fingerprint and artifact hash and whose cutoff is no later than D close
- WHEN replay evaluates D
- THEN D may form a timing intent for a later session open

#### Scenario: Legacy, stale, or future-contaminated prediction fails closed

- GIVEN a prediction whose input snapshot lacks exact `FRESH` status, lacks a
  cutoff/artifact binding, has a mismatched fingerprint, or has `data_as_of`
  after D close
- WHEN replay evaluates D
- THEN it creates no timing signal and the missing/mismatched provenance is
  retained in replay diagnostics

### Requirement: Real evidence produces normalized same-stock replay pairs

The adapter SHALL consume stored adjusted quote rows and causally-bound full-
cohort ranked prediction rows for each requested stock. A timing signal observed on session D
SHALL execute no earlier than a later tradable session's adjusted open. Entry
SHALL use `percentile >= entry_percentile`; exit SHALL use
`percentile <= exit_percentile`. Missing, stale, invalid, out-of-range, wrong-version,
wrong-horizon, unusable, or non-ranked prediction evidence SHALL produce no new
signal. Blocked fills SHALL remain pending until a later tradable open unless a
new causal signal replaces them.

The same-stock buy-and-hold arm SHALL form an entry intent before the first
window session and fill at the first tradable adjusted open inside the window.
The timing arm's first signal SHALL not fill before a later session open. Both arms SHALL
use the same cash, window, positive board lot, commission/minimum commission,
sell stamp duty, slippage strictly below 100%, directional limit/suspension checks, daily adjusted-
close valuation, and final last-close valuation. It SHALL NOT invent a final
close fill. Every actual trade SHALL be explicit `FILLED` evidence with positive
quantity and execution price. The normalized pair SHALL satisfy the P0
assumptions contract and SHALL NOT substitute an index benchmark.

#### Scenario: Prior-close percentile fills at the next tradable open

- GIVEN a valid ranked percentile on D and a tradable adjusted open on D1
- WHEN the timing threshold fires
- THEN the fill date is D1, its price derives from D1 adjusted open plus
  directional friction, and no D fill exists

#### Scenario: A blocked next session delays but does not backfill a price

- GIVEN a valid D signal and a suspended, limit-blocked, or invalid D1 open
- WHEN a later D2 session becomes tradable
- THEN no fill is recorded on D1 and the pending intent first fills on D2

#### Scenario: Both arms retain open holdings at window end

- GIVEN either arm holds shares on the last session
- WHEN replay ends
- THEN equity uses the last adjusted close and no artificial liquidation trade
  is emitted

### Requirement: Replay is deterministic research-only orchestration

The CLI SHALL load the explicit manifest and real stored evidence, invoke the
P0 pooled evaluator with one normalized pair per requested code, and optionally
write a JSON report. It SHALL NOT generate scores, mutate cohort/model state,
persist backtests, or expose a public API. Identical manifest and evidence rows
SHALL produce identical pairs and cohort identity. Every report SHALL remain
`research_only=true` and `validation_status=UNVALIDATED`.

#### Scenario: Running the adapter causes no database write

- GIVEN valid stored evidence and a valid manifest
- WHEN the timing replay CLI completes
- THEN only reads are issued to the data store
- AND the only optional write is the caller-selected JSON report file
