# Strategy Forward Evidence Capture

## ADDED Requirements

### Requirement: Forward evidence MUST be causally certified and immutable

A strategy run record MUST be certified `evidence_kind=FORWARD` only when its
decision timestamp precedes the open of its calendar execution date (the next
trading session after the signal date), the signal date falls inside an ACTIVE
certified window for the record's exact config hash, and no pre-existing record
is overwritten. Once FORWARD, the captured plan MUST NOT be modified by
replace, backfill, or rerun; NAV recomputation MUST NOT alter
`target_holdings`, `status`, `evidence_kind`, or the recorded timestamps.
Legacy or missing provenance MUST remain REPLAY.

#### Scenario: On-time same-session run is certified FORWARD

- GIVEN an ACTIVE certified window for (model_version, horizon, config_hash)
  whose start_date is today's trading session, and today's scores exist
- WHEN the operator runs the strategy for the signal date after market close
- THEN the record is written with `evidence_kind=FORWARD`
- AND `decision_at` precedes the open of its `execution_date` (next session)

#### Scenario: Late or backdated run is REPLAY

- GIVEN the operator runs the strategy for a stale signal date whose execution
  date has already opened (or outside the window)
- WHEN the run completes
- THEN the record is `evidence_kind=REPLAY`
- AND it does not count toward the forward session counter

#### Scenario: Replacing a FORWARD record is rejected

- GIVEN an existing FORWARD record for a signal date
- WHEN the operator attempts `run --replace` (or any rerun writing the same
  unique key)
- THEN the runner fails closed with an error
- AND the FORWARD record's plan is unchanged

#### Scenario: NAV recompute never changes the plan evidence

- GIVEN a FORWARD record with a stored nav snapshot
- WHEN the NAV recompute runs with corrected fees or quotes
- THEN only derived NAV fields may change
- AND `target_holdings` / `status` / `evidence_kind` / `decision_at` /
  `execution_date` / `config_hash` are unchanged
- AND the session counter is unaffected

### Requirement: Certified window and 120-session counter MUST be append-only and exact

Forward evidence MUST be organized in an append-only certified window per
(model_version, horizon, config_hash) with an explicit start date; the
120-session counter MUST count only distinct FORWARD COMPLETED signal dates at
or after that start date within the window. Job SUCCESS, REPLAY records,
replacement, backfill, and NAV recomputation MUST NOT count. Changing the score
source or strategy config MUST close the old window so evidence cannot span
configurations.

#### Scenario: Counter counts only certified forward dates

- GIVEN a window with start_date S and daily FORWARD captures thereafter
- WHEN the progress report is read
- THEN the count equals the distinct FORWARD COMPLETED signal dates in
  [S, end]
- AND skipped, failed, replay, or replaced dates are excluded
- AND gaps are reported but never fabricated into the count

#### Scenario: Config change closes the window

- GIVEN an ACTIVE window on config_hash A and a new strategy config with hash B
- WHEN the operator certifies a new window for B
- THEN window A is CLOSED
- AND only B-window FORWARD dates count toward B's promotion gate

#### Scenario: History never counts toward the 120 sessions

- GIVEN records with `evidence_kind=REPLAY` (or legacy missing provenance) for
  any dates, including 2026-06-10 and 2026-09-04
- WHEN the counter is evaluated
- THEN none of those dates contribute
- AND no backfill or job-SUCCESS record contributes
