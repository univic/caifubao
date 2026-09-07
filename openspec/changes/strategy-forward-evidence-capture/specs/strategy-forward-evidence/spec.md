# Strategy Forward Evidence Capture

## ADDED Requirements

### Requirement: Forward evidence MUST be causally certified and immutable

A strategy run record MUST be certified `evidence_kind=FORWARD` only when its
decision timestamp precedes the open of its calendar execution date — computed
by the #202 helper `next_execution_date(signal_date, ChinaAStock market
calendar)` — the signal date falls inside an ACTIVE certified window for the
record's exact config hash, and no pre-existing COMPLETED plan record with the
same unique key is overwritten (a SKIPPED/FAILED document MAY be rewritten
without `--replace`; a same-day SKIPPED→evening rerun is the intended forward
capture path and MUST classify as FORWARD, never REPLAY). Legacy or missing
provenance MUST remain REPLAY. Certification premise: usable scores for a
signal date exist only after that session's close (scoring cadence), so an
on-time same-session run cannot observe execution-window outcomes.

Once FORWARD, the captured plan MUST NOT be modified by replace, backfill, or
rerun; NAV recomputation MUST NOT alter `target_holdings`, `status`,
`evidence_kind`, `decision_at`, `execution_date`, or `config_hash`.

#### Scenario: On-time same-session run is certified FORWARD

- GIVEN an ACTIVE certified window whose start_date is today's trading session,
  today's scores exist (post-close), and no COMPLETED plan exists for the date
- WHEN the operator runs the strategy for the signal date after market close
- THEN the record is written with `evidence_kind=FORWARD`
- AND `decision_at` precedes the open of its `execution_date` (next session)

#### Scenario: Same-day SKIPPED rerun upgrades to FORWARD

- GIVEN a SKIPPED document for the signal date (e.g. scores were not yet
  usable at the morning attempt)
- WHEN the operator reruns after scores exist, still before the execution
  session opens
- THEN the SKIPPED document is replaced by a FORWARD COMPLETED record
- AND it counts toward the forward session counter

#### Scenario: Late or backdated run is REPLAY

- GIVEN the operator runs the strategy for a stale signal date whose execution
  date has already opened (or outside the window)
- WHEN the run completes
- THEN the record is `evidence_kind=REPLAY`
- AND it does not count toward the forward session counter

#### Scenario: Replacing a FORWARD record is rejected

- GIVEN an existing FORWARD COMPLETED record for a signal date
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

### Requirement: FORWARD records MUST keep portfolio continuity and NAV like REPLAY

FORWARD records MUST participate in portfolio continuity exactly like REPLAY
records: the previous-run holdings query (for the rebalance diff) and the
`run_nav` COMPLETED-run set MUST span both evidence kinds. Only evidence
certification and plan immutability differ between the kinds; evidence-kind
filters MUST NOT silently exclude FORWARD records from the paper track.

#### Scenario: Second forward day rebalances against the first

- GIVEN a FORWARD COMPLETED record on day S and a second on-time run on day S+1
- WHEN the second run computes its rebalance diff
- THEN the previous holdings come from the FORWARD record on S
- AND the diff (added/removed/unchanged) is computed against those holdings

#### Scenario: NAV curve includes FORWARD records

- GIVEN FORWARD COMPLETED runs across a window
- WHEN `run_nav` recomputes the curve over the window
- THEN every FORWARD COMPLETED run contributes a curve point
- AND the recompute never alters their plan evidence

### Requirement: Certified window MUST be append-only and unique per model/horizon

Forward evidence MUST be organized in a certified window per
(model_version, horizon) — ACTIVE uniqueness on that pair — with an explicit
`start_date` equal to the certification session (never backdated). Certifying
a new window MUST close any ACTIVE predecessor on the same
(model_version, horizon), because score-source or strategy-config changes mean
evidence cannot span configurations. The operator MAY explicitly close an
ACTIVE window and open a fresh one under the same key with a new `start_date`
(e.g. after a prolonged pause); the counter then restarts from the new start.

#### Scenario: Config change closes the window

- GIVEN an ACTIVE window on config_hash A and a new strategy config with hash B
- WHEN the operator certifies a new window for B
- THEN window A is CLOSED
- AND only B-window FORWARD dates count toward B's promotion gate

#### Scenario: Same-config explicit restart resets the counter

- GIVEN an ACTIVE window on (model_version, horizon, config_hash A) with a
  pause, and the operator explicitly closes it and certifies a fresh window on
  the same key
- WHEN the new window's progress is read
- THEN the counter counts only FORWARD dates at or after the new start_date
- AND the closed window's dates no longer count toward the new window

### Requirement: 120-session counter MUST be append-only and exact

The 120-session counter MUST count only distinct FORWARD COMPLETED signal
dates from the window's `start_date` through the reporting as-of date while
ACTIVE (or through the close date once CLOSED). Job SUCCESS, REPLAY records,
replacement, backfill, and NAV recomputation MUST NOT count. Missing sessions
within an ACTIVE window MUST be reported as gaps and never fabricated into the
count; a promotion gate (roadmap 0.3) requires count >= 120 on one window plus
the NAV-based evaluation against the research walk-forward expectation.

#### Scenario: Counter counts only certified forward dates

- GIVEN a window with start_date S and daily FORWARD captures thereafter
- WHEN the progress report is read as of date T
- THEN the count equals the distinct FORWARD COMPLETED signal dates in
  [S, T]
- AND skipped, failed, replay, or replaced dates are excluded
- AND gaps are reported but never fabricated into the count

#### Scenario: History never counts toward the 120 sessions

- GIVEN records with `evidence_kind=REPLAY` (or legacy missing provenance) for
  any dates, including 2026-06-10 and 2026-09-04
- WHEN the counter is evaluated
- THEN none of those dates contribute
- AND no backfill or job-SUCCESS record contributes
