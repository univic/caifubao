# Production Capability Roadmap

## ADDED Requirements

### Requirement: Public positioning stays research/demo/MVP

All public-facing material (README, docs, UI copy, OpenClaw/API descriptions) MUST
keep the research/demo/MVP positioning and the no-investment-advice disclaimer, even
as internal capability targets live-trading readiness. No public text MAY present
model output as tradable advice or imply real-money operation.

#### Scenario: Public docs keep the demo positioning

- GIVEN the project README and capability documentation
- WHEN they describe system outputs (scores, recommendations, paper results)
- THEN they keep the "research / learning / demonstration MVP, not investment
  advice" framing
- AND no public page claims live-trading or advisory capability

#### Scenario: Research output is labeled as such

- GIVEN a model output produced outside the ≥120-day paper evidence gate
- WHEN it is surfaced to the user (list, report, dashboard)
- THEN it is labeled research/observation-grade
- AND the underlying model is not marked tradable or promoted

### Requirement: Real-money execution is gated and default-off

This roadmap change MUST NOT authorize real-money execution; any future execution
capability MUST be default-off, paper/audit-first, operator-gated, and stoppable
(kill-switch halting order generation), and the strategy layer MUST remain
paper-only until that gate exists.

#### Scenario: Strategy layer stays paper-only absent the gate

- GIVEN no execution gate has been enabled for an environment
- WHEN a daily strategy run occurs
- THEN it only records target holdings / rebalance lists and paper NAV
- AND it never submits orders or mutates a real account

#### Scenario: Enabling execution requires explicit authorization

- GIVEN an environment where execution capability is being enabled
- WHEN the operator attempts to turn it on
- THEN the enabling step requires explicit per-environment authorization
  (documented operator action, not a default)
- AND a paper/audit trail of the decision exists before any order

### Requirement: Promotion to tradable requires ≥120-day paper evidence

A model version MUST NOT be promoted to "tradable/actionable" status until it has
≥120 trading days of immutable forward paper evidence captured by the separate
forward-capture change, evaluated against its research walk-forward expectation.
The causal-timing correction's `REPLAY` records, historical backfills,
replacements, NAV recomputations, and job `SUCCESS` statuses MUST NOT satisfy
this gate; until the gate is met the version MAY only produce research/watch
outputs.

#### Scenario: Unvalidated model stays research-grade

- GIVEN a model version with less than 120 immutable forward paper sessions,
  regardless of its replay or job-success count (e.g. flip_wide_shadow_v1)
- WHEN a consumer asks for actionable output
- THEN the output is labeled research/observation-grade
- AND the version is not marked tradable or promoted

#### Scenario: Paper evidence window is counted from immutable forward capture

- GIVEN the daily strategy operator chain (job_family strategy_daily)
- WHEN counting progress toward the 120-day gate
- THEN only immutable forward-capture sessions with the required causal timing,
  complete execution-day evidence, and a fixed config hash count
- AND `evidence_kind=REPLAY`, historical backfills, replacements, NAV
  recomputations, and `strategy_daily` `SUCCESS` records do not count
- AND the count and NAV metrics are recorded in the autoresearch ledger

### Requirement: Causal replay integrity precedes forward evidence

The roadmap MUST track P0 causal replay integrity as `IN PROGRESS` before any
forward evidence counter is started. The first causal slice MUST use
`timing_version=paper_causal_v1`, consume only usable score states
(`PENDING`/`TRACKING`/`VERIFIED`/`INSUFFICIENT_DATA`, excluding `BLOCKED`/`FAILED`),
persist actual UTC `decision_at` and the next calendar-session
`execution_date`, and mark all output `evidence_kind=REPLAY`.

#### Scenario: Historical dates do not start the forward window

- GIVEN a replay record dated `2026-06-10` or `2026-09-04`
- WHEN the operator evaluates the 120-session gate
- THEN neither date is certified as the forward start
- AND the record remains replay evidence regardless of run or job `SUCCESS`
