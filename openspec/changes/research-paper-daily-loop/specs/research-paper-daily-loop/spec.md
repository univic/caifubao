## ADDED Requirements

### Requirement: Scheduled research FORWARD capture SHALL pin a certified immutable StrategySpec

A scheduled Research strategy writer SHALL use an explicitly mounted, versioned
StrategySpec, and SHALL refuse to write unless an ACTIVE operator-certified
forward window matches the exact spec hash, \`hash_kind=spec\`, model version,
horizon and permitted signal date. It MUST NOT create, close or replace a
forward window, automatically promote a strategy, or write to Trading state.

#### Scenario: Uncertified or changed candidate is refused

- **GIVEN** a missing window or a window with a different hash or hash kind
- **WHEN** the scheduled runner evaluates a new signal session
- **THEN** the job fails before strategy-plan writes with a nonzero exit code

#### Scenario: Matching candidate creates only a causal record

- **GIVEN** an ACTIVE matching spec window and current usable daily inputs
- **WHEN** the scheduler runs before the next execution-session open
- **THEN** it writes a completed FORWARD research plan and reports the pinned identity

### Requirement: Daily schedule SHALL enforce causal date and input freshness

The scheduled job SHALL derive its signal date only from Asia/Shanghai current
session, SHALL check the current completed market session, quotes, scoring,
signals and persisted halt state, and SHALL reject stale or missing critical
checks. It MUST NOT support an automated backdate, replace or retrospective
evidence upgrade.

#### Scenario: Exchange holiday

- **GIVEN** a complete calendar containing future sessions but excluding today
- **WHEN** the CronJob starts on a market holiday
- **THEN** it reports SKIP_NON_SESSION without creating a strategy plan

#### Scenario: Stale data or unmounted halt state

- **GIVEN** missing same-day scores, quotes, signals or unverified halt state
- **WHEN** the scheduled job assesses readiness
- **THEN** it fails and must not mark the day a successful FORWARD capture

#### Scenario: Retrospective attempt

- **GIVEN** a start time before the evening cutoff or after the execution open
- **WHEN** the scheduled runner evaluates capture eligibility
- **THEN** it refuses to create forward evidence

### Requirement: Schedules SHALL be fail-safe and observable

The public research CronJob SHALL be suspended by default, use a fixed
Asia/Shanghai schedule, forbid overlapping jobs, and expose failed jobs to
Kubernetes monitoring. Once authorized and enabled, the entry point SHALL
record a traced SUCCESS, SKIPPED or FAILED job run; retries for an already
completed same-spec FORWARD session SHALL be idempotent.

#### Scenario: Already captured FORWARD session

- **GIVEN** a completed matching FORWARD record for today's pinned spec
- **WHEN** an otherwise eligible daily job is retried
- **THEN** it reports ALREADY_COMPLETED and does not change that record

#### Scenario: A strategy run is skipped or yields non-forward evidence

- **GIVEN** a critical runtime condition prevents successful FORWARD capture
- **WHEN** the plan runner returns SKIPPED, REPLAY or throws
- **THEN** the scheduled job reports failure instead of a successful trading day
