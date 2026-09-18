# Research Paper Ledger Delta

## ADDED Requirements

### Requirement: Research paper decisions SHALL be causal, factor-driven and immutable

The research paper ledger SHALL derive every decision from the registered
factor-lab panel only, and its composite weights SHALL use exclusively factor ICs
whose forward labels were realised strictly before the decision session (a
label-realisation cutoff of D - h - 1 for a weight-label horizon h), so no future
price can enter the score. A decision SHALL be taken as of a completed session's
close and SHALL name an execution session strictly later than the decision
session. Each decision SHALL be written exactly once as an append-only immutable
record keyed by its decision date, carrying at minimum the decision date, the
intended execution session, the target basket, per-name intended lots, expected
cash, account size, component factors, the weight-label lag, an idempotency key
and `evidence_kind=REPLAY`. A second write for an already recorded decision date
SHALL fail closed and leave the existing record byte-identical. The record SHALL
express research intent only and MUST NOT be represented as an order, an
execution instruction, or advice.

#### Scenario: Weights use only realised ICs

- **GIVEN** a decision session D and a weight-label horizon h
- **WHEN** the causal composite is computed
- **THEN** every IC contributing to the weights SHALL have a label realised
  strictly before D
- **AND** no IC whose label is realised at or after D SHALL contribute

#### Scenario: Duplicate decision date fails closed

- **GIVEN** an already recorded decision for date D
- **WHEN** the decision step is run again for D
- **THEN** it SHALL fail closed without writing
- **AND** the existing record SHALL be unchanged

### Requirement: Research paper marks SHALL replay only recorded decisions

The mark step SHALL replay exactly the recorded decisions in decision-date order,
filling at the recorded execution session's open subject to availability: a name
that is limit-up at the open or suspended SHALL NOT be filled and MUST NOT be
back-filled at a later session (the cash stays idle at zero yield), while a
blocked exit SHALL roll forward until it becomes executable. Costs SHALL be the
documented round-trip rate charged once per round trip plus the per-trade
minimum commission per side. The report SHALL state NAV, drawdown, turnover and
the equal-weight-universe benchmark, and SHALL carry `evidence_kind=REPLAY`.
Every reported position SHALL trace to a recorded decision; no decision may be
invented, dropped or reordered.

#### Scenario: Blocked entry is never back-filled

- **GIVEN** a recorded decision whose execution session has a limit-up or
  suspended target name
- **WHEN** the mark step replays it
- **THEN** the name SHALL NOT be filled and the cash SHALL stay idle
- **AND** no later session SHALL be used to fill it

#### Scenario: Mark is derived only from recorded decisions

- **GIVEN** a state directory of recorded decisions
- **WHEN** the mark step runs
- **THEN** every position SHALL trace to one of those decisions
- **AND** no decision SHALL be invented, dropped or reordered

### Requirement: Research paper records are REPLAY-only and never counted as forward evidence

Every record written by the research paper ledger SHALL carry
`evidence_kind=REPLAY`, including current-session runs and runs whose outcomes
are already visible. The ledger MUST NOT create, modify, or consume as evidence
a `StrategyPaperRun`, a certified forward window, or the 120-session counter;
MUST NOT be read by the score-driven paper report, NAV or forward-progress
commands; MUST NOT be exported as a target list; and MUST NOT be described as
forward-validated, tradable or promoted. Job-run SUCCESS and replay outcomes
MUST NOT advance the forward count.

#### Scenario: Research paper records are REPLAY-only and never counted as forward evidence

- **GIVEN** research paper decision and mark records for any dates, including the
  current session
- **WHEN** the forward-evidence counter or the promotion gate is evaluated
- **THEN** every such record SHALL be `evidence_kind=REPLAY`
- **AND** none SHALL contribute a forward session or any promotion input

#### Scenario: Research ledger does not enter the score-driven paper track

- **GIVEN** a research ledger record
- **WHEN** the operator runs the score-driven paper report, NAV or
  forward-progress commands
- **THEN** the record SHALL NOT be consumed as a score-driven paper run
- **AND** it SHALL NOT be exported as a target list

### Requirement: Research paper tooling stays research-only and unregistered

The ledger SHALL live under `datahub/scripts/` as operator-run research tooling;
SHALL NOT be imported by scoring, signal, strategy, job, API or export modules;
SHALL NOT expose a public REST endpoint; SHALL NOT schedule itself; SHALL NOT
persist a Mongo backtest or mutate any Mongo collection, account, paper-run or
forward-window artifact; and SHALL write only its own research state directory.
Its strategy identifier MUST NOT be registered as a score model version and the
ledger MUST NOT claim the score-driven paper timing version. The score-driven
runner's model-registration and timing-version contracts MUST remain unchanged
and MUST NOT be relaxed to admit this ledger.

#### Scenario: No registration is required or created

- **GIVEN** the research paper ledger is run
- **WHEN** the scoring model registry and the strategy config validator are
  inspected
- **THEN** no score model version SHALL have been created or be required for it
- **AND** no strategy config claiming the score-driven paper timing version SHALL
  have been written or validated

#### Scenario: Production never imports the ledger

- **GIVEN** the production scoring, signal, strategy, job and API modules
- **WHEN** their imports are inspected
- **THEN** none SHALL import the research paper ledger

### Requirement: Research paper results SHALL be recorded with their evidence kind

Every cited research paper result SHALL be appended to
`docs/operations/strategy-experiments-2026-08.md` with its commands, as-of range,
cost assumptions, an explicit REPLAY research-observation label and its material
limitations (blocked-entry skip rate, cash drag, drawdown). The entry MUST NOT
claim forward validation, tradability or promotion.

#### Scenario: Documented result carries its evidence kind

- **GIVEN** a research paper run recorded in the experiments ledger
- **WHEN** a reader inspects the entry
- **THEN** it SHALL name the commands, parameters, cost assumptions and
  `evidence_kind=REPLAY`
- **AND** it SHALL claim no forward validation, tradability or promotion
