# Strategy Target Portfolio Export

## ADDED Requirements

### Requirement: Export MUST stay research-grade until an explicit promotion record exists

Every export MUST carry a research/observation-grade label and the
"research / learning / demonstration MVP, not investment advice" disclaimer, and
that label MUST be applied unconditionally. Promotion MUST NOT be inferred from
`evidence_kind`, forward-session counts, `forward progress`, job statuses, NAV
records, or any other proxy. Because no explicit promotion record exists yet
(roadmap 0.3 is unmet for every model version), every export is research-grade,
and an unpromoted model version MUST NOT be presented as tradable, actionable,
or validated.

#### Scenario: Research-grade label is unconditional

- GIVEN any COMPLETED paper run, including one with `evidence_kind=FORWARD` or a
  certified forward window whose session count has reached 120
- WHEN the export is rendered
- THEN the export carries the research/observation-grade label and the disclaimer
- AND no evidence proxy drops, upgrades, or conditions that label

#### Scenario: Export never claims validation beyond evidence

- GIVEN a run recorded as `evidence_kind=REPLAY`
- WHEN the export is rendered
- THEN the export reports the run's actual evidence kind
- AND it does not describe the output as forward-validated

### Requirement: Export MUST emit a deterministic target-and-rebalance list

The export MUST derive its rows from one persisted COMPLETED paper run and MUST
classify each stock code from that run's rebalance diff: codes in `added` are
`BUY`, codes in `removed` are `SELL`, and codes in `unchanged` are `HOLD`. Those
labels describe the run's rebalance diff; they are NOT a recommendation, an
execution instruction, or a claim that an order should be placed, and the export
MUST NOT be described as actionable or tradable.

#### Scenario: Added, removed, and unchanged map to BUY, SELL, and HOLD

- GIVEN a COMPLETED run with `rebalance = {added: [a], removed: [b],
  unchanged: [c]}` and target holdings weights for `a` and `c`
- WHEN the export builds its rows
- THEN `a` is `BUY`, `b` is `SELL`, and `c` is `HOLD`
- AND the export carries no field or wording presenting the list as advice or as
  an execution instruction

#### Scenario: HOLD means unchanged membership only

- GIVEN a code present in both the previous and the current target holdings
- WHEN the export renders its row
- THEN its side is `HOLD`
- AND `HOLD` carries no guarantee that the position needs no order, because
  equal-weight per-name weights move with the holding count

#### Scenario: SELL rows are not sized from the paper track

- GIVEN a code in `removed`
- WHEN the export renders its row
- THEN the row carries the code and the reason
- AND the export does not fabricate a sell quantity or amount from the paper
  target, because the operator's real account sits outside the paper track

### Requirement: Export MUST carry per-row score evidence without fabricating it

Each row MUST carry the signal-date score and percentile for its stock code when
a prediction for the run's date, model version, and horizon exists, plus a
reason token. Missing evidence MUST be reported as null and MUST NOT be
fabricated.

#### Scenario: Score evidence is joined when present

- GIVEN a signal-date prediction row for an exported stock code
- WHEN the export builds that row
- THEN the row carries that prediction's score and percentile

#### Scenario: Missing evidence stays null

- GIVEN no prediction row for an exported stock code
- WHEN the export builds that row
- THEN its score and percentile are null
- AND no value is invented for it

### Requirement: Export MUST resolve exactly one run or fail closed

The export MUST fail closed with an explicit error when no run matches the
requested date, model version, and horizon, or when the resolved run is not
COMPLETED. Because the persisted run key includes `config_hash`, a request that
matches more than one COMPLETED configuration MUST fail closed as ambiguous
rather than silently selecting one.

#### Scenario: Missing run is an explicit error

- GIVEN no paper run for the requested date, model version, and horizon
- WHEN the operator requests an export
- THEN the command exits non-zero with an explicit "no completed paper run"
  message
- AND it prints no target rows

#### Scenario: Skipped, failed, or running run is not exportable

- GIVEN the only matching run has status `SKIPPED`, `FAILED`, or `RUNNING`
- WHEN the operator requests an export
- THEN the command fails closed with the run's status in the message
- AND it prints no target rows

#### Scenario: Ambiguous configurations fail closed

- GIVEN two COMPLETED runs for the same date, model version, and horizon under
  different `config_hash` values
- WHEN the operator requests an export without naming a configuration
- THEN the command exits non-zero with an ambiguity error naming the config hashes
- AND it prints no target rows

### Requirement: Export MUST derive all amounts from one reported base NAV

Amounts MUST be derived from a single base NAV resolved in this order: an
explicit operator override, then the run's persisted `nav_snapshot.nav` when it
is present and positive, then the run's persisted `config.initial_nav` when it is
present and positive. The export MUST report both the resolved value and its
source, and MUST fail closed rather than substituting any other NAV.

#### Scenario: NAV snapshot takes precedence over the configured initial NAV

- GIVEN a run with both a positive `nav_snapshot.nav` and a positive
  `config.initial_nav`
- WHEN the export resolves the base NAV
- THEN the snapshot value is used
- AND the reported source names the snapshot

#### Scenario: Configured initial NAV is the fallback

- GIVEN a run with no usable `nav_snapshot.nav` and a positive
  `config.initial_nav`
- WHEN the export resolves the base NAV
- THEN the configured value is used
- AND the reported source names the configured initial NAV

#### Scenario: No usable base NAV fails closed

- GIVEN a run with neither a positive NAV snapshot nor a positive configured
  initial NAV
- WHEN the export resolves the base NAV
- THEN the export fails closed with an explicit error
- AND it does not substitute a live or guessed NAV

#### Scenario: Target amounts use the resolved base NAV

- GIVEN rows that carry a target weight
- WHEN amounts are derived
- THEN each such row's amount equals its target weight times the reported base NAV
- AND rows without a target weight carry no amount

### Requirement: Export MUST be read-only and MUST NOT be an execution instruction

The export MUST NOT write to any collection, MUST NOT submit or simulate orders,
MUST NOT mutate paper runs, forward windows, NAV snapshots, or any other
persisted artifact, and MUST NOT create a job-run or audit record. It is a
reading aid over already-persisted evidence.

#### Scenario: Export leaves persistence untouched

- GIVEN any persisted paper run
- WHEN the operator runs an export
- THEN no document is created, modified, or deleted
- AND no job-run record is created
- AND no order is generated or submitted

#### Scenario: Export is not an order instruction

- GIVEN any export
- WHEN it is rendered
- THEN it contains no order, quantity instruction, or execution directive

### Requirement: Export MUST be deterministic and traceable

Row ordering MUST be deterministic for the same run and configuration, and the
export MUST surface the run's strategy name, model version, horizon,
`config_hash`, signal date, execution date, evidence kind, and status so a human
can trace the list back to the decision that produced it.

#### Scenario: Repeated export is identical

- GIVEN the same persisted run and configuration
- WHEN the export is rendered twice
- THEN both renders produce identical rows in identical order

#### Scenario: Traceability fields are present

- GIVEN an exported run
- WHEN the operator inspects the export metadata
- THEN strategy name, model version, horizon, `config_hash`, signal date,
  execution date, evidence kind, and status are present

### Requirement: The written artifact MUST itself carry the label and disclaimer

When rows are written as CSV, the artifact itself MUST carry the
research-grade label and the disclaimer as a leading comment line, so a copied
or redistributed file cannot lose its compliance context. The JSON form MUST
carry both as fields.

#### Scenario: CSV artifact carries the label and disclaimer

- GIVEN an export written as CSV, with or without `--output`
- WHEN a human opens that file alone
- THEN its leading comment line contains the research-grade label and the
  disclaimer
- AND the row header and rows follow that comment line
