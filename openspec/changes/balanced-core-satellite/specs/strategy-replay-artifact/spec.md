# Strategy replay artifact

## ADDED Requirements

### Requirement: A replay artifact SHALL carry the complete decision and NAV process

A strategy replay SHALL be a single read-only research record containing:

- a stable `replay_id` (versioned `<strategy>_<window>_<aum>_vN`),
  `schema_version`, `source_artifact`, `input_hash`, strategy name, as-of range,
  AUM, the validated `config` and `config_hash`;
- one `curve` point per session with `date`, `nav`, `benchmark_nav`, `drawdown`,
  `cash`, `positions_count` and `gross_exposure`;
- one `decisions` record per rebalance with decision date, execution date,
  target weights, added/removed names, cash before/after and estimated cost;
- one `fills` record per executed order with `decision_id`, `execution_date`,
  side, quantity, price and each fee, plus a `blocked_orders` list with the
  blocked/failed reason;
- `metrics` for total and annualised return, Sharpe, maximum drawdown, annual
  turnover, total fees, average cash share and benchmark return;
- `data_as_of` (the maximum `curve.date`), `generated_at`, `evidence_kind=REPLAY`,
  `survivorship_gap`, `limitations[]`, `research_grade=RESEARCH` and a
  non-investment-advice disclaimer.

Every curve point, decision and fill SHALL trace to the same run and config; no
value may be invented or silently dropped.

#### Scenario: A replay shows decisions and the curve

- GIVEN a completed replay with three rebalances
- WHEN the artifact is read
- THEN it contains exactly three decision records
- AND the curve has one point per session in the evaluated window
- AND every fill references a decision and an execution date

#### Scenario: A missing price fails closed

- GIVEN a held name with no usable price in the window
- WHEN the replay is built
- THEN the build fails with the name and date
- AND no partial artifact is persisted

### Requirement: A replay SHALL never be forward evidence

Every replay artifact SHALL persist `evidence_kind=REPLAY`. The API SHALL report
`forward_sessions=0`, `forward_required=120` and `forward_eligible=false` for a
replay, and replay output MUST NOT create, modify or count toward a
`StrategyForwardWindow`, a `StrategyPaperRun` or the score-driven forward
counter. The forward-progress path MUST NOT read `strategy_replays`. The UI SHALL
display the REPLAY label next to any forward-progress badge.

#### Scenario: The forward badge never advances from a replay

- GIVEN repeated replay builds over new sessions
- WHEN the forward progress is read
- THEN the count remains 0/120
- AND the artifact remains REPLAY

### Requirement: A replay artifact SHALL be immutable for a given input set

A `replay_id` SHALL be a versioned slug `<strategy>_<window>_<aum>_vN`.
Rebuilding with the same id and the same input hash SHALL produce byte-identical
content and may replace the stored document. Rebuilding with a different input
hash under an existing id SHALL fail closed; the operator SHALL use a new
versioned id. The API SHALL expose the input hash so a consumer can detect a
changed artifact.

#### Scenario: A changed config under the same id fails

- GIVEN an existing replay id
- WHEN a rebuild uses the same id but a different config hash
- THEN the build fails closed
- AND the stored artifact is unchanged

### Requirement: The replay API SHALL be read-only and authenticated

`GET /api/strategy/replays` SHALL return paginated summaries with default
`page_size=20` and a maximum of 200, and `GET /api/strategy/replays/<replay_id>`
SHALL return one full artifact. Both SHALL require a user JWT and SHALL reject
OpenClaw service tokens. There SHALL be no create, update or delete endpoint. A
missing id SHALL return the existing `{success, message, request_id,
generated_at, data:null}` 404 envelope.

#### Scenario: List and detail are read-only

- GIVEN a persisted replay
- WHEN the list and detail endpoints are called with a user JWT
- THEN both return the artifact
- AND no other HTTP method is accepted
- AND an OpenClaw service token receives 403

#### Scenario: Pagination bounds are clamped

- GIVEN a request with `page_size=10000`
- WHEN the list endpoint responds
- THEN the effective page size is 200

### Requirement: The curve SHALL be bounded for the storage and response contract

A replay SHALL cover at most 5,000 sessions. A longer window SHALL fail closed
with a clear error rather than silently truncating. The list endpoint SHALL NOT
return the full curve or decisions; only the detail endpoint SHALL.

#### Scenario: A 6,000-session window is rejected

- GIVEN a request for a 6,000-session replay
- WHEN the build runs
- THEN it fails with a clear bound error
- AND no artifact is persisted

### Requirement: The frontend SHALL show the complete replay process

The strategy replay page SHALL show the config, metrics, NAV/drawdown/benchmark
chart, the decision timeline and the fills for each decision. It SHALL show the
REPLAY badge, `0/120`, the research-grade disclaimer and the
survivorship/QDII limitations next to the metrics, and it SHALL distinguish an
empty state from an error.

#### Scenario: A decision expands to its fills

- GIVEN a replay with a decision that bought two names
- WHEN the user expands that decision
- THEN the two fills, their quantities, prices and fees are visible

### Requirement: The replay runner SHALL be a separate operator-run research surface

The replay runner and the `strategy_replays` collection SHALL NOT import, read,
write or depend on the frozen `factor_lab_paper_run.py` ledger state,
`StrategyPaperRun`, or `StrategyForwardWindow`. The runner SHALL be operator-run
(not scheduled by a CronJob) and SHALL NOT expose a create/update/delete
endpoint. The backend SHALL only read the collection.

#### Scenario: The replay runner is isolated from the frozen ledger

- GIVEN the replay runner and the frozen research paper ledger
- WHEN their imports and collections are inspected
- THEN the replay runner does not import or write the ledger state
- AND it does not create or modify a `StrategyPaperRun`
- AND no scheduled job invokes it
