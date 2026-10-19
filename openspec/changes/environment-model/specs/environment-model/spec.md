# Environment Model

## ADDED Requirements

### Requirement: Public docs use the two-axis domain/stage vocabulary with reserved stage words

Public documentation that describes runtime environments SHALL use the two-axis
model: a `domain` axis (development / data / research / trading — responsibility,
data ownership, and risk boundary) and a `stage` axis (dev / stable / paper /
production — operational maturity and capital impact). Public docs SHALL NOT
describe the research stable environment or the retired legacy stable
environment as live trading production, and SHALL use the word `production`
only for the `trading/production` real-money stage or for an explicitly labeled
technical/historical name (GitHub Environment, image channel tag, Kubernetes
namespace, retired deployment input, or application config constant such as
`APP_ENV`). The shorthand `prod` SHALL refer only to the future
`trading/production` running target.

#### Scenario: Research stable is not live production

- GIVEN a public doc that describes the current research environment
  (long-running, no real capital) or the retired legacy stable environment
- WHEN it names that environment
- THEN it identifies it as research/stable or as the retired legacy stable
  environment with its historical name labeled as a technical/historical name
- AND it does not claim real-money or live-trading capability for it

#### Scenario: Technical names are labeled as such

- GIVEN a public doc that mentions the GitHub Environments `development`,
  `production`, or `research`, the image channel tags `prod`, `latest`, or
  `develop`, the retired `production` deployment input, or `APP_ENV=PRODUCTION`
- WHEN the reader interprets the name
- THEN the doc states or clearly implies that the name is a technical or
  historical identifier
- AND the doc does not present the name as evidence of live trading capability

#### Scenario: Historical docs carry a superseded notice instead of a rewrite

- GIVEN a historical or archived doc whose environment vocabulary predates the
  two-axis model (for example `docs/operations/roadmap-2026-08.md` or the
  timeline section of `docs/operations/mongodb-resilience.md`)
- WHEN a reader opens it from an active-doc entry point
- THEN the easily misread location carries a dated historical/superseded notice
  pointing to the authoritative environment model
- AND the historical content itself is preserved without rewriting facts

### Requirement: Docs distinguish migration-period online sync from the target snapshot-import boundary for dev

Public docs describing how the dev environment obtains data SHALL state the
target boundary — dev data is disposable, sampled, or imported from controlled
snapshots, and dev must not directly depend on research or trading online
databases — as the target state, and SHALL label the current live direct sync
from the legacy stable environment (the `caifubao-datahub-data-sync` /
`./scripts/caifubao data sync` path) as a migration-period legacy pending
TASK-404, not as the target architecture. Public docs SHALL NOT present the
online direct connection as the target data flow.

#### Scenario: Doc describes the dev data source

- GIVEN a public doc that explains where dev data comes from
- WHEN it describes the current sync path
- THEN the sync source is identified as the legacy stable environment (a
  retired-production-named, migration-period data owner) and the path is
  labeled as migration-period legacy pending TASK-404
- AND the target flow is described as controlled snapshot/export followed by
  import/restore into dev

#### Scenario: Reader asks how dev gets real-scale data

- GIVEN a new reader following the authoritative environment model
- WHEN they look up how dev obtains real-scale test data
- THEN they find the controlled snapshot-import boundary and the current
  migration-period status of the online sync
- AND they do not find instructions to couple dev directly to research or
  trading online databases as a target design

### Requirement: Docs assign FQ/HFQ target ownership to research with the migration-period implementation location named

Public docs describing FQ/HFQ adjusted-price data SHALL state that external
`adj_factor` is external raw market data owned by the data domain, that derived
FQ/HFQ data targets research ownership, and that research must be able to
recompute FQ/HFQ independently within its own data and credential boundary
without requiring a production (or any trading/legacy-stable) runtime to
precompute and sync results. Docs SHALL name the current implementation
location — the datahub module writing `stock_factor_daily` and the hfq fields
— as a migration-period implementation site whose existence does not change the
target ownership.

#### Scenario: Reader asks where FQ/HFQ can be recomputed independently

- GIVEN a new reader following the authoritative environment model
- WHEN they ask where FQ/HFQ can be independently recomputed
- THEN the docs answer that research can recompute it from data-published
  quotes and the external `adj_factor` within research's own boundary
- AND the docs do not describe a design where prod must compute FQ/HFQ first
  and sync it to research or dev

### Requirement: Docs disclose that trading/production and paper trading are not enabled

Public docs SHALL state that the `prod` running target (`trading/production`)
is not enabled and does not exist, that `trading-paper` (`trading/paper`) is
currently only a default-deny manifest skeleton without workloads, and that the
system therefore has no real broker execution capability (no broker adapter,
order idempotency, fill/cash/position reconciliation, or kill-switch). The
docs SHALL reserve live-broker credentials for `trading/production` after all
execution gates pass; `trading/paper` may use only sandbox/paper credentials
and SHALL NOT receive live-broker credentials. The
disclosure SHALL be a factual statement, not a promotion of upcoming live
trading, and public docs SHALL reference
`openspec/changes/production-capability-roadmap/` as the normative source for
promotion gates and execution preconditions instead of restating them as new
normative text. External positioning remains verbatim "research / learning /
demonstration MVP, not investment advice".

#### Scenario: Doc implies live trading exists

- GIVEN any active public doc that mentions trading capability
- WHEN a reader checks what is enabled today
- THEN the doc states that no real execution capability exists and that `prod`
  is a target state only
- AND the doc does not present the GitHub `production` Environment, `prod`
  image tags, or the legacy stable namespace as evidence of live capability

#### Scenario: Paper trading cannot receive live credentials

- GIVEN the future `trading-paper` running target
- WHEN its credential boundary is documented
- THEN only sandbox/paper credentials are allowed
- AND live-broker credentials remain restricted to a separately authorized
  `trading/production` target after all execution gates pass

#### Scenario: Reader asks where the live-trading gate is defined

- GIVEN a reader of the authoritative environment model
- WHEN they look for promotion and live-execution preconditions
- THEN they are pointed to `openspec/changes/production-capability-roadmap/`
  as the normative gate
- AND the environment docs summarize without creating a second divergent
  normative source
