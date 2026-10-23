## MODIFIED Requirements

### Requirement: Dev data import uses a closed allow-list

The dev snapshot import path SHALL accept exactly the collections that today's
online sync surface covers — `stock_daily_quote`, `stock_daily_basic`,
`stock_factor_daily`, `stock_signal_daily`, `finance_market`,
`stock_industry`, `exchange_traded_fund`, `etf_daily_quote` — plus one
dev-side import-state metadata collection that replaces `data_sync_state`.
Any collection not on this allow-list SHALL be rejected by the importer before
any data is applied, and the rejection SHALL be visible in the import run
record. Research-produced snapshots SHALL be the only permitted external
source for dev's daily data import once the import path is active.

#### Scenario: Manifest proposes a collection outside the allow-list

- GIVEN a snapshot manifest that lists a collection not on the allow-list
- WHEN the dev import job starts
- THEN the import aborts before applying any data
- AND the run record names the rejected collection

#### Scenario: Snapshot source is the research data plane

- GIVEN a snapshot produced by the research/data datahub deployment
- WHEN dev imports it
- THEN the import succeeds without dev holding a direct connection credential
  to any research or legacy-stable online database

#### Scenario: ETF collections keep their declared class semantics

- GIVEN a snapshot that contains `exchange_traded_fund` and `etf_daily_quote`
- WHEN the importer applies it
- THEN `exchange_traded_fund` is imported with snapshot-class
  replace/verify semantics like the other small master collections
- AND `etf_daily_quote` is imported with date-partitioned upsert semantics
  keyed by `(code, date)`
- AND the manifest reports both collections with their class, checksum and
  document count like every other allow-listed collection
