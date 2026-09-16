# Dev Snapshot Import

## ADDED Requirements

### Requirement: Dev data import uses a closed allow-list

The dev snapshot import path SHALL accept exactly the collections that today's
online sync surface covers — `stock_daily_quote`, `stock_daily_basic`,
`stock_factor_daily`, `stock_signal_daily`, `finance_market`,
`stock_industry` — plus one dev-side import-state metadata collection that
replaces `data_sync_state`. Any collection not on this allow-list SHALL be
rejected by the importer before any data is applied, and the rejection SHALL
be visible in the import run record. Research-produced snapshots SHALL be the
only permitted external source for dev's daily data import once the import
path is active.

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

### Requirement: Snapshots are described by a versioned manifest with checksums

Every snapshot SHALL carry a versioned manifest that records, per collection:
the object-storage key of its data file, a sha256 checksum, a document count,
the `data_as_of` watermark covered by the snapshot, and the producer image
SHA. The manifest itself SHALL be checksummed. The dev importer SHALL verify
manifest integrity, per-collection checksums and document counts before
applying any data, and SHALL persist a traceability record (manifest id,
producer image SHA, `data_as_of`, applied-at) into the dev import-state
collection.

#### Scenario: Import is traceable to its producer

- GIVEN a dev environment that has just applied a snapshot
- WHEN an operator inspects the dev import-state collection
- THEN they can identify the manifest id, producer image SHA, per-collection
  `data_as_of` watermark and applied-at timestamp of that import

#### Scenario: Manifest itself is corrupted

- GIVEN a snapshot whose manifest checksum does not match its content
- WHEN the importer reads the manifest
- THEN the import aborts with no data applied

### Requirement: Import fails closed on any mismatch

The dev importer SHALL abort without partial application when any of the
following fails: manifest integrity, per-collection checksum, or per-collection
document count against the manifest. The previously applied dev state SHALL
remain intact and consistent after a failed import, and the failure SHALL be
observable in the import run record (health-watcher / job-run semantics used
by other datahub jobs).

#### Scenario: Checksum mismatch mid-import

- GIVEN a snapshot where one collection file's sha256 does not match the
  manifest
- WHEN the importer verifies before applying
- THEN no collection is modified in dev
- AND the failure is recorded as a failed import run

#### Scenario: Document count disagrees with the manifest

- GIVEN a snapshot whose archive restores fewer documents than the manifest
  declares for a collection
- WHEN the importer counts restored documents
- THEN the import aborts and the prior dev state remains authoritative

### Requirement: Import is idempotent per collection class

For date-partitioned collections (`stock_daily_quote`, `stock_daily_basic`,
`stock_factor_daily`, `stock_signal_daily`), the importer SHALL upsert by the
same business keys the online sync used, so re-importing the same snapshot
yields the same state. For snapshot-class collections (`finance_market`,
`stock_industry`) and the import-state metadata collection, the importer SHALL
use explicit replace/drop semantics and SHALL state which collections are
treated as snapshot-class. Re-running a completed import SHALL NOT duplicate
documents and SHALL NOT change previously imported business rows.

#### Scenario: The same snapshot is applied twice

- GIVEN a snapshot that has already been imported into dev
- WHEN the same manifest is imported again
- THEN document counts per collection are unchanged
- AND previously imported rows keep their business-key identity

### Requirement: Freshness metadata follows the snapshot

The importer SHALL propagate each collection's `data_as_of` watermark from
the manifest into dev's freshness/status metadata (the `data_asset_status`
semantics used by the data-quality page and health watcher), so downstream
consumers see the snapshot's actual coverage rather than assuming live
freshness. A stale snapshot SHALL be visible as stale in dev, never as
current.

#### Scenario: Dev imports a weekend snapshot on Monday morning

- GIVEN the latest available snapshot covers data through Friday
- WHEN dev imports it on Monday before the research chain produces Monday's
  data
- THEN dev's data-quality status shows `data_as_of` of Friday for the covered
  collections
- AND downstream consumers can detect the gap instead of trusting it as
  current

### Requirement: The online direct sync path is transitioned to snapshot import

Once the snapshot import path is active and verified in dev, the `data sync`
CLI entry point (`./scripts/caifubao data sync` and its Makefile alias) SHALL
perform snapshot import without establishing any online MongoDB connection to
the research or legacy-stable environments, the dev deployment SHALL no longer
receive `MONGODB_SRC_*` credentials, and the online sync runner and its
CronJob SHALL be deprecated and then removed. Until that transition completes,
the online path and its `MONGODB_SRC_*` configuration SHALL be documented as
a migration-period legacy consistent with the environment-model vocabulary,
without redefining that vocabulary in this change.

#### Scenario: Data sync runs after the transition

- GIVEN the snapshot import path has been verified in dev
- WHEN an operator runs `./scripts/caifubao data sync`
- THEN the command imports from a versioned snapshot
- AND no component in dev holds `MONGODB_SRC_*` credentials or connects to the
  research or legacy-stable online database

#### Scenario: Docs stay honest during the transition

- GIVEN the transition has not completed
- WHEN a reader consults the dev data-acquisition documentation
- THEN the online direct sync is labeled as a migration-period legacy with its
  replacement described
- AND no document presents the online direct connection as the target
  architecture
