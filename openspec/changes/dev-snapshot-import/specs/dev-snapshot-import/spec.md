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
document count against the manifest. Verification SHALL happen before any dev
collection is mutated — document counts are obtained by streaming-counting the
archive contents, or by restoring into a staging database that is verified and
then discarded, never by counting inside dev's live collections. The previously
applied dev state SHALL remain intact and consistent after a failed import, and
the failure SHALL be observable in the import run record (health-watcher /
job-run semantics used by other datahub jobs).

#### Scenario: Checksum mismatch mid-import

- GIVEN a snapshot where one collection file's sha256 does not match the
  manifest
- WHEN the importer verifies before applying
- THEN no collection is modified in dev
- AND the failure is recorded as a failed import run

#### Scenario: Document count disagrees with the manifest

- GIVEN a snapshot where a pre-application verification pass (streaming count
  of the archive, or a restore into a staging database) yields fewer documents
  than the manifest declares for a collection
- WHEN the importer performs that verification
- THEN the import aborts before dev's live collections are touched
- AND the prior dev state remains authoritative

### Requirement: Import is idempotent per collection class

The importer SHALL import idempotently, with semantics that differ per
collection class. For date-partitioned collections (`stock_daily_quote`,
`stock_daily_basic`, `stock_factor_daily`, `stock_signal_daily`), it SHALL
upsert by the same business keys the online sync used, so re-importing the
same snapshot yields the same state. For snapshot-class collections
(`finance_market`, `stock_industry`) and the import-state metadata collection,
it SHALL use explicit replace/drop semantics and SHALL state which collections
are treated as snapshot-class. For `stock_industry` this deliberately
supersedes the online sync's upsert-by-`stock_code` behaviour: snapshot-class
idempotency means state-equivalent replacement, and dev-side rows absent from
the snapshot are deleted. Re-running a completed import SHALL NOT duplicate
documents; for date-partitioned collections it SHALL NOT change previously
imported business rows.

#### Scenario: The same snapshot is applied twice

- GIVEN a snapshot that has already been imported into dev
- WHEN the same manifest is imported again
- THEN document counts per collection are unchanged
- AND for date-partitioned collections, previously imported rows keep their
  business-key identity
- AND for snapshot-class collections, dev holds exactly the snapshot's state

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

The online direct sync path SHALL be transitioned to snapshot import in two
observable stages. Stage 1 (cutover) begins once the snapshot import path has
passed its acceptance observation window defined in this change's tasks
(verified freshness/count alignment on consecutive trading days): from then
on, the `data sync` CLI entry point (`./scripts/caifubao data sync` and its
Makefile alias) SHALL perform snapshot import without establishing any online
MongoDB connection to the research or legacy-stable environments, the dev
deployment SHALL no longer receive `MONGODB_SRC_*` credentials, and the online
sync CronJob SHALL be removed from dev's schedule. Stage 2 (removal) is a
follow-up change that deletes the online sync runner code and any remaining
`MONGODB_SRC_*` configuration; between the two stages the runner SHALL fail
closed — it SHALL refuse to run without source credentials rather than
silently falling back. Until Stage 1 completes, the online path and its
`MONGODB_SRC_*` configuration SHALL be documented as a migration-period legacy
consistent with the environment-model vocabulary, without redefining that
vocabulary in this change.

#### Scenario: Data sync runs after the cutover

- GIVEN the snapshot import path has passed its acceptance observation window
  and the cutover has happened
- WHEN an operator runs `./scripts/caifubao data sync`
- THEN the command imports from a versioned snapshot
- AND no component in dev holds `MONGODB_SRC_*` credentials or connects to the
  research or legacy-stable online database

#### Scenario: Legacy runner cannot silently reconnect

- GIVEN Stage 1 has completed and Stage 2 (runner removal) has not
- WHEN something invokes the still-present online sync runner in dev
- THEN it fails closed because `MONGODB_SRC_*` credentials are absent
- AND it does not connect to any research or legacy-stable database

#### Scenario: Docs stay honest during the transition

- GIVEN Stage 1 has not completed
- WHEN a reader consults the dev data-acquisition documentation
- THEN the online direct sync is labeled as a migration-period legacy with its
  replacement described
- AND no document presents the online direct connection as the target
  architecture
