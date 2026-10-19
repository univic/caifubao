## MODIFIED Requirements

### Requirement: Watermark-Based Incremental Prod-to-Dev Sync

The online direct sync engine SHALL remain the migration-period legacy path
for dev as defined by the environment-model vocabulary: for each
date-partitioned collection it SHALL copy only documents whose date is on or
after the recorded per-collection watermark minus the configured replay
overlap (default three calendar days, replayed idempotently), SHALL advance
each collection's watermark after that collection's sync succeeds, and SHALL
upsert date-partitioned documents idempotently by the collection's configured
business key (`SYNC_UPSERT_KEYS`). Snapshot-class collections without a
business key (currently `finance_market`) SHALL retain the legacy engine's
`_id`-based upsert. The engine SHALL NOT be extended to new collections; dev's
target acquisition path is the snapshot import defined in this change.

#### Scenario: Default run is incremental

- **GIVEN** a collection with a recorded watermark date W from a previous
  successful sync
- **WHEN** the data sync runner executes without an explicit date range
- **THEN** it SHALL read only source documents with `date >= W` minus the
  configured replay overlap (default three calendar days)
- **AND** SHALL upsert them idempotently by the collection's business key from
  `SYNC_UPSERT_KEYS`
- **AND** SHALL advance that collection's watermark to the maximum synced date
  on success

#### Scenario: Upsert keys follow the configured business key

- **GIVEN** a collection whose `SYNC_UPSERT_KEYS` entry is `stock_code,date`
- **WHEN** the runner upserts a batch of documents
- **THEN** the replace/upsert filter SHALL be the business-key tuple, so a
  re-run of the same trading day updates the same documents instead of
  duplicating them regardless of `_id`
- **AND** date-partitioned upserts SHALL NOT rely on carrying `_id` values
  between environments

#### Scenario: Dev-only signal rule and snapshot-class upserts are unchanged

- **GIVEN** the legacy engine's collection surface
- **WHEN** a sync runs
- **THEN** `stock_signal_daily` SHALL be skipped on the dev side (dev-only
  collection, `dev_only` marker) while `finance_market` and `stock_industry`
  continue full sync per their date-field configuration
- **AND** `finance_market` upserts SHALL keep the legacy `_id`-based filter
  (no business key), superseded for that collection only by the snapshot
  import contract

#### Scenario: Full sync requires explicit opt-in

- **GIVEN** any collection state
- **WHEN** a full-collection sync is requested without an explicit date range
  and without the allow-full-sync flag
- **THEN** the runner SHALL refuse the run with a clear error
- **AND** SHALL NOT read or write the collection

#### Scenario: Dry run does not advance the watermark

- **GIVEN** a dry-run sync request
- **WHEN** the runner completes reading and (skipped) writing
- **THEN** the recorded watermark SHALL remain unchanged

#### Scenario: Cold start falls back to full sync

- **GIVEN** a collection with no recorded watermark
- **WHEN** the runner executes
- **THEN** it SHALL treat the run as a full sync, following the repository's
  documented controlled-bootstrap runbook (the original requirement's
  mongodump/mongorestore cold-start note is superseded by this change's
  snapshot import path for dev)

#### Scenario: Legacy path is frozen, not extended

- **GIVEN** the dev-snapshot-import change has landed
- **WHEN** a new collection or new environment sync surface is proposed
- **THEN** it SHALL be served by the snapshot import contract rather than by
  adding collections to the online sync engine
