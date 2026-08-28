# MongoDB Backup, Restore, and Bootstrap

Caifubao's public Kubernetes examples now include a minimal object-storage
backup boundary for MongoDB. The public repository defines only the shape of the
jobs and placeholder values. Real bucket names, endpoints, access keys,
retention rules, private domains, and operator runbooks belong in
`caifubao-private`.

Object storage also hosts the research data lake. MongoDB archives under the
backup prefix are for restore; Parquet files under the data-lake prefix are for
research, backtests, and future autoresearch inputs. See
`docs/operations/data-lake.md`.

## Scope

The resilience path has three layers:

1. Scheduled logical backups with `mongodump`.
2. One-shot restore from an approved object-storage artifact.
3. Empty-database bootstrap validation when no backup can be restored.

The data-lake path is deliberately separate: it can help regenerate and compare
research datasets, but it is not a substitute for `mongorestore` when
non-regenerable MongoDB data must be recovered.

The public MongoDB workload uses a single-replica StatefulSet with an external
PVC. The StatefulSet provides stable Pod identity and controller semantics.
Private node selectors and PV node affinity prevent storage-node drift; private
overlays must still choose the storage class, reclaim policy, placement, and
backup dependency. None of these make a single local volume highly available.

When replicated or snapshot-capable storage is unavailable, a static local PV
is an acceptable interim boundary only when it uses `Retain`, an explicit host
path and node affinity, separate development and production volumes, and an
enabled COS logical backup. Before applying such an overlay, create the host
directories on the selected node and restrict access to the MongoDB container
runtime. Deleting a StatefulSet or retaining a PV is not a backup: node or disk
loss still requires restore from object storage.

Changing `Deployment/mongodb` to `StatefulSet/mongodb` is not an in-place
Kubernetes conversion. Existing clusters must first create a verified backup,
scale the old Deployment to zero, wait for its Pod to exit, and inspect the
PVC/PV before deleting the old controller. A PVC storage class is immutable;
move existing data through an approved backup/restore flow instead of patching
the old PVC. Never allow the old Deployment and new StatefulSet to run against
the same MongoDB volume.

## Backup Template

The public template is defined in:

- `k8s/base/mongodb-backup.yaml`

It creates:

- `mongodb-backup-config` with placeholder object-storage settings.
- `mongodb-backup-secret` with placeholder S3-compatible credentials.
- `mongodb-backup-scripts` with `backup.sh`, `restore.sh`, and
  `sanity-check.sh`.
- AWS CLI config mounted at `/root/.aws/config` with
  `s3.addressing_style = virtual`.
- `mongodb-s3-backup`, a suspended CronJob.

The backup container image is intentionally generic:

```text
caifubao-mongodb-tools:latest
```

The public build recipe is:

- `tools/mongodb-tools/Dockerfile`

Private deployment overlays should publish and replace this with an image that
contains:

- `mongodump`
- `mongorestore`
- `mongosh`
- `aws` CLI compatible with the target S3 endpoint

The backup job prints a sanitized JSON status line with:

- `started_at`
- `finished_at`
- `status`
- `database`
- `namespace`
- `object_key`
- `error_summary`

Do not log MongoDB passwords, object-storage secrets, or signed URLs.

## Tencent Cloud COS Notes

Tencent Cloud COS is compatible with S3-style clients when a custom endpoint is
configured. Private overlays should use values like:

```yaml
AWS_ENDPOINT_URL: "https://cos.ap-guangzhou.myqcloud.com"
AWS_DEFAULT_REGION: "ap-guangzhou"
S3_BUCKET: "caifubao-backups-1250000000"
S3_PREFIX: "mongodb"
```

`AWS_ACCESS_KEY_ID` maps to Tencent Cloud `SecretId`, and
`AWS_SECRET_ACCESS_KEY` maps to `SecretKey`.

COS buckets include the APPID suffix, such as `examplebucket-1250000000`.
The backup and restore jobs force AWS CLI virtual-hosted addressing because new
COS buckets require virtual-hosted-style access.

## Restore Template

The restore job is intentionally not included in `k8s/base/kustomization.yaml`,
because it is destructive: it runs `mongorestore --drop`.

Use this file as a copy-and-edit template:

- `k8s/base/mongodb-restore-job.example.yaml`

Before running it, a private overlay or operator copy must set:

- `MONGODB_RESTORE_OBJECT_KEY`
- real object-storage config and credentials
- the expected namespace
- the expected MongoDB target

The restore job runs:

```text
/scripts/restore.sh && /scripts/sanity-check.sh
```

The sanity check fails if required collections are missing.

## CLI Operations

The unified CLI includes public-safe operator entry points:

```bash
./scripts/caifubao system backup status
./scripts/caifubao system backup trigger
./scripts/caifubao system backup logs
./scripts/caifubao system restore template mongodb/caifubao/20260525T010000Z.archive.gz
./scripts/caifubao system restore status
./scripts/caifubao system restore logs
./scripts/caifubao system bootstrap-check
```

Use `CFB_NAMESPACE` and `KUBECONFIG` to target the intended cluster.

The backup commands operate on the `mongodb-s3-backup` CronJob. The bootstrap
check runs from the datahub pod and verifies required MongoDB collections.
The restore template command only renders YAML; review it in a private overlay
before applying it because restore uses `mongorestore --drop`.

## Empty-Database Bootstrap Order

When no backup exists, regenerate demo-ready data in this order:

1. Create MongoDB and service secrets.
2. Deploy backend, datahub, frontend, and compute worker.
3. Initialize stock master data.
4. Load or regenerate historical quotes.
5. Generate FQ, MA, and technical factors.
6. Generate signals.
7. Generate score predictions.
8. Refresh `data_asset_status` and freshness metadata.
9. Run `./scripts/caifubao system bootstrap-check`.
10. Run backend health, data quality, and OpenClaw read checks.

Regenerated market data is acceptable for demos, but it is not the same as a
full disaster recovery restore.

### Deterministic quote bootstrap gate

Before starting a long quote bootstrap, verify all of the following:

1. The deployed Datahub image was built from the intended immutable commit.
2. Datahub CI tested the assembled image tree after overlaying shared backend
   models and utilities.
3. Quote CronJobs remain suspended while the one-shot bootstrap runs.
4. The one-shot Job uses `backoffLimit: 0` and `restartPolicy: Never`.
5. The operator has selected one completed market trading date and records it
   as the logical run's `as_of_date`.

Invoke the quote runner with the same explicit date for every continuation of
the logical run:

```text
python -m app.jobs.quote_runner \
  --target stock \
  --as-of-date YYYY-MM-DD
```

Do not unsuspend or reuse a bootstrap created from an older image. Create a new
Job from the verified image instead. If the Job is interrupted, replay the new
Job with the original `--as-of-date`; quote persistence is idempotent by
`(code, date)`.

Treat source failures and `NO_DATA` as fatal. A listed stock may remain stale
without failing the market-wide run only when the history source succeeds and
the missing target-date row is attributable to temporary suspension. After the
quote phase, verify persisted freshness against the frozen date before enabling
or manually running factor, signal, scoring, export, or backup stages.

#### Post-bootstrap re-enable (current cluster state)

The 2026-08 production bootstrap passed the quote gate (full market via
tushare, frozen `as_of_date`, snapshot-driven daily updates) and the daily
routine is now enabled in dependency order:

- prod quote-stock (18:00, tushare history + universe) → signal (18:30) →
  scoring (18:35), weekdays Asia/Shanghai;
- dev `data-sync` (19:15) syncs prod → dev instead of pulling sources itself
  (after prod signal/scoring so dev gets the same day's signals; scoring is
  not synced — run scoring_runner manually in dev).

Factor/signal/scoring one-shot stages are run as separate Jobs with `--mode
stale` after the quote gate, matching the sequence in
`docs/operations/agent-cli.md#daily-routine-schedules-asia-shanghai-weekdays`.

### Image assembly validation

The Datahub publish workflow overlays shared modules from `backend/app/model`
and `backend/app/utilities` into the Datahub build context. CI must perform the
same overlay before Datahub tests. A source-tree-only test result is not enough
to approve a Datahub image when shared modules changed.

## Data Survivability Classes

Regenerable or mostly regenerable:

- stock master data
- historical quotes
- FQ factors
- MA and technical factors
- signals
- score predictions
- calibration and freshness summaries

Non-regenerable unless backed up:

- users
- portfolios
- watchlists
- decision journal entries
- service tokens
- audit logs
- task history
- manually curated experiment notes

Before creating meaningful non-regenerable data, enable scheduled object-storage
backups and confirm that at least one restore drill succeeds.

## Autoresearch Readiness

Autoresearch experiments should start only after one of these is true:

- a restore from object storage has passed sanity checks, or
- empty-database bootstrap has reached demo-ready state and the missing
  non-regenerable data is explicitly accepted.

Before that point, autoresearch work should be limited to docs, profile
scaffolding, adapters, and synthetic metric extraction tests.
