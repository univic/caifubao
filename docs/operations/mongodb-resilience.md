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

This is not a replacement for a durable storage backend. Long-lived clusters
should still use explicit MongoDB storage planning: StatefulSet or equivalent
identity, PVC/reclaim policy, node placement assumptions, and a backup
dependency.

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
