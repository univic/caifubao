# Research Data Lake

Caifubao now separates two object-storage responsibilities:

- MongoDB backup archives are for disaster recovery.
- Parquet exports are for research, backtests, and future autoresearch inputs.

The online application continues to read MongoDB through backend APIs. Parquet
files are not an online serving dependency.

## Layout

Use one S3-compatible bucket boundary with separate prefixes:

```text
mongodb/                         # mongodump archives for restore
data-lake/china-a/daily_quotes/  # partitioned Parquet quote data
data-lake/china-a/factors/       # partitioned Parquet factor data
data-lake/china-a/signals/       # partitioned Parquet signal data
research/artifacts/              # future experiment outputs
```

Each Parquet dataset is partitioned by `trade_date`:

```text
data-lake/china-a/daily_quotes/trade_date=2026-05-25/part-2026-05-25.parquet
```

## Export Runner

The datahub image contains a CLI runner:

```bash
python -m app.jobs.parquet_export_runner export --dataset all --lookback-days 7
```

Supported datasets:

- `daily_quotes` from `stock_daily_quote`
- `factors` from `stock_factor_daily`
- `signals` from `stock_signal_daily`
- `all`

Useful dry-run command:

```bash
python -m app.jobs.parquet_export_runner export \
  --dataset daily_quotes \
  --from-date 2026-05-01 \
  --to-date 2026-05-25 \
  --dry-run
```

The runner uses S3-compatible settings from environment variables:

```text
DATA_LAKE_ENDPOINT_URL
DATA_LAKE_REGION
DATA_LAKE_BUCKET
DATA_LAKE_PREFIX
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
```

It forces virtual-hosted-style S3 addressing in the boto3 client, which matches
new Tencent Cloud COS bucket requirements.

## Kubernetes

The public example manifest is:

- `k8s/base/data-lake-export.yaml`

It defines:

- `data-lake-export-config`
- `data-lake-export-secret`
- `caifubao-datahub-parquet-export`, a suspended CronJob

Private overlays own real COS endpoints, bucket names, access keys, schedules,
and whether the CronJob is unsuspended.

## Validation

After cluster initialization:

1. Keep the CronJob suspended.
2. Run a one-shot dry-run Job or exec into datahub:
   `python -m app.jobs.parquet_export_runner export --dataset all --lookback-days 7 --dry-run`
3. Trigger a one-shot export Job with a small lookback window.
4. Confirm COS contains `data-lake/china-a/.../trade_date=.../*.parquet`.
5. Read one file with DuckDB, pandas, or pyarrow before enabling the schedule.

Do not run autoresearch against live MongoDB when the same input can be read
from a Parquet snapshot. Snapshots make experiments comparable across runs and
reduce load on the online database.
