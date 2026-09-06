# Caifubao Agent Operations Guide

This is the canonical operations guide for AI agents (OpenClaw, Claude, etc.)
interacting with the Caifubao dev environment. It describes how to operate the
platform via the unified CLI and Makefile.

## Quick Reference

```bash
# Always start here — check system health
./scripts/caifubao system health   # or: make system-health

# Check if a stock has complete data
./scripts/caifubao data status sz000977

# Score a single stock
./scripts/caifubao score score-one sz000977

# Sync latest data from prod and score all stocks
./scripts/caifubao data sync 2026-05-18
./scripts/caifubao score score-all 2026-05-18
./scripts/caifubao data refresh-status

# Check backup and bootstrap readiness
./scripts/caifubao system backup status
./scripts/caifubao system bootstrap-check
```

## Environment

The CLI connects to the **K3s development cluster** by default.

| Variable | Default | Description |
|:---|:---|:---|
| `KUBECONFIG` | `~/.kube/config` if present, else `/etc/rancher/k3s/k3s.yaml` | Path to k3s kubeconfig (the `/etc/rancher/k3s` path only exists on the K3s server host) |
| `CFB_NAMESPACE` | `caifubao-dev` | K8s namespace |

The CLI executes commands inside the `caifubao-datahub` pod via `kubectl exec`.
No local Python dependencies are required.

## Command Reference

### Data Pipeline

#### `data sync [FROM_DATE] [COLLECTIONS]`
Sync data from prod MongoDB to dev. This is the **first step** after any change
that updates prod data (quote update, factor recompute, etc.).

```
make data-sync
./scripts/caifubao data sync
./scripts/caifubao data sync 2026-05-18 quote,factor,signal
./scripts/caifubao data sync --full quote,factor,signal
```

Collections: `quote` → `stock_daily_quote`, `factor` → `stock_factor_daily`,
`signal` → `stock_signal_daily`, `market` → `finance_market`,
`industry` → `stock_industry`.

Without `FROM_DATE`, date-based collections use the latest date already in dev
from `data_sync_state` as a completed watermark and replay the preceding three
calendar days before catching up to prod. The overlap makes retries idempotent
and includes late corrections. A collection only receives a completed
bootstrap marker after its entire sync finishes; a killed partial bootstrap
therefore cannot silently become an incremental watermark.

An empty or unmarked destination stays in full bootstrap mode. Run that first
bootstrap as a controlled one-time Job without the daily CronJob's three-hour
deadline, then verify the completion markers before enabling the schedule. Use
`--full` only for explicit reconciliation; it reads every source document and
can be expensive across a hybrid network. Low-frequency full reconciliation is
an operator action, not a scheduled daily job. Full runs use a separate job
family so an overlapping incremental runner cannot reap them as stale. Newest
business dates are processed first.

Date-based source and destination collections must have an index whose first
field is `date`. The runner fails before reading data when this precondition is
missing, rather than falling back to a multi-million-document collection scan
and in-memory sort. Build large indexes one at a time during a maintenance
window and verify the query plan before running sync; do not combine an index
build with full-collection statistics on memory-constrained MongoDB nodes.

**Important**: This syncs data but does NOT update `data_asset_status`.
Run `data refresh-status` after syncing.

Daily stock jobs that include factors use one full-market Tushare
`adj_factor(trade_date)` snapshot per target trading day and join it locally to
that day's persisted quotes. Only the target-day FQ/HFQ fields are written.
Initial computation, multi-day gaps, `force`, and backfill retain the per-stock
historical factor path so incomplete history is not hidden by a latest-day-only
update. The standalone `factor_runner --factor fq --mode stale` uses the same
snapshot path; `--mode force` remains historical.

#### `data refresh-status [LIMIT]`
Refresh the `data_asset_status` freshness collection. Must run after any
data sync to update the data quality page.

```
make data-refresh-status
./scripts/caifubao data refresh-status
```

#### `data status <STOCK>`
Check data completeness for a stock. Returns counts and latest dates for
all upstream collections (quote, factor, signal, scores).

```
make data-status STOCK=sz000977
./scripts/caifubao data status sz000977
```

Output example:
```json
{
  "stock": "sz000977",
  "name": "浪潮信息",
  "collections": {
    "stock_daily_quote": {"count": 6290, "latest": "2026-05-18"},
    "stock_factor_daily": {"count": 6281, "latest": "2026-05-18"},
    "stock_signal_daily": {"count": 18, "latest": "2026-05-18"}
  }
}
```

### Scoring

#### `score score-one <STOCK> [--date DATE] [--horizon 5|20|60] [--replace]`
Score a single stock for a specific date and horizon.
`--replace` is always applied (idempotent — safe to re-run).

```
make score-one STOCK=sz000977 DATE=2026-05-18 HORIZON=5
./scripts/caifubao score score-one sz000977 --date 2026-05-18 --horizon 5
```

#### `score score-all [DATE] [HORIZON]`
Score all active stocks. This is the main command to populate the market view.

```
make score-all DATE=2026-05-18
./scripts/caifubao score score-all 2026-05-18
```

**Note**: Scoring ~5,000 stocks takes about 6 minutes. The output includes
per-batch progress logs.

#### `score verify [FROM] [TO]`
Verify score predictions whose target date has passed. Transitions `PENDING`
→ `TRACKING` → `VERIFIED` as trading data becomes available.

```
make score-verify
./scripts/caifubao score verify
```

#### `score report [FROM] [TO]`
Generate a calibration report comparing predicted scores against actual
outcomes.

```
make score-report
./scripts/caifubao score report 2026-04-01 2026-05-18
```

### System

#### `system health`
Full health check: K3s connection, pod status, MongoDB (local + SRC),
CronJob state, and data volume summary.

```
make system-health
./scripts/caifubao system health
```

#### `system cron [status|trigger|suspend|resume] <name>`
Manage dev CronJobs.

```
./scripts/caifubao system cron status          # list all
./scripts/caifubao system cron trigger caifubao-datahub-data-sync
./scripts/caifubao system cron suspend caifubao-datahub-quote-stock
```

#### `system backup [status|trigger|logs] [name]`
Manage the public-safe MongoDB object-storage backup CronJob.

```
./scripts/caifubao system backup status
./scripts/caifubao system backup trigger
./scripts/caifubao system backup logs
```

The public manifest is suspended by default and uses placeholder object-storage
settings. Private overlays must provide real S3-compatible endpoint, bucket,
credentials, and retention policy before enabling the CronJob.

#### `system restore [status|logs|template] [object-key]`
Inspect restore jobs or render the public restore template with an object key.
The rendered template still needs private overlay review before it is applied.

```
./scripts/caifubao system restore status
./scripts/caifubao system restore logs
./scripts/caifubao system restore template mongodb/caifubao/20260525T010000Z.archive.gz
```

The restore template runs `mongorestore --drop`; never apply it to a live
database without an approved restore window.

#### `system bootstrap-check`
Check whether a regenerated MongoDB dataset has the minimum collections needed
for a demo-ready environment.

```
./scripts/caifubao system bootstrap-check
```

This command verifies required MongoDB collections from inside the datahub pod.
It exits non-zero when required collections are missing.

## Common Workflows

### Workflow 1: Score a stock after syncing latest data

```bash
# 1. Sync latest data from prod
./scripts/caifubao data sync $(date +%Y-%m-%d)

# 2. Check if the stock has data
./scripts/caifubao data status sz000977

# 3. Refresh freshness status
./scripts/caifubao data refresh-status

# 4. Score the stock
./scripts/caifubao score score-one sz000977 --date $(date +%Y-%m-%d)

# 5. Verify the result via API
curl -s http://api.dev.cfb.concorde102.cn/api/scores/sz000977/$(date +%Y-%m-%d)/explanation?horizon=5
```

### Workflow 2: Full market update after prod data refresh

```bash
# 1. Sync data
./scripts/caifubao data sync $(date +%Y-%m-%d)

# 2. Score all stocks (takes ~6 min)
./scripts/caifubao score score-all $(date +%Y-%m-%d)

# 3. Refresh freshness (takes ~2 min)
./scripts/caifubao data refresh-status

# 4. Verify market view
curl -s http://api.dev.cfb.concorde102.cn/api/market/comprehensive?date=$(date +%Y-%m-%d)
```

### Workflow 3: Initial dev environment setup

```bash
# 1. Check everything is running
./scripts/caifubao system health

# 2. Sync full data from prod
./scripts/caifubao data sync 2026-01-01

# 3. Refresh freshness
./scripts/caifubao data refresh-status

# 4. Score all stocks for latest trading day
./scripts/caifubao score score-all 2026-05-18
```

### Workflow 4: Post-reset bootstrap readiness

For a long quote rebuild, first follow the deterministic bootstrap gate in
[`mongodb-resilience.md`](./mongodb-resilience.md). Keep scheduled quote jobs
suspended and use a one-shot quote runner with one explicit `--as-of-date` for
the entire logical run. Do not resume a Job created from an older image.

```bash
# 1. Confirm core services and MongoDB are reachable
./scripts/caifubao system health

# 2. Regenerate or sync the required market data
./scripts/caifubao data sync 2026-01-01
./scripts/caifubao data refresh-status
./scripts/caifubao score score-all 2026-05-18

# 3. Confirm the regenerated database is demo-ready
./scripts/caifubao system bootstrap-check

# 4. Confirm object-storage backup wiring before adding non-regenerable data
./scripts/caifubao system backup status
./scripts/caifubao system backup trigger
./scripts/caifubao system backup logs
```

## Architecture Notes

### Module boundaries
- `datahub/` — produces and stores market data, factors, signals, scores
- `backend/` — exposes Flask APIs, auth, light aggregation
- `frontend/` — consumes backend APIs, renders UX
- Dev quote/factor/signal/scoring CronJobs — suspended by default; dev gets its
  market data from prod via the daily `data-sync` CronJob instead of pulling
  sources itself. Use the CLI for manual operations.
- Prod quote/signal/scoring CronJobs — enabled and run the daily routine (see
  below).
- MongoDB backup CronJob — public template is suspended by default; private
  overlays must provide real object-storage config before enabling it

### Daily routine schedules (Asia/Shanghai, weekdays)

| CronJob | Schedule | Purpose |
|:---|:---|:---|
| prod `caifubao-datahub-quote-stock` | `0 18 * * 1-5` | Pull latest quotes + factors only (`DATAHUB_STOCK_HISTORY_SOURCE=tushare`, `DATAHUB_STOCK_UNIVERSE_SOURCE=tushare`); UPD path writes settlement snapshots. Signals/scoring are NOT produced here — they run as the standalone jobs below, gated on this job's persisted data |
| prod `caifubao-datahub-signal` | `30 18 * * 1-5` | Compute MA-cross signals from fresh factors (incremental, stale-only by default) |
| prod `caifubao-datahub-scoring` | `35 18 * * 1-5` | Score latest trading day for all horizons (skips already-complete cohorts) |
| dev `caifubao-datahub-data-sync` | `15 19 * * 1-5` | Sync prod MongoDB → dev (quotes, factors, signals, market, industry; runs after prod signal/scoring so dev gets the same day's signals) |

The quote, signal, and scoring jobs run in dependency order (quote → signal →
scoring). Dev's data-sync runs **after prod's signal and scoring jobs** (19:15)
so it picks up the same day's rows, including signals. Note prod→dev sync does
**not** copy `stock_score_predictions`; dev scoring must be produced by running
`scoring_runner` manually.

Dependency gates are data-aware, not just status-aware: a signal run proceeds
when today's quote job has a SUCCESS record **or** its record (RUNNING or
FAILED — the run may have been killed by `activeDeadlineSeconds` after writing
data) shows both the `check_stock_data_integrity` phase (`validated_count > 0`)
and the `update_ma_factor` phase (`written_count > 0`) completed. Similarly,
scoring proceeds when today's signal run has a SUCCESS record or a record with
`written_total > 0` (preserved on partial failures). This keeps the pipeline
from stalling when a job dies after persisting its data but before recording
completion.

### Data pipeline dependency chain
```
quote → FQ factor → MA factor → signal → scoring → verification
  │        │           │          │         │
  │        │           │          │         └── writes stock_score_predictions
  │        │           │          └── reads factor + quote → writes stock_signal_daily
  │        │           └── reads quote → writes stock_factor_daily
  │        └── reads quote → adds hfq fields to stock_daily_quote
  └── tushare/akshare/baostock → writes stock_daily_quote
```

Stock history defaults to AkShare over HTTPS, but the k8s base deployment and
the daily quote-stock CronJob pin
`DATAHUB_STOCK_HISTORY_SOURCE=tushare` +
`DATAHUB_STOCK_UNIVERSE_SOURCE=tushare` (Tushare `pro.daily`, requires the
private `TUSHARE_TOKEN` secret; history is fetched in year windows and every
call is paced to stay under the 300/min rate limit). Keep the tushare source
pinned for dev deployments and manually triggered full-market runs too:
akshare/eastmoney history endpoints drop connections under sustained polling
and previously stalled dev catchup runs for 30+ minutes before failing.
`DATAHUB_STOCK_HISTORY_SOURCE=baostock` remains available only where outbound
TCP access to `www.baostock.com:10030` is known to work.
`DATAHUB_STOCK_UNIVERSE_SOURCE=tushare` sources the stock universe/list from
tushare (`pro.stock_basic` + the frozen-date daily snapshot) instead of the
eastmoney/sina spot list.

A circuit breaker protects full-market runs: once 25 consecutive history
pulls fail (not attributable to suspension), the quote phase aborts early
with "history source appears unavailable" instead of grinding through the
whole universe before the final validation failure. Re-run against a healthy
source; the run stays fail-closed either way.

Daily incremental updates use a snapshot-driven path: when a stock's latest
quote is exactly one trading day behind, the runner is not suspended, the
target is a stock, and the universe source is tushare, the settlement snapshot
(`pro.daily` for the as-of date) is written directly instead of replaying full
history, and quote freshness is refreshed in one batch (single aggregate +
bulk upsert) after the snapshot write. INC/FULL refreshes, suspended stocks,
and spot-sourced universes still replay history. A stock refresh that
attempts updates but writes zero quote rows fails before factor and scoring
phases.

### Data sync (prod → dev)
The `data sync` command uses the `MONGODB_SRC_*` environment variables
configured in the datahub pod. It reads from prod MongoDB and upserts into
dev MongoDB. Syncable collections: `stock_daily_quote`, `stock_factor_daily`,
`stock_signal_daily`, `finance_market`, `stock_industry`.
The scheduled path is incremental by destination watermark with a three-day
overlap; snapshot collections without a date field remain full-sync because
they are small. `--from-date`/`--to-date` override automatic watermarking, and
`--full` disables it.

`data_sync_state` stores collection-level bootstrap completion and the latest
fully synchronized source watermark. Do not seed it from a partially restored
database: first verify the restore baseline or complete a controlled full sync.

### Key collections in dev MongoDB
| Collection | Records | Source |
|:---|:---|:---|
| `stock_daily_quote` | ~18.5M | prod sync |
| `stock_factor_daily` | ~16.5M | prod sync |
| `stock_signal_daily` | ~50K | prod sync |
| `stock_score_predictions` | varies | local compute |
| `data_asset_status` | ~38K | local compute |
| `data_sync_state` | one row per dated sync collection | local sync control |
| `basic_stock` | ~6.4K | prod sync |

### Secrets lifecycle
The `MONGODB_SRC_PASSWORD` follows this path from source to container:

```
GitHub Secret                     deploy workflow
(MONGODB_SRC_PASSWORD) ───→ write-actions-env.sh ───→ env/root/.env
                                                        │
                                            prepare-worktree.sh
                                                        │
                                         生成 .env.datahub-secret
                                           MONGODB_SRC_USER=xxx
                                           MONGODB_SRC_PASS=<real>
                                                        │
                                            kustomize secretGenerator
                                            envs: .env.datahub-secret
                                                        │
                                          datahub-secret-<hash>
                                          含真实密码 → pod env
```

The kustomize `secretGenerator` creates a **hashed** secret name (e.g.
`datahub-secret-cf49cb4g2c`). The Deployment references this hashed name.
A plain `datahub-secret` (created by the preflight step) also exists but
may not be the one actually mounted by the pod.

### Secrets lifecycle (manual fix)
If sync fails with authentication error:

```bash
# 1. Check what the pod actually sees
./scripts/caifubao system pod | xargs -I {} kubectl -n caifubao-dev exec {} -- env | grep MONGODB_SRC

# 2. Find the hashed secret name the deployment references
kubectl -n caifubao-dev get deploy caifubao-datahub -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="MONGODB_SRC_PASS")].valueFrom.secretKeyRef.name}'

# 3. Patch the hashed secret with correct credentials
HASHED=$(kubectl -n caifubao-dev get deploy caifubao-datahub -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="MONGODB_SRC_PASS")].valueFrom.secretKeyRef.name}')
kubectl -n caifubao-dev patch secret "$HASHED" \
  --type=json -p='[{"op":"replace","path":"/data/MONGODB_SRC_PASS","value":"'$(echo -n "<correct_password>" | base64)'"}]'
kubectl -n caifubao-dev rollout restart deploy caifubao-datahub
```

### Data quality page shows STALE
The `data_asset_status` collection needs refreshing after data sync:
```bash
./scripts/caifubao data refresh-status
```

### Scoring returns 0 stocks
This means no upstream data exists for the requested date. Check:
```bash
./scripts/caifubao data status sh600519  # check a known stock
```
If the latest dates are behind, run `data sync` first.
