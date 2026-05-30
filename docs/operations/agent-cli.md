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
| `KUBECONFIG` | `/etc/rancher/k3s/k3s.yaml` | Path to k3s kubeconfig |
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
```

Collections: `quote` → `stock_daily_quote`, `factor` → `stock_factor_daily`,
`signal` → `stock_signal_daily`, `market` → `finance_market`,
`industry` → `stock_industry`.

**Important**: This syncs data but does NOT update `data_asset_status`.
Run `data refresh-status` after syncing.

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
- Dev CronJobs — suspended by default; use CLI for manual operations
- MongoDB backup CronJob — public template is suspended by default; private
  overlays must provide real object-storage config before enabling it

### Data pipeline dependency chain
```
quote → FQ factor → MA factor → signal → scoring → verification
  │        │           │          │         │
  │        │           │          │         └── writes stock_score_predictions
  │        │           │          └── reads factor + quote → writes stock_signal_daily
  │        │           └── reads quote → writes stock_factor_daily
  │        └── reads quote → adds hfq fields to stock_daily_quote
  └── baostock/akshare → writes stock_daily_quote
```

### Data sync (prod → dev)
The `data sync` command uses the `MONGODB_SRC_*` environment variables
configured in the datahub pod. It reads from prod MongoDB and upserts into
dev MongoDB. Syncable collections: `stock_daily_quote`, `stock_factor_daily`,
`stock_signal_daily`, `finance_market`, `stock_industry`.

### Key collections in dev MongoDB
| Collection | Records | Source |
|:---|:---|:---|
| `stock_daily_quote` | ~18.5M | prod sync |
| `stock_factor_daily` | ~16.5M | prod sync |
| `stock_signal_daily` | ~50K | prod sync |
| `stock_score_predictions` | varies | local compute |
| `data_asset_status` | ~38K | local compute |
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
