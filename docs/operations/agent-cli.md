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

# Paper strategy + forward-evidence window (paper-only; no orders)
# Full operator runbook: docs/operations/strategy-forward-window.md
./scripts/caifubao strategy forward progress --model-version flip_wide_shadow_v1 --horizon 20
```

## Environment

The CLI connects to the **K3s development cluster** by default.

| Variable | Default | Description |
|:---|:---|:---|
| `KUBECONFIG` | `~/.kube/config` if present, else `/etc/rancher/k3s/k3s.yaml` | Path to k3s kubeconfig (the `/etc/rancher/k3s` path only exists on the K3s server host) |
| `CFB_NAMESPACE` | `caifubao-dev` | K8s namespace |
| `CFB_API_BASE` | *(unset)* | Base URL of the API used by the `curl` verification examples below. Real dev/prod hosts live in the private deployment repo or your local env — this repository only ships placeholders. |

The CLI executes commands inside the `caifubao-datahub` pod via `kubectl exec`.
No local Python dependencies are required.

```bash
# Example: point the verification snippets at your own environment
export CFB_API_BASE="https://<your-api-host>"
```

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

#### `score equivalence-check DATE [--horizons 5,20,60] [--mode raw|ranked] [--apply]`

C1 verification (perf tasks 3.5/5.3). Runs the **same** dev trading day through
the legacy per-stock scoring path and the batched per-day path, then diffs every
persisted field (`score`, `rank`, `percentile`, `recommendation`,
`explanation`, `verification`, `input_snapshot`, `base_price`, `target_date`,
`status`, the `stock` reference, `model_version`) for the frozen active cohort.
It also reports the before/after wall time per horizon, which is the 5.3
measurement.

Read-only by default (it just reports the cohort size and the data available for
that date):

```
./scripts/caifubao score equivalence-check 2026-05-18 --horizons 5 --mode ranked
```

**`--apply` is required to run the comparison** and rewrites that date's
predictions with `replace=True` twice: per-stock first, batched last — so the
persisted state ends up as the batched (production) path produces it. The
command exits `1` when any field diverges, so wrap it in `set -e` / CI to fail
closed; roll back the batch path with `DATAHUB_SCORING_BATCH=0` if it does.

```
./scripts/caifubao score equivalence-check 2026-05-18 --apply \
  --mode ranked --report /tmp/c1-equivalence.json
```

**Which environment.** Run the primary check in **dev** (the CLI's default
namespace `caifubao-dev`):

- dev is prod-synced at full-market scale, and its datahub pod (the one
  `_pod_exec` targets) has the same spec as the production scoring pod
  (`1Gi`/`500m`), so the before/after numbers are comparable and the 1 GiB
  window-frame bound is actually exercised;
- `stock_signal_daily` can only be synced into dev-like environments
  (`dev_only` gate), and dev schedules no scoring/signal CronJob — so sync the
  window first, e.g.
  `./scripts/caifubao data sync 2025-09-01 quote,factor,signal,market,industry`
  (must cover the h60 lookback plus the signal-decay window);
- `industry_daily_metrics` is produced by scoring, not synced: run one scoring
  pass for an earlier trading day (or the check itself for an earlier day) if you
  want the "industry metrics present" branch exercised rather than the neutral
  one.

Use **research** only as a supplement — it is production-shaped (full history,
same schedules) and therefore good for larger/multi-day coverage, and it already
has industry metrics for earlier dates. There, pass an isolated
`--model-version` (e.g. `score_v2_202605b-c1check`) so the check cannot rewrite
the version that research/autoresearch reads, and do not use research timings as
the production before/after (different cluster, and the research scoring Job is
`2Gi`/`1000m`).

Never pass `--apply` against **production**: it rewrites that day's predictions
with `replace=True`. For prod, collect the after numbers from
`datahub_job_runs` (`completed_at - started_at`; the collection has no
`elapsed_seconds` field) after deployment (perf task 5.3/5.4).

### Strategy (paper)

The paper strategy runner is invoked as a datahub module inside the datahub pod
(there is no `./scripts/caifubao` wrapper yet). All of its commands are
paper-only; the export below is read-only and never writes.

#### `strategy_runner export --date DATE [--format csv|json] [--output PATH]`

Export one COMPLETED paper run's target portfolio + rebalance list as a
human-checkable checklist: `BUY` (added to the target), `SELL` (removed), `HOLD`
(unchanged), with target weight, target amount, and the signal-date score.
Amounts come from one reported base NAV. Output is **research-grade — not
investment advice, not an order instruction**; the CSV artifact carries that
label and disclaimer in its leading comment line.

```bash
# CSV to stdout, metadata/summary to stderr
./scripts/caifubao system pod | xargs -I {} kubectl -n caifubao-dev exec {} -- \
  python -m app.jobs.strategy_runner export --date 2026-09-11 > target.csv

# Full JSON (metadata + rows)
./scripts/caifubao system pod | xargs -I {} kubectl -n caifubao-dev exec {} -- \
  python -m app.jobs.strategy_runner export --date 2026-09-11 --format json
```

Fails closed (exit 1) when there is no COMPLETED run for the date/model
version/horizon, when the matching run is `SKIPPED`/`FAILED`/`RUNNING`, or when
two COMPLETED runs under different `config_hash` values match — pass
`--config-json` to disambiguate. `SELL` rows intentionally carry no quantity:
the paper target is not your real account, so sizing sells belongs to
reconciliation (roadmap 1.1/2.3).

#### `strategy_runner run|nav|report|forward ...`

Daily paper run, NAV recompute, run inspection, and the certified forward
evidence window (`forward certify|close|progress`). See
`docs/autoresearch/runs/h20-excess-alpha/task-4.4-paper-run-120d.md`.

### Stock timing pool (research-only)

`timing-pool` aggregates an explicit frozen cohort of already-computed timing
and same-stock buy-and-hold result pairs. It never discovers current active
stocks, selects a winner, creates an order, or persists a backtest.

```bash
datahub/.venv/bin/python -m app.jobs.backtest_runner \
  timing-pool /path/to/frozen-cohort-pairs.json \
  --output /path/to/timing-pool-report.json
```

The input JSON must pin `cohort`, UTC `cohort_as_of`, `cohort_source`,
`model_version`, `config`, `window`, and `delisted_completeness` (`VERIFIED`,
`NOT_VERIFIED`, or `UNKNOWN`). `results` is keyed by stock code and each value
must contain separate `timing` and `buy_hold` objects produced under identical
cash, date, board-lot, friction, and next-open assumptions. Missing pairs remain
failed rows and reduce coverage. Both objects must repeat their `stock_code` and
the same `assumptions` object with `initial_cash`, `window`,
`execution_timing=next_trading_day_open`, `valuation_timing=last_close`, positive
`board_lot`, and a `friction` object containing `commission_rate`,
`minimum_commission`, `stamp_duty_rate`, and `slippage_rate`. Output always carries
`research_only=true` and `validation_status=UNVALIDATED`, even if the mechanical
evidence gates pass. Every `daily_values` observation must include an in-window
date and finite non-negative `equity`; only timing-side SELL records with
`status=FILLED`, positive quantity, positive execution price, and an in-window
trade date count toward the completed-trade gate.

### Stock timing replay adapter (P1, research-only)

`timing-replay` builds those same P0 result pairs from real stored quotes and
ranked predictions. It reads MongoDB and may write the selected JSON output
file, but it never generates scores or writes a backtest/model/cohort record.

```bash
PYTHONPATH=datahub datahub/.venv/bin/python -m app.jobs.backtest_runner \
  timing-replay /path/to/timing-replay-p1-manifest.json \
  --output /path/to/timing-replay-report.json
```

The versioned manifest must contain:

- a frozen `point_in_time` cohort whose immutable source URI/SHA-256, member
  count, subsequently-delisted count, and suspended count reconcile with its
  member rows;
- an ACTIVE registry-pinned `ranked` model version/config hash and one fixed
  horizon/entry percentile/exit percentile;
- the authoritative trading calendar, cash, board lot, and complete friction
  object; and
- one immutable prediction-cohort record per signal date, including artifact
  URI/SHA-256, cohort fingerprint, member count, and `data_as_of` no later than
  that session's close.

Every stored prediction must repeat exact `freshness=FRESH`, the daily cohort
fingerprint, artifact hash and cutoff in `input_snapshot`. Legacy ranked predictions that only contain
`scoring_mode` and `cohort_fingerprint` intentionally create no timing signal:
they do not prove which immutable historical universe produced the percentile
or that its inputs stopped at D. This fail-closed result is a data-provenance
gap, not permission to substitute the current active universe.

A complete, versioned shape is available at
[`docs/examples/timing-replay-p1-manifest.json`](../examples/timing-replay-p1-manifest.json).
Its hashes, counts, model version, cohort fingerprints, calendar, and members
are placeholders and must be replaced with the immutable evidence being
replayed; the CLI validates the completed file rather than generating it.

Buy-and-hold forms its intent before the window and fills at the first tradable
adjusted open. Timing observes D's valid percentile after D close and can first
fill at a later tradable adjusted open. Both arms use identical execution,
friction, board-lot, daily close valuation, and no artificial final-close
liquidation. Output remains `research_only=true` and
`validation_status=UNVALIDATED`.

### Stock timing PIT input capture (P2a, research-only)

P2a closes the provenance gap without relabelling historical Mongo rows. It is
forward-only and two-phase: capture the universe after the prior close but
before D opens, then capture D's ranked-scoring inputs after D closes but before
the next session opens. Both commands require the build-injected
`CAIFUBAO_BUILD_REVISION` and exclusively create a new JSON path. The durable
artifact store and the day-by-day operator sequence are in
[`stock-timing-pit-evidence.md`](./stock-timing-pit-evidence.md).

```bash
PYTHONPATH=datahub datahub/.venv/bin/python -m app.jobs.scoring_runner \
  capture-pit-universe --date 2026-09-15 \
  --output /artifacts/universe-2026-09-15.json

PYTHONPATH=datahub datahub/.venv/bin/python -m app.jobs.scoring_runner \
  capture-pit-inputs \
  --universe-artifact /artifacts/universe-2026-09-15.json \
  --model-version ranked-v1 --horizons 20 \
  --output /artifacts/ranked-inputs-2026-09-15.json
```

The input artifact freezes the exact pre-open universe plus bounded quotes,
factors, live/decayed signals, CSI300 history, pre-open industry classification,
pre-D industry metrics, calendar, model config and code revision. Source rows
and hashes are carried together under the shared immutable artifact contract;
numeric source values use declared fixed-unit integer encoding. These commands
never write `StockScorePrediction` or change scheduled scoring. P1 continues to
reject legacy predictions until the P2b consumer scores from and revalidates
this artifact.

### Stock timing PIT artifact consumer (P2b, research-only)

P2b is the scoring half of the forward evidence path. It fully validates the
P2a universe/input pair and reconstructs ranked scoring from the artifact rows
only. The calculation phase does not initialize MongoDB or read current
membership, quotes, factors, signals, industry data, calendar, or stock master.
Set `CAIFUBAO_BUILD_REVISION` to the immutable consumer image/source revision
before running; P2b rejects a missing revision instead of emitting untraceable
predictions.

```bash
export CAIFUBAO_BUILD_REVISION="$(git rev-parse HEAD)"

# Default: artifact-only calculation, no MongoDB access.
PYTHONPATH=datahub datahub/.venv/bin/python -m app.jobs.scoring_runner \
  score-pit-artifacts \
  --universe-artifact /artifacts/universe-2026-09-15.json \
  --input-artifact /artifacts/ranked-inputs-2026-09-15.json \
  --output /artifacts/ranked-predictions-2026-09-15.json

# Optional explicit insert. Use a new output path and a model/date/horizon with
# no existing prediction natural keys.
PYTHONPATH=datahub datahub/.venv/bin/python -m app.jobs.scoring_runner \
  score-pit-artifacts \
  --universe-artifact /artifacts/universe-2026-09-15.json \
  --input-artifact /artifacts/ranked-inputs-2026-09-15.json \
  --output /artifacts/ranked-predictions-2026-09-15-apply.json --apply
```

After the result file is serialized, stdout emits
`prediction_cohorts_by_horizon[H]`, the exact daily record to place under the
P1 manifest's `prediction_cohorts[D]` for that horizon. `artifact_sha256`
binds the exact result-file bytes, while the distinct
`prediction_root_sha256` binds the immutable generated fields of every cohort
member through each row's Merkle proof; sorted `member_codes` lets P1 reconcile
the daily fingerprint/count and proof index even when the replay pool is only a
selected subset. With `--apply`, P2b checks that the
captured model is still ACTIVE and ranked with the same config hash, rejects
the entire run if any natural key exists, then issues one insert-only bulk
write. It never upgrades old rows.

Computable rows are top-level `PENDING` and carry
`input_snapshot.status=RANKED`, `freshness=FRESH`, artifact/cohort hashes and D
close. Missing-quote members remain `BLOCKED` and unranked. `FRESH` is input
provenance, not future verification, profitability, or trading authorization.

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
curl -s "$CFB_API_BASE/api/scores/sz000977/$(date +%Y-%m-%d)/explanation?horizon=5"
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
curl -s "$CFB_API_BASE/api/market/comprehensive?date=$(date +%Y-%m-%d)"
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
