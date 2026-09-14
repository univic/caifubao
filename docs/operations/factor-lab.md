# Factor Lab (research-only)

`datahub/app/lib/factor_lab/` freezes `stock_daily_quote` into one parquet
**panel** and evaluates single factors against it (IC/ICIR, net quantile spread,
turnover, walk-forward decay, hard gates). It is a research measurement layer:
no scoring/signal/strategy/API code reads it, and it never writes to Mongo.

The panel used to live in the datahub pod's `/tmp`, so a pod restart silently
threw away a multi-hour export. It now belongs on a **PersistentVolumeClaim**,
and export/evaluate run as ordinary batch Jobs.

## Panel lifecycle

```text
Mongo (stock_daily_quote + market trade calendar)
   │  factor_lab_runner export        (code-chunked, read-only)
   ▼
PVC: factor-lab-panel  /data/lab.parquet      (~0.4 GB for 2024-2026)
   │  factor_lab_runner evaluate      (per horizon, read-only)
   ▼
/data/sweep_h{1,5,20}.json                    (per-factor report)
```

The parquet column schema plus the label/blocked-reason semantics in
`datahub/app/lib/factor_lab/panel.py` are the stable seam: anything that reads
the panel later should depend on those, not on internals.

## Create the claim once

```bash
kubectl -n caifubao-research apply -f k8s/base/factor-lab-panel.example.yaml
```

`k8s/base/factor-lab-panel.example.yaml` is the claim on its own;
`k8s/base/factor-lab-export.example.yaml` and
`k8s/base/factor-lab-evaluate.example.yaml` are the matching Jobs (kept separate
so an `apply -f` cannot start evaluate before export finishes). None of them is
in `k8s/base/kustomization.yaml`: the lab is research-only and must not land in
every overlay. The real private overlay pins the storage class and node; edit the
examples before applying if the cluster default is not appropriate.

## Run export / evaluate

Apply the export Job, wait for it, then apply the evaluate Job. The claim is
ReadWriteOnce and the parquet footer only exists once the writer closes, so the
two must not overlap:

```bash
kubectl -n caifubao-research apply -f k8s/base/factor-lab-export.example.yaml
kubectl -n caifubao-research wait --for=condition=complete job/factor-lab-export --timeout=30m

kubectl -n caifubao-research apply -f k8s/base/factor-lab-evaluate.example.yaml
kubectl -n caifubao-research logs -f job/factor-lab-evaluate   # ~10-20 min for 2019-2026
```

Change `--horizons` in the evaluate Job for each group (1, then 5, then 20) and
`kubectl delete job factor-lab-evaluate` between runs.

If you deploy through the private overlay, prefer its one-off Job launcher: it
copies the live datahub Deployment's image, generated ConfigMap names and Secret
references, so the Job cannot drift from the deployed environment, and it mounts
the claim for you:

```bash
k8s/jobs-internal/run-datahub-job.sh --namespace caifubao-research \
  --type lab -- export \
  --from-date 2019-01-01 --to-date 2026-09-11 \
  --horizons 1,5,20,60 --output /data/lab.parquet

for h in 1 5 20; do
  k8s/jobs-internal/run-datahub-job.sh --namespace caifubao-research \
    --type lab -- evaluate \
    --panel /data/lab.parquet --all --horizons "$h" \
    --output "/data/sweep_h$h.json"
done
```

Copy a report out of the claim (or read it from a Job that mounts it). The
selector differs by path: the example Jobs are labelled `factor-lab-evaluate`,
the launcher's are `lab-runner`:

```bash
kubectl -n caifubao-research cp \
  "$(kubectl -n caifubao-research get pod -l job.caifubao.io/type=factor-lab-evaluate \
      -o jsonpath='{.items[0].metadata.name}')":/data/sweep_h20.json \
  ./sweep_h20.json
# launcher path instead:
#   -l job.caifubao.io/type=lab-runner
```

## Holding-period x buffer scan

`evaluate` answers "does this factor sort returns?". `holding-scan` answers the
portfolio question that follows: **at which holding period does the edge survive
round-trip friction, and what annual turnover does that cost?**

```bash
python -m app.jobs.factor_lab_runner holding-scan \
  --panel /data/lab.parquet --factor reversal_20 \
  --from-date 20190102 --to-date 20260911 \
  --horizons 5,10,20,40,60 --buffers 1.0,1.5,2.0 \
  --portfolio-size 800 --entry-pct 0.2 \
  --output /data/holding-scan.json
```

Per cell it reports the net-of-cost excess over the same-date eligible
equal-weight benchmark, the information ratio, annual turnover, drawdown and a
turnover-penalised objective (`summary` in the JSON, one line per line on
stdout). Semantics:

- the signal is the **lowest** ranks of the chosen factor (`rank <= entry_pct`),
  which is the reversal side — pass `--factor momentum_20` to scan the momentum
  side directly;
- `--from-date` / `--to-date` anchor the evaluation schedule; leading indicator
  history can remain in the panel without shifting rebalance dates;
- `--buffers` is the hysteresis: a held name is only sold once its percentile
  rises above `entry_pct * buffer`, so `1.0` is a full re-sort every rebalance;
- the holding period sets the rebalance cadence; a name inside the wider exit
  band carries into the next tranche, and friction is charged only to the
  **replaced fraction** of the target book;
- blocked labels (limit-up entry, suspension, …) are dropped from both the
  basket and the benchmark, exactly as in `evaluate`.

Caveat: the registered lab factors are simple price/volume expressions, not the
production scoring engine's eight-component `flip_wide` construction. Use this
command to find the holding-period/turnover trade-off of a *factor family*, then
re-run the chosen cell against the production signal before quoting a number as
the strategy's.

## ETF and small-book replay boundary

The `app.lib.etf_lab` and `app.lib.small_book` helpers are operator-only research
surfaces. They do not write MongoDB, promote a model, schedule themselves, or
emit executable orders. `etf_lab rotate --ledger` appends local JSONL records
labelled `evidence_kind=REPLAY`; these rows never count toward the immutable
120-session forward-evidence gate.

The ETF `pool` command builds membership from one explicitly supplied liquidity
snapshot. The `rotate` CLI does **not** reconstruct historical membership,
delisted instruments, or a survivorship-complete point-in-time cohort. A
historical run using today's pool is a current-snapshot replay and must not be
described as verified PIT. Likewise, ETF labels currently shift across each
instrument's observed quote rows rather than an exchange calendar; gaps are
guarded heuristically, not proven complete. A trustworthy replay requires a
separate frozen cohort/calendar adapter with auditable as-of provenance.

## Resources and caveats

- **ReadWriteOnce.** Export and evaluate must not mount the claim at the same
  time; run them sequentially (and never in parallel in two namespaces).
- **Memory.** Evaluate fits in 2 GiB only because `_load_panel` projects the
  columns it needs at read time (`pd.read_parquet(columns=...)`); an older
  image that reads every column first will be OOMKilled. The example jobs use a
  512 MiB request / 3 GiB limit; raise the limit if a full-history export needs
  it.
- **Run the export beside Mongo.** Export time is dominated by transferring
  quote documents, not by the query or CPU: a Job on another node crawls through
  ~750k documents per chunk. Keep the panel claim and the Jobs on the same node
  as the environment's MongoDB (in this workspace that is `ubuntu-5700x` for
  research).
- **Image contents.** `factor_lab_runner` ships with the `datahub/` source. A
  Job on an image built before that code existed fails with
  `ModuleNotFoundError: No module named 'app.lib.factor_lab'`; deploy a newer
  datahub image (or pass an explicit image override to the launcher).
- **Labels are calendar-checked trading-session offsets**, not row positions:
  a missing quote row yields `blocked_h{h} = missing_session_between` rather
  than silently lengthening the holding period. See
  `openspec/changes/factor-lab-label-semantics/`.
- **Not a deployable strategy.** The long-short spread is a statistical
  construct (A-share retail shorting is not available); the portfolio layer is
  what decides deployability.

## Without a cluster (local fallback)

For a small panel or a quick check, export and evaluate locally against a dev
Mongo if it is reachable:

```bash
PYTHONPATH=datahub datahub/.venv/bin/python -m app.jobs.factor_lab_runner export \
  --from-date 2026-01-01 --to-date 2026-03-31 --horizons 1,5 --output /tmp/lab.parquet
PYTHONPATH=datahub datahub/.venv/bin/python -m app.jobs.factor_lab_runner evaluate \
  --panel /tmp/lab.parquet --all --horizons 5
```
