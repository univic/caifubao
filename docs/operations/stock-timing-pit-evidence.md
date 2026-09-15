# Stock Timing PIT Forward Evidence — Operator Runbook

> The project is an A-share research/learning/demo MVP and **not investment
> advice**. This runbook describes how to accumulate forward point-in-time
> evidence; it authorises neither real-money execution nor model promotion.

The operator command surface is documented in
[`agent-cli.md`](./agent-cli.md#stock-timing-pit-input-capture-p2a-research-only);
this file is the runbook for running that chain day after day on a durable
artifact store.

## 1. The three phases

Each evidence session `D` produces three immutable JSON artifacts:

| Artifact | Command | Legal capture window |
|:---|:---|:---|
| `universe-<D>.json` | `capture-pit-universe --date D` | after `D-1` close, before `D` opens |
| `ranked-inputs-<D>.json` | `capture-pit-inputs` | after `D` closes, before `D+1` opens |
| `ranked-predictions-<D>.json` | `score-pit-artifacts` (dry-run) | any time after the input artifact exists |

`capture-pit-universe` freezes membership and pre-open industry classification.
`capture-pit-inputs` binds that exact universe to the complete ranked scoring
read set for `D` with the model version, config hash, calendar and build
revision. `score-pit-artifacts` reconstructs ranked predictions offline, from
the artifact rows only, and emits the per-horizon Merkle handoff that P1
(`timing-replay`) requires.

Every command refuses to overwrite an existing output path, but that is only a
per-path guard: nothing prevents a second, differently hashed artifact for the
same session under another name. Treat one artifact per session and phase as an
operator rule, and never re-capture a session whose window has closed.

## 2. Durable artifact store

Artifacts live on the `stock-timing-artifacts` PVC mounted at `/artifacts`
(example: [`k8s/base/stock-timing-artifacts.example.yaml`](../../k8s/base/stock-timing-artifacts.example.yaml)).

This is **not** a cache. The universe artifact hashes its capture instant
(`as_of`), so it can never be reproduced; the inputs artifact hashes the session
close instead, so it can be re-captured with the same hash only while its
capture window is still open. Once a window closes, a lost file is a permanent
gap in the forward window. Use a StorageClass with a `Retain` reclaim policy and
never write artifacts to a pod filesystem.

The claim is `ReadWriteOnce`: the two capture phases must not overlap.

## 3. Daily sequence

Each phase is one Job and one file:

| Phase | Example Job |
|:---|:---|
| universe | [`k8s/base/stock-timing-universe.example.yaml`](../../k8s/base/stock-timing-universe.example.yaml) |
| inputs | [`k8s/base/stock-timing-inputs.example.yaml`](../../k8s/base/stock-timing-inputs.example.yaml) |
| score (dry-run) | [`k8s/base/stock-timing-score.example.yaml`](../../k8s/base/stock-timing-score.example.yaml) |

The capture Jobs mount the claim and read `MONGODB_*`; the score Job
deliberately has none, because the default dry run performs no database access.

`CAIFUBAO_BUILD_REVISION` must equal the immutable revision of the image that
executes the command. The published image bakes it; the CLIs reject an empty
value but cannot detect a wrong one, so never hand-write a placeholder.

Before each session, update **every** session-bound path in the manifest you are
about to apply: the universe Job's `--date` and `--output`; the inputs Job's
`--universe-artifact` and `--output`; and the score Job's `--universe-artifact`,
`--input-artifact` and `--output`. A stale pair on the score Job validates and
scores the previous session while writing it under the new session's filename,
so a filename alone proves nothing — check the artifact's own `payload.session`.
Jobs have fixed names and immutable pod templates, and outputs are exclusive, so
re-running a phase means deleting the Job and using the new session's paths:

```bash
# 1. pre-open, for session 2026-09-16
kubectl -n <namespace> apply -f k8s/base/stock-timing-universe.example.yaml
kubectl -n <namespace> wait --for=condition=complete job/stock-timing-universe --timeout=1h

# 2. post-close on 2026-09-16
kubectl -n <namespace> apply -f k8s/base/stock-timing-inputs.example.yaml
kubectl -n <namespace> wait --for=condition=complete job/stock-timing-inputs --timeout=2h

# 3. offline dry-run scoring
kubectl -n <namespace> apply -f k8s/base/stock-timing-score.example.yaml
kubectl -n <namespace> wait --for=condition=complete job/stock-timing-score --timeout=1h

# before the next session
kubectl -n <namespace> delete job stock-timing-universe stock-timing-inputs stock-timing-score
```

The wait timeouts match each Job's `activeDeadlineSeconds`, so a wait timeout
means the Job's own deadline is about to fire.

`capture-pit-inputs` requires the named model version to be registered and
`ACTIVE` with `scoring_mode=ranked`; it fails closed otherwise, so confirm the
registry pin before the session rather than after.

The same three commands can be run by hand with the datahub virtualenv and
`PYTHONPATH=datahub`; see `agent-cli.md` for the exact invocation.

Because the phases have different legal windows, a scheduler must run phase 1
at the next pre-open and phase 2 after the close. A missed window is not
recoverable by re-running later: the capture is rejected.

## 4. Verification

After each phase:

1. Confirm the Job reached `Complete` and the command printed an
   `artifact_id`; record it against the session.
2. Confirm the file exists on the claim and record its `sha256sum`.
3. Confirm the session has exactly one artifact for that phase. The CLI only
   guards the output path, so a duplicate under another name is possible and is
   an operator error, not extra evidence.

`score-pit-artifacts` output carries the exact result-file SHA-256 and the
per-horizon Merkle root/proof over the frozen cohort. P1 accepts a prediction
signal only when that proof, the cohort fingerprint, the artifact hash and the
`data_as_of` binding all validate; a tampered or partial artifact fails closed.

## 5. Handoff to P1 replay

Once a session's artifacts exist, assemble the versioned `timing-replay` P1
manifest (shape:
[`docs/examples/timing-replay-p1-manifest.json`](../examples/timing-replay-p1-manifest.json))
from the frozen cohort and the ranked prediction cohort, then:

```bash
PYTHONPATH=datahub datahub/.venv/bin/python -m app.jobs.backtest_runner \
  timing-replay /path/to/manifest.json --output /path/to/report.json
```

P1 replays a timing arm and a same-stock buy-and-hold arm under identical
next-open execution, board-lot and friction assumptions. Its output stays
`research_only=true` and `validation_status=UNVALIDATED` even when the
mechanical gates pass.

## 6. Boundaries and known gaps

- **No publication without explicit authorisation.** `score-pit-artifacts`
  defaults to dry-run; `--apply` writes `StockScorePrediction` and needs a
  separate, explicit operator decision. The score Job has no `MONGODB_*` env by
  design, so `--apply` also requires adding the same env vars the capture Jobs
  use.
- **Forward-only.** REPLAY rows, backfills and `--replace` re-runs never count
  as forward evidence, no matter how green the Job was.
- **A single-stock baseline is not alpha evidence.** Promotion-level claims need
  the frozen gates: at least 50 names, at least 120 sessions, at least 5 filled
  trades, single-name concentration below 40%, and walk-forward decay at or
  below 20%. Never tune on one stock and present the result as alpha.
- **Tuning ends the window.** Changing the model version, config, horizon or
  percentiles invalidates comparability; start a new forward window instead of
  mixing configurations.
- **Industry classification is a live input.** The universe artifact snapshots
  pre-open classification; if a name has no classification row, the ranked
  inputs do not carry one either. Check coverage before starting a long window
  and fix data gaps before the window rather than during it.
