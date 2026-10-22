# Architecture Hardening — Contracts, Runtime Honesty, and Dead-Code Removal

## Why

A read-only architecture review found four classes of structural problems:

1. **Duplicated semantics that had already drifted.** The backend and datahub
   each defined the same A-share friction model; the backend used a 0.1 % sell
   stamp duty while the research/paper stack used 0.05 % (the rate in force
   since 2023-08-28). Backtest and research P&L were therefore not comparable.
   The same collection models existed twice, and CI hid the drift by copying the
   backend models over the datahub ones before running tests, so datahub tests
   exercised a model set the image never ships.
2. **A shadow ingestion pipeline in the backend.** `backend/app/lib/datahub`,
   `factor_factory`, `signal_man`, `task_controller` and the legacy
   `trade_agent`/`backtest_agent` graph could still write `stock_daily_quote` and
   `stock_factor_daily` from the API pod, contradicting the RULES module
   boundary and the "no trading capability" statement.
3. **Deployment claims that the code did not honour.** The compute-worker image
   could not build (it copied a non-existent directory), referenced a Secret no
   overlay created, had no publish workflow, no retry/reaping, and a liveness
   probe that could never fail. Backend and datahub readiness probes reported
   healthy with MongoDB down. The datahub startup catch-up repaired quotes but
   not the MA-factor evidence the signal gate requires, so a recovered day still
   looked missing downstream.
4. **Client contract gaps.** Unhandled backend errors returned Flask HTML while
   the Vue client parses JSON; the decision view bypassed the authenticated
   axios instance; a failed token refresh left queued requests pending forever;
   and a build-time mock switch could serve fabricated research numbers in a
   production bundle.

## What Changes

- Add `datahub/app/lib/execution/costs.py` as the single source of truth for
  A-share execution costs; the factor lab, strategy engine, ETF lab and timing
  replay import it. The backend mirrors the same constants (separate image) and
  `backend/app/test/test_friction_contract_parity.py` fails on drift.
- Delete the backend shadow pipeline and its unreferenced legacy modules
  (`trade_agent`, `trade_operator`, `trade_planner`, `cartographer`,
  `backtest_agent`, `scenario_director`, `factor_factory`, `signal_man`,
  `opportunity_seeker`, `periodic_task_dispatcher`, `task_controller`,
  `app.lib.datahub`, `app/schemes`) and the legacy models they owned.
- Replace CI's model-copy step with `datahub/app/test/test_model_contract_parity.py`,
  which compares collection names, field names, field types and unique flags for
  every model class the two services share.
- Make compute-worker deployable and honest: root-context Dockerfile with a
  merged `app` package, ConfigMap/Secret wiring like the other services, image
  entry in every overlay, publish workflow, retry budget, stale-RUNNING reaping,
  SIGTERM handling, real concurrency and a heartbeat liveness probe.
- Split liveness from readiness: backend `/health` stays process-only and
  `/health/ready` checks MongoDB; datahub `/health` is liveness and
  `/health/ready` requires MongoDB; the K8s readiness probes use the new paths.
- Make the datahub startup catch-up write the canonical stock+factors slot
  (18:00, `include_factors=True`, merged phase evidence) and let the signal gate
  accept either the CronJob or the catch-up record.
- Return JSON for every backend error (404/405/unhandled 500) with the same
  envelope as success responses.
- Fix the frontend decision view to use the shared axios client, reject queued
  requests when a token refresh fails, and restrict the mock-data switch to dev
  builds.
- Extract the shared contracts package (`shared/caifubao_contracts`): MongoDB
  models, friction constants and UTC/Beijing time helpers installed into the
  backend, datahub and compute-worker images. Service `app/model/*.py` files
  become re-export shims; build contexts move to the repository root.
- Add Prometheus metrics (`/metrics`) and request-id correlation to the backend
  and datahub, with readiness/liveness already split.
- Convert the production/research overlay JSON patches from positional paths
  (`env/4/...`) to strategic-merge patches, and fix the prod industry-sync and
  compute-worker secret wiring.
- Standardize timestamps on naive-UTC storage, offset-bearing API output and
  Beijing trading-calendar days; replace `datetime.now()` call sites.
- Close TASK-404 in the public examples: the stable environment exports a
  checksummed snapshot and dev imports it; no dev CronJob reads the stable
  MongoDB, and `MONGODB_SRC_*` is removed from the examples.
- Hygiene: nginx security headers survive location-level `add_header`; the
  unused runtime `VITE_API_BASE_URL` is removed; `dev-entrypoint.sh` no longer
  hardcodes a developer path; `scripts/update-kustomization.sh` is idempotent;
  the stale "force rebuild" comment and 42 MB of untracked research tarballs stay
  out of git.

## Non-goals

- No shared Python package across the backend and datahub images (build contexts
  are per-service). The parity tests are the guard until packaging is revisited.
- No change to scoring, factor, signal or strategy semantics other than the
  corrected stamp-duty constant.
- No authentication policy change: the 74 unauthenticated routes remain, because
  enforcement is an ingress/product decision recorded in the review, not a
  mechanical edit.
- No MongoDB replica set, backup unsuspension or metrics stack (tracked in the
  roadmap/backlog).
