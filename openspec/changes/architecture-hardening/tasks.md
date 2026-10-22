# Architecture Hardening — Tasks

## 1. Single source of truth

- [x] 1.1 Add `datahub/app/lib/execution/costs.py` with the A-share rate
      constants, `CostProfile` and `side_cost`.
- [x] 1.2 Re-export the constants from `factor_lab.metrics` and use them in the
      ledger defaults so existing callers keep working.
- [x] 1.3 Align the backend backtest friction constants (stamp duty 0.05 %) and
      add `test_friction_contract_parity.py`.

## 2. Module boundary

- [x] 2.1 Delete the backend shadow ingestion pipeline and the unreferenced
      legacy strategy/trading modules.
- [x] 2.2 Delete the legacy models only those modules used (`BackTest`,
      `TradeOpportunity`, `TradePlan`, `Task`).
- [x] 2.3 Delete the stale `test_baostock_interface.py` that loaded a deleted
      file, and the `test_datahub_task_controller_common.py` for the deleted
      controller.

## 3. Model contract

- [x] 3.1 Add `datahub/app/test/test_model_contract_parity.py` comparing shared
      model classes (collection, fields, types, unique flags).
- [x] 3.2 Remove the CI step that copied backend models over datahub models.

## 4. Compute worker

- [x] 4.1 Root-context Dockerfile that bakes a merged `app` package; root
      `.dockerignore`.
- [x] 4.2 Bootstrap module prepares the merged tree for local runs.
- [x] 4.3 Retry budget, stale-RUNNING reaping, SIGTERM handling, configurable
      concurrency, heartbeat file.
- [x] 4.4 ConfigMap/Secret wiring, real probes, `imagePullSecrets`, image entry
      in all overlays, publish workflow and CI build-input check.
- [x] 4.5 Backend served by gunicorn (module-level `app` in `main.py`,
      `GUNICORN_WORKERS`/`GUNICORN_TIMEOUT` in the manifest).

## 5. Health and orchestration

- [x] 5.1 Backend liveness/readiness split; K8s readiness uses `/health/ready`.
- [x] 5.2 Datahub liveness/readiness split; readiness requires MongoDB; connection
      timeouts aligned with the backend.
- [x] 5.3 Startup catch-up records the canonical 18:00 stock+factors slot with
      merged phase evidence; the signal gate accepts the catch-up job name and
      the `target="all"` shape.

## 6. Client contracts

- [x] 6.1 JSON error handlers for HTTP errors and unhandled exceptions.
- [x] 6.2 Decisions view uses the shared axios client for alerts/quality.
- [x] 6.3 Failed refresh rejects queued requests instead of leaving them pending.
- [x] 6.4 Mock data only in dev builds.

## 7. Hygiene

- [x] 7.1 nginx security headers in every location that sets `add_header`.
- [x] 7.2 Remove the unused runtime `VITE_API_BASE_URL`.
- [x] 7.3 Portable `dev-entrypoint.sh`; idempotent `update-kustomization.sh`.
- [x] 7.4 Docs: DESIGN.md description, fee numbers, README core modules, research
      tarball ignore rule.
- [x] 7.5 Dev overlay references its own datahub CronJob file (data-sync and
      health watcher were defined but never rendered).

## 8. Validation

- [x] 8.1 `ruff check` + `ruff format --check` for backend/datahub/compute-worker.
- [x] 8.2 `pytest datahub/app/test/` (1452 passed).
- [x] 8.3 `pytest backend/app/test/`: 332 passed; the 8 failures all raise
      `ServerSelectionTimeoutError` because this workstation has no MongoDB (CI
      provides one).
- [x] 8.4 `npm run lint`, `npm run test:run`, `npm run build`.
- [x] 8.5 `kubectl kustomize` for all three overlays.

## 9. Shared contracts package

- [x] 9.1 `shared/caifubao_contracts` with `models/` (8 shared documents),
      `friction.py`, `time.py`, `json_util.py`; installable `pyproject.toml`.
- [x] 9.2 Backend/datahub `app/model/<name>.py` are re-export shims; parity test
      now asserts shims only (no local classes, complete re-exports).
- [x] 9.3 Backend/datahub/compute-worker images install the package; build
      contexts move to the repository root (Dockerfiles + publish workflows +
      CI checks).
- [x] 9.4 Source checkouts get the package via `app/__init__` fallback,
      conftest paths, worker bootstrap and `dev-entrypoint.sh`.

## 10. Metrics and request correlation

- [x] 10.1 Backend `/metrics` (request count/latency, compute task gauges),
      gunicorn multiprocess support, `/metrics` kept off the DB path.
- [x] 10.2 Datahub `/metrics` (readiness, job last-success, asset status, quote
      freshness) served by the existing health server.
- [x] 10.3 `X-Request-ID` propagation into responses, JSON logs and error bodies.
- [x] 10.4 k8s scrape annotations + `docs/operations/observability.md`.

## 11. Strategic-merge overlay patches

- [x] 11.1 Replace positional `env/4/...` patches for datahub, compute-worker
      and all research/prod CronJobs with strategic-merge patches.
- [x] 11.2 Patch the prod industry-sync job (previously left on the placeholder
      secret) and add the snapshot-export job to the prod overlay.

## 12. Time contract and TASK-404

- [x] 12.1 Shared UTC/Beijing helpers; DB timestamps use naive UTC.
- [x] 12.2 API timestamps carry `+00:00`; request-id in error/log.
- [x] 12.3 Trading-calendar defaults evaluate at call time in Asia/Shanghai.
- [x] 12.4 Stable snapshot export + dev snapshot import CronJobs; `MONGODB_SRC_*`
      removed from public examples; environment model, agent CLI and capability
      docs updated.
