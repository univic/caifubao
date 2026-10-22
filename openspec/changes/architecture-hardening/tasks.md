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
