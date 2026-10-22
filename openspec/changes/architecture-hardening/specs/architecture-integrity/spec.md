# Architecture Integrity

## ADDED Requirements

### Requirement: Execution costs have one source of truth

The system SHALL define A-share execution cost assumptions (commission,
minimum commission, sell stamp duty, transfer fee, slippage, board lot) in
`datahub/app/lib/execution/costs.py`. Every engine that reports P&L — the factor
lab, the paper NAV engine, the ETF lab, the timing replay and the backend
backtest service — SHALL use those values. The backend image cannot import the
datahub package, so it MAY mirror the constants, but a test SHALL fail when the
mirrored values diverge.

#### Scenario: A rate is changed on one side only

- GIVEN the canonical model and the backend mirror
- WHEN a shared rate (commission, minimum commission, stamp duty or slippage)
  differs between them
- THEN `backend/app/test/test_friction_contract_parity.py` fails

#### Scenario: Sell stamp duty uses the current rule

- GIVEN a sell-side trade in any engine
- WHEN the stamp duty is applied
- THEN the rate is 0.05 % of notional (the rule in force since 2023-08-28),
  unless the instrument is explicitly exempt (ETFs)

### Requirement: The backend must not run ingestion, factor or signal jobs

The backend SHALL contain only API, authentication, aggregation and read paths.
It SHALL NOT contain a copy of the datahub ingestion pipeline, a scheduled
factor/signal runner, or a trading agent graph. Collection ownership is
documented in `docs/architecture/environment-model.md`.

#### Scenario: A request or script tries to refresh market data from the API pod

- GIVEN the backend source tree
- WHEN it is inspected for scheduled collection, factor or signal execution
- THEN no such code path exists; those jobs live only under `datahub/` and run
  as datahub CronJobs

### Requirement: Shared model definitions must stay in parity

Both services SHALL keep the same definition for every mongoengine document
class that exists in both model packages. The definition SHALL agree on the
MongoDB collection name, the field names, each field's base type and each
field's `unique` flag. CI SHALL run datahub tests against the datahub models,
not against a copy of the backend models.

#### Scenario: A shared field is renamed on one side

- GIVEN a shared document class
- WHEN one service renames, removes or retypes a field, or changes its
  collection
- THEN `datahub/app/test/test_model_contract_parity.py` fails with the class,
  field and both definitions named

### Requirement: Asynchronous compute tasks are retried and reclaimed

The compute-worker SHALL claim tasks atomically, retry a failed task until its
`max_retries` budget is exhausted, mark it FAILED afterwards, and requeue
RUNNING tasks abandoned by a dead worker at startup. It SHALL stop claiming new
tasks on SIGTERM/SIGINT and let in-flight tasks finish. It SHALL expose a
heartbeat so a wedged process can be restarted by the liveness probe. The image
SHALL be buildable from the repository root and deployable with the same
ConfigMap/Secret contract as the other services.

#### Scenario: A worker dies mid-task

- GIVEN a task left RUNNING by a dead worker
- WHEN a worker starts and the task is older than `TASK_STALE_MINUTES`
- THEN the task is requeued with `retry_count + 1`, or marked FAILED when the
  retry budget is exhausted

#### Scenario: A task fails deterministically

- GIVEN a task whose handler raises or reports failure
- WHEN the failure is recorded
- THEN the task returns to PENDING until `max_retries` is exhausted, and only
  then becomes FAILED

### Requirement: Liveness and readiness have distinct meanings

Readiness SHALL require the service to reach MongoDB; liveness SHALL NOT depend
on MongoDB, so a database outage does not restart every pod. The backend SHALL
serve liveness at `/health` and readiness at `/health/ready`. The datahub service
SHALL serve liveness at `/health` and readiness at `/health/ready`, where
readiness requires MongoDB and, when the legacy scheduler is enabled, a running
scheduler. Kubernetes readiness probes SHALL use `/health/ready`.

#### Scenario: MongoDB is unreachable

- GIVEN a running backend or datahub pod
- WHEN MongoDB stops accepting connections
- THEN readiness fails and the pod leaves the Service endpoints
- AND liveness keeps passing, so the pod is not restarted in a crash loop

### Requirement: A repaired quote day satisfies the signal dependency gate

The datahub startup catch-up SHALL record the same daily slot as the canonical
quote-stock CronJob (18:00, stock target, factors included) and SHALL persist
the merged phase evidence. The signal dependency gate SHALL accept either the
CronJob record or the catch-up record, including the `target="all"` shape.

#### Scenario: The quote CronJob fails but the pod restarts

- GIVEN the canonical quote-stock CronJob did not run for the current trading
  day and quote data is lagging
- WHEN the datahub pod starts after the catch-up window and completes the
  catch-up with factors
- THEN the signal job's dependency gate accepts the day and signals can be
  computed

### Requirement: API errors use the JSON envelope

Every backend error response SHALL be JSON with the same envelope as success
responses (`success`, `message`, `error_code`, `request_id`, `generated_at`,
`data`), including 404, 405 and unhandled 500 responses. Internal exception text
SHALL NOT be returned to clients.

#### Scenario: An unexpected exception occurs

- GIVEN an API route raises an unhandled exception
- WHEN the response is produced
- THEN the status is 500, the body is JSON with `error_code=INTERNAL_ERROR`, and
  the exception message is logged but not returned
