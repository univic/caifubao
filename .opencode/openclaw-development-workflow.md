# OpenClaw Development Workflow for Caifubao

This workflow is for using OpenClaw to direct caifubao development. It is not
the runtime OpenClaw integration contract.

## Operating Model

OpenClaw should behave like a lightweight technical lead:

1. Understand the requested outcome.
2. Identify affected modules.
3. Decide whether OpenSpec must change first.
4. Assign exactly scoped implementation work.
5. Review contracts and behavioral risk.
6. Run the smallest useful validation.
7. Report changed files, checks, and remaining risk.

The orchestrator is the single final owner. Other agents may implement bounded
slices or review, but they do not own merge decisions.

## Core Agents

- `caifubao-orchestrator`: primary owner for routing, planning, integration,
  validation, and final summary.
- `spec-guardian`: read-only spec gate for behavior, contract, auth, freshness,
  scoring, and boundary changes.
- `backend-implementer`: bounded Flask/API/auth/model/test changes.
- `datahub-implementer`: bounded data production, scoring, freshness, runner,
  model, and test changes.
- `frontend-implementer`: bounded Vue/API client/store/view/component changes.
- `k8s-implementer`: bounded public example deployment and workflow changes.
- `contract-reviewer`: read-only API, freshness, auth, OpenClaw compatibility,
  and module-boundary review.
- `qa-reviewer`: read-only safety, regression, test, and repository hygiene
  review.

Use implementers only when their write scope is clear. Do not activate every
agent by default.

## Task Notes

For non-trivial work, the orchestrator should keep this compact checklist:

```text
Outcome:
Module Impact:
Spec Gate: required / not required
Write Scope:
Validation Plan:
Reviewer Requests:
```

## Spec Gate

Run the spec gate before code changes when a task affects:

- API endpoints, response fields, pagination, filtering, or errors
- Auth, service tokens, scopes, token lifecycle, or audit fields
- Freshness semantics, data dates, generated timestamps, or status states
- Scoring, factors, signals, replay, calibration, or look-ahead-bias rules
- Data ownership between datahub, backend, frontend, k8s, and OpenClaw
- Public docs used by downstream consumers

Internal refactors and behavior-preserving fixes do not need a spec update.

## Write-Scope Rule

Each implementation agent must receive an explicit write scope. Examples:

```text
backend-implementer:
Write scope: backend/app/api/v1/integrations/openclaw/ and backend/app/test/test_openclaw_*.py

frontend-implementer:
Write scope: frontend/src/api/scores.ts, frontend/src/stores/market.ts, frontend/src/views/Dashboard/
```

If a task needs cross-module work, split it by data flow:

```text
datahub produces record -> backend exposes API -> frontend renders API response
```

Avoid assigning multiple agents to the same files.

## Review Gates

Use `contract-reviewer` when touching API, OpenClaw, auth, freshness, scoring
contract, or frontend API consumption.

Use `qa-reviewer` before finishing non-trivial changes or public repository
changes.

Reviewers should report findings. They should not take over the implementation
plan unless the orchestrator asks for a revised plan.

## Validation Defaults

- Python changes: relevant `ruff check`, `ruff format --check`, and the smallest
  useful pytest target.
- Backend API changes: focused pytest under `backend/app/test/`.
- Datahub changes: focused datahub tests or runner dry-runs where available.
- Frontend changes: `npm run lint` and `npm run build` when relevant.
- Deployment examples: `kubectl kustomize` or equivalent render check.

## Hard Boundaries

- Do not put OpenClaw investment analysis logic into caifubao.
- Do not expose Mongo collection internals as public API contracts.
- Do not let OpenClaw mutate data, trigger jobs, run backfills, or receive admin
  access.
- Do not commit real credentials, kubeconfigs, private domains, registry
  settings, database dumps, or local environment files.
