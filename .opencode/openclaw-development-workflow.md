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

## Review Gates (MANDATORY)

Reviewers are NOT optional. Every non-trivial change set MUST go through the
appropriate reviewer(s) before being considered complete. The orchestrator
MUST schedule reviewer invocation as part of the implementation plan from
the start — not as an afterthought.

### contract-reviewer — AUTO-TRIGGER when ANY of:
- A new API endpoint is added or an existing endpoint changes signature
- Auth decorators, scopes, or token lifecycle are touched
- Response fields, pagination, filtering, or error shapes change
- Freshness metadata, `data_as_of`, `generated_at`, or status fields change
- Scoring contract, explanation, input_snapshot, or verification shapes change
- OpenClaw integration API or `docs/integrations/openclaw.md` is touched
- A new blueprint is registered

### qa-reviewer — AUTO-TRIGGER when ANY of:
- Cross-module changes (backend + frontend, datahub + backend, etc.)
- New mutation/write endpoints (POST, PUT, DELETE)
- New authentication or authorization code
- Changes to `sys.path`, import paths, or module dependencies
- Any change that creates, modifies, or deletes MongoDB documents
- Before creating a PR or merging to main

### spec-guardian — AUTO-TRIGGER when ANY of:
- A new API endpoint is added
- Auth model, scopes, or token format changes
- Scoring, factor, or signal semantics change
- Data ownership boundaries shift between modules
- `DESIGN.md` or `AGENTS.md` implications exist

### Execution Rules
1. The orchestrator MUST include reviewer invocation in the task plan
   (todowrite) BEFORE starting implementation.
2. Reviewers run AFTER implementation completes and validation passes, but
   BEFORE marking the task as done.
3. If a reviewer reports P1 issues, they MUST be resolved and the reviewer
   re-run (or the fixes verified by the orchestrator with explicit sign-off).
4. P2 warnings MUST be explicitly addressed or acknowledged in the commit
   message or PR description.
5. The orchestrator grows a final summary enumerating each reviewer that ran
   and the outcome.

### Gate Checklist (include in final summary)
```text
[ ] spec-guardian:  triggered / not triggered
[ ] contract-reviewer: triggered / not triggered
[ ] qa-reviewer: triggered / not triggered
```

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
