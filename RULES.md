# Caifubao Agent Rules

This file is the single authority for repository-wide agent rules. `AGENTS.md`
defines workflow, `.project-rules.md` provides commands, and agent/skill files
define role-specific guidance. Those files must link here instead of copying
these rules.

Rules are ordered by priority: P1 > P2 > P3 > P4 > P5 > P6.

<a id="safety"></a>

## P1 — Safety

- Never commit credentials, tokens, kubeconfigs, database dumps, local env
  files, private domains, registry settings, or private runbooks.
- Use `.env.example` placeholders. Keep real deployment overlays and operator
  material in the private `caifubao-private` repository.

<a id="module-boundaries"></a>

## P2 — Module boundaries

| Module      | Owns                                                            | Must not                                                                    |
| ----------- | --------------------------------------------------------------- | --------------------------------------------------------------------------- |
| `datahub/`  | Market data, factors, signals, scoring, freshness, data quality | Render UI or expose user-facing APIs                                        |
| `backend/`  | Flask APIs, auth, service-token checks, light aggregation       | Run scheduled collection or backfill jobs                                   |
| `frontend/` | Vue UI, API consumption, rendering                              | Depend on Mongo shapes or bypass backend APIs                               |
| `k8s/`      | Public example deployment assets                                | Contain private deployment material                                         |
| OpenClaw    | Read-only consumption of backend APIs                           | Access MongoDB or trigger mutation, scheduling, backfills, or admin actions |

API responses are contracts; Mongo collection shapes are not.

<a id="spec-gate"></a>

## P3 — Spec gate

Create or update an OpenSpec change before implementation when work changes:

- public API endpoints, fields, pagination, filtering, or error shapes;
- authentication, authorization, token scope/lifecycle, or audit behavior;
- freshness semantics, `data_as_of`, generated timestamps, or status states;
- scoring, factors, signals, replay, calibration, or look-ahead-bias rules;
- data ownership between modules; or
- public documentation relied on by external users or downstream systems.

The gate is not required for internal refactors, tests, formatting, local
implementation details, or small behavior-preserving fixes.

<a id="surgical-discipline"></a>

## P4 — Surgical discipline

- State assumptions and a verifiable success criterion before editing.
- Make the smallest change that satisfies the request; avoid speculative
  features, one-use abstractions, and unrelated cleanup.
- Match existing style. Clean up only artifacts introduced by the change.
- For bugs, reproduce the failure with a test first when practical.
- Loop on the smallest useful validation until it passes.

<a id="validation"></a>

## P5 — Validation

| Change         | Minimum useful check                                                                              |
| -------------- | ------------------------------------------------------------------------------------------------- |
| Backend Python | CI-pinned `ruff` checks and focused pytest via `backend/venv312/bin/python`                       |
| Datahub Python | CI-pinned `ruff` checks and focused pytest via `datahub/.venv/bin/python`, or a supported dry-run |
| Frontend       | Focused tests when applicable, then `npm run lint` and `npm run build`                            |
| K8s examples   | `kubectl kustomize` or equivalent render check                                                    |
| OpenSpec       | `openspec validate --all --strict`                                                                |
| Rules/docs     | Link, path, command, and duplicate-content checks scoped to changed files                         |

If a check cannot run, report the command and reason. Use the `RUFF_VERSION`
pinned in `.github/workflows/ci.yml`; do not silently substitute another local
version.

<a id="existing-patterns"></a>

## P6 — Existing patterns

- Prefer existing module patterns over new architecture.
- A factor is normally one function returning a dict; a Flask endpoint is
  normally one route unless logic is reused across at least three endpoints.
- Follow current Flask, Vue 3, Vite, Pinia, and Element Plus conventions.
- Use Conventional Commit prefixes: `feat`, `fix`, `docs`, `style`, `refactor`,
  `test`, or `chore`.

<a id="non-trivial"></a>

## Non-trivial changes

A change is non-trivial if it touches runtime behavior, contracts, auth,
scoring, data models, CI, deployment manifests, or multiple modules. A small
docs-only, comments-only, formatting-only, or single-file mechanical edit is
trivial. When uncertain, use the non-trivial workflow.

<a id="review-gates"></a>

## Review gates

Implement, validate, review, check branch conflicts, then publish. Reviewers are
read-only and run after local validation:

| Reviewer            | Required when                                                                                      |
| ------------------- | -------------------------------------------------------------------------------------------------- |
| `spec-guardian`     | The P3 gate may apply                                                                              |
| `contract-reviewer` | API contracts, auth, freshness metadata, or OpenClaw integration are touched                       |
| `qa-reviewer`       | Any non-trivial code, CI, data-model, or deployment change; not docs/comments/formatting-only work |

Resolve and re-review P1 findings. Acknowledge P2 findings and remaining risk.

<a id="branch-and-pr"></a>

## Branch and PR rules

- Use a dedicated branch based on current `origin/develop`; never mix unrelated
  work or edit `main`/`develop` directly. Use a descriptive type prefix such as
  `feature/`, `fix/`, `docs/`, `chore/`, or `codex/`.
- Before handoff, verify the branch is conflict-free with the target base.
- When the requested delivery includes a PR, create it as Draft, wait for every
  CI job to pass, then mark it ready. Do not push or change PR state without the
  authority needed for that external action.

Commands live in [`.project-rules.md`](./.project-rules.md).

<a id="openclaw"></a>

## OpenClaw invariants

- Endpoints stay under `/api/v1/integrations/openclaw`.
- Authentication uses dedicated service tokens with `openclaw:data-read` or
  `openclaw:score-read`, not user JWTs.
- Responses expose adequate freshness metadata and preserve request auditing.
- Investment-analysis logic stays in OpenClaw; caifubao supplies read-only data,
  contracts, freshness, auth, and auditability.

<a id="task-notes"></a>

## Non-trivial task notes

```text
Outcome:
Module Impact:
Spec Gate: required / not required
Assumptions:
Write Scope:
Validation:
Reviewers:
Branch Conflict:
PR/CI: not requested / draft / green / ready
```
