# Caifubao Agent Rules (single authority)

This file is the **single source of truth** for all agent rules in caifubao.
Every other file (agent definitions, workflow docs, AGENTS.md) must reference
this file rather than duplicate rules. When rules change, change them here.

## Rule Priority (P1 > P2 > P3 > P4 > P5 > P6)

When rules conflict, higher priority wins. The agent's first duty is to the
rules at the top.

### P1 — SAFETY (must not violate)

- Do not commit credentials, tokens, kubeconfigs, database dumps, local env
  files, private domains, registry settings, or private runbooks.
- Use `.env.example` files for placeholders.
- Keep real deployment overlays and operator scripts in the private
  `caifubao-private` repository.

### P2 — MODULE BOUNDARIES (must not violate)

| Module | Owns | Must not |
|--------|------|----------|
| `datahub/` | Market data, factors, signals, scoring, freshness, data quality | Render frontend UI, expose user-facing APIs |
| `backend/` | Flask APIs, auth, service-token checks, light aggregation | Run scheduled data collection or backfill jobs |
| `frontend/` | Vue UI, API consumption, rendering | Depend on Mongo collection shapes, bypass backend APIs |
| `k8s/` | Public example deployment assets | Contain real secrets, private domains, registry settings |
| OpenClaw | Downstream read-only consumer of backend APIs | Receive Mongo credentials, trigger mutations/scheduling/backfills/admin |

API responses are the external contract. Mongo collection shape is not.

### P3 — SPEC GATE (required when any of these change)

- Public API endpoints, response fields, pagination, filtering, or error shapes
- Authentication, authorization, service-token scope, token lifecycle, or audit
- Freshness semantics, `data_as_of`, generated timestamps, or status states
- Scoring, factors, signals, replay, calibration, or look-ahead-bias rules
- Data ownership between `datahub`, `backend`, `frontend`, `k8s`, and OpenClaw
- Public docs that external users or downstream systems rely on

Not required for: internal refactors, tests, formatting, small behavior-preserving
fixes, or local implementation details.

### P4 — SURGICAL DISCIPLINE (Karpathy principles, apply to ALL agents)

**1. Think Before Coding**
- State assumptions explicitly before writing code. If ambiguous, ask.
- If a simpler approach exists, say so. If you don't understand something, stop.

**2. Simplicity First**
- No features beyond what was asked. No speculative code.
- No abstractions for single-use code (no class hierarchy for one function).
- If 200 lines could be 50, rewrite it.

**3. Surgical Changes**
- Do not "improve" adjacent code, comments, or formatting.
- Do not refactor things that aren't broken.
- Match existing style (quotes, naming, patterns) — do not reformat.
- Clean up only YOUR orphaned imports/variables. Do not remove pre-existing dead
  code unless asked.
- Every changed line must trace directly to the request.

**4. Goal-Driven Execution**
- Define a verifiable success criterion before writing code.
- For bugs: write a failing test, then implement the fix.
- Loop until verification passes. Do not stop at "looks right".

**Tradeoff:** For trivial tasks (simple typo fixes, one-line changes), use
judgment — not every change needs the full rigor.

### P5 — VALIDATION (run the smallest useful check before considering work done)

| Change type | Command |
|-------------|---------|
| Python (backend or datahub) | `ruff check` + `ruff format --check` + smallest relevant pytest |
| Backend API | Focused pytest under `backend/app/test/` |
| Datahub | Focused datahub tests or runner dry-run |
| Frontend | `cd frontend && npm run lint && npm run build` |
| K8s examples | `kubectl kustomize` or equivalent render check |

If a check cannot be run, say exactly why.

### P6 — EXISTING PATTERNS (prefer, but lower priority than above)

- Prefer the existing patterns in `datahub/app/lib/scoring_engine/` over new patterns.
- A new factor component is one function returning a dict — it does not need its own module.
- An API endpoint is one Flask route function — it does not need a service layer unless
  the logic is shared across 3+ endpoints.
- Follow existing Flask blueprint, model, utility, and test patterns.
- Follow Vue 3, Vite, Pinia, and Element Plus patterns already in the repo.
- Use conventional commits: `feat:`, `fix:`, `docs:`, `style:`, `refactor:`, `test:`, `chore:`.

## Task Notes Template

For every non-trivial task, maintain these notes:

```text
Outcome:
Module Impact:
Spec Gate: required / not required
Assumptions:
Write Scope:
Validation Plan:
Reviewer Requests:
Branch Conflict Check:
Gate Checklist (close-out):
  [ ] spec-guardian:
  [ ] contract-reviewer:
  [ ] qa-reviewer:
  [ ] branch-conflict:
  [ ] draft-pr-ci:
```

## Non-Trivial Task Definition

A task is "non-trivial" (requiring full Gate and Task Notes) unless it meets ALL
of these criteria:
- Changes fewer than 10 lines of non-doc code
- Touches exactly one file
- Does not change API contracts, auth, scoring, data models, CI, or k8s manifests
- Is a behavior-preserving fix (typo, formatting, comment, simple config value)

When in doubt, treat the task as non-trivial. It is always safer to run gates
and skip unnecessary ones than to skip a required gate.

## OpenClaw-Specific Rules

- Endpoints remain under `/api/v1/integrations/openclaw`.
- Authentication uses dedicated service tokens, not user JWTs.
- Required scope: `openclaw:data-read` (broad) or `openclaw:score-read` (narrow).
- Responses include `data_as_of`, generated time, or freshness state.
- OpenClaw cannot trigger mutation, scheduling, backfill, admin actions, or
  direct database access.
- Do not let OpenClaw investment analysis logic enter caifubao. Caifubao
  provides data, contracts, freshness, auth, and auditability.

## Review Gates (for orchestrator)

### Execution Order (enforced)

Reviews run AFTER implementation completes and validation passes. The orchestrator
MUST invoke them — they do not self-activate.

1. **Implement** → 2. **Validate** (P5 checks) → 3. **Review** (below) → 4. **Branch check** → 5. **Draft PR** → 6. **CI Check** → 7. **Done**

Do not skip to "Done" before all gates clear.

### Review Gate Table

Schedule reviewers in the task plan BEFORE implementation starts. These do NOT
self-trigger — the orchestrator MUST explicitly invoke them as subagents.

| Reviewer | Required when |
|----------|---------------|
| `spec-guardian` | New API endpoint, auth change, scoring semantics change, boundary shift |
| `contract-reviewer` | API contract change, auth change, freshness metadata change, OpenClaw integration touched |
| `qa-reviewer` | **Any non-trivial code change** (Python, JS/TS, k8s manifests, CI, DB models, auth, scoring, API). Only skip for: docs-only, comment-only, formatting-only changes. |

P1 issues must be resolved and re-reviewed. P2 warnings must be acknowledged.

### Draft PR + CI Gate (enforced)

Before any change is considered ready for review:

1. **Always create PRs as Draft** (`gh pr create --draft --base develop`).
   A regular PR implies the work is ready for human review and merge. Draft
   status signals that CI validation is still pending.
2. **Wait for all CI checks to pass.** Inspect every job in the CI workflow.
   If any job fails, fix the issue and push again. Do NOT convert to a regular
   PR while any check is failing or still running.
3. **After all CI passes**, convert the Draft to "Ready for review":
   `gh pr ready <PR_NUMBER>`

This gate runs after the review gates above. Both must clear before
the orchestrator considers the task complete.

### Branch Isolation Rule (enforced)

Every non-trivial task requires a dedicated feature/fix branch created from
`develop`. Never edit on another task's branch. Never edit on `develop` or
`main` directly. The orchestrator MUST verify the branch is clean and on-topic
before making the first edit. If in doubt, create a new branch from develop.

### Gate Checklist (include in final summary)

Every non-trivial change MUST close with this checklist in the final summary:

```text
[ ] spec-guardian:  triggered / not triggered
[ ] contract-reviewer: triggered / not triggered
[ ] qa-reviewer:      triggered / not triggered
[ ] branch-conflict:  clean / conflicts resolved
[ ] draft-pr-ci:      created as draft / CI passed / converted to regular
```

## Branch Conflict Check (enforced)

Before any change is marked "done," the orchestrator MUST verify the working
branch is conflict-free against the target base branch (`develop` or `main`):

```bash
git fetch origin develop --quiet
git merge-tree $(git merge-base HEAD origin/develop) origin/develop HEAD
```

- If output contains conflict markers (`<<<<<<<` / `>>>>>>>`) → resolve before completing.
- If output is clean → proceed.
- If the repo has no remote or the check cannot run → state exactly why in the summary.

This check runs AFTER reviews pass and BEFORE the orchestrator closes the task.
It replaces the manual step in `.project-rules.md#5` for agent-driven workflows.

## Review Format

```text
Findings
- [P1] File:line - concise issue and impact.

Open Questions
- Any assumption affecting correctness.

Validation Gaps
- Checks still needed.

Summary
- Brief safety note.
```
