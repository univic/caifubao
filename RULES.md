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
```

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

For non-trivial changes, schedule these reviewers BEFORE implementation:

| Reviewer | Auto-trigger when |
|----------|-------------------|
| `spec-guardian` | New API endpoint, auth change, scoring semantics change, boundary shift |
| `contract-reviewer` | API contract change, auth change, freshness metadata change, OpenClaw integration touched |
| `qa-reviewer` | Cross-module change, new mutation endpoints, new auth code, MongoDB document changes |

After implementation and validation pass, run reviewers before marking task done.
P1 issues must be resolved and re-reviewed. P2 warnings must be acknowledged.

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
