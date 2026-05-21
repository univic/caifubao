# OpenClaw Development Workflow for Caifubao

This workflow is for using OpenClaw to direct caifubao development. It is not
the runtime OpenClaw integration contract.

**Rule authority:** All safety, boundary, discipline, validation, spec-gate, and
OpenClaw-specific rules are defined once in `RULES.md`. This file describes the
agent workflow; `RULES.md` defines the rules agents must follow.

## Operating Model

OpenClaw should behave like a lightweight technical lead:

1. Understand the requested outcome.
2. Identify affected modules.
3. Decide whether OpenSpec must change first.
4. Assign exactly scoped implementation work.
5. Implement the smallest change.
6. Run validation checks — loop until they pass.
7. Run reviewers (contract-reviewer + qa-reviewer) — enforced gates.
8. Run branch conflict check against the target base branch.
9. Report changed files, checks, review outcomes, and remaining risk.
10. Close with the Gate Checklist.

The orchestrator is the single final owner. Other agents may implement bounded
slices or review, but they do not own merge decisions.

## Core Agents

Defined in `AGENTS.md`. Use these roles:

- `caifubao-orchestrator`: primary owner for routing, planning, integration,
  validation, and final summary.
- `spec-guardian`: read-only spec gate decision.
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

```text
Outcome:
Module Impact:
Spec Gate: required / not required
Assumptions:
Write Scope:
Validation Plan:
Reviewer Requests:
```
When closing the task, complete the Gate Checklist (see below).

## Spec Gate

Defined in `RULES.md#spec-gate`. Run the spec gate before code changes when
a task affects endpoints, auth, freshness, scoring, data ownership, or public
docs. Internal refactors and behavior-preserving fixes do not need a spec update.

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

Defined in `RULES.md#review-gates`. Reviewers are NOT optional. Every
non-trivial change set MUST go through the appropriate reviewer(s) before
being considered complete.

### Execution Rules
1. The orchestrator MUST include reviewer invocation in the task plan
   BEFORE starting implementation.
2. Reviewers run AFTER implementation completes and validation passes, but
   BEFORE marking the task as done.
3. If a reviewer reports P1 issues, they MUST be resolved and the reviewer
   re-run (or the fixes verified by the orchestrator with explicit sign-off).
4. P2 warnings MUST be explicitly addressed or acknowledged in the commit
   message or PR description.
5. **qa-reviewer is mandatory for every non-trivial code change.** Only skip
   for docs-only, comment-only, or formatting-only changes.
6. **Branch conflict check is mandatory before completion** — verify the
   working branch is clean against the base branch (see RULES.md#branch-conflict-check).

### Gate Checklist (include in final summary)
```text
[ ] spec-guardian:  triggered / not triggered
[ ] contract-reviewer: triggered / not triggered
[ ] qa-reviewer:      triggered / not triggered
[ ] branch-conflict:  clean / conflicts resolved
```

## Validation Defaults

Defined in `RULES.md#validation`.

## Hard Boundaries

- Do not put OpenClaw investment analysis logic into caifubao.
- Do not expose Mongo collection internals as public API contracts.
- Do not let OpenClaw mutate data, trigger jobs, run backfills, or receive admin
  access.
- Do not commit real credentials, kubeconfigs, private domains, registry
  settings, database dumps, or local environment files.
