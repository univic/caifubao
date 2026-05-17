# AGENTS.md

This file is the canonical operating guide for AI-assisted Caifubao development.
It is loaded by OpenClaw when the caifubao-dev agent works in this repository.

For Git workflow rules (branching, draft PR, CI gating), see
[`.project-rules.md`](./.project-rules.md).

## Project Scope

Caifubao is an A-share quantitative investing MVP for research, learning, and
demonstration. It is not investment advice, trading advice, or a financial
service.

## Repository Roles

- Public caifubao repository: application source, public docs, tests,
  OpenSpec artifacts, public CI, and example deployment assets.
- Private caifubao-private repository: real deployment overlays, secrets,
  private domains, operator runbooks, and private workflow material.

Do not move private deployment material into this public repository.

## Module Boundaries

- datahub/ collects, derives, validates, and stores market data, factors,
  signals, scoring outputs, freshness, and data quality records.
- backend/ exposes Flask APIs, authentication, service-token checks, and
  lightweight aggregation. It must not run data collection jobs.
- frontend/ consumes backend APIs and renders the MVP user experience. It must
  not depend on Mongo collection shapes or bypass backend APIs.
- k8s/ contains public example deployment assets only.
- openspec/ contains behavior, contract, and workflow specifications.
- OpenClaw is a downstream read-only consumer through backend APIs. It must not
  receive Mongo credentials, mutation endpoints, scheduler triggers, backfill
  controls, or admin access.

## OpenClaw Development Workflow

Use caifubao-dev as the primary OpenClaw development agent for this repo. It
should act as a lightweight technical lead, not as a waterfall team.

For non-trivial work, keep these notes in working context:

```text
Outcome:
Module Impact:
Spec Gate: required / not required
Assumptions:
Write Scope:
Validation Plan:
Reviewer Requests:
```

Default sequence:

1. Understand the requested outcome and affected modules.
2. State assumptions explicitly. If something is ambiguous, ask before coding.
3. Decide whether the Spec Gate is required before editing.
4. Assign or perform only explicitly scoped implementation work.
5. Keep write scopes disjoint when using subagents.
6. Review contract and behavioral risk before finishing non-trivial changes.
7. Run the smallest useful validation and loop until it passes.
8. Report changed files, checks run, and remaining risk.

Use subagents sparingly. Prefer these roles over the older generic
Architect/Developer/QA/Scribe model:

- spec-guardian: read-only decision on whether OpenSpec must change.
- backend-implementer: bounded backend/API/auth/model/test changes.
- datahub-implementer: bounded data production, scoring, freshness, runner,
  model, and test changes.
- frontend-implementer: bounded Vue/API client/store/view/component changes.
- k8s-implementer: bounded public deployment example and workflow changes.
- contract-reviewer: read-only API, freshness, auth, OpenClaw compatibility,
  and module-boundary review.
- qa-reviewer: read-only safety, regression, test, and repository hygiene
  review.

Do not activate every role by default. Use implementers only when their write
scope is clear. Reviewers report findings; they do not own merge decisions.

## Surgical Changes

When editing existing code, follow these rules derived from Andrej Karpathy's
observations on LLM coding pitfalls:

- **Don't "improve" adjacent code, comments, or formatting.** If a file has
  mixed styles and your task is to add a function, match the surrounding style.
  Do not reformat the whole file.
- **Don't refactor things that aren't broken.** Even if you'd write it
  differently, leave working code alone.
- **Match existing style.** The codebase uses single quotes? Use single quotes.
- **Clean up only your own mess.** Remove imports, variables, or functions that
  YOUR changes made unused. Do not delete pre-existing dead code unless asked.
- **Every changed line must trace directly to the request.** If you cannot say
  "this line is necessary because...", it does not belong in the diff.

For full Karpathy discipline guidelines (Think Before Coding, Simplicity First,
Surgical Changes, Goal-Driven Execution), see
[`.opencode/skills/karpathy-discipline/SKILL.md`](.opencode/skills/karpathy-discipline/SKILL.md).

## Spec Gate

Run the Spec Gate before code changes when work affects:

- API endpoints, response fields, pagination, filtering, or error shapes.
- Authentication, authorization, service-token scope, token lifecycle, or audit.
- Freshness semantics, data_as_of, generated timestamps, or status states.
- Scoring, factors, signals, replay, calibration, or look-ahead-bias rules.
- Data ownership between datahub, backend, frontend, k8s, and OpenClaw.
- Public docs used by downstream consumers.

Internal refactors, tests, formatting, or behavior-preserving fixes do not need
an OpenSpec update.

Relevant context usually starts with:

- DESIGN.md
- openspec/config.yaml
- openspec/changes/mvp-quant-demo/design.md
- openspec/changes/mvp-quant-demo/tasks.md
- Matching specs under openspec/changes/mvp-quant-demo/specs/
- docs/integrations/openclaw.md for OpenClaw-related work

## Karpathy Code Discipline

Four principles that apply to ALL agents (orchestrator, implementer, reviewer).
These address common LLM coding pitfalls identified by Andrej Karpathy.
Full details: [`.opencode/skills/karpathy-discipline/SKILL.md`](.opencode/skills/karpathy-discipline/SKILL.md).

### Think Before Coding

- State assumptions explicitly before writing code.
- If the request is ambiguous, present multiple interpretations and ask.
- If a simpler approach exists, say so. Push back when warranted.
- If you don't understand something, stop and name what's unclear.

### Simplicity First

- No features beyond what was asked. No speculative code.
- No abstractions for single-use code (no class hierarchy for one function).
- No "flexibility" or "configurability" that wasn't requested.
- If 200 lines could be 50, rewrite it.

### Surgical Changes

(Detailed in the section above.)

### Goal-Driven Execution

- Define a verifiable success criterion before writing code.
- Transform "fix the bug" into "write a failing test, then make it pass."
- For multi-step tasks, state a plan with verification checkpoints.
- Loop until verification passes. Don't stop at "looks right."

## Validation Expectations

- Python changes: run the relevant ruff check, ruff format --check, and the
  smallest useful pytest target.
- Backend API changes: run focused tests under backend/app/test/ when present.
- Datahub changes: run focused datahub tests or runner dry-runs where available.
- Frontend changes: run relevant lint/build checks.
- Deployment example changes: render with kubectl kustomize or an equivalent
  render check.

If a check cannot be run, say exactly why.

**Goal-driven loop:** Do not stop at "looks right." Run the verification. If it
fails, fix the issue and run it again. Report the final passing state with the
command output or summary. For bugs, write a test that reproduces the bug first,
confirm it fails, then implement the fix and confirm the test passes.

## Public Repository Safety

- Do not commit real credentials, tokens, kubeconfigs, database dumps, local
  environment files, private domains, private registry settings, or private
  runbooks.
- Use .env.example files for placeholders.
- Keep real deployment overlays and operator scripts in the private repository.
- Prefer small changes that improve the demo loop: data update, API response,
  frontend display, and local validation.
- Do not invent new architecture when existing local patterns are sufficient.
