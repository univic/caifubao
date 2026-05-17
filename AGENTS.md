# AGENTS.md

This file is the canonical operating guide for AI-assisted Caifubao development.
It is loaded by OpenClaw when the caifubao-dev agent works in this repository.

**Rule authority:** All safety, boundary, discipline, validation, spec-gate, and
OpenClaw-specific rules are defined once in `RULES.md`. This file describes
workflow and process; `RULES.md` defines the rules agents must follow.

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

Defined in `RULES.md#module-boundaries`. Every agent must respect these
boundaries exactly.

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

Surgical change rules are defined in `RULES.md#surgical-discipline`. In
summary: do not "improve" adjacent code, do not refactor unbroken things,
match existing style, and clean up only your own orphans.

## Spec Gate

Spec gate triggers are defined in `RULES.md#spec-gate`. Run the Spec Gate
before code changes when work affects endpoints, auth, freshness, scoring,
data ownership, or public docs.

## Karpathy Code Discipline

Four principles that apply to ALL agents (orchestrator, implementer, reviewer).
These address common LLM coding pitfalls identified by Andrej Karpathy.
Full details: [`.opencode/skills/karpathy-discipline/SKILL.md`](.opencode/skills/karpathy-discipline/SKILL.md).

Rules are summarized in `RULES.md#surgical-discipline`.

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

Defined in `RULES.md#validation`.

**Goal-driven loop:** Do not stop at "looks right." Run the verification. If it
fails, fix the issue and run it again. Report the final passing state with the
command output or summary. For bugs, write a test that reproduces the bug first,
confirm it fails, then implement the fix and confirm the test passes.

## Public Repository Safety

Defined in `RULES.md#safety`. Follow them strictly. Prefer small changes that
improve the demo loop: data update, API response, frontend display, and local
validation. Do not invent new architecture when existing local patterns are
sufficient.
