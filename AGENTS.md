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

The orchestrator is the single final owner. Other agents may implement bounded
slices or review, but they do not own merge decisions.

For non-trivial work, follow this sequence:

1. Understand the requested outcome and affected modules.
2. State assumptions explicitly. If something is ambiguous, ask before coding.
3. Decide whether the Spec Gate is required before editing.
4. Assign exactly scoped implementation work — keep write scopes disjoint.
5. Implement the smallest change that satisfies the request.
6. Run the smallest useful validation and loop until it passes.
7. Run mandatory reviewers (contract-reviewer when contracts touched, qa-reviewer for ALL non-trivial code changes).
8. Run branch conflict check against the target base branch.
9. Report changed files, checks run, review outcomes, and remaining risk. Close with the Gate Checklist.

Keep these notes in working context:

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
```

### Agent Roles

Use these roles over the older generic Architect/Developer/QA/Scribe model:

| Role | Type | Scope |
|:---|:---|:---|
| `spec-guardian` | read-only | OpenSpec gate decision |
| `backend-implementer` | write | Flask API, auth, models, utilities, tests |
| `datahub-implementer` | write | data production, scoring, freshness, runners, tests |
| `frontend-implementer` | write | Vue views, components, API client, Pinia stores |
| `k8s-implementer` | write | public deployment examples, workflow changes |
| `contract-reviewer` | read-only | API contracts, freshness, auth, OpenClaw compatibility |
| `qa-reviewer` | read-only | safety, regression, test, repository hygiene |

Do not activate every role by default. Use implementers only when their write
scope is clear. Reviewers report findings; they do not own merge decisions.

### Review Gates (Mandatory)

Every non-trivial change must go through the appropriate reviewer(s) before
being considered complete. Reviewers run AFTER implementation and validation
pass. If a reviewer reports P1 issues, they must be resolved and re-reviewed.

```text
[ ] spec-guardian:   triggered / not triggered
[ ] contract-reviewer: triggered / not triggered
[ ] qa-reviewer:      triggered / not triggered
[ ] branch-conflict:  clean / conflicts resolved
```

## Spec Gate

Spec gate triggers are defined in `RULES.md#spec-gate`. Run the Spec Gate
before code changes when work affects endpoints, auth, freshness, scoring,
data ownership, or public docs.

## Karpathy Code Discipline

Four principles that apply to ALL agents. These address common LLM coding
pitfalls identified by Andrej Karpathy. Rules are defined in
`RULES.md#surgical-discipline`. The standalone skill was folded into `RULES.md`
— that file is the single authority; do not re-introduce a separate skill.

In summary:

- **Think Before Coding**: State assumptions. If something is ambiguous, ask.
  If a simpler approach exists, say so.
- **Simplicity First**: No features beyond what was asked. No abstractions for
  single-use code. If 200 lines could be 50, rewrite it.
- **Surgical Changes**: Do not "improve" adjacent code, do not refactor
  unbroken things, match existing style, clean up only your own orphans.
- **Goal-Driven Execution**: Define a verifiable success criterion. Transform
  "fix the bug" into "write a failing test, then make it pass." Loop until
  verification passes.

## Skills (load before domain work)

Repository skills live in `skills/<name>/SKILL.md` and are loaded via
`opencode.json` or the table below. Load the matching skill before starting
domain work:

| Skill | When to load |
|:---|:---|
| `caifubao-dev` | Always — operating model, gates, validation, conventions |
| `scoring-factor` | Adding/changing a scoring engine factor |
| `scoring-validation` | Validating scoring/backtest changes (verification, replay, calibration, grid search, factor eval, walk-forward) |
| `openclaw-integration` | Any OpenClaw contract, service-token, or `/api/v1/integrations/openclaw` work |
| `datahub-data-quality` | Freshness, data quality, BSE exclusion, HFQ gaps, deterministic bootstrap |

## OpenSpec Status

The MVP change is archived as the contract ledger:
`openspec/archive/mvp-quant-demo/` (specs, design, tasks). CI still runs
`openspec validate --all --strict` on OpenSpec path changes. Create a new
change under `openspec/changes/` only when a task changes API contracts, auth,
scoring semantics, data ownership, or public docs (see `RULES.md#spec-gate`).

## Validation Expectations

Defined in `RULES.md#validation`.

**Goal-driven loop:** Do not stop at "looks right." Run the verification. If it
fails, fix the issue and run it again. For bugs, write a test that reproduces
the bug first, confirm it fails, then implement the fix and confirm the test
passes.

## Public Repository Safety

Defined in `RULES.md#safety`. Follow them strictly. Do not commit credentials,
tokens, kubeconfigs, database dumps, or local env files. Prefer small changes
that improve the demo loop. Do not invent new architecture when existing local
patterns are sufficient.

## Operations

For dev environment operations (data sync, scoring, health checks), use the
unified CLI: `./scripts/caifubao`. See `docs/operations/agent-cli.md` for
the full command reference.

## Local Python Environment (datahub)

The datahub module requires **Python 3.12** (code uses `datetime.UTC` and
other 3.11+ features). A ready venv lives at `datahub/.venv` — always use it
for local datahub work:

```bash
datahub/.venv/bin/python -m pytest datahub/app/test/     # full test suite
datahub/.venv/bin/ruff check datahub/ && datahub/.venv/bin/ruff format --check datahub/
```

- Interpreter source: `datahub/.tools/python312/` (uv-managed CPython 3.12).
  Rebuild the venv with:
  `datahub/.tools/python312/bin/python3.12 -m venv datahub/.venv &&
   datahub/.venv/bin/pip install -r datahub/requirements.txt pytest`
- Do **not** use the brew Python 3.14 (broken pyexpat — pip/venv unusable) or
  Python 3.10 (`datetime.UTC` missing; pandas-ta 0.4.71b0 requires >=3.12).
- `.venv/` and `.tools/` are git-ignored; never commit them.
