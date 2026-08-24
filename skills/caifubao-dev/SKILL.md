---
name: caifubao-dev
description: Caifubao 仓库操作总纲——模块边界、Spec Gate、审查门禁、验证命令、分支/PR 纪律与领域 skill 索引。任何 caifubao 开发工作前先加载。
license: MIT
compatibility: opencode, dsh
metadata:
  audience: all
  project: caifubao
---

## What this skill covers

How to operate safely and effectively in the caifubao repository: module
boundaries, the spec gate, mandatory review gates, validation commands, branch
and PR discipline, and which domain skill to load for which work.

**Authority:** `RULES.md` is the single source of truth for rules (P1–P6).
`AGENTS.md` describes workflow. This skill is a quick reference — when in
doubt, read `RULES.md`.

## Module boundaries (P2, must not violate)

| Module | Owns | Must not |
|--------|------|----------|
| `datahub/` | Market data, factors, signals, scoring, freshness, data quality | Render frontend UI, expose user-facing APIs |
| `backend/` | Flask APIs, auth, service-token checks, light aggregation | Run scheduled data collection or backfill jobs |
| `frontend/` | Vue UI, API consumption, rendering | Depend on Mongo collection shapes, bypass backend APIs |
| `k8s/` | Public example deployment assets | Contain real secrets, private domains, registry settings |
| OpenClaw | Downstream read-only consumer of backend APIs | Receive Mongo credentials, trigger mutations/scheduling/backfills/admin |

API responses are the external contract. Mongo collection shape is not.

## Spec Gate (P3)

Spec update is **required** when any of these change: public API endpoints /
response fields / pagination / error shapes; auth / authorization / token
scope / audit; freshness semantics (`data_as_of`, timestamps, status states);
scoring / factors / signals / replay / calibration / look-ahead-bias rules;
data ownership between modules; public docs relied on by external consumers.

Not required for: internal refactors, tests, formatting, small
behavior-preserving fixes. The MVP OpenSpec change is archived at
`openspec/archive/mvp-quant-demo/` (contract ledger). Create a new change
under `openspec/changes/` only when the spec gate triggers.

## Validation (P5 — run smallest useful check, loop until pass)

| Change type | Command |
|-------------|---------|
| Python (backend or datahub) | `ruff check` + `ruff format --check` + smallest relevant pytest |
| Backend API | Focused pytest under `backend/app/test/` |
| Datahub | Focused datahub tests or runner dry-run |
| Frontend | `cd frontend && npm run lint && npm run build` |
| K8s examples | `kubectl kustomize` or equivalent render check |
| OpenSpec paths | `openspec validate --all --strict` (CI gate) |

If a check cannot be run, say exactly why.

## Review Gates (mandatory for non-trivial changes)

Non-trivial = more than ~10 lines / >1 file / touches contracts, auth,
scoring, data models, CI, or k8s manifests. When in doubt, treat as
non-trivial.

- `spec-guardian` — triggered when spec gate may apply (endpoints, auth,
  scoring semantics, boundary shift).
- `contract-reviewer` — triggered when API contract, auth, freshness metadata,
  or OpenClaw integration is touched.
- `qa-reviewer` — **every** non-trivial code change (skip only docs/comments/
  formatting-only).
- Reviews run AFTER implementation + validation, BEFORE done. P1 issues must be
  resolved and re-reviewed.

Close every non-trivial task with the Gate Checklist:

```text
[ ] spec-guardian:   triggered / not triggered
[ ] contract-reviewer: triggered / not triggered
[ ] qa-reviewer:      triggered / not triggered
[ ] branch-conflict:  clean / conflicts resolved
[ ] draft-pr-ci:      created as draft / CI passed / converted to regular
```

## Branch & PR discipline

- Every non-trivial task gets a dedicated branch from `develop`; never edit on
  `develop`/`main` or another task's branch.
- Always open PRs as **Draft** (`gh pr create --draft --base develop`); wait
  for all CI checks to pass; only then `gh pr ready <N>`.
- Branch conflict check before done:
  `git fetch origin develop && git merge-tree $(git merge-base HEAD origin/develop) origin/develop HEAD` (must show no conflict markers).
- Conventional commits: `feat:`, `fix:`, `docs:`, `style:`, `refactor:`, `test:`, `chore:`.

## Safety (P1)

Never commit credentials, tokens, kubeconfigs, database dumps, local env
files, private domains, registry settings, or private runbooks. Use
`.env.example` placeholders. Real deployment overlays live in the private
`caifubao-private` repository.

## Skill index (load before domain work)

| Skill | When to load |
|:---|:---|
| `scoring-factor` | Adding/changing a scoring engine factor |
| `scoring-validation` | Validating scoring/backtest changes (verification, replay, calibration, grid search, factor eval, walk-forward) |
| `openclaw-integration` | Any OpenClaw contract, service-token, or `/api/v1/integrations/openclaw` work |
| `datahub-data-quality` | Freshness, data quality, BSE exclusion, HFQ gaps, deterministic bootstrap |

## Operations

Use the unified CLI `./scripts/caifubao` for dev environment operations
(health, data sync, scoring, backup/restore). Full reference:
`docs/operations/agent-cli.md`.

## Self-check

- [ ] Did I state assumptions and confirm module boundaries before editing?
- [ ] Is the spec gate decision explicit (required / not required)?
- [ ] Did I run the smallest validation and loop until it passed?
- [ ] Are the correct reviewers scheduled and their outcomes recorded?
- [ ] Is the branch isolated, conflict-free, and the PR opened as draft with CI green?
