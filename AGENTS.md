# Caifubao Agent Guide

Caifubao is an A-share quantitative-investing MVP for research, learning, and
demonstration. It is not investment or trading advice.

## Sources of truth

- [`RULES.md`](./RULES.md): safety, module boundaries, spec gate, validation,
  reviews, and Git/PR policy.
- [`.project-rules.md`](./.project-rules.md): executable development, OpenSpec,
  Git, and PR commands.
- `openspec/archive/mvp-quant-demo/`: archived contract ledger.
- `skills/*/SKILL.md`: conditional domain guidance; load only when applicable.

Public application code and examples belong here. Secrets, private domains,
real deployment overlays, and operator runbooks belong in
`caifubao-private`.

## Workflow

For non-trivial work:

1. Define the outcome, affected modules, assumptions, write scope, and minimum
   validation.
2. Apply the [`RULES.md` spec gate](./RULES.md#spec-gate) before editing.
3. Use bounded implementers only when ownership is clear; keep parallel write
   scopes disjoint.
4. Implement the smallest sufficient change and validate until green.
5. Run the required read-only reviewers from
   [`RULES.md`](./RULES.md#review-gates).
6. Check conflicts against the target base and report results using the
   [task-note fields](./RULES.md#task-notes).
7. Create or update a PR only when requested or already part of the task's
   delivery scope.

The orchestrator owns integration and final decisions. Reviewers report
findings; they do not edit or decide merges.

## Agent roles

| Role                   | Scope                                                   |
| ---------------------- | ------------------------------------------------------- |
| `spec-guardian`        | Read-only OpenSpec gate decision                        |
| `backend-implementer`  | Flask APIs, auth, models, utilities, tests              |
| `datahub-implementer`  | Data production, scoring, freshness, runners, tests     |
| `frontend-implementer` | Vue views, components, clients, stores                  |
| `k8s-implementer`      | Public deployment examples and assigned workflows       |
| `contract-reviewer`    | Read-only contracts, freshness, auth, OpenClaw review   |
| `qa-reviewer`          | Read-only safety, regression, tests, repository hygiene |

Do not activate every role by default. Delegate only bounded work that advances
the task.

## Skill routing

Load `skills/caifubao-dev/SKILL.md` for all repository work, then only the
matching domain skill:

| Skill                  | Load when                                                                               |
| ---------------------- | --------------------------------------------------------------------------------------- |
| `scoring-factor`       | Adding or changing a scoring factor                                                     |
| `scoring-validation`   | Verifying replay, calibration, grid search, factor evaluation, or walk-forward behavior |
| `openclaw-integration` | OpenClaw contracts, service tokens, or integration endpoints                            |
| `datahub-data-quality` | Freshness, data quality, BSE exclusion, HFQ gaps, or deterministic bootstrap            |

## Local tooling

- Operations: `./scripts/caifubao` (see
  [`docs/operations/agent-cli.md`](./docs/operations/agent-cli.md)).
- Backend environment: `backend/venv312`.
- Datahub requires Python 3.12; use `datahub/.venv`, never the system Python.
- Rebuild datahub only when needed:

```bash
datahub/.tools/python312/bin/python3.12 -m venv datahub/.venv
datahub/.venv/bin/pip install -r datahub/requirements.txt pytest
```

Local environments under `.venv/`, `.tools/`, and `venv312/` are ignored and
must not be committed.
