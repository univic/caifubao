---
name: caifubao-dev
description: Operate safely in the Caifubao repository, including rule authority, module routing, spec/review gates, validation, and conditional domain-skill selection. Load for any Caifubao repository work.
license: MIT
compatibility: opencode, dsh
metadata:
  audience: all
  project: caifubao
---

# Caifubao development entrypoint

Read these files before acting:

1. `RULES.md` — authoritative safety, boundaries, gates, validation, and Git/PR
   policy.
2. `AGENTS.md` — workflow, roles, and domain-skill routing.
3. `.project-rules.md` — only when commands for local development, OpenSpec,
   Git, PRs, or deployment validation are needed.

Do not copy rules from this skill into plans or role prompts. Link to the
authoritative section and add only task-specific assumptions.

## Start each task

- Define outcome, affected modules, write scope, and minimum validation.
- Apply the `RULES.md` spec gate before editing.
- Use a dedicated branch for repository changes; do not mix unrelated work.
- Load only the domain skill that matches the task:

| Skill                  | Trigger                                                                                |
| ---------------------- | -------------------------------------------------------------------------------------- |
| `scoring-factor`       | Add or change a scoring factor                                                         |
| `scoring-validation`   | Validate replay, calibration, grid search, factor evaluation, or walk-forward behavior |
| `openclaw-integration` | OpenClaw contracts, service tokens, or integration endpoints                           |
| `datahub-data-quality` | Freshness, data quality, BSE exclusion, HFQ gaps, or deterministic bootstrap           |

## Finish each task

- Run the minimum checks selected by `RULES.md#validation` until they pass.
- Run only the reviewers required by `RULES.md#review-gates`.
- Check branch conflicts for repository changes.
- Report the `RULES.md#task-notes` fields, including whether PR delivery was
  requested. Do not infer permission to push or change PR state.
