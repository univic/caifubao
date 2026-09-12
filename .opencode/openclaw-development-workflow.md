# OpenClaw Development Workflow

This file adds OpenClaw-specific routing to the repository workflow; it is not
the runtime integration contract. General rules and steps live in `RULES.md`
and `AGENTS.md` and are not repeated here.

## Required context

- `docs/integrations/openclaw.md`
- `skills/openclaw-integration/SKILL.md`
- `openspec/archive/mvp-quant-demo/specs/openclaw-data-access/spec.md`
- Matching active change under `openspec/changes/`, if any

## Routing

- `spec-guardian`: decide whether endpoint, auth, freshness, audit, or contract
  changes require a new OpenSpec change.
- `backend-implementer`: bounded service-token and integration API work.
- `datahub-implementer`: bounded production of data exposed by the API.
- `contract-reviewer`: required for any OpenClaw integration change.
- `qa-reviewer`: required for non-trivial runtime, CI, model, or deployment
  changes.

Give every implementer an explicit write scope and avoid overlapping files.

## Invariants

The authoritative invariants are in `RULES.md#openclaw`. In particular,
OpenClaw is a read-only downstream consumer: it receives no MongoDB access and
cannot trigger mutations, schedules, backfills, or admin actions. Investment
analysis stays outside caifubao.
