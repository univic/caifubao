# AGENTS.md

This public-facing guide describes the module boundaries for Caifubao.

## Project Scope

Caifubao is an A-share quantitative investing MVP for research, learning, and
demonstration. It is not investment advice, trading advice, or a financial
service.

## Module Boundaries

- `datahub/` collects and stores market data.
- `backend/` exposes user-facing APIs and lightweight business aggregation.
- `frontend/` consumes backend APIs and renders the MVP user experience.
- `k8s/` contains example deployment assets only.
- `.github/workflows/ci.yml` contains the public CI baseline.

## Public Repository Rules

- Do not commit real credentials, tokens, kubeconfigs, database dumps, or local
  environment files.
- Use `.env.example` files for placeholders.
- Keep real deployment overlays, registry settings, private domains, and
  operator runbooks outside the public repository.
- Prefer small changes that improve the demo loop: data update, API response,
  frontend display, and local validation.

## Validation Expectations

- Python changes should run the relevant `ruff check`, `ruff format --check`,
  and the smallest useful test.
- Frontend changes should run the relevant lint/build checks.
- Deployment example changes should render with `kubectl kustomize` or an
  equivalent local check.

