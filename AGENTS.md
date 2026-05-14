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

## Branching Rules

- **New branches must be based on `main` or `develop`**, never on feature
  branches or other non-trunk branches.
- Before creating a branch, ensure you are on an up-to-date trunk:
  ```bash
  git checkout develop && git pull origin develop
  git checkout -b feature/your-branch-name
  ```

## Validation Expectations

- Python changes should run the relevant `ruff check`, `ruff format --check`,
  and the smallest useful test.
- Frontend changes should run the relevant lint/build checks.
- Deployment example changes should render with `kubectl kustomize` or an
  equivalent local check.

## Development Environment Rules

- **Always use a virtual environment** for Python development. Never install
  packages directly into the system Python (`pip install` without a venv, or
  with `--break-system-packages`).
- **Default venv**: The project virtual environment is **`.venv/` at the
  repository root**. All Python commands (`python`, `pip`, `pytest`, `ruff`)
  must use this venv.
- The project targets **Python 3.12**. If `.venv/` does not exist, create it
  before doing anything else:
  ```bash
  # First, ensure Python 3.12 is available (via pyenv or system)
  pyenv install 3.12.12      # if not already installed
  /path/to/python3.12 -m venv .venv
  source .venv/bin/activate
  pip install -r backend/requirements.txt
  ```
- If `pyenv` is not available, use any Python 3.12 interpreter (not 3.14 or
  higher — some pinned dependencies do not support 3.14+).
- If `.venv/` already exists, activate it with `source .venv/bin/activate`
  before any Python work.

