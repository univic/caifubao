.PHONY: help dev check test-backend test-frontend seed-score-demo
.PHONY: data-sync data-refresh-status data-status
.PHONY: score-one score-all score-verify score-report
.PHONY: system-health system-cron-list system-cron-trigger
.PHONY: lint lint-datahub lint-backend lint-frontend
.PHONY: docker-build docker-push

CFB     := ./scripts/caifubao
ENV_FILE ?= .env.local
STOCK    ?= sz000977
DATE     ?= $(shell python3 -c 'import datetime;print(datetime.date.today().isoformat())')
HORIZON  ?= 5
FROM     ?= $(shell python3 -c 'import datetime;print((datetime.date.today()-datetime.timedelta(days=7)).isoformat())')
TO       ?= $(shell python3 -c 'import datetime;print(datetime.date.today().isoformat())')

help: ## Show this help
	@echo "caifubao — Makefile targets"
	@echo ""
	@echo "  Data pipeline:"
	@echo "    make data-sync              Sync prod→dev"
	@echo "    make data-refresh-status    Refresh data_asset_status"
	@echo "    make data-status STOCK=sz000977"
	@echo ""
	@echo "  Scoring:"
	@echo "    make score-one STOCK=sz000977 DATE=2026-05-18"
	@echo "    make score-all DATE=2026-05-18"
	@echo "    make score-verify"
	@echo "    make score-report"
	@echo ""
	@echo "  System:"
	@echo "    make system-health          Full health check"
	@echo "    make system-cron-list       List CronJobs"
	@echo ""
	@echo "  Dev:"
	@echo "    make dev                    Start backend+frontend"
	@echo "    make lint                   Lint all modules"
	@echo "    make check                  Full CI check"

# ---- Data pipeline ----
data-sync: ## Sync data from prod to dev
	$(CFB) data sync $(DATE) quote,factor,signal,market,industry

data-refresh-status: ## Refresh data_asset_status freshness
	$(CFB) data refresh-status

data-status: ## Check data completeness (STOCK=<code>)
	$(CFB) data status $(STOCK)

# ---- Scoring ----
score-one: ## Score a single stock (STOCK= DATE= HORIZON=)
	$(CFB) score score-one $(STOCK) --date $(DATE) --horizon $(HORIZON) --replace

score-all: ## Score all stocks (DATE= HORIZON=)
	$(CFB) score score-all $(DATE) $(HORIZON)

score-verify: ## Verify pending predictions (FROM= TO= HORIZON=)
	$(CFB) score verify $(FROM) $(TO) $(HORIZON)

score-report: ## Generate calibration report (FROM= TO= HORIZON=)
	$(CFB) score report $(FROM) $(TO) $(HORIZON)

# ---- System ----
system-health: ## Full environment health check
	$(CFB) system health

system-cron-list: ## List CronJobs in dev
	$(CFB) system cron status

system-cron-trigger: ## Trigger a CronJob (JOB=<name>)
	$(CFB) system cron trigger $(JOB)

# ---- Dev ----
dev:
	./scripts/dev.sh $(ENV_FILE)

check: test-backend test-frontend

test-backend:
	cd backend && ./venv312/bin/pytest app/test/test_portfolios_api.py app/test/test_score_experiments_api.py app/test/test_scores_api.py app/test/test_market_scores_api.py

test-frontend:
	cd frontend && npm run lint && npm run build

seed-score-demo:
	cd backend && ./venv312/bin/python ../scripts/seed_score_demo.py

# ---- Lint ----
lint: lint-datahub lint-backend lint-frontend

lint-datahub:
	cd datahub && ruff check . && ruff format --check .

lint-backend:
	cd backend && ruff check . && ruff format --check .

lint-frontend:
	cd frontend && npm run lint

# ---- Docker (requires registry env) ----
docker-build:
	docker build -t caifubao-datahub:local ./datahub
	docker build -t caifubao-backend:local ./backend
	docker build -t caifubao-frontend:local ./frontend

docker-push:
	@echo "Use private repo deploy workflow for production pushes"
	@echo "  gh workflow run datahub-deploy.yml --repo univic/caifubao-private ..."
