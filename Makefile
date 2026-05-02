.PHONY: dev check test-backend test-frontend seed-score-demo

ENV_FILE ?= .env.local

dev:
	./scripts/dev.sh $(ENV_FILE)

check: test-backend test-frontend

test-backend:
	cd backend && ./venv312/bin/pytest app/test/test_portfolios_api.py app/test/test_score_experiments_api.py app/test/test_scores_api.py app/test/test_market_scores_api.py

test-frontend:
	cd frontend && npm run lint && npm run build

seed-score-demo:
	cd backend && ./venv312/bin/python ../scripts/seed_score_demo.py
