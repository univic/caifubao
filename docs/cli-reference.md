# CLI Reference

All CLI runners live under `datahub/app/jobs/` and connect to MongoDB via the `MONGO_URI`
environment variable (default: `mongodb://localhost:27017/caifubao`).

Run from the repository root with the datahub virtual environment activated.

## Backtest Runner

`python -m app.jobs.backtest_runner <command> [options]`

### single — Single-stock backtest

```bash
# MA_CROSS on 贵州茅台
python -m app.jobs.backtest_runner single sh600519 MA_CROSS 2024-01-01 2024-06-30

# Score threshold strategy on 五粮液
python -m app.jobs.backtest_runner single sz000858 SCORE_THRESHOLD 2024-01-01 2024-12-31 \
  --horizon 20 --entry 75 --exit 45 --stop-loss -8

# Score momentum with custom benchmark
python -m app.jobs.backtest_runner single sh601318 SCORE_MOMENTUM 2024-01-01 2024-06-30 \
  --horizon 5 --score-delta 15 --benchmark-code sh000300

# Dry run (don't save to DB)
python -m app.jobs.backtest_runner single sh600519 BUY_HOLD 2024-01-01 2024-12-31 --no-save
```

### multi — Multi-stock portfolio backtest

```bash
# TOP_N_ROTATION with top 5 stocks, weekly rebalance
python -m app.jobs.backtest_runner multi sh600519,sz000858,sz000001,sh601318,sh600036 \
  TOP_N_ROTATION 2024-01-01 2024-12-31 --horizon 20 --top-n 5 --rebalance-interval 5

# Score-weighted allocation with position cap
python -m app.jobs.backtest_runner multi sh600519,sz000858,sz000001 TOP_N_ROTATION \
  2024-01-01 2024-06-30 --horizon 20 --allocation score_weighted --max-position-pct 0.30
```

### compare — Head-to-head strategy comparison

```bash
# Compare SCORE_THRESHOLD vs MA_CROSS on same stock
python -m app.jobs.backtest_runner compare sh600519 SCORE_THRESHOLD \
  2024-01-01 2024-12-31 --vs MA_CROSS --horizon 20 --entry 75

# Output:
# Winner: SCORE_THRESHOLD  (Δ return: +3.45%,  Δ Sharpe: +0.23)
```

## Scoring Runner (existing)

`python -m app.jobs.scoring_runner <command> [options]`

```bash
# Daily scoring
python -m app.jobs.scoring_runner run --date 2026-05-16

# Historical backfill
python -m app.jobs.scoring_runner backfill --from 2024-01-01 --to 2024-12-31

# Verify predictions
python -m app.jobs.scoring_runner verify --from 2024-01-01 --to 2024-06-30

# Calibration report
python -m app.jobs.scoring_runner report --horizon 20 --from 2024-01-01 --to 2024-12-31
```

## Technical Factor Runner

`python -m app.jobs.tech_factor_runner <command> [options]`

```bash
# List all 8 factors
python -m app.jobs.tech_factor_runner list

# Compute factors for one stock
python -m app.jobs.tech_factor_runner compute sh600519 2024-01-01 2024-12-31 --factors rsi_14,volume_ratio

# Compute all factors, save to JSON
python -m app.jobs.tech_factor_runner compute sh600519 2024-01-01 2024-12-31 --all --output factors.json

# Evaluate factor predictive power (over all stocks)
python -m app.jobs.tech_factor_runner evaluate rsi_14 2024-01-01 2024-12-31 --horizon 20 --save

# Evaluate on single stock
python -m app.jobs.tech_factor_runner evaluate bb_position 2024-01-01 2024-12-31 \
  --stock-code sh600519 --horizon 20
```

## OpenCode Closed-Loop Workflow

The typical closed-loop workflow for validating and fixing issues:

```bash
# 1. Run a backtest and capture output
python -m app.jobs.backtest_runner single sh600519 SCORE_THRESHOLD 2024-01-01 2024-12-31 \
  --horizon 20 --entry 75 2>&1 | tee /tmp/backtest_result.json

# 2. If there's an error, the JSON output contains {"error": {...}}
#    The LLM reads the error and proposes a fix

# 3. After fixing code, re-run the same command to verify
python -m app.jobs.backtest_runner single sh600519 SCORE_THRESHOLD 2024-01-01 2024-12-31 \
  --horizon 20 --entry 75 2>&1 | tee /tmp/backtest_result_v2.json

# 4. Compare strategies to validate improvement
python -m app.jobs.backtest_runner compare sh600519 SCORE_THRESHOLD \
  2024-01-01 2024-12-31 --vs MA_CROSS --horizon 20

# 5. Evaluate a new factor
python -m app.jobs.tech_factor_runner evaluate volume_ratio 2024-01-01 2024-12-31 --horizon 20 --save

# 6. Check calibration report for score quality
python -m app.jobs.scoring_runner report --horizon 20 --from 2024-01-01 --to 2024-12-31
```

## Environment

All runners respect these environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `MONGO_URI` | `mongodb://localhost:27017/caifubao` | MongoDB connection string |
| `APP_ENV` | - | Set to `test` for test environment |

## Prerequisites

- Python 3.12+
- mongoengine, pymongo installed
- MongoDB running and accessible
- Data in collections: `stock_daily_quotes`, `stock_factor_daily`, `stock_score_predictions`
