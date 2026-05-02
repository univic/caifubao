# Multi-horizon Stock Scoring & Closed-loop Tracking

## 1. Overview

The stock scoring system evaluates A-share targets every trading day and produces three horizon-specific opportunity scores:

- `Score5`: expected short-term profit opportunity within the next 5 trading days.
- `Score20`: expected swing opportunity within the next 20 trading days.
- `Score60`: expected medium-term opportunity within the next 60 trading days.

The scoring system is not yet a production dependency, so the next implementation should treat the existing single-score `StockDailyScore` shape as a disposable MVP draft rather than a compatibility constraint. The target design is one explicit prediction record per stock, date, and horizon, with structured explanation data and closed-loop verification stored alongside the prediction.

The goal is not to guarantee returns. The goal is to make every recommendation traceable:

1. What was predicted.
2. Which horizon the prediction applies to.
3. Which quote, factor, signal, and freshness inputs were used.
4. Which components contributed to the score.
5. What happened after 5, 20, or 60 trading days.

## 2. Ownership and Boundaries

- `datahub` owns score generation, verification, backfill, and summary metrics.
- `backend` provides read-only APIs for latest scores, historical predictions, explanations, and closed-loop performance.
- `frontend` displays score rankings, explanations, tracking status, and verified outcomes.
- OpenClaw and other downstream consumers must read scores through backend APIs, not Mongo collections.
- Scoring must not trigger quote collection or factor generation directly. Missing inputs should produce explicit blocked/insufficient status.

## 3. Data Model

### 3.1 Prediction Record

Replace the single T+5-oriented score document with a horizon-oriented prediction record.

Collection/model name recommendation:

- `StockScorePrediction`
- Mongo collection: `stock_score_predictions`

Required fields:

- `stock_code`: String, e.g. `sh600519`.
- `stock_name`: String.
- `date`: DateTime, normalized evaluation trading day.
- `horizon`: Integer, one of `5`, `20`, `60`.
- `score`: Float, normalized 0-100.
- `rank`: Integer, rank within the scored universe for the same date and horizon.
- `percentile`: Float, optional percentile within the same date and horizon.
- `recommendation`: String, one of `BUY`, `WATCH`, `AVOID`, `NONE`.
- `base_price`: Float, close price on the evaluation date.
- `target_date`: DateTime, the N-th trading day after `date`.
- `status`: String, one of `PENDING`, `TRACKING`, `VERIFIED`, `INSUFFICIENT_DATA`, `BLOCKED`, `FAILED`.
- `explanation`: Dict, structured score explanation snapshot.
- `verification`: Dict, closed-loop tracking and result metrics.
- `input_snapshot`: Dict, data freshness and input availability snapshot.
- `model_version`: String, e.g. `score_v2_202604`.
- `generated_at`: DateTime.
- `updated_at`: DateTime.

Indexes:

- Unique compound index on `(stock_code, date, horizon, model_version)`.
- Query index on `(date, horizon, -score)`.
- Query index on `(stock_code, -date, horizon)`.
- Query index on `(status, target_date, horizon)`.

### 3.2 Transitional Compatibility

Because the current scoring feature is not yet relied on by core user workflows, the implementation may remove or stop writing the old T+5-specific fields:

- `score`
- `target_date`
- `actual_price_t5`
- `max_price_in_5d`
- `profit_percentage_t5`
- `max_profit_percentage`
- `is_effective`

If the old `StockDailyScore` model remains temporarily, it should be treated as a read-only legacy adapter or removed once backend/frontend have been moved to `StockScorePrediction`.

## 4. Explanation Contract

Every prediction must include a structured `explanation` object. Natural-language summaries are allowed, but machine-readable components are required.

Required shape:

```json
{
  "summary": "Short-term momentum is strong and price is above MA20.",
  "horizon": 5,
  "score": 76.5,
  "components": [
    {
      "id": "ma10_cross_ma20",
      "group": "signal",
      "label": "MA10 crosses above MA20",
      "raw_value": true,
      "normalized_value": 1.0,
      "weight": 30.0,
      "contribution": 30.0,
      "direction": "positive",
      "evidence": {
        "ma_10": 10.31,
        "ma_20": 10.12
      }
    }
  ],
  "penalties": [
    {
      "id": "high_volatility",
      "group": "risk",
      "label": "Short-term volatility is elevated",
      "raw_value": 0.082,
      "contribution": -5.0,
      "direction": "negative"
    }
  ],
  "thresholds": {
    "buy": 70.0,
    "watch": 50.0,
    "effective_return": 0.02
  }
}
```

Rules:

- Each component must include `id`, `group`, `weight`, `contribution`, and `evidence` when evidence exists.
- Components must be additive and auditable: final score should be reproducible from the saved components and penalties.
- `model_version` must change when weights, thresholds, or component definitions change materially.
- `input_snapshot` must include quote/factor/signal dates and freshness status so historical explanations can be reproduced.

## 5. Scoring Logic

### 5.1 Horizon Meaning

The three scores are separate predictions, not aliases for the same score.

`Score5` emphasizes:

- bullish technical signals such as MA crosses and short breakouts;
- 3-5 trading day momentum;
- short-term price/volume confirmation;
- near-term volatility and limit-up/limit-down risk penalties.

`Score20` emphasizes:

- trend continuation and MA20/MA60 structure;
- pullback quality and rebound potential;
- relative strength versus index or scored universe;
- market breadth and data freshness;
- medium volatility risk penalties.

`Score60` emphasizes:

- medium-term trend quality;
- price position versus MA60/MA120;
- stability and drawdown behavior;
- relative strength persistence;
- basic fundamental or valuation placeholders when reliable inputs become available.

### 5.2 Initial Component Set

The first implementation may stay rule-based but must be parameterized by horizon.

Minimum reusable inputs:

- quotes: close, high, low, volume, turnover when available;
- factors: `ma_10`, `ma_20`, `ma_30`, `ma_60`, `ma_120`;
- signals: `StockSignalDaily` records such as `MA10_CROSS_MA20`;
- freshness: quote/factor/signal availability and dates;
- trading calendar: target dates for 5, 20, and 60 trading days.

Minimum components:

- `signal_strength`: bullish signal contribution.
- `trend_alignment`: price and moving-average structure.
- `momentum`: recent return over horizon-appropriate lookback.
- `breakout_or_position`: recent high breakout or position in range.
- `relative_strength`: stock performance versus universe/index when available.
- `risk_penalty`: volatility, missing data, abnormal trade status, and excessive short-term move.

### 5.3 Configuration

Weights and thresholds should be configuration-driven instead of hard-coded inside the scoring loop.

Minimum config per horizon:

- `effective_threshold`
- `buy_threshold`
- `watch_threshold`
- component weights
- risk penalty caps
- minimum required quote count

Example:

```python
SCORING_CONFIG = {
    5: {
        "effective_threshold": 0.02,
        "buy_threshold": 70,
        "watch_threshold": 50,
        "weights": {
            "signal_strength": 30,
            "momentum": 25,
            "trend_alignment": 20,
            "breakout_or_position": 15,
            "risk_penalty": 10,
        },
    },
    20: {
        "effective_threshold": 0.05,
        "buy_threshold": 70,
        "watch_threshold": 50,
        "weights": {
            "trend_alignment": 30,
            "pullback_quality": 20,
            "relative_strength": 20,
            "momentum": 15,
            "risk_penalty": 15,
        },
    },
    60: {
        "effective_threshold": 0.08,
        "buy_threshold": 70,
        "watch_threshold": 50,
        "weights": {
            "medium_trend": 35,
            "stability": 20,
            "relative_strength": 20,
            "fundamental_placeholder": 10,
            "risk_penalty": 15,
        },
    },
}
```

## 6. Closed-loop Tracking

### 6.1 Tracking Metrics

Each prediction's `verification` object must track:

- `status`: `PENDING`, `TRACKING`, `VERIFIED`, `INSUFFICIENT_DATA`, or `FAILED`.
- `target_date`: horizon target trading day.
- `verified_quote_count`: number of future trading quotes found.
- `expected_quote_count`: horizon value.
- `current_price`: latest available close during tracking.
- `actual_price`: close price on target date when verified.
- `max_price`: max close or high in the evaluation window.
- `min_price`: min close or low in the evaluation window.
- `return_at_target`: `(actual_price - base_price) / base_price`.
- `max_return`: `(max_price - base_price) / base_price`.
- `min_return`: `(min_price - base_price) / base_price`.
- `max_drawdown`: worst drawdown within the tracking window.
- `days_to_max_return`: trading days from prediction date to max return.
- `hit_target`: whether `max_return >= effective_threshold`.
- `hit_stop_loss`: whether risk threshold was breached.
- `verified_at`: verification timestamp.

### 6.2 Status Rules

- `PENDING`: no future quote is available yet.
- `TRACKING`: at least one future quote is available but fewer than `horizon` trading quotes exist.
- `VERIFIED`: the target horizon is reached and required metrics are calculated.
- `INSUFFICIENT_DATA`: target date has passed but required quote data is incomplete.
- `BLOCKED`: prediction could not be generated because required inputs were unavailable.
- `FAILED`: scoring or verification raised an unexpected error.

### 6.3 Verification Job

The verification job must run daily after quote updates. It should:

1. Find all predictions with `status in [PENDING, TRACKING]`.
2. Refresh partial tracking metrics whenever future quotes exist.
3. Mark records `VERIFIED` once the horizon quote window is complete.
4. Mark records `INSUFFICIENT_DATA` when the target date has passed but the quote window is incomplete.
5. Preserve previous explanation and input snapshots unchanged.

## 7. Datahub Workflow

Daily order:

1. quote update;
2. factor update;
3. signal update;
4. score generation for horizons `5`, `20`, `60`;
5. score verification/tracking update;
6. score summary metrics update.

Manual commands should support:

- scoring one date for all horizons;
- scoring one date for one horizon;
- backfilling a date range;
- verifying all pending/tracking predictions;
- dry-run output without Mongo writes.

## 8. Historical Replay and Calibration

Historical replay is a first-class scoring capability, not a temporary script. The scoring system must be able to use historical quotes, factors, and signals to regenerate what the system would have predicted on each evaluation date, then compare those predictions with actual future outcomes.

This capability is different from a full trading backtest. It measures whether scores have predictive separation before the product invests in complex portfolio simulation.

### 8.1 Implementation Approach

The first implementation should use a project-owned lightweight replay and calibration engine:

- Python service code inside `datahub/app/lib/scoring_engine`.
- MongoEngine queries against existing quote, factor, signal, and prediction models.
- Pandas may be used for tabular aggregation and report generation.
- No external trading backtest framework is required for scoring replay.

External backtest frameworks such as `backtrader`, `vectorbt`, `zipline`, or `rqalpha` should not be introduced for scoring calibration unless the project moves into portfolio-level trading simulation with explicit execution rules.

Recommended module layout:

```text
datahub/app/lib/scoring_engine/
  config.py
  components.py
  scoring_service.py
  replay_service.py
  verification_service.py
  calibration_report.py
```

### 8.2 Scoring Backfill

Scoring backfill generates historical `StockScorePrediction` records.

Rules:

- For an evaluation date `T`, scoring must only read data available at or before `T`.
- Backfill must not trigger quote, factor, or signal collection.
- Backfill must write `BLOCKED` records or explicit skipped summaries when required inputs are missing.
- Backfill must support one horizon, all horizons, one stock, all active stocks, date ranges, model-version filters, dry-run, and replace/no-replace modes.
- Default behavior should not overwrite existing predictions with the same `(stock_code, date, horizon, model_version)`.

Recommended command shape:

```bash
python -m app.jobs.scoring_runner backfill \
  --from 2025-01-01 \
  --to 2025-12-31 \
  --horizon 5 \
  --model-version score_v2_202604 \
  --dry-run
```

### 8.3 Verification Backfill

Verification backfill computes historical outcomes for existing predictions.

Rules:

- The date range filters prediction dates, not quote dates.
- Verification may update `PENDING` and `TRACKING` predictions to `TRACKING`, `VERIFIED`, or `INSUFFICIENT_DATA`.
- Verification must use the same horizon-specific metric contract as daily verification.
- Verification must be idempotent for unchanged quote data.

Recommended command shape:

```bash
python -m app.jobs.scoring_runner verify \
  --from 2025-01-01 \
  --to 2025-12-31 \
  --horizon 20 \
  --model-version score_v2_202604
```

### 8.4 Calibration Reports

Calibration reports evaluate whether high scores actually separate stronger future outcomes from weaker ones.

Minimum report dimensions:

- `horizon`
- `model_version`
- score bucket, e.g. `0-20`, `20-40`, `40-60`, `60-80`, `80-100`
- daily Top-N group, e.g. `top_10`, `top_30`, `top_50`
- component id and contribution bucket where possible

Minimum metrics:

- prediction count;
- verified prediction count;
- average score;
- average `return_at_target`;
- average `max_return`;
- average `min_return`;
- average `max_drawdown`;
- hit rate;
- stop-loss hit rate;
- false positives: high-score predictions with poor future outcomes;
- false negatives: low-score predictions with strong future outcomes.

Recommended command shape:

```bash
python -m app.jobs.scoring_runner report \
  --from 2025-01-01 \
  --to 2025-12-31 \
  --horizon 5 \
  --model-version score_v2_202604 \
  --format json
```

### 8.5 Look-ahead Bias Guardrails

Backfill and reports must avoid look-ahead bias:

- Scoring may not read quote rows after the evaluation date.
- Scoring may not read factor or signal rows generated after the evaluation date.
- Verification and calibration may read future quote rows only after prediction records already exist.
- Tests must fail if a scoring component uses future price data.
- Reports must distinguish `prediction_date` ranges from `verification_quote_date` ranges.

### 8.6 Score Experiments

Score experiments provide a research workspace for comparing factor combinations, scoring configurations, and model versions.

MVP model:

- `ScoreExperiment`
- Mongo collection: `score_experiments`

Required fields:

- `name`: Human-readable experiment name.
- `description`: Optional research note.
- `model_version`: Candidate scoring model version.
- `baseline_model_version`: Optional baseline model version for comparison.
- `start_date`: Prediction date range start.
- `end_date`: Prediction date range end.
- `horizons`: List of horizons, each one of `5`, `20`, `60`.
- `config`: Dict snapshot of factor weights, thresholds, or experiment parameters.
- `status`: `CREATED`, `RUNNING`, `COMPLETED`, or `FAILED`.
- `report`: Aggregated metrics generated from verified `StockScorePrediction` records.
- `error_msg`: Failure details when status is `FAILED`.
- `created_at`, `updated_at`, `completed_at`.

The first implementation aggregates already-generated and verified predictions for the selected `model_version`. This allows the research UI to compare model versions and stored factor-weight configurations immediately. A later datahub runner should use the same `ScoreExperiment.config` to regenerate historical predictions under a new `model_version`.

Implemented datahub runner:

```bash
python -m app.jobs.scoring_runner experiment \
  --id <score_experiment_id> \
  --replace
```

The experiment runner loads the stored `ScoreExperiment`, applies `config` as horizon-specific scoring overrides, backfills predictions under `model_version`, verifies them, and writes calibration reports back to the experiment record. `--dry-run`, `--skip-backfill`, and `--skip-verify` are available for safer research iteration.

Minimum report shape per horizon:

- `overall`: aggregate sample count, average score, average target return, average max return, average drawdown, hit rate, and stop-loss hit rate.
- `score_buckets`: metrics for `0-20`, `20-40`, `40-60`, `60-80`, and `80-100`.
- `top_n`: metrics for daily `top_10`, `top_30`, and `top_50`.
- `component_summary`: metrics grouped by explanation component id.
- `false_positives`: high-score samples with negative target return.
- `false_negatives`: low-score samples with strong max return.
- `baseline`: same report for `baseline_model_version` when provided.
- `comparison`: metric deltas between candidate and baseline.

## 9. Backend APIs

Backend APIs should expose score data as a stable read contract, not raw Mongo documents.

Implemented MVP read endpoints:

- `GET /api/scores?horizon=5&date=YYYY-MM-DD&limit=50`
  - Returns the score ranking for one horizon and evaluation date.
  - If `date` is omitted, the backend returns the latest available scoring date for the selected horizon.
  - Supported filters: `model_version`, `min_score`, `recommendation`, `status`, `limit`, and `offset`.
- `GET /api/scores/{stock_code}?horizon=5&from=YYYY-MM-DD&to=YYYY-MM-DD`
  - Returns one stock's score history for one horizon.
  - Supported filters: `model_version`, `limit`, and `offset`.
- `GET /api/scores/{stock_code}/{date}/explanation?horizon=5`
  - Returns the selected prediction with structured `explanation` and `input_snapshot`.

The first read API migration uses `StockScorePrediction` as the source of truth. New backend consumers should not read `StockDailyScore`.

Implemented experiment endpoints:

- `GET /api/score-experiments`
  - Lists recent score experiments with saved config and report snapshots.
- `POST /api/score-experiments`
  - Creates an experiment and, by default, immediately aggregates verified predictions into a report.
- `GET /api/score-experiments/{id}`
  - Returns one experiment.
- `POST /api/score-experiments/{id}/run`
  - Rebuilds the report from current verified prediction data.

Recommended next endpoints:

- `GET /api/scores/performance?horizon=5&from=YYYY-MM-DD&to=YYYY-MM-DD`
- `GET /api/market/comprehensive?date=YYYY-MM-DD&type=stock&horizon=5`

`/api/market/comprehensive` may default to `horizon=5` for ranking, but should return all three horizon summaries when available:

```json
{
  "evaluation": {
    "primary_horizon": 5,
    "scores": {
      "5": {"score": 76.5, "rank": 12, "recommendation": "BUY", "verification": {}},
      "20": {"score": 68.0, "rank": 41, "recommendation": "WATCH", "verification": {}},
      "60": {"score": 54.0, "rank": 130, "recommendation": "NONE", "verification": {}}
    }
  }
}
```

OpenClaw recommendation endpoints must also read `StockScorePrediction`:

- `GET /api/v1/integrations/openclaw/recommendations/daily?horizon=5`
- `GET /api/v1/integrations/openclaw/recommendations/performance?horizon=5`

The legacy T+5 response fields from `StockDailyScore` are deprecated. Downstream integrations should read `horizon`, `rank`, `percentile`, `explanation`, `input_snapshot`, and `verification`.

## 10. Frontend Requirements

Market view:

- Show `Score5`, `Score20`, and `Score60`.
- Allow ranking/sorting by selected horizon.
- Show recommendation badge per selected horizon.
- Show tracking status and verified max return where available.

Stock detail view:

- Show score history by horizon.
- Show explanation components and penalties.
- Show input freshness and model version.
- Show closed-loop tracking progress and verified outcome.

Dashboard:

- Show top `Score5` recommendations by default.
- Show score effectiveness summary once enough verified records exist.

Score experiment view:

- Create an experiment with candidate model version, optional baseline version, date range, horizons, and JSON factor-weight config.
- List previous experiments and rerun report generation.
- Display overall metrics, baseline deltas, score-bucket performance, Top-N performance, and component-level performance by horizon.
- The view is research-first and should prioritize dense comparison tables over marketing-style presentation.

## 11. Performance and Review Metrics

The system should produce aggregate review metrics per horizon and model version:

- count of predictions;
- count of verified predictions;
- average score;
- average `return_at_target`;
- average `max_return`;
- hit rate by score bucket;
- top-N average return;
- false positive examples: high score with negative return;
- false negative examples: low score with high future return.

These metrics are for calibration and operational trust. They are not a substitute for formal strategy backtesting.

## 12. Acceptance Criteria

- [ ] Daily scoring creates one prediction per active stock, date, horizon, and model version.
- [ ] `Score5`, `Score20`, and `Score60` have distinct component weights and thresholds.
- [ ] Every prediction stores structured explanation components, penalties, input snapshot, and model version.
- [ ] Verification updates `PENDING` and `TRACKING` predictions for 5, 20, and 60 trading day horizons.
- [ ] Verified records include target return, max return, min return, drawdown, quote count, and effectiveness flag.
- [x] Historical experiment replay can apply stored factor config overrides and regenerate predictions under a selected model version.
- [ ] Calibration reports summarize score quality by horizon, model version, score bucket, Top-N group, and component contribution.
- [x] Score experiments store model version, baseline, date range, horizons, factor config, and generated report snapshots.
- [ ] Look-ahead bias guardrail tests cover historical scoring replay.
- [x] Backend exposes score list, stock score history, explanation, and market comprehensive score summaries from `StockScorePrediction`.
- [x] Backend exposes score experiment creation, listing, retrieval, rerun, and comparison reports.
- [ ] Backend exposes first-party score performance summaries.
- [x] Frontend provides a research page for score experiments with config input and horizon-level report tables.
- [ ] Frontend can rank by horizon and inspect why a stock received its score.
- [ ] Missing quote/factor/signal data results in explicit `BLOCKED` or `INSUFFICIENT_DATA` status instead of silent zero scores.
