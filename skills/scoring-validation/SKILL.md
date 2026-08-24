---
name: scoring-validation
description: 评分/回测改动的验证与实验闭环（verification/replay/calibration/grid-search/factor-eval/walk-forward/显著性检验）
license: MIT
compatibility: opencode, dsh
metadata:
  audience: contributors
  project: caifubao
---

# 评分/回测改动的验证与实验闭环

## 1. 本 skill 覆盖什么

任何评分（scoring）或策略（strategy/backtest）改动都必须走验证闭环，不能凭单股
回测结论收工。成功标准见 `openspec/archive/mvp-quant-demo/tasks.md`
第 20 节（20.1–20.5 目前均为未勾选状态，即验收未执行）：

- 20.1 SCORE 策略需在全市场样本上至少一项指标（净收益/回撤/Sharpe/信息比率）优于 BUY_HOLD
- 20.2 单股验证不充分；改动必须至少在 top-50 市值股或全活跃市场上验证
- 20.3 参数优化结果必须通过 walk-forward decay 检查（test Sharpe 不得低于 train 超过 20%）
- 20.4 进入 top-N 排名的策略至少 5 笔交易、120+ 交易日、集中度 < 40%
- 20.5 模型版本变更必须附带新旧版本全市场校准报告对比

## 2. 验证命令与文件地图

CLI（均从 `datahub/` 下运行，见 `docs/cli-reference.md`）：

```bash
# 评分：回填 → 验证 → 校准报告
python -m app.jobs.scoring_runner backfill --from 2024-01-01 --to 2024-12-31 [--stock-code X] [--dry-run] [--replace]
python -m app.jobs.scoring_runner verify --from 2024-01-01 --to 2024-06-30
python -m app.jobs.scoring_runner report --horizon 20 --from 2024-01-01 --to 2024-12-31 [--format json|text]
# 实验与网格搜索（scoring_runner 子命令：run/backfill/verify/report/experiment/grid-search）
python -m app.jobs.scoring_runner experiment --id <experiment_id> [--skip-backfill] [--skip-verify]
python -m app.jobs.scoring_runner grid-search --from 2024-01-01 --to 2024-12-31 \
  --weight-grid '{"momentum":[20,25]}' --threshold-grid '{"buy_threshold":[60,70]}' \
  [--horizons 5,20,60] [--baseline-version <v>] [--dry-run]

# 回测（backtest_runner 子命令：single/multi/compare/compare-all/scan/optimize/walk-forward）
python -m app.jobs.backtest_runner single sh600519 SCORE_THRESHOLD 2024-01-01 2024-12-31 --horizon 20 --entry 75
python -m app.jobs.backtest_runner compare sh600519 SCORE_THRESHOLD 2024-01-01 2024-12-31 --vs MA_CROSS --horizon 20
python -m app.jobs.backtest_runner scan SCORE_THRESHOLD 2024-01-01 2024-12-31 --horizon 20 [--min-trades N]
python -m app.jobs.backtest_runner optimize sz000977 SCORE_THRESHOLD 2024-01-01 2024-12-31 --horizon 20 \
  --entry-range 50,60,70 [--no-split]        # 默认启用 train/val/test 切分
python -m app.jobs.backtest_runner walk-forward sh600519 SCORE_THRESHOLD 2024-01-01 2024-12-31 --horizon 20

# 因子评估（tech_factor_runner 子命令：list/compute/evaluate）
python -m app.jobs.tech_factor_runner evaluate rsi_14 2024-01-01 2024-12-31 --horizon 20 [--stock-code X] [--save]
```

服务文件（`datahub/app/lib/scoring_engine/`）：`verification_service.py`、
`replay_service.py`、`calibration_report.py`、`comparison_report.py`、
`experiment_service.py`、`grid_search.py`、`factor_eval.py`、`validation_service.py`。
worker 入口：`compute-worker/worker_app/handlers.py`（score_replay/score_verify/
calibration_report/grid_search/factor_eval/rolling_validation）；`datahub/app/jobs/scoring_runner.py`。

API（backend，`backend/app/api/v1/`，包一层 `_ok(data=...)` 信封）：
- `/api/backtest`：POST `/run`、`/run-multi`、`/optimize`、`/compare`、`/scan`、`/walk-forward`、
  `/decay-analysis`、`/evaluate-factor`、`/landscape`、`/recommendation`；GET `/`、`/<result_id>`、
  `/<result_id>/regime`、`/<result_id>/significance`、`/<result_id>/component-contribution`；DELETE `/<result_id>`
- `/api/score-experiments`：GET `/`、POST `/`、GET `/<id>`、POST `/<id>/run`、
  GET `/compare`、`/consensus`、`/rankings`、`/heatmap`
- `/api/factor-eval`：GET `/reports`、`/reports/<report_id>`、`/components`
- `/api/scores`：GET `/`、`/<stock_code>`、`/<stock_code>/<date>/explanation`、POST `/generate`
  （验证状态通过预测的 `verification` 字段暴露；验证运行本身走 CLI/compute-worker，无独立 REST 端点）

测试文件：`datahub/app/test/test_scoring_service.py`（评分/验证/回放/校准报告，FakeQuote 模式）；
`backend/app/test/test_scores_api.py`、`test_score_experiments_api.py`、
`test_score_experiment_rankings_api.py`、`test_backtest_api.py`、`test_factor_eval_api.py`。

## 3. 各环节入口与产物

- **verification 状态机**（`ScoreVerificationService`，`verification_service.py`）：
  `PENDING → TRACKING → VERIFIED`，另有 `INSUFFICIENT_DATA`（目标日已过但行情不足）、`BLOCKED`。
  `verify_predictions(start_date, end_date, horizon, today)` 只处理 `status__in [PENDING, TRACKING]`
  且 `target_date <= today` 的记录；`verify_single_prediction` 写回 `prediction.verification`：
  `verified_quote_count`、`return_at_target`、`max_return`、`min_return`/`max_drawdown`、
  `days_to_max_return`、`hit_target_close`、`hit_target_intra`、`hit_stop_loss`、
  `effective_threshold`、`stop_loss_threshold`、`verified_at` 等。
- **replay**（`ScoreReplayService.backfill_predictions`）：按日期范围×horizon×股票代码回填
  历史评分（`dry_run`/`replace`），返回 `from/to/horizons/date_count/scored_count`。
- **calibration report**（`ScoreCalibrationReport.generate(start, end, horizon)`）：
  结构含 `distribution`（min/max/mean/std/percentiles/recommendations/miscalibration_flags）、
  `score_buckets`（5 桶 0–20…80–100）、`top_n`（10/30/50）、`component_summary`、
  `false_positives`（score≥70 且 return_at_target<0）、`false_negatives`（score<40 且 max_return≥0.08）。
  校准旗标：BUY 占比 < 3%、AVOID 占比 > 50%、中位数 ≤ 25。
- **实验/网格搜索**：`GridSearchService.create_experiments`（weight_grid × threshold_grid 笛卡尔积，
  权重和校验 target 100）；`ScoreExperimentService.run_experiment(experiment_id, backfill, verify,
  replace, dry_run)` 串联回放→验证→报告，状态 `RUNNING → COMPLETED/FAILED`；
  `ExperimentComparisonReport.compare(candidate_version, baseline_version, start, end, horizon)`
  返回 candidate/baseline 概览 + deltas + verdict（顶 N hit_rate/return 增量）。
- **factor eval**（`FactorEvaluationService.evaluate(factor_values, start, end, forward_horizons=[5,20,60],
  regime_split=False)`）：`ic`（Spearman 秩 IC 的 mean/std，horizon 键控）、`icir`（mean/std）、
  `quintiles`（分层平均前瞻收益，检验单调性）、`correlation`（与 7 个评分组件相关，>0.7 视为冗余）、
  `decay`（IC 随前瞻天数衰减）、可选 `regime_ic`（bull/bear/sideways 分状态 IC）。持久化模型
  `FactorEvalReport`（`backend/app/model/factor_eval.py`，collection `factor_eval_reports`）。
- **walk-forward 与显著性**：`/api/backtest/walk-forward`（窗口默认 120 天、步长 60 天，
  前后半段 Sharpe 衰减 > 20% → `performance_decay=true`）；`/api/backtest/decay-analysis`
  （逐窗 train vs test Sharpe 衰减，总交易日 < 300 给警告）；compute-worker `_handle_rolling_validation`
  （train 年 Y / test 年 Y+1，hit_rate 衰减 > 20% 标 `overfit`）；
  `ValidationService`：`regime_split_report` / `stability_check`（权重扰动 5%）/
  `best_config_recommendation`（复合分 + 自举 CI + 状态稳健性）；
  `GET /api/backtest/<result_id>/significance` 返回 `permutation_test`（H0: 收益均值=0，
  1000 次置换、显著性 0.05）与 `bootstrap_ci`。

## 4. 反过拟合护栏

- **train/val/test 切分**：`/api/backtest/optimize` 默认 60/20/20；参数只在 train+val 上选择，
  最终结果只在 test 段上报（`--no-split` 可关闭，慎用）。
- **最小样本**：`anti_overfitting_flags`（`backend/app/services/backtest_service.py`）——
  交易 < 5 标 `low_sample`、交易日 < 120 标 `insufficient_period`；复合分对 <5 笔交易直接判 `rankable: False`（score -999）。
- **集中度**：单笔 SELL 盈利占总盈利 > 40%（`CONCENTRATION_THRESHOLD=0.40`）标 `concentrated_returns:<pct>` 并计罚分。
- **多重比较**：`num_comparisons > 1` 时追加 `bonferroni_applied:n=<N>`，并按
  `multiple_comparison_flag` 修正 p 值（网格/优化场景必查）。
- **walk-forward decay**：train 与 test Sharpe（或 hit_rate）衰减 > 20% 判定 overfit，排除出 top 排名（tasks 17.2 / 20.3）。

## 5. 最小可重复测试模式（FakeQuote）

来自 `datahub/app/test/test_scoring_service.py`：用内存 `FakeModel` 代替 mongoengine，
`records` 列表 + `objects(**query)`（支持 `__gte/__lte/__gt/__lt/__in/__ne`）+ `save()`；
空子类如 `FakeQuote`/`FakePrediction` 注入服务即可：

```python
class FakeQuote(FakeModel): pass
class FakePrediction(FakeModel): pass

# 要点：先造"未来"日期行情再造验证，断言无未来数据泄漏（参考 test_scoring_does_not_read_future_quotes）
FakeQuote.records.append(FakeQuote(code="sh600000", date=d, close=10.0 + i * 0.2, high=10.1, low=9.9))

service = ScoreVerificationService(quote_model=FakeQuote, prediction_model=FakePrediction)
status = service.verify_single_prediction(prediction, today=datetime.datetime(2026, 4, 18, tzinfo=datetime.UTC))
assert status == "VERIFIED"
assert prediction.verification["verified_quote_count"] == 5
assert prediction.verification["hit_target_close"] is True
```

## 6. 自检清单

- [ ] 改动只作用于新的 `model_version`，未污染线上版本
- [ ] 已跑 `scoring_runner verify` + `report`，校准旗标（BUY<3% / AVOID>50%）为 0
- [ ] 结论在 top-50 市值或全市场验证过（20.2），非单股 cherry-picking
- [ ] optimize 使用 train/val/test 60/20/20，最终结果只在 test 段读取
- [ ] walk-forward decay ≤ 20%；>20% 的配置已标 overfit 并排除出排名
- [ ] top-N 项满足 ≥5 笔交易、≥120 交易日、集中度 < 40%（20.4）
- [ ] 网格/多配置比较做了 Bonferroni 修正并核对 flag
- [ ] 显著性结论来自 permutation p 值与 bootstrap CI，而非单一回测数字
- [ ] 用 FakeQuote 模式写了最小复现测试，含无未来数据断言
- [ ] 模型版本变更附新旧版本校准报告对比（20.5）
