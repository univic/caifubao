# Scoring Percentile-Rank Tasks

## 1. Recommendation logic

- [x] 1.1 `_recommendation` percentile-first: BUY/WATCH/AVOID from cohort percentile
- [x] 1.2 Single-stock fallback to absolute thresholds preserved
- [x] 1.3 Config defaults: buy_percentile 0.95 / watch_percentile 0.80 / avoid_percentile 0.20

## 2. Rank-normalized components

- [x] 2.1 `score_all_stocks` collects raw component values across cohort（`_compute_raw_components` + `score_all_stocks_ranked`）
- [x] 2.2 Per-component cross-sectional rank normalization to [0,1]（`_rank_normalize`，None 排最低、并列同 rank）
- [x] 2.3 Weighted sum of rank-normalized components -> final score（权重归一化到 sum=1）
- [x] 2.4 `score_single_stock` keeps raw weighting (backward compatible)

## 3. Tests

- [x] 3.1 Percentile-driven BUY/WATCH/AVOID/NONE scenarios
- [x] 3.2 Single-stock absolute-threshold fallback
- [x] 3.3 Rank-normalized market-wide score is cross-sectionally comparable（`_rank_normalize` 5 个单测）
- [x] 3.4 Existing scoring/verification tests do not regress（28 passed）

## 4. Review + Merge

- [x] 4.1 spec-guardian + qa-reviewer（spec-guardian PASS、qa-reviewer 两轮修复后 CONFIRMED MERGEABLE）
- [x] 4.2 Branch conflict check + Draft PR + CI green（PR #137 merged）

## 5. Deploy + Validation (operator)

- [ ] 5.1 Publish image; deploy dev; verify
- [ ] 5.2 Re-run 50-stock scoring + verification, compare corr vs baseline
