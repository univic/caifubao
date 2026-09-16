# Task 3.3 — flip_wide 影子版本 operator 验证 runbook

openspec `scoring-direction-versioning` task 3.3：对**一个翻转 model_version** 做
全市场 replay + 校准对比 baseline，通过前**不得 promote**。本文是 operator 执行
手册；执行需要 dev/prod MongoDB 访问（本仓库代码与测试均已就绪）。

> 项目定位：A 股量化研究/学习/演示 MVP，不构成投资建议。
> 语义约束：评分方向翻转只发生在**构造层**（已由 scoring-direction-versioning
> #183 版本化支持）；usage 层始终「选高买入」。本任务注册的是**影子版本**，不改变
> `DEFAULT_MODEL_VERSION`（线上默认评分不变）。

## 0. 前置事实（研究证据）

- flip_wide = 构造层 7 个 alpha 分量方向翻转为 -1（`risk_penalty` 保持默认 -1，
  它是 penalty 本质）；权重与生产 h20 SCORING_CONFIG 完全一致（sum 110）。
- 研究证据：扩展 walk-forward（train 2019-2023 → val 2024/2025 → test 2026H1）
  全窗口正、decay 0.00；官方 profile validation IR **+0.385（正）**，仅被 decay
  0.605 门挡回（train 2024 +0.975 更强）——两协议一致支持 flip_wide，但**都是研究
  层证据，不是生产授权**。
- 候选配置工件：`datahub/research/autoresearch/h20_excess_alpha/flip_wide_registry_config.json`
  （config_hash 由单测锁定，防意外漂移）。

## 1. 注册影子版本（不改变默认）

```bash
PYTHONPATH=datahub datahub/.venv/bin/python -m app.jobs.model_registry_runner register \
  --model-version flip_wide_shadow_v1 \
  --description "flip_wide shadow: construction-layer reversal h20 only; research-only, NOT default" \
  --config-json "$(cat datahub/research/autoresearch/h20_excess_alpha/flip_wide_registry_config.json)"
```

- 注册后 `DEFAULT_MODEL_VERSION`（`score_v2_202605b`）不变；只有显式命名
  `flip_wide_shadow_v1` 的 scoring/backfill/compare 才使用翻转语义。
- 校验：`... model_registry_runner list` 应看到 ACTIVE 记录与
  `config_hash=8c8f3ee4...547dc`。

## 2. 全市场 replay（backfill 翻转版本）

先确认目标窗口 quote/factor/signal 齐全（回放需要历史数据在库）。示例窗口取
**最近且已 T+1 完结的 ≥120 个交易日**（如 `--from 2026-03-01 --to 2026-08-31`，
实际窗口以当日可用数据为准）：

```bash
PYTHONPATH=datahub datahub/.venv/bin/python -m app.jobs.scoring_runner backfill \
  --model-version flip_wide_shadow_v1 --horizon 20 \
  --from 2026-03-01 --to 2026-08-31
```

- 只回放 **horizon 20**（工件只注册了 20 的方向翻转；5/60 未注册 → 默认方向，
  本次不验证）。
- 写库后核验：`scoring_runner report`（见下）应能看到 prediction_count>0。

## 3. 校准报告（翻转版本自身）

```bash
PYTHONPATH=datahub datahub/.venv/bin/python -m app.jobs.scoring_runner report \
  --model-version flip_wide_shadow_v1 --horizon 20 \
  --from 2026-03-01 --to 2026-08-31 --format json
```

- 预期 `bucket_basis: "percentile"`（注册 config 翻转 → percentile 语义；
  正分窗口也不会误按 raw score 分桶）。若 percentile 缺失会显式失败——这是设计。

## 4. 校准对比 vs baseline（task 3.3 判定）

```bash
PYTHONPATH=datahub datahub/.venv/bin/python -m app.jobs.scoring_runner compare \
  --candidate-model-version flip_wide_shadow_v1 \
  --baseline-model-version score_v2_202605b \
  --horizon 20 --from 2026-03-01 --to 2026-08-31 --format json
```

- 预期：`comparison_basis: "percentile"`（跨方向统一 basis）、`avg_score` delta 为
  None（跨方向不报告 raw 分数差）、`comparison_status: "ok"`。
- 判定标准（与官方 runner 硬门对齐）：候选需在**生产 T+1 验证数据**上 hit_rate /
  return 对比 baseline 不劣于研究层结论；若 `insufficient_data`（任一侧无
  VERIFIED 预测）→ 说明 verify 未跑或窗口太近，先补 verify 再比。

## 5. 记录与归档

- 结果记入 `docs/autoresearch/runs/h20-excess-alpha/manual-experiments-ledger.md` 与
  `autoresearch/ledger.jsonl`（同 #187 先例），注明 run 是生产数据 replay 而非
  研究快照。
- 只有通过后，才允许考虑 version bump + Spec Gate + 120 天 paper 路径
  （task 3.4 与 codex 阶段二/三）；**本任务不授权 promote**。

## 6. 执行阻塞（operator）

- 需要 dev/prod MongoDB 与数据链（quote/factor/signal）可用；本仓库会话默认
  无法触达集群，执行前请确认 `./scripts/caifubao system health` 正常。
- 写生产库前需用户显式授权（同 08-31 prod 补跑先例）。
