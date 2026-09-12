# Immutable Artifact Contract Tasks

> 本 change **只交付契约**（proposal/spec/tasks）。所有实现均在 follow-on change 中进行，
> 以保证契约定稿前不动代码。

## 1. 契约定义（本 change 范围）

- [ ] 1.1 本 change 的 spec requirements/scenarios 经 spec-guardian 审阅
- [ ] 1.2 确认与 `production-capability-roadmap` 不变式一致（研究级标注、无真实执行、升格门），
      且不与既有 freshness 命名（`data_as_of`，RULES P3）冲突
- [ ] 1.3 确认哈希边界可实现：`payload` 定义、规范化序列化规则、数字禁止浮点、
      `grade` 在哈希内、`artifact_id` 组合规则、可变字段声明机制

## 2. Out of scope for this change（follow-on change）

以下为**实现**，属后续独立 change（触及代码与存储，需各自过 Spec Gate/评审）：

- [ ] 2.1 共用实现：规范化序列化（键名字典序、UTC `Z` 时刻、**整数 + 声明单位**的数字编码）
      + SHA-256 + `artifact_id` 组合 + `field_manifest` 构建/校验工具，datahub 与 backend 均可引用
- [ ] 2.1b **构建期注入代码版本**（git SHA 或包版本）并接入 `input_snapshot` 与 manifest——
      当前 `datahub/app` 与 `backend/app` 都没有任何版本来源，本项必须先落地，否则
      `input_snapshot` 的「产出代码版本」只能靠手写常量（spec 已明确不接受）
- [ ] 2.2 `datahub` `strategy_engine/export.py` 符合契约（补 `artifact_id`/`artifact_hash`/
      `as_of`/`input_snapshot`/`grade` 入哈希）
- [ ] 2.3 `datahub` `scoring_engine/calibration_report.py` 与 `comparison_report.py` 符合契约
- [ ] 2.4 `datahub` `StockScorePrediction`：重建 `_build_input_snapshot`
      （`scoring_service.py`），补齐版本/内容哈希与产出代码版本；声明可变生命周期字段
      （`status`/`verification`/`updated_at`）为哈希外
- [ ] 2.5 `backend` `DecisionJournal` 等决策产出符合契约；`executed`/`realized_pnl`/
      `updated_at` 声明为哈希外可变字段（与 S3 写通道一并考虑）
- [ ] 2.6 一致性测试：同输入重复产出 → hash/id 相同且不重复追加；`generated_at` 变化不影响
      hash；`grade` 变化 → 新 id 且可检测；哈希内容被改 → 重算不一致；输入快照缺失 → fail closed

## 3. Gates

- [ ] 3.1 spec-guardian 审阅（跨模块契约 + 公开文档 → 必须）
- [ ] 3.2 contract-reviewer（契约语义）；实现阶段另加 qa-reviewer
- [ ] 3.3 branch-conflict vs develop；Draft PR CI green
