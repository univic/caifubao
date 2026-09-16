# Immutable Artifact Contract (interface-strategy S2)

> **Driving decision (inlined, because the source is not in this repository).**
> An operator decision record dated 2026-09-12 ("接口策略决策：AI 原生为主 + 只读证据视图")
> is kept **untracked** in the operator's checkout. Its relevant points, quoted so this
> change is reviewable on its own:
>
> - 系统形态：**AI 原生为默认入口 + 一层薄的、读多写少的「可核对证据视图」**；前端冻结为只读
>   证据查看器；增量投入转向「不可变 artifact + 受控写通道」；人工只在**确认 / 复核 / 授权**
>   三类不可自动化闸门处介入。
> - 「聊天记录不可复现、不可归档，必须落成带 **id / hash / `as_of`** 的不可变 artifact。
>   这是**后端产出**。」
> - 切片：S1 记录决策（doc-only）· **S2 artifact 契约（本 change）** · S3 OpenClaw 写/动作
>   通道（须先建 proposal）· S4 前端收敛。
> - 风险与顺序：「正确顺序为：**证据门槛（120 日前瞻）> 审计/确定性 > 告警 > 交互层**」；
>   「量化数字不得由 LLM 在上下文内计算，必须由后端确定性产出后由 AI 解释」。
>
> 该文件应由其作者作为 **S1** 单独提交；本 change 不代提交它。

## Why

仓库里已经有多个「报告类产出」，但**各自为政、没有共同契约**：

| 产出 | 位置 | 现有可追溯字段 |
|:---|:---|:---|
| 评分预测（含解释） | `datahub` `StockScorePrediction` | `input_snapshot`（仅 status/count/date，**无版本或内容哈希**）、`model_version` |
| 校准报告 | `datahub` `scoring_engine/calibration_report.py` | 无统一哈希 / id / `as_of` |
| 模型与实验对比报告 | `datahub` `scoring_engine/comparison_report.py` | 同上 |
| 目标组合导出 | `datahub` `strategy_engine/export.py` | `config_hash`、`generated_at`，**无 artifact id / `as_of` / 输入快照** |
| 决策与日志类 | `backend` `api/v1/decisions.py`、`DecisionJournal` | 无统一契约；`save()` 就地更新 `updated_at`，`executed`/`realized_pnl` 由决策 API 后续写入 |

后果：同一个结论在不同出口下**不可机械比对**，也没有稳定的引用标识——「人工可核对」只能靠读
文档，而不是靠一个可复算的契约。S2 定义**一份共用契约**。

## What Changes

- **身份字段**：`schema_version`、`artifact_id`、`artifact_hash`、`as_of`、`generated_at`、
  `producer`、`grade`、`input_snapshot`、`field_manifest`、`payload`，datahub 与 backend 共用
  同一组名称与语义（唯一容忍的第二个名字是既有 freshness 字段 `data_as_of`，且须与 `as_of` 同值）。
- **哈希边界写死**：`payload` = 产出方断言的确定性数值/标识字段（**不含**自由文本叙述、**不含**
  声明的可变生命周期字段）；规范化序列化 = UTF-8 JSON + 键名字典序 + 无多余空白 + 非 ASCII 不转义；
  **哈希输入集内数字一律是 JSON 整数**——非整数按 `field_manifest` 为每个字段固定的单位缩放
  （如金额用分、比例用基点），**禁止浮点、禁止十进制字符串**；`as_of` 等时刻统一为 UTC + `Z`
  的 ISO-8601（≥秒精度、不带数字时区偏移）；`artifact_hash` = 该字节串的小写十六进制
  **SHA-256**，输入集**封闭**为 schema/生产者/`as_of`/`grade`/输入快照/**字段清单**/payload。
- **字段清单进哈希**（`field_manifest`）：逐字段声明它是「断言结果 / 叙述 / 可变生命周期」，
  数值字段声明单位，并为其记录**产出代码版本**与**所依赖的输入名**。因此「把断言结果声明成
  生命周期字段以逃出哈希」不再可行，且每个数值都能**仅凭 artifact 自证归属**。
- **`artifact_id` 形态钉死**：`sha256:` + `sha256(producer + "\n" + as_of + "\n" + artifact_hash)`，
  两个模块不可能对同一 artifact 推出不同 id；逐字节相同的重发是**幂等**（不追加新记录，
  `generated_at` 保持首次发布时刻、不被重写）。
- **`as_of` ≠ `generated_at`**；且**不得与既有 freshness 名 `data_as_of` 分叉**——产出方同时暴露
  两者时必须同值。
- **`input_snapshot` 记录可识别来源**（名称 + 版本或内容哈希 + 时间有界输入的自身 `as_of` +
  **产出代码版本**），并把「可复现」的措辞降级为「可识别来源」，不承诺逐位重放。
- **不可变 + 可变字段显式声明**：产出方必须**按名声明**可变生命周期字段（如
  `status`/`verification`/`executed`/`realized_pnl`/`updated_at`）并排除在哈希外；哈希内容一经
  发布不得修改，更新以追加表达。
- **数值必须确定性产出**：哈希载荷内不得出现 LLM 在对话里算出的值；叙述放哈希外。
- **只约束持久化语义，不约束存储介质**：只追加是语义要求；用 Mongo 文档、对象存储还是文件，
  本 change 不规定。

## Non-goals

- **不含实现**：本切片只定义契约；实现（含让既有产出符合契约）列为 follow-on change。
- 不含 OpenClaw 写/动作通道（S3；另需 proposal，触及 API 契约与 auth scope）。
- 不含前端收敛（S4）。
- 不规定存储后端与序列化库的选择（但所选介质必须能满足「只追加」语义）。
- 不改评分/因子/信号语义，不改 freshness 的**取值**语义（只统一命名），不开前瞻窗口，
  不授权真实执行或 promote。
- 不追溯重写既有历史产出；契约从新产出开始适用（历史产出作为旧 schema 可读）。

## Conformance scope（谁需要符合）

按影响面排序，均在 follow-on 实现切片内落地：

1. `datahub` 目标组合导出（`strategy_engine/export.py`）
2. `datahub` 校准报告与对比报告
3. `datahub` `StockScorePrediction`——**当前 `_build_input_snapshot` 只记录 status/count/date，
   无版本或内容哈希，因此今天不满足输入快照要求**；这是最大的存量产出，必须一并改造。
4. `backend` 决策类产出（与 S3 写通道一并考虑）
