# Strategy Target Portfolio Export Tasks

> 基线：develop（含 #202/#204/#207）。范围：`datahub/app/lib/strategy_engine/export.py`
> + `datahub/app/jobs/strategy_runner.py` 的 `export` 子命令 + 策略测试 + 本 change
> 与 operator 文档。roadmap 2.1（A 模式人工下单的最小路径）。
> 只读切片：不写库、不下单、不 promote、不开前瞻窗口、不改评分/选择/NAV 语义。

## 1. Spec + 失败测试

- [x] 1.1 本 change spec requirements/scenarios 经 spec-guardian 审阅
      （首轮 GATE_BLOCKED：P1 "actionable" 措辞与合规不变式冲突 + 5 项 P2；已按
      建议修订为无条件研究级标注、去掉 actionable 框架、SELL 不定量、base NAV
      优先级、config_hash 歧义 fail-closed、CSV 产物自带标注）
- [x] 1.2 失败测试先行（红→绿）：added/removed/unchanged → BUY/SELL/HOLD；
      行含 score/percentile（缺失为 null 不臆造）；单一 base NAV 且报告值与来源；
      研究级标注 + 免责声明恒在且不可由证据代理推断；REPLAY 不得被表述为前瞻已验证；
      无可导出 run / SKIPPED / FAILED → fail closed 且不输出行；多 config_hash 歧义
      → fail closed；同一 run 重复导出逐行一致；traceability 字段齐全；
      CSV 产物首行自带标注与免责声明
- [x] 1.3 确认写范围仅上述文件；扩范围前先报告原因（未扩范围）

## 2. 实现

- [x] 2.1 纯函数 `build_target_export` + `render_csv` + `select_export_run`：
      输入 run 视图 + 单值 base NAV + 可选 score/name 映射，输出元数据 + 确定性排序行；
      歧义/缺失/非 COMPLETED 一律 fail closed
- [x] 2.2 `strategy_runner export` 子命令：读取该 signal date 的 run（按
      model_version/horizon 解析，`--config-json` 给定时按 config_hash 消歧），映射
      Mongo → 纯函数，渲染 csv（默认，产物首行自带标注）或 json，支持 `--output`
- [x] 2.3 focused/full datahub pytest 全绿、Ruff、OpenSpec strict

## 3. Gates

- [ ] 3.1 spec-guardian 复审（修订后）；qa-reviewer 审阅。contract-reviewer 不触发：
      本切片不改 API/鉴权/freshness/OpenClaw，且不在 backend/OpenClaw 暴露
- [ ] 3.2 branch-conflict vs develop；Draft PR CI green
- [ ] 3.3 operator 文档（`docs/operations/agent-cli.md` 策略小节）回填；
      agent-progress 回填；本切片不授权合并、写库、promote 或真实下单
