# Immutable Forward Evidence Capture Tasks

> roadmap NEXT.1。基线：develop（含 #202/#204）。范围：strategy 引擎/model/runner/
> CLI + 测试 + spec/docs；不接调度、不改评分/选择、不下单、不 promote。

## 1. Spec + 失败测试

- [ ] 1.1 本 change spec requirements/scenarios 经 spec-guardian 审阅
- [ ] 1.2 失败测试先行（红→绿）：同日按时运行 → FORWARD；SKIPPED→当日 FORWARD
      升级；滞后/回填日期 → REPLAY；replace 既有 FORWARD COMPLETED 被拒；
      NAV recompute 不改计划证据；**前持仓/调仓 diff 与 NAV 曲线跨 REPLAY+FORWARD
      连续性**（第二日起调仓基于首日 FORWARD）；counter 只数窗口内 distinct
      FORWARD COMPLETED（as-of 口径）；窗口 ACTIVE 唯一性/配置变更关旧窗/同 key
      显式重启计数器归零；REPLAY/06-10/09-04 不计数
- [ ] 1.3 model：`evidence_kind` choices 增 FORWARD，default="REPLAY"（缺省即
      REPLAY）；新增 `strategy_forward_windows` 集合（model_version/horizon 上
      ACTIVE 唯一约束）

## 2. 实现

- [ ] 2.1 runner FORWARD 分类：`decision_at < open(next_execution_date(signal_date,
      ChinaAStock 日历))` 且 signal date ≥ 窗口 start_date 且不覆盖既有 COMPLETED
      （SKIPPED/FAILED 可重写）；否则 REPLAY；replace FORWARD COMPLETED fail-closed；
      前持仓与 run_nav 查询跨 REPLAY+FORWARD
- [ ] 2.2 CLI：`forward certify`（开窗，append-only）、`forward progress`
      （count/120、首末日期、缺口、窗口状态）
- [ ] 2.3 nav recompute 只改派生 NAV 字段（不改 target_holdings/status/evidence_kind/
      timestamps/config_hash）
- [ ] 2.4 focused/full datahub pytest 全绿、Ruff、OpenSpec strict

## 3. Gates

- [ ] 3.1 spec-guardian / contract-reviewer / qa-reviewer
- [ ] 3.2 branch-conflict vs develop；Draft PR CI green
- [ ] 3.3 runbook/roadmap 更新（0.1/0.3 指向本 change 的窗口与 counter）；
      合并后 agent-progress 回填；认证开窗与每日 operator 运行需用户另行授权
