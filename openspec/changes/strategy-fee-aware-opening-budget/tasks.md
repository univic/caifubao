# Strategy NAV Fee-Aware Opening Buy Budget Tasks

> 基线：#202（`87a0ca2`）。范围：仅 `nav.py` BUY sizing、相关策略测试与对应
> spec/docs。DSH 交接（production-capability-roadmap 1.3a）。

## 1. Spec + 失败测试

- [ ] 1.1 本 change 的 spec requirements/scenarios 经 spec-guardian 审阅
- [ ] 1.2 失败测试先行（修复前红）：100% 目标含费用缩量成交；最低佣金边界；
      比例佣金边界；现金不足一手跳过；多标的依序买入不透支；零费用行为不变
      （#202 未来收盘回归保持绿）
- [ ] 1.3 确认只改 `datahub/app/lib/strategy_engine/nav.py` + 策略测试 + 对应
      spec/docs；如需扩大范围先报告原因

## 2. 实现

- [ ] 2.1 BUY sizing：选最大整手数量满足
      `成交额(含滑点执行价) + commission ≤ min(现金, 目标预算)`；
      仅一手都放不下才跳过；依序买入按剩余现金不透支
- [ ] 2.2 focused/full datahub pytest 全绿、Ruff、OpenSpec strict

## 3. Gates

- [ ] 3.1 spec-guardian / contract-reviewer / qa-reviewer 审阅
- [ ] 3.2 branch-conflict vs develop；Draft PR CI green
- [ ] 3.3 agent-progress 回填；本交接不授权合并后续代码 PR、operator 写库、
      promote 或真实下单
