# Research 环境模拟盘（paper）上线准备手册

> 只读/paper，不下单、不接券商、不构成投资建议。本文是“在 research 环境跟随真实
> 行情开模拟盘、检验策略是否实盘有效”的**操作前置条件**与执行清单。
>
> 相关契约：[`strategy-paper-readiness`](../../openspec/changes/strategy-paper-readiness/proposal.md)、
> 既有前瞻窗口手册 [`strategy-forward-window.md`](strategy-forward-window.md)、
> 实盘环路（pre-broker）[`strategy-live-loop.md`](strategy-live-loop.md)。

## 1. 这里的“模拟盘”到底是什么

- `strategy run` 用**当日已落库的真实行情/评分**生成目标组合，写
  `strategy_paper_runs`；`strategy nav` 用真实开收盘价重算 paper NAV。
- 只有 `strategy forward certify` 显式开启、且 `config_hash` 与 ACTIVE 窗口一致的
  记录才算 **FORWARD** 前瞻证据；其余都是 REPLAY。
- 计数窗口是 120 个交易日，**漏一天就是永久缺口**，不能回填。
- 没有订单、没有成交回报、没有券商；`nav` 是纸面估值。

## 2. 开窗前必须先过的门：`paper readiness`

```bash
export CFB_NAMESPACE=caifubao-research

# 只读：不写库、不开窗、不计数
./scripts/caifubao paper readiness \
  --model-version <候选模型> --horizon 20
```

输出 `READY` / `DEGRADED` / `NOT_READY`，逐项 PASS/WARN/FAIL；`NOT_READY` 退出码 1，
`DEGRADED` 退出码 0（WARN 需要人工确认，不是阻断）。检查项：

| 检查 | FAIL 条件 | 说明 |
|:---|:---|:---|
| `quote_freshness` | 最新行情早于“已收盘的应完成交易日” | 期望交易日 = 交易日历中 ≤ 今天、且 15:00 后含今天的最近一天；盘前/盘中用上一交易日 |
| `fq_coverage` | 当日有行情的活跃股票缺 `fq_factor` | 排除北交所（`daily_quote=false`） |
| `scoring_freshness` | 候选模型的最新评分早于期望交易日 | 可按 `--model-version` 过滤 |
| `signal_freshness` | 最新信号早于期望交易日 | |
| `forward_window` | 无 ACTIVE 窗口 / 有缺口 | 无窗 = WARN（提示需要 certify）；缺口 = WARN 并列出缺失交易日 |
| `halt_switch` | 未配置 halt 路径 / 已触发 | 未配置 = WARN（下单路径 fail-closed） |
| `etf_market_data` | ETF 行情落后或 ETF 缺 `fund_adj` FQ | 无 ETF 池 = WARN（ETF 策略证据不可用） |

每次结果都附带 `limitations`（见 §6），`READY` 也不代表可以上钱。

### 2026-10-23 实测（research，镜像 sha-f3f29c8）

```text
overall NOT_READY   expected_session 2026-10-23
quote_freshness  PASS  5217/5225 活跃股票当日有行情
fq_coverage      PASS  当日活跃行情全部有复权价
scoring_freshness FAIL  flip_wide_shadow_v1 最新评分 2026-09-10
signal_freshness PASS  信号最新 2026-10-23
forward_window   WARN  尚无 ACTIVE 窗口（需要 certify）
halt_switch      WARN  未配置 halt 文件路径（下单路径 fail-closed）
etf_market_data  WARN  尚无 ETF 数据
```

**结论：数据链路（行情/FQ/信号）健康，但候选模型没有每日评分，且没有窗口、halt
路径和 ETF 数据。** 这不是“再跑一天就行”的问题，而是三个明确前置项：

1. **候选模型必须每日评分**。research 的 `caifubao-datahub-scoring` 只跑
   `DEFAULT_MODEL_VERSION=score_v2_202605b`；`flip_wide_shadow_v1` /
   `pit_*` 等影子模型是**按需**计算，最新只到 2026-09-10。二选一：
   - 用每日调度的 `score_v2_202605b` 开窗；或
   - 为候选影子模型增加每日评分。先手工验证一天（写库，需授权）：

     ```bash
     # 在 datahub pod 内；日期用最近一个已收盘交易日
     python -m app.jobs.scoring_runner run --model-version flip_wide_shadow_v1 --date <DATE>
     ```

     验证通过后，再挂一个排在默认评分（18:35）之后、默认 `suspend: true` 的
     每日 CronJob，不改变已有 `score_v2_202605b` 的记录。
2. **halt 路径**：在 datahub pod 上设置 `CAIFUBAO_STRATEGY_HALT_FILE`（与私有
   overlay 一致），否则 readiness 永远 WARN、下单路径 fail-closed。
3. **ETF 数据**（若策略涉及 ETF）：按 [`etf-market-data.md`](etf-market-data.md)
   部署后执行 `etf universe` → 抽样 `etf backfill` → `etf daily`。

### 2026-10-24 research 上线后的实测

在授权范围内完成：research datahub 升级到 `sha-62ab25490950`、ETF universe/daily/
抽样 backfill、对 `flip_wide_shadow_v1` 的 2026-10-23 补评分后，readiness 为
**DEGRADED（无 FAIL）**：

| 检查 | 结果 |
|:---|:---|
| quote_freshness | PASS（5,217/5,225，99.85%） |
| fq_coverage | PASS（活跃行情缺因子 = 0） |
| scoring_freshness | PASS（候选模型最新 2026-10-23） |
| signal_freshness | PASS（2026-10-23） |
| etf_market_data | PASS（2,329 只活跃 ETF，2,176 条当日行情 + FQ） |
| forward_window | WARN（尚无 ACTIVE 窗口；未授权开窗） |
| halt_switch | 当时 WARN（未配置 halt 文件路径；2026-10-25 已配置，见下） |

因此“数据链/评分/ETF”已就绪；当时只剩开窗与 halt 配置。候选模型当时只补了
2026-10-23 一天评分，连续开窗前必须先挂每日评分任务（已于 2026-10-25 启用）。

### 2026-10-25 halt 与每日评分启用后的实测

- 配置 `CAIFUBAO_STRATEGY_HALT_FILE` 指向持久化卷，并完成
  `halt engage → status → resume` 全链路验证；最终状态 **OFF**，历史保留。
- 启用 research 的 `caifubao-datahub-etf-daily`（18:40）与
  `caifubao-datahub-scoring-shadow`（19:00，写 `flip_wide_shadow_v1`）；
  `caifubao-datahub-paper-readiness` 仍暂停，开窗后再启用。
- readiness 复跑仍为 **DEGRADED**，但只剩 `forward_window` 一个 WARN；
  quote/fq/scoring/signal/halt/ETF 全部 PASS。
- 下一交易日（2026-10-26）起 ETF 行情与候选评分会自动更新；首次自动运行后应检查
  `datahub_job_runs` 与 readiness，确认 18:40/19:00 的调度确实按预期执行。

## 3. 与已合并 #307 的关系（重要）

PR #307（`7464d6b`）已合并：统一 `StrategySpec`、legacy/spec 双哈希、研究模拟账本、
风控与共享执行原语。其 change 仍标 S0–S3 收尾（8 项未完成），**未启用**
trading paper/LIVE 部署；随后合并的 #308/#309 分别补了 trading 侧账本/halt 基座与
不可变研究发布，但都不等于实盘能力。

对开窗的实际影响：

- FORWARD 判定现在同时要求窗口与 run 的 **`config_hash` 和 `hash_kind` 匹配**
  （`classify_evidence_kind`）；旧记录没有 `hash_kind` 字段，按 legacy 处理。
- 迁移只保证目标组合/调仓等行为对拍，**不会**把旧 FORWARD 记录升级成 Spec 证据。
  legacy 窗口继续用 `--config-json`；Spec 候选必须用 `--spec` **新开窗**、每日用
  完全相同的 spec 内容，不能中途切换命名空间。
- `paper readiness` 已按新语义过滤：输出 `hash_kind`/`spec_hash`/
  `legacy_config_hash`；legacy 窗口匹配显式 legacy 与缺字段行，spec 窗口只匹配
  spec 行。
- 本工作仍不修改 #307 owned 文件
  （`strategy_engine/{nav,replay,runner,tradability,spec,simulation_ledger}.py`、
  `jobs/strategy_runner.py`、`model/strategy.py`、`shared/strategy_*.py`）；
  readiness 只读已持久化字段。
- 推荐顺序：**legacy 或 spec 二选一 → 用同一配置/spec 跑通 1 天并确认 FORWARD →
  `paper readiness` 无 FAIL → 人工授权后 `forward certify --yes`**。窗口只有一次，
  不要在窗口中途迁移 hash 命名空间。

## 4. 开窗前检查清单

- [ ] `paper readiness` 无 FAIL；WARN 已逐条确认并记录。
- [ ] 候选模型每日评分已跑通（readiness `scoring_freshness=PASS`）。
- [ ] 数据链当日完整：`quote → daily_basic → signal → scoring` 无断链；
      `./scripts/caifubao system health` 正常。
- [ ] `CAIFUBAO_STRATEGY_HALT_FILE` 已配置，`strategy halt status` 可读。
- [ ] 若用 ETF：`etf status` 显示当日行情 + FQ 完整。
- [ ] **手工试跑一天**：`strategy run <DATE> --dry-run` 与一次非 dry-run，
      确认记录为 `FORWARD`（或明确为 `REPLAY` 并知道原因）。
- [ ] 已获**显式人工授权**（开窗、每日写库、promote 都需要）。
- [ ] 记录准备使用的完整 `--config-json` / `--spec`、预期 `config_hash` 与
      `hash_kind`（legacy 或 spec）。

## 5. 开窗与每日闭环

```bash
# 1) 开窗（先关闭同键 ACTIVE 前身；start_date = 认证当日）
#    legacy 配置：
./scripts/caifubao strategy forward certify \
  --model-version <MODEL> --horizon 20 --config-json '<CONFIG>' --yes
#    Spec 候选（二选一，不能与 legacy 窗口混用）：
# ./scripts/caifubao strategy forward certify \
#   --model-version <MODEL> --horizon 20 --spec path/to/strategy.json --yes
# 记下打印的 config_hash 与 hash_kind；之后每天必须用完全相同的配置/spec

# 2) 每交易日收盘后（评分完成之后）
./scripts/caifubao paper readiness --model-version <MODEL> --horizon 20
./scripts/caifubao strategy run <DATE> --config-json '<同一 CONFIG>'

# 3) 每周
./scripts/caifubao strategy forward progress --model-version <MODEL> --horizon 20
./scripts/caifubao strategy nav --from <START> --to <TODAY> --config-json '<同一 CONFIG>'
```

迟跑（执行日已开盘）会自动归为 REPLAY、不计入；漏跑不补。把每周 NAV/超额/回撤/
换手记入研究账本并标注证据类型。

## 6. 已知限制（readiness 每次都会打印）

1. **Paper NAV 用原始价**：现金分红不入账，除息日会假跌；总收益被低估。研究回放/
   评分用 HFQ，paper 账本没有。统一研究账本契约已支持 `DIVIDEND`/`SPLIT` 事件，
   但当前 NAV 路径尚未从 `fq_factor` 推导并应用它们；在那之前 paper 结果不能当作
   可上钱的实盘有效性证据。
2. **目标权重不是持仓**：没有账户、没有成交、没有券商；不能把 paper 结果表述为
   可交易建议。
3. **窗口缺口永久**：漏一天就少一天。
4. **ETF 分红**同样受第 1 条影响（`fund_adj` 只用于分析，不等于现金入账）。

## 7. 授权与调度

- 开窗、每日写库、任何 promote 都需要**显式人工授权**；readiness 不构成授权。
- 公共 research overlay 提供三个 **默认 suspend** 的 CronJob：
  - `caifubao-datahub-etf-daily`（18:40）
  - `caifubao-datahub-scoring-shadow`（19:00，可选；只写指定候选模型）
  - `caifubao-datahub-paper-readiness`（19:30，只读）
  私有 overlay 按 one-writer 规则启用；启用前先手工验证一天。
- **2026-10-25 research 实际状态**：`etf-daily` 与 `scoring-shadow` 已直接 patch
  为 `suspend: false` 并由调度器接管；`paper-readiness` 仍为 `suspend: true`。
  注意这是**直接集群状态**，私有 overlay 尚未同步：
  - 私有 overlay 还缺这两个新 CronJob 的 suspend 开关与清单，后续 18:40/19:00
    的调度不会随私有仓库自动重建；
  - 私有 overlay 的 datahub 目标镜像 tag 仍指向旧 tag，下一次私有部署会把 research
    回滚到旧镜像，除非同步成当前 `sha-<commit>`。
  这两项必须在私有仓库收口，否则“集群已启用”不等于“可重复部署”。
- 直接 apply 公共模板到集群时，三个 CronJob 的 `image` 必须固定为当前私有 registry
  的 `sha-<commit>`：模板里的 `caifubao-datahub:latest` 在集群内不可拉取。
  2026-10-25 已把 `etf-daily` / `scoring-shadow` / `paper-readiness` 三个 CronJob
  的镜像固定为当前部署的 `sha-62ab25490950`。
- readiness 只读：它不会开窗、不会写 run、不会改状态，也不计入 120 日证据。

## 8. “运维授权”具体指什么

这里的授权**不是技术权限**：有 kubeconfig、数据库密码或集群 exec 能力，不等于被授权
执行写操作。`RULES.md` 要求对影响运行环境、权威数据或不可逆证据的动作做**显式人工
批准**；本 runbook、readiness 报告和 CI 绿都不构成授权。

| 动作 | 典型命令/位置 | 谁来批准 | 为什么需要 | 可逆性 |
|:---|:---|:---|:---|:---|
| 构建/推送 datahub 镜像 | 私有 publish workflow → 私有 registry | 仓库维护者/运维 | 决定 research/dev 实际运行哪份代码 | 可重构建、可回滚 tag |
| 部署 research / dev | `caifubao-private/scripts/deploy.sh research --ref <sha>` | 私有部署负责人 + `research` GitHub Environment 保护 | 改运行环境；可能启用/停用 writer | 回滚到上一镜像 tag |
| 启用数据 writer | 私有 overlay 的 `DATAHUB_*_SUSPEND=false` | 运维，按 one-writer-at-a-time | 写入权威库、影响 freshness 与证据 | 置回 `true` |
| 首次 ETF universe/backfill | `scripts/caifubao etf universe/backfill` | 数据 owner | 写入大量历史行情/因子 | 可重跑；删除需单独授权 |
| 候选模型补评分 | `scoring_runner run --model-version <V>` | 研究 owner | 写评分，改变 readiness 结果 | 可重算 |
| 开前瞻窗口 | `strategy forward certify --yes` | 策略 owner/授权人 | 120 个交易日时钟**不可回填** | **不可逆**：只能 close 后重开、计数重来 |
| promote / 对外声明可交易 | roadmap gate | 授权人 | 影响对外结论与合规 | 需审计记录 |

- **谁能批准**：本仓库维护者或私有部署负责人；在当前会话里就是你（用户）。
- **怎样算批准**：明确写出“授权在 `<环境>` 执行 `<动作>`，范围/时间窗/回滚方案”。
  一句“继续”或一次代码评审通过都**不算**对部署/写库/开窗的授权。
- **批准后谁执行**：部署与 writer 开关走私有仓库流程；数据/窗口命令可由我按批准范围
  执行，并在 `datahub_job_runs`/进度日志留下记录。
- **不需要授权的动作**（本分支已做）：只读审计、代码/测试/文档、`kubectl kustomize`
  渲染、readiness 只读检查、`--dry-run`。

最小授权请求模板：

```text
授权：在 research 环境
1) 部署 public <sha>（含 #307 + ETF 能力），回滚 tag <old-sha>
2) 执行 etf universe、etf backfill <代码列表>、etf daily <日期>
3) 对 <候选模型> 补 <日期> 评分
4) 以上验证通过后，开窗 <模型>/<horizon>/<legacy|spec>，配置 <config/spec 路径>
不授权：prod、真实券商、promote、任何真实资金动作
```


## 9. Milestone A: guarded research FORWARD daily capture (disabled template)

The public research example now includes
\`caifubao-datahub-strategy-forward-daily\` (22:15 Asia/Shanghai, weekdays),
**suspended by default**. It is a research-plan capture, not an operational
Trading Paper account and not NAV accounting acceptance. The job runs:

\`\`\`bash
python -m app.jobs.strategy_forward_daily \
  --spec /etc/caifubao/research/strategy.json
\`\`\`

The spec must be mounted as read-only from the operator-managed
\`caifubao-research-strategy-spec\` ConfigMap, and its \`spec_hash\`, model
version, horizon and \`hash_kind=spec\` MUST match an already ACTIVE,
separately operator-certified forward window. The job never opens that window,
accepts no \`--date\` or \`--replace\`, and calculates today's session in
Asia/Shanghai. It exits nonzero for stale market/scoring/signal data, an unreadable
or active halt, a wrong/missing window, or a non-FORWARD outcome; a genuine
holiday is logged as SKIP_NON_SESSION without strategy writes.

**Do not enable based on this public template alone.** Private deployment must
mount the same authoritative persistent halt file read by the current Research
runner; the example's blank halt path intentionally prevents a false PASS.
A private writer suspend switch, active-writer acknowledgement and immutable
image pin are required before any activation. Run the existing
\`paper readiness\` and \`strategy run --dry-run\` operator procedures first;
after a separately approved window, perform a supervised real-day smoke before
letting the CronJob own the schedule. Missing days remain real gaps.

This runner records plan-capture job statuses in \`datahub_job_runs\`, but
**does not automatically compute or certify a correct account NAV**. The
research NAV's current dividend/split/distribution gap remains a blocking
milestone-A accounting item, and automated NAV projection should be enabled
only after the corresponding ledger reconciliation tests pass.
