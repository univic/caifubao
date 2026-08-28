# Caifubao 开发交底文档（2026-08-28）

> 本文档面向后续接手开发的 AI 代理（含 Codex）与协作者，总结截至 2026-08-28
> 的近期进展、当前主要问题、OpenSpec 状态与工作流指引。交接后请以本文档为
> 起点，配合 `AGENTS.md` / `RULES.md` / `skills/` 一起阅读。
>
> **规则权威**：所有安全、边界、纪律、验证、spec-gate 规则只定义在
> `RULES.md`；Git 工作流见 `.project-rules.md`。本文档不取代它们。

---

## 1. 项目概况速览

- **目标**：大 A 量化研究/教学/演示 MVP（非投资建议）。
- **技术栈**：Python Flask backend + datahub 数据服务 + Vue 3/Vite/Element Plus
  前端 + MongoDB + K3s + GitHub Actions。
- **模块边界**（`RULES.md#P2`）：
  - `datahub/` — 行情/因子/信号/评分/新鲜度/质量数据
  - `backend/` — Flask API、鉴权、轻聚合
  - `frontend/` — Vue UI，只消费 backend API
  - `k8s/` — 公开部署示例（真实 overlay 在私有 `caifubao-private`）
- **统一运维 CLI**：`./scripts/caifubao`（见 `docs/operations/agent-cli.md`）。
- **技能**：开发前先加载对应 `skills/<name>/SKILL.md`（caifubao-dev /
  datahub-data-quality / scoring-factor / scoring-validation / openclaw-integration）。

---

## 2. 近期进展（2026-08 下旬冲刺）

过去一周多完成了一条 **DataHub 行情链路重构**，全部已合并到 `develop`：

### 2.1 行情来源迁移到 Tushare（#116–#134）

| PR | 内容 |
|:---|:---|
| #116 | 股票历史源改为 HTTPS（原 HTTP 被封锁） |
| #117/#118 | 升级行情客户端；修复 BaoStock 分页截断 |
| #119–#122 | quote 刷新确定性、CI 校验组装模型、bootstrap 对重复 master/解码错误鲁棒 |
| #127 | 新增 tushare 股票历史源（`pro.daily`，按年窗口分页） |
| #128 | 固定 tushare==1.4.29（1.4.26 是 3.4 kB PyPI 空壳包） |
| #129/#131 | 每次调用限速（0.25s/次），压到 300 次/分限额内 |
| #130 | 新增 tushare 股票列表/universe 源（`pro.stock_basic` + 冻结日快照），绕开东财/新浪 spot |
| #132 | 快照驱动的每日增量更新（UPD 路径）：差 1 天的股票直接写 `pro.daily(trade_date)` 结算快照，不再逐只拉历史 |
| #133 | 容忍长期停牌股票历史拉取失败 |
| #134 | `docs/operations/agent-cli.md` 更新每日例行与数据源说明 |

**结果**：quote 每日增量从 ~5,200 次调用 / 30–40 分钟降到 ~3 次调用 / 分钟级，
08-25 全市场验证 OK 5208 / STALE 4（停牌）。FQ stale 路径也已改为每个目标日
一次 `adj_factor(trade_date)` 全市场快照并只写当日；首次计算、多日积压及
force/backfill 仍走逐股全历史，防止用最新日掩盖历史缺口。该路径尚待随新镜像
在 dev 验证调用数、覆盖率与写入量。

### 2.2 复权因子修复（#135，重要）

**历史 bug**：`fq_factor` 原本用每日 `close/previous_close` 累计当作复权因子，
导致因子每个交易日都跳变（sh600309 一年跳 124 次）、`close_hfq` 成为虚构价格
（真实 ~75 元，close_hfq=3255）。而评分/验证/信号全部经 `quote_price()` 优先读
`close_hfq`，**所有下游结果长期建立在虚构复权价上**。

**修复**：新增 `tushare_interface.adj_factor()`（`pro.adj_factor`），
`build_fq_factor_frame` / `update_code` 改用真实因子：
`fq_factor = adj_factor`（除权日外恒定），`close_hfq = close × adj_factor`，
open/high/low_hfq 同比例缩放，缺失因子日回退最近已知值，无因子数据回退 factor=1。

### 2.3 百分位评分（#137）

**背景**：FQ 修复后 50 只股票全链路验证显示评分与 20 日收益 **负相关**
（基线 corr=-0.256；网格搜索最优也只到 -0.178，且分数被压到 max=60，
BUY 绝对阈值 70 不可达 → 无买入信号）。

**改动**：
- 推荐逻辑改为**横截面百分位驱动**：当日/horizon 内 BUY=top 5%、
  WATCH=top 20%、AVOID=bottom 20%；单股路径保留绝对阈值回退。
- 新增 `score_all_stocks_ranked`：先收集全 cohort 原始组件值，逐组件
  横截面 rank 归一化到 [0,1] 再加权求和，使分数横截面可比；
  通过环境变量 `DATAHUB_SCORING_MODE=ranked` 启用，默认 `raw` 不变。

### 2.4 运维加固（#111–#112）

- MongoDB tools 镜像发布（#111）、workload identity 加固（#112）。
- 每日例行（工作日 18:00 后，Asia/Shanghai）：
  prod quote(18:00) → signal(18:30) → scoring(18:35)，dev data-sync 19:15（prod signal/scoring 之后，dev 拿到当日信号；评分不同步，dev 需手动 scoring_runner）。

---

## 3. 当前主要问题

### 3.1 未完成的运营任务（OpenSpec 中已登记，尚未执行）

以下两项已在代码/评审层面完成，但 **部署与全市场验证未做**：

1. **`fq-adj-factor-fix`（#135）**：
   - [ ] 5.1 发布镜像；dev 部署验证 → prod 部署
   - [ ] 5.2 用真实 adj_factor **重算全市场 FQ 因子**（存量数据仍是虚构复权价）
   - [ ] 5.3 重跑 50 只股票评分+验证实验
   - [x] 1.2：`adj_factor()` 已接入共享的逐请求有限重试；重试耗尽、空响应
         或全无效响应会使该 code 失败且不写入，不再静默降级为 factor=1。

2. **`scoring-percentile-rank`（#137）**：
   - [ ] 5.1 发布镜像；dev 部署验证
   - [ ] 5.2 重跑 50 只股票评分+验证，对比 corr vs 基线
   - 注意：`DATAHUB_SCORING_MODE=ranked` 目前**未在公开 k8s 清单中启用**
     （默认 raw）；是否在 prod 启用由私有 overlay 决定。

### 3.2 评分有效性待验证（核心业务风险）

FQ 修复后评分与收益仍为负相关（见 2.3），百分位推荐是当前缓解手段，
但**模型成功标准（`capability-inventory.md` 中的 20.x / 12d.5 项）从未
在全市场验收**。问题 #125 已登记这些 carry-forward 项：
- 全市场 backfill + 新旧模型版本校准对比
- 评分测试补充（signal decay / hybrid threshold 已部分补上，见 #99/#137 测试）
- sz000977 consensus + optimize vs 基线验证
- 成功标准验收（全市场样本、walk-forward decay、最小样本/集中度、校准对比）

### 3.3 分支与发布状态

- `develop` 领先 `main` **73 个提交**（最后一次 promote 是 #124，2026-08-24）。
  main 缺：tushare 迁移、FQ 修复、百分位评分、相关文档。
- 当前工作分支 `codex/scoring-tasks-done` 与 origin 偏离 1 ahead / 1 behind
  （内容一致，仅提交哈希/合并方式差异），建议先 `git pull --rebase` 对齐。
- 发布流程：develop 镜像构建成功 → deploy-dispatch 发 private overlay；
  main 发布需显式 release PR（如 #124 模式）。

### 3.4 文档/能力清单过时

- `docs/capability-inventory.md` 最后更新 2026-05-22，P0/P1/P2 表有多项已过时
  （如"data_as_of 从未填充"——OpenClaw 端点现已填充；决策日志 UI 已实现，
  见 #108）。**建议下次顺手刷新**，把 tushare 源、快照 UPD 路径、FQ 修复、
  百分位评分补进去。
- `openspec/config.yaml` 的 context 未提及 tushare/百分位评分（见 §5）。

### 3.5 性能与 CPU 热点盘点（2026-08-28）

对 prod→dev 同步 / factor / signal / scoring 四条链路完成了全量性能审查
（只读，未改代码）。P0 结论：sync 夜间无水位线全量重灌（30–90 分钟/晚）、
signal 无增量全历史重算（~3,000 万级写/晚）、scoring 逐股 N+1 且 prod 每日
双跑、FQ 全历史回填被 tushare pacing 钉死（**阻塞上面 §3.1 待办 5.2 的
全市场 FQ 重算——先落地 `datahub-perf-optimization` 的 3.9–3.12 再执行重算**）。
完整发现清单（S/Q/F/G/C/W 编号 + file:line + 量级 + 修复）见
[`perf-analysis-2026-08.md`](./perf-analysis-2026-08.md)；分阶段任务清单见
`openspec/changes/datahub-perf-optimization/tasks.md`，后续性能工作以该
change 为准。

---

## 4. OpenSpec 状态（2026-08-28 实况）

`openspec validate --all --strict`：**5 项全部通过**。

| change | 状态 |
|:---|:---|
| `datahub-snapshot-daily-update` | ✅ 全部任务完成（含部署/验证） |
| `datahub-tushare-source` | ✅ 全部任务完成（含部署/验证） |
| `datahub-tushare-universe` | ✅ 全部任务完成（含部署/验证） |
| `fq-adj-factor-fix` | 🟡 代码+评审完成；1.2 / 5.1–5.3 未完成（见 §3.1） |
| `scoring-percentile-rank` | 🟡 代码+评审完成；5.1–5.2 未完成（见 §3.1） |

MVP 变更已归档为契约账本（`openspec/archive/mvp-quant-demo/`），
carry-forward 项挂在 GitHub issue #125。

### 需要的更新

- [x] **`openspec/config.yaml` context**：本次已补充 tushare 数据源、快照更新、
  FQ 修复、百分位评分（见 §5 已改）。
- [ ] **`docs/capability-inventory.md`**：见 §3.4，建议后续刷新。
- [ ] **归档**：3 个已完成的 datahub change 在后续发布稳定后可归档
  （沿用 `openspec archive` 流程，与 mvp-quant-demo 一致）。

---

## 5. 给接手方（Codex 等）的工作指引

1. **先读**：`AGENTS.md` → `RULES.md` → `.project-rules.md` →
   `docs/operations/agent-cli.md` → 本文档。
2. **技能前置**：进入 domain 任务前加载对应 `skills/<name>/SKILL.md`。
3. **Spec Gate**：改动涉及 API 契约/鉴权/新鲜度/评分/数据所有权/公开文档时，
   先跑 spec-guardian，按需新建 `openspec/changes/<name>/`。
4. **最小可用验证**：写完即跑最小验证（后端 `make test-backend`、
   datahub pytest、前端 `make test-frontend`），有 bug 先写复现测试。
5. **评审**：非平凡改动必须过 qa-reviewer（契约改动加 contract-reviewer），
   合并前做 branch-conflict 检查，Draft PR + CI 全绿后再 merge。
6. **建议的下一步**（按优先级）：
   1. 发布新镜像 → dev 部署，验证 FQ 日快照为每目标日一次请求且只写当日；
   2. prod 部署后全市场 FQ 重算 + 重跑 50 只股票评分/验证实验，对比 corr
      （注意：重算前先按 `datahub-perf-optimization` 任务 3.9–3.12 把回填
      改为按交易日快照路径，见 §3.5）；
   3. 决定 prod 是否启用 `DATAHUB_SCORING_MODE=ranked`（私有 overlay）；
   4. 刷新 `docs/capability-inventory.md`；评估归档 3 个已完成 change；
   5. 按 `openspec/changes/datahub-perf-optimization/tasks.md` 推进性能
      优化（先做阶段 0 度量基线与阶段 1 快赢项）。

---

## 6. 关键命令备忘

```bash
./scripts/caifubao system health          # 健康检查
./scripts/caifubao data sync <DATE>       # prod→dev 同步
./scripts/caifubao data refresh-status    # 刷新新鲜度
./scripts/caifubao score score-all <DATE> # 全市场评分
./scripts/caifubao score score-one <CODE> --date <DATE> --horizon 5
./scripts/caifubao system bootstrap-check # 数据组装检查
openspec validate --all --strict          # OpenSpec 校验
```

---

## 7. 2026-08-29 凌晨运维事件记录（补充）

### 7.1 生产事件：vm-4-12 节点三次失联（需运维）

- **13:20Z / 17:18Z（1.5h）/ 03:25Z（截至 04:16 未恢复）三次节点级故障**
  ——网络间歇性通断（ping 时通时断）、kubelet 心跳中断、SSH 不可达。
- 承载 **prod+dev 双环境 MongoDB 与 backend**，单点故障 = 全环境数据/API 不可用。
- 数据安全：PVC 本地盘，每次恢复后数据完好（前两次已验证）。
- **需运维/云控制台物理排查**（网络硬件/配置）；恢复后执行
  `scripts/verify-prod-after-outage.sh` 一键验证。

### 7.2 本次连续工作交付（PR #147-#155，全部合入）

| PR | 内容 |
|---|---|
| #147 | 工作区 venv（Python 3.12.14）+ AGENTS.md 文档化 |
| #148 | health-watcher P0 告警（失败/新鲜度检查 + 示例 CronJob） |
| #149 | backend MongoDB 连接重试 + 60 个预存 backend 测试 CI 修复 |
| #150 | 节点事件记录 + post-outage 验证脚本 |
| #151 | sync 业务键 upsert（E11000 修复，dev 实测） |
| #152 | main 发布（#147-#151 到 main，prod datahub 已部署） |
| #153 | datahub MongoDB 连接重试（补全 #149） |
| #154/#155 | roadmap 状态更新 + TUSHARE_TOKEN 缺口记录 |

### 7.3 改善效果实证（#144/#146 拆分）

- quote-stock：2h 超时被杀 → **70 秒完成**（5 phases，无信号/评分）
- quote-index：恢复后 5m55s 补 3 天指数数据（562 指数全部 08-28）
- 进度持久化/依赖门：被杀进程的 run 记录保留阶段证据（实证）
- dev data-sync：E11000 失败 → 业务键合并成功（modified=1184+2868）

### 7.4 待运维介入（无法自主解决）

1. **vm-4-12 节点恢复**（物理排查）→ verify-prod-after-outage.sh
2. **私有仓库 caifubao-private 补 `TUSHARE_TOKEN`**（datahub-secret 缺失，
   08-29 临时置空降级，tushare 行情源待恢复）
3. **私有仓库修 backend 部署 workflow**（deploy-dispatch 对 backend 不生效）
4. **周一 09-01 18:00** 自动链路确认（quote → signal 18:30 → scoring 18:35
   → dev data-sync 19:15）
