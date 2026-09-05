# Agent 共享进度日志

供多个 agent 快速同步当前进展。新记录必须插入到“进度记录”顶部，按时间倒序排列。

## 记录规则

- 时间使用 `YYYY-MM-DD HH:mm CST`。
- 每条只记录：状态、已完成、验证、下一步、阻塞。
- 状态使用 `进行中`、`已完成` 或 `阻塞`。
- 写清相关公共 PR、提交或文件；避免重复完整交接文档。
- 不记录凭据、内部地址、真实域名、私有 overlay 或其他敏感部署信息。
- 接手任务前先读最新几条；完成阶段性工作后及时更新。

## 条目模板

```markdown
### YYYY-MM-DD HH:mm CST — <简短主题>

- 状态：进行中 / 已完成 / 阻塞
- 已完成：<关键结果>
- 验证：<已运行的检查及结果>
- 下一步：<明确动作；没有则写“无”>
- 阻塞：<阻塞原因；没有则写“无”>
```

## 进度记录

### 2026-09-05 15:10 CST — 3.2 全链路完成 + 三门禁 GATE_OK + PR #188 CI 绿

- 状态：进行中（PR #188 draft 就绪；待用户合并批准后完成 task 3.2/4.4）
- 已完成：承接 codex 14:15 条目，将 config 化 percentile 基座全链路落地并提交
  `822c6ba`（datahub calibration/comparison/experiment_service、compute-worker、
  backend score_experiments、frontend、openspec spec/tasks，12 文件 +826/-94）：
  配置解析 basis（`config_bucket_basis` 读 registered/experiment config 的
  directions，翻转即 percentile），正分窗口不再靠运行时负分探测；无效 percentile
  显式失败（datahub ValueError / backend 稳定 422）；跨方向比较统一 basis 并抑制
  raw avg_score 差值；创建 Draft PR **#188**（CI 全绿：Backend/Datahub
  Tests+Lint、Frontend、OpenSpec 10/10、Private Deploy Dry Run 均 pass）。
- 验证：三门禁全部 **GATE_OK 无 P1**：spec-guardian（沿用既有 change 正确、
  spec 场景与实现一致、10/10 validate、60 tests）；contract-reviewer（basis
  字段/类型/422 信封一致、additive 向后兼容、7/7 backend + 13/13 datahub）；
  qa-reviewer（472 passed、vue-tsc clean、默认配置下与 develop 数值一致、
  负尾不丢失）。P2 收敛于同一缺陷：datahub report 把空 `{}` config 当权威值，
  压掉注册表回退（operator CLI 路径仍会按 raw score 误分桶正分窗口），已修
  `518eb48`（calibration/comparison `_resolved_scoring_config` 空 config 回退
  注册表 + 2 个回归测试 + tasks.md/spec.md 补回 threshold 消费者
  default-direction-only 子项 + scores.py 过期注释）；474 passed、ruff CI 级
  规则 clean、openspec 10/10。
- 下一步：用户批准后 merge #188（squash）→ develop，勾选 tasks 3.2/4.4；
  task 3.3（一个翻转 model_version 全市场 replay + 校准对比 vs baseline）为
  独立后续任务，需 operator 执行。
- 阻塞：PR #188 合并需用户显式批准（未 promote 任何模型版本，线上默认评分不变）。

---

### 2026-09-05 14:15 CST — 接手推进：翻转评分校准改用 percentile，负尾不再丢失

- 状态：进行中（`codex/fix/flipped-score-calibration` 本地实现与验证完成；review/PR 门禁待完成）
- 已完成：接手复核 #182-#187 后，先完成 scoring-direction-versioning 3.2：
  `ScoreCalibrationReport` 遇到负分 cohort 时强制使用 percentile×100 做分布、分桶及
  false-positive/false-negative 取样，缺 percentile 则显式失败；
  `ExperimentComparisonReport` 在任一侧为 signed score 时让候选/基线统一使用 percentile，
  标注 `bucket_basis`/`comparison_basis`，并禁止输出跨方向的 raw `avg_score` 差值；默认正分
  模型继续使用原 0-100 score 口径。同步补充 OpenSpec 场景并勾选 task 3.2。
- 验证：先新增失败测试（4 failed）再实现；聚焦 5 tests passed；datahub 全量
  **464 passed**；相关 ruff/format 通过；`openspec validate --all --strict` **10/10 passed**；
  `git diff --check` 通过；与最新 `origin/develop` 的 branch-conflict 检查 clean。
- 下一步：执行 spec-guardian + contract-reviewer + qa-reviewer，完成 Draft PR CI；随后进行
  task 3.3（一个翻转 model_version 的全市场 replay + 新旧校准对比），再独立处理模型注册表
  真不可变、`scoring_mode` 绑定和 fail-closed。
- 阻塞：强制 reviewer 门禁（含新增 basis 响应字段的 contract review）尚未执行，因此当前
  分支不得标记 ready/merge；未 promote 任何
  模型版本，也未改变线上默认评分。

---

### 2026-09-05 13:30 CST — codex 第一阶段完成：方向缺陷修复 + 模型注册表 + 版本约束 + 记录同步

- 状态：已完成（#182-#187 合并/就绪；codex 三阶段第一阶段 #1-#5 全闭环）
- 已完成：
  - **codex 5 点分析评估**：全部核实——#1 方向版本化 full-flip 塌缩缺陷（qa+spec-guardian
    独立发现：全分量 -1 时分数钳到 0 平局）已修（polarity-aware floor：默认模型保留
    max(0,·) 位一致、真实翻转才开放下界保序）；#2 模型未绑配置、#3 收益非资本约束、
    #4 决策接口混版本、#5 记录未同步均成立。
  - **#183 方向版本化**（merged）：config 支持 per-horizon directions + ranked 应用 +
    openspec change；P1（极性钳制）/P2（消费端前置）/P3 全处理，spec-guardian GATE_OK、
    qa APPROVE。
  - **#185 模型注册表**（merged）：ScoreModelVersion（config_hash/ACTIVE-RETIRED 不可变）
    + ScoringService 配置优先级（显式>注册>内置）+ 注册 CLI + openspec；CI 覆盖 datahub
    model 的坑（backend 为权威源）已修。
  - **#186 决策版本约束**（merged）：decisions.py 5 个生产视图绑定单一 model_version（默认
    DEFAULT、非法 override JSON 400）；backend 不能依赖 datahub lib 的工程约束已解。
  - **#184 2025 归因**（merged）：极端小盘风格轮动（2025H1 全样本最强小盘溢价），非信号
    失效/数据伪影——反转信号活着，压缩的是尾部价差与组合 size 分层。
  - **#182 回测曲线**（merged）：BacktestResultView 加 ECharts 收益率曲线（策略 vs 基准 +
    买卖点）+ benchmark_daily_values。
  - **#187 记录同步**（本 PR）：state.yaml 阶段更新 + ledger 补录 flip_wide 官方 run +
    人工实验总账 manual-experiments-ledger.md。关键发现：flip_wide 官方窗口 validation IR
    **+0.385（正）**，仅因 decay 0.605（train 2024 +0.975 更强）超门 discard——非 IR 负；
    与扩展 walk-forward（decay 0.00）两协议一致支持 flip_wide。
- 验证：#183/#185/#186 CI 全绿（qa+spec-guardian 双审通过）；#184/#182/#187 全绿；
  datahub 460 + backend tests 过；openspec 10 changes 全过。
- 下一步：codex 第二阶段（paper-only strategy_runner、真实资本 NAV、flip_wide 宽基过滤器、
  废弃旧统计接口）；第三阶段前置（shadow 双版本、120 天 paper）待第二/三阶段启动。
- 阻塞：无。

---

### 2026-09-05 00:40 CST — H20 研究链闭环：多 regime 审计 + 基本面因子 + flip_wide 候选与可执行化

- 状态：已完成（#180 单股执行示例 + 收益率曲线；候选生产化待评分构造层版本化）
- 已完成：
  - **数据基建**：#175 tushare daily_basic 接入（stock_daily_basic：pe_ttm/pb/ps_ttm/dv_ttm/
    市值/换手，dev+prod 各 8,695,127 行 / 1,838 交易日 2019-2026，#176 注册进 prod→dev
    data-sync，端到端验证通过）。
  - **#174 多 regime 组件审计**（2019-2026 合并快照 9,203,459 行）：8 个技术分量在全部 8 段
    regime dailyIC 显著为负（含牛市）——反向是结构性、非 regime 依赖；COMPOSITE 买 top
    全 regime 亏（IR −0.11~−2.38）；momentum/relative/real_relative 三合一冗余（40% 权重
    同一押注）。
  - **#177 基本面因子审计 + 构造层翻转 walk-forward**：估值因子（EP/BP/SP/DV）2019-2026
    全程弱到中等正 IC 且 size 中性后不消失（非 size 代理）；turnover 恒负 IC。评估器口径
    发现 selection 宽度决定性——**flip_wide**（8 分量构造层全翻转 + 宽书 800 只）是唯一全
    窗口正候选（train +0.485 / val2024 +0.975 / val2025 +0.385 / test2026H1 +0.426，
    decay 0.00）；baseline current_h20 扩展 walk-forward decay 2.11 硬失败。
  - **#178 估值融合实验**：估值块（EP/BP+低换手）混入 flip_wide 破坏 2025 稳健性
    （+0.385→−0.742 @w=0.5）→ 不加；估值作 regime 条件块或独立组件。
  - **#180 flip_wide 可执行化**：单股反转择时（flip_pct≥0.90 买入 / ≤0.30 卖出，T+1 开盘、
    摩擦、停牌跳过）+ 收益率曲线脚本 `scripts/h20_flipwide_single_stock.py`。示例
    sh600519（2024-2026）：+15.33% vs buy&hold −11.78%（超额 +27.11%，18 笔）。
  - **架构文档**：architecture-layers-strategy-design.md（五层单向压缩流水线 + 策略层
    缺口/实现建议）、research-progress-2026-09-04.md（阶段总结，修正 summary.md 旧结论）。
- 验证：#177/#178/#179 全 CI 绿；flip_wide 单股模拟 T+1 无前视已校验（买入 flip_pct 为
  T-1 信号日值）；快照与 daily_basic 行数核对一致；ruff 0.15.15 通过。
- 下一步：候选生产化的桥=生产 scoring 构造层版本化（方向/权重按 model_version 声明，
  让 flip_wide 语义可跑生产评分）；策略层 strategy_runner（paper-first ≥120 天）；
  2025 异常年归因；估值因子作 regime 条件块再验证。
- 阻塞：无（评分生产数据停更 08-28 待恢复）。

---

### 2026-09-02 12:20 CST — PR #169/#170 合并 + prod 08-31 评分补跑完成

- 状态：已完成
- 已完成：#169（h20 autoresearch bootstrap）与 #170（score-driven 回测执行安全 T+1）均已 squash 合入 develop（GitHub CI 全绿）；prod 08-31 scoring 补跑成功——用一次性 Job 跑 `scoring_runner backfill --from/to 2026-08-31 --model-version score_v2_202605b`，写入 16,659 条（h=5/20/60，status PENDING 15,618 / BLOCKED 1,041），prod 评分日期现覆盖 08-26/27/28/31（共 61,062 条）。
- 验证：#170 评审 contract/qa 无 P1（P2 已修复：compare 改条件化、TOP_N 止损/调仓时序、top-level model_version 字段补入 OpenSpec）；#169 CI 全绿；补跑后按 status/horizon/model_version 核验 08-31 数据。
- 下一步：观察 09-02 正常评分链路（#167 依赖门修复已部署）；dev 08-31 评分由日常 data-sync 自动同步；h20 autoresearch-loop 是否继续待用户决策（建议等生产评分积累 ≥120 交易日）。
- 阻塞：无（研究侧对 dev industry 数据缺失保持已知限制，结论方向不变）。

---

### 2026-09-02 02:40 CST — H20 autoresearch bootstrap 完成（snapshot + baseline）

- 状态：已完成
- 已完成：`codex/autoresearch-h20-bootstrap` 分支完成 H20 研究管线 bootstrap——只读导出全市场快照（3,525,955 行 / 3,159,736 可交易，2024-01-02～2026-07-31，manifest 记录 sha256 与 source_model_version=score_v2_202605b）；validation 基线 current_h20 已跑，score=-999（discard）。修复评审 P1/P2：权重改为按实际和归一（生产 H20 权重和=110 而非 100）；signal_strength/breakout_or_position/industry_momentum 改发 normalized_value（修复 3/8 组件导出为 NaN 导致 25% 权重失效）；walk_forward_decay 由 train-vs-validation IR 真实计算（不再硬编码 0）；annual_turnover 修正为 252/horizon；parity 改用 _build_components 比对数值。性能：real_relative_strength 每代码预计算、上市日按批次推导、历史/信号按 lookback 截断、date→index 映射，全量导出从不可行降至约 55 分钟。
- 验证：parity 0 mismatch（50 行 × 8 组件最大误差 0.0）；datahub 388 测试、ruff、`openspec validate --all --strict` 全绿；spec-guardian 与 qa-reviewer 已跑（P1 已修复）。
- 下一步：`autoresearch-loop` 阶段按需评估 full_reversal / exclude_d8_d9 两个不可变对照（需先决策是否继续，因 validation IR=-0.80 与既有反向结论一致）。
- 阻塞：基线被硬门挡回（117 个可交易日 < 120；walk_forward_decay 5.80 > 0.20），不是管线故障而是样本/过拟合门按设计触发。

---

### 2026-09-01 13:55 CST — 5700X dev 迁移收口 + 增量 data-sync 上线

- 状态：已完成（prod 08-31 评分补跑待授权）
- 已完成：
  - dev 工作负载已整体运行在 5700X；镜像通过 K3s Spegel 从云内节点分发，
    5700X 无需直连 TCR。首次大层冷传约 0.3–0.5 MB/s，缓存命中后的发布可复用层。
  - PR #164（增量 data-sync）与 #165（PyMongo `Cursor.sort` 热修复）已 squash
    合入 develop（`91b8b85`、`211a781`）；日常同步使用 3 天重叠窗口、每集合
    bootstrap marker/watermark、日期索引预检、最新日期优先读取及 3 小时截止线。
  - dev 三个 dated collection 从已验证的 08-28 恢复基线播种 marker；热修复镜像
    上线后真实增量 Job 92.4 秒完成：read 70,203、upsert 14,348、modified 2,961，
    三个 watermark 均推进到 08-31；`data_asset_status` 已刷新 39,419 条。
  - prod 只读检查：08-31 quote/factor/signal 分别为 5,403/5,203/3,741 条，
    与 dev 完全一致；prod score predictions 仍停在 08-28（16,653 条）。08-31
    scoring 于 10:35:01 启动时 signal 尚在 RUNNING，signal 于 10:35:24 成功，
    形成 23 秒竞态，scoring 因 `dependency_failed` 被 SKIPPED。
- 验证：datahub 全量 389 tests passed；两次公共 PR CI 全绿；qa-reviewer 无
  P1/P2；热修复部署与 CronJob 镜像一致且 Pod `1/1 Running`；真实 Job 状态
  SUCCESS；dev/prod 三集合最新日期与当日条数逐项一致；dev 状态分布为
  OK=36,968、NOT_APPLICABLE=2,451。prod 状态为 OK=52,547、STALE=6、
  NOT_APPLICABLE=63，6 个 STALE 均为个股 quote 落后预期日期。
- 下一步：获明确授权后补跑 prod 08-31 scoring；另行修复 signal/scoring 仅隔
  5 分钟导致的依赖门竞态，并观察下一个交易日日常增量 Cron。构建日期索引期间
  曾因并发大集合精确全量 count 触发 prod Mongo OOM restart 1 次，现已稳定；
  后续禁止在索引压力期间对数千万文档执行无必要的精确全量 count。
- 阻塞：prod 08-31 scoring 补跑会写生产数据，待用户明确授权；其余无。

---

### 2026-08-31 22:38 CST — Spegel 混合节点镜像分发上线

- 状态：已完成
- 已完成：5 个 K3s 节点已启用内置分布式镜像；dev 发布链路已改为由可访问上游仓库的云端节点预热不可变镜像，再通过 Spegel 向隔离的 5700X 节点分发；backend、datahub、frontend 均已接入，生产发布行为不变，COS 离线包保留为应急回退。相关私有部署仓库变更已合入 main。
- 验证：所有节点 Ready，原 cordon 节点保持禁调度；节点间 Spegel 所需端口双向可达；真实冷拉取在 5700X 上成功（首次 7 分 29 秒、缓存命中 0.08 秒），真实预热 Job 5 秒完成；prod/dev 核心服务与 MongoDB 均 Running 且重启数为 0；GitHub Deploy Dry Run 完整通过。
- 下一步：观察下一次 dev 三服务发版的端到端层复用与 rollout 时长；另行处理 dev data-sync 超过 3 小时 deadline，以及既有 prod 指数行情/评分更新问题。
- 阻塞：镜像发布链路无阻塞；跨节点首次缺失大层仍受链路带宽限制，稳定基础层可由缓存复用。

---

### 2026-08-31 21:52 CST — prod 08-31 数据更新核查

- 状态：阻塞
- 已完成：只读核查本轮生产定时任务与数据状态；股票行情任务成功（写入 25,705 条），信号任务成功（写入 3,741 条），行情/因子/信号资产最新日期均为 08-31。
- 验证：生产服务与 MongoDB 均正常；信号集合有 3,741 条 08-31 记录。指数行情任务失败（562 个拉取、197 条写入，连续 25 个历史行情拉取失败触发保护）；评分任务记录为 SKIPPED，08-31 新评分为 0，评分集合最新日期仍为 08-28。
- 下一步：修复评分任务对信号任务成功记录的依赖匹配后补跑 08-31 评分；复核并视需要重跑失败的指数行情任务。
- 阻塞：评分任务在 18:35 检查不到 18:30 已成功的 signal_daily 记录而跳过；需修复后才可恢复当日评分更新。

---

### 2026-08-31 21:49 CST — dev 工作面迁移至已恢复节点

- 状态：已完成
- 已完成：dev MongoDB、backend、datahub、frontend 与 dev CronJob 调度已统一迁至已恢复节点；为无公网镜像仓库环境预加载运行镜像并保留离线缓存；新本地存储完成持久挂载；对象存储备份的网络解析兼容已补齐。
- 验证：最终备份恢复 44,909,820 文档、0 失败；10 个集合与迁移前基线一致；应用用户认证、MongoDB/三项应用就绪探针与 Service Endpoint 均正常；普通 Pod 对对象存储的只读检查通过。
- 下一步：按既有运维策略决定是否解除 dev MongoDB 备份 CronJob 暂停；观察下一轮 data-sync 与 health-watcher 在新节点的运行结果。
- 阻塞：无；旧 dev MongoDB PV/PVC 已保留，用于回滚。

---

### 2026-08-29 21:00 CST — PR #162 合入 develop（策略实验 + 链路验证 + 08-28 断链修复）

- 状态：已完成（本轮目标闭环）
- 已完成：PR #162（`docs/operations/strategy-experiments-2026-08.md` +
  `scripts/decile-analysis.py` + agent-progress/handover/roadmap 更新）已 squash
  合入 develop（0627afd）；10 股 dev 全链路验证完成（08-28 行情/因子/信号全齐）；
  prod 08-28 断链（quote CronJob 因 TUSHARE_TOKEN 缺口 FAILED → signal/scoring
  依赖门 SKIPPED）已按调度时间手工补跑 quote→signal→scoring + dev sync 补齐；
  策略全样本反向结论定稿（IC=-0.1206、D9=-11.50% vs D0=-2.88%、分月稳定、
  S1 58% 跑赢市场，窗口下跌市无分位绝对为正 → 边际在回避 D8/D9）。
- 验证：PR #162 CI 通过、mergeable、已合并；dev/prod 08-28 数据全齐
  （prod scores 16,653）；全量 decile 脚本在迁移后 dev 重跑结果与迁移前完全一致。
- 下一步：A1（percentile 反向映射）实施与否待用户决策（实施需 Spec Gate）；
  09-01 周一自动链路前核查 prod cron controller（quote-index 08-24、signal/scoring
  08-26 后未调度 + bootstrap 僵尸 Job 触发 UnexpectedJob）。
- 阻塞：无。

---

### 2026-08-29 20:35 CST — 全链路验证完成 + prod 08-28 断链修复 + 策略结论定稿

- 状态：进行中
- 已完成：
  - **10 股全链路验证完成（dev，08-28）**：行情/因子/信号全齐（信号经 prod
    修复后同步）；10 股评分双口径记录（dev raw + prod score_v2_202605b rank）。
  - **prod 08-28 断链修复**：08-28 quote CronJob 因 TUSHARE_TOKEN 缺口 FAILED
    → signal/scoring 依赖门 SKIPPED → 08-28 信号仅 809/5,551。本次按调度时间
    手工补跑 quote（SUCCESS@10:00Z）→ signal（written 2,456，08-28 信号
    809→3,265）→ scoring（16,653 条）→ dev sync（upsert 2,456）→ dev 10 股
    信号补齐。
  - **策略结论定稿**（`docs/operations/strategy-experiments-2026-08.md`）：
    全样本（n=186,809）确认 ranked_v1_h20 与 20 日收益**反向**：Spearman
    IC=-0.1206；D9=-11.50% vs D0=-2.88%（价差 -8.63pp）；6 月价差 -7.64pp、
    7 月 -9.96pp（稳定）；S1（最低 20%）58% 评分日跑赢市场；该窗口下跌市下
    无分位绝对为正 → 边际在「回避 D8/D9 死亡区」；A1 反转映射提案（最小改动）
    已记录待审。
  - **dev 数据修复**：dev finance_market 重复 ChinaAStock 文档（迁移/恢复引入，
    `basic_stock` 引用与 `FinanceMarket.objects().first()` 不一致 → 全市场被当
    新股票重新 bootstrap，14h ETA）→ 已去重（删 6a8db392，6,113 引用统一指向
    6a8bb027，与 prod 一致）。
  - **新 ops 发现**：prod CronJob 控制器调度异常 —— quote-index lastSchedule
    08-24、signal/scoring 08-26（之后未调度）；08-24 bootstrap 僵尸 Job 触发
    UnexpectedJob 告警；09-01 周一自动链路前需核查 cron controller。
- 验证：dev 10 股 08-28 quote=1 factor=1 signal≥1；prod 08-28 scores 16,653；
  全样本 decile 脚本（scripts/decile-analysis.py）在迁移后 dev 重跑结果与迁移前
  完全一致（数据完好）。
- 下一步：提交 docs/strategy-experiments-2026-08 PR（含脚本）；09-01 观察自动
  链路；评估 A1 反转映射实施（Spec Gate）；核查 prod cron controller。
- 阻塞：无。

---

### 2026-08-29 18:40 CST — vm-4-12 失联处置完成：T0 止血 + dev MongoDB 迁移 + 首次备份

- 状态：已完成
- 已完成：
  - **T0 止血**（两环境 MongoDB）：探针 mongosh exec → tcpSocket、`wiredTigerCacheSizeGB=0.3`、内存 limits 收敛（prod 1Gi / dev 2Gi）；三节点 `swappiness=1` 持久化；vm-4-12 kubelet `eviction-hard`（memory<200Mi / nodefs<5% / imagefs<10%）——swap 风暴机制已结构性消除，3h+ 观测 0 OOMKilled、节点稳定 28–58%。
  - **dev MongoDB 迁移**：从 vm-4-12 迁至 vm-8-15（新挂 50G 盘挂载 /srv/caifubao，fstab UUID 持久化），limit 2Gi；**流式恢复**（`aws s3 cp - | mongorestore --archive=-`）44.9M 文档 0 失败，9/9 集合计数与迁移前基线一致；旧 PVC/PV 留作回滚。
  - **首次备份**：dev + prod 的 COS 备份均成功（`mongodb-s3-backup` CronJob 此前从未运行过）。
  - **修复**：备份/恢复 job pod 的 `app: mongodb` 标签会污染 `mongodb-service` 端点（实测间歇 connection refused，含公共模板）——已改集群 CronJob + 公共模板；公共模板 `restore.sh` 改为流式；prod datahub 缺 `TUSHARE_TOKEN` 导致 CreateContainerConfigError，已从 dev secret 补齐。
  - **文档**：`docs/operations/mongodb-node-migration.md` 迁移 runbook（PR #161，含 qa-reviewer 审查）。
- 验证：kustomize base + 两示例 overlay 通过；qa-reviewer 通过（P1 已修复复核）；PR #161 的私有 overlay dry-run 需分支基于最新 develop（含 #158 backend 亲和修复）后复验。
- 下一步：PR #161 CI 绿后合并；私有 overlay 持久化标签修复 + 定时备份启用 + health-watcher `HEALTH_WEBHOOK_URL` 配置；prod 迁移暂缓观察（3 交易日全绿再定）。
- 阻塞：无。

---

### 2026-08-29 13:20 CST — 全量样本确认评分反向 + 策略文档就绪

- 状态：进行中
- 已完成：dev 全量 decile 分析（**186,809 样本**，ranked_v1_h20，06-01~07-21 评分日，
  20 日前瞻收益）确认评分与 20 日收益**负相关**：D9（最高分位）**-11.50%** vs
  D0（最低分位）-2.88%，价差 -8.63pp；D8/D9 为「死亡区」（-7.97%/-11.50%），
  D4 全样本最优（-1.29%）；S1（最低 20% 组合）58% 交易日跑赢市场，累计跑赢
  市场约 11.5pp，但该窗口（6-7 月下跌市）**无任何分位绝对收益为正** → 边际在
  「回避高位」而非「绝对盈利」。10 股全链路验证（行情/因子/信号 08-28 完整 +
  08-28 评分）已记录。
- 验证：`scripts/decile-analysis.py`（内存安全批处理，base64 管道避开 shell 对
  `$` 展开的坑——此前「0 行」实为 shell 转义 bug，非 MongoDB 问题）；dev 迁移后
  数据 9/9 集合与基线一致。
- 下一步：dev（现 vm-8-15）重跑含 Spearman IC 与分月稳定性的完整分析定稿；
  评估 A1（percentile 反向映射 → 低分位 BUY/高分位 AVOID，最小改动）是否实施
  （需 Spec Gate + 记录）；提交 docs/strategy-experiments PR。
- 阻塞：无（dev MongoDB 已迁至 vm-8-15，节点全 Ready；prod MongoDB 暂留 vm-4-12）。

---

### 2026-08-29 12:15 CST — 节点稳定确认 + #158 发布 main + TUSHARE 验证

- 状态：进行中
- 已完成：vm-4-12 恢复后稳定 6.5h+（心跳正常，无再次失联）；#158（backend affinity 解耦）已发布到 main（#159）；TUSHARE_TOKEN 已补入 prod datahub-secret 并验证有效（tushare 拉取 5551 只 universe，quote 冒烟 50s SUCCESS）；backend 已部署 #149 镜像（sha-2df83f0）并迁离 MongoDB 节点。
- 验证：datahub 368 测试通过；quote 冒烟 5 phases SUCCESS；verify-prod-after-outage.sh 数据链完好（行情/因子/指数/信号 08-28）。
- 下一步：部署 health-watcher CronJob 到 prod（私有 overlay）；周一 09-01 18:00 观察自动链路（quote→signal 18:30→scoring 18:35→dev 19:15）；持续观察节点/tailnet 稳定性。
- 阻塞：无。

---


### 2026-08-29 12:08 CST — Backend 与 MongoDB 调度解耦

- 状态：已完成
- 已完成：公共调度清单已将 backend 与 MongoDB 的硬共址约束改为软反亲和，并优先选择非控制平面节点；公共 PR #158 已合并。部署流程已补充现有 Deployment 调度策略同步，故障自建节点继续保持禁止调度。
- 验证：development 与 production rollout 均成功；两个环境的 backend 已迁离 MongoDB 节点；真实 MongoDB 数据质量查询均返回 HTTP 200；原 MongoDB 节点无内存、磁盘或 PID 压力。
- 下一步：持续观察节点与 tailnet 稳定性；MongoDB HA 暂缓；镜像仓库 mirror/备用 registry 保留为 P2。
- 阻塞：无。
