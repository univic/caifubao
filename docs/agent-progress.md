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
