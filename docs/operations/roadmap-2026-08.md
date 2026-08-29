# 后续开发方向（2026-08，稳定性 + 实盘指导意义）

基于 2026-08-28 生产事故诊断（行情任务超时被 killed、信号/评分停更、prod-dev
同步失败、节点不稳定）与 #144/#146 修复（进度持久化、数据感知依赖门、CronJob
拆分、dev 同步时序）之后的后续建议。目标：**系统稳定性** 与 **实盘指导意义**。

## 一、稳定性方向（按优先级）

### P0 — 失败即告警（当前最大缺口）
08-26~08-28 连续 3 个交易日任务失败无人知晓，全靠人工巡检发现。
- **✅ 已实现（2026-08-29）**：`datahub/app/jobs/health_watcher.py` —— 检查
  `datahub_job_runs` 近 26h FAILED 任务（quote/signal/scoring/data_sync）与
  `data_asset_status` STALE/NO_DATA 资产，输出 JSON 报告；`--fail-on-issues`
  使 CronJob 失败可见；可选 `HEALTH_WEBHOOK_URL` webhook 通知（私有 overlay
  配置真实地址）。示例 CronJob：`caifubao-datahub-health-watcher`
  （工作日 20:00，收盘链路 19:15 之后）。
- **待办**：① 私有 overlay 配置 `HEALTH_WEBHOOK_URL`（钉钉/邮件）；② 告警阈值
  与误报调优（如停牌股 STALE 豁免策略）。

### P1 — 节点治理
- `ubuntu-5700x` 失联超 1 个月（07-25 起 kubelet 停止上报），应**下线清理**
  （保留 NoExecute taint 已驱逐其上 pod，直接删除节点对象）。
- `racknerd-0ab4159` Ready 但被 cordon（08-23），确认后 **uncordon 或明确下线**。
- **`vm-4-12-ubuntu` 2026-08-28/29 多次节点级故障**（13:20 快速恢复；17:18Z 起
  网络失联 40+ 分钟，ping 100% 丢包；08-29 03:25Z（2.3h）与 04:42Z 再次失联）
  ——曾承载 **prod+dev 双环境的 MongoDB 与 backend**，单节点故障 = 全环境
  数据/API 不可用，是当时最大单点风险。
- **08-29 处置进展（见 mongodb-node-migration.md）**：① 两环境 mongodb STS
  原均 `nodeSelector` 硬绑定 `vm-4-12`（local-path PVC 亦绑定），其他节点健康
  也无法自动迁移；② T0 止血（探针/`wiredTigerCacheSizeGB=0.3`/内存 limits/
  `swappiness=1`/eviction-hard）消除 swap 风暴；③ **dev MongoDB 已迁至
  vm-8-15**（流式恢复 44.9M 文档 0 失败），**prod 暂留 vm-4-12**（3 交易日
  观察后按 runbook 迁移）；④ 后续项：prod 迁移、副本集 HA（≥2 副本跨节点）、
  定时备份启用（首备份已成功）。
- 保证 ≥3 个健康 worker 节点冗余；评估节点内存水位（当前 28-58%）。

### P1 — MongoDB 大集合索引
- `stock_daily_quote`（~18M 文档）按 `date` 的 count/校验查询全扫描超时。
  补 `date` 单列索引（mongoengine 模型或运维建索引），校验阶段显著提速。

### P1 — 服务启动 MongoDB 就绪重试
- **✅ 已实现（#149 backend + #153 datahub）**：backend 与 datahub 的
  `connect_to_db` 均增加重试（10×5s），耗尽后保持 exit(1) 契约；08-28/29
  节点重启场景不再崩溃循环（datahub 侧未配 connect/serverSelection 超时，
  最坏窗口 ~350s，可接受）。

### P2 — 镜像仓库稳定性
- 腾讯云 CCR（hkccr.ccs.tencentyun.com）拉取间歇性 `connection reset by peer`
  （smoke job 卡 ContainerCreating 4+ 分钟、dev 曾 33 分钟）。评估：
  ① containerd registry mirror / 国内镜像缓存；② 备用 registry；③ 拉取超时与
  重试增强（imagePullPolicy/containerd 配置）。

### P2 — 行情任务观测与 deadline 对齐
- 阶段级进度持久化已落地（#144），可基于 `datahub_job_runs.phase_stats`
  建立阶段耗时基线与超期告警（如 check_stock_data_integrity > 60min 告警）。
- 观察拆分后 quote job（行情+FQ+MA）实测耗时，`activeDeadlineSeconds=7200`
  是否可收紧到 3600（任务更短 → 被杀窗口更小）。

### P2 — data-sync 补跑自动化
- dev data-sync 失败后（如 18:38 的 MongoDB refused）无自动补跑，次日 19:15
  才重试。方案：失败时启动一次性补跑 job（backoffLimit 已有 1，可加
  startingDeadlineSeconds 内的重试），或失败告警后手动 `scripts/caifubao data sync`。


### P1 — prod datahub-secret 缺 TUSHARE_TOKEN（2026-08-29 发现）
- prod datahub 部署（sha-4d288a9）CreateContainerConfigError：`datahub-secret`
  无 `TUSHARE_TOKEN` key（历史上有，08-23 创建后某次更新丢失/私有 secrets 源缺失）。
- **临时降级**：datahub deployment 将 TUSHARE_TOKEN 置空 + 修正 configmap 引用
  （backend-config-7d4gbmm54t），pod 恢复 Running。
- **影响**：tushare 行情拉取因 token 缺失失败（quote 任务会失败，可由 health-watcher
  告警发现）。
- **待办**：运维在私有仓库 caifubao-private 补 TUSHARE_TOKEN（真实 token），
  重新部署 datahub 恢复 tushare 行情源。

## 二、实盘指导意义方向

### P0 — 评分验证闭环自动化
- `scoring_runner verify`（预测 vs 实际收益）目前手动跑。建议：每日收盘后自动
  verify + 输出校准报告（命中率/IC/分位收益），并同步到决策台。

### P1 — 数据新鲜度 SLA 告警（实盘输入可信度）
- 行情/因子/信号/评分各自有"最晚更新时间"要求，落后即告警（与 P0 告警复用
  同一 watcher）。实盘建议依赖 `data_asset_status` 而非任务状态。

### P1 — 信号/评分质量指标
- 信号命中率、评分分层收益（top/bottom 分位）的日更统计（决策台已有部分），
  形成固定周报；模型漂移检测（已有 miscalibration flags）接入告警。

### P2 — 回测可信度增强
- walk-forward / 置换检验已实现；建议固定基准（如沪深300 + 中证500）与
  样本外区间，评分重校准（Score5/20/60）后自动重跑验证。

### P2 — 决策日志审计
- 推荐/决策的生成依据（因子快照、评分、阈值）完整审计日志（decision journal
  已有基础），支持复盘与合规。

## 三、执行建议

1. **先做 P0 告警**（投入小、收益最大——避免再次"3 天无人知"）。
2. **节点治理 + MongoDB 索引**（运维动作，周末窗口执行）。
3. **观测 1-2 个交易日的拆分后链路**（quote ≤30min、signal/scoring 正常、
   dev 19:15 同步），据实测校准告警阈值与 deadline。
4. 实盘方向以"验证闭环 + 新鲜度 SLA"优先，评分/信号质量指标次之。

### P2 — Backend 测试 CI 连接修复（2026-08-29 发现）
- **问题**：backend 测试套件首次在 CI 全量运行（#149 触发 Backend Tests）暴露 60 个
  预存失败（test_decision_journal_api / test_decisions_dashboard / test_factor_eval_api
  等 API 测试）：client fixture 路径下 mongoengine 查询报
  `You have not defined a default connection`，而 conftest 的 lazy connect
  （`alias=default`）本地验证注册正常（pytest 9.1.1 vs CI 8.3.4）。此前所有 PR 的
  Backend Tests 均因 Detect Changed Areas 判定未触及 backend 而 skipping。
- **影响**：任何 backend 改动触发 Backend Tests 即红，阻塞合入（#149 已标注）。
- **待办**：专项诊断 CI 环境下 conftest 连接初始化时序（可能 pytest 版本/requirements
  依赖差异），修复后 backend 测试可稳定在 CI 运行。

### P1 — backend 与 MongoDB 硬亲和限制故障转移（2026-08-29 确认）
- backend Deployment 配置 `podAffinity requiredDuringScheduling`（app=mongodb，
  topologyKey=hostname）→ MongoDB 所在节点故障时 backend 无法调度到其他节点
  （实测：vm-4-12 失联期间 backend 新副本 Pending 32 分钟，0/5 节点可用）。
- **建议**：评估改为软亲和（preferredDuringScheduling）或移除亲和，让 backend
  在 MongoDB 故障时至少可调度（连接层面由 connect_to_db 重试兜底）。
