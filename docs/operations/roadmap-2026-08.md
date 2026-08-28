# 后续开发方向（2026-08，稳定性 + 实盘指导意义）

基于 2026-08-28 生产事故诊断（行情任务超时被 killed、信号/评分停更、prod-dev
同步失败、节点不稳定）与 #144/#146 修复（进度持久化、数据感知依赖门、CronJob
拆分、dev 同步时序）之后的后续建议。目标：**系统稳定性** 与 **实盘指导意义**。

## 一、稳定性方向（按优先级）

### P0 — 失败即告警（当前最大缺口）
08-26~08-28 连续 4 个交易日任务失败无人知晓，全靠人工巡检发现。
- **方案**：`datahub_job_runs` 状态变化（FAILED/SKIPPED 且非预期）→ 通知
  （钉钉/邮件/webhook）。可用现有 `datahub_job_runs` 集合 + 一个轻量 watcher
  （datahub pod 内定时任务或独立 CronJob）。
- 告警规则：① quote/signal/scoring 任务 FAILED；② 数据新鲜度落后
  （`data_asset_status` latest < expected，利用现有 STALE 判定）；
  ③ data-sync FAILED。

### P1 — 节点治理
- `ubuntu-5700x` 失联超 1 个月（07-25 起 kubelet 停止上报），应**下线清理**
  （保留 NoExecute taint 已驱逐其上 pod，直接删除节点对象）。
- `racknerd-0ab4159` Ready 但被 cordon（08-23），确认后 **uncordon 或明确下线**。
- 保证 ≥3 个健康 worker 节点冗余；评估节点内存水位（当前 40-65%）。

### P1 — MongoDB 大集合索引
- `stock_daily_quote`（~18M 文档）按 `date` 的 count/校验查询全扫描超时。
  补 `date` 单列索引（mongoengine 模型或运维建索引），校验阶段显著提速。

### P1 — 服务启动 MongoDB 就绪重试
- backend/datahub 启动时一次连接失败即 exit（08-28 节点重启时 backend 崩溃循环
  2 次）。`connect_to_db` 增加重试（如 10×5s），消除节点重启引发的崩溃。

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

1. **先做 P0 告警**（投入小、收益最大——避免再次"4 天无人知"）。
2. **节点治理 + MongoDB 索引**（运维动作，周末窗口执行）。
3. **观测 1-2 个交易日的拆分后链路**（quote ≤30min、signal/scoring 正常、
   dev 19:15 同步），据实测校准告警阈值与 deadline。
4. 实盘方向以"验证闭环 + 新鲜度 SLA"优先，评分/信号质量指标次之。
