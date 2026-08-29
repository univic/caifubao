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
