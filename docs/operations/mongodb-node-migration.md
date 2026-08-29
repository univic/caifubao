# MongoDB 单副本节点迁移流程（Runbook）

> 适用范围：把单副本 MongoDB StatefulSet（静态 local PV，`Retain`）从一个节点
> 迁移到另一个节点，数据零丢失、停机窗口约 40 分钟。流程源自 2026-08-29 dev
> 环境实战（`<源节点> → <目标节点>`，节点名脱敏），恢复路径已两次全量验证
> （约 4,500 万文档，0 失败）。
>
> 公开仓库只定义流程、模板形态与踩坑记录；真实节点名、存储类、桶名、凭据、
> 私有 overlay 细节属于 `caifubao-private`。配套模板：
> `k8s/base/mongodb-backup.yaml`、`k8s/base/mongodb-restore-job.example.yaml`，
> 备份/恢复语义见 `docs/operations/mongodb-resilience.md`。

## 适用场景

- 节点治理：节点反复失联、内存/磁盘容量不足、需要把 prod 与 dev 数据服务分置
  不同节点（避免"一个节点挂了两个环境同时断"的共享命运）。
- 前提是目标节点**比源节点更稳/更大**；迁移本身不解决节点硬件问题。

## 前置条件（不满足不要开始）

1. **已有可用备份**：`mongodb-s3-backup` CronJob 曾成功执行，且至少完成一次
   restore 演练（`mongodb-resilience.md`）。迁移 = 备份 + 恢复，不能没有备份兜底。
2. **目标节点磁盘余量 ≥ 数据量 × 1.5**：实测数据 7.5Gi 时，20G 盘（含镜像/日志
   已用 12G）放不下；先 `df -h` 确认真实余量，必要时挂新盘（挂载点沿用
   `/srv/caifubao`，`/etc/fstab` 用 UUID 持久化）。
3. **目标节点内存余量 ≥ mongod limit + restore 尖峰**：建议 limit 2Gi；
   实测 1.25Gi 会在恢复后段 OOMKilled（exit 137）。节点空闲内存不足 2Gi 时
   不要选它。
4. **基线记录**：迁移前记录数据库全部集合的计数（恢复后逐项比对）。
5. **应用用户核验**：mongo 官方镜像 entrypoint 只创建 `root`；应用用户
   （`caifubao_app_<env>`）需要在切流后重建。先从旧数据核验其角色
   （通常为 `readWrite` on 应用库），密码沿用 secret 里的根密码。

## 步骤

### 1. 新鲜备份

```bash
kubectl create job --from=cronjob/mongodb-s3-backup mongodb-backup-manual-$(date +%s) -n <ns>
kubectl wait --for=condition=complete job/<job> -n <ns> --timeout=3600s
kubectl logs job/<job> -n <ns> | grep -E '"status"|object_key'
```

必须等 `"status":"succeeded"` 且拿到 `object_key`。备份 job pod 的标签
**不得包含 `app: mongodb`**（见踩坑 1），使用 `k8s/base/mongodb-backup.yaml`
修复后的模板或私有 overlay 同步后的 CronJob。

### 2. 目标节点准备

- 磁盘：格式化/挂载（如 `mkfs.ext4 /dev/vdb`、`mount /dev/vdb /srv/caifubao`、
  `/etc/fstab` 写 UUID 条目）；`mkdir -p /srv/caifubao/mongodb/<env>`。
- 静态 PV：`Retain`、`nodeAffinity` 指向目标节点、`local.path` 同路径
  （`kubernetes.io/no-provisioner` 存储类必须手动建 PV；`local-path` 会自动建
  目录但回收策略是 `Delete`，不推荐用于数据）。
- 新 PVC：**用新名字**（如 `mongodb-pvc-<node>`），避免与源节点旧 PVC 冲突；
  STS 的 `volumes[].persistentVolumeClaim.claimName` 改指向它。源 PVC/PV 保留
  作为回滚，不删除。

### 3. 切流（数据链停机开始，约 40 分钟）

```bash
kubectl scale sts mongodb -n <ns> --replicas=0
kubectl patch sts mongodb -n <ns> --type=json -p='[
  {"op":"add","path":"/spec/template/spec/nodeSelector","value":{"kubernetes.io/hostname":"<target-node>"}},
  {"op":"replace","path":"/spec/template/spec/volumes/0/persistentVolumeClaim/claimName","value":"mongodb-pvc-<node>"},
  {"op":"replace","path":"/spec/template/spec/containers/0/resources/limits/memory","value":"2Gi"}
]'
kubectl scale sts mongodb -n <ns> --replicas=1
kubectl rollout status sts/mongodb -n <ns> --timeout=300s
```

新实例首次启动由镜像 entrypoint 初始化 `admin.root`
（`MONGO_INITDB_ROOT_USERNAME/PASSWORD` 来自同一 configmap/secret，凭据不变）。
后端/数据服务的连接重试（`connect_to_db` 10×5s）覆盖该窗口。

### 4. 重建应用用户

```js
// 以 root 登录新实例（root 密码 = mongodb-secret 的根密码）
db.getSiblingDB("admin").createUser({
  user: "caifubao_app_<env>",
  pwd: "<同根密码>",
  roles: [{ role: "readWrite", db: "<应用库>" }]   // 以步骤"前置条件 5"核验结果为准
});
```

### 5. 流式恢复（归档不落盘，关键）

```bash
aws s3 cp "s3://<bucket>/<object_key>" - | \
  mongorestore --host <host> --port <port> \
    --username <user> --password <pass> --authenticationDatabase admin \
    --archive=- --gzip --drop
```

- **必须流式**（`--archive=-` 从 stdin 读）：2.3Gi 归档写 emptyDir 会把节点
  nodefs 压到 kubelet 驱逐阈值，恢复 pod 会被 Evicted（实测两次）。
- 恢复 job 的 pod 标签**不得包含 `app: mongodb`**（见踩坑 1）。
- `backoffLimit: 0`：`--drop` 重试会先删库再恢复，非幂等；失败手动重试。
- 恢复 job 资源参考模板 `k8s/base/mongodb-restore-job.example.yaml`
  （requests 100m/256Mi，limits 500m/1Gi，`activeDeadlineSeconds 7200`）；
  实测把 limits 内存提到 1.5Gi 也安全，可按节点余量上调。

### 6. 验证（全部通过才算完成）

1. **计数逐项 vs 基线**：每个集合 `countDocuments()` 与迁移前记录完全一致。
2. **端点**：`mongodb-service` 的 endpoints 只指向新 pod IP。
3. **数据链**：backend/datahub/frontend 均 1/1 Ready，日志无连接错误。
4. prod 环境额外：私有 overlay 的灾后验证脚本（如
   `scripts/verify-prod-after-outage.sh`，属 `caifubao-private`）+ `system bootstrap-check`。

### 7. 收尾 / 回滚

- 源节点 PVC/PV **保留数日**作回滚：回滚 = 把 STS 的 nodeSelector/claimName 改回
  旧值并 scale 1（旧 PV 的 claimRef 指向旧 PVC，对象未删除即可直接回）。
- 确认稳定后清理：删除源 PVC（`Retain` 下 PV 变 Released，宿主盘数据保留），
  由运维按需清理宿主目录；删除一次性迁移 job。

## 实战踩坑记录（2026-08-29 dev 迁移）

| # | 现象 | 根因 | 修复 |
|:--|:--|:--|:--|
| 1 | 备份/恢复中途 `connection refused`（间歇） | job pod 带 `app: mongodb` 标签 → 成为 `mongodb-service` 端点，客户端轮询到自己（无监听端口） | job 标签只保留 `job.caifubao.io/type`；公共模板已修复，私有 overlay 需同步 |
| 2 | 目标 20G 盘放不下数据 | 数据 7.5G + 镜像/日志后磁盘接近满 | 挂 50G 新盘至 `/srv/caifubao`，fstab 持久化 |
| 3 | restore 后段 mongod OOMKilled（exit 137） | limit 1.25Gi 对恢复写突发太紧 | limit 提到 2Gi（需节点空闲内存 ≥2Gi） |
| 4 | 恢复 pod 被 Evicted ×2 | 2.3Gi 归档落 emptyDir 触发 nodefs 驱逐 | 改流式（`aws s3 cp - \| mongorestore --archive=-`） |
| 5 | 新实例认证失败 | 镜像 entrypoint 只建 root，应用用户需重建 | 切流后按原角色重建 `caifubao_app_<env>` |

## 相关

- 备份/恢复语义与数据存活性分级：`docs/operations/mongodb-resilience.md`
- 备份 CronJob 模板：`k8s/base/mongodb-backup.yaml`
- 恢复 Job 模板：`k8s/base/mongodb-restore-job.example.yaml`
