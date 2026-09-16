# docs/archive — 已过期文档归档

这里保存**已被取代或不再维护**的文档。保留而不删除，是为了可追溯（研究证据、
交接上下文、原始结论）。

**不要把归档文档当作当前系统的描述。** 判断当前状态请以
[`docs/README.md`](../README.md) 索引中的现行文档、代码与 `openspec/` 为准。

## 归档清单

| 文件 | 原始路径 | 归档日期 | 归档原因 | 替代 / 现状 |
|:---|:---|:---|:---|:---|
| [handover-2026-08.md](./handover-2026-08.md) | `docs/operations/handover-2026-08.md` | 2026-09-14 | 2026-08-28 的一次性开发交底文档，仓库内无任何入链；其中的进度与待办已由后续进度日志和 openspec change 取代 | 进度见 [`docs/agent-progress.md`](../agent-progress.md)；运维见 [`docs/operations/agent-cli.md`](../operations/agent-cli.md) |
| [component-audit-2024-2026.md](./component-audit-2024-2026.md) | `docs/autoresearch/runs/h20-excess-alpha/component-audit-2024-2026.md` | 2026-09-14 | 文件自身标注「已由完整跨 regime 版本取代」 | [`component-audit-2019-2026.md`](../autoresearch/runs/h20-excess-alpha/component-audit-2019-2026.md) |
| [summary.md](./summary.md) | `docs/autoresearch/runs/h20-excess-alpha/summary.md` | 2026-09-14 | 文件自身标注为「早期记录（2026-08，2025 单年 / 30 只小样本口径）」，其结论已被多 regime 宽书版本更正 | [`research-progress-2026-09-04.md`](../autoresearch/runs/h20-excess-alpha/research-progress-2026-09-04.md) |

## 约定

- 归档文件保留原文，只在顶部加一行归档说明，并把因移动而失效的相对链接改到新位置。
- 需要引用历史结论时，请引用归档文件并注明其口径与日期，不要当作当前结论。
- 新归档请在本表补一行；能从 git 历史或已关闭 PR 明确找回的，可评估直接删除而非归档。
