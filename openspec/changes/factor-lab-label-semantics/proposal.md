## Why

2026-09-13 落地的因子实验室（`datahub/app/lib/factor_lab/`）需要一套**前瞻标签与
A 股可交易性**定义，用于短线/中线因子的 IC、分位与走查评估。仓库里此前已经有 **4 套
互不相同的约定**，各自服务于不同消费者：

| # | 位置 | 标签/可交易性约定 | 消费者 |
|---|---|---|---|
| 1 | `autoresearch_h20_snapshot_runner.py:294` | 平铺 ±9.9% 涨跌停；被挡则**滚动**到下一个可成交日 | h20 autoresearch 快照与评估器 |
| 2 | `backend/app/services/backtest_service.py:259-275`、`:2881-2883` | 同上（±9.9% + roll-forward） | 回测服务 |
| 3 | `datahub/app/lib/scoring_engine/factor_eval.py:29,175,255` | 前向收益按**日历窗** `int(h*1.5)` 天取第 h 根 | tech_factor_runner 单因子评估 |
| 4 | **本 change**：`factor_lab/panel.py:41-56,204-248` | **board-aware** 涨跌停（主板 10/创业板科创 20/北交所 30/ST 5）；不可交易**丢弃**并记录原因；标签为**交易日偏移** T+1 开盘 → 第 h 个交易日开盘 | 因子实验室（研究） |

问题不在于存在多套实现，而在于**差异没有被显式记录**：日历窗 `int(h*1.5)` 连
「周五观测的 h=1」都解析不出来（需要 3 个日历日），而 ±9.9% 平铺会把创业板/科创板的
15% 涨幅误判为涨停；反过来，roll-forward 会悄悄改变持有期，不能用于因子 IC 统计。
research 结论若混用这几种口径，就会重复本项目已经踩过的坑（flip 每日 −9.74% vs
5 日 +0.33% 的调仓口径事故）。

`openspec/changes/h20-autoresearch-replay-semantics/proposal.md` 的 Non-goals 明确
把「H5/H60 研究语义」排除在外，因此短线/中线标签语义需要自己的 record。

## What Changes

- 新增 `openspec/changes/factor-lab-label-semantics/specs/factor-lab-labels/spec.md`，
  把因子实验室的两条约定写成可验证的 requirement：**位置化标签 + 丢弃不可交易**、
  **board-aware 涨跌停**。
- 明确「哪套约定服务哪个消费者」，以及它们**不得共用标签**（尤其 roll-forward 与
  drop 不能混用）。
- 把四条容易「看起来对、其实失效」的语义写成 requirement：日历校验（缺行不得悄悄
  拉长持有期）、镜像空头腿、`previous_close`+开盘价判定涨跌停、以及净成本/闸门口径
  （多空价差每腿各付一次往返、profit_concentration 用单笔占正收益比、走查衰减带符号）。
- 不改动任何现有实现：本 change 只治理新增的研究链路；其余三套保持现状，未来若统一
  需另开 change。

## Impact

- 受影响代码：`datahub/app/lib/factor_lab/panel.py`（新，已实现）。
- 不受影响：scoring engine、signals、replay、calibration、backend API、OpenClaw 合约。
- 数据所有权：`factor_lab` 属 datahub，只读 Mongo、只写本地 parquet。

## Non-goals

- 不统一现有 4 套标签实现（需要单独的迁移 change，且会改变已归档研究结论的口径）。
- 不定义组合层/成交层语义（整手、最低佣金、市场冲击、排队）——那是
  `strategy-paper-*` 与 `backtest` 的范围。
