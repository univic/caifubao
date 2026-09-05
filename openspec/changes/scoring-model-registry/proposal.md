## Why

方向版本化（#183）让 model_version 可以携带翻转方向，但 model_version 本身只是字符串
标签——scoring_runner 只传 model_version，不自动加载该版本的方向/权重/阈值/评分模式
（codex review 第一阶段 #2）。同一版本名在不同调用里可能因隐式内置配置不同而产生不同
分数，历史分数无法追溯到一个确定配置。需要一个不可变注册表把 model_version 绑定到完整
配置。

## What Changes

- **ScoreModelVersion 注册表文档**（collection score_model_versions）：model_version
  唯一、per-horizon 配置覆盖（weights / thresholds / directions）、canonical
  config_hash、scoring_mode、ACTIVE/RETIRED 生命周期。版本不可变：改动 = 注册新版本名 +
  retire 旧版，使已存预测可追溯到唯一配置。
- **ScoringService 配置解析优先级**：显式 scoring_config（experiment/backfill）> ACTIVE
  注册配置（查询层按 model_version + status='ACTIVE' 过滤）> 内置 SCORING_CONFIG。
  未注册版本（含 DEFAULT_MODEL_VERSION）行为与现状完全一致（向后兼容）。
- **注册时配置校验**：horizon 键、direction 键/值经解析路径在 register 时报错（不等到
  scoring 时才崩）。
- **jobs/model_registry_runner**：register/list/retire CLI。

## Non-goals

- 不改变 API 契约、鉴权、新鲜度、前端/OpenClaw/调度器。
- 不自动注册任何现有 model_version（score_v2_202605b 等继续走内置配置，注册为可选）。
- 不改变分数数学；只改变 model_version → config 的解析来源。
- 决策接口（decisions/scores 等）的 model_version 过滤为后续独立 change（codex #4）。
