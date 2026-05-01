## Why

财富宝(caifubao)目前以后端服务为主，缺乏用户界面。投资者需要一个直观的工作台来总览市场行情、查询历史数据、运行回测策略、发现交易机会。建设前端是产品化的关键一步，让系统从"技术可行"走向"用户可用"。

## What Changes

- 新增 Web 前端应用（Vue 3 + Vite）
- 实现市场总览 Dashboard 模块
- 实现历史行情查询模块（含 K 线图表）
- 实现回测系统前端（策略配置、任务提交、结果展示，基于 AkQuant 高性能回测引擎）
- 实现交易信号与机会发现模块
- **实现用户认证与权限管理模块**
- 建设前端工程化基础设施（路由、状态管理、UI 组件库）

## Capabilities

### New Capabilities
- **market-dashboard**: 市场总览工作台，展示指数行情、涨跌幅排行、板块涨跌、资金流向等
- **historical-quotes**: 历史行情查询，支持股票搜索、K线图展示、多指标叠加
- **backtest-system**: 回测系统前端，支持策略配置、回测任务提交、结果可视化
- **signals-opportunities**: 交易信号与机会发现，展示实时信号、预警、推荐机会
- **user-auth**: 用户登录、注册、登出，会话管理
- **user-permissions**: 角色权限管理，普通用户 vs 管理员功能差异
- **frontend-infrastructure**: 前端工程基础设施，包含项目脚手架、路由、状态管理、UI组件库

### Modified Capabilities
- （无）后端当前无前端，本次为全新建设

## Impact

- **新增代码目录**: `frontend/` 
- **技术依赖**: Vue 3, Vite, ECharts, Element Plus, Pinia, Vue Router, AkQuant, akshare
- **后端对接**: 需要后端提供对应的 REST API（用户认证、股票数据、回测任务、信号查询）
- **认证方式**: JWT Token 认证
- **部署方式**: 支持开发服务器 / Docker 构建

---

## 非目标（本次不做）

- 移动端适配
- 实盘交易功能
- 回测引擎核心逻辑修改
