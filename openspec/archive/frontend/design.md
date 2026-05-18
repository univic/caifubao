> **⚠️ ARCHIVED — HISTORICAL REFERENCE ONLY**
> This file describes an abandoned architecture (Django, Celery, AkQuant).
> The current system uses Flask + datahub + Vue 3. Do NOT use as a current spec.
> See `openspec/changes/mvp-quant-demo/` for the active specification.

## Context

财富宝(caifubao)是一个基于 Django + MongoDB 的财经数据量化分析平台，后端提供股票数据、回测引擎、信号生成等服务，但缺少用户界面。

当前系统架构：
```
用户 → (无) → Django API → MongoDB
                    ↓
              Celery Worker (回测任务)
```

目标架构：
```
用户 → Vue 3 前端 → Django API → MongoDB
                         ↓
                   Celery Worker
```

## Goals / Non-Goals

**Goals:**
- 构建完整的 Web 前端应用，提供用户可用的工作台
- 实现市场总览 Dashboard，直观展示市场整体状况
- 实现历史行情查询，支持 K 线及技术指标
- 实现回测系统前端，覆盖策略配置到结果展示全流程
- 实现信号与机会模块，展示系统生成的投资机会
- 实现用户认证与权限管理

**Non-Goals:**
- 移动端 H5 / App（Web 响应式即可）
- 回测引擎核心逻辑修改（后端已具备）
- 实盘交易通道对接

## Decisions

### 1. 前端框架选型: Vue 3 + Vite
| 方案 | 优点 | 缺点 |
|------|------|------|
| Vue 3 + Vite | 上手简单，Composition API 优秀，文档中文 | 团队熟悉度未知 |
| React + Vite | 生态成熟，组件丰富 | 学习曲线中等 |
| Svelte | 性能好 | 生态较小 |

**决策**: Vue 3 + Vite。理由：Composition API 代码组织灵活，Element Plus 对 Vue 支持好，上手门槛低。

### 2. UI 组件库: Element Plus
| 方案 | 适用场景 |
|------|----------|
| Element Plus | Vue 3 企业级后台，表格/表单/布局完善 |
| Ant Design Vue | Ant Design 的 Vue 版本 |
| Naive UI | 较新，组件丰富 |

**决策**: Element Plus。理由：Vue 3 官方推荐的企业级 UI 库，中文文档完善，表格和表单组件丰富。

### 3. 图表库: ECharts
**决策**: ECharts。理由：
- K 线图支持完善
- 金融图表组件丰富（热力图、关系图等）
- 中文文档完善

### 4. 状态管理: Pinia
| 方案 | 复杂度 | 适用场景 |
|------|--------|----------|
| Pinia | 低 | Vue 3 推荐，简单直观 |
| Vuex 4 | 中 | Vue 2 迁移 |
| Redux | 高 | 大型复杂状态 |

**决策**: Pinia。理由：Vue 3 官方推荐，API 简洁，TypeScript 支持好。

### 5. 目录结构
```
frontend/
├── src/
│   ├── api/          # API 请求封装
│   ├── assets/       # 静态资源
│   ├── components/   # 公共组件
│   │   ├── charts/  # 图表组件 (K线、折线等)
│   │   ├── layout/  # 布局组件
│   │   └── common/  # 通用组件
│   ├── views/       # 页面组件
│   │   ├── Dashboard/
│   │   ├── History/
│   │   ├── Backtest/
│   │   └── Signals/
│   ├── stores/      # Pinia store
│   ├── router/      # 路由配置
│   ├── styles/      # 全局样式
│   ├── types/       # TypeScript 类型
│   └── utils/       # 工具函数
├── public/
├── index.html
└── vite.config.ts
```

### 6. 认证方案: JWT Token
| 方案 | 优点 | 缺点 |
|------|------|------|
| JWT Token | 无状态，易扩展，前后端分离 | 需要考虑 token 刷新 |
| Session | 简单易用 | 不适合前后端分离 |
| OAuth2 | 第三方登录 | 复杂度高 |

**决策**: JWT Token。理由：后端 Django 已有用户模型，适合 RESTful API，无状态便于扩展。

**实现细节**:
- Token 存储在 localStorage
- 请求拦截器自动添加 Authorization header
- Token 过期前 5 分钟自动刷新
- 路由守卫检查登录状态

### 7. 权限控制: RBAC
- **USER**: 基础权限，访问 Dashboard/History/Backtest/Signals
- **ADM**: 管理员权限，额外访问用户管理、系统设置
- 菜单根据角色动态渲染
- API 级别权限校验（后端配合）

### 8. 后端 API 对接策略
- **数据查询**: REST API 轮询（Dashboard 30s 刷新）
- **回测任务**: 
  1. 前端 POST 创建任务 → 返回 task_id
  2. 前端轮询 task 状态
  3. 完成后跳转结果页
- **图表数据**: 后端返回 OHLC + volume 数组，前端渲染

### 9. 回测引擎: AkQuant + akshare
| 方案 | 优点 | 缺点 |
|------|------|------|
| AkQuant | Rust 核心高性能，内置可视化，原生 A 股支持 | 较新 |
| QLib | 微软背书，ML 集成强 | 学习曲线较陡 |
| 自建 | 完全可控 | 开发周期长 |

**决策**: AkQuant + akshare。理由：
- 高性能回测（Rust 核心）
- A 股数据直接获取（akshare）
- 内置完整绩效指标
- 一行代码生成 HTML 报告

**集成架构**:
```
前端 Vue 3
    │
    ▼ POST /api/backtest/run
Django View
    │
    ▼ Celery Task
┌─────────────────────────────────────────┐
│ Celery Worker                           │
│  1. akshare 获取行情数据                │
│  2. AkQuant 执行回测                    │
│  3. 生成绩效指标 + 权益曲线              │
│  4. 生成 HTML 报告 (可选)                │
│  5. 结果存入 MongoDB                    │
└─────────────────────────────────────────┘
    │
    ▼ GET /api/backtest/:id
前端展示
```

**后端 API 设计**:
| 接口 | 方法 | 说明 |
|------|------|------|
| `/api/backtests` | GET | 列表 |
| `/api/backtests` | POST | 创建回测任务 |
| `/api/backtests/:id` | GET | 详情/结果 |
| `/api/backtests/:id/status` | GET | 任务状态 |
| `/api/backtests/:id` | DELETE | 删除 |
| `/api/backtests/:id/report` | GET | 下载 HTML 报告 |

**回测配置参数**:
```python
{
    "name": "均线策略测试",
    "symbol": "sh600000",      # 股票代码
    "start_date": "20240101",
    "end_date": "20250101",
    "initial_cash": 1000000,
    "strategy": "ma_cross",
    "params": {
        "fast_ma": 5,
        "slow_ma": 20
    }
}
```

**回测结果结构**:
```python
{
    "status": "COMP",          # CRTD/PEND/RUNN/COMP/FAIL
    "metrics": {
        "total_return_pct": 15.8,
        "annualized_return": 0.158,
        "sharpe_ratio": 1.25,
        "max_drawdown": 0.082,
        "win_rate": 0.62,
        "profit_factor": 1.85,
        "trade_count": 45
    },
    "equity_curve": [...],      # 每日净值
    "trades": [...],            # 交易记录
    "report_html": "..."        # HTML 报告(可选)
}
```

### 10. CSS 方案: SCSS + Element Plus 主题
- 使用 Element Plus 默认主题
- 特殊配色通过 CSS Variables 覆盖
- 使用 SCSS 组织样式

## Risks / Trade-offs

| 风险 | 影响 |  mitigation |
|------|------|-------------|
| 后端 API 尚未完善 | 前端无法对接数据 | 先Mock数据开发，后期对接真实API |
| K线图交互复杂 | 实现难度高 | 使用 ECharts stock chart 组件 |
| 回测任务耗时长 | 用户体验差 | 任务状态实时反馈，完成后通知 |
| 多页面状态管理 | 复杂度上升 | 按模块划分 stores，共享数据用 composables |

## Migration Plan

### Phase 1: 基础建设（第1周）
1. 初始化 Vite + Vue 3 项目
2. 配置路由、Element Plus、Pinia
3. 搭建基础布局（侧边栏、Header）

### Phase 2: Dashboard（第2周）
1. 开发 Dashboard 页面
2. Mock 数据展示
3. 接入真实 API

### Phase 3: 历史行情（第3周）
1. 股票搜索组件
2. K 线图表开发
3. 技术指标叠加

### Phase 4: 回测系统（第4周）
1. 策略配置表单
2. 任务提交与状态轮询
3. 结果展示（收益曲线、绩效指标）

### Phase 5: 信号与机会（第5周）
1. 信号列表页
2. 机会发现页
3. 筛选与排序

### Phase 6: 收尾与部署
1. 响应式调整
2. 构建优化
3. Docker 镜像制作

## Open Questions

1. **后端 API 详细规范** - 当前后端 API 文档是否完善？需要确认股票数据、回测任务、信号查询的接口定义
2. **用户认证** - 是否需要在前端预留认证入口？（本次不做但需预留）
3. **实时数据** - 是否需要 WebSocket 推送？（Dashboard 暂时轮询即可）
