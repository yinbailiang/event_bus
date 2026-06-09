# 高级模板总览

模板层基于 EventBus 核心构建，提供 4 个高级抽象，覆盖常见事件驱动模式。

---

## 快速索引

| 模板 | 模式 | 适用场景 | 文档 |
| - | - | - | - |
| `expect` | 一次性监听 | 等待特定事件、测试断言、底层等待逻辑 | [expect.md](expect.md) |
| `request` | 请求-响应 (RPC) | 同步风格的异步调用、服务间通信 | [request.md](request.md) |
| `pipe` | 双向管道 | 流式数据交换、长连接模拟、持久化双向流 | [pipe.md](pipe.md) |
| `register` | 批量注册 + 依赖注入 | 大型项目模块化组织、延迟注册、避免循环导入 | [register.md](register.md) |
| [middlewares/](middlewares/middlewares.md) | 中间件集合 | 日志、限流、转换、屏蔽、递归防护 | [中间件总览](middlewares/middlewares.md) |

---

## 层次关系

```text
┌─────────────────────────────────────────────────┐
│                    register                     │
│  模块级事件/处理器收集 → 应用启动时批量注册       │
└────────────────────┬────────────────────────────┘
                     │ 注册到
                     ▼
┌─────────────────────────────────────────────────┐
│                   EventBus                      │
│         发布/订阅 · 中间件管道 · 调度循环          │
└──────┬────────────────────────────┬─────────────┘
       │                            │
       ▼                            ▼
┌──────────────┐            ┌──────────────┐
│   request    │            │     pipe     │
│ RPC 调用封装  │◄── 依赖 ──│ 双向管道握手   │
│ (内部用 expect)│           │ (内部用 request│
└──────┬───────┘            │   + expect)   │
       │                    └──────────────┘
       │ 依赖
       ▼
┌──────────────┐
│    expect    │
│ 一次性事件监听 │
└──────────────┘
```

> `request` 和 `pipe` 内部都依赖 `expect` 实现等待逻辑。`pipe` 同时依赖 `request` 完成握手协议。

---

## 全部导出

```python
from event_bus.templates import (
    # expect
    'expect',
    # pipe
    'Pipe', 'InProcessPipe', 'InProcessPipeAllocator', 'PipeAllocator',
    'open_pipe', 'expect_pipe',
    # register
    'ModuleEventRegister', 'ModuleHandlerRegister',
    # request
    'request', 'RequestProtocol', 'ResponseProtocol',
    # middlewares（详见中间件总览）
    'EventBlockMiddleware', 'EventTransformMiddleware',
    'JSONLLoggingMiddleware', 'SQLiteLoggingMiddleware',
    'RateLimitMiddleware', 'RecursionGuardMiddleware',
    'make_rename_transform', 'make_field_inject_transform',
    'make_field_redact_transform', 'make_blocklist_predicate',
    'make_allowlist_predicate', 'RecursionDetectedError',
)
```

---

## 选择指南

| 你想做什么 | 用这个 |
| - | - |
| 发一个请求，等一个响应 | `request` |
| 建立长连接，双向收发数据 | `pipe` |
| 等待某个事件发生一次 | `expect` |
| 按模块组织事件和处理器 | `register` |
| 给发布流程加横切逻辑 | [middlewares/](middlewares/middlewares.md) |
