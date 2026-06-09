# 内置中间件总览

EventBus 提供了 5 个开箱即用的中间件，覆盖日志、限流、转换、屏蔽、递归防护等常见横切关注点。
所有中间件通过 `MiddlewareChain` 注册，按注册顺序形成洋葱管道。

---

## 快速索引

| 中间件 | 阶段 | 用途 | 文档 |
| - | - | - | - |
| `EventBlockMiddleware` | `before_publish` | 按规则屏蔽（丢弃）事件 | [event_block.md](event_block.md) |
| `EventTransformMiddleware` | `before_publish` | 转换事件名 / 注入 / 脱敏字段 | [event_transform.md](event_transform.md) |
| `JSONLLoggingMiddleware` | `on_publish` | JSONL 文件持久化日志 | [logging.md](logging.md) |
| `SQLiteLoggingMiddleware` | `on_publish` | SQLite 数据库持久化日志 | [logging.md](logging.md) |
| `RateLimitMiddleware` | `before_publish` | 滑动窗口速率限制 | [rate_limit.md](rate_limit.md) |
| `RecursionGuardMiddleware` | `before_publish` | 双重递归检测 | [recursion_guard.md](recursion_guard.md) |

---

## 执行顺序

```text
发布请求
  │
  ▼
┌─ before_publish 链 ─────────────────────────────────┐
│  RateLimitMiddleware    ← 最先限流，超限直接丢弃      │
│  RecursionGuardMiddleware ← 递归检测，防止死循环      │
│  EventBlockMiddleware   ← 按规则屏蔽                  │
│  EventTransformMiddleware ← 重命名 / 注入 / 脱敏      │
│  核心发布逻辑             ← 校验 → 构造 Event → 入队  │
└──────────────────────────────────────────────────────┘
  │
  ▼
┌─ on_publish 链 ─────────────────────────────────────┐
│  JSONLLoggingMiddleware  ← 写入 JSONL 文件             │
│  SQLiteLoggingMiddleware ← 写入 SQLite 数据库          │
└──────────────────────────────────────────────────────┘
```

> **建议注册顺序**：限流 → 递归防护 → 屏蔽 → 转换 → 日志。`on_publish` 阶段的日志中间件顺序无关紧要。

---

## 全部导出

```python
from event_bus.templates.middlewares import (
    # 日志
    JSONLLoggingMiddleware,
    SQLiteLoggingMiddleware,
    LogFallback,
    # 限流
    RateLimitMiddleware,
    # 转换
    EventTransformMiddleware,
    TransformFunc,
    make_rename_transform,
    make_field_inject_transform,
    make_field_redact_transform,
    # 屏蔽
    EventBlockMiddleware,
    BlockPredicate,
    make_blocklist_predicate,
    make_allowlist_predicate,
    # 递归防护
    RecursionGuardMiddleware,
    RecursionDetectedError,
    # 工具
    serialize_data,
)
```

---

## 自定义中间件

所有中间件继承 `Middleware` 抽象基类。自定义中间件只需实现所需钩子：

```python
from event_bus import Middleware

class CustomMiddleware(Middleware):
    async def on_setup(self, bus): ...
    async def on_teardown(self, bus): ...
    async def before_publish(self, event_registry, name, source, data, old_event, next): ...
    async def on_publish(self, event, next): ...
    async def on_publish_error(self, error, name, source, data): ...
```

详见 [中间件文档](../middleware.md)。
