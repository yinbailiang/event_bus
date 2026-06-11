# Middleware / MiddlewareChain 文档

## 概述

中间件系统为 EventBus 提供了可插拔的发布流程钩子。通过在发布前后插入自定义逻辑，可以实现日志记录、数据校验、链路追踪、性能监控、消息转换等横切关注点，而无需修改核心业务处理器。

中间件采用**洋葱模型（责任链）**设计：多个中间件按注册顺序层层包裹，每个中间件可以在调用 `next()` 前后执行自己的逻辑。

---

## 架构概念

```text
发布请求
  │
  ▼
┌─────────────────────────────────────┐
│  Middleware 1 (外层)                 │
│  ┌─────────────────────────────────┐ │
│  │ Middleware 2 (内层)             │ │
│  │ ┌─────────────────────────────┐ │ │
│  │ │ 核心发布逻辑                 │ │ │
│  │ │ ├─ 校验 → 构造 → 入队       │ │ │
│  │ │ └─ on_publish 链（洋葱模型） │ │ │
│  │ └─────────────────────────────┘ │ │
│  └─────────────────────────────────┘ │
└─────────────────────────────────────┘
```

> **注意**：`on_publish` 链嵌套在核心发布逻辑内部，而核心发布逻辑是 `before_publish` 链的最内层。因此 `before_publish` 钩子中 `await next(...)` 之后的代码会在 `on_publish` 链**完全结束之后**才执行。

### 两个钩子阶段

| 阶段 | 时机 | 可获取的信息 |
| - | - | - |
| `before_publish` | 事件声明校验、构造 Event 并入队**之前** | 事件名、来源、原始 data、前驱 event |
| `on_publish` | Event 成功入队**之后** | 完整的 Event 对象（含 id、sources、timestamps） |

---

## Middleware

中间件抽象基类。自定义中间件需继承此类并实现 `before_publish` 和 `on_publish` 两个抽象方法。

```python
class Middleware(ABC):
    # 生命周期
    async def on_setup(self, bus: EventBus) -> None: ...
    async def on_teardown(self, bus: EventBus) -> None: ...

    # 发布钩子（必须实现）
    async def before_publish(
        self,
        event_registry: EventRegistry,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
        old_event: Event | None,
        next: BeforePublishNext,
    ) -> None: ...

    async def on_publish(
        self,
        event: Event,
        next: OnPublishNext,
    ) -> None: ...

    # 错误钩子（可选）
    async def on_publish_error(
        self,
        error: Exception,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
    ) -> None: ...
```

### 钩子详解

#### `on_setup(bus)` / `on_teardown(bus)`

总线生命周期钩子。`on_setup` 在总线 `start()` 完成后调用，`on_teardown` 在 `stop()` 结束时**逆序**调用。适用于初始化连接池、注册后台任务等场景。

> **注意**：`on_setup` 中抛出异常的中间件会被自动从链中移除，防止影响总线正常运行。

#### `before_publish(event_registry, name, source, data, old_event, next)`

发布**前**钩子。在 publish 的任何逻辑（含事件声明校验）开始前执行。调用 ``next`` 后才会进入校验 → 构造 Event → 入队流程。

| 参数 | 说明 |
| - | - |
| `event_registry` | 事件注册表，可查询事件声明元数据。 |
| `name` | 事件名称。 |
| `source` | 发布者标识。 |
| `data` | 原始负载数据（字典或 BaseModel，可能为 `None`）。 |
| `old_event` | 前驱事件（链式发布时，可能为 `None`）。 |
| `next` | 调用下一个中间件（或核心发布逻辑）。**必须调用**，否则事件不会入队。 |

#### `on_publish(event, next)`

发布**后**钩子。在 Event 成功入队后执行。可通过 `event.name`、`event.data`、`event.id`、`event.sources` 等获取完整的运行时事件信息。

| 参数 | 说明 |
| - | - |
| `event` | 已入队的完整 Event 对象。 |
| `next` | 调用下一个中间件。 |

#### `on_publish_error(error, name, source, data)`

发布流程中发生异常时回调（可选实现）。按注册**顺序**通知所有中间件，某个中间件的异常不会影响其他中间件的通知。

| 参数 | 说明 |
| - | - |
| `error` | 发生的异常对象。 |
| `name` | 事件名称。 |
| `source` | 发布者标识。 |
| `data` | 原始负载数据。 |

---

## MiddlewareChain

中间件链管理器，负责维护中间件的注册顺序和生命周期。

```python
class MiddlewareChain:
    def __init__(self) -> None
    def add(self, middleware: Middleware) -> "MiddlewareChain"
    def insert(self, index: int, middleware: Middleware) -> "MiddlewareChain"
    def remove(self, middleware: Middleware) -> None
    def clear(self) -> None

    @property
    def middlewares(self) -> list[Middleware]

    def build_before_publish(
        self,
        final_handler: BeforePublishNext,
    ) -> BeforePublishNext
    def build_on_publish(
        self,
        final_handler: OnPublishNext,
    ) -> OnPublishNext

    async def setup(self, bus: EventBus) -> List[Middleware]
    async def teardown(self, bus: EventBus) -> None
    async def on_publish_error(
        self, error: Exception, name: str, source: str,
        data: dict[str, Any] | BaseModel | None
    ) -> None
```

| 方法 / 属性 | 说明 |
| - | - |
| `add(middleware)` | 在链**末尾**追加中间件。返回自身，支持链式调用。重复添加同一实例抛出 `ValueError`。 |
| `insert(index, middleware)` | 在指定位置插入中间件。重复添加同一实例抛出 `ValueError`。 |
| `remove(middleware)` | 移除指定中间件实例。 |
| `clear()` | 清空所有中间件。 |
| `middlewares` | （属性）返回当前中间件列表的副本。 |
| `build_before_publish(final_handler)` | 构建 ``before_publish`` 责任链。传入核心发布逻辑作为末端处理器，返回包装后的可调用链。 |
| `build_on_publish(final_handler)` | 构建 ``on_publish`` 责任链。传入空操作作为末端处理器，返回包装后的可调用链。 |
| `setup(bus)` | 按注册顺序调用所有中间件的 `on_setup`。返回初始化失败的中间件列表（这些中间件已被自动移除）。 |
| `teardown(bus)` | 按注册**逆序**调用所有中间件的 `on_teardown`。单个异常不影响其他。 |
| `on_publish_error(...)` | 按注册顺序通知所有中间件发布异常。 |

---

## 使用示例

### 日志中间件

记录每次发布的事件信息和耗时：

```python
import time
import logging
from event_bus import Middleware, MiddlewareChain

logger = logging.getLogger(__name__)

class LoggingMiddleware(Middleware):
    async def on_setup(self, bus):
        logger.info("LoggingMiddleware initialized")

    async def on_teardown(self, bus):
        logger.info("LoggingMiddleware shutting down")

    async def before_publish(self, event_registry, name, source, data, old_event, next):
        t0 = time.monotonic()
        logger.debug(f"[Publish] {name} from {source}")
        try:
            await next(event_registry, name, source, data, old_event)
        finally:
            elapsed = time.monotonic() - t0
            logger.debug(f"[Publish] {name} completed in {elapsed:.3f}s")

    async def on_publish(self, event, next):
        logger.debug(f"[Enqueued] {event.name} (id={event.id})")
        await next(event)

    async def on_publish_error(self, error, name, source, data):
        logger.error(f"[Error] {name} from {source}: {error}")

# 注册到总线
chain = MiddlewareChain()
chain.add(LoggingMiddleware())

bus = EventBus(reg, h_reg, middleware_chain=chain)
```

### 数据校验中间件

在发布前对数据进行额外校验：

```python
class ValidationMiddleware(Middleware):
    async def on_setup(self, bus): pass
    async def on_teardown(self, bus): pass

    async def before_publish(self, event_registry, name, source, data, old_event, next):
        if name == "order.create" and isinstance(data, dict):
            if data.get("amount", 0) <= 0:
                raise ValueError("Order amount must be positive")
        await next(event_registry, name, source, data, old_event)

    async def on_publish(self, event, next):
        await next(event)
```

### 短路中间件

在某些条件下阻止事件发布（不调用 `next`）：

```python
class RateLimitMiddleware(Middleware):
    def __init__(self, max_per_second: int = 100):
        self._max = max_per_second
        self._count = 0
        self._window_start = time.monotonic()

    async def on_setup(self, bus): pass
    async def on_teardown(self, bus): pass

    async def before_publish(self, event_registry, name, source, data, old_event, next):
        now = time.monotonic()
        if now - self._window_start > 1.0:
            self._window_start = now
            self._count = 0
        self._count += 1
        if self._count > self._max:
            logger.warning(f"Rate limit exceeded, dropping {name}")
            return  # 不调用 next，事件被丢弃
        await next(event_registry, name, source, data, old_event)

    async def on_publish(self, event, next):
        await next(event)
```

### 多中间件洋葱顺序

```python
chain = MiddlewareChain()
chain.add(LoggingMiddleware())       # 最外层
chain.add(ValidationMiddleware())    # 中层
chain.add(RateLimitMiddleware())     # 最内层

# 执行顺序（on_publish 链嵌套在 before_publish 链内部）：
#   Logging.before 进入 → Validation.before 进入 → RateLimit.before 进入
#     → 核心发布逻辑（校验 → 构造 → 入队）
#     → Logging.on 进入 → Validation.on 进入 → RateLimit.on 进入
#         → 空操作（on_publish 链终点）
#       ← RateLimit.on 退出 ← Validation.on 退出 ← Logging.on 退出
#   ← RateLimit.before 退出 ← Validation.before 退出 ← Logging.before 退出
```

### 运行时动态管理

中间件链支持运行时动态增删，且变更即时生效。可通过总线代理在处理器内部访问：

```python
class DynamicMiddleware(Middleware):
    async def on_setup(self, bus):
        self._proxy = bus.proxy(self.__class__.__name__)

    async def on_teardown(self, bus): pass

    async def before_publish(self, event_registry, name, source, data, old_event, next):
        # 根据条件动态注入中间件
        if name == "sensitive.event":
            self._proxy.middleware.add(AuditMiddleware())
        await next(event_registry, name, source, data, old_event)

    async def on_publish(self, event, next):
        await next(event)
```

> **注意**：处理器通过 `bus_proxy.middleware` 获取的 `MiddlewareChain` 实例与总线共享同一实例，
> 增删操作直接影响后续所有发布流程。链在每次分发时按需构建，变更即时生效。
