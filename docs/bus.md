# EventBus 文档

## 概述

`EventBus` 是事件分发中枢，负责任务队列、并发控制、错误上报及生命周期管理。`Proxy` 是对外发布事件的唯一接口。系统内置 `ShutdownEvent` 和 `TaskErrorEvent` 两个特殊事件。

---

## EventBus

```python
class EventBus:
    def __init__(
        self,
        event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
        max_queue_size: int = 1024,
        max_handler_semaphore: int = 256,
        shutdown: ShutdownConfig = ShutdownConfig(),
        middleware_chain: Optional[MiddlewareChain] = None
    ) -> None

    # 生命周期
    async def start(self) -> None
    async def stop(self) -> None
    async def __aenter__(self) -> "EventBus"
    async def __aexit__(self, ...) -> Optional[bool]

    # 创建代理
    def proxy(self, source: str, raw_event: Optional[Event] = None) -> Proxy

    # 可观测性
    @property
    def is_running(self) -> bool
    @property
    def is_publishing_enabled(self) -> bool
    @property
    def active_task_count(self) -> int
    @property
    def queue_size(self) -> int
```

### 构造参数

| 参数 | 类型 | 默认值 | 说明 |
| - | - | - | - |
| `event_registry` | `EventRegistry` | (必需) | 事件注册表实例。 |
| `handler_registry` | `EventHandlerRegistry` | (必需) | 处理器注册表实例。 |
| `max_queue_size` | `int` | `1024` | 事件队列最大容量，超出后 `put` 阻塞。 |
| `max_handler_semaphore` | `int` | `256` | 最大并发处理器数量（信号量）。 |
| `shutdown` | `ShutdownConfig` | `ShutdownConfig()` | 停机行为配置，参见 [ShutdownConfig](#shutdownconfig)。 |
| `middleware_chain` | `Optional[MiddlewareChain]` | `None` | 中间件链，用于在发布流程中插入自定义逻辑。参见 [中间件文档](middleware.md)。 |

构造时自动注册 `ShutdownEvent` 和 `TaskErrorEvent`（若注册表中不存在），并预构建中间件责任链。

### 生命周期

| 方法 | 说明 |
| - | - |
| `start()` | 启动调度循环。重复调用安全。 |
| `stop()` | 优雅停止：发布 `__shutdown__` → 拒绝新发布 → 等待队列排空 → 取消调度 → 等待活跃任务完成。重复调用安全。 |
| `async with EventBus(...) as bus:` | 上下文管理器，自动启停。退出时 `stop()` 异常不会掩盖上下文体异常。 |

### 可观测性

| 属性 | 类型 | 说明 |
| - | - | - |
| `is_running` | `bool` | 总线是否在运行。 |
| `is_publishing_enabled` | `bool` | 是否允许发布新事件（停止过程中为 `False`）。 |
| `active_task_count` | `int` | 当前活跃的处理器任务数。 |
| `queue_size` | `int` | 事件队列中待处理的事件数。 |

---

## EventBus.Proxy

总线代理，**唯一允许对外发布事件的接口**。自动记录事件来源和处理链。

```python
class Proxy:
    async def publish(
        self,
        name: str,
        data: Optional[Union[Dict[str, Any], BaseModel]] = None
    ) -> None

    @property
    def handlers_registry(self) -> EventHandlerRegistry
    @property
    def events_registry(self) -> EventRegistry
```

| 成员 | 说明 |
| - | - |
| `publish(name, data=None)` | 发布一个事件。`data` 可为字典或 Pydantic 模型实例。总线未运行时抛 `RuntimeError`，停止中抛 `BusShuttingDown`，未知事件抛 `ValueError`，负载类型不匹配抛 `TypeError`。 |
| `handlers_registry` | 只读访问处理器注册表。 |
| `events_registry` | 只读访问事件注册表。 |

---

## ShutdownConfig

总线停机行为配置，用于控制优雅停止过程中的超时参数。

```python
class ShutdownConfig(BaseModel):
    queue_timeout_min: float = 1.0
    queue_timeout_max: float = 15.0
    tasks_timeout: float = 15.0
    avg_wait_time: float = 0.05
```

| 字段 | 类型 | 默认值 | 说明 |
| - | - | - | - |
| `queue_timeout_min` | `float` | `1.0` | 队列排空最小等待时间（秒）。 |
| `queue_timeout_max` | `float` | `15.0` | 队列排空最大等待时间（秒）。 |
| `tasks_timeout` | `float` | `15.0` | 活跃处理器任务完成等待时间（秒）。 |
| `avg_wait_time` | `float` | `0.05` | 每个事件平均处理时间估算（秒），用于动态计算队列排空超时。 |

实际队列排空超时 = `max(queue_timeout_min, min(queue_timeout_max, queue_size × avg_wait_time))`。

---

## 异常

| 异常类 | 说明 |
| - | - |
| `BusShuttingDown` | 总线正在停止时尝试发布新事件抛出。继承自 `Exception`。调用方应捕获并执行清理逻辑。 |

---

## 内置事件

| 事件声明 | 事件名 | 负载 | 说明 |
| - | - | - | - |
| `ShutdownEvent` | `event_bus.__shutdown__` | 无 | 总线开始停止时发布，通知处理器执行清理。 |
| `TaskErrorEvent` | `event_bus.__task_error__` | `TaskErrorPayload` | 处理器执行失败时发布，用于错误监控。 |

### TaskErrorPayload 字段

| 字段 | 类型 | 说明 |
| - | - | - |
| `error_event` | `Event` | 触发异常的事件实例。 |
| `handler_id` | `Optional[str]` | 发生异常的处理器内部注册 ID。 |
| `handler_name` | `str` | 发生异常的处理器类名。 |
| `error_type` | `str` | 异常类型名（如 `"ValueError"`）。 |
| `error_message` | `str` | 异常消息。 |

---

## 使用示例

### 基础组装与启动

```python
from event_bus import EventBus, EventRegistry, EventHandlerRegistry

# 1. 声明事件
class MyPayload(BaseModel):
    message: str

class MyEvent(EventDeclaration):
    name = "my.event"
    payload_type = MyPayload

# 2. 注册事件
reg = EventRegistry()
reg.register(MyEvent)

# 3. 实现处理器
class MyHandler(EventHandler):
    def __init__(self):
        super().__init__(subscriptions=["my.event"])

    async def handle(self, payload, bus_proxy, raw_event):
        print(f"Received: {payload.message}")

# 4. 注册处理器
h_reg = EventHandlerRegistry()
h_reg.register(MyHandler())

# 5. 启动总线并发布
async with EventBus(reg, h_reg) as bus:
    await bus.proxy("cli").publish("my.event", {"message": "Hello"})
    await asyncio.sleep(0.1)  # 等待处理器输出
```

### 链式发布

处理器中可通过 `bus_proxy.publish` 发布新事件，形成处理链，总线会自动追踪来源：

```python
class OrderHandler(EventHandler):
    def __init__(self):
        super().__init__(subscriptions=["order.created"])

    async def handle(self, payload, bus_proxy, raw_event):
        # 处理订单...
        # 链式发布通知事件
        await bus_proxy.publish("notification.send", {
            "type": "order_confirmed",
            "order_id": payload.order_id
        })
```
