# EventBus 异步事件总线文档

## 概述

EventBus 是一个基于 asyncio 的轻量级事件总线，实现发布/订阅模式，用于在异步应用中解耦组件间的通信。它提供了强类型事件声明、正则表达式订阅、并发控制、超时保护及优雅停机等能力。

---

## 核心概念

| 组件 | 职责 |
| - | - |
| **Event** | 运行时事件实例，包含事件名、负载数据、处理链追踪信息（ID、来源、时间戳）。 |
| **EventDeclaration** | 声明一个事件类型的元数据：事件名称和可选的 Pydantic 负载模型。 |
| **EventRegistry** | 集中管理所有已注册的事件声明，发布时进行校验。 |
| **EventHandler** | 事件处理器基类。通过继承并实现 `handle` 方法定义业务逻辑，可声明订阅的事件类型（支持正则）。 |
| **EventHandlerRegistry** | 管理所有处理器实例，根据事件名匹配对应的处理器列表。 |
| **EventBus** | 事件分发中枢，负责任务队列、并发控制、错误上报及生命周期管理。 |

---

## 工作流程

### 1. 事件声明与注册

应用启动时，需将事件声明类注册到 `EventRegistry`，使总线识别合法事件类型及其负载结构。

```python
class UserLoginPayload(BaseModel):
    user_id: str
    timestamp: datetime

class UserLoginEvent(EventDeclaration):
    name = "user.login"
    payload_type = UserLoginPayload

registry = EventRegistry()
registry.register(UserLoginEvent)
```

### 2. 处理器订阅与注册

实现 `EventHandler` 子类，通过 `subscriptions` 指定监听的模式（支持正则），并通过 `handle` 方法处理事件。处理器实例注册到 `EventHandlerRegistry`。

```python
class LoginHandler(EventHandler):
    def __init__(self):
        super().__init__(subscriptions=[r"user\..*"])  # 匹配所有 user.* 事件

    async def handle(self, payload, bus_proxy, raw_event):
        if isinstance(payload, UserLoginPayload):
            print(f"User {payload.user_id} logged in")
            # 可调用 bus_proxy.publish 发布新事件

handler_registry = EventHandlerRegistry()
handler_registry.register(LoginHandler())
```

### 3. 启动总线与发布事件

创建 `EventBus` 实例并启动后，通过 `Proxy` 发布事件。Proxy 提供受限的总线访问接口，并自动记录事件来源。

```python
bus = EventBus(registry, handler_registry)
await bus.start()

# 发布事件
proxy = bus.proxy(source="AuthService")
await proxy.publish("user.login", {"user_id": "123", "timestamp": datetime.now()})
```

### 4. 事件分发与处理

- 发布的事件进入异步队列，由内部调度循环取出。
- 根据事件名匹配所有订阅处理器，为每个处理器创建独立任务。
- 通过信号量（Semaphore）限制并发处理器数量，防止过载。
- 每个处理器执行受超时控制，超时或异常均会触发内置错误事件。

### 5. 优雅停止

调用 `bus.stop()` 时：

- 拒绝新事件发布（抛出 `BusShuttingDown`）。
- 发布内置的 `__shutdown__` 事件通知处理器执行清理。
- 等待队列中已有事件处理完毕（可配置超时）。
- 取消调度循环，等待所有活跃处理器任务结束。

---

## 使用示例

### 基础发布/订阅

```python
class MyPayload(BaseModel):
    message: str

class MyEvent(EventDeclaration):
    name = "my.event"
    payload_type = MyPayload

class MyHandler(EventHandler):
    def __init__(self):
        super().__init__(subscriptions=["my.event"])

    async def handle(self, payload, bus_proxy, raw_event):
        print(f"Received: {payload.message}")

# 组装并运行
reg = EventRegistry()
reg.register(MyEvent)
h_reg = EventHandlerRegistry()
h_reg.register(MyHandler())

async with EventBus(reg, h_reg) as bus:   # 上下文管理器自动启停
    await bus.proxy("cli").publish("my.event", {"message": "Hello"})
    await asyncio.sleep(1)  # 等待处理器输出
```

### 正则订阅与链式发布

```python
class AuditHandler(EventHandler):
    def __init__(self):
        super().__init__(subscriptions=[r"order\..*"])   # 匹配所有 order. 事件

    async def handle(self, payload, bus_proxy, raw_event):
        await bus_proxy.publish("audit.log", {"event": raw_event.name})
```

---

## 内置事件

| 事件名 | 触发时机 | 负载类型 | 用途 |
| - | - | - | - |
| `event_bus.__shutdown__` | 总线开始停止时 | 无 | 通知处理器执行清理工作 |
| `event_bus.__task_error__` | 处理器执行失败时 | `TaskErrorPayload` | 错误监控与告警 |

---

## API 参考

### Event

运行时事件实例，由总线在发布时自动创建。

```python
class Event(BaseModel):
    name: str                           # 事件类型名
    data: Optional[BaseModel] = None    # 负载数据（Pydantic 模型实例）
    id: str                             # 事件 UUID（自动生成）
    sources: List[str]                  # 处理链来源记录
    timestamps: List[datetime]          # 各环节时间戳
```

| 字段 | 类型 | 说明 |
| - | - | - |
| `name` | `str` | 事件类型名称，对应 `EventDeclaration.name`。 |
| `data` | `Optional[BaseModel]` | 发布时传入的负载数据。无负载事件为 `None`。 |
| `id` | `str` | 事件唯一标识（UUID hex），自动生成。 |
| `sources` | `List[str]` | 事件经过的处理节点名称列表，用于追踪处理链。 |
| `timestamps` | `List[datetime]` | 每个处理节点的时间戳列表，与 `sources` 一一对应。 |

---

### EventDeclaration

事件声明的抽象基类。所有自定义事件类型必须继承此类。

```python
class EventDeclaration(ABC):
    name: ClassVar[str]                              # 事件名称（必须为非空字符串）
    payload_type: ClassVar[Optional[Type[BaseModel]]] = None  # 负载模型类（可选）
```

| 类变量 | 类型 | 说明 |
| - | - | - |
| `name` | `ClassVar[str]` | **必需**。事件类型的唯一标识名，不能为空字符串或纯空格。 |
| `payload_type` | `ClassVar[Optional[Type[BaseModel]]]` | 负载数据的 Pydantic 模型类。`None` 表示该事件无负载。 |

子类化时自动校验 `name` 非空和 `payload_type` 为合法类型，违反则抛出 `TypeError`。

---

### EventRegistry

事件注册表，管理所有合法的事件声明。

```python
class EventRegistry:
    def __init__(self) -> None
    def register(self, event_decl: Type[EventDeclaration]) -> None
    def unregister(self, event_name: str) -> None
    def get(self, name: str) -> Optional[Type[EventDeclaration]]
    def list_names(self) -> List[str]
```

| 方法 | 说明 |
| - | - |
| `register(event_decl)` | 注册一个事件声明类。若同名已存在则抛出 `ValueError`。 |
| `unregister(event_name)` | 注销指定名称的事件声明（不存在则静默忽略）。 |
| `get(name)` | 按名称查找事件声明，不存在返回 `None`。 |
| `list_names()` | 返回所有已注册事件名称的列表。 |

---

### EventHandler

事件处理器抽象基类。所有业务处理器必须继承并实现 `handle` 方法。

```python
class EventHandler(ABC):
    def __init__(
        self,
        subscriptions: Optional[List[str]] = None,
        handle_timeout: Optional[float] = 1.0
    ) -> None

    async def __call__(self, bus_proxy: EventBus.Proxy, event: Event) -> None

    @abstractmethod
    async def handle(
        self,
        payload: Optional[BaseModel],
        bus_proxy: EventBus.Proxy,
        raw_event: Event
    ) -> None: ...
```

| 参数 / 方法 | 说明 |
| - | - |
| `subscriptions` | 订阅的事件名模式列表，支持正则表达式。如 `[r"user\..*"]` 匹配所有 `user.*` 事件。 |
| `handle_timeout` | 单次 `handle` 调用的超时时间（秒）。`None` 表示无限等待。默认 `1.0`。 |
| `__call__` | 总线内部入口，自动解包 `Event` 后调用 `handle(payload, bus_proxy, raw_event)`。 |
| `handle(payload, bus_proxy, raw_event)` | **子类必须实现**。`payload` 为已解包的负载（可能为 `None`）。`bus_proxy` 提供受限的总线访问。`raw_event` 为完整事件对象。 |

---

### EventHandlerRegistry

处理器注册表，管理事件处理器实例与事件类型的匹配关系。

```python
class EventHandlerRegistry:
    def __init__(self, regex_cache_maxsize: int = 256) -> None
    def register(self, handler: EventHandler) -> str
    def unregister(self, handler_id: str) -> bool
    def get(self, handler_id: str) -> Optional[EventHandler]
    def get_handlers(self, event_type: str) -> List[tuple[str, EventHandler]]

    @property
    def handlers_count(self) -> int
    @property
    def all_handlers(self) -> Dict[str, EventHandler]
    @property
    def regex_cache_info(self) -> Dict[str, Any]
```

| 方法 / 属性 | 说明 |
| - | - |
| `__init__(regex_cache_maxsize=256)` | 构造注册表。`regex_cache_maxsize` 限制正则编译缓存条目数，超出后 LRU 淘汰。 |
| `register(handler)` | 注册处理器实例，返回唯一 handler ID（UUID hex）。 |
| `unregister(handler_id)` | 注销处理器。返回 `True` 表示成功，`False` 表示 ID 不存在。 |
| `get(handler_id)` | 按 ID 获取处理器实例。 |
| `get_handlers(event_type)` | 获取匹配 `event_type` 的 `(handler_id, handler)` 元组列表。 |
| `handlers_count` | （属性）当前注册的处理器总数。 |
| `all_handlers` | （属性）返回所有注册处理器的副本 `Dict[str, EventHandler]`。 |
| `regex_cache_info` | （属性）返回正则缓存状态 `{"size": int, "max_size": int}`。 |

---

### EventBus

事件分发中枢。

```python
class EventBus:
    def __init__(
        self,
        event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
        max_queue_size: int = 1024,
        max_handler_semaphore: int = 256,
        shutdown: ShutdownConfig = ShutdownConfig()
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

**构造参数：**

| 参数 | 类型 | 默认值 | 说明 |
| - | - | - | - |
| `event_registry` | `EventRegistry` | (必需) | 事件注册表实例。 |
| `handler_registry` | `EventHandlerRegistry` | (必需) | 处理器注册表实例。 |
| `max_queue_size` | `int` | `1024` | 事件队列最大容量，超出后 `put` 阻塞。 |
| `max_handler_semaphore` | `int` | `256` | 最大并发处理器数量（信号量）。 |
| `shutdown` | `ShutdownConfig` | `ShutdownConfig()` | 停机行为配置，参见 [ShutdownConfig](#shutdownconfig)。 |

构造时自动注册 `ShutdownEvent` 和 `TaskErrorEvent`（若注册表中不存在）。

**生命周期：**

| 方法 | 说明 |
| - | - |
| `start()` | 启动调度循环。重复调用安全。 |
| `stop()` | 优雅停止：发布 `__shutdown__` → 拒绝新发布 → 等待队列排空 → 取消调度 → 等待活跃任务完成。重复调用安全。 |
| `async with EventBus(...) as bus:` | 上下文管理器，自动启停。退出时 `stop()` 异常不会掩盖上下文体异常。 |

**发布（通过 Proxy）：**

| 方法 | 说明 |
| - | - |
| `proxy(source, raw_event=None)` | 创建 `Proxy` 实例。`source` 标记发布者名称。链式发布时传入 `raw_event` 以继承处理链。 |

**可观测性：**

| 属性 | 类型 | 说明 |
| - | - | - |
| `is_running` | `bool` | 总线是否在运行。 |
| `is_publishing_enabled` | `bool` | 是否允许发布新事件（停止过程中为 `False`）。 |
| `active_task_count` | `int` | 当前活跃的处理器任务数。 |
| `queue_size` | `int` | 事件队列中待处理的事件数。 |

---

### EventBus.Proxy

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
    @property
    def bus(self) -> EventBus
```

| 成员 | 说明 |
| - | - |
| `publish(name, data=None)` | 发布一个事件。`data` 可为字典或 Pydantic 模型实例。总线未运行时抛 `RuntimeError`，停止中抛 `BusShuttingDown`，未知事件抛 `ValueError`，负载类型不匹配抛 `TypeError`。 |
| `handlers_registry` | 只读访问处理器注册表。 |
| `events_registry` | 只读访问事件注册表。 |
| `bus` | 只读访问总线实例。 |

---

### ShutdownConfig

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

### 异常

| 异常类 | 说明 |
| - | - |
| `BusShuttingDown` | 总线正在停止时尝试发布新事件抛出。继承自 `Exception`。调用方应捕获并执行清理逻辑。 |

---

### 内置事件

| 事件声明 | 事件名 | 负载 | 说明 |
| - | - | - | - |
| `ShutdownEvent` | `event_bus.__shutdown__` | 无 | 总线开始停止时发布，通知处理器执行清理。 |
| `TaskErrorEvent` | `event_bus.__task_error__` | `TaskErrorPayload` | 处理器执行失败时发布，用于错误监控。 |

**TaskErrorPayload 字段：**

| 字段 | 类型 | 说明 |
| - | - | - |
| `error_event` | `Event` | 触发异常的事件实例。 |
| `handler_id` | `Optional[str]` | 发生异常的处理器内部注册 ID。 |
| `handler_name` | `str` | 发生异常的处理器类名。 |
| `error_type` | `str` | 异常类型名（如 `"ValueError"`）。 |
| `error_message` | `str` | 异常消息。 |

---

## 关键特性

- **强类型负载校验**：发布时自动校验数据类型与结构，防止无效数据流入。
- **正则表达式订阅**：支持灵活的事件名匹配规则。
- **背压控制**：通过队列大小与并发信号量限制系统负载。
- **超时保护**：每个处理器可独立设置超时，避免单任务阻塞总线。
- **错误隔离**：单个处理器异常不会影响其他处理器的执行，异常信息通过内置错误事件统一上报。
- **优雅停机**：保证停止过程中已入队事件被完整处理，避免数据丢失。
- **可观测性**：提供活跃任务数、队列长度等监控指标。

---

## 注意事项

- 所有 `EventHandler.handle` 实现**不应包含阻塞操作**，必须使用异步 I/O。
- 事件负载模型应继承 `pydantic.BaseModel` 以确保数据验证。
- 处理器中可通过 `bus_proxy.publish` 发布新事件，形成处理链，总线会自动追踪来源。
- 停止过程中发布新事件将抛出 `BusShuttingDown` 异常，调用方需妥善处理。
