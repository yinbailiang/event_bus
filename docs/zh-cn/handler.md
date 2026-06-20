# EventHandler / EventHandlerRegistry 文档

## 概述

`EventHandler` 是事件处理器的抽象基类，通过订阅模式（`str` 全字匹配，`Regex` 正则匹配）监听感兴趣的事件。
`EventHandlerRegistry` 管理所有处理器实例的增删查。

---

## EventHandler

事件处理器抽象基类。所有业务处理器必须继承并实现 `handle` 方法。

```python
class EventHandler(ABC):
    def __init__(
        self,
        subscriptions: Optional[List[Regex | str]] = None,
        handle_timeout: Optional[float] = 32.0
    ) -> None

    async def __call__(self, bus: EventBus, event: Event) -> None

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
| `subscriptions` | 订阅的事件名模式列表。`str` 全字匹配（`event_type == subscription`），`Regex` 正则全匹配。如 `["user.login"]` 只匹配 `"user.login"`，`[Regex(r"user\..*")]` 匹配所有 `user.*` 事件。 |
| `handle_timeout` | 单次 `handle` 调用的超时时间（秒）。`None` 表示无限等待。默认 `32.0`。 |
| `__call__` | 总线内部入口，接收原始 ``EventBus`` 实例，内部调用 ``bus.proxy(handler_name, event)`` 创建代理后传递给 ``handle()``。 |
| `handle(payload, bus_proxy, raw_event)` | **子类必须实现**。`payload` 为已解包的负载（可能为 `None`）。`bus_proxy` 提供受限的总线访问。`raw_event` 为完整事件对象。 |

### 使用示例

```python
from event_bus import EventHandler, Regex

class LoginHandler(EventHandler):
    def __init__(self):
        super().__init__(subscriptions=[Regex(r"user\..*")])  # 匹配所有 user.* 事件

    async def handle(self, payload, bus_proxy, raw_event):
        if isinstance(payload, UserLoginPayload):
            print(f"User {payload.user_id} logged in at {payload.timestamp}")
            # 可通过 bus_proxy.publish 发布新事件，形成处理链
```

### 订阅模式

`subscriptions` 支持两种匹配方式：

- **`str`** — 全字匹配（`event_type == subscription`），精确匹配事件名；
- **`Regex`** — 正则全匹配（`Regex.fullmatch(event_type)`），灵活匹配事件名模式。

```python
from event_bus import EventHandler, Regex

class AuditHandler(EventHandler):
    def __init__(self):
        # 全字匹配单个事件
        # 正则匹配所有 order.* 和 payment.* 事件
        super().__init__(subscriptions=["user.login", Regex(r"order\..*"), Regex(r"payment\..*")])

    async def handle(self, payload, bus_proxy, raw_event):
        print(f"Audit: {raw_event.name}")
```

---

## EventHandlerRegistry

处理器注册表，管理事件处理器实例的增删查。**匹配路由由 [Matcher](matcher.md) 负责**。

```python
class EventHandlerRegistry:
    def __init__(self) -> None
    def register(self, handler: EventHandler) -> str
    def unregister(self, handler_id: str) -> bool
    def get(self, handler_id: str) -> Optional[EventHandler]
    def clear(self) -> None
    def __len__(self) -> int
    def __contains__(self, handler_id: str) -> bool
    def __iter__(self) -> Iterator[tuple[str, EventHandler]]

    @property
    def version(self) -> int
    @property
    def handlers_count(self) -> int
    @property
    def all_handlers(self) -> Dict[str, EventHandler]
```

| 方法 / 属性 | 说明 |
| - | - |
| `register(handler)` | 注册处理器实例，返回唯一 handler ID（UUID hex）。版本号递增。 |
| `unregister(handler_id)` | 注销处理器。返回 `True` 表示成功，`False` 表示 ID 不存在。版本号递增。 |
| `get(handler_id)` | 按 ID 获取处理器实例。 |
| `clear()` | 清除所有已注册处理器。版本号递增。 |
| `__len__()` | 支持 `len(registry)`。 |
| `__contains__()` | 支持 `handler_id in registry`。 |
| `__iter__()` | 支持 `for hid, h in registry` 迭代。 |
| `version` | 注册表版本号，每次变更（增/删/清空）递增，供 [Matcher](matcher.md) 感知变化。 |
| `handlers_count` | 当前注册的处理器总数。 |
| `all_handlers` | 返回所有注册处理器的副本 `Dict[str, EventHandler]`。 |

### 使用示例

```python
from event_bus import EventHandlerRegistry, EventHandler

handler_registry = EventHandlerRegistry()
handler_id = handler_registry.register(my_handler)
assert handler_id in handler_registry
assert handler_registry.version == 1  # 每次注册递增

handler_registry.unregister(handler_id)
assert handler_registry.version == 2  # 注销也递增
```

> **注意**：`get_handlers(event_type)` 已移除。匹配功能移至 [Matcher](matcher.md)，总线内部自动使用。
