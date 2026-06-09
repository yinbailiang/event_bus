# EventHandler / EventHandlerRegistry 文档

## 概述

`EventHandler` 是事件处理器的抽象基类，通过订阅模式（支持正则表达式）监听感兴趣的事件。`EventHandlerRegistry` 管理所有处理器实例并负责事件路由匹配。

---

## EventHandler

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
| `subscriptions` | 订阅的事件名模式列表，支持正则表达式。如 `["user\..*"]` 匹配所有 `user.*` 事件。 |
| `handle_timeout` | 单次 `handle` 调用的超时时间（秒）。`None` 表示无限等待。默认 `1.0`。 |
| `__call__` | 总线内部入口，自动解包 `Event` 后调用 `handle(payload, bus_proxy, raw_event)`。 |
| `handle(payload, bus_proxy, raw_event)` | **子类必须实现**。`payload` 为已解包的负载（可能为 `None`）。`bus_proxy` 提供受限的总线访问。`raw_event` 为完整事件对象。 |

### 使用示例

```python
class LoginHandler(EventHandler):
    def __init__(self):
        super().__init__(subscriptions=[r"user\..*"])  # 匹配所有 user.* 事件

    async def handle(self, payload, bus_proxy, raw_event):
        if isinstance(payload, UserLoginPayload):
            print(f"User {payload.user_id} logged in at {payload.timestamp}")
            # 可通过 bus_proxy.publish 发布新事件，形成处理链
```

### 正则订阅

`subscriptions` 支持正则表达式，实现灵活的事件名匹配：

```python
class AuditHandler(EventHandler):
    def __init__(self):
        # 匹配所有 order.* 和 payment.* 事件
        super().__init__(subscriptions=[r"order\..*", r"payment\..*"])

    async def handle(self, payload, bus_proxy, raw_event):
        print(f"Audit: {raw_event.name}")
```

正则编译结果会被 LRU 缓存，避免重复编译开销。

---

## EventHandlerRegistry

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

### 使用示例

```python
handler_registry = EventHandlerRegistry()
hid = handler_registry.register(LoginHandler())

# 获取匹配某个事件的所有处理器
matched = handler_registry.get_handlers("user.login")
for handler_id, handler in matched:
    print(f"Handler {handler_id}: {handler.__class__.__name__}")

# 注销处理器
handler_registry.unregister(hid)
```
