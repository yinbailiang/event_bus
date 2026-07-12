# EventHandler / EventHandlerRegistry

## Overview

`EventHandler` is the abstract base class for event handlers. It matches events via
subscription patterns — exact `str` or `Regex` regular expressions.
`EventHandlerRegistry` manages handler instance CRUD.

---

## EventHandler

Abstract base class. All business handlers must subclass and implement `handle()`.

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

| Parameter/Method | Description |
| - | - |
| `subscriptions` | Event patterns this handler listens to. `str` for exact match (`event_type == subscription`), `Regex` for full regex match. E.g., `["user.login"]` only matches `"user.login"`, `[Regex(r"user\..*")]` matches all `user.*` events. |
| `handle_timeout` | Per-invocation timeout (seconds). `None` = no limit. Default `32.0`. |
| `__call__` | Internal entry point. Receives the raw `EventBus`, creates a proxy via `bus.proxy(handler_name, event)`, then delegates to `handle()`. |
| `handle(payload, bus_proxy, raw_event)` | **Subclass must implement.** `payload` is the unpacked payload (may be `None`). `bus_proxy` provides limited bus access. `raw_event` is the full event object. |

### `handle()` Signature

```python
async def handle(
    self,
    payload: Optional[BaseModel],      # Typed payload (None if event has no payload)
    bus_proxy: EventBus.Proxy,          # Proxy for publishing follow-up events
    raw_event: Event                    # Full event metadata (id, sources, timestamps)
) -> None:
```

### Usage

```python
from event_bus import EventHandler, Regex

class LoginHandler(EventHandler):
    def __init__(self):
        super().__init__(subscriptions=[Regex(r"user\..*")])  # match all user.* events

    async def handle(self, payload, bus_proxy, raw_event):
        if isinstance(payload, UserLoginPayload):
            print(f"User {payload.user_id} logged in at {payload.timestamp}")
            # Chain-publish via bus_proxy.publish
```

### Subscription Patterns

`subscriptions` supports two matching modes:

| Type | Example | Matches |
| - | - | - |
| Exact `str` | `"order.created"` | Exact match only |
| `Regex` | `Regex(r"order\..*")` | `order.created`, `order.paid`, etc. |
| `Regex` | `Regex(r"(?!system\.).*")` | All events except `system.*` |

```python
from event_bus import EventHandler, Regex

class AuditHandler(EventHandler):
    def __init__(self):
        # Exact match for single event
        # Regex match for all order.* and payment.* events
        super().__init__(subscriptions=["user.login", Regex(r"order\..*"), Regex(r"payment\..*")])

    async def handle(self, payload, bus_proxy, raw_event):
        print(f"Audit: {raw_event.name}")
```

---

## EventHandlerRegistry

Manages handler instance registration, lookup, and removal.

```python
class EventHandlerRegistry:
    def __init__(self) -> None
    def register(self, handler: EventHandler) -> str
    def get(self, handler_id: str) -> Optional[EventHandler]
    def unregister(self, handler_id: str) -> bool
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

| Method/Property | Description |
| - | - |
| `register(handler)` | Register a handler instance, returns a unique handler ID (UUID hex). Version increments. |
| `unregister(handler_id)` | Remove by ID. Returns `True` if removed, `False` if ID not found. Version increments. |
| `get(handler_id)` | Lookup by ID, returns `None` if not found. |
| `clear()` | Remove all registered handlers. Version increments. |
| `__len__()` | Supports `len(registry)`. |
| `__contains__()` | Supports `handler_id in registry`. |
| `__iter__()` | Supports `for hid, h in registry` iteration. |
| `version` | Monotonic version, incremented on every change (add/remove/clear). Used by [Matcher](matcher.md) for invalidation. |
| `handlers_count` | Total number of registered handlers. |
| `all_handlers` | Snapshot copy of all registered handlers as `Dict[str, EventHandler]`. |

### Usage

```python
from event_bus import EventHandlerRegistry, EventHandler

handler_registry = EventHandlerRegistry()
handler_id = handler_registry.register(my_handler)
assert handler_id in handler_registry
assert handler_registry.version == 1  # increments on every register

handler_registry.unregister(handler_id)
assert handler_registry.version == 2  # unregister also increments
```

> **Note**: `get_handlers(event_type)` has been removed. Matching logic moved to [Matcher](matcher.md); the bus uses it internally.
