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

| Parameter | Type | Default | Description |
| - | - | - | - |
| `subscriptions` | `Optional[List[Regex \| str]]` | `[]` | Event patterns this handler listens to. |
| `handle_timeout` | `Optional[float]` | `32.0` | Per-invocation timeout (seconds). `None` = no limit. |
| `__call__` | — | — | Internal entry point. Receives the raw ``EventBus``, creates a proxy via ``bus.proxy(handler_name, event)``, then delegates to ``handle()``. |

### `handle()` Signature

```python
async def handle(
    self,
    payload: Optional[BaseModel],      # Typed payload (None if event has no payload)
    bus_proxy: EventBus.Proxy,          # Proxy for publishing follow-up events
    raw_event: Event                    # Full event metadata (id, sources, timestamps)
) -> None:
```

### Subscription Patterns

| Type | Example | Matches |
| - | - | - |
| Exact `str` | `"order.created"` | Exact match only |
| `Regex` | `Regex(r"order\..*")` | `order.created`, `order.paid`, etc. |
| `Regex` | `Regex(r"(?!system\.).*")` | All events except `system.*` |

```python
from event_bus import EventHandler, Regex

class OrderHandler(EventHandler):
    def __init__(self):
        super().__init__([
            "system.heartbeat",           # exact match
            Regex(r"order\..*"),          # regex match
        ])

    async def handle(self, payload, bus_proxy, raw_event):
        ...
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
    def all_handlers(self) -> Dict[str, EventHandler]
```

| Method | Description |
| - | - |
| `register(handler)` | Register a handler instance, returns auto-generated ID. |
| `get(handler_id)` | Lookup by ID, returns `None` if not found. |
| `unregister(handler_id)` | Remove by ID. Returns `True` if removed. |
| `clear()` | Remove all handlers. |
| `version` | Monotonic version, incremented on every change. |
| `all_handlers` | Snapshot copy of all registered handlers. |
