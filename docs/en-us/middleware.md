# Middleware / MiddlewareChain

## Overview

The middleware system provides pluggable hooks into the publish flow. By inserting custom logic
before and after publishes, you can implement logging, validation, tracing, monitoring,
message transformation, and other cross-cutting concerns **without modifying core business handlers**.

Middlewares use the **onion model (chain of responsibility)** design: multiple middlewares
wrap around each other in registration order, with each middleware able to run logic both
before and after calling `next()`.

---

## Architecture

```text
publish request
  │
  ▼
┌─────────────────────────────────────┐
│  Middleware 1 (outer)               │
│  ┌─────────────────────────────────┐ │
│  │ Middleware 2 (inner)            │ │
│  │ ┌─────────────────────────────┐ │ │
│  │ │ Core publish logic          │ │ │
│  │ │ ├─ validate → build → queue │ │ │
│  │ │ └─ on_publish chain (onion) │ │ │
│  │ └─────────────────────────────┘ │ │
│  └─────────────────────────────────┘ │
└─────────────────────────────────────┘
```

The `on_publish` chain is nested **inside** the core publish logic,
which is the innermost layer of the `before_publish` chain.
Code after `await next(...)` in `before_publish` executes **after**
the entire `on_publish` chain completes.

### Two Hook Phases

| Phase | Timing | Available Info |
| - | - | - |
| `before_publish` | **Before** validation, Event construction, and enqueue | Event name, source, raw data, predecessor event |
| `on_publish` | **After** Event is successfully enqueued | Complete Event object (id, sources, timestamps) |

---

## Middleware

Abstract base class. Custom middlewares must implement `before_publish` and `on_publish`.

```python
class Middleware(ABC):
    # Lifecycle
    async def on_setup(self, bus: EventBus) -> None: ...
    async def on_teardown(self, bus: EventBus) -> None: ...

    # Publish hooks (required)
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

    # Error hook (optional)
    async def on_publish_error(
        self,
        error: Exception,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
    ) -> None: ...
```

### Lifecycle Hooks

`on_setup` is called after `bus.start()` completes. `on_teardown` is called in
**reverse registration order** when `stop()` finishes. Middlewares that raise
exceptions in `on_setup` are automatically removed from the chain.

### `before_publish`

Runs **before** any publish logic (including event declaration validation).
**Must** call `await next(...)` for the event to proceed.

| Parameter | Description |
| - | - |
| `event_registry` | Access event declaration metadata. |
| `name` | Event type name. |
| `source` | Publisher identifier. |
| `data` | Raw payload (dict or BaseModel, may be `None`). |
| `old_event` | Predecessor event in a chain (may be `None`). |
| `next` | Call to invoke next middleware (or core publish). **Must be called** for event to proceed. |

### `on_publish`

Runs **after** the Event is successfully enqueued. Full runtime event info available.

| Parameter | Description |
| - | - |
| `event` | Complete Event object (name, data, id, sources, timestamps). |
| `next` | Call to invoke next middleware. |

### `on_publish_error`

Called when the publish flow raises an exception (optional). All middlewares are notified
in registration order. One middleware's error doesn't block others from being notified.

---

## MiddlewareChain

Manages an ordered list of middlewares with lazy chain building.

```python
class MiddlewareChain:
    def add(self, middleware: Middleware) -> 'MiddlewareChain'
    def insert(self, index: int, middleware: Middleware) -> 'MiddlewareChain'
    def remove(self, middleware: Middleware) -> None
    def clear(self) -> None

    async def setup(self, bus: EventBus) -> List[Middleware]
    async def teardown(self, bus: EventBus) -> None

    @property
    def middlewares(self) -> list[Middleware]
```

### Building the Chain

On each publish, `build_before_publish()` and `build_on_publish()` lazily construct
the chain. Results are cached and auto-invalidated when the middleware list changes.

```python
# Runtime add/remove — next publish picks up changes immediately
bus.proxy("admin").middleware.add(RateLimitMiddleware(max_requests=10))
bus.proxy("admin").middleware.remove(some_mw)
```

---

## Built-in Middlewares

See [templates/middlewares/](templates/middlewares/middlewares.md) for detailed docs.

| Middleware | Hook | Description |
| - | - | - |
| `JSONLLoggingMiddleware` | `on_publish` | Append events to JSONL file (zero deps). |
| `SQLiteLoggingMiddleware` | `on_publish` | Log events to SQLite database (requires `aiosqlite`). |
| `RateLimitMiddleware` | `before_publish` | Sliding-window rate limiting (global or per-event). |
| `EventTransformMiddleware` | `before_publish` | Transform event name, data fields (rename, redact, inject). |
| `EventBlockMiddleware` | `before_publish` | Block events matching predicates (allowlist/blocklist). |
| `EventForwardMiddleware` | `before_publish` | Forward events to another EventBus instance. |
| `RecursionGuardMiddleware` | `before_publish` | Prevent infinite event loops (configurable depth). |
| `MetricsMiddleware` | `before_publish` | Lightweight Prometheus/OTel-style metrics. |

---

## Custom Middleware Example

```python
from event_bus import (
    BeforePublishNext, Event, EventBus, EventRegistry,
    Middleware, OnPublishNext,
)

class TimingMiddleware(Middleware):
    """Logs publish duration per event type."""

    async def on_setup(self, bus: EventBus) -> None:
        self._bus = bus

    async def on_teardown(self, bus: EventBus) -> None:
        pass

    async def before_publish(
        self,
        event_registry: EventRegistry,
        name: str,
        source: str,
        data,
        old_event: Event | None,
        next: BeforePublishNext,
    ) -> None:
        import time
        t0 = time.perf_counter()
        await next(event_registry, name, source, data, old_event)
        elapsed = time.perf_counter() - t0
        logger.info("Event %s took %.4fs", name, elapsed)

    async def on_publish(
        self, event: Event, next: OnPublishNext
    ) -> None:
        await next(event)

# Usage
chain = MiddlewareChain()
chain.add(TimingMiddleware())
bus = EventBus(events, handlers, middleware_chain=chain)
```
