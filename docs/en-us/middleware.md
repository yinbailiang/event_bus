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

`on_setup` is called after `bus.start()` completes, and **immediately when hot-adding
via `add()` / `insert()` on a running bus**. `on_teardown` is called in **reverse
registration order** when `stop()` finishes, and immediately when `remove()` /
`clear()` is called on a running bus.

Middlewares that raise exceptions in `on_setup` are automatically removed from
the chain (at startup) or rejected (at hot-add).

> **Warning**: Runtime `remove()` does not wait for in-flight hooks to complete.
> After `on_teardown` is called, a middleware instance that has already entered
> `before_publish` / `on_publish` may still execute. Middleware authors should
> ensure their own state cleanup is safe against these residual calls.

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

Manages an ordered list of middlewares with lazy chain building and hot-reload support.

```python
class MiddlewareChain:
    def __init__(self) -> None

    # CRUD (async — triggers lifecycle immediately when bus is running)
    async def add(self, middleware: Middleware) -> 'MiddlewareChain'
    async def insert(self, index: int, middleware: Middleware) -> 'MiddlewareChain'
    async def remove(self, middleware: Middleware) -> None
    async def clear(self) -> None

    @property
    def middlewares(self) -> list[Middleware]

    # Chain builders
    def build_before_publish(
        self, final_handler: BeforePublishNext
    ) -> BeforePublishNext
    def build_on_publish(
        self, final_handler: OnPublishNext
    ) -> OnPublishNext
    def build_on_publish_error(
        self, final_handler: OnPublishErrorNext
    ) -> OnPublishErrorNext

    # Lifecycle
    async def setup(self, bus: EventBus) -> List[Middleware]
    async def teardown(self, bus: EventBus) -> None
```

| Method | Description |
| - | - |
| `add(mw)` | **async**. Append to the chain. Calls `on_setup` immediately if bus is running. Returns self. |
| `insert(i, mw)` | **async**. Insert at position. Calls `on_setup` immediately if bus is running. |
| `remove(mw)` | **async**. Remove from chain. Calls `on_teardown` immediately if bus is running. Raises `ValueError` if not found. |
| `clear()` | **async**. Remove all middlewares. Calls `on_teardown` on each if bus is running. |
| `middlewares` | Returns a copy of the current middleware list. |
| `build_before_publish(f)` | Build the `before_publish` chain with `f` as the innermost handler. |
| `build_on_publish(f)` | Build the `on_publish` chain with `f` as the innermost handler. |
| `build_on_publish_error(f)` | Build the `on_publish_error` chain with `f` as the innermost handler. |
| `setup(bus)` | Call `on_setup` on all middlewares in registration order. Returns failed ones (auto-removed). |
| `teardown(bus)` | Call `on_teardown` on all middlewares in **reverse** order. **Idempotent** — safe to call repeatedly. |

### Hot Reload

Middlewares can be added or removed at runtime via `bus.proxy().middleware`.
Changes take effect on the **next** publish — the chain cache is invalidated
automatically.

```python
# Inside a handler or middleware
chain = bus_proxy.middleware

# Hot-add — on_setup is called immediately
await chain.add(AuditMiddleware())

# Hot-remove — on_teardown is called immediately
# Note: in-flight chains may still invoke the middleware after this
await chain.remove(some_mw)

# Clear all
await chain.clear()
```

> **Warning**: `remove()` returns immediately without waiting for in-flight hooks.
> Middleware authors should make `on_teardown` idempotent — see
> `Middleware.on_teardown` docs.

### Building the Chain

On each publish, `build_before_publish()`, `build_on_publish()`, and
`build_on_publish_error()` lazily construct the chain. Results are cached
and auto-invalidated when the middleware list changes.

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
await chain.add(TimingMiddleware())
bus = EventBus(events, handlers, middleware_chain=chain)
```
