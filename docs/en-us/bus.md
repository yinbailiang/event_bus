# EventBus

## Overview

`EventBus` is the central dispatch hub — responsible for task queuing, concurrency control,
error reporting, and lifecycle management. `Proxy` is the only interface for publishing events.
The system includes two built-in special events: `ShutdownEvent` and `TaskErrorEvent`.

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

    # Lifecycle
    async def start(self) -> None
    async def stop(self) -> None
    async def __aenter__(self) -> "EventBus"
    async def __aexit__(self, ...) -> Optional[bool]

    # Proxy
    def proxy(self, source: str, raw_event: Optional[Event] = None) -> Proxy

    # Observability
    @property
    def is_running(self) -> bool
    @property
    def is_publishing_enabled(self) -> bool
    @property
    def active_task_count(self) -> int
    @property
    def queue_size(self) -> int
```

### Constructor Parameters

| Parameter | Type | Default | Description |
| - | - | - | - |
| `event_registry` | `EventRegistry` | (required) | Event registry instance. |
| `handler_registry` | `EventHandlerRegistry` | (required) | Handler registry instance. |
| `max_queue_size` | `int` | `1024` | Max event queue capacity. `put()` blocks when full. |
| `max_handler_semaphore` | `int` | `256` | Max concurrent handlers (semaphore). |
| `shutdown` | `ShutdownConfig` | `ShutdownConfig()` | Shutdown behavior config. |
| `middleware_chain` | `Optional[MiddlewareChain]` | `None` | Middleware chain for publish hooks. |

Constructing automatically registers `ShutdownEvent` and `TaskErrorEvent` (if not present),
and creates an internal [Matcher](matcher.md) for event-to-handler routing.

### Lifecycle

| Method | Description |
| - | - |
| `start()` | Starts the dispatch loop. Idempotent. |
| `stop()` | Graceful shutdown: publish `__shutdown__` → reject new publishes → drain queue → cancel dispatch → wait for active tasks. Idempotent. |
| `async with EventBus(...) as bus:` | Context manager, auto start/stop. `stop()` errors on exit won't mask body exceptions. |

### Observability

| Property | Type | Description |
| - | - | - |
| `is_running` | `bool` | Whether the bus is running. Remains `True` during shutdown drain/wait. |
| `is_publishing_enabled` | `bool` | Whether new events are accepted. Cleared immediately after `stop()` begins. |
| `active_task_count` | `int` | Number of currently active handler tasks. |
| `queue_size` | `int` | Number of events currently queued for dispatch. |

#### State Separation During Shutdown

| Phase | `is_running` | `is_publishing_enabled` |
| - | - | - |
| Normal operation | `True` | `True` |
| Shutting down (draining) | `True` | **`False`** |
| Stopped | `False` | `False` |

#### Shutdown Sequence Diagram

```text
start()                                   stop()
  │                                         │
  │  dispatch_task created                   │  1. publish __shutdown__
  │  _running.set()           ◄── running ───►  _running.clear()
  │  _enable_publish.set()    ◄── publish ───►  _enable_publish.clear()
  │                                         │
  │  mw_chain.setup()                       │  2. drain queue (queue.join())
  │                                         │  3. cancel dispatch_task
  ▼                                         ▼  4. wait active tasks
is_running=True                             │  5. mw_chain.teardown()
is_publishing_enabled=True                  ▼
                                          is_running=False
                                          is_publishing_enabled=False
```

---

## EventBus.Proxy

The proxy is the **only** interface for publishing events. It auto-records event source and chain.

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
    def middleware(self) -> MiddlewareChain
```

| Member | Description |
| - | - |
| `publish(name, data=None)` | Publish an event. `data` can be a dict or pydantic model. Raises `RuntimeError` if not started, `BusShuttingDown` if stopping, `ValueError` for unknown events, `TypeError` for payload mismatch. |
| `handlers_registry` | Read-only access to handler registry. |
| `events_registry` | Read-only access to event registry. |
| `middleware` | Access middleware chain for runtime modification. |

### Usage

```python
# Basic publish
await bus.proxy("my_service").publish("user.login", {"user_id": "42"})

# Chained publish (event forwarding)
class LoginHandler(EventHandler):
    async def handle(self, payload, bus_proxy, raw_event):
        await bus_proxy.publish("user.profile_loaded", profile_data)
```

---

## Built-in Events

### ShutdownEvent

```python
class ShutdownEvent(EventDeclaration):
    name = 'event_bus.__shutdown__'
    # payload_type = None (no payload)
```

Published automatically when `stop()` is called, before queue draining begins.
Handlers can subscribe to this event to perform cleanup before shutdown.

### TaskErrorEvent

```python
class TaskErrorPayload(BaseModel):
    error_event: Event
    handler_id: Optional[str]
    handler_name: str
    error_type: str
    error_message: str

class TaskErrorEvent(EventDeclaration):
    name = 'event_bus.__task_error__'
    payload_type = TaskErrorPayload
```

Published when a handler raises an exception. Source is always `"EventBusErrorReporter"`.
Use this for centralized error monitoring.

```python
class ErrorMonitor(EventHandler):
    def __init__(self):
        super().__init__(["event_bus.__task_error__"])

    async def handle(self, payload, bus_proxy, raw_event):
        if isinstance(payload, TaskErrorPayload):
            logger.error(
                "Handler %s failed on %s: %s",
                payload.handler_name,
                payload.error_event.name,
                payload.error_message,
            )
```

---

## ShutdownConfig

```python
class ShutdownConfig(BaseModel):
    queue_timeout_min: float = 1.0    # Min queue drain wait (seconds)
    queue_timeout_max: float = 15.0   # Max queue drain wait (seconds)
    tasks_timeout: float = 15.0       # Active task completion wait (seconds)
    avg_wait_time: float = 0.05       # Estimated per-event processing time (seconds)
```

Queue drain timeout is dynamically calculated:
```
timeout = max(queue_timeout_min, min(queue_timeout_max, queue_size × avg_wait_time))
```

---

## Exceptions

| Exception | When |
| - | - |
| `BusShuttingDown` | Publish attempted while bus is stopping. Handlers should treat as a cleanup signal. |
| `RuntimeError` | Publish attempted before bus has started. |
| `ValueError` | Unknown event name, or payload mismatch (required vs. provided). |
| `TypeError` | Payload type doesn't match the event declaration. |
