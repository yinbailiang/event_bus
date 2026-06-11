# EventForwardMiddleware

## Overview

`EventForwardMiddleware` is a unidirectional cross-bus event forwarding middleware that
synchronously forwards events from a source bus to another `EventBus` instance during the
`on_publish` phase. Suitable for system integration, event mirroring, multi-tenant routing,
and similar scenarios.

---

## Architecture

```text
Source Bus                                Target Bus
  │                                         │
  │  publish("order.created")               │
  ▼                                         │
┌─────────────────────────┐                 │
│  before_publish chain    │                 │
│  ┌─────────────────────┐ │                 │
│  │ Core publish         │ │                 │
│  │ (validate → enqueue) │ │                 │
│  └─────────────────────┘ │                 │
│  on_publish chain        │                 │
│  ┌─────────────────────┐ │                 │
│  │ EventForwardMiddleware │─── publish ──► │
│  │ (error-isolated)     │ │                 │
│  └─────────────────────┘ │                 │
└─────────────────────────┘                 ▼
                                       ┌─────────────┐
                                       │ Target Handler│
                                       └─────────────┘
```

> **Key design**: Forwarding happens in the `on_publish` phase, after the source event has
> been validated and enqueued. Forwarding failures do **not** affect the source bus's
> normal operation (error isolation).

---

## Quick Start

```python
from event_bus import EventBus, EventRegistry, EventHandlerRegistry, MiddlewareChain
from event_bus.templates.middlewares import EventForwardMiddleware

# 1. Create two buses
source_bus = EventBus(source_registry, source_handlers)
audit_bus = EventBus(audit_registry, audit_handlers)

# 2. Register forwarding middleware
fw = EventForwardMiddleware(
    target=audit_bus,
    source_name="main→audit",  # Source name shown on the target bus
)
chain = MiddlewareChain()
chain.add(fw)

# 3. Source bus uses the forwarding middleware
source_bus = EventBus(
    source_registry,
    source_handlers,
    middleware_chain=chain,
)

# 4. Start both buses
await audit_bus.start()
await source_bus.start()

# 5. Publish event — automatically forwarded to audit_bus
await source_bus.proxy("svc").publish("order.created", {...})
```

---

## API Reference

### `EventForwardMiddleware`

```python
class EventForwardMiddleware(Middleware):
    def __init__(
        self,
        target: Union[EventBus, TargetBusProvider],
        source_name: str = "event_forward",
        event_filter: Optional[EventFilter] = None,
        forward_system_events: bool = False,
    ) -> None
```

| Parameter | Type | Default | Description |
| - | - | - | - |
| `target` | `EventBus \| TargetBusProvider` | (required) | Target bus or callback returning a target bus. |
| `source_name` | `str` | `"event_forward"` | Source identifier used when publishing on the target bus. |
| `event_filter` | `Optional[EventFilter]` | `None` | Event filter callback. `None` forwards all non-system events. |
| `forward_system_events` | `bool` | `False` | Whether to forward `event_bus.*` system events. |

## Usage Patterns

### Static Target Bus

```python
fw = EventForwardMiddleware(target=audit_bus, source_name="prod->audit")
```

### Dynamic Target Bus

Dynamically retrieve the latest bus instance on each forward:

```python
def get_tenant_bus() -> EventBus:
    tenant_id = current_tenant.get()
    return tenant_bus_registry[tenant_id]

fw = EventForwardMiddleware(target=get_tenant_bus, source_name="router")
```

### Event Filtering

```python
# Only forward order-related events
fw = EventForwardMiddleware(
    target=other_bus,
    event_filter=lambda e: e.name.startswith("order."),
)

# Using factory function: allowlist mode
from event_bus.templates import make_event_name_filter

fw = EventForwardMiddleware(
    target=other_bus,
    event_filter=make_event_name_filter("order.created", "order.paid", mode="white"),
)

# Async filtering
async def async_filter(event: Event) -> bool:
    user = await get_user_from_event(event)
    return user.tier == "premium"

fw = EventForwardMiddleware(target=other_bus, event_filter=async_filter)
```

---

## Type Aliases

### `EventFilter`

```python
EventFilter = Callable[[Event], Union[bool, Awaitable[bool]]]
```

Event filter callback signature: receives an `Event` object, returns `bool` synchronously
or asynchronously.

### `TargetBusProvider`

```python
TargetBusProvider = Callable[[], Union[EventBus, Awaitable[EventBus]]]
```

Target bus provider signature: returns an `EventBus` instance synchronously or
asynchronously.

---

## Built-in Factory Functions

### `make_event_name_filter`

```python
def make_event_name_filter(
    *event_names: str,
    mode: Literal['white', 'black'] = 'white',
) -> EventFilter:
```

Creates an event-name-based filter callback.

| Parameter | Description |
| - | - |
| `event_names` | Event names to match. |
| `mode` | `"white"` for allowlist, `"black"` for blocklist. |

### `make_bidirectional_forward`

```python
def make_bidirectional_forward(
    bus_a: EventBus | TargetBusProvider,
    bus_b: EventBus | TargetBusProvider,
    *,
    source_a_to_b: str = 'a→b',
    source_b_to_a: str = 'b→a',
    event_filter: EventFilter | None = None,
    anti_recursion: bool = True,
    forward_system_events: bool = False,
) -> tuple[EventForwardMiddleware, EventForwardMiddleware]:
```

Creates a pair of unidirectional forwarding middlewares in one call, enabling
bidirectional event sync between two buses.

| Parameter | Type | Default | Description |
| - | - | - | - |
| `bus_a` | `EventBus \| TargetBusProvider` | (required) | Bus A or its factory callback. |
| `bus_b` | `EventBus \| TargetBusProvider` | (required) | Bus B or its factory callback. |
