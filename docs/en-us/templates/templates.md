# Templates

Advanced patterns built on top of the core event bus. These are optional — import from
`event_bus.templates` to use them.

> Some templates require optional dependencies. Install with:
>
> ```bash
> pip install infinity_bus[templates]
> ```

---

## Table of Contents

| Template | Description |
| - | - |
| [handler](simple_handler.md) | Function-to-handler decorator — convert sync/async functions into EventHandler subclasses with signature validation. |
| [expect](expect.md) | One-shot event listener — wait for a specific event and get a future. |
| [request](request.md) | RPC-style request/response over the event bus. |
| [pipe](pipe.md) | Bidirectional async pipe abstraction (in-process or networked). |
| [register](register.md) | Bulk event/handler registration with decorator syntax. |
| [middlewares/](middlewares/middlewares.md) | Built-in middlewares: logging, rate-limiting, forwarding, blocking, transform, recursion guard. |

---

## handler

```python
from event_bus.templates import handler

@handler(UserCreated)
async def send_welcome_email(payload: UserCreatedPayload) -> None:
    print(f"Welcome, {payload.email}!")

handler_registry.register(send_welcome_email())
```

See [simple_handler.md](simple_handler.md) for signature validation, sync/async support, and custom timeouts.

## expect

```python
from event_bus.templates import expect

async with expect(bus_proxy, "user.login") as future:
    await bus_proxy.publish("auth.request", {...})
    event = await future  # blocks until user.login received
```

See [expect.md](expect.md) for filtering, error handling, and advanced usage.

## request

```python
from event_bus.templates import request

response = await request(
    bus_proxy,
    req_event="order.create",
    req_data={"item": "widget"},
    resp_event="order.created",
    timeout=10.0,
)
```

See [request.md](request.md) for protocol definition and error handling.

## pipe

```python
from event_bus.templates import open_pipe

async with open_pipe(pipe_id="my_pipe") as pipe:
    await pipe.send(MyData(...))
    reply = await pipe.receive()
```

See [pipe.md](pipe.md) for allocators and multi-process pipes.

## register

```python
from event_bus.templates import ModuleEventRegister, ModuleHandlerRegister

events = ModuleEventRegister("orders")

@events.event
class OrderCreated(EventDeclaration):
    name = "order.created"
    payload_type = OrderPayload

events.register_all_events(event_registry)
```

See [register.md](register.md) for atomic registration and error handling.

## Built-in Middlewares

See [middlewares/middlewares.md](middlewares/middlewares.md) for full documentation.

```python
from event_bus.templates.middlewares import (
    JSONLLoggingMiddleware,
    RateLimitMiddleware,
    MetricsMiddleware,
)

chain = MiddlewareChain()
chain.add(JSONLLoggingMiddleware("events.jsonl"))
chain.add(RateLimitMiddleware(max_requests=100, window_seconds=1.0))
chain.add(MetricsMiddleware())
```
