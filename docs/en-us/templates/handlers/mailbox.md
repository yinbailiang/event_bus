# Mailbox Handler

## Overview

`MailboxHandler` is a **mailbox-pattern** `EventHandler` abstract base class. It enqueues incoming events into an internal mailbox queue, and subclasses implement the `process()` method to dequeue and handle events one at a time. Ideal for scenarios requiring **serial consumption**, **backpressure control**, or a **custom task loop**.

Unlike directly subclassing `EventHandler` and implementing `handle()`, `MailboxHandler` decouples "receiving events" and "processing events" into two separate coroutines:

- `handle()` — runs in the bus dispatch context, only responsible for enqueuing events into the mailbox.
- `process()` — runs in its own `asyncio.Task`, consuming events from the queue at its own pace.

---

## Use Cases

- Scenarios requiring **strictly serial** event processing (avoid concurrent execution within the same handler).
- **Backpressure control** for burst traffic (via bounded queue capacity).
- **Custom event loops** (e.g., periodic cleanup, timed batch processing, state-machine-driven consumption).
- Emulating the **mailbox** semantics of the Actor model.
- Aggregating events from multiple sources into a single, ordered consumer.

---

## Class Signature

```python
class MailboxHandler(EventHandler, ABC):
    def __init__(
        self,
        subscriptions: list[str | Regex],
        config: MailboxConfig | None = None,
    ) -> None: ...

    async def process(self) -> None: ...
    async def get(self) -> tuple[Event, EventBus.Proxy]: ...
```

| Member | Type | Description |
| - | - | - |
| `subscriptions` | `list[str \| Regex]` | Event patterns to subscribe to. `ShutdownEvent` is automatically appended. |
| `config` | `MailboxConfig \| None` | Mailbox configuration. Uses defaults when `None`. |
| `process()` | `@abstractmethod async` | Custom task loop that subclasses must implement. Use `await self.get()` to fetch the next event. |
| `get()` | `async → (Event, Proxy)` | Dequeues the next `(event, bus proxy)` from the mailbox. Blocks when the queue is empty. |
| `bus` | `EventBus \| None` | The bound `EventBus` instance, available after the first event arrives. |
| `is_running` | `bool` | Whether the `process()` background task is currently running. |

---

## MailboxConfig

```python
class MailboxConfig(BaseModel):
    queue_put_timeout: float | None = None   # Enqueue timeout (seconds); None = wait indefinitely
    restart_delay: float = 0.5               # Base restart delay after exception (seconds)
    restart_jitter: float = 0.2              # Random jitter added to restart delay, prevents thundering herd
    max_queue_size: int = 0                  # Max mailbox queue capacity; 0 = unbounded
```

| Parameter | Default | Description |
| - | - | - |
| `queue_put_timeout` | `None` | Enqueue timeout. `None` waits forever; a value raises `RuntimeError` on timeout. |
| `restart_delay` | `0.5` | Base seconds to wait before restarting after `process()` exits with an exception. |
| `restart_jitter` | `0.2` | Random offset in `[0, jitter]` added to `restart_delay`, avoiding thundering herd with multiple instances. |
| `max_queue_size` | `0` | Maximum queue capacity. `0` means unbounded. |

> Actual restart wait time = `restart_delay + random(0, restart_jitter)`

---

## How It Works

```text
Event arrives
  │
  ▼
handle() ─── ShutdownEvent? ─── Yes ─── Cancel process() task ─── Return
  │
  │ No
  ▼
First call? ─── Yes ─── Create process() background Task
  │
  ▼
put(event, proxy) → Enqueue
  │
  ▼
process() coroutine (independent Task)
  │
  ▼
await get() → Dequeue (event, proxy)
  │
  ▼
Business logic (publish new events via proxy.publish())
  │
  ▼
Loop back to get()
```

1. **Lazy start**: The `process()` background `asyncio.Task` is created only when the **first non-ShutdownEvent** arrives.
2. **Enqueue**: `handle()` places `(Event, EventBus.Proxy)` tuples into the internal `asyncio.Queue`.
3. **Serial consumption**: `process()` loops on `await self.get()` to handle events one by one.
4. **Exception restart**: If `process()` exits due to a non-`CancelledError` exception, `_process_loop` waits `restart_delay + jitter` seconds, then re-invokes `process()`.
5. **Graceful shutdown**: On `ShutdownEvent`, the `process()` task is cancelled and awaited to completion.

---

## Examples

### Basic: Serial Event Collection

```python
from event_bus import Event, EventBus
from event_bus.templates.handlers.mailbox import MailboxHandler

class MyHandler(MailboxHandler):
    def __init__(self):
        super().__init__(subscriptions=['my.event'])
        self.received: list[Event] = []

    async def process(self) -> None:
        while True:
            event, proxy = await self.get()
            self.received.append(event)
            # Publish new events via proxy.publish()
```

### Custom Loop: Timed Batch Processing

```python
import asyncio

class BatchHandler(MailboxHandler):
    def __init__(self):
        super().__init__(subscriptions=['data.input'])

    async def process(self) -> None:
        while True:
            batch: list[Event] = []
            try:
                event, _ = await asyncio.wait_for(self.get(), timeout=1.0)
                batch.append(event)
                while True:
                    try:
                        event, _ = await asyncio.wait_for(self.get(), timeout=0.1)
                        batch.append(event)
                    except asyncio.TimeoutError:
                        break
            except asyncio.TimeoutError:
                pass
            if batch:
                await self._process_batch(batch)

    async def _process_batch(self, batch: list[Event]) -> None:
        print(f'Processed {len(batch)} events')
```

### Exception Restart: Auto-Recovery After Crash

```python
class RobustHandler(MailboxHandler):
    def __init__(self):
        super().__init__(
            subscriptions=['task.request'],
            config=MailboxConfig(restart_delay=0.5, restart_jitter=0.2),
        )

    async def process(self) -> None:
        while True:
            event, proxy = await self.get()
            try:
                await self._handle_task(event)
            except Exception:
                # Failure of a single event continues to the next.
                # Only a crash of process() as a whole triggers a restart.
                pass
```

### Accessing the Bus

```python
class BusAwareHandler(MailboxHandler):
    def __init__(self):
        super().__init__(subscriptions=['status.check'])

    async def process(self) -> None:
        while True:
            event, proxy = await self.get()
            # Access the raw bus via self.bus
            if self.bus and self.bus.is_running:
                await proxy.publish('status.reply', {'ok': True})
```

---

## Important Notes

- **`process()` must be an infinite loop**: `_process_loop` re-invokes `process()` after it returns normally. If `process()` runs once and returns, it will be called again immediately, creating a busy loop. Always wrap your logic in `while True`.
- **Event loss on crash**: If `process()` crashes after `get()` returns but before processing completes, that event is lost. The restart loop calls `get()` again for the next event — it does not retry the lost one.
- **ShutdownEvent auto-subscription**: `ShutdownEvent` is automatically added to subscriptions at construction time. When received, it cancels the `process()` task and does **not** enqueue the shutdown event.
- **Single bus capture**: `self.bus` is set on the first `handle()` call and never changes. If the same handler is referenced by multiple buses, `bus` points to the first one only.
- **Only non-`CancelledError` exceptions** trigger a restart. `KeyboardInterrupt` and `SystemExit` (subclasses of `BaseException`) are not caught and will propagate upward.

---

## Comparison with Raw EventHandler

| Feature | Raw EventHandler | MailboxHandler |
| - | - | - |
| Concurrency model | Events may execute concurrently | Enforced serial consumption |
| Backpressure | Relies on bus Semaphore | Additional queue capacity limit |
| Task lifecycle | Managed by the bus | Independent `asyncio.Task`, lazy start |
| Custom loop | Not supported | Batch, timer, and other flexible patterns |
| Error recovery | Relies on `TaskErrorEvent` | Built-in restart mechanism |
| Shutdown behavior | Relies on `ShutdownEvent` subscription | Auto-cancels background task |
