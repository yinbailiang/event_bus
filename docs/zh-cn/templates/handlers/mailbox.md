# Mailbox Handler 邮箱模式处理器

## 概述

`MailboxHandler` 是一个**邮箱模式**的 `EventHandler` 抽象基类。它将事件入队到内部邮箱队列，由子类实现 `process()` 方法从队列中逐一取出事件并处理。适合需要**串行消费**、**背压控制**、**自定义任务循环**的场景。

与直接继承 `EventHandler` 实现 `handle()` 不同，`MailboxHandler` 将"接收事件"和"处理事件"解耦到两个不同的协程中：

- `handle()` — 运行在总线的调度上下文中，仅负责将事件放入邮箱队列。
- `process()` — 运行在独立的 `asyncio.Task` 中，按自己的节奏从队列取事件处理。

---

## 使用场景

- 需要**严格串行**处理事件的场景（避免同一处理器的并发执行）。
- 需要对突发流量提供**背压控制**（通过限制队列容量）。
- 需要**自定义事件循环**的场景（如定时清理、定期批处理、状态机驱动的消费逻辑）。
- 模拟 Actor 模型的**邮箱**语义。
- 多个事件源的数据需要在单一消费者中按序聚合处理。

---

## 类签名

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

| 成员 | 类型 | 说明 |
| - | - | - |
| `subscriptions` | `list[str \| Regex]` | 订阅的事件模式列表。`ShutdownEvent` 会自动追加，无需手动添加。 |
| `config` | `MailboxConfig \| None` | 邮箱配置，`None` 时使用默认值。 |
| `process()` | `@abstractmethod async` | 子类必须实现的自定义任务循环。通过 `await self.get()` 获取下一事件。 |
| `get()` | `async → (Event, Proxy)` | 从邮箱取出下一个 `(事件, 总线代理)`。队列空时阻塞等待。 |
| `bus` | `EventBus \| None` | 当前绑定的 `EventBus` 实例，首次事件到达后可用。 |
| `is_running` | `bool` | `process()` 后台任务是否正在运行。 |

---

## MailboxConfig 配置

```python
class MailboxConfig(BaseModel):
    queue_put_timeout: float | None = None   # 入队超时（秒），None 表示无限等待
    restart_delay: float = 0.5               # 异常后重启等待间隔（秒）
    restart_jitter: float = 0.2              # 重启等待随机偏移上限（秒），避免惊群
    max_queue_size: int = 0                  # 邮箱队列最大容量，0 表示无限制
```

| 参数 | 默认值 | 说明 |
| - | - | - |
| `queue_put_timeout` | `None` | 入队超时。`None` 无限等待；设值后超时将抛出 `RuntimeError`。 |
| `restart_delay` | `0.5` | `process()` 异常退出后，重启前等待的基础秒数。 |
| `restart_jitter` | `0.2` | 在 base delay 上叠加的随机偏移 `[0, jitter]`，用于多实例场景防惊群。 |
| `max_queue_size` | `0` | 队列容量上限。`0` 表示无界队列。 |

> 实际重启等待时间 = `restart_delay + random(0, restart_jitter)`

---

## 工作流程

```text
事件到达
  │
  ▼
handle() ─── 检查是否 ShutdownEvent ─── 是 ─── 取消 process() 任务 ─── 返回
  │
  │ 否
  ▼
首次调用？── 是 ─── 创建 process() 后台 Task
  │
  ▼
put(event, proxy) → 入队
  │
  ▼
process() 协程（独立 Task）
  │
  ▼
await get() → 取出 (event, proxy)
  │
  ▼
处理业务逻辑（可通过 proxy.publish() 发布新事件）
  │
  ▼
循环回到 get()
```

1. **惰性启动**：`process()` 作为后台 `asyncio.Task` 仅在**首个非 ShutdownEvent 事件到达**时创建。
2. **事件入队**：`handle()` 将 `(Event, EventBus.Proxy)` 元组放入内部 `asyncio.Queue`。
3. **串行消费**：`process()` 循环调用 `await self.get()` 逐一取出事件处理。
4. **异常重启**：若 `process()` 因非 `CancelledError` 异常退出，`_process_loop` 会在等待 `restart_delay + jitter` 后重新调用 `process()`。
5. **优雅关闭**：收到 `ShutdownEvent` 时取消 `process()` 任务，并等待其完成。

---

## 使用示例

### 基础用法：串行事件收集

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
            # 可以通过 proxy.publish() 发布新事件
```

### 自定义任务循环：定时批处理

```python
import asyncio

class BatchHandler(MailboxHandler):
    def __init__(self):
        super().__init__(subscriptions=['data.input'])

    async def process(self) -> None:
        while True:
            batch: list[Event] = []
            # 收集一批事件，或等待超时
            try:
                event, _ = await asyncio.wait_for(self.get(), timeout=1.0)
                batch.append(event)
                # 尝试继续收集
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
        print(f'处理了 {len(batch)} 个事件')
```

### 异常重启：崩溃后自动恢复

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
                # 单个事件失败继续处理下一个
                # 注意：process() 整体崩溃才会触发重启
                pass
```

### 使用 bus 属性访问总线

```python
class BusAwareHandler(MailboxHandler):
    def __init__(self):
        super().__init__(subscriptions=['status.check'])

    async def process(self) -> None:
        while True:
            event, proxy = await self.get()
            # 通过 self.bus 访问原始总线
            if self.bus and self.bus.is_running:
                await proxy.publish('status.reply', {'ok': True})
```

---

## 注意事项

- **process() 必须是无限循环**：`_process_loop` 在 `process()` 正常返回后会重新调用它。如果 `process()` 只执行一次就返回，它将立即被再次调用，形成忙等循环。请始终使用 `while True` 包裹业务逻辑。
- **事件丢失**：若 `process()` 在 `get()` 返回后、处理完成前崩溃，该事件会丢失。`_process_loop` 重启后会从下一个 `get()` 开始，不会重试已取出的事件。
- **ShutdownEvent 自动订阅**：构造时自动将 `ShutdownEvent` 加入订阅列表。收到此事件时取消 `process()` 任务，且**不会**将其放入邮箱队列。
- **只捕获一个 Bus**：`self.bus` 在首次 `handle()` 调用时设置，后续不会改变。如果同一 handler 被多个总线索引用，`bus` 仅指向第一个。
- **`CancelledError` 以外的异常**才会触发重启。`KeyboardInterrupt` 和 `SystemExit` 属于 `BaseException`，当前不会被捕获，会向上传播。

---

## 与直接继承 EventHandler 的对比

| 特性 | 直接继承 EventHandler | MailboxHandler |
| - | - | - |
| 并发模型 | 每个事件可能并发执行 | 强制串行消费 |
| 背压控制 | 依赖总线 Semaphore | 额外提供队列容量限制 |
| 任务生命周期 | 由总线管理 | 独立的 `asyncio.Task`，惰性启动 |
| 自定义循环 | 不支持 | 支持批处理、定时器等灵活模式 |
| 异常恢复 | 依赖 `TaskErrorEvent` | 内建重启机制 |
| 关闭行为 | 依赖 `ShutdownEvent` 订阅 | 自动取消后台任务 |
