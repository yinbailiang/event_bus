# EventBus 异步事件总线文档

## 概述

EventBus 是一个基于 asyncio 的轻量级事件总线，实现发布/订阅模式，用于在异步应用中解耦组件间的通信。它提供了强类型事件声明、正则表达式订阅、并发控制、超时保护及优雅停机等能力。

---

## 核心概念

| 组件 | 职责 |
| - | - |
| **Event** | 运行时事件实例，包含事件名、负载数据、处理链追踪信息（ID、来源、时间戳）。 |
| **EventDeclaration** | 声明一个事件类型的元数据：事件名称和可选的 Pydantic 负载模型。 |
| **EventRegistry** | 集中管理所有已注册的事件声明，发布时进行校验。 |
| **EventHandler** | 事件处理器基类。通过继承并实现 `handle` 方法定义业务逻辑，可声明订阅的事件类型（支持正则）。 |
| **EventHandlerRegistry** | 管理所有处理器实例，根据事件名匹配对应的处理器列表。 |
| **EventBus** | 事件分发中枢，负责任务队列、并发控制、错误上报及生命周期管理。 |

---

## 工作流程

### 1. 事件声明与注册

应用启动时，需将事件声明类注册到 `EventRegistry`，使总线识别合法事件类型及其负载结构。

```python
class UserLoginPayload(BaseModel):
    user_id: str
    timestamp: datetime

class UserLoginEvent(EventDeclaration):
    name = "user.login"
    payload_type = UserLoginPayload

registry = EventRegistry()
registry.register(UserLoginEvent)
```

### 2. 处理器订阅与注册

实现 `EventHandler` 子类，通过 `subscriptions` 指定监听的模式（支持正则），并通过 `handle` 方法处理事件。处理器实例注册到 `EventHandlerRegistry`。

```python
class LoginHandler(EventHandler):
    def __init__(self):
        super().__init__(subscriptions=[r"user\..*"])  # 匹配所有 user.* 事件

    async def handle(self, payload, bus_proxy, raw_event):
        if isinstance(payload, UserLoginPayload):
            print(f"User {payload.user_id} logged in")
            # 可调用 bus_proxy.publish 发布新事件

handler_registry = EventHandlerRegistry()
handler_registry.register(LoginHandler())
```

### 3. 启动总线与发布事件

创建 `EventBus` 实例并启动后，通过 `Proxy` 发布事件。Proxy 提供受限的总线访问接口，并自动记录事件来源。

```python
bus = EventBus(registry, handler_registry)
await bus.start()

# 发布事件
proxy = bus.proxy(source="AuthService")
await proxy.publish("user.login", {"user_id": "123", "timestamp": datetime.now()})
```

### 4. 事件分发与处理

- 发布的事件进入异步队列，由内部调度循环取出。
- 根据事件名匹配所有订阅处理器，为每个处理器创建独立任务。
- 通过信号量（Semaphore）限制并发处理器数量，防止过载。
- 每个处理器执行受超时控制，超时或异常均会触发内置错误事件。

### 5. 优雅停止

调用 `bus.stop()` 时：

- 拒绝新事件发布（抛出 `BusShuttingDown`）。
- 发布内置的 `__shutdown__` 事件通知处理器执行清理。
- 等待队列中已有事件处理完毕（可配置超时）。
- 取消调度循环，等待所有活跃处理器任务结束。

---

## 使用示例

### 基础发布/订阅

```python
class MyPayload(BaseModel):
    message: str

class MyEvent(EventDeclaration):
    name = "my.event"
    payload_type = MyPayload

class MyHandler(EventHandler):
    def __init__(self):
        super().__init__(subscriptions=["my.event"])

    async def handle(self, payload, bus_proxy, raw_event):
        print(f"Received: {payload.message}")

# 组装并运行
reg = EventRegistry()
reg.register(MyEvent)
h_reg = EventHandlerRegistry()
h_reg.register(MyHandler())

async with EventBus(reg, h_reg) as bus:   # 上下文管理器自动启停
    await bus.proxy("cli").publish("my.event", {"message": "Hello"})
```

### 正则订阅与链式发布

```python
class AuditHandler(EventHandler):
    def __init__(self):
        super().__init__(subscriptions=[r"order\..*"])   # 匹配所有 order. 事件

    async def handle(self, payload, bus_proxy, raw_event):
        await bus_proxy.publish("audit.log", {"event": raw_event.name})
```

---

## 内置事件

| 事件名 | 触发时机 | 负载类型 | 用途 |
| - | - | - | - |
| `event_bus.__shutdown__` | 总线开始停止时 | 无 | 通知处理器执行清理工作 |
| `event_bus.__task_error__` | 处理器执行失败时 | `TaskErrorPayload` | 错误监控与告警 |

---

## 关键特性

- **强类型负载校验**：发布时自动校验数据类型与结构，防止无效数据流入。
- **正则表达式订阅**：支持灵活的事件名匹配规则。
- **背压控制**：通过队列大小与并发信号量限制系统负载。
- **超时保护**：每个处理器可独立设置超时，避免单任务阻塞总线。
- **错误隔离**：单个处理器异常不会影响其他处理器的执行，异常信息通过内置错误事件统一上报。
- **优雅停机**：保证停止过程中已入队事件被完整处理，避免数据丢失。
- **可观测性**：提供活跃任务数、队列长度等监控指标。

---

## 注意事项

- 所有 `EventHandler.handle` 实现**不应包含阻塞操作**，必须使用异步 I/O。
- 事件负载模型应继承 `pydantic.BaseModel` 以确保数据验证。
- 处理器中可通过 `bus_proxy.publish` 发布新事件，形成处理链，总线会自动追踪来源。
- 停止过程中发布新事件将抛出 `BusShuttingDown` 异常，调用方需妥善处理。
