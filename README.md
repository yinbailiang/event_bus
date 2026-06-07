# EventBus — 异步事件总线

[![Test](https://github.com/yin_bailiang/event_bus/actions/workflows/test.yml/badge.svg)](https://github.com/yin_bailiang/event_bus/actions/workflows/test.yml)
[![Python](https://img.shields.io/badge/python-3.12%2B-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE.md)

基于 **asyncio** 的轻量级事件总线，实现发布/订阅模式，用于在异步应用中解耦组件间的通信。

## ✨ 特性

- **强类型负载校验** — 基于 Pydantic，发布时自动校验数据类型与结构
- **正则表达式订阅** — 支持灵活的事件名匹配规则
- **背压控制** — 队列大小与并发信号量双重限流，防止过载
- **超时保护** — 每个处理器可独立设置超时，避免单任务阻塞总线
- **错误隔离** — 单个处理器异常不影响其他处理器，错误通过内置事件统一上报
- **优雅停机** — 保证停止过程中已入队事件被完整处理，避免数据丢失
- **可观测性** — 提供活跃任务数、队列长度等监控指标

## 📦 安装

```bash
pip install event_bus
```

或从源码安装：

```bash
git clone https://github.com/yin_bailiang/event_bus.git
cd event_bus
pip install -e ".[test]"
```

## 🚀 快速开始

### 基础发布/订阅

```python
import asyncio
from pydantic import BaseModel
from event_bus import (
    EventBus, EventDeclaration, EventHandler,
    EventRegistry, EventHandlerRegistry,
)

# 1. 定义负载
class MyPayload(BaseModel):
    message: str

# 2. 声明事件
class MyEvent(EventDeclaration):
    name = "my.event"
    payload_type = MyPayload

# 3. 实现处理器
class MyHandler(EventHandler):
    def __init__(self):
        super().__init__(subscriptions=["my.event"])

    async def handle(self, payload, bus_proxy, raw_event):
        print(f"Received: {payload.message}")

# 4. 组装并运行
async def main():
    reg = EventRegistry()
    reg.register(MyEvent)
    h_reg = EventHandlerRegistry()
    h_reg.register(MyHandler())

    async with EventBus(reg, h_reg) as bus:
        await bus.proxy("cli").publish("my.event", {"message": "Hello, EventBus!"})
        await asyncio.sleep(1)  # 等待处理器输出

asyncio.run(main())
```

### 请求-响应模式

```python
from event_bus.templates.request import request, RequestProtocol, ResponseProtocol

# 定义请求/响应负载与事件（详见文档）
# ...

resp = await request(
    bus_proxy=proxy,
    req_event="user.get.request",
    req_data={"user_id": 123},
    resp_event="user.get.response",
    timeout=10.0,
)
resp.raise_if_failed()
```

## 🧱 架构

| 组件 | 职责 |
| - | - |
| **Event** | 运行时事件实例，含名称、负载、处理链追踪 |
| **EventDeclaration** | 事件类型元数据声明（名称 + 可选 Pydantic 负载模型） |
| **EventRegistry** | 集中管理已注册的事件声明，发布时校验 |
| **EventHandler** | 处理器基类，实现 `handle` 方法定义业务逻辑 |
| **EventHandlerRegistry** | 管理处理器实例，按事件名匹配处理器列表 |
| **EventBus** | 事件分发中枢：任务队列、并发控制、错误上报、生命周期 |

## 📚 文档

- [EventBus 核心](docs/event_bus.md)
- [Expect 一次性监听器](docs/templates/expect.md)
- [Request 请求-响应模板](docs/templates/request.md)
- [Pipe 双向管道](docs/templates/pipe.md)

## 🧪 测试

```bash
# 运行全部测试
pytest --cov=src -v

# 仅运行核心测试
pytest tests/event_bus_test.py -v

# 仅运行模板测试
pytest tests/templates/ -v
```

## 📄 许可证

[MIT](LICENSE.md)
