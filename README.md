# InfinityBus — 异步事件总线

[![Test](https://github.com/yinbailiang/event_bus/actions/workflows/test.yml/badge.svg)](https://github.com/yinbailiang/event_bus/actions/workflows/test.yml)
[![Coverage](https://img.shields.io/badge/coverage-90%25+-brightgreen)](ENGINEERING.md)
[![Pyright](https://img.shields.io/badge/pyright-strict-blue)](ENGINEERING.md)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE.md)
[![PyPI Version](https://img.shields.io/pypi/v/infinity_bus)](https://pypi.org/project/infinity_bus/)
[![Supported Python](https://img.shields.io/badge/python-3.12%2B-blue)](https://pypi.org/project/infinity_bus/)

**强类型、可扩展的异步事件总线——中间件管道 + 高级模板。**

## ✨ 特性

| 类别 | 能力 |
| - | - |
| 类型安全 | Pydantic 负载校验 · pyright **strict** · **零** `# type: ignore` |
| 灵活订阅 | **正则表达式**匹配事件名 · 通配符处理器 |
| 中间件管道 | 洋葱模型 · `before_publish` / `on_publish` 双钩子 · 5 个内置中间件 |
| 高级模板 | `expect` 一次性监听 · `request` RPC 调用 · `pipe` 双向管道 · `register` 批量注册 |
| 生产可靠 | 优雅停机 · 背压控制 · 超时保护 · 错误隔离 · 可观测性 |
| 工程纪律 | 90%+ 测试覆盖 · 85%+ docstring 覆盖 · pre-commit 自动门禁 |

> 和同类项目不同：InfinityBus 是**可扩展的**（中间件洋葱管道），而非把所有功能硬编码在核心类里。
> 详见 [中间件系统](docs/middleware.md)。

## 📦 安装

```bash
pip install infinity_bus
```

或从源码安装：

```bash
git clone https://github.com/yinbailiang/event_bus.git
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
import asyncio
from pydantic import BaseModel
from event_bus import (
    EventBus, EventDeclaration, EventHandler,
    EventRegistry, EventHandlerRegistry,
)
from event_bus.templates.request import (
    request, RequestProtocol, ResponseProtocol,
)

# 1. 定义请求/响应负载
class GetUserRequest(RequestProtocol):
    user_id: int

class GetUserResponse(ResponseProtocol):
    user_name: str
    email: str

# 2. 声明事件
class GetUserRequestEvent(EventDeclaration):
    name = "user.get.request"
    payload_type = GetUserRequest

class GetUserResponseEvent(EventDeclaration):
    name = "user.get.response"
    payload_type = GetUserResponse

# 3. 实现服务端处理器
class GetUserHandler(EventHandler):
    def __init__(self):
        super().__init__(subscriptions=["user.get.request"])

    async def handle(self, payload, bus_proxy, raw_event):
        if not isinstance(payload, GetUserRequest):
            return
        resp = GetUserResponse(
            session_id=payload.session_id,
            request_id=payload.request_id,
            success=True,
            user_name="Alice",
            email="alice@example.com",
        )
        await bus_proxy.publish("user.get.response", resp)

# 4. 组装并运行
async def main():
    reg = EventRegistry()
    reg.register(GetUserRequestEvent)
    reg.register(GetUserResponseEvent)
    h_reg = EventHandlerRegistry()
    h_reg.register(GetUserHandler())

    async with EventBus(reg, h_reg) as bus:
        proxy = bus.proxy("cli")
        resp = await request(
            bus_proxy=proxy,
            req_event="user.get.request",
            req_data={"user_id": 123},
            resp_event="user.get.response",
            timeout=10.0,
        )
        resp.raise_if_failed()
        print(f"User: {resp.user_name} ({resp.email})")

asyncio.run(main())
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
| **Middleware** | 中间件基类，洋葱管道：`before_publish` / `on_publish` 双钩子 |
| **MiddlewareChain** | 责任链管理器，按序包裹发布流程 |
| **templates** | 高级模板：`expect` 监听、`request` RPC、`pipe` 管道、`register` 批量注册 |
| **middlewares** | 内置中间件：日志(JSONL+SQLite)、限流、转换、屏蔽、递归防护 |

## 📚 文档

| 文档 | 内容 |
| - | - |
| [核心总览](docs/event_bus.md) | Event / EventDeclaration / EventHandler / EventBus / Middleware 核心概念 |
| [中间件系统](docs/middleware.md) | `Middleware` 基类、`MiddlewareChain` 洋葱管道 |
| [高级模板](docs/templates/templates.md) | `expect`、`request`、`pipe`、`register` 四大模板总览 |
| [内置中间件](docs/templates/middlewares/middlewares.md) | 日志、限流、转换、屏蔽、递归防护 |
| [工程质量](ENGINEERING.md) | 类型安全、测试覆盖、pre-commit 门禁、模块化规范 |

## 🧪 测试

```bash
# 运行全部测试
pytest --cov=src -v

# 仅运行模板测试
pytest tests/templates/ -v
```

## 📄 许可证

[MIT](LICENSE.md)

## 此项目属于 InfinitySystem

![icon](docs/res/infinity_icon/256x256.png)
