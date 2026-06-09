"""event_bus - 基于 asyncio 的轻量级事件总线"""

__version__ = "1.3.2"

from .event import (
    Event,
    EventDeclaration,
    EventRegistry,
)

from .handler import (
    EventHandler,
    EventHandlerRegistry,
)

from .middleware import (
    Middleware,
    MiddlewareChain,
)

from .bus import (
    EventBus,
    BusShuttingDown,
    ShutdownEvent,
    TaskErrorPayload,
    TaskErrorEvent,
    ShutdownConfig,
)

__all__: list[str] = [
    "__version__",
    "Event",
    "EventDeclaration",
    "EventRegistry",
    "EventHandler",
    "EventHandlerRegistry",
    "Middleware",
    "MiddlewareChain",
    "EventBus",
    "BusShuttingDown",
    "ShutdownEvent",
    "TaskErrorPayload",
    "TaskErrorEvent",
    "ShutdownConfig",
]
