"""event_bus - 基于 asyncio 的轻量级事件总线"""

__version__ = "1.2.0"

from .core import (
    Event,
    EventDeclaration,
    EventRegistry,
    EventHandler,
    EventHandlerRegistry,
    EventBus,
    BusShuttingDown,
    ShutdownEvent,
    TaskErrorPayload,
    TaskErrorEvent,
)

__all__: list[str] = [
    "__version__",
    "Event",
    "EventDeclaration",
    "EventRegistry",
    "EventHandler",
    "EventHandlerRegistry",
    "EventBus",
    "BusShuttingDown",
    "ShutdownEvent",
    "TaskErrorPayload",
    "TaskErrorEvent",
]
