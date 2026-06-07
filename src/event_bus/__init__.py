"""event_bus - 基于 asyncio 的轻量级事件总线"""

from .core import (
    __version__,
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

__all__ = [
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
