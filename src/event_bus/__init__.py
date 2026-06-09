"""event_bus - 基于 asyncio 的轻量级事件总线"""

__version__ = '1.3.6'

from .bus import (
    BusShuttingDown,
    EventBus,
    ShutdownConfig,
    ShutdownEvent,
    TaskErrorEvent,
    TaskErrorPayload,
)
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
    BeforePublishNext,
    Middleware,
    MiddlewareChain,
    OnPublishNext,
)

__all__: list[str] = [
    '__version__',
    'Event',
    'EventDeclaration',
    'EventRegistry',
    'EventHandler',
    'EventHandlerRegistry',
    'Middleware',
    'MiddlewareChain',
    'BeforePublishNext',
    'OnPublishNext',
    'EventBus',
    'BusShuttingDown',
    'ShutdownEvent',
    'TaskErrorPayload',
    'TaskErrorEvent',
    'ShutdownConfig',
]
