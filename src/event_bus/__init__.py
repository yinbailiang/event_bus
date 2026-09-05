"""event_bus - 基于 asyncio 的轻量级事件总线"""

__version__ = '3.0.0'

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
    Regex,
)
from .matcher import (
    Matcher,
)
from .middleware import (
    BeforePublishNext,
    Middleware,
    MiddlewareChain,
    OnPublishErrorNext,
    OnPublishNext,
)
from .queue import (
    EventQueue,
    InMemoryEventQueue,
    InMemoryEventQueueConfig,
)

__all__: list[str] = [
    '__version__',
    'Event',
    'EventDeclaration',
    'EventRegistry',
    'EventHandler',
    'EventHandlerRegistry',
    'Regex',
    'Matcher',
    'Middleware',
    'MiddlewareChain',
    'BeforePublishNext',
    'OnPublishNext',
    'OnPublishErrorNext',
    'EventQueue',
    'InMemoryEventQueue',
    'InMemoryEventQueueConfig',
    'EventBus',
    'BusShuttingDown',
    'ShutdownEvent',
    'TaskErrorPayload',
    'TaskErrorEvent',
    'ShutdownConfig',
]
