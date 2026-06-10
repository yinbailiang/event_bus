import logging
import re
import uuid
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Dict, List, Optional

from pydantic import BaseModel

from .event import Event

if TYPE_CHECKING:
    from . import EventBus

logger = logging.getLogger(__name__)


class Regex:
    """编译后的正则表达式包装器"""

    def __init__(self, pattern: str) -> None:
        self._pattern: str = pattern
        self._compile: re.Pattern[str] = re.compile(pattern)

    @property
    def pattern(self) -> str:
        """返回原始正则表达式字符串。"""
        return self._pattern

    def fullmatch(self, string: str) -> Optional[re.Match[str]]:
        """委托给已编译正则的 ``fullmatch``。"""
        return self._compile.fullmatch(string)

    def __str__(self) -> str:
        return self._pattern

    def __repr__(self) -> str:
        return f'Regex({self._pattern!r})'

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Regex):
            return self._pattern == other._pattern
        if isinstance(other, str):
            return self._pattern == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._pattern)


class EventHandler(ABC):
    """事件处理器基类，所有具体事件处理器应继承此类"""

    def __init__(
        self, subscriptions: Optional[List[Regex | str]] = None, handle_timeout: Optional[float] = 32.0
    ) -> None:
        self.subscriptions: List[Regex | str] = subscriptions.copy() if subscriptions is not None else []
        self.handle_timeout: Optional[float] = handle_timeout

    async def __call__(self, bus_proxy: 'EventBus.Proxy', event: Event) -> None:
        """事件处理器入口，自动解包事件数据"""
        await self.handle(event.data, bus_proxy, event)

    @abstractmethod
    async def handle(self, payload: Optional[BaseModel], bus_proxy: 'EventBus.Proxy', raw_event: Event) -> None: ...


class EventHandlerRegistry:
    """事件处理器注册表，负责管理事件类型与处理器的映射关系。"""

    def __init__(self) -> None:
        self._handlers: Dict[str, EventHandler] = {}

    def __len__(self) -> int:
        """返回已注册处理器数量。"""
        return len(self._handlers)

    def __contains__(self, handler_id: str) -> bool:
        """检查处理器 ID 是否已注册。"""
        return handler_id in self._handlers

    def __iter__(self):
        """迭代所有 (handler_id, handler) 对。"""
        return iter(self._handlers.items())

    def clear(self) -> None:
        """清除所有已注册处理器。"""
        self._handlers.clear()

    def register(self, handler: EventHandler) -> str:
        """注册一个事件处理器实例"""
        id = uuid.uuid4().hex
        self._handlers[id] = handler
        return id

    def get(self, handler_id: str) -> Optional[EventHandler]:
        """根据ID获取事件处理器实例"""
        return self._handlers.get(handler_id)

    def unregister(self, handler_id: str) -> bool:
        """注销一个事件处理器实例"""
        if handler_id in self._handlers:
            del self._handlers[handler_id]
            return True
        return False

    def get_handlers(self, event_type: str) -> List[tuple[str, EventHandler]]:
        """获取匹配事件类型的所有处理器实例及其注册ID"""
        matched: List[tuple[str, EventHandler]] = []
        for handler_id, handler in self._handlers.items():
            for subscription in handler.subscriptions:
                if self._match_pattern(event_type, subscription):
                    matched.append((handler_id, handler))
                    break
        return matched

    def _match_pattern(self, event_type: str, subscription: Regex | str) -> bool:
        """匹配订阅模式与事件类型。"""
        if isinstance(subscription, Regex):
            return subscription.fullmatch(event_type) is not None
        return event_type == subscription

    @property
    def handlers_count(self) -> int:
        """（兼容属性）返回已注册处理器数量，等价于 ``len(registry)``。"""
        return len(self._handlers)

    @property
    def all_handlers(self) -> Dict[str, EventHandler]:
        """获取所有注册的事件处理器实例"""
        return self._handlers.copy()
