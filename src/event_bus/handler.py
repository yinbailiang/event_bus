import logging
import re
import uuid
from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Pattern

from pydantic import BaseModel

from .event import Event

if TYPE_CHECKING:
    from . import EventBus

logger = logging.getLogger(__name__)


class EventHandler(ABC):
    """事件处理器基类，所有具体事件处理器应继承此类"""

    def __init__(self, subscriptions: Optional[List[str]] = None, handle_timeout: Optional[float] = 1.0) -> None:
        self.subscriptions: List[str] = (
            subscriptions.copy() if subscriptions is not None else []
        )  # 订阅的事件类型列表，支持正则表达式
        self.handle_timeout: Optional[float] = handle_timeout

    async def __call__(self, bus_proxy: 'EventBus.Proxy', event: Event) -> None:
        """事件处理器入口，自动解包事件数据"""
        await self.handle(event.data, bus_proxy, event)

    @abstractmethod
    async def handle(self, payload: Optional[BaseModel], bus_proxy: 'EventBus.Proxy', raw_event: Event) -> None:
        pass


class EventHandlerRegistry:
    """事件处理器注册表，负责管理事件类型与处理器的映射关系"""

    def __init__(self, regex_cache_maxsize: int = 256) -> None:
        self._regex_cache: OrderedDict[str, Pattern[str]] = OrderedDict()
        self._regex_cache_maxsize: int = regex_cache_maxsize
        self._handlers: Dict[str, EventHandler] = {}

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
            for pattern in handler.subscriptions:
                if self._match_pattern(event_type, pattern):
                    matched.append((handler_id, handler))
                    break
        return matched

    def _match_pattern(self, event_type: str, pattern: str) -> bool:
        """使用正则表达式匹配事件类型（LRU 缓存编译结果，防止无限膨胀）"""
        if pattern not in self._regex_cache:
            if len(self._regex_cache) >= self._regex_cache_maxsize:
                self._regex_cache.popitem(last=False)  # 淘汰最旧条目
            self._regex_cache[pattern] = re.compile(pattern)
        else:
            self._regex_cache.move_to_end(pattern)  # 命中则标记为最近使用
        return re.fullmatch(self._regex_cache[pattern], event_type) is not None

    @property
    def handlers_count(self) -> int:
        return len(self._handlers)

    @property
    def all_handlers(self) -> Dict[str, EventHandler]:
        """获取所有注册的事件处理器实例"""
        return self._handlers.copy()

    @property
    def regex_cache_info(self) -> Dict[str, Any]:
        """获取正则表达式缓存的当前状态"""
        return {
            'size': len(self._regex_cache),
            'max_size': self._regex_cache_maxsize,
        }
