from typing import Dict, List

from .event import EventRegistry
from .handler import EventHandler, EventHandlerRegistry, Regex


class Matcher:
    """事件匹配器，基于事件注册表与处理器注册表预计算分派表。

    构造时遍历所有已知事件类型，预先完成订阅模式匹配，
    生成 ``{事件名: [(处理器ID, 处理器), ...]}`` 的分派表。
    运行时通过注册表版本号自动感知变更并重建缓存；
    对动态新增的事件类型自动触发匹配并缓存。"""

    def __init__(self, event_registry: EventRegistry, handler_registry: EventHandlerRegistry) -> None:
        self._events: EventRegistry = event_registry
        self._handlers: EventHandlerRegistry = handler_registry
        self._table: Dict[str, List[tuple[str, EventHandler]]] = {}
        self._event_version: int = -1
        self._handler_version: int = -1
        self._ensure_fresh()

    # ------------------------------------------------------------------
    # 版本感知
    # ------------------------------------------------------------------

    def _is_stale(self) -> bool:
        """检查任一注册表版本是否已过期。"""
        return self._event_version != self._events.version or self._handler_version != self._handlers.version

    def _ensure_fresh(self) -> None:
        """若注册表版本过期则自动重建分派表。"""
        if self._is_stale():
            self._rebuild()

    # ------------------------------------------------------------------
    # 分派表构建
    # ------------------------------------------------------------------

    def _rebuild(self) -> None:
        """重建预计算分派表。

        遍历事件注册表中所有已声明的事件类型，
        对每个事件类型匹配处理器注册表中的订阅模式。
        """
        self._table.clear()
        handler_pairs: List[tuple[str, EventHandler]] = list(self._handlers)
        for event_name in self._events.list_names():
            matched: List[tuple[str, EventHandler]] = []
            for hid, handler in handler_pairs:
                for subscription in handler.subscriptions:
                    if self._match_pattern(event_name, subscription):
                        matched.append((hid, handler))
                        break
            self._table[event_name] = matched
        self._event_version = self._events.version
        self._handler_version = self._handlers.version

    # ------------------------------------------------------------------
    # 匹配入口
    # ------------------------------------------------------------------

    def match(self, event_type: str) -> List[tuple[str, EventHandler]]:
        """获取匹配事件类型的所有处理器实例及其注册ID。

        自动感知注册表版本变更并重建缓存；
        优先从预计算分派表查找；未命中时动态匹配并缓存结果。
        """
        self._ensure_fresh()

        if event_type in self._table:
            return self._table[event_type]

        # 动态匹配：遍历所有处理器订阅
        matched: List[tuple[str, EventHandler]] = []
        for hid, handler in self._handlers:
            for subscription in handler.subscriptions:
                if self._match_pattern(event_type, subscription):
                    matched.append((hid, handler))
                    break
        self._table[event_type] = matched
        return matched

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------

    def _match_pattern(self, event_type: str, subscription: Regex | str) -> bool:
        """匹配订阅模式与事件类型。"""
        if isinstance(subscription, Regex):
            return subscription.fullmatch(event_type) is not None
        return event_type == subscription

    # ------------------------------------------------------------------
    # 公共属性
    # ------------------------------------------------------------------

    @property
    def dispatch_table(self) -> Dict[str, List[tuple[str, EventHandler]]]:
        """返回当前预计算分派表的只读副本，自动感知版本变更。"""
        self._ensure_fresh()
        return self._table.copy()
