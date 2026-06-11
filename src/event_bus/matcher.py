from typing import Dict, List, Set

from .event import EventRegistry
from .handler import EventHandler, EventHandlerRegistry, Regex


class Matcher:
    """事件匹配器，基于事件注册表与处理器注册表预计算分派表。

    构造时遍历所有已知事件类型，预先完成订阅模式匹配，
    生成 ``{事件名: [处理器ID, ...]}`` 的轻量分派表。
    精确 ``str`` 订阅构建反向索引（O(1) 查表），
    ``Regex`` 订阅单独维护扫描列表；
    运行时通过注册表版本号自动感知变更并重建缓存。"""

    def __init__(self, event_registry: EventRegistry, handler_registry: EventHandlerRegistry) -> None:
        self._events: EventRegistry = event_registry
        self._handlers: EventHandlerRegistry = handler_registry
        self._table: Dict[str, List[str]] = {}
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

        单次遍历处理器注册表，将订阅分为两类：
        - 精确 ``str`` → 构建反向索引 ``{event_type: [handler_id, ...]}``
        - ``Regex`` → 归入扫描列表
        然后对每个已知事件类型，合并反向索引命中 + 正则扫描结果。
        """
        self._table.clear()

        # 单次遍历：分离精确匹配（可索引）与正则匹配（需扫描）
        exact_index: Dict[str, List[str]] = {}
        regex_list: List[tuple[str, Regex]] = []

        for hid, handler in self._handlers:
            for sub in handler.subscriptions:
                if isinstance(sub, Regex):
                    regex_list.append((hid, sub))
                else:
                    exact_index.setdefault(sub, []).append(hid)

        # 对每个已知事件名，合并两类匹配结果（handler 去重）
        for event_name in self._events.list_names():
            matched: List[str] = []
            seen: Set[str] = set()
            for hid in exact_index.get(event_name, ()):
                if hid not in seen:
                    matched.append(hid)
                    seen.add(hid)
            for hid, regex in regex_list:
                if hid not in seen and regex.fullmatch(event_name) is not None:
                    matched.append(hid)
                    seen.add(hid)
            self._table[event_name] = matched

        self._event_version = self._events.version
        self._handler_version = self._handlers.version

    # ------------------------------------------------------------------
    # 匹配入口
    # ------------------------------------------------------------------

    def match(self, event_type: str) -> List[tuple[str, EventHandler]]:
        """获取匹配事件类型的所有处理器实例及其注册ID。

        自动感知注册表版本变更并重建缓存；
        所有事件类型均预先注册，直接从分派表 O(1) 查表。
        """
        self._ensure_fresh()
        return self._resolve_ids(self._table.get(event_type, []))

    def _resolve_ids(self, ids: List[str]) -> List[tuple[str, EventHandler]]:
        """将处理器 ID 列表解析为 ``(hid, handler)`` 元组列表。"""
        result: List[tuple[str, EventHandler]] = []
        for hid in ids:
            handler = self._handlers.get(hid)
            if handler is not None:
                result.append((hid, handler))
        return result

    # ------------------------------------------------------------------
    # 公共属性
    # ------------------------------------------------------------------

    @property
    def dispatch_table(self) -> Dict[str, List[tuple[str, EventHandler]]]:
        """返回当前预计算分派表的只读副本，自动感知版本变更。"""
        self._ensure_fresh()
        return {name: self._resolve_ids(ids) for name, ids in self._table.items()}
