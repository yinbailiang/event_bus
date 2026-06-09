from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Callable, Dict, List

from pydantic import BaseModel

from .event import Event, EventRegistry

if TYPE_CHECKING:
    from .bus import EventBus

logger = logging.getLogger(__name__)


BeforePublishNext = Callable[
    [EventRegistry, str, str, Dict[str, Any] | BaseModel | None, Event | None],
    Any,
]
"""before_publish 链中 next 回调的签名"""

OnPublishNext = Callable[
    [Event],
    Any,
]
"""on_publish 链中 next 回调的签名"""


class Middleware(ABC):
    """事件总线中间件基类。"""

    async def on_setup(self, bus: EventBus) -> None:
        """总线启动后回调。"""
        pass

    async def on_teardown(self, bus: EventBus) -> None:
        """总线停止前回调。"""
        pass

    @abstractmethod
    async def before_publish(
        self,
        event_registry: EventRegistry,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
        old_event: Event | None,
        next: BeforePublishNext,
    ) -> None:
        """发布**前**钩子 —— 在事件声明校验完成后、入队前执行。"""
        ...

    @abstractmethod
    async def on_publish(
        self,
        event: Event,
        next: OnPublishNext,
    ) -> None:
        """发布**后**钩子 —— 在事件成功入队后执行。

        通过 ``event.name``、``event.data``、``event.sources`` 等
        可获取完整的运行时事件信息。
        """
        ...

    async def on_publish_error(
        self,
        error: Exception,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
    ) -> None:
        """发布流程中发生异常时回调。"""
        pass


class MiddlewareChain:
    """中间件链管理器。"""

    def __init__(self) -> None:
        self._middlewares: list[Middleware] = []

    def add(self, middleware: Middleware) -> 'MiddlewareChain':
        """在链**末尾**追加一个中间件。"""
        if middleware in self._middlewares:
            raise ValueError(f'Middleware {middleware.__class__.__name__} is already in the chain')
        self._middlewares.append(middleware)
        return self

    def insert(self, index: int, middleware: Middleware) -> 'MiddlewareChain':
        """在指定位置插入中间件。"""
        if middleware in self._middlewares:
            raise ValueError(f'Middleware {middleware.__class__.__name__} is already in the chain')
        self._middlewares.insert(index, middleware)
        return self

    def remove(self, middleware: Middleware) -> None:
        """移除指定中间件实例。"""
        self._middlewares.remove(middleware)

    def clear(self) -> None:
        """清空所有中间件。"""
        self._middlewares.clear()

    @property
    def middlewares(self) -> list[Middleware]:
        """返回当前注册的中间件列表"""
        return list(self._middlewares)

    async def setup(self, bus: EventBus) -> List[Middleware]:
        """按注册顺序通知所有中间件执行初始化。"""
        error_middlewares: List[Middleware] = []
        for mw in self._middlewares:
            try:
                await mw.on_setup(bus)
            except Exception:
                error_middlewares.append(mw)
                logger.exception('Middleware %s on_setup failed', mw.__class__.__name__)
        for error_mw in error_middlewares:
            self._middlewares.remove(error_mw)
        return error_middlewares

    async def teardown(self, bus: EventBus) -> None:
        """按注册**逆序**通知所有中间件执行清理。"""
        for mw in reversed(self._middlewares):
            try:
                await mw.on_teardown(bus)
            except Exception:
                logger.exception('Middleware %s on_teardown failed', mw.__class__.__name__)

    def build_before_publish(
        self,
        final_handler: BeforePublishNext,
    ) -> BeforePublishNext:
        """构建 ``before_publish`` 责任链。"""
        handler = final_handler
        for mw in reversed(self._middlewares):
            handler = self._wrap_before(mw, handler)
        return handler

    def build_on_publish(
        self,
        final_handler: OnPublishNext,
    ) -> OnPublishNext:
        """构建 ``on_publish`` 责任链。"""
        handler = final_handler
        for mw in reversed(self._middlewares):
            handler = self._wrap_on(mw, handler)
        return handler

    async def on_publish_error(
        self,
        error: Exception,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
    ) -> None:
        """按注册顺序通知所有中间件发布异常。"""
        for mw in self._middlewares:
            try:
                await mw.on_publish_error(error, name, source, data)
            except Exception:
                logger.exception(
                    'Middleware %s on_publish_error failed',
                    mw.__class__.__name__,
                )

    @staticmethod
    def _wrap_before(
        middleware: Middleware,
        next_handler: BeforePublishNext,
    ) -> BeforePublishNext:
        """将一个中间件的 before_publish 包裹在 next_handler 之外。"""

        async def wrapped(
            event_registry: EventRegistry,
            name: str,
            source: str,
            data: dict[str, Any] | BaseModel | None,
            old_event: Event | None,
        ) -> None:
            await middleware.before_publish(
                event_registry,
                name,
                source,
                data,
                old_event,
                next_handler,
            )

        return wrapped

    @staticmethod
    def _wrap_on(
        middleware: Middleware,
        next_handler: OnPublishNext,
    ) -> OnPublishNext:
        """将一个中间件的 on_publish 包裹在 next_handler 之外。"""

        async def wrapped(
            event: Event,
        ) -> None:
            await middleware.on_publish(
                event,
                next_handler,
            )

        return wrapped
