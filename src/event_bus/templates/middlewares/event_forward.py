import logging
from inspect import isawaitable
from typing import Any, Awaitable, Callable, Dict, Literal, Optional, Set, Union

from pydantic import BaseModel

from event_bus import (
    BeforePublishNext,
    Event,
    EventBus,
    EventRegistry,
    Middleware,
    OnPublishNext,
)

logger = logging.getLogger(__name__)

EventFilter = Callable[[Event], Union[bool, Awaitable[bool]]]
"""事件过滤回调签名：接收 Event，返回是否转发。"""

TargetBusProvider = Callable[[], Union[EventBus, Awaitable[EventBus]]]
"""目标总线提供者签名：返回目标 EventBus 实例。"""


class EventForwardMiddleware(Middleware):
    """单向跨总线事件转发中间件。

    在 ``on_publish`` 阶段将事件转发到另一个 EventBus 实例。
    转发过程完全异步且失败隔离——目标总线异常不会影响源总线的正常运行。

    参数
    ----
    target:
        目标总线或返回目标总线的回调。支持：
        - 直接传入 ``EventBus`` 实例（总线已启动且持续运行）
        - 传入 ``Callable[[], EventBus]`` 工厂回调（每次转发时获取最新实例）
    source_name:
        在目标总线上发布时使用的来源标识，默认 ``"event_forward"``。
    event_filter:
        事件过滤回调，返回 ``True`` 表示需要转发。
        为 ``None`` 时转发所有非系统事件。
        支持同步或异步回调。
    forward_system_events:
        是否转发 ``event_bus.*`` 系统事件，默认 ``False``。
    """

    _SYSTEM_EVENT_PREFIX = 'event_bus.'

    def __init__(
        self,
        target: Union[EventBus, TargetBusProvider],
        source_name: str = 'event_forward',
        event_filter: Optional[EventFilter] = None,
        forward_system_events: bool = False,
    ) -> None:
        self._target: EventBus | TargetBusProvider = target
        self._source_name: str = source_name
        self._filter: EventFilter | None = event_filter
        self._forward_system: bool = forward_system_events

        # 可观测性计数器
        self._forwarded_count: int = 0
        self._failed_count: int = 0
        self._skipped_count: int = 0

    async def before_publish(
        self,
        event_registry: EventRegistry,
        name: str,
        source: str,
        data: Dict[str, Any] | BaseModel | None,
        old_event: Event | None,
        next: BeforePublishNext,
    ) -> None:
        """透传"""
        await next(event_registry, name, source, data, old_event)

    async def on_publish(
        self,
        event: Event,
        next: OnPublishNext,
    ) -> None:
        """在事件成功入队后，将其转发到目标总线。"""
        try:
            await self._do_forward(event)
        except Exception:
            logger.exception(
                'EventForward: 转发事件 %s (id=%s) 时发生未预期异常',
                event.name,
                event.id,
            )
        finally:
            await next(event)

    async def _do_forward(self, event: Event) -> None:
        """执行实际的转发逻辑。"""
        if not self._forward_system and event.name.startswith(self._SYSTEM_EVENT_PREFIX):
            self._skipped_count += 1
            return

        if self._filter is not None:
            try:
                result = self._filter(event)
                if isawaitable(result):
                    result = await result
                if not result:
                    self._skipped_count += 1
                    return
            except Exception:
                self._skipped_count += 1
                logger.warning(
                    'EventForward: 过滤回调异常，跳过事件 %s (id=%s)',
                    event.name,
                    event.id,
                    exc_info=True,
                )
                return

        try:
            target_bus = await self._resolve_target()
        except Exception:
            self._failed_count += 1
            logger.exception('EventForward: 获取目标总线失败，事件 %s (id=%s)', event.name, event.id)
            return

        if event.data is not None:
            forward_data: Optional[Dict[str, Any]] = event.data.model_dump()
        else:
            forward_data = None

        try:
            await target_bus.proxy(self._source_name, event).publish(event.name, forward_data)
            self._forwarded_count += 1
            logger.debug(
                'EventForward: 已转发 %s (id=%s) → target_bus',
                event.name,
                event.id,
            )
            return
        except Exception as e:
            self._failed_count += 1
            logger.warning(
                'EventForward: 转发 %s 失败 %s',
                event.name,
                e,
            )

    async def _resolve_target(self) -> EventBus:
        """解析目标总线实例"""
        if isinstance(self._target, EventBus):
            raw: Union[EventBus, Awaitable[EventBus]] = self._target
        else:
            raw = self._target()
        if isawaitable(raw):
            return await raw
        return raw

    @property
    def forwarded_count(self) -> int:
        """累计成功转发的事件数。"""
        return self._forwarded_count

    @property
    def failed_count(self) -> int:
        """累计转发失败的事件数（含目标不可达、publish 异常）。"""
        return self._failed_count

    @property
    def skipped_count(self) -> int:
        """累计被跳过的事件数（系统事件、过滤器拒绝、过滤器异常）。"""
        return self._skipped_count


def make_event_name_filter(
    *event_names: str,
    mode: Literal['white', 'black'] = 'white',
) -> EventFilter:
    """创建一个基于事件名的过滤回调。"""

    name_set: Set[str] = set(event_names)

    match mode:
        case 'white':

            async def _allow_filter(event: Event) -> bool:
                return event.name in name_set

            return _allow_filter
        case 'black':

            async def _block_filter(event: Event) -> bool:
                return event.name not in name_set

            return _block_filter
