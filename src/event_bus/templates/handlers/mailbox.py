"""邮箱模式处理器基类 — 事件入队，子类自定义任务循环"""

import asyncio
import logging
import random
from abc import ABC, abstractmethod
from typing import Optional

from pydantic import BaseModel, Field

from event_bus import Event, EventBus, EventHandler, Regex, ShutdownEvent

logger = logging.getLogger(__name__)


class MailboxConfig(BaseModel):
    """邮箱处理器配置"""

    queue_put_timeout: Optional[float] = Field(
        default=None,
        description='入队超时（秒），None 表示无限等待。',
    )
    restart_delay: float = Field(
        default=0.5,
        description='restart 策略下重启前的等待间隔（秒）。',
    )
    restart_jitter: float = Field(
        default=0.2,
        description='restart 策略下重启等待的随机偏移上限（秒），避免惊群。',
    )
    max_queue_size: int = Field(
        default=0,
        description='邮箱队列最大容量，0 表示无限制。',
    )

    def restart_sleep(self) -> float:
        """重启等待时间（含随机偏移）"""
        return self.restart_delay + random.uniform(0, self.restart_jitter)


class MailboxHandler(EventHandler, ABC):
    """邮箱模式处理器

    首次 ``handle()`` 被调用时自动启动 ``process()`` 作为后台 Task。
    ``get()`` 返回 ``(event, proxy)``——proxy 携带正确的事件链追踪。

    用法::

        class MyHandler(MailboxHandler):
            def __init__(self):
                super().__init__(subscriptions=['sim.timer.tick'])

            async def process(self) -> None:
                while True:
                    event, proxy = await self.get()
                    await proxy.publish(...)
    """

    def __init__(self, subscriptions: list[str | Regex], config: MailboxConfig | None = None) -> None:
        subs = subscriptions.copy()
        if ShutdownEvent.name not in subs:
            subs.append(ShutdownEvent.name)
        self._config: MailboxConfig = config or MailboxConfig()
        super().__init__(subscriptions=subs, handle_timeout=None)
        self._queue: asyncio.Queue[tuple[Event, EventBus.Proxy]] = asyncio.Queue(maxsize=self._config.max_queue_size)
        self._task: asyncio.Task[None] | None = None
        self._bus: EventBus | None = None

    # ------------------------------------------------------------------
    # EventHandler 入口 — 捕获 bus + 入队 + 惰性启动
    # ------------------------------------------------------------------

    async def __call__(self, bus: EventBus, event: Event) -> None:
        """捕获原始 EventBus，供 ``process()`` 通过 ``self.bus`` 访问"""
        if self._bus is None:
            self._bus = bus
        return await super().__call__(bus, event)

    async def handle(
        self,
        payload: BaseModel | None,
        bus_proxy: EventBus.Proxy,
        raw_event: Event,
    ) -> None:
        """事件到达 → 入队；首次调用时启动 process() 后台任务"""

        if raw_event.name == ShutdownEvent.name:
            if self._task is not None:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
                self._task = None
            return

        if self._task is None:
            self._task = asyncio.create_task(self._process_loop(), name=f'{self.__class__.__name__}._process_loop')

        await self.put(raw_event, bus_proxy)

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    @property
    def bus(self) -> EventBus | None:
        """当前绑定的 EventBus，首次 ``handle()`` 调用后可用"""
        return self._bus

    async def put(self, event: Event, proxy: EventBus.Proxy) -> None:
        """将事件入队，供 ``process()`` 处理"""
        try:
            await asyncio.wait_for(self._queue.put((event, proxy)), timeout=self._config.queue_put_timeout)
        except asyncio.TimeoutError:
            raise RuntimeError(f'{self.__class__.__name__} 事件入队超时')

    async def _process_loop(self) -> None:
        """process() 异常处理循环"""
        while True:
            try:
                await self.process()
            except asyncio.CancelledError:
                logger.info(f'{self.__class__.__name__} process() 被取消')
                break
            except Exception:
                logger.exception(f'{self.__class__.__name__} process() 异常')
                sleep_time = self._config.restart_sleep()
                logger.warning(f'{self.__class__.__name__} process() 异常，等待 {sleep_time:.3f}s 后重启')
                try:
                    await asyncio.sleep(sleep_time)
                except asyncio.CancelledError:
                    logger.info(f'{self.__class__.__name__} process() 重启等待被取消')
                    break

    # ------------------------------------------------------------------
    # 子类接口
    # ------------------------------------------------------------------

    async def get(self) -> tuple[Event, EventBus.Proxy]:
        """从邮箱取下一个 ``(事件, 总线代理)``（阻塞至有事件到达）

        proxy 携带正确的事件链，publish 时会自动追溯因果。
        """
        return await self._queue.get()

    @abstractmethod
    async def process(self) -> None:
        """子类实现：自定义任务循环，通过 ``event, proxy = await self.get()`` 逐事件处理"""
        ...
