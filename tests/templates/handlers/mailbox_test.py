"""MailboxHandler 完整测试

覆盖：生命周期、入队/出队、关闭行为、异常重启、集成总线。
"""

import asyncio
from typing import AsyncGenerator

import pytest

from event_bus import (
    Event,
    EventBus,
    EventDeclaration,
    EventHandlerRegistry,
    EventRegistry,
    InMemoryEventQueue,
    InMemoryEventQueueConfig,
    Regex,
)
from event_bus.templates.handlers.mailbox import MailboxConfig, MailboxHandler


# ---------------------------------------------------------------------------
# 测试用事件
# ---------------------------------------------------------------------------
class MailboxPingEvent(EventDeclaration):
    name = 'mailbox.ping'


class MailboxPongEvent(EventDeclaration):
    name = 'mailbox.pong'


# ---------------------------------------------------------------------------
# 测试用 MailboxHandler 子类
# ---------------------------------------------------------------------------
class _CollectHandler(MailboxHandler):
    """收集所有收到的事件到列表中"""

    def __init__(self, subscriptions: list[str | Regex], config: MailboxConfig | None = None):
        super().__init__(subscriptions=subscriptions, config=config)
        self.received: list[tuple[Event, EventBus.Proxy]] = []

    async def process(self) -> None:
        while True:
            event, proxy = await self.get()
            self.received.append((event, proxy))


class _EchoHandler(MailboxHandler):
    """收到 ping 后发布 pong"""

    def __init__(self):
        super().__init__(subscriptions=[MailboxPingEvent.name])

    async def process(self) -> None:
        while True:
            _event, proxy = await self.get()
            await proxy.publish(MailboxPongEvent.name)


class _CrashThenRecoverHandler(MailboxHandler):
    """前 N 次 process() 抛异常，之后正常收集"""

    def __init__(self, subscriptions: list[str | Regex], crash_count: int):
        super().__init__(subscriptions=subscriptions, config=MailboxConfig(restart_delay=0.0, restart_jitter=0.0))
        self.crash_count = crash_count
        self.crash_remaining = crash_count
        self.received: list[Event] = []

    async def process(self) -> None:
        while True:
            event, _proxy = await self.get()
            if self.crash_remaining > 0:
                self.crash_remaining -= 1
                raise RuntimeError(f'simulated crash #{self.crash_count - self.crash_remaining}')
            self.received.append(event)


class _CancellableProcessHandler(MailboxHandler):
    """process() 可被取消"""

    def __init__(self, subscriptions: list[str | Regex]):
        super().__init__(subscriptions=subscriptions)
        self.process_started = asyncio.Event()
        self.process_ended = asyncio.Event()
        self.was_cancelled = False

    async def process(self) -> None:
        self.process_started.set()
        try:
            while True:
                await self.get()
        except asyncio.CancelledError:
            self.was_cancelled = True
            self.process_ended.set()
            raise


class _SleepDuringRestartHandler(MailboxHandler):
    """process() 抛异常后进入 restart sleep，验证 sleep 期间可被取消"""

    def __init__(self, subscriptions: list[str | Regex]):
        super().__init__(subscriptions=subscriptions, config=MailboxConfig(restart_delay=10.0, restart_jitter=0.0))
        self.restart_entered = asyncio.Event()
        self.was_cancelled = False

    async def process(self) -> None:
        while True:
            _event, _proxy = await self.get()
            self.restart_entered.set()
            raise RuntimeError('boom')


class _BusCaptureHandler(MailboxHandler):
    """验证 bus property"""

    def __init__(self, subscriptions: list[str | Regex]):
        super().__init__(subscriptions=subscriptions)

    async def process(self) -> None:
        while True:
            await self.get()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def event_registry() -> EventRegistry:
    reg = EventRegistry()
    reg.register(MailboxPingEvent)
    reg.register(MailboxPongEvent)
    return reg


@pytest.fixture
def handler_registry() -> EventHandlerRegistry:
    return EventHandlerRegistry()


@pytest.fixture
async def running_bus(
    event_registry: EventRegistry, handler_registry: EventHandlerRegistry
) -> AsyncGenerator[EventBus, None]:
    bus = EventBus(event_registry, handler_registry, queue=InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=32)))
    await bus.start()
    yield bus
    await bus.stop()


# ============================================================================
# 生命周期
# ============================================================================
class TestLifecycle:
    """任务惰性启动、is_running、bus 属性"""

    async def test_task_not_started_on_init(self) -> None:
        """构造后 process() 任务未启动"""
        handler = _CollectHandler(subscriptions=['mailbox.ping'])
        assert not handler.is_running

    async def test_task_starts_on_first_event(
        self, running_bus: EventBus, handler_registry: EventHandlerRegistry
    ) -> None:
        """首个事件到达时自动启动 process() 后台任务"""
        handler = _CollectHandler(subscriptions=['mailbox.ping'])
        handler_registry.register(handler)
        assert not handler.is_running

        await running_bus._publish('mailbox.ping', source='test')
        await asyncio.sleep(0.05)
        assert handler.is_running

    async def test_bus_property_none_before_event(self) -> None:
        """事件到达前 bus 属性为 None"""
        handler = _BusCaptureHandler(subscriptions=['mailbox.ping'])
        assert handler.bus is None

    async def test_bus_property_set_after_event(
        self, running_bus: EventBus, handler_registry: EventHandlerRegistry
    ) -> None:
        """事件到达后 bus 属性指向正确的 EventBus"""
        handler = _BusCaptureHandler(subscriptions=['mailbox.ping'])
        handler_registry.register(handler)

        await running_bus._publish('mailbox.ping', source='test')
        await asyncio.sleep(0.05)
        assert handler.bus is running_bus


# ============================================================================
# 入队 / 出队
# ============================================================================
class TestQueueing:
    """事件入队顺序、批量处理"""

    async def test_events_fifo_order(self, running_bus: EventBus, handler_registry: EventHandlerRegistry) -> None:
        """事件按入队顺序被 process() 取出"""
        handler = _CollectHandler(subscriptions=['mailbox.ping'])
        handler_registry.register(handler)

        await running_bus._publish('mailbox.ping', source='test')
        await running_bus._publish('mailbox.ping', source='test')
        await running_bus._publish('mailbox.ping', source='test')
        await asyncio.sleep(0.1)

        assert len(handler.received) == 3
        for i in range(1, len(handler.received)):
            prev_t = handler.received[i - 1][0].timestamps[-1]
            curr_t = handler.received[i][0].timestamps[-1]
            assert prev_t <= curr_t

    async def test_multiple_events_all_processed(
        self, running_bus: EventBus, handler_registry: EventHandlerRegistry
    ) -> None:
        """大量事件入队后全部被处理"""
        handler = _CollectHandler(subscriptions=['mailbox.ping'])
        handler_registry.register(handler)

        n = 50
        for _ in range(n):
            await running_bus._publish('mailbox.ping', source='test')
        await asyncio.sleep(0.3)

        assert len(handler.received) == n


# ============================================================================
# 关闭行为
# ============================================================================
class TestShutdown:
    """ShutdownEvent 的处理逻辑"""

    async def test_shutdown_cancels_running_task(
        self, running_bus: EventBus, handler_registry: EventHandlerRegistry
    ) -> None:
        """ShutdownEvent 取消正在运行的 process() 任务"""
        handler = _CollectHandler(subscriptions=['mailbox.ping'])
        handler_registry.register(handler)

        await running_bus._publish('mailbox.ping', source='test')
        await asyncio.sleep(0.05)
        assert handler.is_running

        await running_bus.stop()
        await asyncio.sleep(0.05)
        assert not handler.is_running

    async def test_shutdown_before_any_event_does_not_crash(self, handler_registry: EventHandlerRegistry) -> None:
        """任务未启动时收到 ShutdownEvent，不会崩溃也不会创建任务"""
        handler = _CollectHandler(subscriptions=['mailbox.ping'])
        handler_registry.register(handler)
        assert not handler.is_running

        # 直接调用 handle 传入 shutdown 事件 — 不应崩溃
        event = Event(name='event_bus.__shutdown__', data=None)
        await handler.handle(None, None, event)  # type: ignore[arg-type]
        assert not handler.is_running

    async def test_shutdown_event_not_queued(
        self, running_bus: EventBus, handler_registry: EventHandlerRegistry
    ) -> None:
        """ShutdownEvent 不会进入邮箱队列"""
        handler = _CollectHandler(subscriptions=['mailbox.ping'])
        handler_registry.register(handler)

        await running_bus._publish('mailbox.ping', source='test')
        await asyncio.sleep(0.05)

        await running_bus.stop()
        await asyncio.sleep(0.05)

        event_names = [e.name for e, _ in handler.received]
        assert 'event_bus.__shutdown__' not in event_names
        assert 'mailbox.ping' in event_names

    async def test_shutdown_clears_task_reference(
        self, running_bus: EventBus, handler_registry: EventHandlerRegistry
    ) -> None:
        """关闭后 _task 被置为 None"""
        handler = _CollectHandler(subscriptions=['mailbox.ping'])
        handler_registry.register(handler)

        await running_bus._publish('mailbox.ping', source='test')
        await asyncio.sleep(0.05)
        assert handler._task is not None

        await running_bus.stop()
        await asyncio.sleep(0.05)
        assert handler._task is None

    async def test_is_running_false_after_shutdown(
        self, running_bus: EventBus, handler_registry: EventHandlerRegistry
    ) -> None:
        """关闭后 is_running 返回 False"""
        handler = _CollectHandler(subscriptions=['mailbox.ping'])
        handler_registry.register(handler)

        await running_bus._publish('mailbox.ping', source='test')
        await asyncio.sleep(0.05)
        assert handler.is_running

        await running_bus.stop()
        await asyncio.sleep(0.05)
        assert not handler.is_running


# ============================================================================
# 异常处理与重启
# ============================================================================
class TestExceptionHandling:
    """process() 异常 → 重启、CancelledError → 停止"""

    async def test_process_exception_triggers_restart(self) -> None:
        """process() 抛非 CancelledError 异常后自动重启并继续处理"""
        handler = _CrashThenRecoverHandler(subscriptions=['mailbox.ping'], crash_count=2)

        handler._bus = object()  # type: ignore[assignment]
        handler._task = asyncio.create_task(handler._process_loop())
        await asyncio.sleep(0.02)

        for _ in range(3):
            handler._queue.put_nowait((Event(name='mailbox.ping', data=None), None))  # type: ignore[arg-type]

        await asyncio.sleep(0.2)
        handler._task.cancel()
        try:
            await handler._task
        except asyncio.CancelledError:
            pass

        # 前 2 次崩溃（事件丢失），第 3 次正常收集
        assert handler.crash_remaining == 0
        assert len(handler.received) == 1

    async def test_cancelled_error_stops_loop(self) -> None:
        """CancelledError 使 _process_loop 退出"""
        handler = _CancellableProcessHandler(subscriptions=['mailbox.ping'])

        handler._bus = object()  # type: ignore[assignment]
        handler._task = asyncio.create_task(handler._process_loop())
        await handler.process_started.wait()

        handler._task.cancel()
        try:
            await handler._task
        except asyncio.CancelledError:
            pass

        assert handler.was_cancelled

    async def test_cancelled_during_restart_sleep_stops_loop(self) -> None:
        """restart sleep 期间收到取消信号，正确退出"""
        handler = _SleepDuringRestartHandler(subscriptions=['mailbox.ping'])

        handler._bus = object()  # type: ignore[assignment]
        handler._task = asyncio.create_task(handler._process_loop())
        await asyncio.sleep(0.02)

        handler._queue.put_nowait((Event(name='mailbox.ping', data=None), None))  # type: ignore[arg-type]
        await handler.restart_entered.wait()
        await asyncio.sleep(0.05)

        handler._task.cancel()
        try:
            await handler._task
        except asyncio.CancelledError:
            pass

        assert handler._task.done()


# ============================================================================
# 集成测试
# ============================================================================
class TestIntegration:
    """与 EventBus 的完整集成"""

    async def test_ping_pong_via_mailbox(self, running_bus: EventBus, handler_registry: EventHandlerRegistry) -> None:
        """MailboxHandler 收到 ping 后通过 proxy 发布 pong"""
        echo = _EchoHandler()
        collect = _CollectHandler(subscriptions=[MailboxPongEvent.name])
        handler_registry.register(echo)
        handler_registry.register(collect)

        await running_bus._publish('mailbox.ping', source='test')
        await asyncio.sleep(0.1)

        assert len(collect.received) == 1
        assert collect.received[0][0].name == 'mailbox.pong'

    async def test_event_chain_tracking(self, running_bus: EventBus, handler_registry: EventHandlerRegistry) -> None:
        """proxy 携带正确的事件链信息"""
        handler = _CollectHandler(subscriptions=[MailboxPingEvent.name])
        handler_registry.register(handler)

        await running_bus._publish('mailbox.ping', source='origin')
        await asyncio.sleep(0.1)

        assert len(handler.received) == 1
        event, _proxy = handler.received[0]
        assert 'origin' in event.sources

    async def test_handler_receives_events_before_shutdown(
        self, running_bus: EventBus, handler_registry: EventHandlerRegistry
    ) -> None:
        """总线关闭前发布的事件在关闭前被处理"""
        handler = _CollectHandler(subscriptions=['mailbox.ping'])
        handler_registry.register(handler)

        await running_bus._publish('mailbox.ping', source='test')
        await asyncio.sleep(0.05)
        assert len(handler.received) >= 1


# ============================================================================
# MailboxConfig
# ============================================================================
class TestMailboxConfig:
    """配置项默认值与行为"""

    def test_default_values(self) -> None:
        config = MailboxConfig()
        assert config.queue_put_timeout is None
        assert config.restart_delay == 0.5
        assert config.restart_jitter == 0.2
        assert config.max_queue_size == 0

    def test_restart_sleep_in_range(self) -> None:
        config = MailboxConfig(restart_delay=0.5, restart_jitter=0.2)
        for _ in range(100):
            s = config.restart_sleep()
            assert 0.5 <= s < 0.7

    def test_custom_config(self) -> None:
        config = MailboxConfig(
            queue_put_timeout=5.0,
            restart_delay=1.0,
            restart_jitter=0.0,
            max_queue_size=100,
        )
        assert config.queue_put_timeout == 5.0
        assert config.restart_delay == 1.0
        assert config.restart_jitter == 0.0
        assert config.max_queue_size == 100
        assert config.restart_sleep() == 1.0

    def test_max_queue_size_unbounded_by_default(self) -> None:
        """默认 max_queue_size=0 → 无界"""
        handler = _CollectHandler(subscriptions=['mailbox.ping'])
        assert handler._queue.maxsize == 0

    def test_max_queue_size_bounded(self) -> None:
        """max_queue_size > 0 时队列容量受限"""
        handler = _CollectHandler(
            subscriptions=['mailbox.ping'],
            config=MailboxConfig(max_queue_size=5),
        )
        assert handler._queue.maxsize == 5


# ============================================================================
# 边界场景
# ============================================================================
class TestEdgeCases:
    """边界与竞态场景"""

    async def test_concurrent_events_during_startup(
        self, running_bus: EventBus, handler_registry: EventHandlerRegistry
    ) -> None:
        """并发事件到达时仅创建一个 process() 任务"""
        handler = _CollectHandler(subscriptions=['mailbox.ping'])
        handler_registry.register(handler)

        async def publish_many():
            for _ in range(10):
                await running_bus._publish('mailbox.ping', source='test')

        tasks = [asyncio.create_task(publish_many()) for _ in range(3)]
        await asyncio.gather(*tasks)
        await asyncio.sleep(0.2)

        assert len(handler.received) == 30
        assert handler.is_running

    async def test_handler_with_regex_subscription(
        self, running_bus: EventBus, handler_registry: EventHandlerRegistry
    ) -> None:
        """支持 Regex 订阅"""
        handler = _CollectHandler(subscriptions=[Regex(r'mailbox\..*')])
        handler_registry.register(handler)

        await running_bus._publish('mailbox.ping', source='test')
        await running_bus._publish('mailbox.pong', source='test')
        await asyncio.sleep(0.1)

        names = [e.name for e, _ in handler.received]
        assert 'mailbox.ping' in names
        assert 'mailbox.pong' in names

    async def test_shutdown_event_auto_subscribed(self) -> None:
        """ShutdownEvent 自动加入订阅列表"""
        handler = _CollectHandler(subscriptions=['mailbox.ping'])
        assert 'event_bus.__shutdown__' in handler.subscriptions

    async def test_shutdown_not_duplicated_in_subscriptions(self) -> None:
        """手动添加 ShutdownEvent 时不会重复"""
        handler = _CollectHandler(subscriptions=['mailbox.ping', 'event_bus.__shutdown__'])
        assert handler.subscriptions.count('event_bus.__shutdown__') == 1
