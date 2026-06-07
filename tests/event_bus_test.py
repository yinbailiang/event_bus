import asyncio
import time
import pytest
import logging
from typing import List, Dict, Optional, Type, Any, Callable, Awaitable
from pydantic import BaseModel, Field

from event_bus import (
    EventBus,
    Event,
    EventDeclaration,
    EventHandler,
    EventRegistry,
    EventHandlerRegistry,
    TaskErrorPayload,
    TaskErrorEvent,
    BusShuttingDown,
)

# 抑制日志噪音
logging.getLogger("core.event_bus").setLevel(logging.WARNING)


# ============================================================================
# 测试用 Payload 与事件声明（集中管理）
# ============================================================================
class BusTestPayload(BaseModel):
    value: int
    msg: str = Field(default="test")


class TestEventDecl(EventDeclaration):
    name = "test.event"
    payload_type = BusTestPayload


class SlowEventDecl(EventDeclaration):
    name = "test.slow"


class BlockEventDecl(EventDeclaration):
    name = "test.block"


class UserLoginEventDecl(EventDeclaration):
    name = "user.login"


class UserLogoutEventDecl(EventDeclaration):
    name = "user.logout"


class AdminLoginEventDecl(EventDeclaration):
    name = "admin.login"


# 所有需要预先注册的事件列表（基础注册）
BASE_EVENT_DECLS: List[Type[EventDeclaration]] = [
    TestEventDecl,
    SlowEventDecl,
    BlockEventDecl,
    UserLoginEventDecl,
    UserLogoutEventDecl,
    AdminLoginEventDecl,
    TaskErrorEvent,
]


# ============================================================================
# 可复用的 Handler 工厂与基类
# ============================================================================
class BaseTestHandler(EventHandler):
    """提供常用辅助方法的测试 Handler 基类"""

    def __init__(self, subscriptions: List[str], handle_timeout: float = 1.0):
        super().__init__(subscriptions, handle_timeout)
        self._started = asyncio.Event()
        self._completed = asyncio.Event()
        self._error: Optional[Exception] = None

    async def handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event) -> None:
        self._started.set()
        try:
            await self._do_handle(payload, bus_proxy, raw_event)
        except Exception as e:
            self._error = e
            raise
        finally:
            self._completed.set()

    async def _do_handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event) -> None:
        """子类应实现此方法"""
        raise NotImplementedError

    async def wait_started(self, timeout: float = 1.0) -> None:
        await asyncio.wait_for(self._started.wait(), timeout)

    async def wait_completed(self, timeout: float = 1.0) -> None:
        await asyncio.wait_for(self._completed.wait(), timeout)

    @property
    def error(self) -> Optional[Exception]:
        return self._error


class CountingHandler(BaseTestHandler):
    """统计 test.event 成功处理次数（value >= 0）"""

    def __init__(self):
        super().__init__(["test.event"])
        self.count = 0

    async def _do_handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event):
        if isinstance(payload, BusTestPayload):
            if payload.value >= 0:
                self.count += 1
            else:
                raise ValueError(f"Invalid value: {payload.value}")


class SlowHandler(BaseTestHandler):
    """模拟耗时处理，可配置延迟"""

    def __init__(self, delay: float = 0.5, subscriptions: Optional[List[str]] = None, handle_timeout: float=0.1):
        super().__init__(subscriptions or ["test.slow"], handle_timeout=handle_timeout)
        self.delay = delay
        self.completed = 0

    async def _do_handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event):
        await asyncio.sleep(self.delay)
        self.completed += 1


class BlockingHandler(BaseTestHandler):
    """可控制阻塞的 Handler，用于测试背压/关闭"""

    def __init__(self, subscriptions: Optional[List[str]] = None):
        super().__init__(subscriptions or ["test.block"], handle_timeout=10.0)
        self._block = asyncio.Event()

    async def _do_handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event):
        await self._block.wait()  # 永久阻塞直到外部释放

    def release(self):
        self._block.set()


class ConcurrentTrackingHandler(BaseTestHandler):
    """跟踪并发数，用于测试 Semaphore 限流"""

    def __init__(self, subscriptions: Optional[List[str]] = None):
        super().__init__(subscriptions or ["test.block"], handle_timeout=10.0)
        self.active_count = 0
        self.max_seen = 0
        self._lock = asyncio.Lock()
        self._done = asyncio.Event()

    async def _do_handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event):
        async with self._lock:
            self.active_count += 1
            self.max_seen = max(self.max_seen, self.active_count)

        await self._done.wait()  # 外部控制何时结束

        async with self._lock:
            self.active_count -= 1

    def release_all(self):
        self._done.set()


class ErrorSpyHandler(BaseTestHandler):
    """捕获 __task_error__ 事件"""

    def __init__(self):
        super().__init__(["event_bus.__task_error__"])
        self.captured: List[Dict[str, str]] = []

    async def _do_handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event):
        if isinstance(payload, TaskErrorPayload):
            self.captured.append(
                {
                    "handler": payload.handler_name,
                    "type": payload.error_type,
                    "msg": payload.error_message,
                }
            )


class PatternSpyHandler(BaseTestHandler):
    """记录匹配正则的事件名"""

    def __init__(self, pattern: str = r"user\..*"):
        super().__init__([pattern])
        self.triggered: List[str] = []

    async def _do_handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event):
        self.triggered.append(raw_event.name)


# ============================================================================
# Fixtures 设计：提供干净、可定制的 EventBus
# ============================================================================
def create_event_registry(event_classes: List[Type[EventDeclaration]]) -> EventRegistry:
    """工厂函数：根据事件类列表创建注册表"""
    reg = EventRegistry()
    for cls in event_classes:
        reg.register(cls)
    return reg


@pytest.fixture
def base_event_registry() -> EventRegistry:
    """包含所有基础测试事件的注册表"""
    return create_event_registry(BASE_EVENT_DECLS)


@pytest.fixture
def empty_event_registry() -> EventRegistry:
    """空注册表，测试需要时可手动注册"""
    return EventRegistry()


@pytest.fixture
def handler_registry() -> EventHandlerRegistry:
    """空 Handler 注册表"""
    return EventHandlerRegistry()


@pytest.fixture
def event_bus_factory(
    base_event_registry: EventRegistry, handler_registry: EventHandlerRegistry
) -> Callable[..., EventBus]:
    """返回一个可配置参数的 EventBus 工厂函数"""

    def _create(
        max_queue_size: int = 10,
        max_handler_semaphore: int = 20,
        registry: Optional[EventRegistry] = None,
        h_registry: Optional[EventHandlerRegistry] = None,
    ) -> EventBus:
        return EventBus(
            registry or base_event_registry,
            h_registry or handler_registry,
            max_queue_size=max_queue_size,
            max_handler_semaphore=max_handler_semaphore,
        )

    return _create


@pytest.fixture
async def event_bus(event_bus_factory: Callable[..., EventBus]):
    """默认的、已启动的 EventBus，测试结束后自动停止"""
    bus: EventBus = event_bus_factory()
    await bus.start()
    yield bus
    await bus.stop()


# ============================================================================
# 辅助工具函数
# ============================================================================
async def wait_for_condition(
    condition: Callable[[], bool], timeout: float = 2.0, interval: float = 0.01
) -> None:
    """轮询等待条件成立，避免固定 sleep"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        await asyncio.sleep(interval)
    raise TimeoutError(f"Condition not met within {timeout}s")


async def publish_many(
    bus: EventBus, event_name: str, payloads: List[Any], client_name: str = "pub"
) -> List[Awaitable[Any]]:
    """批量发布事件，返回 awaitable 列表"""
    proxy = bus.proxy(client_name)
    return [proxy.publish(event_name, p) for p in payloads]


# ============================================================================
# 测试用例
# ============================================================================
@pytest.mark.asyncio
async def test_high_concurrency_throughput(event_bus_factory: Callable[..., EventBus]) -> None:
    """验证高并发下无事件丢失"""
    event_bus = event_bus_factory(max_queue_size=1024, max_handler_semaphore=256)
    handler = CountingHandler()
    event_bus.register_handler(handler)

    N = 65536
    async with event_bus:
        tasks = [event_bus.proxy("pub").publish("test.event", {"value": i}) for i in range(N)]
        await asyncio.gather(*tasks)

        # 等待所有任务完成（使用轮询而非固定 sleep）
        await wait_for_condition(lambda: handler.count == N, timeout=2.0)
        assert handler.count == N, f"吞吐量丢失: 期望 {N}, 实际 {handler.count}"


@pytest.mark.asyncio
async def test_backpressure_no_deadlock(event_bus_factory: Callable[..., EventBus]) -> None:
    """测试并发限流：当活跃 Handler 超过 Semaphore 限制时，新任务应等待"""
    # 构建具有较小 Semaphore 的总线
    bus = event_bus_factory(max_handler_semaphore=2)
    handler = ConcurrentTrackingHandler(subscriptions=["test.block"])
    bus.register_handler(handler)

    N = 10
    async with bus:
        tasks = [asyncio.create_task(bus.proxy("bp_pub").publish("test.block", None)) for _ in range(N)]

        # 等待至少有一个任务开始执行
        await handler.wait_started()
        # 允许所有等待中的任务开始（但受 Semaphore 限制）
        await asyncio.sleep(0.05)  # 短暂等待调度
        # 验证并发上限为 2
        assert handler.max_seen == 2, f"Semaphore 限流失效: max_seen={handler.max_seen}"

        # 释放所有阻塞，让任务完成
        handler.release_all()
        await asyncio.gather(*tasks)

    assert handler.active_count == 0


@pytest.mark.asyncio
async def test_error_isolation_and_propagation(event_bus: EventBus) -> None:
    """验证单个 Handler 错误不影响其他 Handler，且错误被正确上报"""
    counter = CountingHandler()
    spy = ErrorSpyHandler()
    event_bus.register_handler(counter)
    event_bus.register_handler(spy)

    tasks: List[Awaitable[Any]] = await publish_many(
        event_bus,
        "test.event",
        [{"value": 1}, {"value": -1}, {"value": 2}],
    )
    await asyncio.gather(*tasks)  # 等待发布完成
    await wait_for_condition(lambda: len(spy.captured) >= 1)

    assert counter.count == 2, "有效事件应被正常计数"
    assert len(spy.captured) == 1
    assert spy.captured[0]["type"] == "ValueError"


@pytest.mark.asyncio
async def test_handler_timeout_handling(event_bus: EventBus) -> None:
    """验证 Handler 超时后触发错误事件，且不影响总线运行"""
    slow = SlowHandler(delay=0.5, subscriptions=["test.slow"])
    spy = ErrorSpyHandler()
    event_bus.register_handler(slow)
    event_bus.register_handler(spy)

    await event_bus.proxy("timeout_pub").publish("test.slow", None)

    # 等待超时错误被捕获
    await wait_for_condition(lambda: len(spy.captured) >= 1, timeout=1.0)
    assert slow.completed == 0, "超时任务不应完成计数"
    assert spy.captured[0]["type"] == "TimeoutError"


@pytest.mark.asyncio
async def test_pattern_matching_routing(event_bus: EventBus) -> None:
    """验证正则表达式订阅的路由正确性"""
    spy = PatternSpyHandler(pattern=r"user\..*")
    event_bus.register_handler(spy)

    await event_bus.proxy("route_pub").publish("user.login", None)
    await event_bus.proxy("route_pub").publish("user.logout", None)
    await event_bus.proxy("route_pub").publish("admin.login", None)

    await wait_for_condition(lambda: len(spy.triggered) >= 2)
    assert set(spy.triggered) == {"user.login", "user.logout"}


@pytest.mark.asyncio
async def test_graceful_shutdown_and_cleanup(event_bus: EventBus) -> None:
    """验证总线优雅停止后资源清理完全"""
    handler = CountingHandler()
    event_bus.register_handler(handler)

    # 发布一个事件并等待处理
    await event_bus.proxy("shut_pub").publish("test.event", {"value": 1})
    await wait_for_condition(lambda: handler.count == 1)

    # 停止总线
    await event_bus.stop()

    assert not event_bus.is_running
    assert event_bus.get_active_task_count() == 0
    assert event_bus.get_queue_size() == 0


@pytest.mark.slow
@pytest.mark.asyncio
async def test_long_running_stability(event_bus_factory: Callable[..., EventBus]) -> None:
    """长时间运行压力测试：验证无资源泄漏和队列溢出"""
    bus: EventBus = event_bus_factory(max_queue_size=10, max_handler_semaphore=25)
    handler = CountingHandler()
    bus.register_handler(handler)

    duration = 10  # 秒
    rate = 512  # 每秒事件数
    expected = duration * rate

    async with bus:
        start = time.time()
        while time.time() - start < duration:
            batch = [
                bus.proxy("stress_pub").publish("test.event", {"value": i})
                for i in range(rate)
            ]
            await asyncio.gather(*batch)
            await asyncio.sleep(1.0)

            active = bus.get_active_task_count()
            qsize = bus.get_queue_size()
            assert active <= 25, f"任务泄漏: active={active}"
            assert qsize <= 10, f"队列溢出: qsize={qsize}"

    # 最终计数应接近期望值（允许少许误差）
    assert handler.count >= expected * 0.95
    assert bus.get_active_task_count() == 0


@pytest.mark.asyncio
async def test_shutting_down_exception(event_bus: EventBus) -> None:
    """验证总线停止过程中新发布事件应抛出 BusShuttingDown 或 RuntimeError"""
    # 注册一个慢 Handler 使停止过程持续一段时间
    slow = SlowHandler(delay=2.0, subscriptions=["test.slow"])
    event_bus.register_handler(slow)

    # 发布一个慢事件
    asyncio.create_task(event_bus.proxy("slow_pub").publish("test.slow", None))
    await slow.wait_started()  # 确保 Handler 已开始执行

    # 启动停止流程（不等待完成）
    stop_task = asyncio.create_task(event_bus.stop())

    # 在停止过程中尝试发布新事件，应触发异常
    caught_exception = None
    for _ in range(20):  # 重试最多 1 秒
        try:
            await event_bus.proxy("probe").publish("test.event", {"value": 1})
            await asyncio.sleep(0.05)
        except (BusShuttingDown, RuntimeError) as e:
            caught_exception = type(e)
            break

    assert caught_exception in (BusShuttingDown, RuntimeError), \
        f"Expected BusShuttingDown or RuntimeError, got {caught_exception}"

    # 等待完全停止
    await stop_task
    assert not event_bus.is_running

    # 停止后发布事件应抛出 RuntimeError
    with pytest.raises(RuntimeError, match="EventBus is not running"):
        await event_bus.proxy("after_stop").publish("test.event", {"value": 2})