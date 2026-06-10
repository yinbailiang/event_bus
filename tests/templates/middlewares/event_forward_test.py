"""事件转发中间件测试：EventForwardMiddleware。"""

import asyncio
from typing import Any, List, Optional

import pytest
from pydantic import BaseModel

from event_bus import (
    Regex,
    Event,
    EventBus,
    EventDeclaration,
    EventHandler,
    EventHandlerRegistry,
    EventRegistry,
    MiddlewareChain,
)
from event_bus.templates import (
    EventForwardMiddleware,
    make_event_name_filter,
)

from conftest import (
    MiddlewareTestPayload,
    SimplePingHandler,
    create_event_registry,
    BASE_EVENT_DECLS,
)


# ============================================================================
# 辅助：用于验证转发结果的 Handler
# ============================================================================


class ForwardSpyHandler(EventHandler):
    """记录所有接收到的事件名和来源，用于验证转发结果。"""

    def __init__(self, subscriptions: List[str | Regex]) -> None:
        super().__init__(subscriptions)
        self.received_names: List[str] = []
        self.received_sources: List[str] = []
        self.received_payloads: List[Optional[BaseModel]] = []
        self._event = asyncio.Event()

    async def handle(
        self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event
    ) -> None:
        self.received_names.append(raw_event.name)
        self.received_sources.append(raw_event.sources[-1] if raw_event.sources else "")
        self.received_payloads.append(payload)
        self._event.set()

    async def wait_received(self, timeout: float = 2.0) -> None:
        await asyncio.wait_for(self._event.wait(), timeout)


# ============================================================================
# EventForwardMiddleware
# ============================================================================


class TestEventForwardMiddleware:
    """事件转发中间件"""

    @pytest.mark.asyncio
    async def test_basic_forward(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """源总线事件自动转发到目标总线"""
        # 1. 创建目标总线（独立注册表 + 处理器）
        target_registry = create_event_registry(BASE_EVENT_DECLS)
        target_handlers = EventHandlerRegistry()
        spy = ForwardSpyHandler(["mw.ping"])
        target_handlers.register(spy)

        target_bus = EventBus(target_registry, target_handlers, max_queue_size=10)

        # 2. 源总线装配转发中间件
        fw = EventForwardMiddleware(target=target_bus, source_name="forward-test")
        chain = MiddlewareChain()
        chain.add(fw)

        source_bus = EventBus(
            base_event_registry,
            handler_registry,
            max_queue_size=10,
            middleware_chain=chain,
        )

        async with target_bus:
            async with source_bus:
                await source_bus.proxy("src").publish(
                    "mw.ping", {"key": "hello", "count": 42}
                )
                await spy.wait_received(timeout=3.0)

        # 目标总线收到了转发的事件
        assert len(spy.received_names) >= 1
        assert spy.received_names[0] == "mw.ping"
        assert spy.received_sources[0] == "forward-test"

    @pytest.mark.asyncio
    async def test_forward_with_payload(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """转发事件的负载数据完整传递"""
        target_registry = create_event_registry(BASE_EVENT_DECLS)
        target_handlers = EventHandlerRegistry()
        spy = ForwardSpyHandler(["mw.ping"])
        target_handlers.register(spy)

        target_bus = EventBus(target_registry, target_handlers, max_queue_size=10)

        fw = EventForwardMiddleware(target=target_bus, source_name="payload-test")
        chain = MiddlewareChain()
        chain.add(fw)

        source_bus = EventBus(
            base_event_registry,
            handler_registry,
            max_queue_size=10,
            middleware_chain=chain,
        )

        async with target_bus:
            async with source_bus:
                await source_bus.proxy("src").publish(
                    "mw.ping", {"key": "data-check", "count": 99}
                )
                await spy.wait_received(timeout=3.0)

        assert len(spy.received_payloads) >= 1
        payload = spy.received_payloads[0]
        assert isinstance(payload, MiddlewareTestPayload)
        assert payload.key == "data-check"
        assert payload.count == 99

    @pytest.mark.asyncio
    async def test_event_filter_whitelist(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """白名单过滤：仅转发指定事件"""
        target_registry = create_event_registry(BASE_EVENT_DECLS)
        target_handlers = EventHandlerRegistry()
        spy = ForwardSpyHandler(["mw.ping"])
        target_handlers.register(spy)

        target_bus = EventBus(target_registry, target_handlers, max_queue_size=10)

        # 仅转发 mw.ping
        event_filter = make_event_name_filter("mw.ping", mode="white")
        fw = EventForwardMiddleware(
            target=target_bus,
            source_name="filter-test",
            event_filter=event_filter,
        )
        chain = MiddlewareChain()
        chain.add(fw)

        source_bus = EventBus(
            base_event_registry,
            handler_registry,
            max_queue_size=10,
            middleware_chain=chain,
        )

        async with target_bus:
            async with source_bus:
                # 发布 mw.ping → 应被转发
                await source_bus.proxy("src").publish(
                    "mw.ping", {"key": "forwarded", "count": 1}
                )
                # 发布 user.login → 不应被转发
                await source_bus.proxy("src").publish("user.login", None)
                await spy.wait_received(timeout=3.0)

        # 仅 mw.ping 被转发
        assert len(spy.received_names) == 1
        assert spy.received_names[0] == "mw.ping"

    @pytest.mark.asyncio
    async def test_event_filter_blacklist(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """黑名单过滤：排除指定事件，转发其余"""
        target_registry = create_event_registry(BASE_EVENT_DECLS)
        target_handlers = EventHandlerRegistry()
        spy = ForwardSpyHandler(["mw.ping", "user.login"])
        target_handlers.register(spy)

        target_bus = EventBus(target_registry, target_handlers, max_queue_size=10)

        # 排除 mw.ping，其余全部转发
        event_filter = make_event_name_filter("mw.ping", mode="black")
        fw = EventForwardMiddleware(
            target=target_bus,
            source_name="blacklist-test",
            event_filter=event_filter,
        )
        chain = MiddlewareChain()
        chain.add(fw)

        source_bus = EventBus(
            base_event_registry,
            handler_registry,
            max_queue_size=10,
            middleware_chain=chain,
        )

        async with target_bus:
            async with source_bus:
                await source_bus.proxy("src").publish(
                    "mw.ping", {"key": "blocked", "count": 1}
                )
                await source_bus.proxy("src").publish("user.login", None)
                # 等待足够时间让两个事件都被处理
                await asyncio.sleep(0.3)

        # mw.ping 被排除，仅 user.login 被转发
        forwarded = [n for n in spy.received_names if n == "user.login"]
        assert len(forwarded) >= 1
        assert "mw.ping" not in spy.received_names

    @pytest.mark.asyncio
    async def test_system_events_not_forwarded_by_default(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """系统事件（event_bus.*）默认不转发"""
        target_registry = create_event_registry(BASE_EVENT_DECLS)
        target_handlers = EventHandlerRegistry()

        # 同时订阅用户事件和系统事件
        spy = ForwardSpyHandler(
            ["mw.ping", "event_bus.__shutdown__", "event_bus.__task_error__"]
        )
        target_handlers.register(spy)

        target_bus = EventBus(target_registry, target_handlers, max_queue_size=10)

        fw = EventForwardMiddleware(
            target=target_bus,
            source_name="no-sys-test",
            forward_system_events=False,  # 默认值
        )
        chain = MiddlewareChain()
        chain.add(fw)

        source_bus = EventBus(
            base_event_registry,
            handler_registry,
            max_queue_size=10,
            middleware_chain=chain,
        )

        async with target_bus:
            async with source_bus:
                await source_bus.proxy("src").publish(
                    "mw.ping", {"key": "user-event", "count": 1}
                )
                await spy.wait_received(timeout=3.0)

        # 用户事件被转发
        assert "mw.ping" in spy.received_names
        # 来自转发源的事件中没有系统事件（目标总线自身的生命周期事件除外）
        for i, name in enumerate(spy.received_names):
            if spy.received_sources[i] == "no-sys-test":
                assert not name.startswith("event_bus."), (
                    f"system event '{name}' was forwarded but should not be"
                )

    @pytest.mark.asyncio
    async def test_system_events_forwarded_when_enabled(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """设置 forward_system_events=True 时系统事件也被转发"""
        # 注册一个自定义系统事件用于测试（无负载）
        class CustomSysEvent(EventDeclaration):
            name = "event_bus.custom_test"

        # 源和目标都需要注册该事件
        source_registry = create_event_registry(BASE_EVENT_DECLS)
        source_registry.register(CustomSysEvent)

        target_registry = create_event_registry(BASE_EVENT_DECLS)
        target_registry.register(CustomSysEvent)

        target_handlers = EventHandlerRegistry()
        spy = ForwardSpyHandler(["event_bus.custom_test"])
        target_handlers.register(spy)

        target_bus = EventBus(target_registry, target_handlers, max_queue_size=10)

        fw = EventForwardMiddleware(
            target=target_bus,
            source_name="sys-fwd-test",
            forward_system_events=True,
        )
        chain = MiddlewareChain()
        chain.add(fw)

        source_bus = EventBus(
            source_registry,
            handler_registry,
            max_queue_size=10,
            middleware_chain=chain,
        )

        async with target_bus:
            async with source_bus:
                await source_bus.proxy("src").publish("event_bus.custom_test", None)
                await spy.wait_received(timeout=3.0)

        # 系统事件被转发
        sys_events = [n for n in spy.received_names if n.startswith("event_bus.")]
        assert len(sys_events) >= 1

    @pytest.mark.asyncio
    async def test_dynamic_target_provider(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """动态目标总线提供者：每次转发时获取最新实例"""
        target_registry = create_event_registry(BASE_EVENT_DECLS)
        target_handlers = EventHandlerRegistry()
        spy = ForwardSpyHandler(["mw.ping"])
        target_handlers.register(spy)

        target_bus = EventBus(target_registry, target_handlers, max_queue_size=10)

        # 使用工厂函数提供目标总线
        def get_target() -> EventBus:
            return target_bus

        fw = EventForwardMiddleware(target=get_target, source_name="dynamic-test")
        chain = MiddlewareChain()
        chain.add(fw)

        source_bus = EventBus(
            base_event_registry,
            handler_registry,
            max_queue_size=10,
            middleware_chain=chain,
        )

        async with target_bus:
            async with source_bus:
                await source_bus.proxy("src").publish(
                    "mw.ping", {"key": "dynamic", "count": 7}
                )
                await spy.wait_received(timeout=3.0)

        assert len(spy.received_names) >= 1
        assert spy.received_names[0] == "mw.ping"
        assert spy.received_sources[0] == "dynamic-test"

    @pytest.mark.asyncio
    async def test_custom_sync_filter(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """自定义同步过滤回调"""
        target_registry = create_event_registry(BASE_EVENT_DECLS)
        target_handlers = EventHandlerRegistry()
        spy = ForwardSpyHandler(["mw.ping"])
        target_handlers.register(spy)

        target_bus = EventBus(target_registry, target_handlers, max_queue_size=10)

        def count_filter(event: Event) -> bool:
            if event.data is not None:
                data_dict = event.data.model_dump()
                return data_dict.get("count", 0) > 10
            return False

        fw = EventForwardMiddleware(
            target=target_bus,
            source_name="custom-filter",
            event_filter=count_filter,
        )
        chain = MiddlewareChain()
        chain.add(fw)

        source_bus = EventBus(
            base_event_registry,
            handler_registry,
            max_queue_size=10,
            middleware_chain=chain,
        )

        async with target_bus:
            async with source_bus:
                # count=5 → 不转发
                await source_bus.proxy("src").publish(
                    "mw.ping", {"key": "low", "count": 5}
                )
                # count=100 → 转发
                await source_bus.proxy("src").publish(
                    "mw.ping", {"key": "high", "count": 100}
                )
                await spy.wait_received(timeout=3.0)

        assert len(spy.received_payloads) >= 1
        payload = spy.received_payloads[0]
        assert isinstance(payload, MiddlewareTestPayload)
        assert payload.key == "high"
        assert payload.count == 100

    @pytest.mark.asyncio
    async def test_custom_async_filter(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """自定义异步过滤回调"""
        target_registry = create_event_registry(BASE_EVENT_DECLS)
        target_handlers = EventHandlerRegistry()
        spy = ForwardSpyHandler(["mw.ping"])
        target_handlers.register(spy)

        target_bus = EventBus(target_registry, target_handlers, max_queue_size=10)

        async def async_filter(event: Event) -> bool:
            await asyncio.sleep(0.01)  # 模拟异步检查
            if event.data is not None:
                data_dict = event.data.model_dump()
                return data_dict.get("key", "") == "async-ok"
            return False

        fw = EventForwardMiddleware(
            target=target_bus,
            source_name="async-filter",
            event_filter=async_filter,
        )
        chain = MiddlewareChain()
        chain.add(fw)

        source_bus = EventBus(
            base_event_registry,
            handler_registry,
            max_queue_size=10,
            middleware_chain=chain,
        )

        async with target_bus:
            async with source_bus:
                await source_bus.proxy("src").publish(
                    "mw.ping", {"key": "async-no", "count": 1}
                )
                await source_bus.proxy("src").publish(
                    "mw.ping", {"key": "async-ok", "count": 2}
                )
                await spy.wait_received(timeout=3.0)

        assert len(spy.received_payloads) >= 1
        payload = spy.received_payloads[0]
        assert isinstance(payload, MiddlewareTestPayload)
        assert payload.key == "async-ok"

    @pytest.mark.asyncio
    async def test_error_isolation_target_unreachable(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """转发失败不影响源总线正常运行 —— 目标总线未启动"""
        target_registry = create_event_registry(BASE_EVENT_DECLS)
        target_handlers = EventHandlerRegistry()

        # 目标总线不启动
        target_bus = EventBus(target_registry, target_handlers, max_queue_size=10)

        fw = EventForwardMiddleware(target=target_bus, source_name="error-iso")
        chain = MiddlewareChain()
        chain.add(fw)

        handler = SimplePingHandler()
        handler_registry.register(handler)

        source_bus = EventBus(
            base_event_registry,
            handler_registry,
            max_queue_size=10,
            middleware_chain=chain,
        )

        # 源总线正常启动，目标总线未启动
        async with source_bus:
            await source_bus.proxy("src").publish(
                "mw.ping", {"key": "still-works", "count": 1}
            )
            await handler.wait_received(timeout=3.0)

        # 源总线的 handler 仍然正常收到事件
        assert len(handler.received) >= 1
        assert handler.received[0].key == "still-works"

    @pytest.mark.asyncio
    async def test_filter_exception_does_not_block(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """过滤回调异常时事件被跳过，不影响后续事件"""
        target_registry = create_event_registry(BASE_EVENT_DECLS)
        target_handlers = EventHandlerRegistry()
        spy = ForwardSpyHandler(["mw.ping"])
        target_handlers.register(spy)

        target_bus = EventBus(target_registry, target_handlers, max_queue_size=10)

        call_count = 0

        def faulty_filter(event: Event) -> bool:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("filter exploded")
            return True

        fw = EventForwardMiddleware(
            target=target_bus,
            source_name="filter-error",
            event_filter=faulty_filter,
        )
        chain = MiddlewareChain()
        chain.add(fw)

        source_bus = EventBus(
            base_event_registry,
            handler_registry,
            max_queue_size=10,
            middleware_chain=chain,
        )

        async with target_bus:
            async with source_bus:
                # 第一次：过滤回调抛异常 → 跳过
                await source_bus.proxy("src").publish(
                    "mw.ping", {"key": "first", "count": 1}
                )
                # 第二次：过滤回调正常 → 转发
                await source_bus.proxy("src").publish(
                    "mw.ping", {"key": "second", "count": 2}
                )
                await spy.wait_received(timeout=3.0)

        # 第二个事件被正常转发
        assert len(spy.received_payloads) >= 1
        assert isinstance(spy.received_payloads[0], MiddlewareTestPayload)
        assert spy.received_payloads[0].key == "second"
        assert call_count >= 2

    @pytest.mark.asyncio
    async def test_no_filter_forwards_all_non_system(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """无过滤器时转发所有非系统事件"""
        target_registry = create_event_registry(BASE_EVENT_DECLS)
        target_handlers = EventHandlerRegistry()
        spy = ForwardSpyHandler(["mw.ping", "user.login"])
        target_handlers.register(spy)

        target_bus = EventBus(target_registry, target_handlers, max_queue_size=10)

        fw = EventForwardMiddleware(target=target_bus, source_name="no-filter")
        chain = MiddlewareChain()
        chain.add(fw)

        source_bus = EventBus(
            base_event_registry,
            handler_registry,
            max_queue_size=10,
            middleware_chain=chain,
        )

        async with target_bus:
            async with source_bus:
                await source_bus.proxy("src").publish(
                    "mw.ping", {"key": "all", "count": 1}
                )
                await source_bus.proxy("src").publish("user.login", None)
                await asyncio.sleep(0.3)

        # 两个非系统事件都被转发
        assert len(spy.received_names) >= 2
        assert "mw.ping" in spy.received_names
        assert "user.login" in spy.received_names

    @pytest.mark.asyncio
    async def test_source_name_default(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """默认 source_name 为 'event_forward'"""
        target_registry = create_event_registry(BASE_EVENT_DECLS)
        target_handlers = EventHandlerRegistry()
        spy = ForwardSpyHandler(["mw.ping"])
        target_handlers.register(spy)

        target_bus = EventBus(target_registry, target_handlers, max_queue_size=10)

        # 不指定 source_name，使用默认值
        fw = EventForwardMiddleware(target=target_bus)
        chain = MiddlewareChain()
        chain.add(fw)

        source_bus = EventBus(
            base_event_registry,
            handler_registry,
            max_queue_size=10,
            middleware_chain=chain,
        )

        async with target_bus:
            async with source_bus:
                await source_bus.proxy("src").publish(
                    "mw.ping", {"key": "default", "count": 1}
                )
                await spy.wait_received(timeout=3.0)

        assert len(spy.received_sources) >= 1
        assert spy.received_sources[0] == "event_forward"
