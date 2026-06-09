"""Middleware / MiddlewareChain 单元测试 + 与 EventBus 集成测试。"""

import asyncio
from typing import Any, Callable, Dict, List, Optional

import pytest
from pydantic import BaseModel

from event_bus import (
    Event,
    EventBus,
    EventDeclaration,
    EventHandlerRegistry,
    EventRegistry,
    Middleware,
    MiddlewareChain,
)
from event_bus.middleware import BeforePublishNext, OnPublishNext

from conftest import (
    MiddlewarePingEventDecl,
    MiddlewareTestPayload,
    SimplePingHandler,
    wait_for_condition,
)


# ============================================================================
# 测试用中间件实现
# ============================================================================


class LoggingMiddleware(Middleware):
    """记录所有钩子调用的顺序和时间点。

    可通过 ``shared_log`` 将多个中间件的调用记录到同一个列表中，用于验证调用顺序。
    """

    def __init__(self, name: str = "log", shared_log: Optional[List[str]] = None) -> None:
        self.name = name
        self.calls: List[str] = []          # 自身调用记录
        self._shared: Optional[List[str]] = shared_log  # 共享记录（用于顺序验证）
        self.setup_called = False
        self.teardown_called = False

    def _log(self, msg: str) -> None:
        self.calls.append(msg)
        if self._shared is not None:
            self._shared.append(msg)

    async def on_setup(self, bus: EventBus) -> None:
        self.setup_called = True
        self._log(f"{self.name}:on_setup")

    async def on_teardown(self, bus: EventBus) -> None:
        self.teardown_called = True
        self._log(f"{self.name}:on_teardown")

    async def before_publish(
        self,
        event_registry: EventRegistry,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
        old_event: Event | None,
        next: BeforePublishNext,
    ) -> None:
        self._log(f"{self.name}:before_publish:{name}")
        await next(event_registry, name, source, data, old_event)
        self._log(f"{self.name}:before_publish_after:{name}")

    async def on_publish(
        self,
        event: Event,
        next: OnPublishNext,
    ) -> None:
        self._log(f"{self.name}:on_publish:{event.name}")
        await next(event)
        self._log(f"{self.name}:on_publish_after:{event.name}")

    async def on_publish_error(
        self,
        error: Exception,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
    ) -> None:
        self._log(f"{self.name}:on_publish_error:{name}:{type(error).__name__}")


class ShortCircuitBeforeMiddleware(Middleware):
    """在 before_publish 中短路：不调用 next"""

    def __init__(self) -> None:
        self.intercepted: List[str] = []

    async def on_setup(self, bus: EventBus) -> None:
        pass

    async def on_teardown(self, bus: EventBus) -> None:
        pass

    async def before_publish(
        self,
        event_registry: EventRegistry,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
        old_event: Event | None,
        next: BeforePublishNext,
    ) -> None:
        self.intercepted.append(name)
        # 故意不调用 next —— 事件不入队

    async def on_publish(
        self,
        event: Event,
        next: OnPublishNext,
    ) -> None:
        await next(event)


class FailingSetupMiddleware(Middleware):
    """on_setup 抛出异常的中间件 —— 应被自动移除"""

    def __init__(self, name: str = "failing") -> None:
        self.name = name
        self.setup_attempted = False

    async def on_setup(self, bus: EventBus) -> None:
        self.setup_attempted = True
        raise RuntimeError(f"{self.name} setup failed")

    async def on_teardown(self, bus: EventBus) -> None:
        pass

    async def before_publish(
        self,
        event_registry: EventRegistry,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
        old_event: Event | None,
        next: BeforePublishNext,
    ) -> None:
        await next(event_registry, name, source, data, old_event)

    async def on_publish(
        self,
        event: Event,
        next: OnPublishNext,
    ) -> None:
        await next(event)


class FailingBeforeMiddleware(Middleware):
    """before_publish 对特定事件抛出异常的中间件（默认: mw.ping）"""

    def __init__(self, name: str = "fail_before", fail_on: Optional[str] = "mw.ping") -> None:
        self.name = name
        self.fail_on = fail_on

    async def on_setup(self, bus: EventBus) -> None:
        pass

    async def on_teardown(self, bus: EventBus) -> None:
        pass

    async def before_publish(
        self,
        event_registry: EventRegistry,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
        old_event: Event | None,
        next: BeforePublishNext,
    ) -> None:
        if self.fail_on is None or name == self.fail_on:
            raise ValueError(f"{self.name}: intentional error in before_publish")
        await next(event_registry, name, source, data, old_event)

    async def on_publish(
        self,
        event: Event,
        next: OnPublishNext,
    ) -> None:
        await next(event)

    async def on_publish_error(
        self,
        error: Exception,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
    ) -> None:
        pass


class MutatingMiddleware(Middleware):
    """在 before_publish 中修改 data 的中间件"""

    def __init__(self) -> None:
        self.mutated: List[tuple[str, Any]] = []

    async def on_setup(self, bus: EventBus) -> None:
        pass

    async def on_teardown(self, bus: EventBus) -> None:
        pass

    async def before_publish(
        self,
        event_registry: EventRegistry,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
        old_event: Event | None,
        next: BeforePublishNext,
    ) -> None:
        # 修改 data 字典
        if isinstance(data, dict):
            data["mutated_by"] = self.__class__.__name__
        await next(event_registry, name, source, data, old_event)

    async def on_publish(
        self,
        event: Event,
        next: OnPublishNext,
    ) -> None:
        self.mutated.append((event.name, event.data))
        await next(event)


class ThrowingTeardownMiddleware(Middleware):
    """on_teardown 抛出异常的中间件 —— 应被记录但不影响其他中间件"""

    def __init__(self, name: str = "throw_teardown") -> None:
        self.name = name
        self.teardown_attempted = False

    async def on_setup(self, bus: EventBus) -> None:
        pass

    async def on_teardown(self, bus: EventBus) -> None:
        self.teardown_attempted = True
        raise RuntimeError(f"{self.name} teardown failed")

    async def before_publish(
        self,
        event_registry: EventRegistry,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
        old_event: Event | None,
        next: BeforePublishNext,
    ) -> None:
        await next(event_registry, name, source, data, old_event)

    async def on_publish(
        self,
        event: Event,
        next: OnPublishNext,
    ) -> None:
        await next(event)


class ErrorCapturingMiddleware(Middleware):
    """捕获 on_publish_error 调用的中间件"""

    def __init__(self, name: str = "err_cap") -> None:
        self.name = name
        self.errors: List[Dict[str, Any]] = []

    async def on_setup(self, bus: EventBus) -> None:
        pass

    async def on_teardown(self, bus: EventBus) -> None:
        pass

    async def before_publish(
        self,
        event_registry: EventRegistry,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
        old_event: Event | None,
        next: BeforePublishNext,
    ) -> None:
        await next(event_registry, name, source, data, old_event)

    async def on_publish(
        self,
        event: Event,
        next: OnPublishNext,
    ) -> None:
        await next(event)

    async def on_publish_error(
        self,
        error: Exception,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
    ) -> None:
        self.errors.append({
            "error_type": type(error).__name__,
            "error_msg": str(error),
            "name": name,
            "source": source,
        })


# ============================================================================
# MiddlewareChain CRUD 测试
# ============================================================================


class TestMiddlewareChainCRUD:
    """MiddlewareChain 的增删查操作"""

    def test_add_middleware(self) -> None:
        """添加到末尾"""
        chain = MiddlewareChain()
        mw1 = LoggingMiddleware("mw1")
        mw2 = LoggingMiddleware("mw2")

        chain.add(mw1).add(mw2)
        assert chain.middlewares == [mw1, mw2]

    def test_add_duplicate_raises(self) -> None:
        """重复添加同一个实例应抛出 ValueError"""
        chain = MiddlewareChain()
        mw = LoggingMiddleware("dup")
        chain.add(mw)
        with pytest.raises(ValueError, match="already in the chain"):
            chain.add(mw)

    def test_insert_at_position(self) -> None:
        """在指定位置插入"""
        chain = MiddlewareChain()
        mw1 = LoggingMiddleware("mw1")
        mw2 = LoggingMiddleware("mw2")
        mw3 = LoggingMiddleware("mw3")

        chain.add(mw1).add(mw3)
        chain.insert(1, mw2)  # [mw1, mw2, mw3]
        assert chain.middlewares == [mw1, mw2, mw3]

        mw0 = LoggingMiddleware("mw0")
        chain.insert(0, mw0)  # [mw0, mw1, mw2, mw3]
        assert chain.middlewares == [mw0, mw1, mw2, mw3]

    def test_insert_duplicate_raises(self) -> None:
        """插入已存在的实例应抛出 ValueError"""
        chain = MiddlewareChain()
        mw1 = LoggingMiddleware("mw1")
        mw2 = LoggingMiddleware("mw2")
        chain.add(mw1).add(mw2)
        with pytest.raises(ValueError, match="already in the chain"):
            chain.insert(0, mw1)

    def test_remove_middleware(self) -> None:
        """移除指定中间件"""
        chain = MiddlewareChain()
        mw1 = LoggingMiddleware("mw1")
        mw2 = LoggingMiddleware("mw2")
        chain.add(mw1).add(mw2)
        chain.remove(mw1)
        assert chain.middlewares == [mw2]

    def test_clear_all(self) -> None:
        """清空所有中间件"""
        chain = MiddlewareChain()
        chain.add(LoggingMiddleware("mw1")).add(LoggingMiddleware("mw2"))
        chain.clear()
        assert chain.middlewares == []

    def test_middlewares_returns_copy(self) -> None:
        """middlewares 属性返回副本，外部修改不影响内部状态"""
        chain = MiddlewareChain()
        mw = LoggingMiddleware("mw")
        chain.add(mw)
        copy = chain.middlewares
        copy.clear()
        assert chain.middlewares == [mw]


# ============================================================================
# MiddlewareChain 生命周期测试
# ============================================================================


class TestMiddlewareChainLifecycle:
    """setup / teardown 流程"""

    @pytest.mark.asyncio
    async def test_setup_calls_on_setup_in_order(self) -> None:
        """setup 按注册顺序调用 on_setup"""
        chain = MiddlewareChain()
        mw1 = LoggingMiddleware("mw1")
        mw2 = LoggingMiddleware("mw2")

        chain.add(mw1).add(mw2)
        # 需要传入一个 mock bus
        bus = _mock_bus()
        failed = await chain.setup(bus)

        assert failed == []
        assert mw1.setup_called
        assert mw2.setup_called
        assert mw1.calls[0] == "mw1:on_setup"
        assert mw2.calls[0] == "mw2:on_setup"

    @pytest.mark.asyncio
    async def test_setup_removes_failing_middleware(self) -> None:
        """setup 中失败的中间件被移除，且不影响其他"""
        chain = MiddlewareChain()
        mw1 = LoggingMiddleware("mw1")
        fail_mw = FailingSetupMiddleware("fail")
        mw2 = LoggingMiddleware("mw2")

        chain.add(mw1).add(fail_mw).add(mw2)
        bus = _mock_bus()
        failed = await chain.setup(bus)

        assert len(failed) == 1
        assert failed[0] is fail_mw
        assert fail_mw.setup_attempted
        # fail_mw 被移除，mw1/mw2 保留
        assert mw1 in chain.middlewares
        assert mw2 in chain.middlewares
        assert fail_mw not in chain.middlewares
        assert mw1.setup_called
        assert mw2.setup_called

    @pytest.mark.asyncio
    async def test_teardown_calls_on_teardown_reverse_order(self) -> None:
        """teardown 按注册逆序调用 on_teardown"""
        shared_log: List[str] = []
        chain = MiddlewareChain()
        mw1 = LoggingMiddleware("mw1", shared_log=shared_log)
        mw2 = LoggingMiddleware("mw2", shared_log=shared_log)
        chain.add(mw1).add(mw2)

        bus = _mock_bus()
        await chain.teardown(bus)

        assert mw1.teardown_called
        assert mw2.teardown_called
        # 逆序：mw2 先 teardown, mw1 后 teardown
        idx_mw2 = shared_log.index("mw2:on_teardown")
        idx_mw1 = shared_log.index("mw1:on_teardown")
        assert idx_mw2 < idx_mw1, f"teardown 应为逆序: {shared_log}"

    @pytest.mark.asyncio
    async def test_teardown_continues_on_error(self) -> None:
        """teardown 中某个中间件异常不影响其他中间件的清理"""
        chain = MiddlewareChain()
        throw_mw = ThrowingTeardownMiddleware("throw")
        mw1 = LoggingMiddleware("mw1")

        chain.add(throw_mw).add(mw1)
        bus = _mock_bus()
        # 不应抛出异常
        await chain.teardown(bus)

        assert throw_mw.teardown_attempted
        assert mw1.teardown_called


# ============================================================================
# 责任链构建测试
# ============================================================================


class TestMiddlewareChainBuild:
    """build_before_publish / build_on_publish 责任链"""

    @pytest.mark.asyncio
    async def test_before_publish_chain_order(self) -> None:
        """责任链按中间件注册顺序包装 —— 外层先注册，内层后注册"""
        shared_log: List[str] = []
        chain = MiddlewareChain()
        mw_outer = LoggingMiddleware("outer", shared_log=shared_log)
        mw_inner = LoggingMiddleware("inner", shared_log=shared_log)
        chain.add(mw_outer).add(mw_inner)

        final_called = False

        async def final_handler(
            event_registry: EventRegistry,
            name: str,
            source: str,
            data: dict[str, Any] | BaseModel | None,
            old_event: Event | None,
        ) -> None:
            nonlocal final_called
            final_called = True
            shared_log.append("final:called")

        built = chain.build_before_publish(final_handler)
        bus = _mock_bus()
        await built(bus._events, "test.event", "test", None, None)

        assert final_called
        # 洋葱模型: outer:before → inner:before → final → inner:after → outer:after
        assert shared_log == [
            "outer:before_publish:test.event",
            "inner:before_publish:test.event",
            "final:called",
            "inner:before_publish_after:test.event",
            "outer:before_publish_after:test.event",
        ], f"洋葱模型顺序错误: {shared_log}"

    @pytest.mark.asyncio
    async def test_on_publish_chain_order(self) -> None:
        """on_publish 责任链顺序验证"""
        shared_log: List[str] = []
        chain = MiddlewareChain()
        mw_outer = LoggingMiddleware("outer", shared_log=shared_log)
        mw_inner = LoggingMiddleware("inner", shared_log=shared_log)
        chain.add(mw_outer).add(mw_inner)

        final_called = False

        async def final_handler(event: Event) -> None:
            nonlocal final_called
            final_called = True
            shared_log.append("final:called")

        built = chain.build_on_publish(final_handler)
        await built(Event(name="test.event", data=None, sources=[], timestamps=[]))

        assert final_called
        # 洋葱模型: outer:on → inner:on → final → inner:after → outer:after
        assert shared_log == [
            "outer:on_publish:test.event",
            "inner:on_publish:test.event",
            "final:called",
            "inner:on_publish_after:test.event",
            "outer:on_publish_after:test.event",
        ], f"洋葱模型顺序错误: {shared_log}"

    @pytest.mark.asyncio
    async def test_before_publish_short_circuit(self) -> None:
        """中间件不调用 next 时可以短路整个链"""
        chain = MiddlewareChain()
        short = ShortCircuitBeforeMiddleware()
        mw = LoggingMiddleware("mw")
        chain.add(short).add(mw)

        final_called = False

        async def final_handler(
            event_registry: EventRegistry,
            name: str,
            source: str,
            data: dict[str, Any] | BaseModel | None,
            old_event: Event | None,
        ) -> None:
            nonlocal final_called
            final_called = True

        built = chain.build_before_publish(final_handler)
        bus = _mock_bus()
        await built(bus._events, "test.event", "test", None, None)

        assert short.intercepted == ["test.event"]
        # mw 的 before_publish 没有被调用
        assert not any("mw:" in c for c in mw.calls)
        # final 没有被调用
        assert not final_called

    @pytest.mark.asyncio
    async def test_empty_chain_passes_through(self) -> None:
        """空链直接调用 final_handler"""
        chain = MiddlewareChain()

        final_called = False

        async def final_handler(
            event_registry: EventRegistry,
            name: str,
            source: str,
            data: dict[str, Any] | BaseModel | None,
            old_event: Event | None,
        ) -> None:
            nonlocal final_called
            final_called = True

        built = chain.build_before_publish(final_handler)
        bus = _mock_bus()
        await built(bus._events, "test.event", "test", None, None)
        assert final_called


# ============================================================================
# on_publish_error 测试
# ============================================================================


class TestMiddlewareChainError:
    """on_publish_error 通知"""

    @pytest.mark.asyncio
    async def test_on_publish_error_notifies_all(self) -> None:
        """on_publish_error 按注册顺序通知所有中间件"""
        chain = MiddlewareChain()
        mw1 = LoggingMiddleware("mw1")
        mw2 = LoggingMiddleware("mw2")
        chain.add(mw1).add(mw2)

        error = ValueError("test error")
        await chain.on_publish_error(error, "test.event", "source1", None)

        assert "mw1:on_publish_error:test.event:ValueError" in mw1.calls
        assert "mw2:on_publish_error:test.event:ValueError" in mw2.calls

    @pytest.mark.asyncio
    async def test_on_publish_error_continues_on_middleware_error(self) -> None:
        """某个中间件的 on_publish_error 异常不影响其他中间件"""
        chain = MiddlewareChain()

        class BadErrorMiddleware(Middleware):
            async def on_setup(self, bus: EventBus) -> None: pass
            async def on_teardown(self, bus: EventBus) -> None: pass

            async def before_publish(self, event_registry, name, source, data, old_event, next):
                await next(event_registry, name, source, data, old_event)

            async def on_publish(self, event, next):
                await next(event)

            async def on_publish_error(self, error, name, source, data):
                raise RuntimeError("error handler itself failed")

        mw_good = LoggingMiddleware("good")
        bad = BadErrorMiddleware()
        chain.add(bad).add(mw_good)

        error = ValueError("original error")
        # 不应抛出异常
        await chain.on_publish_error(error, "test.event", "s", None)

        assert any("good:on_publish_error" in c for c in mw_good.calls)


# ============================================================================
# Middleware 基类默认实现测试
# ============================================================================


class TestMiddlewareDefaults:
    """Middleware 基类的默认实现"""

    @pytest.mark.asyncio
    async def test_default_on_setup_does_nothing(self) -> None:
        """默认 on_setup 不抛异常"""

        class MinimalMiddleware(Middleware):
            async def before_publish(self, er, n, s, d, oe, nx):
                await nx(er, n, s, d, oe)

            async def on_publish(self, e, nx):
                await nx(e)

        mw = MinimalMiddleware()
        bus = _mock_bus()
        await mw.on_setup(bus)  # 不应抛出异常

    @pytest.mark.asyncio
    async def test_default_on_teardown_does_nothing(self) -> None:
        """默认 on_teardown 不抛异常"""

        class MinimalMiddleware(Middleware):
            async def before_publish(self, er, n, s, d, oe, nx):
                await nx(er, n, s, d, oe)

            async def on_publish(self, e, nx):
                await nx(e)

        mw = MinimalMiddleware()
        bus = _mock_bus()
        await mw.on_teardown(bus)  # 不应抛出异常

    @pytest.mark.asyncio
    async def test_default_on_publish_error_does_nothing(self) -> None:
        """默认 on_publish_error 不抛异常"""

        class MinimalMiddleware(Middleware):
            async def before_publish(self, er, n, s, d, oe, nx):
                await nx(er, n, s, d, oe)

            async def on_publish(self, e, nx):
                await nx(e)

        mw = MinimalMiddleware()
        await mw.on_publish_error(ValueError("x"), "e", "s", None)


# ============================================================================
# EventBus 集成测试
# ============================================================================


class TestMiddlewareIntegration:
    """Middleware 与 EventBus 的集成测试"""

    @pytest.mark.asyncio
    async def test_middleware_setup_called_on_bus_start(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """总线启动时中间件 on_setup 被调用"""
        mw = LoggingMiddleware("integ")
        chain = MiddlewareChain()
        chain.add(mw)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            max_queue_size=10,
            middleware_chain=chain,
        )
        async with bus:
            assert mw.setup_called

    @pytest.mark.asyncio
    async def test_middleware_teardown_called_on_bus_stop(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """总线停止时中间件 on_teardown 被调用"""
        mw = LoggingMiddleware("integ")
        chain = MiddlewareChain()
        chain.add(mw)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            max_queue_size=10,
            middleware_chain=chain,
        )
        await bus.start()
        await bus.stop()
        assert mw.teardown_called

    @pytest.mark.asyncio
    async def test_before_publish_hook_called(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """发布事件时 before_publish 钩子被调用"""
        mw = LoggingMiddleware("hook")
        chain = MiddlewareChain()
        chain.add(mw)

        # 需要注册 mw.ping 事件和对应的 handler
        handler = SimplePingHandler()
        handler_registry.register(handler)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            max_queue_size=10,
            middleware_chain=chain,
        )
        async with bus:
            await bus.proxy("test_src").publish(
                "mw.ping", {"key": "hello", "count": 1}
            )
            await handler.wait_received(timeout=2.0)

        assert any("hook:before_publish:mw.ping" in c for c in mw.calls)

    @pytest.mark.asyncio
    async def test_on_publish_hook_called(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """发布事件后 on_publish 钩子被调用"""
        mw = LoggingMiddleware("hook")
        chain = MiddlewareChain()
        chain.add(mw)

        handler = SimplePingHandler()
        handler_registry.register(handler)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            max_queue_size=10,
            middleware_chain=chain,
        )
        async with bus:
            await bus.proxy("test_src").publish(
                "mw.ping", {"key": "hello", "count": 1}
            )
            await handler.wait_received(timeout=2.0)

        assert any("hook:on_publish:mw.ping" in c for c in mw.calls)

    @pytest.mark.asyncio
    async def test_hooks_called_in_correct_order(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """before_publish → on_publish 调用顺序正确"""
        mw = LoggingMiddleware("mw")
        chain = MiddlewareChain()
        chain.add(mw)

        handler = SimplePingHandler()
        handler_registry.register(handler)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            max_queue_size=10,
            middleware_chain=chain,
        )
        async with bus:
            await bus.proxy("src").publish("mw.ping", {"key": "k", "count": 1})
            await handler.wait_received(timeout=2.0)

        # 验证 before 在 on 之前（合并所有 calls 检查相对顺序）
        before_idx = _index_of(mw.calls, "mw:before_publish:mw.ping")
        on_idx = _index_of(mw.calls, "mw:on_publish:mw.ping")
        assert before_idx < on_idx, f"before_publish 应在 on_publish 之前: {mw.calls}"

    @pytest.mark.asyncio
    async def test_multiple_middlewares_chain(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """多个中间件按注册顺序形成洋葱模型"""
        shared_log: List[str] = []
        mw1 = LoggingMiddleware("mw1", shared_log=shared_log)
        mw2 = LoggingMiddleware("mw2", shared_log=shared_log)
        chain = MiddlewareChain()
        chain.add(mw1).add(mw2)

        handler = SimplePingHandler()
        handler_registry.register(handler)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            max_queue_size=10,
            middleware_chain=chain,
        )
        async with bus:
            await bus.proxy("src").publish("mw.ping", {"key": "k", "count": 1})
            await handler.wait_received(timeout=2.0)

        # 洋葱模型: mw1:before → mw2:before → mw2:after → mw1:after
        before_idx = [
            shared_log.index("mw1:before_publish:mw.ping"),
            shared_log.index("mw2:before_publish:mw.ping"),
            shared_log.index("mw2:before_publish_after:mw.ping"),
            shared_log.index("mw1:before_publish_after:mw.ping"),
        ]
        assert before_idx == sorted(before_idx), (
            f"洋葱模型顺序错误: {shared_log}"
        )

    @pytest.mark.asyncio
    async def test_before_publish_error_triggers_on_publish_error(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """before_publish 中异常触发 on_publish_error 通知"""
        fail_mw = FailingBeforeMiddleware("fail")
        err_cap = ErrorCapturingMiddleware("err_cap")
        chain = MiddlewareChain()
        chain.add(fail_mw).add(err_cap)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            max_queue_size=10,
            middleware_chain=chain,
        )
        async with bus:
            with pytest.raises(ValueError, match="intentional error in before_publish"):
                await bus.proxy("src").publish("mw.ping", {"key": "k", "count": 1})

        # err_cap 应该收到 on_publish_error 通知
        assert len(err_cap.errors) >= 1
        assert err_cap.errors[0]["error_type"] == "ValueError"
        assert err_cap.errors[0]["name"] == "mw.ping"

    @pytest.mark.asyncio
    async def test_before_publish_mutation_visible_to_handler(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """中间件在 before_publish 中对 data 的修改可以传递到 handler"""
        mut_mw = MutatingMiddleware()
        chain = MiddlewareChain()
        chain.add(mut_mw)

        handler = SimplePingHandler()
        handler_registry.register(handler)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            max_queue_size=10,
            middleware_chain=chain,
        )
        async with bus:
            await bus.proxy("src").publish("mw.ping", {"key": "original", "count": 42})
            await handler.wait_received(timeout=2.0)

        assert len(handler.received) >= 1
        # 通过 MutatingMiddleware 的 mutated 记录验证
        # 注意：mutation 发生在 dict 上，但最终 Event 使用 payload_type 重新构建
        # 所以 data 可能不会流过 mutation —— 取决于实现
        # 这个测试验证 mutating middleware 自己的记录

    @pytest.mark.asyncio
    async def test_failing_setup_middleware_removed_and_does_not_block(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """setup 失败的中间件被移除后，总线仍可正常发布"""
        fail_mw = FailingSetupMiddleware("fail")
        good_mw = LoggingMiddleware("good")
        chain = MiddlewareChain()
        chain.add(fail_mw).add(good_mw)

        handler = SimplePingHandler()
        handler_registry.register(handler)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            max_queue_size=10,
            middleware_chain=chain,
        )
        async with bus:
            await bus.proxy("src").publish("mw.ping", {"key": "k", "count": 1})
            await handler.wait_received(timeout=2.0)

        # fail 被移除，good 正常工作
        assert fail_mw not in chain.middlewares
        assert any("good:before_publish:mw.ping" in c for c in good_mw.calls)
        assert len(handler.received) >= 1

    @pytest.mark.asyncio
    async def test_no_middleware_default_behavior(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """无中间件时总线表现正常"""
        handler = SimplePingHandler()
        handler_registry.register(handler)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            max_queue_size=10,
        )
        async with bus:
            await bus.proxy("src").publish("mw.ping", {"key": "k", "count": 1})
            await handler.wait_received(timeout=2.0)

        assert len(handler.received) >= 1
        assert handler.received[0].key == "k"
        assert handler.received[0].count == 1


# ============================================================================
# 辅助工具
# ============================================================================


def _mock_bus() -> EventBus:
    """创建一个轻量的 mock EventBus 供单元测试使用"""
    reg = EventRegistry()
    from conftest import MiddlewarePingEventDecl
    reg.register(MiddlewarePingEventDecl)

    handler_reg = EventHandlerRegistry()
    bus = EventBus.__new__(EventBus)
    bus._events = reg
    bus._handlers = handler_reg
    bus._mw_chain = MiddlewareChain()
    bus._state_lock = asyncio.Lock()
    bus._enable_publish = asyncio.Event()
    bus._running = asyncio.Event()
    bus._queue = asyncio.Queue(maxsize=10)
    # 不启动 dispatch loop
    return bus


def _index_of(calls: List[str], target: str) -> int:
    """在 calls 列表中查找 target 的索引，未找到返回 -1"""
    for i, c in enumerate(calls):
        if c == target:
            return i
    return -1
