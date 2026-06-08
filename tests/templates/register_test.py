"""ModuleEventRegister 和 ModuleHandlerRegister 的单元测试"""

import pytest
from typing import Any, Dict, Optional, cast
from pydantic import BaseModel

from event_bus import (
    EventDeclaration,
    EventHandler,
    EventHandlerRegistry,
    EventRegistry
)
from event_bus.templates.register import ModuleEventRegister, ModuleHandlerRegister


# ============================================================================
# 测试用事件声明
# ============================================================================
class _TestPayload(BaseModel):
    value: int


class _EventA(EventDeclaration):
    name = "test.module.a"
    payload_type = _TestPayload


class _EventB(EventDeclaration):
    name = "test.module.b"
    payload_type = None


class _EventC(EventDeclaration):
    name = "test.module.c"
    payload_type = _TestPayload


# ============================================================================
# 测试用 Handler
# ============================================================================
class _SimpleHandler(EventHandler):
    def __init__(self, extra: str = "default"):
        super().__init__(subscriptions=["test.event"])
        self.extra = extra

    async def handle(self, payload, bus_proxy, raw_event) -> None: # type: ignore
        pass


class _AnotherHandler(EventHandler):
    def __init__(self, db: Optional[Any] = None):
        super().__init__(subscriptions=["test.another"])
        self.db = db

    async def handle(self, payload, bus_proxy, raw_event) -> None: # type: ignore
        pass


# ============================================================================
# Fixtures
# ============================================================================
@pytest.fixture
def empty_event_registry() -> EventRegistry:
    return EventRegistry()


@pytest.fixture
def empty_handler_registry() -> EventHandlerRegistry:
    return EventHandlerRegistry()


@pytest.fixture
def module_events() -> ModuleEventRegister:
    return ModuleEventRegister("test_module")


@pytest.fixture
def module_handlers() -> ModuleHandlerRegister:
    return ModuleHandlerRegister("test_module")


# ============================================================================
# ModuleEventRegister 测试
# ============================================================================
class TestModuleEventRegister:
    """测试模块事件注册器"""

    # ---- 初始化 ----
    def test_init_creates_empty_set(self) -> None:
        reg = ModuleEventRegister("my_mod")
        assert reg.name == "my_mod"
        assert reg.events == set()

    # ---- add_event ----
    def test_add_event_adds_to_set(self, module_events: ModuleEventRegister) -> None:
        module_events.add_event(_EventA)
        assert _EventA in module_events.events

    def test_add_event_idempotent(self, module_events: ModuleEventRegister) -> None:
        """重复添加同一事件声明不会产生重复项"""
        module_events.add_event(_EventA)
        module_events.add_event(_EventA)
        assert len(module_events.events) == 1

    # ---- event 装饰器 ----
    def test_event_decorator_registers_class(
        self, module_events: ModuleEventRegister
    ) -> None:
        @module_events.event
        class _DecoratedEvent(EventDeclaration):
            name = "test.decorated"
            payload_type = None

        assert _DecoratedEvent in module_events.events

    def test_event_decorator_returns_class_unchanged(
        self, module_events: ModuleEventRegister
    ) -> None:
        """装饰器应原样返回类（不改变类型）"""
        result = module_events.event(_EventA)
        assert result is _EventA

    # ---- register_all_events ----
    def test_register_all_events_populates_registry(
        self,
        module_events: ModuleEventRegister,
        empty_event_registry: EventRegistry,
    ) -> None:
        module_events.add_event(_EventA)
        module_events.add_event(_EventB)

        module_events.register_all_events(empty_event_registry)

        assert empty_event_registry.get("test.module.a") is _EventA
        assert empty_event_registry.get("test.module.b") is _EventB

    def test_register_all_events_duplicate_name_raises(
        self,
        module_events: ModuleEventRegister,
        empty_event_registry: EventRegistry,
    ) -> None:
        """当两个事件声明同名时，第二次 register 应抛出 ValueError"""
        # 手动注册一个同名事件
        empty_event_registry.register(_EventA)
        module_events.add_event(_EventA)

        with pytest.raises(ValueError, match="重复的事件声明"):
            module_events.register_all_events(empty_event_registry)

    def test_register_all_events_empty_noop(
        self,
        module_events: ModuleEventRegister,
        empty_event_registry: EventRegistry,
    ) -> None:
        """空注册器调用 register_all_events 不应报错"""
        module_events.register_all_events(empty_event_registry)
        assert empty_event_registry.list_names() == []

    # ---- get_all_event_names ----
    def test_get_all_event_names_returns_names(
        self, module_events: ModuleEventRegister
    ) -> None:
        module_events.add_event(_EventA)
        module_events.add_event(_EventB)
        module_events.add_event(_EventC)

        names = module_events.get_all_event_names()
        assert set(names) == {"test.module.a", "test.module.b", "test.module.c"}

    def test_get_all_event_names_empty(self, module_events: ModuleEventRegister) -> None:
        assert module_events.get_all_event_names() == []

    # ---- __repr__ ----
    def test_repr(self, module_events: ModuleEventRegister) -> None:
        module_events.add_event(_EventA)
        r = repr(module_events)
        assert "test_module" in r
        assert "1" in r

    def test_repr_empty(self, module_events: ModuleEventRegister) -> None:
        r = repr(module_events)
        assert "test_module" in r
        assert "0" in r


# ============================================================================
# ModuleHandlerRegister 测试
# ============================================================================
class TestModuleHandlerRegister:
    """测试模块处理器注册器"""

    # ---- 初始化 ----
    def test_init_creates_empty_set(self) -> None:
        reg = ModuleHandlerRegister("my_mod")
        assert reg.name == "my_mod"
        assert reg.handlers == set()

    # ---- add_handler ----
    def test_add_handler_adds_to_set(
        self, module_handlers: ModuleHandlerRegister
    ) -> None:
        module_handlers.add_handler(_SimpleHandler, depends=lambda: {"extra": "hi"})
        assert len(module_handlers.handlers) == 1

    def test_add_handler_same_entry_idempotent(
        self, module_handlers: ModuleHandlerRegister
    ) -> None:
        """同一个 (handler_type, depends) 重复添加不应产生重复"""
        dep = lambda: {"extra": "hi"}
        module_handlers.add_handler(_SimpleHandler, depends=dep)
        module_handlers.add_handler(_SimpleHandler, depends=dep)
        assert len(module_handlers.handlers) == 1

    def test_add_handler_different_depends_not_deduped(
        self, module_handlers: ModuleHandlerRegister
    ) -> None:
        """不同 depends 的同类型 handler 应视为不同注册项"""
        module_handlers.add_handler(_SimpleHandler, depends=lambda: {"extra": "a"})
        module_handlers.add_handler(_SimpleHandler, depends=lambda: {"extra": "b"})
        assert len(module_handlers.handlers) == 2

    # ---- handler 装饰器 ----
    def test_handler_decorator_registers_class(
        self, module_handlers: ModuleHandlerRegister
    ) -> None:
        @module_handlers.handler()
        class _DecoratedHandler(EventHandler): # type: ignore
            def __init__(self):
                super().__init__(subscriptions=["test.x"])

            async def handle(self, payload, bus_proxy, raw_event): # type: ignore
                pass

        assert len(module_handlers.handlers) == 1

    def test_handler_decorator_returns_class_unchanged(
        self, module_handlers: ModuleHandlerRegister
    ) -> None:
        """装饰器应原样返回类"""
        decorator = module_handlers.handler()
        result = decorator(_SimpleHandler)
        assert result is _SimpleHandler

    def test_handler_decorator_with_custom_depends(
        self, module_handlers: ModuleHandlerRegister
    ) -> None:
        """装饰器接受 depends 参数并存储"""
        factory = lambda: {"extra": "injected"}
        decorator = module_handlers.handler(depends=factory)
        decorator(_SimpleHandler)

        # 验证存储了正确的 depends
        entry = next(iter(module_handlers.handlers))
        assert entry[0] is _SimpleHandler
        assert entry[1] is factory

    # ---- register_all_handlers ----
    def test_register_all_handlers_instantiates_and_registers(
        self,
        module_handlers: ModuleHandlerRegister,
        empty_handler_registry: EventHandlerRegistry,
    ) -> None:
        module_handlers.add_handler(_SimpleHandler, depends=lambda: {"extra": "test"})
        module_handlers.register_all_handlers(empty_handler_registry)

        assert empty_handler_registry.handlers_count == 1
        handlers = empty_handler_registry.get_handlers("test.event")
        assert len(handlers) == 1
        assert isinstance(handlers[0][1], _SimpleHandler)
        assert handlers[0][1].extra == "test"

    def test_register_all_handlers_passes_dependencies(
        self,
        module_handlers: ModuleHandlerRegister,
        empty_handler_registry: EventHandlerRegistry,
    ) -> None:
        """验证依赖工厂的返回值正确传递给处理器构造器"""
        fake_db = object()
        module_handlers.add_handler(
            _AnotherHandler, depends=lambda: {"db": fake_db}
        )
        module_handlers.register_all_handlers(empty_handler_registry)

        handlers = empty_handler_registry.get_handlers("test.another")
        assert cast(_AnotherHandler, handlers[0][1]).db is fake_db

    def test_register_all_handlers_multiple_handlers(
        self,
        module_handlers: ModuleHandlerRegister,
        empty_handler_registry: EventHandlerRegistry,
    ) -> None:
        """注册多个处理器，全部应出现在注册表中"""
        module_handlers.add_handler(_SimpleHandler, depends=lambda: {"extra": "a"})
        module_handlers.add_handler(_AnotherHandler, depends=lambda: {"db": None})

        module_handlers.register_all_handlers(empty_handler_registry)

        assert empty_handler_registry.handlers_count == 2
        assert len(empty_handler_registry.get_handlers("test.event")) == 1
        assert len(empty_handler_registry.get_handlers("test.another")) == 1

    def test_register_all_handlers_empty_noop(
        self,
        module_handlers: ModuleHandlerRegister,
        empty_handler_registry: EventHandlerRegistry,
    ) -> None:
        """空注册器调用 register_all_handlers 不应报错"""
        module_handlers.register_all_handlers(empty_handler_registry)
        assert empty_handler_registry.handlers_count == 0

    def test_register_all_handlers_default_depends_empty_dict(
        self,
        empty_handler_registry: EventHandlerRegistry,
    ) -> None:
        """默认 depends (lambda: {}) 时处理器应使用默认参数构造"""
        reg = ModuleHandlerRegister("test")
        reg.add_handler(_SimpleHandler, depends=lambda: {})
        reg.register_all_handlers(empty_handler_registry)

        handlers = empty_handler_registry.get_handlers("test.event")
        assert cast(_SimpleHandler, handlers[0][1]).extra == "default"

    def test_register_all_handlers_depends_factory_called_per_registration(
        self,
        empty_handler_registry: EventHandlerRegistry,
    ) -> None:
        """每次 register_all_handlers 调用时 depends 工厂被重新调用"""
        call_count = 0

        def counting_factory() -> Dict[str, Any]:
            nonlocal call_count
            call_count += 1
            return {"extra": str(call_count)}

        reg = ModuleHandlerRegister("test")
        reg.add_handler(_SimpleHandler, depends=counting_factory)

        # 两次调用 register_all_handlers（第二次注册到新 registry）
        reg.register_all_handlers(empty_handler_registry)
        reg2 = EventHandlerRegistry()
        reg.register_all_handlers(reg2)

        assert call_count == 2
        # 第二次调用时 extra 应为 "2"
        handlers = reg2.get_handlers("test.event")
        assert cast(_SimpleHandler, handlers[0][1]).extra == "2"

    # ---- __repr__ ----
    def test_repr(self, module_handlers: ModuleHandlerRegister) -> None:
        module_handlers.add_handler(_SimpleHandler, depends=lambda: {})
        r = repr(module_handlers)
        assert "test_module" in r
        assert "1" in r

    def test_repr_empty(self, module_handlers: ModuleHandlerRegister) -> None:
        r = repr(module_handlers)
        assert "test_module" in r
        assert "0" in r
