import pytest
from typing import Any, Optional
from pydantic import BaseModel

from event_bus import Event, EventHandler, EventHandlerRegistry, EventBus, Regex


# ============================================================================
# EventHandlerRegistry CRUD
# ============================================================================


@pytest.mark.asyncio
async def test_handler_register_crud(handler_registry: EventHandlerRegistry):
    """测试 Handler 注册表的增删查功能"""

    class SampleHandler(EventHandler):
        def __init__(self):
            super().__init__(["sample.event"])

        async def handle(
            self,
            payload: Optional[BaseModel],
            bus_proxy: Any,
            raw_event: Event,
        ) -> None:
            pass

    handler = SampleHandler()
    hid = handler_registry.register(handler)

    assert handler_registry.handlers_count == 1
    assert handler_registry.all_handlers == {hid: handler}
    assert handler_registry.get(hid) == handler
    assert hid in handler_registry

    assert handler_registry.unregister(hid) is True
    assert handler_registry.get(hid) is None
    assert hid not in handler_registry
    assert handler_registry.handlers_count == 0

    assert handler_registry.unregister("invalid_id") is False


@pytest.mark.asyncio
async def test_handler_subscriptions_copy():
    """subscriptions 应为独立副本，外部修改不影响 Handler"""

    subs: list[str | Regex] = ["a.event"]

    class MyHandler(EventHandler):
        def __init__(self):
            super().__init__(subs)

        async def handle(self, payload: Optional[BaseModel], bus_proxy: 'EventBus.Proxy', raw_event: Event) -> None:
            pass

    handler = MyHandler()
    subs.append("b.event")
    assert handler.subscriptions == ["a.event"]


# ============================================================================
# Regex 协议方法
# ============================================================================


def test_regex_str_and_repr():
    """Regex.__str__ 返回原始模式，__repr__ 返回可求值形式"""
    r = Regex(r"user\..*")
    assert str(r) == r"user\..*"
    assert repr(r) == f"Regex({r.pattern!r})"


def test_regex_eq():
    """Regex 可与 Regex 或 str 进行相等比较"""
    a = Regex(r"a\..*")
    b = Regex(r"a\..*")
    c = Regex(r"b\..*")

    assert a == b
    assert a != c
    assert a == r"a\..*"
    assert a != r"b\..*"
    assert a != 42  # 非 Regex/str 返回 NotImplemented


def test_regex_hash():
    """相同模式的 Regex 哈希值相同，可放入 set"""
    a = Regex(r"a\..*")
    b = Regex(r"a\..*")
    c = Regex(r"b\..*")

    assert hash(a) == hash(b)
    assert hash(a) != hash(c)

    s = {a, b, c}
    assert len(s) == 2


def test_regex_pattern_property():
    """pattern 属性返回原始正则字符串"""
    r = Regex(r"user\.\w+")
    assert r.pattern == r"user\.\w+"


def test_regex_fullmatch():
    """fullmatch 正确匹配完整字符串"""
    r = Regex(r"user\.\w+")
    assert r.fullmatch("user.login") is not None
    assert r.fullmatch("user.login.extra") is None
    assert r.fullmatch("admin.login") is None


# ============================================================================
# EventHandler 协议方法
# ============================================================================


@pytest.mark.asyncio
async def test_handler_call_delegates_to_handle(handler_registry: EventHandlerRegistry):
    """EventHandler.__call__ 应解包 event.data 并委托给 handle()"""
    received_payload: Optional[BaseModel] = None

    class CallTestHandler(EventHandler):
        def __init__(self):
            super().__init__(["test.call"])

        async def handle(self, payload: Optional[BaseModel], bus_proxy: 'EventBus.Proxy', raw_event: Event) -> None:
            nonlocal received_payload
            received_payload = payload

    handler = CallTestHandler()
    handler_registry.register(handler)

    # 直接调用 __call__，模拟总线分发
    from pydantic import BaseModel as PydanticBaseModel

    class DummyPayload(PydanticBaseModel):
        value: int

    payload = DummyPayload(value=42)
    event = Event(name="test.call", data=payload)
    # 需要一个 mock proxy
    from unittest.mock import MagicMock
    mock_proxy = MagicMock()
    await handler(mock_proxy, event)

    assert received_payload is not None
    assert received_payload.value == 42


# ============================================================================
# EventHandlerRegistry 协议方法
# ============================================================================


def test_registry_len(handler_registry: EventHandlerRegistry):
    """__len__ 返回已注册处理器数量"""
    assert len(handler_registry) == 0

    class LenHandler(EventHandler):
        def __init__(self):
            super().__init__(["len.event"])

        async def handle(self, payload: Optional[BaseModel], bus_proxy: 'EventBus.Proxy', raw_event: Event) -> None:
            pass

    handler_registry.register(LenHandler())
    assert len(handler_registry) == 1

    handler_registry.register(LenHandler())
    assert len(handler_registry) == 2


def test_registry_contains(handler_registry: EventHandlerRegistry):
    """__contains__ 检查 handler ID 是否已注册"""
    class ContainsHandler(EventHandler):
        def __init__(self):
            super().__init__(["contains.event"])

        async def handle(self, payload: Optional[BaseModel], bus_proxy: 'EventBus.Proxy', raw_event: Event) -> None:
            pass

    hid = handler_registry.register(ContainsHandler())
    assert hid in handler_registry
    assert "nonexistent" not in handler_registry


def test_registry_iter(handler_registry: EventHandlerRegistry):
    """__iter__ 迭代所有 (handler_id, handler) 对"""
    class IterHandler(EventHandler):
        def __init__(self):
            super().__init__(["iter.event"])

        async def handle(self, payload: Optional[BaseModel], bus_proxy: 'EventBus.Proxy', raw_event: Event) -> None:
            pass

    h1 = IterHandler()
    h2 = IterHandler()
    hid1 = handler_registry.register(h1)
    hid2 = handler_registry.register(h2)

    items = dict(handler_registry)
    assert items[hid1] is h1
    assert items[hid2] is h2
    assert len(items) == 2


def test_registry_clear(handler_registry: EventHandlerRegistry):
    """clear() 清除所有已注册处理器"""
    class ClearHandler(EventHandler):
        def __init__(self):
            super().__init__(["clear.event"])

        async def handle(self, payload: Optional[BaseModel], bus_proxy: 'EventBus.Proxy', raw_event: Event) -> None:
            pass

    handler_registry.register(ClearHandler())
    handler_registry.register(ClearHandler())
    assert len(handler_registry) == 2

    handler_registry.clear()
    assert len(handler_registry) == 0
    assert handler_registry.handlers_count == 0
