import pytest
from typing import Any, Optional
from pydantic import BaseModel

from event_bus import Event, EventHandler, EventHandlerRegistry, EventBus, Regex


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
    assert handler_registry.get_handlers("sample.event") == [(hid, handler)]
    assert handler_registry.get_handlers("nonexistent.event") == []
    assert handler_registry.handlers_count == 1
    assert handler_registry.all_handlers == {hid: handler}
    assert handler_registry.get(hid) == handler

    assert handler_registry.unregister(hid) is True
    assert handler_registry.get(hid) is None
    assert handler_registry.get_handlers("sample.event") == []
    assert handler_registry.handlers_count == 0

    assert handler_registry.unregister("invalid_id") is False


@pytest.mark.asyncio
async def test_handler_pattern_matching(handler_registry: EventHandlerRegistry):
    """测试 Handler 的正则表达式订阅功能"""

    class PatternHandler(EventHandler):
        def __init__(self):
            super().__init__([Regex(r"user\..*")])

        async def handle(
            self,
            payload: Optional[BaseModel],
            bus_proxy: Any,
            raw_event: Event,
        ) -> None:
            pass

    handler = PatternHandler()
    hid = handler_registry.register(handler)

    assert handler_registry.get_handlers("user.login") == [(hid, handler)]
    assert handler_registry.get_handlers("user.logout") == [(hid, handler)]
    assert handler_registry.get_handlers("admin.login") == []

    # 精确匹配应不命中
    assert handler_registry.get_handlers(r"user\..*") == []


@pytest.mark.asyncio
async def test_handler_multiple_patterns(handler_registry: EventHandlerRegistry):
    """一个 Handler 可订阅多个模式（Regex 与 str 混合）"""

    class MultiHandler(EventHandler):
        def __init__(self):
            super().__init__([Regex(r"a\..*"), Regex(r"b\..*")])

        async def handle(self, payload: Optional[BaseModel], bus_proxy: 'EventBus.Proxy', raw_event: Event) -> None:
            pass

    handler = MultiHandler()
    handler_registry.register(handler)

    assert len(handler_registry.get_handlers("a.foo")) == 1
    assert len(handler_registry.get_handlers("b.bar")) == 1
    assert len(handler_registry.get_handlers("c.baz")) == 0


@pytest.mark.asyncio
async def test_handler_str_exact_and_regex_mixed(
    handler_registry: EventHandlerRegistry,
) -> None:
    """str 全字匹配 vs Regex 正则匹配：混合订阅应各自命中"""

    class MixedHandler(EventHandler):
        def __init__(self):
            super().__init__(["order.created", Regex(r"order\..*")])

        async def handle(self, payload: Optional[BaseModel], bus_proxy: 'EventBus.Proxy', raw_event: Event) -> None:
            pass

    handler_registry.register(MixedHandler())

    # str 全字匹配：只命中完全相等的
    assert len(handler_registry.get_handlers("order.created")) == 1
    # Regex 匹配：通配
    assert len(handler_registry.get_handlers("order.deleted")) == 1
    assert len(handler_registry.get_handlers("order.shipped")) == 1
    # 都不匹配
    assert len(handler_registry.get_handlers("user.login")) == 0


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
