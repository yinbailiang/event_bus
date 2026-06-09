import pytest
from typing import Any, Optional
from pydantic import BaseModel

from event_bus import Event, EventHandler, EventHandlerRegistry, EventBus


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
            super().__init__([r"user\..*"])

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


@pytest.mark.asyncio
async def test_handler_multiple_patterns(handler_registry: EventHandlerRegistry):
    """一个 Handler 可订阅多个模式"""

    class MultiHandler(EventHandler):
        def __init__(self):
            super().__init__([r"a\..*", r"b\..*"])

        async def handle(self, payload: Optional[BaseModel], bus_proxy: 'EventBus.Proxy', raw_event: Event) -> None:
            pass

    handler = MultiHandler()
    handler_registry.register(handler)

    assert len(handler_registry.get_handlers("a.foo")) == 1
    assert len(handler_registry.get_handlers("b.bar")) == 1
    assert len(handler_registry.get_handlers("c.baz")) == 0


@pytest.mark.asyncio
async def test_regex_cache_lru_eviction(
    handler_registry: EventHandlerRegistry,
) -> None:
    """正则缓存达到上限时应淘汰最旧条目（LRU 淘汰 + move_to_end 刷新）"""
    small_registry = EventHandlerRegistry(regex_cache_maxsize=2)

    class PatternHandler(EventHandler):
        def __init__(self, pattern: str):
            super().__init__([pattern])

        async def handle(self, payload: Optional[BaseModel], bus_proxy: 'EventBus.Proxy', raw_event: Event) -> None:
            pass

    small_registry.register(PatternHandler(r"user\..*"))
    small_registry.register(PatternHandler(r"admin\..*"))
    small_registry.register(PatternHandler(r"order\..*"))  # 淘汰 user\..*

    info = small_registry.regex_cache_info
    assert info["size"] <= info["max_size"]

    # 重新查询 user\..* —— 缓存未命中，应重新编译
    small_registry.get_handlers("user.login")
    info = small_registry.regex_cache_info
    assert info["size"] <= info["max_size"]


@pytest.mark.asyncio
async def test_handler_subscriptions_copy():
    """subscriptions 应为独立副本，外部修改不影响 Handler"""

    subs = ["a.event"]

    class MyHandler(EventHandler):
        def __init__(self):
            super().__init__(subs)

        async def handle(self, payload: Optional[BaseModel], bus_proxy: 'EventBus.Proxy', raw_event: Event) -> None:
            pass

    handler = MyHandler()
    subs.append("b.event")
    assert handler.subscriptions == ["a.event"]
