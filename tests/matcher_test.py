from typing import Any, Optional

from pydantic import BaseModel

from event_bus import (
    Event,
    EventDeclaration,
    EventHandler,
    EventHandlerRegistry,
    EventRegistry,
    Matcher,
    Regex,
)

# ============================================================================
# 测试用事件声明
# ============================================================================


class _EventAlpha(EventDeclaration):
    name = 'test.alpha'


class _EventBeta(EventDeclaration):
    name = 'test.beta'


class _EventGamma(EventDeclaration):
    name = 'test.gamma'


# ============================================================================
# 测试用 Handler
# ============================================================================


class _SimpleHandler(EventHandler):
    def __init__(self, subscriptions: list[str | Regex]) -> None:
        super().__init__(subscriptions)

    async def handle(self, payload: Optional[BaseModel], bus_proxy: Any, raw_event: Event) -> None:
        pass


# ============================================================================
# 辅助
# ============================================================================


def _make_registry(*decls: type[EventDeclaration]) -> EventRegistry:
    reg = EventRegistry()
    for d in decls:
        reg.register(d)
    return reg


# ============================================================================
# 预计算分派表
# ============================================================================


class TestPrecomputedDispatchTable:
    """构造 Matcher 时应对已注册事件预计算分派表"""

    def test_known_events_hit_precomputed_table(self) -> None:
        """已注册事件应在分派表中命中，无需动态匹配"""
        events = _make_registry(_EventAlpha, _EventBeta)
        handlers = EventHandlerRegistry()
        h = _SimpleHandler(['test.alpha'])
        hid = handlers.register(h)

        m = Matcher(events, handlers)

        # 命中预计算表
        assert m.match('test.alpha') == [(hid, h)]
        assert m.match('test.beta') == []

        # dispatch_table 属性暴露预计算结果
        table = m.dispatch_table
        assert table['test.alpha'] == [(hid, h)]
        assert table['test.beta'] == []

    def test_empty_event_registry_produces_empty_table(self) -> None:
        """无已注册事件时分派表为空"""
        events = EventRegistry()
        handlers = EventHandlerRegistry()
        handlers.register(_SimpleHandler(['test.alpha']))

        m = Matcher(events, handlers)
        assert m.dispatch_table == {}

    def test_empty_handler_registry_known_events_match_nothing(self) -> None:
        """有事件但无处理器时，分派表每个事件映射为空列表"""
        events = _make_registry(_EventAlpha, _EventBeta)
        handlers = EventHandlerRegistry()

        m = Matcher(events, handlers)
        table = m.dispatch_table
        assert table['test.alpha'] == []
        assert table['test.beta'] == []


# ============================================================================
# 版本感知自动重建
# ============================================================================


class TestVersionAwareRebuild:
    """注册表版本号变更时 Matcher 应自动重建分派表"""

    def test_new_handler_triggers_rebuild(self) -> None:
        """注册新处理器后版本号递增，Matcher 自动感知并重建"""
        events = _make_registry(_EventAlpha)
        handlers = EventHandlerRegistry()

        m = Matcher(events, handlers)
        assert m.match('test.alpha') == []

        # 注册处理器
        h = _SimpleHandler(['test.alpha'])
        hid = handlers.register(h)

        # 自动感知版本变化，分派表更新
        assert m.match('test.alpha') == [(hid, h)]

    def test_unregister_handler_triggers_rebuild(self) -> None:
        """注销处理器后版本号递增，Matcher 自动更新"""
        events = _make_registry(_EventAlpha)
        handlers = EventHandlerRegistry()
        h = _SimpleHandler(['test.alpha'])
        hid = handlers.register(h)

        m = Matcher(events, handlers)
        assert m.match('test.alpha') == [(hid, h)]

        handlers.unregister(hid)
        assert m.match('test.alpha') == []

    def test_clear_handlers_triggers_rebuild(self) -> None:
        """清空处理器后版本号递增，Matcher 自动更新"""
        events = _make_registry(_EventAlpha)
        handlers = EventHandlerRegistry()
        handlers.register(_SimpleHandler(['test.alpha']))

        m = Matcher(events, handlers)
        assert len(m.match('test.alpha')) == 1

        handlers.clear()
        assert m.match('test.alpha') == []

    def test_new_event_triggers_rebuild(self) -> None:
        """注册新事件声明后版本号递增，Matcher 自动重建"""
        events = EventRegistry()
        handlers = EventHandlerRegistry()
        h = _SimpleHandler(['test.alpha'])
        hid = handlers.register(h)

        m = Matcher(events, handlers)

        # 预计算表为空
        assert m.dispatch_table == {}

        # 注册事件 → 版本变更 → 重建
        events.register(_EventAlpha)

        # 现在预计算表应包含 test.alpha
        assert m.match('test.alpha') == [(hid, h)]
        assert 'test.alpha' in m.dispatch_table

    def test_unregister_event_triggers_rebuild(self) -> None:
        """注销事件后版本号递增，Matcher 自动更新"""
        events = _make_registry(_EventAlpha)
        handlers = EventHandlerRegistry()
        h = _SimpleHandler(['test.alpha'])
        handlers.register(h)

        m = Matcher(events, handlers)
        assert 'test.alpha' in m.dispatch_table

        events.unregister('test.alpha')
        # 注销事件后分派表不再包含该 key
        assert 'test.alpha' not in m.dispatch_table

    def test_both_registries_change_simultaneously(self) -> None:
        """两个注册表同时变更，只重建一次"""
        events = _make_registry(_EventAlpha, _EventBeta)
        handlers = EventHandlerRegistry()

        m = Matcher(events, handlers)
        assert m.match('test.alpha') == []
        assert m.match('test.beta') == []

        # 同时注册处理器和新事件
        h = _SimpleHandler(['test.alpha', 'test.beta'])
        handlers.register(h)
        events.register(_EventGamma)

        assert len(m.match('test.alpha')) == 1
        assert len(m.match('test.beta')) == 1
        # test.gamma 在分派表中出现
        assert 'test.gamma' in m.dispatch_table


# ============================================================================
# 订阅模式匹配
# ============================================================================


class TestSubscriptionPatterns:
    """精确匹配与正则匹配"""

    def test_exact_string_match(self) -> None:
        """str 订阅仅匹配完全相等的 event_type"""
        events = _make_registry(_EventAlpha, _EventBeta)
        handlers = EventHandlerRegistry()
        h = _SimpleHandler(['test.alpha'])
        hid = handlers.register(h)

        m = Matcher(events, handlers)
        assert m.match('test.alpha') == [(hid, h)]
        assert m.match('test.beta') == []
        assert m.match('test.') == []

    def test_regex_match(self) -> None:
        """Regex 订阅通过 fullmatch 匹配"""
        events = _make_registry(_EventAlpha, _EventBeta, _EventGamma)
        handlers = EventHandlerRegistry()
        h = _SimpleHandler([Regex(r'test\..*')])
        hid = handlers.register(h)

        m = Matcher(events, handlers)
        assert m.match('test.alpha') == [(hid, h)]
        assert m.match('test.beta') == [(hid, h)]
        assert m.match('test.gamma') == [(hid, h)]

    def test_regex_fullmatch_not_partial(self) -> None:
        """Regex.fullmatch 要求完整匹配，不支持部分匹配"""
        events = _make_registry(_EventAlpha)
        handlers = EventHandlerRegistry()
        h = _SimpleHandler([Regex(r'test\.alpha')])
        hid = handlers.register(h)

        m = Matcher(events, handlers)
        assert m.match('test.alpha') == [(hid, h)]
        # 前缀不匹配
        assert m.match('test.alpha.extra') == []

    def test_mixed_str_and_regex(self) -> None:
        """同一 Handler 混合 str 和 Regex 订阅"""
        events = _make_registry(_EventAlpha, _EventBeta, _EventGamma)
        handlers = EventHandlerRegistry()
        h = _SimpleHandler(['test.alpha', Regex(r'test\.beta')])
        hid = handlers.register(h)

        m = Matcher(events, handlers)
        assert m.match('test.alpha') == [(hid, h)]
        assert m.match('test.beta') == [(hid, h)]
        # test.gamma 不匹配 str 也不匹配 regex
        assert m.match('test.gamma') == []

    def test_multiple_handlers_same_event(self) -> None:
        """多个处理器匹配同一事件时全部返回"""
        events = _make_registry(_EventAlpha)
        handlers = EventHandlerRegistry()
        h1 = _SimpleHandler(['test.alpha'])
        h2 = _SimpleHandler([Regex(r'test\..*')])
        hid1 = handlers.register(h1)
        hid2 = handlers.register(h2)

        m = Matcher(events, handlers)
        result = m.match('test.alpha')
        assert len(result) == 2
        assert (hid1, h1) in result
        assert (hid2, h2) in result

    def test_handler_matches_only_first_subscription(self) -> None:
        """同一 handler 匹配多个订阅时仅出现一次（break 去重）"""
        events = _make_registry(_EventAlpha)
        handlers = EventHandlerRegistry()
        # 两个订阅都匹配 test.alpha
        h = _SimpleHandler(['test.alpha', Regex(r'test\..*')])
        hid = handlers.register(h)

        m = Matcher(events, handlers)
        result = m.match('test.alpha')
        assert result == [(hid, h)]  # 仅出现一次


# ============================================================================
# dispatch_table 属性
# ============================================================================


class TestDispatchTableProperty:
    """dispatch_table 返回分派表的只读副本"""

    def test_returns_copy_not_reference(self) -> None:
        """修改返回值不影响内部状态"""
        events = _make_registry(_EventAlpha)
        handlers = EventHandlerRegistry()
        m = Matcher(events, handlers)

        table = m.dispatch_table
        table['intruded'] = []

        assert 'intruded' not in m.dispatch_table

    def test_reflects_rebuild(self) -> None:
        """版本变更后 dispatch_table 反映最新状态"""
        events = _make_registry(_EventAlpha)
        handlers = EventHandlerRegistry()
        m = Matcher(events, handlers)

        assert m.dispatch_table['test.alpha'] == []

        handlers.register(_SimpleHandler(['test.alpha']))
        assert len(m.dispatch_table['test.alpha']) == 1


# ============================================================================
# 边界情况
# ============================================================================


class TestEdgeCases:
    """边界与异常情况"""

    def test_match_with_empty_subscriptions(self) -> None:
        """处理器无订阅时不匹配任何事件"""
        events = _make_registry(_EventAlpha)
        handlers = EventHandlerRegistry()
        h = _SimpleHandler([])
        handlers.register(h)

        m = Matcher(events, handlers)
        assert m.match('test.alpha') == []

    def test_match_nonexistent_event_with_no_handlers(self) -> None:
        """无事件、无处理器时 match 返回空"""
        m = Matcher(EventRegistry(), EventHandlerRegistry())
        assert m.match('anything') == []

    def test_version_stable_no_rebuild_on_match(self) -> None:
        """版本未变时多次 match 不触发重建"""
        events = _make_registry(_EventAlpha)
        handlers = EventHandlerRegistry()
        handlers.register(_SimpleHandler(['test.alpha']))

        m = Matcher(events, handlers)
        v_before = events.version
        hv_before = handlers.version

        # 多次 match 后版本号不变
        m.match('test.alpha')
        m.match('test.alpha')
        m.match('test.alpha')

        assert events.version == v_before
        assert handlers.version == hv_before
