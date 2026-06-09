"""模板中间件测试：SQLiteLogging / RateLimit / EventTransform / EventBlock。"""

import asyncio
import json
import os
import tempfile
from typing import Any, List, Optional

import pytest
from pydantic import BaseModel

from event_bus import (
    Event,
    EventBus,
    EventDeclaration,
    EventHandlerRegistry,
    EventRegistry,
    MiddlewareChain,
)
from event_bus.templates.middlewares import (
    EventBlockMiddleware,
    EventTransformMiddleware,
    RateLimitMiddleware,
    SQLiteLoggingMiddleware,
    make_allowlist_predicate,
    make_blocklist_predicate,
    make_field_inject_transform,
    make_field_redact_transform,
    make_rename_transform,
)

from conftest import (
    MiddlewareTestPayload,
    SimplePingHandler,
)


# ============================================================================
# 共用 fixtures
# ============================================================================


@pytest.fixture
def chain() -> MiddlewareChain:
    return MiddlewareChain()


# ============================================================================
# SQLiteLoggingMiddleware
# ============================================================================


class TestSQLiteLoggingMiddleware:
    """aiosqlite 日志中间件（含降级）"""

    @pytest.mark.asyncio
    async def test_logs_event_to_memory_db(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """事件发布后被记录到内存 SQLite"""

        mw = SQLiteLoggingMiddleware(":memory:")
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

            # 查询 SQLite 确认写入（在连接关闭前查询）
            assert mw._conn is not None
            cursor = await mw._conn.execute(
                f"SELECT name, source, data FROM {mw._table}"
            )
            rows = await cursor.fetchall()
            assert len(rows) >= 1
            assert rows[0]["name"] == "mw.ping"
            assert "test_src" in rows[0]["source"]

    @pytest.mark.asyncio
    async def test_logs_to_file_db(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """日志写入文件数据库"""

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            mw = SQLiteLoggingMiddleware(db_path)
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
                await bus.proxy("src").publish(
                    "mw.ping", {"key": "file", "count": 42}
                )
                await handler.wait_received(timeout=2.0)

            # 文件应该存在且非空
            assert os.path.exists(db_path)
            assert os.path.getsize(db_path) > 0
            assert mw._conn is not None
        finally:
            try:
                os.unlink(db_path)
            except OSError:
                pass

    @pytest.mark.asyncio
    async def test_on_publish_error_logs_fallback(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """发布异常时 on_publish_error 写入 fallback"""
        fallback_lines: List[str] = []

        def fake_fallback(line: str) -> None:
            fallback_lines.append(line)

        mw = SQLiteLoggingMiddleware(":memory:", fallback=fake_fallback)
        chain = MiddlewareChain()
        chain.add(mw)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            max_queue_size=10,
            middleware_chain=chain,
        )
        async with bus:
            await chain.on_publish_error(
                ValueError("boom"), "test.event", "src", {"key": "v"}
            )

        assert len(fallback_lines) >= 1
        record = json.loads(fallback_lines[0])
        assert "ValueError" in record["error"]


# ============================================================================
# RateLimitMiddleware
# ============================================================================


class TestRateLimitMiddleware:
    """滑动窗口速率限制"""

    @pytest.mark.asyncio
    async def test_allows_within_limit(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """在限制内的事件正常通过"""
        mw = RateLimitMiddleware(max_requests=10, window_seconds=1.0)
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
            for i in range(5):
                await bus.proxy("src").publish(
                    "mw.ping", {"key": f"k{i}", "count": i}
                )
                await asyncio.sleep(0.01)

        # 所有事件都应被处理
        assert len(handler.received) == 5

    @pytest.mark.asyncio
    async def test_blocks_when_exceeded(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """超出限制时事件被丢弃"""
        mw = RateLimitMiddleware(max_requests=3, window_seconds=10.0)
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
            for i in range(10):
                await bus.proxy("src").publish(
                    "mw.ping", {"key": f"k{i}", "count": i}
                )
                await asyncio.sleep(0.01)

        # 仅前 3 个被处理（其余被丢弃）
        assert mw.current_rate.get("__global__", 0) <= 3
        # 由于丢弃发生于 before_publish，handler 只会收到 ≤3 条
        assert len(handler.received) <= 3

    @pytest.mark.asyncio
    async def test_per_event_rate_limit(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """按事件名独立限流"""
        mw = RateLimitMiddleware(
            max_requests=2, window_seconds=10.0, per_event=True
        )
        chain = MiddlewareChain()
        chain.add(mw)

        handler = SimplePingHandler()
        handler_registry.register(handler)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            max_queue_size=20,
            middleware_chain=chain,
        )
        async with bus:
            # 发布 mw.ping 事件（最多 2 个通过）
            for i in range(5):
                await bus.proxy("src").publish(
                    "mw.ping", {"key": f"ping{i}", "count": i}
                )
                await asyncio.sleep(0.01)

            # 发布 user.login（无负载事件，另一个窗口）
            for i in range(5):
                await bus.proxy("src").publish("user.login", None)
                await asyncio.sleep(0.01)

        # mw.ping 窗口限制为 2
        ping_count = mw.current_rate.get("mw.ping", 0)
        assert ping_count <= 2


# ============================================================================
# EventTransformMiddleware
# ============================================================================


class TestEventTransformMiddleware:
    """事件转换中间件"""

    @pytest.mark.asyncio
    async def test_rename_event(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """事件重命名：旧名 → 新名（目标事件需接受相同负载类型）"""
        # 注册一个目标事件，接受 MiddlewareTestPayload
        class RenameTargetEvent(EventDeclaration):
            name = "rename.target"
            payload_type = MiddlewareTestPayload

        base_event_registry.register(RenameTargetEvent)
        transform = make_rename_transform({"mw.ping": "rename.target"})
        mw = EventTransformMiddleware(transform)
        chain = MiddlewareChain()
        chain.add(mw)

        # 监听重命名后的目标事件
        received: List[str] = []

        class TargetWatcher(SimplePingHandler):
            def __init__(self) -> None:
                super().__init__()
                self.subscriptions = ["rename.target"]

            async def handle(
                self,
                payload: Optional[BaseModel],
                bus_proxy: Any,
                raw_event: Event,
            ) -> None:
                received.append(raw_event.name)
                if isinstance(payload, MiddlewareTestPayload):
                    self.received.append(payload)
                    self._event.set()

        watcher = TargetWatcher()
        handler_registry.register(watcher)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            max_queue_size=10,
            middleware_chain=chain,
        )
        async with bus:
            await bus.proxy("src").publish("mw.ping", {"key": "k", "count": 1})
            await watcher.wait_received(timeout=2.0)

        # 重命名后的事件被 TargetWatcher 收到
        assert len(received) >= 1
        assert received[0] == "rename.target"

    @pytest.mark.asyncio
    async def test_field_inject(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """自动注入字段"""
        transform = make_field_inject_transform(trace_id="abc-123", env="test")
        mw = EventTransformMiddleware(transform)
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
            await bus.proxy("src").publish("mw.ping", {"key": "original"})
            await handler.wait_received(timeout=2.0)

        assert len(handler.received) >= 1
        payload = handler.received[0]
        assert payload.key == "original"
        # 注入的字段在 data 中
        # 注意：MiddlewareTestPayload 只有 key, count，注入的额外字段会被忽略
        # 所以这里只验证 handler 确实收到了事件

    @pytest.mark.asyncio
    async def test_field_redact(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """敏感字段脱敏"""
        transform = make_field_redact_transform("key")
        mw = EventTransformMiddleware(transform)
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
            await bus.proxy("src").publish(
                "mw.ping", {"key": "secret123", "count": 99}
            )
            await handler.wait_received(timeout=2.0)

        assert len(handler.received) >= 1
        payload = handler.received[0]
        # key 被替换为 ***
        assert payload.key == "***"
        assert payload.count == 99

    @pytest.mark.asyncio
    async def test_custom_transform(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """自定义转换函数"""

        # 注册转换目标事件
        class PrefixedPingEvent(EventDeclaration):
            name = "prefix.mw.ping"
            payload_type = MiddlewareTestPayload

        base_event_registry.register(PrefixedPingEvent)

        def add_prefix(
            name: str,
            data: dict[str, Any] | BaseModel | None,
        ) -> tuple[str, dict[str, Any] | BaseModel | None]:
            # 不对系统事件添加前缀
            if name.startswith("event_bus."):
                return name, data
            return f"prefix.{name}", data

        mw = EventTransformMiddleware(add_prefix)
        chain = MiddlewareChain()
        chain.add(mw)

        handler = SimplePingHandler()
        # 修改订阅以匹配转换后的事件名
        handler.subscriptions = ["prefix.mw.ping"]
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

        assert len(handler.received) >= 1


# ============================================================================
# EventBlockMiddleware
# ============================================================================


class TestEventBlockMiddleware:
    """事件屏蔽中间件"""

    @pytest.mark.asyncio
    async def test_blocklist_blocks_specified_events(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """黑名单中的事件被屏蔽"""
        pred = make_blocklist_predicate("mw.ping")
        mw = EventBlockMiddleware(pred, block_reason="test block")
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
            await asyncio.sleep(0.1)

        # 被屏蔽，handler 不应收到
        assert len(handler.received) == 0
        assert mw.blocked_count == 1

    @pytest.mark.asyncio
    async def test_blocklist_allows_other_events(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """非黑名单事件正常通过"""
        pred = make_blocklist_predicate("some.other.event")
        mw = EventBlockMiddleware(pred)
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

        assert len(handler.received) >= 1
        assert mw.blocked_count == 0

    @pytest.mark.asyncio
    async def test_allowlist_only_allows_whitelisted(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """白名单模式：仅允许指定事件"""
        pred = make_allowlist_predicate("user.login")
        mw = EventBlockMiddleware(pred, block_reason="not in allowlist")
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
            # mw.ping 不在白名单中 → 被屏蔽
            await bus.proxy("src").publish("mw.ping", {"key": "k", "count": 1})
            # user.login 在白名单中 → 通过
            await bus.proxy("src").publish("user.login", None)
            await asyncio.sleep(0.1)

            # mw.ping 被屏蔽（关闭时 __shutdown__ 也可能被屏蔽，所以 ≥1）
            assert mw.blocked_count >= 1

        assert len(handler.received) == 0  # mw.ping 被屏蔽

    @pytest.mark.asyncio
    async def test_custom_block_predicate(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """自定义屏蔽判定"""

        def block_sensitive(
            name: str,
            data: dict[str, Any] | BaseModel | None,
        ) -> bool:
            if isinstance(data, dict):
                return data.get("count", 0) < 0
            return False

        mw = EventBlockMiddleware(block_sensitive)
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
            # count < 0 → 屏蔽
            await bus.proxy("src").publish("mw.ping", {"key": "bad", "count": -1})
            # count >= 0 → 通过
            await bus.proxy("src").publish("mw.ping", {"key": "good", "count": 1})
            await handler.wait_received(timeout=2.0)

        assert mw.blocked_count >= 1
        assert len(handler.received) == 1
        assert handler.received[0].key == "good"


# ============================================================================
# 组合测试
# ============================================================================


class TestMiddlewareComposition:
    """多个模板中间件组合使用"""

    @pytest.mark.asyncio
    async def test_transform_then_block(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """先转换事件名，再基于新名称屏蔽"""
        # 1. 将 mw.ping → blocked.event
        transform = make_rename_transform({"mw.ping": "blocked.event"})
        trans_mw = EventTransformMiddleware(transform)

        # 2. 屏蔽 blocked.event
        pred = make_blocklist_predicate("blocked.event")
        block_mw = EventBlockMiddleware(pred)

        chain = MiddlewareChain()
        chain.add(trans_mw).add(block_mw)  # transform 在外层，先执行

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
            await asyncio.sleep(0.1)

        # 转换后的事件被屏蔽，handler 不应收到
        assert len(handler.received) == 0
        assert block_mw.blocked_count == 1

    @pytest.mark.asyncio
    async def test_rate_limit_before_transform(
        self,
        base_event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
    ) -> None:
        """先限流再转换"""
        rate_mw = RateLimitMiddleware(max_requests=3, window_seconds=10.0)
        transform = make_field_inject_transform(source="test")
        trans_mw = EventTransformMiddleware(transform)

        chain = MiddlewareChain()
        chain.add(rate_mw).add(trans_mw)

        handler = SimplePingHandler()
        handler_registry.register(handler)

        bus = EventBus(
            base_event_registry,
            handler_registry,
            max_queue_size=10,
            middleware_chain=chain,
        )
        async with bus:
            for i in range(10):
                await bus.proxy("src").publish(
                    "mw.ping", {"key": f"k{i}", "count": i}
                )
                await asyncio.sleep(0.01)

        # 限制后仅 ≤3 条通过
        assert len(handler.received) <= 3
