import asyncio
import json
import logging
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Set

import aiosqlite
from pydantic import BaseModel

from ..event import Event, EventRegistry
from ..middleware import BeforePublishNext, Middleware, MiddlewareChain, OnPublishNext

if __name__ == '__main__':
    from ..bus import EventBus  # pragma: no cover

logger = logging.getLogger(__name__)

# ============================================================================
# 1. SQLite 日志中间件（支持降级）
# ============================================================================

LogFallback = Callable[[str], None]
"""降级日志回调签名：接收一条 JSON 序列化的日志行。"""


class SQLiteLoggingMiddleware(Middleware):
    """将事件发布记录持久化到 SQLite 数据库，允许自定义表名与存储列。

    特性
    ----
    - 使用 ``aiosqlite`` 异步写入，不阻塞事件循环。
    - 内置**降级机制**：若 SQLite 不可用（模块未安装、磁盘满、写入失败），
      自动 fallback 到 ``logging.warning`` 或用户提供的回调。
    - 支持自定义建表 DDL 和插入 SQL，满足不同审计粒度需求。
    - *不会* 阻止事件发布 —— 日志写入失败仅警告，不影响正常业务流程。

    参数
    ----
    db_path:
        SQLite 数据库文件路径。``":memory:"`` 表示内存数据库。
    table_name:
        表名，默认 ``"event_log"``。
    extra_columns:
        除 ``(name, sources, data, event_id, event_ids, timestamps)`` 之外的额外列定义。
        例如 ``["user_agent TEXT", "trace_id TEXT"]``。
    fallback:
        降级回调。接收一条 JSON 字符串。为 ``None`` 时使用 ``logging.warning``。
    """

    # 默认表 DDL
    DEFAULT_DDL = """\
CREATE TABLE IF NOT EXISTS {table} (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT    NOT NULL,
    sources     TEXT    NOT NULL DEFAULT '[]',
    data        TEXT,
    event_id    TEXT    NOT NULL,
    event_ids   TEXT    NOT NULL DEFAULT '[]',
    timestamps  TEXT    NOT NULL DEFAULT '[]'
)"""

    DEFAULT_INSERT = """\
INSERT INTO {table} (name, sources, data, event_id, event_ids, timestamps)
VALUES (:name, :sources, :data, :event_id, :event_ids, :timestamps)"""

    def __init__(
        self,
        db_path: str = ':memory:',
        *,
        table_name: str = 'event_log',
        extra_columns: Optional[List[str]] = None,
        fallback: Optional[LogFallback] = None,
    ) -> None:
        self._db_path = db_path
        self._table = table_name
        self._extra_columns = extra_columns or []
        self._fallback: LogFallback = fallback or (lambda line: logger.warning('SQLiteLog fallback: %s', line))

        self._conn: Any = None  # aiosqlite.Connection
        self._ready: bool = False
        self._ddl: str = ''
        self._insert_sql: str = ''

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    async def on_setup(self, bus: 'EventBus') -> None:
        try:
            self._conn = await aiosqlite.connect(self._db_path)
            await self._conn.execute('PRAGMA journal_mode=WAL;')
            await self._conn.execute('PRAGMA synchronous=NORMAL;')
            self._conn.row_factory = aiosqlite.Row
            await self._ensure_table()
            self._ready = True
            logger.info('SQLiteLoggingMiddleware 就绪: %s', self._db_path)
        except Exception:
            logger.exception('SQLiteLoggingMiddleware 初始化失败，降级运行')
            self._ready = False

    async def on_teardown(self, bus: 'EventBus') -> None:
        if self._conn is not None:
            try:
                await self._conn.close()
            except Exception:
                logger.exception('关闭 SQLite 连接失败')

    # ------------------------------------------------------------------
    # 钩子
    # ------------------------------------------------------------------

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
        await self._log_event(event)
        await next(event)

    async def on_publish_error(
        self,
        error: Exception,
        name: str,
        source: str,
        data: dict[str, Any] | BaseModel | None,
    ) -> None:
        # 错误事件也记录
        record: Dict[str, Any] = {
            'name': name,
            'sources': json.dumps([source], ensure_ascii=False),
            'data': _serialize_data(data),
            'event_id': 'ERROR',
            'event_ids': '[]',
            'timestamps': json.dumps([datetime.now(timezone.utc).isoformat()], ensure_ascii=False),
            'error': f'{type(error).__name__}: {error}',
        }
        self._fallback(json.dumps(record, ensure_ascii=False))

    # ------------------------------------------------------------------
    # 内部
    # ------------------------------------------------------------------

    async def _ensure_table(self) -> None:
        columns_def = self.DEFAULT_DDL
        if self._extra_columns:
            # 在默认 DDL 末尾插入额外列
            base = self.DEFAULT_DDL.rstrip(')')
            columns_def = base + ',\n    ' + ',\n    '.join(self._extra_columns) + '\n)'
        self._ddl = columns_def.format(table=self._table)
        await self._conn.execute(self._ddl)
        await self._conn.commit()

        self._insert_sql = self.DEFAULT_INSERT.format(table=self._table)

    async def _log_event(self, event: Event) -> None:
        row: Dict[str, Any] = {
            'name': event.name,
            'sources': json.dumps(event.sources, ensure_ascii=False),
            'data': _serialize_data(event.data),
            'event_id': event.id,
            'event_ids': json.dumps(event.event_ids, ensure_ascii=False),
            'timestamps': json.dumps([t.isoformat() for t in event.timestamps], ensure_ascii=False),
        }
        if self._ready and self._conn is not None:
            try:
                await self._conn.execute(self._insert_sql, row)
                await self._conn.commit()
                return
            except Exception:
                logger.exception('SQLite 写入失败，降级处理')
                self._ready = False  # 一次失败后全部降级
        self._fallback(json.dumps(row, ensure_ascii=False))


# ============================================================================
# 2. 速率限制中间件
# ============================================================================


class RateLimitMiddleware(Middleware):
    """基于**滑动窗口**的速率限制中间件。

    特性
    ----
    - 支持**全局限流**和**按事件名限流**两种模式。
    - 超过限制时自动丢弃事件（不调用 ``next``），并记录警告日志。
    - 纯内存实现，无外部依赖。

    参数
    ----
    max_requests:
        时间窗口内允许的最大请求数。
    window_seconds:
        滑动窗口大小（秒）。
    per_event:
        若为 ``True``，按事件名独立计数；否则全局共享一个窗口。
    """

    def __init__(
        self,
        max_requests: int = 100,
        window_seconds: float = 1.0,
        *,
        per_event: bool = False,
    ) -> None:
        self._max = max_requests
        self._window = window_seconds
        self._per_event = per_event

        # name → deque[float]
        self._buckets: Dict[str, List[float]] = defaultdict(list)
        self._lock = asyncio.Lock()

    async def on_setup(self, bus: 'EventBus') -> None:
        pass

    async def on_teardown(self, bus: 'EventBus') -> None:
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
        key = name if self._per_event else '__global__'
        now = time.monotonic()

        async with self._lock:
            bucket = self._buckets[key]
            # 清理过期时间戳
            cutoff = now - self._window
            while bucket and bucket[0] < cutoff:
                bucket.pop(0)

            if len(bucket) >= self._max:
                logger.warning(
                    'RateLimit 触发: event=%s, limit=%d/%ds',
                    name,
                    self._max,
                    self._window,
                )
                return  # 丢弃事件

            bucket.append(now)

        await next(event_registry, name, source, data, old_event)

    async def on_publish(
        self,
        event: Event,
        next: OnPublishNext,
    ) -> None:
        await next(event)

    @property
    def current_rate(self) -> Dict[str, int]:
        """返回当前各窗口的请求计数快照。"""
        now = time.monotonic()
        cutoff = now - self._window
        return {k: sum(1 for t in v if t >= cutoff) for k, v in self._buckets.items()}


# ============================================================================
# 3. 事件转换中间件
# ============================================================================


TransformFunc = Callable[
    [str, dict[str, Any] | BaseModel | None],
    tuple[str, dict[str, Any] | BaseModel | None],
]
"""事件转换函数签名：(name, data) -> (new_name, new_data)。"""


class EventTransformMiddleware(Middleware):
    """在 ``before_publish`` 阶段对事件名和/或负载数据进行转换。

    典型场景
    --------
    - **事件重命名**：将旧版事件名映射到新版。
    - **数据脱敏**：在持久化前移除敏感字段。
    - **数据补全**：自动注入通用字段（如 ``trace_id``、``timestamp``）。
    - **协议适配**：将外部系统的事件格式转换为内部格式。

    参数
    ----
    transform:
        转换函数，签名为 ``(name, data) -> (new_name, new_data)``。
    """

    def __init__(self, transform: TransformFunc) -> None:
        self._transform = transform

    async def on_setup(self, bus: 'EventBus') -> None:
        pass

    async def on_teardown(self, bus: 'EventBus') -> None:
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
        new_name, new_data = self._transform(name, data)
        await next(event_registry, new_name, source, new_data, old_event)

    async def on_publish(
        self,
        event: Event,
        next: OnPublishNext,
    ) -> None:
        await next(event)


# ============================================================================
# 预置转换函数
# ============================================================================


def make_rename_transform(
    mapping: Dict[str, str],
) -> TransformFunc:
    """创建一个简单的事件重命名转换。

    Example::

        transform = make_rename_transform({"old.event": "new.event"})
        EventTransformMiddleware(transform)
    """

    def _rename(
        name: str,
        data: dict[str, Any] | BaseModel | None,
    ) -> tuple[str, dict[str, Any] | BaseModel | None]:
        return mapping.get(name, name), data

    return _rename


def make_field_inject_transform(
    **static_fields: Any,
) -> TransformFunc:
    """创建一个自动注入静态字段的转换。

    Example::

        transform = make_field_inject_transform(env="prod", version="1.0")
        EventTransformMiddleware(transform)
    """

    def _inject(
        name: str,
        data: dict[str, Any] | BaseModel | None,
    ) -> tuple[str, dict[str, Any] | BaseModel | None]:
        if isinstance(data, dict):
            merged = {**static_fields, **data}
            return name, merged
        return name, data

    return _inject


def make_field_redact_transform(
    *fields: str,
    replacement: str = '***',
) -> TransformFunc:
    """创建一个脱敏转换，将指定字段替换为 ``replacement``。

    Example::

        transform = make_field_redact_transform("password", "token")
        EventTransformMiddleware(transform)
    """

    def _redact(
        name: str,
        data: dict[str, Any] | BaseModel | None,
    ) -> tuple[str, dict[str, Any] | BaseModel | None]:
        if isinstance(data, dict):
            for field in fields:
                if field in data:
                    data[field] = replacement
        return name, data

    return _redact


# ============================================================================
# 4. 事件屏蔽中间件
# ============================================================================


BlockPredicate = Callable[[str, dict[str, Any] | BaseModel | None], bool]
"""屏蔽判定函数签名：(name, data) -> bool。返回 ``True`` 表示屏蔽该事件。"""


class EventBlockMiddleware(Middleware):
    """根据规则屏蔽（丢弃）特定事件，不调用下游中间件也不入队。

    典型场景
    --------
    - **功能开关**：通过配置动态开启/关闭某类事件。
    - **A/B 测试**：按用户分组过滤事件。
    - **环境隔离**：在开发环境中屏蔽外部通知类事件。
    - **噪音过滤**：屏蔽高频但无业务价值的调试事件。

    参数
    ----
    block_predicate:
        判定函数，签名为 ``(name, data) -> bool``。
    block_reason:
        屏蔽时日志中包含的原因描述。
    """

    def __init__(
        self,
        block_predicate: BlockPredicate,
        *,
        block_reason: str = 'blocked by predicate',
    ) -> None:
        self._predicate = block_predicate
        self._reason = block_reason
        self._blocked_count: int = 0

    async def on_setup(self, bus: 'EventBus') -> None:
        pass

    async def on_teardown(self, bus: 'EventBus') -> None:
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
        if self._predicate(name, data):
            self._blocked_count += 1
            logger.debug(
                'EventBlock: 屏蔽事件 %s (reason=%s, total_blocked=%d)',
                name,
                self._reason,
                self._blocked_count,
            )
            return  # 不调用 next，事件被丢弃
        await next(event_registry, name, source, data, old_event)

    async def on_publish(
        self,
        event: Event,
        next: OnPublishNext,
    ) -> None:
        await next(event)

    @property
    def blocked_count(self) -> int:
        """累计已屏蔽事件数。"""
        return self._blocked_count


# ============================================================================
# 预置屏蔽函数
# ============================================================================


def make_blocklist_predicate(
    *event_names: str,
) -> BlockPredicate:
    """创建一个基于事件名黑名单的屏蔽判定。

    Example::

        pred = make_blocklist_predicate("debug.heartbeat", "debug.ping")
        EventBlockMiddleware(pred, block_reason="debug events disabled")
    """

    blocked: Set[str] = set(event_names)

    def _predicate(
        name: str,
        data: dict[str, Any] | BaseModel | None,
    ) -> bool:
        return name in blocked

    return _predicate


def make_allowlist_predicate(
    *event_names: str,
) -> BlockPredicate:
    """创建一个基于事件名白名单的屏蔽判定 —— 仅允许白名单中的事件通过。

    Example::

        pred = make_allowlist_predicate("user.login", "user.logout")
        EventBlockMiddleware(pred, block_reason="not in allowlist")
    """

    allowed: Set[str] = set(event_names)

    def _predicate(
        name: str,
        data: dict[str, Any] | BaseModel | None,
    ) -> bool:
        return name not in allowed

    return _predicate


# ============================================================================
# 内部工具
# ============================================================================


def _serialize_data(data: dict[str, Any] | BaseModel | None) -> Optional[str]:
    """将负载数据序列化为 JSON 字符串。"""
    if data is None:
        return None
    if isinstance(data, BaseModel):
        return data.model_dump_json()
    else:
        try:
            return json.dumps(data, ensure_ascii=False, default=str)
        except (TypeError, ValueError):
            pass
    return str(data)


# ============================================================================
# 5. 递归防护中间件
# ============================================================================


class RecursionDetectedError(RuntimeError):
    """事件发布递归调用被检测并拦截。"""

    pass


class RecursionGuardMiddleware(Middleware):
    """递归调用防护中间件：双重检测，防止自递归和互递归。

    检测逻辑在 ``before_publish`` 阶段执行，不消耗队列资源。

    **第一层：per-source 计数**
        同一 ``source`` 在事件链的 ``sources`` 中出现次数 ≥ ``max_depth`` 时拒绝。
        防范单模块自身递归。

    **第二层：绝对链长**
        事件链 ``event_ids`` 长度 ≥ ``max_chain_length`` 时拒绝，无论各 source
        计数如何。防范多模块互递归（K 个模块互递归可达 ``max_depth × K`` 轮）。

    参数
    ----
    max_depth:
        同一 ``source`` 在事件链中允许出现的最大次数。默认 3。
    max_chain_length:
        事件链绝对最大长度。默认 50。设为 ``None`` 禁用此层检测。
    ignore_sources:
        不参与 **per-source 计数** 检查的发布者名称集合。
        注意：不影响绝对链长检测。
    """

    def __init__(
        self,
        max_depth: int = 3,
        max_chain_length: Optional[int] = 50,
        ignore_sources: Optional[Set[str]] = None,
    ) -> None:
        self.max_depth = max_depth
        self.max_chain_length = max_chain_length  # None → 禁用链长检查
        self._ignore = ignore_sources or set()

    async def on_setup(self, bus: 'EventBus') -> None:
        pass

    async def on_teardown(self, bus: 'EventBus') -> None:
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
        if old_event is not None:
            # 第一层：绝对链长（防范互递归），None 表示禁用
            if self.max_chain_length is not None:
                chain_len = len(old_event.event_ids) + 1  # +1 计入当前事件
                if chain_len > self.max_chain_length:
                    raise RecursionDetectedError(
                        f'Chain length exceeded: {chain_len} > {self.max_chain_length} '
                        f'(max_chain_length={self.max_chain_length})'
                    )

            # 第二层：per-source 计数（防范自递归）
            if source not in self._ignore:
                count = old_event.sources.count(source) + 1
                if count > self.max_depth:
                    raise RecursionDetectedError(
                        f"Recursion detected: source '{source}' appears "
                        f'{count} times in the event chain '
                        f'(max_depth={self.max_depth})'
                    )

        await next(event_registry, name, source, data, old_event)

    async def on_publish(
        self,
        event: Event,
        next: OnPublishNext,
    ) -> None:
        await next(event)


# ============================================================================
# 中间件预设工厂
# ============================================================================


def production_chain(
    db_path: str = 'event_bus.db',
    *,
    rate_limit: int = 1000,
    rate_window: float = 1.0,
    max_depth: int = 5,
    max_chain_length: Optional[int] = 50,
    chain: Optional[MiddlewareChain] = None,
) -> MiddlewareChain:
    """生产环境预设：日志 + 限流 + 递归防护。

    适用场景：正式部署，需要审计追溯和过载保护。

    ``chain`` 参数允许传入已有的链，预设将追加到末尾。
    """

    chain = chain or MiddlewareChain()
    chain.add(SQLiteLoggingMiddleware(db_path))
    chain.add(RateLimitMiddleware(max_requests=rate_limit, window_seconds=rate_window))
    chain.add(
        RecursionGuardMiddleware(
            max_depth=max_depth,
            max_chain_length=max_chain_length,
            ignore_sources={'EventBus', 'EventBusErrorReporter'},
        )
    )
    return chain


def development_chain(
    *,
    chain: Optional[MiddlewareChain] = None,
) -> MiddlewareChain:
    """开发环境预设：内存日志 + 严格递归检测。

    适用场景：本地开发，快速发现逻辑 bug（递归、死循环）。
    """
    chain = chain or MiddlewareChain()
    chain.add(SQLiteLoggingMiddleware(':memory:'))
    chain.add(
        RecursionGuardMiddleware(
            max_depth=2,
            max_chain_length=20,
        )
    )
    return chain


def secure_chain(
    *,
    rate_limit: int = 500,
    rate_window: float = 1.0,
    max_depth: int = 3,
    block_events: Optional[tuple[str, ...]] = None,
    chain: Optional[MiddlewareChain] = None,
) -> MiddlewareChain:
    """安全防护预设：限流 + 递归防护 + 可选事件屏蔽。

    适用场景：对外暴露接口、多租户环境，需要防刷和事件白名单。

    ``chain`` 参数允许传入已有的链，预设将追加到末尾。
    """
    chain = chain or MiddlewareChain()
    chain.add(RateLimitMiddleware(max_requests=rate_limit, window_seconds=rate_window))
    chain.add(
        RecursionGuardMiddleware(
            max_depth=max_depth,
            max_chain_length=max_depth * 10,
            ignore_sources={'EventBus', 'EventBusErrorReporter'},
        )
    )
    if block_events:
        chain.add(
            EventBlockMiddleware(
                make_blocklist_predicate(*block_events),
                block_reason='blocked by secure_chain preset',
            )
        )
    return chain


def minimal_chain(
    *,
    max_depth: int = 5,
    chain: Optional[MiddlewareChain] = None,
) -> MiddlewareChain:
    """最小预设：仅递归防护。

    适用场景：嵌入式、性能敏感，不需要日志和限流。

    ``chain`` 参数允许传入已有的链，预设将追加到末尾。
    """
    chain = chain or MiddlewareChain()
    chain.add(
        RecursionGuardMiddleware(
            max_depth=max_depth,
            max_chain_length=None,  # 仅靠 per-source 计数
        )
    )
    return chain
