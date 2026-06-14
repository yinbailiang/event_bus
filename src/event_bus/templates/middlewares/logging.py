import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from pydantic import BaseModel

from event_bus import (
    BeforePublishNext,
    Event,
    EventBus,
    EventRegistry,
    Middleware,
    OnPublishNext,
)

# ---------------------------------------------------------------------------
# aiosqlite 惰性导入 —— 仅 SQLiteLoggingMiddleware 需要
# ---------------------------------------------------------------------------
_aiosqlite: Any = None
_aiosqlite_import_error: Optional[ImportError] = None

try:
    import aiosqlite as _aiosqlite
except ImportError as _e:
    _aiosqlite_import_error = _e


logger = logging.getLogger(__name__)

# ============================================================================
# 共享类型与工具
# ============================================================================

LogFallback = Callable[[str], None]
"""降级日志回调签名：接收一条 JSON 序列化的日志行。"""


def serialize_data(data: Dict[str, Any] | BaseModel | None) -> Optional[str]:
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
# JSONL 文件日志（默认）
# ============================================================================


class JSONLLoggingMiddleware(Middleware):
    """将事件发布记录追加到 JSONL 文件，每行一条 JSON。

    特性
    ----
    - **零依赖**：纯文件追加，无需数据库驱动。
    - **人类可读**：每行一条格式化的 JSON，可直接用 ``tail -f``、``jq`` 等工具消费。
    - **降级机制**：文件不可写时自动 fallback 到 ``logging.warning`` 或自定义回调。
    - **不阻塞**：文件写入失败仅警告，不影响事件正常流程。
    - **自动建目录**：文件路径的父目录不存在时自动创建。

    参数
    ----
    file_path:
        JSONL 文件路径，默认 ``"events.jsonl"``。
    fallback:
        降级回调。接收一条 JSON 字符串。为 ``None`` 时使用 ``logging.warning``。
    extra_fields:
        除默认字段外追加的静态字段，例如 ``{"service": "api-gateway"}``。
    """

    def __init__(
        self,
        file_path: str = 'events.jsonl',
        fallback: Optional[LogFallback] = None,
        extra_fields: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._file_path = file_path
        self._fallback: LogFallback = fallback or (lambda line: logger.warning('JSONL fallback: %s', line))
        self._extra = extra_fields or {}
        self._ready: bool = False

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    async def on_setup(self, bus: EventBus) -> None:  # noqa: ARG002
        """创建目录并测试文件可写性。"""
        import os

        try:
            parent = os.path.dirname(self._file_path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            # 测试文件是否可写
            await asyncio.to_thread(self._test_write)
            self._ready = True
            logger.info('JSONLLoggingMiddleware 就绪: %s', self._file_path)
        except Exception:
            logger.exception('JSONLLoggingMiddleware 初始化失败，降级运行')
            self._ready = False

    async def on_teardown(self, bus: EventBus) -> None:  # noqa: ARG002
        """No-op."""
        pass

    # ------------------------------------------------------------------
    # 钩子
    # ------------------------------------------------------------------

    async def before_publish(
        self,
        event_registry: EventRegistry,
        name: str,
        source: str,
        data: Dict[str, Any] | Any | None,
        old_event: Event | None,
        next: BeforePublishNext,
    ) -> None:
        """Propagate to next (日志在 on_publish 中记录）。"""
        await next(event_registry, name, source, data, old_event)

    async def on_publish(
        self,
        event: Event,
        next: OnPublishNext,
    ) -> None:
        """将事件写入 JSONL 文件。"""
        await self._log_event(event)
        await next(event)

    async def on_publish_error(
        self,
        error: Exception,
        name: str,
        source: str,
        data: Dict[str, Any] | Any | None,
    ) -> None:
        """记录发布异常到 fallback 通道。"""
        record: Dict[str, Any] = {
            'name': name,
            'source': source,
            'data': serialize_data(data),
            'event_id': 'ERROR',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'error': f'{type(error).__name__}: {error}',
        }
        self._fallback(json.dumps(record, ensure_ascii=False))

    def _test_write(self) -> None:
        """同步方法：测试文件是否可写（由 asyncio.to_thread 调用）。"""
        with open(self._file_path, 'a', encoding='utf-8'):
            pass

    def _write_line(self, line: str) -> None:
        """同步方法：追加一行到 JSONL 文件（由 asyncio.to_thread 调用）。"""
        with open(self._file_path, 'a', encoding='utf-8') as f:
            f.write(line + '\n')
            f.flush()

    async def _log_event(self, event: Event) -> None:
        record: Dict[str, Any] = {
            'name': event.name,
            'source': event.sources[-1] if event.sources else '',
            'data': serialize_data(event.data),
            'event_id': event.id,
            'event_ids': event.event_ids,
            'sources': event.sources,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            **self._extra,
        }
        line = json.dumps(record, ensure_ascii=False)

        if self._ready:
            try:
                await asyncio.to_thread(self._write_line, line)
                return
            except Exception:
                logger.exception('JSONL 写入失败，降级处理')
                self._ready = False  # 一次失败后全部降级
        self._fallback(line)


# ============================================================================
# SQLite 数据库日志
# ============================================================================


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

    async def on_setup(self, bus: EventBus) -> None:
        """连接 SQLite 并建表。"""
        if _aiosqlite is None:
            raise ImportError(
                'SQLiteLoggingMiddleware 需要 aiosqlite 包，请执行: pip install infinity_bus[templates]'
            ) from _aiosqlite_import_error
        try:
            self._conn = await _aiosqlite.connect(self._db_path)
            await self._conn.execute('PRAGMA journal_mode=WAL;')
            await self._conn.execute('PRAGMA synchronous=NORMAL;')
            self._conn.row_factory = _aiosqlite.Row
            await self._ensure_table()
            self._ready = True
            logger.info('SQLiteLoggingMiddleware 就绪: %s', self._db_path)
        except Exception:
            logger.exception('SQLiteLoggingMiddleware 初始化失败，降级运行')
            self._ready = False

    async def on_teardown(self, bus: EventBus) -> None:
        """关闭 SQLite 连接。"""
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
        data: Dict[str, Any] | BaseModel | None,
        old_event: Event | None,
        next: BeforePublishNext,
    ) -> None:
        """Propagate to next (日志在 on_publish 中记录）。"""
        await next(event_registry, name, source, data, old_event)

    async def on_publish(
        self,
        event: Event,
        next: OnPublishNext,
    ) -> None:
        """将事件写入 SQLite。"""
        await self._log_event(event)
        await next(event)

    async def on_publish_error(
        self,
        error: Exception,
        name: str,
        source: str,
        data: Dict[str, Any] | BaseModel | None,
    ) -> None:
        """记录发布异常到 fallback 通道。"""
        # 错误事件也记录
        record: Dict[str, Any] = {
            'name': name,
            'sources': json.dumps([source], ensure_ascii=False),
            'data': serialize_data(data),
            'event_id': 'ERROR',
            'event_ids': '[]',
            'timestamps': json.dumps(
                [datetime.now(timezone.utc).isoformat()],
                ensure_ascii=False,
            ),
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
            'data': serialize_data(event.data),
            'event_id': event.id,
            'event_ids': json.dumps(event.event_ids, ensure_ascii=False),
            'timestamps': json.dumps(
                [t.isoformat() for t in event.timestamps],
                ensure_ascii=False,
            ),
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

    @property
    def is_connect(self) -> bool:
        return self._conn is not None
