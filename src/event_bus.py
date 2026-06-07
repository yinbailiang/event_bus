import asyncio
import logging
import re
import types
import uuid
from datetime import datetime
from typing import Any, ClassVar, Dict, List, Optional, Set, Type, Pattern, Union
from pydantic import BaseModel, Field
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

class Event(BaseModel):
    """事件数据类"""
    name: str = Field(description="事件类型")
    data: Optional[BaseModel] = Field(default=None, description="事件附加数据")

    # metadata
    id: str = Field(default_factory=lambda: uuid.uuid4().hex, description="事件UUID")
    sources: List[str] = Field(default_factory=list, description="事件处理链")
    timestamps: List[datetime] = Field(default_factory=lambda:[], description="事件时间戳")

class EventDeclaration(ABC):
    """事件声明抽象基类"""
    name: ClassVar[str]
    payload_type: ClassVar[Optional[Type[BaseModel]]] = None

class EventRegistry:
    """事件注册表"""

    def __init__(self) -> None:
        self._events: Dict[str, Type[EventDeclaration]] = {}

    def register(self, event_decl: Type[EventDeclaration]) -> None:
        """手动注册事件声明"""
        if event_decl.name in self._events:
            raise ValueError(f"重复的事件声明 {event_decl.name}")
        self._events[event_decl.name] = event_decl

    def unregister(self, event_name: str) -> None:
        """注销事件声明"""
        if event_name in self._events:
            del self._events[event_name]

    def get(self, name: str) -> Optional[Type[EventDeclaration]]:
        return self._events.get(name)

    def list_names(self) -> List[str]:
        return list(self._events.keys())

class EventHandler(ABC):
    """事件处理器基类，所有具体事件处理器应继承此类"""
    
    def __init__(self, subscriptions: Optional[List[str]] = None, handle_timeout: Optional[float] = 1.0) -> None:
        self.subscriptions: List[str] = subscriptions.copy() if subscriptions is not None else [] # 订阅的事件类型列表，支持正则表达式
        self.handle_timeout: Optional[float] = handle_timeout

    async def __call__(self, bus_proxy: 'EventBus.Proxy', event: Event) -> None:
        """事件处理器入口，自动解包事件数据"""
        await self.handle(event.data, bus_proxy, event)
    
    @abstractmethod
    async def handle(self, payload: Optional[BaseModel], bus_proxy: 'EventBus.Proxy', raw_event: Event) -> None:
        pass

class EventHandlerRegistry:
    """事件处理器注册表，负责管理事件类型与处理器的映射关系"""
    
    def __init__(self) -> None:
        self.regex_cache: Dict[str, Pattern[str]] = {}
        self._handlers: Dict[str, EventHandler] = {}

    def register(self, handler: EventHandler) -> str:
        """注册一个事件处理器实例"""
        id = uuid.uuid4().hex
        self._handlers[id] = handler
        return id
    
    def get(self, handler_id: str) -> Optional[EventHandler]:
        """根据ID获取事件处理器实例"""
        return self._handlers.get(handler_id)
    
    def unregister(self, handler_id: str) -> bool:
        """注销一个事件处理器实例"""
        if handler_id in self._handlers:
            del self._handlers[handler_id]
            return True
        return False

    def get_handlers(self, event_type: str) -> List[EventHandler]:
        """获取匹配事件类型的所有处理器实例"""
        matched_handlers: List[EventHandler] = []
        for _, handler in self._handlers.items():
            for pattern in handler.subscriptions:
                if self._match_pattern(event_type, pattern):
                    matched_handlers.append(handler)
                    break
        return matched_handlers
    
    def get_handlers_count(self) -> int:
        return len(self._handlers)

    def _match_pattern(self, event_type: str, pattern: str) -> bool:
        """使用正则表达式匹配事件类型"""
        if pattern not in self.regex_cache:
            self.regex_cache[pattern] = re.compile(pattern)
        return re.fullmatch(self.regex_cache[pattern], event_type) is not None

class BusShuttingDown(Exception):
    """总线正在停止，拒绝新发布，请求处理器执行清理并退出"""
    pass

class ShutdownEvent(EventDeclaration):
    name = "event_bus.__shutdown__"

class TaskErrorPayload(BaseModel):
    error_event: Event = Field(description="发生异常的事件")
    handler_name: str = Field(description="发生异常的处理器")
    error_type: str = Field(description="异常类型")
    error_message: str = Field(description="异常消息")

class TaskErrorEvent(EventDeclaration):
    name = "event_bus.__task_error__"
    payload_type = TaskErrorPayload

class EventBus:
    """
    异步事件总线，支持订阅/发布模式

    系统内置事件:
    event_bus.__task_error__ 任务执行失败时发送，载荷为 TaskErrorPayload，发布者为 EventBusErrorReporter
    event_bus.__shutdown__ 总线将要关闭时发送, 无载荷, 发布者为 EventBus
    """

    events_avg_wait_time: ClassVar[float] = 0.05
    events_wait_timeout_min: ClassVar[float] = 1.0
    events_wait_timeout_max: ClassVar[float] = 15.0

    tasks_wait_timeout: ClassVar[float] = 15.0

    class Proxy:
        """事件总线代理，提供给处理器调用以访问总线功能"""
        def __init__(self, bus: 'EventBus', source: str, raw_event: Optional[Event] = None) -> None:
            self._bus: EventBus = bus
            self._source: str = source
            self._raw_event: Optional[Event] = raw_event

        async def publish(self, name: str, data: Optional[Union[Dict[str, Any], BaseModel]] = None) -> None:
            await self._bus._publish(name, self._source, data, self._raw_event)
        
        @property
        def handlers_registry(self) -> EventHandlerRegistry:
            return self._bus._handlers
        
        @property
        def events_registry(self) -> EventRegistry:
            return self._bus._events
        
        @property
        def bus(self) -> 'EventBus':
            return self._bus


    def __init__(self, event_registry: EventRegistry, handler_registry: EventHandlerRegistry, max_queue_size: int = 1024, max_handler_semaphore: int = 256) -> None:
        self._events: EventRegistry = event_registry
        self._handlers: EventHandlerRegistry = handler_registry
        
        if self._events.get(ShutdownEvent.name) is None:
            self._events.register(ShutdownEvent)
        if self._events.get(TaskErrorEvent.name) is None:
            self._events.register(TaskErrorEvent)

        self._state_lock: asyncio.Lock = asyncio.Lock()
        self._enable_publish: asyncio.Event = asyncio.Event()
        self._running: asyncio.Event = asyncio.Event()
        self._queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=max_queue_size)
        self._dispatch_task: Optional[asyncio.Task[None]] = None

        self._handler_semaphore = asyncio.Semaphore(max_handler_semaphore)
        self._active_tasks: Set[asyncio.Task[Any]] = set()

    async def _publish(self, name: str, source: str, data: Optional[Union[Dict[str, Any], BaseModel]] = None, old_event: Optional[Event] = None) -> None:
        """发布事件到总线"""
        if not self._enable_publish.is_set():
            if self._running.is_set():
                logger.warning("EventBus is stopping, cannot publish new events")
                raise BusShuttingDown("EventBus is stopping, cannot publish new events")
            else:
                logger.warning("EventBus is not running, cannot publish events")
                raise RuntimeError("EventBus is not running, cannot publish events")

        event_declaration: Optional[Type[EventDeclaration]] = self._events.get(name)
        if not event_declaration:
            logger.error(f"Unknown event type: {name}")
            raise ValueError(f"Unknown event type: {name}")
        
        payload: Optional[BaseModel] = None
        if event_declaration.payload_type:
            if data is None:
                raise ValueError(f"Event {name} requires payload data, but none provided")
            
            elif isinstance(data, BaseModel):
                if not isinstance(data, event_declaration.payload_type):
                    raise TypeError(
                        f"Payload type mismatch for event '{name}': "
                        f"expected {event_declaration.payload_type.__name__}, got {type(data).__name__}"
                    )
                payload = data.model_copy()
            
            else: # 静态类型检查已检查
                payload = event_declaration.payload_type(**data)
        else:
            if data is not None:
                raise ValueError(f"Event {name} does not accept payload data")
        
        event = Event(
            name=name,
            data=payload,
            sources=old_event.sources.copy() if old_event else [],
            timestamps=old_event.timestamps.copy() if old_event else [],
        )
        event.sources.append(source)
        event.timestamps.append(datetime.now())
        await self._queue.put(event)
        logger.debug(f"Event published: {event.name} (id={event.id})")

    async def start(self) -> None:
        """启动事件分发循环"""
        async with self._state_lock:
            if self._running.is_set():
                return
            try:
                self._dispatch_task = asyncio.create_task(self._dispatch_loop())
            except Exception:
                logger.exception("Error occurred while starting event bus")
                raise
            self._running.set()
            self._enable_publish.set()
            logger.info("EventBus started")
    
    async def stop(self) -> None:
        """停止事件总线"""
        async with self._state_lock:
            if not self._running.is_set():
                return
        
            await self._publish(ShutdownEvent.name, source="EventBus", data=None)

            self._enable_publish.clear() # 阻止新消息入队

            try:
                timeout: float = max(self.events_wait_timeout_min,min(self.events_wait_timeout_max,self._queue.qsize() * self.events_avg_wait_time))
                await asyncio.wait_for(self._queue.join(), timeout=timeout) # 等待队列处理完毕，避免丢失事件
            except asyncio.TimeoutError:
                logger.warning("Timeout while waiting for event queue to drain during shutdown")


            self._running.clear()
            if self._dispatch_task:
                self._dispatch_task.cancel()
                try:
                    await self._dispatch_task
                except asyncio.CancelledError:
                    pass

            await self._wait_all_tasks_done() # 等待所有处理器任务完成
        
            logger.info("EventBus stopped")
    
    async def __aenter__(self) -> "EventBus":
        """异步上下文管理器入口"""
        await self.start()
        return self

    async def __aexit__(self, exc_type: Optional[type], exc_val: Optional[BaseException], exc_tb: Optional[types.TracebackType]) -> Optional[bool]:
        """异步上下文管理器出口"""
        await self.stop()
        return None

    def proxy(self, source: str, raw_event: Optional[Event] = None) -> Proxy:
        """创建一个事件总线代理实例，供事件处理器调用"""
        return EventBus.Proxy(self, source, raw_event)

    async def _handler_wrapper(self, handler: EventHandler, bus_proxy: 'EventBus.Proxy', event: Event) -> None:
        """事件处理器包装器"""
        try:
            async with self._handler_semaphore: # 控制并发处理器数量，避免过载
                async with asyncio.timeout(handler.handle_timeout):
                    await handler(bus_proxy, event)
        except BaseException as e:
            if "EventBusErrorReporter" not in event.sources:
                try:
                    await self._publish(
                        name=TaskErrorEvent.name, 
                        source="EventBusErrorReporter", 
                        data=TaskErrorPayload(
                            error_event=event, 
                            handler_name=handler.__class__.__name__, 
                            error_type=type(e).__name__, 
                            error_message=str(e) 
                        ),
                        old_event = event
                    )
                except BusShuttingDown as err:
                    logger.warning(f"Skipping task_error publish during shutdown: {err}")
                except Exception:
                    logger.exception("Failed to publish task_error event")
            raise e
            
    async def _dispatch_loop(self) -> None:
        """事件分发主循环"""
        await self._running.wait()  # 等待事件总线启动
        while self._running.is_set():
            event: Optional[Event] = None
            try:
                event = await self._queue.get()
                for handler in self._handlers.get_handlers(event.name):
                    self._register_task(asyncio.create_task(self._handler_wrapper(handler, self.proxy(handler.__class__.__name__, event), event)))
            except Exception:
                logger.exception("Unexpected error in dispatch loop")
            finally:
                if event:
                    self._queue.task_done()

    def _register_task(self, task: asyncio.Task[Any]) -> None:
        self._active_tasks.add(task)
        task.add_done_callback(self._on_task_done)

    def _on_task_done(self, task: asyncio.Task[Any]) -> None:
        """任务完成时的回调（在任务完成后立即触发）"""
        self._active_tasks.discard(task)

        try:
            if exc := task.exception():
                raise exc
        except asyncio.CancelledError:
            logger.debug(f"Handler task cancelled: {task.get_name()}")
        except asyncio.InvalidStateError:
            logger.warning(f"Task {task.get_name()} callback triggered in invalid state")
        except Exception:
            logger.exception(f"Handler task failed: {task.get_name()}")
    
    async def _wait_all_tasks_done(self) -> None:
        """等待所有未完成的任务完成，适用于事件总线停止时调用"""
        if self._active_tasks:
            try:
                logger.info(f"Waiting for {len(self._active_tasks)} active handler tasks to complete...")
                done, pending = await asyncio.wait(self._active_tasks.copy(), return_when=asyncio.ALL_COMPLETED, timeout=self.tasks_wait_timeout)
                logger.info(f"All handler tasks completed. Total: {len(done)}")
                if pending:
                    logger.warning(f"Timeout: {len(pending)} tasks pending, cancelling...")
                    for task in pending:
                        if not task.done():
                            task.cancel()
                            try:
                                await task
                            except asyncio.CancelledError:
                                pass
            except BaseException as e:
                logger.exception("Unexpected error in wait all task done")
                raise e

    @property
    def is_running(self) -> bool: return self._running.is_set()
    @property
    def is_publishing_enabled(self) -> bool: return self._enable_publish.is_set()
    def get_active_task_count(self) -> int: return len(self._active_tasks)
    def get_queue_size(self) -> int: return self._queue.qsize()
    def register_handler(self, handler: EventHandler) -> str: return self._handlers.register(handler)