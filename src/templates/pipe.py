from abc import ABC, abstractmethod
import asyncio
import logging
from types import TracebackType
import uuid
from contextlib import asynccontextmanager
from typing import AsyncIterator, Dict, Literal, Optional

from pydantic import BaseModel, Field

from event_bus import EventBus, Event
from .expect import expect
from .request import RequestProtocol, ResponseProtocol, request

logger = logging.getLogger(__name__)

class PipeHandshakeError(Exception): pass
class PipeTeardownError(Exception): pass
class PipeClosedError(Exception): pass

class Pipe(ABC):

    def __init__(self, maxsize: Optional[int] = None) -> None:
        super().__init__()
        pass

    async def __aenter__(self) -> "Pipe":
        await self.open()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> Optional[bool]:
        await self.close()

    @abstractmethod
    async def open(self) -> None:
        pass

    @abstractmethod
    async def close(self) -> None:
        pass

    @abstractmethod
    async def send(self, data: BaseModel) -> None:
        pass

    @abstractmethod
    async def receive(self) -> BaseModel:
        pass

class PipeRegistry:
    """单例管道注册表，管理所有活跃管道实例。"""

    _instance: Optional["PipeRegistry"] = None
    _lock = asyncio.Lock()

    def __init__(self):
        self._pipes: Dict[str, Pipe] = {}

    @classmethod
    async def get_instance(cls) -> "PipeRegistry":
        if cls._instance is not None:
            return cls._instance
        async with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
        return cls._instance

    def register(self, pipe_id: str, pipe: Pipe) -> None:
        if pipe_id in self._pipes:
            raise ValueError(f"Pipe with id {pipe_id} already exists")
        self._pipes[pipe_id] = pipe

    def get(self, pipe_id: str) -> Optional[Pipe]:
        return self._pipes.get(pipe_id)

    def pop(self, pipe_id: str) -> Optional[Pipe]:
        return self._pipes.pop(pipe_id, None)

    def remove(self, pipe_id: str) -> None:
        self._pipes.pop(pipe_id, None)

class InProcessPipe(Pipe):

    """简单的 asyncio.Queue 包装，支持背压"""
    def __init__(self, maxsize: Optional[int] = None) -> None:
        super().__init__(maxsize=maxsize)
        self._queue: asyncio.Queue[BaseModel] =  asyncio.Queue() if maxsize is None else asyncio.Queue(maxsize=maxsize)
        self._closed = asyncio.Event()

    async def send(self, data: BaseModel) -> None:
        if self._closed.is_set():
            raise PipeClosedError("Pipe is closed")
        await self._queue.put(data)

    async def receive(self) -> BaseModel:
        if self._queue.qsize() != 0:
            data: BaseModel = await self._queue.get()
            self._queue.task_done()
            return data
        if self._closed.is_set():
            raise PipeClosedError("Pipe is closed")

        get_task: asyncio.Task[BaseModel] = asyncio.create_task(self._queue.get())
        wait_task: asyncio.Task[Literal[True]] = asyncio.create_task(self._closed.wait())
        done, _ = await asyncio.wait([get_task, wait_task], return_when=asyncio.FIRST_COMPLETED)

        if get_task in done:
            wait_task.cancel()
            try:
                await wait_task
            except asyncio.CancelledError:
                pass
            data: BaseModel = get_task.result()
            self._queue.task_done()
            return data

        get_task.cancel()
        try:
            await get_task
        except asyncio.CancelledError:
            pass
        raise PipeClosedError("Pipe is closed")
        
    async def open(self) -> None:
        if self._closed.is_set():
            self._closed.clear()
        pass

    async def close(self) -> None:
        if not self._closed.is_set():
            self._closed.set()
        pass

class PipeOpenRequest(RequestProtocol):
    pipe_id: str = Field(description="管道ID")

class PipeLinkedResponse(ResponseProtocol):
    pass

@asynccontextmanager
async def open_pipe(
    bus_proxy: EventBus.Proxy,
    req_event: str,
    resp_event: str,
    handshake_timeout: float = 5.0,
    pipe_type: type[Pipe] = InProcessPipe,
    maxsize: Optional[int] = None,
    session_id: Optional[str] = None,
) -> AsyncIterator[Pipe]:
    registry: PipeRegistry = await PipeRegistry.get_instance()
    session_id = session_id or uuid.uuid4().hex
    pipe_id: str = session_id
    pipe: Pipe = pipe_type(maxsize=maxsize)

    # 将管道注册到全局表
    registry.register(pipe_id, pipe)
    logger.debug(f"Pipe registered with id={pipe_id}")

    try:
        try:
            resp: ResponseProtocol = await request(
                bus_proxy=bus_proxy,
                req_event=req_event,
                req_data={
                    "pipe_id": pipe_id,
                },
                resp_event=resp_event,
                session_id=session_id,
                timeout=handshake_timeout,
            )
        except asyncio.TimeoutError as e:
            raise PipeHandshakeError(f"Handshake timeout") from e
        except Exception as e:
            raise PipeHandshakeError(f"Handshake failed: {e}") from e

        if not isinstance(resp, PipeLinkedResponse) or not resp.success:  
            raise PipeHandshakeError(f"Handshake failed: {resp}")

        logger.debug(f"Pipe handshake successful for id={pipe_id}")

        async with pipe:
            yield pipe

    finally:
        if registry.get(pipe_id) is not None:
            registry.remove(pipe_id)
        logger.debug(f"Pipe {pipe_id} removed from registry")


@asynccontextmanager
async def expect_pipe(
    bus_proxy: EventBus.Proxy,
    req_event: str,
    resp_event: str,
    session_id: Optional[str] = None,
    timeout: float = 5.0,
) -> AsyncIterator[Pipe]:
    """等待一个管道连接请求，返回已建立的 Pipe 实例。"""

    registry: PipeRegistry = await PipeRegistry.get_instance()

    def request_filter(event: Event) -> bool:
        if not isinstance(event.data, PipeOpenRequest):
            return False
        if session_id is not None and event.data.session_id != session_id:
            return False
        return True

    try:
        async with expect(bus_proxy,req_event,request_filter) as future:
            req_event_obj: Event = await asyncio.wait_for(future, timeout=timeout)
    except asyncio.TimeoutError as e:
        raise PipeHandshakeError("Handshake timeout") from e
    
    req_data: Optional[BaseModel] = req_event_obj.data
    if not isinstance(req_data, PipeOpenRequest):
        raise PipeHandshakeError("Invalid request payload type")

    pipe_id: str = req_data.pipe_id
    pipe: Optional[Pipe] = registry.pop(pipe_id)  # 取出并移除，确保一对一所有权
    if pipe is None:
        error_resp = PipeLinkedResponse(
            session_id=req_data.session_id,
            request_id=req_data.request_id,
            success=False,
            error_msg=f"Pipe {pipe_id} not found"
        )
        await bus_proxy.publish(resp_event, error_resp.model_dump())
        raise PipeHandshakeError(f"Pipe {pipe_id} not found")

    
    success_resp = PipeLinkedResponse(
        session_id=req_data.session_id,
        request_id=req_data.request_id,
        success=True
    )
    await bus_proxy.publish(resp_event, success_resp.model_dump())
    logger.debug(f"Pipe accepted: {pipe_id}")

    async with pipe:
        yield pipe