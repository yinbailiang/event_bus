from abc import ABC, abstractmethod
import asyncio
import logging
from types import TracebackType
import uuid
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, Literal, Optional

from pydantic import BaseModel, Field

from .. import EventBus, Event
from .expect import expect
from .request import RequestProtocol, ResponseProtocol, request

logger = logging.getLogger(__name__)

class PipeHandshakeError(Exception): pass
class PipeTeardownError(Exception): pass
class PipeClosedError(Exception): pass

class Pipe(ABC):

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
    async def open(self) -> None: pass

    @abstractmethod
    async def close(self) -> None: pass

    @abstractmethod
    async def send(self, data: BaseModel) -> None: pass

    @abstractmethod
    async def receive(self) -> BaseModel: pass

class PipeAllocator(ABC):

    @abstractmethod
    async def allocate(self, **kwargs: Dict[str, Any]) -> str:
        """创建一个管道实例并返回其唯一标识符。"""
        pass

    @abstractmethod
    async def release(self, pipe_id: str) -> None:
        """释放指定管道，移除其注册。"""
        pass

    @abstractmethod
    async def get(self, pipe_id: str) -> Optional[Pipe]:
        """根据 ID 获取管道实例，不存在时返回 None。"""
        pass

class InProcessPipe(Pipe):
    """简单的 asyncio.Queue 包装，支持背压"""

    def __init__(self, maxsize: Optional[int] = None) -> None:
        super().__init__()
        self._queue: asyncio.Queue[BaseModel] =  asyncio.Queue() if maxsize is None else asyncio.Queue(maxsize=maxsize)
        self._closed = asyncio.Event()

    async def send(self, data: BaseModel) -> None:
        if self._closed.is_set():
            raise PipeClosedError("Pipe is closed")
        await self._queue.put(data)

    async def receive(self) -> BaseModel:
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

class InProcessPipeAllocator(PipeAllocator):
    """进程内管道分配器，管理所有活跃管道实例。

    支持自定义默认管道类型，并允许在 `allocate()` 时提供参数。
    """

    def __init__(
        self,
        pipe_type: type[Pipe] = InProcessPipe,
    ) -> None:
        self._pipes: Dict[str, Pipe] = {}
        self._pipe_type = pipe_type

    async def allocate(
        self,
        **kwargs: Dict[str, Any],
    ) -> str:
        pipe_id: str = uuid.uuid4().hex
        pipe: Pipe = self._pipe_type(**kwargs)

        if pipe_id in self._pipes: raise ValueError(f"Pipe with id {pipe_id} already exists")
        self._pipes[pipe_id] = pipe
        return pipe_id

    async def get(self, pipe_id: str) -> Optional[Pipe]:
        return self._pipes.get(pipe_id)

    async def release(self, pipe_id: str) -> None:
        self._pipes.pop(pipe_id, None)

_default_allocator: Optional[InProcessPipeAllocator] = None
def get_default_allocator() -> InProcessPipeAllocator:
    global _default_allocator
    if _default_allocator is None:
        _default_allocator = InProcessPipeAllocator()
    return _default_allocator

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
    session_id: Optional[str] = None,
    allocator: Optional[InProcessPipeAllocator] = None,
    pipe_kargs: Optional[Dict[str, Any]] = None,
) -> AsyncIterator[Pipe]:
    if allocator is None:
        allocator = get_default_allocator()

    session_id = session_id or uuid.uuid4().hex

    pipe_id: str = await allocator.allocate(**(pipe_kargs or {}))
    pipe: Optional[Pipe] = await allocator.get(pipe_id)
    if pipe is None:
        raise PipeHandshakeError(f"Failed to allocate pipe {pipe_id}")

    logger.debug(f"Pipe allocated with id={pipe_id}")

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

        if not isinstance(resp, PipeLinkedResponse):  
            raise PipeHandshakeError(f"Handshake failed: expect PipeLinkedResponse but {resp.__class__.__name__}")
        if not resp.success:
            raise PipeHandshakeError(f"Handshake failed: {resp.error_msg}")

        logger.debug(f"Pipe handshake successful for id={pipe_id}")

        async with pipe:
            yield pipe

    finally:
        if await allocator.get(pipe_id) is not None:
            await allocator.release(pipe_id)
        logger.debug(f"Pipe {pipe_id} released from allocator")


@asynccontextmanager
async def expect_pipe(
    bus_proxy: EventBus.Proxy,
    req_event: str,
    resp_event: str,
    session_id: Optional[str] = None,
    timeout: float = 5.0,
    allocator: Optional[InProcessPipeAllocator] = None,
) -> AsyncIterator[Pipe]:
    """等待一个管道连接请求，返回已建立的 Pipe 实例。"""

    if allocator is None:
        allocator = get_default_allocator()

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
    pipe: Optional[Pipe] = await allocator.get(pipe_id)
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