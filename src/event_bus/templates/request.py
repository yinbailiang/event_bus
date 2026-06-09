import asyncio
import logging
import uuid
from typing import Dict, Any, Optional, Type, cast

from pydantic import BaseModel, Field

from .. import EventDeclaration, EventBus, Event

from .expect import expect

logger = logging.getLogger(__name__)


# ---------- 基础协议 ----------
class RequestProtocol(BaseModel):
    """请求协议基类，业务请求 Payload 须继承此类。"""
    session_id: str = Field(description="会话ID")
    request_id: str = Field(description="请求ID")


class ResponseProtocol(BaseModel):
    """响应协议基类，业务响应 Payload 须继承此类。"""
    session_id: str = Field(description="会话ID")
    request_id: str = Field(description="请求ID")
    success: bool = Field(default=True, description="操作是否成功")
    error_msg: Optional[str] = Field(default=None, description="失败时的错误信息")

    def raise_if_failed(self) -> None:
        """若响应失败则抛出 RuntimeError。"""
        if not self.success and self.error_msg:
            raise RuntimeError(self.error_msg)

async def request(
    bus_proxy: EventBus.Proxy,
    req_event: str,
    req_data: Dict[str, Any],
    resp_event: str,
    session_id: Optional[str] = None,
    timeout: Optional[float] = 60.0,
) -> ResponseProtocol:
    # ----- 1. 校验事件声明 -----
    req_decl: Optional[Type[EventDeclaration]] = bus_proxy.events_registry.get(req_event)
    if req_decl is None:
        raise ValueError(f"请求事件 '{req_event}' 未注册")
    if req_decl.payload_type is None or not issubclass(req_decl.payload_type, RequestProtocol):
        raise TypeError(f"请求事件 '{req_event}' 负载必须继承 RequestProtocol")

    resp_decl: Optional[Type[EventDeclaration]] = bus_proxy.events_registry.get(resp_event)
    if resp_decl is None:
        raise ValueError(f"响应事件 '{resp_event}' 未注册")
    if resp_decl.payload_type is None or not issubclass(resp_decl.payload_type, ResponseProtocol):
        raise TypeError(f"响应事件 '{resp_event}' 负载必须继承 ResponseProtocol")

    # ----- 2. 准备请求数据 -----
    payload_data: Dict[str, Any] = req_data.copy()
    session_id = session_id if session_id is not None else uuid.uuid4().hex
    request_id: str = uuid.uuid4().hex
    payload_data["session_id"] = session_id
    payload_data["request_id"] = request_id

    # ----- 3. 定义响应过滤器（匹配会话和请求ID）-----
    def response_filter(event: Event) -> bool:
        payload: Optional[BaseModel] = event.data
        if not isinstance(payload, ResponseProtocol):
            raise TypeError(f"响应 payload 应为 ResponseProtocol，实际为 {type(payload)}")
        return payload.session_id == session_id and payload.request_id == request_id

    # ----- 4. 使用 expect 等待响应，并发布请求 -----
    async with expect(
        bus_proxy=bus_proxy,
        event_patterns=resp_event,
        filter_func=response_filter,
    ) as future:
        # 发布请求事件（可能抛出 BusShuttingDown）
        await bus_proxy.publish(req_event, payload_data)

        # 等待响应（带超时控制）
        if timeout is None:
            resp: Event = await future
        else:
            resp: Event = await asyncio.wait_for(future, timeout=timeout)

    if resp.data is None: raise RuntimeError("Unexpected None response")
    return cast(ResponseProtocol, resp.data)