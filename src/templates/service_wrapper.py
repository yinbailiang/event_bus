import inspect
from types import EllipsisType
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar, get_type_hints
from pydantic import BaseModel, Field, create_model

from event_bus import Event, EventDeclaration, EventHandler, EventBus
from .request import RequestProtocol, ResponseProtocol


class ServiceWrapperResult:
    """服务包装结果，包含生成的事件声明列表和处理器实例"""
    def __init__(
        self,
        events: List[Type[EventDeclaration]],
        handler: EventHandler
    ):
        self.events: List[Type[EventDeclaration]] = events
        self.handler: EventHandler = handler

ServiceT= TypeVar('ServiceT')
def wrap_service(
    service_instance: ServiceT, # type: ignore
    service_name: str,
    method_filter: Optional[Callable[[str, Any], bool]] = None,
) -> ServiceWrapperResult:
    """
    将普通服务实例包装为事件驱动的服务处理器。

    Args:
        service_instance: 服务实例（如 ConversationManager）
        service_name: 服务名称，用于生成事件名前缀，如 "conversation"
        method_filter: 可选过滤函数，参数为 (method_name, method)，返回 True 才处理

    Returns:
        ServiceWrapperResult 包含生成的事件声明类和处理器实例
    """
    if method_filter is None:
        # 默认过滤：忽略以 '_' 开头的私有方法，忽略特殊方法（__xxx__）
        def default_filter(name: str, _: Any) -> bool:
            return not name.startswith('_')
        method_filter = default_filter

    service_cls: Type[ServiceT] = type(service_instance)
    events: List[Type[EventDeclaration]] = []
    method_map: Dict[str, Dict[str, Any]] = {}

    # 扫描所有公共方法
    for method_name, method in inspect.getmembers(service_cls, predicate=inspect.isfunction):
        if not method_filter(method_name, method):
            continue

        # 获取签名和类型提示
        sig: inspect.Signature = inspect.signature(method)
        hints: Dict[str, Any] = get_type_hints(method)
        return_type = hints.get('return', Any)

        # 生成请求负载模型
        req_fields: Dict[str, Any] = {}
        for param_name, param in sig.parameters.items():
            if param_name == 'self':
                continue
            param_type = hints.get(param_name, Any)
            default: EllipsisType | Any = ... if param.default is inspect.Parameter.empty else param.default
            req_fields[param_name] = (param_type, Field(default=default))

        # 请求负载继承 RequestProtocol，并包含额外参数字段
        req_model_name = f"{service_name.capitalize()}{method_name.capitalize()}Request"
        req_model = create_model(
            req_model_name,
            __base__=RequestProtocol,
            **req_fields
        )

        # 生成响应负载模型
        # 如果返回类型是 None 或空，使用空模型；否则包含一个 'result' 字段
        if return_type is type(None) or return_type is None:
            resp_fields: Dict[str, Any] = {}
        else:
            resp_fields = {'result': (Optional[return_type], Field(default=None, description='返回值'))}

        resp_model_name = f"{service_name.capitalize()}{method_name.capitalize()}Response"
        resp_model = create_model(
            resp_model_name,
            __base__=ResponseProtocol,
            **resp_fields
        )

        # 生成请求事件声明类
        req_event_name: str = f"{service_name}.{method_name}.request"
        req_event_cls = type(
            f"{req_model_name}Event",
            (EventDeclaration,),
            {"name": req_event_name, "payload_type": req_model}
        )
        events.append(req_event_cls)

        # 生成响应事件声明类
        resp_event_name: str = f"{service_name}.{method_name}.response"
        resp_event_cls = type(
            f"{resp_model_name}Event",
            (EventDeclaration,),
            {"name": resp_event_name, "payload_type": resp_model}
        )
        events.append(resp_event_cls)

        # 记录方法信息供处理器使用
        method_map[req_event_name] = {
            "method": method,
            "is_async": inspect.isawaitable(method),
            "req_model": req_model,
            "resp_model": resp_model,
            "resp_event_name": resp_event_name,
            "sig": sig,
        }

    # 动态创建处理器类
    class DynamicServiceHandler(EventHandler):
        def __init__(self, service: Any):
            super().__init__(subscriptions=list(method_map.keys()))
            self._service = service

        async def handle(self, payload: Optional[BaseModel], bus_proxy: EventBus.Proxy, raw_event: Event) -> None:
            event_name = raw_event.name
            if event_name not in method_map:
                return

            info: Dict[str, Any] = method_map[event_name]
            method = info["method"]
            is_async = info["is_async"]
            req_model = info["req_model"]
            resp_model = info["resp_model"]
            resp_event_name = info["resp_event_name"]

            if not isinstance(payload, RequestProtocol):
                raise TypeError('不正确的 payload 类型')
            if not isinstance(payload, req_model):
                raise TypeError('不正确的 payload 类型')

            # 从 payload 提取参数，忽略 RequestProtocol 内置字段
            kwargs: Dict[str, Any] = payload.model_dump(exclude={'session_id', 'request_id'})

            # 获取 session_id 和 request_id 用于响应
            session_id: str = payload.session_id
            request_id: str = payload.request_id

            success = True
            error_msg = None
            result = None

            try:
                if is_async:
                    result = await method(self._service, **kwargs)
                else:
                    # 在线程池中运行同步方法避免阻塞事件循环
                    result = method(self._service, **kwargs)
            except Exception as e:
                success = False
                error_msg = str(e)

            # 构造响应负载
            resp_data: Dict[str, Any] = {
                "session_id": session_id,
                "request_id": request_id,
                "success": success,
                "error_msg": error_msg,
            }
            if 'result' in resp_model.model_fields:
                resp_data['result'] = result if success else None

            response = resp_model(**resp_data)
            await bus_proxy.publish(resp_event_name, response)

    handler_instance = DynamicServiceHandler(service_instance)
    return ServiceWrapperResult(events, handler_instance)