from typing import Any, Callable, Dict, Set, Type, TypeVar, Tuple, List
from .. import EventDeclaration, EventHandler, EventHandlerRegistry, EventRegistry

EventDeclT = TypeVar('EventDeclT', bound=Type[EventDeclaration])
class ModuleEventRegister:
    """
    模块事件注册器，用于收集并统一向 EventRegistry 注册事件声明。

    使用示例:
        module_events = ModuleEventRegister("my_module")

        @module_events.event
        class MyRequest(EventDeclaration):
            name = "my.request"
            payload_type = MyPayload

        # 在应用启动时一次性注册所有事件
        module_events.register_all_events(event_registry)
    """

    def __init__(self, name: str) -> None:
        self.name: str = name
        self.events: Set[Type[EventDeclaration]] = set()

    def add_event(self, event_decl: Type[EventDeclaration]) -> None:
        """手动添加事件声明类"""
        if event_decl not in self.events:
            self.events.add(event_decl)

    def event(self, event_cls: EventDeclT) -> EventDeclT:
        """
        装饰器：将事件声明类自动添加到注册器中。

        Args:
            event_cls: 继承自 EventDeclaration 的类

        Returns:
            原类（保持类型不变）

        Example:
            @module_events.event
            class MyEvent(EventDeclaration):
                name = "my.event"
                payload_type = MyPayload
        """
        self.add_event(event_cls)
        return event_cls

    def register_all_events(self, event_registry: EventRegistry) -> None:
        """
        将所有收集到的事件声明注册到指定的 EventRegistry 实例中。

        Args:
            event_registry: 目标事件注册表
        """
        for event_decl in self.events:
            event_registry.register(event_decl)

    def get_all_event_names(self) -> List[str]:
        return [e.name for e in self.events]

    def __repr__(self) -> str:
        return f"<ModuleEventRegister name={self.name} events={len(self.events)}>"
    

HandlerT = TypeVar('HandlerT', bound=Type[EventHandler])
class ModuleHandlerRegister:
    """
    模块处理器注册器，用于收集并统一向 EventHandlerRegistry 注册处理器。

    使用示例:
        module_handlers = ModuleHandlerRegister("my_module")

        @module_handlers.handler(depends=lambda: {"db": get_db_connection()})
        class MyHandler(EventHandler):
            def __init__(self, db):
                super().__init__(["my.event"])
                self.db = db
            # ...

        # 在应用启动时一次性注册所有处理器
        module_handlers.register_all_handlers(handler_registry)
    """

    def __init__(self, name: str) -> None:
        self.name: str = name
        # 存储 (处理器类, 依赖工厂函数) 的元组
        self.handlers: Set[Tuple[Type[EventHandler], Callable[[], Dict[str, Any]]]] = set()

    def add_handler(
        self,
        handler_type: Type[EventHandler],
        depends: Callable[[], Dict[str, Any]]
    ) -> None:
        """
        手动添加处理器类型及其依赖工厂函数。

        Args:
            handler_type: 继承自 EventHandler 的类
            depends: 返回依赖字典的可调用对象，字典键为构造器参数名
        """
        handler_entry: Tuple[Type[EventHandler], Callable[[], Dict[str, Any]]] = (handler_type, depends)
        if handler_entry not in self.handlers:
            self.handlers.add(handler_entry)

    def handler(self, depends: Callable[[], Dict[str, Any]] = lambda: {}) -> Callable[[HandlerT], HandlerT]:
        """
        装饰器工厂：返回一个类装饰器，用于将处理器类自动添加到注册器中。

        Args:
            depends: 返回依赖字典的可调用对象，用于构造处理器实例。
                     字典键应对应处理器 __init__ 的参数名。

        Returns:
            类装饰器函数

        Example:
            @module_handlers.handler(depends=lambda: {"conv_manager": conv_manager})
            class CoreLogicHandler(EventHandler):
                def __init__(self, conv_manager):
                    super().__init__(["ui.input.submit"])
                    self.conv_manager = conv_manager
        """
        def decorator(cls: HandlerT) -> HandlerT:
            self.add_handler(cls, depends)
            return cls
        return decorator

    def register_all_handlers(self, handler_registry: EventHandlerRegistry) -> None:
        """
        将所有收集到的处理器实例化并注册到指定的 EventHandlerRegistry 中。

        Args:
            handler_registry: 目标处理器注册表
        """
        for handler_type, depends_factory in self.handlers:
            # 调用依赖工厂获取参数字典
            kwargs: Dict[str, Any] = depends_factory()
            # 实例化处理器并注册
            handler_instance: EventHandler = handler_type(**kwargs)
            handler_registry.register(handler_instance)

    def __repr__(self) -> str:
        return f"<ModuleHandlerRegister name={self.name} handlers={len(self.handlers)}>"