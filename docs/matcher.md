# Matcher 文档

## 概述

`Matcher` 是事件匹配器，基于事件注册表和处理器注册表**预计算分派表**，将事件类型高效路由到匹配的处理器。总线内部自动构造，用户无需直接使用。

---

## 架构定位

```text
EventRegistry ──┐
                ├──> Matcher ──> {事件名: [处理器ID, ...]}
HandlerRegistry ┘      ↑
                       │ 版本感知自动重建
```

`Matcher` 取代了旧 `EventHandlerRegistry.get_handlers()` 的匹配职责。
注册表专注存储（CRUD），匹配逻辑独立为 `Matcher`，遵循单一职责原则。

---

## Matcher

```python
class Matcher:
    def __init__(self, event_registry: EventRegistry, handler_registry: EventHandlerRegistry) -> None

    def match(self, event_type: str) -> List[tuple[str, EventHandler]]

    @property
    def dispatch_table(self) -> Dict[str, List[tuple[str, EventHandler]]]
```

### 构造参数

| 参数 | 类型 | 说明 |
| - | - | - |
| `event_registry` | `EventRegistry` | 事件注册表，提供所有已知事件类型名。 |
| `handler_registry` | `EventHandlerRegistry` | 处理器注册表，提供所有已注册处理器及其订阅。 |

构造时自动遍历所有已知事件类型，预计算分派表。

### 预计算分派表

分派表是一个 `{事件名: [处理器ID, ...]}` 的映射：

- **精确 `str` 订阅** → 构建反向索引 `{event_type: [hid, ...]}`，O(1) 查表
- **`Regex` 订阅** → 独立维护扫描列表，仅对正则订阅执行 `fullmatch`

内存仅存处理器 ID（字符串），`match()` 调用时按需从注册表解析为 `(hid, handler)` 元组。

### 版本感知

两个注册表各有 `version` 属性（每次增删递增）。`Matcher` 在每次 `match()` / `dispatch_table` 访问时自动对比版本号，发现变更即重建分派表，无需手动通知。

```text
match() 调用 → 检查版本 (O(1) int 比较) → stale?  → _rebuild()
                                            ↓ fresh
                                         查分派表 (O(1) dict)
```

### match()

```python
def match(self, event_type: str) -> List[tuple[str, EventHandler]]
```

- 已知事件 → O(1) 分派表查表
- 返回 `(handler_id, handler)` 元组列表
- 自动感知注册表版本变更

### dispatch_table

```python
@property
def dispatch_table(self) -> Dict[str, List[tuple[str, EventHandler]]]
```

返回当前分派表的只读副本，自动感知版本变更。主要用于调试和可观测性。

---

## 与总线的关系

`EventBus` 构造时内部创建 `Matcher`，调度循环通过 `self._matcher.match(event.name)` 查找处理器：

```python
class EventBus:
    def __init__(self, event_registry, handler_registry, ...):
        self._matcher = Matcher(event_registry, handler_registry)  # 内部自动

    async def _dispatch_loop(self):
        ...
        for handler_id, handler in self._matcher.match(event.name):
            ...
```

用户无需关心 `Matcher` 的存在，只需提供两个注册表即可。
