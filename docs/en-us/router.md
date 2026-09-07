# Matcher

## Overview

`Matcher` is the event-to-handler router. It pre-computes a dispatch table from the event
and handler registries, routing event types to matching handlers efficiently.
It is auto-constructed by `EventBus` — users do not need to create it directly.

---

## Architecture

```text
EventRegistry ──┐
                ├──> Matcher ──> {event_name: [handler_id, ...]}
HandlerRegistry ┘      ↑
                       │ auto-rebuild on version change
```

`Matcher` replaced the old `EventHandlerRegistry.get_handlers()` matching logic.
Registries focus on storage (CRUD), matching is a separate concern — single responsibility.

---

## Matcher

```python
class Matcher:
    def __init__(
        self,
        event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry
    ) -> None

    def match(self, event_type: str) -> List[tuple[str, EventHandler]]

    @property
    def dispatch_table(self) -> Dict[str, List[tuple[str, EventHandler]]]
```

### Constructor Parameters

| Parameter | Type | Description |
| - | - | - |
| `event_registry` | `EventRegistry` | Provides all known event type names. |
| `handler_registry` | `EventHandlerRegistry` | Provides all registered handlers and their subscriptions. |

The dispatch table is pre-computed at construction time by iterating over all known event types.

### Pre-computed Dispatch Table

The dispatch table is a `{event_name: [handler_id, ...]}` mapping:

- **Exact `str` subscriptions** → reverse index `{event_type: [hid, ...]}`, O(1) lookup
- **`Regex` subscriptions** → separate scan list, only `fullmatch` against regex subscriptions

Only handler IDs (strings) are stored in memory. `match()` resolves them to `(hid, handler)` tuples on demand from the registry.

### Version-Aware Caching

Both registries expose a `version` property (incremented on every add/remove). `Matcher` automatically compares versions on every `match()` / `dispatch_table` access, rebuilding the dispatch table when a change is detected — no manual notification needed.

```text
match() call → check version (O(1) int compare) → stale? → _rebuild()
                                                   ↓ fresh
                                            lookup table (O(1) dict)
```

### `match()`

```python
def match(self, event_type: str) -> List[tuple[str, EventHandler]]
```

- Known event → O(1) dispatch table lookup
- Returns `(handler_id, handler)` tuple list
- Auto-detects registry version changes

### `dispatch_table`

```python
@property
def dispatch_table(self) -> Dict[str, List[tuple[str, EventHandler]]]
```

Returns a read-only snapshot of the current dispatch table. Auto-refreshes on version change. Primarily used for debugging and observability.

---

## Relationship with EventBus

`EventBus` internally creates a `Matcher` at construction time. The dispatch loop uses `self._matcher.match(event.name)` to find handlers:

```python
class EventBus:
    def __init__(self, event_registry, handler_registry, ...):
        self._matcher = Matcher(event_registry, handler_registry)  # internal, automatic

    async def _dispatch_loop(self):
        ...
        for handler_id, handler in self._matcher.match(event.name):
            ...
```

Users do not need to interact with `Matcher` directly — just provide the two registries.

---

## Why Pre-compute?

Without pre-computation, every event dispatch would require iteration over all handlers
and regex matching. With pre-computation:

- Exact `str` subscriptions → O(1) reverse-index lookup
- `Regex` subscriptions → scan list only (no event iteration)
- Version-aware caching → rebuild only when registries change, not per dispatch
