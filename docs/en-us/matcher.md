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

| Method/Property | Description |
| - | - |
| `match(event_type)` | Returns `[(handler_id, handler), ...]` for matched handlers. O(1) table lookup. |
| `dispatch_table` | Read-only snapshot of the full dispatch table. Auto-refreshes on version change. |

---

## How It Works

### Pre-computed Dispatch Table

On construction (and whenever registries change), single-pass rebuild:

1. **Separate subscriptions**:
   - Exact `str` → build reverse index `{event_name: [handler_id, ...]}`
   - `Regex` → scan list (checked against each event name)
2. **Merge per event**: for each known event name, combine reverse-index hits + regex scan results
3. **Deduplicate** handlers (no duplicate invocations)

### Version-Aware Caching

```python
# Registries change → version increments → next match() auto-rebuilds
events.register(NewEvent)
handlers.register(NewHandler())
# Matcher._is_stale() → True → _rebuild() on next match()
```

The dispatch loop simply calls `matcher.match(event.name)` — version checking
and rebuilding are transparent.

---

## Why Pre-compute?

Without pre-computation, every event dispatch would require iteration over all handlers
and regex matching. With pre-computation:

- Exact `str` subscriptions → O(1) reverse-index lookup
- `Regex` subscriptions → scan list only (no event iteration)
- Version-aware caching → rebuild only when registries change, not per dispatch
