# SKILL: 修改核心 API

**触发**：用户要求修改 `EventBus` / `Event` / `EventHandler` / `Middleware` 等核心类的公开 API。

## 约束

1. 尽量避免破坏公开API，或保证API向后兼容
2. 修改后运行全量类型检查：`uv run pyright src/`。
3. 修改后运行全量测试：`uv run pytest tests/ --cov=event_bus --cov-report=term-missing -q`。
4. 更新 `src/event_bus/__init__.py` 中的 `__all__`。
5. 更新 `docs/` 中对应文档。
