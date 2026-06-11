# SKILL: 编写测试

**触发**：用户要求为新功能或 bug 修复编写测试。

## 规范

- 测试文件与源码文件一一对应，路径镜像。
- 使用 `tests/conftest.py` 中的共享 fixture（`event_registry` / `handler_registry` / `bus` 等）。
- 每个测试函数应独立、可重入，不依赖执行顺序。
- 覆盖：正常路径、边界条件、异常情况、并发场景。
- 运行验证：`pytest tests/ --cov=event_bus --cov-report=term-missing -q --tb=line`。
