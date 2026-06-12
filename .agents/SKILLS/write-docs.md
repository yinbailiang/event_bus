# SKILL: 编写文档

**触发**：用户要求写文档、补文档、更新文档、添加示例。

## 规范

- **双语镜像**：`docs/zh-cn/` 与 `docs/en-us/` 目录结构完全对应，新增文档必须同时创建中英版本。
- **文件命名**：与 `src/event_bus/` 模块路径一一对应。如 `src/event_bus/templates/handler.py` → `docs/zh-cn/templates/handler.md`。
- **更新索引**：新增文档后必须更新对应层级的索引文件：
  - 模板层 → `docs/*/templates/templates.md`
  - 中间件层 → `docs/*/templates/middlewares/middlewares.md`
  - 核心层 → `docs/*/event_bus.md`
- **结构约定**：概述 → 签名/参数表 → 使用场景 → 工作流程 → 示例 → 注意事项。

## 步骤

1. 确认源码模块位置，推导出对应的文档路径。
2. 先写中文版，再翻译为英文版（或同步写作）。
3. 更新索引文件，添加新文档的链接。
4. 检查 README 中特性表、架构表是否需要同步更新。
5. 运行 `uv run interrogate src/event_bus/` 验证 docstring 覆盖率未下降。

## 风格

- 中文文档：术语统一（处理器、负载、总线、中间件、注册表）。
- 英文文档：使用项目既有术语（handler、payload、bus、middleware、registry）。
- 示例代码优先展示最简用法，复杂场景放在后面。
- 参数表用 Markdown 表格，包含类型和说明。
