# SKILL: 代码审查前检查

**触发**：提交代码或用户要求 pre-commit 检查。

## 步骤

1. `uv run ruff check src/` — lint 检查
2. `uv run ruff format --check src/` — 格式检查
3. **零遮蔽检查**：`src/` 目录下不得出现任何 `# type:` 或 `# pyright` 注释
   - Windows: `Select-String -Path src\**\*.py -Pattern "# type:|# pyright"` 确认无匹配
   - Unix: `grep -r "# type:\|# pyright" src/` 确认无输出
4. `uv run pyright src/` — 类型检查（必须零错误）
5. `uv run interrogate src/event_bus/` — docstring 覆盖率
6. `uv run pytest tests/ --cov=event_bus --cov-report=term-missing -q --tb=line` — 测试+覆盖率
7. 如有问题，修复后重新全量运行。
