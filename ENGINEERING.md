# 工程质量

InfinityBus 是一个工程纪律驱动的项目。以下是所有质量保障措施的完整说明。

---

## 类型安全

### Pyright Strict

项目启用 [Pyright](https://github.com/microsoft/pyright) **strict** 模式，这是 Python 生态中最严格的类型检查级别。

```toml
# pyproject.toml
[tool.pyright]
typeCheckingMode = "strict"
reportMissingTypeStubs = false
reportMissingImports = "error"
```

**生产代码零遮蔽。** 整个 `src/` 目录没有任何 `# type: ignore` 或 `# pyright: ignore` 注释。

| 指标 | 数值 |
| - | - |
| 类型检查模式 | `strict` |
| 生产代码 `# type: ignore` | **0** |

### Ruff Lint

仅忽略两条规则，均有明确理由：

```toml
[tool.ruff.lint]
ignore = [
    "E501",     # 行长度由 formatter 处理
    "ASYNC109", # async 函数的 timeout 参数是有意的 API 设计
]
```

启用规则组：`E`（pycodestyle）、`F`（pyflakes）、`I`（isort）、`ASYNC`（异步最佳实践）、`PLE`（pylint 错误）。

---

## 文档覆盖

使用 [interrogate](https://github.com/econchick/interrogate) 强制公开 API 的 docstring 覆盖。

```toml
[tool.interrogate]
fail-under = 60
ignore-init-method = true
ignore-init-module = true
ignore-magic = true
ignore-private = true
ignore-semiprivate = true
ignore-nested-functions = true
```

| 指标 | 数值 |
| - | - |
| 公开 API docstring 覆盖 | **85%+** |
| 最低阈值 | 60% |
| 文档文件 | 16 篇 `.md` |

---

## 测试

### 覆盖率

```text
160 passed · 1 deselected · 0 failed · 11.7s
总覆盖率: 92%（1119 statements / 94 missed）
```

7 个模块达到 **100%** 覆盖，最低模块 > 82%。

### 架构

```text
tests/
├── conftest.py             共享 fixtures、测试用 Payload/Handler、工具函数
├── bus_test.py             EventBus 集成测试
├── event_test.py           Event / Declaration / Registry
├── handler_test.py         EventHandler / Registry
├── middleware_test.py      Middleware / MiddlewareChain
└── templates/
    ├── expect_test.py
    ├── request_test.py
    ├── pipe_test.py
    ├── register_test.py
    └── middlewares/
        ├── event_block_test.py
        ├── event_transform_test.py
        ├── logging_test.py
        ├── rate_limit_test.py
        └── recursion_guard_test.py
```

测试结构完全镜像源码结构。`pytest-asyncio` + `asyncio_mode = "auto"`，异步测试零样板代码。

### 运行

```bash
pytest --cov=src -v                     # 全量
pytest -m "not slow"                    # 排除压力测试（默认）
pytest tests/bus_test.py -v             # 单模块
pytest tests/templates/ -v              # 模板层
```

---

## Pre-commit 门禁

每次提交自动执行，全部通过才允许 commit：

| 钩子 | 范围 | 作用 |
| - | - | - |
| `ruff-check` | `^src/` | Lint + 自动修复 |
| `ruff-format` | `^src/` | 格式化 |
| `pyright` | `^src/` | `--level error` 类型检查 |
| `interrogate` | `^src/` | docstring 覆盖 >= 60% |
| `check-ast` | 全项目 | Python 语法校验 |
| `check-toml/yaml/json` | 全项目 | 配置文件校验 |
| `check-merge-conflict` | 全项目 | 合并冲突残留 |
| `end-of-file-fixer` | 非 `.py` | 文件末尾换行 |
| `trailing-whitespace` | 非 `.py` | 行尾空白（`.py` 由 ruff 处理） |

```bash
pre-commit run --all-files   # 手动全量运行
```

---

## 模块化

### KISS 原则

| 指标 | 数值 |
| - | - |
| 最大单文件 | 300+ 行 (`bus.py`) |
| 模块平均 | ~130 行/文件 |
| 生产文件数 | 16 个 `.py` |

每个文件一个职责，打开瞬间看完。新增功能不改旧代码。

### 目录结构

```text
src/event_bus/
├── __init__.py           公开 API 入口
├── event.py              事件系统（Event / Declaration / Registry）
├── handler.py            处理器系统（EventHandler / Registry）
├── bus.py                事件总线（EventBus / Proxy / 停机）
├── middleware.py          中间件系统（Middleware / Chain / 洋葱模型）
└── templates/
    ├── expect.py          一次性事件监听
    ├── request.py         请求-响应 RPC
    ├── pipe.py            双向管道
    ├── register.py        模块级批量注册
    └── middlewares/
        ├── event_block.py       事件屏蔽
        ├── event_transform.py   事件转换
        ├── logging.py           JSONL + SQLite 日志
        ├── rate_limit.py        速率限制
        └── recursion_guard.py   递归防护
```

---

## 版本策略

- **SemVer**：`MAJOR.MINOR.PATCH`
- Git tag 触发 PyPI 发布

---

## CI

测试矩阵覆盖 Python 3.12+，每次 push 自动运行 `pytest` + `pre-commit`。

```yaml
# .github/workflows/test.yml
pytest --cov=src --cov-report=xml
```
