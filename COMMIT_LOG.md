# Commit Log — `infinity_bus`

> 共 **95** 个提交 · 18 个版本标签 · 2026-06-07 ~ 2026-07-12

## 版本标签

| Tag | Commit |
| --- | ------ |
| `v2.2.0` | `e27164d` |
| `v2.1.0` | `6c0005b` |
| `v2.0.0` | `b1383d8` |
| `v1.5.6` | `7ffc90b` |
| `v1.5.5` | `8abe9fa` |
| `v1.5.4` | `2a4a243` |
| `v1.5.3` | `38974e5` |
| `v1.5.2` | `4de6258` |
| `v1.5.1` | `4311c8e` |
| `v1.5.0` | `6a102ff` |
| `v1.4.2` | `0009ff7` |
| `v1.4.1` | `fb7cbed` |
| `v1.4.0` | `3549ddb` |
| `v1.3.6` | `0b7f50f` |
| `v1.3.5` | `599fbd3` |
| `v1.3.3` | `c78e3dc` |
| `v1.3.2` | `37b1eb9` |
| `v1.3.1` | `eb1a363` |

---

## 全部提交

### 2026-07-12

- **`e6f9f58`** `fix(middleware): race condition in add/insert rollback, type hints; docs overhaul`
  - Fixed `add()`/`insert()` race: middleware is now added to chain **before** `on_setup()`, and removed on failure for clean rollback
  - Corrected `BeforePublishNext`/`OnPublishNext`/`OnPublishErrorNext` return type from `Any` to `Awaitable[None]`
  - Changed `raise e` to bare `raise` in `_execute_handler` to preserve original traceback
  - Changed project description to English in `pyproject.toml`
  - Docs (EN+ZH): expanded bus/handler/matcher/middleware with usage examples, error types, system events, onion ordering, hot-reload patterns
  - Docs (EN): event_forward anti-recursion mechanism and full parameter reference
  - Docs (ZH): templates.md fully translated

### 2026-07-09

- **`e27164d`** `feat(simple_handler): introduce GenericEventHandler, remove _handle_timeout override`
  - Added GenericEventHandler as intermediate base class between EventHandler and decorator-generated handlers; direct instantiation raises NotImplementedError
  - @handler decorator now returns Type[GenericEventHandler] instead of Type[EventHandler] for better type safety
  - Generated _Handler.**init**() no longer accepts_handle_timeout parameter; timeout is now fixed at decoration time via handle_timeout kwarg
  - Switched from super().**init** to explicit EventHandler.**init** to bypass GenericEventHandler's NotImplementedError guard
  - Fixed subscriptions closure capture: event_name → event_decl.name in **init**
  - Removed unused imports (Any, Optional, Event) from simple_handler_test
  - Bumped version to 2.2.0
  - BREAKING: generated handler subclasses no longer accept _handle_timeout arg

### 2026-07-07

- **`6c0005b`** `chore: bump version to 2.1.0`

### 2026-07-06

- **`b1383d8`** `feat(middleware): hot-reload support with async CRUD API`
  - MiddlewareChain add/insert/remove/clear changed to async
  - Runtime add/remove triggers on_setup/on_teardown immediately
  - on_publish_error refactored to chain-of-responsibility (build_on_publish_error)
  - teardown made idempotent (safe to call repeatedly)
  - Snapshot-based iteration in setup/teardown/clear to prevent race
  - _bus tracking prevents setup-after-setup and add-during-teardown
  - Documented teardown race condition as middleware author responsibility
  - Added TestMiddlewareHotReload (10 new tests)
  - BREAKING: add/insert/remove/clear are now async (await required)

### 2026-06-20

- **`7ffc90b`** `chore: bump version to 1.5.6`

### 2026-06-14

- **`6dd677f`** `docs: fix some reference`
- **`8abe9fa`** `chore: bump version to 1.5.5`
- **`98cd678`** `fix(middlewares): jsonl write race`
- **`88d1f45`** `docs: updete dependency count line`
- **`2a4a243`** `chore: bump version to 1.5.4`
- **`bd05954`** `fix(middlewares): jsonl logger output error`
- **`38974e5`** `chore: bump version to 1.5.3`
- **`62b568a`** `fix(templates): export pipe errors` + `docs: sync changes`

### 2026-06-13

- **`37f4132`** `docs(agents): add write-docs skill, and cross-references`
- **`137e1f9`** `docs: update comparison table to v1.5.2`
- **`a873b43`** `docs(ENGINEERING.md): fix linter waring`
- **`f37c75d`** `docs: add contributing guide`
- **`4de6258`** `chore: bump version to 1.5.2` + `docs: add commit message convention to ENGINEERING.md`
- **`9e164bf`** 新增handler快速定义装饰模板

### 2026-06-12

- **`9e796fb`** 指标中间件文档补全
- **`3be29cb`** 文档小修复
- **`214d504`** 新增指标上报中间件+双语文档

### 2026-06-11

- **`4311c8e`** v1.5.1
- **`a57a8e5`** 文档fix
- **`e14a2aa`** 新增模板中间件快捷方法
- **`b0d8e4e`** AGENTS文档修复
- **`772dfaf`** 智能体工程学修复
- **`97b2246`** Agent工程学修复
- **`1fc8c60`** 通用技能SKILL
- **`1f02ecf`** 工作流改进
- **`4d63f2d`** AI引导小改
- **`d86c1ca`** 基础智能体工程学设计
- **`3e41d8d`** 添加事务性注册
- **`bbfb26f`** 文档更新
- **`6a102ff`** v1.5.0
- **`28c3ca3`** 测试修复v2
- **`6fbfe92`** 测试修复
- **`309d54c`** 改进派发器逻辑
- **`0009ff7`** v1.4.2
- **`8775a37`** 支持动态中间件删改+文档修复
- **`b807699`** README小改
- **`194ce4d`** 小修复+测试升级
- **`81d0ad3`** README升级
- **`fb7cbed`** 升级版本标注
- **`2bbd45c`** 跨总线转发中间件+大量小修复

### 2026-06-10

- **`3549ddb`** Handler订阅表改进
- **`03d9c4b`** README改进
- **`22bc1ef`** README改进
- **`6c9aba9`** 修改文档覆盖表述
- **`26910c2`** 弃用 AsyncIterator
- **`59c6a3f`** 修复开发工具集
- **`0b7f50f`** v1.3.6
- **`c9b4fee`** CI改进加入pre-commit
- **`24c8b3f`** 依赖模块化 · 教程改进 · CI测试仅拉取test子包
- **`eb1232f`** README改进
- **`7f60719`** 工程化文档和README改进
- **`599fbd3`** 添加文档索引，升级readme
- **`ba3770e`** 添加文档字符串门控
- **`e4071e6`** 中间件模板文档
- **`3bc3f35`** 拆分中间件模板
- **`ffdd7f0`** 添加pre-commit
- **`7c711dc`** 文档升级
- **`8cc3297`** icons
- **`c78e3dc`** (版本号修改)
- **`18f5433`** (CI 配置修改)
- **`43c910a`** `fix: exclude .gitignore from release assets, restore workflow_dispatch`
- **`37b1eb9`** (版本号修改)
- **`c419f83`** (README 修改)
- **`eb1a363`** (CI 配置修改)
- **`ee11176`** (CI 配置修改)
- **`9c6c4db`** (pyproject.toml 修改)
- **`59653aa`** `fix: release step only on tag push`
- **`de0a671`** v1.3.0
- **`d5ee475`** 改名
- **`3d4fb00`** 改名

### 2026-06-09

- **`0618aa8`** 事件模型改进文档FIX
- **`431caae`** 添加预设中间件工厂
- **`ee2cbd9`** 添加递归防护中间件
- **`caa7c39`** 改进事件链模型和日志功能
- **`9e6443c`** 多系统测试
- **`a453c5a`** 版本升级
- **`b57c177`** 添加中间件+文档Fix

### 2026-06-08

- **`be89ac3`** (README 修改)
- **`0fbcf2d`** (docs 修改)
- **`a18c3e9`** 文档修复
- **`fa15923`** 注册器文档
- **`4c1b2f1`** 测试修复
- **`3d3bce9`** 测试修复+小修小补
- **`e772e5e`** core.py 重构:
  - 去掉 2 个 `# type: ignore` → strict PyLance 零例外
  - 停机参数 ClassVar → ShutdownConfig(BaseModel) → 实例级 + JSON 加载
  - regex_cache dict → OrderedDict LRU(256) → 防无限膨胀
  - TaskErrorPayload 加了 handler_id → 精确错误追踪
  - get_handlers 返回 (id, handler) 元组 → API 干净无重复
  - 删了 register_handler → 去耦合
  - get_active_task_count/get_queue_size → property → 风格统一
  - `__version__` → "1.2.0"

### 2026-06-07

- **`dc3498d`** 修复包结构
- **`a1c799c`** 修复 giturl
- **`f86458f`** 修复一些小东西
- **`903a3cb`** Test和CD脚本
- **`1173058`** README
- **`f1e4abb`** 第一版提交
