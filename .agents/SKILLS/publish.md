# SKILL: 项目发布

**触发**：发布、release、publish、打版、上线、PyPI、版本号升级。

## 流程概览

```text
更新版本号 → 本地验证 → 提交推送 → 打 Git Tag → 推送 Tag → CI/CD 自动发布
```

推送 `v*` 格式的 tag 后，[publish.yml](../../.github/workflows/publish.yml) 自动执行：

1. **pre-commit** — Ruff / Pyright / interrogate 门禁
2. **test** — 三平台 × 两 Python 版本矩阵测试（覆盖率 ≥90%）
3. **build-and-publish** — 构建 wheel → GitHub Release → PyPI 发布

## 步骤

1. **确认状态**

   ```bash
   uv run python -c "import tomllib; print(tomllib.load(open('pyproject.toml','rb'))['project']['version'])"
   git status
   git branch
   ```

2. **更新版本号** — 编辑 `pyproject.toml` 和 `__init__.py` 中的 `version` 字段，遵循 [SemVer](https://semver.org/lang/zh-CN/)：

   | 类型 | 场景 | 示例 |
   | - | - | - |
   | patch | Bug 修复、文档更新 | 1.5.0 → 1.5.1 |
   | minor | 新功能、向后兼容 | 1.5.0 → 1.6.0 |
   | major | 破坏性 API 变更 | 1.5.0 → 2.0.0 |

3. **本地最终验证**

   ```bash
   uv run pre-commit run --all-files
   uv run pytest tests/ --cov=event_bus --cov-report=term-missing -v
   ```

4. **同步工程基准快照**（每次发版维护一次，防止 ENGINEERING / COMMIT_LOG / README 漂移）

   以本次全量验证输出为准，更新以下文件的基准数据：

   | 文件 | 更新内容 | 权威来源 |
   | - | - | - |
   | `ENGINEERING.md` | 测试数 / 覆盖率、100% 模块清单、模块树（含新增 `.py`） | `pytest` 摘要 |
   | `COMMIT_LOG.md` | 顶部版本标签表新增行 + 本版本提交条目 | `git log --oneline <上一tag>..HEAD` |
   | `README.md` / `README_zh.md` | 对照表 Baseline Version = `v<版本> <commit>`、测试数行 | `git rev-parse HEAD` |

   若发现基准已漂移（例如模块树缺文件、覆盖数与实际不符、COMMIT_LOG 落后），本次发版应一并修正。

5. **提交版本更新**

   ```bash
   git add -A   # 版本号 + 基准快照（如需可拆为独立 docs 提交）
   git commit -m "chore: bump version to <新版本号>"
   git push
   ```

6. **打 Tag 并推送** — Tag 必须以 `v` 开头，否则不会触发 publish workflow：

   ```bash
   git tag -a v<新版本号>
   git push origin v<新版本号>
   ```

7. **监控 CI/CD** — 前往 [Actions](https://github.com/yinbailiang/event_bus/actions) 查看 workflow 执行状态。

8. **验证发布**
   - PyPI: <https://pypi.org/project/infinity_bus/>
   - Releases: <https://github.com/yinbailiang/event_bus/releases>

   ```bash
   uv pip install --index-url https://pypi.org/simple/ infinity_bus==<新版本号>
   ```

## 注意事项

- **禁止手动创建 GitHub Release**，CI/CD 自动创建并附加 `.whl` / `.tar.gz`。
- 慢测（`@pytest.mark.slow`）已由 CI 全量覆盖：`test.yml` / `publish.yml` 均以 `-m ""` 运行（含慢测），本地 `pytest` 默认跳过属预期。
- CI 失败时先修复问题，再删除错误 tag 重新打：

  ```bash
  git tag -d v<版本号>
  git push --delete origin v<版本号>
  ```

- PyPI 版本**不可覆盖**，发版前务必确认版本号正确。
