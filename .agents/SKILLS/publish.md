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

2. **更新版本号** — 编辑 `pyproject.toml` 中的 `version` 字段，遵循 [SemVer](https://semver.org/lang/zh-CN/)：

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

4. **提交版本更新**

   ```bash
   git add pyproject.toml
   git commit -m "chore: bump version to <新版本号>"
   git push
   ```

5. **打 Tag 并推送** — Tag 必须以 `v` 开头，否则不会触发 publish workflow：

   ```bash
   git tag -a v<新版本号> -m "Release v<新版本号>"
   git push origin v<新版本号>
   ```

6. **监控 CI/CD** — 前往 [Actions](https://github.com/yinbailiang/event_bus/actions) 查看 workflow 执行状态。

7. **验证发布**
   - PyPI: <https://pypi.org/project/infinity_bus/>
   - Releases: <https://github.com/yinbailiang/event_bus/releases>

   ```bash
   uv pip install --index-url https://pypi.org/simple/ infinity_bus==<新版本号>
   ```

## 注意事项

- **禁止手动创建 GitHub Release**，CI/CD 自动创建并附加 `.whl` / `.tar.gz`。
- CI 失败时先修复问题，再删除错误 tag 重新打：

  ```bash
  git tag -d v<版本号>
  git push --delete origin v<版本号>
  ```

- PyPI 版本**不可覆盖**，发版前务必确认版本号正确。
