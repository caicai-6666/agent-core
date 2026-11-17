# 提取来源与文件清单

原项目基线提交：`d4ccd12697d6d13cf4574c3dcb896884e623d7dc`。来源为知识后端 `app/agent/core/`，提取时没有修改该目录。

## 算法保留核对

原 Core 共 104 个 Python 文件；新库保留其中 100 个。仅进行包命名空间替换后逐字一致的有 93 个，包括 runtime、上下文预算、压缩算法、原生轨迹和工具注册状态机。

以下 7 个文件存在明确适配修改：

- `src/agent_core/core/file/pdf.py`：解除业务身份/环境配置依赖，或调整通用提示词与显式配置默认值。
- `src/agent_core/core/model.py`：解除业务身份/环境配置依赖，或调整通用提示词与显式配置默认值。
- `src/agent_core/core/prompt/__init__.py`：解除业务身份/环境配置依赖，或调整通用提示词与显式配置默认值。
- `src/agent_core/core/prompt/role.py`：解除业务身份/环境配置依赖，或调整通用提示词与显式配置默认值。
- `src/agent_core/core/subgraph/file_retrieval/schema.py`：解除业务身份/环境配置依赖，或调整通用提示词与显式配置默认值。
- `src/agent_core/core/tool/__init__.py`：解除业务身份/环境配置依赖，或调整通用提示词与显式配置默认值。
- `src/agent_core/core/tool/external_expert.py`：解除业务身份/环境配置依赖，或调整通用提示词与显式配置默认值。

以下业务文件不进入通用库，宿主可用工具/提示词接口接回：

- 原 `app/agent/core/prompt/company.py`。
- 原 `app/agent/core/prompt/platform_collaboration.py`。
- 原 `app/agent/core/tool/platform_a2a.py`。
- 原 `app/agent/core/tool/sub_management_platform.py`。

---

## 本次新增文件路径

以下均位于新库目录。`core/` 是保留的运行机制；`agent/tasks/storage/memory/providers` 是新的可移植接入层；`tests` 是可分发回归；`description` 与随包 docs 提供开发者规范。此清单记录初次提取范围，当前版本以仓库提交和版本标签为准。

| 文件 | 用途 |
| --- | --- |
| `AGENTS.md` | 构建、安装或项目入口 |
| `MANIFEST.in` | 构建、安装或项目入口 |
| `README.md` | 构建、安装或项目入口 |
| `src/build_support.py` | 构建、安装或项目入口 |
| `description/architecture/data/storage.md` | 开发规范、接入契约或维护说明 |
| `description/architecture/system/mechanisms.md` | 开发规范、接入契约或维护说明 |
| `description/capability/application/integration.md` | 开发规范、接入契约或维护说明 |
| `description/documentation.md` | 开发规范、接入契约或维护说明 |
| `description/project.md` | 开发规范、接入契约或维护说明 |
| `description/readme.md` | 开发规范、接入契约或维护说明 |
| `description/standard/agent-context-management.md` | 开发规范、接入契约或维护说明 |
| `description/standard/experiment.md` | 开发规范、接入契约或维护说明 |
| `description/standard/prompt-engineering.md` | 开发规范、接入契约或维护说明 |
| `description/standard/tool-feedback.md` | 开发规范、接入契约或维护说明 |
| `examples/minimal.py` | 可运行的业务定制示例 |
| `pyproject.toml` | 构建、安装或项目入口 |
| `src/agent_core/__init__.py` | 公共 API、持久化编排、数据库或模型适配 |
| `src/agent_core/__main__.py` | 公共 API、持久化编排、数据库或模型适配 |
| `src/agent_core/adapters/__init__.py` | 公共 API、持久化编排、数据库或模型适配 |
| `src/agent_core/adapters/sqlite.py` | 公共 API、持久化编排、数据库或模型适配 |
| `src/agent_core/agent.py` | 公共 API、持久化编排、数据库或模型适配 |
| `src/agent_core/assets/tool-tag/deepseek-v4.1-flash.txt` | 包内模型工具格式模板 |
| `src/agent_core/assets/tool-tag/glm-5.3-flash.txt` | 包内模型工具格式模板 |
| `src/agent_core/assets/tool-tag/qwen3.8-flash-next-nvfp4.txt` | 包内模型工具格式模板 |
| `src/agent_core/config/__init__.py` | 公共 API、持久化编排、数据库或模型适配 |
| `src/agent_core/config/deepseek.py` | 公共 API、持久化编排、数据库或模型适配 |
| `src/agent_core/config/mllm.py` | 公共 API、持久化编排、数据库或模型适配 |
| `src/agent_core/core/__init__.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/context.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/context_budget/__init__.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/context_budget/calculator.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/context_budget/fifo.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/context_rendering/__init__.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/context_rendering/attachment.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/context_rendering/block.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/context_rendering/history.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/context_rendering/messages.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/context_rendering/page.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/context_rendering/summary.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/context_rendering/task.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/context_rendering/workspace.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/expert.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/file/__init__.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/file/cache.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/file/image_viewer.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/file/pdf.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/file/registry_viewer.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/guidance.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/journal.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/json_codec.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/model.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/native_messages.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/output.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/page_display.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/prompt/__init__.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/prompt/embedding.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/prompt/guidance.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/prompt/interaction.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/prompt/role.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/prompt/workspace.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/query_pool.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/runtime.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/__init__.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/context_management/__init__.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/context_management/agent.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/context_management/prompt.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/context_management/schema.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/context_management/topic_summary/__init__.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/context_management/topic_summary/input_filter.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/context_management/topic_summary/prompt.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/context_management/topic_summary/rendering.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/context_management/topic_summary/sampling.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/context_management/topic_summary/schema.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/context_management/topic_summary/workflow.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/context_management/validation.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/context_management/workspace_compression/__init__.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/context_management/workspace_compression/errors.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/context_management/workspace_compression/pending.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/context_management/workspace_compression/runtime.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/file_retrieval/__init__.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/file_retrieval/agent.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/file_retrieval/pool.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/file_retrieval/prompt.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/file_retrieval/ranking.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/file_retrieval/schema.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/file_retrieval/tool.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/file_retrieval/workflow.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/memory_retrieval/__init__.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/memory_retrieval/agent.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/memory_retrieval/pool.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/memory_retrieval/prompt.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/memory_retrieval/rendering.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/memory_retrieval/schema.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/memory_retrieval/session.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/memory_retrieval/tool.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/memory_retrieval/workflow.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/web_page/__init__.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/web_page/image.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/web_page/node.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/web_page/prompt.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/web_page/routing.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/web_page/schema.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/web_page/state.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/subgraph/web_page/workflow.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/tool/__init__.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/tool/base/__init__.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/tool/base/finish_task.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/tool/context.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/tool/current_time.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/tool/date_calculator.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/tool/external_expert.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/tool/feedback.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/tool/file_candidates.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/tool/file_search.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/tool/hooks.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/tool/image_viewer.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/tool/memory_search.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/tool/memory_viewer.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/tool/progress.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/tool/registry.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/tool/registry_file.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/tool/result.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/tool/web_page.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/tool/web_search.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/tool/workspace.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/web.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/core/workspace.py` | 迁移原核心模块；职责见机制清单 |
| `src/agent_core/infrastructure/__init__.py` | 公共 API、持久化编排、数据库或模型适配 |
| `src/agent_core/infrastructure/deepseek.py` | 公共 API、持久化编排、数据库或模型适配 |
| `src/agent_core/infrastructure/mllm.py` | 公共 API、持久化编排、数据库或模型适配 |
| `src/agent_core/infrastructure/model_concurrency.py` | 公共 API、持久化编排、数据库或模型适配 |
| `src/agent_core/infrastructure/pdf_lock.py` | 公共 API、持久化编排、数据库或模型适配 |
| `src/agent_core/infrastructure/public_dns.py` | 公共 API、持久化编排、数据库或模型适配 |
| `src/agent_core/infrastructure/registry_file.py` | 公共 API、持久化编排、数据库或模型适配 |
| `src/agent_core/infrastructure/tool_tag.py` | 公共 API、持久化编排、数据库或模型适配 |
| `src/agent_core/infrastructure/vllm_tokenizer.py` | 公共 API、持久化编排、数据库或模型适配 |
| `src/agent_core/memory.py` | 公共 API、持久化编排、数据库或模型适配 |
| `src/agent_core/model/__init__.py` | 公共 API、持久化编排、数据库或模型适配 |
| `src/agent_core/model/node_persistence.py` | 公共 API、持久化编排、数据库或模型适配 |
| `src/agent_core/model/question_file.py` | 公共 API、持久化编排、数据库或模型适配 |
| `src/agent_core/model/question_tree.py` | 公共 API、持久化编排、数据库或模型适配 |
| `src/agent_core/output_stream.py` | 公共 API、持久化编排、数据库或模型适配 |
| `src/agent_core/providers.py` | 公共 API、持久化编排、数据库或模型适配 |
| `src/agent_core/py.typed` | 公共 API、持久化编排、数据库或模型适配 |
| `src/agent_core/repository/__init__.py` | 公共 API、持久化编排、数据库或模型适配 |
| `src/agent_core/repository/question_tree.py` | 公共 API、持久化编排、数据库或模型适配 |
| `src/agent_core/storage.py` | 公共 API、持久化编排、数据库或模型适配 |
| `src/agent_core/tasks.py` | 公共 API、持久化编排、数据库或模型适配 |
| `src/agent_core/testing.py` | 公共 API、持久化编排、数据库或模型适配 |
| `src/agent_core/tools.py` | 公共 API、持久化编排、数据库或模型适配 |
| `src/agent_core/vectors.py` | 公共 API、持久化编排、数据库或模型适配 |
| `tests/conftest.py` | 隔离回归或测试装配 |
| `tests/helpers.py` | 隔离回归或测试装配 |
| `tests/test_compaction_internal_feedback.py` | 隔离回归或测试装配 |
| `tests/test_context_feedback.py` | 隔离回归或测试装配 |
| `tests/test_date_calculation_feedback.py` | 隔离回归或测试装配 |
| `tests/test_file_candidates_feedback.py` | 隔离回归或测试装配 |
| `tests/test_file_search_feedback.py` | 隔离回归或测试装配 |
| `tests/test_library.py` | 隔离回归或测试装配 |
| `tests/test_memory_search_feedback.py` | 隔离回归或测试装配 |
| `tests/test_memory_view_feedback.py` | 隔离回归或测试装配 |
| `tests/test_native_tool_content.py` | 隔离回归或测试装配 |
| `tests/test_workspace_feedback.py` | 隔离回归或测试装配 |
| `description/architecture/system/extraction.md` | 提取基线与逐文件修改范围 |
