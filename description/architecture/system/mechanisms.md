# 核心机制迁移清单

库从知识沉淀后端现有 Core 提取算法，新增可移植持久宿主，不用演示循环替代原有实现。当前后端的生产运行路径仍使用原实现；库通过隔离实验验收后，可由后续任务逐步替换。

---

## 已保留与重新装配

| 机制 | 库中的实现 | 责任 |
| --- | --- | --- |
| 原生工具循环、有界纠错、轮数限制、取消清理 | `core/runtime.py` | 保留原算法 |
| 主循环与内部子 Agent 的 token 预算 | `core/context_budget/` | 保留 3:7 工作区/轨迹预算、80% 提醒、95% 自动整理 |
| FIFO 完整任务前缀、累计摘要 | `core/context_budget/fifo.py`、`core/subgraph/context_management/` | 不压缩当前活动任务，不遗漏终态语义 |
| 主题规划、并行主题摘要、来源校验与合并 | `core/subgraph/context_management/topic_summary/` | 原实现，审计串行保存 |
| 工作区压缩、待提交编辑保护 | `core/subgraph/context_management/workspace_compression/` | 原实现，候选验证后再提交 |
| 原生工具对、私有推理与纠错轨迹隔离 | `core/native_messages.py`、`core/runtime.py` | 原实现，成功后清除临时失败 |
| 模块化系统提示词装配与工作区消息按需卸载（`PromptOptions`） | `core/prompt/`、`core/context_rendering/messages.py`、`core/runtime.py`、`agent.py`、`sessions.py`、`runtime.py` | 按工具装配与工作区内容自动开关工作区协议、工作区 `user` 消息与 `compact_context` 指引；支持关闭交互/相对时间或传入自定义 `prompt_builder` |
| 临时请求提示（开场提示与周期进展提醒） | `core/guidance.py`、`agent.py`、`sessions.py`、`runtime.py` | 仅附加到单次模型请求，不入历史或工作区；支持实例默认与单任务内存覆盖 |
| Pydantic 参数、字段说明、工具独立顺序钩子（含 `with_hooks` 与 `tool_hooks`） | `core/tool/registry.py`、`hooks.py`、`agent.py`、`sessions.py`、`runtime.py` | 原实现；支持对全部静态与动态内置工具按名附加钩子，新业务钩子自动位于末尾 `FeedbackOnlyHook` 之前，展示与结构化结果分离 |
| 工具执行期运行时上下文（`ContextVar`） | `core/tool/runtime_context.py`、`core/runtime.py`、`core/tool/file_search.py` | 执行边界内向组合工具暴露只读 `core`/`journal`/`thinking_effort`，支持静态装配子循环工具 |
| 工具结果对象缓存池、权限受限句柄与事务提交 | `core/tool/cache.py`、`registry.py` | 可选声明，按 `task`/`session` 作用域隔离 LRU，提交确认后落池 |
| 事务前检查点、提交后回调、幂等任务 | `tasks.py`、`storage.py` | 库拥有状态机，底层数据库只提供 CAS |
| 任务父链、持久化分支、全量树视图、工作区版本、节点计时与摘要重启恢复 | `tasks.py`、`sessions.py`、`runtime.py` | 支持 `parent_id` 历史分支/新根、`advance_head=False` 旁路探索、节点单调计时（`started_at`/`finished_at`/`duration_ms`）、`redirect(..., deferred=True)` 与 `TreeView`/`TreeNode` 只读全量视图，库内统一不委托消费者重写 |
| 用户/树隔离、执行租约与自动故障恢复 | `agent.py`、`tasks.py`、`sessions.py`、`scheduler.py`、`runtime.py` | 绑定可信作用域与 `active_lease` 租约续期/失租防脑裂；支持租约过期自动僵尸收束、残留展示流收尾、心跳 `heartbeat_pending` 联动清理与可选零副作用重入队，默认不重放外部副作用 |
| 持久化队列合并、毒丸隔离、重启恢复与优雅停机 | `sessions.py`、`runtime.py`、`agent.py` | 支持可配置分隔符（默认 `\n\n---\n\n`）、同源隔离、`supplemental` 保序去重与多维合并上限；前置确定性错误隔离为 `runtime_rejected:<id>`；提供无状态 `queue_status()`、`resume()` 与 `close(graceful=True, timeout=...)` |
| 公开消息记忆、统一向量、混合召回和来源复核 | `memory.py`、`vectors.py` | 数据库可替换，来源语义固定 |
| 检索子 Agent、LRU 分页、签名资料、页面折叠与 `CachePool` 集成 | `core/subgraph/memory_retrieval/`、`core/tool/memory_search.py`、`core/tool/memory_viewer.py`、`core/page_display.py` | 共享 `scope='task'` `CachePool` 存放结果集，正文持续回查权威父链 |
| 通用 PDF 逐页查看、视觉预算与 `CachePool` 集成 | `core/file/pdf.py`、`core/file/pdf_viewer.py`、`core/tool/pdf_viewer.py` | 按需安装 `agent-core[pdf]`，任意 `file_id`、授权复核与自动关闭驱逐 |
| 通用文件检索子 Agent、候选分页查看与 `CachePool` 集成 | `core/subgraph/file_retrieval/`、`core/tool/file_search.py`、`core/tool/file_candidates.py` | 解耦为 `FileSearchBackend` 协议，共享 `CachePool` 存放结果集 |
| 文件备忘持久化、检索二次缩窄与可折叠分页查看 | `file_notes.py`、`core/tool/file_notes.py`、`adapters/sqlite.py`、`adapters/mysql.py` | 按 `scope` 隔离增删查（不覆盖写入），检索与分页共享 `scope='task'` `CachePool` 与签名资料页 |
| SSE 候选预览、单调序号续传、持久化公开展示与无竞态终态 | `output_stream.py`、`tasks.py`、`agent.py`、`sessions.py`、`runtime.py` | 保留增量 JSON/Unicode 解码；统一单调 `sequence` 与 SSE `id:` 帧、支持 `after_sequence` 断点续传与 `ReplayUnavailableError`；`DisplayRecorder` 持久化非 delta 公开事件到 `display:<task_id>` 并在途叠加实时 `streaming_messages`，终态先落库再推流 |
| 供应商替换、vLLM 默认可用适配 | `providers.py`、`infrastructure/` | 日常与 Embedding 分别注入 |
| wheel、包内模板、独立安装 | `pyproject.toml`、`assets/tool-tag/` | 无 `app.*` 或后端绝对路径依赖 |

`core/journal.NodeJournal` 保留原低层协议供高级宿主适配；通常应使用 `Agent` 与 `Storage`，不要把旧 MySQL repository 接口当作新库的数据库必需接口。

---

## 留在宿主的业务能力

钉钉消息收发、员工/职位、个人画像、日程、附件转换/上传登记、文件可见性、跨平台 A2A 和中间件认证属于具体产品。新库不将这些业务作为默认工具，也不隐式注入公司提示词。

这些能力可以通过自定义工具、FinishTask 子类和工具钩子接入。高层 Agent 可选启用[空闲心跳、静默裁剪与定时任务](../../capability/application/integration.md#空闲心跳与定时任务)。宿主提供可信用户和任务树范围，以及业务消息交付。

通用 PDF 查看工具（`view_pdf`）与通用文件检索工具（`search_files`、`view_file_candidates`）均不绑定特定注册中心或业务平台标识，仅依赖宿主显式注入的加载/授权回调与 `FileSearchBackend` 实现，并通过框架 `CachePool` 隔离管理内存句柄。

---

## 验证边界

迁移回归涵盖原生工具、失败轨迹清理、工作区、内部压缩、记忆/文件检索与分页反馈；库级测试涵盖 SQLite 原子性、重启、父链分支、真实模型消息反馈、SSE 与取消、来源隔离、索引空间冲突及提交失败不交付。

真实服务实验记录在知识后端 `experiment/agent-core-package/`，结论以每次运行的 analysis.md 为准。不把替身模型测试描述为真实模型压缩质量验证，也不声称已经替换生产数据库或上线新 Agent。
