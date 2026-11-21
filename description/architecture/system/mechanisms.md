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
| Pydantic 参数、字段说明、工具独立顺序钩子 | `core/tool/registry.py`、`hooks.py` | 原实现；展示与结构化结果分离 |
| 事务前检查点、提交后回调、幂等任务 | `tasks.py`、`storage.py` | 库拥有状态机，底层数据库只提供 CAS |
| 任务父链、分支、工作区版本、摘要重启恢复 | `tasks.py` | 库内统一，不委托给消费者重写 |
| 用户/树隔离、执行中断恢复 | `agent.py`、`tasks.py` | 绑定可信作用域，不自动重放外部副作用 |
| 公开消息记忆、统一向量、混合召回和来源复核 | `memory.py`、`vectors.py` | 数据库可替换，来源语义固定 |
| 检索子 Agent、LRU 分页、签名资料、页面折叠 | `core/subgraph/memory_retrieval/`、`core/query_pool.py`、`core/page_display.py` | 原流程和渲染 |
| 专家求助与续问、独立 LRU、在途保护和审计 | `core/expert.py`、`core/tool/external_expert.py` | 外部 Provider 必须显式注入 |
| 文件/PDF/图像阅读与授权入口、缓存预算 | `core/file/`、`core/tool/image_viewer.py` | 原能力按需注册，视觉预算显式传入 |
| 文件检索子 Agent、候选结果查看 | `core/subgraph/file_retrieval/` | 保留通用规划，具体搜索服务由宿主提供 |
| 网页查询、原文/精炼路径与图片 | `core/web.py`、`core/subgraph/web_page/` | 按需装配，网络和模型配置显式提供 |
| SSE 候选预览、提交确认、中断 | `output_stream.py`、`agent.py` | 保留增量 JSON/Unicode 解码；与普通调用同一 Runtime |
| 供应商替换、vLLM 默认可用适配 | `providers.py`、`infrastructure/` | 日常、Embedding、专家分别注入 |
| wheel、包内模板、独立安装 | `pyproject.toml`、`assets/tool-tag/` | 无 `app.*` 或后端绝对路径依赖 |

`core/journal.NodeJournal` 保留原低层协议供高级宿主适配；通常应使用 `Agent` 与 `Storage`，不要把旧 MySQL repository 接口当作新库的数据库必需接口。

---

## 留在宿主的业务能力

钉钉消息收发、员工/职位、个人画像、日程、附件转换/上传登记、文件可见性、跨平台 A2A 和中间件认证属于具体产品。新库不将这些业务作为默认工具，也不隐式注入公司提示词。

这些能力可以通过自定义工具、FinishTask 子类和工具钩子接入。高层 Agent 可选启用[空闲心跳、静默裁剪与定时任务](../../capability/application/integration.md#空闲心跳与定时任务)。宿主提供可信用户和任务树范围，以及业务消息交付。

保留的注册中心文件工具只依赖显式文件读取/搜索适配接口，不附带知识后端认证客户端；资源 ID 的兼容约定见相应工具 Schema。全新业务文件 ID 可另注册工具，避免强行套用注册中心约定。

---

## 验证边界

迁移回归涵盖原生工具、失败轨迹清理、工作区、内部压缩、记忆/文件检索与分页反馈；库级测试涵盖 SQLite 原子性、重启、父链分支、外部专家续问、真实模型消息反馈、SSE 与取消、来源隔离、索引空间冲突及提交失败不交付。

真实服务实验记录在知识后端 `experiment/agent-core-package/`，结论以每次运行的 analysis.md 为准。不把替身模型测试描述为真实模型压缩质量验证，也不声称已经替换生产数据库或上线新 Agent。
