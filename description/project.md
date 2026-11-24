# Agent Core Python 库

为业务后端提供任务树、上下文、工具循环、压缩、持久化和记忆检索；用户只实现数据库基础接口，提供日常模型、Embedding 与业务角色和工具。

库不读取宿主 `.env`，不依赖知识沉淀后端 `app`，不接管 HTTP 服务、员工身份、钉钉或跨平台中间件。

实现入口为 `src/agent_core/`。继承的核心算法位于 `core/`；`AgentRuntime` 提供树实例、驻留及调度接口，复用 `Agent` 的任务执行与提交机制；SQLite、MySQL 适配器提供事务与重启边界。适配说明和机制清单见 [文档导航](readme.md)。
