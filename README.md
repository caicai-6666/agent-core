# Agent Core

可安装的 Python 任务型 Agent 库。提供持久任务树、原生工具循环、工作区、自动上下文压缩、混合记忆检索，以及普通返回与 SSE 事件流。

Python 3.11+。库不读取 `.env`，不依赖知识沉淀后端，调用方提供模型；持久树使用已初始化的 SQLite 或 MySQL 数据库。

发行名为 `agent-core`，Python 导入名为 `agent_core`。

```bash
pip install "agent-core @ git+https://github.com/caicai-6666/agent-core.git@main"
# 需要内置通用 PDF 查看工具（view_pdf）时额外安装 pdf 可选依赖：
pip install "agent-core[pdf] @ git+https://github.com/caicai-6666/agent-core.git@main"
```

本地源码也可使用 `pip install /path/to/agent-core`（含 PDF 渲染依赖时为 `pip install '/path/to/agent-core[pdf]'`）。访问私有仓库时需要先配置 GitHub 认证；安装地址中不要填写凭据。

新接入可用 `AgentRuntime` 绑定模型与数据库，以 `create` 新建树、以 `restore` 恢复树或派生冻结分身。业务侧通过 `await agent.generate(...)` 提交任务，返回的句柄支持等待结果及订阅事件。[树实例接口和使用示例](description/capability/application/integration.md#树实例接口)

- [接入指南](description/capability/application/integration.md)：两个模型（日常模型与 Embedding）、业务角色、工具与普通/流式调用。
- [统一会话运行时](description/capability/application/integration.md#统一会话运行时)：持久会话、内存会话、冻结分身、优先级队列及可打断交互。
- [数据库适配契约](description/architecture/data/storage.md)：原子提交、任务树、记忆检索与故障恢复。
- [机制迁移清单](description/architecture/system/mechanisms.md)：现有机制如何保留以及业务边界。
- [可运行示例](examples/minimal.py)：无需模型服务器的确定性示例。

安装后运行 `python -m agent_core docs` 可定位随 wheel 分发的接入文档、开发规范和示例。

开发验证：

```bash
pip install -e '.[test]'
python -m pytest
python -m build
```

数据库和模型连接由宿主持有并关闭。生产接入前，请为自己的数据库运行适配契约测试，并核实模型的真实 token 计数和工具调用协议。当前库不会自动替换原后端的生产 Agent。
