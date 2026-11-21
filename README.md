# Agent Core

可安装的 Python 任务型 Agent 库。提供持久任务树、原生工具循环、工作区、自动上下文压缩、混合记忆检索、专家会话，以及普通返回与 SSE 事件流。

Python 3.11+。库不读取 `.env`，不依赖知识沉淀后端，调用方显式提供三个模型和一个数据库适配器。

发行名为 `agent-core`，Python 导入名为 `agent_core`。

```bash
pip install "agent-core @ git+https://github.com/caicai-6666/agent-core.git@v0.1.0"
```

本地源码也可使用 `pip install /path/to/agent-core`。访问私有仓库时需要先配置 GitHub 认证；安装地址中不要填写凭据。

- [接入指南](description/capability/application/integration.md)：三个模型、业务角色、工具与普通/流式调用。
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
