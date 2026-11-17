# Python 库接入

先读 [项目边界](../../project.md) 和 [存储契约](../../architecture/data/storage.md)。默认公开入口为 `agent_core.Agent`；直接使用 `core.AgentCore` 是给高级宿主的底层接口，内存 Journal 不等于持久化。

---

## 调用方需要准备什么

| 输入 | 必须提供的内容 | 库负责的内容 |
| --- | --- | --- |
| 日常模型 | `ChatProvider.create_chat_completion`、精确 token 计数器、生成预算 | 工具循环、内部检索规划、工作区与历史压缩、纠错 |
| Embedding | `space_id`、`dimensions`、`embed(texts, purpose=...)` | 公开来源选择、统一 L2/float32 向量、来源复核、结果集 |
| 外部专家 | `ExpertProvider.create_response(input_items=...)` | 专家提示词、独立会话、续问、LRU、原始响应审计 |
| 数据库 | `Storage.read/atomic/index/search`，或直接用 `SQLiteStorage` | 任务树、父链、检查点、幂等、工作区版本、压缩窗口、恢复 |
| 业务角色 | `role`：职责、规则、边界、输入输出与示例 | 追加统一工具交互和工作区协议 |
| 个性化工具 | `RegisteredTool` 或 `tool_factory(ToolContext)` | 参数 Schema 校验、独立钩子、权威提交与反馈消息 |

三类模型都必须提供，即使暂时关闭专家工具或记忆工具也不省略配置。各模型可以使用完全不同的服务。`RuntimeSettings` 不绑定 vLLM HTTP 协议；vLLM 由随包适配器提供。

---

## 使用内置 vLLM 适配器

```python
from agent_core import Agent, Models, SQLiteStorage
from agent_core.providers import (
    VLLMClient, VLLMSettings, VLLMEmbedding, ChatExpert, vllm_counter,
)

# 地址和凭据由宿主配置层取得；库不自动读取 .env。
daily_settings = VLLMSettings(base_url=daily_url, model=daily_model,
                              tool_tag_enabled=False)
daily = VLLMClient(daily_settings)
expert_client = VLLMClient(expert_settings)  # 也可以换成外部 API 的实现
expert = ChatExpert(expert_client, generation=expert_settings.generation)
embedding = VLLMEmbedding(client=embedding_openai_client, model=embedding_model,
                          space_id="my-model:generic-text-retrieval-v1:l2", dimensions=4096)
store = SQLiteStorage(database_path)
agent = Agent(models=Models(daily, embedding, expert, vllm_counter(daily_settings)),
              storage=store, settings=daily_settings, role="你的完整业务角色提示词")
result = await agent.run("本次任务", owner=trusted_user_id, tree_id=conversation_id)
print(result.answer)
```

`VLLMEmbedding` 沿用原项目 ChatML 文本检索指令；普通 OpenAI-compatible 服务可用 `OpenAIEmbedding`。不同预处理、模型、维度、归一化方式必须使用不同 `space_id`，不能换模型后复用旧空间。

自定义日常 API 实现应返回 `MLLMCompletion` 和 `MLLMToolCall`（见 `infrastructure/mllm.py`），保留原始响应及结束原因。`on_raw_chunk` 接收私有原始分片；`on_tool_delta` 接收累计工具消息快照，不能传单个增量碎片替代累计参数。关闭流式的供应商可以返回完整对象；SSE 最终结果依然可用，但没有真正的提前正文预览。

`json_schema` 用于内部主题摘要等受控输出，不能只实现主 Agent 的工具调用而忽略该参数。模型不原生支持 Schema 时，适配器可以显式提示格式，返回后仍由库严格校验，不能把非法输出伪装成合法完成。

`Models.count_tokens(messages, tools, generation)` 必须计算实际模型协议对应的上下文 token 数；生产不能使用字符数近似或固定值。内置 vLLM 计数器使用服务端 tokenize。

专家协议沿用 Responses 风格的文字消息及输出列表；`ChatExpert` 可把任意聊天 Provider 转成该协议。原有外部 Responses 适配器仍可从 `infrastructure.deepseek` 引入，但不会成为默认配置，不会隐式联网搜索或读取密钥。

---

## 注册业务工具

```python
from textwrap import dedent
from pydantic import BaseModel, Field
from agent_core import RegisteredTool, ToolExecutionResult
from agent_core.core.tool.hooks import FeedbackOnlyHook
from agent_core.core.tool.feedback import with_feedback

class Arguments(BaseModel):
    project: str = Field(description="需要查询的项目全称；不能猜测用户未提供的名称。")

async def execute(args):
    row = await project_repository.find(args.project)
    if row is None:
        return ToolExecutionResult(status="failed", tool_result={
            "reason": "没有找到该项目，本次没有返回项目资料。",
            "action": "请核实项目全称后再查询。",
        })
    return ToolExecutionResult(status="succeeded", tool_result={"date": row.date})

def render(args, execution, context):
    data = execution.result.tool_result
    if execution.result.status == "failed":
        text = dedent("""
            ### 项目查询未完成

            原因：{reason}
            下一步：{action}
        """).strip().format(**data)
    else:
        text = dedent("""
            ### 项目计划

            状态：查询成功
            交付日期：{date}
        """).strip().format(**data)
    return with_feedback(execution, text)

tool = RegisteredTool("project_schedule", "按项目全称查询交付计划。", Arguments,
    lambda name, raw, workspace, args: workspace,
    executor=execute, hooks=(FeedbackOnlyHook(render),))
```

将 `tool` 放入 `Agent(tools=[tool], ...)`；需要可信用户/树作用域、私有审计等资源时使用 `tool_factory(ToolContext)` 为每个任务创建注册项，不把任务身份放入模型参数。

所有字段（含嵌套和动态属性）必须具有准确 description；注册时检查实际 JSON Schema。工具主体返回结构化结果，末尾 `FeedbackOnlyHook` 只渲染正文。业务钩子按工具独立、顺序运行，不能修改执行状态；失败同样提供友好反馈。

外部写操作须自行支持幂等，可信调用 ID 可从 `core.tool.registry.current_tool_call_id` 取得。`persist_result=True` 可随检查点保存结构化结果。`ToolExecution.after_commit` 只在提交成功后执行；它不是跨数据库分布式事务。可靠外部交付仍应由宿主提供 outbox/幂等接口，禁止把提交后网络发送描述为原子保证。

可通过 `builtin_names` 选择工作区、时间、`emit_progress` 中途输出与上下文整理工具；默认全选。`finish_task` 始终必需，可继承 `FinishTaskTool` 自定义参数与钩子。文件/PDF、注册中心文件查询、网页阅读工具需另行显式装配，其文件授权、网络读取和搜索资源由宿主提供。

---

## 普通调用、分支和事件流

- 普通运行：`await agent.run(question, owner=..., tree_id=..., task_id=...)`。
- 默认继承最近结束任务；`parent_id=<task_id>` 从指定任务创建分支；`parent_id=None` 开始同树的新根。
- `task_id` 为可选 32 位小写 UUID hex。主动传入便于幂等与失败回查；相同 ID 和输入且已完成时直接读回结果，不再调用模型。
- 数据边界由可信 `owner` 和 `tree_id` 绑定。调用方先认证，库不接受模型决定用户身份。

```python
from contextlib import aclosing

async with aclosing(agent.stream("任务", owner=user_id, tree_id=tree_id)) as events:
    async for event in events:
        yield event.sse()  # 宿主框架设置 text/event-stream
```

事件包括 `task.started`、`progress`、`message.delta`、`message.completed`、`task.completed`、`task.failed`、`task.cached` 和索引延期时的 `memory.deferred`。

`message.delta` 是候选预览；只有 `message.completed` 的 `status=completed` 才确认提交，`interrupted` 不可保存成正式答案。`task.completed` 包含最终 `AgentResult`。带正文转换钩子或条件交付的工具自动关闭预览，等待最终验收后发布。

退出消费时必须使用 `aclosing` 或显式 `aclose()`，让库取消生产者并等待清理。`Agent.close()` 要求已停止活动任务，仅关闭库自己持有的会话资源；注入的数据库和三个客户端由调用方关闭。

---

## 故障与恢复

先用 `agent.repository(owner, tree_id).load(task_id)` 查看权威状态。进程崩溃后，确认旧执行器已停止，再调用 `agent.recover(...)` 将在途任务转为失败并释放树；用户已取消或调整方向时可明确指定 `status="cancelled"` 或 `status="superseded"`，保留相应终态语义。库保留已提交轨迹和私有审计，不自动重放可能已执行的工具。

新任务将读取已确认的工作区、完整历史、终态语义和压缩窗口。需要继续业务时创建后续任务，并在涉及外部写操作前核实未确认动作的实际状态。幂等任务 ID 不等于任意外部工具自动具备 exactly-once 语义。
