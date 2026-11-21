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

默认参数解析器按工具的 Pydantic Schema 解码模型输出的对象和数组，包括多编码成 JSON 文本的字段。对带 discriminator 的联合类型，解析器只处理标签选定分支的字段。普通文本按原值传入，后续业务类型校验继续生效。

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

---

## 空闲心跳与定时任务

高层 `Agent` 沿用个人助理的空闲心跳、静默裁剪和提醒工具。宿主显式绑定已授权的用户及任务树，在运行中的异步事件循环启动调度器。普通接入默认关闭这两项能力。

```python
from agent_core import Agent, SchedulerSettings

agent = Agent(models=models, storage=store, role=role, reminder_tools=True)
scheduler = agent.scheduler(
    owner=user_id,
    tree_id=conversation_id,
    settings=SchedulerSettings(
        heartbeat_interval_seconds=1800,
        heartbeat_message="请检查当前项目的待办和已有进展。",
        poll_seconds=30,
    ),
    publish=publish,  # 可选异步函数，签名为 publish(kind, text)
)
scheduler.start()
# 服务退出时先等待在途任务结束，再关闭宿主持有的数据库及模型连接。
await agent.close()
```

- **心跳**：`heartbeat_interval_seconds=-1` 关闭，正整数开启。首次空闲扫描即可触发，以最近一次确认完成的心跳计时。省略 `heartbeat_message` 时使用“请结合已有记忆、工作区和待办，检查现在是否有值得推进、核查或整理的事项。”任务输入和历史均标记为系统心跳，记忆索引中仅收录该任务的公开输出。
- **定时任务**：`reminder_tools=True` 注册 `create_reminder`、`list_reminders`、`delete_reminder`。支持一次性、固定秒数间隔以及按时区的日、周、月排期。默认时区为 Asia/Shanghai。月末缺日策略需明确选择；夏令时跳过不存在的时刻，重复时刻取第一次。创建或取消与工具检查点一起提交。已入队的提醒保留，停机错过多个周期时合并为一次。仅使用提醒时，保留心跳间隔默认值并启动调度器。
- **运行方式**：可以让宿主周期性等待 `scheduler.tick()` 代替 `start()`；一次检查最多执行一个到期任务。按已登记的任务树逐个恢复调度器，提醒排期、队列与心跳确认时间从数据库恢复。到期提醒优先，当前执行中的任务继续运行。库使用同一存储适配器完成入队与领取，外部员工目录、消息中间件和钉钉交付由宿主接入。业务系统另有用户消息积压时，应在业务队列空闲后调用 `tick()`。
- **收尾与裁剪**：默认结束工具通过 `send_to_user` 决定是否调用宿主交付回调。空转心跳可在首次主生成中用 `finish_task(content="无新事项", send_to_user=false, prune_current_task=true)` 静默裁剪。已执行其他工具、产生额外生成或摘要、发生纠错、修改业务工作区、携带附件或需要通知时，保留该轮。模型可根据拒绝反馈改为正常结束。宿主自定义结束工具或附加结束钩子时沿用其交付行为，标准静默裁剪仅用于库可验证的默认结束工具。
- **故障处理**：心跳执行失败、进程崩溃或交付未确认时，后续心跳等待宿主核实。通过该作用域 `heartbeat` 记录的 `heartbeat_pending` 定位任务；若仍在执行态，先按故障恢复流程终止旧执行，再在确认处理结果后调用 `await agent.repository(user_id, tree_id).acknowledge_heartbeat(task_id)`。确认操作启动下一个间隔。存储事务和裁剪适配方式见[存储契约](../../architecture/data/storage.md#调度与心跳裁剪)。

---

## 统一会话运行时

业务系统在同一事件循环复用 `agent.sessions`。先选择会话形态，再选择执行模式；由宿主认证并传入可信 owner。首次绑定持久树时保存执行模式，重启按同一模式重新绑定。

| 入口 | 上下文与轨迹 |
| --- | --- |
| `await agent.sessions.persistent(owner=..., tree_id=..., mode=...)` | 使用宿主存储。按已提交父节点复用内存快照，冷启动从任务树恢复。 |
| `await agent.sessions.memory(owner=..., context=..., mode=...)` | 使用会话独享的内存 SQLite，初始上下文可省略。释放时关闭内存库，纯内存应用可省略 Agent 的 storage 参数。 |
| `await agent.sessions.fork(parent, task_id=..., tools=..., mode=...)` | 从持久本体指定的已结束节点复制快照。默认取当前已提交 head，本体在途期间可咨询。后续轮次、工作区、审计、索引均在分身内存中。 |

三个入口均支持通过 `tools` 增加会话级注册项，沿用 Agent 的角色、模型和业务工具。工具可执行宿主授权的业务写入。分身复用本体的提醒仓库，提醒先落到本体，再向分身提交内存回执。两种存储之间没有分布式事务；提交结果不明时应回查本体提醒列表。本体调度器到点后在本体上下文中执行。纯内存会话的提醒随该会话释放。

### 提交、插队与打断

```python
session = await agent.sessions.persistent(
    owner=user_id, tree_id=conversation_id, mode="interruptible",
)
task = await session.submit("整理供应商报价")
# 业务事件保留真实来源。priority 越大越靠前，同级按入队顺序。
event_task = await session.submit("项目审批已通过", source="business", priority=10)
# 在另一请求中明确替代仍在执行的 task；宿主保留原任务 ID。
replacement = await session.submit("改为只比较 A 和 B", replace_task_id=task.id)
async for event in replacement.events():
    yield event.sse()
```

- 默认 `mode="queue"` 始终等待当前任务收尾。高优先级仅改变等待顺序。
- `mode="interruptible"` 同样对普通提交排队。`await session.stop(task_id)` 等待停止并返回终态；`replace_task_id` 在旧任务确认 `superseded` 后优先接续。迟到的指令被拒绝，已进入最终工具收尾时以该工具的实际结果为准。可从运行时任务的 `task.started` 事件或 `session.active_task_id` 获取当前执行标识。
- 停止时取消主模型生成；已进入的工具继续至结果与检查点确认。工具执行结果不确定时，旧任务失败，依赖其成功替代的指令不会开始。已提交的外部写入保留。工具自行阻塞时，安全切换需要等待工具返回。
- `await task.result()` 返回 `AgentResult`，已打断任务抛出带终态的 `TaskInterrupted`。`task.events()` 可订阅任一模式，断开订阅后执行继续。提交时传入 `preview=False` 可关闭候选正文预览，进度与已提交输出仍可观察。每个句柄保留最近 256 个事件，超窗时回查结果。旧的 `agent.stream()` 仍沿用断流取消契约。
- `session.scheduler(...).start()` 显式开启已有心跳或提醒调度。等待中的业务输入优先，调度不抢占。冻结分身取得本体的调度器；内存会话的调度器随会话关闭。

### 驻留与部署

Agent 的 `session_capacity` 默认 100，`session_ttl_seconds` 默认 1800。运行时自动清理空闲会话；达到容量上限时先驱逐最久未用的空闲会话，全部活跃则返回冲突。执行、排队与快照读取期间保护对应会话。宿主也可显式 `await agent.sessions.release(session)`。使用 `await agent.sessions.get(owner=..., session_id=...)` 续接驻留会话，内存会话和分身释放后返回 `SessionExpired`。

同一会话的提交、SSE 与停止请求应路由到同一运行进程。任务领取依赖数据库版本校验；跨进程竞争后本地句柄可以回查持久终态，实时分片与取消信号由执行进程持有。持久会话重新绑定会继续未领取的队列，崩溃遗留的活动任务须确认旧执行器停止后走 `agent.recover()`。内存会话的队列随进程结束。需要排队和打断时使用 `session.submit()`；原有直接调用 `agent.run()` 的忙时冲突语义保留。
