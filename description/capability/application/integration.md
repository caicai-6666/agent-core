# Python 库接入

先读 [项目边界](../../project.md) 和 [存储契约](../../architecture/data/storage.md)。树实例入口为 `agent_core.AgentRuntime`，现有 `agent_core.Agent` 接口继续可用；直接使用 `core.AgentCore` 是给高级宿主的底层接口，内存 Journal 不等于持久化。

---

## 树实例接口

一个 runtime 共享一组模型；可以在 runtime 上绑定默认数据库，也可在每次 `create` 或 `restore` 时传入 `database=` 选择另一数据库。每个数据库适配器拥有独立的驻留池，容量与空闲期限分别由 `capacity` 和 `ttl_seconds` 配置。调用者先校验业务权限，再用树 ID 打开实例。不同数据库可使用相同树 ID。数据库部署和业务扩展字段见[数据库准备](../../architecture/data/storage.md#数据库准备)。

```python
from agent_core import AgentRuntime, SQLiteStorage

database = SQLiteStorage.connect(database_path)
runtime = AgentRuntime(models=models, database=database, capacity=100)
agent = await runtime.create(
    tree_id="project-42", role=project_role, tools=["workspace_add", project_tool],
    persist_new_nodes=True, execution_mode="interruptible", workspace_retention="per_node",
)
job = await agent.generate("整理交付约定", external_id="group-event-123", source="business")
result = await job.result()

# 基于指定已提交节点派生咨询分身，可装配自己的角色和工具集合。
consultant = await runtime.restore(
    tree_id="project-42", start_node_id=job.id, role=consultation_role,
    tools=[lookup_tool], persist_new_nodes=False,
)
reply = await (await consultant.generate("哪些交付条件还要确认？")).result()
await consultant.close()
await runtime.close()
await database.close()
```

创建持久树时，`persist_new_nodes=True` 决定新任务节点写入数据库，`workspace_retention` 单独决定保留哪些工作区版本。

| `workspace_retention` | 适用场景 |
|---|---|
| `"per_node"`（默认） | 每个已提交节点保留对应工作区。需要以后从指定历史节点恢复上下文、派生分身时使用。 |
| `"latest"` | 保留任务轨迹，工作区只持久化最新版本。适合始终从当前树头继续的助理。 |

策略保存在树上，`restore` 自动读取；纯内存实例和分身产生的新工作区仅驻留内存。

| 操作 | 实例与存储行为 |
|---|---|
| `create(..., persist_new_nodes=False)` | 指定 ID 创建纯内存树，可以使用无数据库 runtime。驱逐、释放或关闭后结束其生命周期。 |
| `create(..., persist_new_nodes=True)` | 原子创建数据库中的树和执行模式；已有 ID 抛出 `StorageConflict`。 |
| `restore(..., persist_new_nodes=True)` | 找不到树时抛出 `KeyError`。复用驻留状态或按已登记的策略冷恢复。持久实例的句柄在空闲驱逐后可以继续 `generate`。 |
| `restore(..., persist_new_nodes=False)` | 复制树头或 `start_node_id` 的已提交上下文；`instance_id` 是分身自己的标识，`tree_id` 是来源。旧工作区已按 `latest` 策略释放时，历史派生返回冲突。 |

`tools` 接收库内置工具的名称（如 `workspace_add`、`emit_progress`、`create_reminder`、`add_file_note`、`delete_file_note`、`search_file_notes`、`view_file_note_query`）和业务 `RegisteredTool` 对象。每个实例仅装配所选工具和必需的 `finish_task`。可显式传入满足终态契约的自定义 `finish_task` 注册。恢复或派生时由宿主重新提供角色与工具；同一驻留本体需保持一致，业务回调和数据库凭据不写入树记录。分身可独立选择具有副作用的工具，库内提醒归属本体，文件备忘随分身独立内存库与本体隔离。

`generate` 返回任务句柄。用 `await job.result()` 等待终态，或用 `async for event in job.events()` 观察事件。`execution_mode="queue"` 依优先级及入队顺序领取；`"interruptible"` 同样排队，额外支持 `await agent.stop(job.id)` 和 `await agent.redirect(job.id, "新指令")`。定向替代优先接续旧任务；普通业务队列先处理已到期提醒，再处理等待中的业务任务，空闲时执行心跳。

优先级实例可在 `runtime.create(..., heartbeat=SchedulerSettings(heartbeat_interval_seconds=1800))` 时启用心跳，省略即关闭。`heartbeat_message` 可覆盖默认消息。实例自行启动轮询并保持驻留，关闭实例或运行时时结束轮询。持久树恢复时自动沿用存储的配置；内存实例的配置随实例释放，分身沿用本体的心跳归属。

首次空闲扫描即可触发心跳。只有树没有在途任务、没有到期提醒、也没有已激活的等待任务，才领取已到间隔的心跳。即使业务任务的优先级为负数，它也先于心跳；心跳一旦开始，随后到达的业务任务按队列规则等待。

- `priority` 是整数，数值越大越早被领取。
- `await agent.generate("补充交付日期", priority=10, merge_with_previous=True)` 会追加到最近入队、同优先级、已就绪的普通等待任务。目标保留原任务 ID 和排队位置，两次提交共享执行结果；同一驻留实例也共享事件流。默认每次提交创建独立任务。没有符合条件的目标时按普通任务入队。
- 合并按入队事务提交顺序保留每段 prompt 和补充上下文，附件按文件 ID 去重。任务来源、创建时间和推理档位沿用目标任务；任何一次输入关闭预览时，该任务关闭预览。待激活输入和定向替代输入不能同时请求合并。在途任务、到期提醒和空闲心跳均不作为合并目标。
- 合并和领取使用同一队列版本校验。目标被抢先领取时重新选择，合并收据与队列更新在同一事务提交。`external_id` 重投返回原任务 ID，每段输入只追加一次。
- `external_id` 在本实例的任务作用域内去重；相同输入并发投递共用任务 ID，冲突输入报错。持久树的收据跨重启保留。使用 `latest` 时较早的任务仍有收据，完整结果回读遵循历史工作区保留策略。
- `created_at` 可传业务事件的带时区时间，用作模型的任务时间。省略时取提交时间。
- `deferred=True` 登记任务后等待显式激活；宿主可在首次订阅时调用 `await job.activate()`。重启后恢复实例并调用 `await agent.activate(task_id)` 可重新获得句柄。等待激活的任务不会阻塞其他已就绪任务。
- `agent.recover(task_id)` 用于宿主确认旧执行进程已经停止后的故障收束。任务不会重放。
- 新接口使用数据库内固定的树命名空间。接续旧 `Agent` 创建的树时，用 `AgentRuntime(..., legacy_owner=原owner)` 绑定其原有命名空间；原数据库的执行模式与工作区策略继续有效。拥有同名树的多个旧 owner 应分别绑定。

---

## 调用方需要准备什么

| 输入 | 必须提供的内容 | 库负责的内容 |
| --- | --- | --- |
| 日常模型 | `ChatProvider.create_chat_completion`、精确 token 计数器、生成预算 | 工具循环、内部检索规划、工作区与历史压缩、纠错 |
| Embedding | `space_id`、`dimensions`、`embed(texts, purpose=...)` | 公开来源选择、统一 L2/float32 向量、来源复核、结果集 |
| 数据库 | `Storage.read/atomic/index/search`，或直接用 `SQLiteStorage` | 任务树、父链、检查点、幂等、工作区版本、压缩窗口、恢复 |
| 业务角色 | `role`：职责、规则、边界、输入输出与示例 | 追加统一工具交互和工作区协议 |
| 个性化工具 | `RegisteredTool` 或 `tool_factory(ToolContext)` | 参数 Schema 校验、独立钩子、权威提交与反馈消息 |

两类模型（日常模型与 Embedding）都必须提供，即使暂时关闭记忆工具也不省略配置。各模型可以使用完全不同的服务。`RuntimeSettings` 不绑定 vLLM HTTP 协议；vLLM 由随包适配器提供。

---

## 使用内置 vLLM 适配器

```python
from agent_core import Agent, Models, SQLiteStorage
from agent_core.providers import (
    VLLMClient, VLLMSettings, VLLMEmbedding, vllm_counter,
)

# 地址和凭据由宿主配置层取得；库不自动读取 .env。
daily_settings = VLLMSettings(base_url=daily_url, model=daily_model,
                              tool_tag_enabled=False)
daily = VLLMClient(daily_settings)
embedding = VLLMEmbedding(client=embedding_openai_client, model=embedding_model,
                          space_id="my-model:generic-text-retrieval-v1:l2", dimensions=4096)
store = SQLiteStorage(database_path)
agent = Agent(models=Models(daily, embedding, vllm_counter(daily_settings)),
              storage=store, settings=daily_settings, role="你的完整业务角色提示词")
result = await agent.run("本次任务", owner=trusted_user_id, tree_id=conversation_id)
print(result.answer)
```

`VLLMEmbedding` 沿用原项目 ChatML 文本检索指令；普通 OpenAI-compatible 服务可用 `OpenAIEmbedding`。不同预处理、模型、维度、归一化方式必须使用不同 `space_id`，不能换模型后复用旧空间。

自定义日常 API 实现应返回 `MLLMCompletion` 和 `MLLMToolCall`（见 `infrastructure/mllm.py`），保留原始响应及结束原因。`on_raw_chunk` 接收私有原始分片；`on_tool_delta` 接收累计工具消息快照，不能传单个增量碎片替代累计参数。关闭流式的供应商可以返回完整对象；SSE 最终结果依然可用，但没有真正的提前正文预览。

`json_schema` 用于内部主题摘要等受控输出，不能只实现主 Agent 的工具调用而忽略该参数。模型不原生支持 Schema 时，适配器可以显式提示格式，返回后仍由库严格校验，不能把非法输出伪装成合法完成。

`Models.count_tokens(messages, tools, generation)` 必须计算实际模型协议对应的上下文 token 数；生产不能使用字符数近似或固定值。内置 vLLM 计数器使用服务端 tokenize。

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

可通过 `builtin_names` 选择工作区、`emit_progress` 中途输出与上下文整理工具；默认全选。`finish_task` 始终必需，可继承 `FinishTaskTool` 自定义参数与钩子。通用 PDF 查看工具（`view_pdf`）与通用文件检索/候选查看工具（`search_files`、`view_file_candidates`）需按需显式装配，均与框架 `CachePool` 集成，其文件加载、授权与搜索后端由宿主提供。

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

退出消费时必须使用 `aclosing` 或显式 `aclose()`，让库取消生产者并等待清理。`Agent.close()` 要求已停止活动任务，仅关闭库自己持有的会话资源；注入的数据库和模型客户端由调用方关闭。

---

## 故障与恢复

先用 `agent.repository(owner, tree_id).load(task_id)` 查看权威状态。进程崩溃后，确认旧执行器已停止，再调用 `agent.recover(...)` 将在途任务转为失败并释放树；用户已取消或调整方向时可明确指定 `status="cancelled"` 或 `status="superseded"`，保留相应终态语义。库保留已提交轨迹和私有审计，不自动重放可能已执行的工具。

新任务将读取已确认的工作区、完整历史、终态语义和压缩窗口。需要继续业务时创建后续任务，并在涉及外部写操作前核实未确认动作的实际状态。幂等任务 ID 不等于任意外部工具自动具备 exactly-once 语义。

---

## 空闲心跳与定时任务

以下为直接使用 `Agent` 的接入方式。树实例使用上文的 `heartbeat` 配置。

`Agent` 沿用个人助理的空闲心跳、静默裁剪和提醒工具。宿主显式绑定已授权的用户及任务树，在运行中的异步事件循环启动调度器。普通接入默认关闭这两项能力。

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
- `session.scheduler(...).start()` 显式开启已有心跳或提醒调度。已到期提醒优先于等待中的普通业务输入，调度不抢占。冻结分身取得本体的调度器；内存会话的调度器随会话关闭。

### 驻留与部署

Agent 的 `session_capacity` 默认 100，`session_ttl_seconds` 默认 1800。运行时自动清理空闲会话；达到容量上限时先驱逐最久未用的空闲会话，全部活跃则返回冲突。执行、排队与快照读取期间保护对应会话。宿主也可显式 `await agent.sessions.release(session)`。使用 `await agent.sessions.get(owner=..., session_id=...)` 续接驻留会话，内存会话和分身释放后返回 `SessionExpired`。

同一会话的提交、SSE 与停止请求应路由到同一运行进程。任务领取依赖数据库版本校验；跨进程竞争后本地句柄可以回查持久终态，实时分片与取消信号由执行进程持有。持久会话重新绑定会继续未领取的队列，崩溃遗留的活动任务须确认旧执行器停止后走 `agent.recover()`。内存会话的队列随进程结束。需要排队和打断时使用 `session.submit()`；原有直接调用 `agent.run()` 的忙时冲突语义保留。

---

## 继承自定义工具

从 `agent_core` 导入 `BaseTool`，在子类上声明 `name`、`description` 和 Pydantic `arguments_model`。将子类实例放入 `tools=[ProjectSchedule()]` 即可；完整示例见 [minimal.py](../../../examples/minimal.py)。

| 扩展点 | 实现约定 |
| --- | --- |
| `execute(arguments)` | 同步或异步执行，返回 `ToolExecutionResult` 或带提交回调的 `ToolExecution`。需要预演工作区修改时覆盖 `prepare`。 |
| `hooks` | 按声明顺序执行成功路径的业务钩子，可异步。末尾的展示钩子由基类安装。 |
| `render_success(arguments, result, context)` | 从本次已确认结果生成普通工具的非空文本。可预期的失败在业务分支用 `self.failure(code, reason, required_action, mode=...)` 返回，默认按原因及下一步排版；需要业务格式时覆盖 `render_failure`。 |
| `retry_guidance` | 可选，填写反复尝试同一方法后适合本工具的替代操作。省略使用库的通用建议。 |

`failure` 的 `mode` 默认是 `correctable`。已确认操作失败用 `failed`，权限或宿主生命周期要求终止用 `dependency`。外部写入无法确认时返回 `status="unknown"`，由库停止以避免重复写入。输出工具、可折叠工具和已有注册项可继续用 `RegisteredTool` 声明专用能力。计数与回执生命周期见[工具反馈渲染规范](../../standard/tool-feedback.md#重复调用失败时的指导)。

---

## 工具结果对象缓存池

工具结果对象缓存池用于在同一任务或同一会话驻留期内复用内存结果对象（如查询句柄、解析后的结构化对象），不写入数据库，也不直接暴露给模型。

> **可选声明：** 工具默认不声明任何缓存池（`read_pools = ()`、`write_pools = ()`），保持普通一次性执行，无额外分配或生命周期开销。

### 声明规格与访问模式

通过 `CachePool[T](name, capacity=10, scope="session", on_evict=None)` 定义不可变缓存池规格，并在 `BaseTool` 子类或 `RegisteredTool` 上通过 `read_pools`、`write_pools` 声明权限。工具在 `execute(arguments)` 内调用 `self.cache(pool)`（函数式 `RegisteredTool` 使用 `current_tool_cache(pool)`）获取受限句柄，由库按声明权限强校验：

| 声明方式 | 可得句柄 | 适用场景 |
| --- | --- | --- |
| 均不声明（默认） | 不可访问（访问抛 `PermissionError`） | 普通一次性工具 |
| 仅 `write_pools` | `WriteOnlyCache`（`put`、`delete`） | 配套工具中的生产者（生成句柄写入池） |
| 仅 `read_pools` | `ReadOnlyCache`（`get`、`__contains__`、`keys`） | 配套工具中的消费者/分页查看器（未命中可返回 `correctable` 失败） |
| 同时声明 `read_pools` 与 `write_pools` | 同池为 `ReadWriteCache`；不同池分别只读/只写 | 查缓存→未命中计算并写入，或读池 A 写池 B 的流水线 |

```python
from agent_core import BaseTool, CachePool, ToolExecutionResult

query_pool = CachePool[dict]("query_results", capacity=8, scope="session")

class RunQuery(BaseTool):
    name = "run_query"
    description = "执行查询并缓存结果对象。"
    arguments_model = QueryArgs
    write_pools = (query_pool,)

    async def execute(self, arguments):
        self.cache(query_pool).put(arguments.key, {"rows": [...]})
        return ToolExecutionResult(status="succeeded", tool_result={"key": arguments.key})
```

同一作用域内禁止不同 `CachePool` 实例使用相同 `name`（注册或访问时抛出 `ValueError`）；不同作用域（`task` 与 `session`）允许同名。

### 事务提交与作用域生命周期

- **事务一致提交**：`execute` 期间的 `put`/`delete` 仅暂存在单次工具调用内（读写句柄支持同调用读己所写）。只有工具最终返回 `status="succeeded"` 且 `journal.commit` 成功后，暂存变更才随 `after_commit` 落入权威 LRU；工具返回失败、抛出异常、被取消或业务钩子抛出 `ToolOperationFailed` 时均丢弃暂存，不污染缓存。
- **LRU 与驱逐回调**：`get` 命中并提交后刷新最近使用顺序；超出 `capacity` 时淘汰最久未用条目并调用 `on_evict(key, value)`。作用域关闭时对剩余条目逐一触发 `on_evict`。
- **`scope="task"`**：仅存活于单次任务执行，任务结束后立即清理（内置记忆检索工具 `search_memory` / `view_memory_query` 即使用 `scope="task"` 共享 `CachePool`，由 `AgentCore.run` 自动管理生命周期）。
- **`scope="session"`**：由 `Agent` 按可信 `(owner, tree_id)` 隔离管理（受 `session_capacity` 有界 LRU 保护，任务运行期间自动钉住不驱逐）。在同树连续任务间复用，跨树、跨 `owner` 以及冻结分身（`restore(..., persist_new_nodes=False)` / `sessions.fork`）均完全隔离；会话释放、空闲 TTL 驱逐或 `Agent.close()` 时清理。

---

## 通用 PDF 查看与文件检索工具

两套内置可选文件工具均基于 `CachePool` 管理会话/任务期内存句柄，不绑定特定数据库表、注册中心或平台协议。

### 通用 PDF 查看（`view_pdf`）

PDF 渲染依赖（`pymupdf`、`Pillow`）作为可选扩展 `agent-core[pdf]` 提供，未安装时不影响 `import agent_core`；仅在实际打开/渲染 PDF 时按需导入，缺失时抛出提示执行 `pip install "agent-core[pdf]"` 的 `ImportError`。

```python
from agent_core import (
    PDFSource,
    PDFViewer,
    build_pdf_view_registration,
    create_pdf_cache_pool,
)

pdf_pool = create_pdf_cache_pool(name="pdf_documents", capacity=4, scope="session")
viewer = PDFViewer(
    load_pdf=lambda file_id: PDFSource(path=resolve_path(file_id), name=resolve_name(file_id)),
    pool=pdf_pool,
    authorize=lambda file_id: check_permission(user_id, file_id),
)
view_pdf_tool = build_pdf_view_registration(viewer)
```

- **任意 `file_id` 与两种模式**：接受任意非空字符串 `file_id`。传入 `load_pdf` 时声明 `read_pools=(pool,)` 与 `write_pools=(pool,)`，缓存命中直接复用已打开 `PDFDocument`，未命中加载并在提交后写入缓存池；省略 `load_pdf` 时仅声明 `read_pools=(pool,)`，只读消费前置工具写入同一 `CachePool` 的 `PDFDocument`。
- **自动关闭与权限复核**：`create_pdf_cache_pool` 默认绑定 `on_evict` 自动调用 `PDFDocument.close()`。每次查看与页面解析（`resolve_page`）都会重新执行 `authorize`，撤权后不回放缓存图像。
- **逐页折叠与游标**：省略 `page_number` 自动翻至下一页，越界或末页返回 `correctable` 失败且不推进游标；页面内容通过 `FoldableToolContent` 与 `page_resolver` 按需展开。

### 通用文件检索与候选查看（`search_files` / `view_file_candidates`）

宿主只需实现 `FileSearchBackend` 协议（`settings: FileSearchSettings`、`async read(file_ids=None) -> Sequence[FileSearchRecord]`、`async search(field, query, scope=None) -> Sequence[tuple[str, float]]`，其中 `field` 为 `'name' | 'summary' | 'tag'`），即可装配检索子 Agent 与分页候选查看器：

```python
from agent_core import (
    FileCandidateViewer,
    build_file_candidates_registration,
    build_file_search_registration,
    create_file_search_cache_pool,
)

search_pool = create_file_search_cache_pool(name="file_search_results", capacity=10, scope="task")
search_tool = build_file_search_registration(
    backend=my_backend,
    core=core,
    journal=lambda: current_journal,
    cache_pool=search_pool,
)
candidates_tool = build_file_candidates_registration(
    FileCandidateViewer(my_backend, cache_pool=search_pool),
)
```

- **共享 `CachePool` 与二次缩窄**：`search_files` 声明对 `cache_pool` 的读写权限，将 `FileSearchResultSet` 随 `journal.commit` 原子写入缓存池，并支持传入已有 `result_id` 在原结果集范围内二次检索；`result_id` 缺失或失效时返回 `correctable` 反馈。
- **权威回查与折叠分页**：`view_file_candidates` 仅声明只读权限，按页回查 `backend.read(ids)` 获取最新文件元信息与权限状态，不在缓存池中保存文件名或摘要；缓存驱逐或作用域关闭时触发 `on_evict` 使结果集失效，阻止旧页面引用继续解析。

### 文件备忘工具（`add_file_note` / `delete_file_note` / `search_file_notes` / `view_file_note_query`）

文件备忘工具按 `(owner, tree_id)` 作用域持久化到存储适配器的 `file_notes` 表（`SQLiteStorage` 与 `MySQLStorage` 均实现 `FileNoteStore` 协议），默认关闭，可按需通过 `Agent(..., file_note_tools=True, file_note_authorize=...)` 或 `AgentRuntime.create(tools=["add_file_note", "delete_file_note", "search_file_notes", "view_file_note_query"])` 启用：

```python
from agent_core import AgentRuntime

runtime = AgentRuntime(
    models=models,
    database=database,
    file_note_authorize=lambda file_id: check_permission(user_id, file_id),  # 可选，支持同步或异步
)
agent = await runtime.create(
    tree_id="project-42",
    role=project_role,
    tools=["add_file_note", "delete_file_note", "search_file_notes", "view_file_note_query"],
)
```

- **不覆盖写入与幂等删除**：`add_file_note` 与 `delete_file_note` 均为不声明缓存池的一次性工具。同一作用域内重复 `file_id` 会被拒绝（不提供覆盖更新工具）；需要修改时先删除再重新添加。`delete_file_note` 在目标不存在时幂等返回。提交阶段结果不明时返回 `status="unknown"`，避免盲目重放。
- **可选授权回调**：提供 `file_note_authorize(file_id)` 时，`add_file_note` 在写入前执行授权校验；返回 `False` 或抛出 `FileNoteRejected` 均返回可纠正失败，不写入数据库。
- **检索、二次缩窄与可折叠分页**：`search_file_notes` 支持中英文文件名/摘要关键词（`and`/`or`）、时间窗口（`start_time` 含、`end_time` 不含）及基于已有 `query_id` 的二次缩窄，命中结果经 `scope="task"` 的 `CachePool` 随 `journal.commit` 原子发布（最多保留 1000 条并向二次缩窄传递截断标记；空结果不入池）。`view_file_note_query` 只读消费同一 `CachePool`，以 HMAC 签名的 `FoldableToolContent` 按页展开；缓存失效或驱逐时返回 `correctable` 反馈并阻止旧页面引用解析。
- **作用域隔离**：不同 `owner`、不同 `tree_id` 及冻结分身（`restore(..., persist_new_nodes=False)`）的备忘完全隔离。
