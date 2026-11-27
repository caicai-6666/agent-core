# 存储、任务与记忆契约

权威逻辑实现于 `tasks.py`、`memory.py`；数据库实现只提供 [Storage](../../../src/agent_core/storage.py) 的四个存储操作和 `close()` 生命周期方法。技术可替换，但原子性和范围隔离不能降低。

---

## 数据库准备

SQLite 与 MySQL 使用相同的权威记录和记忆文档语义。发布包携带[SQLite 建表脚本](../../../src/agent_core/adapters/schema/sqlite.sql)和[MySQL 建表脚本](../../../src/agent_core/adapters/schema/mysql.sql)。由宿主在部署阶段执行相应脚本。SQLite 需要 FTS5，MySQL 需要 InnoDB 与支持 JSON 的 MySQL 8+。MySQL 宿主安装 `agent-core[mysql]` 并创建 `asyncmy` 连接池。

```python
import sqlite3
from agent_core import MySQLStorage, SQLiteStorage, schema_sql

# 部署或升级未标版本的旧 SQLite 数据库时执行一次。
with sqlite3.connect(database_path) as conn:
    conn.executescript(schema_sql("sqlite", from_version=0))

# 业务启动时选择一种绑定。
database = SQLiteStorage.connect(database_path)
# database = await MySQLStorage.connect(pool)
```

`agent_core_schema` 登记结构版本，当前为 1。两种 `connect` 都检查版本、必需列类型和唯一键。部署脚本可重复执行；旧 SQLite 表保留数据和扩展列。兼容构造器 `SQLiteStorage(path)` 仍可为旧调用者自动准备本地库。MySQL 连接池由宿主负责关闭。

| 表 | 用途 | 唯一键 |
|---|---|---|
| records | 树、节点、工作区、队列、外部输入收据及检查点 | scope、key |
| spaces | 记忆向量空间与维度 | scope |
| memory | 公开记忆来源及向量索引 | scope、task_id、source_id |
| file_notes | 按作用域隔离的文件备忘（文件名、摘要与更新时间） | scope、file_id |

SQLite 另有 `memory_fts` 支撑记忆 BM25 检索、`file_notes_fts` 支撑文件备忘全文检索。MySQL 版在已授权候选范围内计算记忆 BM25 和精确余弦排名，再以 RRF 合并，适用于候选集可在内存处理的规模；`file_notes` 则使用 `name` 与 `summary` 的 ngram FULLTEXT 索引。

业务方可为上述四张业务表扩展列。可空列和具有默认值的列可直接增加；无默认值的必填列通过 `insert_fields(table, core_row) -> dict` 提供新行的业务值，两种 `connect` 都支持该回调。回调须同步、可重复执行且无副作用。列名与核心字段独立，值以 SQL 参数绑定。库只对创建行调用回调，更新已有行只改核心列。显式删除或心跳裁剪整行时，该行的扩展值随之删除。

---

## 权威存储

`read(scope, key)` 返回 `Record(version, value)` 或 None，必须是独立快照，不得暴露共享可变对象。

`atomic(scope, writes)` 对同一 scope 内多个 key 进行原子 compare-and-swap：

- `expected=None` 表示必须不存在；否则版本必须严格相等。
- 创建版本为 1；更新每次递增 1。
- 任意版本冲突必须整体回滚并抛 `StorageConflict`。
- 同一 key 不得在同一批次重复。事务提交未确认则抛异常；禁止返回虚假成功、部分提交或自动重放外部工具。
- 多进程、多连接也必须具备相同隔离保证，不能只在单个 Python 实例中加锁。

库将 owner 与 tree_id 用 JSON 数组编码为 scope，不依赖路径拼接。数据库键为库的内部实现，不进入模型提示词。

| 键 | 内容与生命周期 |
| --- | --- |
| `tree` | 当前 head、唯一 active 任务、任务 ID 列表及工作区保留策略 |
| `active_lease` | 当前在途任务的执行租约（`task_id`、`owner_id`、`expires_at`、`updated_at`）；与 `tree.active` 同事务 CAS 写入/释放，运行期由 `renew_lease()` 定期续租，作为自动僵尸恢复的 CAS 守卫 |
| `task:<id>` | 父任务、用户原文、附件、计时字段（`started_at`、`finished_at`、`duration_ms`）、`advance_head` 标记、终态、按策略保存的工作区、完整已接受轨迹、公开输出、结构化结果（旧记录缺省按兼容默认值反序列化） |
| `display:<task-id>` | 面向前端展示的公开事件日志（`events`、`streaming_messages`、`last_sequence`）；仅在非 delta 公开事件（`task.started`、`progress`、`message.completed`、`memory.deferred`、终态 `task.<status>`）时 CAS 持久化，严禁写入私有 `exchanges`、`audit`、工作区大对象或内部 `tool_result` |
| `generation:<id>` | 私有原始模型响应、分片、用途及失败；不进入普通历史/记忆检索 |
| `checkpoint:<id>` | 不可变提交状态、逐节点模式下的工作区引用、轨迹长度、公开消息 ID 与结构化结果 |
| `workspace:<hash>` | 逐节点模式按内容去重的不可变工作区版本 |
| `workspace:latest` | 最新模式的树头任务 ID 及完整工作区，随任务终态原子替换 |
| `audit:<task-id>` | 运行纠错、清理、延期等私有事件 |
| `embedding:<task-id>:<space-hash>` | 成功索引来源指纹；缺失或变化时可从权威任务重建 |

同一树同时只运行一个任务；可以有多个根和分支，显式 parent 决定上下文。不同树/用户可并发。任务开始时原子占用树并记录 UTC `started_at`；进程内终态写入使用单调时钟计算的 `duration_ms` 与 UTC `finished_at`，并与释放 `active` 同批提交（仅当 `advance_head=True` 时推进 `tree['head']`，`workspace_retention="latest"` 下禁止 `advance_head=False`）。重命名等业务元数据应独立于该执行版本。`TaskRepository.tree(include_display=True, live_displays=None)` 按 `tree['tasks']` 顺序返回只读 `TreeView`（节点类型为 `TreeNode`，含 `parent_id`、`started_at`、`finished_at`、`duration_ms`、`outputs`、`output_ids`、摘要窗口、轮数、最终答复及可选 `display: TaskDisplay | None`），跳过已静默裁剪且记录不存在的槽位，不加载工作区大对象与私有审计。`message.delta` 仅更新进程内 `DisplayRecorder` 缓存而不逐片写库，在途刷新由 `live_displays` 叠加最新 `streaming_messages` 与 `last_sequence`；跨进程崩溃 `recover()` 在收束任务终态时按 `finished_at - started_at` 回填 `duration_ms`，若存在 `display:<task-id>` 则同事务将残留 `streaming_messages` 收束为 `status="interrupted"` 的 `message.completed` 并追加终态事件。

MySQL 可使用事务和锁定行实现 CAS；SQLite 参考实现使用 `BEGIN IMMEDIATE`。Elasticsearch 单文档 CAS 不自动满足跨文档原子性：需要按 scope 设计单个权威聚合文档，或组合事务型权威存储与 ES 检索，不能直接逐条 update 冒充本契约。

---

## 工作区版本保留

通过 `Agent(..., workspace_retention="latest")` 选择策略。配置适用于该实例管理的持久任务树，包括直接运行、流式运行、驻留会话和调度任务。每棵树首次打开持久会话或创建任务时绑定策略，重启后使用相同配置。已有树按默认的 `per_node` 读取，配置冲突会抛出 `StorageConflict`。

| 策略 | 存储与读取 |
| --- | --- |
| `per_node`（默认） | 每个节点及其已提交检查点均可定位当时工作区；相同内容共享快照。可从历史节点恢复、继续分支或创建冻结分身。 |
| `latest` | 每棵树保留一份终态工作区，检查点继续记录动作和输出。任务运行期间另存一份可覆盖的在途工作区，支持异常终止恢复，也允许分身读取上一个终态。任务结束时在同一事务内发布新树头、替换终态工作区并释放在途副本。静默裁剪的心跳保留原树头。 |

最新模式仍保存任务轨迹、公开输出、工具结果及私有审计。按旧任务 ID 回查这些记录仍可用；请求旧节点的完整工作区、以该节点为父任务或重复读取含旧工作区的完整 `AgentResult` 会收到 `StorageConflict`。已创建的分身继续使用创建时复制的快照。纯内存会话与分身继续在独立内存库维护上下文。

---

## 上下文与压缩恢复

沿所选任务的父链按顺序加载，绝不把其他分支当成祖先。摘要检查点的 `covered_turns` 只消费当时窗口的完整历史前缀，累积摘要替换旧摘要；当前任务轨迹不参与该前缀裁剪。

每个任务保存原生已接受工具对，工作区按树的保留策略写入；失败纠错只保留私有审计，成功后不进入下一轮模型或后续任务。失败/取消任务保留终态语义，不能显示成成功答复。库不自动恢复正在执行的生成，也不重复执行未确认的外部动作。

---

## 向量与全文检索

公开记忆来源保持与原项目一致：用户消息、每一条中途公开消息、成功最终消息。每条独立向量化并持有稳定 source_id；不得把多条中途消息合并为一个不可定位来源。私有模型思考和审计不参与检索。

库统一校验维度、有限数值、非零向量，归一化并保存 float32 精度。调用方必须明确 embedding space_id，改变模型/前处理/维度时更换空间并显式重建索引。

`index(scope, space_id, dimensions, documents)` 幂等写入按任务/source_id 唯一的索引文档；同一 scope 不允许混用空间。原文属于缓存副本，权威来源仍是任务及公开消息。

`search(...)`：

1. 输入已授权父链任务 ID、公开字段、文本和查询向量。
2. 全文/BM25 类召回与向量召回分别排序，使用 RRF 合并；分数非负，不是置信度。
3. 返回 `(task_id, source_id, source_hash, score)`，不得返回范围外任务。
4. 库回查真实来源及 hash，再按任务合并分数、最多保留 20 个任务。
5. 结果集仅保存任务引用、用 LRU 管理；分页时再次回查父链与正文，页面通过签名和可折叠资料窗口展示。

最终回答提交后尝试索引，失败不撤销权威答案，保存延期审计；后续检索自动补齐索引。当前 `run()` 会等待本次索引尝试，流式的已提交最终消息已在此前发布；大规模部署可围绕权威终态任务接入后台索引执行器，但不得改变来源复核规则。

SQLite 用 FTS5 BM25 和精确余弦排序，中文逐字加二元组切分；它是可运行参考实现，不声称具有 HNSW 或中文词法分析器的性能。MySQL/MyVector、ES 等适配者可使用自己的原生索引，保持过滤、RRF、维度校验与回查契约。

---

## 验收与生命周期

适配者运行 `agent_core.testing.verify_storage(factory)`，其中 factory 返回一个全新隔离库的适配器。测试覆盖读写、原子回滚、并发 CAS、范围隔离、混合检索和空间冲突；再加入自己的多连接/进程测试。参考回归位于 `tests/test_library.py`。

数据库路径必须由宿主指定。库不会自动读取宿主配置、修改业务表、清空数据库或执行破坏性迁移。新 schema 升级应由适配者显式管理。

---

## 调度与心跳裁剪

同一 `owner/tree_id` 作用域内，`reminders` 保存排期，`reminder_queue` 保存冻结的到期事项。模型可见的创建、删除回执与权威检查点同事务提交。扫描器原子写入队列与下一次排期；消费者用同一事务出队、创建任务并占用树。竞争失败的扫描重新读取持久化状态。启用调度后，宿主重启需要重新绑定该树。

任务 `source` 区分用户、心跳和定时任务；旧记录默认是用户任务。同一作用域的 `heartbeat` 记录用 `heartbeat_pending` 保存未确认的心跳 ID，`last_heartbeat_at` 保存确认时间。保留的心跳在执行及交付确认后更新时间；成功静默裁剪在裁剪事务内确认。当 `TaskRepository.recover_expired_active()` 自动收束来源为 `heartbeat` 的过期僵尸任务时，在同一事务内清除 `tree.active`、`active_lease` 及匹配的 `heartbeat.heartbeat_pending`，但**不更新 `last_heartbeat_at`**，以便 `AgentScheduler._tick()` 在自动恢复后按原周期继续调度提醒与心跳。

存储适配器提供 `supports_atomic_delete=True` 表示支持 `Write(key, expected_version, None)` 同事务按版本删除。启用该声明前须验证删除与其他写入可整体回滚；SQLite 已实现。旧适配器可正常运行心跳，模型请求裁剪时会得到保留记录的反馈。

裁剪提交会复核可信任务来源、首轮主生成、唯一结束调用、静默交付、工作区和树版本。事务删除本次任务、展示日志（`display:<task-id>`，存在时一并删除）、生成、初始检查点及该检查点新增的工作区快照，保留共享快照及原父链，并写入最小 `pruned:<task-id>` 完成账本防止重放。裁剪发生在外发和记忆索引之前；任何版本冲突都会整体回滚。

---

## 会话队列与冻结分身

`runtime_session` 保存该任务树的执行模式。`runtime_queue` 保存等待项的任务 ID、原始请求、业务来源、优先级、可选的替代目标、可选的 `parent_id`（缺省按 `"latest"` 处理以兼容历史队列）、可选的 `advance_head`（缺省按 `True` 处理；定向替代未显式指定时继承被替代任务的策略）、可选的 `deferred` 标记（允许与 `replace_task_id` 组合，先立即中断旧任务再等待显式激活），以及合并元信息 `merged_count`（缺省按 `1` 兼容旧项）、`last_merged_at`（缺省按 `created_at` 兼容旧项）与 `supplemental_parts`（保序去重片段列表，缺省由 `supplemental` 初始化）。合并仅限同 `source`（不跨 `user`/`business`）、同优先级、同 `parent_id`、同 `advance_head` 的已就绪普通等待项，默认以 `\n\n---\n\n` 拼接并按需校验条数、字节数、附件数与时间间隔上限。

成功领取时，出队、树占用、`active_lease` 写入和创建任务同事务提交。以下永久性失败场景会在任务尚未进入执行态前原子出队并写入 `runtime_rejected:<id>`，不创建虚假执行，也不反复重试或击溃消费循环：

1. 替代目标未以 `superseded` 结束或不符合续接条件（`advance_head=True` 须为当前 `head`，`advance_head=False` 须仍在树中）；
2. 指定的 `parent_id` 不存在或在 `latest` 策略下历史工作区已释放；
3. **毒丸隔离**：队列项触发确定性输入/校验错误（`ValueError`、`TypeError`、`ValidationError`、`KeyError`）时记录 `{"error_type": ..., "message": ...}` 并继续消费后续合法项。

瞬时存储/基础设施异常仍保留原队列项供下次 `resume()` 重试。普通业务注入使用 `business` 来源，在当前及历史上下文中展示为业务系统任务，不作为用户口述内容进入记忆索引。

冻结分身的基础快照与本体隔离，冷恢复或热复用均以已提交终态节点为准；分身结束后的工作区、轨迹及向量保存在独享内存库。本体的提醒写入使用本体作用域的版本校验。宿主自定义工具的数据库连接及副作用归宿主所有。
