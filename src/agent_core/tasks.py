"""库拥有的任务树和检查点状态机；不要求数据库适配者重写记忆策略。"""
import hashlib
import json
from copy import deepcopy
from datetime import datetime, timezone
from time import monotonic
from typing import Literal
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field
from .core import AgentInput, AgentResult
from .core.model import ContextInjection, HistoryTurn
from .core.workspace import WorkspacePayload, empty_workspace
from .core.journal import MemoryJournal
from .core.native_messages import messages_from_exchanges
from .model.question_tree import new_tree_id
from .storage import Storage, Write, StorageConflict


class DisplayEvent(BaseModel):
    """公开展示日志中的单条事件；不包含私有推理、审计或内部工具回执。"""
    model_config = ConfigDict(frozen=True)
    sequence: int
    type: str
    data: dict


class StreamingMessage(BaseModel):
    """正在流式生成中的公开正文累积前缀。"""
    model_config = ConfigDict(frozen=True)
    message_id: str
    message_kind: str
    text: str


class TaskDisplay(BaseModel):
    """单任务的公开展示日志与实时流式前缀快照。"""
    model_config = ConfigDict(frozen=True)
    events: list[DisplayEvent] = Field(default_factory=list)
    streaming_messages: list[StreamingMessage] = Field(default_factory=list)
    last_sequence: int = 0


def _completed_display_data(result: AgentResult) -> dict:
    """提取可公开持久化的终态摘要，避免把大工作区或私有审计写入展示日志。"""
    data = {
        'answer': result.answer,
        'intermediate': list(result.intermediate),
        'rounds': result.rounds,
    }
    if result.delivery_error_type is not None:
        data['delivery_error_type'] = result.delivery_error_type
    return data


class Task(BaseModel):
    model_config = ConfigDict(extra='forbid')
    id: str
    parent_id: str | None
    source: Literal['user', 'business', 'heartbeat', 'reminder'] = 'user'
    question: str
    created_at: AwareDatetime
    started_at: AwareDatetime | None = None
    finished_at: AwareDatetime | None = None
    duration_ms: int | None = None
    advance_head: bool = True
    attachments: list[dict] = Field(default_factory=list)
    status: Literal['generating','complete','failed','cancelled','superseded'] = 'generating'
    workspace: dict | None = Field(default_factory=lambda: empty_workspace().model_dump(mode='json'))
    exchanges: list[dict] = Field(default_factory=list)
    outputs: list[tuple[str,str]] = Field(default_factory=list)
    output_ids: list[str] = Field(default_factory=list)
    checkpoint_ids: list[str] = Field(default_factory=list)
    tool_results: dict = Field(default_factory=dict)
    generation_ids: list[str] = Field(default_factory=list)
    rounds: int = 0
    result: dict | None = None

    def cached_result(self):
        if self.workspace is None:
            raise StorageConflict('该任务的历史工作区已释放，无法返回当时的完整结果')
        return AgentResult.model_validate(self.result)


class TreeNode(BaseModel):
    """任务树全量视图中的只读节点摘要；不包含工作区大对象与私有审计。"""
    model_config = ConfigDict(frozen=True)
    id: str
    parent_id: str | None
    source: str
    status: str
    question: str
    attachments: list[dict] = Field(default_factory=list)
    created_at: AwareDatetime
    started_at: AwareDatetime | None = None
    finished_at: AwareDatetime | None = None
    duration_ms: int | None = None
    outputs: list[tuple[str, str]] = Field(default_factory=list)
    output_ids: list[str] = Field(default_factory=list)
    summary: str = ''
    covered_turns: int = 0
    rounds: int = 0
    answer: str | None = None
    display: TaskDisplay | None = None


class TreeView(BaseModel):
    """整棵任务树（主线与全部历史分支）的只读结构视图。"""
    model_config = ConfigDict(frozen=True)
    tree_id: str
    head: str | None
    active: str | None
    workspace_retention: str
    nodes: list[TreeNode] = Field(default_factory=list)


class TaskRepository:
    def __init__(self, storage: Storage, *, owner: str, tree_id: str, workspace_retention="per_node"):
        if not owner.strip() or not tree_id.strip():
            raise ValueError('必须明确绑定用户和任务树')
        if workspace_retention not in ('per_node', 'latest'):
            raise ValueError('workspace_retention 必须为 per_node 或 latest')
        self.workspace_retention = workspace_retention
        self.tree_id = tree_id
        self.storage = storage
        # 结构编码避免字符串连接造成用户/树边界碰撞。
        self.scope = json.dumps([owner, tree_id], ensure_ascii=False, separators=(',', ':'))

    def _tree_state(self, tree):
        if tree is None:
            return {'head': None, 'active': None, 'tasks': [], 'workspace_retention': self.workspace_retention}
        if tree.value.get('workspace_retention', 'per_node') != self.workspace_retention:
            raise StorageConflict('此任务树已绑定其他工作区保留策略')
        return deepcopy(tree.value)

    async def bind(self):
        # 驻留会话在接收排队任务前登记策略，直接运行则随首项任务登记。
        tree = await self.storage.read(self.scope, 'tree')
        state = self._tree_state(tree)
        if tree is None:
            try:
                await self.storage.atomic(self.scope, [Write('tree', None, state)])
            except StorageConflict:
                self._tree_state(await self.storage.read(self.scope, 'tree'))

    async def _restore_workspace(self, task):
        if task.workspace is None and self.workspace_retention == 'latest':
            head = await self.storage.read(self.scope, 'workspace:latest')
            if head and head.value['task_id'] == task.id:
                task.workspace = head.value['workspace']
                if task.result is not None:
                    task.result['workspace'] = task.workspace

    async def load(self, task_id, *, include_workspace=True):
        record = await self.storage.read(self.scope, 'task:' + task_id)
        if record is None:
            raise KeyError('当前用户任务树中不存在该任务')
        task = Task.model_validate(record.value)
        if include_workspace:
            await self._restore_workspace(task)
        return task, record.version

    async def path(self, parent_id):
        result, visited = [], set()
        while parent_id is not None:
            if parent_id in visited:
                raise ValueError('任务父链形成循环')
            visited.add(parent_id)
            task, _ = await self.load(parent_id, include_workspace=not result)
            if task.status == 'generating':
                raise StorageConflict('父任务尚未结束，不能继承未完成轨迹')
            result.append(task)
            parent_id = task.parent_id
        return list(reversed(result))

    def extend_context(self, context, tasks):
        summary, turns = context.summary, list(context.turns)
        workspace = context.workspace
        for task in tasks:
            for item in task.exchanges:
                if item.get('kind') == 'context_summary':
                    count = item.get('covered_turns')
                    if type(count) is not int or not 0 < count <= len(turns):
                        raise ValueError('持久化压缩窗口不合法，禁止猜测恢复边界')
                    summary = item['text']
                    turns = turns[count:]
            turns.append(HistoryTurn(source=task.source, question=task.question, created_at=task.created_at,
                attachments=task.attachments, status=task.status,
                answer=next((text for kind,text in reversed(task.outputs) if kind == 'final'), ''),
                intermediate=[text for kind,text in task.outputs if kind == 'intermediate'],
                native_messages=messages_from_exchanges(task.exchanges)))
        if tasks:
            if tasks[-1].workspace is None:
                raise StorageConflict('该节点的工作区版本已释放，无法恢复历史快照')
            workspace = WorkspacePayload.model_validate(tasks[-1].workspace)
        return ContextInjection(summary=summary, turns=turns, workspace=workspace,
                                supplemental=context.supplemental)

    async def context(self, parent_id):
        return self.extend_context(ContextInjection(), await self.path(parent_id))

    async def tree(self, include_display: bool = True, live_displays: dict[str, TaskDisplay] | None = None) -> TreeView:
        """读取整棵任务树（按创建顺序包含主线与全部历史分支），跳过已裁剪任务。"""
        if type(include_display) is not bool:
            raise ValueError('include_display 必须为布尔值')
        record = await self.storage.read(self.scope, 'tree')
        if record is None:
            return TreeView(tree_id=self.tree_id, head=None, active=None,
                            workspace_retention=self.workspace_retention, nodes=[])
        state = self._tree_state(record)
        nodes: list[TreeNode] = []
        for task_id in state['tasks']:
            task_record = await self.storage.read(self.scope, 'task:' + task_id)
            if task_record is None:
                # 静默裁剪后的任务记录已删除，视图跳过不存在的槽位。
                continue
            task = Task.model_validate(task_record.value)
            summary = ''
            covered_turns = 0
            for item in task.exchanges:
                if item.get('kind') == 'context_summary':
                    summary = item.get('text', '')
                    count = item.get('covered_turns', 0)
                    if type(count) is int and count > 0:
                        covered_turns += count
            if isinstance(task.result, dict) and 'answer' in task.result:
                answer = task.result['answer']
            else:
                answer = next((text for kind, text in reversed(task.outputs) if kind == 'final'), None)
            display = None
            if include_display:
                if live_displays and task.id in live_displays:
                    display = live_displays[task.id]
                else:
                    display_rec = await self.storage.read(self.scope, 'display:' + task.id)
                    if display_rec is not None:
                        display = TaskDisplay.model_validate(display_rec.value)
            nodes.append(TreeNode(
                id=task.id,
                parent_id=task.parent_id,
                source=task.source,
                status=task.status,
                question=task.question,
                attachments=task.attachments,
                created_at=task.created_at,
                started_at=task.started_at,
                finished_at=task.finished_at,
                duration_ms=task.duration_ms,
                outputs=task.outputs,
                output_ids=task.output_ids,
                summary=summary,
                covered_turns=covered_turns,
                rounds=task.rounds,
                answer=answer,
                display=display,
            ))
        return TreeView(
            tree_id=self.tree_id,
            head=state['head'],
            active=state['active'],
            workspace_retention=state.get('workspace_retention', self.workspace_retention),
            nodes=nodes,
        )

    async def begin(self, request: AgentInput, *, task_id=None, parent_id='latest', dispatch=None,
                    advance_head: bool = True):
        if type(advance_head) is not bool:
            raise ValueError('advance_head 必须为布尔值')
        if not advance_head and self.workspace_retention == 'latest':
            raise StorageConflict('workspace_retention=latest 模式下不允许 advance_head=False')
        task_id = task_id or new_tree_id()
        if len(task_id) != 32 or any(c not in '0123456789abcdef' for c in task_id):
            raise ValueError('任务 ID 必须为 32 位小写十六进制 UUID')
        tree = await self.storage.read(self.scope, 'tree')
        if dispatch and (tree.version if tree else None) != dispatch.tree_version:
            raise StorageConflict('任务树已变化，本轮调度重新检查')
        state = self._tree_state(tree)
        existing = await self.storage.read(self.scope, 'task:' + task_id)
        if existing:
            task = Task.model_validate(existing.value)
            if (task.source != request.source or task.question != request.question or task.attachments != [a.model_dump() for a in request.attachments]
                    or (parent_id != 'latest' and task.parent_id != parent_id)
                    or task.advance_head != advance_head):
                raise StorageConflict('任务 ID 已用于其他输入')
            await self._restore_workspace(task)
            return task, existing.version, False
        if await self.storage.read(self.scope, 'pruned:' + task_id):
            raise StorageConflict('该心跳已完成裁剪，不能重放')
        if state['active'] is not None:
            raise StorageConflict('该树已有在途任务；等待完成或确认旧执行器停止后恢复')
        parent_id = state['head'] if parent_id == 'latest' else parent_id
        context = await self.context(parent_id)
        task = Task(id=task_id, source=request.source, parent_id=parent_id, question=request.question,
            created_at=request.created_at, started_at=datetime.now(timezone.utc),
            advance_head=advance_head,
            attachments=[a.model_dump() for a in request.attachments],
            workspace=context.workspace.model_dump(mode='json'))
        state['active'] = task.id
        state['tasks'].append(task.id)
        await self.storage.atomic(self.scope, [Write('tree', tree.version if tree else None, state),
            Write('task:' + task.id, None, task.model_dump(mode='json')), *(dispatch.writes if dispatch else ())])
        return task, 1, True

    async def save(self, task, version, *, extra=()):
        data = task.model_dump(mode='json')
        writes = list(extra)
        if task.status != 'generating':
            tree = await self.storage.read(self.scope, 'tree')
            state = self._tree_state(tree)
            if self.workspace_retention == 'latest':
                # 在途任务保留最近检查点以供崩溃恢复；终态工作区只由树头快照持有。
                data['workspace'] = None
                if data['result'] is not None:
                    data['result'].pop('workspace', None)
                if state['active'] == task.id:
                    latest = await self.storage.read(self.scope, 'workspace:latest')
                    writes.append(Write('workspace:latest', latest.version if latest else None,
                                        {'task_id': task.id, 'workspace': task.workspace}))
            if state['active'] == task.id:
                # advance_head=False 时仅释放执行权，保留原有主线 head 不变。
                new_head = task.id if task.advance_head else state['head']
                state = {**state, 'active': None, 'head': new_head}
                writes.append(Write('tree', tree.version, state))
        writes.append(Write('task:' + task.id, version, data))
        await self.storage.atomic(self.scope, writes)
        return version + 1

    async def acknowledge_heartbeat(self, task_id):
        """确认该次心跳已经处理完毕；失败或交付不明时由宿主核实后调用。"""
        task, _ = await self.load(task_id)
        if task.source != 'heartbeat' or task.status == 'generating':
            raise StorageConflict('只能确认已终止的心跳任务')
        while True:
            heartbeat = await self.storage.read(self.scope, 'heartbeat')
            if not heartbeat or heartbeat.value.get('heartbeat_pending') != task_id:
                return
            state = {'heartbeat_pending': None,
                     'last_heartbeat_at': datetime.now(timezone.utc).isoformat()}
            try:
                await self.storage.atomic(self.scope, [Write('heartbeat', heartbeat.version, state)])
                return
            except StorageConflict:
                # 与其他确认者竞争时只回查确认记录，绝不重放模型或工具。
                continue

    async def recover(self, task_id, *, status="failed"):
        """只有宿主确认旧进程停止后调用；保留已提交动作，不重放未知副作用。"""
        if status not in ("failed", "cancelled", "superseded"):
            raise ValueError("恢复终态仅允许失败、取消或用户调整方向")
        task, version = await self.load(task_id)
        if task.status == 'generating':
            now = datetime.now(timezone.utc)
            task.status = status
            task.finished_at = now
            if task.started_at is not None:
                task.duration_ms = max(0, int((now - task.started_at).total_seconds() * 1000))
            extra = []
            display_key = 'display:' + task.id
            display_rec = await self.storage.read(self.scope, display_key)
            if display_rec is not None:
                # 崩溃恢复时一并收束残留流式消息并追加终态展示事件，与任务终态同事务落库。
                display = TaskDisplay.model_validate(display_rec.value)
                events = list(display.events)
                seq = display.last_sequence
                for msg in display.streaming_messages:
                    seq += 1
                    events.append(DisplayEvent(
                        sequence=seq,
                        type='message.completed',
                        data={
                            'message_id': msg.message_id,
                            'message_kind': msg.message_kind,
                            'text': msg.text,
                            'status': 'interrupted',
                            'task_id': task.id,
                        },
                    ))
                seq += 1
                events.append(DisplayEvent(
                    sequence=seq,
                    type='task.' + status,
                    data={'task_id': task.id},
                ))
                final_display = TaskDisplay(events=events, streaming_messages=[], last_sequence=seq)
                extra.append(Write(display_key, display_rec.version, final_display.model_dump(mode='json')))
            await self.save(task, version, extra=extra)
        return task


class DisplayRecorder:
    """单任务的公开展示日志记录器与统一 SSE 序号源。

    - message.delta 仅推进内存序号并累积 streaming_messages，不落库（避免分片写放大）；
    - 非 delta 公开事件先 CAS 持久化到 display:<task_id>，再推送给 SSE 订阅者；
    - 严禁记录私有 exchanges、audit 或内部 tool_result。
    """

    def __init__(self, repository: TaskRepository, task_id: str, *, sink=None):
        self.repository = repository
        self.task_id = task_id
        self._sink = sink
        self._events: list[DisplayEvent] = []
        self._streaming: dict[str, dict] = {}
        self._sequence: int = 0
        self._version: int | None = None
        self._terminal: bool = False
        self.pruned: bool = False

    def snapshot(self) -> TaskDisplay:
        """返回当前进程内只读快照，供在途 tree() 合并实时流式前缀与最新序号。"""
        return TaskDisplay(
            events=list(self._events),
            streaming_messages=[StreamingMessage(**item) for item in self._streaming.values()],
            last_sequence=self._sequence,
        )

    def _payload(self, data: dict) -> dict:
        return {**data, 'task_id': self.task_id}

    async def _persist(self) -> None:
        key = 'display:' + self.task_id
        body = self.snapshot().model_dump(mode='json')
        while True:
            try:
                await self.repository.storage.atomic(
                    self.repository.scope,
                    [Write(key, self._version, body)],
                )
                self._version = 1 if self._version is None else self._version + 1
                return
            except StorageConflict:
                existing = await self.repository.storage.read(self.repository.scope, key)
                self._version = existing.version if existing is not None else None

    async def record(self, kind: str, data: dict):
        """记录公开事件：delta 仅写内存；非 delta 先落库再推 SSE。"""
        if self.pruned:
            return None
        from .agent import AgentEvent
        payload = self._payload(data)
        self._sequence += 1
        seq = self._sequence
        event = AgentEvent(type=kind, data=payload, sequence=seq)
        if kind == 'message.delta':
            msg_id = payload.get('message_id')
            msg_kind = payload.get('message_kind', '')
            delta = payload.get('delta', '')
            if msg_id:
                entry = self._streaming.get(msg_id)
                if entry is None:
                    self._streaming[msg_id] = {
                        'message_id': msg_id,
                        'message_kind': msg_kind,
                        'text': delta,
                    }
                else:
                    entry['text'] += delta
            if self._sink is not None:
                await self._sink(event)
            return event

        if kind == 'message.completed':
            msg_id = payload.get('message_id')
            if msg_id:
                self._streaming.pop(msg_id, None)

        self._events.append(DisplayEvent(sequence=seq, type=kind, data=payload))
        await self._persist()
        if self._sink is not None:
            await self._sink(event)
        return event

    async def finalize_streaming(self) -> list:
        """将仍处于流式预览中的消息收束为 interrupted 的 message.completed 并清空缓存。"""
        emitted = []
        for item in list(self._streaming.values()):
            event = await self.record('message.completed', {
                'message_id': item['message_id'],
                'message_kind': item['message_kind'],
                'text': item['text'],
                'status': 'interrupted',
            })
            if event is not None:
                emitted.append(event)
        self._streaming.clear()
        return emitted

    async def finish(self, status: str, data: dict | None = None, *, sse_data: dict | None = None):
        """先收束未完结流式消息并持久化终态 display:<task_id>，再推送终态 SSE 事件。"""
        if self._terminal or self.pruned:
            return None
        self._terminal = True
        if self._streaming:
            await self.finalize_streaming()
        from .agent import AgentEvent
        display_payload = self._payload(data or {})
        sse_payload = self._payload(sse_data if sse_data is not None else (data or {}))
        self._sequence += 1
        seq = self._sequence
        self._events.append(DisplayEvent(sequence=seq, type='task.' + status, data=display_payload))
        await self._persist()
        event = AgentEvent(type='task.' + status, data=sse_payload, sequence=seq)
        if self._sink is not None:
            await self._sink(event)
        return event


class PersistentJournal(MemoryJournal):
    def __init__(self, repository, task, version):
        super().__init__(task.id)
        self.repository, self.task, self.version = repository, task, version
        self.workspace = WorkspacePayload.model_validate(task.workspace)
        self.control = None
        self.pruned = False
        self.allow_prune = False
        self.reminders = None
        self.initial_workspace = deepcopy(task.workspace)
        self.created_workspace_key = None
        self._start_mono = monotonic()

    def _finish_timing(self, candidate: Task) -> None:
        # 进程内耗时使用单调时钟，避免系统校时导致负数或跳变。
        candidate.finished_at = datetime.now(timezone.utc)
        candidate.duration_ms = max(0, int((monotonic() - self._start_mono) * 1000))

    def prune_rejection(self):
        if not getattr(self.repository.storage, 'supports_atomic_delete', False):
            return '当前存储适配器未支持同事务裁剪。'
        if self.task.source != 'heartbeat' or self.task.attachments:
            return '仅允许裁剪无附件的可信心跳。'
        if self.task.exchanges or self.task.outputs or len(self.task.checkpoint_ids) != 1:
            return '本次已有其他交互或已提交动作。'
        if (len(self.generations) != 1 or self.generations[0].status != 'complete'
                or self.generations[0].payload.purpose != 'main' or len(self.task.generation_ids) != 1):
            return '本轮含多次生成、失败或内部整理，需保留记录。'
        return None

    async def _prune(self, workspace, exchanges):
        calls = [call for message in exchanges for call in message.get('tool_calls', [])]
        current = workspace.model_dump(mode='json')
        current['task_constraints']['task'] = self.initial_workspace['task_constraints']['task']
        if (self.prune_rejection() or current != self.initial_workspace or len(exchanges) != 2 or len(calls) != 1
                or calls[0]['function']['name'] != 'finish_task'):
            raise StorageConflict('终态校验未通过，保留心跳记录')
        args = json.loads(calls[0]['function']['arguments'])
        if args.get('send_to_user') is not False or args.get('prune_current_task') is not True:
            raise StorageConflict('心跳收尾未确认静默裁剪')
        repo = self.repository
        tree = await repo.storage.read(repo.scope, 'tree')
        state = deepcopy(tree.value)
        if state['active'] != self.task.id or state['head'] != self.task.parent_id:
            raise StorageConflict('心跳已不是活动末端，不能裁剪')
        state['tasks'].remove(self.task.id)
        state['active'] = None
        heartbeat = await repo.storage.read(repo.scope, 'heartbeat')
        if not heartbeat or heartbeat.value.get('heartbeat_pending') != self.task.id:
            raise StorageConflict('心跳确认记录不匹配，不能裁剪')
        ack = Write('heartbeat', heartbeat.version, {'heartbeat_pending': None,
                    'last_heartbeat_at': datetime.now(timezone.utc).isoformat()})
        # 仅删除本轮初始检查点新建的工作区。若命中共享快照，必须保留其他节点的引用。
        snapshots = [Write('checkpoint:' + self.task.checkpoint_ids[0], 1, None)]
        if self.created_workspace_key:
            snapshots.append(Write(self.created_workspace_key, 1, None))
        display_rec = await repo.storage.read(repo.scope, 'display:' + self.task.id)
        if display_rec is not None:
            snapshots.append(Write('display:' + self.task.id, display_rec.version, None))
        await repo.storage.atomic(repo.scope, [
            Write('tree', tree.version, state), ack, *snapshots, Write('task:' + self.task.id, self.version, None),
            Write('generation:' + self.task.generation_ids[0], 1, None),
            Write('pruned:' + self.task.id, None, {'source': 'heartbeat', 'status': 'complete'})])
        self.pruned = True
        self.status = 'complete'


    async def _save_generation(self, event):
        candidate = self.task.model_copy(deep=True)
        candidate.generation_ids.append(event.id)
        if event.payload.purpose == 'main':
            candidate.rounds += 1
        self.version = await self.repository.save(candidate, self.version,
            extra=[Write('generation:' + event.id, None, event.model_dump(mode='json'))])
        self.task = candidate

    async def commit(self, *, workspace, exchanges, output=None, final=False, output_id=None, tool_results=None):
        if self.status != 'generating':
            raise StorageConflict('任务已经结束，不能提交迟到结果')
        candidate = self.task.model_copy(deep=True)
        candidate.workspace = workspace.model_dump(mode='json')
        candidate.exchanges = deepcopy(exchanges)
        if tool_results:
            candidate.tool_results.update(deepcopy(tool_results))
        if output is not None:
            candidate.outputs.append(('final' if final else 'intermediate', output))
            candidate.output_ids.append(output_id or new_tree_id())
        if final:
            candidate.status = 'complete'
            self._finish_timing(candidate)
            candidate.result = AgentResult(answer=output, workspace=workspace,
                intermediate=[text for kind,text in candidate.outputs if kind == 'intermediate'],
                rounds=candidate.rounds).model_dump(mode='json')
        # 先持久化，后更新进程内权威状态，最后由 Core 调用 after_commit。
        checkpoint_id = new_tree_id()
        candidate.checkpoint_ids.append(checkpoint_id)
        checkpoint = {'task_id': candidate.id, 'exchange_count': len(exchanges),
                      'output_ids': candidate.output_ids, 'status': candidate.status,
                      'tool_results': candidate.tool_results}
        extra = []
        created_workspace_key = None
        if self.repository.workspace_retention == 'per_node':
            workspace_id = hashlib.sha256(json.dumps(candidate.workspace, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
            workspace_key = 'workspace:' + workspace_id
            checkpoint['workspace_id'] = workspace_id
            if await self.repository.storage.read(self.repository.scope, workspace_key) is None:
                extra.append(Write(workspace_key, None, candidate.workspace))
                created_workspace_key = workspace_key
        extra.append(Write('checkpoint:' + checkpoint_id, None, checkpoint))
        reminder_action = None
        for call_id, action in (tool_results or {}).items():
            if final and self.allow_prune and action['name'] == 'finish_task' and action['result'].get('prune'):
                await self._prune(workspace, exchanges)
                return
            if self.reminders and action['name'] in ('create_reminder', 'delete_reminder'):
                call = next(c for m in reversed(exchanges) for c in m.get('tool_calls', []) if c['id'] == call_id)
                reminder_action = (action['name'], action['result'], json.loads(call['function']['arguments']), self.task.id)
        # 分身轨迹位于独立内存库，提醒属于本体；先确认持久业务写入，再提交内存回执。
        if reminder_action and self.reminders.repository is not self.repository:
            target = self.reminders.repository
            while True:
                write = await self.reminders.change(*reminder_action)
                try:
                    await target.storage.atomic(target.scope, [write])
                    break
                except StorageConflict:
                    latest = await self.reminders.read()
                    if latest is None or latest.version == write.expected:
                        raise
            reminder_action = None
        while True:
            write = await self.reminders.change(*reminder_action) if reminder_action else None
            try:
                self.version = await self.repository.save(candidate, self.version,
                    extra=[*extra, write] if write else extra)
                break
            except StorageConflict:
                # 仅当排期器推进了提醒版本时重新构造写集，不重放工具或隐藏任务版本冲突。
                if not write:
                    raise
                latest = await self.reminders.read()
                _, task_version = await self.repository.load(self.task.id)
                if task_version != self.version or latest is None or latest.version == write.expected:
                    raise
        if self.allow_prune and not self.task.checkpoint_ids:
            self.created_workspace_key = created_workspace_key
        self.task = candidate
        await super().commit(workspace=workspace, exchanges=exchanges, output=output,
            final=final, output_id=output_id, tool_results=tool_results)

    async def abort(self, status):
        if status == 'cancelled' and self.control and self.control.reason:
            # 非主动取消的工具可能已产生外部效果，不能把结果不明当作安全替代。
            unknown = any(item.get('event') == 'execution_cancelled' for item in self.audit)
            status = 'failed' if unknown else self.control.reason
        if self.status == 'generating':
            candidate = self.task.model_copy(deep=True)
            candidate.status = status
            self._finish_timing(candidate)
            self.version = await self.repository.save(candidate, self.version)
            self.task = candidate
        if status == 'superseded':
            # 树与任务终态已在上述事务提交，随后才允许替代任务占用执行权。
            self.status = status
        else:
            await super().abort(status)

    async def flush_audit(self):
        # 异常路径与成功后清理记录不进入任务模型上下文。
        if self.pruned:
            return
        previous = await self.repository.storage.read(self.repository.scope, 'audit:' + self.node_id)
        await self.repository.storage.atomic(self.repository.scope,
            [Write('audit:' + self.node_id, previous.version if previous else None, {'events': self.audit})])
