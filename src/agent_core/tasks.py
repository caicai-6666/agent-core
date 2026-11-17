"""库拥有的任务树和检查点状态机；不要求数据库适配者重写记忆策略。"""
from copy import deepcopy
from datetime import datetime, timezone
from typing import Literal
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field
from .core import AgentInput, AgentResult
from .core.model import ContextInjection, HistoryTurn
from .core.workspace import WorkspacePayload, empty_workspace
from .core.journal import MemoryJournal
from .core.native_messages import messages_from_exchanges
from .model.question_tree import new_tree_id
from .storage import Storage, Write, StorageConflict


class Task(BaseModel):
    model_config = ConfigDict(extra='forbid')
    id: str
    parent_id: str | None
    question: str
    created_at: AwareDatetime
    attachments: list[dict] = Field(default_factory=list)
    status: Literal['generating','complete','failed','cancelled','superseded'] = 'generating'
    workspace: dict = Field(default_factory=lambda: empty_workspace().model_dump(mode='json'))
    exchanges: list[dict] = Field(default_factory=list)
    outputs: list[tuple[str,str]] = Field(default_factory=list)
    output_ids: list[str] = Field(default_factory=list)
    checkpoint_ids: list[str] = Field(default_factory=list)
    tool_results: dict = Field(default_factory=dict)
    generation_ids: list[str] = Field(default_factory=list)
    rounds: int = 0
    result: dict | None = None


class TaskRepository:
    def __init__(self, storage: Storage, *, owner: str, tree_id: str):
        if not owner.strip() or not tree_id.strip():
            raise ValueError('必须明确绑定用户和任务树')
        import json
        self.storage = storage
        # 结构编码避免字符串连接造成用户/树边界碰撞。
        self.scope = json.dumps([owner, tree_id], ensure_ascii=False, separators=(',', ':'))

    async def load(self, task_id):
        record = await self.storage.read(self.scope, 'task:' + task_id)
        if record is None:
            raise KeyError('当前用户任务树中不存在该任务')
        return Task.model_validate(record.value), record.version

    async def path(self, parent_id):
        result, visited = [], set()
        while parent_id is not None:
            if parent_id in visited:
                raise ValueError('任务父链形成循环')
            visited.add(parent_id)
            task, _ = await self.load(parent_id)
            if task.status == 'generating':
                raise StorageConflict('父任务尚未结束，不能继承未完成轨迹')
            result.append(task)
            parent_id = task.parent_id
        return list(reversed(result))

    async def context(self, parent_id):
        summary, turns = None, []
        workspace = empty_workspace()
        for task in await self.path(parent_id):
            for item in task.exchanges:
                if item.get('kind') == 'context_summary':
                    count = item.get('covered_turns')
                    if type(count) is not int or not 0 < count <= len(turns):
                        raise ValueError('持久化压缩窗口不合法，禁止猜测恢复边界')
                    summary = item['text']
                    turns = turns[count:]
            workspace = WorkspacePayload.model_validate(task.workspace)
            turns.append(HistoryTurn(question=task.question, created_at=task.created_at,
                attachments=task.attachments, status=task.status,
                answer=next((text for kind,text in reversed(task.outputs) if kind == 'final'), ''),
                intermediate=[text for kind,text in task.outputs if kind == 'intermediate'],
                native_messages=messages_from_exchanges(task.exchanges)))
        return ContextInjection(summary=summary, turns=turns, workspace=workspace)

    async def begin(self, request: AgentInput, *, task_id=None, parent_id='latest'):
        task_id = task_id or new_tree_id()
        if len(task_id) != 32 or any(c not in '0123456789abcdef' for c in task_id):
            raise ValueError('任务 ID 必须为 32 位小写十六进制 UUID')
        tree = await self.storage.read(self.scope, 'tree')
        state = deepcopy(tree.value) if tree else {'head': None, 'active': None, 'tasks': []}
        existing = await self.storage.read(self.scope, 'task:' + task_id)
        if existing:
            task = Task.model_validate(existing.value)
            if (task.question != request.question or task.attachments != [a.model_dump() for a in request.attachments]
                    or (parent_id != 'latest' and task.parent_id != parent_id)):
                raise StorageConflict('任务 ID 已用于其他输入')
            return task, existing.version, False
        if state['active'] is not None:
            raise StorageConflict('该树已有在途任务；等待完成或确认旧执行器停止后恢复')
        parent_id = state['head'] if parent_id == 'latest' else parent_id
        context = await self.context(parent_id)
        task = Task(id=task_id, parent_id=parent_id, question=request.question,
            created_at=request.created_at, attachments=[a.model_dump() for a in request.attachments],
            workspace=context.workspace.model_dump(mode='json'))
        state['active'] = task.id
        state['tasks'].append(task.id)
        await self.storage.atomic(self.scope, [Write('tree', tree.version if tree else None, state),
            Write('task:' + task.id, None, task.model_dump(mode='json'))])
        return task, 1, True

    async def save(self, task, version, *, extra=()):
        writes = [Write('task:' + task.id, version, task.model_dump(mode='json')), *extra]
        if task.status != 'generating':
            tree = await self.storage.read(self.scope, 'tree')
            if tree.value['active'] == task.id:
                state = {**tree.value, 'active': None, 'head': task.id}
                writes.append(Write('tree', tree.version, state))
        await self.storage.atomic(self.scope, writes)
        return version + 1

    async def recover(self, task_id, *, status="failed"):
        """只有宿主确认旧进程停止后调用；保留已提交动作，不重放未知副作用。"""
        if status not in ("failed", "cancelled", "superseded"):
            raise ValueError("恢复终态仅允许失败、取消或用户调整方向")
        task, version = await self.load(task_id)
        if task.status == 'generating':
            task.status = status
            await self.save(task, version)
        return task


class PersistentJournal(MemoryJournal):
    def __init__(self, repository, task, version):
        super().__init__(task.id)
        self.repository, self.task, self.version = repository, task, version
        self.workspace = WorkspacePayload.model_validate(task.workspace)

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
            candidate.result = AgentResult(answer=output, workspace=workspace,
                intermediate=[text for kind,text in candidate.outputs if kind == 'intermediate'],
                rounds=candidate.rounds).model_dump(mode='json')
        # 先持久化，后更新进程内权威状态，最后由 Core 调用 after_commit。
        import hashlib, json
        workspace_id = hashlib.sha256(json.dumps(candidate.workspace, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        workspace_key = 'workspace:' + workspace_id
        existing = await self.repository.storage.read(self.repository.scope, workspace_key)
        checkpoint_id = new_tree_id()
        candidate.checkpoint_ids.append(checkpoint_id)
        extra = [Write('checkpoint:' + checkpoint_id, None, {
            'task_id': candidate.id, 'workspace_id': workspace_id,
            'exchange_count': len(exchanges), 'output_ids': candidate.output_ids,
            'status': candidate.status, 'tool_results': candidate.tool_results})]
        if existing is None:
            extra.append(Write(workspace_key, None, candidate.workspace))
        self.version = await self.repository.save(candidate, self.version, extra=extra)
        self.task = candidate
        await super().commit(workspace=workspace, exchanges=exchanges, output=output,
            final=final, output_id=output_id, tool_results=tool_results)

    async def abort(self, status):
        if self.status == 'generating':
            candidate = self.task.model_copy(deep=True)
            candidate.status = status
            self.version = await self.repository.save(candidate, self.version)
            self.task = candidate
        await super().abort(status)

    async def flush_audit(self):
        # 异常路径与成功后清理记录不进入任务模型上下文。
        previous = await self.repository.storage.read(self.repository.scope, 'audit:' + self.node_id)
        await self.repository.storage.atomic(self.repository.scope,
            [Write('audit:' + self.node_id, previous.version if previous else None, {'events': self.audit})])
