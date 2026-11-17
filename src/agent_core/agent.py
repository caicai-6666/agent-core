"""持久 Agent：普通/事件流共用同一核心与任务提交路径。"""
import asyncio
from contextlib import suppress
from dataclasses import dataclass, asdict
from functools import partial
import json
from pydantic import BaseModel, ConfigDict
from .core import AgentCore, AgentInput, AgentResult, CoreSettings, AgentCoreError
from .core.model import ContextInjection
from .core.prompt import build_system_prompt
from .core.tool import ToolRegistry, FinishTaskTool, build_builtin_registrations
from .core.tool.external_expert import build_external_expert_registrations
from .core.tool.memory_search import build_memory_search_registration
from .core.tool.memory_viewer import MemoryQueryViewer, build_memory_view_registration
from .core.tool.hooks import FeedbackOnlyHook
from .core.expert import ExpertSessions
from .tasks import TaskRepository, PersistentJournal
from .memory import Memory
from .providers import Models, RuntimeSettings
from .storage import StorageConflict
from .output_stream import InteractionOutputStream
from .tools import build_progress_tool


class FinishTask(FinishTaskTool):
    description = '完成当前任务并保存完整最终答复；说明已完成事实与尚未解决的问题，不把计划或工具尝试说成执行成功。'


@dataclass(frozen=True)
class ToolContext:
    repository: TaskRepository
    journal: PersistentJournal
    memory: Memory


class AgentEvent(BaseModel):
    model_config = ConfigDict(frozen=True)
    type: str
    data: dict

    def sse(self):
        return 'event: ' + self.type + '\ndata: ' + json.dumps(self.data, ensure_ascii=False) + '\n\n'


class Agent:
    def __init__(self, *, models: Models, storage, role: str, settings=None, options=None,
                 tools=(), tool_factory=None, builtin_names=None, finish_task=None,
                 memory_tools=True, expert_tools=True):
        if not isinstance(role, str) or not role.strip():
            raise ValueError('必须提供业务角色提示词')
        self.models, self.storage, self.role = models, storage, role
        self.settings = settings or RuntimeSettings()
        self.options = options or CoreSettings()
        self.tools, self.tool_factory = tuple(tools), tool_factory
        self.builtin_names = set(builtin_names) if builtin_names is not None else None
        self.finish_task = finish_task or FinishTask()
        self.memory_tools, self.expert_tools = memory_tools, expert_tools
        self.experts = ExpertSessions(client=models.expert,
            capacity=self.options.expert_session_cache_max_sessions)
        self._running = set()
        self._closed = False

    def repository(self, owner, tree_id):
        return TaskRepository(self.storage, owner=owner, tree_id=tree_id)

    async def recover(self, *, owner, tree_id, task_id, status="failed"):
        if self._running:
            raise StorageConflict('本 Agent 仍有活动任务，不能进行故障恢复')
        return await self.repository(owner, tree_id).recover(task_id, status=status)

    async def run(self, question: str, *, owner: str, tree_id: str, task_id=None,
                  parent_id='latest', attachments=(), thinking_effort='none',
                  supplemental='', publish=None, _events=None, _preview=False):
        if self._closed:
            raise RuntimeError('Agent 已关闭')
        running = asyncio.current_task()
        self._running.add(running)
        journal = pool = None
        try:
            request = AgentInput(question=question, attachments=list(attachments), thinking_effort=thinking_effort)
            repository = self.repository(owner, tree_id)
            task, version, created = await repository.begin(request, task_id=task_id, parent_id=parent_id)
            if not created:
                if task.status == 'complete' and task.result is not None:
                    if _events:
                        await _events('task.cached', {'task_id': task.id})
                    return AgentResult.model_validate(task.result)
                raise StorageConflict('该任务已有执行记录；不会重放，读取状态或创建新的后续任务')
            journal = PersistentJournal(repository, task, version)
            if _events:
                await _events('task.started', {'task_id': task.id})
            context = await repository.context(task.parent_id)
            context = context.model_copy(update={'supplemental': supplemental})
            request = request.model_copy(update={'created_at':task.created_at, 'context':context})
            memory = Memory(repository, self.models.embedding)
            entries = [*build_builtin_registrations(), build_progress_tool()]
            if self.builtin_names is not None:
                known = {entry.name for entry in entries}
                if self.builtin_names - known:
                    raise ValueError('选择了不存在的内置工具')
                entries = [entry for entry in entries if entry.name in self.builtin_names]
            entries += [self.finish_task.build_registration(), *self.tools]
            if self.tool_factory:
                entries += list(self.tool_factory(ToolContext(repository, journal, memory)))
            if self.expert_tools:
                entries += build_external_expert_registrations(
                    get_pool=lambda: self.experts.get(owner, tree_id), on_audit=journal.record_expert)
            core = AgentCore(self.settings, client=self.models.daily, counter=self.models.count_tokens,
                options=self.options, registry=ToolRegistry(entries),
                prompt_builder=partial(build_system_prompt, role=self.role))
            if self.memory_tools:
                pool = memory.pool(parent_id=task.parent_id, capacity=self.options.memory_cache_max_queries,
                    page_size=self.options.memory_page_size)
                entries += [build_memory_search_registration(pool=pool,
                    search=partial(memory.search, parent_id=task.parent_id), reference_time=lambda:task.created_at,
                    core=core, journal=lambda:journal, thinking_effort=thinking_effort),
                    build_memory_view_registration(MemoryQueryViewer(pool))]
                core = core.with_registry(ToolRegistry(entries))
            observer = None
            if _events:
                # 条件交付/正文转换工具只能在验收后发布，不提前泄露候选正文。
                safe_preview = all(entry.deliver_output is None and
                    (entry.output_kind is None or all(isinstance(hook, FeedbackOnlyHook) for hook in entry.hooks))
                    for entry in entries)
                if _preview and safe_preview:
                    observer = InteractionOutputStream(_events, output_kinds=core.registry.output_kinds())
                async def committed(kind, text):
                    await _events('message.completed', {'message_kind':kind, 'text':text, 'status':'completed'})
                publish = committed
            async def progress(value):
                if _events:
                    await _events('progress', asdict(value))
            result = await core.run(request, journal=journal, publish=publish,
                publish_progress=progress if _events else None, output_observer=observer)
            # 交付失败不撤销答案，但幂等回读也必须保留已确认的交付失败事实。
            if journal.task.result != result.model_dump(mode='json'):
                candidate = journal.task.model_copy(deep=True)
                candidate.result = result.model_dump(mode='json')
                journal.version = await repository.save(candidate, journal.version)
                journal.task = candidate
            # 向量缓存不是权威任务提交的一部分；失败可在下次检索时重建。
            try:
                await memory.index_task(journal.task)
            except Exception as error:
                journal.audit.append({'event':'embedding_deferred','error_type':type(error).__name__})
                if _events:
                    await _events('memory.deferred', {'task_id':task.id})
            return result
        except BaseException:
            if journal is not None and journal.status == 'generating':
                with suppress(Exception):
                    await journal.abort('cancelled' if asyncio.current_task().cancelling() else 'failed')
            raise
        finally:
            try:
                if pool is not None:
                    pool.close()
                if journal is not None:
                    await journal.flush_audit()
            finally:
                self._running.discard(running)

    async def stream(self, question, *, preview=True, **kwargs):
        queue = asyncio.Queue(maxsize=64)
        listening = True
        async def emit(kind, data):
            if listening:
                await queue.put(AgentEvent(type=kind, data=data))
        async def produce():
            try:
                result = await self.run(question, **kwargs, _events=emit, _preview=preview)
                await emit('task.completed', {'result':result.model_dump(mode='json')})
            except asyncio.CancelledError:
                raise
            except Exception as error:
                await emit('task.failed', {'error_type':type(error).__name__,
                    'message':'执行或收尾未正常结束，请通过任务 ID 回查已提交状态；不要直接重放外部动作。'})
            finally:
                if listening:
                    await queue.put(None)
        worker = asyncio.create_task(produce())
        try:
            while (event := await queue.get()) is not None:
                yield event
        finally:
            # 使用 aclosing 或显式 aclose；断开消费即取消运行并等待检查点清理。
            listening = False
            worker.cancel()
            with suppress(asyncio.CancelledError):
                await worker

    async def close(self):
        if self._running:
            raise RuntimeError('先取消并等待活动任务，再关闭 Agent')
        self._closed = True
        await self.experts.close()
        # storage 和三个 provider 均由调用方注入，关闭责任留在调用方。
