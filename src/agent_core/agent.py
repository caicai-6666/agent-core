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
from .core.subgraph.memory_retrieval import create_memory_cache_pool
from .core.tool import BaseTool, ToolRegistry, FinishTaskTool, build_builtin_registrations
from .core.tool.file_notes import FILE_NOTE_TOOL_NAMES, FileNoteTools
from .core.tool.memory_search import build_memory_search_registration
from .core.tool.memory_viewer import MemoryQueryViewer, build_memory_view_registration
from .core.tool.hooks import FeedbackOnlyHook, normalize_tool_hooks
from .core.tool.cache import SessionCacheScopes
from .file_notes import FileNoteStore, create_file_note_cache_pool
from .tasks import TaskRepository, PersistentJournal
from .memory import Memory
from .providers import Models, RuntimeSettings
from .storage import StorageConflict
from .output_stream import InteractionOutputStream
from .tools import build_progress_tool
from .heartbeat import scheduled_finish, supports_heartbeat_pruning, wrap_heartbeat_prune_guard
from .reminder_tools import ReminderTools
from .sessions import SessionRuntime
from .scheduler import AgentScheduler, Reminders, SchedulerSettings


class FinishTask(FinishTaskTool):
    description = '完成当前任务并保存完整最终答复；说明已完成事实与尚未解决的问题，不把计划或工具尝试说成执行成功。'


_UNSET = object()


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
    def __init__(self, *, models: Models, storage=None, role: str, settings=None, options=None,
                 tools=(), tool_factory=None, builtin_names=None, finish_task=None,
                 memory_tools=True, reminder_tools=False,
                 file_note_tools: bool = False, file_note_authorize=None,
                 guidance_factory=None, tool_hooks=None,
                 session_capacity=100, session_ttl_seconds=1800, workspace_retention="per_node"):
        if not isinstance(role, str) or not role.strip():
            raise ValueError('必须提供业务角色提示词')
        if workspace_retention not in ('per_node', 'latest'):
            raise ValueError('workspace_retention 必须为 per_node 或 latest')
        if type(file_note_tools) is not bool:
            raise ValueError('file_note_tools 必须为布尔值')
        if file_note_authorize is not None and not callable(file_note_authorize):
            raise TypeError('file_note_authorize 必须可调用或为 None')
        if guidance_factory is not None and not callable(guidance_factory):
            raise TypeError('guidance_factory 必须可调用或为 None')
        self.workspace_retention = workspace_retention
        self.models, self.storage, self.role = models, storage, role
        self.settings = settings or RuntimeSettings()
        self.options = options or CoreSettings()
        self.tools, self.tool_factory = tuple(tools), tool_factory
        self.builtin_names = set(builtin_names) if builtin_names is not None else None
        self.finish_task = finish_task or FinishTask()
        self.memory_tools = memory_tools
        self.cache_scopes = SessionCacheScopes(max_scopes=session_capacity)
        self.reminder_tools = reminder_tools
        self.file_note_tools = file_note_tools
        self.file_note_authorize = file_note_authorize
        self.guidance_factory = guidance_factory
        self.tool_hooks = normalize_tool_hooks(tool_hooks)
        self._schedulers = {}
        self._running = set()
        self._closed = False
        self.sessions = SessionRuntime(self, capacity=session_capacity, ttl_seconds=session_ttl_seconds)

    def scheduler(self, *, owner, tree_id, settings=None, publish=None):
        """取得显式绑定的调度器；start 开启轮询，tick 仅检查一次。"""
        if self._closed:
            raise RuntimeError('Agent 已关闭')
        key = (owner, tree_id)
        config = settings or SchedulerSettings()
        existing = self._schedulers.get(key)
        if existing is None:
            existing = AgentScheduler(self, owner, tree_id, config, publish)
            self._schedulers[key] = existing
        elif existing.settings != config or existing.publish != publish:
            raise ValueError('同一任务树已绑定其他调度配置')
        return existing

    def repository(self, owner, tree_id):
        if self.storage is None:
            raise ValueError('持久任务树需要存储适配器')
        return TaskRepository(self.storage, owner=owner, tree_id=tree_id, workspace_retention=self.workspace_retention)

    async def recover(self, *, owner, tree_id, task_id, status="failed"):
        if self._running:
            raise StorageConflict('本 Agent 仍有活动任务，不能进行故障恢复')
        return await self.repository(owner, tree_id).recover(task_id, status=status)

    async def run(self, question: str, *, owner: str, tree_id: str, task_id=None,
                  parent_id='latest', attachments=(), thinking_effort='none',
                  supplemental='', publish=None, _events=None, _preview=False, _dispatch=None,
                  _repository=None, _control=None, _reminder_repository=None, _session_tools=(), _role=None,
                  _guidance_factory=_UNSET, _tool_hooks=_UNSET, created_at=None):
        if self._closed:
            raise RuntimeError('Agent 已关闭')
        running = asyncio.current_task()
        self._running.add(running)
        journal = None
        try:
            request = AgentInput(source=_dispatch.kind if _dispatch else 'user', question=question, attachments=list(attachments), thinking_effort=thinking_effort,
                                 **({'created_at': created_at} if created_at is not None else {}))
            repository = _repository or self.repository(owner, tree_id)
            task, version, created = await repository.begin(request, task_id=task_id, parent_id=parent_id, dispatch=_dispatch)
            if not created:
                if task.status == 'complete' and task.result is not None:
                    if _events:
                        await _events('task.cached', {'task_id': task.id})
                    return task.cached_result()
                raise StorageConflict('该任务已有执行记录；不会重放，读取状态或创建新的后续任务')
            journal = PersistentJournal(repository, task, version)
            journal.control = _control
            if _control:
                _control.started = True
            if _events:
                await _events('task.started', {'task_id': task.id})
            context = await repository.context(task.parent_id)
            context = context.model_copy(update={'supplemental': supplemental or context.supplemental})
            request = request.model_copy(update={'created_at':task.created_at, 'context':context})
            memory = Memory(repository, self.models.embedding)
            # 新树接口按实例精确装配；旧 Agent 调用保留原有的默认工具集合。
            managed = _role is not None
            custom = [tool for tool in _session_tools if not isinstance(tool,str)]
            selected = {tool for tool in _session_tools if isinstance(tool,str)}
            entries = [*build_builtin_registrations(), build_progress_tool()]
            if managed:
                entries = [tool for tool in entries if tool.name in selected]
            elif self.builtin_names is not None:
                known = {entry.name for entry in entries}
                if self.builtin_names - known:
                    raise ValueError('选择了不存在的内置工具')
                entries = [entry for entry in entries if entry.name in self.builtin_names]
            use_reminders = bool(selected & {'create_reminder', 'list_reminders', 'delete_reminder'}) if managed else self.reminder_tools
            use_memory = bool(selected & {'search_memory', 'view_memory_query'}) if managed else self.memory_tools
            use_file_notes = bool(selected & set(FILE_NOTE_TOOL_NAMES)) if managed else self.file_note_tools
            if use_reminders:
                journal.reminders = Reminders(_reminder_repository or repository)
                entries += [tool for tool in ReminderTools(journal.reminders).registrations() if not managed or tool.name in selected]
            if use_file_notes:
                if not isinstance(repository.storage, FileNoteStore):
                    raise TypeError('当前存储适配器不支持文件备忘能力（需实现 FileNoteStore）')
                file_note_cache_pool = create_file_note_cache_pool(scope='task')
                note_entries = FileNoteTools(
                    repository.storage,
                    repository.scope,
                    authorize=self.file_note_authorize,
                    cache_pool=file_note_cache_pool,
                ).registrations()
                entries += [tool for tool in note_entries if not managed or tool.name in selected]
            effective_tool_hooks = self.tool_hooks if _tool_hooks is _UNSET else normalize_tool_hooks(_tool_hooks)
            hook_map = dict(effective_tool_hooks)

            def attach_hooks(entry):
                if isinstance(entry, BaseTool):
                    entry = entry.build_registration()
                extra = hook_map.get(entry.name)
                return entry.with_hooks(*extra) if extra else entry

            entries = [attach_hooks(entry) for entry in entries]
            custom_finish = managed and any(tool.name == 'finish_task' for tool in custom)
            default_finish = not custom_finish and type(self.finish_task) is FinishTask and not self.finish_task.hooks
            scheduled = default_finish and task.source in ('heartbeat', 'reminder')
            journal.allow_prune = scheduled and task.source == 'heartbeat'
            if not custom_finish:
                finish = scheduled_finish(journal) if scheduled else self.finish_task.build_registration()
                finish = attach_hooks(finish)
                # 默认心跳 finish_task 附加外部钩子时，用裁剪守卫包裹以确保裁剪拒绝跳过额外业务钩子；
                # 自定义 FinishTask 子类或挂钩子时，若满足心跳裁剪契约仍可启用安全静默裁剪。
                if task.source == 'heartbeat' and (
                    (scheduled and 'finish_task' in hook_map)
                    or (not scheduled and supports_heartbeat_pruning(finish))
                ):
                    journal.allow_prune = True
                    finish = wrap_heartbeat_prune_guard(finish, journal)
                entries.append(finish)
            if not managed:
                entries.extend(attach_hooks(tool) for tool in self.tools)
            if custom_finish and task.source == 'heartbeat':
                wrapped_custom = []
                for tool in custom:
                    tool = attach_hooks(tool)
                    if tool.name == 'finish_task' and supports_heartbeat_pruning(tool):
                        journal.allow_prune = True
                        tool = wrap_heartbeat_prune_guard(tool, journal)
                    wrapped_custom.append(tool)
                entries.extend(wrapped_custom)
            else:
                entries.extend(attach_hooks(tool) for tool in custom)
            if self.tool_factory and not managed:
                entries += [attach_hooks(tool) for tool in self.tool_factory(ToolContext(repository, journal, memory))]
            effective_guidance = self.guidance_factory if _guidance_factory is _UNSET else _guidance_factory
            if effective_guidance is not None and not callable(effective_guidance):
                raise TypeError('guidance_factory 必须可调用或为 None')
            core = AgentCore(self.settings, client=self.models.daily, counter=self.models.count_tokens,
                options=self.options, registry=ToolRegistry(entries),
                prompt_builder=partial(build_system_prompt, role=_role or self.role),
                guidance_factory=effective_guidance)
            if use_memory:
                memory_cache_pool = create_memory_cache_pool(
                    capacity=self.options.memory_cache_max_queries,
                    scope='task',
                )
                memory_entries = [
                    build_memory_search_registration(
                        search=partial(memory.search, parent_id=task.parent_id),
                        reference_time=lambda: task.created_at,
                        core=core,
                        journal=lambda: journal,
                        thinking_effort=thinking_effort,
                        cache_pool=memory_cache_pool,
                        page_size=self.options.memory_page_size,
                    ),
                    build_memory_view_registration(
                        MemoryQueryViewer(
                            memory.reader(parent_id=task.parent_id),
                            cache_pool=memory_cache_pool,
                            page_size=self.options.memory_page_size,
                        ),
                    ),
                ]
                entries += [attach_hooks(tool) for tool in memory_entries if not managed or tool.name in selected]
                core = core.with_registry(ToolRegistry(entries))
            if hook_map:
                unknown_hook_tools = sorted(set(hook_map) - set(core.registry.entries))
                if unknown_hook_tools:
                    raise ValueError(f"tool_hooks 指定了未启用或不存在的工具：{', '.join(unknown_hook_tools)}")
            observer = None
            if _events:
                # 条件交付/正文转换工具只能在验收后发布，不提前泄露候选正文。
                safe_preview = all(entry.deliver_output is None and
                    (entry.output_kind is None or all(isinstance(hook, FeedbackOnlyHook) for hook in entry.hooks))
                    for entry in core.registry.entries.values())
                if _preview and safe_preview:
                    observer = InteractionOutputStream(_events, output_kinds=core.registry.output_kinds())
                async def committed(kind, text):
                    await _events('message.completed', {'message_kind':kind, 'text':text, 'status':'completed'})
                publish = committed
            async def progress(value):
                if _events:
                    await _events('progress', asdict(value))
            # 仅当本轮工具声明了 session 作用域缓存池时才按 (owner, tree_id) 绑定并钉住。
            if core.registry.has_pools('session'):
                session_cache = self.cache_scopes.get(owner, tree_id)
                with session_cache.pin():
                    result = await core.run(request, journal=journal, publish=publish,
                        publish_progress=progress if _events else None, output_observer=observer,
                        _session_cache=session_cache)
            else:
                result = await core.run(request, journal=journal, publish=publish,
                    publish_progress=progress if _events else None, output_observer=observer)
            if journal.pruned:
                return result
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
            if task.source == 'heartbeat' and result.delivery_error_type is None:
                await repository.acknowledge_heartbeat(task.id)
            return result
        except BaseException:
            if journal is not None and journal.status == 'generating':
                with suppress(Exception):
                    await journal.abort('cancelled' if asyncio.current_task().cancelling() else 'failed')
            raise
        finally:
            try:
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
        await asyncio.gather(*(scheduler.close() for scheduler in tuple(self._schedulers.values())))
        for session in tuple(self.sessions._residents.values()):
            if session.managed_heartbeat:
                await session._scheduler.close()
        if self._running:
            raise RuntimeError('先取消并等待活动任务，再关闭 Agent')
        await self.sessions.close()
        self._closed = True
        self.cache_scopes.close()
        # storage 和两个 provider 均由调用方注入，关闭责任留在调用方。
