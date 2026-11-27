"""持久 Agent：普通/事件流共用同一核心与任务提交路径。"""
import asyncio
from contextlib import suppress
from dataclasses import dataclass, asdict
from functools import partial
import json
import logging
from math import isfinite
from time import monotonic
from pydantic import BaseModel, ConfigDict
from .core import AgentCore, AgentInput, AgentResult, CoreSettings, AgentCoreError
from .core.model import ContextInjection
from .core.prompt import PromptOptions, build_system_prompt, resolve_prompt_flags
from .core.workspace import empty_workspace
from .core.subgraph.memory_retrieval import create_memory_cache_pool
from .core.tool import BaseTool, ToolRegistry, FinishTaskTool, build_builtin_registrations
from .core.tool.file_notes import FILE_NOTE_TOOL_NAMES, FileNoteTools
from .core.tool.memory_search import build_memory_search_registration
from .core.tool.memory_viewer import MemoryQueryViewer, build_memory_view_registration
from .core.tool.hooks import FeedbackOnlyHook, normalize_tool_hooks
from .core.tool.cache import SessionCacheScopes
from .file_notes import FileNoteStore, create_file_note_cache_pool
from .model.question_tree import new_tree_id
from .tasks import DisplayRecorder, PersistentJournal, TaskRepository, _completed_display_data, validate_lease_seconds
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
logger = logging.getLogger(__name__)


def _validate_close_options(graceful: bool, timeout: float) -> float:
    if type(graceful) is not bool:
        raise ValueError('graceful 必须为布尔值')
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not isfinite(timeout) or timeout <= 0:
        raise ValueError('timeout 必须为有限正数')
    return float(timeout)


@dataclass(frozen=True)
class ToolContext:
    repository: TaskRepository
    journal: PersistentJournal
    memory: Memory


class AgentEvent(BaseModel):
    model_config = ConfigDict(frozen=True)
    type: str
    data: dict
    sequence: int = 0

    def sse(self):
        payload = json.dumps(self.data, ensure_ascii=False)
        if self.sequence > 0:
            return f'id: {self.sequence}\nevent: {self.type}\ndata: {payload}\n\n'
        return 'event: ' + self.type + '\ndata: ' + payload + '\n\n'


class Agent:
    def __init__(self, *, models: Models, storage=None, role: str, settings=None, options=None,
                 tools=(), tool_factory=None, builtin_names=None, finish_task=None,
                 memory_tools=True, reminder_tools=False,
                 file_note_tools: bool = False, file_note_authorize=None,
                 guidance_factory=None, tool_hooks=None, prompt_options: PromptOptions | None = None,
                 session_capacity=100, session_ttl_seconds=1800, workspace_retention="per_node",
                 lease_seconds: float = 300.0, owner_id: str | None = None):
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
        if prompt_options is not None and not isinstance(prompt_options, PromptOptions):
            raise TypeError('prompt_options 必须为 PromptOptions 实例或 None')
        if owner_id is not None and (not isinstance(owner_id, str) or not owner_id.strip()):
            raise ValueError('owner_id 必须为非空字符串')
        self.lease_seconds = validate_lease_seconds(lease_seconds)
        self.owner_id = owner_id if owner_id is not None else new_tree_id()
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
        self.prompt_options = prompt_options or PromptOptions()
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
                  _guidance_factory=_UNSET, _tool_hooks=_UNSET, _prompt_options: PromptOptions | None = None,
                  created_at=None, advance_head: bool = True, _display=None, _event_sink=None):
        if self._closed:
            raise RuntimeError('Agent 已关闭')
        if type(advance_head) is not bool:
            raise ValueError('advance_head 必须为布尔值')
        if _prompt_options is not None and not isinstance(_prompt_options, PromptOptions):
            raise TypeError('prompt_options 必须为 PromptOptions 实例或 None')
        running = asyncio.current_task()
        self._running.add(running)
        journal = None
        recorder = None
        owns_display = False
        lease_task = None
        lease_lost = False
        try:
            request = AgentInput(source=_dispatch.kind if _dispatch else 'user', question=question, attachments=list(attachments), thinking_effort=thinking_effort,
                                 **({'created_at': created_at} if created_at is not None else {}))
            repository = _repository or self.repository(owner, tree_id)
            task, version, created = await repository.begin(
                request, task_id=task_id, parent_id=parent_id, dispatch=_dispatch, advance_head=advance_head,
                owner_id=self.owner_id, lease_seconds=self.lease_seconds,
            )
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

            async def _renew_loop():
                nonlocal lease_lost
                interval = self.lease_seconds / 3
                deadline = monotonic() + self.lease_seconds
                while True:
                    await asyncio.sleep(interval)
                    if journal.pruned or journal.status != 'generating':
                        return
                    try:
                        ok = await repository.renew_lease(task.id, self.owner_id, self.lease_seconds)
                    except Exception as renew_error:
                        logger.warning('任务租约续期异常：%s', type(renew_error).__name__)
                        if monotonic() < deadline:
                            continue
                        ok = False
                    if ok:
                        deadline = monotonic() + self.lease_seconds
                        continue
                    # 续租返回 False 时核实任务是否已正常完成或被静默裁剪，避免误伤终态收尾。
                    if journal.pruned or journal.status != 'generating':
                        return
                    try:
                        current_task, _ = await repository.load(task.id, include_workspace=False)
                        if current_task.status == 'complete':
                            return
                    except KeyError:
                        return
                    except Exception:
                        pass
                    # 租约已丢失或被接管：立即打断并取消本执行任务，防止脑裂。
                    lease_lost = True
                    if _control is not None:
                        with suppress(StorageConflict):
                            _control.interrupt('cancelled')
                    if running is not None and not running.done():
                        running.cancel()
                    return

            lease_task = asyncio.create_task(_renew_loop())
            if _display is not None:
                recorder = _display
                owns_display = False
            else:
                if _event_sink is not None:
                    sink = _event_sink
                elif _events is not None:
                    async def sink(event):
                        await _events(event.type, event.data)
                else:
                    sink = None
                recorder = DisplayRecorder(repository, task.id, sink=sink)
                owns_display = True
            await recorder.record('task.started', {'task_id': task.id})
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
            effective_prompt_options = self.prompt_options if _prompt_options is None else _prompt_options
            # 按已装配工具集合与继承工作区内容解析开关；后续动态追加的记忆工具不影响工作区与压缩判断。
            has_nonempty_workspace = (
                task.workspace != empty_workspace().model_dump(mode='json')
                if task.workspace
                else False
            )
            use_workspace, use_compaction = resolve_prompt_flags(
                effective_prompt_options,
                [entry.name for entry in entries],
                has_nonempty_workspace=has_nonempty_workspace,
            )
            if effective_prompt_options.prompt_builder is not None:
                prompt_builder = effective_prompt_options.prompt_builder
            else:
                prompt_builder = partial(
                    build_system_prompt,
                    role=_role or self.role,
                    output_guidance=effective_prompt_options.output_guidance,
                    include_interaction=effective_prompt_options.interaction,
                    include_workspace=use_workspace,
                    include_compaction=use_compaction,
                    include_relative_time=effective_prompt_options.relative_time,
                )
            core = AgentCore(self.settings, client=self.models.daily, counter=self.models.count_tokens,
                options=self.options, registry=ToolRegistry(entries),
                prompt_builder=prompt_builder,
                guidance_factory=effective_guidance,
                include_workspace=use_workspace)
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
            user_publish = publish
            # 条件交付/正文转换工具只能在验收后发布，不提前泄露候选正文。
            safe_preview = all(entry.deliver_output is None and
                (entry.output_kind is None or all(isinstance(hook, FeedbackOnlyHook) for hook in entry.hooks))
                for entry in core.registry.entries.values())
            if _preview and safe_preview:
                observer = InteractionOutputStream(recorder.record, output_kinds=core.registry.output_kinds())

            async def committed(kind, text):
                # 无流式预览时复用已提交的 output_id，保证 message.completed 始终带完整标识且不重复发布。
                message_id = journal.task.output_ids[-1] if journal.task.output_ids else new_tree_id()
                await recorder.record('message.completed', {
                    'message_id': message_id,
                    'message_kind': kind,
                    'text': text,
                    'status': 'completed',
                })
                if user_publish is not None:
                    await user_publish(kind, text)

            publish = committed

            async def progress(value):
                await recorder.record('progress', asdict(value))

            # 仅当本轮工具声明了 session 作用域缓存池时才按 (owner, tree_id) 绑定并钉住。
            if core.registry.has_pools('session'):
                session_cache = self.cache_scopes.get(owner, tree_id)
                with session_cache.pin():
                    result = await core.run(request, journal=journal, publish=publish,
                        publish_progress=progress, output_observer=observer,
                        _session_cache=session_cache)
            else:
                result = await core.run(request, journal=journal, publish=publish,
                    publish_progress=progress, output_observer=observer)
            # 核心循环已落库终态并释放租约，立即停止续租协程以避免干扰后续索引与收尾。
            if lease_task is not None:
                lease_task.cancel()
                with suppress(asyncio.CancelledError):
                    await lease_task
                lease_task = None
            if journal.pruned:
                recorder.pruned = True
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
                await recorder.record('memory.deferred', {'task_id':task.id})
            if task.source == 'heartbeat' and result.delivery_error_type is None:
                await repository.acknowledge_heartbeat(task.id)
            if owns_display:
                await recorder.finish(
                    'completed',
                    _completed_display_data(result),
                    sse_data={'result': result.model_dump(mode='json')},
                )
            return result
        except BaseException as error:
            if lease_task is not None:
                lease_task.cancel()
                with suppress(asyncio.CancelledError):
                    await lease_task
                lease_task = None
            if journal is not None and journal.status == 'generating':
                with suppress(Exception):
                    await journal.abort('cancelled' if (lease_lost or asyncio.current_task().cancelling()) else 'failed')
            if (owns_display and recorder is not None and journal is not None
                    and not journal.pruned and journal.status != 'generating'):
                if isinstance(error, asyncio.CancelledError):
                    with suppress(Exception):
                        await recorder.finish(journal.task.status)
                else:
                    with suppress(Exception):
                        await recorder.finish(
                            journal.task.status,
                            {'error_type': type(error).__name__},
                            sse_data={
                                'error_type': type(error).__name__,
                                'message': '执行或收尾未正常结束，请通过任务 ID 回查已提交状态；不要直接重放外部动作。',
                            },
                        )
            if lease_lost and running is not None and running.cancelling():
                # 仅抵消由本任务租约丢失触发的一次内部 cancel，避免污染复用协程的 cancelling 计数。
                running.uncancel()
            raise
        finally:
            try:
                if lease_task is not None:
                    lease_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await lease_task
            finally:
                try:
                    if journal is not None:
                        await journal.flush_audit()
                finally:
                    self._running.discard(running)

    async def stream(self, question, *, preview=True, **kwargs):
        queue = asyncio.Queue(maxsize=64)
        listening = True
        sequence = 0
        terminal_emitted = False

        async def push(event):
            nonlocal sequence, terminal_emitted
            if event.sequence > sequence:
                sequence = event.sequence
            if event.type in ('task.completed', 'task.failed', 'task.cancelled', 'task.superseded'):
                terminal_emitted = True
            if listening:
                await queue.put(event)

        async def emit(kind, data):
            nonlocal sequence, terminal_emitted
            sequence += 1
            event = AgentEvent(type=kind, data=data, sequence=sequence)
            if kind in ('task.completed', 'task.failed', 'task.cancelled', 'task.superseded'):
                terminal_emitted = True
            if listening:
                await queue.put(event)

        async def produce():
            try:
                result = await self.run(question, **kwargs, _events=emit, _event_sink=push, _preview=preview)
                if not terminal_emitted:
                    await emit('task.completed', {'result': result.model_dump(mode='json')})
            except asyncio.CancelledError:
                raise
            except Exception as error:
                if not terminal_emitted:
                    await emit('task.failed', {'error_type': type(error).__name__,
                        'message': '执行或收尾未正常结束，请通过任务 ID 回查已提交状态；不要直接重放外部动作。'})
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

    async def close(self, *, graceful: bool = False, timeout: float = 30.0):
        timeout = _validate_close_options(graceful, timeout)
        if not graceful:
            await asyncio.gather(*(scheduler.close() for scheduler in tuple(self._schedulers.values())))
            for session in tuple(self.sessions._residents.values()):
                if session.managed_heartbeat:
                    await session._scheduler.close()
            if self._running:
                raise RuntimeError('先取消并等待活动任务，再关闭 Agent')
            await self.sessions.close(graceful=False, timeout=timeout)
            self._closed = True
            self.cache_scopes.close()
            return
            # storage 和两个 provider 均由调用方注入，关闭责任留在调用方。

        # 优雅停机：先阻止继续消费与调度新任务，再限时等待在途单任务自然收束，超时后取消并落库释放租约。
        residents = tuple(self.sessions._residents.values())
        schedulers = list(self._schedulers.values())
        for session in residents:
            session._stopping = True
            if session._scheduler is not None and session._scheduler not in schedulers:
                schedulers.append(session._scheduler)
        for scheduler in schedulers:
            scheduler._stop.set()
        self._closed = True

        deadline = monotonic() + timeout
        current = asyncio.current_task()
        while True:
            pending = {
                task
                for task in (
                    *(s._worker for s in tuple(self.sessions._residents.values())),
                    *(sc._worker for sc in schedulers),
                    *tuple(self._running),
                )
                if task is not None and task is not current and not task.done()
            }
            if not pending:
                break
            remaining = deadline - monotonic()
            if remaining <= 0:
                break
            await asyncio.wait(pending, timeout=remaining)

        # 若超时期满仍有任务在途，依次打断并取消运行中的任务，等待 Agent.run 完成 journal.abort 与租约释放。
        for session in tuple(self.sessions._residents.values()):
            active = session._active
            if active is not None:
                with suppress(StorageConflict):
                    active.control.interrupt('cancelled')
        to_cancel = {
            task
            for task in (
                *tuple(self._running),
                *(s._worker for s in tuple(self.sessions._residents.values())),
            )
            if task is not None and task is not current and not task.done()
        }
        for task in to_cancel:
            task.cancel()
        if to_cancel:
            await asyncio.gather(*to_cancel, return_exceptions=True)

        remaining_schedulers = {
            sc._worker
            for sc in schedulers
            if sc._worker is not None and sc._worker is not current and not sc._worker.done()
        }
        for task in remaining_schedulers:
            task.cancel()
        if remaining_schedulers:
            await asyncio.gather(*remaining_schedulers, return_exceptions=True)

        for scheduler in schedulers:
            key = (scheduler.owner, scheduler.tree_id)
            if self._schedulers.get(key) is scheduler:
                del self._schedulers[key]

        await self.sessions.close(graceful=True, timeout=max(0.001, deadline - monotonic()))
        self.cache_scopes.close()
        # storage 和两个 provider 均由调用方注入，关闭责任留在调用方。
