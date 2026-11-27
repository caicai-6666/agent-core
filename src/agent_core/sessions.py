"""任务树、纯内存会话与冻结分身的驻留、排队和交互控制。"""
import asyncio
import hashlib
import json
import logging
from collections import deque
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import datetime
from math import isfinite
from time import monotonic

from pydantic import AwareDatetime, BaseModel, ConfigDict, ValidationError

from .adapters import SQLiteStorage
from .core import AgentInput
from .core.model import ContextInjection
from .core.prompt import PromptOptions
from .core.tool.hooks import normalize_tool_hooks
from .model.question_tree import new_tree_id
from .scheduler import Dispatch, Reminders, SchedulerSettings, utc_now
from .storage import StorageConflict, Write
from .tasks import DisplayRecorder, TaskRepository, _completed_display_data

_UNSET = object()
DEFAULT_MERGE_SEPARATOR = '\n\n---\n\n'
logger = logging.getLogger(__name__)


class QueueStatus(BaseModel):
    """任务树持久化队列与活动租约的只读状态快照。"""
    model_config = ConfigDict(frozen=True)
    tree_id: str
    ready_count: int
    deferred_count: int
    active_task_id: str | None
    lease_owner_id: str | None
    lease_expires_at: AwareDatetime | None
    lease_expired: bool


class ResumeResult(BaseModel):
    """队列恢复与消费唤醒的结果摘要。"""
    model_config = ConfigDict(frozen=True)
    recovered_task_id: str | None
    queued_count: int
    draining: bool


class SessionExpired(RuntimeError):
    """内存会话释放后不能以同一引用继续。"""


class ReplayUnavailableError(RuntimeError):
    """请求的事件序号超出重放窗口或尚未产生，调用方应通过 tree()/display 快照重同步。"""


class TaskInterrupted(RuntimeError):
    def __init__(self, status):
        super().__init__(status)
        self.status = status


class _Control:
    """仅取消仍在生成的模型请求；已开始的工具及其检查点必须先收束。"""
    def __init__(self):
        self.started = False
        self.reason = None
        self.model = None
        self.finishing = False

    def interrupt(self, reason):
        if self.reason or self.finishing:
            raise StorageConflict('任务已在结束或打断中')
        self.reason = reason
        if self.model is not None:
            self.model.cancel()

    def checkpoint(self):
        if self.reason:
            raise asyncio.CancelledError()

    def before_tool(self, *, final):
        self.checkpoint()
        self.finishing = final

    async def generate(self, request):
        self.model = asyncio.ensure_future(request)
        if self.reason:
            self.model.cancel()
        try:
            return await self.model
        finally:
            self.model = None


class _ResidentRepository(TaskRepository):
    def __init__(self, storage, *, owner, tree_id, baseline, workspace_retention):
        super().__init__(storage, owner=owner, tree_id=tree_id, workspace_retention=workspace_retention)
        self.baseline = baseline
        self.tip = None
        self.snapshot = baseline

    async def context(self, parent_id):
        # head 指向已终止任务。只在这个水位上复用；跳分支或冷启动走真实父链。
        if parent_id is None:
            return self.baseline.model_copy(deep=True)
        if parent_id == self.tip and self.workspace_retention == 'latest':
            # 其他进程也可能推进树头，缓存命中不能复活已经释放的历史版本。
            task, _ = await self.load(parent_id)
            if task.workspace is None:
                raise StorageConflict('该节点的工作区版本已释放，无法恢复历史快照')
        if parent_id != self.tip:
            task, _ = await self.load(parent_id)
            if task.status == 'generating':
                raise StorageConflict('无法冻结在途任务')
            if task.parent_id == self.tip:
                snapshot = self.extend_context(self.snapshot, [task])
            else:
                snapshot = self.extend_context(self.baseline, await self.path(parent_id))
            self.tip, self.snapshot = parent_id, snapshot
        return self.snapshot.model_copy(deep=True)


class SessionTask:
    """一个任务的结果与有界事件窗口；断开观察不会取消队列中的任务。"""
    def __init__(self, task_id):
        self._activate = self._wake_observer = None
        self._observers = 0
        self.id = task_id
        self.status = 'queued'
        self._done = asyncio.Event()
        self._changed = asyncio.Event()
        self._events = deque(maxlen=256)
        self._sequence = 0
        self._result = self._error = None

    async def activate(self):
        """显式激活待执行项；可重复调用，不重放任务。"""
        if self._activate:
            await self._activate(self.id)
        return self

    async def _push(self, event):
        """接收已由 DisplayRecorder 统一编号的事件并唤醒 SSE 订阅者。"""
        if event.sequence != self._sequence + 1:
            raise RuntimeError('事件序号不连续')
        self._events.append(event)
        self._sequence = event.sequence
        self._changed.set()
        self._changed = asyncio.Event()

    async def emit(self, kind, data):
        from .agent import AgentEvent
        self._sequence += 1
        self._events.append(AgentEvent(type=kind, data={**data, 'task_id': self.id}, sequence=self._sequence))
        self._changed.set()
        self._changed = asyncio.Event()

    async def finish(self, status, *, result=None, error=None, recorder=None):
        self.status, self._result, self._error = status, result, error
        sse_data = {'result': result.model_dump(mode='json')} if result is not None else {}
        if recorder is not None and not recorder.pruned:
            # 先将终态展示持久化到 display:<task_id>，再通过 sink 推给 SSE 订阅者，避免前端回查竞态。
            display_data = _completed_display_data(result) if result is not None else {}
            await recorder.finish(status, display_data, sse_data=sse_data)
        else:
            await self.emit('task.' + status, sse_data)
        self._done.set()
        self._activate = self._wake_observer = None

    @contextmanager
    def _observe(self):
        self._observers += 1
        try:
            if self._wake_observer and not self._done.is_set():
                self._wake_observer()
            yield
        finally:
            self._observers -= 1

    async def result(self):
        with self._observe():
            await self._done.wait()
        if self._error is not None:
            raise self._error
        return self._result

    async def events(self, after_sequence: int = 0):
        if type(after_sequence) is not int or after_sequence < 0:
            raise ValueError('after_sequence 必须为非负整数')
        with self._observe():
            first = self._sequence - len(self._events)
            if after_sequence < first or after_sequence > self._sequence:
                raise ReplayUnavailableError('事件序号超出当前内存重放窗口，请通过 tree() 快照重同步')
            cursor = after_sequence
            while True:
                first = self._sequence - len(self._events)
                if cursor < first:
                    raise ReplayUnavailableError('事件窗口已过期，请通过 tree() 快照重同步')
                if cursor < self._sequence:
                    event = self._events[cursor - first]
                    cursor += 1
                    yield event
                elif self._done.is_set():
                    return
                else:
                    await self._changed.wait()


@dataclass
class _Active:
    task: SessionTask
    control: _Control
    display: DisplayRecorder | None = None


class Session:
    def __init__(self, runtime, owner, identifier, kind, mode, repository, reminder_repository, tools):
        self.runtime, self.owner, self.id = runtime, owner, identifier
        self.kind, self.mode = kind, mode
        self.repository = repository
        self.role = None
        self.tools = tuple(tools)
        self.guidance_factory = None
        self.tool_hooks = ()
        self.prompt_options = None
        self._scheduler = None
        self.managed_heartbeat = None
        self._reminder_repository = reminder_repository
        self._lock = asyncio.Lock()
        self._tickets = {}
        # 单任务 guidance_factory 覆盖为进程内回调，不写入持久队列 JSON，任务终态或会话释放时清理。
        self._task_guidance = {}
        self._active = self._worker = None
        self._expired = False
        self._stopping = False
        self._pins = 0
        self.touched = monotonic()

    @property
    def busy(self):
        return bool(self._pins or self._worker or self._active or self._lock.locked())

    @property
    def evictable(self):
        # 显式启动的轮询器负责持续存活，只有关闭轮询后才进入普通空闲驱逐。
        return not self.busy and not (self._scheduler and self._scheduler.running)

    def _touch(self):
        if self._expired or self._stopping or self.runtime.closed:
            raise SessionExpired('会话已释放，请重新打开本体或创建新的临时会话')
        self.touched = monotonic()

    async def _queue(self):
        return await self.repository.storage.read(self.repository.scope, 'runtime_queue')

    @staticmethod
    def _enqueue(
        items,
        item,
        merge_with_previous,
        *,
        merge_separator: str = DEFAULT_MERGE_SEPARATOR,
        merge_max_messages: int | None = None,
        merge_max_bytes: int | None = None,
        merge_max_attachments: int | None = None,
        merge_interval_seconds: int | float | None = None,
    ):
        if not isinstance(merge_separator, str):
            raise ValueError('merge_separator 必须为字符串')
        if merge_with_previous:
            # 逆序扫描保持原 FIFO 位置，只选择尚在普通队列中且同来源、同级的任务。
            for index in reversed(range(len(items))):
                target = items[index]
                if (target['priority'] != item['priority'] or target.get('deferred')
                        or target.get('replace') is not None
                        or target['source'] not in ('user', 'business')
                        or target['source'] != item['source']
                        or target.get('parent_id', 'latest') != item.get('parent_id', 'latest')
                        or target.get('advance_head', True) != item.get('advance_head', True)):
                    continue
                # 命中最近一条同级且同来源的可合并候选后，若因任一上限不能合并，必须 break 另起新任务；
                # 否则继续往前扫描会把新消息合并到更早的任务里，造成队列执行乱序。
                target_count = target.get('merged_count', 1)
                if merge_max_messages is not None and target_count >= merge_max_messages:
                    break
                if merge_interval_seconds is not None:
                    prev_raw = target.get('last_merged_at') or target.get('created_at')
                    new_raw = item.get('created_at')
                    if not prev_raw or not new_raw:
                        break
                    delta = (datetime.fromisoformat(new_raw) - datetime.fromisoformat(prev_raw)).total_seconds()
                    # 时钟回拨（delta < 0）或超过窗口上限时不再合并。
                    if delta < 0 or delta > merge_interval_seconds:
                        break
                combined_question = target['question'] + merge_separator + item['question']
                if merge_max_bytes is not None and len(combined_question.encode('utf-8')) > merge_max_bytes:
                    break
                attachments = {ref['file_id']: ref for ref in target['attachments']}
                for ref in item['attachments']:
                    if attachments.setdefault(ref['file_id'], ref) != ref:
                        raise ValueError('同一附件在合并任务中声明了不同类型')
                if merge_max_attachments is not None and len(attachments) > merge_max_attachments:
                    break
                # 保序去重 supplemental，兼容不含 supplemental_parts 的历史队列项。
                existing_parts = target.get('supplemental_parts')
                if existing_parts is None:
                    parts = [target['supplemental']] if target.get('supplemental') else []
                else:
                    parts = list(existing_parts)
                new_sup = item.get('supplemental', '')
                target_sup = target.get('supplemental', '')
                if new_sup and new_sup not in parts and new_sup != target_sup:
                    parts.append(new_sup)
                    merged_supplemental = '\n\n'.join(parts)
                else:
                    merged_supplemental = target_sup
                merged = {
                    **target,
                    'question': combined_question,
                    'attachments': list(attachments.values()),
                    'supplemental': merged_supplemental,
                    'supplemental_parts': parts,
                    'preview': target.get('preview', True) and item['preview'],
                    'merged_count': target_count + 1,
                    'last_merged_at': item['created_at'],
                }
                return [*items[:index], merged, *items[index + 1:]], target['id']
        return [*items, item], item['id']

    async def submit(self, question, *, priority=0, merge_with_previous=False, source='user', attachments=(), thinking_effort='none',
                     supplemental='', replace_task_id=None, preview=True, external_id=None, created_at=None, deferred=False,
                     guidance_factory=_UNSET, parent_id: str | None = 'latest', advance_head: bool = _UNSET,
                     merge_separator: str = DEFAULT_MERGE_SEPARATOR,
                     merge_max_messages: int | None = None,
                     merge_max_bytes: int | None = None,
                     merge_max_attachments: int | None = None,
                     merge_interval_seconds: int | float | None = None):
        if type(priority) is not int or source not in ('user', 'business') or not isinstance(supplemental, str):
            raise ValueError('需要整数优先级、有效输入来源和文本上下文')
        if type(preview) is not bool:
            raise ValueError('preview 必须为布尔值')
        if external_id is not None and (not isinstance(external_id,str) or not external_id.strip()):
            raise ValueError('外部任务 ID 必须为非空字符串')
        if type(deferred) is not bool:
            raise ValueError('deferred 必须为布尔值')
        if type(merge_with_previous) is not bool:
            raise ValueError('merge_with_previous 必须为布尔值')
        if merge_with_previous and (deferred or replace_task_id is not None):
            raise ValueError('只有就绪的普通输入可以请求合并')
        if not isinstance(merge_separator, str):
            raise ValueError('merge_separator 必须为字符串')
        if merge_max_messages is not None and (type(merge_max_messages) is not int or merge_max_messages < 1):
            raise ValueError('merge_max_messages 必须为正整数')
        if merge_max_bytes is not None and (type(merge_max_bytes) is not int or merge_max_bytes < 1):
            raise ValueError('merge_max_bytes 必须为正整数')
        if merge_max_attachments is not None and (type(merge_max_attachments) is not int or merge_max_attachments < 0):
            raise ValueError('merge_max_attachments 必须为非负整数')
        if merge_interval_seconds is not None and (
            isinstance(merge_interval_seconds, bool)
            or not isinstance(merge_interval_seconds, (int, float))
            or not isfinite(merge_interval_seconds)
            or merge_interval_seconds < 0
        ):
            raise ValueError('merge_interval_seconds 必须为有限非负数')
        if parent_id not in ('latest', None):
            if not isinstance(parent_id, str) or len(parent_id) != 32 or any(c not in '0123456789abcdef' for c in parent_id):
                raise ValueError('parent_id 必须为 latest、None 或 32 位小写十六进制任务 ID')
        if replace_task_id is not None and parent_id != 'latest':
            raise ValueError('定向替代任务不能同时指定历史 parent_id')
        if advance_head is _UNSET:
            # 定向替代未显式指定 advance_head 时，在消费阶段沿用被替代任务的策略；普通任务默认为 True。
            queued_advance_head = None if replace_task_id is not None else True
        else:
            if type(advance_head) is not bool:
                raise ValueError('advance_head 必须为布尔值')
            queued_advance_head = advance_head
        if queued_advance_head is False and self.repository.workspace_retention == 'latest':
            raise StorageConflict('workspace_retention=latest 模式下不允许 advance_head=False')
        if guidance_factory is not _UNSET and guidance_factory is not None and not callable(guidance_factory):
            raise TypeError('guidance_factory 必须可调用或为 None')
        request = AgentInput(question=question, attachments=list(attachments), source=source, thinking_effort=thinking_effort,
                             **({'created_at':created_at} if created_at is not None else {}))
        ticket = SessionTask(new_tree_id())
        created_iso = request.created_at.isoformat()
        item = {'id': ticket.id, 'question': question, 'priority': priority, 'source': source,
                'replace': replace_task_id, 'attachments': [a.model_dump(mode='json') for a in request.attachments],
                'thinking_effort': thinking_effort, 'supplemental': supplemental,
                'supplemental_parts': [supplemental] if supplemental else [],
                'preview': preview,
                'parent_id': parent_id, 'advance_head': queued_advance_head,
                'created_at': created_iso, 'deferred': deferred,
                'merged_count': 1, 'last_merged_at': created_iso}
        receipt_key = None
        if external_id is not None:
            receipt_key = 'input:' + hashlib.sha256(external_id.encode()).hexdigest()
            fingerprint = {**item,'created_at':item['created_at'] if created_at is not None else None}
            fingerprint.pop('id')
            fingerprint.pop('supplemental_parts', None)
            fingerprint.pop('merged_count', None)
            fingerprint.pop('last_merged_at', None)
            # 默认请求保持旧版指纹，历史 external_id 可继续重投。
            if parent_id == 'latest':
                fingerprint.pop('parent_id', None)
            if fingerprint.get('advance_head') is not False:
                fingerprint.pop('advance_head', None)
            if merge_with_previous:
                fingerprint['merge_with_previous'] = True
            if merge_separator != DEFAULT_MERGE_SEPARATOR:
                fingerprint['merge_separator'] = merge_separator
            if merge_max_messages is not None:
                fingerprint['merge_max_messages'] = merge_max_messages
            if merge_max_bytes is not None:
                fingerprint['merge_max_bytes'] = merge_max_bytes
            if merge_max_attachments is not None:
                fingerprint['merge_max_attachments'] = merge_max_attachments
            if merge_interval_seconds is not None:
                fingerprint['merge_interval_seconds'] = merge_interval_seconds
            fingerprint = hashlib.sha256(json.dumps(fingerprint,ensure_ascii=False,sort_keys=True).encode()).hexdigest()
        await self.runtime.evict_idle()
        async with self._lock:
            self._touch()
            while True:
                if receipt_key:
                    receipt = await self.repository.storage.read(self.repository.scope,receipt_key)
                    if receipt:
                        if receipt.value['fingerprint'] != fingerprint:
                            raise StorageConflict('外部任务 ID 已用于不同请求')
                        identifier = receipt.value['id']
                        existing = self._tickets.get(identifier)
                        if existing is None:
                            existing = SessionTask(identifier)
                            queued = await self._queue()
                            if queued and any(i['id'] == identifier and i.get('deferred') for i in queued.value['items']):
                                existing.status = 'pending_activation'
                            self._tickets[identifier] = existing
                        existing._activate, existing._wake_observer = self.activate, self._wake
                        self._wake()
                        return existing
                active = self._target(replace_task_id) if replace_task_id is not None else None
                record = await self._queue()
                items = record.value['items'] if record else []
                pending, identifier = self._enqueue(
                    items,
                    item,
                    merge_with_previous,
                    merge_separator=merge_separator,
                    merge_max_messages=merge_max_messages,
                    merge_max_bytes=merge_max_bytes,
                    merge_max_attachments=merge_max_attachments,
                    merge_interval_seconds=merge_interval_seconds,
                )
                try:
                    writes = [Write('runtime_queue', record.version if record else None, {'items': pending})]
                    if receipt_key:
                        writes.append(Write(receipt_key,None,{'id':identifier,'fingerprint':fingerprint}))
                    await self.repository.storage.atomic(self.repository.scope,writes)
                    break
                except StorageConflict:
                    continue
            if identifier != ticket.id:
                ticket = self._tickets.get(identifier) or SessionTask(identifier)
            self._tickets[ticket.id] = ticket
            if guidance_factory is not _UNSET:
                self._task_guidance[ticket.id] = guidance_factory
            ticket._activate, ticket._wake_observer = self.activate, self._wake
            if deferred:
                ticket.status = 'pending_activation'
            else:
                self._wake()
            if active:
                # 若原任务在入队期间已进入终态，消费者会拒绝替代请求，不会改停下一任务。
                if not active.control.finishing and not active.task._done.is_set():
                    active.control.interrupt('superseded')
        return ticket

    async def activate(self, task_id):
        async with self._lock:
            self._touch()
            while True:
                queue = await self._queue()
                items = queue.value['items'] if queue else []
                entry = next((item for item in items if item['id']==task_id),None)
                if entry is None:
                    # 可能已被另一个执行者领取；先验证标识确实属于这棵树。
                    await self.repository.load(task_id)
                    break
                if not entry.get('deferred'):
                    break
                entry['deferred'] = False
                try:
                    await self.repository.storage.atomic(self.repository.scope,[Write('runtime_queue',queue.version,{'items':items})])
                    break
                except StorageConflict:
                    continue
            ticket = self._tickets.setdefault(task_id,SessionTask(task_id))
            if ticket.status == 'pending_activation':
                ticket.status = 'queued'
            self._wake()
            return ticket

    @property
    def active_task_id(self):
        return self._active.task.id if self._active and self._active.control.started else None

    async def _scheduled(self, message, identifier, dispatch, publish):
        async with self._lock:
            if self._active or self._expired or self._stopping:
                return
            queue = await self._queue()
            if queue and any(not i.get('deferred') for i in queue.value['items']):
                return
            ticket = SessionTask(identifier)
            recorder = DisplayRecorder(self.repository, identifier, sink=ticket._push)
            active = self._active = _Active(ticket, _Control(), display=recorder)
        try:
            result = await self.runtime.agent.run(message, owner=self.owner, tree_id=self.id,
                task_id=identifier, publish=publish, _dispatch=dispatch, _control=active.control,
                _display=recorder,
                _repository=self.repository, _reminder_repository=self._reminder_repository, _session_tools=self.tools, _role=self.role,
                **({'_guidance_factory': self.guidance_factory} if self.guidance_factory is not None else {}),
                **({'_tool_hooks': dict(self.tool_hooks)} if self.tool_hooks else {}),
                **({'_prompt_options': self.prompt_options} if self.prompt_options is not None else {}))
            await ticket.finish('completed', result=result, recorder=recorder)
            return result
        except asyncio.CancelledError:
            if not active.control.reason:
                raise
            task, _ = await self.repository.load(identifier)
            if task.status not in ('superseded', 'cancelled'):
                raise StorageConflict('调度任务停止终态未确认')
            await ticket.finish(task.status, error=TaskInterrupted(task.status), recorder=recorder)
        finally:
            if not ticket._done.is_set():
                await ticket.finish('failed', error=RuntimeError('调度任务未正常结束'),
                                    recorder=recorder if active.control.started else None)
            self._active = None
            self.touched = monotonic()

    def scheduler(self, *, settings=None, publish=None):
        from .scheduler import AgentScheduler, SchedulerSettings
        self._touch()
        if self.role is not None and self.kind == 'fork':
            parent = self.runtime.resident(self.owner,self._reminder_repository.tree_id)
            if parent is None:
                raise SessionExpired('提醒已归属本体；请恢复本体后启动调度')
            return parent.scheduler(settings=settings,publish=publish)
        if self.kind != 'memory' and self.role is None:
            return self.runtime.agent.scheduler(owner=self.owner, tree_id=self._reminder_repository.tree_id,
                                                settings=settings, publish=publish)
        config = settings or SchedulerSettings()
        if self._scheduler is None or self._scheduler._stop.is_set():
            self._scheduler = AgentScheduler(self.runtime.agent, self.owner, self.id, config, publish, session=self)
        elif self._scheduler.settings != config or self._scheduler.publish != publish:
            raise StorageConflict('内存会话已绑定其他调度配置')
        return self._scheduler

    def _target(self, task_id):
        if self.mode != 'interruptible':
            raise StorageConflict('队列模式只允许调整等待顺序')
        if not self._active or not self._active.control.started or self._active.task.id != task_id:
            raise StorageConflict('指定任务已非当前执行任务')
        if self._active.control.reason or self._active.control.finishing:
            raise StorageConflict('指定任务已在结束或打断中')
        return self._active

    async def stop(self, task_id):
        async with self._lock:
            self._touch()
            target = self._target(task_id)
            target.control.interrupt('cancelled')
        await target.task._done.wait()
        return target.task.status

    def _wake(self):
        if not self._stopping and (self._worker is None or self._worker.done()):
            self._worker = asyncio.create_task(self._drain())

    async def resume(self, *, recover_expired: bool = True, recover_unleased: bool = False) -> ResumeResult:
        """幂等恢复过期在途任务并唤醒就绪队列消费。"""
        if type(recover_expired) is not bool:
            raise ValueError('recover_expired 必须为布尔值')
        if type(recover_unleased) is not bool:
            raise ValueError('recover_unleased 必须为布尔值')
        self._touch()
        recovered = (
            await self.repository.recover_expired_active(recover_unleased=recover_unleased)
            if recover_expired
            else None
        )
        async with self._lock:
            self._touch()
            record = await self._queue()
            items = record.value['items'] if record else []
            queued_count = sum(1 for entry in items if not entry.get('deferred'))
            if queued_count > 0 and not self._stopping:
                self._wake()
            draining = bool(self._worker and not self._worker.done())
        return ResumeResult(
            recovered_task_id=recovered.id if recovered else None,
            queued_count=queued_count,
            draining=draining,
        )

    async def _observe_other_workers(self, queued):
        # 另一个进程可能先领取同一持久队列；本地等待者仍以权威终态收束。
        for identifier, ticket in list(self._tickets.items()):
            if identifier in queued:
                continue
            try:
                task, _ = await self.repository.load(identifier)
            except KeyError:
                rejected = await self.repository.storage.read(self.repository.scope, 'runtime_rejected:' + identifier)
                if rejected is None:
                    raise
                reason = rejected.value
                error_type = reason.get('error_type')
                if error_type == 'KeyError':
                    err = KeyError(reason.get('message', '当前用户任务树中不存在该任务'))
                elif error_type in ('ValueError', 'ValidationError'):
                    err = ValueError(reason.get('message', '排队任务输入校验失败'))
                elif error_type == 'TypeError':
                    err = TypeError(reason.get('message', '排队任务参数类型不合法'))
                elif reason.get('message'):
                    err = StorageConflict(reason['message'])
                else:
                    err = StorageConflict('原任务未被替代，未执行新指令')
                await ticket.finish('failed', error=err)
                self._tickets.pop(identifier)
                self._task_guidance.pop(identifier, None)
                continue
            if task.status == 'generating':
                ticket.status = 'running'
                continue
            if task.status == 'complete':
                try:
                    result = task.cached_result()
                except StorageConflict as error:
                    # 较晚观察到终态时，任务仍是成功的，只是完整结果所需的工作区已过期。
                    await ticket.finish('completed', error=error)
                else:
                    await ticket.finish('completed', result=result)
            else:
                error = (TaskInterrupted(task.status) if task.status in ('cancelled', 'superseded')
                         else RuntimeError('任务执行失败，请回查任务记录'))
                await ticket.finish(task.status, error=error)
            self._tickets.pop(identifier)
            self._task_guidance.pop(identifier, None)

    async def _select_next(self, ready, queue):
        # 定向替代紧随旧任务，普通业务按优先级及到达顺序领取。
        item = max(ready,key=lambda x:(x['replace'] is not None,x['priority']))
        rest = [entry for entry in queue.value['items'] if entry['id'] != item['id']]
        consume = Write('runtime_queue',queue.version,{'items':rest})
        if item['replace'] or self.kind=='fork' or not self.runtime.agent.reminder_tools:
            return item, (consume,)

        repo = self.repository
        await Reminders(repo).materialize_due(utc_now())
        due = await repo.storage.read(repo.scope,'reminder_queue')
        if not due or not due.value['items']:
            # 领取时若另一执行者已经物化到期提醒，CAS 失败后重新比较来源优先级。
            guard = Write('reminder_queue',due.version if due else None,{'items':[]})
            return item, (consume,guard)
        first, *remaining = due.value['items']
        reminder = dict(id=first['id'],source='reminder',question=first['message'],replace=None,
                        attachments=[],thinking_effort='none',supplemental='',created_at=first.get('scheduled_for'))
        return reminder, (Write('runtime_queue',queue.version,queue.value),
                          Write('reminder_queue',due.version,{'items':remaining}))

    async def _is_dispatch_contention(self, dispatch):
        # 仅当树版本、在途标记或队列版本已变化时视为并发竞争，允许重新选队。
        tree = await self.repository.storage.read(self.repository.scope, 'tree')
        if (tree.version if tree else None) != dispatch.tree_version:
            return True
        if tree and tree.value.get('active') is not None:
            return True
        for write in dispatch.writes:
            if write.key in ('runtime_queue', 'reminder_queue'):
                current = await self.repository.storage.read(self.repository.scope, write.key)
                if (current.version if current else None) != write.expected:
                    return True
        return False

    async def _reject_queued(self, item_id, reason):
        # 永久性启动失败（如父节点不存在或历史工作区已释放）须从队列移除并写拒绝标记，避免死循环重试。
        while True:
            record = await self._queue()
            items = record.value['items'] if record else []
            rest = [entry for entry in items if entry['id'] != item_id]
            if len(rest) == len(items):
                return False
            try:
                await self.repository.storage.atomic(self.repository.scope, [
                    Write('runtime_queue', record.version, {'items': rest}),
                    Write('runtime_rejected:' + item_id, None, reason),
                ])
                return True
            except StorageConflict:
                continue

    async def _drain(self):
        repo = self.repository
        last_stale_check = 0.0
        try:
            while True:
                has_remote_active = False
                async with self._lock:
                    if self._stopping:
                        self._worker = None
                        return
                    record = await self._queue()
                    items = record.value['items'] if record else []
                    queued_ids = {item['id'] for item in items}
                    await self._observe_other_workers(queued_ids)
                    if not items and not self._tickets:
                        self._worker = None
                        return
                    items = [item for item in items if not item.get('deferred')]
                    if (not items and self._tickets.keys() <= queued_ids
                            and not any(t._observers for t in self._tickets.values())):
                        self._worker = None
                        return
                    # max 保留首个同分项；定向替代在旧任务确认结束后优先接续。
                    tree = await repo.storage.read(repo.scope, 'tree')
                    has_remote_active = (
                        self._active is None
                        and tree is not None
                        and tree.value['active'] is not None
                    )
                    waiting = self._active is not None or not items or (tree and tree.value['active'] is not None)
                    if not waiting:
                        try:
                            item, writes = await self._select_next(items, record)
                        except StorageConflict:
                            continue
                        ticket = self._tickets.setdefault(item['id'], SessionTask(item['id']))
                        effective_parent_id = item.get('parent_id', 'latest')
                        effective_advance_head = item.get('advance_head', True)
                        if item['replace']:
                            try:
                                old, _ = await repo.load(item['replace'])
                            except KeyError as error:
                                msg = error.args[0] if error.args and isinstance(error.args[0], str) else str(error)
                                try:
                                    await repo.storage.atomic(repo.scope, [*writes,
                                        Write('runtime_rejected:' + ticket.id, None,
                                              {'error_type': 'KeyError', 'message': msg})])
                                except StorageConflict:
                                    continue
                                await ticket.finish('failed', error=error)
                                self._tickets.pop(ticket.id, None)
                                self._task_guidance.pop(ticket.id, None)
                                continue
                            valid_target = (
                                old.status == 'superseded'
                                and tree is not None
                                and (
                                    (old.advance_head and tree.value['head'] == old.id)
                                    or (not old.advance_head and old.id in tree.value['tasks'])
                                )
                            )
                            if not valid_target:
                                try:
                                    await repo.storage.atomic(repo.scope, [*writes,
                                        Write('runtime_rejected:' + ticket.id, None, {'target': old.id})])
                                except StorageConflict:
                                    continue
                                await ticket.finish('failed', error=StorageConflict('原任务未被替代，未执行新指令'))
                                self._tickets.pop(ticket.id)
                                self._task_guidance.pop(ticket.id, None)
                                continue
                            if not old.advance_head:
                                # 被替代任务为不推进主线的分支探索时，替代任务续接该分支且默认沿用 advance_head=False。
                                effective_parent_id = old.id
                                if effective_advance_head is None:
                                    effective_advance_head = False
                            elif effective_advance_head is None:
                                effective_advance_head = True
                        elif effective_advance_head is None:
                            effective_advance_head = True
                        dispatch = Dispatch(item['source'], tree.version if tree else None, writes)
                        scheduled = item['source'] == 'reminder'
                        recorder = DisplayRecorder(repo, ticket.id, sink=None if scheduled else ticket._push)
                        self._active = _Active(ticket, _Control(), display=recorder)
                        ticket.status = 'running'
                if waiting:
                    await asyncio.sleep(.05)
                    if self._stopping:
                        continue
                    if has_remote_active:
                        # 远程/旧进程残留的过期租约按节流间隔自动探测并收束，恢复后立即继续消费。
                        throttle = min(5.0, self.runtime.agent.lease_seconds / 3)
                        if monotonic() - last_stale_check >= throttle:
                            last_stale_check = monotonic()
                            recovered = await repo.recover_expired_active()
                            if recovered is not None:
                                continue
                    continue
                try:
                    if self._stopping:
                        return
                    scheduler = self._scheduler or self.runtime.agent._schedulers.get((self.owner,self.id))
                    task_guidance = self._task_guidance.get(ticket.id, _UNSET)
                    effective_guidance = (
                        task_guidance
                        if task_guidance is not _UNSET
                        else (self.guidance_factory if self.guidance_factory is not None else _UNSET)
                    )
                    result = await self.runtime.agent.run(item['question'], owner=self.owner, tree_id=self.id,
                        task_id=ticket.id, parent_id=effective_parent_id, advance_head=effective_advance_head,
                        attachments=item['attachments'], thinking_effort=item['thinking_effort'],
                        supplemental=item['supplemental'], _display=recorder,
                        publish=scheduler.publish if scheduled and scheduler else None,
                        _preview=False if scheduled else item.get('preview', True),
                        _repository=repo, _reminder_repository=self._reminder_repository, _session_tools=self.tools,
                        _control=self._active.control, _dispatch=dispatch, _role=self.role, created_at=item.get('created_at'),
                        **({'_guidance_factory': effective_guidance} if effective_guidance is not _UNSET else {}),
                        **({'_tool_hooks': dict(self.tool_hooks)} if self.tool_hooks else {}),
                        **({'_prompt_options': self.prompt_options} if self.prompt_options is not None else {}))
                except StorageConflict as error:
                    # 入队/领取竞争只在请求仍留队且确有版本竞争时重选；非竞争冲突（如历史工作区已释放）直接出队失败。
                    if not self._active.control.started:
                        if scheduled:
                            self._tickets.pop(ticket.id, None)
                            continue
                        if await self._is_dispatch_contention(dispatch):
                            continue
                        await self._reject_queued(ticket.id, {'error_type': 'StorageConflict', 'message': str(error)})
                        await ticket.finish('failed', error=error)
                    else:
                        await ticket.finish('failed', error=error, recorder=recorder)
                except (ValueError, TypeError, ValidationError, KeyError) as error:
                    # 确定性输入/校验错误在任务尚未正式开始前直接隔离出队，避免毒丸阻塞后续排队项。
                    if not self._active.control.started:
                        if scheduled:
                            self._tickets.pop(ticket.id, None)
                            continue
                        msg = error.args[0] if isinstance(error, KeyError) and error.args and isinstance(error.args[0], str) else str(error)
                        await self._reject_queued(ticket.id, {'error_type': type(error).__name__, 'message': msg})
                        await ticket.finish('failed', error=error)
                    else:
                        await ticket.finish('failed', error=error, recorder=recorder)
                except asyncio.CancelledError:
                    if not self._active.control.started:
                        if self._stopping:
                            return
                        raise
                    task, _ = await repo.load(ticket.id)
                    if task.status in ('cancelled', 'superseded'):
                        await ticket.finish(task.status, error=TaskInterrupted(task.status), recorder=recorder)
                    else:
                        await ticket.finish('failed', error=StorageConflict('停止终态未确认，请回查任务'), recorder=recorder)
                except Exception as error:
                    if not self._active.control.started:
                        if self._stopping:
                            return
                        raise
                    await ticket.finish('failed', error=error, recorder=recorder)
                else:
                    await ticket.finish('completed', result=result, recorder=recorder)
                finally:
                    self._active = None
                    if ticket._done.is_set():
                        self._tickets.pop(ticket.id, None)
                        self._task_guidance.pop(ticket.id, None)
                    self.touched = monotonic()
        except Exception as error:
            # 瞬时存储或基础设施故障时保留持久队列供后续 resume() 重试，仅唤醒当前本地等待方。
            logger.warning('会话队列消费异常退出：%s', type(error).__name__)
            async with self._lock:
                for ticket in self._tickets.values():
                    await ticket.finish('failed', error=error)
                self._tickets.clear()
                self._task_guidance.clear()
                self._worker = self._active = None
        finally:
            self._active = None
            if self._worker is asyncio.current_task():
                self._worker = None

    def _live_displays(self):
        # 仅在任务已正式开始后叠加内存实时展示快照（含流式前缀与最新序号）。
        if self._active is not None and self._active.display is not None and self._active.control.started:
            return {self._active.task.id: self._active.display.snapshot()}
        return None

    async def snapshot(self, task_id=None):
        await self.runtime.evict_idle()
        self._touch()
        self._pins += 1
        try:
            tree = await self.repository.storage.read(self.repository.scope, 'tree')
            parent = task_id if task_id is not None else (tree.value['head'] if tree else None)
            return await self.repository.context(parent)
        finally:
            self._pins -= 1
            self.touched = monotonic()

    async def tree(self, include_display: bool = True):
        await self.runtime.evict_idle()
        self._touch()
        self._pins += 1
        try:
            live_displays = self._live_displays() if include_display else None
            return await self.repository.tree(include_display=include_display, live_displays=live_displays)
        finally:
            self._pins -= 1
            self.touched = monotonic()


class SessionRuntime:
    """一个宿主进程管理一组会话。按可信 owner 分区；长期服务应复用同一实例。"""
    def __init__(self, agent, *, capacity=100, ttl_seconds=1800):
        if type(capacity) is not int or capacity < 1 or not isfinite(ttl_seconds) or ttl_seconds <= 0:
            raise ValueError('驻留容量和空闲期限必须为正数')
        self.agent = agent
        self.capacity, self.ttl = capacity, ttl_seconds
        self._residents = {}
        self._lock = asyncio.Lock()
        self.closed = False
        self._maintenance = None

    def resident(self, owner, identifier):
        return self._residents.get((owner, identifier))

    async def _release(self, session, *, force: bool = False):
        if not force and session.busy:
            raise StorageConflict('会话有等待、执行或恢复操作')
        session._expired = True
        for ticket in session._tickets.values():
            await ticket.finish('expired',error=SessionExpired('本地任务句柄已释放；持久树可使用任务 ID 重新激活'))
        session._tickets.clear()
        session._task_guidance.clear()
        # 调用者可以长期持有公开句柄，驱逐时必须主动释放深复制的历史与工作区。
        session.repository.baseline = ContextInjection()
        session.repository.snapshot = session.repository.baseline
        session.repository.tip = None
        self._residents.pop((session.owner, session.id), None)
        if session._scheduler:
            await session._scheduler.close()
        # 无论持久树、内存会话还是冻结分身，释放或空闲驱逐时均同步释放对应的会话缓存作用域。
        self.agent.cache_scopes.release(session.owner, session.id)
        if session.kind != 'persistent':
            await session.repository.storage.close()

    async def _sweep(self):
        for session in list(self._residents.values()):
            if session.evictable and monotonic() - session.touched >= self.ttl:
                await self._release(session)

    async def _maintain(self):
        try:
            while self._residents:
                await asyncio.sleep(max(.05, min(60, self.ttl)))
                await self.evict_idle()
        finally:
            self._maintenance = None

    async def evict_idle(self):
        async with self._lock:
            await self._sweep()

    async def _settle_idle(self, session):
        worker = session._worker
        if worker and not session._tickets and session._active is None:
            queue = await session._queue()
            if not queue or not queue.value['items']:
                await asyncio.shield(worker)

    async def release(self, session):
        await self._settle_idle(session)
        async with self._lock:
            if self._residents.get((session.owner, session.id)) is not session:
                raise SessionExpired('会话已释放')
            await self._release(session)

    async def _open(self, owner, identifier, kind, mode, baseline, reminders=None, tools=(), *, role=None,
                    retention=None, create=False, heartbeat=None, guidance_factory=None, tool_hooks=None,
                    prompt_options: PromptOptions | None = None):
        if mode not in ('queue', 'interruptible') or not owner.strip() or not identifier.strip():
            raise ValueError('需要有效用户、会话标识和执行模式')
        if guidance_factory is not None and not callable(guidance_factory):
            raise TypeError('guidance_factory 必须可调用或为 None')
        if prompt_options is not None and not isinstance(prompt_options, PromptOptions):
            raise TypeError('prompt_options 必须为 PromptOptions 实例或 None')
        effective_prompt_options = self.agent.prompt_options if prompt_options is None else prompt_options
        normalized_hooks = normalize_tool_hooks(tool_hooks)
        if heartbeat is not None:
            heartbeat = SchedulerSettings.model_validate(heartbeat)
            if mode != 'queue' or heartbeat.heartbeat_interval_seconds < 1:
                raise ValueError('实例心跳需要队列执行模式和正数间隔')
        async with self._lock:
            if self.closed or self.agent._closed:
                raise RuntimeError('运行时已关闭')
            await self._sweep()
            existing = self._residents.get((owner, identifier))
            if existing is not None:
                if create:
                    raise StorageConflict('该树已存在')
                if (existing.kind != kind or existing.mode != mode or existing.tools != tuple(tools)
                        or existing.role != role or existing.guidance_factory != guidance_factory
                        or existing.tool_hooks != normalized_hooks
                        or existing.prompt_options != effective_prompt_options):
                    raise StorageConflict('同一会话已绑定其他形态或执行模式')
                if heartbeat is not None and heartbeat != existing.managed_heartbeat:
                    raise StorageConflict('该树已绑定其他心跳策略')
                existing._touch()
                if kind == 'persistent' and not existing.busy:
                    queue = await existing._queue()
                    if queue and queue.value['items']:
                        existing._wake()
                return existing
            if len(self._residents) >= self.capacity:
                idle = [session for session in self._residents.values() if session.evictable]
                if not idle:
                    raise StorageConflict('所有驻留会话均在使用中')
                await self._release(min(idle, key=lambda session: session.touched))
            store = self.agent.storage if kind == 'persistent' else SQLiteStorage(':memory:')
            if store is None:
                raise ValueError('持久会话需要存储适配器')
            repo = _ResidentRepository(store, owner=owner, tree_id=identifier, baseline=baseline.model_copy(deep=True),
                                       workspace_retention=(retention or self.agent.workspace_retention) if kind == 'persistent' else 'per_node')
            session = Session(self, owner, identifier, kind, mode, repo, reminders or repo, tools)
            session.role = role
            session.guidance_factory = guidance_factory
            session.tool_hooks = normalized_hooks
            session.prompt_options = effective_prompt_options
            if kind == 'persistent':
                policy_value = {'mode': mode}
                if heartbeat is not None:
                    policy_value['heartbeat'] = heartbeat.model_dump()
                if create:
                    # 树和运行约定同事务创建；并发创建者中只有一个可以成功。
                    await store.atomic(repo.scope,[Write('tree',None,repo._tree_state(None)),
                                       Write('runtime_session',None,policy_value)])
                else:
                    await repo.bind()
                # 会话执行约定跨进程与重启保持一致，禁止队列绑定方绕过不可打断策略。
                policy = await store.read(repo.scope, 'runtime_session')
                if policy is None:
                    try:
                        await store.atomic(repo.scope, [Write('runtime_session', None, policy_value)])
                    except StorageConflict:
                        policy = await store.read(repo.scope, 'runtime_session')
                if policy is not None and policy.value['mode'] != mode:
                    raise StorageConflict('此任务树已登记其他执行模式')
                if policy is not None and role is not None:
                    saved = policy.value.get('heartbeat')
                    if saved is not None:
                        registered = SchedulerSettings.model_validate(saved)
                        if heartbeat is not None and heartbeat != registered:
                            raise StorageConflict('该树已绑定其他心跳策略')
                        heartbeat = registered
                    elif heartbeat is not None:
                        await store.atomic(repo.scope, [Write('runtime_session', policy.version,
                            {**policy.value, 'heartbeat': heartbeat.model_dump()})])
                # 重启重新绑定后继续未领取的任务；已占树的任务须走显式故障恢复。
                queue = await session._queue()
                if queue and queue.value['items']:
                    session._wake()
            if heartbeat is not None:
                # 轮询器跟随实例驻留，重复恢复复用同一消费者。
                session.managed_heartbeat = heartbeat
                session.scheduler(settings=heartbeat).start()
            self._residents[(owner, identifier)] = session
            if self._maintenance is None:
                self._maintenance = asyncio.create_task(self._maintain())
            return session

    async def persistent(self, *, owner, tree_id, mode='queue', tools=(), guidance_factory=None, tool_hooks=None,
                         prompt_options: PromptOptions | None = None):
        return await self._open(owner, tree_id, 'persistent', mode, ContextInjection(), tools=tools,
                                guidance_factory=guidance_factory, tool_hooks=tool_hooks, prompt_options=prompt_options)

    async def memory(self, *, owner, context=None, mode='queue', tools=(), guidance_factory=None, tool_hooks=None,
                     prompt_options: PromptOptions | None = None):
        return await self._open(owner, new_tree_id(), 'memory', mode, context or ContextInjection(), tools=tools,
                                guidance_factory=guidance_factory, tool_hooks=tool_hooks, prompt_options=prompt_options)

    async def fork(self, parent, *, task_id=None, mode='queue', tools=(), guidance_factory=None, tool_hooks=None,
                   prompt_options: PromptOptions | None = None):
        if parent.runtime is not self or parent.kind != 'persistent':
            raise ValueError('冻结分身必须绑定本运行时的持久会话')
        snapshot = await parent.snapshot(task_id)
        return await self._open(parent.owner, new_tree_id(), 'fork', mode, snapshot, self.agent.repository(parent.owner, parent.id),
                                tools, guidance_factory=guidance_factory, tool_hooks=tool_hooks, prompt_options=prompt_options)

    async def get(self, *, owner, session_id):
        async with self._lock:
            await self._sweep()
            session = self._residents.get((owner, session_id))
            if session is None:
                raise SessionExpired('会话未驻留或已释放')
            session._touch()
            return session

    async def close(self, *, graceful: bool = False, timeout: float = 30.0):
        if type(graceful) is not bool:
            raise ValueError('graceful 必须为布尔值')
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not isfinite(timeout) or timeout <= 0:
            raise ValueError('timeout 必须为有限正数')
        if not graceful:
            await asyncio.gather(*(self._settle_idle(s) for s in tuple(self._residents.values())))
            async with self._lock:
                if any(session.busy for session in self._residents.values()):
                    raise StorageConflict('运行时仍有排队或执行中的任务')
                self.closed = True
                if self._maintenance is not None:
                    self._maintenance.cancel()
                    with suppress(asyncio.CancelledError):
                        await self._maintenance
                for session in list(self._residents.values()):
                    await self._release(session)
            return

        # 优雅停机：通知各驻留会话停止继续拉取持久队列，限时等待在途单任务完成，超时后取消并清理本地句柄。
        residents = tuple(self._residents.values())
        schedulers = []
        for session in residents:
            session._stopping = True
            if session._scheduler is not None:
                session._scheduler._stop.set()
                schedulers.append(session._scheduler)

        deadline = monotonic() + float(timeout)
        current = asyncio.current_task()
        while True:
            pending = {
                task
                for task in (
                    *(s._worker for s in tuple(self._residents.values())),
                    *(sc._worker for sc in schedulers),
                )
                if task is not None and task is not current and not task.done()
            }
            if not pending:
                break
            remaining = deadline - monotonic()
            if remaining <= 0:
                break
            await asyncio.wait(pending, timeout=remaining)

        for session in tuple(self._residents.values()):
            active = session._active
            if active is not None:
                with suppress(StorageConflict):
                    active.control.interrupt('cancelled')
        to_cancel = {
            s._worker
            for s in tuple(self._residents.values())
            if s._worker is not None and s._worker is not current and not s._worker.done()
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

        async with self._lock:
            self.closed = True
            if self._maintenance is not None:
                self._maintenance.cancel()
                with suppress(asyncio.CancelledError):
                    await self._maintenance
            for session in list(self._residents.values()):
                await self._release(session, force=True)
