"""任务树、纯内存会话与冻结分身的驻留、排队和交互控制。"""
import asyncio
from collections import deque
from contextlib import suppress
from dataclasses import dataclass
from math import isfinite
from time import monotonic

from .adapters import SQLiteStorage
from .core import AgentInput
from .core.model import ContextInjection
from .model.question_tree import new_tree_id
from .scheduler import Dispatch
from .storage import StorageConflict, Write
from .tasks import TaskRepository


class SessionExpired(RuntimeError):
    """内存会话释放后不能以同一引用继续。"""


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
        self.id = task_id
        self.status = 'queued'
        self._done = asyncio.Event()
        self._changed = asyncio.Event()
        self._events = deque(maxlen=256)
        self._sequence = 0
        self._result = self._error = None

    async def emit(self, kind, data):
        from .agent import AgentEvent
        self._events.append(AgentEvent(type=kind, data={**data, 'task_id': self.id}))
        self._sequence += 1
        self._changed.set()
        self._changed = asyncio.Event()

    async def finish(self, status, *, result=None, error=None):
        self.status, self._result, self._error = status, result, error
        data = {'result': result.model_dump(mode='json')} if result is not None else {}
        await self.emit('task.' + status, data)
        self._done.set()

    async def result(self):
        await self._done.wait()
        if self._error is not None:
            raise self._error
        return self._result

    async def events(self):
        cursor = 0
        while True:
            first = self._sequence - len(self._events)
            if cursor < first:
                raise RuntimeError('事件窗口已过期，请回查任务结果')
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


class Session:
    def __init__(self, runtime, owner, identifier, kind, mode, repository, reminder_repository, tools):
        self.runtime, self.owner, self.id = runtime, owner, identifier
        self.kind, self.mode = kind, mode
        self.repository = repository
        self.tools = tuple(tools)
        self._scheduler = None
        self._reminder_repository = reminder_repository
        self._lock = asyncio.Lock()
        self._tickets = {}
        self._active = self._worker = None
        self._expired = False
        self._pins = 0
        self.touched = monotonic()

    @property
    def busy(self):
        return bool(self._pins or self._worker or self._active or self._lock.locked())

    def _touch(self):
        if self._expired or self.runtime.closed:
            raise SessionExpired('会话已释放，请重新打开本体或创建新的临时会话')
        self.touched = monotonic()

    async def _queue(self):
        return await self.repository.storage.read(self.repository.scope, 'runtime_queue')

    async def submit(self, question, *, priority=0, source='user', attachments=(), thinking_effort='none',
                     supplemental='', replace_task_id=None, preview=True):
        if type(priority) is not int or source not in ('user', 'business') or not isinstance(supplemental, str):
            raise ValueError('需要整数优先级、有效输入来源和文本上下文')
        if type(preview) is not bool:
            raise ValueError('preview 必须为布尔值')
        request = AgentInput(question=question, attachments=list(attachments), source=source, thinking_effort=thinking_effort)
        ticket = SessionTask(new_tree_id())
        item = {'id': ticket.id, 'question': question, 'priority': priority, 'source': source,
                'replace': replace_task_id, 'attachments': [a.model_dump(mode='json') for a in request.attachments],
                'thinking_effort': thinking_effort, 'supplemental': supplemental, 'preview': preview}
        await self.runtime.evict_idle()
        async with self._lock:
            self._touch()
            active = self._target(replace_task_id) if replace_task_id is not None else None
            while True:
                record = await self._queue()
                items = record.value['items'] if record else []
                try:
                    await self.repository.storage.atomic(self.repository.scope, [
                        Write('runtime_queue', record.version if record else None, {'items': [*items, item]})])
                    break
                except StorageConflict:
                    continue
            self._tickets[ticket.id] = ticket
            self._wake()
            if active:
                # 若原任务在入队期间已进入终态，消费者会拒绝替代请求，不会改停下一任务。
                if not active.control.finishing and not active.task._done.is_set():
                    active.control.interrupt('superseded')
        return ticket

    @property
    def active_task_id(self):
        return self._active.task.id if self._active and self._active.control.started else None

    async def _scheduled(self, message, identifier, dispatch, publish):
        async with self._lock:
            if self._active or self._expired:
                return
            queue = await self._queue()
            if queue and queue.value['items']:
                return
            ticket = SessionTask(identifier)
            active = self._active = _Active(ticket, _Control())
        try:
            result = await self.runtime.agent.run(message, owner=self.owner, tree_id=self.id,
                task_id=identifier, publish=publish, _dispatch=dispatch, _control=active.control,
                _repository=self.repository, _reminder_repository=self._reminder_repository, _session_tools=self.tools)
            await ticket.finish('completed', result=result)
            return result
        except asyncio.CancelledError:
            if not active.control.reason:
                raise
            task, _ = await self.repository.load(identifier)
            if task.status not in ('superseded', 'cancelled'):
                raise StorageConflict('调度任务停止终态未确认')
            await ticket.finish(task.status, error=TaskInterrupted(task.status))
        finally:
            if not ticket._done.is_set():
                await ticket.finish('failed', error=RuntimeError('调度任务未正常结束'))
            self._active = None
            self.touched = monotonic()

    def scheduler(self, *, settings=None, publish=None):
        from .scheduler import AgentScheduler, SchedulerSettings
        self._touch()
        if self.kind != 'memory':
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
        if self._worker is None:
            self._worker = asyncio.create_task(self._drain())

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
                await ticket.finish('failed', error=StorageConflict('原任务未被替代，未执行新指令'))
                self._tickets.pop(identifier)
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

    async def _drain(self):
        repo = self.repository
        try:
            while True:
                async with self._lock:
                    record = await self._queue()
                    items = record.value['items'] if record else []
                    await self._observe_other_workers({item['id'] for item in items})
                    if not items and not self._tickets:
                        self._worker = None
                        return
                    # max 保留首个同分项；定向替代在旧任务确认结束后优先接续。
                    tree = await repo.storage.read(repo.scope, 'tree')
                    waiting = self._active is not None or not items or (tree and tree.value['active'] is not None)
                    if not waiting:
                        item = max(items, key=lambda x: (x['replace'] is not None, x['priority']))
                        write = Write('runtime_queue', record.version, {'items': [x for x in items if x['id'] != item['id']]})
                        ticket = self._tickets.setdefault(item['id'], SessionTask(item['id']))
                        if item['replace']:
                            old, _ = await repo.load(item['replace'])
                            if old.status != 'superseded' or tree.value['head'] != old.id:
                                try:
                                    await repo.storage.atomic(repo.scope, [write,
                                        Write('runtime_rejected:' + ticket.id, None, {'target': old.id})])
                                except StorageConflict:
                                    continue
                                await ticket.finish('failed', error=StorageConflict('原任务未被替代，未执行新指令'))
                                self._tickets.pop(ticket.id)
                                continue
                        dispatch = Dispatch(item['source'], tree.version if tree else None, (write,))
                        self._active = _Active(ticket, _Control())
                        ticket.status = 'running'
                if waiting:
                    await asyncio.sleep(.05)
                    continue
                try:
                    result = await self.runtime.agent.run(item['question'], owner=self.owner, tree_id=self.id,
                        task_id=ticket.id, attachments=item['attachments'], thinking_effort=item['thinking_effort'],
                        supplemental=item['supplemental'], _events=ticket.emit, _preview=item.get('preview', True),
                        _repository=repo, _reminder_repository=self._reminder_repository, _session_tools=self.tools,
                        _control=self._active.control, _dispatch=dispatch)
                except StorageConflict as error:
                    # 入队/领取竞争只在请求仍留队时重选；已开始的工具绝不重放。
                    if not self._active.control.started:
                        # 已被其他执行器领取时，下轮通过任务记录观察其终态。
                        continue
                    await ticket.finish('failed', error=error)
                except asyncio.CancelledError:
                    task, _ = await repo.load(ticket.id)
                    if task.status in ('cancelled', 'superseded'):
                        await ticket.finish(task.status, error=TaskInterrupted(task.status))
                    else:
                        await ticket.finish('failed', error=StorageConflict('停止终态未确认，请回查任务'))
                except Exception as error:
                    if not self._active.control.started:
                        raise
                    await ticket.finish('failed', error=error)
                else:
                    await ticket.finish('completed', result=result)
                finally:
                    self._active = None
                    if ticket._done.is_set():
                        self._tickets.pop(ticket.id, None)
                    self.touched = monotonic()
        except Exception as error:
            # 存储不可用时唤醒所有等待方。未领取的持久请求仍可在恢复后处理。
            async with self._lock:
                for ticket in self._tickets.values():
                    await ticket.finish('failed', error=error)
                self._tickets.clear()
                self._worker = self._active = None

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

    async def _release(self, session):
        if session.busy:
            raise StorageConflict('会话有等待、执行或恢复操作')
        session._expired = True
        self._residents.pop((session.owner, session.id))
        if session._scheduler:
            await session._scheduler.close()
        if session.kind != 'persistent':
            self.agent.experts.release(session.owner, session.id)
            await session.repository.storage.close()

    async def _sweep(self):
        for session in list(self._residents.values()):
            if not session.busy and monotonic() - session.touched >= self.ttl:
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

    async def _open(self, owner, identifier, kind, mode, baseline, reminders=None, tools=()):
        if mode not in ('queue', 'interruptible') or not owner.strip() or not identifier.strip():
            raise ValueError('需要有效用户、会话标识和执行模式')
        async with self._lock:
            if self.closed or self.agent._closed:
                raise RuntimeError('运行时已关闭')
            await self._sweep()
            existing = self._residents.get((owner, identifier))
            if existing is not None:
                if existing.kind != kind or existing.mode != mode or existing.tools != tuple(tools):
                    raise StorageConflict('同一会话已绑定其他形态或执行模式')
                existing._touch()
                if kind == 'persistent' and not existing.busy:
                    queue = await existing._queue()
                    if queue and queue.value['items']:
                        existing._wake()
                return existing
            if len(self._residents) >= self.capacity:
                idle = [session for session in self._residents.values() if not session.busy]
                if not idle:
                    raise StorageConflict('所有驻留会话均在使用中')
                await self._release(min(idle, key=lambda session: session.touched))
            store = self.agent.storage if kind == 'persistent' else SQLiteStorage(':memory:')
            if store is None:
                raise ValueError('持久会话需要存储适配器')
            repo = _ResidentRepository(store, owner=owner, tree_id=identifier, baseline=baseline.model_copy(deep=True),
                                       workspace_retention=self.agent.workspace_retention if kind == 'persistent' else 'per_node')
            session = Session(self, owner, identifier, kind, mode, repo, reminders or repo, tools)
            if kind == 'persistent':
                await repo.bind()
                # 会话执行约定跨进程与重启保持一致，禁止队列绑定方绕过不可打断策略。
                policy = await store.read(repo.scope, 'runtime_session')
                if policy is None:
                    try:
                        await store.atomic(repo.scope, [Write('runtime_session', None, {'mode': mode})])
                    except StorageConflict:
                        policy = await store.read(repo.scope, 'runtime_session')
                if policy is not None and policy.value['mode'] != mode:
                    raise StorageConflict('此任务树已登记其他执行模式')
                # 重启重新绑定后继续未领取的任务；已占树的任务须走显式故障恢复。
                queue = await session._queue()
                if queue and queue.value['items']:
                    session._wake()
            self._residents[(owner, identifier)] = session
            if self._maintenance is None:
                self._maintenance = asyncio.create_task(self._maintain())
            return session

    async def persistent(self, *, owner, tree_id, mode='queue', tools=()):
        return await self._open(owner, tree_id, 'persistent', mode, ContextInjection(), tools=tools)

    async def memory(self, *, owner, context=None, mode='queue', tools=()):
        return await self._open(owner, new_tree_id(), 'memory', mode, context or ContextInjection(), tools=tools)

    async def fork(self, parent, *, task_id=None, mode='queue', tools=()):
        if parent.runtime is not self or parent.kind != 'persistent':
            raise ValueError('冻结分身必须绑定本运行时的持久会话')
        snapshot = await parent.snapshot(task_id)
        return await self._open(parent.owner, new_tree_id(), 'fork', mode, snapshot, self.agent.repository(parent.owner, parent.id), tools)

    async def get(self, *, owner, session_id):
        async with self._lock:
            await self._sweep()
            session = self._residents.get((owner, session_id))
            if session is None:
                raise SessionExpired('会话未驻留或已释放')
            session._touch()
            return session

    async def close(self):
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
