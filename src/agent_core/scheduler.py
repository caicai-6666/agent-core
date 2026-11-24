"""个人助理的到期提醒与空闲心跳，按可信任务树隔离。"""
import asyncio
from dataclasses import dataclass
from datetime import datetime
import logging

from pydantic import BaseModel, ConfigDict, Field, field_validator
from .model.question_tree import new_tree_id, utc_now
from .reminder_schedule import AssistantReminder, next_occurrence
from .storage import StorageConflict, Write

logger = logging.getLogger(__name__)
HEARTBEAT_MESSAGE = '请结合已有记忆、工作区和待办，检查现在是否有值得推进、核查或整理的事项。'


class SchedulerSettings(BaseModel):
    model_config = ConfigDict(frozen=True, extra='forbid')
    heartbeat_interval_seconds: int = Field(default=-1, strict=True)
    heartbeat_message: str = Field(default=HEARTBEAT_MESSAGE, min_length=1, pattern=r'\S')
    poll_seconds: float = Field(default=30, gt=0, allow_inf_nan=False)

    @field_validator('heartbeat_interval_seconds')
    @classmethod
    def valid_interval(cls, value):
        if value != -1 and value <= 0:
            raise ValueError('心跳间隔须为正整数秒，或用 -1 关闭')
        return value


class Reminders:
    def __init__(self, repository):
        self.repository = repository

    async def read(self):
        return await self.repository.storage.read(self.repository.scope, 'reminders')

    async def find(self, identifier):
        record = await self.read()
        value = record.value.get(identifier) if record else None
        return AssistantReminder.model_validate(value) if value else None

    async def list_current(self, *, offset, limit):
        record = await self.read()
        rows = [AssistantReminder.model_validate(v) for v in record.value.values()
                if v['status'] in ('enabled', 'paused')] if record else []
        return sorted(rows, key=lambda r: (r.created_at, r.id))[offset:offset + limit]

    async def materialize_due(self, now):
        record = await self.read()
        if not record:
            return
        rows = dict(record.value)
        due = sorted((AssistantReminder.model_validate(v) for v in rows.values()
                      if v['status'] == 'enabled' and datetime.fromisoformat(v['next_fire_at']) <= now),
                     key=lambda row: (row.next_fire_at, row.id))
        if not due:
            return
        repo = self.repository
        queue = await repo.storage.read(repo.scope, 'reminder_queue')
        pending = list(queue.value['items']) if queue else []
        for row in due:
            future = next_occurrence(row.schedule_config, row.timezone, now, end_at=row.end_at)
            pending.append({'id': new_tree_id(), 'reminder_id': row.id, 'message': row.content,
                            'scheduled_for': row.next_fire_at.isoformat()})
            updated = row.model_copy(update={'next_fire_at': future, 'updated_at': now,
                                             'status': 'enabled' if future else 'completed'})
            rows[row.id] = updated.model_dump(mode='json')
        # 先冻结本次到期事项再推进原排期；删除未来排期不能撤回已入队事项。
        await repo.storage.atomic(repo.scope, [Write('reminders', record.version, rows),
            Write('reminder_queue', queue.version if queue else None, {'items': pending})])

    async def change(self, action, result, args, task_id):
        record = await self.read()
        rows = dict(record.value) if record else {}
        identifier = result['reminder_id']
        if action == 'create_reminder':
            row = AssistantReminder(**args, id=identifier, created_by_task_id=task_id,
                                    next_fire_at=result['next_fire_at'])
        else:
            row = AssistantReminder.model_validate(rows[identifier]).model_copy(update={
                'status': 'cancelled', 'next_fire_at': None, 'updated_at': utc_now()})
        rows[identifier] = row.model_dump(mode='json')
        return Write('reminders', record.version if record else None, rows)


@dataclass(frozen=True)
class Dispatch:
    kind: str
    tree_version: int | None
    writes: tuple = ()


class AgentScheduler:
    """一个实例绑定一个已授权的用户与树；tick 可由宿主事件循环直接驱动。"""
    def __init__(self, agent, owner, tree_id, settings, publish=None, session=None):
        self.agent, self.owner, self.tree_id = agent, owner, tree_id
        self.session = session
        self.repository = session.repository if session else agent.repository(owner, tree_id)
        self.settings, self.publish = settings, publish
        self._stop = asyncio.Event()
        self._worker = None

    async def tick(self):
        if self._stop.is_set() or self.agent._closed:
            return
        session = self.session or self.agent.sessions.resident(self.owner, self.tree_id)
        if session is None:
            return await self._tick(None)
        if session._expired:
            return
        session._pins += 1
        try:
            return await self._tick(session)
        finally:
            session._pins -= 1

    async def _tick(self, session):
        repo = self.repository
        now = utc_now()
        if self.agent.reminder_tools:
            await Reminders(repo).materialize_due(now)
        tree = await repo.storage.read(repo.scope, 'tree')
        if tree and tree.value['active']:
            return
        queued = await repo.storage.read(repo.scope, 'runtime_queue')
        if queued and any(not item.get('deferred') for item in queued.value['items']):
            return
        version = tree.version if tree else None
        queue = await repo.storage.read(repo.scope, 'reminder_queue')
        pending = queue.value['items'] if queue else []
        task_id = new_tree_id()
        if pending:
            if not self.agent.reminder_tools:
                return
            first = pending[0]
            write = Write('reminder_queue', queue.version, {'items': pending[1:]})
            dispatch = Dispatch('reminder', version, (write,))
            task_id, message = first['id'], first['message']
        else:
            interval = self.settings.heartbeat_interval_seconds
            if interval < 0:
                return
            heartbeat = await repo.storage.read(repo.scope, 'heartbeat')
            state = heartbeat.value if heartbeat else {}
            last = state.get('last_heartbeat_at')
            if last and (now - datetime.fromisoformat(last)).total_seconds() < interval:
                return
            if state.get('heartbeat_pending'):
                return
            write = Write('heartbeat', heartbeat.version if heartbeat else None,
                          {**state, 'heartbeat_pending': task_id})
            # 到期提醒在扫描后入队时放弃本次心跳，由下一次领取重新判断。
            reminder_guard = Write('reminder_queue', queue.version if queue else None, {'items': []})
            dispatch = Dispatch('heartbeat', version, (write, reminder_guard))
            message = self.settings.heartbeat_message
        if self._stop.is_set():
            return
        # 扫描后若业务输入先入队，领取事务须重新检查，不能越过已经排队的业务任务。
        guard = Write('runtime_queue', queued.version if queued else None, queued.value if queued else {'items': []})
        dispatch = Dispatch(dispatch.kind, version, (*dispatch.writes, guard))
        if session:
            return await session._scheduled(message, task_id, dispatch, self.publish)
        return await self.agent.run(message, owner=self.owner, tree_id=self.tree_id,
                                    task_id=task_id, publish=self.publish, _dispatch=dispatch)

    async def _background_tick(self):
        try:
            await self.tick()
        except StorageConflict:
            # 另一实例先领取时从持久化状态重新选择。
            pass
        except Exception as error:
            logger.warning('调度检查未完成：%s', type(error).__name__)

    async def _loop(self):
        worker = None
        try:
            while not self._stop.is_set():
                if worker is None or worker.done():
                    if worker is not None:
                        await worker
                    worker = asyncio.create_task(self._background_tick())
                elif self.agent.reminder_tools:
                    # 消费者执行慢任务时，生产者仍按原排期冻结提醒，不抢占执行中的任务。
                    try:
                        await Reminders(self.repository).materialize_due(utc_now())
                    except StorageConflict:
                        pass
                    except Exception as error:
                        logger.warning('到期提醒入队失败：%s', type(error).__name__)
                try:
                    await asyncio.wait_for(self._stop.wait(), self.settings.poll_seconds)
                except TimeoutError:
                    pass
        finally:
            if worker is not None:
                await worker

    @property
    def running(self):
        return self._worker is not None and not self._worker.done() and not self._stop.is_set()

    def start(self):
        if self._stop.is_set() or self.agent._closed:
            raise RuntimeError('调度器已关闭')
        if self._worker is None:
            self._worker = asyncio.create_task(self._loop())

    async def close(self):
        self._stop.set()
        if self._worker is not None:
            await self._worker
        key = (self.owner, self.tree_id)
        if self.agent._schedulers.get(key) is self:
            del self.agent._schedulers[key]
