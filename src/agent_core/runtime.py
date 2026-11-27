"""面向业务调用方的树实例；共用一个执行器、驻留池和数据库连接池。"""
import asyncio
from datetime import datetime
from typing import Literal

from .agent import Agent, _validate_close_options
from .core.prompt import PromptOptions
from .core.tool import BaseTool, RegisteredTool, ToolRegistry, build_builtin_registrations
from .core.tool.hooks import normalize_tool_hooks
from .core.model import ContextInjection
from .model.question_tree import new_tree_id
from .scheduler import SchedulerSettings, utc_now
from .sessions import DEFAULT_MERGE_SEPARATOR, QueueStatus, ResumeResult, SessionExpired, _UNSET
from .tasks import TaskRepository, validate_lease_seconds


def _instance_tools(tools):
    available = {tool.name for tool in build_builtin_registrations()} | {
        'finish_task','emit_progress','create_reminder','list_reminders','delete_reminder',
        'search_memory','view_memory_query',
        'add_file_note','delete_file_note','search_file_notes','view_file_note_query'}
    result = tuple(tool.build_registration() if isinstance(tool, BaseTool) else tool for tool in tools)
    names = set()
    for tool in result:
        if isinstance(tool,str) and tool in available:
            name = tool
        elif isinstance(tool,RegisteredTool):
            name = tool.name
        else:
            raise ValueError('工具须为已注册的工具对象或库支持的工具名称')
        if name in names:
            raise ValueError(f'实例的工具集合中存在重名工具 {name}')
        names.add(name)
    registrations = ToolRegistry([tool for tool in result if isinstance(tool,RegisteredTool)])
    if 'finish_task' in registrations.entries:
        registrations.require_mode_tools()
    return result


def _validate_instance_tool_hooks(tools, tool_hooks):
    """校验 tool_hooks 仅指向当前实例已装配的工具（含默认始终启用的 finish_task）。"""
    normalized = normalize_tool_hooks(tool_hooks)
    if not normalized:
        return
    assembled = {tool if isinstance(tool, str) else tool.name for tool in tools} | {'finish_task'}
    unknown = [name for name, _ in normalized if name not in assembled]
    if unknown:
        raise ValueError(f"tool_hooks 指定了未启用或不存在的工具：{', '.join(unknown)}")


class TreeAgent:
    def __init__(self, runtime, session, tree_id):
        self._runtime = runtime
        self._session = session
        self.tree_id = tree_id
        self.instance_id = session.id
        self._closed = False

    @property
    def persist_new_nodes(self):
        return self._session.kind == 'persistent'

    async def _resident(self):
        if self._closed:
            raise SessionExpired('实例已关闭')
        session = self._session
        if self.persist_new_nodes:
            # 公开句柄可长期持有，驱逐只释放驻留上下文。重新绑定时仍验证执行配置并保留提示工厂、工具钩子与提示词选项。
            session = await self._runtime._restore_session(
                session.runtime.agent, self.tree_id, session.role, session.tools, session.mode,
                guidance_factory=session.guidance_factory,
                tool_hooks=dict(session.tool_hooks) if session.tool_hooks else None,
                prompt_options=session.prompt_options,
            )
            self._session = session
        return session

    async def generate(self, question, *, priority=0, merge_with_previous=False, external_id=None, created_at=None,
                       deferred=False, source='user', attachments=(), thinking_effort='none', supplemental='', preview=True,
                       guidance_factory=_UNSET, parent_id: str | None = 'latest', advance_head: bool = True,
                       merge_separator: str = DEFAULT_MERGE_SEPARATOR,
                       merge_max_messages: int | None = None,
                       merge_max_bytes: int | None = None,
                       merge_max_attachments: int | None = None,
                       merge_interval_seconds: int | float | None = None):
        session = await self._resident()
        return await session.submit(question,priority=priority,merge_with_previous=merge_with_previous,external_id=external_id,created_at=created_at,
            deferred=deferred,source=source,attachments=attachments,thinking_effort=thinking_effort,
            supplemental=supplemental,preview=preview,guidance_factory=guidance_factory,parent_id=parent_id,
            advance_head=advance_head,merge_separator=merge_separator,merge_max_messages=merge_max_messages,
            merge_max_bytes=merge_max_bytes,merge_max_attachments=merge_max_attachments,
            merge_interval_seconds=merge_interval_seconds)

    async def activate(self, task_id):
        return await (await self._resident()).activate(task_id)

    async def stop(self, task_id):
        return await (await self._resident()).stop(task_id)

    async def redirect(self, task_id, instruction, **options):
        return await (await self._resident()).submit(instruction,replace_task_id=task_id,**options)

    async def recover(self, task_id, *, status='failed'):
        """宿主确认旧执行器停止后收束遗留任务，保留检查点，不重放工具。"""
        session = await self._resident()
        if session.active_task_id is not None:
            raise RuntimeError('实例仍有活动任务，不能恢复')
        return await session.repository.recover(task_id,status=status)

    async def recover_stale(
        self,
        *,
        now: datetime | None = None,
        status: Literal['failed', 'cancelled', 'superseded'] = 'failed',
        recover_unleased: bool = False,
        stale_owner_ids: set[str] | None = None,
        requeue_if_unstarted: bool = False,
    ):
        """按租约状态自动收束当前任务树中已过期或指定失效持有者的在途任务。"""
        session = await self._resident()
        return await session.repository.recover_expired_active(
            now=now,
            status=status,
            recover_unleased=recover_unleased,
            stale_owner_ids=stale_owner_ids,
            requeue_if_unstarted=requeue_if_unstarted,
        )

    async def resume(self, *, recover_expired: bool = True, recover_unleased: bool = False) -> ResumeResult:
        """幂等恢复过期在途任务并唤醒当前任务树的持久队列消费。"""
        session = await self._resident()
        return await session.resume(recover_expired=recover_expired, recover_unleased=recover_unleased)

    async def snapshot(self, task_id=None):
        return await (await self._resident()).snapshot(task_id)

    async def tree(self, include_display: bool = True):
        return await (await self._resident()).tree(include_display=include_display)

    async def scheduler(self, **options):
        return (await self._resident()).scheduler(**options)

    async def close(self):
        if not self._closed and not self._session._expired:
            if self._session.managed_heartbeat:
                await self._session._scheduler.close()
            await self._session.runtime.release(self._session)
        self._closed = True


class AgentRuntime:
    """模型与运行配置共享，每个数据库适配器独立管理同名的树和驻留容量。"""
    def __init__(self, *, models, database=None, capacity=100, ttl_seconds=1800,
                 legacy_owner=None, settings=None, options=None, file_note_authorize=None,
                 lease_seconds: float = 300.0):
        self.database = database
        # 新接口在数据库内使用固定命名空间；可显式绑定旧版 owner 来接续现存树。
        self._owner = 'agent-core:trees' if legacy_owner is None else legacy_owner
        if not self._owner.strip():
            raise ValueError('旧版 owner 不能为空')
        self.lease_seconds = validate_lease_seconds(lease_seconds)
        self.owner_id = new_tree_id()
        self._engine_options = dict(models=models,session_capacity=capacity,session_ttl_seconds=ttl_seconds,
                                    settings=settings,options=options,reminder_tools=True,
                                    file_note_authorize=file_note_authorize,
                                    lease_seconds=self.lease_seconds,owner_id=self.owner_id)
        self._engines = {}
        self._closed = False

    def _engine_for(self, database):
        if self._closed:
            raise RuntimeError('运行时已关闭')
        database = self.database if database is None else database
        # 使用适配器身份隔离连接和驻留状态。不同数据库中的同名树可同时执行。
        key = id(database)
        if key not in self._engines:
            self._engines[key] = Agent(storage=database,role='根据实例的业务角色执行任务。',**self._engine_options)
        return self._engines[key]

    async def _metadata(self, engine, tree_id):
        repo = engine.repository(self._owner,tree_id)
        tree = await repo.storage.read(repo.scope,'tree')
        if tree is None:
            raise KeyError('数据库中不存在指定树')
        return repo, tree.value

    async def _restore_session(self, engine, tree_id, role, tools, mode, heartbeat=None, *,
                               guidance_factory=None, tool_hooks=None, prompt_options: PromptOptions | None = None):
        repo, tree = await self._metadata(engine,tree_id)
        if mode is None:
            policy = await repo.storage.read(repo.scope,'runtime_session')
            mode = policy.value['mode'] if policy else 'queue'
        return await engine.sessions._open(self._owner,tree_id,'persistent',mode,ContextInjection(),
            tools=tools,role=role,retention=tree.get('workspace_retention','per_node'),heartbeat=heartbeat,
            guidance_factory=guidance_factory,tool_hooks=tool_hooks,prompt_options=prompt_options)

    async def create(self, *, tree_id, role, database=None, tools=(), persist_new_nodes=True, execution_mode='queue',
                     workspace_retention='per_node', heartbeat: SchedulerSettings | None = None,
                     guidance_factory=None, tool_hooks=None, prompt_options: PromptOptions | None = None):
        engine = self._engine_for(database)
        tools = _instance_tools(tools)
        _validate_instance_tool_hooks(tools, tool_hooks)
        if prompt_options is not None and not isinstance(prompt_options, PromptOptions):
            raise TypeError('prompt_options 必须为 PromptOptions 实例或 None')
        if not isinstance(role,str) or not role.strip() or type(persist_new_nodes) is not bool:
            raise ValueError('需要明确角色和布尔持久化选项')
        if workspace_retention not in ('per_node','latest'):
            raise ValueError('未知的工作区保留策略')
        if persist_new_nodes and engine.storage is None:
            raise ValueError('创建持久树需要已准备的数据库')
        owner = self._owner if persist_new_nodes else 'agent-core:memory'
        session = await engine.sessions._open(owner,tree_id,'persistent' if persist_new_nodes else 'memory',
            execution_mode,ContextInjection(),tools=tools,role=role,retention=workspace_retention,create=True,
            heartbeat=heartbeat,guidance_factory=guidance_factory,tool_hooks=tool_hooks,prompt_options=prompt_options)
        return TreeAgent(self,session,tree_id)

    async def restore(self, *, tree_id, role, database=None, tools=(), persist_new_nodes=True, execution_mode=None,
                      start_node_id=None, heartbeat: SchedulerSettings | None = None,
                      guidance_factory=None, tool_hooks=None, prompt_options: PromptOptions | None = None):
        engine = self._engine_for(database)
        tools = _instance_tools(tools)
        _validate_instance_tool_hooks(tools, tool_hooks)
        if prompt_options is not None and not isinstance(prompt_options, PromptOptions):
            raise TypeError('prompt_options 必须为 PromptOptions 实例或 None')
        if not isinstance(role,str) or not role.strip() or type(persist_new_nodes) is not bool:
            raise ValueError('需要明确角色和布尔持久化选项')
        if persist_new_nodes:
            if start_node_id is not None:
                raise ValueError('历史起点用于派生分身，持久实例恢复当前树头')
            session = await self._restore_session(engine,tree_id,role,tools,execution_mode,heartbeat,
                                                  guidance_factory=guidance_factory,tool_hooks=tool_hooks,
                                                  prompt_options=prompt_options)
        else:
            if heartbeat is not None:
                raise ValueError('分身使用本体的心跳策略')
            _, tree = await self._metadata(engine,tree_id)
            repo = TaskRepository(engine.storage,owner=self._owner,tree_id=tree_id,
                                  workspace_retention=tree.get('workspace_retention','per_node'))
            node = tree['head'] if start_node_id is None else start_node_id
            context = await repo.context(node)
            session = await engine.sessions._open(self._owner,new_tree_id(),'fork','queue' if execution_mode is None else execution_mode,
                context,reminders=repo,tools=tools,role=role,guidance_factory=guidance_factory,tool_hooks=tool_hooks,
                prompt_options=prompt_options)
        return TreeAgent(self,session,tree_id)

    async def tree(self, tree_id: str, *, database=None, include_display: bool = True):
        """只读获取数据库中整棵任务树的结构视图，无需预先装配角色或工具。"""
        if type(include_display) is not bool:
            raise ValueError('include_display 必须为布尔值')
        engine = self._engine_for(database)
        _, state = await self._metadata(engine, tree_id)
        repo = TaskRepository(engine.storage, owner=self._owner, tree_id=tree_id,
                              workspace_retention=state.get('workspace_retention', 'per_node'))
        live_displays = None
        if include_display:
            resident = engine.sessions.resident(self._owner, tree_id)
            if resident is not None:
                live_displays = resident._live_displays()
        return await repo.tree(include_display=include_display, live_displays=live_displays)

    async def queue_status(self, tree_id: str, *, database=None) -> QueueStatus:
        """无状态只读查询任务树的持久队列与活动租约状态，不创建驻留会话也不占用容量。"""
        engine = self._engine_for(database)
        repo, state = await self._metadata(engine, tree_id)
        active_task_id = state.get('active')
        lease_rec = await repo.storage.read(repo.scope, 'active_lease')
        queue_rec = await repo.storage.read(repo.scope, 'runtime_queue')
        items = queue_rec.value['items'] if queue_rec else []
        ready_count = sum(1 for entry in items if not entry.get('deferred'))
        deferred_count = len(items) - ready_count

        lease_owner_id = None
        lease_expires_at = None
        lease_expired = False
        if active_task_id is not None:
            now = utc_now()
            lease_val = lease_rec.value if lease_rec is not None else None
            if lease_val is None or lease_val.get('task_id') != active_task_id:
                lease_expired = True
            else:
                lease_owner_id = lease_val.get('owner_id')
                expires_raw = lease_val.get('expires_at')
                if expires_raw is None:
                    lease_expired = True
                else:
                    lease_expires_at = datetime.fromisoformat(expires_raw)
                    lease_expired = lease_expires_at <= now
        return QueueStatus(
            tree_id=tree_id,
            ready_count=ready_count,
            deferred_count=deferred_count,
            active_task_id=active_task_id,
            lease_owner_id=lease_owner_id,
            lease_expires_at=lease_expires_at,
            lease_expired=lease_expired,
        )

    async def close(self, *, graceful: bool = False, timeout: float = 30.0):
        timeout = _validate_close_options(graceful, timeout)
        self._closed = True
        engines = tuple(self._engines.values())
        if graceful:
            await asyncio.gather(*(engine.close(graceful=True, timeout=timeout) for engine in engines))
        else:
            for engine in engines:
                await engine.close(graceful=False, timeout=timeout)
