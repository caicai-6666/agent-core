"""面向业务调用方的树实例；共用一个执行器、驻留池和数据库连接池。"""
from .agent import Agent
from .core.tool import BaseTool, RegisteredTool, ToolRegistry, build_builtin_registrations
from .core.model import ContextInjection
from .model.question_tree import new_tree_id
from .scheduler import SchedulerSettings
from .sessions import SessionExpired, _UNSET
from .tasks import TaskRepository


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
            # 公开句柄可长期持有，驱逐只释放驻留上下文。重新绑定时仍验证执行配置并保留提示工厂。
            session = await self._runtime._restore_session(
                session.runtime.agent, self.tree_id, session.role, session.tools, session.mode,
                guidance_factory=session.guidance_factory,
            )
            self._session = session
        return session

    async def generate(self, question, *, priority=0, merge_with_previous=False, external_id=None, created_at=None,
                       deferred=False, source='user', attachments=(), thinking_effort='none', supplemental='', preview=True,
                       guidance_factory=_UNSET):
        session = await self._resident()
        return await session.submit(question,priority=priority,merge_with_previous=merge_with_previous,external_id=external_id,created_at=created_at,
            deferred=deferred,source=source,attachments=attachments,thinking_effort=thinking_effort,
            supplemental=supplemental,preview=preview,guidance_factory=guidance_factory)

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

    async def snapshot(self, task_id=None):
        return await (await self._resident()).snapshot(task_id)

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
                 legacy_owner=None, settings=None, options=None, file_note_authorize=None):
        self.database = database
        # 新接口在数据库内使用固定命名空间；可显式绑定旧版 owner 来接续现存树。
        self._owner = 'agent-core:trees' if legacy_owner is None else legacy_owner
        if not self._owner.strip():
            raise ValueError('旧版 owner 不能为空')
        self._engine_options = dict(models=models,session_capacity=capacity,session_ttl_seconds=ttl_seconds,
                                    settings=settings,options=options,reminder_tools=True,
                                    file_note_authorize=file_note_authorize)
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

    async def _restore_session(self, engine, tree_id, role, tools, mode, heartbeat=None, *, guidance_factory=None):
        repo, tree = await self._metadata(engine,tree_id)
        if mode is None:
            policy = await repo.storage.read(repo.scope,'runtime_session')
            mode = policy.value['mode'] if policy else 'queue'
        return await engine.sessions._open(self._owner,tree_id,'persistent',mode,ContextInjection(),
            tools=tools,role=role,retention=tree.get('workspace_retention','per_node'),heartbeat=heartbeat,
            guidance_factory=guidance_factory)

    async def create(self, *, tree_id, role, database=None, tools=(), persist_new_nodes=True, execution_mode='queue',
                     workspace_retention='per_node', heartbeat: SchedulerSettings | None = None, guidance_factory=None):
        engine = self._engine_for(database)
        tools = _instance_tools(tools)
        if not isinstance(role,str) or not role.strip() or type(persist_new_nodes) is not bool:
            raise ValueError('需要明确角色和布尔持久化选项')
        if workspace_retention not in ('per_node','latest'):
            raise ValueError('未知的工作区保留策略')
        if persist_new_nodes and engine.storage is None:
            raise ValueError('创建持久树需要已准备的数据库')
        owner = self._owner if persist_new_nodes else 'agent-core:memory'
        session = await engine.sessions._open(owner,tree_id,'persistent' if persist_new_nodes else 'memory',
            execution_mode,ContextInjection(),tools=tools,role=role,retention=workspace_retention,create=True,
            heartbeat=heartbeat,guidance_factory=guidance_factory)
        return TreeAgent(self,session,tree_id)

    async def restore(self, *, tree_id, role, database=None, tools=(), persist_new_nodes=True, execution_mode=None,
                      start_node_id=None, heartbeat: SchedulerSettings | None = None, guidance_factory=None):
        engine = self._engine_for(database)
        tools = _instance_tools(tools)
        if not isinstance(role,str) or not role.strip() or type(persist_new_nodes) is not bool:
            raise ValueError('需要明确角色和布尔持久化选项')
        if persist_new_nodes:
            if start_node_id is not None:
                raise ValueError('历史起点用于派生分身，持久实例恢复当前树头')
            session = await self._restore_session(engine,tree_id,role,tools,execution_mode,heartbeat,
                                                  guidance_factory=guidance_factory)
        else:
            if heartbeat is not None:
                raise ValueError('分身使用本体的心跳策略')
            _, tree = await self._metadata(engine,tree_id)
            repo = TaskRepository(engine.storage,owner=self._owner,tree_id=tree_id,
                                  workspace_retention=tree.get('workspace_retention','per_node'))
            node = tree['head'] if start_node_id is None else start_node_id
            context = await repo.context(node)
            session = await engine.sessions._open(self._owner,new_tree_id(),'fork','queue' if execution_mode is None else execution_mode,
                context,reminders=repo,tools=tools,role=role,guidance_factory=guidance_factory)
        return TreeAgent(self,session,tree_id)

    async def close(self):
        self._closed = True
        for engine in tuple(self._engines.values()):
            await engine.close()
