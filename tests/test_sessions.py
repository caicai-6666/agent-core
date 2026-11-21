import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from agent_core import SessionExpired, StorageConflict, TaskInterrupted
from agent_core.scheduler import Reminders
from test_library import make, response, storage


async def test_priority_fifo_business_and_cold_reopen(storage):
    agent, model, _ = make(storage, [response(identity=str(i)) for i in range(4)])
    session = await agent.sessions.persistent(owner='u', tree_id='work')
    original = model.create_chat_completion
    started, release = asyncio.Event(), asyncio.Event()
    async def block(**kwargs):
        started.set()
        await release.wait()
        return await original(**kwargs)
    model.create_chat_completion = block
    first = await session.submit('首项')
    await started.wait()
    low = await session.submit('低')
    high = await session.submit('高一', priority=2, source='business')
    second = await session.submit('高二', priority=2)
    with pytest.raises(StorageConflict):
        await session.stop(first.id)
    release.set()
    await asyncio.gather(*(t.result() for t in (first, low, high, second)))
    tree = await storage.read(session.repository.scope, 'tree')
    assert tree.value['tasks'] == [first.id, high.id, second.id, low.id]
    assert '业务系统任务' in str(model.inputs[1]['messages'][-1])
    await agent.sessions.release(session)
    reopened = await agent.sessions.persistent(owner='u',tree_id='work')
    assert len((await reopened.snapshot()).turns) == 4
    await agent.close()


async def test_directed_interrupt_and_preview(storage):
    agent, model, _ = make(storage, [response(arguments={'content':'新的答案'})])
    session = await agent.sessions.persistent(owner='alice', tree_id='live', mode='interruptible')
    started = asyncio.Event()
    original = model.create_chat_completion
    async def block(**kwargs):
        if not started.is_set():
            await kwargs['on_tool_delta']({'tool_calls':[{'id':'f','function':{
                'name':'finish_task','arguments':'{"content":"候选'}}]})
            started.set()
            await asyncio.Future()
        return await original(**kwargs)
    model.create_chat_completion = block
    old = await session.submit('原方向')
    await started.wait()
    new = await session.submit('调整方向',replace_task_id=old.id)
    with pytest.raises(TaskInterrupted, match='superseded'):
        await old.result()
    assert (await new.result()).answer == '新的答案'
    events = [e async for e in old.events()]
    assert any(e.data.get('status') == 'interrupted' for e in events)
    assert events[-1].type == 'task.superseded'
    assert (await session.repository.load(old.id))[0].status == 'superseded'
    with pytest.raises(StorageConflict):
        await session.stop(old.id)
    assert '用户调整方向' in str(model.inputs[-1]['messages'])
    await agent.close()


async def test_memory_and_frozen_fork_reminder(storage):
    args = {'content':'明天核对库存', 'schedule_type':'once',
            'schedule_config':{'at':(datetime.now(timezone.utc)+timedelta(days=1)).isoformat()}}
    agent, model, _ = make(storage, [response(),response(), response('create_reminder',args,'rem'),
        response(), response(arguments={'content':'补货','send_to_user':False})], reminder_tools=True)
    runtime = agent.sessions
    parent = await runtime.persistent(owner='u', tree_id='project')
    first = await parent.submit('冻结的事实')
    await first.result()
    fork = await runtime.fork(parent)
    later = await parent.submit('仅本体可见')
    await later.result()
    temp = await fork.submit('创建提醒')
    await temp.result()
    assert '冻结的事实' in str(model.inputs[2]['messages'])
    assert '仅本体可见' not in str(model.inputs[2]['messages'])
    assert not await storage.read(parent.repository.scope, 'task:'+temp.id)
    assert not storage._db.execute('SELECT 1 FROM records WHERE scope=?',(fork.repository.scope,)).fetchone()
    await runtime.release(fork)
    with pytest.raises(SessionExpired):
        await fork.submit('续问')
    reminders = Reminders(parent.repository)
    row, = await reminders.list_current(offset=0,limit=3)
    await reminders.materialize_due(row.next_fire_at)
    await agent.scheduler(owner='u',tree_id='project').tick()
    assert '仅本体可见' in str(model.inputs[-1]['messages'])
    assert '创建提醒' not in str(model.inputs[-1]['messages'])
    await agent.close()


async def test_stop_waits_for_tool_commit_and_keeps_receipt(storage):
    from agent_core import RegisteredTool, ToolExecutionResult
    from agent_core.core.tool.current_time import GetCurrentTimeArguments
    from agent_core.core.tool.feedback import with_feedback
    from agent_core.core.tool.hooks import FeedbackOnlyHook
    start, release = asyncio.Event(), asyncio.Event()
    writes = []
    async def execute(args):
        start.set()
        await release.wait()
        writes.append('created')
        return ToolExecutionResult(status='succeeded', tool_result={'created':True})
    tool = RegisteredTool('write_ticket', '创建一张工单', GetCurrentTimeArguments,
        lambda name, raw, ws, args: ws, executor=execute,
        hooks=(FeedbackOnlyHook(lambda args, ex, ctx: with_feedback(ex, '工单已创建')),))
    agent, _, _ = make(storage, [response('write_ticket',{},'write'),response()],tools=[tool])
    session = await agent.sessions.memory(owner='u',mode='interruptible')
    job = await session.submit('创建工单')
    await start.wait()
    stop = asyncio.create_task(session.stop(job.id))
    await asyncio.sleep(0)
    assert not stop.done()
    next_job = await session.submit('核实工单')
    release.set()
    assert await stop == 'cancelled'
    assert writes == ['created']
    task, _ = await session.repository.load(job.id)
    assert '工单已创建' in str(task.exchanges)
    assert (await next_job.result()).answer
    assert not storage._db.execute('SELECT 1 FROM records').fetchone()
    await agent.close()


async def test_expiry_capacity_and_memory_without_database(storage, monkeypatch):
    import agent_core.sessions as sessions
    now = [100.]
    monkeypatch.setattr(sessions, 'monotonic', lambda: now[0])
    agent, model, _ = make(None, [response()],session_capacity=1,session_ttl_seconds=10)
    memory = await agent.sessions.memory(owner='alice')
    with pytest.raises(SessionExpired):
        await agent.sessions.get(owner='bob',session_id=memory.id)
    started, release = asyncio.Event(), asyncio.Event()
    original = model.create_chat_completion
    async def block(**kwargs):
        started.set()
        await release.wait()
        return await original(**kwargs)
    model.create_chat_completion = block
    job = await memory.submit('内存任务')
    await started.wait()
    now[0] += 11
    with pytest.raises(StorageConflict):
        await agent.sessions.memory(owner='bob')
    release.set()
    await job.result()
    await agent.sessions._settle_idle(memory)
    now[0] += 11
    with pytest.raises(SessionExpired):
        await memory.submit('过期追问')
    await agent.close()


async def test_reopen_recovers_pending_queue_and_preserves_mode(storage):
    from agent_core.storage import Write
    from agent_core.core import AgentInput
    agent, _, _ = make(storage, [])
    session = await agent.sessions.persistent(owner='u', tree_id='recover')
    # 模拟进程在入队提交后、领取前退出；另一个任务已占树，先由宿主核实并恢复。
    old, _, _ = await session.repository.begin(AgentInput(question='旧任务'))
    item = {'id':'c'*32,'source':'business','question':'待处理事件','replace':None,'priority':1,
            'attachments':[],'supplemental':'','thinking_effort':'none'}
    await storage.atomic(session.repository.scope,[Write('runtime_queue',None,{'items':[item]})])
    await agent.close()
    restarted, client, _ = make(storage, [response()])
    with pytest.raises(StorageConflict):
        await restarted.sessions.persistent(owner='u',tree_id='recover',mode='interruptible')
    resumed = await restarted.sessions.persistent(owner='u',tree_id='recover')
    await restarted.recover(owner='u',tree_id='recover',task_id=old.id)
    await asyncio.wait_for(asyncio.shield(resumed._worker),2)
    assert len(client.inputs) == 1 and (await resumed.repository.load(item['id']))[0].status == 'complete'
    await restarted.close()


async def test_memory_ttl_releases_without_next_access(storage):
    agent, _, _ = make(storage, [],session_ttl_seconds=.03)
    temp = await agent.sessions.memory(owner='u')
    await asyncio.wait_for(asyncio.shield(agent.sessions._maintenance),1)
    assert temp._expired
    await agent.close()


async def test_redirect_does_not_follow_unknown_external_result(storage):
    from agent_core import AgentCoreError, RegisteredTool, ToolExecutionResult
    from agent_core.core.tool.current_time import GetCurrentTimeArguments
    from agent_core.core.tool.feedback import with_feedback
    from agent_core.core.tool.hooks import FeedbackOnlyHook
    started, release = asyncio.Event(), asyncio.Event()
    writes = []
    async def operation(args):
        started.set()
        await release.wait()
        writes.append(1)
        return ToolExecutionResult(status='unknown', tool_result={})
    tool = RegisteredTool('order_goods','提交订单',GetCurrentTimeArguments,lambda n,r,w,a:w,
        executor=operation,hooks=(FeedbackOnlyHook(lambda a,e,c:with_feedback(e,'订单结果未确认。')),))
    agent, model, _ = make(storage,[response('order_goods',{},'order')])
    session = await agent.sessions.persistent(owner='u',tree_id='orders',mode='interruptible',tools=[tool])
    old = await session.submit('下单')
    await started.wait()
    replacement = await session.submit('改为五件',replace_task_id=old.id)
    release.set()
    with pytest.raises(AgentCoreError):
        await old.result()
    with pytest.raises(StorageConflict):
        await replacement.result()
    assert len(model.inputs) == len(writes) == 1
    assert not await storage.read(session.repository.scope,'task:'+replacement.id)
    await agent.close()


async def test_fork_during_live_execution_uses_last_committed_head(storage):
    agent, model, _ = make(storage,[response(),response(),response()])
    parent = await agent.sessions.persistent(owner='u',tree_id='parent')
    previous = await parent.submit('已确认条款')
    await previous.result()
    original = model.create_chat_completion
    entered, release = asyncio.Event(), asyncio.Event()
    async def block(**kwargs):
        if not entered.is_set():
            entered.set()
            await release.wait()
        return await original(**kwargs)
    model.create_chat_completion = block
    active = await parent.submit('待处理变更')
    await entered.wait()
    child = await agent.sessions.fork(parent)
    query = await child.submit('复述条款')
    await query.result()
    assert '已确认条款' in str(model.inputs[-1]['messages'])
    assert '待处理变更' not in str(model.inputs[-1]['messages'])
    release.set()
    await active.result()
    assert len((await child.snapshot()).turns) == 2
    await agent.close()


async def test_memory_reminder_scheduler_is_scoped_to_memory(storage):
    args = {'content':'临时检查','schedule_type':'once',
            'schedule_config':{'at':(datetime.now(timezone.utc)+timedelta(days=1)).isoformat()}}
    agent, _, _ = make(storage,[response('create_reminder',args,'r'),response(),
        response(arguments={'content':'已检查','send_to_user':False})],reminder_tools=True)
    session = await agent.sessions.memory(owner='u')
    ticket = await session.submit('为本会话设置定时任务')
    await ticket.result()
    reminders = Reminders(session.repository)
    reminder, = await reminders.list_current(offset=0,limit=5)
    await reminders.materialize_due(reminder.next_fire_at)
    assert (await session.scheduler().tick()).answer == '已检查'
    assert not storage._db.execute('SELECT 1 FROM records').fetchone()
    await agent.close()


@pytest.mark.parametrize('policy', ['per_node', 'latest'])
async def test_workspace_retention_cold_history_and_fork(storage, policy):
    import json
    def edit(text):
        return response('workspace_add', {'path':'/task_constraints/supplements','value':text}, text)
    agent, _, _ = make(storage, [edit('旧条款'), response(), edit('新条款'), response()],
                       workspace_retention=policy)
    parent = await agent.sessions.persistent(owner='u',tree_id='project')
    first = await parent.submit('登记旧条款')
    first_result = await first.result()
    child = await agent.sessions.fork(parent)
    second = await parent.submit('增加新条款')
    second_result = await second.result()
    assert (await child.snapshot()).workspace == first_result.workspace
    assert '新条款' in str(second_result.workspace)
    assert (await parent.snapshot()).workspace == second_result.workspace
    scope = parent.repository.scope
    records = {key:json.loads(value) for key,value in storage._db.execute(
        'SELECT key,value FROM records WHERE scope=?',(scope,))}
    versions = [k for k in records if k.startswith('workspace:')]
    checkpoints = [v for k,v in records.items() if k.startswith('checkpoint:')]
    assert checkpoints
    if policy == 'latest':
        assert versions == ['workspace:latest']
        assert records[versions[0]] == {'task_id':second.id,'workspace':second_result.workspace.model_dump(mode='json')}
        for task_id in (first.id, second.id):
            row = records['task:'+task_id]
            assert row['workspace'] is None and 'workspace' not in row['result']
        assert all('workspace_id' not in c for c in checkpoints)
        with pytest.raises(StorageConflict,match='释放'):
            await agent.sessions.fork(parent, task_id=first.id)
    else:
        assert len(versions) >= 2
        assert all('workspace:'+c['workspace_id'] in records for c in checkpoints)
    await agent.close()

    cold, model, _ = make(storage, [],workspace_retention=policy)
    reopened = await cold.sessions.persistent(owner='u',tree_id='project')
    assert (await reopened.snapshot()).workspace == second_result.workspace
    if policy == 'per_node':
        assert (await reopened.snapshot(first.id)).workspace == first_result.workspace
    else:
        with pytest.raises(StorageConflict,match='释放'):
            await reopened.snapshot(first.id)
        with pytest.raises(StorageConflict,match='释放'):
            await cold.run('登记旧条款',owner='u',tree_id='project',task_id=first.id)
    assert (await cold.run('增加新条款',owner='u',tree_id='project',task_id=second.id)) == second_result
    assert not model.inputs
    await cold.close()


async def test_latest_workspace_pruning_recovery_and_policy_binding(storage):
    from agent_core import SchedulerSettings
    from agent_core.core import AgentInput
    from agent_core.tasks import PersistentJournal
    from agent_core.storage import Write

    silent = {'content':'无待办','send_to_user':False,'prune_current_task':True}
    agent, _, _ = make(storage,[response(),response(arguments=silent)], workspace_retention='latest')
    await agent.run('建立项目', owner='u',tree_id='work')
    repo = agent.repository('u','work')
    before = await storage.read(repo.scope,'workspace:latest')
    await agent.scheduler(owner='u',tree_id='work',settings=SchedulerSettings(heartbeat_interval_seconds=1)).tick()
    assert await storage.read(repo.scope,'workspace:latest') == before

    # 模拟终态前进程退出；恢复使用最后一个已提交检查点，树头在恢复事务之前仍指向原任务。
    task, version, _ = await repo.begin(AgentInput(question='更新条款'))
    journal = PersistentJournal(repo,task,version)
    ws = journal.workspace.model_copy(deep=True)
    ws.task_constraints.supplements['supplement_test'] = '已提交条款'
    await journal.commit(workspace=ws, exchanges=[])
    assert await storage.read(repo.scope,'workspace:latest') == before
    await agent.recover(owner='u',tree_id='work',task_id=task.id)
    assert (await repo.context(task.id)).workspace == ws
    assert (await storage.read(repo.scope,'workspace:latest')).value['task_id'] == task.id

    legacy, _, _ = make(storage,[response()])
    with pytest.raises(StorageConflict,match='保留策略'):
        await legacy.sessions.persistent(owner='u',tree_id='work')
    # 无策略字段的历史树按原有逐节点语义读取。
    old_repo = legacy.repository('u','old')
    await storage.atomic(old_repo.scope,[Write('tree',None,{'head':None,'active':None,'tasks':[]})])
    await legacy.run('旧树',owner='u',tree_id='old')
    with pytest.raises(StorageConflict,match='保留策略'):
        await agent.run('误配',owner='u',tree_id='old')
    await agent.close()
    await legacy.close()


async def test_latest_workspace_hot_cache_after_another_writer_advances(storage):
    first, _, _ = make(storage,[response()],workspace_retention='latest')
    other, _, _ = make(storage,[response()],workspace_retention='latest')
    parent = await first.sessions.persistent(owner='u',tree_id='shared')
    job = await parent.submit('初始版本')
    await job.result()
    frozen = await first.sessions.fork(parent)
    # 独立执行者推进树头后，热缓存也必须遵守同一历史版本可见性。
    await other.run('后续版本',owner='u',tree_id='shared')
    with pytest.raises(StorageConflict,match='释放'):
        await parent.snapshot(job.id)
    assert len((await parent.snapshot()).turns) == 2
    assert len((await frozen.snapshot()).turns) == 1
    await first.close()
    await other.close()
