"""安装包级契约：真实 SQLite、替身模型、故障与重启；不连接业务数据。"""
import asyncio
from contextlib import aclosing
from dataclasses import replace
import json
from pathlib import Path
from unittest.mock import AsyncMock
import pytest
from pydantic import BaseModel, Field
from agent_core import Agent, Models, RuntimeSettings, SQLiteStorage, StorageConflict, FinishTask
from agent_core.core import AgentInput, AgentCoreError
from agent_core.core.tool import RegisteredTool, ToolExecution, ToolExecutionResult
from agent_core.core.tool.hooks import FeedbackOnlyHook
from agent_core.core.tool.feedback import with_feedback
from agent_core.core.workspace import empty_workspace
from agent_core.providers import ExpertReply
from agent_core.infrastructure.mllm import _completion
from agent_core.storage import Write, MemoryDocument
from agent_core.tasks import PersistentJournal
from agent_core.memory import Memory, public_sources
from agent_core.core.subgraph.memory_retrieval.schema import RetrievalPlan, FieldQuery


def response(name='finish_task', arguments=None, identity='call'):
    return _completion({'id':'test', 'model':'test', 'choices':[{'finish_reason':'tool_calls',
        'message':{'role':'assistant', 'content':'PRIVATE-NOTE', 'tool_calls':[{'id':identity,'type':'function',
        'function':{'name':name,'arguments':json.dumps(arguments if arguments is not None else {'content':'测试完成'}, ensure_ascii=False)}}]}}], 'usage':{}})


class Embedding:
    space_id, dimensions = 'test-space-v1', 3
    async def embed(self, texts, *, purpose):
        return [[float('苹果' in text)+0.1, float('合同' in text)+0.1, 1.0] for text in texts]


class Expert:
    def __init__(self): self.inputs = []
    async def create_response(self, *, input_items):
        self.inputs.append(input_items)
        output = {'type':'message','role':'assistant','content':[{'type':'output_text','text':'专家已分析'}]}
        return ExpertReply('expert', 'test', '专家已分析', 'completed', 1, 1, 2,
            {'output':[output],'status':'completed'}, (output,))


class Scripted:
    def __init__(self, responses): self.responses, self.inputs = list(responses), []
    async def create_chat_completion(self, **kwargs):
        self.inputs.append(kwargs)
        result = self.responses.pop(0)
        if isinstance(result, BaseException): raise result
        if kwargs.get('on_tool_delta'):
            await kwargs['on_tool_delta'](result.assistant_message)
        return result


def make(storage, responses, **kwargs):
    client, expert = Scripted(responses), Expert()
    agent = Agent(models=Models(client, Embedding(), expert, AsyncMock(return_value=10)),
        storage=storage, role='你是合成测试助理，使用工具完成任务。', **kwargs)
    return agent, client, expert


@pytest.fixture
async def storage(tmp_path):
    store = SQLiteStorage(tmp_path/'tasks.sqlite')
    yield store
    await store.close()


async def test_atomic_adapter_contract(storage):
    await storage.atomic('a', [Write('one', None, {'v':1}), Write('two', None, {'v':2})])
    with pytest.raises(StorageConflict):
        await storage.atomic('a', [Write('one', 1, {'v':3}), Write('two', 0, {'v':4})])
    assert (await storage.read('a','one')).value == {'v':1}
    assert await storage.read('b','one') is None
    async def update():
        try: await storage.atomic('a', [Write('one',1,{'v':5})]); return True
        except StorageConflict: return False
    assert sum(await asyncio.gather(update(),update())) == 1


async def test_run_restart_idempotency_and_branches(storage):
    agent, client, _ = make(storage, [response(arguments={'content':'苹果答案'}), response(arguments={'content':'合同答案'}), response()])
    first, second, branch = '1'*32, '2'*32, '3'*32
    await agent.run('苹果问题', owner='alice', tree_id='main', task_id=first)
    await agent.run('合同问题', owner='alice', tree_id='main', task_id=second)
    assert '苹果问题' in str(client.inputs[1]['messages'])
    await agent.run('分支问题', owner='alice', tree_id='main', task_id=branch, parent_id=first)
    assert '合同问题' not in str(client.inputs[2]['messages'])
    await agent.close()
    restarted, new_client, _ = make(storage, [])
    cached = await restarted.run('苹果问题', owner='alice', tree_id='main', task_id=first)
    assert cached.answer == '苹果答案' and not new_client.inputs
    with pytest.raises(StorageConflict):
        await restarted.run('不同问题', owner='alice', tree_id='main', task_id=first)
    with pytest.raises(KeyError):
        await restarted.repository('bob','main').load(first)
    context = await restarted.repository('alice','main').context(branch)
    assert [turn.question for turn in context.turns] == ['苹果问题','分支问题']
    assert 'PRIVATE-NOTE' not in str(context)
    await restarted.close()


async def test_custom_tool_hooks_and_model_feedback(storage):
    class Args(BaseModel):
        value: int = Field(description='待加一的整数')
    seen = []
    def execute(args):
        return ToolExecution(ToolExecutionResult(status='succeeded',tool_result={'value':args.value+1}),
            after_commit=lambda:seen.append('committed'))
    def render(args, execution, context):
        return with_feedback(execution, f"### 计算完成\n\n结果：{execution.result.tool_result['value']}")
    tool = RegisteredTool('increment','将一个整数加一',Args,lambda n,r,w,a:w,executor=execute,
        hooks=(FeedbackOnlyHook(render),),persist_result=True)
    agent, client, _ = make(storage, [response('increment',{'value':4},'calc'),response(identity='end')], tools=[tool])
    await agent.run('加一',owner='alice',tree_id='tools',task_id='4'*32)
    assert seen == ['committed']
    messages = client.inputs[-1]['messages']
    assert any(m['role']=='tool' and m['content']=='### 计算完成\n\n结果：5' for m in messages)
    task,_ = await agent.repository('alice','tools').load('4'*32)
    assert task.tool_results['calc']['result']=={'value':5}


async def test_durable_failure_cleanup_and_private_audit(storage):
    bad = response('workspace_add', {'path':'/known_information','value':'INVALID-FACT'}, 'bad')
    good = response('get_current_time', {}, 'good')
    agent, client, _ = make(storage, [bad,good,response(identity='end')])
    await agent.run('纠正',owner='alice',tree_id='correction',task_id='5'*32)
    assert '<system-guidence>' in str(client.inputs[1]['messages'])
    assert 'INVALID-FACT' not in str(client.inputs[2]['messages'])
    task,_ = await agent.repository('alice','correction').load('5'*32)
    assert 'INVALID-FACT' not in str(task.exchanges)
    generation = await storage.read(agent.repository('alice','correction').scope,'generation:'+task.generation_ids[0])
    assert 'INVALID-FACT' in str(generation.value)


async def test_stream_committed_output_and_cancellation(storage):
    agent, _, _ = make(storage, [response(arguments={'content':'流式🎨答案'})])
    events = [event async for event in agent.stream('流式',owner='a',tree_id='stream')]
    assert any(event.type=='message.delta' for event in events)
    assert events[-1].type=='task.completed'
    assert events[-1].data['result']['answer']=='流式🎨答案'
    assert '\ndata: ' in events[-1].sse()
    waiting = asyncio.Event()
    async def wait(**kwargs):
        waiting.set(); await asyncio.Event().wait()
    agent.models.daily.create_chat_completion = wait
    async with aclosing(agent.stream('取消',owner='a',tree_id='cancel',task_id='6'*32)) as stream:
        started = await anext(stream)
        assert started.type=='task.started'
        await waiting.wait()
    task,_ = await agent.repository('a','cancel').load('6'*32)
    assert task.status=='cancelled' and not task.outputs
    assert not agent._running


async def test_memory_hybrid_isolation_and_cache(storage):
    agent,_,_ = make(storage,[response(),response(),response()])
    await agent.run('苹果订单',owner='a',tree_id='memory',task_id='7'*32)
    await agent.run('合同价格',owner='a',tree_id='memory',task_id='8'*32)
    await agent.run('苹果另一个分支',owner='a',tree_id='memory',task_id='9'*32,parent_id=None)
    memory=Memory(agent.repository('a','memory'),agent.models.embedding)
    plan=RetrievalPlan(queries=(FieldQuery(field='user_input',query='苹果'),))
    hits=await memory.search(plan,parent_id='8'*32)
    assert hits[0].node_id=='7'*32
    assert '9'*32 not in [hit.node_id for hit in hits]
    assert await Memory(agent.repository('b','memory'),agent.models.embedding).search(plan,parent_id=None)==()
    class Changed(Embedding): space_id='changed'
    with pytest.raises(ValueError):
        await Memory(agent.repository('a','memory'),Changed()).search(plan,parent_id='8'*32)


async def test_search_and_view_actual_model_roundtrip(storage):
    agent,client,_=make(storage,[response()])
    await agent.run('苹果记忆',owner='a',tree_id='query',task_id='a'*32)
    original=client.create_chat_completion
    calls=iter([response('search_memory',{'query':'找苹果'},'find'),
        response('set_user_input_query',{'query':'苹果'},'set'), response('execute_query',{},'query')])
    stage=0
    async def generate(**kwargs):
        nonlocal stage
        client.inputs.append(kwargs)
        stage+=1
        if stage<=3: return next(calls)
        if stage==4:
            import re
            text='\n'.join(m.get('content','') or '' for m in kwargs['messages'] if m['role']=='tool')
            query_id=re.search(r'查询 ID：([0-9a-f-]+)',text).group(1)
            return response('view_memory_query',{'query_id':query_id},'view')
        assert '任务 ID：'+'a'*32 in str(kwargs['messages'])
        return response(identity='end')
    client.create_chat_completion=generate
    await agent.run('找之前任务',owner='a',tree_id='query')
    assert stage==5


async def test_expert_uses_independent_provider_and_continues(storage):
    agent,client,expert=make(storage,[])
    async def generate(**kwargs):
        if not expert.inputs:
            return response('ask_external_expert',{'question':'解释术语'},'ask')
        if len(expert.inputs)==1:
            pool=agent.experts.get('a','expert')
            sid=next(iter(pool._sessions))
            return response('continue_external_expert',{'question':'补充条件','session_id':sid},'continue')
        return response(identity='end')
    client.create_chat_completion=generate
    await agent.run('专家问题',owner='a',tree_id='expert')
    assert len(expert.inputs)==2
    assert '专家已分析' in str(expert.inputs[1])
    assert 'PRIVATE-NOTE' not in str(expert.inputs)


async def test_recovery_preserves_checkpoint_and_does_not_replay(storage):
    agent,_,_=make(storage,[])
    repo=agent.repository('a','recover')
    task,version,_=await repo.begin(AgentInput(question='中断任务'),task_id='b'*32)
    journal=PersistentJournal(repo,task,version)
    await journal.commit(workspace=empty_workspace(),exchanges=[],tool_results={'op':{'name':'effect','result':{'done':True}}})
    with pytest.raises(StorageConflict):
        await agent.run('中断任务',owner='a',tree_id='recover',task_id='b'*32)
    recovered=await agent.recover(owner='a',tree_id='recover',task_id='b'*32)
    assert recovered.status=='failed' and recovered.tool_results['op']['result']['done']
    context=await repo.context(recovered.id)
    assert context.turns[-1].status=='failed'


async def test_compaction_checkpoints_survive_reopen(tmp_path):
    path=tmp_path/'reopen.sqlite'
    storage=SQLiteStorage(path)
    agent,_,_=make(storage,[response(),response()])
    await agent.run('第一任务',owner='a',tree_id='summary',task_id='c'*32)
    await agent.run('第二任务',owner='a',tree_id='summary',task_id='d'*32)
    repo=agent.repository('a','summary')
    task,version,_=await repo.begin(AgentInput(question='第三任务'),task_id='e'*32)
    journal=PersistentJournal(repo,task,version)
    await journal.commit(workspace=empty_workspace(),exchanges=[{'kind':'context_summary','text':'第一任务的持久摘要','covered_turns':1}],output='第三答复',final=True)
    await agent.close(); await storage.close()
    reopened=SQLiteStorage(path)
    try:
        agent,_,_=make(reopened,[])
        context=await agent.repository('a','summary').context('e'*32)
        assert context.summary=='第一任务的持久摘要'
        assert [t.question for t in context.turns]==['第二任务','第三任务']
    finally: await reopened.close()


async def test_no_publication_when_commit_fails(storage):
    class Broken:
        def __getattr__(self,name): return getattr(storage,name)
        async def atomic(self,scope,writes):
            if any(w.key.startswith('task:') and w.value.get('status')=='complete' for w in writes):
                raise OSError('simulated disk error')
            return await storage.atomic(scope,writes)
    agent,_,_=make(Broken(),[response()])
    published=AsyncMock()
    with pytest.raises(AgentCoreError):
        await agent.run('失败',owner='a',tree_id='failure',publish=published)
    published.assert_not_awaited()


async def test_cross_connection_concurrency_and_render_hook_protection(tmp_path):
    path=tmp_path/'shared.sqlite'
    first,second=SQLiteStorage(path),SQLiteStorage(path)
    try:
        await first.atomic('s',[Write('v',None,{})])
        async def update(store):
            try: await store.atomic('s',[Write('v',1,{'updated':True})]);return True
            except StorageConflict:return False
        assert sum(await asyncio.gather(update(first),update(second)))==1
    finally:
        await first.close();await second.close()
    def invalid(args,execution,context):
        return replace(execution,output='changed')
    with pytest.raises(ValueError):
        await FeedbackOnlyHook(invalid)(None,ToolExecution(ToolExecutionResult(status='succeeded')),None)


async def test_public_adapter_contract(tmp_path):
    from agent_core.testing import verify_storage
    checks=await verify_storage(lambda: SQLiteStorage(tmp_path/'contract.sqlite'))
    assert len(checks)==6


async def test_isolated_hooks_cannot_modify_other_tools(storage):
    from agent_core.core.tool import ToolRegistry, OutputArguments
    seen=[]
    def hook_a(args,execution,context):
        seen.append('a');return execution
    def hook_b(args,execution,context):
        seen.append('b');return execution
    hooks=[hook_a]
    a=RegisteredTool('a','工具甲',OutputArguments,lambda n,r,w,a:w,hooks=hooks)
    hooks.append(hook_b)
    b=RegisteredTool('b','工具乙',OutputArguments,lambda n,r,w,a:w,hooks=(hook_b,))
    registry=ToolRegistry([a,b])
    await registry.execute('a',OutputArguments(content='甲'))
    assert seen==['a']
    await registry.execute('b',OutputArguments(content='乙'))
    assert seen==['a','b']


async def test_intermediate_messages_keep_independent_memory_sources(storage):
    agent,_,_=make(storage,[response('emit_progress',{'content':'已确认苹果数量'},'p1'),
        response('emit_progress',{'content':'已确认合同日期'},'p2'),response(identity='end')])
    await agent.run('公开进展',owner='a',tree_id='sources',task_id='f'*32)
    repo=agent.repository('a','sources')
    task,_=await repo.load('f'*32)
    assert len(task.output_ids)==3 and len(set(task.output_ids))==3
    assert len(task.checkpoint_ids)==4
    for checkpoint_id in task.checkpoint_ids:
        checkpoint=await storage.read(repo.scope,'checkpoint:'+checkpoint_id)
        assert await storage.read(repo.scope,'workspace:'+checkpoint.value['workspace_id'])
    from agent_core.memory import public_sources
    sources=public_sources(task)
    assert len(sources)==4
    assert [text for field,identifier,text in sources if field=='intermediate_output']==['已确认苹果数量','已确认合同日期']


async def test_recovery_keeps_superseded_semantics(storage):
    agent,_,_=make(storage,[])
    repo=agent.repository('a','superseded')
    task,_,_=await repo.begin(AgentInput(question='旧方向'),task_id='0'*32)
    await agent.recover(owner='a',tree_id='superseded',task_id=task.id,status='superseded')
    context=await repo.context(task.id)
    assert context.turns[0].status=='superseded'


async def test_cached_result_preserves_delivery_failure(storage):
    agent,client,_=make(storage,[response()])
    publish=AsyncMock(side_effect=OSError('synthetic delivery failure'))
    result=await agent.run('交付失败',owner='a',tree_id='delivery',task_id='a1'*16,publish=publish)
    assert result.delivery_error_type=='OSError'
    cached=await agent.run('交付失败',owner='a',tree_id='delivery',task_id='a1'*16)
    assert cached.delivery_error_type=='OSError' and len(client.inputs)==1


async def test_idle_heartbeat_prunes_only_silent_first_round(storage):
    from agent_core import SchedulerSettings
    silent = {'content': '没有待办', 'send_to_user': False, 'prune_current_task': True}
    agent, client, _ = make(storage, [response(), response(arguments=silent)])
    repo = agent.repository('alice', 'heart')
    await agent.run('建立上下文', owner='alice', tree_id='heart', task_id='1'*32)
    old_tree = await storage.read(repo.scope, 'tree')
    send = AsyncMock()
    config = SchedulerSettings(heartbeat_interval_seconds=60, heartbeat_message='检查项目待办')
    scheduler = agent.scheduler(owner='alice', tree_id='heart', settings=config, publish=send)
    result = await scheduler.tick()
    assert result.answer == '没有待办'
    tree = await storage.read(repo.scope, 'tree')
    assert tree.value['tasks'] == old_tree.value['tasks'] and tree.value['head'] == '1'*32
    assert (await storage.read(repo.scope,'heartbeat')).value['last_heartbeat_at'] and tree.value['active'] is None
    assert '系统心跳检查' in str(client.inputs[-1]['messages'])
    assert '检查项目待办' in str(client.inputs[-1]['messages'])
    send.assert_not_awaited()
    assert await scheduler.tick() is None
    # 冷启动只保留去重账本，当前心跳无节点、生成、审计或记忆索引。
    keys = dict(storage._db.execute('SELECT key,value FROM records WHERE scope=?', (repo.scope,)))
    marker, = [key for key in keys if key.startswith('pruned:')]
    assert marker.replace('pruned:', 'task:') not in keys
    assert len([k for k in keys if k.startswith('generation:')]) == 1
    assert len([k for k in keys if k.startswith('audit:')]) == 1
    await agent.close()
    restarted, _, _ = make(storage, [])
    assert await restarted.scheduler(owner='alice', tree_id='heart', settings=config).tick() is None
    with pytest.raises(StorageConflict):
        await restarted.run('重复', owner='alice', tree_id='heart', task_id=marker[7:])
    await restarted.close()


async def test_heartbeat_after_tool_keeps_evidence(storage):
    from agent_core import SchedulerSettings
    silent = {'content':'核对完成', 'send_to_user':False, 'prune_current_task':True}
    agent, client, _ = make(storage, [response('get_current_time', {}, 'a'),
        response(arguments=silent, identity='b'), response(arguments={**silent, 'prune_current_task':False}, identity='c')])
    repo = agent.repository('alice', 'keep')
    await agent.scheduler(owner='alice', tree_id='keep', settings=SchedulerSettings(heartbeat_interval_seconds=30)).tick()
    assert '本次已有其他交互' in str(client.inputs[-1]['messages'])
    tree = await storage.read(repo.scope, 'tree')
    task, _ = await repo.load(tree.value['head'])
    assert task.source == 'heartbeat' and len(task.generation_ids) == 3
    assert '本次已有其他交互' not in str(task.exchanges)
    assert task.question not in [s[2] for s in public_sources(task)]
    await agent.close()


async def test_reminder_checkpoint_due_restart_and_deletion(storage):
    from datetime import datetime, timedelta, timezone
    from agent_core.scheduler import Reminders
    now = datetime.now(timezone.utc)
    args = {'content':'核对库存', 'schedule_type':'once',
            'schedule_config':{'at':(now + timedelta(days=1)).isoformat()}}
    agent, client, _ = make(storage, [
        response('create_reminder',{**args, 'schedule_config':{'at':(now-timedelta(days=1)).isoformat()}},'bad'),
        response('create_reminder',args,'create'), response('list_reminders',{'offset':0},'list'), response()
    ], reminder_tools=True)
    await agent.run('明天核对库存', owner='u',tree_id='t')
    assert '没有未来触发时间' in str(client.inputs[1]['messages'])
    assert '提醒已创建' in str(client.inputs[2]['messages'])
    assert '没有未来触发时间' not in str(client.inputs[2]['messages'])
    assert '核对库存' in str(client.inputs[3]['messages'])
    repo = agent.repository('u','t')
    reminders = Reminders(repo)
    row, = await reminders.list_current(offset=0,limit=10)
    await reminders.materialize_due(now + timedelta(days=2))
    await agent.close()
    agent, client, _ = make(storage, [response('delete_reminder',{'reminder_id':row.id},'del'),response(),
        response(arguments={'content':'测试完成','send_to_user':False})],reminder_tools=True)
    await agent.run('取消后续提醒',owner='u',tree_id='t')
    assert (await reminders.find(row.id)).status == 'cancelled'
    scheduler = agent.scheduler(owner='u',tree_id='t')
    assert (await scheduler.tick()).answer == '测试完成'
    assert '## 定时任务' in str(client.inputs[-1]['messages'])
    assert await scheduler.tick() is None
    await agent.close()


async def test_competing_schedulers_and_failed_pruning_transaction(storage, monkeypatch):
    from agent_core import SchedulerSettings
    silent = {'content':'无需处理','send_to_user':False,'prune_current_task':True}
    config = SchedulerSettings(heartbeat_interval_seconds=60)
    agents = [make(storage, [response(arguments=silent)])[0] for _ in range(2)]
    results = await asyncio.gather(*(a.scheduler(owner='race',tree_id='t',settings=config).tick()
                                    for a in agents), return_exceptions=True)
    assert sum(getattr(r, 'answer', None) == '无需处理' for r in results) == 1
    assert all(r is None or isinstance(r, StorageConflict) or getattr(r,'answer',None) for r in results)
    for a in agents:
        await a.close()

    original = storage.atomic
    async def conflict(scope, writes):
        if any(w.key.startswith('pruned:') for w in writes):
            # 批次末尾强制版本冲突，确认之前的 DELETE 与树指针一同回滚。
            writes = [*writes, Write('absent', 99, {})]
        return await original(scope, writes)
    monkeypatch.setattr(storage,'atomic',conflict)
    agent, _, _ = make(storage,[response(arguments=silent)])
    repo = agent.repository('rollback','t')
    scheduler = agent.scheduler(owner='rollback',tree_id='t',settings=config)
    with pytest.raises(AgentCoreError):
        await scheduler.tick()
    tree = await storage.read(repo.scope,'tree')
    identifier = (await storage.read(repo.scope,'heartbeat')).value['heartbeat_pending']
    task, _ = await repo.load(identifier)
    assert task.status == 'failed' and tree.value['head'] == identifier
    assert await storage.read(repo.scope,'generation:'+task.generation_ids[0])
    assert await storage.read(repo.scope,'checkpoint:'+task.checkpoint_ids[0])
    assert not await storage.read(repo.scope,'pruned:'+identifier)
    assert await scheduler.tick() is None
    await repo.acknowledge_heartbeat(identifier)
    assert (await storage.read(repo.scope,'heartbeat')).value['heartbeat_pending'] is None
    await agent.close()


async def test_background_scheduler_default_off_and_custom_message(storage):
    from agent_core import SchedulerSettings
    from agent_core.scheduler import HEARTBEAT_MESSAGE
    agent, client, _ = make(storage, [response(arguments={'content':'有待办','send_to_user':True})])
    disabled = agent.scheduler(owner='u', tree_id='disabled')
    assert await disabled.tick() is None and not client.inputs
    delivered = asyncio.Event()
    async def publish(kind, text):
        assert (kind, text) == ('final','有待办')
        delivered.set()
    service = agent.scheduler(owner='u', tree_id='enabled',
        settings=SchedulerSettings(heartbeat_interval_seconds=60), publish=publish)
    service.start()
    await asyncio.wait_for(delivered.wait(), 2)
    await agent.close()
    assert HEARTBEAT_MESSAGE in str(client.inputs[0]['messages'])
    assert len(client.inputs) == 1 and not agent._schedulers


def test_calendar_dst_gap_and_missing_month_day():
    from datetime import datetime
    from agent_core.reminder_schedule import next_occurrence
    from agent_core.reminder_tools import parse_create_reminder
    def next_time(after, **rule):
        args = parse_create_reminder(json.dumps({'content':'检查待办','timezone':'America/New_York',
            'schedule_type':'calendar','schedule_config':{'start_date':'2026-01-01', **rule}}))
        return next_occurrence(args.schedule_config, args.timezone, datetime.fromisoformat(after))
    assert next_time('2026-03-08T00:00:00-05:00',frequency='daily',local_time='02:30:00').isoformat() == '2026-03-09T06:30:00+00:00'
    assert next_time('2026-02-01T00:00:00-05:00',frequency='monthly',day_of_month=31,
                     missing_day='last_day',local_time='09:00:00').day == 28


async def test_reminder_create_rollback(storage, monkeypatch):
    agent, client, _ = make(storage, [response('create_reminder',{'content':'核查',
        'schedule_type':'once','schedule_config':{'at':'2099-05-01T10:00:00+08:00'}})],reminder_tools=True)
    original = storage.atomic
    async def conflict(scope, writes):
        if any(w.key == 'reminders' for w in writes):
            writes = [*writes, Write('absent', 99, {})]
        return await original(scope, writes)
    monkeypatch.setattr(storage,'atomic',conflict)
    with pytest.raises(AgentCoreError):
        await agent.run('安排核查',owner='alice',tree_id='rollback')
    repo = agent.repository('alice','rollback')
    assert await storage.read(repo.scope,'reminders') is None
    assert len(client.inputs) == 1
    schema = next(t['function']['parameters'] for t in client.inputs[0]['tools'] if t['function']['name']=='create_reminder')
    assert all(field['description'] for definition in [schema,*schema['$defs'].values()]
               for field in definition['properties'].values())
    await agent.close()
