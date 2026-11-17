"""原生工具与附带说明兼容：验收工具、保留审计、隔离未提交正文。"""
import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from helpers import assistant_core
from helpers import exploration_core
from types import SimpleNamespace as AuthenticatedUser
from agent_core.core import AgentInput, AgentCoreError, CoreSettings, MemoryJournal
from agent_core.config.mllm import MLLMSettings, MLLMGenerationSettings
from agent_core.infrastructure.mllm import _completion


def response(identity, name=None, args=None, *, content='附带说明：这不是工具结果', finish='tool_calls', refusal=None):
    message = {'role': 'assistant', 'content': content}
    if name:
        message['tool_calls'] = [{'id': identity, 'type': 'function', 'function': {
            'name': name, 'arguments': json.dumps(args, ensure_ascii=False)}}]
    if refusal:
        message['refusal'] = refusal
    return _completion({'id': 'synthetic-response', 'model': 'synthetic-model', 'choices': [
        {'message': message, 'finish_reason': finish}], 'usage': {}})


def build(factory, responses):
    client = SimpleNamespace(create_chat_completion=AsyncMock(side_effect=responses))
    core = factory(MLLMSettings(base_url='http://model.test', tool_tag_enabled=False),
                   client=client, counter=AsyncMock(return_value=10), options=CoreSettings(),
                   **({"user": AuthenticatedUser(userid="synthetic-owner")} if factory is assistant_core else {}))
    return core, client


class NativeToolContentTests(unittest.IsolatedAsyncioTestCase):
    async def test_three_consecutive_mixed_workspace_calls_execute_in_both_modes(self):
        for factory in [assistant_core, exploration_core]:
            with self.subTest(mode=factory.__module__):
                final = {'content': '通过工具交付的正式答案'}
                if factory is assistant_core:
                    final['send_to_user'] = True
                responses = [
                    response('replace', 'workspace_replace', {'path': '/task_constraints/task', 'value': '准时整理资料'}),
                    response('add1', 'workspace_add', {'path': '/known_information', 'value': {
                        'content': '事实甲', 'source': '合成来源甲', 'status': 'confirmed'}}),
                    response('add2', 'workspace_add', {'path': '/known_information', 'value': {
                        'content': '事实乙', 'source': '合成来源乙', 'status': 'confirmed'}}),
                    response('finish', 'finish_task', final),
                ]
                core, client = build(factory, responses)
                journal = MemoryJournal()
                publish = AsyncMock()
                result = await core.run(AgentInput(question='合成任务'), journal=journal, publish=publish)
                self.assertEqual(result.answer, final['content'])
                self.assertEqual(journal.workspace.task_constraints.task, '准时整理资料')
                self.assertEqual(len(journal.workspace.known_information), 2)
                self.assertFalse(any(e['event'] == 'rejected' for e in journal.audit))
                self.assertNotIn('附带说明：这不是工具结果', str(journal.exchanges))
                self.assertNotIn('附带说明：这不是工具结果', str(journal.outputs))
                self.assertTrue(all(g.payload.assistant_message['content'].startswith('附带说明')
                                    for g in journal.generations))
                self.assertEqual(journal.outputs, [('final', final['content'])])
                for call in client.create_chat_completion.await_args_list:
                    self.assertNotIn('附带说明：这不是工具结果', str(call.kwargs['messages']))
                self.assertEqual(journal.status, 'complete')

    async def test_invalid_arguments_with_content_still_rejected_and_cleared_after_success(self):
        core, client = build(assistant_core, [
            response('bad', 'workspace_add', {'path': '/known_information', 'value': 'INVALID-FACT'}),
            response('good', 'workspace_add', {'path': '/known_information', 'value': {
                'content': '正确事实', 'source': '合成来源', 'status': 'confirmed'}}),
            response('finish', 'finish_task', {'content': '完成', 'send_to_user': False}),
        ])
        journal = MemoryJournal()
        await core.run(AgentInput(question='合成纠错任务'), journal=journal)
        self.assertEqual(sum(e['event'] == 'rejected' for e in journal.audit), 1)
        self.assertEqual(len(journal.workspace.known_information), 1)
        self.assertNotIn('INVALID-FACT', str(journal.exchanges))
        self.assertIn('INVALID-FACT', str(journal.generations))
        self.assertNotIn('INVALID-FACT', str(client.create_chat_completion.await_args_list[-1].kwargs['messages']))
        self.assertNotIn('<system-guidence>', str(journal.exchanges))
        self.assertEqual(journal.status, 'complete')

    async def test_missing_unknown_truncated_refused_or_multiple_calls_still_cannot_execute(self):
        multiple = response('one', 'finish_task', {'content': '不应交付', 'send_to_user': True})
        # 模型适配器的不可变完成对象通过 model_copy 扩展第二个原生调用。
        second = response('two', 'finish_task', {'content': '不应交付', 'send_to_user': True})
        from dataclasses import replace
        multiple = replace(multiple, tool_calls=(*multiple.tool_calls, *second.tool_calls))
        cases = [
            response('none', content='finish_task {"content":"不应交付"}', finish='stop'),
            response('unknown', 'not_a_tool', {}),
            response('truncated', 'finish_task', {'content': '不应交付', 'send_to_user': True}, finish='length'),
            response('refused', 'finish_task', {'content': '不应交付', 'send_to_user': True}, refusal='refused'),
            multiple,
        ]
        for candidate in cases:
            with self.subTest(reason=candidate.finish_reason, calls=len(candidate.tool_calls)):
                core, _ = build(assistant_core, [candidate])
                core.options = core.options.model_copy(update={'max_consecutive_errors': 1})
                journal = MemoryJournal()
                with self.assertRaises(AgentCoreError):
                    await core.run(AgentInput(question='合成非法输出'), journal=journal)
                self.assertEqual(journal.outputs, [])
                self.assertEqual(journal.exchanges, [])
                self.assertEqual(journal.status, 'failed')

    async def test_memory_retrieval_executes_mixed_calls_and_keeps_history_clean(self):
        from agent_core.core.subgraph.memory_retrieval.agent import run_memory_retrieval
        generate = AsyncMock(side_effect=[
            response('set', 'set_user_input_query', {'query': '合成历史问题'}),
            response('query', 'execute_query', {}),
        ])
        search = AsyncMock(return_value=())
        result = await run_memory_retrieval({'query': '合成需求', 'reference_time': 0},
            search=search, generate=generate, counter=AsyncMock(return_value=1),
            generation=MLLMGenerationSettings(), context_window=100000)
        self.assertEqual(result.status, 'success')
        search.assert_awaited_once()
        self.assertNotIn('附带说明：这不是工具结果', str(generate.await_args_list[1].args[0]))

    async def test_file_retrieval_executes_mixed_calls_and_retains_private_audit(self):
        from agent_core.core.subgraph.file_retrieval.agent import run_retrieval
        from agent_core.core.subgraph.file_retrieval.pool import CandidatePool
        generate = AsyncMock(side_effect=[
            response('search', 'search_files_by_name', {'query': '合成文件'}),
            response('finish', 'finish_file_retrieval', {'explanation': '未找到候选'}),
        ])
        search = AsyncMock(return_value=[])
        audit = AsyncMock()
        result = await run_retrieval({'query': '合成需求'}, pool=CandidatePool(), search=search,
            generate=generate, counter=AsyncMock(return_value=1), generation=MLLMGenerationSettings(),
            context_window=100000, audit=audit)
        self.assertEqual(result, (None, '未找到候选'))
        search.assert_awaited_once()
        self.assertNotIn('附带说明：这不是工具结果', str(generate.await_args_list[1].args[0]))
        self.assertTrue(all('附带说明' in call.args[0]['assistant_message']['content']
                            for call in audit.await_args_list))

    async def test_exploration_preview_uses_only_tool_argument_content(self):
        from agent_core.output_stream import InteractionOutputStream
        events = []
        async def publish(event, data):
            events.append((event, data))
        observer = InteractionOutputStream(publish, output_kinds={'finish_task': 'final'})
        await observer.begin()
        item = response('stream', 'finish_task', {'content': '正式答案'})
        await observer.on_delta(item.assistant_message)
        self.assertEqual(observer.text, '正式答案')
        self.assertTrue(observer.active)
        await observer.prepare_commit(SimpleNamespace(name='finish_task', call_id='stream'), '正式答案')
        await observer.complete('正式答案')
        self.assertNotIn('附带说明：这不是工具结果', str(events))
        self.assertEqual(events[-1][1]['status'], 'completed')
        self.assertEqual(events[-1][1]['text'], '正式答案')
