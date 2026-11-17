"""工作区反馈钩子与提交边界；仅使用内存状态和模型替身。"""
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from agent_core.core import AgentInput, AgentCoreError, MemoryJournal
from agent_core.core.tool import ToolRegistry
from agent_core.core.tool.workspace import build_workspace_registrations
from agent_core.core.workspace import empty_workspace
from test_native_tool_content import build, response, assistant_core, exploration_core


class WorkspaceFeedbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_both_modes_add_replace_delete_and_clear_rejection(self):
        for factory in (assistant_core, exploration_core):
            final = {'content': '完成'}
            if factory is assistant_core:
                final['send_to_user'] = False
            core, client = build(factory, [
                response('bad', 'workspace_delete', {'path': '/known_information/info_missing'}),
                response('add', 'workspace_add', {'path': '/known_information', 'value': {
                    'content': '原事实', 'source': '用户', 'status': 'user_reported'}}),
                response('replace', 'workspace_replace', {'path': '/known_information/info_fixed/content',
                                                         'value': '更新事实'}),
                response('delete', 'workspace_delete', {'path': '/known_information/info_fixed'}),
                response('done', 'finish_task', final),
            ])
            journal = MemoryJournal()
            with patch('agent_core.core.tool.workspace.uuid4', return_value=SimpleNamespace(hex='fixed')):
                await core.run(AgentInput(question='整理工作区'), journal=journal)
            requests = client.create_chat_completion.await_args_list
            self.assertIn('path 指向的位置在最新工作区中不存在', str(requests[1].kwargs['messages']))
            for index, identity, title, target in [
                (2, 'add', '工作区新增', '目标容器：/known_information'),
                (3, 'replace', '工作区替换', '目标路径：/known_information/info_fixed/content'),
                (4, 'delete', '工作区删除', '目标路径：/known_information/info_fixed'),
            ]:
                messages = requests[index].kwargs['messages']
                receipt = next(m for m in messages if m.get('role') == 'tool' and m['tool_call_id'] == identity)
                text = receipt['content']
                self.assertTrue(text.startswith('### ' + title + '\n\n工作区修改成功。当前使用量：10/'))
                self.assertIn(' token（', text)
                self.assertTrue(text.endswith(target))
                saved = next(m for m in journal.exchanges if m.get('role') == 'tool' and m['tool_call_id'] == identity)
                self.assertEqual(saved['content'], text)
                self.assertNotIn('info_missing', str(messages))
                self.assertFalse(any(m.get('role') == 'user' and
                    str(m.get('content', '')).startswith('<system-guidence>') for m in messages))
            self.assertEqual(journal.workspace.known_information, {})
            self.assertIn('原事实', str(requests[2].kwargs['messages']))
            self.assertIn('更新事实', str(requests[3].kwargs['messages']))
            self.assertEqual(len(journal.generations), 5)
            self.assertEqual(sum(e['event'] == 'rejected' for e in journal.audit), 1)

    async def test_commit_failure_does_not_send_success_receipt(self):
        class FailingJournal(MemoryJournal):
            async def commit(self, **kwargs):
                if kwargs.get('exchanges'):
                    raise RuntimeError('synthetic commit failure')
                await super().commit(**kwargs)
        core, client = build(assistant_core, [response('replace', 'workspace_replace', {
            'path': '/task_constraints/task', 'value': '任务'})])
        journal = FailingJournal()
        with self.assertRaises(AgentCoreError):
            await core.run(AgentInput(question='整理工作区'), journal=journal)
        self.assertEqual(journal.exchanges, [])
        self.assertEqual(journal.outputs, [])
        client.create_chat_completion.assert_awaited_once()

    async def test_standalone_registry_preserves_machine_receipt_without_claiming_commit(self):
        registry = ToolRegistry(build_workspace_registrations())
        args, candidate = registry.prepare('workspace_replace',
            '{"path":"/task_constraints/task","value":"任务"}', empty_workspace())
        execution = await registry.execute('workspace_replace', args)
        self.assertEqual(candidate.task_constraints.task, '任务')
        self.assertEqual(execution.result.tool_result, {'status': 'accepted'})
        self.assertIn('等待宿主提交', execution.result.content.text)
        self.assertNotIn('修改成功', execution.result.content.text)


if __name__ == '__main__':
    unittest.main()
