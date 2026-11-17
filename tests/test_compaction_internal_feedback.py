"""真实整理编排与内存模型替身；不使用业务数据目录或外部服务。"""
import json
import unittest
from unittest.mock import AsyncMock

from agent_core.core import MemoryJournal
from agent_core.core.workspace import read_workspace
from test_native_tool_content import build, response, assistant_core


class InternalCompactionFeedbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_internal_loops_render_clear_failures_and_keep_private_audit(self):
        topic = {
            'topic_id': 'topic_one',
            'title': '有效事实',
            'scope': '保存来源支持的结论',
            'task_ids': ['task_1'],
            'previous_topic_ids': [],
        }
        core, client = build(assistant_core, [
            response('edit_bad', 'workspace_delete', {'path': '/known_information/info_missing'}),
            response('edit_good', 'workspace_delete', {'path': '/task_constraints/supplements/supplement_one'}),
            response('edit_done', 'finish_compression', {'summary': '删除冗余补充'}),
            response('topic_bad', 'extract_topic', {**topic, 'task_ids': ['missing']}),
            response('topic_good', 'extract_topic', topic),
            response('topic_done', 'finish_topic_planning', {'summary': '选择一个主题'}),
            response('summary', content=json.dumps({'summary': {
                'current_memory': [{'content': '已核对事实', 'task_ids': ['task_1'], 'sources': []}],
                'open_items': [],
            }}), finish='stop'),
        ])
        workspace = read_workspace({
            'task_constraints': {'task': None, 'supplements': {'supplement_one': '冗余要求'}},
            'known_information': {},
            'remaining_directions': {},
            'explored_directions': {},
        })

        async def count(messages, tools, generation):
            return 100 if not tools and '冗余要求' in str(messages) else 10

        core.counter = AsyncMock(side_effect=count)
        journal = MemoryJournal()
        result = await core._compress(
            client,
            journal,
            source={'questions_and_answers': [{'question': '核对事实', 'answer': '已核对事实'}]},
            workspace=workspace,
            generation=core.settings.generation,
            workspace_budget=100,
            summary_limit=1000,
        )
        requests = client.create_chat_completion.await_args_list
        self.assertEqual(len(requests), 7)
        self.assertIn('path 指向的位置在最新工作区中不存在', str(requests[1].kwargs['messages']))
        edit_messages = requests[2].kwargs['messages']
        edit_receipt = next(m for m in edit_messages if m.get('role') == 'tool')
        self.assertTrue(edit_receipt['content'].startswith(
            '### 工作区副本编辑\n\n状态：修改成功，仅更新整理副本。\n'
            '位置：/task_constraints/supplements/supplement_one\n当前使用量：10/100 token'))
        self.assertNotIn('info_missing', str(edit_messages))
        self.assertNotIn('<system-guidence>', str([m for m in edit_messages if m['role'] != 'system']))
        self.assertIn('未提供的来源', str(requests[4].kwargs['messages']))
        planning_messages = requests[5].kwargs['messages']
        topic_receipt = next(m for m in planning_messages if m.get('role') == 'tool')
        self.assertEqual(topic_receipt['content'],
            '### 主题选择\n\n'
            '状态：主题选择已接受，最新主题集合见本轮提供的资料。\n'
            '下一步：继续选择或更新主题；筛选完成后调用 finish_topic_planning。')
        self.assertNotIn('未提供的来源', str(planning_messages))
        self.assertNotIn('missing', str(planning_messages))
        self.assertIsNotNone(requests[6].kwargs.get('json_schema'))
        self.assertEqual(requests[6].kwargs['tools'], [])
        self.assertEqual(result.workspace.task_constraints.supplements, {})
        self.assertEqual(workspace.task_constraints.supplements, {'supplement_one': '冗余要求'})
        self.assertIn('已核对事实', result.summary)
        self.assertEqual(len(journal.generations), 7)
        self.assertIn('info_missing', str(journal.generations))
        self.assertIn('missing', str(journal.generations))
        self.assertEqual(journal.exchanges, [])


if __name__ == '__main__':
    unittest.main()
