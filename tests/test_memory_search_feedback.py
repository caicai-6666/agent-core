"""主助理到内部检索循环的反馈验收；模型与检索替身，不写业务数据。"""
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock

from agent_core.core import AgentInput, AgentCoreError, MemoryJournal
from agent_core.core.tool import ToolRegistry
from agent_core.core.tool.memory_search import build_memory_search_registration
from agent_core.core.subgraph.memory_retrieval.pool import MemoryQueryPool
from agent_core.core.subgraph.memory_retrieval.schema import RankedTask
from test_native_tool_content import build, response, assistant_core, exploration_core


class MemorySearchFeedbackTests(unittest.IsolatedAsyncioTestCase):
    def setup_core(self, factory, outcome, journal):
        final = {'content': '完成'}
        if factory is assistant_core:
            final['send_to_user'] = False
        core, client = build(factory, [
            response('search', 'search_memory', {'query': '找回历史方案'}),
            response('missing', 'execute_query', {}),
            response('time', 'set_time_filter', {'date': '2026-11-09'}),
            response('status', 'set_status_filter', {'statuses': ['complete']}),
            response('user', 'set_user_input_query', {'query': '历史方案'}),
            response('progress', 'set_intermediate_output_query', {'query': None}),
            response('answer', 'set_final_output_query', {'query': '方案结论'}),
            response('execute', 'execute_query', {}),
            response('done', 'finish_task', final),
        ])
        pool = MemoryQueryPool(AsyncMock())
        search = AsyncMock(side_effect=outcome) if isinstance(outcome, Exception) else AsyncMock(return_value=outcome)
        entry = build_memory_search_registration(
            pool=pool,
            search=search,
            reference_time=lambda: datetime(2026, 11, 9, tzinfo=timezone.utc),
            core=core,
            journal=lambda: journal,
            thinking_effort='none',
        )
        return core.with_registry(ToolRegistry([*core.registry.entries.values(), entry])), client, pool, search

    async def test_actual_outer_and_internal_feedback_for_hits_empty_and_dependency_failure(self):
        for factory in (assistant_core, exploration_core):
            for outcome in ((RankedTask(node_id='a' * 32, score=1.0),), (), RuntimeError('PRIVATE-dependency')):
                with self.subTest(mode=factory.__module__, outcome=type(outcome).__name__):
                    journal = MemoryJournal()
                    core, client, pool, search = self.setup_core(factory, outcome, journal)
                    try:
                        await core.run(AgentInput(question='查历史'), journal=journal)
                        requests = client.create_chat_completion.await_args_list
                        self.assertEqual(len(requests), 9)
                        self.assertIn('本次未执行查询', str(requests[2].kwargs['messages']))
                        self.assertIn('set_user_input_query', str(requests[2].kwargs['messages']))
                        for index in range(3, 8):
                            messages = requests[index].kwargs['messages']
                            receipts = [m['content'] for m in messages if m.get('role') == 'tool']
                            self.assertEqual(len(receipts), index - 2)
                            self.assertTrue(all(t.startswith('### 记忆检索条件') for t in receipts))
                            self.assertTrue(all('尚未执行检索' in t for t in receipts))
                            self.assertNotIn('本次未执行查询', str(messages))
                        receipt = next(m['content'] for m in requests[-1].kwargs['messages']
                                       if m.get('role') == 'tool' and m['tool_call_id'] == 'search')
                        self.assertTrue(receipt.startswith('### 记忆检索'))
                        self.assertNotIn('PRIVATE-', receipt)
                        search.assert_awaited_once()
                        plan = search.await_args.args[0]
                        self.assertEqual([q.field for q in plan.queries], ['user_input', 'final_output'])
                        self.assertEqual(plan.statuses, ('complete',))
                        if isinstance(outcome, Exception):
                            self.assertIn('查询失败', receipt)
                            self.assertIn('不能据此判断没有相关记忆', receipt)
                            self.assertEqual(len(pool._entries), 0)
                            self.assertIn('查询失败', str(journal.exchanges))  # 执行失败事实保留，参数纠错轨迹仍清理。
                        elif outcome:
                            self.assertEqual(len(pool._entries), 1)
                            self.assertIn(next(iter(pool._entries)), receipt)
                            self.assertIn('匹配任务：1 条', receipt)
                            saved = next(m['content'] for m in journal.exchanges if m.get('tool_call_id') == 'search')
                            self.assertEqual(saved, receipt)
                        else:
                            self.assertIn('未找到匹配任务', receipt)
                            self.assertEqual(len(pool._entries), 0)
                        self.assertEqual(len(journal.generations), 9)
                    finally:
                        pool.close()

    async def test_checkpoint_failure_does_not_publish_result_set(self):
        class FailingJournal(MemoryJournal):
            async def commit(self, **kwargs):
                if kwargs.get('exchanges'):
                    raise RuntimeError('synthetic checkpoint failure')
                await super().commit(**kwargs)

        journal = FailingJournal()
        core, client, pool, _ = self.setup_core(
            assistant_core, (RankedTask(node_id='a' * 32, score=1.0),), journal,
        )
        try:
            with self.assertRaises(AgentCoreError):
                await core.run(AgentInput(question='查历史'), journal=journal)
            self.assertEqual(len(pool._entries), 0)
            self.assertEqual(journal.exchanges, [])
            self.assertEqual(client.create_chat_completion.await_count, 8)
        finally:
            pool.close()


if __name__ == '__main__':
    unittest.main()
