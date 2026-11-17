"""可折叠记忆回执、真实资料渲染和提交边界；仅内存状态与模型替身。"""
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

from agent_core.core import AgentInput, AgentCoreError, MemoryJournal
from agent_core.core.model import ContextInjection, HistoryTurn
from agent_core.core.page_display import PageDisplayWindow
from agent_core.core.context_rendering.page import render_tool_receipt
from agent_core.core.tool import ToolRegistry
from agent_core.core.tool.result import ToolExecutionResult, FoldableToolContent
from agent_core.core.tool.memory_viewer import MemoryQueryViewer, build_memory_view_registration
from agent_core.core.subgraph.memory_retrieval.pool import MemoryQueryPool
from agent_core.core.workspace import empty_workspace
from test_native_tool_content import build, response, assistant_core, exploration_core


class MemoryViewFeedbackTests(unittest.IsolatedAsyncioTestCase):
    def setup_pool(self):
        async def read(ids):
            return [SimpleNamespace(write=SimpleNamespace(
                node=SimpleNamespace(id=i, created_at=datetime(2026, 11, 9, tzinfo=timezone.utc), status='complete'),
                content=SimpleNamespace(question='历史事实', answer='历史结论', attachments=[], exchanges=[]),
            ), messages=()) for i in ids]
        pool = MemoryQueryPool(read, page_size=1)
        rid, commit = pool.prepare_tasks(('a' * 32, 'b' * 32))
        commit()
        self.addCleanup(pool.close)
        entry = build_memory_view_registration(MemoryQueryViewer(pool))
        return pool, rid, entry

    async def test_visible_hidden_unavailable_and_legacy_receipts(self):
        pool, rid, entry = self.setup_pool()
        registry = ToolRegistry([entry])
        args, _ = registry.prepare('view_memory_query', '{"query_id":"' + rid + '"}', empty_workspace())
        execution = await registry.execute('view_memory_query', args)
        self.assertEqual(pool._entries[rid].current_page, 0)
        execution.after_commit()
        self.assertEqual(pool._entries[rid].current_page, 1)
        hidden = render_tool_receipt(execution.result)
        self.assertIn('正文当前未展示', hidden)
        self.assertIn('第 1 / 2 页', hidden)
        window = PageDisplayWindow(registry.resolve_page, max_rounds=1)
        window.register('view', execution.result, tool_name='view_memory_query')
        receipt = {'role': 'tool', 'tool_call_id': 'view', 'content': hidden}
        visible, shown = await window.inject([receipt])
        self.assertIn('本页正文见随后', visible[0]['content'])
        self.assertIn('任务 ID：' + 'a' * 32, str(visible))
        pool.close()
        unavailable, _ = await window.inject([receipt])
        self.assertIn('本页资料当前无法读取', unavailable[0]['content'])
        self.assertEqual(len(unavailable), 1)
        window.consume(shown, finish_reason='tool_calls')
        folded, _ = await window.inject([receipt])
        self.assertEqual(folded, [receipt])
        legacy = ToolExecutionResult(status='succeeded', tool_result={'query_id': rid},
            content=FoldableToolContent(pages=execution.result.content.pages))
        self.assertTrue(render_tool_receipt(legacy).startswith('{'))

    async def test_both_modes_recover_page_error_and_restore_hidden_history(self):
        for factory in (assistant_core, exploration_core):
            pool, rid, entry = self.setup_pool()
            final = {'content': '完成'}
            if factory is assistant_core:
                final['send_to_user'] = False
            core, client = build(factory, [
                response('bad', 'view_memory_query', {'query_id': rid, 'page': 3}),
                response('good', 'view_memory_query', {'query_id': rid}),
                response('done', 'finish_task', final),
            ])
            core = core.with_registry(ToolRegistry([*core.registry.entries.values(), entry]))
            journal = MemoryJournal()
            await core.run(AgentInput(question='查看历史'), journal=journal)
            failed = str(client.create_chat_completion.await_args_list[1].kwargs['messages'])
            self.assertIn('请求页码超出结果集范围', failed)
            self.assertIn('1 至 2', failed)
            success = str(client.create_chat_completion.await_args_list[2].kwargs['messages'])
            self.assertIn('任务 ID：' + 'a' * 32, success)
            self.assertNotIn('请求页码超出结果集范围', success)
            self.assertEqual(pool._entries[rid].current_page, 1)
            saved = next(m['content'] for m in journal.exchanges if m.get('tool_call_id') == 'good')
            self.assertIn('正文当前未展示', saved)
            self.assertNotIn('历史结论', saved)
            self.assertEqual(sum(e['event'] == 'rejected' for e in journal.audit), 1)
            self.assertEqual(len(journal.generations), 3)
            other, next_client = build(factory, [response('next', 'finish_task', final)])
            await other.run(AgentInput(question='继续', context=ContextInjection(turns=[
                HistoryTurn(question='查看历史', answer='完成', native_messages=journal.exchanges),
            ])))
            restored = str(next_client.create_chat_completion.await_args.kwargs['messages'])
            self.assertIn(saved, restored.replace('\\n', '\n'))
            self.assertNotIn('历史结论', restored)

    async def test_query_expiry_dependency_failure_and_commit_failure(self):
        pool, rid, entry = self.setup_pool()
        registry = ToolRegistry([entry])
        for identity, reason in [('missing', '查询结果不存在或已释放'), (rid, '历史任务读取未完成')]:
            pool.reader = AsyncMock(side_effect=RuntimeError('PRIVATE-error'))
            args = entry.arguments_model(query_id=identity)
            execution = await registry.execute('view_memory_query', args)
            self.assertEqual(execution.result.status, 'failed')
            self.assertIn(reason, render_tool_receipt(execution.result))
            self.assertNotIn('PRIVATE-', render_tool_receipt(execution.result))
            self.assertEqual(pool._entries[rid].current_page, 0)

        pool, rid, entry = self.setup_pool()
        class FailingJournal(MemoryJournal):
            async def commit(self, **kwargs):
                if kwargs.get('exchanges'):
                    raise RuntimeError('synthetic commit failure')
                await super().commit(**kwargs)
        core, client = build(assistant_core, [response('view', 'view_memory_query', {'query_id': rid})])
        core = core.with_registry(ToolRegistry([*core.registry.entries.values(), entry]))
        journal = FailingJournal()
        with self.assertRaises(AgentCoreError):
            await core.run(AgentInput(question='查看历史'), journal=journal)
        self.assertEqual(pool._entries[rid].current_page, 0)
        self.assertEqual(journal.exchanges, [])
        client.create_chat_completion.assert_awaited_once()


if __name__ == '__main__':
    unittest.main()
