"""文件分页可读反馈与授权回查；内存替身，不访问业务文件或数据库。"""
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from agent_core.core import AgentInput, AgentCoreError, MemoryJournal
from agent_core.core.model import ContextInjection, HistoryTurn
from agent_core.core.page_display import PageDisplayWindow
from agent_core.core.context_rendering.page import render_tool_receipt
from agent_core.core.tool import ToolRegistry
from agent_core.core.tool.result import ToolPageUnavailable
from agent_core.core.tool.file_candidates import FileCandidateViewer, build_file_candidates_registration
from agent_core.core.subgraph.file_retrieval.pool import FileResultPool
from agent_core.core.subgraph.file_retrieval.ranking import rank_candidates
from test_native_tool_content import build, response, assistant_core, exploration_core


class FileCandidatesFeedbackTests(unittest.IsolatedAsyncioTestCase):
    def setup_pool(self):
        rows = {
            i: SimpleNamespace(id=i, name='合同' + i, platform='source', source_id='original-' + i, passport=['passport-' + i])
            for i in ('101', '202')
        }
        async def read(ids, platforms):
            return [rows[i] for i in ids if i in rows]
        pool = FileResultPool(read, page_size=1)
        rid, commit = pool.prepare(rank_candidates((('101', 2.0), ('202', 1.0)), search_type='name'), None, '待核实')
        commit()
        viewer = FileCandidateViewer(pool)
        entry = build_file_candidates_registration(viewer)
        self.addCleanup(pool.close)
        return pool, rid, rows, viewer, entry

    async def test_visibility_revocation_and_signed_commit_only_reference(self):
        pool, rid, rows, viewer, entry = self.setup_pool()
        registry = ToolRegistry([entry])
        execution = await registry.execute(entry.name, entry.arguments_model(result_id=rid))
        reference = execution.result.content.pages[0]
        with self.assertRaises(ToolPageUnavailable):
            await viewer.resolve_page(reference)
        self.assertEqual(pool.entries[rid].page, 0)
        execution.after_commit()
        self.assertEqual(pool.entries[rid].page, 1)
        hidden = render_tool_receipt(execution.result)
        self.assertIn('本页候选当前未展示', hidden)
        window = PageDisplayWindow(registry.resolve_page, max_rounds=1)
        window.register('view', execution.result, tool_name=entry.name)
        receipt = {'role': 'tool', 'tool_call_id': 'view', 'content': hidden}
        shown_messages, shown = await window.inject([receipt])
        self.assertIn('本页候选见随后', shown_messages[0]['content'])
        self.assertIn('合同101', str(shown_messages))
        self.assertIn('original-101', str(shown_messages))
        self.assertIn('passport-101', str(shown_messages))
        del rows['101']
        revoked, _ = await window.inject([receipt])
        self.assertIn('此候选已不可用', str(revoked))
        self.assertNotIn('合同101', str(revoked))
        self.assertNotIn('`101`', str(revoked))
        self.assertNotIn('passport-101', str(revoked))
        pool.close()
        unavailable, _ = await window.inject([receipt])
        self.assertIn('本页文件资料当前无法读取', unavailable[0]['content'])
        self.assertEqual(len(unavailable), 1)
        window.consume(shown, finish_reason='tool_calls')
        folded, _ = await window.inject([receipt])
        self.assertEqual(folded, [receipt])

    async def test_both_modes_page_recovery_saved_receipt_and_history_restore(self):
        for factory in (assistant_core, exploration_core):
            pool, rid, _, _, entry = self.setup_pool()
            final = {'content': '完成'}
            if factory is assistant_core:
                final['send_to_user'] = False
            core, client = build(factory, [
                response('bad', entry.name, {'result_id': rid, 'page': 3}),
                response('good', entry.name, {'result_id': rid}),
                response('done', 'finish_task', final),
            ])
            core = core.with_registry(ToolRegistry([*core.registry.entries.values(), entry]))
            journal = MemoryJournal()
            await core.run(AgentInput(question='查看候选'), journal=journal)
            requests = client.create_chat_completion.await_args_list
            self.assertIn('页码超出结果集范围', str(requests[1].kwargs['messages']))
            self.assertIn('1 至 2', str(requests[1].kwargs['messages']))
            self.assertNotIn('页码超出结果集范围', str(requests[2].kwargs['messages']))
            self.assertIn('合同101', str(requests[2].kwargs['messages']))
            self.assertEqual(pool.entries[rid].page, 1)
            saved = next(m['content'] for m in journal.exchanges if m.get('tool_call_id') == 'good')
            self.assertIn('本页候选当前未展示', saved)
            self.assertEqual(len(journal.generations), 3)
            self.assertEqual(sum(e['event'] == 'rejected' for e in journal.audit), 1)
            other, next_client = build(factory, [response('next', 'finish_task', final)])
            await other.run(AgentInput(question='继续', context=ContextInjection(turns=[
                HistoryTurn(question='查看候选', answer='完成', native_messages=journal.exchanges),
            ])))
            messages = next_client.create_chat_completion.await_args.kwargs['messages']
            self.assertIn(saved, [m.get('content') for m in messages if m.get('role') == 'tool'])
            self.assertNotIn('合同101', str(messages))

    async def test_expired_last_page_dependency_and_out_of_scope_errors(self):
        pool, rid, _, _, entry = self.setup_pool()
        registry = ToolRegistry([entry])
        expired = await registry.execute(entry.name, entry.arguments_model(result_id='missing'))
        self.assertIn('结果集不存在或已释放', expired.result.content.text)
        last = await registry.execute(entry.name, entry.arguments_model(result_id=rid, page=2))
        last.after_commit()
        exhausted = await registry.execute(entry.name, entry.arguments_model(result_id=rid))
        self.assertIn('已经到达末页', exhausted.result.content.text)
        self.assertIn('明确指定 1 至 2', exhausted.result.content.text)
        for reader, reason in [
            (AsyncMock(side_effect=RuntimeError('PRIVATE-error')), '文件列表读取未完成'),
            (AsyncMock(return_value=[SimpleNamespace(id='999')]), '回读文件超出请求范围'),
        ]:
            pool.reader = reader
            execution = await registry.execute(entry.name, entry.arguments_model(result_id=rid, page=1))
            self.assertEqual(execution.result.status, 'failed')
            self.assertIn(reason, execution.result.content.text)
            self.assertNotIn('PRIVATE-', execution.result.content.text)
            self.assertEqual(pool.entries[rid].page, 2)

    async def test_commit_failure_never_issues_page_or_advances_cursor(self):
        pool, rid, _, viewer, entry = self.setup_pool()
        class FailingJournal(MemoryJournal):
            async def commit(self, **kwargs):
                if kwargs.get('exchanges'):
                    raise RuntimeError('synthetic checkpoint failure')
                await super().commit(**kwargs)
        core, client = build(assistant_core, [response('view', entry.name, {'result_id': rid})])
        core = core.with_registry(ToolRegistry([*core.registry.entries.values(), entry]))
        journal = FailingJournal()
        with self.assertRaises(AgentCoreError):
            await core.run(AgentInput(question='查看候选'), journal=journal)
        self.assertEqual(pool.entries[rid].page, 0)
        self.assertEqual(viewer.issued, set())
        self.assertEqual(journal.exchanges, [])
        client.create_chat_completion.assert_awaited_once()


if __name__ == '__main__':
    unittest.main()
