"""上下文整理反馈：内存宿主与整理替身，不访问模型、数据库或数据目录。"""
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from agent_core.core import AgentCoreError, AgentInput, MemoryJournal
from agent_core.core.model import ContextInjection, HistoryTurn
from agent_core.core.tool import ToolRegistry
from agent_core.core.tool.context import build_context_registrations
from agent_core.core.workspace import empty_workspace
from test_native_tool_content import build, response, assistant_core, exploration_core


def history():
    return ContextInjection(
        summary='原有摘要',
        turns=[HistoryTurn(question=f'历史问题{i}', answer=f'历史答案{i}') for i in range(3)],
    )


def stub_compaction(core, context, *, prefix_count=2):
    core.fifo_usage = AsyncMock(
        return_value=SimpleNamespace(full=False, prefix_count=prefix_count, warning=False),
    )
    core._compress = AsyncMock(
        return_value=SimpleNamespace(summary='更新后的摘要', workspace=context.workspace),
    )


class ContextFeedbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_both_modes_commit_receipt_and_keep_remaining_and_active_history(self):
        for factory in (assistant_core, exploration_core):
            with self.subTest(mode=factory.__module__):
                final = {'content': '完成'}
                if factory is assistant_core:
                    final['send_to_user'] = False
                core, client = build(factory, [
                    response('bad', 'compact_context', {'focus': ''}),
                    response('time', 'get_current_time', {}),
                    response('compact', 'compact_context', {'focus': '保留事实'}),
                    response('done', 'finish_task', final),
                ])
                context = history()
                stub_compaction(core, context)
                journal = MemoryJournal()
                await core.run(AgentInput(question='当前任务', context=context), journal=journal)

                requests = client.create_chat_completion.await_args_list
                self.assertIn('focus', str(requests[1].kwargs['messages']))
                messages = requests[3].kwargs['messages']
                receipts = [m for m in messages if m.get('role') == 'tool']
                self.assertEqual([m['tool_call_id'] for m in receipts], ['time', 'compact'])
                expected = (
                    '### 上下文整理\n\n'
                    '状态：整理成功\n'
                    '已整理历史任务：2 条\n'
                    '保留范围：未纳入整理的历史任务及本轮活动记录。\n'
                    '下一步：依据更新后的摘要和工作区继续当前任务。'
                )
                self.assertEqual(receipts[-1]['content'], expected)
                saved = next(m for m in journal.exchanges if m.get('tool_call_id') == 'compact')
                self.assertEqual(saved['content'], expected)
                checkpoint = next(m for m in journal.exchanges if m.get('kind') == 'context_summary')
                self.assertEqual(checkpoint['covered_turns'], 2)
                self.assertEqual(checkpoint['text'], '更新后的摘要')
                self.assertIn('更新后的摘要', str(messages))
                self.assertIn('历史问题2', str(messages))
                self.assertNotIn('历史问题0', str(messages))
                self.assertNotIn('历史问题1', str(messages))
                self.assertFalse(any(
                    m.get('role') == 'user' and str(m.get('content', '')).startswith('<system-guidence>')
                    for m in messages
                ))
                core._compress.assert_awaited_once()
                self.assertEqual(core._compress.await_args.kwargs['source']['summary'], '原有摘要')
                self.assertEqual(len(journal.generations), 4)
                self.assertEqual(sum(e['event'] == 'rejected' for e in journal.audit), 1)

    async def test_no_history_or_compression_failure_never_sends_success(self):
        for failure in ('no_history', 'compression', 'capacity'):
            with self.subTest(failure=failure):
                core, client = build(assistant_core, [
                    response('compact', 'compact_context', {'focus': '保留事实'}),
                ])
                context = history() if failure != 'no_history' else ContextInjection()
                stub_compaction(core, context, prefix_count=0 if failure == 'no_history' else 2)
                if failure == 'compression':
                    core._compress.side_effect = RuntimeError('private compression error')
                elif failure == 'capacity':
                    async def usage(*args, **kwargs):
                        return SimpleNamespace(
                            full=core._compress.await_count > 0,
                            prefix_count=2,
                            warning=False,
                        )
                    core.fifo_usage.side_effect = usage
                journal = MemoryJournal()
                with self.assertRaises(AgentCoreError):
                    await core.run(AgentInput(question='当前任务', context=context), journal=journal)
                self.assertEqual(journal.exchanges, [])
                client.create_chat_completion.assert_awaited_once()
                if failure == 'no_history':
                    core._compress.assert_not_awaited()

    async def test_failed_commit_does_not_publish_candidate_summary_or_receipt(self):
        class FailingJournal(MemoryJournal):
            async def commit(self, **kwargs):
                if kwargs.get('exchanges'):
                    raise RuntimeError('synthetic commit failure')
                await super().commit(**kwargs)

        core, client = build(assistant_core, [
            response('compact', 'compact_context', {'focus': '保留事实'}),
        ])
        context = history()
        stub_compaction(core, context)
        journal = FailingJournal()
        with self.assertRaises(AgentCoreError):
            await core.run(AgentInput(question='当前任务', context=context), journal=journal)
        self.assertEqual(journal.exchanges, [])
        client.create_chat_completion.assert_awaited_once()

    async def test_standalone_registry_cannot_claim_compaction_success(self):
        registry = ToolRegistry(build_context_registrations())
        args, _ = registry.prepare('compact_context', '{"focus":"保留事实"}', empty_workspace())
        execution = await registry.execute('compact_context', args)
        self.assertEqual(execution.result.tool_result, {'status': 'accepted'})
        self.assertIn('尚不能确认完成', execution.result.content.text)
        self.assertNotIn('整理成功', execution.result.content.text)


if __name__ == '__main__':
    unittest.main()
