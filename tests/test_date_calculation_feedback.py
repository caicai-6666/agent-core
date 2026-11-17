"""日期工具反馈验收：纯内存 Core 与模型替身，不使用业务数据目录。"""
import unittest

from agent_core.core import AgentInput, MemoryJournal
from agent_core.core.tool.date_calculator import build_date_calculation_registration, calculate_date
from agent_core.core.tool.registry import ToolRegistry
from agent_core.core.workspace import empty_workspace
from test_native_tool_content import response, build, assistant_core, exploration_core


class DateFeedbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_calculation_facts_preserved_at_calendar_boundaries(self):
        import json
        registry = ToolRegistry([build_date_calculation_registration()])
        cases = [
            ({'start_time': '2026-01-31', 'months': 1, 'days': 1}, '2026-03-01T00:00:00+08:00', True),
            ({'start_time': '2024-02-29', 'years': 1}, '2025-02-28T00:00:00+08:00', True),
            ({'start_time': '2024-02-29', 'years': 1, 'months': -12}, '2024-02-29T00:00:00+08:00', False),
            ({'start_time': '2026-01-01T00:00:00-05:00', 'seconds': -1}, '2025-12-31T23:59:59-05:00', False),
            ({'start_time': '2026-11-09', 'days': None}, '2026-11-09T00:00:00+08:00', False),
        ]
        for raw, expected, clamped in cases:
            with self.subTest(raw=raw):
                args, _ = registry.prepare('calculate_date', json.dumps(raw), empty_workspace())
                execution = await registry.execute('calculate_date', args)
                self.assertEqual(execution.result.tool_result, calculate_date(args))
                self.assertEqual(execution.result.tool_result['result_time'], expected)
                self.assertEqual(execution.result.tool_result['month_end_clamped'], clamped)
                self.assertIn('计算结果：' + expected, execution.result.content.text)
                self.assertIn('年月阶段月末调整：' + ('已调整为目标月份最后一天' if clamped else '未发生'),
                              execution.result.content.text)
                self.assertIsNone(execution.after_commit)
                self.assertIsNone(execution.output)

    async def test_both_modes_receive_failure_then_success_and_clear_failure_chain(self):
        for factory in (assistant_core, exploration_core):
            for overflow in ({'years': 10000}, {'days': 10**20}, {'seconds': -10**20}):
                with self.subTest(mode=factory.__module__, overflow=overflow):
                    final = {'content': '完成'}
                    if factory is assistant_core:
                        final['send_to_user'] = False
                    core, client = build(factory, [
                        response('invalid-date', 'calculate_date', {'start_time': '2026-02-30'}),
                        response('overflow', 'calculate_date', {'start_time': '2026-01-31', **overflow}),
                        response('good', 'calculate_date', {'start_time': '2026-01-31', 'months': 1}),
                        response('end', 'finish_task', final),
                    ])
                    journal = MemoryJournal()
                    await core.run(AgentInput(question='计算日期'), journal=journal)
                    requests = client.create_chat_completion.await_args_list
                    schema_feedback = [m for m in requests[1].kwargs['messages']
                                       if m.get('role') == 'user' and
                                       str(m.get('content', '')).startswith('<system-guidence>')]
                    self.assertTrue(schema_feedback)
                    self.assertIn('start_time', str(schema_feedback))
                    failed = next(m for m in requests[2].kwargs['messages']
                                  if m.get('role') == 'tool' and m['tool_call_id'] == 'overflow')
                    self.assertEqual(failed['content'],
                        '### 日期计算\n\n状态：计算失败\n'
                        '原因：日期或增减量超出运算支持范围，本次未生成结果。\n'
                        '下一步：请核对 start_time 并缩小 years、months、days、hours、minutes、seconds 中的增减量，'
                        '确保年月调整及最终日期均在公元 1 至 9999 年内，再调用 calculate_date。')
                    messages = requests[3].kwargs['messages']
                    receipts = [m for m in messages if m.get('role') == 'tool']
                    self.assertEqual([m['tool_call_id'] for m in receipts], ['good'])
                    expected = ('### 日期计算\n\n状态：计算成功\n'
                                '开始时间：2026-01-31T00:00:00+08:00\n'
                                '计算结果：2026-02-28T00:00:00+08:00\n'
                                '结果星期：星期六\n年月阶段月末调整：已调整为目标月份最后一天')
                    self.assertEqual(receipts[0]['content'], expected)
                    self.assertFalse(any(m.get('role') == 'user' and
                                         str(m.get('content', '')).startswith('<system-guidence>')
                                         for m in messages))
                    saved = next(m for m in journal.exchanges if m.get('role') == 'tool')
                    self.assertEqual(saved['content'], expected)
                    self.assertNotIn('计算失败', str(journal.exchanges))
                    self.assertEqual(len(journal.generations), 4)
                    self.assertEqual(sum(e['event'] == 'rejected' for e in journal.audit), 2)


if __name__ == '__main__':
    unittest.main()
