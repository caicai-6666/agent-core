"""文件检索反馈链路；内存结果池与模型、后端替身，不访问真实数据。"""
import re
import unittest
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

from agent_core.core import AgentInput, AgentCoreError, MemoryJournal
from agent_core.core.tool import ToolRegistry
from agent_core.core.tool.file_search import build_file_search_registration
from agent_core.core.subgraph.file_retrieval.agent import run_retrieval
from agent_core.core.subgraph.file_retrieval.pool import CandidatePool, FileResultPool
from agent_core.config.mllm import MLLMGenerationSettings
from test_native_tool_content import build, response, assistant_core, exploration_core


def candidate_ids(messages):
    return [re.search(r'候选结果集 ID：(candidate-set:[^\s]+)', m['content'])[1]
            for m in messages if m.get('role') == 'tool' and '候选结果集 ID：' in m['content']]


class FileSearchFeedbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_internal_all_tools_and_invalid_reference_recovery(self):
        requests, audit = [], []

        async def generate(messages, tools, generation):
            requests.append(deepcopy(messages))
            ids = candidate_ids(messages)
            step = len(requests)
            if step == 1:
                return response('bad', 'union_file_results', {'left_result_id': 'missing', 'right_result_id': 'missing'})
            if step == 2:
                return response('name', 'search_files_by_name', {'query': '合同'})
            if step == 3:
                return response('summary', 'search_files_by_summary', {'query': '设备验收', 'result_id': ids[-1]})
            if step == 4:
                return response('passport', 'search_files_by_passport', {'passport': ' Exact Passport '})
            if step == 5:
                return response('union', 'union_file_results', {'left_result_id': ids[-2], 'right_result_id': ids[-1]})
            return response('finish', 'finish_file_retrieval', {'result_id': ids[-1], 'explanation': '候选待核实'})

        async def search(field, query, scope):
            if field == 'passport':
                self.assertEqual(query, ' Exact Passport ')
                return (('2', 1.0),)
            if field == 'abstract':
                self.assertEqual(scope, ('1',))
            return (('1', 1.0),)

        async def record(event):
            audit.append(event)

        pool = CandidatePool(initial=('1', '2'))
        try:
            ranking, explanation = await run_retrieval(
                '合同或通行证', pool=pool, search=search, generate=generate,
                counter=AsyncMock(return_value=10), generation=MLLMGenerationSettings(),
                context_window=1000000, audit=record,
            )
            self.assertEqual({i for i, _ in ranking.hits}, {'1', '2'})
            self.assertEqual(explanation, '候选待核实')
            self.assertIn('候选结果集不存在或已失效', str(requests[1]))
            for messages in requests[2:]:
                self.assertNotIn('missing', str(messages))
                self.assertNotIn('不存在或已失效', str(messages))
                self.assertTrue(all(m['content'].startswith('### 文件候选集合')
                                    for m in messages if m.get('role') == 'tool'))
            self.assertEqual(len(audit), 6)
            self.assertIn('missing', str(audit[0]))
        finally:
            pool.close()

    async def test_outer_results_and_commit_boundary_in_both_modes(self):
        for factory in (assistant_core, exploration_core):
            for outcome in ('hit', 'empty', 'failure', 'commit_failure'):
                with self.subTest(mode=factory.__module__, outcome=outcome):
                    class Journal(MemoryJournal):
                        async def commit(self, **kwargs):
                            if outcome == 'commit_failure' and kwargs.get('exchanges'):
                                raise RuntimeError('synthetic commit failure')
                            await super().commit(**kwargs)

                    journal = Journal()
                    pool = FileResultPool(AsyncMock())
                    core, client = build(factory, [])
                    calls = []

                    async def generate(**kwargs):
                        messages = kwargs['messages']
                        calls.append(deepcopy(messages))
                        if len(calls) == 1:
                            return response('search', 'search_files', {'query': '合同'})
                        if len(calls) == 2:
                            return response('name', 'search_files_by_name', {'query': '合同'})
                        if len(calls) == 3 and outcome != 'failure':
                            ids = candidate_ids(messages)
                            return response('finish', 'finish_file_retrieval', {
                                'result_id': ids[-1] if ids else None, 'explanation': '需核实原文',
                            })
                        if outcome == 'hit':
                            self.assertEqual(len(pool.entries), 1)
                        else:
                            self.assertFalse(pool.entries)
                        final = {'content': '完成'}
                        if factory is assistant_core:
                            final['send_to_user'] = False
                        return response('done', 'finish_task', final)

                    client.create_chat_completion.side_effect = generate

                    async def read(userid, ids, platforms):
                        return [SimpleNamespace(id='1')] if ids is None or '1' in ids else []

                    search = AsyncMock(
                        side_effect=RuntimeError('PRIVATE-error') if outcome == 'failure' else None,
                        return_value=() if outcome == 'empty' else (('1', 1.0),),
                    )
                    backend = SimpleNamespace(
                        read=read, search=search,
                        settings=SimpleNamespace(cache_max_queries=10, rrf_k=60, history_weight=.5, max_rounds=16),
                    )
                    entry = build_file_search_registration(
                        pool=pool, backend=backend, userid='owner', core=core,
                        journal=lambda: journal, thinking_effort='none',
                    )
                    core = core.with_registry(ToolRegistry([*core.registry.entries.values(), entry]))
                    try:
                        if outcome == 'commit_failure':
                            with self.assertRaises(AgentCoreError):
                                await core.run(AgentInput(question='找合同'), journal=journal)
                            self.assertFalse(pool.entries)
                            self.assertEqual(journal.exchanges, [])
                            self.assertEqual(len(calls), 3)
                            continue
                        await core.run(AgentInput(question='找合同'), journal=journal)
                        receipt = next(m['content'] for m in calls[-1]
                                       if m.get('role') == 'tool' and m['tool_call_id'] == 'search')
                        self.assertTrue(receipt.startswith('### 文件检索'))
                        self.assertNotIn('PRIVATE-', receipt)
                        if outcome == 'hit':
                            self.assertIn(next(iter(pool.entries)), receipt)
                            self.assertIn('view_file_candidates', receipt)
                            self.assertIn('需核实原文', receipt)
                        elif outcome == 'empty':
                            self.assertIn('未生成结果集 ID', receipt)
                            self.assertIn('当前条件下未找到候选', str(calls[2]))
                        else:
                            self.assertIn('查询失败', receipt)
                            self.assertIn('请稍后重新检索', receipt)
                        self.assertTrue(any(g.payload.purpose == 'file_retrieval' for g in journal.generations))
                    finally:
                        pool.close()


if __name__ == '__main__':
    unittest.main()
