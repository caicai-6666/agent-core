"""主模型文件检索入口；结果通过 CachePool 暂存并在 journal 提交后发布，失败不泄漏内部集合。"""
import asyncio
from contextlib import AsyncExitStack
from dataclasses import replace
import logging
from textwrap import dedent
from uuid import uuid4
from langgraph.errors import NodeCancelledError
from ..context_rendering.block import text_block
from ..json_codec import ModelActionRejected
from ..model import AgentDependencyError
from ..subgraph.file_retrieval import (
    CandidatePool,
    DEFAULT_FILE_SEARCH_CACHE_POOL,
    FileResultError,
    FileSearchBackend,
    FileSearchResultSet,
    FileSearchSettings,
    SearchFilesArguments,
    build_file_retrieval_subgraph,
    rank_candidates,
)
from .cache import CachePool, current_tool_cache
from .hooks import FeedbackOnlyHook
from .progress import ToolProgress
from .registry import RegisteredTool
from .result import OrdinaryToolContent, ToolExecutionResult


def render_file_search_feedback(arguments, execution, context):
    result = execution.result.tool_result
    if execution.result.status == 'failed':
        text = dedent("""
            ### 文件检索

            状态：查询失败，未发布结果集。
            原因：{message}
            下一步：{required_action}
        """).strip().format(**result)
    else:
        explanation = text_block(result['explanation'])
        if result['count'] == 0:
            text = dedent("""
                ### 文件检索

                状态：查询完成，未返回可查看的候选。
                候选文件：0 个
                未生成结果集 ID，无需调用 view_file_candidates。

                查询范围与限制：
                {explanation}
            """).strip().format(explanation=explanation)
        else:
            text = dedent("""
                ### 文件检索

                状态：查询成功
                结果集 ID：{result_id}
                候选文件：{count} 个
                下一步：使用此结果集 ID 调用 view_file_candidates 查看；命中不代表原文已核实。

                查询范围与限制：
                {explanation}
            """).strip().format(result_id=result['result_id'], count=result['count'], explanation=explanation)
    return replace(
        execution,
        result=execution.result.model_copy(update={'content': OrdinaryToolContent(text=text)}),
    )


def build_file_search_registration(
    *,
    backend: FileSearchBackend,
    core,
    journal,
    thinking_effort: str = 'none',
    cache_pool: CachePool[FileSearchResultSet] = DEFAULT_FILE_SEARCH_CACHE_POOL,
) -> RegisteredTool:
    """构建通用文件检索工具；结果集写入共享 CachePool，随主循环提交原子生效。"""
    if not isinstance(cache_pool, CachePool):
        raise TypeError('cache_pool 必须为 CachePool 实例')

    async def execute(args: SearchFilesArguments):
        candidates = None
        try:
            cache = current_tool_cache(cache_pool)
            parent = None
            if args.result_id is not None:
                parent = cache.get(args.result_id)
                if parent is None or parent.closed:
                    raise FileResultError(
                        '结果集不存在或已释放。',
                        '请重新使用 search_files 检索并取得新的最终结果集 ID；若必须限定原范围，不要删除 result_id 后扩大查询。',
                    )
            initial = tuple(file_id for file_id, _ in parent.ranking.hits) if parent is not None else None
            # 本次初始范围由后端权威回读确定；撤权或空交集都不能退回全库。
            rows = await backend.read(initial)
            allowed = tuple(row.file_id for row in rows)
            if initial is not None and not set(allowed) <= set(initial):
                raise FileResultError(
                    '回读文件超出父结果集范围，本次查询未完成。',
                    '请重新检索后再查看；不要使用未通过范围校验的候选集合。',
                )
            if parent is not None and parent.closed:
                raise FileResultError(
                    '读取期间结果集已释放，本次查询未完成。',
                    '请重新使用 search_files 检索并取得新的最终结果集 ID。',
                )
            if not allowed:
                return ToolExecutionResult(
                    status='succeeded',
                    tool_result={
                        'status': 'success',
                        'count': 0,
                        'explanation': '当前限定范围内没有可访问文件，未生成结果集。',
                    },
                )
            previous = (
                rank_candidates(tuple((file_id, 1.0) for file_id in allowed), search_type='exact', parent=parent.ranking, exact=True)
                if parent is not None else None
            )
            settings = getattr(backend, 'settings', None) or FileSearchSettings()
            candidates = CandidatePool(
                initial=allowed,
                initial_ranking=previous,
                capacity=settings.cache_max_queries,
                k=settings.rrf_k,
                history_weight=settings.history_weight,
            )

            async def search(field, query, scope):
                if parent is not None and parent.closed:
                    raise FileResultError(
                        '读取期间结果集已释放，本次查询未完成。',
                        '请重新使用 search_files 检索并取得新的最终结果集 ID。',
                    )
                result = await backend.search(field, query, scope)
                if parent is not None and parent.closed:
                    raise FileResultError(
                        '读取期间结果集已释放，本次查询未完成。',
                        '请重新使用 search_files 检索并取得新的最终结果集 ID。',
                    )
                return result

            async with AsyncExitStack() as stack:
                from agent_core.infrastructure.mllm import MLLMClient
                client = core.client if core.client is not None else await stack.enter_async_context(MLLMClient(core.settings))

                async def generate(messages, tools, generation):
                    return await core._invoke(client, journal(), messages, tools, generation, 'file_retrieval')

                async def record_retrieval(event):
                    try:
                        await journal().record_file_retrieval(event)
                    except Exception as error:
                        raise AgentDependencyError('文件检索审计保存失败，已停止') from error

                graph = build_file_retrieval_subgraph(
                    pool=candidates,
                    search=search,
                    generate=generate,
                    counter=core.counter,
                    generation=core.generation(thinking_effort),
                    context_window=core.settings.context_window_tokens,
                    tool_template=core.tool_call_template,
                    max_rounds=settings.max_rounds,
                    audit=record_retrieval,
                )
                try:
                    ranking, explanation = (await graph.ainvoke({'query': args.query}))['result']
                except ModelActionRejected as error:
                    # 内部规划器已耗尽或明确失败，不算外层主助手的参数错误。
                    return ToolExecutionResult(
                        status='failed',
                        failure_mode='failed',
                        tool_result={
                            'code': 'file_search_failed',
                            'message': error.reason,
                            'required_action': error.required_action,
                        },
                    )
            if parent is not None and parent.closed:
                raise FileResultError(
                    '读取期间结果集已释放，本次查询未完成。',
                    '请重新使用 search_files 检索并取得新的最终结果集 ID。',
                )
            if ranking is None:
                return ToolExecutionResult(
                    status='succeeded',
                    tool_result={'status': 'success', 'count': 0, 'explanation': explanation},
                )
            current = await backend.read(tuple(file_id for file_id, _ in ranking.hits))
            ids = {row.file_id for row in current}
            if not ids:
                return ToolExecutionResult(
                    status='succeeded',
                    tool_result={
                        'status': 'success',
                        'count': 0,
                        'explanation': '候选已不可访问，请重新检索。',
                    },
                )
            ranking = rank_candidates(
                tuple((file_id, 1.0) for file_id in ids),
                search_type='exact',
                parent=ranking,
                exact=True,
            )
            rid = 'file-query:' + str(uuid4())
            entry = FileSearchResultSet(result_id=rid, ranking=ranking, explanation=explanation)
            # 写入由 CachePool 事务暂存，仅在主循环 journal.commit 成功后原子提交到作用域 LRU。
            cache.put(rid, entry)
            return ToolExecutionResult(
                status='succeeded',
                tool_result={
                    'status': 'success',
                    'result_id': rid,
                    'count': len(ranking.hits),
                    'explanation': explanation,
                },
            )
        except NodeCancelledError:
            raise asyncio.CancelledError() from None
        except ModelActionRejected as error:
            return ToolExecutionResult(
                status='failed',
                failure_mode='correctable',
                tool_result={
                    'code': 'file_search_rejected',
                    'message': error.reason,
                    'required_action': error.required_action,
                },
            )
        except FileResultError as error:
            return ToolExecutionResult(
                status='failed',
                failure_mode='correctable',
                tool_result={
                    'code': 'file_result_unavailable',
                    'message': error.reason if error.required_action else '结果集引用未通过有效性检查，本次查询未完成。',
                    'required_action': error.required_action or '请重新取得有效的最终结果集 ID；若必须限定原范围，不要删除 result_id 后扩大查询。',
                },
            )
        except (AgentDependencyError, PermissionError):
            raise
        except Exception as error:
            logging.getLogger(__name__).warning('文件检索失败：error_type=%s', type(error).__name__)
            return ToolExecutionResult(
                failure_mode='failed',
                status='failed',
                tool_result={
                    'code': 'file_search_unavailable',
                    'message': '文件检索未能完成，未发布可用结果；不能据此判断没有相关文件。',
                    'required_action': '请稍后按原范围重新检索；不要使用本次尚未发布的候选引用。',
                },
            )
        finally:
            if candidates is not None:
                candidates.close()

    def progress(args: SearchFilesArguments):
        # 只压缩展示空白并截断状态长度，实际检索保留完整 query 和条件关系。
        text = ''.join(char for char in ' '.join(args.query.split()) if char.isprintable())
        prefix = '正在检索文件：'
        limit = 2000 - len(prefix)
        if len(text) > limit:
            text = text[:limit - 1] + '…'
        return ToolProgress(type='local-search', message=prefix + text)

    return RegisteredTool(
        'search_files',
        SearchFilesArguments.__doc__,
        SearchFilesArguments,
        lambda name, raw, workspace, args: workspace,
        executor=execute,
        progress=ToolProgress(type='local-search', message='正在检索文件'),
        progress_factory=progress,
        hooks=(FeedbackOnlyHook(render_file_search_feedback),),
        read_pools=(cache_pool,),
        write_pools=(cache_pool,),
    )
