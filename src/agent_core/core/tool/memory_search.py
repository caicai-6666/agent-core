"""主助手只提交自然语言检索需求；祖先范围及参考时间由宿主绑定，结果集通过 CachePool 原子发布。"""
import asyncio
from contextlib import AsyncExitStack
from dataclasses import replace
from textwrap import dedent
from uuid import uuid4
from langgraph.errors import NodeCancelledError
from pydantic import Field
from ..subgraph.memory_retrieval import (
    DEFAULT_MEMORY_CACHE_POOL,
    MemoryQueryEntry,
    build_memory_retrieval_subgraph,
    validate_memory_result,
)
from ..subgraph.memory_retrieval.schema import RetrievalModel, MemoryRetrievalRequest
from .cache import CachePool, current_tool_cache
from .hooks import FeedbackOnlyHook
from .progress import ToolProgress
from .registry import RegisteredTool
from .result import OrdinaryToolContent, ToolExecutionResult


def render_memory_search_feedback(arguments, execution, context):
    result = execution.result.tool_result
    if execution.result.status == 'failed':
        if 'message' in result:
            text = dedent("""
                ### 记忆检索

                状态：查询失败，未生成可查看的结果集。
                原因及处理方向：{message}
            """).strip().format(message=result['message'])
        else:
            text = dedent("""
                ### 记忆检索

                状态：查询失败，未生成可查看的结果集。
                原因：{reason}
                下一步：{required_action}
            """).strip().format(**result)
    elif result['total'] == 0:
        text = dedent("""
            ### 记忆检索

            状态：查询完成，未找到匹配任务。
            匹配任务：0 条
            未生成查询 ID，无需调用 view_memory_query。
        """).strip()
    else:
        text = dedent("""
            ### 记忆检索

            状态：查询成功
            查询 ID：{query_id}
            匹配任务：{total} 条
            总页数：{total_pages} 页
            下一步：使用此查询 ID 调用 view_memory_query 查看。
        """).strip().format(**result)
    return replace(
        execution,
        result=execution.result.model_copy(update={'content': OrdinaryToolContent(text=text)}),
    )


class SearchMemoryArguments(RetrievalModel):
    """当你需要找回先前讨论的问题、处理进展或结论时，使用这个工具。描述希望找回的历史内容和明确限制，补充当前上下文中缺失的信息。成功有结果时返回query_id，再用view_memory_query查看；空结果不生成ID，无需翻页。"""
    query: str = Field(
        strict=True,
        min_length=1,
        max_length=4096,
        description='自然语言检索需求，说明想找回的问题、公开进展或结论，可包含明确日期和任务结束状态；保留已知对象与条件，不猜测历史事实，不编写SQL或节点标识。',
    )


def build_memory_search_registration(
    *,
    search,
    reference_time,
    core,
    journal,
    thinking_effort: str = 'none',
    cache_pool: CachePool[MemoryQueryEntry] = DEFAULT_MEMORY_CACHE_POOL,
    page_size: int = 3,
) -> RegisteredTool:
    """构建记忆检索工具；结果集写入共享 CachePool，随主循环 journal.commit 原子生效。"""
    if not isinstance(cache_pool, CachePool):
        raise TypeError('cache_pool 必须为 CachePool 实例')
    if type(page_size) is not int or page_size < 1:
        raise ValueError('每页条数必须为正整数')

    async def execute(args: SearchMemoryArguments):
        async with AsyncExitStack() as stack:
            from agent_core.infrastructure.mllm import MLLMClient
            client = core.client if core.client is not None else await stack.enter_async_context(MLLMClient(core.settings))

            async def generate(messages, tools, generation):
                return await core._invoke(client, journal(), messages, tools, generation, 'memory_retrieval')

            graph = build_memory_retrieval_subgraph(
                search=search,
                generate=generate,
                counter=core.counter,
                generation=core.generation(thinking_effort),
                context_window=core.settings.context_window_tokens,
                tool_template=core.tool_call_template,
            )
            try:
                result = (await graph.ainvoke({'request': MemoryRetrievalRequest(
                    query=args.query,
                    reference_time=int(reference_time().timestamp() * 1000),
                )}))['result']
            except NodeCancelledError:
                # LangGraph 会将节点内部取消包装为普通异常；还原取消，避免 Core 误记为失败。
                raise asyncio.CancelledError() from None
        if result.status == 'error':
            return ToolExecutionResult(
                status='failed',
                failure_mode='failed',
                tool_result={'code': result.error_code, 'message': result.error},
            )
        if not result.tasks:
            return ToolExecutionResult(
                status='succeeded',
                tool_result={
                    'total': 0,
                    'total_pages': 0,
                    'message': '未召回任务，未生成查询ID，无需翻页。',
                },
            )
        items = validate_memory_result(result)
        query_id = str(uuid4())
        entry = MemoryQueryEntry(query_id=query_id, items=items)
        # 写入由 CachePool 事务暂存，仅在主循环 journal.commit 成功后原子落入作用域 LRU。
        current_tool_cache(cache_pool).put(query_id, entry)
        return ToolExecutionResult(
            status='succeeded',
            tool_result={
                'query_id': query_id,
                'total': len(items),
                'total_pages': (len(items) + page_size - 1) // page_size,
                'message': '查询已完成，请调用view_memory_query查看。',
            },
        )

    return RegisteredTool(
        'search_memory',
        SearchMemoryArguments.__doc__,
        SearchMemoryArguments,
        lambda name, raw, workspace, args: workspace,
        executor=execute,
        progress=ToolProgress(type='thinking', message='正在检索历史记忆'),
        hooks=(FeedbackOnlyHook(render_memory_search_feedback),),
        write_pools=(cache_pool,),
    )
