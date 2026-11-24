"""最终文件候选分页与可折叠资料引用；结果集从 CachePool 读取，页面内容持续回查后端权威权限。"""
import asyncio
from dataclasses import replace
import hashlib
import hmac
import logging
import secrets
from textwrap import dedent
from uuid import uuid4
from ..subgraph.file_retrieval.backend import FileSearchBackend, FileSearchSettings
from ..subgraph.file_retrieval.pool import (
    DEFAULT_FILE_SEARCH_CACHE_POOL,
    FileResultError,
    FileSearchResultSet,
    render_candidate_page,
)
from ..subgraph.file_retrieval.schema import ViewFileCandidatesArguments
from .cache import CachePool, current_tool_cache
from .hooks import FeedbackOnlyHook
from .progress import ToolProgress
from .registry import RegisteredTool
from .result import (
    FoldableReceiptText,
    FoldableToolContent,
    OrdinaryToolContent,
    ToolExecution,
    ToolExecutionResult,
    ToolPageReference,
    ToolPageUnavailable,
)


def render_file_candidates_feedback(arguments, execution, context):
    result = execution.result.tool_result
    if execution.result.status == 'failed':
        text = dedent("""
            ### 文件候选查看

            状态：查看失败，本次未返回页面，页码未推进。
            原因：{message}
            下一步：{required_action}
        """).strip().format(**result)
        content = OrdinaryToolContent(text=text)
    else:
        template = dedent("""
            ### 文件候选查看

            状态：查看记录已提交
            结果集 ID：{result_id}
            当前页：第 {page} / {total_pages} 页
            结果集候选：{count} 个
            资料状态：{display_state}
            下一步：{next_step}
        """).strip()
        next_step = (
            '可省略 page 查看下一页，或指定有效页码重看。'
            if result['has_next'] else '已到末页；需要重看时请明确指定有效页码。'
        )
        feedback = FoldableReceiptText(
            visible=template.format(
                **result,
                display_state='本页候选见随后提供的文件资料，候选命中不代表原文已核实。',
                next_step=next_step,
            ),
            hidden=template.format(
                **result,
                display_state='本页候选当前未展示，不能从查看记录推断文件信息。',
                next_step='需要核对时使用此结果集 ID 和页码重新查看；引用失效则重新检索。',
            ),
            unavailable=template.format(
                **result,
                display_state='本页文件资料当前无法读取，不能据此推断文件信息。',
                next_step='请重新检索文件并查看新结果。',
            ),
        )
        content = execution.result.content.model_copy(update={'feedback': feedback})
    return replace(
        execution,
        result=execution.result.model_copy(update={'content': content}),
    )


class FileCandidateViewer:
    """从共享 CachePool 读取检索结果集，并在每次查看与页面解析时回查后端权威文件详情。"""

    def __init__(
        self,
        backend: FileSearchBackend,
        *,
        cache_pool: CachePool[FileSearchResultSet] = DEFAULT_FILE_SEARCH_CACHE_POOL,
        page_size: int | None = None,
    ):
        if not isinstance(cache_pool, CachePool):
            raise TypeError('cache_pool 必须为 CachePool 实例')
        settings = getattr(backend, 'settings', None) or FileSearchSettings()
        resolved_page_size = settings.page_size if page_size is None else page_size
        if type(resolved_page_size) is not int or resolved_page_size < 1:
            raise FileResultError('分页大小必须为正整数')
        self.backend = backend
        self.cache_pool = cache_pool
        self.page_size = resolved_page_size
        self.key = secrets.token_bytes(32)
        # 仅保留已提交的精确页面引用及其对应结果集对象；页面正文永不缓存。
        self.issued: dict[str, FileSearchResultSet] = {}
        self._lock = asyncio.Lock()

    def _sign(self, resource: str, locator: str, nonce: str) -> str:
        return hmac.new(self.key, f'{resource}\n{locator}\n{nonce}'.encode(), hashlib.sha256).hexdigest()

    def _prune_closed(self) -> None:
        stale = [display_id for display_id, entry in self.issued.items() if entry.closed]
        for display_id in stale:
            self.issued.pop(display_id, None)

    async def _read_page(self, entry: FileSearchResultSet, target: int) -> tuple[dict, str]:
        total = len(entry.ranking.hits)
        pages = (total + self.page_size - 1) // self.page_size
        if type(target) is not int or not 1 <= target <= pages:
            raise FileResultError(
                '页码超出结果集范围。',
                f'请将 page 设为 1 至 {pages} 之间的整数，或省略 page 查看下一页。',
            )
        start = (target - 1) * self.page_size
        ids = [file_id for file_id, _ in entry.ranking.hits[start:start + self.page_size]]
        rows = {row.file_id: row for row in await self.backend.read(ids)}
        if not set(rows) <= set(ids):
            raise FileResultError(
                '回读文件超出请求范围，本次页面未接受。',
                '请重新检索后再查看；不要使用未通过范围校验的文件资料。',
            )
        if entry.closed:
            raise FileResultError(
                '读取期间结果集已释放，本次页面未接受。',
                '请使用 search_files 重新检索并查看新结果。',
            )
        text = render_candidate_page(
            rows,
            ids,
            start=start,
            page=target,
            total_pages=pages,
            total=total,
        )
        metadata = {
            'result_id': entry.result_id,
            'page': target,
            'total_pages': pages,
            'count': total,
            'has_next': target < pages,
        }
        return metadata, text

    async def view(self, result_id: str, page: int | None = None):
        try:
            async with self._lock:
                cache = current_tool_cache(self.cache_pool)
                entry = cache.get(result_id)
                if entry is None or entry.closed:
                    raise FileResultError(
                        '结果集不存在或已释放。',
                        '请使用 search_files 重新检索并取得新的最终结果集 ID，不复用已失效或其他任务的引用。',
                    )
                total = len(entry.ranking.hits)
                pages = (total + self.page_size - 1) // self.page_size
                target = entry.page + 1 if page is None else page
                if page is None and target > pages:
                    raise FileResultError(
                        '已经到达末页，没有下一页。',
                        f'如需重看，请明确指定 1 至 {pages} 之间的页码；不要继续省略 page 请求下一页。',
                    )
                metadata, _ = await self._read_page(entry, target)
        except FileResultError as error:
            # 只消费就近校验提供的定向反馈，不直接回显任意异常字符串。
            return ToolExecutionResult(
                status='failed',
                failure_mode='correctable',
                tool_result={
                    'code': 'file_page_invalid',
                    'message': error.reason if error.required_action else '文件列表未通过读取校验。',
                    'required_action': error.required_action or '请重新检索并查看新结果，不使用未通过校验的文件资料。',
                },
            )
        except Exception as error:
            logging.getLogger(__name__).warning('文件列表读取失败：error_type=%s', type(error).__name__)
            return ToolExecutionResult(
                failure_mode='failed',
                status='failed',
                tool_result={
                    'code': 'file_page_unavailable',
                    'message': '文件列表读取未完成，暂时无法取得页面。',
                    'required_action': '请稍后重试查看；不能根据读取失败判断文件不存在或权限已撤销。',
                },
            )
        locator, nonce = f"page:{metadata['page']}", str(uuid4())
        reference = ToolPageReference(
            resource_id=result_id,
            locator=locator,
            display_id=nonce + '.' + self._sign(result_id, locator, nonce),
            media_type='text',
            description=f"文件查询结果，第{metadata['page']}/{metadata['total_pages']}页",
        )

        def commit():
            self._prune_closed()
            if not entry.closed:
                entry.page = metadata['page']
                # 页面内容不缓存；仅允许已提交的精确引用在结果集有效期内被解析。
                self.issued[reference.display_id] = entry

        return ToolExecution(
            ToolExecutionResult(
                status='succeeded',
                content=FoldableToolContent(pages=[reference]),
                tool_result=metadata,
            ),
            after_commit=commit,
        )

    async def resolve_page(self, reference: ToolPageReference):
        try:
            nonce, signature = reference.display_id.split('.', 1)
            prefix, number = reference.locator.split(':', 1)
            entry = self.issued.get(reference.display_id)
            if (entry is None
                    or entry.closed
                    or entry.result_id != reference.resource_id
                    or reference.media_type != 'text'
                    or prefix != 'page'
                    or not number.isdigit()
                    or int(number) < 1
                    or not hmac.compare_digest(signature, self._sign(reference.resource_id, reference.locator, nonce))):
                if entry is not None and entry.closed:
                    self.issued.pop(reference.display_id, None)
                raise ValueError('无效页面引用')
            async with self._lock:
                _, text = await self._read_page(entry, int(number))
            return [{'type': 'text', 'text': text}]
        except Exception:
            raise ToolPageUnavailable('文件列表已不可读，需要时请重新检索或查看。') from None


def build_file_candidates_registration(
    viewer: FileCandidateViewer | None = None,
    *,
    backend: FileSearchBackend | None = None,
    cache_pool: CachePool[FileSearchResultSet] = DEFAULT_FILE_SEARCH_CACHE_POOL,
    page_size: int | None = None,
) -> RegisteredTool:
    """构建文件候选分页查看工具；与 search_files 共享同一 CachePool。"""
    if viewer is None:
        if backend is None:
            raise ValueError('必须提供 viewer 或 backend')
        viewer = FileCandidateViewer(backend, cache_pool=cache_pool, page_size=page_size)
    elif cache_pool is not DEFAULT_FILE_SEARCH_CACHE_POOL and viewer.cache_pool is not cache_pool:
        raise ValueError('传入的 cache_pool 与 viewer.cache_pool 不一致')
    return RegisteredTool(
        'view_file_candidates',
        ViewFileCandidatesArguments.__doc__ + f'每页最多{viewer.page_size}条。',
        ViewFileCandidatesArguments,
        lambda name, raw, workspace, args: workspace,
        executor=lambda args: viewer.view(**args.model_dump()),
        return_types=('ordinary', 'foldable'),
        page_resolver=viewer.resolve_page,
        progress=ToolProgress(type='thinking', message='正在查看文件列表'),
        hooks=(FeedbackOnlyHook(render_file_candidates_feedback),),
        read_pools=(viewer.cache_pool,),
    )
