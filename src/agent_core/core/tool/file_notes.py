"""文件备忘工具集：增删查与可折叠分页查看；检索结果集通过 CachePool 事务原子发布。"""
import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import replace
import hashlib
import hmac
import inspect
import logging
import secrets
from textwrap import dedent
from typing import Literal
from uuid import uuid4

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from ...file_notes import (
    DEFAULT_FILE_NOTE_CACHE_POOL,
    MAX_SEARCH_FILE_IDS,
    MAX_SEARCH_LIMIT,
    FileNote,
    FileNoteId,
    FileNoteQueryEntry,
    FileNoteRejected,
    FileNoteStore,
    FileNoteWriteUncertain,
    NoteName,
    NoteSummary,
    validate_note_scope,
)
from .cache import CachePool, current_tool_cache
from .feedback import with_feedback
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

logger = logging.getLogger(__name__)

FILE_NOTE_TOOL_NAMES: tuple[str, ...] = (
    'add_file_note',
    'delete_file_note',
    'search_file_notes',
    'view_file_note_query',
)

FileNoteAuthorizer = Callable[[str], bool | Awaitable[bool]]


class _FileNoteArguments(BaseModel):
    model_config = ConfigDict(extra='forbid')


def _escape_single_line(text: str) -> str:
    """将外部单行字段中的换行与控制符折叠为空格，防止伪造反馈字段结构。"""
    return ' '.join(text.replace('\r\n', '\n').replace('\r', '\n').split('\n'))


def _blockquote(text: str) -> str:
    """将多行摘要包裹为 Markdown 引用块，与工具反馈字段保持清晰边界。"""
    lines = text.replace('\r\n', '\n').replace('\r', '\n').split('\n')
    return '\n'.join(f'> {line}' if line else '>' for line in lines)


def render_add_file_note_feedback(arguments, execution, context):
    result = execution.result.tool_result
    if execution.result.status == 'unknown':
        text = dedent("""
            ### 文件备忘添加

            状态：写入结果不明，不能确认是否已保存
            原因：{message}
            下一步：{required_action}
        """).strip().format(**result)
    elif execution.result.status != 'succeeded':
        text = dedent("""
            ### 文件备忘添加

            状态：添加失败，本次未写入
            原因：{message}
            下一步：{required_action}
        """).strip().format(
            message=result.get('message', result.get('reason')),
            required_action=result['required_action'],
        )
    else:
        text = dedent("""
            ### 文件备忘添加

            状态：添加成功
            文件 ID：{file_id}
            文件名称：{name}
            更新时间（UTC）：{updated_at}
            摘要：
            {summary_quote}
        """).strip().format(
            file_id=result['file_id'],
            name=_escape_single_line(result['name']),
            updated_at=result['updated_at'],
            summary_quote=_blockquote(result['summary']),
        )
    return with_feedback(execution, text)


def render_delete_file_note_feedback(arguments, execution, context):
    result = execution.result.tool_result
    if execution.result.status == 'unknown':
        text = dedent("""
            ### 文件备忘删除

            状态：删除结果不明，不能确认是否已删除
            原因：{message}
            下一步：{required_action}
        """).strip().format(**result)
    elif execution.result.status != 'succeeded':
        text = dedent("""
            ### 文件备忘删除

            状态：删除失败，当前状态未确认变更
            原因：{message}
            下一步：{required_action}
        """).strip().format(
            message=result.get('message', result.get('reason')),
            required_action=result['required_action'],
        )
    else:
        state = '已删除' if result['deleted'] else '备忘已不存在，无需删除'
        text = dedent("""
            ### 文件备忘删除

            状态：{state}
            文件 ID：{file_id}
        """).strip().format(state=state, file_id=result['file_id'])
    return with_feedback(execution, text)


def render_search_file_notes_feedback(arguments, execution, context):
    result = execution.result.tool_result
    if execution.result.status != 'succeeded':
        text = dedent("""
            ### 文件备忘检索

            状态：检索失败，未生成可查看的结果集
            原因：{message}
            下一步：{required_action}
        """).strip().format(
            message=result.get('message', result.get('reason')),
            required_action=result['required_action'],
        )
        return with_feedback(execution, text)
    if result['total'] == 0:
        lines = [
            '### 文件备忘检索',
            '',
            '状态：查询完成，未找到匹配的文件备忘',
            '匹配条数：0 条',
            '未生成查询 ID，无需调用 view_file_note_query。',
        ]
        if result.get('truncated'):
            lines.append('范围提示：所依赖的上游查询结果曾被截断，本次仅在原截断范围内筛选。')
        return with_feedback(execution, '\n'.join(lines))
    truncation_note = (
        f'（结果已截断至 {MAX_SEARCH_FILE_IDS} 条，如需更精确请补充筛选条件）'
        if result.get('truncated') else ''
    )
    text = dedent("""
        ### 文件备忘检索

        状态：查询成功
        查询 ID：{query_id}
        匹配条数：{total} 条{truncation_note}
        总页数：{total_pages} 页
        下一步：使用此查询 ID 调用 view_file_note_query 查看详情，或传入此查询 ID 继续缩小筛选范围。
    """).strip().format(**result, truncation_note=truncation_note)
    return with_feedback(execution, text)


def render_view_file_note_query_feedback(arguments, execution, context):
    result = execution.result.tool_result
    if execution.result.status != 'succeeded':
        text = dedent("""
            ### 文件备忘查看

            状态：查看失败，本次未返回页面
            原因：{message}
            下一步：{required_action}
        """).strip().format(
            message=result.get('message', result.get('reason')),
            required_action=result['required_action'],
        )
        content = OrdinaryToolContent(text=text)
        return replace(execution, result=execution.result.model_copy(update={'content': content}))
    truncation_note = (
        f'（结果集已截断至 {MAX_SEARCH_FILE_IDS} 条）'
        if result.get('truncated') else ''
    )
    next_step = (
        f"可指定 page={result['page'] + 1} 查看下一页，或指定 1 至 {result['total_pages']} 之间的页码重看。"
        if result['has_next'] else f"已到末页；如需重看，请指定 1 至 {result['total_pages']} 之间的页码。"
    )
    template = dedent("""
        ### 文件备忘查看

        状态：查看记录已提交
        查询 ID：{query_id}
        当前页：第 {page} / {total_pages} 页
        匹配条数：{total} 条{truncation_note}
        资料状态：{display_state}
        下一步：{next_step}
    """).strip()
    values = {**result, 'truncation_note': truncation_note}
    feedback = FoldableReceiptText(
        visible=template.format(
            **values,
            display_state='本页文件备忘详情见随后提供的资料。',
            next_step=next_step,
        ),
        hidden=template.format(
            **values,
            display_state='本页正文当前未展示，不能从查看记录推断文件备忘内容。',
            next_step='需要核对时使用此查询 ID 和页码重新查看；查询已失效则重新检索。',
        ),
        unavailable=template.format(
            **values,
            display_state='本页文件备忘资料当前无法读取，不能据此推断内容。',
            next_step='请重新调用 search_file_notes 检索并查看新结果。',
        ),
    )
    content = execution.result.content.model_copy(update={'feedback': feedback})
    return replace(execution, result=execution.result.model_copy(update={'content': content}))


def render_file_note_page(
    items: tuple[FileNote, ...],
    *,
    page: int,
    total_pages: int,
    total: int,
    start_rank: int,
    truncated: bool,
) -> str:
    """将单页文件备忘渲染为可折叠资料正文，隔离外部名称与多行摘要。"""
    header = f'# 文件备忘查询结果（第 {page} / {total_pages} 页，共 {total} 条）'
    if truncated:
        header += f'\n\n> 注意：查询结果已截断至 {MAX_SEARCH_FILE_IDS} 条；如需完整覆盖，请缩小筛选条件。'
    sections = [header]
    for offset, note in enumerate(items):
        rank = start_rank + offset
        section = dedent("""
            ---

            ### {rank}. {name}

            文件 ID：{file_id}
            更新时间（UTC）：{updated_at}
            摘要：
            {summary_quote}
        """).strip().format(
            rank=rank,
            name=_escape_single_line(note.name),
            file_id=note.file_id,
            updated_at=note.updated_at.isoformat(),
            summary_quote=_blockquote(note.summary),
        )
        sections.append(section)
    return '\n\n'.join(sections)


class AddFileNoteArguments(_FileNoteArguments):
    """当你需要为某个文件新增一条备忘记录（保存文件名与关键摘要，便于后续检索）时，使用这个工具。同一文件标识不能重复添加，不会覆盖已有备忘；若需修改已有备忘，请先调用 delete_file_note 删除原记录后再重新添加。"""

    file_id: FileNoteId = Field(
        strict=True,
        description='需要添加备忘的文件标识（1 至 255 字符）；原样使用已知的完整 file_id，不能猜测或编造。',
    )
    name: NoteName = Field(
        strict=True,
        description='文件名称或简短标题（1 至 200 字符），便于后续按名称关键词检索。',
    )
    summary: NoteSummary = Field(
        strict=True,
        description='文件关键内容、结论或用途摘要（UTF-8 编码不超过 65535 字节），便于后续按摘要关键词检索。',
    )


class DeleteFileNoteArguments(_FileNoteArguments):
    """当你需要删除某个文件的备忘记录，或在写入新摘要前清理旧备忘时，使用这个工具。删除操作具有幂等性：目标备忘已不存在时也会正常返回，无需重复处理。"""

    file_id: FileNoteId = Field(
        strict=True,
        description='需要删除备忘的文件标识（1 至 255 字符）；原样使用已有备忘对应的完整 file_id。',
    )


class SearchFileNotesArguments(_FileNoteArguments):
    """当你需要按文件名、摘要关键词或更新时间范围查找已记录的文件备忘时，使用这个工具。有匹配结果时返回 query_id 与总页数，随后使用 view_file_note_query 分页查看详情；未命中时不生成 query_id，无需翻页。传入已有 query_id 可在上次命中范围内进一步缩小筛选条件。"""

    query_id: str | None = Field(
        default=None,
        strict=True,
        min_length=1,
        description='可选的已有查询 ID；传入后仅在该查询命中的文件范围内继续筛选，省略则在全部文件备忘中检索。',
    )
    name_keyword: str | None = Field(
        default=None,
        strict=True,
        description='可选的文件名检索关键词；省略或留空表示不按文件名过滤。',
    )
    summary_keyword: str | None = Field(
        default=None,
        strict=True,
        description='可选的摘要检索关键词；省略或留空表示不按摘要过滤。',
    )
    match_mode: Literal['and', 'or'] = Field(
        default='and',
        description='同时指定文件名和摘要关键词时的组合逻辑：and 表示两者都须匹配，or 表示匹配其一即可；默认 and。',
    )
    start_time: AwareDatetime | None = Field(
        default=None,
        description='可选的更新时间下界（含该时刻），使用带时区偏移的 ISO 8601 格式，例如 2026-11-01T00:00:00+08:00；省略表示不限起始时间。',
    )
    end_time: AwareDatetime | None = Field(
        default=None,
        description='可选的更新时间上界（不含该时刻），使用带时区偏移的 ISO 8601 格式；必须晚于 start_time，省略表示不限结束时间。',
    )

    @model_validator(mode='after')
    def validate_time_window(self):
        if self.start_time is not None and self.end_time is not None and self.end_time <= self.start_time:
            raise ValueError('end_time 必须晚于 start_time')
        return self


class ViewFileNoteQueryArguments(_FileNoteArguments):
    """当你需要分页阅读 search_file_notes 命中的文件备忘详情时，使用这个工具。传入检索返回的 query_id 和目标页码即可展开该页的文件 ID、名称、更新时间与摘要；结果集失效时请重新调用 search_file_notes。"""

    query_id: str = Field(
        strict=True,
        min_length=1,
        description='由 search_file_notes 返回的完整查询 ID；失效时重新检索，不能自行构造或复用已失效的标识。',
    )
    page: int = Field(
        default=1,
        strict=True,
        ge=1,
        description='从 1 开始的页码，默认第 1 页；须在 1 至该查询总页数之间。',
    )


class FileNoteTools:
    """绑定可信 scope 的文件备忘工具集；增删为一次性工具，检索与查看共享 CachePool。"""

    def __init__(
        self,
        store: FileNoteStore,
        scope: str,
        *,
        authorize: FileNoteAuthorizer | None = None,
        cache_pool: CachePool[FileNoteQueryEntry] = DEFAULT_FILE_NOTE_CACHE_POOL,
        page_size: int = 10,
    ):
        if not isinstance(store, FileNoteStore):
            raise TypeError('store 必须实现 FileNoteStore 协议')
        self.scope = validate_note_scope(scope)
        if authorize is not None and not callable(authorize):
            raise TypeError('authorize 必须可调用或为 None')
        if not isinstance(cache_pool, CachePool):
            raise TypeError('cache_pool 必须为 CachePool 实例')
        if type(page_size) is not int or page_size < 1:
            raise ValueError('page_size 必须为正整数')
        self.store = store
        self.authorize = authorize
        self.cache_pool = cache_pool
        self.page_size = page_size
        self._key = secrets.token_bytes(32)
        # 仅记录已提交的精确页面引用及其结果集对象；驱逐或关闭后阻断旧引用解析。
        self._issued: dict[str, tuple[str, FileNoteQueryEntry]] = {}
        self._lock = asyncio.Lock()

    def _sign(self, resource: str, locator: str, nonce: str) -> str:
        return hmac.new(self._key, f'{resource}\n{locator}\n{nonce}'.encode(), hashlib.sha256).hexdigest()

    def _prune_closed(self) -> None:
        stale = [display_id for display_id, (_, entry) in self._issued.items() if entry.closed]
        for display_id in stale:
            self._issued.pop(display_id, None)

    async def add_file_note(self, args: AddFileNoteArguments) -> ToolExecutionResult:
        if self.authorize is not None:
            try:
                decision = self.authorize(args.file_id)
                if inspect.isawaitable(decision):
                    decision = await decision
            except FileNoteRejected as error:
                return ToolExecutionResult(
                    status='failed',
                    failure_mode='correctable',
                    tool_result={
                        'code': error.code,
                        'message': error.reason,
                        'required_action': error.required_action,
                    },
                )
            except Exception as error:
                logger.warning('文件备忘授权校验失败：error_type=%s', type(error).__name__)
                return ToolExecutionResult(
                    status='failed',
                    failure_mode='failed',
                    tool_result={
                        'code': 'file_note_auth_failed',
                        'message': '校验文件访问权限未能完成，本次未写入备忘。',
                        'required_action': '请稍后重试，不要声称备忘已保存。',
                    },
                )
            if not decision:
                return ToolExecutionResult(
                    status='failed',
                    failure_mode='correctable',
                    tool_result={
                        'code': 'file_note_unauthorized',
                        'message': '当前无权为该文件添加备忘，本次未写入。',
                        'required_action': '请核对文件标识是否属于当前可访问范围，勿为无权访问的文件添加备忘。',
                    },
                )
        try:
            note = await self.store.add_file_note(self.scope, args.file_id, args.name, args.summary)
        except FileNoteRejected as error:
            return ToolExecutionResult(
                status='failed',
                failure_mode='correctable',
                tool_result={
                    'code': error.code,
                    'message': error.reason,
                    'required_action': error.required_action,
                },
            )
        except FileNoteWriteUncertain:
            return ToolExecutionResult(
                status='unknown',
                tool_result={
                    'code': 'file_note_write_uncertain',
                    'message': '写入文件备忘在提交阶段结果不明，不能确认是否已保存。',
                    'required_action': '请不要盲目重试写入；请先调用 search_file_notes 回查确认实际状态。',
                },
            )
        except Exception as error:
            logger.warning('添加文件备忘失败：error_type=%s', type(error).__name__)
            return ToolExecutionResult(
                status='failed',
                failure_mode='failed',
                tool_result={
                    'code': 'file_note_add_failed',
                    'message': '添加文件备忘未能完成，本次未写入。',
                    'required_action': '请稍后重试，不要声称备忘已保存。',
                },
            )
        return ToolExecutionResult(
            status='succeeded',
            tool_result={
                'file_id': note.file_id,
                'name': note.name,
                'summary': note.summary,
                'updated_at': note.updated_at.isoformat(),
            },
        )

    async def delete_file_note(self, args: DeleteFileNoteArguments) -> ToolExecutionResult:
        try:
            deleted = await self.store.delete_file_note(self.scope, args.file_id)
        except FileNoteRejected as error:
            return ToolExecutionResult(
                status='failed',
                failure_mode='correctable',
                tool_result={
                    'code': error.code,
                    'message': error.reason,
                    'required_action': error.required_action,
                },
            )
        except FileNoteWriteUncertain:
            return ToolExecutionResult(
                status='unknown',
                tool_result={
                    'code': 'file_note_delete_uncertain',
                    'message': '删除文件备忘在提交阶段结果不明，不能确认是否已删除。',
                    'required_action': '请不要盲目重试；请先调用 search_file_notes 回查确认该备忘是否仍然存在。',
                },
            )
        except Exception as error:
            logger.warning('删除文件备忘失败：error_type=%s', type(error).__name__)
            return ToolExecutionResult(
                status='failed',
                failure_mode='failed',
                tool_result={
                    'code': 'file_note_delete_failed',
                    'message': '删除文件备忘未能完成，当前状态未确认变更。',
                    'required_action': '请稍后重试，不要声称备忘已删除。',
                },
            )
        return ToolExecutionResult(
            status='succeeded',
            tool_result={
                'file_id': args.file_id,
                'deleted': bool(deleted),
            },
        )

    async def search_file_notes(self, args: SearchFileNotesArguments) -> ToolExecutionResult:
        cache = current_tool_cache(self.cache_pool)
        file_ids: list[str] | None = None
        inherited_truncated = False
        if args.query_id is not None:
            parent = cache.get(args.query_id)
            if parent is None or parent.closed:
                # 严禁在 query_id 失效时静默退化为全量检索。
                return ToolExecutionResult(
                    status='failed',
                    failure_mode='correctable',
                    tool_result={
                        'code': 'file_note_query_not_found',
                        'message': '指定的查询 ID 不存在或已失效，本次未执行检索。',
                        'required_action': '请省略 query_id 重新调用 search_file_notes 执行全量检索，不要复用已失效的查询 ID。',
                    },
                )
            file_ids = [item.file_id for item in parent.items]
            inherited_truncated = parent.truncated
        try:
            rows = await self.store.search_file_notes(
                self.scope,
                file_ids=file_ids,
                name_keyword=args.name_keyword,
                summary_keyword=args.summary_keyword,
                match_mode=args.match_mode,
                start_time=args.start_time,
                end_time=args.end_time,
                limit=MAX_SEARCH_LIMIT,
            )
        except FileNoteRejected as error:
            return ToolExecutionResult(
                status='failed',
                failure_mode='correctable',
                tool_result={
                    'code': error.code,
                    'message': error.reason,
                    'required_action': error.required_action,
                },
            )
        except ValueError:
            return ToolExecutionResult(
                status='failed',
                failure_mode='correctable',
                tool_result={
                    'code': 'file_note_search_invalid',
                    'message': '检索条件未通过校验，本次未执行检索。',
                    'required_action': '请检查关键词、匹配方式和时间范围后重新调用 search_file_notes。',
                },
            )
        except Exception as error:
            logger.warning('检索文件备忘失败：error_type=%s', type(error).__name__)
            return ToolExecutionResult(
                status='failed',
                failure_mode='failed',
                tool_result={
                    'code': 'file_note_search_failed',
                    'message': '文件备忘检索未能完成，本次未返回结果。',
                    'required_action': '请稍后重试检索，不要将查询失败当作没有匹配备忘。',
                },
            )
        truncated = inherited_truncated or len(rows) > MAX_SEARCH_FILE_IDS
        rows = rows[:MAX_SEARCH_FILE_IDS]
        if not rows:
            return ToolExecutionResult(
                status='succeeded',
                tool_result={
                    'query_id': None,
                    'total': 0,
                    'total_pages': 0,
                    'truncated': truncated,
                },
            )
        query_id = str(uuid4())
        entry = FileNoteQueryEntry(items=tuple(rows), truncated=truncated)
        # 写入由 CachePool 事务暂存，仅在主循环 journal.commit 成功后原子落入作用域 LRU。
        cache.put(query_id, entry)
        total = len(rows)
        total_pages = (total + self.page_size - 1) // self.page_size
        return ToolExecutionResult(
            status='succeeded',
            tool_result={
                'query_id': query_id,
                'total': total,
                'total_pages': total_pages,
                'truncated': truncated,
            },
        )

    async def view_file_note_query(self, args: ViewFileNoteQueryArguments) -> ToolExecution | ToolExecutionResult:
        async with self._lock:
            cache = current_tool_cache(self.cache_pool)
            entry = cache.get(args.query_id)
            if entry is None or entry.closed:
                return ToolExecutionResult(
                    status='failed',
                    failure_mode='correctable',
                    tool_result={
                        'code': 'file_note_query_not_found',
                        'message': '查询结果不存在或已失效，未返回页面。',
                        'required_action': '请重新调用 search_file_notes 检索并使用新返回的查询 ID 查看。',
                    },
                )
            total = len(entry.items)
            total_pages = (total + self.page_size - 1) // self.page_size
            if not 1 <= args.page <= total_pages:
                return ToolExecutionResult(
                    status='failed',
                    failure_mode='correctable',
                    tool_result={
                        'code': 'file_note_page_out_of_range',
                        'message': f'请求页码 {args.page} 超出结果集范围（共 {total_pages} 页），未返回页面。',
                        'required_action': f'请将 page 指定为 1 至 {total_pages} 之间的整数后重新查看。',
                    },
                )
            resource = 'file-note-query:' + args.query_id
            locator = f'page:{args.page}'
            nonce = str(uuid4())
            reference = ToolPageReference(
                resource_id=resource,
                locator=locator,
                display_id=nonce + '.' + self._sign(resource, locator, nonce),
                media_type='text',
                description=f'文件备忘查询结果，第{args.page}/{total_pages}页',
            )
            metadata = {
                'query_id': args.query_id,
                'page': args.page,
                'total_pages': total_pages,
                'total': total,
                'truncated': entry.truncated,
                'has_next': args.page < total_pages,
            }

            def commit():
                self._prune_closed()
                if not entry.closed:
                    self._issued[reference.display_id] = (args.query_id, entry)

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
            if (reference.media_type != 'text'
                    or not reference.resource_id.startswith('file-note-query:')
                    or prefix != 'page'
                    or not number.isdigit()
                    or int(number) < 1
                    or not hmac.compare_digest(signature, self._sign(reference.resource_id, reference.locator, nonce))):
                raise ValueError('签名或引用格式无效')
        except (ValueError, AttributeError):
            raise ToolPageUnavailable('文件备忘页面引用无效，需要时请重新调用 search_file_notes 检索。') from None

        query_id = reference.resource_id.removeprefix('file-note-query:')
        issued = self._issued.get(reference.display_id)
        if issued is None:
            raise ToolPageUnavailable('文件备忘页面已不可读，需要时请重新调用 search_file_notes 检索。')
        issued_query_id, entry = issued
        if entry.closed or issued_query_id != query_id:
            if entry.closed:
                self._issued.pop(reference.display_id, None)
            raise ToolPageUnavailable('文件备忘查询结果已失效，需要时请重新调用 search_file_notes 检索。')

        page = int(number)
        total = len(entry.items)
        total_pages = (total + self.page_size - 1) // self.page_size
        if not 1 <= page <= total_pages:
            raise ToolPageUnavailable('文件备忘页码超出范围，需要时请重新调用 search_file_notes 检索。')
        start = (page - 1) * self.page_size
        page_items = entry.items[start:start + self.page_size]
        text = render_file_note_page(
            page_items,
            page=page,
            total_pages=total_pages,
            total=total,
            start_rank=start + 1,
            truncated=entry.truncated,
        )
        return [{'type': 'text', 'text': text}]

    def registrations(self) -> tuple[RegisteredTool, ...]:
        noop_prepare = lambda name, raw, workspace, args: workspace
        return (
            RegisteredTool(
                'add_file_note',
                AddFileNoteArguments.__doc__,
                AddFileNoteArguments,
                noop_prepare,
                executor=self.add_file_note,
                hooks=(FeedbackOnlyHook(render_add_file_note_feedback),),
            ),
            RegisteredTool(
                'delete_file_note',
                DeleteFileNoteArguments.__doc__,
                DeleteFileNoteArguments,
                noop_prepare,
                executor=self.delete_file_note,
                hooks=(FeedbackOnlyHook(render_delete_file_note_feedback),),
            ),
            RegisteredTool(
                'search_file_notes',
                SearchFileNotesArguments.__doc__,
                SearchFileNotesArguments,
                noop_prepare,
                executor=self.search_file_notes,
                progress=ToolProgress(type='thinking', message='正在检索文件备忘'),
                hooks=(FeedbackOnlyHook(render_search_file_notes_feedback),),
                read_pools=(self.cache_pool,),
                write_pools=(self.cache_pool,),
            ),
            RegisteredTool(
                'view_file_note_query',
                ViewFileNoteQueryArguments.__doc__ + f'每页最多展示 {self.page_size} 条。',
                ViewFileNoteQueryArguments,
                noop_prepare,
                executor=self.view_file_note_query,
                return_types=('ordinary', 'foldable'),
                page_resolver=self.resolve_page,
                progress=ToolProgress(type='thinking', message='正在查看文件备忘'),
                hooks=(FeedbackOnlyHook(render_view_file_note_query_feedback),),
                read_pools=(self.cache_pool,),
            ),
        )


def build_file_note_registrations(
    store: FileNoteStore,
    scope: str,
    *,
    authorize: FileNoteAuthorizer | None = None,
    cache_pool: CachePool[FileNoteQueryEntry] = DEFAULT_FILE_NOTE_CACHE_POOL,
    page_size: int = 10,
) -> tuple[RegisteredTool, ...]:
    """构建文件备忘四个内置工具的注册项集合。"""
    return FileNoteTools(
        store,
        scope,
        authorize=authorize,
        cache_pool=cache_pool,
        page_size=page_size,
    ).registrations()
