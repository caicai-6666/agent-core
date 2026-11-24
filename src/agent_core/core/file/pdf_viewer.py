"""通用 PDF 查看器：集成框架 CachePool、按需授权校验与独立阅读游标。"""
from __future__ import annotations

import asyncio
import base64
from collections.abc import Awaitable, Callable
from inspect import isawaitable
from os import PathLike
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from ..model import AgentDependencyError
from ..tool.cache import CachePool, current_tool_cache
from ..tool.result import (
    FoldableToolContent,
    ToolExecution,
    ToolExecutionResult,
    ToolPageReference,
    ToolPageUnavailable,
)
from .pdf import DEFAULT_PDF_CACHE_POOL, PDFDocument, PDFOpenError, PDFRenderConfig, PDFSource

PDFLoadResult = bytes | Path | PathLike[str] | PDFSource | PDFDocument
PDFLoader = Callable[[str], PDFLoadResult | Awaitable[PDFLoadResult]]
PDFAuthorizer = Callable[[str], Any]
PDFFileNameResolver = Callable[[str], str | None | Awaitable[str | None]]
FailureMode = Literal['correctable', 'failed', 'dependency']


class PDFViewError(ValueError):
    """PDF 查看失败；保留稳定原因码、已知状态与可执行修正方向。"""

    def __init__(
        self,
        code: str,
        reason: str,
        required_action: str,
        *,
        failure_mode: FailureMode = 'correctable',
    ) -> None:
        super().__init__(reason)
        self.code = code
        self.reason = reason
        self.required_action = required_action
        self.failure_mode: FailureMode = failure_mode


async def _maybe_await(value: Any) -> Any:
    return await value if isawaitable(value) else value


class PDFViewer:
    """通用 PDF 逐页查看器。

    支持两种协作模式：
    1. 提供 ``load_pdf``：命中 ``CachePool`` 直接复用已打开文档，未命中时加载并写入缓存池；
    2. 不提供 ``load_pdf``：只读消费前置工具写入同一 ``CachePool`` 的 ``PDFDocument``。
    """

    def __init__(
        self,
        *,
        load_pdf: PDFLoader | None = None,
        pool: CachePool[PDFDocument] | None = None,
        authorize: PDFAuthorizer | None = None,
        file_name: PDFFileNameResolver | None = None,
        config: PDFRenderConfig | None = None,
    ) -> None:
        if load_pdf is not None and not callable(load_pdf):
            raise TypeError('load_pdf 必须可调用或为 None')
        if pool is not None and not isinstance(pool, CachePool):
            raise TypeError('pool 必须为 CachePool 实例或 None')
        if authorize is not None and not callable(authorize):
            raise TypeError('authorize 必须可调用或为 None')
        if file_name is not None and not callable(file_name):
            raise TypeError('file_name 必须可调用或为 None')
        if config is not None and not isinstance(config, PDFRenderConfig):
            raise TypeError('config 必须为 PDFRenderConfig 实例或 None')

        self.load_pdf = load_pdf
        self.pool: CachePool[PDFDocument] = pool if pool is not None else DEFAULT_PDF_CACHE_POOL
        self.authorize = authorize
        self._file_name = file_name
        self.config = config or PDFRenderConfig()

        # 阅读游标与已发布页面引用按 viewer 实例隔离，仅在 after_commit 确认后推进。
        self._positions: dict[str, int] = {}
        self._pages: dict[str, tuple[ToolPageReference, str, int, PDFDocument]] = {}
        self._lock = asyncio.Lock()
        self._closed = False

    @property
    def read_pools(self) -> tuple[CachePool[PDFDocument], ...]:
        return (self.pool,)

    @property
    def write_pools(self) -> tuple[CachePool[PDFDocument], ...]:
        return (self.pool,) if self.load_pdf is not None else ()

    @property
    def has_file_name(self) -> bool:
        return self._file_name is not None

    def _require_open(self) -> None:
        if self._closed:
            raise PDFViewError(
                'viewer_closed',
                '当前 PDF 查看会话已结束，无法继续读取页面。',
                '请在有效任务中重新发起查看。',
                failure_mode='dependency',
            )

    @staticmethod
    def _validate_file_id(file_id: Any) -> str:
        if not isinstance(file_id, str) or not file_id.strip():
            raise PDFViewError(
                'invalid_arguments',
                '文件标识必须为非空字符串。',
                '请填写上下文或前置工具返回的完整非空文件标识。',
            )
        return file_id

    async def _run_authorize(self, file_id: str) -> None:
        if self.authorize is None:
            return
        decision = await _maybe_await(self.authorize(file_id))
        self._require_open()
        if decision is False:
            raise PDFViewError(
                'file_unavailable',
                '目标 PDF 文件不存在或当前无权访问。',
                '请核对文件标识及访问权限后重新选择可读取的文件。',
            )

    async def get_file_name(self, file_id: str) -> str | None:
        self._require_open()
        file_id = self._validate_file_id(file_id)
        if self._file_name is None:
            return None
        name = await _maybe_await(self._file_name(file_id))
        self._require_open()
        if isinstance(name, str) and name.strip():
            return name
        return None

    async def _open_document(self, file_id: str) -> PDFDocument:
        assert self.load_pdf is not None
        loaded = await _maybe_await(self.load_pdf(file_id))
        self._require_open()
        if isinstance(loaded, PDFDocument):
            if loaded.closed:
                raise PDFOpenError('加载得到的 PDF 文档已关闭，无法读取')
            return loaded
        if isinstance(loaded, (bytes, PathLike, PDFSource)):
            return await asyncio.to_thread(PDFDocument, loaded, config=self.config)
        raise PDFOpenError('未取得有效的 PDF 文件内容')

    @staticmethod
    def _page_description(file_id: str, target: int, count: int, file_name: str | None = None) -> str:
        lines = ['文件类型: PDF']
        if file_name:
            lines.append(f'文件名称: {file_name}')
        lines.extend([
            f'文件ID: {file_id}',
            f'页码: {target}',
            f'总页数: {count}',
        ])
        return '\n'.join(lines)

    @staticmethod
    def _map_error(error: Exception) -> PDFViewError:
        if isinstance(error, (AgentDependencyError, ImportError)):
            raise error
        if isinstance(error, PDFViewError):
            return error
        if isinstance(error, (PermissionError, LookupError, FileNotFoundError)):
            return PDFViewError(
                'file_unavailable',
                '目标 PDF 文件不存在或当前无权访问。',
                '请核对文件标识，或改用当前任务可访问的 PDF 文件标识。',
            )
        if isinstance(error, PDFOpenError):
            return PDFViewError(
                'invalid_file',
                str(error),
                '请确认目标文件是未加密且未损坏的有效 PDF 文档。',
                failure_mode='failed',
            )
        if isinstance(error, (ValueError, TypeError)):
            return PDFViewError(
                'invalid_arguments',
                'PDF 查看参数未通过校验。',
                '请核对文件标识与从 1 开始的整数页码后重试。',
            )
        return PDFViewError(
            'read_failed',
            'PDF 文件暂时无法读取，本次未返回页面。',
            '请稍后重试查看；若持续失败，请说明文件读取受限。',
            failure_mode='failed',
        )

    async def view(self, file_id: str, page_number: int | None = None) -> ToolExecution:
        async with self._lock:
            newly_opened: PDFDocument | None = None
            try:
                self._require_open()
                file_id = self._validate_file_id(file_id)
                if page_number is not None and (type(page_number) is not int or page_number < 1):
                    raise PDFViewError(
                        'invalid_arguments',
                        '页码必须为从 1 开始的正整数；省略表示继续查看下一页。',
                        '请填写大于等于 1 的整数页码，或省略 page_number 查看下一页。',
                    )

                # 每次查看均执行授权校验，即使命中缓存也不能绕过最新权限。
                await self._run_authorize(file_id)

                cache = current_tool_cache(self.pool)
                cached = cache.get(file_id)
                if cached is not None and not isinstance(cached, PDFDocument):
                    raise PDFOpenError('缓存池中对应对象不是有效的 PDFDocument')

                if cached is not None and not cached.closed:
                    document = cached
                else:
                    if self.load_pdf is None:
                        raise PDFViewError(
                            'file_not_found',
                            '目标 PDF 文件尚未加载到当前缓存池，或已因容量限制被释放。',
                            '请核对文件标识，或先调用加载工具准备该 PDF 文件后再查看。',
                        )
                    newly_opened = await self._open_document(file_id)
                    document = newly_opened

                target = page_number if page_number is not None else self._positions.get(file_id, 0) + 1
                if target > document.page_count:
                    if page_number is None:
                        raise PDFViewError(
                            'end_of_file',
                            f'该 PDF 文件已到最后一页（共 {document.page_count} 页），未继续翻页。',
                            f'如需重看，请明确指定 1 至 {document.page_count} 之间的 page_number。',
                        )
                    raise PDFViewError(
                        'page_out_of_range',
                        f'请求页码 {target} 超出范围，该 PDF 文件共 {document.page_count} 页。',
                        f'请填写 1 至 {document.page_count} 之间的整数页码。',
                    )

                # 在返回前完成目标页渲染，确保越界或损坏页面不会提前写入缓存池或推进游标。
                await asyncio.to_thread(document.get_page, target)

                # 加载并渲染后再次确认查看器仍开放，并将新打开文档暂存到缓存池（随提交生效）。
                self._require_open()
                if newly_opened is not None:
                    cache.put(file_id, newly_opened)
                    newly_opened = None

                display_name = document.name
                if display_name is None and self._file_name is not None:
                    try:
                        display_name = await self.get_file_name(file_id)
                    except AgentDependencyError:
                        raise
                    except Exception:
                        display_name = None

                reference = ToolPageReference(
                    resource_id=file_id,
                    display_id=str(uuid4()),
                    locator=f'page:{target}',
                    media_type='image',
                    description=self._page_description(file_id, target, document.page_count, display_name),
                )

                def committed() -> None:
                    self._require_open()
                    self._positions[file_id] = target
                    self._pages[reference.display_id] = (reference, file_id, target, document)

                payload: dict[str, Any] = {
                    'status': 'success',
                    'file_id': file_id,
                    'page_number': target,
                    'page_count': document.page_count,
                    'has_next': target < document.page_count,
                }
                if display_name:
                    payload['file_name'] = display_name

                return ToolExecution(
                    ToolExecutionResult(
                        status='succeeded',
                        tool_result=payload,
                        content=FoldableToolContent(pages=[reference]),
                    ),
                    after_commit=committed,
                )
            except Exception as error:
                if newly_opened is not None:
                    newly_opened.close()
                mapped = self._map_error(error)
                return ToolExecution(
                    ToolExecutionResult(
                        status='failed',
                        failure_mode=mapped.failure_mode,
                        tool_result={
                            'status': 'error',
                            'code': mapped.code,
                            'reason': mapped.reason,
                            'required_action': mapped.required_action,
                        },
                    )
                )

    async def resolve_page(self, reference: ToolPageReference) -> list[dict[str, Any]]:
        async with self._lock:
            try:
                self._require_open()
                validated = ToolPageReference.model_validate(reference)
                issued = self._pages.get(validated.display_id)
                if issued is None or issued[0] != validated:
                    raise ToolPageUnavailable('PDF 页面引用无效，请重新调用查看工具。')
                _, file_id, number, document = issued

                # 展示期仍须重新执行授权校验，避免权限撤销后继续回放缓存图像。
                await self._run_authorize(file_id)

                if not document.closed:
                    page = await asyncio.to_thread(document.get_page, number)
                elif self.load_pdf is not None:
                    temp_doc = await self._open_document(file_id)
                    try:
                        if number > temp_doc.page_count:
                            raise ToolPageUnavailable('PDF 页数已变化，原页面引用不可读取。')
                        page = await asyncio.to_thread(temp_doc.get_page, number)
                    finally:
                        temp_doc.close()
                else:
                    raise ToolPageUnavailable('PDF 文档已从缓存释放，当前页面不可读取。')

                encoded = await asyncio.to_thread(lambda: base64.b64encode(page.png).decode('ascii'))
                return [{'type': 'image_url', 'image_url': {'url': 'data:image/png;base64,' + encoded}}]
            except AgentDependencyError:
                raise
            except ToolPageUnavailable:
                raise
            except Exception:
                raise ToolPageUnavailable('PDF 页面当前不可读取，请核对访问权限或重新查看。') from None

    async def aclose(self) -> None:
        async with self._lock:
            self._closed = True
            self._positions.clear()
            self._pages.clear()
