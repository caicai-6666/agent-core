"""内存 PDF 文档模型与视觉预算内 PNG 渲染；按需懒加载 pymupdf，所有 MuPDF 操作共用进程锁。"""
from __future__ import annotations

from dataclasses import dataclass
from math import ceil, isfinite
from os import PathLike
from pathlib import Path
from typing import Any

from agent_core.core.tool.cache import CachePool, CacheScopeKind
from agent_core.infrastructure.pdf_lock import serialized_pdf_operation

_PDF_EXTRA_INSTALL_HINT = '缺少 PDF 渲染依赖 pymupdf，请先执行 `pip install "agent-core[pdf]"` 安装可选依赖。'


def _import_pymupdf() -> Any:
    """按需导入 pymupdf，避免未安装 agent-core[pdf] 时阻断库导入。"""
    try:
        import pymupdf
    except ImportError as error:
        raise ImportError(_PDF_EXTRA_INSTALL_HINT) from error
    return pymupdf


class PDFOpenError(ValueError):
    """只携带可公开的格式失败，不传播底层 PDF 解析异常。"""


@dataclass(frozen=True)
class PDFRenderConfig:
    """PDF 页面渲染的缩放比例与视觉 token 预算配置。"""
    max_scale: float = 2.0
    patch_size: int = 32
    max_visual_tokens: int = 8192

    def __post_init__(self) -> None:
        if not isfinite(self.max_scale) or self.max_scale <= 0:
            raise ValueError('渲染比例必须为有限正数')
        for value in (self.patch_size, self.max_visual_tokens):
            if type(value) is not int or value < 1:
                raise ValueError('视觉预算参数必须为正整数')


@dataclass(frozen=True)
class PDFPage:
    """单页已渲染的不可变 PNG 数据及视觉 token 统计。"""
    number: int
    png: bytes
    width: int
    height: int
    visual_tokens: int


@dataclass(frozen=True)
class PDFSource:
    """通用 PDF 来源描述；支持内存字节或本地路径，并可选携带展示文件名。"""
    content: bytes | None = None
    path: Path | str | None = None
    name: str | None = None

    def __post_init__(self) -> None:
        has_content = self.content is not None
        has_path = self.path is not None
        if has_content == has_path:
            raise ValueError('PDFSource 必须且只能提供 content 或 path 之一')
        if has_content and (not isinstance(self.content, bytes) or not self.content):
            raise PDFOpenError('PDF 内容为空或无效')
        if has_path:
            if isinstance(self.path, str):
                if not self.path.strip():
                    raise ValueError('PDF 文件路径不能为空')
                object.__setattr__(self, 'path', Path(self.path))
            elif isinstance(self.path, PathLike):
                object.__setattr__(self, 'path', Path(self.path))
            else:
                raise TypeError('PDF 文件路径必须为 Path 或字符串')
        if self.name is not None and not isinstance(self.name, str):
            raise TypeError('PDF 展示名称必须为字符串或 None')


def _tokens(width: int, height: int, config: PDFRenderConfig) -> int:
    return ceil(width / config.patch_size) * ceil(height / config.patch_size)


def _load_source_bytes(source: bytes | Path | PathLike[str] | PDFSource) -> tuple[bytes, str | None]:
    if isinstance(source, PDFSource):
        if source.content is not None:
            return source.content, source.name
        assert source.path is not None
        path = Path(source.path)
        return path.read_bytes(), source.name or path.name
    if isinstance(source, bytes):
        return source, None
    if isinstance(source, PathLike):
        path = Path(source)
        return path.read_bytes(), path.name
    raise TypeError('PDF 来源必须为 bytes、Path 或 PDFSource')


class PDFDocument:
    """持有已打开 PDF 文档与按需缓存的不可变页面；不保存 Base64、用户身份或阅读位置。"""

    @serialized_pdf_operation
    def __init__(
        self,
        source: bytes | Path | PathLike[str] | PDFSource,
        *,
        config: PDFRenderConfig | None = None,
        name: str | None = None,
    ) -> None:
        self._document: Any = None
        self._pages: dict[int, PDFPage] = {}
        self.config = config or PDFRenderConfig()
        if not isinstance(self.config, PDFRenderConfig):
            raise TypeError('页面配置不合法')
        if name is not None and not isinstance(name, str):
            raise TypeError('PDF 展示名称必须为字符串或 None')

        # 先检查可选依赖，确保缺失 pymupdf 时明确提示安装 agent-core[pdf]。
        pymupdf = _import_pymupdf()
        content, source_name = _load_source_bytes(source)
        self.name: str | None = name if name is not None else source_name
        if not isinstance(content, bytes) or not content:
            raise PDFOpenError('PDF 内容为空或无效')

        try:
            document = pymupdf.open(stream=content)
            try:
                if not document.is_pdf:
                    raise PDFOpenError('文件实际格式不是有效的 PDF 文档')
                if document.needs_pass:
                    raise PDFOpenError('PDF 文件需要打开密码，当前不支持读取加密文件')
                if document.page_count < 1:
                    raise PDFOpenError('PDF 文件不包含任何页面')
                self.page_count: int = document.page_count
            except BaseException:
                document.close()
                raise
            self._document = document
        except PDFOpenError:
            raise
        except (pymupdf.FileDataError, RuntimeError, ValueError):
            raise PDFOpenError('PDF 文件损坏或无法解析') from None

    @property
    @serialized_pdf_operation
    def closed(self) -> bool:
        return self._document is None

    @property
    @serialized_pdf_operation
    def cached_pages(self) -> tuple[int, ...]:
        return tuple(sorted(self._pages))

    @serialized_pdf_operation
    def get_page(self, number: int) -> PDFPage:
        if self._document is None:
            raise RuntimeError('PDF 文档已关闭')
        if type(number) is not int or not 1 <= number <= self.page_count:
            raise ValueError('页码超出范围')
        if number in self._pages:
            return self._pages[number]
        pymupdf = _import_pymupdf()
        page = self._document[number - 1]
        width, height = page.rect.width, page.rect.height
        if not all(isfinite(value) and value > 0 for value in (width, height)):
            raise ValueError('页面尺寸不合法')
        config = self.config

        def fits(scale: float) -> bool:
            return _tokens(ceil(width * scale), ceil(height * scale), config) <= config.max_visual_tokens

        scale = config.max_scale
        if not fits(scale):
            lower, upper = 0.0, scale
            for _ in range(64):
                middle = (lower + upper) / 2
                if fits(middle):
                    lower = middle
                else:
                    upper = middle
            scale = lower * (1 - 1e-6)
        if scale <= 0:
            raise ValueError('页面无法在视觉预算内渲染')
        pixmap = page.get_pixmap(matrix=pymupdf.Matrix(scale, scale), alpha=False)
        tokens = _tokens(pixmap.width, pixmap.height, config)
        if tokens > config.max_visual_tokens:
            raise ValueError('实际页面渲染超出视觉预算')
        result = PDFPage(number, pixmap.tobytes('png'), pixmap.width, pixmap.height, tokens)
        self._pages[number] = result
        return result

    @serialized_pdf_operation
    def close(self) -> None:
        if self._document is not None:
            self._document.close()
            self._document = None
        self._pages.clear()

    def __enter__(self) -> PDFDocument:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()


def _close_pdf_document(_key: str, document: PDFDocument) -> None:
    document.close()


def create_pdf_cache_pool(
    name: str = 'pdf_documents',
    *,
    capacity: int = 4,
    scope: CacheScopeKind = 'session',
) -> CachePool[PDFDocument]:
    """创建带自动关闭驱逐回调的 PDF 文档缓存池规格。"""
    return CachePool(name, capacity=capacity, scope=scope, on_evict=_close_pdf_document)


DEFAULT_PDF_CACHE_POOL: CachePool[PDFDocument] = create_pdf_cache_pool()
