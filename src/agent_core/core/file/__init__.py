"""通用 PDF 文档模型与基于 CachePool 的逐页查看器。"""
from .pdf import (
    DEFAULT_PDF_CACHE_POOL,
    PDFDocument,
    PDFOpenError,
    PDFPage,
    PDFRenderConfig,
    PDFSource,
    create_pdf_cache_pool,
)
from .pdf_viewer import PDFViewError, PDFViewer

__all__ = [
    'DEFAULT_PDF_CACHE_POOL',
    'PDFDocument',
    'PDFOpenError',
    'PDFPage',
    'PDFRenderConfig',
    'PDFSource',
    'PDFViewError',
    'PDFViewer',
    'create_pdf_cache_pool',
]
