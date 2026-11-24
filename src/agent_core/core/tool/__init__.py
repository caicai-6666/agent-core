"""主助手工具公共入口；按能力拆分定义，通过注册表统一组合。"""
from .registry import RegisteredTool, ToolRegistry, build_registry, build_builtin_registrations, check_descriptions
from .base import FinishTaskTool, OutputArguments
from .base_tool import BaseTool
from .cache import CachePool, CacheScope, ReadOnlyCache, WriteOnlyCache, ReadWriteCache, current_tool_cache
from .context import CompactArguments, build_context_registrations
from .file_candidates import FileCandidateViewer, build_file_candidates_registration, render_file_candidates_feedback
from .file_notes import (
    FILE_NOTE_TOOL_NAMES,
    AddFileNoteArguments,
    DeleteFileNoteArguments,
    FileNoteTools,
    SearchFileNotesArguments,
    ViewFileNoteQueryArguments,
    build_file_note_registrations,
    render_add_file_note_feedback,
    render_delete_file_note_feedback,
    render_search_file_notes_feedback,
    render_view_file_note_query_feedback,
)
from .file_search import build_file_search_registration, render_file_search_feedback
from .hooks import ToolHook, ToolHookContext
from .memory_search import build_memory_search_registration, render_memory_search_feedback
from .memory_viewer import MemoryQueryViewer, build_memory_view_registration, render_memory_view_feedback
from .pdf_viewer import ViewPDFArguments, build_pdf_view_registration, render_pdf_feedback
from .progress import ToolProgress
from .result import (
    ToolExecution,
    ToolExecutionResult,
    ToolGuidance,
    ToolPageReference,
    OrdinaryToolContent,
    FoldableToolContent,
    ToolContent,
    ToolContentType,
)
from .workspace import (
    ExplorationResult,
    WorkspaceAddArguments,
    WorkspaceReplaceArguments,
    WorkspaceDeleteArguments,
    WorkspaceEdit,
    WorkspacePath,
    prepare_workspace_edit,
    parse_workspace_tool_arguments,
    build_workspace_registrations,
)

__all__ = [
    'ToolProgress',
    'ViewPDFArguments',
    'build_pdf_view_registration',
    'render_pdf_feedback',
    'build_file_search_registration',
    'render_file_search_feedback',
    'FileCandidateViewer',
    'build_file_candidates_registration',
    'render_file_candidates_feedback',
    'FILE_NOTE_TOOL_NAMES',
    'AddFileNoteArguments',
    'DeleteFileNoteArguments',
    'SearchFileNotesArguments',
    'ViewFileNoteQueryArguments',
    'FileNoteTools',
    'build_file_note_registrations',
    'render_add_file_note_feedback',
    'render_delete_file_note_feedback',
    'render_search_file_notes_feedback',
    'render_view_file_note_query_feedback',
    'build_memory_search_registration',
    'render_memory_search_feedback',
    'MemoryQueryViewer',
    'build_memory_view_registration',
    'render_memory_view_feedback',
    'RegisteredTool',
    'ToolRegistry',
    'build_registry',
    'check_descriptions',
    'OutputArguments',
    'CompactArguments',
    'ExplorationResult',
    'WorkspaceAddArguments',
    'WorkspaceReplaceArguments',
    'WorkspaceDeleteArguments',
    'WorkspaceEdit',
    'WorkspacePath',
    'prepare_workspace_edit',
    'parse_workspace_tool_arguments',
    'build_workspace_registrations',
    'FinishTaskTool',
    'build_builtin_registrations',
    'build_context_registrations',
    'ToolHook',
    'ToolHookContext',
    'ToolExecution',
    'ToolExecutionResult',
    'ToolGuidance',
    'ToolPageReference',
    'OrdinaryToolContent',
    'FoldableToolContent',
    'ToolContent',
    'ToolContentType',
    'BaseTool',
    'CachePool',
    'CacheScope',
    'ReadOnlyCache',
    'WriteOnlyCache',
    'ReadWriteCache',
    'current_tool_cache',
]
