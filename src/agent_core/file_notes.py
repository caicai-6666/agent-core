"""文件备注领域模型、缓存池规格与存储协议；供适配器与工具层共享。"""
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Annotated, Literal, Protocol, runtime_checkable

from pydantic import AfterValidator, AwareDatetime, BaseModel, ConfigDict, StringConstraints

from .core.tool.cache import CachePool, CacheScopeKind

MAX_FILE_NOTE_ID_LENGTH = 255
MAX_NOTE_NAME_LENGTH = 200
MAX_NOTE_SUMMARY_BYTES = 65535
MAX_SEARCH_FILE_IDS = 1000
MAX_SEARCH_LIMIT = 1001


def _validate_summary_bytes(value: str) -> str:
    # 与 MySQL TEXT 上限保持一致，按 UTF-8 字节数限制摘要大小。
    if len(value.encode('utf-8')) > MAX_NOTE_SUMMARY_BYTES:
        raise ValueError(f'文件备注摘要 UTF-8 编码长度不能超过 {MAX_NOTE_SUMMARY_BYTES} 字节')
    return value


FileNoteId = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=MAX_FILE_NOTE_ID_LENGTH),
]
NoteName = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=MAX_NOTE_NAME_LENGTH),
]
NoteSummary = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1),
    AfterValidator(_validate_summary_bytes),
]


class FileNote(BaseModel):
    """单条文件备注记录；不可变且禁止未声明字段。"""
    model_config = ConfigDict(frozen=True, extra='forbid')

    file_id: FileNoteId
    name: NoteName
    summary: NoteSummary
    updated_at: AwareDatetime


class FileNoteRejected(ValueError):
    """文件备注业务冲突或约束拒绝；携带就近校验提供的定向反馈字段。"""

    def __init__(self, reason: str, required_action: str, *, code: str = 'file_note_rejected'):
        super().__init__(reason)
        self.reason = reason
        self.required_action = required_action
        self.code = code


class FileNoteWriteUncertain(RuntimeError):
    """文件备注写入进入提交阶段后结果不明；调用方不能盲重试，须先回查确认。"""


@dataclass
class FileNoteQueryEntry:
    """存入 CachePool 的文件备注检索结果集；驱逐或作用域关闭时标记失效。"""
    items: tuple[FileNote, ...]
    truncated: bool = False
    closed: bool = False

    def close(self) -> None:
        """在缓存驱逐或作用域关闭时标记失效，阻断旧分页引用继续访问。"""
        self.closed = True


def _close_file_note_entry(_key: str, entry: FileNoteQueryEntry) -> None:
    entry.close()


def create_file_note_cache_pool(
    name: str = 'file_note_search_results',
    capacity: int = 10,
    scope: CacheScopeKind = 'task',
) -> CachePool[FileNoteQueryEntry]:
    """创建文件备注检索结果集缓存池规格；驱逐或作用域关闭时自动使结果集失效。"""
    return CachePool(name=name, capacity=capacity, scope=scope, on_evict=_close_file_note_entry)


DEFAULT_FILE_NOTE_CACHE_POOL: CachePool[FileNoteQueryEntry] = create_file_note_cache_pool()


def validate_note_scope(scope: str) -> str:
    """校验备注隔离作用域为非空字符串。"""
    if not isinstance(scope, str) or not scope.strip():
        raise ValueError('scope 必须为非空字符串')
    return scope


def validate_file_note_id(file_id: str) -> str:
    """校验并去除首尾空白后的文件标识；不限制为 UUID。"""
    if not isinstance(file_id, str):
        raise ValueError('file_id 必须为字符串')
    cleaned = file_id.strip()
    if not 1 <= len(cleaned) <= MAX_FILE_NOTE_ID_LENGTH:
        raise ValueError(f'file_id 去除首尾空白后长度必须在 1 至 {MAX_FILE_NOTE_ID_LENGTH} 字符之间')
    return cleaned


def ensure_aware_utc(value: datetime, *, field_name: str = 'updated_at') -> datetime:
    """校验时间为带时区的 datetime 并转换为 UTC。"""
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f'{field_name} 必须为包含时区信息的 datetime')
    return value.astimezone(timezone.utc)


def format_note_timestamp(value: datetime) -> str:
    """格式化为固定宽度 UTC ISO-8601 字符串，保证跨数据库字典序与时间序一致。"""
    utc = ensure_aware_utc(value)
    return (
        f'{utc.year:04d}-{utc.month:02d}-{utc.day:02d}'
        f'T{utc.hour:02d}:{utc.minute:02d}:{utc.second:02d}.{utc.microsecond:06d}+00:00'
    )


def parse_note_timestamp(value: str) -> datetime:
    """解析存储层的规范 UTC ISO-8601 时间戳。"""
    return ensure_aware_utc(datetime.fromisoformat(value))


def build_file_note(
    file_id: str,
    name: str,
    summary: str,
    *,
    updated_at: datetime | None = None,
) -> tuple[FileNote, str]:
    """校验写入参数并生成规范化 FileNote 与固定宽度 UTC 时间戳字符串。"""
    timestamp = datetime.now(timezone.utc) if updated_at is None else ensure_aware_utc(updated_at)
    note = FileNote(file_id=file_id, name=name, summary=summary, updated_at=timestamp)
    return note, format_note_timestamp(note.updated_at)


def normalize_search_params(
    scope: str,
    *,
    file_ids: Sequence[str] | None = None,
    name_keyword: str | None = None,
    summary_keyword: str | None = None,
    match_mode: Literal['and', 'or'] = 'and',
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    limit: int = MAX_SEARCH_LIMIT,
) -> tuple[str, list[str] | None, str | None, str | None, Literal['and', 'or'], str | None, str | None, int]:
    """统一校验并规范化备注检索条件，供 SQLite 与 MySQL 适配器共用。"""
    validated_scope = validate_note_scope(scope)
    if match_mode not in ('and', 'or'):
        raise ValueError("match_mode 必须为 'and' 或 'or'")
    if type(limit) is not int or not 1 <= limit <= MAX_SEARCH_LIMIT:
        raise ValueError(f'limit 必须为 1 至 {MAX_SEARCH_LIMIT} 之间的整数')

    start_dt = ensure_aware_utc(start_time, field_name='start_time') if start_time is not None else None
    end_dt = ensure_aware_utc(end_time, field_name='end_time') if end_time is not None else None
    if start_dt is not None and end_dt is not None and end_dt <= start_dt:
        raise ValueError('end_time 必须晚于 start_time')

    normalized_ids: list[str] | None = None
    if file_ids is not None:
        if isinstance(file_ids, (str, bytes)):
            raise ValueError('file_ids 必须为字符串序列，不能直接传入单个字符串')
        raw_ids = list(file_ids)
        if len(raw_ids) > MAX_SEARCH_FILE_IDS:
            raise ValueError(f'file_ids 数量不能超过 {MAX_SEARCH_FILE_IDS}')
        normalized_ids = list(dict.fromkeys(validate_file_note_id(item) for item in raw_ids))

    if name_keyword is not None and not isinstance(name_keyword, str):
        raise ValueError('name_keyword 必须为字符串或 None')
    if summary_keyword is not None and not isinstance(summary_keyword, str):
        raise ValueError('summary_keyword 必须为字符串或 None')
    cleaned_name = name_keyword.strip() if name_keyword is not None else None
    cleaned_summary = summary_keyword.strip() if summary_keyword is not None else None

    return (
        validated_scope,
        normalized_ids,
        cleaned_name or None,
        cleaned_summary or None,
        match_mode,
        format_note_timestamp(start_dt) if start_dt is not None else None,
        format_note_timestamp(end_dt) if end_dt is not None else None,
        limit,
    )


@runtime_checkable
class FileNoteStore(Protocol):
    """文件备注存储协议；全部操作必须严格按 scope 隔离。"""

    async def add_file_note(
        self,
        scope: str,
        file_id: str,
        name: str,
        summary: str,
        *,
        updated_at: datetime | None = None,
    ) -> FileNote: ...

    async def delete_file_note(self, scope: str, file_id: str) -> bool: ...

    async def search_file_notes(
        self,
        scope: str,
        *,
        file_ids: Sequence[str] | None = None,
        name_keyword: str | None = None,
        summary_keyword: str | None = None,
        match_mode: Literal['and', 'or'] = 'and',
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int = MAX_SEARCH_LIMIT,
    ) -> list[FileNote]: ...
