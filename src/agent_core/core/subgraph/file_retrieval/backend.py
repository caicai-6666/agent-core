"""通用文件检索后端协议与数据结构；权限、租户与底层存储由业务侧封装。"""
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
import math
from types import MappingProxyType
from typing import Literal, Protocol, runtime_checkable

FileSearchField = Literal['name', 'summary', 'tag']


@dataclass(frozen=True)
class FileSearchRecord:
    """后端返回的通用文件候选元信息，不绑定特定数据库或平台表结构。"""
    file_id: str
    name: str
    summary: str | None = None
    tags: tuple[str, ...] = ()
    metadata: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self):
        if not isinstance(self.file_id, str) or not self.file_id.strip():
            raise ValueError('文件 ID 必须为非空字符串')
        if not isinstance(self.name, str):
            raise TypeError('文件名称必须为字符串')
        if self.summary is not None and not isinstance(self.summary, str):
            raise TypeError('文件摘要必须为字符串或 None')
        normalized_tags = tuple(self.tags)
        if any(not isinstance(tag, str) for tag in normalized_tags):
            raise TypeError('文件标签必须为字符串序列')
        object.__setattr__(self, 'tags', normalized_tags)
        normalized_meta = {str(k): str(v) for k, v in dict(self.metadata).items()}
        object.__setattr__(self, 'metadata', MappingProxyType(normalized_meta))


@dataclass(frozen=True)
class FileSearchSettings:
    """文件检索子循环、RRF 排序与分页参数配置。"""
    cache_max_queries: int = 10
    rrf_k: int = 60
    history_weight: float = 0.5
    max_rounds: int = 16
    page_size: int = 5

    def __post_init__(self):
        for attr, label in (
            ('cache_max_queries', '内部候选池容量'),
            ('rrf_k', 'RRF 常数 k'),
            ('max_rounds', '最大检索轮数'),
            ('page_size', '分页大小'),
        ):
            value = getattr(self, attr)
            if type(value) is not int or value < 1:
                raise ValueError(f'{label}必须为正整数')
        if (isinstance(self.history_weight, bool)
                or not isinstance(self.history_weight, (int, float))
                or not math.isfinite(self.history_weight)
                or not 0.0 <= self.history_weight <= 1.0):
            raise ValueError('历史权重必须是 0 到 1 之间的有限数值')


@runtime_checkable
class FileSearchBackend(Protocol):
    """通用文件检索后端协议；MySQL、ES、PG 或本地索引均可按此接入。"""

    @property
    def settings(self) -> FileSearchSettings:
        """返回检索与分页参数配置。"""
        ...

    async def read(
        self,
        file_ids: Sequence[str] | None = None,
    ) -> Sequence[FileSearchRecord]:
        """回读当前调用方有权限访问的文件记录；file_ids 为 None 时返回可访问全集。"""
        ...

    async def search(
        self,
        field: FileSearchField,
        query: str,
        scope: Sequence[str] | None = None,
    ) -> Sequence[tuple[str, float]]:
        """在给定 scope（或当前可访问全集）内按字段检索，返回 (file_id, score) 序列。"""
        ...
