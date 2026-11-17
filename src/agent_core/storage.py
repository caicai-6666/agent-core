"""数据库基础接口；业务状态机在库内，适配器只负责原子 CAS 和混合检索。"""
from dataclasses import dataclass
from typing import Protocol


class StorageConflict(RuntimeError):
    """版本不匹配；不能自动重复外部副作用。"""


@dataclass(frozen=True)
class Record:
    version: int
    value: dict


@dataclass(frozen=True)
class Write:
    key: str
    expected: int | None
    value: dict


@dataclass(frozen=True)
class MemoryDocument:
    task_id: str
    field: str
    source_id: str
    text: str
    vector: tuple[float, ...]
    source_hash: str


class Storage(Protocol):
    async def read(self, scope: str, key: str) -> Record | None: ...

    async def atomic(self, scope: str, writes: list[Write]) -> None:
        """全部成功或全部回滚；expected=None 表示必须不存在，否则严格比对版本。

        每次成功版本 +1（创建从 1 开始）；必须隔离 scope，禁止部分成功。
        提交结果不明时抛出原异常，调用方只能回查，不能盲重试。
        """
        ...

    async def index(self, scope: str, *, space_id: str, dimensions: int,
                    documents: list[MemoryDocument]) -> None: ...

    async def search(self, scope: str, *, space_id: str, dimensions: int,
                     task_ids: list[str], field: str, query: str,
                     vector: list[float], limit: int) -> list[tuple[str, str, str, float]]:
        """返回 (task_id, source_id, source_hash, 非负 RRF 分数)，仅在允许任务范围内召回。

        BM25 类文本召回和向量召回分别排名，以 reciprocal rank fusion 合并。
        维度/空间不符必须失败；索引是缓存，返回前库仍回查权威来源。
        """
        ...

    async def close(self) -> None:
        """关闭适配器自己持有的资源；由宿主在 Agent 停止后调用。"""
        ...
