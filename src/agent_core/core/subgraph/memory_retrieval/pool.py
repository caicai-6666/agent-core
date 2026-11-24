"""记忆结果集缓存条目与校验；只保留有序任务 ID 与翻页游标，正文每次回查权威任务。"""
from dataclasses import dataclass
import re
from ...tool.cache import CachePool, CacheScopeKind


@dataclass
class MemoryQueryEntry:
    """存入 CachePool 的记忆检索结果集；只保存有序任务引用与翻页游标，不缓存历史正文。"""
    query_id: str
    items: tuple[str, ...]
    current_page: int = 0
    closed: bool = False

    def close(self) -> None:
        """在缓存驱逐或作用域关闭时标记失效，阻断旧页面引用继续解析。"""
        self.closed = True


class MemoryQueryViewRejected(ValueError):
    """记忆结果集就近校验提供的定向反馈；调用方不解析异常字符串。"""

    def __init__(self, reason: str, required_action: str):
        super().__init__(reason)
        self.reason = reason
        self.required_action = required_action


def validate_memory_tasks(ids) -> tuple[str, ...]:
    """校验任务引用数量、格式与唯一性，保持原有顺序。"""
    items = tuple(ids)
    if (not 1 <= len(items) <= 20
            or any(not isinstance(i, str) or not re.fullmatch(r'[0-9a-f]{32}', i) for i in items)
            or len(set(items)) != len(items)):
        raise ValueError('记忆任务引用数量、格式或唯一性不合法')
    return items


def validate_memory_result(result) -> tuple[str, ...]:
    """校验检索子 Agent 返回的成功排名结果并提取有序任务 ID。"""
    if result.status != 'success' or not result.tasks:
        raise ValueError('失败或空结果不能建立查询')
    rankings = tuple((task.node_id, task.score) for task in result.tasks)
    if (len(rankings) > 20
            or len({identifier for identifier, _ in rankings}) != len(rankings)
            or any(a[1] < b[1] for a, b in zip(rankings, rankings[1:]))):
        raise ValueError('查询排名不合法')
    return validate_memory_tasks(tuple(identifier for identifier, _ in rankings))


def _close_memory_entry(_key: str, entry: MemoryQueryEntry) -> None:
    entry.close()


def create_memory_cache_pool(
    *,
    name: str = 'memory_search_results',
    capacity: int = 10,
    scope: CacheScopeKind = 'task',
) -> CachePool[MemoryQueryEntry]:
    """创建记忆检索结果集缓存池规格；驱逐或作用域关闭时自动使结果集失效。"""
    return CachePool(name=name, capacity=capacity, scope=scope, on_evict=_close_memory_entry)


DEFAULT_MEMORY_CACHE_POOL: CachePool[MemoryQueryEntry] = create_memory_cache_pool()
