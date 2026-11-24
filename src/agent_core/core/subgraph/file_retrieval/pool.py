"""单次检索内部候选池、结果集缓存条目与候选页排版；不缓存文件名、摘要或权限。"""
from collections import OrderedDict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import json
import re
from uuid import uuid4
from ...json_codec import ModelActionRejected
from ...tool.cache import CachePool, CacheScopeKind
from .backend import FileSearchRecord
from .ranking import CandidateRanking, rank_candidates, union_candidates


class CandidatePool:
    """单次文件检索规划期间的临时候选排名池；任务或检索结束后即关闭释放。"""

    def __init__(self, *, initial=None, initial_ranking=None, capacity=10, k=60, history_weight=0.5):
        self.initial = None if initial is None else frozenset(initial)
        self.initial_ranking = initial_ranking
        self.capacity, self.k, self.history_weight = capacity, k, history_weight
        self.entries: OrderedDict[str, CandidateRanking] = OrderedDict()
        self.closed = False

    def get(self, rid: str) -> CandidateRanking:
        if self.closed or rid not in self.entries:
            raise ModelActionRejected(
                '候选结果集不存在或已失效，本次操作未完成。',
                '重新执行候选检索并使用新返回的候选结果集 ID；不能把失效引用当作空结果或擅自扩大范围。',
            )
        self.entries.move_to_end(rid)
        return self.entries[rid]

    def scope(self, rid: str | None):
        if self.closed:
            raise ModelActionRejected('本次候选查询已经结束，不能继续使用。', '重新发起文件检索，不复用已结束查询的候选 ID。')
        return tuple(i for i, _ in self.get(rid).hits) if rid is not None else self.initial

    def _store(self, ranking: CandidateRanking) -> str:
        if self.closed or (self.initial is not None and not {i for i, _ in ranking.hits} <= self.initial):
            raise ValueError('候选超出初始范围或查询已结束')
        if any(not isinstance(i, str) or not i.strip() for i, _ in ranking.hits):
            raise ValueError('文件 ID 必须为非空字符串')
        rid = 'candidate-set:' + str(uuid4())
        self.entries[rid] = ranking
        while len(self.entries) > self.capacity:
            self.entries.popitem(last=False)
        return rid

    def put(self, hits, kind: str, parent: str | None = None, *, exact: bool = False) -> str:
        old = self.get(parent) if parent is not None else self.initial_ranking
        return self._store(rank_candidates(
            hits, search_type=kind, parent=old, exact=exact, k=self.k, history_weight=self.history_weight,
        ))

    def union(self, left: str, right: str) -> str:
        a, b = self.get(left), self.get(right)
        return left if left == right else self._store(union_candidates(a, b, k=self.k))

    def close(self) -> None:
        self.closed = True
        self.entries.clear()


@dataclass
class FileSearchResultSet:
    """存入 CachePool 的最终文件检索结果集；只保存排名、说明与翻页游标，不缓存权威文件详情。"""
    result_id: str
    ranking: CandidateRanking
    explanation: str
    page: int = 0
    closed: bool = False

    def close(self) -> None:
        """在缓存驱逐或作用域关闭时标记失效，阻断旧页面引用继续解析。"""
        self.closed = True


def _close_result_set(_key: str, entry: FileSearchResultSet) -> None:
    entry.close()


def create_file_search_cache_pool(
    *,
    name: str = 'file_search_results',
    capacity: int = 10,
    scope: CacheScopeKind = 'task',
) -> CachePool[FileSearchResultSet]:
    """创建文件检索结果集缓存池规格；驱逐或作用域关闭时自动使结果集失效。"""
    return CachePool(name=name, capacity=capacity, scope=scope, on_evict=_close_result_set)


DEFAULT_FILE_SEARCH_CACHE_POOL: CachePool[FileSearchResultSet] = create_file_search_cache_pool()


class FileResultError(ValueError):
    """可向模型展示的本地结果引用或页码错误。"""

    def __init__(self, reason: str, required_action: str | None = None):
        super().__init__(reason)
        self.reason = reason
        self.required_action = required_action


def plain_name(value: str | None) -> str:
    """外部名称与文本作为资料转义，避免换行或 Markdown 语法伪造列表项与指令标题。"""
    normalized = ' '.join((value or '未命名文件').split())
    return re.sub(r'([\\`*_{}\[\]()#+!|>~<])', r'\\\1', normalized)


def inline_code(value: str) -> str:
    """将业务标识按代码片段安全展示，保留原值并使用动态反引号边界防止逃逸。"""
    display = json.dumps(value, ensure_ascii=False)[1:-1]
    fence = '`' * (max((len(run) for run in re.findall(r'`+', display)), default=0) + 1)
    return f'{fence} {display} {fence}' if display.startswith('`') or display.endswith('`') else f'{fence}{display}{fence}'


def format_code_list(values: Sequence[str] | None) -> str:
    """将标签或标识列表逐项转为代码文本，不让业务值变成 Markdown。"""
    if values is None:
        return '资料未提供'
    if not values:
        return '暂无'
    return '、'.join(inline_code(value) for value in values)


def render_candidate_page(
    rows: Mapping[str, FileSearchRecord],
    ids: Sequence[str],
    *,
    start: int,
    page: int,
    total_pages: int,
    total: int,
) -> str:
    """将当页权威回读的文件记录排版为可读 Markdown 资料页。"""
    lines = ['# 文件查询结果', '', f'第 {page} / {total_pages} 页 · 本次召回 {total} 个候选', '']
    for offset, file_id in enumerate(ids, start + 1):
        row = rows.get(file_id)
        if row is None:
            # 删除或撤权时连 ID 也不重放；保留原排名位置，不据此猜测具体原因。
            lines.extend([f'{offset}. 此候选已不可用，请重新检索。', ''])
            continue
        item_lines = [
            f'{offset}. **文件名称**：{plain_name(row.name)}  ',
            f'   **文件 ID**：{inline_code(file_id)}  ',
        ]
        if row.summary is not None and row.summary.strip():
            item_lines.append(f'   **摘要**：{plain_name(row.summary)}  ')
        if row.tags:
            item_lines.append(f'   **标签**：{format_code_list(row.tags)}  ')
        for meta_key, meta_value in row.metadata.items():
            item_lines.append(f'   **{plain_name(meta_key)}**：{plain_name(meta_value)}  ')
        item_lines.append('')
        lines.extend(item_lines)
    lines.append('还有下一页。' if page < total_pages else '已到最后一页。')
    return '\n'.join(lines)
