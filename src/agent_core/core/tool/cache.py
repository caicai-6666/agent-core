"""工具结果对象缓存池：声明式规格、按作用域隔离的 LRU 存储与事务提交边界。"""
from collections import OrderedDict
from collections.abc import Callable
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Generic, Literal, TypeVar

T = TypeVar('T')
D = TypeVar('D')
CacheScopeKind = Literal['session', 'task']

_MISSING = object()
_DELETED = object()


@dataclass(frozen=True)
class CachePool(Generic[T]):
    """不可变缓存池规格声明；实际 LRU 存储由库按作用域隔离管理。"""
    name: str
    capacity: int = 10
    scope: CacheScopeKind = 'session'
    on_evict: Callable[[str, T], None] | None = None

    def __post_init__(self):
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError('缓存池名称必须为非空字符串')
        if type(self.capacity) is not int or self.capacity < 1:
            raise ValueError('缓存池容量必须为正整数')
        if self.scope not in ('session', 'task'):
            raise ValueError('缓存池作用域必须为 session 或 task')
        if self.on_evict is not None and not callable(self.on_evict):
            raise TypeError('缓存池驱逐回调必须可调用')


class LruCache(Generic[T]):
    """有界 LRU 内存存储；仅由库内部作用域持有，不直接暴露给模型。"""

    def __init__(self, *, capacity: int, on_evict: Callable[[str, T], None] | None = None):
        if type(capacity) is not int or capacity < 1:
            raise ValueError('缓存容量必须为正整数')
        if on_evict is not None and not callable(on_evict):
            raise TypeError('驱逐回调必须可调用')
        self.capacity = capacity
        self._on_evict = on_evict
        self._entries: OrderedDict[str, T] = OrderedDict()
        self.closed = False

    def get(self, key: str, default: D = None) -> T | D:
        if self.closed or key not in self._entries:
            return default
        self._entries.move_to_end(key)
        return self._entries[key]

    def peek(self, key: str, default: Any = None) -> Any:
        """不刷新最近使用顺序的读取；供工具执行期暂存事务使用。"""
        if self.closed or key not in self._entries:
            return default
        return self._entries[key]

    def touch(self, key: str) -> None:
        if not self.closed and key in self._entries:
            self._entries.move_to_end(key)

    def put(self, key: str, value: T) -> None:
        if self.closed:
            return
        self._entries[key] = value
        self._entries.move_to_end(key)
        evicted: list[tuple[str, T]] = []
        while len(self._entries) > self.capacity:
            evicted.append(self._entries.popitem(last=False))
        if self._on_evict is not None:
            for evicted_key, evicted_value in evicted:
                self._on_evict(evicted_key, evicted_value)

    def delete(self, key: str) -> None:
        if not self.closed:
            self._entries.pop(key, None)

    def __contains__(self, key: object) -> bool:
        return not self.closed and key in self._entries

    def keys(self) -> tuple[str, ...]:
        return () if self.closed else tuple(self._entries.keys())

    def __len__(self) -> int:
        return 0 if self.closed else len(self._entries)

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        items = list(self._entries.items())
        self._entries.clear()
        if self._on_evict is not None:
            first_error: Exception | None = None
            for evicted_key, evicted_value in items:
                try:
                    self._on_evict(evicted_key, evicted_value)
                except Exception as error:
                    if first_error is None:
                        first_error = error
            if first_error is not None:
                raise first_error


class CacheScope:
    """单个 task 或 session 作用域内的缓存池容器；防止同名不同规格实例冲突。"""

    def __init__(self, scope: CacheScopeKind):
        if scope not in ('session', 'task'):
            raise ValueError('缓存作用域必须为 session 或 task')
        self.scope: CacheScopeKind = scope
        self._specs: dict[str, CachePool[Any]] = {}
        self._stores: dict[str, LruCache[Any]] = {}
        self._pins = 0
        self.closed = False

    @property
    def busy(self) -> bool:
        return self._pins > 0

    @contextmanager
    def pin(self):
        """任务运行期间钉住会话作用域，避免被容量驱逐提前释放。"""
        self._pins += 1
        try:
            yield self
        finally:
            self._pins -= 1

    def store_for(self, pool: CachePool[T]) -> LruCache[T]:
        if pool.scope != self.scope:
            raise ValueError(f'缓存池 {pool.name!r} 声明作用域为 {pool.scope!r}，与当前作用域 {self.scope!r} 不匹配')
        if self.closed:
            raise RuntimeError(f'缓存作用域 {self.scope!r} 已关闭')
        existing = self._specs.get(pool.name)
        if existing is not None:
            if existing is not pool:
                raise ValueError(f'作用域 {self.scope!r} 中已存在同名的不同 CachePool 实例：{pool.name!r}')
            return self._stores[pool.name]
        store: LruCache[T] = LruCache(capacity=pool.capacity, on_evict=pool.on_evict)
        self._specs[pool.name] = pool
        self._stores[pool.name] = store
        return store

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        stores = list(self._stores.values())
        self._stores.clear()
        self._specs.clear()
        first_error: Exception | None = None
        for store in stores:
            try:
                store.close()
            except Exception as error:
                if first_error is None:
                    first_error = error
        if first_error is not None:
            raise first_error


class SessionCacheScopes:
    """按 (owner, tree_id) 管理的会话级缓存作用域有界 LRU。"""

    def __init__(self, *, max_scopes: int = 100):
        if type(max_scopes) is not int or max_scopes < 1:
            raise ValueError('会话缓存作用域容量必须为正整数')
        self._max_scopes = max_scopes
        self._scopes: OrderedDict[tuple[str, str], CacheScope] = OrderedDict()
        self._closed = False

    def get(self, owner: str, tree_id: str) -> CacheScope:
        if self._closed:
            raise RuntimeError('会话缓存作用域管理器已关闭')
        key = (owner, tree_id)
        scope = self._scopes.get(key)
        if scope is None:
            while len(self._scopes) >= self._max_scopes:
                victim = next((candidate for candidate, entry in self._scopes.items() if not entry.busy), None)
                if victim is None:
                    raise RuntimeError('所有会话缓存作用域均在使用中')
                self.release(*victim)
            scope = CacheScope('session')
            self._scopes[key] = scope
        self._scopes.move_to_end(key)
        return scope

    def release(self, owner: str, tree_id: str) -> None:
        scope = self._scopes.pop((owner, tree_id), None)
        if scope is not None:
            scope.close()

    def close(self) -> None:
        self._closed = True
        scopes = list(self._scopes.values())
        self._scopes.clear()
        first_error: Exception | None = None
        for scope in scopes:
            try:
                scope.close()
            except Exception as error:
                if first_error is None:
                    first_error = error
        if first_error is not None:
            raise first_error


class _PoolTransaction(Generic[T]):
    """单次工具调用内的暂存事务：支持同调用读己所写，仅在提交确认后落入权威 LRU。"""
    __slots__ = ('_store', '_staged', '_touched')

    def __init__(self, store: LruCache[T]):
        self._store = store
        self._staged: dict[str, Any] = {}
        self._touched: list[str] = []

    def get(self, key: str, default: D = None) -> T | D:
        if key in self._staged:
            staged = self._staged[key]
            return default if staged is _DELETED else staged
        value = self._store.peek(key, _MISSING)
        if value is _MISSING:
            return default
        self._touched.append(key)
        return value

    def contains(self, key: object) -> bool:
        if key in self._staged:
            return self._staged[key] is not _DELETED
        return key in self._store

    def keys(self) -> tuple[str, ...]:
        result = [key for key in self._store.keys() if key not in self._staged]
        for key, value in self._staged.items():
            if value is not _DELETED:
                result.append(key)
        return tuple(result)

    def put(self, key: str, value: T) -> None:
        # 重复写入同一键时重插以保留最终写入顺序。
        self._staged.pop(key, None)
        self._staged[key] = value

    def delete(self, key: str) -> None:
        self._staged.pop(key, None)
        self._staged[key] = _DELETED

    @property
    def has_pending(self) -> bool:
        return bool(self._staged or self._touched)

    def commit(self) -> None:
        if self._store.closed:
            return
        for key in self._touched:
            self._store.touch(key)
        for key, value in self._staged.items():
            if value is _DELETED:
                self._store.delete(key)
            else:
                self._store.put(key, value)


class _CacheHandle(Generic[T]):
    __slots__ = ('_tx',)

    def __init__(self, tx: _PoolTransaction[T]):
        self._tx = tx


class ReadOnlyCache(_CacheHandle[T]):
    """只读缓存池句柄；不提供 put/delete。"""
    __slots__ = ()

    def get(self, key: str, default: D = None) -> T | D:
        return self._tx.get(key, default)

    def __contains__(self, key: object) -> bool:
        return self._tx.contains(key)

    def keys(self) -> tuple[str, ...]:
        return self._tx.keys()


class WriteOnlyCache(_CacheHandle[T]):
    """只写缓存池句柄；不提供 get/__contains__/keys。"""
    __slots__ = ()

    def put(self, key: str, value: T) -> None:
        self._tx.put(key, value)

    def delete(self, key: str) -> None:
        self._tx.delete(key)


class ReadWriteCache(ReadOnlyCache[T], WriteOnlyCache[T]):
    """读写缓存池句柄；同次调用内支持读己所写，提交后写入 LRU。"""
    __slots__ = ()


class _EmptyCacheAccessor:
    """未声明任何缓存池的工具使用的零分配守卫。"""
    __slots__ = ()

    def access(self, pool: CachePool[Any]) -> Any:
        raise PermissionError(f'当前工具未声明缓存池 {pool.name!r} 的访问权限')

    def has_pending(self) -> bool:
        return False

    def commit(self) -> None:
        pass


_EMPTY_ACCESSOR = _EmptyCacheAccessor()


class _ToolCacheAccessor:
    """单次工具执行期的权限校验与事务聚合器。"""
    __slots__ = ('_tool_name', '_read_pools', '_write_pools', '_task_cache', '_session_cache', '_transactions')

    def __init__(self, *, tool_name: str, read_pools: tuple[CachePool[Any], ...],
                 write_pools: tuple[CachePool[Any], ...],
                 task_cache: CacheScope | None, session_cache: CacheScope | None):
        self._tool_name = tool_name
        self._read_pools = read_pools
        self._write_pools = write_pools
        self._task_cache = task_cache
        self._session_cache = session_cache
        self._transactions: dict[int, _PoolTransaction[Any]] = {}

    def access(self, pool: CachePool[T]) -> ReadOnlyCache[T] | WriteOnlyCache[T] | ReadWriteCache[T]:
        # 按实例身份校验权限，防止同名不同规格的 CachePool 绕过声明。
        can_read = any(declared is pool for declared in self._read_pools)
        can_write = any(declared is pool for declared in self._write_pools)
        if not can_read and not can_write:
            raise PermissionError(f'工具 {self._tool_name!r} 未声明缓存池 {pool.name!r} 的访问权限')
        tx = self._transactions.get(id(pool))
        if tx is None:
            scope = self._task_cache if pool.scope == 'task' else self._session_cache
            if scope is None:
                raise RuntimeError(f'当前执行上下文未提供 {pool.scope!r} 缓存作用域')
            tx = _PoolTransaction(scope.store_for(pool))
            self._transactions[id(pool)] = tx
        if can_read and can_write:
            return ReadWriteCache(tx)
        if can_read:
            return ReadOnlyCache(tx)
        return WriteOnlyCache(tx)

    def has_pending(self) -> bool:
        return any(tx.has_pending for tx in self._transactions.values())

    def commit(self) -> None:
        for tx in self._transactions.values():
            if tx.has_pending:
                tx.commit()


_current_tool_cache_accessor: ContextVar[_ToolCacheAccessor | _EmptyCacheAccessor | None] = ContextVar(
    'current_tool_cache_accessor', default=None
)


def current_tool_cache(pool: CachePool[T]) -> ReadOnlyCache[T] | WriteOnlyCache[T] | ReadWriteCache[T]:
    """在工具 execute 期间获取已声明缓存池的受限句柄；供 RegisteredTool 函数与 BaseTool.cache 共用。"""
    if not isinstance(pool, CachePool):
        raise TypeError('必须传入 CachePool 实例')
    accessor = _current_tool_cache_accessor.get()
    if accessor is None:
        raise RuntimeError('只能在工具 execute 执行期间访问缓存池')
    return accessor.access(pool)


@contextmanager
def bind_tool_cache(entry: Any, *, task_cache: CacheScope | None, session_cache: CacheScope | None):
    """在单次工具执行边界内绑定缓存访问器；未声明缓存池的工具零额外分配。"""
    if not entry.read_pools and not entry.write_pools:
        accessor: _ToolCacheAccessor | _EmptyCacheAccessor = _EMPTY_ACCESSOR
    else:
        accessor = _ToolCacheAccessor(
            tool_name=entry.name,
            read_pools=entry.read_pools,
            write_pools=entry.write_pools,
            task_cache=task_cache,
            session_cache=session_cache,
        )
    token = _current_tool_cache_accessor.set(accessor)
    try:
        yield accessor
    finally:
        _current_tool_cache_accessor.reset(token)
