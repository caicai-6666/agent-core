"""可运行的 SQLite 参考实现；FTS5 BM25 + 精确余弦召回，适合本机与小规模部署。"""
import asyncio
from collections import defaultdict
from functools import partial
import json
import math
import re
import sqlite3
from ..storage import Record, Write, MemoryDocument, StorageConflict


def tokens(text):
    # FTS5 默认不分中文词：中文逐字并加入二元组，英文保留词边界。
    parts = re.findall(r'[a-z0-9_]+|[\u3400-\u9fff]', text.lower())
    pairs = [a + b for a, b in zip(parts, parts[1:]) if len(a) == len(b) == 1 and ord(a) > 127 and ord(b) > 127]
    return parts + pairs


from ..vectors import validate_vector


class SQLiteStorage:
    supports_atomic_delete = True

    def __init__(self, path):
        self.path = str(path)
        self._lock = asyncio.Lock()
        self._db = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self._db.execute('PRAGMA busy_timeout=5000')
        self._db.execute('PRAGMA journal_mode=WAL')
        self._db.executescript('''
            CREATE TABLE IF NOT EXISTS records (
                scope TEXT NOT NULL, key TEXT NOT NULL, version INTEGER NOT NULL,
                value TEXT NOT NULL, PRIMARY KEY(scope, key));
            CREATE TABLE IF NOT EXISTS spaces (
                scope TEXT PRIMARY KEY, space_id TEXT NOT NULL, dimensions INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS memory (
                scope TEXT NOT NULL, task_id TEXT NOT NULL, field TEXT NOT NULL,
                source_id TEXT NOT NULL, text TEXT NOT NULL, vector TEXT NOT NULL, source_hash TEXT NOT NULL,
                UNIQUE(scope, task_id, source_id));
            CREATE VIRTUAL TABLE IF NOT EXISTS memory_fts USING fts5(tokens);
        ''')

    async def _call(self, function, *args):
        # 取消等待线程不能提前释放连接锁，否则下一次调用可能进入未结束的事务。
        async with self._lock:
            future = asyncio.create_task(asyncio.to_thread(function, *args))
            try:
                return await asyncio.shield(future)
            except asyncio.CancelledError:
                await future
                raise

    async def read(self, scope, key):
        def execute():
            row = self._db.execute('SELECT version,value FROM records WHERE scope=? AND key=?', (scope, key)).fetchone()
            return Record(row[0], json.loads(row[1])) if row else None
        return await self._call(execute)

    async def atomic(self, scope, writes):
        if not writes or len({w.key for w in writes}) != len(writes):
            raise ValueError('原子写入必须非空且 key 不能重复')
        def execute():
            self._db.execute('BEGIN IMMEDIATE')
            try:
                for write in writes:
                    row = self._db.execute('SELECT version FROM records WHERE scope=? AND key=?', (scope, write.key)).fetchone()
                    actual = row[0] if row else None
                    if actual != write.expected:
                        raise StorageConflict('存储版本冲突，操作未提交')
                    if write.value is None:
                        if actual is None:
                            raise StorageConflict('待删记录不存在')
                        self._db.execute('DELETE FROM records WHERE scope=? AND key=?', (scope, write.key))
                        continue
                    self._db.execute('INSERT INTO records VALUES (?,?,?,?) ON CONFLICT(scope,key) DO UPDATE SET version=excluded.version,value=excluded.value',
                        (scope, write.key, (actual or 0) + 1, json.dumps(write.value, ensure_ascii=False, allow_nan=False)))
                self._db.commit()
            except BaseException:
                self._db.rollback()
                raise
        await self._call(execute)

    def _space(self, scope, space_id, dimensions, *, create=False):
        row = self._db.execute('SELECT space_id,dimensions FROM spaces WHERE scope=?', (scope,)).fetchone()
        if row and row != (space_id, dimensions):
            raise ValueError('Embedding 空间或维度已改变，请显式重建独立索引')
        if not row and create:
            self._db.execute('INSERT INTO spaces VALUES (?,?,?)', (scope, space_id, dimensions))

    async def index(self, scope, *, space_id, dimensions, documents):
        for document in documents:
            validate_vector(document.vector, dimensions)
        def execute():
            self._db.execute('BEGIN IMMEDIATE')
            try:
                self._space(scope, space_id, dimensions, create=True)
                for d in documents:
                    row = self._db.execute('SELECT rowid FROM memory WHERE scope=? AND task_id=? AND source_id=?',
                        (scope, d.task_id, d.source_id)).fetchone()
                    if row:
                        self._db.execute('DELETE FROM memory_fts WHERE rowid=?', row)
                    self._db.execute('INSERT INTO memory VALUES (?,?,?,?,?,?,?) ON CONFLICT(scope,task_id,source_id) DO UPDATE SET text=excluded.text,vector=excluded.vector,source_hash=excluded.source_hash',
                        (scope, d.task_id, d.field, d.source_id, d.text, json.dumps(d.vector), d.source_hash))
                    rowid = self._db.execute('SELECT rowid FROM memory WHERE scope=? AND task_id=? AND source_id=?',
                        (scope, d.task_id, d.source_id)).fetchone()[0]
                    self._db.execute('INSERT INTO memory_fts(rowid,tokens) VALUES (?,?)', (rowid, ' '.join(tokens(d.text))))
                self._db.commit()
            except BaseException:
                self._db.rollback()
                raise
        await self._call(execute)

    async def search(self, scope, *, space_id, dimensions, task_ids, field, query, vector, limit):
        validate_vector(vector, dimensions)
        if not task_ids:
            return []
        def execute():
            self._space(scope, space_id, dimensions)
            allowed = set(task_ids)
            rows = self._db.execute('SELECT rowid,task_id,vector,source_hash,source_id FROM memory WHERE scope=? AND field=?', (scope, field)).fetchall()
            rows = [row for row in rows if row[1] in allowed]
            by_id = {row[0]: row for row in rows}
            norm = math.sqrt(sum(x*x for x in vector))
            def cosine(row):
                other = json.loads(row[2])
                return sum(a*b for a,b in zip(vector, other)) / (norm * math.sqrt(sum(x*x for x in other)))
            dense = sorted(rows, key=lambda row: (-cosine(row), row[1]))[:limit]
            words = list(dict.fromkeys(tokens(query)))
            lexical = []
            if words:
                match = ' OR '.join('"' + word + '"' for word in words)
                lexical = [by_id[row[0]] for row in self._db.execute('SELECT rowid FROM memory_fts WHERE memory_fts MATCH ? ORDER BY bm25(memory_fts),rowid', (match,)) if row[0] in by_id][:limit]
            scores = defaultdict(float)
            for route in (dense, lexical):
                for rank, row in enumerate(route, 1):
                    scores[row[0]] += 1 / (60 + rank)
            return [(by_id[key][1], by_id[key][4], by_id[key][3], score) for key,score in sorted(scores.items(), key=lambda item: (-item[1], item[0]))[:limit]]
        return await self._call(execute)

    async def close(self):
        await self._call(self._db.close)
