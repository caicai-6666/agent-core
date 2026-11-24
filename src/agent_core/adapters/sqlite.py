"""可运行的 SQLite 参考实现；FTS5 BM25 + 精确余弦召回，适合本机与小规模部署。"""
import asyncio
from collections import defaultdict
import json
import math
import re
import sqlite3
from pathlib import Path
from .schema_contract import COLUMNS, KEYS, schema_sql, insert_values, columns_sql, validate_columns, validate_version
from ..file_notes import (
    FileNote,
    FileNoteRejected,
    FileNoteWriteUncertain,
    build_file_note,
    normalize_search_params,
    parse_note_timestamp,
    validate_file_note_id,
    validate_note_scope,
)
from ..storage import Record, StorageConflict
from ..vectors import validate_vector


def tokens(text):
    # FTS5 默认不分中文词：中文逐字并加入二元组，英文保留词边界。
    parts = re.findall(r'[a-z0-9_]+|[\u3400-\u9fff]', text.lower())
    pairs = [a + b for a, b in zip(parts, parts[1:]) if len(a) == len(b) == 1 and ord(a) > 127 and ord(b) > 127]
    return parts + pairs


def _note_fts_tokens(text: str) -> list[str]:
    # 英文数字按连续词切分，中文仅在连续汉字段内生成单字与二元组，避免跨空格误拼接。
    result: list[str] = []
    for segment in re.findall(r'[a-z0-9]+|[\u3400-\u9fff]+', text.lower()):
        if ord(segment[0]) <= 127:
            result.append(segment)
        else:
            result.extend(segment)
            if len(segment) > 1:
                result.extend(segment[i:i + 2] for i in range(len(segment) - 1))
    return result


def _build_sqlite_keyword_clause(column: str, fts_column: str, keyword: str) -> tuple[str, list[str]]:
    # 有词元时走 FTS5（英数支持前缀匹配，中文按单字/二元组精确匹配）；纯符号回退不区分大小写子串匹配。
    query_tokens = list(dict.fromkeys(_note_fts_tokens(keyword)))
    if not query_tokens:
        return f'instr(lower({column}), ?) > 0', [keyword.lower()]
    terms = [
        f'{fts_column}:"{tok}"*' if ord(tok[0]) <= 127 else f'{fts_column}:"{tok}"'
        for tok in query_tokens
    ]
    return 'rowid IN (SELECT rowid FROM file_notes_fts WHERE file_notes_fts MATCH ?)', [' AND '.join(terms)]


class SQLiteStorage:
    supports_atomic_delete = True

    def __init__(self, path, *, initialize=True, insert_fields=None):
        self.insert_fields = insert_fields
        self.path = str(path)
        self._lock = asyncio.Lock()
        target = self.path if initialize else Path(path).resolve().as_uri() + '?mode=rw'
        self._db = sqlite3.connect(target, uri=not initialize, check_same_thread=False, isolation_level=None)
        self._db.execute('PRAGMA busy_timeout=5000')
        self._db.execute('PRAGMA journal_mode=WAL')
        try:
            if initialize:
                self._db.executescript(schema_sql('sqlite'))
            self.validate_schema()
        except BaseException:
            self._db.close()
            raise

    def validate_schema(self):
        for table, expected in COLUMNS.items():
            columns = self._db.execute(f'PRAGMA table_info("{table}")').fetchall()
            actual = {row[1]: row[2].upper() for row in columns}
            validate_columns(table, actual, expected, 'sqlite')
            primary = tuple(row[1] for row in sorted(columns, key=lambda row: row[5]) if row[5])
            unique = {primary}
            for index in self._db.execute(f'PRAGMA index_list("{table}")'):
                if index[2] and not index[4]:
                    unique.add(tuple(row[2] for row in self._db.execute(
                        'SELECT * FROM pragma_index_info(?) ORDER BY seqno', (index[1],))))
            if KEYS[table] not in unique:
                raise ValueError(f'{table} 缺少必需唯一键 {KEYS[table]}')
        fts = self._db.execute("SELECT sql FROM sqlite_master WHERE name='memory_fts'").fetchone()
        if not fts or 'using fts5' not in fts[0].lower():
            raise ValueError('缺少 FTS5 索引 memory_fts')
        self._db.execute('SELECT tokens FROM memory_fts LIMIT 0')
        notes_fts = self._db.execute("SELECT sql FROM sqlite_master WHERE name='file_notes_fts'").fetchone()
        if not notes_fts or 'using fts5' not in notes_fts[0].lower():
            raise ValueError('缺少 FTS5 索引 file_notes_fts')
        self._db.execute('SELECT name_tokens, summary_tokens FROM file_notes_fts LIMIT 0')
        try:
            version = self._db.execute('SELECT version FROM agent_core_schema WHERE id=1').fetchone()
        except sqlite3.OperationalError as error:
            raise ValueError('缺少结构版本表，请先执行 SQLite 建表或升级脚本') from error
        validate_version(version)

    @classmethod
    def connect(cls, path, *, insert_fields=None):
        """绑定已经部署的数据库；不创建文件或表。"""
        if not Path(path).is_file():
            raise ValueError('SQLite 数据库尚未准备')
        return cls(path, initialize=False, insert_fields=insert_fields)

    def _insert(self, table, values):
        columns, args = insert_values(table, values, self.insert_fields)
        placeholders = ','.join('?' for _ in columns)
        self._db.execute(f'INSERT INTO {table} ({columns_sql(columns)}) VALUES ({placeholders})', args)

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
                    value = json.dumps(write.value, ensure_ascii=False, allow_nan=False)
                    if row is None:
                        self._insert('records', dict(scope=scope, key=write.key, version=1, value=value))
                    else:
                        self._db.execute('UPDATE records SET version=?,value=? WHERE scope=? AND key=?',
                                         (actual + 1, value, scope, write.key))
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
            self._insert('spaces', dict(scope=scope, space_id=space_id, dimensions=dimensions))

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
                        self._db.execute('UPDATE memory SET field=?,text=?,vector=?,source_hash=? WHERE rowid=?',
                                         (d.field,d.text,json.dumps(d.vector),d.source_hash,row[0]))
                    else:
                        self._insert('memory', dict(scope=scope,task_id=d.task_id,field=d.field,source_id=d.source_id,
                                                    text=d.text,vector=json.dumps(d.vector),source_hash=d.source_hash))
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

    async def add_file_note(self, scope, file_id, name, summary, *, updated_at=None):
        validated_scope = validate_note_scope(scope)
        note, updated_at_str = build_file_note(file_id, name, summary, updated_at=updated_at)

        def execute():
            self._db.execute('BEGIN IMMEDIATE')
            committing = False
            try:
                existing = self._db.execute(
                    'SELECT 1 FROM file_notes WHERE scope=? AND file_id=?',
                    (validated_scope, note.file_id),
                ).fetchone()
                if existing:
                    raise FileNoteRejected(
                        '该文件已存在备注，不能重复添加或直接覆盖。',
                        '如需更新，请先删除旧备注后再重新添加。',
                        code='file_note_exists',
                    )
                self._insert(
                    'file_notes',
                    dict(
                        scope=validated_scope,
                        file_id=note.file_id,
                        name=note.name,
                        summary=note.summary,
                        updated_at=updated_at_str,
                    ),
                )
                rowid = self._db.execute(
                    'SELECT rowid FROM file_notes WHERE scope=? AND file_id=?',
                    (validated_scope, note.file_id),
                ).fetchone()[0]
                self._db.execute(
                    'INSERT INTO file_notes_fts(rowid, name_tokens, summary_tokens) VALUES (?, ?, ?)',
                    (rowid, ' '.join(_note_fts_tokens(note.name)), ' '.join(_note_fts_tokens(note.summary))),
                )
                committing = True
                self._db.commit()
                return note
            except FileNoteRejected:
                self._db.rollback()
                raise
            except sqlite3.IntegrityError as error:
                self._db.rollback()
                if self._db.execute(
                    'SELECT 1 FROM file_notes WHERE scope=? AND file_id=?',
                    (validated_scope, note.file_id),
                ).fetchone():
                    raise FileNoteRejected(
                        '该文件已存在备注，不能重复添加或直接覆盖。',
                        '如需更新，请先删除旧备注后再重新添加。',
                        code='file_note_exists',
                    ) from error
                raise
            except Exception as error:
                try:
                    self._db.rollback()
                except Exception:
                    pass
                if committing:
                    raise FileNoteWriteUncertain(
                        '文件备注提交结果不明，请先查询确认后再决定后续操作。'
                    ) from error
                raise
            except BaseException:
                try:
                    self._db.rollback()
                except Exception:
                    pass
                raise

        return await self._call(execute)

    async def delete_file_note(self, scope, file_id):
        validated_scope = validate_note_scope(scope)
        normalized_file_id = validate_file_note_id(file_id)

        def execute():
            self._db.execute('BEGIN IMMEDIATE')
            committing = False
            try:
                row = self._db.execute(
                    'SELECT rowid FROM file_notes WHERE scope=? AND file_id=?',
                    (validated_scope, normalized_file_id),
                ).fetchone()
                if row is None:
                    self._db.rollback()
                    return False
                self._db.execute('DELETE FROM file_notes_fts WHERE rowid=?', row)
                self._db.execute('DELETE FROM file_notes WHERE rowid=?', row)
                committing = True
                self._db.commit()
                return True
            except Exception as error:
                try:
                    self._db.rollback()
                except Exception:
                    pass
                if committing:
                    raise FileNoteWriteUncertain(
                        '文件备注删除提交结果不明，请先查询确认后再决定后续操作。'
                    ) from error
                raise
            except BaseException:
                try:
                    self._db.rollback()
                except Exception:
                    pass
                raise

        return await self._call(execute)

    async def search_file_notes(
        self,
        scope,
        *,
        file_ids=None,
        name_keyword=None,
        summary_keyword=None,
        match_mode='and',
        start_time=None,
        end_time=None,
        limit=1001,
    ):
        (
            validated_scope,
            normalized_ids,
            cleaned_name,
            cleaned_summary,
            validated_mode,
            start_str,
            end_str,
            validated_limit,
        ) = normalize_search_params(
            scope,
            file_ids=file_ids,
            name_keyword=name_keyword,
            summary_keyword=summary_keyword,
            match_mode=match_mode,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )
        if normalized_ids is not None and not normalized_ids:
            return []

        def execute():
            clauses = ['scope=?']
            params: list[object] = [validated_scope]
            if normalized_ids is not None:
                marks = ','.join('?' for _ in normalized_ids)
                clauses.append(f'file_id IN ({marks})')
                params.extend(normalized_ids)
            if start_str is not None:
                clauses.append('updated_at >= ?')
                params.append(start_str)
            if end_str is not None:
                clauses.append('updated_at < ?')
                params.append(end_str)

            keyword_clauses: list[str] = []
            keyword_params: list[str] = []
            if cleaned_name is not None:
                clause, kparams = _build_sqlite_keyword_clause('name', 'name_tokens', cleaned_name)
                keyword_clauses.append(clause)
                keyword_params.extend(kparams)
            if cleaned_summary is not None:
                clause, kparams = _build_sqlite_keyword_clause('summary', 'summary_tokens', cleaned_summary)
                keyword_clauses.append(clause)
                keyword_params.extend(kparams)
            if len(keyword_clauses) == 2:
                joiner = ' AND ' if validated_mode == 'and' else ' OR '
                clauses.append(f'({joiner.join(keyword_clauses)})')
                params.extend(keyword_params)
            elif len(keyword_clauses) == 1:
                clauses.append(keyword_clauses[0])
                params.extend(keyword_params)

            sql = (
                'SELECT file_id, name, summary, updated_at FROM file_notes '
                f"WHERE {' AND '.join(clauses)} "
                'ORDER BY updated_at DESC, file_id ASC LIMIT ?'
            )
            params.append(validated_limit)
            rows = self._db.execute(sql, params).fetchall()
            return [
                FileNote(
                    file_id=row[0],
                    name=row[1],
                    summary=row[2],
                    updated_at=parse_note_timestamp(row[3]),
                )
                for row in rows
            ]

        return await self._call(execute)

    async def close(self):
        await self._call(self._db.close)
