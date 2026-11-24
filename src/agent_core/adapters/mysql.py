"""MySQL 8+ 适配器。CAS 写入依赖 InnoDB 事务，连接池由调用者复用。"""
from collections import Counter, defaultdict
from contextlib import asynccontextmanager
import asyncio
import json
import math

from .schema_contract import COLUMNS, KEYS, columns_sql, insert_values, validate_columns, validate_version
from .sqlite import tokens
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


class MySQLStorage:
    supports_atomic_delete = True

    def __init__(self, pool, insert_fields):
        self.pool = pool
        self.insert_fields = insert_fields

    @classmethod
    async def connect(cls, pool, *, insert_fields=None):
        storage = cls(pool, insert_fields)
        async with storage._transaction() as cur:
            for table, required in COLUMNS.items():
                await cur.execute('SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s',(table,))
                row = await cur.fetchone()
                if not row or row[0] != 'InnoDB':
                    raise ValueError(f'{table} 缺失或不支持 InnoDB 事务')
                await cur.execute('SELECT COLUMN_NAME,DATA_TYPE,COLLATION_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s',(table,))
                columns = await cur.fetchall()
                validate_columns(table, {name:kind for name,kind,_ in columns}, required, 'mysql')
                for name,_,collation in columns:
                    if name in KEYS[table] and collation and not collation.endswith('_bin'):
                        raise ValueError(f'{table}.{name} 必须使用区分二进制标识的排序规则')
                await cur.execute('SELECT INDEX_NAME,COLUMN_NAME,SUB_PART FROM information_schema.STATISTICS '
                                  'WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s AND NON_UNIQUE=0 ORDER BY INDEX_NAME,SEQ_IN_INDEX',(table,))
                indexes = defaultdict(list)
                for index, column, prefix in await cur.fetchall():
                    indexes[index].append(column if prefix is None else None)
                if list(KEYS[table]) not in indexes.values():
                    raise ValueError(f'{table} 缺少必需唯一键 {KEYS[table]}')
            from asyncmy.errors import ProgrammingError, OperationalError
            try:
                await cur.execute('SELECT version FROM agent_core_schema WHERE id=1')
            except (ProgrammingError, OperationalError) as error:
                if error.args[0] in (1054,1146):
                    raise ValueError('缺少结构版本表或版本字段，请先部署数据库结构') from error
                raise
            validate_version(await cur.fetchone())
        return storage

    @asynccontextmanager
    async def _transaction(self):
        async with self.pool.acquire() as conn:
            await conn.begin()
            try:
                async with conn.cursor() as cur:
                    yield cur
                await conn.commit()
            except BaseException:
                await conn.rollback()
                raise

    async def _insert(self, cur, table, values):
        columns, args = insert_values(table,values,self.insert_fields)
        await cur.execute(f'INSERT INTO {table} ({columns_sql(columns)}) VALUES ({",".join(["%s"]*len(args))})', args)

    async def read(self, scope, key):
        async with self._transaction() as cur:
            await cur.execute('SELECT version,value FROM records WHERE scope=%s AND `key`=%s',(scope,key))
            row = await cur.fetchone()
            return Record(row[0],json.loads(row[1])) if row else None

    async def atomic(self, scope, writes):
        if not writes or len({w.key for w in writes}) != len(writes):
            raise ValueError('原子写入必须非空且 key 不能重复')
        from asyncmy.errors import IntegrityError, OperationalError
        try:
            async with self._transaction() as cur:
                # 全部执行者按相同顺序获取行锁；提交结果不明时直接传播异常。
                for w in sorted(writes,key=lambda w:w.key):
                    if w.value is None:
                        await cur.execute('DELETE FROM records WHERE scope=%s AND `key`=%s AND version=%s',
                                          (scope,w.key,w.expected))
                    else:
                        value = json.dumps(w.value,ensure_ascii=False,allow_nan=False)
                        if w.expected is None:
                            try:
                                await self._insert(cur,'records',dict(scope=scope,key=w.key,version=1,value=value))
                            except IntegrityError as error:
                                if error.args[0] == 1062:
                                    # 业务方可另设主键。只将库自身逻辑键的竞争解释为 CAS 冲突。
                                    await cur.execute('SELECT 1 FROM records WHERE scope=%s AND `key`=%s FOR UPDATE',
                                                      (scope, w.key))
                                    if await cur.fetchone():
                                        raise StorageConflict('存储版本冲突，操作未提交') from error
                                raise
                        else:
                            await cur.execute('UPDATE records SET value=%s,version=version+1 WHERE scope=%s AND `key`=%s AND version=%s',
                                              (value,scope,w.key,w.expected))
                    if cur.rowcount != 1:
                        raise StorageConflict('存储版本冲突，操作未提交')
        except OperationalError as error:
            if error.args[0]==1213:
                raise StorageConflict('并发事务死锁，操作已回滚') from error
            raise

    async def _space(self, cur, scope, space_id, dimensions, *, create=False):
        # 锁定空间行后在同一事务内更新文档，避免读后写覆盖其他索引版本。
        sql = 'SELECT space_id,dimensions FROM spaces WHERE scope=%s'
        await cur.execute(sql + (' FOR UPDATE' if create else ''),(scope,))
        row = await cur.fetchone()
        if row and tuple(row)!=(space_id,dimensions):
            raise ValueError('Embedding 空间或维度已改变，请显式重建独立索引')
        if not row and create:
            await self._insert(cur,'spaces',dict(scope=scope,space_id=space_id,dimensions=dimensions))

    async def index(self, scope, *, space_id, dimensions, documents):
        async with self._transaction() as cur:
            await self._space(cur,scope,space_id,dimensions,create=True)
            for d in documents:
                validate_vector(d.vector,dimensions)
                args=(scope,d.task_id,d.source_id)
                await cur.execute('SELECT 1 FROM memory WHERE scope=%s AND task_id=%s AND source_id=%s FOR UPDATE',args)
                vector=json.dumps(d.vector,allow_nan=False)
                if await cur.fetchone():
                    await cur.execute('UPDATE memory SET field=%s,text=%s,`vector`=%s,source_hash=%s WHERE scope=%s AND task_id=%s AND source_id=%s',
                                      (d.field,d.text,vector,d.source_hash,*args))
                else:
                    await self._insert(cur,'memory',dict(scope=scope,task_id=d.task_id,source_id=d.source_id,
                        field=d.field,text=d.text,vector=vector,source_hash=d.source_hash))

    async def search(self, scope, *, space_id, dimensions, task_ids, field, query, vector, limit):
        validate_vector(vector,dimensions)
        if not task_ids:
            return []
        async with self._transaction() as cur:
            await self._space(cur,scope,space_id,dimensions)
            marks=','.join(['%s']*len(task_ids))
            await cur.execute(f'SELECT task_id,source_id,source_hash,text,`vector` FROM memory WHERE scope=%s AND field=%s '
                              f'AND task_id IN ({marks}) ORDER BY task_id,source_id',(scope,field,*task_ids))
            rows=await cur.fetchall()
        if not rows:
            return []
        return await asyncio.to_thread(self._rank,rows,query,vector,limit)

    @staticmethod
    def _rank(rows, query, vector, limit):
        # 在已授权候选中计算 BM25 与余弦，不要求部署 MySQL 向量插件。
        docs=[Counter(tokens(row[3])) for row in rows]
        lengths=[sum(doc.values()) for doc in docs]
        average=sum(lengths)/len(rows) or 1
        words=set(tokens(query))
        idf = {}
        for word in words:
            frequency = sum(word in doc for doc in docs)
            idf[word] = math.log1p((len(rows) - frequency + .5) / (frequency + .5))
        lexical={}
        dense={}
        norm=math.hypot(*vector)
        for i,row in enumerate(rows):
            v=json.loads(row[4])
            dense[i]=sum(a*b for a,b in zip(v,vector))/(math.hypot(*v)*norm)
            score=sum(idf[w]*docs[i][w]*2.2/(docs[i][w]+1.2*(.25+.75*lengths[i]/average))
                      for w in words if w in docs[i])
            if score:
                lexical[i]=score
        scores=defaultdict(float)
        for route in (lexical,dense):
            for rank,i in enumerate(sorted(route,key=route.get,reverse=True)[:limit],1):
                scores[i]+=1/(60+rank)
        return [(*rows[i][:3],scores[i]) for i in sorted(scores,key=lambda i:(-scores[i],i))[:limit]]

    async def add_file_note(self, scope, file_id, name, summary, *, updated_at=None):
        validated_scope = validate_note_scope(scope)
        note, updated_at_str = build_file_note(file_id, name, summary, updated_at=updated_at)
        from asyncmy.errors import IntegrityError

        async with self.pool.acquire() as conn:
            await conn.begin()
            committing = False
            try:
                async with conn.cursor() as cur:
                    await cur.execute(
                        'SELECT 1 FROM file_notes WHERE scope=%s AND file_id=%s FOR UPDATE',
                        (validated_scope, note.file_id),
                    )
                    if await cur.fetchone():
                        raise FileNoteRejected(
                            '该文件已存在备注，不能重复添加或直接覆盖。',
                            '如需更新，请先删除旧备注后再重新添加。',
                            code='file_note_exists',
                        )
                    try:
                        await self._insert(
                            cur,
                            'file_notes',
                            dict(
                                scope=validated_scope,
                                file_id=note.file_id,
                                name=note.name,
                                summary=note.summary,
                                updated_at=updated_at_str,
                            ),
                        )
                    except IntegrityError as error:
                        if error.args[0] == 1062:
                            await cur.execute(
                                'SELECT 1 FROM file_notes WHERE scope=%s AND file_id=%s FOR UPDATE',
                                (validated_scope, note.file_id),
                            )
                            if await cur.fetchone():
                                raise FileNoteRejected(
                                    '该文件已存在备注，不能重复添加或直接覆盖。',
                                    '如需更新，请先删除旧备注后再重新添加。',
                                    code='file_note_exists',
                                ) from error
                        raise
                # 仅在进入提交阶段后的异常视为结果不明，避免将前置校验或冲突错误误判为不确定提交。
                committing = True
                await conn.commit()
                return note
            except FileNoteRejected:
                await conn.rollback()
                raise
            except Exception as error:
                try:
                    await conn.rollback()
                except Exception:
                    pass
                if committing:
                    raise FileNoteWriteUncertain(
                        '文件备注提交结果不明，请先查询确认后再决定后续操作。'
                    ) from error
                raise
            except BaseException:
                try:
                    await conn.rollback()
                except Exception:
                    pass
                raise

    async def delete_file_note(self, scope, file_id):
        validated_scope = validate_note_scope(scope)
        normalized_file_id = validate_file_note_id(file_id)
        async with self.pool.acquire() as conn:
            await conn.begin()
            committing = False
            try:
                async with conn.cursor() as cur:
                    await cur.execute(
                        'DELETE FROM file_notes WHERE scope=%s AND file_id=%s',
                        (validated_scope, normalized_file_id),
                    )
                    deleted = cur.rowcount > 0
                committing = True
                await conn.commit()
                return deleted
            except Exception as error:
                try:
                    await conn.rollback()
                except Exception:
                    pass
                if committing:
                    raise FileNoteWriteUncertain(
                        '文件备注删除提交结果不明，请先查询确认后再决定后续操作。'
                    ) from error
                raise
            except BaseException:
                try:
                    await conn.rollback()
                except Exception:
                    pass
                raise

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

        clauses = ['scope=%s']
        params: list[object] = [validated_scope]
        if normalized_ids is not None:
            marks = ','.join(['%s'] * len(normalized_ids))
            clauses.append(f'file_id IN ({marks})')
            params.extend(normalized_ids)
        if start_str is not None:
            clauses.append('updated_at >= %s')
            params.append(start_str)
        if end_str is not None:
            clauses.append('updated_at < %s')
            params.append(end_str)

        keyword_clauses: list[str] = []
        keyword_params: list[str] = []
        if cleaned_name is not None:
            keyword_clauses.append('MATCH(name) AGAINST(%s IN NATURAL LANGUAGE MODE)')
            keyword_params.append(cleaned_name)
        if cleaned_summary is not None:
            keyword_clauses.append('MATCH(summary) AGAINST(%s IN NATURAL LANGUAGE MODE)')
            keyword_params.append(cleaned_summary)
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
            'ORDER BY updated_at DESC, file_id ASC LIMIT %s'
        )
        params.append(validated_limit)

        async with self._transaction() as cur:
            await cur.execute(sql, params)
            rows = await cur.fetchall()
        return [
            FileNote(
                file_id=row[0],
                name=row[1],
                summary=row[2],
                updated_at=parse_note_timestamp(row[3]),
            )
            for row in rows
        ]

    async def close(self):
        """连接池属于宿主，适配器没有私有连接需要关闭。"""
