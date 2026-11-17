"""供数据库适配者直接运行的契约校验；仅可对隔离测试库执行。"""
import asyncio
from inspect import isawaitable
from uuid import uuid4
from .storage import Write, StorageConflict, MemoryDocument


async def verify_storage(factory):
    """factory 返回全新适配器；测试数据用随机 scope 隔离，最后调用 close。"""
    storage = factory()
    if isawaitable(storage):
        storage = await storage
    scope = 'contract:' + uuid4().hex
    checks = []
    try:
        assert await storage.read(scope, 'a') is None
        await storage.atomic(scope, [Write('a', None, {'value':1}), Write('b', None, {'value':2})])
        checks.append('create')
        read = await storage.read(scope, 'a')
        assert read.version == 1 and read.value == {'value':1}
        read.value['value'] = 99
        assert (await storage.read(scope, 'a')).value == {'value':1}
        assert await storage.read(scope + ':other', 'a') is None
        checks.append('scope-and-snapshot')
        try:
            await storage.atomic(scope, [Write('a',1,{'value':3}), Write('b',0,{'value':4})])
        except StorageConflict:
            pass
        else:
            raise AssertionError('版本冲突没有阻止提交')
        assert (await storage.read(scope,'a')).value == {'value':1}
        checks.append('atomic-rollback')
        async def update():
            try:
                await storage.atomic(scope, [Write('a',1,{'value':5})])
                return True
            except StorageConflict:
                return False
        assert sum(await asyncio.gather(update(),update())) == 1
        checks.append('concurrent-cas')
        docs = [MemoryDocument('1'*32,'user_input','source1','苹果交付',(1.,0.),'hash1'),
                MemoryDocument('2'*32,'user_input','source2','合同审阅',(0.,1.),'hash2')]
        await storage.index(scope, space_id='contract-v1', dimensions=2, documents=docs)
        await storage.index(scope, space_id='contract-v1', dimensions=2, documents=docs)
        hits = await storage.search(scope, space_id='contract-v1', dimensions=2,
            task_ids=['1'*32], field='user_input', query='苹果',vector=[1.,0.],limit=5)
        assert hits and all(hit[0]=='1'*32 and hit[1]=='source1' and hit[2]=='hash1' and hit[3]>=0 for hit in hits)
        assert len(hits)==1
        checks.append('hybrid-isolation-and-idempotency')
        try:
            await storage.search(scope, space_id='changed', dimensions=2,
                task_ids=['1'*32],field='user_input',query='苹果',vector=[1.,0.],limit=5)
        except ValueError:
            pass
        else:
            raise AssertionError('向量空间混用没有被拒绝')
        checks.append('embedding-space')
        return checks
    finally:
        await storage.close()
