"""权威父链、统一公开记忆、向量索引刷新和召回回查。"""
from collections import defaultdict
import hashlib
from textwrap import dedent
from .vectors import normalize_vector
from .storage import MemoryDocument
from .core.query_pool import QueryResultPool
from .core.subgraph.memory_retrieval.schema import MemoryRetrievalResult, RankedTask
from .core.context_rendering.history import render_turn
from .core.model import HistoryTurn
from .core.native_messages import messages_from_exchanges


def public_fields(task):
    # 与当前项目一致：三路公开来源；模型思考与私有审计绝不进入记忆索引。
    return {'user_input': task.question,
            'intermediate_output': '\n\n'.join(text for kind,text in task.outputs if kind == 'intermediate'),
            'final_output': '\n\n'.join(text for kind,text in task.outputs if kind == 'final') if task.status == 'complete' else ''}


def public_sources(task):
    sources = [('user_input', task.id, task.question)]
    if len(task.output_ids) != len(task.outputs):
        raise ValueError('公开消息缺少稳定来源 ID')
    for identifier, (kind, text) in zip(task.output_ids, task.outputs):
        if kind == 'final' and task.status != 'complete':
            continue
        sources.append(('final_output' if kind == 'final' else 'intermediate_output', identifier, text))
    return sources


def digest(text):
    return hashlib.sha256(text.encode()).hexdigest()


class Memory:
    def __init__(self, repository, embedding):
        self.repository, self.embedding = repository, embedding

    async def vectors(self, texts, purpose):
        values = await self.embedding.embed(texts, purpose=purpose)
        if len(values) != len(texts):
            raise ValueError('Embedding 数量不匹配')
        return [normalize_vector(value, self.embedding.dimensions) for value in values]

    async def index_task(self, task):
        if task.status == 'generating':
            return
        fields = [(field, source_id, text) for field, source_id, text in public_sources(task) if text.strip()]
        # 只有同一来源内容、同一向量空间都未变化时才跳过计算。
        key = 'embedding:' + task.id + ':' + digest(self.embedding.space_id)
        fingerprints = {source_id: digest(text) for field,source_id,text in fields}
        cached = await self.repository.storage.read(self.repository.scope, key)
        marker = {'fields': fingerprints, 'dimensions': self.embedding.dimensions}
        if cached and cached.value == marker:
            return
        vectors = await self.vectors([text for _,_,text in fields], 'document') if fields else []
        await self.repository.storage.index(self.repository.scope, space_id=self.embedding.space_id,
            dimensions=self.embedding.dimensions, documents=[MemoryDocument(task.id, field, source_id, text, tuple(vector), digest(text))
            for (field,source_id,text), vector in zip(fields, vectors)])
        from .storage import Write, StorageConflict
        try:
            await self.repository.storage.atomic(self.repository.scope, [Write(key, cached.version if cached else None, marker)])
        except StorageConflict:
            # 仅索引缓存标记允许并发刷新；权威任务或工具动作不重试。
            pass

    async def search(self, plan, *, parent_id):
        path = await self.repository.path(parent_id)
        eligible = []
        for task in path:
            stamp = int(task.created_at.timestamp() * 1000)
            if plan.statuses and task.status not in plan.statuses:
                continue
            if plan.created_from is not None and stamp < plan.created_from:
                continue
            if plan.created_before is not None and stamp >= plan.created_before:
                continue
            eligible.append(task)
            await self.index_task(task)
        scores = defaultdict(float)
        for query in plan.queries:
            vector = (await self.vectors([query.query], 'query'))[0]
            rows = await self.repository.storage.search(self.repository.scope,
                space_id=self.embedding.space_id, dimensions=self.embedding.dimensions,
                task_ids=[task.id for task in eligible], field=query.field, query=query.query, vector=vector, limit=50)
            allowed = {task.id for task in eligible}
            for task_id, source_id, source_hash, score in rows:
                if task_id not in allowed:
                    continue
                task, _ = await self.repository.load(task_id)
                source = next((text for field,identifier,text in public_sources(task) if field == query.field and identifier == source_id), None)
                if task.status != 'generating' and source is not None and digest(source) == source_hash:
                    scores[task_id] += score
        return tuple(
            RankedTask(node_id=key, score=value) for key,value in sorted(scores.items(), key=lambda item: (-item[1],item[0]))[:20])

    def pool(self, *, parent_id, capacity=10, page_size=3):
        from types import SimpleNamespace
        from .core.subgraph.memory_retrieval.pool import MemoryQueryPool
        from .model.question_file import AttachmentReference
        async def read(ids):
            allowed = {task.id for task in await self.repository.path(parent_id)}
            tasks = []
            for identifier in ids:
                if identifier not in allowed:
                    raise ValueError('资料不在当前父链范围')
                task, _ = await self.repository.load(identifier)
                fields = public_fields(task)
                content = SimpleNamespace(question=task.question, answer=fields['final_output'],
                    exchanges=task.exchanges, attachments=[AttachmentReference.model_validate(a) for a in task.attachments])
                messages = [SimpleNamespace(kind=kind, text=text) for kind,text in task.outputs]
                tasks.append(SimpleNamespace(write=SimpleNamespace(node=task, content=content), messages=messages))
            return tasks
        return MemoryQueryPool(read, capacity=capacity, page_size=page_size)
