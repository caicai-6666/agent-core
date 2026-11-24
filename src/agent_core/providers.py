"""两类模型契约：连接归调用方所有，库不读取环境或猜测供应商协议。"""
from dataclasses import dataclass
from typing import Callable, Protocol, Literal
from pydantic import BaseModel, ConfigDict, Field, model_validator
from .config.mllm import MLLMGenerationSettings, MLLMSettings
from .infrastructure.mllm import MLLMCompletion, MLLMClient
from .infrastructure.vllm_tokenizer import count_chat_tokens


class RuntimeSettings(BaseModel):
    model_config = ConfigDict(frozen=True, extra='forbid')
    context_window_tokens: int = Field(default=32768, gt=0)
    generation: MLLMGenerationSettings = Field(default_factory=MLLMGenerationSettings)
    tool_tag_enabled: Literal[False] = False

    @model_validator(mode='after')
    def validate_budget(self):
        if self.generation.max_completion_tokens >= self.context_window_tokens:
            raise ValueError('最大生成长度必须小于上下文窗口')
        return self


class ChatProvider(Protocol):
    async def create_chat_completion(self, *, messages: list[dict], tools=None,
        tool_choice='auto', generation=None, stream=False, on_raw_chunk=None,
        json_schema=None, on_tool_delta=None) -> MLLMCompletion: ...


class EmbeddingProvider(Protocol):
    # 修改模型、维度、前缀或归一化方法时必须改变 space_id，禁止混用向量空间。
    space_id: str
    dimensions: int

    async def embed(self, texts: list[str], *, purpose: Literal['query', 'document']) -> list[list[float]]: ...


@dataclass(frozen=True)
class Models:
    daily: ChatProvider
    embedding: EmbeddingProvider
    count_tokens: Callable

    def __post_init__(self):
        for value, method in ((self.daily, 'create_chat_completion'),
                              (self.embedding, 'embed')):
            if not callable(getattr(value, method, None)):
                raise TypeError(f'必须提供模型接口 {method}')
        if not callable(self.count_tokens):
            raise TypeError('必须提供与日常模型匹配的 token 计数器')
        if not self.embedding.space_id or type(self.embedding.dimensions) is not int or self.embedding.dimensions < 1:
            raise ValueError('Embedding 必须声明空间标识和正整数维度')


def vllm_counter(settings: MLLMSettings):
    async def count(messages, tools, generation):
        return await count_chat_tokens(messages, tools, settings, generation=generation)
    return count


class OpenAIEmbedding:
    """OpenAI-compatible Embedding（包括 vLLM）；查询/文档前缀显式配置。"""
    def __init__(self, *, client, model: str, space_id: str, dimensions: int,
                 query_prefix='', document_prefix=''):
        self.client, self.model = client, model
        self.space_id, self.dimensions = space_id, dimensions
        self.query_prefix, self.document_prefix = query_prefix, document_prefix

    async def embed(self, texts, *, purpose):
        prefix = self.query_prefix if purpose == 'query' else self.document_prefix
        response = await self.client.embeddings.create(model=self.model, input=[prefix + text for text in texts], encoding_format="float")
        data = sorted(response.data, key=lambda item: item.index)
        if [item.index for item in data] != list(range(len(texts))):
            raise ValueError('Embedding 返回数量或索引不匹配')
        return [list(item.embedding) for item in data]


class VLLMEmbedding(OpenAIEmbedding):
    """沿用现有记忆模型的 ChatML 文本指令，查询和文档使用同一语义规范。"""
    async def embed(self, texts, *, purpose):
        from .core.prompt.embedding import render_text_embedding_input
        return await super().embed([render_text_embedding_input(text) for text in texts], purpose=purpose)


VLLMClient = MLLMClient
VLLMSettings = MLLMSettings
GenerationSettings = MLLMGenerationSettings
