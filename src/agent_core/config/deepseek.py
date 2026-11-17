"""外部专家 Responses 配置；与主模型独立，凭据只在首次求助时必填。"""
import os
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, SecretStr, ValidationError, model_validator

from importlib.resources import files


class DeepSeekSettings(BaseModel):
    model_config = ConfigDict(frozen=True, extra='forbid')

    base_url: str = 'https://api.deepseek.com'
    api_key: SecretStr | None = Field(default=None, repr=False)
    model: str = Field(default='deepseek-v4-pro', min_length=1)
    reasoning_effort: Literal['none', 'low', 'high', 'max'] = 'high'
    timeout_seconds: int = Field(default=300, gt=0)
    max_concurrent_requests: int = Field(default=3, gt=0)
    max_completion_tokens: int = Field(default=8192, gt=0)
    search_timeout_seconds: float = Field(default=45, gt=0, le=300, allow_inf_nan=False)
    search_count: int = Field(default=10, ge=1, le=50)
    search_trust_env: bool = False

    @model_validator(mode='after')
    def validate_connection(self):
        url = HttpUrl(self.base_url)
        if url.username or url.password or url.query or url.fragment:
            raise ValueError('专家地址不能包含凭据、查询参数或片段')
        if not self.model.strip() or self.model != self.model.strip():
            raise ValueError('专家模型名称不能为空白或包含首尾空白')
        return self


def load_deepseek_settings(*args, **kwargs):
    raise ValueError("库不读取宿主环境，请显式注入模型配置或客户端")
