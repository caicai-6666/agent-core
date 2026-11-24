"""宿主可继承的普通函数工具，沿用公共注册表、执行事实与提交机制。"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, TypeVar

from pydantic import BaseModel

from .cache import CachePool, ReadOnlyCache, ReadWriteCache, WriteOnlyCache, current_tool_cache
from .feedback import with_feedback
from .hooks import FeedbackOnlyHook, ToolHook
from .registry import RegisteredTool
from .result import ToolExecutionResult

T = TypeVar('T')


@dataclass(frozen=True)
class _OnSuccess:
    hook: ToolHook

    def __call__(self, arguments, execution, context):
        if execution.result.status == 'succeeded':
            return self.hook(arguments, execution, context)
        return execution


class BaseTool(ABC):
    """子类声明名称、说明和参数模型，实现执行方法与成功反馈。"""
    name: str
    description: str
    arguments_model: type[BaseModel]
    hooks = ()
    retry_guidance = None
    read_pools: tuple[CachePool[Any], ...] = ()
    write_pools: tuple[CachePool[Any], ...] = ()

    def cache(self, pool: CachePool[T]) -> ReadOnlyCache[T] | WriteOnlyCache[T] | ReadWriteCache[T]:
        """在 execute 期间按声明权限获取缓存池受限句柄。"""
        return current_tool_cache(pool)

    def prepare(self, name, raw, workspace, arguments):
        return workspace

    @abstractmethod
    def execute(self, arguments):
        """返回结构化执行结果，支持异步实现。"""

    @abstractmethod
    def render_success(self, arguments, result, context):
        """从已确认的结构化结果生成普通工具的可读正文。"""

    @staticmethod
    def failure(code, reason, required_action, *, mode='correctable'):
        """由判定失败的业务分支提供稳定原因标识、已知状态和可执行的下一步。"""
        if not all(isinstance(text, str) and text.strip() for text in (code, reason, required_action)):
            raise ValueError('工具失败需要明确原因标识、原因和下一步动作')
        return ToolExecutionResult(status='failed', failure_mode=mode, tool_result={
            'code': code, 'reason': reason, 'required_action': required_action})

    def render_failure(self, arguments, result, context):
        return f"原因：{result.tool_result['reason']}\n下一步：{result.tool_result['required_action']}"

    def _render(self, arguments, execution, context):
        if execution.result.status == 'unknown':
            return execution
        render = self.render_success if execution.result.status == 'succeeded' else self.render_failure
        return with_feedback(execution, render(arguments, execution.result, context))

    def build_registration(self):
        hooks = tuple(_OnSuccess(hook) for hook in self.hooks)
        return RegisteredTool(self.name, self.description, self.arguments_model, self.prepare,
            executor=self.execute, hooks=(*hooks, FeedbackOnlyHook(self._render)),
            retry_guidance=self.retry_guidance,
            read_pools=self.read_pools, write_pools=self.write_pools)
