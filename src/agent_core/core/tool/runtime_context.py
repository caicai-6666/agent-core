"""工具执行期运行时上下文：通过 ContextVar 向组合工具暴露当前任务的宿主核心、日志与推理档位。"""
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ToolRuntimeContext:
    """单次工具调用期间可见的只读运行时句柄。"""
    core: Any
    journal: Any
    thinking_effort: str


_CURRENT_TOOL_RUNTIME: ContextVar[ToolRuntimeContext | None] = ContextVar(
    'agent_core_tool_runtime', default=None
)


def current_tool_runtime() -> ToolRuntimeContext:
    """在工具 execute 执行期间读取当前任务的运行时上下文；在工具执行外调用时抛出 RuntimeError。"""
    context = _CURRENT_TOOL_RUNTIME.get()
    if context is None:
        raise RuntimeError('只能在工具 execute 执行期间访问工具运行时上下文')
    return context


@contextmanager
def bind_tool_runtime(context: ToolRuntimeContext):
    """在单次工具执行边界内绑定运行时上下文，退出时无条件恢复原值。"""
    if not isinstance(context, ToolRuntimeContext):
        raise TypeError('必须传入 ToolRuntimeContext 实例')
    token = _CURRENT_TOOL_RUNTIME.set(context)
    try:
        yield context
    finally:
        _CURRENT_TOOL_RUNTIME.reset(token)
