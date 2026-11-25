"""工具独立的执行后钩子；上下文提供宿主事实与私有审计能力。"""
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from inspect import isawaitable

from pydantic import BaseModel

from agent_core.model.node_persistence import GenerationPayload

from .result import ToolExecution


@dataclass(frozen=True)
class ToolHookContext:
    # 每次 execute 单独绑定，禁止把任务或 journal 缓存在共享钩子实例上。
    record_generation: Callable[[str, GenerationPayload], Awaitable[None]] | None = None

    # 宿主已完成候选验收与容量计算；正文仍须随成功提交后才能展示。
    workspace_feedback: str | None = None

    # 宿主完成摘要验收后提供的完整历史任务数量；不含本轮活动轨迹。
    compacted_turns: int | None = None


ToolHook = Callable[
    [BaseModel, ToolExecution, ToolHookContext],
    ToolExecution | Awaitable[ToolExecution],
]


@dataclass(frozen=True)
class FeedbackOnlyHook:
    """只允许替换反馈内容，保证最终正文不变，因而不阻断既有流式预览。"""

    render: ToolHook

    async def __call__(self, arguments, execution, context):
        original = execution.result.model_copy(deep=True)
        value = self.render(arguments, execution, context)
        value = await value if isawaitable(value) else value
        if (not isinstance(value, ToolExecution)
                or value.output != execution.output
                or value.after_commit is not execution.after_commit
                or value.result.status != original.status
                or value.result.failure_mode != original.failure_mode
                or value.result.tool_result != original.tool_result
                or value.result.system_guidence != original.system_guidence):
            raise ValueError('反馈专用钩子只能修改展示内容')
        return value


NormalizedToolHooks = tuple[tuple[str, tuple[ToolHook, ...]], ...]


def normalize_tool_hooks(
    tool_hooks: Mapping[str, Sequence[ToolHook]] | None,
) -> NormalizedToolHooks:
    """规范化并严格校验按工具名附加的钩子映射，返回不可变且可值比较的有序元组。"""
    if tool_hooks is None:
        return ()
    if not isinstance(tool_hooks, Mapping):
        raise TypeError('tool_hooks 必须为工具名到钩子序列的映射或 None')
    normalized: dict[str, tuple[ToolHook, ...]] = {}
    for name, hooks in tool_hooks.items():
        if not isinstance(name, str) or not name.strip():
            raise ValueError('tool_hooks 的键必须为非空工具名称字符串')
        # 拒绝裸字符串、字节串以及裸可调用对象，避免被按字符拆解或静默误用。
        if not isinstance(hooks, Sequence) or isinstance(hooks, (str, bytes, bytearray)):
            raise TypeError(f'工具 {name!r} 的钩子必须为非空可调用序列')
        hook_tuple = tuple(hooks)
        if not hook_tuple:
            raise ValueError(f'工具 {name!r} 的钩子序列不能为空')
        if any(not callable(hook) for hook in hook_tuple):
            raise TypeError(f'工具 {name!r} 存在不可调用的钩子')
        normalized[name] = hook_tuple
    return tuple(sorted(normalized.items()))
