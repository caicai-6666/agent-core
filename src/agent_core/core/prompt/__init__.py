"""主助手稳定提示词公共入口：角色、交互与工作区按需模块化装配。"""
from collections.abc import Callable, Collection
from dataclasses import dataclass
from typing import Literal

from agent_core.infrastructure.tool_tag import tool_tag_section
from .role import AGENT_CORE_ROLE_PROMPT
from .interaction import AGENT_CORE_INTERACTION_PROMPT_TEMPLATE, build_agent_core_interaction_prompt
from .workspace import AGENT_CORE_WORKSPACE_PROMPT, build_agent_core_workspace_prompt
from .guidance import SYSTEM_GUIDENCE_TEMPLATE, SystemGuidenceKind, build_system_guidence_message

# 识别工作区能力的工具集合；自动模式下只要装配其中任意工具或存在非空历史工作区即加载工作区协议与消息。
WORKSPACE_TOOL_NAMES = frozenset({'workspace_add', 'workspace_replace', 'workspace_delete'})


@dataclass(frozen=True)
class PromptOptions:
    """系统提示词与工作区上下文装配选项；不可变且支持按值比较与哈希。"""

    workspace: Literal['auto'] | bool = 'auto'
    compaction: Literal['auto'] | bool = 'auto'
    interaction: bool = True
    relative_time: bool = True
    output_guidance: str = ''
    prompt_builder: Callable[..., str] | None = None

    def __post_init__(self) -> None:
        if self.workspace != 'auto' and type(self.workspace) is not bool:
            raise ValueError("workspace 必须为 'auto' 或布尔值")
        if self.compaction != 'auto' and type(self.compaction) is not bool:
            raise ValueError("compaction 必须为 'auto' 或布尔值")
        if type(self.interaction) is not bool:
            raise ValueError('interaction 必须为布尔值')
        if type(self.relative_time) is not bool:
            raise ValueError('relative_time 必须为布尔值')
        if not isinstance(self.output_guidance, str):
            raise TypeError('output_guidance 必须为字符串')
        if self.prompt_builder is not None and not callable(self.prompt_builder):
            raise TypeError('prompt_builder 必须可调用或为 None')


def resolve_prompt_flags(
    options: PromptOptions,
    tool_names: Collection[str],
    *,
    has_nonempty_workspace: bool = False,
) -> tuple[bool, bool]:
    """根据 PromptOptions、已装配工具名称及已有工作区状态解析 (use_workspace, use_compaction)。"""
    if not isinstance(options, PromptOptions):
        raise TypeError('prompt_options 必须为 PromptOptions 实例')
    if type(has_nonempty_workspace) is not bool:
        raise ValueError('has_nonempty_workspace 必须为布尔值')
    names = set(tool_names)
    if options.workspace == 'auto':
        use_workspace = bool(names & WORKSPACE_TOOL_NAMES) or has_nonempty_workspace
    else:
        # 显式布尔值优先于历史工作区内容，允许调用方强制卸载或强制加载。
        use_workspace = options.workspace
    if options.compaction == 'auto':
        use_compaction = 'compact_context' in names
    else:
        use_compaction = options.compaction
    return use_workspace, use_compaction


def build_system_prompt(
    *,
    role: str = AGENT_CORE_ROLE_PROMPT,
    tool_call_template: str = '',
    output_guidance: str = '',
    include_interaction: bool = True,
    include_workspace: bool = True,
    include_compaction: bool = True,
    include_relative_time: bool = True,
) -> str:
    if not isinstance(role, str) or not role.strip():
        raise ValueError('角色提示词不能为空')
    if (
        type(include_interaction) is not bool
        or type(include_workspace) is not bool
        or type(include_compaction) is not bool
        or type(include_relative_time) is not bool
    ):
        raise ValueError('系统提示词模块开关必须为布尔值')
    if not isinstance(tool_call_template, str) or not isinstance(output_guidance, str):
        raise TypeError('tool_call_template 与 output_guidance 必须为字符串')

    sections = [role.strip()]
    if include_interaction:
        sections.append(
            build_agent_core_interaction_prompt(
                tool_call_template=tool_call_template,
                output_guidance=output_guidance,
                include_workspace=include_workspace,
                include_compaction=include_compaction,
                include_relative_time=include_relative_time,
            )
        )
    else:
        # 关闭默认交互说明时仍按需附加工具标签格式与自定义输出指引，保障标签式模型正常工作。
        tag_section = tool_tag_section(tool_call_template)
        if tag_section:
            sections.append(tag_section)
        if output_guidance.strip():
            sections.append(output_guidance.strip())
    if include_workspace:
        sections.append(build_agent_core_workspace_prompt(include_compaction=include_compaction))
    return '\n\n'.join(part for part in sections if part)


SYSTEM_PROMPT = build_system_prompt()

__all__ = [
    'AGENT_CORE_ROLE_PROMPT',
    'AGENT_CORE_INTERACTION_PROMPT_TEMPLATE',
    'build_agent_core_interaction_prompt',
    'AGENT_CORE_WORKSPACE_PROMPT',
    'build_agent_core_workspace_prompt',
    'SYSTEM_GUIDENCE_TEMPLATE',
    'SystemGuidenceKind',
    'build_system_guidence_message',
    'WORKSPACE_TOOL_NAMES',
    'PromptOptions',
    'resolve_prompt_flags',
    'build_system_prompt',
    'SYSTEM_PROMPT',
]
