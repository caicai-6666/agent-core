"""无业务交付语义的中途输出工具；完整最终输出仍使用 FinishTask。"""
from textwrap import dedent
from .core.tool import RegisteredTool, OutputArguments
from .core.tool.hooks import FeedbackOnlyHook
from .core.tool.feedback import with_feedback


def render_progress(arguments, execution, context):
    if execution.result.status != 'succeeded':
        return execution
    return with_feedback(execution, dedent('''
        ### 中途说明

        状态：中途正文已保存，当前任务继续。
        下一步：继续必要的工具操作；完成后使用 finish_task 提交最终答复。
    ''').strip())


def build_progress_tool():
    return RegisteredTool('emit_progress',
        '在长任务中向用户说明已确认的进展、限制或当前发现；正文会被保存，但不结束任务。'
        '不要把计划说成已完成，也不要输出私有推理草稿。',
        OutputArguments, lambda name, raw, workspace, args: workspace,
        output_kind='intermediate', hooks=(FeedbackOnlyHook(render_progress),))
