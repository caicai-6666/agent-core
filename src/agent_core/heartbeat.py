"""仅可信心跳使用静默收尾；业务定制的结束工具继续遵循其自身交付契约。"""
from pydantic import Field
from .core.tool import OutputArguments, RegisteredTool, ToolExecutionResult
from .core.tool.hooks import FeedbackOnlyHook
from .core.tool.feedback import with_feedback


class ScheduledFinishArguments(OutputArguments):
    send_to_user: bool = Field(strict=True, description='是否向用户交付本次检查结果。无值得通知的新事项时填写 false。')


class HeartbeatFinishArguments(ScheduledFinishArguments):
    prune_current_task: bool = Field(default=False, strict=True, description='仅在本次心跳未执行其他动作、无新结论且静默结束时填 true，清理本次空转记录。')


def scheduled_finish(journal):
    heartbeat = journal.task.source == 'heartbeat'

    def execute(args):
        prune = heartbeat and args.prune_current_task
        reason = journal.prune_rejection() if prune else None
        if prune and args.send_to_user:
            reason = '本次要求对外通知，必须保留交付记录。'
        if reason:
            return ToolExecutionResult(status='failed', tool_result={'message': reason +
                ' 本次尚未结束或裁剪；请将 prune_current_task 改为 false，保留处理结果。'})
        return ToolExecutionResult(status='succeeded', tool_result={'prune': prune})

    def render(args, execution, context):
        text = ('状态：本轮任务已结束。' if execution.result.status == 'succeeded'
                else execution.result.tool_result['message'])
        return with_feedback(execution, ('### 心跳检查' if heartbeat else '### 定时任务') + '\n\n' + text)

    description = ('结束本次心跳，说明已确认的结果。例如仅检查现有上下文且无事可做时，可静默结束并裁剪；'
                   '执行过工具或需要通知用户时须保留记录。' if heartbeat else
                   '结束本次定时任务，保存已确认的结果。例如提醒需要用户阅读时选择通知；仅内部核查时静默保存。'
                   '任务未完成时说明具体限制，不声称已完成。')
    return RegisteredTool('finish_task', description, HeartbeatFinishArguments if heartbeat else ScheduledFinishArguments,
        lambda name, raw, workspace, args: workspace, executor=execute, output_kind='final',
        deliver_output=lambda args: args.send_to_user, hooks=(FeedbackOnlyHook(render),), persist_result=True)
