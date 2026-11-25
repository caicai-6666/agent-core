"""仅可信心跳使用静默收尾；满足裁剪契约的自定义结束工具也可接入安全静默裁剪。"""
from dataclasses import replace
from inspect import isawaitable

from pydantic import Field
from .core.tool import OutputArguments, RegisteredTool, ToolExecution, ToolExecutionResult
from .core.tool.result import OrdinaryToolContent
from .core.tool.hooks import FeedbackOnlyHook
from .core.tool.feedback import with_feedback


class ScheduledFinishArguments(OutputArguments):
    send_to_user: bool = Field(strict=True, description='是否向用户交付本次检查结果。无值得通知的新事项时填写 false。')


class HeartbeatFinishArguments(ScheduledFinishArguments):
    prune_current_task: bool = Field(default=False, strict=True, description='仅在本次心跳未执行其他动作、无新结论且静默结束时填 true，清理本次空转记录。')


def supports_heartbeat_pruning(entry: RegisteredTool) -> bool:
    """判断结束工具是否声明了可验证的心跳静默裁剪契约（含 bool 参数与条件交付回调）。"""
    if not isinstance(entry, RegisteredTool):
        return False
    if entry.name != 'finish_task' or entry.output_kind != 'final' or entry.deliver_output is None:
        return False
    fields = entry.arguments_model.model_fields
    send_field = fields.get('send_to_user')
    prune_field = fields.get('prune_current_task')
    return (
        send_field is not None
        and prune_field is not None
        and send_field.annotation is bool
        and prune_field.annotation is bool
    )


def wrap_heartbeat_prune_guard(entry: RegisteredTool, journal) -> RegisteredTool:
    """为满足裁剪契约的自定义 finish_task 包装心跳裁剪前置守卫与同事务裁剪标记。"""
    if not supports_heartbeat_pruning(entry):
        raise ValueError('仅支持满足心跳裁剪契约的 finish_task 工具')

    original_executor = entry.executor
    prune_rejected = False

    async def guarded_executor(args):
        nonlocal prune_rejected
        prune_rejected = False
        prune = bool(getattr(args, 'prune_current_task', False))
        if prune:
            reason = journal.prune_rejection()
            deliver = entry.deliver_output(args)
            if type(deliver) is not bool:
                raise ValueError('输出交付条件必须返回布尔值')
            if getattr(args, 'send_to_user', True) or deliver:
                reason = '本次要求对外通知，必须保留交付记录。'
            if reason:
                prune_rejected = True
                message = reason + ' 本次尚未结束或裁剪；请将 prune_current_task 改为 false，保留处理结果。'
                return ToolExecutionResult(
                    status='failed',
                    failure_mode='correctable',
                    content=OrdinaryToolContent(text='### 心跳检查\n\n' + message),
                    tool_result={'message': message},
                )
        if original_executor is None:
            tool_result = {'status': 'accepted', 'prune': True} if prune else {'status': 'accepted'}
            return ToolExecutionResult(status='succeeded', tool_result=tool_result)
        raw = original_executor(args)
        if isawaitable(raw):
            raw = await raw
        execution = replace(raw) if isinstance(raw, ToolExecution) else ToolExecution(ToolExecutionResult.model_validate(raw))
        if prune and execution.result.status == 'succeeded':
            # 仅在成功路径追加 prune 标记，保留原业务字段供自定义钩子和持久提交消费。
            merged = {**execution.result.tool_result, 'prune': True}
            execution = replace(
                execution,
                result=execution.result.model_copy(update={'tool_result': merged}),
            )
        return execution

    def wrap_hook(hook):
        # 裁剪前置拒绝时跳过自定义业务钩子与渲染钩子，避免读取缺失业务键导致异常；正常路径原样执行。
        if isinstance(hook, FeedbackOnlyHook):
            original_render = hook.render

            def guarded_render(arguments, execution, context):
                if prune_rejected:
                    return execution
                return original_render(arguments, execution, context)

            return FeedbackOnlyHook(guarded_render)

        def guarded_hook(arguments, execution, context):
            if prune_rejected:
                return execution
            return hook(arguments, execution, context)

        return guarded_hook

    wrapped_hooks = tuple(wrap_hook(hook) for hook in entry.hooks)
    return replace(
        entry,
        executor=guarded_executor,
        hooks=wrapped_hooks,
        persist_result=True,
    )


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
        if execution.result.status == 'succeeded':
            text = '状态：本轮任务已结束。'
        else:
            data = execution.result.tool_result
            if 'message' in data:
                text = data['message']
            else:
                text = f"状态：任务未结束\n原因：{data['reason']}\n下一步：{data['required_action']}"
        return with_feedback(execution, ('### 心跳检查' if heartbeat else '### 定时任务') + '\n\n' + text)

    description = ('结束本次心跳，说明已确认的结果。例如仅检查现有上下文且无事可做时，可静默结束并裁剪；'
                   '执行过工具或需要通知用户时须保留记录。' if heartbeat else
                   '结束本次定时任务，保存已确认的结果。例如提醒需要用户阅读时选择通知；仅内部核查时静默保存。'
                   '任务未完成时说明具体限制，不声称已完成。')
    return RegisteredTool('finish_task', description, HeartbeatFinishArguments if heartbeat else ScheduledFinishArguments,
        lambda name, raw, workspace, args: workspace, executor=execute, output_kind='final',
        deliver_output=lambda args: args.send_to_user, hooks=(FeedbackOnlyHook(render),), persist_result=True)
