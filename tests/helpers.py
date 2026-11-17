from pydantic import Field
from agent_core import AgentCore, FinishTask
from agent_core.core.tool import build_registry, OutputArguments
class AssistantArguments(OutputArguments):
    send_to_user: bool = Field(default=False, description="是否向用户交付已提交正文")
class AssistantFinish(FinishTask):
    arguments_model = AssistantArguments
    deliver_output = staticmethod(lambda args: args.send_to_user)
def assistant_core(settings, **kwargs):
    kwargs.pop('user', None)
    return AgentCore(settings, registry=build_registry(finish_task=AssistantFinish()), **kwargs)
def exploration_core(settings, **kwargs):
    return AgentCore(settings, registry=build_registry(finish_task=FinishTask()), **kwargs)
