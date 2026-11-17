"""离线可运行示例；替换三个 Provider 后即可接实际服务，存储及运行机制不变。"""
import argparse
import asyncio
import hashlib
import json
from pathlib import Path
from textwrap import dedent
from uuid import uuid4
from pydantic import BaseModel, Field
from agent_core import Agent, Models, SQLiteStorage, RegisteredTool, ToolExecutionResult
from agent_core.infrastructure.mllm import MLLMCompletion, MLLMToolCall
from agent_core.providers import ExpertReply
from agent_core.core.tool.hooks import FeedbackOnlyHook
from agent_core.core.tool.feedback import with_feedback


class DemoChat:
    async def create_chat_completion(self, **kwargs):
        feedback = [m for m in kwargs['messages'] if m['role']=='tool']
        name, args = ('finish_task', {'content':'工具已确认交付日期为 2026-12-08。'}) if feedback else ('project_schedule', {'project':'青禾'})
        call = MLLMToolCall(uuid4().hex, name, json.dumps(args,ensure_ascii=False))
        message={'role':'assistant','content':None,'tool_calls':[{'id':call.call_id,'type':'function',
            'function':{'name':call.name,'arguments':call.arguments}}]}
        if kwargs.get('on_tool_delta'):
            await kwargs['on_tool_delta'](message)
        return MLLMCompletion('demo','demo',message,(call,),'tool_calls',{}, {'message':message})


class DemoEmbedding:
    space_id, dimensions = 'demo-hash-not-semantic-v1', 8
    async def embed(self, texts, *, purpose):
        return [[float(v+1) for v in hashlib.sha256(text.encode()).digest()[:8]] for text in texts]


class DemoExpert:
    async def create_response(self, *, input_items):
        item={'type':'message','role':'assistant','content':[{'type':'output_text','text':'离线专家示例'}]}
        return ExpertReply('demo','demo','离线专家示例','completed',0,0,0,{'output':[item]},(item,))


async def demo_count(messages, tools, generation):
    # 仅离线替身使用；生产必须换成实际模型 tokenizer。
    return len(json.dumps([messages,tools],ensure_ascii=False))//3


class ScheduleArgs(BaseModel):
    project: str = Field(description='需要查询交付日期的合成项目名称。')


def render(args, execution, context):
    return with_feedback(execution, dedent("""
        ### 项目计划

        状态：查询成功
        交付日期：2026-12-08
    """).strip())


async def main(database):
    Path(database).parent.mkdir(parents=True,exist_ok=True)
    storage=SQLiteStorage(database)
    tool=RegisteredTool('project_schedule','查询合成项目的交付日期。',ScheduleArgs,
        lambda n,r,w,a:w,executor=lambda a:ToolExecutionResult(status='succeeded',tool_result={'date':'2026-12-08'}),
        hooks=(FeedbackOnlyHook(render),))
    agent=Agent(models=Models(DemoChat(),DemoEmbedding(),DemoExpert(),demo_count),storage=storage,
        role='你是项目计划助理，根据查询工具返回的事实回答。',tools=[tool])
    try:
        result=await agent.run('查询青禾项目',owner='demo',tree_id='normal')
        print(result.answer)
        async for event in agent.stream('查询青禾项目',owner='demo',tree_id='stream'):
            print(event.sse(),end='')
    finally:
        await agent.close();await storage.close()


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--database',required=True,help='显式指定隔离示例数据库路径')
    asyncio.run(main(parser.parse_args().database))
