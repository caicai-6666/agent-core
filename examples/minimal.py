"""离线可运行示例；替换两个 Provider 后即可接实际服务，存储及运行机制不变。"""
import argparse
import asyncio
import hashlib
import json
import sqlite3
from pathlib import Path
from uuid import uuid4
from pydantic import BaseModel, Field
from agent_core import AgentRuntime, Models, SQLiteStorage, BaseTool, ToolExecutionResult, schema_sql
from agent_core.infrastructure.mllm import MLLMCompletion, MLLMToolCall


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


async def demo_count(messages, tools, generation):
    # 仅离线替身使用；生产必须换成实际模型 tokenizer。
    return len(json.dumps([messages,tools],ensure_ascii=False))//3


class ScheduleArgs(BaseModel):
    project: str = Field(description='需要查询交付日期的合成项目名称。')


class ProjectSchedule(BaseTool):
    name = 'project_schedule'
    description = '按项目名称查询交付日期。'
    arguments_model = ScheduleArgs
    retry_guidance = '请先向用户确认项目名称，再查询交付日期。'

    def execute(self, arguments):
        if arguments.project != '青禾':
            return self.failure('project_missing', '未找到该项目的交付计划。',
                                '请核对项目名称，本次未取得日期。')
        return ToolExecutionResult(status='succeeded', tool_result={'date': '2026-12-08'})

    def render_success(self, arguments, result, context):
        return f"已查到{arguments.project}项目的交付日期：{result.tool_result['date']}。"


async def main(database):
    Path(database).parent.mkdir(parents=True,exist_ok=True)
    # 示例同时承担首次部署；业务服务启动时直接连接已准备好的库。
    connection = sqlite3.connect(database)
    try:
        connection.executescript(schema_sql('sqlite'))
    finally:
        connection.close()
    storage = SQLiteStorage.connect(database)
    runtime = AgentRuntime(models=Models(DemoChat(), DemoEmbedding(), demo_count), database=storage)
    try:
        agent = await runtime.create(tree_id=uuid4().hex, role='你是项目计划助理，根据查询工具返回的事实回答。',
                                     tools=[ProjectSchedule()], persist_new_nodes=True,
                                     workspace_retention='latest')
        task = await agent.generate('查询青禾项目')
        async for event in task.events():
            print(event.sse(), end='')
        print((await task.result()).answer)
    finally:
        await runtime.close()
        await storage.close()


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--database',required=True,help='显式指定隔离示例数据库路径')
    asyncio.run(main(parser.parse_args().database))
