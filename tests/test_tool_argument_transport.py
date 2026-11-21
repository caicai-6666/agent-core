import json
from typing import Literal

import pytest
from pydantic import BaseModel, Field

from agent_core import RegisteredTool, ToolRegistry
from agent_core.core.json_codec import ModelActionRejected
from agent_core.core.workspace import empty_workspace


class Weekly(BaseModel):
    kind: Literal['weekly'] = Field(description='每周日期列表')
    dates: list[int] = Field(strict=True, description='日期编号')


class Note(BaseModel):
    kind: Literal['note'] = Field(description='说明文本')
    dates: str = Field(description='用户原文')


class Arguments(BaseModel):
    schedule: Weekly | Note = Field(discriminator='kind', description='带类型的规则')
    content: str = Field(description='原样保留的业务内容')


def parse(schedule):
    registry = ToolRegistry([RegisteredTool('schedule', '记录日程', Arguments, lambda n, r, w, a: w)])
    payload = json.dumps({'schedule': schedule, 'content':'[1,2]'})
    return registry.prepare('schedule', payload, empty_workspace())[0]


def test_public_registration_decodes_only_selected_branch():
    weekly = dict(kind='weekly', dates=[1, 2])
    encoded = dict(weekly, dates='[1,2]')
    for value in (weekly, json.dumps(weekly), encoded, json.dumps(encoded)):
        result = parse(value)
        assert result.schedule.dates == [1, 2]
        assert result.content == '[1,2]'
    assert parse(json.dumps(dict(kind='note', dates='[1,2]'))).schedule.dates == '[1,2]'


@pytest.mark.parametrize('schedule', [
    {'kind':'missing', 'dates':'[1]'}, {'kind': [], 'dates':'[1]'},
    {'kind':'weekly', 'dates':'broken'}, {'kind':'weekly', 'dates':'[NaN]'},
    {'kind':'weekly', 'dates':'{"x":1,"x":2}'},
])
def test_invalid_arguments_still_fail(schedule):
    with pytest.raises(ModelActionRejected):
        parse(schedule)
