"""助理持久化契约；只验证数据，不启动消费者、调度器或读取实际配置。"""
from calendar import monthrange
from datetime import date, datetime, timedelta, timezone
from typing import Annotated, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import AwareDatetime, Field, field_validator, model_validator
from .model.question_tree import TreeID, TreeModel, new_tree_id, utc_now

ExternalID = Annotated[str, Field(strict=True, min_length=1, max_length=128, pattern=r'\S')]
Text = Annotated[str, Field(strict=True, min_length=1, pattern=r'\S')]


class AssistantModel(TreeModel):
    @field_validator('*')
    @classmethod
    def normalize_dates(cls, value):
        # 数据库存 UTC；不得把机器本地时区隐式用于提醒或原消息时间。
        if isinstance(value, datetime):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError('时间必须包含时区')
            return value.astimezone(timezone.utc)
        return value


class OnceSchedule(AssistantModel):
    at: AwareDatetime


class IntervalSchedule(AssistantModel):
    start_at: AwareDatetime
    every_seconds: int = Field(strict=True, ge=1, le=2147483647)


class CalendarSchedule(AssistantModel):
    frequency: Literal['daily', 'weekly', 'monthly']
    start_date: date
    local_time: str = Field(strict=True, pattern=r'^(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]$')
    weekdays: tuple[Annotated[int, Field(strict=True, ge=1, le=7)], ...] | None = None
    day_of_month: int | None = Field(default=None, strict=True, ge=1, le=31)
    missing_day: Literal['skip', 'last_day'] | None = None

    @model_validator(mode='after')
    def validate_calendar(self):
        if self.frequency == 'weekly':
            if not self.weekdays or len(set(self.weekdays)) != len(self.weekdays):
                raise ValueError('每周提醒必须指定不重复的星期，周一为 1')
        elif self.weekdays is not None:
            raise ValueError('只有每周提醒可以指定星期')
        if self.frequency == 'monthly':
            if self.day_of_month is None or self.missing_day is None:
                raise ValueError('每月提醒必须指定日期及缺失日期处理规则')
        elif self.day_of_month is not None or self.missing_day is not None:
            raise ValueError('只有每月提醒可以指定月内日期规则')
        return self


class AssistantReminder(AssistantModel):
    id: TreeID = Field(default_factory=new_tree_id)
    content: Text = Field(max_length=10000)
    schedule_type: Literal['once', 'interval', 'calendar']
    schedule_config: OnceSchedule | IntervalSchedule | CalendarSchedule
    timezone: str = Field(default='Asia/Shanghai', strict=True, min_length=1, max_length=64)
    next_fire_at: AwareDatetime | None = None
    end_at: AwareDatetime | None = None
    status: Literal['enabled', 'paused', 'completed', 'cancelled'] = 'enabled'
    created_by_task_id: ExternalID | None = None
    created_at: AwareDatetime = Field(default_factory=utc_now)
    updated_at: AwareDatetime = Field(default_factory=utc_now)

    @model_validator(mode='after')
    def validate_record(self):
        expected = {'once': OnceSchedule, 'interval': IntervalSchedule, 'calendar': CalendarSchedule}
        if not isinstance(self.schedule_config, expected[self.schedule_type]):
            raise ValueError('提醒类型与时间规则不一致')
        try:
            ZoneInfo(self.timezone)
        except (ValueError, ZoneInfoNotFoundError):
            raise ValueError('提醒时区必须是有效 IANA 时区') from None
        if self.status == 'enabled' and self.next_fire_at is None:
            raise ValueError('启用提醒必须有下一次触发时间')
        if self.status in ('completed', 'cancelled') and self.next_fire_at is not None:
            raise ValueError('结束的提醒不能有下一次触发时间')
        if self.end_at is not None and self.next_fire_at is not None and self.next_fire_at > self.end_at:
            raise ValueError('下一次触发超过截止时间')
        if self.next_fire_at is not None:
            config = self.schedule_config
            if isinstance(config, OnceSchedule) and self.next_fire_at != config.at:
                raise ValueError('一次性提醒触发时间必须与规则一致')
            if isinstance(config, IntervalSchedule):
                delta = self.next_fire_at - config.start_at
                if delta < timedelta(0) or delta % timedelta(seconds=config.every_seconds):
                    raise ValueError('间隔提醒必须按起始时刻的固定周期推进')
            if isinstance(config, CalendarSchedule):
                local = self.next_fire_at.astimezone(ZoneInfo(self.timezone))
                if local.date() < config.start_date or local.strftime('%H:%M:%S') != config.local_time or local.microsecond:
                    raise ValueError('日历触发时间与开始日期或本地时刻不一致')
                if config.frequency == 'weekly' and local.isoweekday() not in config.weekdays:
                    raise ValueError('日历触发时间与星期不一致')
                if config.frequency == 'monthly':
                    from calendar import monthrange
                    last = monthrange(local.year, local.month)[1]
                    day = min(config.day_of_month, last) if config.missing_day == 'last_day' else config.day_of_month
                    if local.day != day:
                        raise ValueError('日历触发时间与月内日期不一致')
        if self.updated_at < self.created_at:
            raise ValueError('更新时间不能早于创建时间')
        return self



def next_occurrence(config, zone, after, *, end_at=None):
    """返回严格晚于 after 的时刻；重复的本地时刻仅取第一次，不存在的时刻跳过。"""
    if after.tzinfo is None or after.utcoffset() is None:
        raise ValueError('比较时间必须带时区')
    after = after.astimezone(timezone.utc)
    tz = ZoneInfo(zone)
    candidate = None
    if isinstance(config, OnceSchedule):
        candidate = config.at if config.at > after else None
    elif isinstance(config, IntervalSchedule):
        step = timedelta(seconds=config.every_seconds)
        cycles = max(0, (after - config.start_at) // step + 1)
        try:
            candidate = config.start_at + cycles * step
        except OverflowError:
            return None
    else:
        day = max(config.start_date, after.astimezone(tz).date())
        clock = datetime.strptime(config.local_time, '%H:%M:%S').time()
        while True:
            if end_at is not None and day > end_at.astimezone(tz).date():
                return None
            matches = config.frequency == 'daily'
            if config.frequency == 'weekly':
                matches = day.isoweekday() in config.weekdays
            if config.frequency == 'monthly':
                last = monthrange(day.year, day.month)[1]
                target = min(config.day_of_month, last) if config.missing_day == 'last_day' else config.day_of_month
                matches = day.day == target
            if matches:
                local = datetime.combine(day, clock)
                options = set()
                for fold in (0, 1):
                    value = local.replace(tzinfo=tz, fold=fold).astimezone(timezone.utc)
                    # round-trip 排除夏令时跳跃产生的虚构墙上时刻。
                    if value.astimezone(tz).replace(tzinfo=None) == local:
                        options.add(value)
                if options and min(options) > after:
                    candidate = min(options)
                    break
            try:
                day += timedelta(days=1)
            except OverflowError:
                return None
    if candidate is None or (end_at is not None and candidate > end_at):
        return None
    return candidate.astimezone(timezone.utc)


def describe_schedule(config, zone):
    if isinstance(config, OnceSchedule):
        return '一次性：' + config.at.astimezone(ZoneInfo(zone)).isoformat()
    if isinstance(config, IntervalSchedule):
        return f'从 {config.start_at.astimezone(ZoneInfo(zone)).isoformat()} 起，每 {config.every_seconds} 秒'
    if config.frequency == 'daily':
        rule = '每天'
    elif config.frequency == 'weekly':
        rule = '每周' + '、'.join('一二三四五六日'[day - 1] for day in sorted(config.weekdays))
    else:
        rule = f'每月 {config.day_of_month} 日（日期不存在时' + ('取月末' if config.missing_day == 'last_day' else '跳过该月') + '）'
    return f'{rule} {config.local_time}，从 {config.start_date.isoformat()} 起'
