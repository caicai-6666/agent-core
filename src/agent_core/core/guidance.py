"""模式注入的临时请求提示协议与开场/周期进展提醒策略；每次运行独立持有状态。"""
from collections.abc import Callable
from dataclasses import dataclass
from math import isfinite
import time
from typing import Final, Protocol, TYPE_CHECKING

from .prompt.guidance import build_system_guidence_message

if TYPE_CHECKING:
    from .model import CoreSettings

DEFAULT_OPENING_PROMPT: Final[str] = (
    '遇到需要多步检索或分析的复杂任务时，适时调用 emit_progress 向用户汇报阶段性进展；简单任务可直接完成。'
)
DEFAULT_REMINDER_PROMPT: Final[str] = (
    '请调用 emit_progress 简要汇报当前阶段进展、已确认事实与下一步计划；若已具备完整答案，可直接调用 finish_task。'
)


class RequestGuidance(Protocol):
    def before_request(self, round_number: int) -> dict[str, str] | None:
        """返回本次模型请求专用消息；不写入历史、工作区或摘要。"""
        ...

    def after_output(self, kind: str) -> None:
        """只在公开输出提交并成功交付后通知；失败和取消不调用。"""
        ...


def _validate_progress_args(
    *,
    interval_seconds: float | None,
    opening_prompt: str | None,
    reminder_prompt: str | None,
    clock: Callable[[], float],
) -> float | None:
    if interval_seconds is not None:
        if isinstance(interval_seconds, bool) or not isinstance(interval_seconds, (int, float)):
            raise TypeError('interval_seconds 必须为正数或 None')
        if not isfinite(interval_seconds) or interval_seconds <= 0:
            raise ValueError('interval_seconds 必须为有限正数或 None')
        interval_seconds = float(interval_seconds)
    if opening_prompt is not None and (not isinstance(opening_prompt, str) or not opening_prompt.strip()):
        raise ValueError('opening_prompt 必须为非空字符串或 None')
    if reminder_prompt is not None and (not isinstance(reminder_prompt, str) or not reminder_prompt.strip()):
        raise ValueError('reminder_prompt 必须为非空字符串或 None')
    if not callable(clock):
        raise TypeError('clock 必须可调用')
    return interval_seconds


@dataclass(frozen=True)
class _ProgressGuidanceFactory:
    """支持值相等比较的可复用工厂，便于持久会话恢复时核对配置一致性。"""
    interval_seconds: float | None
    opening_prompt: str | None
    reminder_prompt: str | None
    clock: Callable[[], float]

    def __call__(self, options: 'CoreSettings') -> 'ProgressGuidance':
        return ProgressGuidance(
            interval_seconds=self.interval_seconds,
            opening_prompt=self.opening_prompt,
            reminder_prompt=self.reminder_prompt,
            clock=self.clock,
        )


class ProgressGuidance:
    """开场提示与周期性阶段进展提醒；仅生成本次请求专用消息，不写入历史或工作区。"""

    def __init__(
        self,
        *,
        interval_seconds: float | None = 60.0,
        opening_prompt: str | None = DEFAULT_OPENING_PROMPT,
        reminder_prompt: str | None = DEFAULT_REMINDER_PROMPT,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.interval_seconds = _validate_progress_args(
            interval_seconds=interval_seconds,
            opening_prompt=opening_prompt,
            reminder_prompt=reminder_prompt,
            clock=clock,
        )
        self.opening_prompt = opening_prompt
        self.reminder_prompt = reminder_prompt
        self._clock = clock
        self._last_time: float | None = None

    def before_request(self, round_number: int) -> dict[str, str] | None:
        now = self._clock()
        if round_number == 1:
            # 首轮记录起始时间；若配置了开场提示，则仅附加到本次模型请求。
            self._last_time = now
            if self.opening_prompt is None:
                return None
            return build_system_guidence_message(
                kind='action_guidance',
                reason='任务开始，若涉及多步处理需保持阶段进展对用户可见。',
                required_action=self.opening_prompt,
            )
        if self.interval_seconds is None or self.reminder_prompt is None:
            return None
        if self._last_time is None:
            self._last_time = now
            return None
        if now - self._last_time >= self.interval_seconds:
            # 触发周期提醒后重置计时起点，避免后续每一轮重复提醒。
            self._last_time = now
            return build_system_guidence_message(
                kind='action_guidance',
                reason='当前已持续执行一段时间且尚未向用户汇报新的阶段进展。',
                required_action=self.reminder_prompt,
            )
        return None

    def after_output(self, kind: str) -> None:
        # 每次公开输出（含 emit_progress / finish_task）确认交付后重置周期计时。
        self._last_time = self._clock()

    @classmethod
    def factory(
        cls,
        *,
        interval_seconds: float | None = 60.0,
        opening_prompt: str | None = DEFAULT_OPENING_PROMPT,
        reminder_prompt: str | None = DEFAULT_REMINDER_PROMPT,
        clock: Callable[[], float] = time.monotonic,
    ) -> Callable[['CoreSettings'], 'ProgressGuidance']:
        """预先校验参数并返回可复用的工厂对象；同参数构造的工厂支持 == 比较。"""
        normalized_interval = _validate_progress_args(
            interval_seconds=interval_seconds,
            opening_prompt=opening_prompt,
            reminder_prompt=reminder_prompt,
            clock=clock,
        )
        return _ProgressGuidanceFactory(
            interval_seconds=normalized_interval,
            opening_prompt=opening_prompt,
            reminder_prompt=reminder_prompt,
            clock=clock,
        )
