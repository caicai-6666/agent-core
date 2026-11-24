"""按单次任务统计重复失败；计数与工具对象及持久历史分离。"""

DEFAULT_RETRY_GUIDANCE = '请暂停以当前方式调用此工具。优先使用已有结果或其他可用方法；缺少必要信息时请求补充，无法继续时如实说明限制。'


class ToolFailureTracker:
    def __init__(self, max_errors):
        # 默认第二次同原因失败时提示；上限为二时只剩一次反馈机会。
        self.threshold = max(1, min(2, max_errors - 1))
        self.counts = {}

    def failure(self, name, reason, guidance=None):
        if name is None:
            return ''
        causes = self.counts.setdefault(name, {})
        causes[reason] = causes.get(reason, 0) + 1
        if causes[reason] < self.threshold:
            return ''
        return guidance or DEFAULT_RETRY_GUIDANCE

    def feedback(self, text, name, reason):
        advice = self.failure(name, reason)
        return f'{text}\n{advice}' if advice else text

    def success(self, name):
        self.counts.pop(name, None)
