class QuestionTreeConflict(RuntimeError):
    """权威状态版本冲突，禁止盲目重放外部动作。"""
