"""向量统一校验与精度；数据库实现不能决定记忆语义。"""
import math
import struct


def validate_vector(vector, dimensions):
    if len(vector) != dimensions or not all(type(x) in (float, int) and math.isfinite(x) for x in vector):
        raise ValueError('Embedding 维度或数值不合法')
    if not any(vector):
        raise ValueError('Embedding 不能是零向量')


def normalize_vector(vector, dimensions):
    validate_vector(vector, dimensions)
    norm = math.hypot(*vector)
    if not math.isfinite(norm):
        raise ValueError('Embedding 范数超出范围')
    values = [struct.unpack('<f', struct.pack('<f', value/norm))[0] for value in vector]
    validate_vector(values, dimensions)
    return values
