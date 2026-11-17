import re

class RegistryFileError(ValueError):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def validate_resource_id(value):
    if (not isinstance(value,str) or re.fullmatch(r'[1-9][0-9]{0,19}',value) is None
            or int(value)>18446744073709551615):
        raise ValueError('文件ID须为注册中心正式资源ID的十进制字符串')
    return value
