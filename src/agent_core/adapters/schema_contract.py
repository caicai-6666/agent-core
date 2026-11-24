"""仅校验库依赖的列；业务扩展列由数据库默认值或建行回调提供。"""
from pathlib import Path
import re

COLUMNS = {
    'records': ('scope', 'key', 'version', 'value'),
    'spaces': ('scope', 'space_id', 'dimensions'),
    'memory': ('scope', 'task_id', 'field', 'source_id', 'text', 'vector', 'source_hash'),
    'file_notes': ('scope', 'file_id', 'name', 'summary', 'updated_at'),
}
KEYS = {
    'records': ('scope', 'key'),
    'spaces': ('scope',),
    'memory': ('scope', 'task_id', 'source_id'),
    'file_notes': ('scope', 'file_id'),
}


SCHEMA_VERSION = 1


def schema_sql(dialect, *, from_version=0):
    """空库或未标版本的旧 SQLite 库共用幂等基线；未来版本必须显式迁移。"""
    if from_version not in (0, SCHEMA_VERSION):
        raise ValueError('当前库不支持从该结构版本升级')
    if dialect not in ('sqlite','mysql'):
        raise ValueError('不支持的数据库类型')
    return (Path(__file__).parent/'schema'/f'{dialect}.sql').read_text(encoding='utf-8')


def validate_version(row):
    if row is None or row[0] != SCHEMA_VERSION:
        raise ValueError(f'数据库结构版本不兼容，当前库要求版本 {SCHEMA_VERSION}')


def validate_columns(table, actual, expected, dialect):
    for name in expected:
        kind = actual.get(name, '').lower()
        numeric = name in ('version','dimensions')
        if dialect == 'sqlite':
            valid = 'int' in kind if numeric else any(x in kind for x in ('text','char','clob'))
        else:
            valid = kind in ('int','bigint') if numeric else kind in (
                'varchar','varbinary','text','longtext','mediumtext','json')
        if not valid:
            raise ValueError(f'{table}.{name} 缺失或类型不兼容')


def insert_values(table, values, provider):
    # 仅在新行调用。核心字段不允许被宿主覆盖，字段名即使来自配置也须校验。
    extra = provider(table, dict(values)) if provider else {}
    if not isinstance(extra, dict) or any(not isinstance(k,str) or not re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*',k)
            or k.lower() in values for k in extra):
        raise ValueError('扩展字段必须为独立、合法的 SQL 列名')
    merged = {**values, **extra}
    return list(merged), list(merged.values())


def columns_sql(columns):
    return ','.join(f'`{name}`' for name in columns)
