from __future__ import annotations

import json
import sqlite3
import typing
from collections import namedtuple
from contextlib import contextmanager

from .db_types import *
from .translator_psql import TranslatorPSQL
from .exceptions import *

if typing.TYPE_CHECKING:
    from typing import *
    from .db_table import DBTable
    from .db_field import DBField
    from .db_query import DBQuery, DBSubqueryField

SQLITE_ADAPTERS = [
    (datetime, lambda dt: dt.isoformat()),
    (date, lambda dt: dt.isoformat()),
    (timedelta, lambda dt: str(dt)),
    (bool, lambda dt: 'true' if dt else 'false'),
    (dict, lambda dt: json.dumps(dt)),
    (UUID, lambda dt: str(dt)),
]

SQLITE_CONVERTERS = [
    ("DATETIME", lambda dt: datetime.fromisoformat(dt.decode('ascii'))),
    ("DATE", lambda dt: date.fromisoformat(dt.decode('ascii'))),
    ("TIMEDELTA", lambda dt: timedelta(seconds=float(dt))),
    ("BOOLEAN", lambda dt: dt == b'true'),
    ("JSON", lambda dt: json.loads(dt.decode('UTF-8'))),
    ("UUID", lambda dt: UUID(dt.decode('ascii'))),
]

MULTI_COMMANDS_MARK = '--@\n'

class TranslatorSQLite(TranslatorPSQL):
    TYPES_MAP = {
        int: 'INTEGER',
        float: 'REAL',
        str: 'TEXT',
        bytes: 'BLOB',
        datetime: 'DATETIME',
        time: 'TIME',
        date: 'DATE',
        timedelta: 'TIMEDELTA',
        bool: 'BOOLEAN',
        dict: 'JSON',
        UUID: 'UUID',
        IntEnum: 'INTEGER',
        StrEnum: 'TEXT',
    }

    TYPES_BY_OID = {
        None: str
    }

    supports_schema = False
    supports_default = False
    supports_copy = False

    json_object_func_name = 'json_object'

    arg_prefix = ':'
    arg_suffix = ''
    arg_unnamed = '?'

    @classmethod
    def table_name(cls, table: type[DBTable]) -> str:
        return f'"{table.DB.table}"'

    @classmethod
    def json_serialize(cls, field: DBField, value: str) -> str:
        #if field.type is str:
        #    return value
        if field.type in (str, int, float, bool, bytes, UUID):
            return f'CAST({value} AS TEXT)'
        if field.ref:
            return cls.json_serialize(field.type.DB.pk, value)
        if field.type in (datetime, timedelta):
            return f'unixepoch({value})'
        if field.type in (date, time):
            return f'unixepoch({value})'
        raise QuazyFieldTypeError(f'Type `{field.type.__name__}` is not supported for serialization')

    @classmethod
    def json_deserialize(cls, field: DBField, field_path: str) -> str:
        if field.type is str:
            return field_path
        if field.type in (int, float, bool, bytes, UUID):
            return f'CAST({field_path} as {cls.type_cast(field)})'
        if field.ref:
            return cls.json_deserialize(field.type.DB.pk, field_path)
        if field.type is datetime:
            return f'datetime({field_path}, "unixepoch")'
        if field.type is date:
            return f'date({field_path}, "unixepoch")'
        if field.type is time:
            return f'time({field_path}, "unixepoch")'
        #if field.type is timedelta:
        #    return f"({field_path} || ' seconds')::interval"
        raise QuazyFieldTypeError(f'Type `{field.type.__name__}` is not supported for serialization')


    @classmethod
    def pk_type_name(cls, ctype: type) -> str:
        if ctype is int:
            return 'INTEGER'
        raise QuazyTranslatorException(f'Unsupported DB column serial type {ctype}')

    @classmethod
    def column_options(cls, field: DBField, table: type[DBTable]) -> str:
        res: list[str] = []
        if field.unique:
            res.append('UNIQUE')
        if field.required and not table.DB.extendable:
            res.append('NOT NULL')
        if field.default_sql:
            if field.default_sql == "now()":
                res.append('DEFAULT CURRENT_TIMESTAMP')
            else:
                res.append(f'DEFAULT {field.default_sql}')
        if field.pk:
            res.append('PRIMARY KEY AUTOINCREMENT')
        return ' '.join(res)

    @classmethod
    def delete_selected(cls, query: DBQuery, subquery: DBSubqueryField) -> str:
        sql = ''
        if query.with_queries:
            sql += cls.with_select(query.with_queries)

        sql += f'''DELETE FROM {cls.table_name(query.table_class)}
        WHERE "{query.table_class.DB.pk.column}" IN (
        SELECT "{query.table_class.DB.pk.column}" FROM "{subquery._path}"
        WHERE {cls.table_name(query.table_class)}."{query.table_class.DB.pk.column}" = "{subquery._path}"."{query.table_class.DB.pk.column}")'''

        return sql

    @classmethod
    def select_all_tables(cls) -> str:
        return f"SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"

    @classmethod
    def drop_table_by_name(cls, table_name: str) -> str:
        res = MULTI_COMMANDS_MARK
        res += f'PRAGMA foreign_keys = OFF;\n'
        res += f'DROP TABLE IF EXISTS {table_name};\n'
        res += f'PRAGMA foreign_keys = ON;\n'
        return res

    @classmethod
    def add_reference(cls, table: type[DBTable], field: DBField) -> str:
        if not field.ref:
            raise QuazyTranslatorException(f'Field {field.name} is not reference')
        if field.required:
            actions = 'ON DELETE CASCADE'
        else:
            actions = 'ON DELETE SET NULL'

        res = MULTI_COMMANDS_MARK
        res += f'ALTER TABLE {cls.table_name(table)} ADD COLUMN "{field.column}_ref$" {cls.type_name(field)} REFERENCES {cls.table_name(field.type)} ("{field.type.DB.pk.column}") {actions};\n'
        res += f'UPDATE {cls.table_name(table)} SET "{field.column}_ref$" = "{field.column}" WHERE "{field.column}_ref$" IS NULL;\n'
        res += f'ALTER TABLE {cls.table_name(table)} DROP COLUMN "{field.column}";\n'
        res += f'ALTER TABLE {cls.table_name(table)} RENAME COLUMN "{field.column}_ref$" TO "{field.column}";\n'
        return res

    @classmethod
    def drop_reference(cls, table: type[DBTable], field: DBField) -> str:
        if not field.ref:
            raise QuazyTranslatorException(f'Field {field.name} is not reference')
        res = MULTI_COMMANDS_MARK
        res += f'ALTER TABLE {cls.table_name(table)} ADD COLUMN "{field.column}_ref$" {cls.type_name(field)};\n'
        res += f'UPDATE {cls.table_name(table)} SET "{field.column}_ref$" = "{field.column}" WHERE "{field.column}_ref$" IS NULL;\n'
        res += f'ALTER TABLE {cls.table_name(table)} DROP COLUMN "{field.column}";\n'
        res += f'ALTER TABLE {cls.table_name(table)} RENAME COLUMN "{field.column}_ref$" TO "{field.column}";\n'
        return res

    @classmethod
    def select_many_indices(cls, middle_table: type[DBTable], primary_index: str, secondary_index: str) -> str:
        return f'''SELECT group_concat("{secondary_index}") FROM
            {cls.table_name(middle_table)}
        WHERE
            "{primary_index}" = {cls.place_arg("value")}
        '''

    @classmethod
    def insert_many_index(cls, middle_table: type[DBTable], primary_index: str, secondary_index: str) -> str:
        return f'''INSERT INTO
            {cls.table_name(middle_table)} ("{primary_index}", "{secondary_index}")
        VALUES (:v1, :v2)
        '''

def namedtuple_row(cursor: sqlite3.Cursor, row: Sequence[Any]) -> tuple:
    desc = cursor.description
    return namedtuple("Row", (c[0] for c in desc))._make(row)

def dict_row(cursor: sqlite3.Cursor, row: Sequence[Any]) -> dict[str, Any]:
    fields = [column[0] for column in cursor.description]
    return {key: value for key, value in zip(fields, row)}

def kwargs_row(func: Callable[..., DBTable]) -> Callable[[sqlite3.Cursor, Sequence[Any]], DBTable]:
    def kwargs_row_(cur: sqlite3.Cursor, row: Sequence[Any]) -> DBTable:
        desc = cur.description
        names = [d[0] for d in desc]
        return func(**dict(zip(names, row)))
    return kwargs_row_

class ConnectionFactory(sqlite3.Connection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.execute("PRAGMA foreign_keys = ON")

    @contextmanager
    def cursor(self, binary: bool = False, row_factory = None):
        cur = super().cursor()
        cur.row_factory = row_factory
        yield cur
        cur.close()

    def execute(self, sql: str, values: Optional[Sequence[Any]] = ()) -> Iterable[Any]:
        if sql.startswith(MULTI_COMMANDS_MARK):
            return self.executescript(sql[len(MULTI_COMMANDS_MARK):])
        else:
            return super().execute(sql, values)

    @contextmanager
    def transaction(self):
        yield

def register_sqlite_converters() -> None:
    for typ, conv in SQLITE_CONVERTERS:
        sqlite3.register_converter(typ, conv)
    for typ, conv in SQLITE_ADAPTERS:
        sqlite3.register_adapter(typ, conv)

register_sqlite_converters()
sqlite3.enable_callback_tracebacks(True)
