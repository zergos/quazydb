from __future__ import annotations

import json
from .db_types import *
from .exceptions import *

import typing
if typing.TYPE_CHECKING:
    from typing import *
    from .db import DBTable, DBField


class Translator:
    TYPES_MAP = {
        int: 'integer',
        float: 'double precision',
        str: 'text',
        bytes: 'bytea',
        datetime: 'timestamp',
        time: 'time',
        date: 'date',
        timedelta: 'interval',
        bool: 'boolean',
        dict: 'jsonb',
        UUID: 'uuid',
    }

    @classmethod
    def type_name(cls, field: DBField, primary: bool = True) -> str:
        if field.pk and primary:
            return cls.pk_type_name(field.type)
        if field.type in cls.TYPES_MAP:
            return cls.TYPES_MAP[field.type]
        # TODO: Decimal
        # TODO: enum
        # TODO: array
        if field.ref:
            return cls.type_name(field.type._pk_, False)
        raise QuazyTranslatorException(f'Unsupported DB column type {field.name} ({field.type})')

    @classmethod
    def pk_type_name(cls, ctype: Type) -> str:
        if ctype is int:
            return 'serial'
        if ctype is UUID:
            return 'uuid'
        raise QuazyTranslatorException(f'Unsupported DB column serial type {ctype}')

    @classmethod
    def column_options(cls, field: DBField) -> str:
        res: List[str] = []
        if field.unique:
            res.append('UNIQUE')
        if field.required:
            res.append('NOT NULL')
        if field.default_sql:
            res.append(f'DEFAULT {field.default_sql!r}')
        if field.pk:
            res.append('PRIMARY KEY')
            if field.type is UUID:
                res.append('DEFAULT gen_random_uuid()')
        return ' '.join(res)

    @classmethod
    def table_name(cls, table: Type[DBTable]) -> str:
        schema = table._schema_ + '"."' if table._schema_ else ''
        return f'"{schema}{table._table_}"'

    @classmethod
    def create_index(cls, table: Type[DBTable], field: DBField) -> str:
        unique = 'UNIQUE' if field.unique else ''
        return f'CREATE {unique} INDEX ON {cls.table_name(table)} ("{field.column}")'

    @classmethod
    def create_table(cls, table: Type[DBTable]) -> str:
        cols = ', '.join(
            f'"{field.column}" {cls.type_name(field)} {cls.column_options(field)}'
            for field in table.fields.values()
            if field.type is not Many and not field.prop
        )
        res = f'CREATE TABLE {cls.table_name(table)} ({cols})'
        return res

    @classmethod
    def add_reference(cls, table: Type[DBTable], field: DBField) -> str:
        if not field.ref:
            raise QuazyTranslatorException(f'Field {field.name} is not reference')
        if field.required:
            actions = 'ON DELETE CASCADE'
        else:
            actions = 'ON DELETE SET NULL'
        res = f'ALTER TABLE {cls.table_name(table)} ADD CONSTRAINT fk_{table._table_}_{field.column} FOREIGN KEY ({field.column}) REFERENCES {cls.table_name(field.type)} ("{field.type._pk_.column}") {actions}'
        return res

    @classmethod
    def get_value(cls, field: DBField, value: Any) -> Any:
        if field.type is dict:
            return json.dumps(value)
        if field.ref:
            return value.id
        return value

    @classmethod
    def insert(cls, table: Type[DBTable], fields: List[Tuple[DBField, Any]]) -> Tuple[str, List[any]]:
        sql_values: List[str] = []
        values: List = []
        idx = 1
        for field, value in fields:
            if field.pk:
                sql_values.append('DEFAULT')
            elif value == DefaultValue:
                if field.default is None:
                    sql_values.append('DEFAULT')
                else:
                    sql_values.append(f'${idx}')
                    idx += 1
                    if not callable(field.default):
                        values.append(field.default)
                    else:
                        values.append(field.default())
            else:
                sql_values.append(f'${idx}')
                idx += 1
                values.append(cls.get_value(field, value))

        columns = ','.join(f'"{field.column}"' for field, _ in fields)
        row = ','.join(sql_values)
        res = f'INSERT INTO {cls.table_name(table)} ({columns}) VALUES ({row}) RETURNING "{table._pk_.column}"'
        return res, values
