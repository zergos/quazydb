from __future__ import annotations

import json
from .db_types import *
from .exceptions import *

import typing
if typing.TYPE_CHECKING:
    from typing import *
    from .db import DBTable, DBField
    from .query import DBQuery, DBSQL, DBJoinKind


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
            if not field.many_field and not field.prop
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
    def insert(cls, table: Type[DBTable], fields: List[Tuple[DBField, Any]]) -> Tuple[str, Dict[str, any]]:
        sql_values: List[str] = []
        values: Dict[str, Any] = {}
        idx = 1
        for field, value in fields:
            if field.many_field:
                continue
            if field.pk:
                sql_values.append('DEFAULT')
            elif value == DefaultValue:
                if field.default is None:
                    sql_values.append('DEFAULT')
                else:
                    sql_values.append(f'%(v{idx})s')
                    if not callable(field.default):
                        values[f'v{idx}'] = field.default
                    else:
                        values[f'v{idx}'] = field.default()
                    idx += 1
            else:
                sql_values.append(f'%(v{idx})s')
                values[f'v{idx}'] = cls.get_value(field, value)
                idx += 1

        columns = ','.join(f'"{field.column}"' for field, _ in fields if not field.many_field)
        row = ','.join(sql_values)
        res = f'INSERT INTO {cls.table_name(table)} ({columns}) VALUES ({row}) RETURNING "{table._pk_.column}"'
        return res, values

    @classmethod
    def clear(cls, table: Type[DBTable]) -> str:
        return f'TRUNCATE {cls.table_name(table)}'

    @classmethod
    def delete_related(cls, table: Type[DBTable], column: str) -> str:
        return f'DELETE FROM {cls.table_name(table)} WHERE "{column}" = %s'

    @classmethod
    def update(cls, table: Type[DBTable], fields: List[Tuple[DBField, Any]]) -> Tuple[str, Dict[str, any]]:
        sql_values: List[str] = []
        values: Dict[str, Any] = {}
        idx = 2
        filtered = [f for f in fields if not f[0].many_field and not f[0].pk]
        for field, value in filtered:
            sql_values.append(f'%(v{idx})s')
            values[f'v{idx}'] = cls.get_value(field, value)
            idx += 1

        sets: List[str] = []
        for field, sql_value in zip(filtered, sql_values):
            sets.append(f'"{field[0].column}" = {sql_value}')

        sets_sql = ', '.join(sets)
        res = f'UPDATE {cls.table_name(table)} SET {sets_sql} WHERE "{table._pk_.column}" = %(v1)s'
        return res, values

    @classmethod
    def sql_value(cls, value: Union[DBSQL, str]):
        from .query import DBSQL
        if isinstance(value, DBSQL):
            return repr(value)
        return value.replace("'", "''")

    @classmethod
    def select(cls, query: DBQuery):
        from .query import DBJoinKind

        sql = 'SELECT\n'
        fields = []
        for field, value in query.fields.items():
            fields.append(f'{cls.sql_value(value)} AS "{field}"')
        joins = []
        for join_name, join in query.joins.items():
            if join.kind == DBJoinKind.SOURCE:
                op = 'FROM'
            else:
                op = f'{join.kind.value} JOIN'
            joins.append(f'{op} {cls.table_name(join.source)} AS "{join_name}"' + (f'\n\tON {cls.sql_value(join.condition)}' if join.condition else ''))
        filters = []
        group_filters = []
        for filter in query.filters:
            if not filter.aggregated:
                filters.append(cls.sql_value(filter))
            else:
                group_filters.append(cls.sql_value(filter))
        for group_filter in query.group_filters:
            group_filters.append(cls.sql_value(group_filter))
        groups = []
        for group in query.groups:
            groups.append(cls.sql_value(group))
        if not groups and query.has_aggregates:
            for n, field in enumerate(query.fields.values()):
                if not field.aggregated:
                    groups.append(f'{n+1}')
        orders = []
        for order in query.sort_list:
            orders.append(cls.sql_value(order))

        if not fields:
            raise QuazyTranslatorException('No fields selected')

        sql += '\t' + ',\n\t'.join(fields) + '\n'
        sql += '\n'.join(joins) + '\n'
        if filters:
            sql += 'WHERE\n\t' + '\n\tAND '.join(filters) + '\n'
        if groups:
            sql += 'GROUP BY\n\t' + '\n\t'.join(groups) + '\n'
            if group_filters:
                sql += 'HAVING\n\t' + '\n\tAND '.join(group_filters) + '\n'
        if orders:
            sql += 'ORDER BY\n\t' + '\n\t'.join(orders) + '\n'

        # sql = sql % dict((key, f'%({key})s') for key in query.args.keys())
        return sql
