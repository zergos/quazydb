from __future__ import annotations

import json
import inspect
from enum import IntEnum

from .db_types import *
from .exceptions import *

import typing
if typing.TYPE_CHECKING:
    from typing import *
    from .db import DBTable, DBField
    from .query import DBQuery, DBSQL, DBJoinKind, DBWithClause, DBQueryField, DBSubqueryField


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
        IntEnum: 'int',
    }

    TYPES_BY_OID = {
        23: int,
        701: float,
        25: str,
        17: bytes,
        1114: datetime,
        1083: time,
        1082: date,
        1186: timedelta,
        16: bool,
        3802: json,
        2950: UUID,
    }

    @classmethod
    def type_name(cls, field: DBField, primary: bool = True) -> str:
        if field.pk and primary:
            return cls.pk_type_name(field.type)
        if field.type in cls.TYPES_MAP:
            return cls.TYPES_MAP[field.type]
        if inspect.isclass(field.type) and issubclass(field.type, IntEnum):
            return cls.TYPES_MAP[IntEnum]
        # TODO: Decimal
        # TODO: array
        if field.ref:
            return cls.type_name(field.type.DB.pk, False)
        raise QuazyTranslatorException(f'Unsupported DB column type {field.name} ({field.type})')

    @classmethod
    def type_cast(cls, field: DBField) -> str:
        if field.type in cls.TYPES_MAP:
            return cls.TYPES_MAP[field.type]
        if field.ref:
            return cls.type_cast(field.type.DB.pk)
        raise QuazyTranslatorException(f'Unsupported DB column type {field.name} ({field.type})')

    @classmethod
    def serialize(cls, field: DBField, value: str) -> str:
        #if field.type is str:
        #    return value
        if field.type in (str, int, float, bool, bytes, UUID):
            return f'{value}::text'
        if field.ref:
            return cls.serialize(field.type.DB.pk, value)
        if field.type in (datetime, timedelta):
            return f'CAST(extract(epoch from {value}) as integer)'
        if field.type in (date, time):
            return f'CAST(extract(epoch from {value}::timestamp) as integer)'
        raise QuazyFieldTypeError(f'Type `{field.type.__name__}` is not supported for serialization')

    @classmethod
    def deserialize(cls, field: DBField, field_path: str) -> str:
        if field.type is str:
            return field_path
        if field.type in (int, float, bool, bytes, UUID):
            return f'CAST({field_path} as {cls.type_cast(field)})'
        if field.ref:
            return cls.deserialize(field.type.DB.pk, field_path)
        if field.type is datetime:
            return f'to_timestamp(({field_path})::integer)'
        if field.type is date:
            return f'date(to_timestamp({field_path}))'
        if field.type is time:
            return f'to_timestamp(({field_path})::integer)::time'
        if field.type is timedelta:
            return f"({field_path} || ' seconds')::interval"
        raise QuazyFieldTypeError(f'Type `{field.type.__name__}` is not supported for serialization')

    @classmethod
    def pk_type_name(cls, ctype: type) -> str:
        if ctype is int:
            return 'serial'
        if ctype is UUID:
            return 'uuid'
        raise QuazyTranslatorException(f'Unsupported DB column serial type {ctype}')

    @classmethod
    def column_options(cls, field: DBField, table: type[DBTable]) -> str:
        res: list[str] = []
        if field.unique:
            res.append('UNIQUE')
        if field.required and not table.DB.extendable:
            res.append('NOT NULL')
        if field.default_sql:
            res.append(f'DEFAULT {field.default_sql!r}')
        if field.pk:
            res.append('PRIMARY KEY')
            if field.type is UUID:
                res.append('DEFAULT gen_random_uuid()')
        return ' '.join(res)

    @classmethod
    def table_name(cls, table: type[DBTable]) -> str:
        schema = table.DB.schema + '"."' if table.DB.schema else ''
        return f'"{schema}{table.DB.table}"'

    @classmethod
    def table_name2(cls, schema: str, table_name: str) -> str:
        schema =  schema + '"."' if schema else ''
        return f'"{schema}{table_name}'

    @classmethod
    def subquery_name(cls, subquery: DBQuery) -> str:
        return subquery.name

    @classmethod
    def create_index(cls, table: type[DBTable], field: DBField) -> str:
        unique = 'UNIQUE' if field.unique else ''
        return f'CREATE {unique} INDEX IF NOT EXISTS {table.DB.table}_{field.column}_index ON {cls.table_name(table)} ("{field.column}")'

    @classmethod
    def drop_index(cls, table: type[DBTable], field: DBField) -> str:
        res = f'DROP INDEX {table.DB.table}_{field.column}_index'
        return res

    @classmethod
    def set_default_value(cls, table: type[DBTable], field: DBField, sql_value: str) -> str:
        res = f'ALTER TABLE {cls.table_name(table)} ALTER COLUMN {field.column} SET DEFAULT {sql_value}'
        return res

    @classmethod
    def create_schema(cls, name: str):
        return f'CREATE SCHEMA IF NOT EXISTS {name}'

    @classmethod
    def create_table(cls, table: type[DBTable]) -> str:
        cols = ', '.join(
            f'"{field.column}" {cls.type_name(field)} {cls.column_options(field, table)}'
            for field in table.DB.fields.values()
            #if not field.many_field and not field.prop
            if not field.prop
        )
        res = f'CREATE TABLE {cls.table_name(table)} ({cols})'
        return res

    @classmethod
    def add_field(cls, table: type[DBTable], field: DBField):
        col = f'"{field.column}" {cls.type_name(field)} {cls.column_options(field)}'
        res = f'ALTER TABLE {cls.table_name(table)} ADD COLUMN {col}'
        return res

    @classmethod
    def drop_field(cls, table: type[DBTable], field: DBField):
        res = f'ALTER TABLE {cls.table_name(table)} DROP COLUMN {field.column}'
        return res

    @classmethod
    def rename_field(cls, table: type[DBTable], old_name: str, new_name: str):
        res = f'ALTER TABLE {cls.table_name(table)} RENAME COLUMN {old_name} TO {new_name}'
        return res

    @classmethod
    def alter_field_type(cls, table: type[DBTable], field: DBField):
        res = f'ALTER TABLE {cls.table_name(table)} ALTER COLUMN {field.column} TYPE {cls.type_name(field)} USING {field.column}::{cls.type_name(field)}'
        return res

    @classmethod
    def drop_table(cls, table: type[DBTable]) -> str:
        res = f'DROP TABLE {cls.table_name(table)}'
        return res

    @classmethod
    def rename_table(cls, schema: str, old_table_name: str, new_table_name: str) -> str:
        res = f"ALTER TABLE {cls.table_name2(schema, old_table_name)} RENAME TO {cls.table_name2(schema, new_table_name)}"
        return res

    @classmethod
    def add_reference(cls, table: type[DBTable], field: DBField) -> str:
        if not field.ref:
            raise QuazyTranslatorException(f'Field {field.name} is not reference')
        if field.required:
            actions = 'ON DELETE CASCADE'
        else:
            actions = 'ON DELETE SET NULL'
        res = f'ALTER TABLE {cls.table_name(table)} ADD CONSTRAINT fk_{table.DB.table}_{field.column} FOREIGN KEY ("{field.column}") REFERENCES {cls.table_name(field.type)} ("{field.type.DB.pk.column}") {actions}'
        return res

    @classmethod
    def drop_reference(cls, table: type[DBTable], field: DBField) -> str:
        if not field.ref:
            raise QuazyTranslatorException(f'Field {field.name} is not reference')
        res = f'ALTER TABLE {cls.table_name(table)} DROP CONSTRAINT fk_{table.DB.table}_{field.column}'
        return res

    @classmethod
    def set_not_null(cls, table: type[DBTable], field: DBField) -> str:
        res = f'ALTER TABLE {cls.table_name(table)} ALTER COLUMN {field.column} SET NOT NULL'
        return res

    @classmethod
    def drop_not_null(cls, table: type[DBTable], field: DBField) -> str:
        res = f'ALTER TABLE {cls.table_name(table)} ALTER COLUMN {field.column} DROP NOT NULL'
        return res

    @classmethod
    def get_value(cls, field: DBField, value: Any) -> Any:
        if field.type is dict:
            return json.dumps(value)
        if field.ref:
            return getattr(value, field.type.DB.pk.name)
        if issubclass(field.type, IntEnum):
            return value.value
        return value

    @classmethod
    def insert(cls, table: type[DBTable], fields: list[tuple[DBField, Any]]) -> tuple[str, dict[str, Any]]:
        sql_values: list[str] = []
        values: dict[str, Any] = {}
        body_values: dict[str, Any] = {}
        idx = 1
        for field, value in fields:
            if not field.prop:  # attr
                if field.default_sql or field.body:
                    continue
                if field.pk:
                    sql_values.append('DEFAULT')
                elif value is DefaultValue:
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

            else:  # prop
                if field.default_sql:
                    body_values[field.name] = field.default_sql
                elif value is DefaultValue:
                    if field.default in None:
                        body_values[field.name] = 'null'
                    else:
                        body_values[field.name] = cls.serialize(field, f'%(v{idx})s')
                        if not callable(field.default):
                            values[f'v{idx}'] = field.default
                        else:
                            values[f'v{idx}'] = field.default()
                        idx += 1
                else:
                    body_values[field.name] = cls.serialize(field, f'%(v{idx})s')
                    values[f'v{idx}'] = cls.get_value(field, value)
                    idx += 1

        #columns = ','.join(f'"{field.column}"' for field, _ in fields if not field.many_field)
        columns = ','.join(f'"{field.column}"' for field, _ in fields if not field.default_sql and not field.prop and not field.body)
        row = ','.join(sql_values)

        if table.DB.body:
            if columns:
                columns += ','
            columns += f'"{table.DB.body.column}"'
            if body_values:
                body_value = ', '.join(f"'{name}',{value}" for name, value in body_values.items())
                body_value = f'json_build_object({body_value})'
            else:
                body_value = "'{}'::jsonb"
            if row:
                row += ','
            row += body_value

        res = f'INSERT INTO {cls.table_name(table)} ({columns}) VALUES ({row}) RETURNING "{table.DB.pk.column}"'
        return res, values

    @classmethod
    def clear(cls, table: type[DBTable]) -> str:
        return f'TRUNCATE {cls.table_name(table)}'

    @classmethod
    def delete_related(cls, table: type[DBTable], column: str) -> str:
        return f'DELETE FROM {cls.table_name(table)} WHERE "{column}" = %s'

    @classmethod
    def update(cls, table: type[DBTable], fields: list[tuple[DBField, Any]]) -> tuple[str, dict[str, Any]]:
        sql_values: list[str] = []
        values: dict[str, Any] = {}
        idx = 2
        #filtered = [f for f in fields if not f[0].many_field and not f[0].pk]
        filtered = [f for f in fields if not f[0].pk]
        for field, value in filtered:
            sql_values.append(f'%(v{idx})s')
            values[f'v{idx}'] = cls.get_value(field, value)
            idx += 1

        sets: list[str] = []
        props: list[str] = []
        for field, sql_value in zip(filtered, sql_values):
            if not field[0].prop:
                sets.append(f'"{field[0].column}" = {sql_value}')
            else:
                props.append("'{}',{}".format(field[0].column, cls.serialize(field[0], sql_value)))

        sets_sql = ', '.join(sets)
        if table.DB.body and props:
            if sets_sql:
                sets_sql += ', '
            body_value = ', '.join(props)
            sets_sql += f'"{table.DB.body.column}" = "{table.DB.body.column}" || json_build_object({body_value})'

        res = f'UPDATE {cls.table_name(table)} SET {sets_sql} WHERE "{table.DB.pk.column}" = %(v1)s'
        return res, values

    @classmethod
    def sql_value(cls, value: Union[DBSQL, DBQueryField, str]) -> str:
        from .query import DBSQL, DBQueryField
        if isinstance(value, DBSQL):
            return repr(value)
        elif isinstance(value, DBQueryField):
            return str(value)
        return value #.replace("'", "''")

    @classmethod
    def with_select(cls, with_queries: list[DBWithClause]):
        sql = "WITH\n"
        with_blocks = []
        for sub in with_queries:
            render = cls.select(sub.query)
            render = render.replace('%(_arg_', f'%(_{cls.subquery_name(sub.query)}_arg_')
            block = f'{cls.subquery_name(sub.query)} AS {"NOT MATERIALIZED" if sub.not_materialized else ""} (\n{render})\n'
            with_blocks.append(block)
        sql += ',\n'.join(with_blocks)
        return sql

    @classmethod
    def select(cls, query: DBQuery) -> str:
        from .query import DBJoinKind, DBQueryField
        from .db import DBTable

        sql = ''
        if query.with_queries:
            sql += cls.with_select(query.with_queries)

        sql += 'SELECT\n'
        fields = []
        for field, value in query.fields.items():
            fields.append(f'{cls.sql_value(value)} AS "{field}"') if field != '*' else fields.append(cls.sql_value(value))
            if isinstance(value, DBQueryField):
                view = value._field.type._view(value)
                if view is not None:
                    fields.append(f'{view} AS "{field}__view"')
        joins = []
        for join_name, join in query.joins.items():
            if join.kind == DBJoinKind.SOURCE:
                op = 'FROM'
            else:
                op = f'{join.kind.value} JOIN'
            if inspect.isclass(join.source) and issubclass(join.source, DBTable):
                joins.append(f'{op} {cls.table_name(join.source)} AS "{join_name}"' + (f'\n\tON {cls.sql_value(join.condition)}' if join.condition else ''))
            else:
                joins.append(f'{op} {cls.subquery_name(join.source)}')
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

        if query.window[0] is not None:
            sql += f'OFFSET {query.window[0]}\n'
        if query.window[1] is not None:
            sql += f'LIMIT {query.window[1]}\n'

        # sql = sql % dict((key, f'%({key})s') for key in query.args.keys())
        return sql

    @classmethod
    def delete(cls, table: type[DBTable]) -> str:
        return f'DELETE FROM {cls.table_name(table)}'

    @classmethod
    def delete_selected(cls, query: DBQuery, subquery: DBSubqueryField) -> str:
        sql = ''
        if query.with_queries:
            sql += cls.with_select(query.with_queries)

        sql += f'''DELETE FROM {cls.table_name(query.table_class)} USING "{subquery._path}"
        WHERE {cls.table_name(query.table_class)}."{query.table_class.DB.pk.name}" = "{subquery._path}"."{query.table_class.DB.pk.name}"'''

        return sql

    @classmethod
    def select_all_tables(cls) -> str:
        return """SELECT
        	table_schema as schema, table_name as table
        FROM
        	information_schema.tables
        WHERE
        	table_type LIKE 'BASE TABLE' AND table_schema NOT IN ('pg_catalog', 'information_schema')"""

    @classmethod
    def is_table_exists(cls, table: DBTable) -> str:
        return f"""SELECT EXISTS (
        SELECT FROM 
            pg_tables
        WHERE 
            schemaname = '{table.DB.schema}' AND 
            tablename  = '{table.DB.table}'
        )"""

    @classmethod
    def select_many_indices(cls, middle_table: type[DBTable], primary_index: str, secondary_index: str) -> str:
        return f'''SELECT array_agg("{secondary_index}") FROM
            {cls.table_name(middle_table)}
        WHERE
            "{primary_index}" = %(value)s
        '''

    @classmethod
    def delete_many_indices(cls, middle_table: type[DBTable], primary_index: str, secondary_index: str) -> str:
        return f'''DELETE FROM
            {cls.table_name(middle_table)}
        WHERE
            "{primary_index}" = %(value)s AND
            "{secondary_index}" in %(indices)s
        '''

    @classmethod
    def insert_many_index(cls, middle_table: type[DBTable], primary_index: str, secondary_index: str) -> str:
        return f'''INSERT INTO
            {cls.table_name(middle_table)} ("{primary_index}", "{secondary_index}")
        VALUES
            (%(value)s, %(index)s)
        '''
