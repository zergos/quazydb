from __future__ import annotations

import json
import inspect

from psycopg import AsyncCursor

from .db_types import *
from .translator import Translator, ArgStr
from .exceptions import *

import typing
if typing.TYPE_CHECKING:
    from typing import *
    from . import DBTable, DBField
    from .db_query import DBQuery, DBSQL, DBJoinKind, DBWithClause, DBQueryField, DBSubqueryField, DBChainedFilter


class TranslatorPSQL(Translator):
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
        StrEnum: 'text',
    }

    TYPES_BY_OID = {
        None: str,
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

    arg_prefix = '%('
    arg_suffix = ')s'

    json_object_func_name = 'jsonb_build_object'

    @classmethod
    def type_name(cls, field: DBField, primary: bool = True) -> str:
        if field.pk and primary:
            return cls.pk_type_name(field.type)
        if field.type in cls.TYPES_MAP:
            return cls.TYPES_MAP[field.type]
        if inspect.isclass(field.type) and issubclass(field.type, Enum):
            return cls.TYPES_MAP[field.type.__bases__[0]]
        # TODO: Decimal
        # TODO: array
        if field.ref:
            return cls.type_name(field.type.DB.pk, False)
        raise QuazyTranslatorException(f'Unsupported DB column type {field.name} ({field.type})')

    @classmethod
    def type_cast(cls, expr: str, typ: type) -> str:
        if typ in cls.TYPES_MAP:
            return f'({expr})::{cls.TYPES_MAP[typ]}'
        raise QuazyTranslatorException(f'Unsupported type ({typ})')

    @classmethod
    def json_serialize(cls, field: DBField, value: str) -> str:
        if field.type is str:
            if type(value) is ArgStr:
                return f'{value}::text'
            else:
                return repr(value)
        if field.type in (int, float, bool, bytes, UUID):
            if field.pk:
                return value
            return f'{value}::{cls.type_name(field)}'
        if field.ref:
            return cls.json_serialize(field.type.DB.pk, value)
        if field.type in (datetime, timedelta):
            return f'CAST(extract(epoch from {value}) as bigint)'
        if field.type in (date, time):
            return f'CAST(extract(epoch from {value}::timestamp) as bigint)'
        raise QuazyFieldTypeError(f'Type `{field.type.__name__}` is not supported for serialization')

    @classmethod
    def json_deserialize(cls, field: DBField, field_path: str) -> str:
        if field.type is str:
            return f'{field_path}::text'
        if field.type in (int, float, bool, bytes, UUID):
            return cls.type_cast(field_path, field.type)
        if field.ref:
            return cls.json_deserialize(field.type.DB.pk, field_path)
        if field.type is datetime:
            return f'to_timestamp(({field_path})::bigint)'
        if field.type is date:
            return f'date(to_timestamp(({field_path})::bigint))'
        if field.type is time:
            return f'to_timestamp(({field_path})::bigint)::time'
        if field.type is timedelta:
            return f"({field_path} || ' seconds')::interval"
        raise QuazyFieldTypeError(f'Type `{field.type.__name__}` is not supported for serialization')

    @classmethod
    def json_merge(cls, field1: str, field2: str) -> str:
        return f'{field1} || {field2}'

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
            res.append(f'DEFAULT {field.default_sql}')
        if field.pk:
            res.append('PRIMARY KEY')
            if field.type is UUID:
                res.append('DEFAULT gen_random_uuid()')
        return ' '.join(res)

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
    def create_schema(cls, name: str) -> str:
        return f'CREATE SCHEMA IF NOT EXISTS {name}'

    @classmethod
    def create_table(cls, table: type[DBTable]) -> str:
        cols = ', '.join(
            f'"{field.column}" {cls.type_name(field)} {cls.column_options(field, table)}'
            for field in table.DB.fields.values()
            #if not field.many_field and not field.prop
            if not field.property
        )
        res = f'CREATE TABLE IF NOT EXISTS {cls.table_name(table)} ({cols})'
        return res

    @classmethod
    def drop_table_by_name(cls, table_name: str) -> str:
        return f'DROP TABLE IF EXISTS {table_name} CASCADE'

    @classmethod
    def add_field(cls, table: type[DBTable], field: DBField) -> str:
        col = f'"{field.column}" {cls.type_name(field)} {cls.column_options(field, table)}'
        res = f'ALTER TABLE {cls.table_name(table)} ADD COLUMN {col}'
        return res

    @classmethod
    def drop_field(cls, table: type[DBTable], field: DBField) -> str:
        res = f'ALTER TABLE {cls.table_name(table)} DROP COLUMN {field.column}'
        return res

    @classmethod
    def rename_field(cls, table: type[DBTable], old_name: str, new_name: str):
        res = f'ALTER TABLE {cls.table_name(table)} RENAME COLUMN {old_name} TO {new_name}'
        return res

    @classmethod
    def alter_field_type(cls, table: type[DBTable], field: DBField) -> str:
        res = f'ALTER TABLE {cls.table_name(table)} ALTER COLUMN {field.column} TYPE {cls.type_name(field)} USING {field.column}::{cls.type_name(field)}'
        return res

    @classmethod
    def rename_table(cls, schema: str, old_table_name: str, new_table_name: str) -> str:
        res = f"ALTER TABLE {cls.table_name_by_schema(schema, old_table_name)} RENAME TO \"{new_table_name}\""
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
        from .db_table import DBTable
        if field.type is dict:
            return json.dumps(value)
        if field.ref and (isinstance(value, DBTable) or isinstance(value, DBTable.ItemGetter)):
            return getattr(value, field.type.DB.pk.name)
        if issubclass(field.type, IntEnum):
            return value.value
        return value

    @classmethod
    def insert(cls, item: DBTable, fields: list[tuple[DBField, Any]]) -> tuple[str, dict[str, Any]]:
        sql_values: list[str] = []
        values: dict[str, Any] = {}
        body_values: dict[str, Any] = {}
        defailt_fields = []
        idx = 1
        for field, value in fields:
            if not inspect.isclass(value) and callable(value):
                value = value()
            if not field.property:  # attr
                if field.default_sql or field.body:
                    continue
                if field.pk:
                    if cls.supports_default:
                        sql_values.append('DEFAULT')
                    else:
                        defailt_fields.append(field)
                elif value is DefaultValue or value is Unassigned:
                    if field.default is Unassigned:
                        # sql_values.append('DEFAULT')
                        defailt_fields.append(field)
                    else:
                        sql_values.append(cls.place_arg(f'v{idx}'))
                        if not callable(field.default):
                            values[f'v{idx}'] = cls.get_value(field, field.default)
                        else:
                            values[f'v{idx}'] = cls.get_value(field, field.default(item))
                        idx += 1
                else:
                    sql_values.append(cls.place_arg(f'v{idx}'))
                    if not callable(value):
                        values[f'v{idx}'] = cls.get_value(field, value)
                    else:
                        values[f'v{idx}'] = cls.get_value(field, value())
                    idx += 1

            else:  # prop
                if field.default_sql:
                    body_values[field.name] = field.default_sql
                elif value is DefaultValue:
                    if field.default is None:
                        body_values[field.name] = 'null'
                    else:
                        body_values[field.name] = cls.json_serialize(field, cls.place_arg(f'v{idx}'))
                        if not callable(field.default):
                            values[f'v{idx}'] = field.default
                        else:
                            values[f'v{idx}'] = field.default(item)
                        idx += 1
                else:
                    body_values[field.name] = cls.json_serialize(field, cls.place_arg(f'v{idx}'))
                    values[f'v{idx}'] = cls.get_value(field, value)
                    idx += 1

        #columns = ','.join(f'"{field.column}"' for field, _ in fields if not field.many_field)
        columns = ','.join(f'"{field.column}"'
                           for field, _ in fields
                           if not field.default_sql and not field.property and not field.body
                           and field not in defailt_fields
                           )
        if not columns and not body_values:
            return f'INSERT INTO {cls.table_name(item)} DEFAULT VALUES RETURNING "{item.DB.pk.column}"', ()

        row = ','.join(sql_values)

        if item.DB.body:
            if columns:
                columns += ','
            columns += f'"{item.DB.body.column}"'
            if body_values:
                body_value = ', '.join(f"'{name}',{value}" for name, value in body_values.items())
                body_value = f'{cls.json_object_func_name}({body_value})'
            else:
                body_value = "'{}'::jsonb"
            if row:
                row += ','
            row += body_value

        res = f'INSERT INTO {cls.table_name(item)} ({columns}) VALUES ({row}) RETURNING "{item.DB.pk.column}"'
        return res, values

    @classmethod
    def clear(cls, table: type[DBTable]) -> str:
        return f'TRUNCATE {cls.table_name(table)}'

    @classmethod
    def delete_related(cls, table: type[DBTable], column: str) -> str:
        return f'DELETE FROM {cls.table_name(table)} WHERE "{column}" = {cls.arg_unnamed}'

    @classmethod
    def update(cls, table: type[DBTable], fields: list[tuple[DBField, Any]], query: DBQuery | str = None) -> tuple[str, dict[str, Any]]:
        sql_values: list[str] = []
        values: dict[str, Any] = {}
        idx = 1
        #filtered = [f for f in fields if not f[0].many_field and not f[0].pk]
        filtered = [f for f in fields if not f[0].pk]
        for field, value in filtered:
            sql_values.append(cls.place_arg(f'v{idx}'))
            values[f'v{idx}'] = cls.get_value(field, value)
            idx += 1

        sets: list[str] = []
        props: list[str] = []
        for field, sql_value in zip(filtered, sql_values):
            if not field[0].property:
                sets.append(f'"{field[0].column}" = {sql_value}')
            else:
                props.append("'{}',{}".format(field[0].column, cls.json_serialize(field[0], sql_value)))

        sets_sql = ', '.join(sets)
        if table.DB.body and props:
            if sets_sql:
                sets_sql += ', '
            body_value = ', '.join(props)
            sets_sql += (f'"{table.DB.body.column}" = '+
                        cls.json_merge(f'"{table.DB.body.column}"',
                                       f'{cls.json_object_func_name}({body_value})'))

        if query is None:
            where_sql = f'"{table.DB.pk.column}" = {cls.place_arg("pk")}'
        elif isinstance(query, str):
            where_sql = query
        else:
            filters = []
            for filter in query.filters:
                filters.append(cls.sql_value(filter))
            where_sql = '\n\tAND '.join(filters) or 'TRUE'

        res = f'UPDATE {cls.table_name(table)} SET {sets_sql} WHERE {where_sql}'
        return res, values

    @classmethod
    def sql_value(cls, value: Union[DBSQL, DBQueryField, str]) -> str:
        from .db_query import DBSQL, DBQueryField
        if isinstance(value, DBSQL):
            return repr(value)
        elif isinstance(value, DBQueryField):
            return str(value)
        return value #.replace("'", "''")

    @classmethod
    def with_select(cls, with_queries: list[DBWithClause]) -> str:
        sql = "WITH\n"
        with_blocks = []
        for sub in with_queries:
            render = cls.select(sub.query, is_root=False)
            render = render.replace(cls.arg_prefix + '_arg_', f'{cls.arg_prefix}_{cls.subquery_name(sub.query)}_arg_')
            block = f'{cls.subquery_name(sub.query)} AS {"NOT MATERIALIZED" if sub.not_materialized else ""} (\n{render})\n'
            with_blocks.append(block)
        sql += ',\n'.join(with_blocks)
        return sql

    @classmethod
    def select(cls, query: DBQuery, chained_mode: int = 0, is_root: bool = True) -> str:
        from .db_query import DBJoinKind, DBQueryField, DBQuery
        from .db_table import DBTable

        sql = ''
        if is_root and chained_mode == 0 and query.with_queries:
            sql += cls.with_select(query.with_queries)

        if chained_mode == 0 and query.chained_opts:
            sql_part1 = cls.select(query, 1)
            sql_part2 = cls.select(query, 2)
            chained_sql = f'''
WITH RECURSIVE "_chain" AS (
{sql_part1}
UNION
{sql_part2}
)
'''
            if sql:
                sql = sql.replace("WITH", f'{chained_sql},\n')
            else:
                sql = chained_sql
            sql += f'SELECT * FROM "_chain"'
            return sql

        sql += 'SELECT\n'
        if query.is_distinct:
            sql += 'DISTINCT\n'
        fields = []
        for field, value in query.fields.items():
            fields.append(f'{cls.sql_value(value)} AS "{field}"') if field != '*' else fields.append(cls.sql_value(value))
            if isinstance(value, DBQueryField):
                view = value._table._view_(value)
                if view is not None:
                    fields.append(f'{view} AS "{field}__view"')
        sources = []
        joins = []
        for join_name, join in query.joins.items():
            if join.kind == DBJoinKind.SOURCE:
                if isinstance(join.with_table, DBQuery):
                    sources.append(cls.subquery_name(join.with_table))
                else:
                    sources.append(f'{cls.table_name(join.with_table)} AS "{join_name}"')
            else:
                op = f'{join.kind.value} JOIN'
                if isinstance(join.with_table, DBQuery):
                    sources.append(f'{op} {cls.subquery_name(join.with_table)}')
                else:
                    joins.append(f'{op} {cls.table_name(join.with_table)} AS "{join_name}"' + (f'\n\tON {cls.sql_value(join.condition.format(join_alias=join_name))}' if join.condition else ''))

        if chained_mode == 2:
            joins.append(f'INNER JOIN "_chain" as "_chain" \n\tON "{query.table_class.DB.table}"."{query.chained_opts.id_name}" = "_chain"."{query.chained_opts.next_name}"')

        filters = []
        group_filters = []
        if chained_mode == 1:
            filters.append(f'"{query.table_class.DB.table}"."{query.chained_opts.id_name}" = {query.chained_opts.sql_value}')
        for filter in query.filters:
            filters.append(cls.sql_value(filter))
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
        if sources:
            sql += f'FROM {sources[0]}\n'
        if joins:
            sql += '\n'.join(joins) + '\n'
        if len(sources) > 1:
            sql += ', ' + ', '.join(sources[1:]) + '\n'
        if filters:
            sql += 'WHERE\n\t' + '\n\tAND '.join(filters) + '\n'
        if groups:
            sql += 'GROUP BY\n\t' + ',\n\t'.join(groups) + '\n'
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
        WHERE {cls.table_name(query.table_class)}."{query.table_class.DB.pk.column}" = "{subquery._path}"."{query.table_class.DB.pk.column}"'''

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
            "{primary_index}" = {cls.place_arg("value")}
        '''

    @classmethod
    def delete_many_indices(cls, middle_table: type[DBTable], primary_index: str, secondary_index: str) -> str:
        return f'''DELETE FROM
            {cls.table_name(middle_table)}
        WHERE
            "{primary_index}" = {cls.place_arg("value")} AND
            "{secondary_index}" in %(indices)s
        '''

    @classmethod
    def insert_many_index(cls, middle_table: type[DBTable], primary_index: str, secondary_index: str) -> str:
        return f'''COPY
            {cls.table_name(middle_table)} ("{primary_index}", "{secondary_index}")
        FROM STDIN
        '''
