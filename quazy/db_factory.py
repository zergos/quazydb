from __future__ import annotations

import sys
import inspect
from collections import defaultdict
from contextlib import contextmanager

import psycopg
import psycopg_pool
from psycopg.rows import namedtuple_row, class_row, dict_row

from .exceptions import *
from .db_table import *
from .db_field import *
from .db_types import *
from .translator import Translator

import typing

if typing.TYPE_CHECKING:
    from typing import *
    from types import SimpleNamespace
    from .query import DBQuery, DBSQL

__all__ = ['DBFactory']


T = typing.TypeVar('T', bound='DBTable')


class DBFactory:
    _trans = Translator

    def __init__(self, connection_pool, debug_mode: bool = False):
        self._connection_pool: psycopg_pool.ConnectionPool = connection_pool
        self._tables: list[type[DBTable]] = list()
        self._debug_mode = debug_mode

    @staticmethod
    def postgres(**kwargs) -> DBFactory | None:
        debug_mode = kwargs.pop("debug_mode", False)
        conninfo = kwargs.pop("conninfo")
        try:
            pool = psycopg_pool.ConnectionPool(conninfo, kwargs=kwargs)
            pool.wait()
        except Exception as e:
            print(str(e))
            return None
        return DBFactory(pool, debug_mode)

    def use(self, cls: type[DBTable], schema: str = 'public'):
        cls.DB.db = self
        if not cls.DB.schema:
            cls.DB.schema = schema
        if cls not in self._tables:
            self._tables.append(cls)
        setattr(self, cls.__name__, cls)
        return cls

    def use_module(self, name: str = None, schema: str = 'public'):
        if name:
            if name in sys.modules:
                globalns = vars(sys.modules[name])
            else:
                __import__(name)
                globalns = vars(sys.modules[name])
        else:
            globalns = sys._getframe(1).f_locals
        if s := globalns.get('_SCHEMA_'):
            schema = s
        tables: list[type[DBTable]] = list()
        for v in globalns.values():
            if inspect.isclass(v) and v is not DBTable and issubclass(v, DBTable) and not v.DB.meta:
                tables.append(v)
                self.use(v, schema)
        for table in tables:
            table.resolve_types(globalns)
        for table in tables:
            table.resolve_types_many(lambda t: self.use(t, schema))

    def __contains__(self, item: str | DBTable) -> bool:
        if isinstance(item, str):
            return any(item == table.__qualname__ for table in self._tables)
        else:
            return item in self._tables
        
    def __getitem__(self, item: str) -> type[DBTable]:
        for table in self._tables:
            if table.__qualname__ == item:
                return table
        raise KeyError(item)

    def query(self, table_class: Optional[type[T]] = None) -> DBQuery[T]:
        from .query import DBQuery
        return DBQuery[T](self, table_class)

    def get(self, table_class: type[T], pk: Any = None, **fields) -> T:
        query = self.query(table_class)
        if pk is not None:
            query.filter(pk=pk)
        for k, v in fields.items():
            query.filter(lambda s: getattr(s, k) == v)
        return query.fetchone()

    def get_connection(self) -> psycopg.Connection:
        return self._connection_pool.getconn()

    def release_connection(self, conn: psycopg.Connection):
        self._connection_pool.putconn(conn)

    @contextmanager
    def connection(self, reuse_conn: psycopg.Connection = None) -> psycopg.Connection:
        if reuse_conn is not None:
            yield reuse_conn
        else:
            with self._connection_pool.connection() as conn:
                yield conn

    def clear(self, schema: str = None):
        with self.connection() as conn:  # type: psycopg.Connection
            tables = []
            for res in conn.execute(self._trans.select_all_tables()):
                if not schema or schema == res[0]:
                    tables.append(f'"{res[0]}"."{res[1]}"')
            with conn.transaction():
                for table in tables:
                    conn.execute(f'DROP TABLE {table} CASCADE')

    def all_tables(self, schema: str = None, for_stub: bool = False) -> list[type[DBTable]]:

        all_tables = self._tables.copy()
        for table in self._tables:
            all_tables.extend(table.DB.subtables.values())

        ext: dict[str, list[type[DBTable]]] = defaultdict(list)
        for t in all_tables.copy():
            if t.DB.extendable and not for_stub:
                ext[t.DB.table].append(t)
                all_tables.remove(t)
            elif schema and t.DB.schema != schema:
                all_tables.remove(t)

        if for_stub:
            def add_bases(t):
                if t not in all_tables:
                    all_tables.insert(0, t)
                for base in t.__bases__:
                    if issubclass(base, DBTable) and base is not DBTable:
                        add_bases(base)

            for t in all_tables.copy():
                add_bases(t)

        if schema:
            for tname, tables in ext.copy().items():
                if not any(t.DB.schema == schema for t in tables):
                    del ext[tname]

        for tables in ext.values():
            fields = {}
            annotations = {}
            field_sources = {}
            root_class: type[DBTable] | None = None
            for t in tables:
                if t.DB.is_root:
                    root_class = t

                for fname, field in t.DB.fields.items():
                    if src := field_sources.get(fname):
                        if field.type != fields[fname].type and not issubclass(t, src) and not issubclass(src, t):
                            raise QuazyFieldNameError(f'Same column `{field.name}` in different branches has different type')
                    else:
                        fields[fname] = field
                        field_sources[fname] = t
                    if fname in t.__annotations__:
                        annotations[fname] = t.__annotations__[fname]

            for fname, f in fields.items():
                if fname not in annotations:
                    annotations[fname] = f.type

            TableClass: type[DBTable] = typing.cast(type[DBTable], type(root_class.__qualname__+"Combined", (DBTable, ), {
                '__qualname__': root_class.__qualname__+"Combined",
                '__module__': root_class.__module__,
                '__annotations__': annotations,
                '_db_': self,
                '_table_': root_class.DB.table,
                '_extendable_': True,
                **fields
            }))
            all_tables.append(TableClass)

        return all_tables

    def missed_tables(self, schema: str = None) -> list[type[DBTable]]:
        all_tables = self.all_tables(schema)

        with self.select(self._trans.select_all_tables()) as created_tables_query:
            #created_tables = [(t.schema, t.table) for t in created_tables_query]
            created_tables = created_tables_query.fetchall()

        for table in all_tables.copy():
            if (table.DB.schema, table.DB.table) in created_tables or schema and table.DB.schema != schema:
                all_tables.remove(table)

        return all_tables

    def check(self, schema: str = None) -> bool:
        return len(self.missed_tables(schema)) == 0

    def table_exists(self, table: DBTable) -> bool:
        with self.select(self._trans.is_table_exists(table)) as res:
            return res.fetchone()[0]

    def create(self, schema: str = None):
        all_tables = self.missed_tables(schema)
        if not all_tables:
            return

        all_schemas = set()
        for table in all_tables:
            if table.DB.schema:
                all_schemas.add(table.DB.schema)

        with self.connection() as conn:  # type: psycopg.Connection
            with conn.transaction():
                for schema in all_schemas:
                    conn.execute(self._trans.create_schema(schema))
                for table in all_tables:
                    conn.execute(self._trans.create_table(table))
                    for field in table.DB.fields.values():
                        if field.indexed:
                            conn.execute(self._trans.create_index(table, field))

                for table in all_tables:
                    for field in table.DB.fields.values():
                        if field.ref and not field.prop:
                            conn.execute(self._trans.add_reference(table, field))

    def insert(self, item: T) -> T:
        item._before_insert(self)
        fields: list[tuple[DBField, Any]] = []
        for name, field in item.DB.fields.items():
            if field.cid:
                fields.append((field, item.DB.discriminator))
            elif field.body:
                continue
            elif field.required and not field.pk and not field.default and not field.default_sql:
                value = getattr(item, name, None)
                if value is None:
                    raise QuazyMissedField(f"Field `{name}` value is missed for `{item.__class__.__name__}`")
                fields.append((field, value))
            else:
                fields.append((field, getattr(item, name, DefaultValue)))

        with self.connection() as conn:  # type: psycopg.Connection

            sql, values = self._trans.insert(item.__class__, fields)
            item.pk = conn.execute(sql, values).fetchone()[0]

            for field_name, table in item.DB.subtables.items():
                for row in getattr(item, field_name):
                    setattr(row, item.DB.table, item)
                    fields.clear()
                    for name, field in table.DB.fields.items():
                        fields.append((field, getattr(row, name, DefaultValue)))
                    sql, values = self._trans.insert(row.__class__, fields)
                    new_sub_id = conn.execute(sql, values).fetchone()[0]
                    setattr(row, table.DB.pk.name, new_sub_id)

            for field_name, field in item.DB.many_fields.items():
                for row in getattr(item, field_name):
                    if getattr(row, field.source_field) != item.pk:
                        setattr(row, field.source_field, item.pk)
                        self.save(row)

            for field_name, field in item.DB.many_to_many_fields.items():
                for row in getattr(item, field_name):
                    if not row.pk:
                        self.save(row)

                # delete old items, add new items
                new_indices = set(row.pk for row in getattr(item, field_name))
                old_indices_sql = self._trans.select_many_indices(field.middle_table, field.source_field, field.source_table.DB.table)
                results = conn.execute(old_indices_sql, {"value": item.pk}).fetchone()
                old_indices = set(results[0]) if results[0] else set()

                indices_to_delete = list(old_indices - new_indices)
                indices_to_add = list(new_indices - old_indices)

                if indices_to_delete:
                    delete_indices_sql = self._trans.delete_many_indices(field.middle_table, field.source_field, field.source_table.DB.table)
                    conn.execute(delete_indices_sql, {"value": item.pk, "indices": indices_to_delete})

                if indices_to_add:
                    new_indices_sql = self._trans.insert_many_index(field.middle_table, field.source_field, field.source_table.DB.table)
                    for index in indices_to_add:
                        conn.execute(new_indices_sql, {"value": item.pk, "index": index})

        item._after_insert(self)
        return item

    def update(self, item: T) -> T:
        item._before_update(self)
        fields: list[tuple[DBField, Any]] = []
        for name in item._modified_fields_:
            field = item.DB.fields[name]
            fields.append((field, getattr(item, name, DefaultValue)))
        with self.connection() as conn:
            sql, values = self._trans.update(item.__class__, fields)
            if not values:
                return item
            values['v1'] = getattr(item, item.DB.pk.name)
            conn.execute(sql, values)

            for table in item.DB.subtables.values():
                sql = self._trans.delete_related(table, item.DB.table)
                conn.execute(sql, (getattr(item, item.DB.pk.name), ))
                for row in getattr(item, table.DB.snake_name):
                    self.insert(row)
        item._after_update(self)
        return item

    @contextmanager
    def select(self, query: Union[DBQuery, str], as_dict: bool = False) -> Iterator[DBTable | SimpleNamespace]:
        from quazy.query import DBQuery
        with self.connection() as conn:
            if isinstance(query, DBQuery):
                sql = self._trans.select(query)
                if self._debug_mode: print(sql)
                if as_dict:
                    row_factory = dict_row
                elif query.fetch_objects:
                    row_factory = class_row(lambda **kwargs: query.table_class(_db_=self, **kwargs))
                else:
                    row_factory = namedtuple_row
                with conn.cursor(binary=True, row_factory=row_factory) as curr:
                    yield curr.execute(sql, query.args)
            else:
                with conn.cursor(binary=True, row_factory=dict_row if as_dict else namedtuple_row) as curr:
                    yield curr.execute(query)

    def describe(self, query: Union[DBQuery[T], str]) -> list[DBField]:
        from quazy.query import DBQuery
        from psycopg.rows import no_result
        if typing.TYPE_CHECKING:
            from psycopg.cursor import BaseCursor, RowMaker
            from psycopg.rows import DictRow

        def types_row(cursor: BaseCursor[Any, DictRow]) -> RowMaker[DictRow]:
            desc = cursor.description
            if desc is None:
                return no_result

            has_fields = isinstance(query, DBQuery) and query.table_class is not None
            for col in desc:
                if has_fields and (field:=query.table_class.DB.fields.get(col.name)) is not None:
                    res.append(field)
                else:
                    field = DBField(col.name)
                    field.prepare(col.name)
                    field.type = self._trans.TYPES_BY_OID[col.type_code]
                    res.append(field)

            return no_result

        res = []
        with self.connection() as conn:
            if isinstance(query, DBQuery):
                query = query.copy().set_window(limit=0)
                sql = self._trans.select(query)
                if self._debug_mode: print(sql)
                with conn.cursor(binary=True, row_factory=types_row) as curr:
                    curr.execute(sql, query.args)
            else:
                with conn.cursor(binary=True, row_factory=types_row) as curr:
                    curr.execute(query)

        return res


    def save(self, item: T, lookup_field: str | None = None) -> T:
        pk_name = item.__class__.DB.pk.name
        if lookup_field:
            row_id = self.query(item.__class__)\
                .filter(lambda x: getattr(x, lookup_field) == getattr(item, lookup_field))\
                .set_window(limit=1)\
                .select(pk_name)\
                .fetchvalue()
            if row_id:
                setattr(item, pk_name, row_id)
                self.update(item)
            else:
                self.insert(item)
        else:
            if getattr(item, pk_name):
                self.update(item)
            else:
                self.insert(item)
        return item

    def delete(self, table: type[DBTable] = None, *,
               item: T = None,
               id: Any = None,
               items: typing.Iterator[T] = None,
               query: DBQuery[T] = None,
               filter: Callable[[T], DBSQL] = None,
               reuse_conn: psycopg.Connection = None):
        if id is not None:
            if table is None:
                raise QuazyWrongOperation("Both `id` and `table` should be specified")
            with self.connection(reuse_conn) as conn:
                sql = self._trans.delete_related(table, table.DB.pk.name)
                conn.execute(sql, (id, ))
        elif item is not None:
            item._before_delete(self)
            with self.connection(reuse_conn) as conn:
                sql = self._trans.delete_related(type(item), item.DB.pk.name)
                conn.execute(sql, (item.pk, ))
            item._after_delete(self)
        elif items is not None:
            with self.connection(reuse_conn) as conn:
                for item in items:
                    self.delete(item=item, reuse_conn=conn)
        elif query is not None:
            if not query.fetch_objects:
                raise QuazyWrongOperation('Query should be objects related')
            builder = self.query(query.table_class)
            sub = builder.with_query(query.select("pk"), not_materialized=True)
            with self.connection(reuse_conn) as conn:
                sql = self._trans.delete_selected(builder, sub)
                if self._debug_mode: print(sql)
                conn.execute(sql, builder.args)
        elif filter is not None:
            if table is None:
                raise QuazyWrongOperation("Both `filter` and `table` should be specified")
            query = self.query(table).filter(filter)
            self.delete(query=query)
        elif table is not None:
            with self.connection(reuse_conn) as conn:
                sql = self._trans.delete(table)
                conn.execute(sql)
        else:
            raise QuazyWrongOperation('Specify one of the arguments')

