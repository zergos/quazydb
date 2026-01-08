"""Database factory.

This module represents one class `DBFactory` as a start point to any connection with the database.
"""
from __future__ import annotations

import asyncio
import sys
import inspect
from collections import defaultdict
from contextlib import contextmanager, asynccontextmanager, nullcontext

from .exceptions import *
from .db_table import *
from .db_field import *
from .db_types import *
from .db_types import DBTableT, Unassigned
from .translator import Translator
from .helpers import make_async, hybrid_contextmanager

import typing

if typing.TYPE_CHECKING:
    from typing import *
    from types import SimpleNamespace
    from .db_query import DBQuery, DBQueryAsync, DBSQL
    from .db_protocol import *

__all__ = ['DBFactory', 'DBFactoryAsync']

class DBFactory:
    """Basic database factory class

    Use this class to arrange connection to a database, parse table models and run queries.

    Note:
        It supports a Postgres database only at the moment

    Example:
        .. code-block::

            db = DBFactory.postgres(conninfo="postgresql://quazy:quazy@localhost/quazy")
            db._debug_mode = True
            db.bind_module()
            db.clear()
            db.create()
            ...

    """
    async_mode: ClassVar[bool] = False

    def __init__(self, connection_pool: DBPoolLike, translator: Translator, named_factory: DBRowFactoryLike, dict_factory: DBRowFactoryLike, class_factory: DBRowFactoryLike):
        """Basic constructor for database connection

        Create a connection via a specific connection pool.

        Note:
            Normally is not intended to run manually, as it intended to use of proxy constructors
            like `postgres()` below

        Note:
            It is highly appreciated to work with databases supported pools.

        Args:
             connection_pool: Connection (or pool) to work with the database
             debug_mode: Debug mode - writes all queries to logs before execution
        """
        self._connection_pool: DBPoolLike = connection_pool
        self._translator = translator
        self._tables: list[type[DBTable]] = list()
        self._named_factory = named_factory
        self._dict_factory = dict_factory
        self._class_factory = class_factory
        self._debug_mode = False

    @property
    def translator(self):
        """Translator contains all database specific primitives to construct SQL code"""
        return self._translator

    @staticmethod
    def postgres(conninfo: str, **kwargs) -> DBFactory | None:
        """Proxy constructor Postgres specific connection

        Args:
            conninfo: connection string to connect to a database
            **kwargs: all keywords arguments passed to `psycopg` constructor, except `debug_mode`

        Returns:
            DBFactory instance or None

        Example:
            db = DBFactory.postgres(conninfo="postgresql://quazy:quazy@localhost/quazy")
        """
        import psycopg
        from psycopg.rows import namedtuple_row, dict_row, kwargs_row
        from .translator_psql import TranslatorPSQL

        connection = psycopg.connect(conninfo, **kwargs)
        connection.close()

        class PsycopgConnection:
            conn: ContextManager[psycopg.Connection] = None
            def connection(self) -> ContextManager[DBConnectionLike] | DBConnectionLike:
                if self.conn is None or self.conn.closed:
                    self.conn = psycopg.connect(conninfo, **kwargs)
                return self.conn
            @contextmanager
            def cursor(self, read_only: bool=False) -> Generator[DBCursorLike]:
                with self.connection().cursor(binary=True) as curr:
                    yield curr
                if not read_only:
                    self.conn.commit()
            def close(self):
                self.conn.close()

        return DBFactory(PsycopgConnection(), TranslatorPSQL, namedtuple_row, dict_row, kwargs_row)

    @staticmethod
    def postgres_pool(conninfo: str, **kwargs) -> DBFactory | None:
        """Proxy constructor Postgres specific pool

        Args:
            conninfo: connection string to connect to a database
            **kwargs: all keywords arguments passed to `psycopg` constructor, except `debug_mode`

        Returns:
            DBFactory instance or None

        Example:
            db = DBFactory.postgres_pool(conninfo="postgresql://quazy:quazy@localhost/quazy")
        """
        from psycopg_pool.pool import ConnectionPool
        from psycopg.rows import namedtuple_row, dict_row, kwargs_row

        from .translator_psql import TranslatorPSQL

        try:
            pool = ConnectionPool(conninfo, kwargs=kwargs)
            pool.wait()
        except Exception as e:
            print(str(e))
            return None

        class PsycopgPoolConnection:
            def connection(self) -> ContextManager[DBConnectionLike]:
                return pool.connection()
            @contextmanager
            def cursor(self, read_only:bool=False) -> Generator[DBCursorLike]:
                with self.connection() as conn:
                    with conn.cursor(binary=True) as curr:
                        yield curr
            def close(self):
                pool.close()

        return DBFactory(PsycopgPoolConnection(), TranslatorPSQL, namedtuple_row, dict_row, kwargs_row)

    @staticmethod
    def sqlite(conn_uri: str, **kwargs) -> DBFactory | None:
        """Proxy constructor SQLite specific connection

        Args:
            conn_uri: connection string to connect to a database
            kwargs: all keywords arguments passed to `sqlite3` constructor, except `debug_mode`

        Example:
            db = DBFactory.sqlite("file:test.db?mode=rwc")
        """
        import sqlite3
        from .translator_sqlite import (TranslatorSQLite, namedtuple_row, dict_row, kwargs_row,
                                        ConnectionFactory)

        detect_types = kwargs.pop("detect_types", sqlite3.PARSE_DECLTYPES)
        connection = sqlite3.connect(conn_uri, uri=True, detect_types=detect_types, factory=ConnectionFactory, **kwargs)

        class SQLiteConnection:
            @contextmanager
            def connection(self) -> Generator[DBConnectionLike]:
                yield connection
            def cursor(self, read_only:bool=False) -> ContextManager[DBCursorLike]:
                return connection.cursor()
            def close(self):
                connection.close()

        return DBFactory(SQLiteConnection(), TranslatorSQLite, namedtuple_row, dict_row, kwargs_row)

    def close(self) -> Awaitable[None] | None:
        """Close the database connection and release all resources"""
        self._connection_pool.close()

    def bind(self, cls: type[DBTable], schema: str = 'public'):
        """Bind a specific table to the factory instance

        Args:
            cls: Table to bind
            schema: schema name
        """
        cls.DB.db = self
        if not cls.DB.schema:
            cls.DB.schema = schema

        for subtab in cls.DB.subtables.values():
            subtab.DB.db = self
            if not subtab.DB.schema:
                subtab.DB.schema = schema

        if cls not in self._tables:
            self._tables.append(cls)
        setattr(self, cls.__name__, cls)
        return cls

    def bind_module(self, name: str = None, schema: str = 'public'):
        """Bind a specific module by name to the database factory

        Args:
            name: module name to bind. If not specified, bind current module
            schema: schema name to use
        """
        if name:
            if name in sys.modules:
                globalns = vars(sys.modules[name])
            else:
                __import__(name)
                globalns = vars(sys.modules[name])
        else:
            globalns = sys._getframe(1).f_globals
        if s := globalns.get('_SCHEMA_'):
            schema = s
        tables: list[type[DBTable]] = list()
        for v in globalns.values():
            if inspect.isclass(v) and v is not DBTable and issubclass(v, DBTable) and not v.DB.meta:
                tables.append(v)
                self.bind(v, schema)
        for table in tables:
            table.resolve_types(globalns)
        for table in tables:
            table.resolve_types_many(lambda t: self.bind(t, schema))
        for table in tables:
            table.setup_validators()

    def unbind(self, schema: str = "public"):
        """Unbind all tables

        Arguments:
            schema: schema name to unbind. "Public" by default. If "None", unbind all tables from all schemas.
        """
        if schema is None:
            self._tables.clear()
        else:
            for table in self._tables.copy():
                if table.DB.schema == schema:
                    self._tables.remove(table)

    def __contains__(self, item: str | DBTable) -> bool:
        """Check if DBTable exists in the database factory"""
        if isinstance(item, str):
            return any(item == table.__qualname__ for table in self._tables)
        else:
            return item in self._tables
        
    def __getitem__(self, item: str) -> type[DBTable]:
        """Get DBTable class by name"""
        for table in self._tables:
            if table.__qualname__ == item:
                return table
        raise KeyError(item)

    def query(self, table_class: Optional[type[DBTableT]] = None, name: Optional[str] = None) -> DBQuery[DBTableT]:
        """Create DBQuery instance

        Create a DBQuery instance bound to a specified DBTable or to the whole schema

        Args:
            table_class: DBTable class to use or None
            name: name of the query for subquery request

        Returns:
            DBQuery instance

        Example:
            q = db.query(Street).select('name')
            q = db.query().select(name=lambda s: s.street.name)
        """
        from .db_query import DBQuery
        return DBQuery[DBTableT](self, table_class, name)

    def get(self, table_class: type[DBTableT], pk: Any = None, **fields) -> Awaitable[DBTableT] | DBTableT:
        """Request one row from a database table

        Hint:
            This method is not intended to be used directly. It is shorter to call the `get` method from the `DBTable` instance.

        Args:
            table_class: DBTable class to use
            pk: primary key value to filter row (optional)
            **fields: field values to filter row if no pk is specified (optional)

        Returns:
            DBTable instance
        """
        query = self.query(table_class)
        if pk is not None:
            query.filter(pk=pk)
        for k, v in fields.items():
            query.filter(lambda s: getattr(s, k) == v)
        return query.fetch_one()

    @hybrid_contextmanager
    def connection(self) -> AsyncGenerator[DBConnectionLike] | Generator[DBConnectionLike]:
        """Context manager for connection"""
        with self._connection_pool.connection() as conn:
            yield conn

    @hybrid_contextmanager
    def cursor(self, read_only: bool=False, _curr: DBCursorLike=None) -> AsyncGenerator[DBCursorLike] | Generator[DBCursorLike]:
        with self._connection_pool.cursor(read_only) if _curr is None else nullcontext(_curr) as cursor:
            yield cursor

    def clear(self, schema: str = None) -> Awaitable[None] | None:
        """Drop all known tables in a database

        Args:
            schema: schema name to use

        Warning:
            It works without attention. Please double-check before calling.
        """
        with self.cursor() as curr:  # type: DBConnectionLike
            tables = []
            for res in curr.execute(self._translator.select_all_tables()):
                if self._translator.supports_schema and (not schema or schema == res[0]):
                    tables.append(f'"{res[0]}"."{res[1]}"')
                else:
                    tables.append(f'"{res[0]}"')
            for table_name in tables:
                curr.execute(self._translator.drop_table_by_name(table_name))

    def all_tables(self, schema: str = None, for_stub: bool = False) -> list[type[DBTable]]:
        """Get all known tables in the database

        Args:
            schema: schema name to use
            for_stub: whether to return tables only for stub purposes (uses internally)

        Returns:
            List of table classes
        """

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
                '__annotate_func__': lambda f: annotations,
                '_db_': self,
                '_table_': root_class.DB.table,
                '_extendable_': True,
                **fields
            }))
            all_tables.append(TableClass)

        return all_tables

    def missed_tables(self, schema: str = None) -> Awaitable[list[type[DBTable]]] | list[type[DBTable]]:
        """Get all tables added as models but not created in the database yet

        :meta private:
        """
        all_tables = self.all_tables(schema)

        with self.select(self._translator.select_all_tables()) as created_tables_query:
            #created_tables = [(t.schema, t.table) for t in created_tables_query]
            created_tables = created_tables_query.fetchall()

        for table in all_tables.copy():
            if (table.DB.schema, table.DB.table) in created_tables or schema and table.DB.schema != schema:
                all_tables.remove(table)

        return all_tables

    def check(self, schema: str = None) -> bool:
        """Check all tables are created in the database

        :meta private:
        """
        return len(self.missed_tables(schema)) == 0

    def table_exists(self, table: DBTable) -> Awaitable[bool] | bool:
        """Check if a table exists in the database

        :meta private:"""
        with self.select(self._translator.is_table_exists(table)) as res:
            return res.fetch_one()[0]

    def create(self, schema: str = None) -> Awaitable[None] | None:
        """Create all added tables in the database"""
        all_tables = self.missed_tables(schema)
        if not all_tables:
            return

        all_schemas = set()
        for table in all_tables:
            if table.DB.schema:
                all_schemas.add(table.DB.schema)

        with self.cursor() as curr:
            # create schemas
            if self._translator.supports_schema:
                for schema in all_schemas:
                    curr.execute(self._translator.create_schema(schema))

            # create tables
            for table in all_tables:
                curr.execute(self._translator.create_table(table))

            # create foreign keys
            for table in all_tables:
                for field in table.DB.fields.values():
                    if field.ref and not field.property:
                        curr.execute(self._translator.add_reference(table, field))

            # create indices
            for table in all_tables:
                for field in table.DB.fields.values():
                    if field.indexed:
                        curr.execute(self._translator.create_index(table, field))

    def insert(self, item: DBTableT, _curr: DBCursorLike=None) -> Awaitable[DBTableT] | DBTableT:
        """Insert item into the database

        Args:
            item: instance of DBTable to insert

        Returns:
            updated item, just for chain calls

        Note:
            Inserted item could contain subtables of subclasses and many-to-many sets.
            In this case they will be bulk inserted.
        """
        item._before_insert(self)
        fields: list[tuple[DBField, Any]] = []
        for name, field in item.DB.fields.items():
            if field.cid:
                fields.append((field, item.DB.discriminator))
                continue
            elif field.body:
                continue
            elif field.required and not field.pk and field.default is Unassigned and not field.default_sql:
                value = getattr(item, name, None)
                if value is None:
                    raise QuazyMissedField(f"Field `{name}` value is missed for `{item.__class__.__name__}`")
            else:
                value = getattr(item, name, DefaultValue)
            if not inspect.isclass(value) and callable(value):
                # materialize values on saving
                value = value()
                object.__setattr__(item, name, value)
            fields.append((field, value))

        with self.cursor(_curr=_curr) as curr:
            sql, values = getattr(self._translator, "insert")(item, fields)
            if self._debug_mode: print(sql)
            curr.execute(sql, values)
            rows = curr.fetchone()
            item.pk = rows[0]
            item._modified_fields_.clear()

            for field_name, table in item.DB.subtables.items():
                for row in getattr(item, field_name):
                    setattr(row, item.DB.table, item)
                    self.insert(row, _curr=curr)

            if not item.DB.use_slots:
                for field_name, field in item.DB.many_fields.items():
                    for row in getattr(item, field_name):
                        if getattr(row, field.foreign_field) != item.pk:
                            setattr(row, field.foreign_field, item.pk)
                            self.save(row, _curr=curr)

                for field_name, field in item.DB.many_to_many_fields.items():
                    for row in getattr(item, field_name):
                        if not row.pk:
                            self.save(row, _curr=curr)

                    new_indices_sql = self._translator.insert_many_index(field.middle_table, item.DB.table,
                                                                         field.foreign_table.DB.table)
                    if self._translator.supports_copy:
                        with curr.copy(new_indices_sql) as copy:
                            for row in getattr(item, field_name):
                                copy.write_row((item.pk, row.pk))
                    else:
                        curr.executemany(new_indices_sql, [(item.pk, row.pk) for row in getattr(item, field_name)])

        item._after_insert(self)
        return item

    def update(self, item: DBTableT, _curr: DBCursorLike=None) -> Awaitable[DBTableT] | DBTableT:
        """Update item changes to a database

        Args:
            item: instance of DBTable to insert

        Returns:
            updated item, just for chain calls

        Note:
            If an item instance has subtables of subclasses, they will be bulk updated (delete and insert).
        """
        item._before_update(self)
        fields: list[tuple[DBField, Any]] = []
        for name in item._modified_fields_:
            field = item.DB.fields[name]
            fields.append((field, getattr(item, name, DefaultValue)))
        with self.cursor(_curr=_curr) as curr:
            if fields:
                sql, values = self._translator.update(item.__class__, fields)
                values['pk'] = getattr(item, item.DB.pk.name)
                curr.execute(sql, values)
                item._modified_fields_.clear()

            for table in item.DB.subtables.values():
                sql = self._translator.delete_related(table, item.DB.table)
                curr.execute(sql, (getattr(item, item.DB.pk.name), ))
                for row in getattr(item, table.DB.snake_name):
                    self.insert(row, _curr=curr)

            for field_name, field in item.DB.many_fields.items():
                for row in getattr(item, field_name):
                    if getattr(row, field.foreign_field) != item:
                        setattr(row, field.foreign_field, item)
                        self.save(row, _curr=curr)

            for field_name, field in item.DB.many_to_many_fields.items():
                for row in getattr(item, field_name):
                    if not row.pk:
                        self.save(row, _curr=curr)

                # delete old items, add new items
                new_indices = set(row.pk for row in getattr(item, field_name))
                old_indices_sql = self._translator.select_many_indices(field.middle_table, item.DB.table, field.foreign_table.DB.table)
                curr.execute(old_indices_sql, {"value": item.pk})
                results = curr.fetchone()
                old_indices = set(results[0]) if results[0] else set()

                indices_to_delete = list(old_indices - new_indices)
                indices_to_add = list(new_indices - old_indices)

                if indices_to_delete:
                    delete_indices_sql = self._translator.delete_many_indices(field.middle_table, item.DB.table, field.foreign_table.DB.table)
                    curr.execute(delete_indices_sql, {"value": item.pk, "indices": indices_to_delete})

                if indices_to_add:
                    new_indices_sql = self._translator.insert_many_index(field.middle_table, item.DB.table, field.foreign_table.DB.table)
                    if self._translator.supports_copy:
                        with curr.copy(new_indices_sql) as copy:
                            for index in indices_to_add:
                                copy.write_row((item.pk, index))
                    else:
                        curr.executemany(new_indices_sql, [(item.pk, index) for index in indices_to_add])

        item._after_update(self)
        return item

    @hybrid_contextmanager
    def select(self, query: Union[DBQuery, str], as_dict: bool = False) -> (
            AsyncGenerator[Iterator[DBTable | SimpleNamespace | dict[str, Any]]] |
            Generator[Iterator[DBTable | SimpleNamespace | dict[str, Any]]]):
        """Select items from the database

        It performs a prepared query to a database and yields results. Is not intended for direct calls.
        Use preferably `select` method of DBQuery instance.
        Result type depends on query type:
        if a query is based on a specific DBTable, the result is an instance of DBTable
        if a query is based on a whole schema, the result is SimpleNamespace

        Args:
            query: instance of DBQuery or string
            as_dict: results yield as dict instead of instance of DBTable/SimpleNamespace

        Yields:
            instance of DBTable/SimpleNamespace or dict
        """
        with self.cursor(read_only=True) as curr:
            if not isinstance(query, str):
                if not query.is_frozen:
                    sql = self._translator.select(query)
                    if self._debug_mode: print(sql)
                else:
                    sql = query.frozen_sql
                if as_dict:
                    row_factory = self._dict_factory
                elif query.fetch_objects:
                    row_factory = self._class_factory(lambda **kwargs: query.table_class.raw(_db_=self, **kwargs))
                else:
                    row_factory = self._named_factory
                curr.row_factory = row_factory
                yield curr.execute(sql, query.args)
            else:
                curr.row_factory = self._dict_factory if as_dict else self._named_factory
                yield curr.execute(query)

    def update_many(self, query: DBQuery[DBTableT], **values) -> Awaitable[None] | None:
        """Update items in the database by a query

        Arguments:
            query: instance of DBQuery bound to a table
            **values: dict of values to update
        """
        fields: list[tuple[DBField, Any]] = []
        for name, value in values.items():
            field = query.table_class.DB.fields[name]
            fields.append((field, value))

        with self.cursor() as curr:
            sql, values = self._translator.update(query.table_class, fields, query)
            if self._debug_mode: print(sql)
            curr.execute(sql, query.args | values)

    def describe(self, query: Union[DBQuery[DBTableT], str]) -> list[DBField]:
        """Describe query result fields information

        It performs a prepared query to a database requesting zero rows just to collect information about fields.

        Args:
            query: instance of DBQuery or string

        Returns:
            list of `DBField` instances
        """
        from quazy.db_query import DBQuery
        if typing.TYPE_CHECKING:
            from psycopg.cursor import BaseCursor, RowMaker
            from psycopg.rows import DictRow

        def extract_description(cols_info: Sequence[tuple[str, ...]]) -> list[DBField]:
            res = []
            has_fields = isinstance(query, DBQuery) and query.table_class is not None
            for col in cols_info:
                if has_fields and (field := query.table_class.DB.fields.get(col[0])) is not None:
                    res.append(field)
                else:
                    field = DBField(col[0])
                    field.prepare(col[0])
                    field.type = self._translator.TYPES_BY_OID[col[1]]
                    res.append(field)
            return res

        with self.cursor() as curr:
            if isinstance(query, DBQuery):
                query = query.copy().set_window(limit=0)
                sql = self._translator.select(query)
                if self._debug_mode: print(sql)
                curr.execute(sql, query.args)
                return extract_description(curr.description)
            else:
                curr.execute(query)
                return extract_description(curr.description)


    def save(self, _item: DBTableT, _lookup_field: str | None = None, _curr: DBCursorLike=None, **kwargs) -> Awaitable[DBTableT] | DBTableT:
        """Save item to the database

        It checks whether an item needs to be inserted or updated. Specify `lookup_field` to avoid searching by a primary key.

        Args:
            _item: instance of DBTable
            _lookup_field: field name or None
            kwargs: additional values to update item fields before saving it to the database

        Returns:
            updated instance, just for chain calls
        """
        for name, value in kwargs.items():
            setattr(_item, name, value)
        pk_name = _item.__class__.DB.pk.name
        if _lookup_field:
            row_id = self.query(_item.__class__)\
                .filter(lambda x: x[_lookup_field] == getattr(_item, _lookup_field))\
                .set_window(limit=1)\
                .select(pk_name)\
                .fetch_value()
            if row_id:
                setattr(_item, pk_name, row_id)
                self.update(_item, _curr=_curr)
            else:
                self.insert(_item, _curr=_curr)
        else:
            if getattr(_item, pk_name):
                self.update(_item, _curr=_curr)
            else:
                self.insert(_item, _curr=_curr)
        return _item

    def delete(self, table: type[DBTable] = None, *,
               item: DBTableT = None,
               id: Any = None,
               items: typing.Iterator[DBTableT] = None,
               query: DBQuery[DBTableT] = None,
               filter: Callable[[DBTableT], DBSQL] = None,
               _curr: DBCursorLike = None) -> Awaitable[None] | None:
        """Delete an item or items from the database

        It has many possible ways to delete expected items:
         * by item
         * by table and id (for a primary key)
         * by item list
         * by query based on DBTable
         * by table and lamba filter

        Args:
            table: DBTable class
            item: instance of DBTable
            id: instance primary key id
            items: iterable of DBTable instances
            query: instance of DBQuery based on DBTable
            filter: callable lamba added to a query

        Raises:
            QuazyWrongOperation: wrong arguments usage
        """
        if id is not None:
            if table is None:
                raise QuazyWrongOperation("Both `id` and `table` should be specified")
            with self.cursor(_curr=_curr) as curr:
                sql = self._translator.delete_related(table, table.DB.pk.column)
                curr.execute(sql, (id, ))
        elif item is not None:
            item._before_delete(self)
            with self.cursor(_curr=_curr) as curr:
                sql = self._translator.delete_related(type(item), item.DB.pk.column)
                curr.execute(sql, (item.pk, ))
            item._after_delete(self)
        elif items is not None:
            with self.cursor(_curr=_curr) as curr:
                for item in items:
                    self.delete(item=item, _curr=curr)
        elif query is not None:
            if not query.fetch_objects:
                raise QuazyWrongOperation('Query should be objects related')
            builder = self.query(query.table_class)
            sub = builder.with_query(query.select("pk"), not_materialized=True)
            with self.cursor(_curr=_curr) as curr:
                sql = self._translator.delete_selected(builder, sub)
                if self._debug_mode: print(sql)
                curr.execute(sql, builder.args)
        elif filter is not None:
            if table is None:
                raise QuazyWrongOperation("Both `filter` and `table` should be specified")
            query = self.query(table).filter(filter)
            self.delete(query=query, _curr=_curr)
        elif table is not None:
            with self.cursor(_curr=_curr) as curr:
                sql = getattr(self._translator, "delete")(table)
                curr.execute(sql)
        else:
            raise QuazyWrongOperation('Specify one of the arguments')


ASYNC_COMMON_FUNCS = ('cursor', 'execute', 'executemany', 'fetchone', 'fetchall', 'copy', 'write_row')

class DBFactoryAsync(DBFactory):
    async_mode: ClassVar[bool] = True

    @staticmethod
    def postgres(conninfo: str, **kwargs) -> Self | None:
        import psycopg
        from psycopg.rows import namedtuple_row, dict_row, kwargs_row
        from .translator_psql import TranslatorPSQL

        debug_mode = kwargs.pop("debug_mode", False)
        connection = psycopg.connect(conninfo, **kwargs)
        connection.close()

        class PsycopgConnection:
            conn: psycopg.AsyncConnection = None
            async def connection(self) -> ContextManager[DBConnectionLike] | DBConnectionLike:
                if self.conn is None or self.conn.closed:
                    self.conn = await psycopg.AsyncConnection.connect(conninfo, **kwargs)
                return self.conn
            @asynccontextmanager
            async def cursor(self, read_only: bool=False) -> ContextManager[DBCursorLike]:
                conn = await self.connection()
                async with conn.cursor(binary=True) as curr:
                    yield curr
                if not read_only:
                    await self.conn.commit()
            async def close(self):
                await self.conn.close()

        return DBFactoryAsync(PsycopgConnection(), TranslatorPSQL, namedtuple_row, dict_row, kwargs_row, debug_mode)

    @staticmethod
    def postgres_pool(conninfo: str, **kwargs) -> DBFactory | None:
        import psycopg
        from psycopg_pool.pool_async import AsyncConnectionPool
        from psycopg.rows import namedtuple_row, dict_row, kwargs_row

        from .translator_psql import TranslatorPSQL

        connection = psycopg.connect(conninfo, **kwargs)
        connection.close()

        class PsycopgPoolConnection:
            pool: AsyncConnectionPool = None

            @asynccontextmanager
            async def connection(self) -> AsyncGenerator[DBConnectionLike]:
                if self.pool is None:
                    self.pool = AsyncConnectionPool(conninfo, kwargs=kwargs)
                    await self.pool.open()
                    await self.pool.wait()
                async with self.pool.connection() as conn:
                    yield conn
            @asynccontextmanager
            async def cursor(self, read_only:bool=False) -> AsyncGenerator[DBCursorLike]:
                async with self.connection() as conn:
                    async with conn.cursor(binary=True) as curr:
                        yield curr
            async def close(self):
                await self.pool.close()

        return DBFactoryAsync(PsycopgPoolConnection(), TranslatorPSQL, namedtuple_row, dict_row, kwargs_row)

    @staticmethod
    def sqlite(conn_uri: str, **kwargs) -> DBFactory | None:
        try:
            import aiosqlite
        except ImportError:
            raise NotImplementedError('SQLite async is not implemented. Install `aiosqlite` module')

        import sqlite3
        from .translator_sqlite import (TranslatorSQLite, namedtuple_row, dict_row, kwargs_row,
                                        ConnectionFactoryAsync)

        detect_types = kwargs.pop("detect_types", sqlite3.PARSE_DECLTYPES)

        class SQLiteConnection:
            conn: aiosqlite.Connection = None
            async def connection(self) -> DBConnectionLike:
                if self.conn is None:
                    connector = lambda: sqlite3.connect(conn_uri, uri=True, detect_types=detect_types, **kwargs)
                    self.conn = await ConnectionFactoryAsync(connector, iter_chunk_size=64)
                    await self.conn.execute("PRAGMA foreign_keys = ON")
                return self.conn
            @asynccontextmanager
            async def cursor(self, read_only:bool=False) -> AsyncGenerator[DBCursorLike]:
                conn = await self.connection()
                async with conn.cursor() as curr:
                    yield curr
            async def close(self):
                await self.conn.close()

        return DBFactoryAsync(SQLiteConnection(), TranslatorSQLite, namedtuple_row, dict_row, kwargs_row)


    def query(self, table_class: Optional[type[DBTableT]] = None, name: Optional[str] = None) -> DBQueryAsync[DBTableT]:
        from .db_query import DBQueryAsync
        return DBQueryAsync[DBTableT](self, table_class, name)

    connection = make_async(DBFactory.connection, ('connection',))
    cursor = make_async(DBFactory.cursor, ('cursor',))
    close = make_async(DBFactory.close, ('close',))
    get = make_async(DBFactory.get, ('fetch_one',))
    clear = make_async(DBFactory.clear, ASYNC_COMMON_FUNCS + ('results',))
    missed_tables = make_async(DBFactory.missed_tables, ('select', 'fetchall'))
    create = make_async(DBFactory.create, ASYNC_COMMON_FUNCS + ('missed_tables', ))
    table_exists = make_async(DBFactory.table_exists, ASYNC_COMMON_FUNCS + ('select',))
    insert = make_async(DBFactory.insert, ASYNC_COMMON_FUNCS + ('insert', 'save'))
    update = make_async(DBFactory.update, ASYNC_COMMON_FUNCS + ('insert', 'save'))
    update_many = make_async(DBFactory.update_many, ASYNC_COMMON_FUNCS)
    select = make_async(DBFactory.select, ASYNC_COMMON_FUNCS)
    save = make_async(DBFactory.save, ASYNC_COMMON_FUNCS + ('fetch_value', 'update', 'insert'))
    delete = make_async(DBFactory.delete, ASYNC_COMMON_FUNCS + ('delete',))
