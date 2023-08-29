from __future__ import annotations

import sys
import re
import inspect
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field as data_field
from enum import IntEnum

import psycopg
import psycopg_pool
from psycopg.rows import namedtuple_row, class_row, dict_row

from .exceptions import *
from .db_types import *
from .translator import Translator

import typing
import types
from typing import ClassVar

if typing.TYPE_CHECKING:
    from typing import *
    from types import SimpleNamespace
    import asyncpg
    from .query import DBQuery, DBQueryField

__all__ = ['DBFactory', 'DBField', 'DBTable', 'UX', 'Many']


def camel2snake(name: str) -> str:
    return camel2snake.r.sub(r'_\1', name).lower()


camel2snake.r = re.compile(
    '((?<=[a-z0-9])[A-Z]|(?!^)(?<!_)[A-Z](?=[a-z]))')  # tnx to https://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-snake-case


T = typing.TypeVar('T', bound='DBTable')

class DBFactory:
    _trans = Translator

    def __init__(self, connection_pool, debug_mode: bool = False):
        self._connection_pool: psycopg_pool.ConnectionPool = connection_pool
        self._tables: list[type[DBTable]] = list()
        self._debug_mode = debug_mode

    @staticmethod
    def postgres(**kwargs) -> DBFactory:
        debug_mode = kwargs.pop("debug_mode", False)
        conninfo = kwargs.pop("conninfo")
        try:
            pool = psycopg_pool.ConnectionPool(conninfo, kwargs=kwargs)
            pool.wait()
        except Exception as e:
            print(str(e))
        return DBFactory(pool, debug_mode)

    def use(self, cls: type[DBTable], schema: str = 'public'):
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

    def query(self, table_class: Optional[type[T]] = None) -> DBQuery[T]:
        from .query import DBQuery
        return DBQuery[T](self, table_class)

    def lookup(self, table_class: type[T], **fields) -> T:
        query = self.query(table_class)
        query.select_all()
        for k, v in fields.items():
            query.filter(lambda s: getattr(s, k) == v)
        return query.fetchone()

    def get(self, table_class: type[T], **fields) -> T:
        result = self.lookup(table_class, **fields)
        return result and table_class(**result._asdict())

    def get_connection(self) -> psycopg.Connection:
        return self._connection_pool.getconn()

    def release_connection(self, conn: psycopg.Connection):
        self._connection_pool.putconn(conn)

    @contextmanager
    def connection(self) -> psycopg.Connection:
        with self._connection_pool.connection() as conn:
            yield conn

    def clear(self, schema: str = None):
        with self._connection_pool.connection() as conn:  # type: psycopg.Connection
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

    def create(self, schema: str = None):
        all_tables = self.missed_tables(schema)
        if not all_tables:
            return

        all_schemas = set()
        for table in self._tables:
            if table.DB.schema:
                all_schemas.add(table.DB.schema)

        with self._connection_pool.connection() as conn:  # type: psycopg.Connection
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

        with self._connection_pool.connection() as conn:  # type: psycopg.Connection

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
        with self._connection_pool.connection() as conn:
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
        return  item

    @contextmanager
    def select(self, query: Union[DBQuery, str], as_dict: bool = False) -> Iterator[DBTable | SimpleNamespace]:
        from quazy.query import DBQuery
        with self._connection_pool.connection() as conn:
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

    def save(self, item: T, lookup_field: str | None = None) -> T:
        pk_name = item.__class__.DB.pk.name
        if lookup_field:
            row = self.query(item.__class__)\
                .filter(lambda x: getattr(x, lookup_field) == getattr(item, lookup_field))\
                .set_window(limit=1)\
                .select(pk_name)\
                .fetchone()
            if row:
                setattr(item, pk_name, row[0])
                self.update(item)
            else:
                self.insert(item)
        else:
            if getattr(item, pk_name):
                self.update(item)
            else:
                self.insert(item)
        return item

    def table_exists(self, table: DBTable) -> bool:
        with self.select(self._trans.is_table_exists(table)) as res:
            return res.fetchone()[0]


@dataclass
class DBField:
    name: str = data_field(default='', init=False)         # field name in Python
    column: str = data_field(default='')                   # field/column name in database
    type: Union[type[DBTable], type[Any]] = data_field(default=None, init=False)  # field type class
    pk: bool = data_field(default=False)                   # is it primary key?
    cid: bool = data_field(default=False)                  # is it storage of table name for inherited tables ?
    ref: bool = data_field(default=False, init=False)      # is it foreign key (reference) ?
    body: bool = data_field(default=False)                 # is it body field for properties?
    prop: bool = data_field(default=False)                 # is it property field?
    required: bool = data_field(default=True, init=False)  # is field not null ?
    indexed: bool = data_field(default=False)              # is it indexed for fast search ?
    unique: bool = data_field(default=False)               # is it unique ?
    default: Union[Any, Callable[[], Any]] = data_field(default=None)  # default value at Python level
    default_sql: str = data_field(default=None)            # default value at SQL level
    reverse_name: str = data_field(default=None)           # reverse name for reference fields
    # many_field: bool = data_field(default=False, init=False)
    ux: Optional[UX] = data_field(default=None)            # UX/UI options

    def set_name(self, name: str):
        self.name = name
        if not self.column:
            self.column = self.name
        if not self.ux:
            self.ux = UX(self.name)
        elif not self.ux.title:
            self.ux.title = self.name

    def _dump_schema(self) -> dict[str, Any]:
        res = {
            'name': self.name,
            'column': self.column,
            'type': db_type_name(self.type) if not self.ref else self.type.__name__,
        }

        for col in 'pk cid ref body prop required indexed unique'.split():
            if val := getattr(self, col):
                res[col] = val
        if val := self.default_sql:
            res['default_sql'] = val
        return res

    @classmethod
    def _load_schema(cls, state: dict[str, Any]) -> DBField:
        name = state.pop('name')
        f_type = state.pop('type')
        ref = state.pop('ref')
        required = state.pop('required')
        field = DBField(**state)
        field.set_name(name)
        field.ref = ref
        field.required = required
        field._pre_type = db_type_by_name(f_type)
        return field


@dataclass
class UX:
    title: str = data_field(default='')
    width: int = data_field(default=None)
    choices: Mapping = data_field(default=None)
    blank: bool = data_field(default=False)
    readonly: bool = data_field(default=False)
    multiline: bool = data_field(default=False)


@dataclass
class DBManyField:
    source_table: type[DBTable]
    source_field: str | None = None


@dataclass
class DBManyToManyField(DBManyField):
    middle_table: type[DBTable] | None = None


class MetaTable(type):
    db_base_class: type

    def __new__(cls, clsname: str, bases: tuple[type[DBTable], ...], attrs: dict[str, Any]):
        if clsname == 'DBTable':
            cls.db_base_class = attrs['DB']
            return super().__new__(cls, clsname, bases, attrs)

        if 'DB' in attrs:
            raise QuazyError(f'Should not define `DB` subclass directly in `{clsname}`, use `_name_` form')

        spec_attrs = {}
        for name in 'table schema just_for_typing extendable discriminator meta'.split():
            src_name = f'_{name}_'
            if value := attrs.pop(src_name, None):
                spec_attrs[name] = value
                
        DB = typing.cast(type[DBTable.DB], super().__new__(cls, clsname+'DB', (cls.db_base_class, ), spec_attrs))
        attrs['DB'] = DB

        DB.many_fields = dict()
        DB.many_to_many_fields = dict()
        MetaTable.collect_fields(bases, DB, attrs)

        qualname = attrs['__qualname__']
        if not DB.table:
            DB.table = camel2snake(qualname.replace('.', ''))

        if '.' in qualname:
            # save owner class name
            chunks = qualname.split('.')
            base_cls_name = '.'.join(chunks[:-1])
            DB.owner = base_cls_name
            field_name = camel2snake(chunks[-1])+'s'
            DB.snake_name = field_name
            if field_name in DB.fields:
                raise QuazySubclassError(f'Subclass name {qualname} repeats field name')
        else:
            DB.snake_name = camel2snake(qualname)+'s'

        DB.subtables = dict()

        if '_discriminator_' not in attrs:
            DB.discriminator = attrs['__qualname__'] if DB.cid else None
        return super().__new__(cls, clsname, bases, attrs)

    @staticmethod
    def collect_fields(bases: tuple[type[DBTable], ...], DB: type[DBTable.DB], attrs: dict[str, Any]):

        if DB.extendable:
            DB.is_root = True

        fields = MetaTable.collect_bases_fields(bases, DB)

        has_pk = False
        for name, t in attrs.get('__annotations__', {}).items():  # type: str, type
            if name.startswith('_'):
                continue
            field = attrs.pop(name, DBField())
            if isinstance(field, DBField):
                field.set_name(name)
                if not field.type:
                    field.type = t
                if field.pk:
                    has_pk = True
                    DB.pk = field
                elif t is FieldCID or isinstance(t, str) and t.startswith(FieldCID.__name__) or field.cid:
                    # check CID
                    if not DB.extendable:
                        raise QuazyFieldTypeError(f'Table `{attrs["__qualname__"]}` is not declared with _extendable_ attribute')
                    elif DB.cid:
                        raise QuazyFieldTypeError(f'Table `{attrs["__qualname__"]}` has CID field already inherited from extendable')

                    field.cid = True
                    DB.cid = field
                elif t is FieldBody or t == FieldBody.__name__ or field.body:
                    if DB.body:
                        raise QuazyFieldTypeError(f'Table `{attrs["__qualname__"]}` has body field already')

                    field.body = True
                    DB.body = field
            else:
                field = DBField(default=field)
                field.set_name(name)
                field.type = t

            fields[name] = field

        # check seed proper declaration
        if DB.cid and not DB.extendable:
            raise QuazyFieldTypeError(f'CID field is declared, but table `{attrs["__qualname__"]}` is not declared with `extendable` attribute')

        if not has_pk:
            pk = DBField(pk=True)
            pk.set_name('id')
            pk.type = int
            fields['id'] = pk
            DB.pk = pk

        DB.fields = fields

    @staticmethod
    def collect_bases_fields(bases: tuple[type, ...], DB: type[DBTable.DB]) -> dict[str, DBField]:
        fields: dict[str, DBField] = dict()
        for base in bases:
            if base is DBTable:
                break
            if issubclass(base, DBTable):
                fields.update(base.DB.fields)

                if base.DB.extendable:
                    if DB.extendable:
                        raise QuazySubclassError('Multiple inheritance of extendable tables is not supported')
                    DB.extendable = True
                    DB.is_root = False
                    DB.cid = base.DB.cid
                    DB.table = base.DB.table
                    DB.body = base.DB.body

        return fields


class DBTable(metaclass=MetaTable):
    # initial attributes
    _table_: ClassVar[str]
    _schema_: ClassVar[str]
    _just_for_typing_: ClassVar[str]
    _extendable_: ClassVar[bool]
    _discriminator_: ClassVar[typing.Any]
    _meta_: ClassVar[bool]

    # state attributes
    _db_: DBFactory | None
    _modified_fields_: set[str]

    class DB:
        table: ClassVar[str] = None            # Database table name *
        schema: ClassVar[str] = None           # Database schema name *
        just_for_typing: ClassVar[bool] = False # Mark table as virtual (defined inline for foreign schema imports)
        snake_name: ClassVar[str]              # "snake" style table name in plural
        extendable: ClassVar[bool] = False     # support for extendable classes
        cid: ClassVar[DBField] = None          # CID field (if declared)
        is_root: ClassVar[bool] = False        # is root of extendable tree
        discriminator: ClassVar[typing.Any]    # inherited table inner code
        owner: ClassVar[typing.Union[str, type[DBTable]]] = None # table owner of sub table
        subtables: ClassVar[dict[str, type[DBTable]]] = None   # sub tables list
        meta: ClassVar[bool] = False           # mark table as meta table (abstract) *
        pk: ClassVar[DBField] = None           # reference to primary field
        body: ClassVar[DBField] = None         # reference to body field of None
        many_fields: ClassVar[dict[str, DBManyField]] = None
        many_to_many_fields: ClassVar[dict[str, DBManyToManyField]] = None
        fields: ClassVar[dict[str, DBField]] = None  # list of all fields
        # * marked attributes are able to modify by descendants

    class ItemGetter:
        def __init__(self, db: DBFactory, table: type[DBTable], pk_id: Any, view: str = None):
            self._db = db
            self._table = table
            self._pk_id = pk_id
            self._view = view

        def __str__(self):
            return self._view or self._pk_id

        def __getattr__(self, item):
            if item.startswith('_'):
                return super().__getattribute__(self, item)

            related = self._db.query(self._table).select('pk', item).get(self._pk_id)
            return getattr(related, item)

    @classmethod
    def resolve_types(cls, globalns):

        # eval annotations
        for name, t in typing.get_type_hints(cls, globalns, globals()).items():
            if name not in cls.DB.fields: # or cls.fields[name].type is not None:
                continue
            field: DBField = cls.DB.fields[name]
            if cls.resolve_type(t, field, globalns):
                setattr(cls, name, list())
                del cls.DB.fields[name]

        # eval owner
        if isinstance(cls.DB.owner, str):
            base_cls: type[DBTable] = getattr(sys.modules[cls.__module__], cls.DB.owner)
            # field_name = camel2snake(cls.__name__)
            field = DBField()
            field.set_name(base_cls.DB.table)
            field.type = base_cls
            field.ref = True
            field.required = True
            cls.DB.owner = base_cls
            cls.DB.fields[field.column] = field

        # resolve types for subclasses
        for name, t in vars(cls).items():
            if inspect.isclass(t) and issubclass(t, DBTable):
                cls.DB.subtables[t.DB.snake_name] = t
                t.DB.schema = cls.DB.schema
                t.resolve_types(globalns)

    @classmethod
    def resolve_type(cls, t: Union[type, typing._GenericAlias], field: DBField, globalns) -> bool | None:
        if t in KNOWN_TYPES or inspect.isclass(t) and issubclass(t, IntEnum):
            # Base type
            field.type = t
            return
        elif hasattr(t, '__origin__'):  # Union cannot be used with isinstance()
            if t.__origin__ is typing.Union and len(t.__args__) == 2 and t.__args__[1] is type(None):
                # 'Optional' annotation
                field.required = False
                DBTable.resolve_type(t.__args__[0], field, globalns)
                return
            elif t.__origin__ in [Many, ManyToMany] and len(t.__args__) == 1:
                # resolve Many later
                t2 = t.__args__[0]
                if isinstance(t2, typing.ForwardRef):
                    field_type = t2._evaluate(globalns, {})
                elif inspect.isclass(t2) and issubclass(t2, DBTable):
                    field_type = t2
                else:
                    raise QuazyFieldTypeError(f'Many type should be reference to other DBTable')
                if t.__origin__ is Many:
                    cls.DB.many_fields[field.name] = DBManyField(field_type, field.reverse_name)
                else:
                    cls.DB.many_to_many_fields[field.name] = DBManyToManyField(field_type, field.reverse_name)
                return True
            elif t.__origin__ is FieldCID:
                # Field CID declaration
                field.type = t.__args__[0] if t.__args__ else str
                return
            elif t.__origin__ is Property:
                field.prop = True
                cls.resolve_type(t.__args__[0], field, globalns)
                return
        elif isinstance(t, types.UnionType):
            if len(t.__args__) == 2 and t.__args__[1] is type(None):
                field.required = False
                DBTable.resolve_type(t.__args__[0], field, globalns)
                return
        elif t is FieldCID:
            field.type = str
            return
        elif t is FieldBody:
            field.type = dict
            return
        elif isinstance(t, typing.ForwardRef):
            field.ref = True
            field.type = t._evaluate(globalns, {})
            return
        elif inspect.isclass(t) and issubclass(t, DBTable):
            # Foreign key
            field.ref = True
            field.type = t
            return
        raise QuazyFieldTypeError(f'type {t} is not supported as field type')

    @classmethod
    def resolve_types_many(cls, add_middle_table: Callable[[type[DBTable]], Any]):
        # eval refs
        for name, field in cls.DB.fields.items():
            if field.ref:
                rev_name = field.reverse_name or cls.DB.snake_name
                if rev_name in field.type.DB.many_fields:
                    if field.type.DB.many_fields[rev_name].source_table is not cls:
                        raise QuazyFieldNameError(f'Cannot reuse Many field in table `{field.type.__name__}` with name `{rev_name}`, it is associated with table `{field.type.DB.many_fields[rev_name].source_table.__name__}`. Set different `reverse_name`.')
                    field.type.DB.many_fields[rev_name].source_field = name
                else:
                    field.type.DB.many_fields[rev_name] = DBManyField(cls, name)

        # check Many fields connected
        for name, field in cls.DB.many_fields.items():
            if not field.source_field or field.source_field not in field.source_table.DB.fields:
                raise QuazyFieldTypeError(f'Cannot find reference from table `{field.source_table.__name__}` to table `{cls.__name__}` to connect with Many field `{name}`. Add field to source table or change field type to `ManyToMany`')

        # check and connect ManyToMany fields
        for name, field in cls.DB.many_to_many_fields.items():
            if field.middle_table:
                continue

            middle_table_name = "{}{}".format(cls.__qualname__, name.capitalize())
            middle_table_inner_name = "{}_{}".format(cls.DB.table, name)
            rev_name = field.source_field or cls.DB.snake_name
            if rev_name in field.source_table.DB.many_to_many_fields and field.source_table.DB.many_to_many_fields[rev_name].source_table is not cls:
                raise QuazyFieldNameError(f'Cannot reuse ManyToMany field in table `{field.source_table.__name__}` with name `{rev_name}`, it is associated with table `{field.source_table.DB.many_to_many_fields[rev_name].source_table.__name__}`. Set different `reverse_name`.')

            f1 = DBField(field.source_table.DB.table, indexed=True)
            f1.set_name(f1.column)
            f1.type = field.source_table
            f1.ref = True
            f2 = DBField(cls.DB.table, indexed=True)
            f2.set_name(f2.column)
            f2.type = cls
            f2.ref = True

            TableClass: type[DBTable] = typing.cast(type[DBTable],
                type(middle_table_name, (DBTable,), {
                    '__qualname__': middle_table_name,
                    '__module__': cls.__module__,
                    '__annotations__': {
                        f1.name: f1.type,
                        f2.name: f2.type
                    },
                    '_table_': middle_table_inner_name,
                    f1.name: f1,
                    f2.name: f2,
                }))
            add_middle_table(TableClass)

            field.middle_table = TableClass
            field.source_field = f2.column
            field.source_table.DB.many_to_many_fields[rev_name].middle_table = TableClass
            field.source_table.DB.many_to_many_fields[rev_name].source_field = f1.column


    def __init__(self, **initial):
        self._modified_fields_: set[str] | None = None
        self._db_ = initial.pop('_db_', None)
        #self.id: Union[None, int, UUID] = None
        #for field_name, field in self.fields.items():
        #    if field.many_field:
        #        setattr(self, field_name, set())
        for field_name, field in self.DB.many_fields.items():
            setattr(self, field_name, set())
        for k, v in initial.items():
            if k.endswith("__view"):
                continue
            if k not in self.DB.fields and k not in self.DB.many_fields and k not in self.DB.many_to_many_fields:
                raise QuazyFieldNameError(f'Wrong field name `{k}` in new instance of `{self.__class__.__name__}`')
            # TODO: validate types
            if self._db_:
                if field := self.DB.fields.get(k):
                    if issubclass(field.type, IntEnum):
                        setattr(self, k, field.type(v))
                        continue
                    elif field.ref:
                        view = initial.get(f'{k}__view', None)
                        setattr(self, k, DBTable.ItemGetter(self._db_, field.type, v, view))
                        continue
            setattr(self, k, v)
        if self.DB.pk.name not in initial:
            self.pk = None
        for field_name in self.DB.subtables:
            setattr(self, field_name, list())
        self._modified_fields_ = set()

    def __setattr__(self, key, value):
        if key in self.DB.fields:
            if self._modified_fields_ is not None:
                self._modified_fields_.add(key)
        return super().__setattr__(key, value)

    @classmethod
    def _dump_schema(cls) -> dict[str, Any]:
        res = {
            'qualname': cls.__qualname__,
            'module': cls.__module__,
            'table': cls.DB.table,
            'schema': cls.DB.schema,
            'fields': {name: f._dump_schema() for name, f in cls.DB.fields.items()},
        }
        for col in 'extendable discriminator just_for_typing'.split():
            if val := getattr(cls.DB, col):
                res[col] = val
        return res

    @classmethod
    def _load_schema(cls, state: dict[str, Any]) -> type[DBTable]:
        fields = {name: DBField._load_schema(f) for name, f in state['fields'].items()}
        TableClass: type[DBTable] = typing.cast(type[DBTable], type(state['qualname'], (DBTable, ), {
            '__qualname__': state['qualname'],
            '__module__': state['module'],
            '__annotations__': {name: f._pre_type for name, f in fields.items()},
            '_table_': state['table'],
            '_schema_': state['schema'],
            '_just_for_typing_': state.get('just_for_typing', False),
            '_extendable_': state.get('extendable', False),
            '_discriminator_': state.get('discriminator'),
            **fields
        }))
        for name, f in TableClass.DB.fields.items():
            if f.pk:
                TableClass.DB.pk = f
            elif f.cid:
                TableClass.DB.cid = f

        return TableClass

    @property
    def pk(self):
        return getattr(self, self.DB.pk.name)

    @pk.setter
    def pk(self, value):
        setattr(self, self.DB.pk.name, value)

    def __repr__(self):
        res = []
        for k, v in vars(self).items():
            if not k.startswith('_'):
                res.append(f'{k}: {str(v)} ({type(v).__name__})')
        return '\n'.join(res)

    @classmethod
    def _view(cls, item: DBQueryField):
        return None

    def __eq__(self, other):
        return self.pk == other.pk if isinstance(other, DBTable) else other

    def __ne__(self, other):
        return self.pk != other.pk if isinstance(other, DBTable) else other

    def _before_update(self, db: DBFactory):
        pass

    def _after_update(self, db: DBFactory):
        pass

    def _before_insert(self, db: DBFactory):
        pass

    def _after_insert(self, db: DBFactory):
        pass
