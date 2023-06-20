from __future__ import annotations

import sys
import re
import inspect
from contextlib import contextmanager
from dataclasses import dataclass, field as data_field

import psycopg
import psycopg_pool
from psycopg.rows import namedtuple_row

from .exceptions import *

from .db_types import *
from .translator import Translator

import typing
from typing import ClassVar
if typing.TYPE_CHECKING:
    from typing import *
    from types import SimpleNamespace
    import asyncpg
    from .query import DBQuery, DBJoin, DBJoinKind

__all__ = ['DBFactory', 'DBField', 'DBTable', 'UX', 'Many']


def camel2snake(name: str) -> str:
    return camel2snake.r.sub(r'_\1', name).lower()


camel2snake.r = re.compile(
    '((?<=[a-z0-9])[A-Z]|(?!^)(?<!_)[A-Z](?=[a-z]))')  # tnx to https://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-snake-case


class DBFactory:
    _trans = Translator

    def __init__(self, connection_pool, debug_mode: bool = False):
        self._connection_pool: psycopg_pool.ConnectionPool = connection_pool
        self._tables: List[Type[DBTable]] = []
        self._debug_mode = debug_mode

    @staticmethod
    def postgres(*args, **kwargs) -> DBFactory:
        debug_mode = kwargs.pop("debug_mode", False)
        conninfo = kwargs.pop("conninfo")
        try:
            pool = psycopg_pool.ConnectionPool(conninfo, *args, kwargs=kwargs)
            pool.wait()
        except Exception as e:
            print(str(e))
        return DBFactory(pool, debug_mode)

    def use(self, cls: Type[DBTable], schema: str = 'public'):
        cls._schema_ = schema
        self._tables.append(cls)
        setattr(self, cls.__name__, cls)
        return cls

    def use_module(self, name: str = None, schema: str = 'public'):
        if name:
            globalns = vars(sys.modules[name])
        else:
            globalns = sys._getframe(1).f_locals
        tables: List[Type[DBTable]] = []
        for v in globalns.values():
            if inspect.isclass(v) and v is not DBTable and issubclass(v, DBTable) and not v._meta_:
                tables.append(v)
                self.use(v, schema)
        for table in tables:
            table.resolve_types(globalns)

    def query(self, table_class: Optional[Type[DBTable]] = None) -> DBQuery:
        from .query import DBQuery
        return DBQuery(self, table_class)

    def lookup(self, table_class: Type[DBTable], **fields) -> SimpleNamespace:
        query = self.query(table_class)
        query.select_all()
        for k, v in fields.items():
            query.filter(lambda s: getattr(s, k) == v)
        return query.fetchone()

    def get(self, table_class: Type[DBTable], **fields) -> DBTable:
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

    def clear(self):
        with self._connection_pool.connection() as conn:  # type: psycopg.Connection
            tables = []
            for res in conn.execute('select schemaname, tablename from pg_tables where schemaname not in (\'pg_catalog\', \'information_schema\')'):
                tables.append(f'"{res[0]}"."{res[1]}"')
            with conn.transaction():
                for table in tables:
                    conn.execute(f'DROP TABLE {table} CASCADE')

    def all_tables(self, schema: str = 'public') -> list[typing.Type[DBTable]]:
        all_tables = set(self._tables)
        for table in self._tables:
            if table._schema_ == schema:
                all_tables = all_tables.union(table._subtables_.values())
        return all_tables

    def create(self, schema: str = None):
        all_schemas = set()
        all_tables = set(self._tables)
        for table in self._tables:
            all_tables = all_tables.union(table._subtables_.values())
            if table._schema_:
                all_schemas.add(table._schema_)

        with self.select("SELECT CONCAT(table_schema,'.',table_name) as table FROM information_schema.tables WHERE table_type LIKE 'BASE TABLE' AND table_schema NOT IN ('pg_catalog', 'information_schema')") as created_tables_query:
            created_tables = [t.table for t in created_tables_query.fetchall()]
        for table in list(all_tables):
            if table._schema_ + '.' + table._table_ in created_tables or schema and table._schema_ != schema:
                all_tables.remove(table)

        if not all_tables:
            return

        with self._connection_pool.connection() as conn:  # type: psycopg.Connection
            with conn.transaction():
                for schema in all_schemas:
                    conn.execute(self._trans.create_schema(schema))
                for table in all_tables:
                    conn.execute(self._trans.create_table(table))
                    for field in table.fields.values():
                        if field.indexed:
                            conn.execute(self._trans.create_index(table, field))

                for table in all_tables:
                    for field in table.fields.values():
                        if field.ref: # and not field.many_field:
                            conn.execute(self._trans.add_reference(table, field))

    def insert(self, item: DBTable):
        item._before_insert(self)
        fields: List[Tuple[DBField, Any]] = []
        for name, field in item.fields.items():
            fields.append((field, getattr(item, name, DefaultValue)))
        with self._connection_pool.connection() as conn:  # type: psycopg.Connection
            sql, values = self._trans.insert(item.__class__, fields)
            with conn.cursor(binary=True) as curr:
                new_id = curr.execute(sql, values).fetchone()[0]
            setattr(item, item._pk_.name, new_id)

            for field_name, table in item._subtables_.items():
                for row in getattr(item, field_name):
                    setattr(row, item._table_, item)
                    fields.clear()
                    for name, field in table.fields.items():
                        fields.append((field, getattr(row, name, DefaultValue)))
                    sql, values = self._trans.insert(row.__class__, fields)
                    with conn.cursor(binary=True) as curr:
                        new_sub_id = conn.execute(sql, values).fetchone()[0]
                    setattr(row, table._pk_.name, new_sub_id)

        item._after_insert(self)

    def update(self, item: DBTable):
        item._before_update(self)
        fields: List[Tuple[DBField, Any]] = []
        for name in item._modified_fields_:
            field = item.fields[name]
            fields.append((field, getattr(item, name, DefaultValue)))
        with self._connection_pool.connection() as conn:
            sql, values = self._trans.update(item.__class__, fields)
            values['v1'] = getattr(item, item._pk_.name)
            with conn.cursor(binary=True) as curr:
                curr.execute(sql, values)

            for table in item._subtables_.values():
                if table._is_dependent_:
                    with conn.cursor(binary=True) as curr:
                        sql = self._trans.delete_related(table, item._table_)
                        curr.execute(sql, (getattr(item, item._pk_.name), ))
                    for row in getattr(item, table._snake_name_):
                        self.insert(row)
        item._after_update(self)

    @contextmanager
    def select(self, query: Union[DBQuery, str]):
        from quazy.query import DBQuery
        with self._connection_pool.connection() as conn:
            if isinstance(query, DBQuery):
                sql = self._trans.select(query)
                if self._debug_mode: print(sql)
                with conn.cursor(binary=True, row_factory=namedtuple_row) as curr:
                    yield curr.execute(sql, query.args)
            else:
                with conn.cursor(binary=True, row_factory=namedtuple_row) as curr:
                    yield curr.execute(query)

    def save(self, item: DBTable, lookup_field: str | None = None):
        pk_name = item.__class__._pk_.name
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


@dataclass
class DBField:
    name: str = data_field(default='', init=False)         # field name in Python
    column: str = data_field(default='')                   # field/column name in database
    type: Union[Type[DBTable], Type[Any]] = data_field(default=None, init=False)  # field type class
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

    def _dump_schema(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'column': self.column,
            'type': db_type_name(self.type),
            'pk': self.pk,
            'cid': self.cid,
            'ref': self.ref,
            'body': self.body,
            'prop': self.prop,
            'required': self.required,
            'indexed': self.indexed,
            'unique': self.unique,
            'default_sql': self.default_sql,
        }

    @classmethod
    def _load_schema(cls, state: Dict[str, Any]) -> DBField:
        field = cls(**state)
        field.type = db_type_by_name(field.type)
        return field


@dataclass
class UX:
    title: str = data_field(default='')
    width: int = data_field(default=None)
    choices: Mapping = data_field(default=None)
    blank: bool = data_field(default=False)
    readonly: bool = data_field(default=False)
    multiline: bool = data_field(default=False)


class MetaTable(type):
    def __new__(cls, clsname: str, bases: Tuple[Type[DBTable], ...], attrs):
        if clsname == 'DBTable':
            return super().__new__(cls, clsname, bases, attrs)

        attrs['_many_fields_'] = {}
        MetaTable.collect_fields(bases, attrs)

        qualname = attrs['__qualname__']
        if '_table_' not in attrs:
            attrs['_table_'] = camel2snake(qualname.replace('.', ''))

        if '.' in qualname:
            # save owner class name
            chunks = qualname.split('.')
            base_cls_name = '.'.join(chunks[:-1])
            attrs['_owner_'] = base_cls_name
            field_name = camel2snake(chunks[-1])+'s'
            attrs['_snake_name_'] = field_name
            if field_name in attrs['fields']:
                raise QuazySubclassError(f'Subclass name {qualname} repeats field name')
            attrs['_is_dependent_'] = True
        else:
            attrs['_snake_name_'] = camel2snake(qualname)+'s'

        if '_meta_' not in attrs:
            attrs['_meta_'] = False
        attrs['_subtables_'] = {}
        return super().__new__(cls, clsname, bases, attrs)

    @staticmethod
    def collect_fields(bases: Tuple[Type[DBTable], ...], attrs: Dict[str, Any]):
        fields = MetaTable.collect_bases_fields(bases)
        has_pk = False
        if '__annotations__' in attrs:
            for name, info in attrs['__annotations__'].items():  # type: str, Dict[str, Type]
                if name.startswith('_') or name == 'fields':
                    continue
                field: DBField = attrs.get(name, DBField())
                field.set_name(name)
                fields[name] = field
                if field.pk:
                    has_pk = True
                    attrs['_pk_'] = field

        if not has_pk:
            pk = DBField(pk=True)
            pk.set_name('id')
            pk.type = int
            fields['id'] = pk
            attrs['_pk_'] = pk

        attrs['fields'] = fields

    @staticmethod
    def collect_bases_fields(bases: Tuple[Type, ...]) -> Dict[str, DBField]:
        # TODO: extendable
        fields = {}
        for base in bases:
            if base is DBTable:
                break
            if not issubclass(base, DBTable):
                continue
            base_fields = MetaTable.collect_bases_fields(base.__bases__)
            fields.update(base_fields)
            fields.update(base.fields)
        return fields


class DBTable(metaclass=MetaTable):
    _table_: ClassVar[str]                   # Database table name *
    _snake_name_: ClassVar[str]              # "snake" style table name in plural
    _extendable_: ClassVar[bool] = False     # support for extandables by props *
    _schema_: ClassVar[str] = None           # Database schema name *
    _types_resolved_: ClassVar[bool] = False # field types resolution status
    _owner_: ClassVar[typing.Union[str, typing.Type[DBTable]]] = None # table owner of sub table
    _subtables_: ClassVar[typing.Dict[str, typing.Type[DBTable]]] = None   # sub tables list
    _meta_: ClassVar[bool] = False           # mark table as meta table (abstract) *
    _pk_: ClassVar[DBField] = None           # reference to primary field
    #_is_dependent_: bool = False              # table dependent to another (class in class)
    _many_fields_: ClassVar[typing.Dict[str, typing.Type[DBTable]]] = None
    fields: ClassVar[typing.Dict[str, DBField]] = None  # list of all fields
    # * marked attributes are able to modify by descendants

    @classmethod
    def resolve_types(cls, globalns):
        if cls._types_resolved_:
            return

        # eval annotations
        for name, t in typing.get_type_hints(cls, globalns, globals()).items():
            if name not in cls.fields or cls.fields[name].type is not None:
                continue
            field: DBField = cls.fields[name]
            if cls.resolve_type(t, field, globalns):
                del cls.fields[name]
                continue
            if field.ref:
                fname = field.reverse_name or cls._snake_name_
                if fname in field.type.fields and field.type.fields[fname].type != cls:
                    raise QuazyFieldNameError(f'Cannot create Many field in table {field.type.__name__} by name {fname}. Set different reverse_name.')
                #rev_field = DBField()
                #rev_field.set_name(fname)
                #rev_field.many_field = True
                #rev_field.type = cls
                #field.type.fields[fname] = rev_field
                field.type._many_fields_[fname] = cls
                #field.type._subtables_[fname] = cls
                setattr(field.type, fname, set())

        # eval sub classes
        for name, t in cls.__dict__.items():
            if not name.startswith('_') and inspect.isclass(t) and issubclass(t, DBTable):
                cls._subtables_[t._snake_name_] = t
                t._schema_ = cls._schema_
                #cls._many_fields_[name] = t
                #field = DBField()
                #field.set_name(t._snake_name_)
                #field.many_field = True
                #field.type = t
                #field.reverse_name = cls._table_
                #cls.fields[field.column] = field
                t.resolve_types(globalns)

        # eval owner
        if isinstance(cls._owner_, str):
            base_cls: DBTable = getattr(sys.modules[cls.__module__], cls._owner_)
            # field_name = camel2snake(cls.__name__)
            field = DBField()
            field.set_name(base_cls._table_)
            field.type = base_cls
            field.ref = True
            field.required = True
            cls._owner_ = field
            cls.fields[field.column] = field

        # resolve types for subclasses
        for name, value in vars(cls).items():
            if inspect.isclass(value) and issubclass(value, DBTable):
                value.resolve_types(globalns)

        cls._types_resolved_ = True

    @classmethod
    def resolve_type(cls, t: Union[Type, typing._GenericAlias], field: DBField, globalns) -> bool:
        if t in KNOWN_TYPES:
            # Base type
            field.type = t
            return False
        elif hasattr(t, '__origin__'):  # Union cannot be used with isinstance()
            if t.__origin__ is typing.Union and len(t.__args__) == 2 and t.__args__[1] is type(None):
                # 'Optional' annotation
                field.required = False
                DBTable.resolve_type(t.__args__[0], field, globalns)
                return False
            elif t.__origin__ is set and len(t.__args__) == 1:
                # 'Many' annotation
                #field.many_field = True
                #DBTable.resolve_type(t.__args__[0], field, globalns)
                t2 = t.__args__[0]
                if isinstance(t2, typing.ForwardRef):
                    t3 = t2._evaluate(globalns, {})
                    #cls._subtables_[field.name] = t3
                    cls._many_fields_[field.name] = t3
                elif inspect.isclass(t2):
                    #cls._subtables_[field.name] = t2
                    cls._subtables_[field.name] = t2
                else:
                    raise QuazyFieldTypeError(f'Use DBTable type with many field instead of {t2}')
                return True
        elif isinstance(t, typing.ForwardRef):
            field.ref = True
            field.type = t._evaluate(globalns, {})
            return False
        elif inspect.isclass(t):
            # Foreign key
            field.ref = True
            field.type = t
            return False
        raise QuazyFieldTypeError(f'Type {t} is not supported as field type')

    def __init__(self, **initial):
        self._modified_fields_: Set[str] = set()
        self.id: Union[None, int, UUID] = None
        #for field_name, field in self.fields.items():
        #    if field.many_field:
        #        setattr(self, field_name, set())
        for field_name, field in self._many_fields_.items():
            setattr(self, field_name, set())
        for k, v in initial.items():
            if k not in self.fields:
                raise QuazyFieldNameError(f'Wrong field name {k} in new instance of {self._table_}')
            # TODO: validate types
            setattr(self, k, v)
        for field_name in self._subtables_:
            setattr(self, field_name, set())

    def __setattr__(self, key, value):
        if key in self.fields:
            self._modified_fields_.add(key)
        return super().__setattr__(key, value)

    @classmethod
    def _dump_schema(cls):
        return {
            'qualname': cls.__qualname__,
            'table': cls._table_,
            'fields': {name:f._dump_schema() for name, f in cls.fields},
            'subtables': {name:t._dump_schema() for name, t in cls._subtables_},
        }

    @classmethod
    def _load_schema(cls, state):
        fields = {name: DBField._load_schema(f) for name, f in state['fields']}
        fields |= {name: DBTable._load_schema(t) for name, t in state['subtables']}
        return type(state['qualname'], (DBTable, ), {'_table_': state['table'], **fields})

    def _before_update(self, db: DBFactory):
        pass

    def _after_update(self, db: DBFactory):
        pass

    def _before_insert(self, db: DBFactory):
        pass

    def _after_insert(self, db: DBFactory):
        pass
