from __future__ import annotations

import sys
import re
import inspect
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field as data_field

import psycopg
import psycopg_pool
from psycopg.rows import namedtuple_row, class_row, dict_row

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
        self._tables: list[Type[DBTable]] = list()
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

    def use(self, cls: Type[DBTable], schema: str = 'public'):
        if not cls._schema_:
            cls._schema_ = schema
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
        tables: list[Type[DBTable]] = list()
        for v in globalns.values():
            if inspect.isclass(v) and v is not DBTable and issubclass(v, DBTable) and not v._meta_:
                tables.append(v)
                self.use(v, schema)
        for table in tables:
            table.resolve_types(globalns)
        for table in tables:
            table.resolve_types_many(lambda t: self.use(t, schema))

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

    def clear(self, schema: str = None):
        with self._connection_pool.connection() as conn:  # type: psycopg.Connection
            tables = []
            for res in conn.execute(self._trans.select_all_tables()):
                if not schema or schema == res[0]:
                    tables.append(f'"{res[0]}"."{res[1]}"')
            with conn.transaction():
                for table in tables:
                    conn.execute(f'DROP TABLE {table} CASCADE')

    def all_tables(self, schema: str = None) -> list[typing.Type[DBTable]]:

        all_tables = self._tables.copy()
        for table in self._tables:
            all_tables.extend(table._subtables_.values())

        ext: Dict[str, List[Type[DBTable]]] = defaultdict(list)
        for t in all_tables.copy():
            if t._extendable_:
                ext[t._table_].append(t)
                all_tables.remove(t)
            elif schema and t._schema_ != schema:
                all_tables.remove(t)

        if schema:
            for tname, tables in ext.copy().items():
                if not any(t._schema_ == schema for t in tables):
                    del ext[tname]

        for tables in ext.values():
            fields = {}
            annotations = {}
            field_sources = {}
            root_class: Type[DBTable] | None = None
            for t in tables:
                if t._is_root_:
                    root_class = t

                for fname, field in t._fields_.items():
                    if src := field_sources.get(fname):
                        if field.type != fields[fname].type and not issubclass(t, src) and not issubclass(src, t):
                            raise QuazyFieldNameError(f'Same column `{field.name}` in different branches has different type')
                    else:
                        fields[fname] = field
                        field_sources[fname] = t
                    if fname in t.__annotations__:
                        annotations[fname] = t.__annotations__[fname]

            TableClass: Type[DBTable] = typing.cast(typing.Type[DBTable], type(t.__qualname__+"Combined", (DBTable, ), {
                '__qualname__': t.__qualname__+"Combined",
                '__module__': root_class.__module__,
                '__annotations__': annotations,
                '_table_': root_class._table_,
                '_extendable_': False,
                '_types_resolved_': True,
                **fields
            }))
            all_tables.append(TableClass)

        return all_tables

    def missed_tables(self, schema: str = None) -> set[Type[DBTable]]:
        all_tables = self.all_tables(schema)

        with self.select(self._trans.select_all_tables()) as created_tables_query:
            #created_tables = [(t.schema, t.table) for t in created_tables_query]
            created_tables = created_tables_query.fetchall()

        for table in all_tables.copy():
            if (table._schema_, table._table_) in created_tables or schema and table._schema_ != schema:
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
            if table._schema_:
                all_schemas.add(table._schema_)

        with self._connection_pool.connection() as conn:  # type: psycopg.Connection
            with conn.transaction():
                for schema in all_schemas:
                    conn.execute(self._trans.create_schema(schema))
                for table in all_tables:
                    conn.execute(self._trans.create_table(table))
                    for field in table._fields_.values():
                        if field.indexed:
                            conn.execute(self._trans.create_index(table, field))

                for table in all_tables:
                    for field in table._fields_.values():
                        if field.ref: # and not field.many_field:
                            conn.execute(self._trans.add_reference(table, field))

    def insert(self, item: DBTable):
        item._before_insert(self)
        fields: List[Tuple[DBField, Any]] = []
        for name, field in item._fields_.items():
            fields.append((field, getattr(item, name, DefaultValue)))
        with self._connection_pool.connection() as conn:  # type: psycopg.Connection

            sql, values = self._trans.insert(item.__class__, fields)
            item.pk = conn.execute(sql, values).fetchone()[0]

            for field_name, table in item._subtables_.items():
                for row in getattr(item, field_name):
                    setattr(row, item._table_, item)
                    fields.clear()
                    for name, field in table._fields_.items():
                        fields.append((field, getattr(row, name, DefaultValue)))
                    sql, values = self._trans.insert(row.__class__, fields)
                    new_sub_id = conn.execute(sql, values).fetchone()[0]
                    setattr(row, table._pk_.name, new_sub_id)

            for field_name, field in item._many_fields_.items():
                for row in getattr(item, field_name):
                    if getattr(row, field.source_field) != item.pk:
                        setattr(row, field.source_field, item.pk)
                        self.save(row)

            for field_name, field in item._many_to_many_fields_.items():
                for row in getattr(item, field_name):
                    if not row.pk:
                        self.save(row)

                # delete old items, add new items
                new_indices = set(row.pk for row in getattr(item, field_name))
                old_indices_sql = self._trans.select_many_indices(field.middle_table, field.source_field, field.source_table._table_)
                results = conn.execute(old_indices_sql, {"value": item.pk}).fetchone()
                old_indices = set(results[0]) if results[0] else set()

                indices_to_delete = list(old_indices - new_indices)
                indices_to_add = list(new_indices - old_indices)

                if indices_to_delete:
                    delete_indices_sql = self._trans.delete_many_indices(field.middle_table, field.source_field, field.source_table._table_)
                    conn.execute(delete_indices_sql, {"value": item.pk, "indices": indices_to_delete})

                if indices_to_add:
                    new_indices_sql = self._trans.insert_many_index(field.middle_table, field.source_field, field.source_table._table_)
                    for index in indices_to_add:
                        conn.execute(new_indices_sql, {"value": item.pk, "index": index})

        item._after_insert(self)

    def update(self, item: DBTable):
        item._before_update(self)
        fields: List[Tuple[DBField, Any]] = []
        for name in item._modified_fields_:
            field = item._fields_[name]
            fields.append((field, getattr(item, name, DefaultValue)))
        with self._connection_pool.connection() as conn:
            sql, values = self._trans.update(item.__class__, fields)
            values['v1'] = getattr(item, item._pk_.name)
            conn.execute(sql, values)

            for table in item._subtables_.values():
                sql = self._trans.delete_related(table, item._table_)
                conn.execute(sql, (getattr(item, item._pk_.name), ))
                for row in getattr(item, table._snake_name_):
                    self.insert(row)
        item._after_update(self)

    @contextmanager
    def select(self, query: Union[DBQuery, str], as_dict: bool = False):
        from quazy.query import DBQuery
        with self._connection_pool.connection() as conn:
            if isinstance(query, DBQuery):
                sql = self._trans.select(query)
                if self._debug_mode: print(sql)
                if as_dict:
                    row_maker = dict_row
                elif query.fetch_objects:
                    row_maker = class_row(query.table_class)
                else:
                    row_maker = namedtuple_row
                with conn.cursor(binary=True, row_factory=row_maker) as curr:
                    yield curr.execute(sql, query.args)
            else:
                with conn.cursor(binary=True, row_factory=dict_row if as_dict else namedtuple_row) as curr:
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

    def table_exists(self, table: DBTable) -> bool:
        with self.select(self._trans.is_table_exists(table)) as res:
            return res.fetchone()[0]


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
    def _load_schema(cls, state: Dict[str, Any]) -> DBField:
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
    source_table: Type[DBTable]
    source_field: str | None = None


@dataclass
class DBManyToManyField(DBManyField):
    middle_table: Type[DBTable] | None = None


class MetaTable(type):
    def __new__(cls, clsname: str, bases: Tuple[Type[DBTable], ...], attrs):
        if clsname == 'DBTable':
            return super().__new__(cls, clsname, bases, attrs)

        attrs['_many_fields_'] = dict()
        attrs['_many_to_many_fields_'] = dict()
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
            if field_name in attrs['_fields_']:
                raise QuazySubclassError(f'Subclass name {qualname} repeats field name')
        else:
            attrs['_snake_name_'] = camel2snake(qualname)+'s'

        if '_meta_' not in attrs:
            attrs['_meta_'] = False
        attrs['_subtables_'] = dict()

        if '_discriminator_' not in attrs:
            attrs['_discriminator_'] = attrs['__qualname__'] if '_cid_' in attrs else None
        return super().__new__(cls, clsname, bases, attrs)

    @staticmethod
    def collect_fields(bases: Tuple[Type[DBTable], ...], attrs: Dict[str, Any]):

        if attrs.get('_extendable_'):
            attrs['_is_root_'] = True

        fields = MetaTable.collect_bases_fields(bases, attrs)

        has_pk = False
        for name, t in attrs.get('__annotations__', {}).items():  # type: str, str
            if name.startswith('_'):
                continue
            field = attrs.pop(name, DBField())
            if isinstance(field, DBField):
                field.set_name(name)
                if not field.type:
                    field.type = t
                if field.pk:
                    has_pk = True
                    attrs['_pk_'] = field
                elif t is FieldCID or field.cid:
                    # check CID
                    if not attrs.get('_extendable_'):
                        raise QuazyFieldTypeError(f'Table `{attrs["__qualname__"]}` is not declared with _extendable_ attribute')
                    elif attrs.get('_cid_'):
                        raise QuazyFieldTypeError(f'Table `{attrs["__qualname__"]}` has CID field already inherited from extendable')

                    field.cid = True
                    attrs['_cid_'] = field
                elif t is FieldBody or field.body:
                    if attrs.get('_body_'):
                        raise QuazyFieldTypeError(f'Table `{attrs["__qualname__"]}` has body field already')

                    attrs['_body_'] = field
            else:
                field = DBField(default=field)
                field.set_name(name)
                field.type = t

            fields[name] = field

        # check seed proper declaration
        if attrs.get('_cid_') and not attrs.get('_extendable_'):
            raise QuazyFieldTypeError(f'CID field is declared, but table `{attrs["__qualname__"]}` is not declared with _extendable_ attribute')

        if not has_pk:
            pk = DBField(pk=True)
            pk.set_name('id')
            pk.type = int
            fields['id'] = pk
            attrs['_pk_'] = pk

        # do not inherit schema
        if '_schema_' not in attrs:
            attrs['_schema_'] = None

        attrs['_fields_'] = fields

    @staticmethod
    def collect_bases_fields(bases: Tuple[Type, ...], attrs: Dict[str, Any]) -> Dict[str, DBField]:
        fields: Dict[str, DBField] = dict()
        for base in bases:
            if base is DBTable:
                break
            if issubclass(base, DBTable):
                base_fields = MetaTable.collect_bases_fields(base.__bases__, attrs)
                fields.update(base_fields)
                fields.update(base._fields_)

                if base._extendable_:
                    if attrs.get('_extendable_'):
                        raise QuazySubclassError('Multiple inheritance of extendable tables is not supported')
                    attrs['_extendable_'] = True
                    attrs['_is_root_'] = False
                    attrs['_cid_'] = base._cid_
                    attrs['_table_'] = base._table_
                    attrs['_body_'] = base._body_

        return fields


class DBTable(metaclass=MetaTable):
    _table_: ClassVar[str]                   # Database table name *
    _just_for_typing_: ClassVar[bool] = False # Mark table as virtual (defined inline for foreign schema imports)
    _snake_name_: ClassVar[str]              # "snake" style table name in plural
    _extendable_: ClassVar[bool] = False     # support for extendable classes
    _cid_: ClassVar[DBField] = None          # CID field (if declared)
    _is_root_: ClassVar[bool] = False        # is root of extendable tree
    _discriminator_: ClassVar[typing.Any]    # inherited table inner code
    _schema_: ClassVar[str] = None           # Database schema name *
    _types_resolved_: ClassVar[bool] = False # field types resolution status
    _many_types_resolved_: ClassVar[bool] = False
    _owner_: ClassVar[typing.Union[str, typing.Type[DBTable]]] = None # table owner of sub table
    _subtables_: ClassVar[typing.Dict[str, typing.Type[DBTable]]] = None   # sub tables list
    _meta_: ClassVar[bool] = False           # mark table as meta table (abstract) *
    _pk_: ClassVar[DBField] = None           # reference to primary field
    _body_: ClassVar[DBField] = None         # reference to body field of None
    _many_fields_: ClassVar[typing.Dict[str, DBManyField]] = None
    _many_to_many_fields_: ClassVar[typing.Dict[str, DBManyToManyField]] = None
    _fields_: ClassVar[typing.Dict[str, DBField]] = None  # list of all fields
    # * marked attributes are able to modify by descendants

    @classmethod
    def resolve_types(cls, globalns):
        if cls._types_resolved_:
            return

        # eval annotations
        for name, t in typing.get_type_hints(cls, globalns, globals()).items():
            if name not in cls._fields_: # or cls.fields[name].type is not None:
                continue
            field: DBField = cls._fields_[name]
            if cls.resolve_type(t, field, globalns):
                setattr(cls, name, list())
                del cls._fields_[name]

        # eval owner
        if isinstance(cls._owner_, str):
            base_cls: Type[DBTable] = getattr(sys.modules[cls.__module__], cls._owner_)
            # field_name = camel2snake(cls.__name__)
            field = DBField()
            field.set_name(base_cls._table_)
            field.type = base_cls
            field.ref = True
            field.required = True
            cls._owner_ = base_cls
            cls._fields_[field.column] = field

        # resolve types for subclasses
        for name, t in vars(cls).items():
            if name != '_owner_' and inspect.isclass(t) and issubclass(t, DBTable):
                cls._subtables_[t._snake_name_] = t
                t._schema_ = cls._schema_
                t.resolve_types(globalns)

    @classmethod
    def resolve_type(cls, t: Union[Type, typing._GenericAlias], field: DBField, globalns) -> bool | None:
        if t in KNOWN_TYPES:
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
                    cls._many_fields_[field.name] = DBManyField(field_type, field.reverse_name)
                else:
                    cls._many_to_many_fields_[field.name] = DBManyToManyField(field_type, field.reverse_name)
                return True
            elif t.__origin__ is FieldCID:
                # Field CID declaration
                field.type = t.__args__[0] if t.__args__ else str
                return
            elif t.__origin__ is FieldBody:
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
        raise QuazyFieldTypeError(f'Type {t} is not supported as field type')

    @classmethod
    def resolve_types_many(cls, add_middle_table: Callable[[Type[DBTable]], Any]):
        # eval refs
        for name, field in cls._fields_.items():
            if field.ref:
                rev_name = field.reverse_name or cls._snake_name_
                if rev_name in field.type._many_fields_:
                    if field.type._many_fields_[rev_name].source_table is not cls:
                        raise QuazyFieldNameError(f'Cannot reuse Many field in table `{field.type.__name__}` with name `{rev_name}`, it is associated with table `{field.type._many_fields_[rev_name].source_table.__name__}`. Set different `reverse_name`.')
                    field.type._many_fields_[rev_name].source_field = name
                else:
                    field.type._many_fields_[rev_name] = DBManyField(cls, name)

        # check Many fields connected
        for name, field in cls._many_fields_.items():
            if not field.source_field or field.source_field not in field.source_table._fields_:
                raise QuazyFieldTypeError(f'Cannot find reference from table `{field.source_table.__name__}` to table `{cls.__name__}` to connect with Many field `{name}`. Add field to source table or change field type to `ManyToMany`')

        # check and connect ManyToMany fields
        for name, field in cls._many_to_many_fields_.items():
            if field.middle_table:
                continue

            middle_table_name = "{}{}".format(cls.__qualname__, name.capitalize())
            middle_table_inner_name = "{}_{}".format(cls._table_, name)
            rev_name = field.source_field or cls._snake_name_
            if rev_name in field.source_table._many_to_many_fields_ and field.source_table._many_to_many_fields_[rev_name].source_table is not cls:
                raise QuazyFieldNameError(f'Cannot reuse ManyToMany field in table `{field.source_table.__name__}` with name `{rev_name}`, it is associated with table `{field.source_table._many_to_many_fields_[rev_name].source_table.__name__}`. Set different `reverse_name`.')

            f1 = DBField(field.source_table._table_, indexed=True)
            f1.set_name(f1.column)
            f1.type = field.source_table
            f1.ref = True
            f2 = DBField(cls._table_, indexed=True)
            f2.set_name(f2.column)
            f2.type = cls
            f2.ref = True

            TableClass: Type[DBTable] = typing.cast(typing.Type[DBTable],
                type(middle_table_name, (DBTable,), {
                    '__qualname__': middle_table_name,
                    '__module__': cls.__module__,
                    '__annotations__': {
                        f1.name: f1.type,
                        f2.name: f2.type
                    },
                    '_table_': middle_table_inner_name,
                    '_types_resolved_': True,
                    f1.name: f1,
                    f2.name: f2,
                }))
            add_middle_table(TableClass)

            field.middle_table = TableClass
            field.source_field = f2.column
            field.source_table._many_to_many_fields_[rev_name].middle_table = TableClass
            field.source_table._many_to_many_fields_[rev_name].source_field = f1.column

    def __init__(self, **initial):
        self._modified_fields_: Set[str] = set()
        #self.id: Union[None, int, UUID] = None
        #for field_name, field in self.fields.items():
        #    if field.many_field:
        #        setattr(self, field_name, set())
        for field_name, field in self._many_fields_.items():
            setattr(self, field_name, set())
        for k, v in initial.items():
            if k not in self._fields_ and k not in self._many_fields_ and k not in self._many_to_many_fields_:
                raise QuazyFieldNameError(f'Wrong field name `{k}` in new instance of `{self.__class__.__name__}`')
            # TODO: validate types
            setattr(self, k, v)
        if self._pk_.name not in initial:
            self.pk = None
        for field_name in self._subtables_:
            setattr(self, field_name, list())

    def __setattr__(self, key, value):
        if key in self._fields_:
            self._modified_fields_.add(key)
        return super().__setattr__(key, value)

    @classmethod
    def _dump_schema(cls):
        res = {
            'qualname': cls.__qualname__,
            'module': cls.__module__,
            'table': cls._table_,
            'schema': cls._schema_,
            'fields': {name: f._dump_schema() for name, f in cls._fields_.items()},
        }
        for col in 'extendable discriminator just_for_typing'.split():
            if val := getattr(cls, f"_{col}_"):
                res[col] = val
        return res

    @classmethod
    def _load_schema(cls, state):
        fields = {name: DBField._load_schema(f) for name, f in state['fields'].items()}
        TableClass: typing.Type[DBTable] = typing.cast(typing.Type[DBTable], type(state['qualname'], (DBTable, ), {
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
        for name, f in TableClass._fields_.items():
            if f.pk:
                TableClass._pk_ = f
            elif f.cid:
                TableClass._cid_ = f

        return TableClass

    @property
    def pk(self):
        return getattr(self, self._pk_.name)

    @pk.setter
    def pk(self, value):
        setattr(self, self._pk_.name, value)

    def _before_update(self, db: DBFactory):
        pass

    def _after_update(self, db: DBFactory):
        pass

    def _before_insert(self, db: DBFactory):
        pass

    def _after_insert(self, db: DBFactory):
        pass
