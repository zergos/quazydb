from __future__ import annotations

import sys
import re
import inspect
from dataclasses import dataclass, field as data_field, InitVar
from . import syncpg
from .syncpg import func_sync
from .exceptions import *

from .db_types import *
from .translator import Translator

import typing
from typing import ClassVar
if typing.TYPE_CHECKING:
    from typing import *
    import asyncpg

__all__ = ['DBFactory', 'DBField', 'DBTable', 'UX', 'Many']


def camel2snake(name: str) -> str:
    return camel2snake.r.sub(r'_\1', name).lower()


camel2snake.r = re.compile(
    '((?<=[a-z0-9])[A-Z]|(?!^)(?<!_)[A-Z](?=[a-z]))')  # tnx to https://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-snake-case


class DBFactory:
    _trans = Translator

    def __init__(self, connection_pool):
        self._connection_pool = connection_pool
        self._tables: List[Type[DBTable]] = []

    @staticmethod
    def postgres(*args, **kwargs) -> DBFactory:
        pool = syncpg.create_pool(*args, **kwargs)
        pool._async__init__()
        return DBFactory(pool)

    def use(self, cls: Type[DBTable]):
        self._tables.append(cls)
        setattr(self, cls.__name__, cls)
        return cls

    def use_module(self, name: str = None, schema: str = None):
        if name:
            globalns = vars(sys.modules[name])
        else:
            globalns = sys._getframe(1).f_locals
        tables: List[Type[DBTable]] = []
        for v in globalns.values():
            if inspect.isclass(v) and v is not DBTable and issubclass(v, DBTable) and not v._meta_:
                tables.append(v)
                self.use(v)
        for table in tables:
            table.resolve_types(globalns)

    def get_connection(self) -> asyncpg.connection.Connection:
        return self._connection_pool.acquire()

    @func_sync
    async def clear(self):
        async with self.get_connection() as conn:  # type: asyncpg.connection.Connection
            tables = []
            for res in await conn.fetch('select schemaname, tablename from pg_tables where schemaname not in (\'pg_catalog\', \'information_schema\')'):
                tables.append(f'"{res[0]}"."{res[1]}"')
            async with conn.transaction():
                for table in tables:
                    await conn.execute(f'DROP TABLE {table} CASCADE')

    @func_sync
    async def create(self):
        all_tables = self._tables.copy()
        for table in self._tables:
            all_tables.extend(table._subtables_)
        async with self.get_connection() as conn:  #type: asyncpg.connection.Connection
            async with conn.transaction():
                for table in all_tables:
                    await conn.execute(self._trans.create_table(table))
                    for field in table.fields.values():
                        if field.indexed:
                            await conn.execute(self._trans.create_index(table, field))

                for table in all_tables:
                    for field in table.fields.values():
                        if field.ref:
                            await conn.execute(self._trans.add_reference(table, field))

    @func_sync
    async def insert(self, item: DBTable):
        fields: List[Tuple[DBField, Any]] = []
        for name, field in item.fields.items():
            fields.append((field, getattr(item, name, DefaultValue)))
        async with self.get_connection() as conn:
            sql, values = self._trans.insert(item.__class__, fields)
            new_id = await conn.fetchval(sql, *values)
            setattr(item, item._pk_.name, new_id)

            for table in item._subtables_:
                for row in getattr(item, table._subname_):
                    setattr(row, item._table_, item)
                    fields.clear()
                    for name, field in table.fields.items():
                        fields.append((field, getattr(row, name, DefaultValue)))
                    sql, values = self._trans.insert(row.__class__, fields)
                    new_sub_id = await conn.fetchval(sql, *values)
                    setattr(row, table._pk_.name, new_sub_id)


@dataclass
class DBField:
    name: str = data_field(default='', init=False)
    column: str = data_field(default='')
    type: Type = data_field(default=None, init=False)
    pk: bool = data_field(default=False)
    cid: bool = data_field(default=False)
    ref: bool = data_field(default=False, init=False)
    body: bool = data_field(default=False)
    prop: bool = data_field(default=False)
    required: bool = data_field(default=True, init=False)
    indexed: bool = data_field(default=False)
    unique: bool = data_field(default=False)
    default: Union[Any, Callable[[], Any]] = data_field(default=None)
    default_sql: str = data_field(default=None)
    reverse_name: str = data_field(default=None)
    many_field: bool = data_field(default=False, init=False)
    ux: Optional[UX] = data_field(default=None)

    def set_name(self, name: str):
        self.name = name
        if not self.column:
            self.column = self.name
        if not self.ux:
            self.ux = UX(self.name)
        elif not self.ux.title:
            self.ux.title = self.name


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
            attrs['_subname_'] = field_name
            if field_name in attrs['fields']:
                raise QuazySubclassError(f'Subclass name {qualname} repeats field name')

        if '_meta_' not in attrs:
            attrs['_meta_'] = False
        attrs['_subtables_'] = []
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
    _table_: ClassVar[str]
    _extendable_: ClassVar[bool] = False
    _schema_: ClassVar[str] = None
    _types_resolved_: ClassVar[bool] = False
    _owner_: ClassVar[typing.Union[str, typing.Type[DBTable]]] = None
    _subtables_: ClassVar[typing.List[typing.Type[DBTable]]] = None
    _subname_: ClassVar[str] = None
    _meta_: ClassVar[bool] = False
    _pk_: ClassVar[DBField] = None
    fields: ClassVar[typing.Dict[str, DBField]] = None

    @classmethod
    def resolve_types(cls, globalns):
        if cls._types_resolved_:
            return

        # eval annotations
        for name, t in typing.get_type_hints(cls, globalns, globals()).items():
            if name not in cls.fields or cls.fields[name].type is not None:
                continue
            cls.resolve_type(t, cls.fields[name], globalns)

        # eval sub classes
        for name, t in cls.__dict__.items():
            if not name.startswith('_') and inspect.isclass(t) and issubclass(t, DBTable):
                cls._subtables_.append(t)
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

    @staticmethod
    def resolve_type(t: Union[Type, typing._GenericAlias], field: DBField, globalns):
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
            elif t.__origin__ is set and len(t.__args__) == 1:
                # 'Many' annotation
                field.many_field = True
                DBTable.resolve_type(t.__args__[0], field, globalns)
                return
        elif isinstance(t, typing.ForwardRef):
            field.ref = True
            field.type = t._evaluate(globalns, {})
            return
        elif inspect.isclass(t):
            # Foreign key
            field.ref = True
            field.type = t
            return
        raise QuazyFieldTypeError(f'Type {t} is not supported as field type')

    def __init__(self, **initial):
        self.id: Union[None, int, UUID] = None
        for k, v in initial.items():
            if k not in self.fields:
                raise QuazyFieldNameError(f'Wrong field name {k} in new instance of {self._table_}')
            # TODO: validate types
            setattr(self, k, v)
        for cls in self._subtables_:
            setattr(self, cls._subname_, set())
