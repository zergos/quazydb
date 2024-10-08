from __future__ import annotations
import typing
import types
import re
import sys
import inspect

from .db_field import DBField, UX, DBManyField, DBManyToManyField
from .db_types import *
from .exceptions import *

if typing.TYPE_CHECKING:
    from typing import *
    from .db_factory import DBFactory
    from .query import DBQuery, DBQueryField, DBSQL

__all__ = ['DBTable']

def camel2snake(name: str) -> str:
    return camel2snake.r.sub(r'_\1', name).lower()

camel2snake.r = re.compile(
    '((?<=[a-z0-9])[A-Z]|(?!^)(?<!_)[A-Z](?=[a-z]))')  # tnx to https://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-snake-case


class MetaTable(type):
    db_base_class: type[DBTable.DB]

    def __new__(cls, clsname: str, bases: tuple[type[DBTable], ...], attrs: dict[str, Any]):
        if clsname == 'DBTable':
            cls.db_base_class = attrs['DB']
            return super().__new__(cls, clsname, bases, attrs)

        if 'DB' in attrs:
            raise QuazyError(f'Should not define `DB` subclass directly in `{clsname}`, use `_name_` form')

        spec_attrs = {}
        for name in 'db table title schema just_for_typing extendable discriminator meta'.split():
            src_name = f'_{name}_'
            if value := attrs.pop(src_name, None):
                spec_attrs[name] = value

        DB = typing.cast(type[DBTable.DB], super().__new__(cls, clsname + 'DB', (cls.db_base_class,), spec_attrs))
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
            field_name = camel2snake(chunks[-1]) + 's'
            DB.snake_name = field_name
            if field_name in DB.fields:
                raise QuazySubclassError(f'Subclass name {qualname} repeats field name')
        else:
            DB.snake_name = camel2snake(qualname) + 's'

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
                field.prepare(name)
                if not field.type:
                    field.type = t
                if field.pk:
                    has_pk = True
                    DB.pk = field
                elif t is FieldCID or isinstance(t, str) and t.startswith(FieldCID.__name__) or field.cid:
                    # check CID
                    if not DB.extendable:
                        raise QuazyFieldTypeError(
                            f'Table `{attrs["__qualname__"]}` is not declared with _extendable_ attribute')
                    elif DB.cid:
                        raise QuazyFieldTypeError(
                            f'Table `{attrs["__qualname__"]}` has CID field already inherited from extendable')

                    field.cid = True
                    DB.cid = field
                elif t is FieldBody or t == FieldBody.__name__ or field.body:
                    if DB.body:
                        raise QuazyFieldTypeError(f'Table `{attrs["__qualname__"]}` has body field already')

                    field.body = True
                    DB.body = field
            else:
                field = DBField(default=field)
                field.prepare(name)
                field.type = t
                field.required = False

            fields[name] = field

        # check seed proper declaration
        if DB.cid and not DB.extendable:
            raise QuazyFieldTypeError(
                f'CID field is declared, but table `{attrs["__qualname__"]}` is not declared with `extendable` attribute')

        if not has_pk:
            pk = DBField(pk=True)
            pk.type = int
            pk.prepare('id')
            pk.ux.blank = True
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

    def __getitem__(cls, item: type[DBTable]):
        if not cls.DB.db:
            raise QuazyWrongOperation("Table is not assigned to a database")
        return cls.DB.db.get(cls, item)


class DBTable(metaclass=MetaTable):
    # initial attributes
    _table_: typing.ClassVar[str]
    _title_: typing.ClassVar[str]
    _schema_: typing.ClassVar[str]
    _just_for_typing_: typing.ClassVar[str]
    _extendable_: typing.ClassVar[bool]
    _discriminator_: typing.ClassVar[typing.Any]
    _meta_: typing.ClassVar[bool]
    _lookup_field_: typing.ClassVar[str]

    # state attributes
    _db_: DBFactory | None
    _modified_fields_: set[str]

    class DB:
        db: typing.ClassVar[DBFactory] | None = None  # Database owner, if specified
        table: typing.ClassVar[str] = None  # Database table name *
        title: typing.ClassVar[str] = None  # Table title for UI
        schema: typing.ClassVar[str] = None  # Database schema name *
        just_for_typing: typing.ClassVar[
            bool] = False  # Mark table as virtual (defined inline for foreign schema imports)
        snake_name: typing.ClassVar[str]  # "snake" style table name in plural
        extendable: typing.ClassVar[bool] = False  # support for extendable classes
        cid: typing.ClassVar[DBField] = None  # CID field (if declared)
        is_root: typing.ClassVar[bool] = False  # is root of extendable tree
        discriminator: typing.ClassVar[typing.Any]  # inherited table inner code
        owner: typing.ClassVar[typing.Union[str, type[DBTable]]] = None  # table owner of sub table
        subtables: typing.ClassVar[dict[str, type[DBTable]]] = None  # sub tables list
        meta: typing.ClassVar[bool] = False  # mark table as meta table (abstract) *
        pk: typing.ClassVar[DBField] = None  # reference to primary field
        body: typing.ClassVar[DBField] = None  # reference to body field of None
        many_fields: typing.ClassVar[dict[str, DBManyField]] = None
        many_to_many_fields: typing.ClassVar[dict[str, DBManyToManyField]] = None
        fields: typing.ClassVar[dict[str, DBField]] = None  # list of all fields
        lookup_field: typing.ClassVar[str] = None  # field name for text search
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
        from .db_factory import DBFactory

        # eval annotations
        for name, t in typing.get_type_hints(cls, globals() | globalns, locals()).items():
            if name not in cls.DB.fields:  # or cls.fields[name].type is not None:
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
            field.prepare(base_cls.DB.table)
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
        if t in KNOWN_TYPES or inspect.isclass(t) and issubclass(t, Enum):
            # Base type
            field.type = t
            return

        elif inspect.isclass(t) and issubclass(t, DBField):
            for k, v in t.__dict__.items():
                if k.startswith('_'):
                    continue
                if issubclass(v, UX):
                    for kk, vv in v.__dict__.items():
                        if not kk.startswith('_'):
                            setattr(field.ux, kk, vv)
                else:
                    setattr(field, k, v)
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
                        raise QuazyFieldNameError(
                            f'Cannot reuse Many field in table `{field.type.__name__}` with name `{rev_name}`, it is associated with table `{field.type.DB.many_fields[rev_name].source_table.__name__}`. Set different `reverse_name`.')
                    field.type.DB.many_fields[rev_name].source_field = name
                else:
                    field.type.DB.many_fields[rev_name] = DBManyField(cls, name)

        # check Many fields connected
        for name, field in cls.DB.many_fields.items():
            if not field.source_field or field.source_field not in field.source_table.DB.fields:
                raise QuazyFieldTypeError(
                    f'Cannot find reference from table `{field.source_table.__name__}` to table `{cls.__name__}` to connect with Many field `{name}`. Add field to source table or change field type to `ManyToMany`')

        # check and connect ManyToMany fields
        for name, field in cls.DB.many_to_many_fields.items():
            if field.middle_table:
                continue

            middle_table_name = "{}{}".format(cls.__qualname__, name.capitalize())
            middle_table_inner_name = "{}_{}".format(cls.DB.table, name)
            rev_name = field.source_field or cls.DB.snake_name
            if rev_name in field.source_table.DB.many_to_many_fields and field.source_table.DB.many_to_many_fields[
                rev_name].source_table is not cls:
                raise QuazyFieldNameError(
                    f'Cannot reuse ManyToMany field in table `{field.source_table.__name__}` with name `{rev_name}`, it is associated with table `{field.source_table.DB.many_to_many_fields[rev_name].source_table.__name__}`. Set different `reverse_name`.')

            f1 = DBField(field.source_table.DB.table, indexed=True)
            f1.prepare(f1.column)
            f1.type = field.source_table
            f1.ref = True
            f2 = DBField(cls.DB.table, indexed=True)
            f2.prepare(f2.column)
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
        self._db_: DBFactory = initial.pop('_db_', self.DB.db)
        # self.id: Union[None, int, UUID] = None
        # for field_name, field in self.fields.items():
        #    if field.many_field:
        #        setattr(self, field_name, set())
        for field_name, field in self.DB.many_fields.items():
            setattr(self, field_name, set())
        for k, v in initial.items():
            if k.endswith("__view"):
                continue
            if self._db_:
                if field := self.DB.fields.get(k):
                    if issubclass(field.type, Enum):
                        setattr(self, k, field.type(v) if v is not None else None)
                        continue
                    elif field.ref:
                        view = initial.get(f'{k}__view', None)
                        setattr(self, k, DBTable.ItemGetter(self._db_, field.type, v, view))
                        continue
            # else:
            if k not in self.DB.fields and k not in self.DB.many_fields and k not in self.DB.many_to_many_fields:
                raise QuazyFieldNameError(f'Wrong field name `{k}` in new instance of `{self.__class__.__name__}`')

            # TODO: validate types
            setattr(self, k, v)
        if self.DB.pk.name not in initial:
            self.pk = None
        for field_name in self.DB.subtables:
            setattr(self, field_name, list())
        self._modified_fields_ = set(initial.keys())

    def __setattr__(self, key, value):
        if key in self.DB.fields:
            if self._modified_fields_ is not None:
                self._modified_fields_.add(key)
        return super().__setattr__(key, value)

    @classmethod
    def check_db(cls):
        if not cls.DB.db:
            raise QuazyWrongOperation("Table is not assigned to a database")

    @classmethod
    def get(cls, item):
        cls.check_db()
        return cls.DB.db.get(cls, item)

    def save(self):
        self.check_db()
        return self.DB.db.save(self)

    def delete(self):
        self.check_db()
        self.DB.db.delete(item=self)

    @classmethod
    def select(cls) -> DBQuery[typing.Self]:
        cls.check_db()
        return cls.DB.db.query(cls)

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
        TableClass: type[DBTable] = typing.cast(type[DBTable], type(state['qualname'], (DBTable,), {
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

    def inspect(self):
        res = []
        for k, v in vars(self).items():
            if not k.startswith('_'):
                res.append(f'{k}: {str(v)} ({type(v).__name__})')
        return '\n'.join(res)

    @classmethod
    def _view(cls, item: DBQueryField):
        return None

    @classmethod
    def get_lookup_field(cls, item: DBQueryField) -> DBSQL | None:
        if cls.DB.lookup_field:
            return item[cls.DB.lookup_field]

    def __eq__(self, other):
        return self.pk == other.pk if isinstance(other, DBTable) else other

    def __ne__(self, other):
        return self.pk != other.pk if isinstance(other, DBTable) else other

    def _before_update(self, db: DBFactory):
        ...

    def _after_update(self, db: DBFactory):
        ...

    def _before_insert(self, db: DBFactory):
        ...

    def _after_insert(self, db: DBFactory):
        ...

    def _before_delete(self, db: DBFactory):
        ...

    def _after_delete(self, db: DBFactory):
        ...
