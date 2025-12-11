from __future__ import annotations

import typing
import types
import re
import sys
import inspect
import itertools

if sys.version_info >= (3, 14):
    import annotationlib

try:
    from pydantic import TypeAdapter, ConfigDict
    VALIDATE_DATA = True
    PYDANTIC_CONFIG = ConfigDict(arbitrary_types_allowed=True, strict=False)
except ImportError:
    VALIDATE_DATA = False
    pass

from .db_field import DBField, UX, DBManyField, DBManyToManyField
from .db_types import *
from .db_types import T
from .exceptions import *

if typing.TYPE_CHECKING:
    from typing import *
    from .db_factory import DBFactory
    from .db_query import DBQuery, DBQueryField, DBSQL, FDBSQL

__all__ = ['DBTable']

def camel2snake(name: str) -> str:
    return camel2snake.r.sub(r'_\1', name).lower()

camel2snake.r = re.compile(
    '((?<=[a-z0-9])[A-Z]|(?!^)(?<!_)[A-Z](?=[a-z]))')  # tnx to https://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-snake-case

def get_annotated_id(t: str) -> str | None:
    groups = get_annotated_id.r.match(t)
    return groups and groups.group(2)

get_annotated_id.r = re.compile(r'''^(.*?)(Many|ManyToMany|Property|FieldCID|ClassVar|ObjVar)\[(.+?)]$''')

class MetaTable(type):
    db_base_class: type[DBTable.DB]

    def __new__(cls, clsname: str, bases: tuple[type[DBTable], ...], attrs: dict[str, Any], **kwargs: Any) -> type[DBTable]:
        if clsname == 'DBTable':
            cls.db_base_class = attrs['DB']
            return super().__new__(cls, clsname, bases, attrs)

        if 'DB' in attrs:
            raise QuazyError(f'Should not define `DB` subclass directly in `{clsname}`, use `_name_` attributes')

        spec_attrs = {}
        for name in ('db table title schema just_for_typing extendable discriminator'
                     ' meta lookup_field validate metadata use_slots').split():
            src_name = f'_{name}_'
            if (value := attrs.pop(src_name, None)) is not None:
                spec_attrs[name] = value

        if 'title' not in spec_attrs:
            spec_attrs['title'] = clsname

        if 'validate' in spec_attrs and spec_attrs['validate'] is True and not VALIDATE_DATA:
            raise QuazyError('`validate` attribute is set to `True`, but `pydantic` is not installed')

        DB = typing.cast(type[DBTable.DB], type(clsname + 'DB', (cls.db_base_class,), spec_attrs))
        DB.many_fields = dict()
        DB.many_to_many_fields = dict()
        DB.defaults = dict()
        attrs['DB'] = DB


        if DB.use_slots:
            attrs['__slots__'] = []

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
                raise QuazyFieldNameError(f'Subclass name {qualname} repeats (explicit or implicit) field name')
        else:
            DB.snake_name = camel2snake(qualname) + 's'

        DB.subtables = dict()

        if 'discriminator' not in spec_attrs:
            DB.discriminator = attrs['__qualname__'] if DB.cid else None

        new_cls = super().__new__(cls, attrs['__qualname__'], bases, attrs, **kwargs)
        """
        if sys.version_info >= (3, 14):
            from annotationlib import get_annotate_from_class_namespace, call_annotate_function, Format
            annotate_func = get_annotate_from_class_namespace(attrs)
            def wrapped_annotate(format):
                import annotationlib
                annos = call_annotate_function(annotate_func, format, owner=new_cls)
                return {key: value for key, value in annos.items() if key in DB.fields}
            if annotate_func:
                new_cls.__annotate__ = wrapped_annotate
        """
        return new_cls

    @staticmethod
    def collect_fields(bases: tuple[type[DBTable], ...], DB: type[DBTable.DB], attrs: dict[str, Any]):

        if DB.extendable:
            DB.is_root = True

        fields = MetaTable.collect_bases_fields(bases, DB)

        has_pk = False
        if sys.version_info < (3, 14):
            annotations = attrs.get('__annotations__', {})
        else:
            annotate_func = annotationlib.get_annotate_from_class_namespace(attrs)
            annotations = annotationlib.call_annotate_function(annotate_func, annotationlib.Format.FORWARDREF) if annotate_func else {}

        for name, t in annotations.items():  # type: str, type
            if name.startswith('_'):
                continue
            if (isinstance(t, typing._GenericAlias) and t.__name__ == "ClassVar"
                or isinstance(t, str) and get_annotated_id(t) == "ClassVar"):
                continue
            if DB.use_slots:
                attrs['__slots__'].append(name)
            if (isinstance(t, typing._AnnotatedAlias) and t.__metadata__[0] == 'ObjVar'
                or isinstance(t, str) and get_annotated_id(t) == 'ObjVar'):
                if DB.use_slots:
                    del attrs[name]
                    attrs['__slots__'].append(name)
                continue
            if not DB.use_slots:
                field = attrs.get(name, DBField())
            else:
                field = attrs.pop(name, DBField())
            if isinstance(field, DBField):
                field.prepare(name)
                if not DB.use_slots:
                    attrs[name] = field.default
                if field.default is not Unassigned:
                    DB.defaults[name] = field.default
                if field.pk:
                    has_pk = True
                    DB.pk = field
                elif (isinstance(t, typing._AnnotatedAlias) and t.__metadata__[0] == 'FieldCID'
                      or isinstance(t, str) and get_annotated_id(t) == 'FieldCID'):
                    # check CID
                    if not DB.extendable:
                        raise QuazyFieldTypeError(
                            f'Table `{attrs["__qualname__"]}` is not declared with `_extendable_` attribute')
                    elif DB.cid:
                        raise QuazyFieldTypeError(
                            f'Table `{attrs["__qualname__"]}` has CID field already inherited from extendable')

                    field.cid = True
                    DB.cid = field
                elif t is FieldBody or t == 'FieldBody' or field.body:
                    if DB.body:
                        raise QuazyFieldTypeError(f'Table `{attrs["__qualname__"]}` has body field already')

                    field.body = True
                    DB.body = field
            else:
                field = DBField(default=field)
                field.prepare(name)
                field.required = True
                DB.defaults[name] = field.default

            fields[name] = field

        # check seed proper declaration
        if DB.cid and not DB.extendable:
            raise QuazyFieldTypeError(
                f'CID field is declared, but table `{attrs["__qualname__"]}` is not declared with `_extendable_` attribute')

        if not has_pk:
            pk = DBField(pk=True)
            pk.type = int
            pk.prepare('id')
            pk.ux.blank = True
            fields['id'] = pk
            DB.pk = pk
            if DB.use_slots:
                attrs['__slots__'].append('id')

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
                        raise QuazyNotSupported('Multiple inheritance of extendable tables is not supported')
                    DB.extendable = True
                    DB.is_root = False
                    DB.cid = base.DB.cid
                    DB.table = base.DB.table
                    DB.body = base.DB.body

        return fields

    def resolve_types(cls, globalns):
        """Resolve fields types from annotations

        :meta private:"""
        from .db_factory import DBFactory

        # eval annotations
        if sys.version_info >= (3, 14):
            annotations = annotationlib.get_annotations(cls, format=annotationlib.Format.FORWARDREF)
        else:
            annotations = cls.__annotations__

        type_hints = typing.get_type_hints(cls, globals() | globalns, {'DBFactory': DBFactory})
        for name, t in type_hints.items():
            if name not in cls.DB.fields:  # or cls.fields[name].type is not None:
                continue
            field: DBField = cls.DB.fields[name]
            annotation = annotations.get(name, None)
            if cls.resolve_type(t, annotation, field, globalns):
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
                t.DB.db = cls.DB.db
                t.resolve_types(globalns)

    def resolve_type(cls, t: type, ta: type | None, field: DBField, globalns) -> bool | None:
        """Resolve field types from annotations

        :meta private:"""
        if inspect.isclass(t) and issubclass(t, DBField):
            # import custom field attributes
            for k, v in t.__dict__.items():
                if k.startswith('_'):
                    continue
                if inspect.isclass(v) and issubclass(v, UX):
                    for kk, vv in v.__dict__.items():
                        if not kk.startswith('_'):
                            setattr(field.ux, kk, vv)
                else:
                    setattr(field, k, v)

        elif (ta is not None and
              (isinstance(ta, typing._AnnotatedAlias) and (meta_name:=ta.__metadata__[0]) or
               isinstance(ta, str) and (meta_name:=get_annotated_id(ta)) is not None)):
            if meta_name == 'FieldCID':
                field.type = t
            elif meta_name == 'Property':
                field.property = True
                field.required = False
                cls.resolve_type(t, None, field, globalns)
            elif meta_name in ('Many', 'ManyToMany'):
                field_type = t.__args__[0]
                if not inspect.isclass(field_type) or not issubclass(field_type, DBTable):
                    raise QuazyFieldTypeError(f'Many type `{t}` should be referenced to another DBTable')
                if meta_name == 'Many':
                    cls.DB.many_fields[field.name] = DBManyField(field_type, field.reverse_name)
                else:
                    cls.DB.many_to_many_fields[field.name] = DBManyToManyField(field_type, field.reverse_name)
                return True

        elif isinstance(t, typing._UnionGenericAlias) or isinstance(t, types.UnionType):
            if len(args:=typing.get_args(t)) == 2 and args[1] is type(None):
                # 'Optional' annotation
                field.required = False
                DBTable.resolve_type(args[0], None, field, globalns)

        elif t is FieldBody:
            field.type = dict

        elif inspect.isclass(t) and issubclass(t, DBTable):
            # Foreign key
            field.ref = True
            field.type = t
            if cls.DB.use_slots and field.name not in cls.DB.defaults:
                cls.DB.defaults[field.name] = None

        elif t in KNOWN_TYPES or inspect.isclass(t) and issubclass(t, Enum):
            # Base type
            field.type = t

        else:
            raise QuazyFieldTypeError(f'type `{t}` is not supported as field type for `{cls.__qualname__}.{field.name}`')

    def resolve_types_many(cls, add_middle_table: Callable[[type[DBTable]], Any]):
        """Resolve referred types from annotations

        :meta private:"""
        # eval refs
        for name, field in cls.DB.fields.items():
            if field.ref:
                rev_name = field.reverse_name or cls.DB.snake_name
                if rev_name in field.type.DB.many_fields:
                    if field.type.DB.many_fields[rev_name].foreign_table is not cls:
                        raise QuazyFieldNameError(
                            f'Cannot reuse Many field in table `{field.type.__name__}` with name `{rev_name}`, it is associated with table `{field.type.DB.many_fields[rev_name].source_table.__name__}`. Set different `reverse_name`.')
                    field.type.DB.many_fields[rev_name].foreign_field = name
                else:
                    field.type.DB.many_fields[rev_name] = DBManyField(cls, name)

        # check Many fields connected
        for name, field in cls.DB.many_fields.items():
            if not field.foreign_field or field.foreign_field not in field.foreign_table.DB.fields:
                raise QuazyFieldTypeError(
                    f'Cannot find reference from table `{field.foreign_table.__name__}` to table `{cls.__name__}` to connect with Many field `{name}`. Add field to source table or change field type to `ManyToMany`')

        # check and connect ManyToMany fields
        for name, field in cls.DB.many_to_many_fields.items():
            if field.middle_table:
                continue

            middle_table_name = "{}{}".format(cls.__qualname__, name.capitalize())
            middle_table_inner_name = "{}_{}".format(cls.DB.table, name)
            rev_name = field.foreign_field or cls.DB.snake_name
            if rev_name in field.foreign_table.DB.many_to_many_fields and field.foreign_table.DB.many_to_many_fields[
                rev_name].foreign_table is not cls:
                raise QuazyFieldNameError(
                    f'Cannot reuse ManyToMany field in table `{field.foreign_table.__name__}` with name `{rev_name}`, it is associated with table `{field.foreign_table.DB.many_to_many_fields[rev_name].source_table.__name__}`. Set different `reverse_name`.')

            f1 = DBField(field.foreign_table.DB.table, indexed=True)
            f1.prepare(f1.column)
            f1.type = field.foreign_table
            f1.ref = True
            f2 = DBField(cls.DB.table, indexed=True)
            f2.prepare(f2.column)
            f2.type = cls
            f2.ref = True

            TableClass: type[DBTable] = typing.cast(type[DBTable],
                                                    type(middle_table_name, (DBTable,), {
                                                        '__qualname__': middle_table_name,
                                                        '__module__': cls.__module__,
                                                        '__annotate_func__': lambda f: {
                                                            f1.name: f1.type,
                                                            f2.name: f2.type
                                                        },
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
            field.foreign_field = rev_name
            field.foreign_table.DB.many_to_many_fields[rev_name].middle_table = TableClass
            field.foreign_table.DB.many_to_many_fields[rev_name].foreign_field = name


class DBTable(metaclass=MetaTable):
    """Table model constructor

    All class attributes are considered as database table fields.
    Types could be set with annotations or/and directly as `DBField` instance.
    There are several special class attributes used to set DBTable details and behaviour.

    Attributes:
        _table_:           database table internal name
        _title_:           user-friendly table name
        _schema_:          explicit schema name
        _just_for_typing_: internal flag used for migrations
        _extendable_:      set `extendable` flag for a table
        _discriminator_:   SQL safe CID value to specify table in extended mode
        _meta_:            mark the table as pure abstract, just for inheritance with common field set
        _lookup_field_:    specify field name for text search. For integrations only.
        _use_slots_:       use slots for database fields (enabled by default)
        _validate_:        validate fields types using `pydantic` (default: True if `pydantic` is installed)
        _metadata_:        any custom metadata

    Note:
        special field name `pk` is reserved as property for direct primary key access
    """
    # initial attributes
    _table_: typing.ClassVar[str]
    _title_: typing.ClassVar[str]
    _schema_: typing.ClassVar[str]
    _just_for_typing_: typing.ClassVar[bool]
    _extendable_: typing.ClassVar[bool]
    _discriminator_: typing.ClassVar[typing.Any]
    _meta_: typing.ClassVar[bool]
    _lookup_field_: typing.ClassVar[str]
    _use_slots_: typing.ClassVar[bool]
    _validate_: typing.ClassVar[bool]
    _metadata_: typing.ClassVar[dict[str, typing.Any]]

    # state attributes
    __slots__ = ('_db_', '_modified_fields_')
    _db_: DBFactory | None
    _modified_fields_: set[str]

    class DB:
        """DBTable meta-subclass with internal information

        It has only class-based attributes, intended for read-only. Instances aren't supported.

        Attributes:
            db:                     `DBFactory` linked to a table, if already specified
            table:                  database table internal name
            title:                  user-friendly table name
            schema:                 explicit schema name
            just_for_typing:        internal flag used for migrations
            snake_name:             "snake" style table name in plural
            extendable:             support for extendable classes
            cid:                    CID field reference (if declared)
            is_root:                is table a root of extendable tables chain
            discriminator:          inherited table inner code
            owner:                  table owner of subtable
            subtables:              subtables list
            meta:                   table marked as meta table (pure abstract)
            pk:                     reference to primary field
            body:                   reference to body field of None
            many_fields:            dict of field sets, when this table is referred from another table
            many_to_many_fields:    dict of field sets, when two tables referred to each other
            fields:                 all fields dict
            defaults:               default values for fields
            lookup_field:           field name for text search for integrations
            use_slots:              use slots for database fields
            validate:               validate fields types using `pydantic`
            validators:             dict of validators for fields
            metadata:               any custom metadata
        """
        db: typing.ClassVar[DBFactory]
        table: typing.ClassVar[str | None] = None
        title: typing.ClassVar[str | None] = None
        schema: typing.ClassVar[str | None] = None
        just_for_typing: typing.ClassVar[bool] = False
        snake_name: typing.ClassVar[str]
        extendable: typing.ClassVar[bool] = False
        cid: typing.ClassVar[DBField | None] = None
        is_root: typing.ClassVar[bool] = False
        discriminator: typing.ClassVar[typing.Any | None] = None
        owner: typing.ClassVar[typing.Union[str, type[DBTable]] | None] = None
        subtables: typing.ClassVar[dict[str, type[DBTable]] | None] = None
        meta: typing.ClassVar[bool] = False
        pk: typing.ClassVar[DBField]
        body: typing.ClassVar[DBField | None] = None
        many_fields: typing.ClassVar[dict[str, DBManyField] | None] = None
        many_to_many_fields: typing.ClassVar[dict[str, DBManyToManyField] | None] = None
        fields: typing.ClassVar[dict[str, DBField]]
        defaults: typing.ClassVar[dict[str, typing.Any]]
        lookup_field: typing.ClassVar[str | None] = None
        use_slots: typing.ClassVar[bool] = False
        validate: typing.ClassVar[bool] = VALIDATE_DATA
        validators: typing.ClassVar[dict[str, TypeAdapter[Any]]]
        metadata: typing.ClassVar[dict[str, typing.Any]]

    class ItemGetter(typing.Generic[T]):
        def __init__(self, db: DBFactory, table: type[T], field_name: str, pk_value: Any, view: str = None):
            self._db = db
            self._table = table
            self._attr_name = field_name
            self._pk_value = pk_value
            self._view = view
            self._cache = dict()

        def __str__(self):
            return self._view or f'{self._table.__qualname__}[{self._pk_value}]'

        def __getattr__(self, item):
            if item.startswith('_'):
                return super().__getattribute__(self, item)

            if item == 'pk' or item == self._table.DB.pk.name:
                return self._pk_value

            if item in self._cache:
                return self._cache[item]

            value = self._db.query(self._table).select(item).filter(pk=self._pk_value).fetch_value()
            self._cache[item] = value
            return value

        def fetch(self, *fields) -> T:
            actual_fields = fields + ('pk',) if fields else ()
            value = self._db.query(self._table).select(*actual_fields).filter(pk=self._pk_value).fetch_one()
            self._cache |= value.__dict__
            return value

    class ListGetter(list, typing.Generic[T]):
        def __init__(self, db: DBFactory, table: type[DBTable], field_name: str, pk_value: Any):
            self._db = db
            self._table = table
            self._field_name = field_name
            self._pk_value = pk_value
            super().__init__()

        def fetch(self, *fields) -> list[T]:
            actual_fields = fields + ('pk',) if fields else ()
            self[:] = self._db.query(self._table).select(*actual_fields).filter(lambda x: getattr(x, self._field_name) == self._pk_value).fetch_all()
            return self

    @classmethod
    def setup_validators(cls):
        if cls.DB.validate:
            validators = {}
            for name, field in cls.DB.fields.items():
                if field.pk:
                    adapter = TypeAdapter(field.type | cls, config=PYDANTIC_CONFIG)
                elif field.ref:
                    adapter = TypeAdapter(field.type | field.type.DB.pk.type, config=PYDANTIC_CONFIG)
                else:
                    adapter = TypeAdapter(field.type, config=PYDANTIC_CONFIG)
                validators[name] = adapter
            cls.DB.validators = validators

            for table in cls.DB.subtables.values():
                table.setup_validators()

    def __pre_init__(self, **initial):
        self._modified_fields_: set[str] | None = None
        self._db_: DBFactory = initial.pop('_db_', self.DB.db)
        self_id = initial.get(self.DB.pk.name, None)
        object.__setattr__(self, self.DB.pk.name, None)

        if not self.DB.use_slots:
            for field_name, field in self.DB.many_fields.items():
                if self_id is not None:
                    object.__setattr__(self, field_name, DBTable.ListGetter(self._db_, field.foreign_table, field.foreign_field, self_id))
                else:
                    object.__setattr__(self, field_name, list())

            for field_name, subtable in self.DB.subtables.items():
                if self_id is not None:
                    object.__setattr__(self, field_name, DBTable.ListGetter(self._db_, subtable, self.DB.table, self_id))
                else:
                    object.__setattr__(self, field_name, list())

            for field_name, field in self.DB.many_to_many_fields.items():
                if self_id is not None:
                    object.__setattr__(self, field_name, DBTable.ListGetter(self._db_, field.foreign_table, field.foreign_field, self_id))
                else:
                    object.__setattr__(self, field_name, list())

    def __init__(self, **initial):
        """DBTable instance constructor

        Args:
            **initial: fields initial values
        """
        self.__pre_init__(**initial)
        for k, v in initial.items():
            if field := self.DB.fields.get(k):
                if issubclass(field.type, Enum):
                    setattr(self, k, field.type(v) if v is not None else None)
                    continue
            # else:
            if k not in self.DB.fields and k not in self.DB.many_fields and k not in self.DB.many_to_many_fields:
                raise QuazyFieldNameError(f'Wrong field name `{k}` in new instance of `{self.__class__.__name__}`')

            setattr(self, k, v)
        if self.DB.use_slots:
            for k, v in self.DB.defaults.items():
                if k not in initial:
                    if not callable(v):
                        object.__setattr__(self, k, v)
                    else:
                        object.__setattr__(self, k, v(self))
        self._modified_fields_ = set(initial.keys())

    @classmethod
    def raw(cls, **initial) -> Self:
        self = cls.__new__(cls)
        self.__pre_init__(**initial)
        for k, v in initial.copy().items():
            if k.endswith("__view"):
                continue
            if field := self.DB.fields.get(k):
                if issubclass(field.type, Enum):
                    object.__setattr__(self, k, field.type(v) if v is not None else None)
                    continue
                elif field.ref:
                    view = initial.pop(f'{k}__view', None)
                    object.__setattr__(self, k, DBTable.ItemGetter(self._db_, field.type, field.name, v.pk if isinstance(v, DBTable) else v, view))
                    continue
                elif field.property and not cls.DB.db.translator.supports_cast_converter:
                    v = cls.DB.db.translator.cast_value(field, v)
            object.__setattr__(self, k, v)
        self._modified_fields_ = set()
        return self

    def __setattr__(self, key, value):
        if key in self.DB.fields:
            if self.DB.validate:
                from pydantic import ValidationError
                try:
                    value = self.DB.validators[key].validate_python(value)
                except ValidationError as e:
                    raise QuazyFieldTypeError(f'Field `{key}` in `{self.__class__.__name__}` has wrong type: {e}')
            if getattr(self, '_modified_fields_', None) is not None:
                self._modified_fields_.add(key)
        return super().__setattr__(key, value)

    def __class_getitem__(cls, item: Any) -> Self:
        if not cls.DB.db:
            raise QuazyWrongOperation(f"Table `{cls.__qualname__}` is not assigned to a database")
        return cls.DB.db.get(cls, item)

    @classmethod
    def check_db(cls):
        """Check whether DBTable is assigned to DBFactory

        Raises:
            QuazyWrongOperation: table is not assigned

        :meta private:"""
        if not cls.DB.db:
            raise QuazyWrongOperation(f"Table `{cls.__qualname__}` is not assigned to a database")

    @classmethod
    def get(cls, pk: Any = None, **fields) -> Self:
        """Get DBTable instance by primary key value

        Args:
            pk: primary key value to get an item (optional)
            **fields: fields values to find item if no pk is specified (optional)
        """
        cls.check_db()
        return cls.DB.db.get(cls, pk, **fields)

    def save(self, **kwargs) -> Self:
        """Save DBTable instance changes to a database

        Args:
            kwargs: additional values to update item fields before saving it to the database
        """
        self.check_db()
        return self.DB.db.save(self, **kwargs)

    def load(self, selected_field_name: str | None = None) -> Self:
        """Load related items from foreign tables

        Args:
            selected_field_name: any related field name to load, if not specified, all related fields will be loaded.
        """
        self.check_db()
        if selected_field_name is not None:
            getattr(self, selected_field_name).fetch()
        else:
            for field_name in itertools.chain(self.DB.subtables, self.DB.many_fields, self.DB.many_to_many_fields):
                getattr(self, field_name).fetch()

        return self

    def fetch(self, *fields) -> Self:
        return self

    def delete(self):
        """Delete DBTable instance from a database"""
        self.check_db()
        self.DB.db.delete(item=self)

    @classmethod
    def query(cls, name: str | None = None) -> DBQuery[Self]:
        """Create a DBQuery instance for queries associated with this table

        Args:
            name: name of the query for subquery request

        Hint:
            Use identical method name `select` for your preference.
        """
        cls.check_db()
        return cls.DB.db.query(cls, name)

    @classmethod
    def select(cls, *field_names: str, **fields: FDBSQL) -> DBQuery[Self]:
        """Create a DBQuery instance and specify selected fields

        Read `DBQuery.select()` for details.
        """
        return cls.query().select(*field_names, **fields)

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
        """get a primary key value"""
        return getattr(self, self.DB.pk.name)

    @pk.setter
    def pk(self, value):
        """set a primary key value"""
        setattr(self, self.DB.pk.name, value)

    def inspect(self) -> str:
        """Inspect table in simple text format

        key: value (type)
        """
        res = []
        for k, v in vars(self).items():
            if not k.startswith('_'):
                res.append(f'{k}: {str(v)} ({type(v).__name__})')
        return '\n'.join(res)

    @classmethod
    def _view_(cls, item: DBQueryField[typing.Self]):
        """virtual method to override DBTable item presentation

        Originally, each table item is requester as a primary key value (integer number for ex.). It is more
        convenient to see user-friendly presentation, like `name`, `caption` or several fields combined.

        Example:
            .. code-block:: python

                class User(DBTable):
                    name: str

                    @classmethod
                    def _view_(cls, item: DBQueryField):
                        return item.name

        :meta public:
        """
        return None

    @classmethod
    def get_lookup_field(cls, item: DBQueryField) -> DBSQL | None:
        """return lookup field"""
        if cls.DB.lookup_field:
            return item[cls.DB.lookup_field]

    def __eq__(self, other):
        return self.pk == other.pk if isinstance(other, DBTable) else other

    def __ne__(self, other):
        return self.pk != other.pk if isinstance(other, DBTable) else other

    def __str__(self):
        return f'{self.DB.title}[{self.pk}]'

    __repr__ = __str__

    def _before_update(self, db: DBFactory):
        """abstract event before update to the database"""

    def _after_update(self, db: DBFactory):
        """abstract event after update to the database"""

    def _before_insert(self, db: DBFactory):
        """abstract event before insert to the database"""

    def _after_insert(self, db: DBFactory):
        """abstract event after insert to the database"""

    def _before_delete(self, db: DBFactory):
        """abstract event before delete from the database"""

    def _after_delete(self, db: DBFactory):
        """abstract event after delete from the database"""
