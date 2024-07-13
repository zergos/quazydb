from __future__ import annotations

import typing
from dataclasses import dataclass, field as data_field

if typing.TYPE_CHECKING:
    from typing import *
    from .db_table import DBTable

__all__ = ['DBField', 'UX']

@dataclass
class DBField:
    name: str = data_field(default='', init=False)         # field name in Python
    column: str = ''                                       # field/column name in database
    type: Union[type[DBTable], type[Any]] = data_field(default=None, init=False)  # noqa field type class
    pk: bool = False                                       # is it primary key?
    cid: bool = False                                      # is it storage of table name for inherited tables ?
    ref: bool = data_field(default=False, init=False)      # is it foreign key (reference) ?
    body: bool = False                                     # is it body field for properties?
    prop: bool = False                                     # is it property field?
    required: bool = data_field(default=True, init=False)  # is field not null ?
    indexed: bool = False                                  # is it indexed for fast search ?
    unique: bool = False                                   # is it unique ?
    default: Union[Any, Callable[[], Any]] = None          # default value at Python level
    default_sql: str = None                                # default value at SQL level
    reverse_name: str = None                               # reverse name for reference fields
    # many_field: bool = data_field(default=False, init=False)
    ux: Optional[UX] = None                                # UX/UI options

    def __post_init__(self):
        if self.default is not None or self.default_sql is not None:
            self.required = False

    def prepare(self, name: str):
        self.name = name
        if not self.column:
            self.column = self.name
        if not self.ux:
            self.ux = UX(self.name)
        else:
            if not self.ux.title:
                self.ux.title = self.name
        self.ux.field = self

    def _dump_schema(self) -> dict[str, Any]:
        from .db_types import db_type_name

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
        from .db_types import db_type_by_name

        name = state.pop('name')
        f_type = state.pop('type')
        ref = state.pop('ref', False)
        required = state.pop('required', False)
        field = DBField(**state)
        field.prepare(name)
        field.ref = ref
        field.required = required
        field._pre_type = db_type_by_name(f_type)
        return field


@dataclass
class UX:
    field: DBField = data_field(init=False)
    _name: str = ''
    _type: type = None
    title: str = ''
    width: int = None
    choices: Mapping = None
    blank: bool = False
    readonly: bool = False
    multiline: bool = False
    hidden: bool = False
    sortable: bool = True
    resizable: bool = True

    def __post_init__(self):
        if self.name and not self.title:
            self.title = self.name

    @property
    def name(self) -> str:
        return self.field.name if hasattr(self, "field") else self._name

    @property
    def type(self) -> type:
        return self.field.type if hasattr(self, "field") else self._type

@dataclass
class DBManyField:
    source_table: type[DBTable]
    source_field: str | None = None


@dataclass
class DBManyToManyField(DBManyField):
    middle_table: type[DBTable] | None = None


