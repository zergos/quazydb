from __future__ import annotations

import typing
from dataclasses import dataclass, field as data_field

if typing.TYPE_CHECKING:
    from typing import *
    from .db_table import DBTable

__all__ = ['DBField', 'UX', 'Unassigned']

class Unassigned:
    pass

@dataclass
class DBField:
    """Table field description class"""

    _type: 'type[DBTable] | type' = data_field(default=None, init=False)
    @property
    def type(self):
        """field type class"""
        return self._type

    @type.setter
    def type(self, value):
        self._type = value
        if self.ux:
            self.ux.type = value

    name: str = data_field(default='', init=False)  #: field name in Python
    column: str = ''  #: field/column name in database
    pk: bool = False  #: is it a primary key?
    cid: bool = False  #: is it storage of table name for :ref:`derived tables <extendable>` ?
    ref: bool = data_field(default=False, init=False)  #: is it a foreign key (reference)?
    body: bool = False  #: is it a body field for :ref:`properties <properties>`?
    property: bool = False  #: is it a :ref:`properties <properties>` field?
    required: bool = data_field(default=True, init=False)  #: is a field not *null* ?
    indexed: bool = False  #: is it indexed for fast search ?
    unique: bool = False  #: is it unique ?
    default: Union[Any, Callable[[DBTable], Any]] = Unassigned  #: default value at Python level
    default_sql: str = None  #: default value at SQL level
    reverse_name: str = None  #: reverse name for reference fields
    # many_field: bool = data_field(default=False, init=False)
    ux: Optional[UX] = None  #: UX/UI specific attributes

    def __post_init__(self):
        if self.default is not object or self.default_sql is not None:
            self.required = False

    def prepare(self, name: str):
        self.name = name
        if not self.column:
            self.column = self.name
        if not self.ux:
            self.ux = UX(self.name, blank=not self.required, hidden=self.body)
        else:
            if not self.ux.title:
                self.ux.title = self.name
        self.ux.field = self

    def _dump_schema(self) -> dict[str, Any]:
        from .db_types import db_type_name

        res = {
            'name': self.name,
            'column': self.column,
            'type': db_type_name(self.type) if not self.ref else self.type.__qualname__,
        }

        for col in 'pk cid ref body property required indexed unique'.split():
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
        field._type = db_type_by_name(f_type)
        return field


@dataclass
class UX:
    """Base class for visual representation of field, UI specific

    This class contains the most common visual representation properties. It isn't used in Quazy directly, but it
    helps to integrate with any GUI framework.
    """
    _field: DBField = data_field(init=False)
    name: str = ''  #:
    type: 'type' = None  #:
    title: str = ''  #: user level title of a field
    width: int = None  #: integer size in GUI specific units (usually, letters amount)
    choices: Mapping[str, Any] = None  #: select value by user level title from dropdown list
    blank: bool = False  #: allow field unfilled
    readonly: bool = False  #: disable modifications of a field
    multiline: bool = False  #: enable multiline editor for a text field
    hidden: bool = False  #: hide field from UI
    sortable: bool = True  #: allows sorting by field values in tables
    resizable: bool = True  #: allows resizing column of field in tables
    meta: dict[str, Any] = data_field(default_factory=dict)  #: additional data for UI specific

    def __post_init__(self):
        if self.name and not self.title:
            self.title = self.name

    @property
    def field(self) -> DBField:
        """reference to original field"""
        return self._field

    @field.setter
    def field(self, field: DBField):
        self._field = field
        self.name = field.name
        self.type = field.type


@dataclass
class DBManyField:
    foreign_table: type[DBTable]
    foreign_field: str | None = None


@dataclass
class DBManyToManyField(DBManyField):
    middle_table: type[DBTable] | None = None


