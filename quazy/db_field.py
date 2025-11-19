from __future__ import annotations

import inspect
import typing
from dataclasses import dataclass, field as data_field

if typing.TYPE_CHECKING:
    from typing import *
    from .db_table import DBTable

__all__ = ['DBField', 'UX']

@dataclass
class DBField:
    """Table field description class

    Attributes:
        name:         field name in Python
        column:       field/column name in database
        type:         field type class
        pk:           is it a primary key?
        cid:          is it storage of table name for inherited tables ?
        ref:          is it a foreign key (reference)?
        body:         is it a body field for properties?
        property:     is it a property field?
        required:     is a field not null ?
        indexed:      is it indexed for fast search ?
        unique:       is it unique ?
        default:      default value at Python level
        default_sql:  default value at SQL level
        reverse_name: reverse name for reference fields
        ux:           UX/UI specific attributes
    """
    name: str = data_field(default='', init=False)
    column: str = ''
    type: 'type[DBTable] | type' = data_field(default=None, init=False)
    pk: bool = False
    cid: bool = False
    ref: bool = data_field(default=False, init=False)
    body: bool = False
    property: bool = False
    required: bool = data_field(default=True, init=False)
    indexed: bool = False
    unique: bool = False
    default: Union[Any, Callable[[DBTable], Any]] = object
    default_sql: str = None
    reverse_name: str = None
    # many_field: bool = data_field(default=False, init=False)
    ux: Optional[UX] = None

    def __post_init__(self):
        if self.default is not object or self.default_sql is not None:
            self.required = False



    def prepare(self, name: str):
        self.name = name
        if not self.column:
            self.column = self.name
        if not self.ux:
            self.ux = UX(self.name, blank=not self.required)
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
        field._pre_type = db_type_by_name(f_type)
        return field


@dataclass
class UX:
    """Base class for visual representation of field, UI specific

    This class contains the most common visual representation properties. It isn't used in Quazy directly, but it
    helps to integrate with any GUI framework.

    Attributes:
        field:     reference to original field
        title:     user level title of a field
        width:     integer size in GUI specific units (usually, letters amount)
        choices:   select value by user level title from dropdown list
        blank:     allow field unfilled
        readonly:  disable modifications of a field
        multiline: enable multiline editor for a text field
        hidden:    hide field from UI
        sortable:  allows sorting by field values in tables
        resizable: allows resizing column of field in tables
        meta:      additional data for UI specific
    """
    field: DBField = data_field(init=False)
    _name: str = ''
    _type: 'type' = None
    title: str = ''
    width: int = None
    choices: Mapping[str, Any] = None
    blank: bool = False
    readonly: bool = False
    multiline: bool = False
    hidden: bool = False
    sortable: bool = True
    resizable: bool = True
    meta: dict[str, Any] = data_field(default_factory=dict)

    def __post_init__(self):
        if self.name and not self.title:
            self.title = self.name

    @property
    def name(self) -> str:
        """original field name"""
        return self.field.name if hasattr(self, "field") else self._name

    @property
    def type(self) -> 'type':
        """original field type"""
        return self.field.type if hasattr(self, "field") else self._type

@dataclass
class DBManyField:
    foreign_table: type[DBTable]
    foreign_field: str | None = None


@dataclass
class DBManyToManyField(DBManyField):
    middle_table: type[DBTable] | None = None


