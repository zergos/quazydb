import inspect
import typing
from typing import Optional
from datetime import datetime, timedelta, date, time
from decimal import Decimal
from uuid import UUID
from enum import IntEnum, Enum
try:
    from enum import StrEnum
except ImportError:
    from strenum import StrEnum  # noqa

from .db_field import DBField, UX

__all__ = ['Optional', 'datetime', 'timedelta', 'date', 'time', 'Decimal', 'UUID', 'Many', 'DefaultValue', 'KNOWN_TYPES',
           'db_type_name', 'db_type_by_name', 'FieldCID', 'FieldBody', 'Property', 'ManyToMany', 'IntEnum', 'StrEnum',
           'Enum', 'Text']


class DefaultValue:
    pass


KNOWN_TYPES = (
    int, str, float, bool, bytes,
    datetime, timedelta, date, time,
    Decimal,
    UUID,
    dict
)

TYPE_MAP = {
    'int': int,
    'str': str,
    'float': float,
    'bool': bool,
    'bytes': bytes,
    'datetime': datetime,
    'timedelta': timedelta,
    'date': date,
    'time': time,
    'Decimal': Decimal,
    'UUID': UUID,
    'dict': dict,
    'IntEnum': int,
    'StrEnum': str,
}


T = typing.TypeVar('T')


class Many(typing.Generic[T]):
    def append(self, item: T): ...

    def remove(self, item: T): ...

    def __getitem__(self, item) -> T: ...

    def __setitem__(self, key, value: T): ...

    def __iter__(self) -> T: ...


class ManyToMany(Many, typing.Generic[T]):
    pass


class FieldCID(typing.Generic[T]):
    pass


class FieldBody:
    pass


class Property(typing.Generic[T]):
    pass

class Text(DBField, str):
    type = str
    class UX(UX):
        multiline = True

def db_type_name(t: type) -> str:
    if t in KNOWN_TYPES:
        return t.__name__
    elif inspect.isclass(t) and issubclass(t, Enum):
        return 'Enum '+t.__name__
    else:
        raise TypeError(f"Unsupported field type {t}")


def db_type_by_name(name: str) -> type | str:
    if name in TYPE_MAP:
        return TYPE_MAP[name]
    elif name.startswith('Enum'):
        return TYPE_MAP[name.split()[1]]
    return name
