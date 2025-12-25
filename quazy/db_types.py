from __future__ import annotations

import inspect
import typing
from abc import abstractmethod
from typing import Optional
from datetime import datetime, timedelta, date, time
from decimal import Decimal
from uuid import UUID
from enum import IntEnum, Enum
try:
    from enum import StrEnum
except ImportError:
    from strenum import StrEnum  # noqa

from .db_field import DBField, UX, Unassigned

__all__ = ['Optional', 'datetime', 'timedelta', 'date', 'time', 'Decimal', 'UUID', 'Many', 'DefaultValue', 'KNOWN_TYPES',
           'db_type_name', 'db_type_by_name', 'FieldCID', 'FieldBody', 'Property', 'ManyToMany', 'IntEnum', 'StrEnum',
           'Enum', 'Text', 'Unassigned']

if typing.TYPE_CHECKING:
    from .db_table import DBTable


class DefaultValue:
    pass

KNOWN_TYPES = (
    int, str, float, bool, bytes,
    datetime, timedelta, date, time,
    Decimal,
    UUID,
    dict,
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

DBTableT = typing.TypeVar('DBTableT', bound='DBTable')
AnyT = typing.TypeVar('AnyT')

class ManyProtocol(typing.MutableSequence[DBTableT]):
    @abstractmethod
    def fetch(self) -> typing.Awaitable[typing.Self] | typing.Self: ...

Many = typing.Annotated[ManyProtocol[DBTableT], "Many"]
ManyToMany = typing.Annotated[ManyProtocol[DBTableT], "ManyToMany"]
FieldCID = typing.Annotated[AnyT, 'FieldCID']
Property = typing.Annotated[AnyT, 'Property']
ObjVar = typing.Annotated[AnyT, 'ObjVar']

class FieldBody:
    pass

class Text(DBField, str):
    type = str
    class UX(UX):
        multiline = True

def db_type_name(t: type) -> str:
    if t in KNOWN_TYPES:
        return t.__name__
    elif inspect.isclass(t):
        if issubclass(t, IntEnum):
            return 'int'
        if issubclass(t, StrEnum):
            return 'str'
    raise TypeError(f"Unsupported field type {t}")


def db_type_by_name(name: str) -> type | str:
    if name in TYPE_MAP:
        return TYPE_MAP[name]
    return name
