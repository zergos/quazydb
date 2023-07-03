import typing
from typing import Optional
from datetime import datetime, timedelta, date, time
from decimal import Decimal
from uuid import UUID

__all__ = ['Optional', 'datetime', 'timedelta', 'date', 'time', 'Decimal', 'UUID', 'Many', 'DefaultValue', 'KNOWN_TYPES',
           'db_type_name', 'db_type_by_name', 'FieldCID', 'FieldBody', 'Property']

Many = typing.Set


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
}


T = typing.TypeVar('T')


class FieldCID(typing.Generic[T]):
    pass


class FieldBody:
    pass


class Property(typing.Generic[T]):
    pass


def db_type_name(t: typing.Type[typing.Any]) -> str:
    if t in KNOWN_TYPES:
        return t.__name__
    else:
        raise TypeError("Unsupported field type {t}")


def db_type_by_name(name: str) -> typing.Type[typing.Any] | str:
    if name not in TYPE_MAP:
        return name
    return TYPE_MAP[name]
