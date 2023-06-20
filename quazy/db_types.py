import typing
from typing import Optional
from datetime import datetime, timedelta, date, time
from decimal import Decimal
from uuid import UUID

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


def db_type_name(t: typing.Type[typing.Any]) -> str:
    if t in KNOWN_TYPES:
        return t.__name__
    else:
        raise TypeError("Unsupported field type")


def db_type_by_name(name: str) -> typing.Type[typing.Any] | str:
    if name not in globals():
        return name
    t = globals()[name]
    if t in KNOWN_TYPES:
        return t
    else:
        return name
