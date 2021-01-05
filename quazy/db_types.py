import typing
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