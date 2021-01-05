from __future__ import annotations

import typing
from decimal import Decimal
from datetime import datetime
from quazy.db import DBFactory, DBTable, Many

from typing import Optional
if typing.TYPE_CHECKING:
    from typing import *


class NamedTable(DBTable):
    _meta_ = True
    name: str


class Client(NamedTable):
    city: 'City'

    def __str__(self) -> str:
        return self.city.name


class City(NamedTable):
    pass


class Unit(NamedTable):
    weigth: float


class Item(NamedTable):
    base_unit: Unit
    description: Optional[str]
    cities: Many[City]

    class Units(DBTable):
        unit: Unit
        cnt: int


class Sale(DBTable):
    date: datetime
    number: str
    client: Client

    class Rows(DBTable):
        item: Item
        unit: Unit
        qty: float


db = DBFactory.postgres(database="quazy", user="quazy", password="quazy")
db.use_module()

db.clear()
db.create()

