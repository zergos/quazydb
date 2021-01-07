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
    weight: float


class Item(NamedTable):
    base_unit: Unit
    description: Optional[str]
    # cities: Many[City]

    class Unit(DBTable):
        unit: Unit
        cnt: int


class Sale(DBTable):
    date: datetime
    number: str
    client: Client

    class Row(DBTable):
        item: Item
        unit: Unit
        qty: float


if __name__ == '__main__':
    db = DBFactory.postgres(database="quazy", user="quazy", password="quazy")
    db.use_module()

    db.clear()
    db.create()

    krasnodar = City(name='Krasnodar')
    db.insert(krasnodar)

    qty = Unit(name='qty', weight=1)
    db.insert(qty)
    pack = Unit(name='pack', weight=10)
    db.insert(pack)

    buyer = Client(name='Andrey', city=krasnodar)
    db.insert(buyer)

    potato = Item(name='Potato', base_unit=qty)
    potato.units.add(Item.Unit(unit=pack, cnt=10))
    db.insert(potato)

    print('Done')
