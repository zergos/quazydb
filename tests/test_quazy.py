from __future__ import annotations

import typing
from decimal import Decimal
from random import randint
from datetime import datetime, timedelta
from quazy.db import DBFactory, DBTable, Many
from quazy.query import DBQuery

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
    potato.units.add(Item.Unit(unit=pack, cnt=20))
    db.insert(potato)

    potato.name = 'New potato'
    db.update(potato)

    day1 = datetime.now() - timedelta(days=30)
    for i in range(10):
        sell = Sale(date=day1, number=i, client=buyer)
        for k in range(3):
            sell.rows.append(Sale.Row(item=potato, unit=pack, qty=randint(10,100)))
        day1 += timedelta(days=1)

    with DBQuery(db) as (q, s):
        q.select(s.sales.date)
        q.select(q.sum(s.sales.rows.qty))
        q.sort_by(-1)
    q.print_all()

    print('Done')
