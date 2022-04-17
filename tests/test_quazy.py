from __future__ import annotations

import sys
import typing
from decimal import Decimal
from random import randint
from datetime import datetime, timedelta
from types import SimpleNamespace

from quazy.db import DBFactory, DBTable, DBField, Many
from quazy.query import DBQuery, DBQueryField

from typing import Optional
if typing.TYPE_CHECKING:
    from typing import *


class NamedTable(DBTable):
    _meta_ = True
    name: str

    def __str__(self):
        return f'Named item is {self.name}'


class Client(NamedTable):
    city: 'City' = DBField(reverse_name='clients')
    fact_city: 'City' = DBField(reverse_name='fact_clients')

    def __str__(self) -> str:
        return self.city.name


class City(NamedTable):
    pass


class Unit(NamedTable):
    weight: float


class Item(NamedTable):
    base_unit: Unit
    description: Optional[str]
    #cities: Many[City]

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
    db = DBFactory.postgres("postgresql://quazy:quazy@localhost/quazy")
    db.use_module()

    #import jsonpickle
    #print(jsonpickle.encode(db._tables))

    #sys.exit()

    db.clear()
    db.create()

    krasnodar = City(name='Krasnodar')
    db.insert(krasnodar)

    novoross = City(name='Novorossiysk')
    db.insert(novoross)

    qty = Unit(name='qty', weight=1)
    db.insert(qty)
    pack = Unit(name='pack', weight=10)
    db.insert(pack)

    buyer = Client(name='Andrey', city=krasnodar, fact_city=novoross)
    db.insert(buyer)

    potato = Item(name='Potato', base_unit=qty)
    potato.units.add(Item.Unit(unit=pack, cnt=10))
    potato.units.add(Item.Unit(unit=pack, cnt=20))
    #potato.cities.add(krasnodar)
    #potato.cities.add(novoross)
    db.insert(potato)

    potato.name = 'New potato'
    db.update(potato)

    day1 = datetime.now() - timedelta(days=30)
    for i in range(10):
        sell = Sale(date=day1, number=str(i), client=buyer)
        for k in range(3):
            sell.rows.add(Sale.Row(item=potato, unit=pack, qty=randint(10,100)))
        day1 += timedelta(days=1)
        db.insert(sell)

    with db.query() as q, q.get_scheme() as s:
        q.reuse()
        q.select(data=lambda s: s.sales.date, date_sum=q.sum(s.sales.rows.qty * s.sales.rows.unit.weight))
        q.sort_by(2)
        q.filter(s.sales.date >= day1 - timedelta(days=5))
        q.filter(q.fields['date_sum'] > 80)
    res = db.select(q)
    for row in res:
        print(row)
    #print(res)

    cnt_test = db.query(Sale).fetch_count()
    print(cnt_test)

    pot = db.get(Item, name='New potato')
    print(pot)

    #print(db.query(novoross.fact_clients).select(name=lambda s: s.name).fetchone())
    print(db.query(Client).filter(lambda s: s.fact_city == novoross).select(name=lambda s: s.name).fetchone())

    print('Done')
