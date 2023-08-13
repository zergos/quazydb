from __future__ import annotations

import sys
import typing
from decimal import Decimal
from random import randint
from datetime import datetime, timedelta
from types import SimpleNamespace

sys.path.append(r'D:\projects\bol\quazydb')

from quazy.db import DBFactory, DBTable, DBField, Many, ManyToMany
from quazy.query import DBQuery, DBQueryField

from quazy.db_types import FieldCID, FieldBody, Property

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


class Catalog(DBTable):
    _extendable_ = True
    cid: FieldCID[str]
    name: str


class ItemCatalog(Catalog):
    unit: Unit


class User(NamedTable):
    apps: ManyToMany[App]


class App(NamedTable):
    users: ManyToMany[User]


import logging

logger = logging.getLogger('psycopg')
logger.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

logger.addHandler(console_handler)


if __name__ == '__main__':
    db = DBFactory.postgres(conninfo="postgresql://quazy:quazy@localhost/quazy")
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
    potato.units.append(Item.Unit(unit=pack, cnt=10))
    potato.units.append(Item.Unit(unit=pack, cnt=20))
    #potato.cities.add(krasnodar)
    #potato.cities.add(novoross)
    db.insert(potato)

    potato.name = 'New potato'
    db.update(potato)

    day1 = datetime.now() - timedelta(days=30)
    for i in range(10):
        sell = Sale(date=day1, number=str(i), client=buyer)
        for k in range(3):
            sell.rows.append(Sale.Row(item=potato, unit=pack, qty=randint(10,100)))
        day1 += timedelta(days=1)
        db.insert(sell)

    with db.query() as q, q.get_scheme() as s:
        q.reuse()
        q.select(date=s.sales.date, date_sum=q.sum(s.sales.rows.qty * s.sales.rows.unit.weight))
        q.sort_by(2)
        q.filter(s.sales.date >= day1 - timedelta(days=5))
        q.filter(q.fields['date_sum'] > 80)
    with db.select(q) as res:
        for row in res:
            print(row)
    #print(res)

    cnt_test = db.query(Sale).fetch_count()
    print(cnt_test)

    with db.query() as q2:
        sub = q2.with_query(q)
        q2.select(total_max=q2.max(sub.date_sum))
    max_sum = q2.fetchone().total_max
    print(max_sum)

    pot = db.get(Item, name='New potato')
    print(pot)

    #print(db.query(novoross.fact_clients).select(name=lambda s: s.name).fetchone())
    print(db.query(Client).filter(fact_city=novoross).select(name=lambda s: s.name).fetchone())

    user = User(name="zergos", apps=list(App(name=f'app{i+1}') for i in range(10)))
    db.insert(user)

    print('Done')
