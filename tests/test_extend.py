import typing

from quazy import *
from quazy.stub import gen_stub

class Catalog(DBTable):
    _schema_ = "system"
    _extendable_ = True
    cid: FieldCID[str]
    body: FieldBody
    name: str

    class Row(DBTable):
        _extendable_ = True
        body: FieldBody

class Entity(Catalog):
    _schema_ = "entities"
    location: str

    class Row(Catalog.Row):
        contact_person: Property[str]
        phone_number: Property[str]

class Customer(Entity):
    _schema_ = "customers"
    shipment_days: int
    details: Property[str]

    class Row(Catalog.Row):
        item: Property[str]

        def __repr__(self):
            return self.item

if __name__ == "__main__":
    db = DBFactory.postgres("postgresql://quazy:quazy@localhost/quazy")
    #db = DBFactory.sqlite("file:quazy.db?mode=rwc")
    db.bind_module()

    db.clear()

    db._debug_mode = True
    db.create()

    with open("test_extend.pyi", "wt") as f:
        f.write(gen_stub(db))

    c = Customer(name="John", location="NY", shipment_days=7, details="toys")
    c.rows.append(Customer.Row(item="School Bus"))
    c.save()

    print(Customer.Row.select("item").fetch_all())
    print(Customer.get(name="John").rows.fetch())