#from __future__ import annotations

import random
import typing

from quazy import DBFactory, DBTable, DBField, Many, ManyToMany, Text, DBQueryField, DBQuery, FieldBody, Property
from quazy.exceptions import QuazyFieldTypeError
from quazy.stub import gen_stub
from datetime import datetime, timedelta


class Product(DBTable):
    local_var: typing.ClassVar[str]
    name: str
    price: float
    description: str | None

class Box(DBTable):
    location: str

class Fruit(DBTable):
    name: str
    box: Box


class Receipt(DBTable):
    created_at: datetime = DBField(default_sql="now()")

    class Item(DBTable):
        name: str
        price: float
        qty: float
        total: float = DBField(default=lambda x: x.price * x.qty)


class Book(DBTable):
    name: str
    sellers: ManyToMany['Seller']

    def __repr__(self):
        return self.name

class Seller(DBTable):
    name: str
    books: ManyToMany[Book]


class User(DBTable):
    name: str

class Task(DBTable):
    title: str
    sender: User = DBField(reverse_name="tasks_send")
    receiver: User = DBField(reverse_name="tasks_received")

    class History(DBTable):
        record_date: datetime = DBField(default_sql="now()")
        description: str | None

class SomeTable(DBTable):
    name: str
    opt: int | None = None

    def __str__(self):
        return self.name

class One(DBTable):
    name: str
    numbers: int

class Two(DBTable):
    name: str
    numbers: int


class BookA(DBTable):
    title: str
    description: Text | None
    author: str | None
    year: int | None
    pages: int | None

    @classmethod
    def _view_(cls, item: DBQueryField[typing.Self]):
        return item.title

class Storage(DBTable):
    book: BookA
    qty: int

    def __str__(self):
        return f'{self.book.pk}:: {self.book} -> {self.qty}'


class Customer(DBTable):
    name: str

class Sale(DBTable):
    sell_date: datetime
    customer: Customer
    amount: float


class File(DBTable):
    name: str
    size: int

class FileDanger(File):
    _validate_ = False

class Test(DBTable):
    a: int
    b: int

    @property
    def c(self) -> int:
        return self.a + self.b

    @staticmethod
    def c__view(x: DBQueryField['Test']) -> DBQuery:
        return x.a + x.b

    @property
    def d(self) -> int:
        return self.a * self.b

class Config(DBTable):
    data: FieldBody
    last_request: Property[datetime] = lambda x: datetime.now()


class Order(DBTable):
    name: str

class Row(DBTable):
    _use_slots_ = True
    order: Order


if __name__ == "__main__":
    #db = DBFactory.postgres("postgresql://quazy:quazy@127.0.0.1/quazy")
    db = DBFactory.sqlite("file:quazy.db?mode=rwc")
    db.bind_module()

    db.clear()
    db.create()

    for i in range(100):
        db.insert(Product(name=f'Product #{i + 1}', price=random.randint(1, 1000) / 100))

    q = Product.query().filter(lambda x: x.price >= 5)
    print("Total amount:", q.fetch_count())
    print("Average price:", q.fetch_avg("price"))
    print("Products:")
    for x in q:
        print(x.name, "->", x.price)


    box1 = Box(location="Top left corner").save()
    box2 = Box(location="Top right corner").save()
    Fruit(name="apple", box=box1).save()
    box2.fruits.append(Fruit(name="banana", box=box2))
    box2.save()

    for x in Fruit.select("name", location=lambda x: x.box.location):
        print(x)

    for x in Box.select("location", fruit=lambda x: x.fruits.name):
        print(x)

    r = Receipt()
    r.items.append(Receipt.Item(name="Cheese", price=1100, qty=0.1))
    r.items.append(Receipt.Item(name="Sausage", price=160, qty=0.5))
    r.items.append(Receipt.Item(name="Milk", price=56, qty=1))
    db.insert(r)

    q = Receipt.Item.select("name", "total").filter(receipt=r)
    print("Total sum:", q.fetch_sum("total"))
    print("Items:", ", ".join(q.fetch_list()))


    b1 = Book(name="Alice in wonderland").save()
    b2 = Book(name="Rust for noobies").save()
    b3 = Book(name="Backside of the life").save()
    s1 = Seller(name="Fancy Books").save()
    s2 = Seller(name="Alibazon").save()
    s3 = Seller(name="Booksabon").save()
    s1.books.append(b1)
    s1.books.append(b3)
    s1.save()
    s2.books.append(b2)
    s2.books.append(b3)
    s2.save()
    s3.books.append(b1)
    s3.books.append(b2)
    s3.save()
    q = Book.select(seller="sellers.name").filter(name="Alice in wonderland")
    print(q.fetch_list())

    print("Books:", Seller.get(name="Alibazon").books.fetch())

    s = Seller.get(name="Alibazon")
    s.load()
    for b in s.books.fetch():
        print(b.name)

    b = Book.get(name="Rust for noobies")
    b.load()
    for s in b.sellers:
        print(s.name)


    j = User(name="John").save()
    b = User(name="Bob").save()
    Task(title="Task 1", sender=b, receiver=j).save()
    Task(title="Task 2", sender=b, receiver=j).save()
    Task(title="Task 3", sender=j, receiver=b).save()

    q = User.select("name", task=lambda x: x.tasks_received.title).where(name="John")
    print(q.fetch_all(as_dict=True))

    u = User.get(name="John")
    for t in u.tasks_received.fetch():
        print(t.title)


    SomeTable(name="One").save()
    SomeTable(name="Two").save()

    qs = SomeTable.query().filter(lambda x: x.opt.is_null)
    print(qs.fetch_all())

    q7 = SomeTable.query()
    q7.filter(name=q7.var("name"))

    for n in ('One', 'Two'):
        q7["name"] = n
        item = q7.fetch_one()
        print(item)

    for n in ('One', 'Two'):
        with SomeTable.query() as q8:
            q8.reuse()
            q8.filter(name=q8.var("name")) # this line runs once
            print("hello once")
        q8["name"] = n
        print(q8.fetch_one())

    q8b = SomeTable.query()
    q8b.filter(name=q8b.var("name")).freeze()

    for n in ('One', 'Two'):
        q8b["name"] = n
        print(q8b.fetch_one())

    with open("test_short.pyi", "wt") as f:
        f.write(gen_stub(db))

    One(name="Con", numbers=3785).save()
    One(name="Sir", numbers=6566).save()
    One(name="Mon", numbers=1554).save()

    Two(name="Phaz", numbers=9985).save()
    Two(name="Jorn", numbers=6566).save()
    Two(name="Dil", numbers=3154).save()

    q = db.query()
    q.select(one="ones.name", two="twos.name")
    q.filter(lambda x: x.ones.numbers == x.twos.numbers)
    for x in q[:]:
        print(x)

    b1 = BookA(title="Alice in wonderland", description="A good book for kid").save()
    b2 = BookA(title="Rust for noobies", description="Not for kids").save()
    b3 = BookA(title="Backside of the life", description="For zombies").save()

    Storage(book=b1, qty=5).save()
    Storage(book=b2, qty=8).save()
    Storage(book=b3, qty=10).save()

    for x in Storage.query():
        print(x.book, ':', x.book.description)

    for x in Storage.select("book", "qty"):
        print(x)
        #print(BookA[x.book_id].description)

    db._debug_mode = True
    q = Sale.query()
    print("Rows:", q.fetch_count())
    print("Total:", q.fetch_sum("amount"))

    q = Customer.query()
    q.select("name", total=lambda x: x.sales.amount.sum)
    q.filter(lambda x: x.sales.sell_date > datetime.now() - timedelta(days=7))
    q.filter(lambda x: q['total'] > 1000)
    for x in q:
        print(x)

    File(name="test.txt", size=1024)
    File(name="test2.txt", size='1024')
    try:
        File(name=123, size='1024')
    except QuazyFieldTypeError as e:
        print(e)
    FileDanger(name=123, size='1024')

    t = Test(a=1, b=2).save()
    print(t.c, t.d)

    db._debug_mode = True
    t2 = Test.select("c", "d").filter(lambda x: (x.c == 3) & (x.d == 2)).fetch_one()
    print(t2.c, t2.d)

    c = Config().save()
    print(c.last_request)

    o = Order(name="test")
    o.rows.append(Row())
    o.save()
    # it is OK

    db.close()