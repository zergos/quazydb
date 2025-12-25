import asyncio
import selectors
import typing
import random
from datetime import datetime

from quazy import DBFactory, DBTable, DBField, Many, ManyToMany, Text, DBQueryField, DBQuery, FieldBody, Property, \
    DBFactoryAsync


class Product(DBTable):
    local_var: typing.ClassVar[str]
    name: str
    price: float
    description: str | None

class Book(DBTable):
    name: str
    sellers: ManyToMany['Seller']

    def __repr__(self):
        return self.name

class Seller(DBTable):
    name: str
    books: ManyToMany[Book]

class Config(DBTable):
    data: FieldBody
    last_request: Property[datetime] = lambda x: datetime.now()


async def main():
    #db = DBFactoryAsync.postgres_pool("postgresql://quazy:quazy@127.0.0.1/quazy")
    db = DBFactoryAsync.sqlite("file:quazy.db?mode=rwc")
    db.bind_module()

    await db.clear()
    await db.create()

    for i in range(100):
        await db.insert(Product(name=f'Product #{i + 1}', price=random.randint(1, 1000) / 100))

    q = Product.query().filter(lambda x: x.price >= 5)
    print("Total amount:", await q.fetch_count())
    print("Average price:", await q.fetch_avg("price"))
    print("Products:")
    async for x in q:
        print(x.name, "->", x.price)

    b1 = await Book(name="Alice in wonderland").save()
    b2 = await Book(name="Rust for noobies").save()
    b3 = await Book(name="Backside of the life").save()
    s1 = await Seller(name="Fancy Books").save()
    s2 = await Seller(name="Alibazon").save()
    s3 = await Seller(name="Booksabon").save()
    s1.books.append(b1)
    s1.books.append(b3)
    await s1.save()
    s2.books.append(b2)
    s2.books.append(b3)
    await s2.save()
    s3.books.append(b1)
    s3.books.append(b2)
    await s3.save()
    q = Book.select(seller="sellers.name").filter(name="Alice in wonderland")
    print(await q.fetch_list())

    print("Books:", await (await Seller.get(name="Alibazon")).books.fetch())

    s = await Seller.get(name="Alibazon")
    await s.load()
    for b in await s.books.fetch():
        print(b.name)

    b = await Book.get(name="Rust for noobies")
    await b.load()
    for s in b.sellers:
        print(s.name)

    c = await Config().save()
    print(c.last_request)

if __name__ == "__main__":
    asyncio.run(main(), loop_factory=lambda: asyncio.SelectorEventLoop(selectors.SelectSelector()))
