# quazydb

![Logo](/docs/source/images/logo_mini.png)

Powerful yet simple asynchronous Python ORM

Let's combine all modern ORMs with business essence into something intuitive and simple.

## Example:

```python
import random

from quazy import DBFactory, DBTable


class Product(DBTable):
    name: str
    price: float
    description: str = None


if __name__ == "__main__":
    db = DBFactory.postgres("postgresql://quazy:quazy@127.0.0.1/quazy")
    db._debug_mode = True
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
```

Or code in async:

```python
import asyncio
import random

from quazy import DBFactoryAsync, DBTable

class Product(DBTable):
    name: str
    price: float
    description: str = None


async def main():
    db = DBFactoryAsync.postgres("postgresql://quazy:quazy@127.0.0.1/quazy")
    db._debug_mode = True
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

if __name__ == "__main__":
    asyncio.run(main())
```

## Documentation

https://quazydb.readthedocs.io/en/latest/