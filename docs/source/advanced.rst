Advanced usage
##############

Fields declaration
==================

Types
-----

+-----------+------------------+
| Python    | pSQL             |
+===========+==================+
| int       | integer          |
+-----------+------------------+
| float     | double precision |
+-----------+------------------+
| str       | text             |
+-----------+------------------+
| bytes     | bytea            |
+-----------+------------------+
| datetime  | timestamp        |
+-----------+------------------+
| time      | time             |
+-----------+------------------+
| date      | date             |
+-----------+------------------+
| timedelta | interval         |
+-----------+------------------+
| bool      | boolean          |
+-----------+------------------+
| dict      | jsonb            |
+-----------+------------------+
| UUID      | uuid             |
+-----------+------------------+
| IntEnum   | integer          |
+-----------+------------------+
| StrEnum   | text             |
+-----------+------------------+

Declaration
-----------

..  code-block:: python

    from quazy import DBTable, DBField, Optional

    class Customer(DBTable):
        # just a simple field definition, required by default (couldn't be None)
        simply_name: str
        # field could be None
        required_prop: Optional[str]
        required_alt: str = DBField(required=False)
        # indexed for fast search
        lookup_field: str = DBField(indexed=True)
        unique_field: str = DBField(unique=True)
        # default values
        predef_simple: str = "ABC"
        predef_alt: str = DBField(default="ABC")
        predef_lambda: str = lambda x: x.simply_name + "XYZ"
        predef_by_sql: str = DBField(default_sql="now()")

Enumerated types
----------------

Just use builtin types: `IntEnum` and `StrEnum`

..  code-block:: python

    from quazy import DBTable, IntEnum

    class Customer(DBTable):
        class Level(IntEnum):
            BASIC = 1
            VERIFIED = 2
            VIP = 10

        name: str
        level: Customer.Level = Customer.Level.BASIC

    db.insert(Customer(name="John", level=Customer.Level.VIP))

Primary key
-----------

It is supposed that each table has it's own primary key, even if it's not explicitly declared.
In cases you don't bother, Quazy can create `id: int` key field implicitly.

Note:
    There is only one primary key per data table supported yet.

..  code-block:: python

    from quazy import DBTable, DBField

    class Customer(DBTable):
        # there is implicit declaration of `id` field
        name: str

    class CustomerWithId(DBTable):
        myid: int = DBField(pk=True)
        name: str

    class CustomerWithUUID(DBTable):
        # UUID type is also supported
        uid: UUID = DBField(pk=True)
        name: str

Referenced tables
=================

One-to-many relations
---------------------

..  code-block:: python

    class Box(DBTable):
        location: str

    class Fruit(DBTable):
        name: str
        box: Box

    box1 = Box(location="Top left corner").save()
    box2 = Box(location="Top right corner").save()
    Fruit(name="apple", box=box1).save()
    box2.fruits.append(Fruit(name="banana", box=box2))
    box2.save()

    for x in Fruit.select("name", location=lambda x: x.box.location):
        print(x)

    for x in Box.select("location", fruit=lambda x: x.fruits.name):
        print(x)

In the example above `Box` object has implicit field `fruits`, having all fruits in it.
It is also possible specify "reverse" field name explicitly.

..  code-block:: python

    class User(DBTable):
        name: str

    class Task(DBTable):
        title: str
        sender: User = DBField(reverse_name="tasks_send")
        receiver: User = DBField(reverse_name="tasks_received")

    j = User(name="John").save()
    b = User(name="Bob").save()
    Task(title="Task 1", sender=b, receiver=j).save()
    Task(title="Task 2", sender=b, receiver=j).save()
    Task(title="Task 3", sender=j, receiver=b).save()

    q = User.select("name", task=lambda x: x.tasks_received.title).where(name="John")
    print(q.fetchall(as_dict=True))

    u = User.get(name="John").load("tasks_received")
    for t in u.tasks_received:
        print(t.title)

Many-to-many relations
----------------------

..  code-block:: python

    class Book(DBTable):
        name: str
        sellers: 'Many[Seller]'

    class Seller(DBTable):
        name: str
        books: Many[Book]

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
    print(q.fetchlist())

Substitute tables
=================

..  code-block:: python

    class Receipt(DBTable):
        created_at: datetime = DBField(default_sql="now()")

        class Item(DBTable):
            name: str
            price: float
            qty: float
            total: float = DBField(default=lambda x: x.price * x.qty)

    r = Receipt()
    r.items.append(Receipt.Item(name="Cheese", price=1100, qty=0.1))
    r.items.append(Receipt.Item(name="Sausage", price=160, qty=0.5))
    r.items.append(Receipt.Item(name="Milk", price=56, qty=1))
    db.insert(r)

    q = Receipt.Item.select("name", "total").filter(receipt=r)
    print("Total sum:", q.fetch_sum("total"))
    print("Items:", ", ".join(q.fetchlist()))

Extendable tables
=================

