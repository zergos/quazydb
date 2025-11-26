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

    # schema name for all tables in this module
    # "public" by default, if not specified
    _SCHEMA_ = "playground"

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
    print(q.fetch_all(as_dict=True))

    u = User.get(name="John").load("tasks_received")
    for t in u.tasks_received:
        print(t.title)

    # alternativery via `fetch`
    u = User.get(name="John")
    for t in u.tasks_received.fetch():
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
    print(q.fetch_list())

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
    print("Items:", ", ".join(q.fetch_list()))

Meta tables
===========

Normally, you can inherit tables one to another. It will create both tables in database with the same set of fields.

..  code-block:: python

    class Fruit(DBTable):
        name: str

    class Animal(Fruit):
        # "name" field inherited
        age: int

But if you have groups of common usable tables fields, you can use "meta" tables as base classes for your tables:

..  code-block:: python

    # everybody used to has a name
    class NamedTable(DBTable):
        _meta_ = True
        name: str

    class Fruit(NamedTable):
        pass

    class User(NamedTable):
        pass

    # let's make it globally distributed
    class GlobalTable(DBTable):
        _meta_ = True
        uuid: UUID = DBField(pk=True)

    class Sale(GlobalTable):
        pass

    class Transaction(GlobalTable):
        pass

    # use multiple meta-tables at once
    class Customer(NamedTable, GlobalTable):
        pass


Joined Table Inheritance
========================

Let's imaging you have several catalogs with it's own specific fields, but the same logic processing, storing and
presentation. It's usually a normal practice to store all such catalogs in one physical table.

It is supposed that one additional field must be provided, with table identifier, to separate datas.
In QuazyDB such field is provided via `FieldCID[]` annotation. Actual type could be any, but if it is a string, QuazyDB
engine fill it by table class name by default. Otherwise, `_discriminator_` value should be provided.

.. hint:: Query engine deals with this logic implicitly, adding proper folter to discriminator field for any requests.

In the example below only one table created in database, named `catalog`.

..  code-block:: python

    class Catalog(DBTable):
        _extendable_ = True
        cid: FieldCID[str]
        number: int

    class Supplier(Catalog):
        name: str
        agreement: str | None

    class Customer(Catalog):
        name: str
        start_date: datetime
        vip_class: int | None

    Supplier(number=99, name="Golden nuts").save()
    Customer(number=56, name="Hungry mouse").save()

    print(Supplier.select("name").fetch_list())
    print(Customer.select("name").fetch_list())


Lightweight JSON properties
===========================

Every modern database engine has a support to JSON field types. More then that, it gives rich features to use
in-JSON fields in SQL queries for sophisticated selections and filters.

..  admonition:: Why should we care?

    Let's imagine you have users registry on your social networking platform. How many fields you have to add to your
    `User` table to satisfy all needs? You can't be sure about amount, but you are pretty sure that every little change
    to a database could a painful enough. So, why don't you just put all user-specific fields in one JSON field and
    forget about any migration pain ever?

QuazyDB is intruduced special annotation generic `Property[]`, which points that field belongs to JSON structure.
It is also obligated to specify special `body` (or any other name) field with type `BodyField`.

..  note::

    Property can not be marked as `required`, because it is in the essence of it's dynamic nature.
    It also can not have `default_sql` value.

..  code-block:: python

    # this table is created with only one column `body`
    class Journal(NamedTable):
        body: FieldBody
        title: Property[str]
        price: Property[float]
        pub_date: Property[datetime]

    Journal(title="Xakep", price=9.99, pub_date=datetime(2010, 1, 10)).save()
    Journal.get(title="Xakep").inspect()


IDE-friendly code completion
============================

To code even more faster, there are several IDE friendly tricks performed:
 * `Query` object is based on `Generic[T]`, where `T` is a specific `DBTable` class. It helps to access table fields
   for query results.
 * There are many fields implicitly created. To make it visible, generate stub `pyi` helper file.
 * Stub files also describe constructor arguments names.

..  code-block:: python

    from quazy.stub import gen_stub

    # generate stub file
    with open("test.pyi", "wt") as f:
        f.write(gen_stub(db))

There is an example of generated stub:

..  code-block:: python
    :caption: Source code

    class User(DBTable):
        name: str

    class Task(DBTable):
        title: str
        sender: User = DBField(reverse_name="tasks_sent")
        receiver: User = DBField(reverse_name="tasks_received")

        class History(DBTable):
            record_date: datetime = DBField(default_sql="now()")
            description: str | None

..  code-block:: python
    :caption: Stub file

    class User(DBTable):
        name: str
        id: int
        tasks_sent: list["Task"]
        tasks_received: list["Task"]
        def __init__(self, name: str = None, id: int = None, tasks_send: list["Task"] = None, tasks_received: list["Task"] = None): ...


    class Task(DBTable):
        title: str
        sender: "User"
        receiver: "User"
        id: int
        historys: list["Task.History"]
        def __init__(self, title: str = None, sender: "User" = None, receiver: "User" = None, id: int = None, historys: list["Task.History"] = None): ...

        class History(DBTable):
            record_date: datetime | None
            description: str | None
            id: int
            task: "Task"
            def __init__(self, record_date: datetime = None, description: str = None, id: int = None, task: "Task" = None): ...


Migrations
==========

Migration means put any modifications to the database schema. Quazy can analyze your object models and keep
data schema updated.

It is allowed to apply and revert any modification, moving via modifications tree.

There is additional table named `migration` created in schema `migrations` in the database when activated.
Each row contains migration index, schema tables snapshot and necessary commands list to perform changes
in the database.

Migrations module
-----------------

..  automodule:: quazy.migrations
    :members:

Example
-------

..  code-block:: python
    :caption: Initial migration

    class SomeTable(DBTable):
        name: str

    db = DBFactory.postgres(conninfo="postgresql://quazy:quazy@127.0.0.1/quazy")

    db.bind_module()
    db.clear()
    db.create()

    activate_migrations(db)
    diff = compare_schema(db)
    print("Initial:")
    print(diff.info())
    apply_changes(db, diff)


..  code-block:: python
    :caption: Any new migration

    class SomeTable(DBTable):
        name: str
        value: int

    diff = compare_schema(db)
    print(diff.info())
    apply_changes(db, diff)

..  code-block:: python
    :caption: Revert migration

    diff = compare_schema(db, migration_index="0001")
    print(diff.info())
    apply_changes(db, diff)

..  code-block:: python
    :caption: Rename table or field

    class AwesomeTable(DBTable):
        name: str
        integer_value: int

    diff = compare_schema(db, [("SomeTable", "AwesomeTable"), ("value", "integer_value")])
    print(diff.info())
    apply_changes(db, diff)

..  warning::

    Use renaming for refactoring and misspells correction to avoid deletion and possible data lost.


Data validation
===============

It is possible to enable data validaton with `pydantic` module. Validation is enabled by default if this module is
installed, otherwise, `QuazyDB` should be installed by specifying this explicitly::

    pip install quazydb[strict]

Validation example:

..  code-block:: python

    class File(DBTable):
        name: str
        size: int

    class FileDanger(DBTable):
        _validate_ = False

        name: str
        size: int

    File(name="test.txt", size=1024)
    # this is good
    File(name="test2.txt", size='1024')
    # this is also good, because '1024' is a valid number (by powers of `pydantic`)
    File(name=123, size='1024')
    # quazy.exceptions.QuazyFieldTypeError: Field `name` in `File` has wrong type: 1 validation error for str
    #  Input should be a valid string [type=string_type, input_value=123, input_type=int]
    FileDanger(name=123, size='1024')
    # this is ok (take your own care for saving to database)


Evaluated fields
================

There is an opportunity to create evaluated fields, just in case storing is not needed.

..  code-block:: python

    class Test(DBTable):
    a: int
    b: int

    # regular field, defined as property for runtime evaluation
    @property
    def c(self) -> int:
        return self.a + self.b

    # field, defined as static method for SQL evaluation, always postfixed with `__view`
    # exposed with query builder only
    @staticmethod
    def c__view(x: DBQueryField[Test]) -> DBQuery:
        return x.a + x.b

    # another field, defined as property, but without mapped view function
    # if this expression is query compatible - query builder will call it implicitly
    @property
    def d(self) -> int:
        return self.a * self.b

    t = Test(a=1, b=2).save()
    print(t.c, t.d)

    # `c` is selected by `c__view` method and `d` is selected by property expression
    t2 = Test.select("c", "d").filter(lambda x: (x.c == 3) & (x.d == 2)).fetch_one()
    print(t2.c, t2.d)

There is SQL generated::

    SELECT
        "test".a+"test".b AS "c",
        "test".a*"test".b AS "d"
    FROM "public"."test" AS "test"
    WHERE
        "test".a+"test".b=%(_arg_1)s AND "test".a*"test".b=%(_arg_2)s

