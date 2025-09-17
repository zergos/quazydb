Why another ORM?
################

From all modern frameworks like `Django`_, `SQLAlchemy`_ , `PonyORM`_ and even `peewee`_ we have many beautiful instruments to perform
many task. They cover wide area of database management problems, each focused on specific approach.

But, let's say I have my own way of beautiful vision of how the things should be the way I would love to.

.. _Django: https://www.djangoproject.com/
.. _SQLAlchemy: https://www.sqlalchemy.org/
.. _PonyORM: https://ponyorm.org/
.. _peewee: https://docs.peewee-orm.com/en/latest/

Models definition
=================

Use dataclasses
---------------

Why don't use typing templates from python bundled `dataclasses <https://docs.python.org/3/library/dataclasses.html>`__?

.. code-block::

   class Measure(DBTable):
       name: str
       weight: float = 1.0

   class Product(DBTable):
       name: str
       base_measure: Measure
       description: str = None

It has enough information about types and default values. Isn't it?

Subclasses
----------

It is very handy to declare strictly one-two-many related tables as a subclass.

.. code-block::

   class Product(DBTable):
       name: str
       base_measure: Measure
       description: str = None

       class Measure(DBTable):
           measure: Measure
           cnt: int


Many-to-many
------------

Let's make many-to-many relation definition simpler:

.. code-block::

   class User(NamedTable):
       apps: ManyToMany[App]

   class App(NamedTable):
       users: ManyToMany[User]


or similar for one-to-many relation:

.. code-block::

   class User(NamedTable):
       app: 'App'

   class App(NamedTable):
       users: Many[User] # this meta-fields would be provided implicitly


JSON fields
-----------

I like simple migrations for object-oriented databases. So, let's decribe JSONb content fields explicitly.

.. code-block::

   class Journal(NamedTable):
       body: FieldBody
       title: Property[str]
       price: Property[float]
       group: Property[GroupCatalog]
       pub_date: Property[datetime]

Here is `body` column typed JSONb in the table `Journal`. And other fields are just content in it.

Polymorphic entities
--------------------

I like to store similar entities in one table, but separate logically.

.. code-block::

   class Catalog(DBTable):
       _extendable_ = True
       cid: FieldCID[str]
       name: str

   class ItemCatalog(Catalog):
       unit: Unit

   class GroupCatalog(Catalog):
       pass


Enumerated types
----------------

I like `Enum`, `IntEnum` and `StrEnum` features.

.. code-block::

   class Journal(NamedTable):
       class ContentClass(IntEnum):
           MEDIUM = 1
           BLOG = 2
           HIGHLIGHTS = 3

       cc: Journal.ContentClass

Queries
=======


Lambdas way
-----------

Select fields by names or calculate by `lambdas`:

..  code-block::

    query = db.query(Item).select("name", "base_unit", unit=lambda x: x.base_unit.name)

    fields = query.describe()
    for f in fields:
        print(f'{f.name} - {f.type.__name__}')

    print(query.fetchall())

Filter by lambdas:

..  code-block::

    last_date = datetime.now() - timedelta(days=7)
    query = db.query(News).filter(lambda x: x.created_at >= last_date).select('title')

    for title in query.fetch_list():
        print(title)


Precompiled queries
-------------------

Let's use Postgres binary protocol to precompile and reuse queries.

..  code-block::

    with db.query() as q, q.get_scheme() as s: # this block runs once, query `q` is cached for miltiple run
        q.reuse()
        q.select(date=s.sales.date, date_sum=q.sum(s.sales.rows.qty * s.sales.rows.unit.weight))
        q.sort_by(2)
        q.filter(s.sales.date >= day1 - timedelta(days=5))
        q.filter(q.fields['date_sum'] > 80)
    with db.select(q) as res:
        for row in res:
            print(row)


Migrations
==========

Do you have growing database? No problem, flexible migrations bundled.

..  code-block::

    db = DBFactory.postgres(conninfo="postgresql://quazy:quazy@localhost/quazy")
    db.use_module()
    commands, new_tables = get_changes(db, 'public')
    apply_changes(db, 'public', commands, new_tables, 'my new migration')


IDE friendly
============

Just make `stub` file to provide little help to your IDE and other works will be performed by magic Generics.

..  code-block::

    with open("test_quazy.pyi", "wt") as f:
        f.write(gen_stub(db))

It redefines all implicit fields explicitly and adds constructors.
