Queiries
########

QyazyDB supports wide subset of SQL features, including expressions, filters, joins and grouping.

Basics
======

The query is represented by `DBQuery` class.

It is bound to a specific database `DBFactory` and optionally to a `DBTable`.

Queries, which is not bound to a table, works as cross-table queries.

Queiries works as subqueries for other queries.

It is possible to extract data row as dict::

    row['col'] = "hello"

or as `SimpleNamespace`::

    row.col = "hello"

or as `DBTable` instance::

    row.col = "hello"
    row.save()

It is possible to request one row, list of rows, or interate through result rows sequentially.

It is possible to build queries as chain calls::

    db.query().select(...).filter(...).group_by(...).exclude(...).fetch_all()

Expressions
-----------

Query object allows to use lambda expressions in almost every method, like `select`, `filter`, 'sort_by` and so on.
It is possible to write Python-like expressions to make translated to SQL directly, but with several nuances.

+-------------------+-------------------------------+
| Python            | pSQL                          |
+===================+===============================+
| expr[1]           | expr[1]                       |
+-------------------+-------------------------------+
| expr[1:2]         | substr(expr, 1, 2)            |
+-------------------+-------------------------------+
| expr['a']         | expr->'a'                     |
+-------------------+-------------------------------+
| expr + a          | expr + a                      |
+-------------------+-------------------------------+
| expr - a          | expr - a                      |
+-------------------+-------------------------------+
| expr * a          | expr * a                      |
+-------------------+-------------------------------+
| expr / a          | expr / a                      |
+-------------------+-------------------------------+
| expr % a          | expr % a                      |
+-------------------+-------------------------------+
| expr ** a         | expr ^ a                      |
+-------------------+-------------------------------+
| abs(a)            | @(a)                          |
+-------------------+-------------------------------+
| -expr             | -expr                         |
+-------------------+-------------------------------+
| expr & a          | expr AND a                    |
+-------------------+-------------------------------+
| expr | a          | expr OR a                     |
+-------------------+-------------------------------+
| expr.bin_and(a)   | expr & a                      |
+-------------------+-------------------------------+
| expr.bin_or(a)    | expr | a                      |
+-------------------+-------------------------------+
| expr ^ a          | expr # a                      |
+-------------------+-------------------------------+
| ~expr             | NOT expr                      |
+-------------------+-------------------------------+
| expr.invert()     | ~expr                         |
+-------------------+-------------------------------+
| expr << a         | expr IN a                     |
+-------------------+-------------------------------+
| expr.lshift(a)    | expr << a                     |
+-------------------+-------------------------------+
| expr >> a         | expr >> a                     |
+-------------------+-------------------------------+
| expr == a         | expr = a                      |
+-------------------+-------------------------------+
| expr != a         | expr <> a                     |
+-------------------+-------------------------------+
| expr < a          | expr < a                      |
+-------------------+-------------------------------+
| expr > a          | expr > a                      |
+-------------------+-------------------------------+
| expr <= a         | expr <= a                     |
+-------------------+-------------------------------+
| expr >= a         | expr >= a                     |
+-------------------+-------------------------------+
| expr.as_integer   | expr::integer                 |
+-------------------+-------------------------------+
| expr.as_string    | expr::text                    |
+-------------------+-------------------------------+
| expr.as_float     | expr::double precision        |
+-------------------+-------------------------------+
| round(a)          | round(a)                      |
+-------------------+-------------------------------+
| trunc(a)          | trunc(a)                      |
+-------------------+-------------------------------+
| expr.contains(a)  | expr LIKE '%a%'               |
+-------------------+-------------------------------+
| expr.upper()      | expr.upper()                  |
+-------------------+-------------------------------+
| expr.lower()      | expr.lower()                  |
+-------------------+-------------------------------+
| len(expr)         | length(expr, 'UTF8')          |
+-------------------+-------------------------------+
| expr.is_null      | expr IS NULL                  |
+-------------------+-------------------------------+
| expr.is_not_null  | expr IS NOT NULL              |
+-------------------+-------------------------------+
| expr.left(n)      | left(expr, n)                 |
+-------------------+-------------------------------+
| expr.right()      | right(expr, n)                |
+-------------------+-------------------------------+
| expr.starswith(s) | left(expr, length(s)) = s     |
+-------------------+-------------------------------+
| expr.endswith()   | right(expr, length(s)) = s    |
+-------------------+-------------------------------+
| expr.min          | min(expr)                     |
+-------------------+-------------------------------+
| expr.max          | max(expr)                     |
+-------------------+-------------------------------+
| expr.count        | count(expr)                   |
+-------------------+-------------------------------+
| expr.count_       | count(distinct expr)          |
|    distinct       |                               |
+-------------------+-------------------------------+
| expr.avg          | avg(expr)                     |
+-------------------+-------------------------------+

..  note::

    There is no native overload for several kind of operators, like `OR`, `AND`, `NOT`, 'IN' etc. Actually, there is
    overloads supported, but it is limited to specifit returning types. So, `__or__` method should return `bool` and
    nothing more. As a trade-off we've reused bitwise operators for that purpose, like `|` -> `OR`, which
    evaluation priority is lower and it is needed to use parenhesis for this kind.

Examples
========

Single table
------------

..  code-block:: python

    class SomeTable(DBTable):
        col: str

    SomeTable(col="hello").save()
    db.insert(SomeTable(col="world"))

    # request all rows as `SomeTable` instances
    q1 = SomeTable.query()
    q2 = db.query(SomeTable)

    # request all rows as `SimpleNamespace` ala named tuple
    q1.select("name")
    for x in q1:
        print(x.name)

    # request all rows as dict
    for x in q1.fetch_all(as_dict=True):
        print(x['name'])

    # simple filter
    q4 = SomeTable.query().filter(name="world")
    q5 = SomeTable.query().filter(lambda x: x.name == "world")

    # iterate via elements
    for x in q4:
        print(x)

    for x in q5.fetch_all():
        print(x)

    for x in q4.select("name").fetch_values():
        print(x)

    with q4.execute(as_dict=True) as curr:
        for x in curr:
            print(x)

    # reuse queries
    q7 = SomeTable.query()
    q7.filter(name=q7.var("name"))

    for n in ('hello', 'world'):
        q7["name"] = n
        item = q7.fetch_one()
        print(item)

    # reuse query builders
    for n in ('hello', 'world'):
        with SomeTable.query() as q8:
            q8.reuse()
            q8.filter(name=q7.var("name")) # this line run once
            print("hello once")
        q8["name"] = n
        print(q8.fetch_one())

    # freeze quieries
    q9 = SomeTable.query()
    q9.filter(name=q9.var("name")).freeze()

    # at this point SQL code is statically generated and query is restricted against changes

    for n in ('One', 'Two'):
        q9["name"] = n
        print(q9.fetch_one())


Multiple tables
---------------

..  code-block:: python

    class One(DBTable):
        name: str
        numbers: int

    class Two(DBTable):
        name: str
        numbers: int

    One(name="Con", numbers=3785).save()
    One(name="Sir", numbers=6566).save()
    One(name="Mon", numbers=1554).save()

    Two(name="Phaz", numbers=9985).save()
    Two(name="Jorn", numbers=6566).save()
    Two(name="Dil", numbers=3154).save()

    q = db.query()
    # all tables are accessible from the schema by snake_names
    q.select(one="ones.name", two="twos.name")
    q.filter(lambda x: x.ones.numbers == x.twos.numbers)
    for x in q:
        print(x)


Aggregates
----------

Deal with aggregates without worries about `GROUP BY` and `HAVING` clauses.

..  code-block:: python

    class Customer(DBTable):
        name: str

    class Sale(DBTable):

        customer: Customer
        amount: float

    q = Sale.query()
    print("Rows:", q.fetch_count())
    print("Total:", q.fetch_sum("amount"))

    q = Customer.query()
    q.select("name", total=lambda x: x.sales.amount.sum)
    q.filter(lambda x: x.sales.sell_date > datetime.now() - timedelta(days=7))
    q.filter(lambda x: x.sales.amount.sum > 1000)

SQL code generated::

    SELECT
        "customer".name AS "name",
        sum("customer__sales".amount) AS "total"
    FROM "public"."customer" AS "customer"
    INNER JOIN "public"."sale" AS "customer__sales"
        ON "customer".id = "customer__sales".customer
    WHERE
        "customer__sales".sell_date>%(_arg_1)s
    GROUP BY
        1
    HAVING
        sum("customer__sales".amount)>%(_arg_2)s

Custom items represenantion
---------------------------

Let's imagine, you have a warehouse with books.

..  code-block:: python

    class Book(DBTable):
        title: str
        description: Text | None
        author: str | None
        year: int | None
        pages: int | None

    class Storage(DBTable):
        book: Book
        qty: int

    b1 = Book(title="Alice in wonderland", description="A good book for kid").save()
    b2 = Book(title="Rust for noobies", description="Not for kids").save()
    b3 = Book(title="Backside of the life", description="For zombies").save()

    Storage(book=b1, qty=5).save()
    Storage(book=b2, qty=8).save()
    Storage(book=b3, qty=10).save()


Now we want to request all your rests.

..  code-block:: python

    for x in Storage.select("book", "qty"):
        print(x)


As a result we get::

    Row(book=1, qty=5)
    Row(book=2, qty=8)
    Row(book=3, qty=10)

Not something we want to, because there are no book names. Let's extend query.

..  code-block:: python

    for x in Storage.select("book.title", "qty"):
        print(x)

It gives::

    Row(book_title='Alice in wonderland', qty=5)
    Row(book_title='Rust for noobies', qty=8)
    Row(book_title='Backside of the life', qty=10)


Looks better. But what if we want dive deeper to book properties?
It seems, we need to extract `id` explicitly and get a book by it.

..  code-block:: python

    for x in Storage.Select("book.id", "book.title", "qty"):
        print(x)
        print(Book[x.id].description)

We get::

    Row(book_id=1, book_title='Alice in wonderland', qty=5)
    A good book for kid
    Row(book_id=2, book_title='Rust for noobies', qty=8)
    Not for kids
    Row(book_id=3, book_title='Backside of the life', qty=10)
    For zombies

But it's even much better to define a presentation path with special method override

..  code-block:: python

    class Book(DBTable):
        title: str
        description: Text | None
        author: str | None
        year: int | None
        pages: int | None

        @classmethod
        def _view_(cls, item: DBQueryField[typing.Self]):
            return item.title

    for x in Storage.select("book", "qty"):
        print(x)

We are getting `__view` field now::

    Row(book=1, book__view='Alice in wonderland', qty=5)
    Row(book=2, book__view='Rust for noobies', qty=8)
    Row(book=3, book__view='Backside of the life', qty=10)

..  note::

    Method `_view_` should return `DBSQL` object, so it has to perform QuazyDB-compatible expression.
    Strings or any other values are unsupported.

Let's simplify even more and check actual SQL query:

..  code-block:: python

    for x in Storage.query():
        print(x)

Actual query::

    SELECT
        "storage".book AS "book",
        "storage__books".title AS "book__view",
        "storage".qty AS "qty",
        "storage".id AS "id"
    FROM "public"."storage" AS "storage"
    INNER JOIN "public"."book" AS "storage__books"
        ON "storage".book = "storage__books".id

Results::

    Storage[1]
    Storage[2]
    Storage[3]

Results doesn't seem interesing. Let's make it human-readable

..  code-block:: python

    class Storage(DBTable):
        book: BookA
        qty: int

        def __str__(self):
            return f'{self.book} -> {self.qty}'

And new results::

    Alice in wonderland -> 5
    Rust for noobies -> 8
    Backside of the life -> 10

Row `id` is accessible also via `pk` property:

..  code-block:: python

    class Storage(DBTable):
        book: BookA
        qty: int

        def __str__(self):
            return f'{self.book.pk}:: {self.book} -> {self.qty}'

Results::

    1:: Alice in wonderland -> 5
    2:: Rust for noobies -> 8
    3:: Backside of the life -> 10

More then that is it even possible to perform implicit `get`

..  code-block:: python

    for x in Storage.query():
        print(x)
        print(x.book.description)

Voila::

    1:: Alice in wonderland -> 5
    A good book for kid
    2:: Rust for noobies -> 8
    Not for kids
    3:: Backside of the life -> 10
    For zombies

Such operation performs simple query for each execution, with no caching. Use with aware::

    SELECT
        "book".description AS "description"
    FROM "public"."book" AS "book"
    WHERE
        "book".id=%(_arg_1)s

There is a helper method to fetch all fields or related item and cache for a further usage.

..  code-block:: python

    for x in Storage.query():
        print(x)
        # call `fetch` one time and no further calls necessary
        print(x.book.fetch().description)
        # just use `x.book.description` after

SQL `CASE` statement
--------------------

QuazyDB supports SQL `CASE ...` syntax out-of-the box:

..  code-block:: python

    class User(DBTable):
        name: str
        age: int

    q = db.query(User)
    c = q.case().
        condition("baby", lambda x: x.age <= 1).
        condition("toddler", lambda x: x.age <= 3).
        condition("kid", lambda x: x.age < 18).
        default("adult")
    for x in q.select("name", age_category=c):
        print(x.name, "is a", x.age_category)

Chained selection
-----------------

QuazyDB has strong support of One-to-Self relations, in case you build hierarchy of elements.

..  code-block:: python

    class Chained(DBTable):
        name: str
        next: 'Chained | None'

        @classmethod
        def _view_(cls, item):
            return item.name


    # prefill table
    for i in range(20):
        db.insert(Chained(name=f"Chained #{i+1}"))
    # update with random links
    for i in range(20):
        link = i+2+randint(0, 3)
        if link < 20:
            Chained[i+1].save(next=link)

    q = db.query(Chained2).chained("id", "next", 1)

    for x in q:
        print(x.name, "==>", x.next)

The output would randomly get to this::

    Chained #1 ==> Chained #3
    Chained #3 ==> Chained #7
    Chained #7 ==> Chained #9
    Chained #9 ==> Chained #12
    Chained #12 ==> Chained #14
    Chained #14 ==> Chained #16
    Chained #16 ==> Chained #18
    Chained #18 ==> Chained[None]
