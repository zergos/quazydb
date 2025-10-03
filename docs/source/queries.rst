Queiries
########

QyazyDB supports wide subset of SQL features, including expressions, filters, joins and grouping.

Theory
======

The query is represented by `DBQuery` class.

It is bound to a specific database `DBFactory` and optionally to a `DBTable`.

Queries, which is not bound to a table, works as cross-table queries.

Queiries works as subqueries for other queries.

It is possible to extract data row as dict

    row['col'] = "hello"

or as `SimpleNamespace`

    row.col = "hello"

or as `DBTable` instance

    row.col = "hello"
    row.save()

It is possible to request one row, list of rows, or interate through result rows sequentially.

It is possible to build queries as chain calls

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
    q.select(one="ones.name", two="twos.name")
    q.filter(lambda x: x.ones.numbers == x.twos.numbers)
    for x in q:
        print(x)
