Getting started
###############

**Python 3.10+** is supported.
Including new **Python 3.14** type hinting changes.

Supported database connectors:

    psycopg
    sqlite3

Install the module::

    pip install quazydb

Optionally, prepare empty database on Postgres::

    CREATE DATABASE quazy;
    CREATE USER quazy WITH PASSWORD 'quazy';
    GRANT ALL PRIVILEGES ON DATABASE quazy TO quazy;

..  note::

    Started from version `1.2.1` `psycopg` is not included in package dependencies. Since then, it should be specified explicitly:
    `pip install quazydb[psycopg]`

Everithing is ready to run your first script

..  code-block:: python

    # describe any tables
    class SomeData(DBTable):
        name: str

    # create connection via DBFactory
    # connect via SQLite
    db = DBFactory.sqlite("file:quazy.db?mode=rwc")
    # connect via Postgres
    db = DBFactory.postgres("postgresql://username:password@server/database")
    # connect via Postgres pool
    db = DBFactory.postgres_pool("postgresql://username:password@server/database")
    # bind all tables from current module
    db.bind_module()

    # clear and create all tables
    db.clear()
    db.create()

    # insert some data
    SomeData(name="hello").save()
    SomeData(name="world").save()

    # fetch data and print
    for x in SomeData.select():
        print(x.name)
