Getting started
###############

Install the module::

    pip install quazydb

Prepare empty database::

    CREATE DATABASE quazy;
    CREATE USER quazy WITH PASSWORD 'quazy';
    GRANT ALL PRIVILEGES ON DATABASE quazy TO quazy;

Everithing is ready to run your first script

..  code-block:: python

    # describe any tables
    class SomeData(DBTable):
        name: str

    # create connection via DBFactory
    db = DBFactory.postgres(conninfo="postgresql://username:password@server/database")

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
