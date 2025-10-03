Getting started
###############

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
