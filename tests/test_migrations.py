import os

from quazy import DBFactory
from quazy.migrations import *
from quazy.exceptions import QuazyError
from quazy.migrations import dump_changes

if __name__ == '__main__':
    db = DBFactory.postgres(conninfo="postgresql://quazy:quazy@127.0.0.1/quazy")

    db.bind_module("tests.migration_1")
    #db._debug_mode = True
    db.clear()
    db.create()

    activate_migrations(db)
    diff = compare_schema(db)
    print("Initial:")
    print(diff.info())
    apply_changes(db, diff)
    db.unbind()

    for i in range(2, 6):
        print("\nMigration #", i)
        db.bind_module(f'tests.migration_{i}')
        if i == 5:
            rename_list = [('value2', 'value2_renamed')]
        else:
            rename_list = None
        diff = compare_schema(db, rename_list)
        print(diff.info())
        apply_changes(db, diff)
        db.unbind()

    print("\nRevert to 0002:")
    diff = compare_schema(db, migration_index="0002")
    print(diff.info())
    apply_changes(db, diff)

    for mig in get_migrations_list(db):
        print(mig)

    print("\nRevert to 0003:")
    diff = compare_schema(db, migration_index="0003")
    print(diff.info())
    apply_changes(db, diff)

    for mig in get_migrations_list(db):
        print(mig)

    print("\nMigration #", 6)
    db.bind_module(f'tests.migration_{6}')
    diff = compare_schema(db, [('ExtraTabel', 'ExtraTable')])
    print(diff.info())
    apply_changes(db, diff)
    db.unbind()

    for mig in get_migrations_list(db):
        print(mig)

    print("\nRevert to 0005: (should fail)")
    try:
        diff = compare_schema(db, migration_index="0005")
    except QuazyError as e:
        print(e)

    try:
        os.mkdir("migrations")
    except FileExistsError:
        pass
    dump_changes(db, "public", "migrations")
